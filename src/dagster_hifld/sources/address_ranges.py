"""Candidate-only capture of the official Census TIGER national Address Ranges.

The retired HIFLD Open inventory row pins the producer's own national Address
Range geodatabase download. This adapter discovers the currently maintained
national publication of the same product on www2.census.gov and captures it
losslessly as the staged canonical source. The archive holds one attribute-only
FileGDB layer, ``Address_Ranges``, whose native row identifiers are contiguous
GDB OBJECTIDs. The DataLumos record is banned identification context and is
never fetched.
"""
from __future__ import annotations

import hashlib
import json
import platform
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyogrio
import pyogrio.raw
import pyproj

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import processing_identity

DATASET_SLUG = "address-ranges"
FILE_SLUG = DATASET_SLUG
GEODATABASE_PRODUCT = "addr"
FEATURE_COUNT_RANGE = (25_000_000, 50_000_000)
SOURCE_HOST = "www2.census.gov"
PUBLISHER = "U.S. Census Bureau, Geography Division"
MAX_ARCHIVE_BYTES = 2_100_000_000
MAX_UNCOMPRESSED_BYTES = 5_500_000_000
MAX_ARCHIVE_MEMBERS = 256
LAYER_NAME = "Address_Ranges"
SCAN_BATCH_ROWS = 200_000
# Native FileGDB layer declaration: (name, pyogrio read_info dtype, declared
# width). TLID/TFIDL/TFIDR are native doubles (read_info float64); every other
# column is a fixed-width string (read_info object). Widths arrive through the
# Arrow scan's GDAL field metadata; the read_info schema check pins names and
# physical kinds in declared order.
EXPECTED_FIELD_DECLARATIONS = [
    ("TLID", "float64", None),
    ("TFIDL", "float64", None),
    ("TFIDR", "float64", None),
    ("LINEARID", "object", 22),
    ("FULLNAME", "object", 100),
    ("FROMHN", "object", 12),
    ("TOHN", "object", 12),
    ("EDGE_MTFCC", "object", 5),
    ("ROAD_MTFCC", "object", 5),
    ("PARITYL", "object", 1),
    ("PARITYR", "object", 1),
    ("OFFSETL", "object", 1),
    ("OFFSETR", "object", 1),
    ("SIDE", "object", 1),
    ("ZIP", "object", 5),
    ("PLUS4", "object", 4),
    ("FROMTYP", "object", 1),
    ("TOTYP", "object", 1),
    ("ARID", "object", 22),
    ("MTFCC", "object", 5),
]
FIELD_NAMES = [name for name, _kind, _width in EXPECTED_FIELD_DECLARATIONS]
EXPECTED_FIELD_KINDS = [(name, kind) for name, kind, _width in EXPECTED_FIELD_DECLARATIONS]
EXPECTED_FIELD_WIDTHS = {name: width for name, _kind, width in EXPECTED_FIELD_DECLARATIONS
                         if width is not None}
DOMAIN_FIELDS = ("EDGE_MTFCC", "ROAD_MTFCC", "PARITYL", "PARITYR", "OFFSETL",
                 "OFFSETR", "SIDE", "FROMTYP", "TOTYP", "MTFCC")
DOUBLE_FIELDS = ("TLID", "TFIDL", "TFIDR")
# Identity fields observed live with zero nulls on 2026-11-15; a null in any
# of them fails the capture closed. ZIP carries 3,236 observed nulls and is
# therefore recorded, never required.
REQUIRED_NON_NULL_FIELDS = ("LINEARID", "ARID", "MTFCC")
# Fixed-width producer value patterns verified live with zero violations on
# 2026-11-15. FROMHN/TOHN are producer-stamped text up to the declared width:
# the population includes letter-prefixed and hyphenated Census formats
# ("N2600", "89-00") and quoted non-ASCII house numbers for Puerto Rico roads
# ('"Ñ00"', '"Ñ99"'), so the honest pinned contract is declared-width,
# control-character-free text rather than digits-only.
VALUE_PATTERNS = {
    "LINEARID": re.compile(r"^[0-9]{1,22}$"),
    "ZIP": re.compile(r"^[0-9]{5}$"),
    "PLUS4": re.compile(r"^[0-9]{4}$"),
    "FROMHN": re.compile(r"^[^\x00-\x1F\x7F]{1,12}$"),
    "TOHN": re.compile(r"^[^\x00-\x1F\x7F]{1,12}$"),
    "ARID": re.compile(r"^[0-9]{1,22}$"),
}
MAX_FULLNAME_LENGTH = 100
SCAN_DIGEST_SCHEME = (
    "sha256 over, in source row order, the concatenation of per-batch "
    f"prefixes 'batch:{SCAN_BATCH_ROWS}:<batch_index>\\n' followed by each "
    "declared column's canonical compact JSON array "
    "(separators ',' ':' , ensure_ascii, allow_nan=False) for that batch"
)
OBJECTID_DIGEST_SCHEME = (
    "sha256 over canonical JSON of the pinned native OBJECTID roster bounds "
    "after a full vectorized verification that the roster is exactly the "
    "contiguous ascending range 1..row_count with no duplicates or gaps"
)

LINEAGE = (
    "The HIFLD Open inventory row 'Address Ranges' pins the producing agency's "
    "own download, the Census TIGER national Address Range geodatabase. This "
    "pipeline discovers the producer's currently maintained national publication "
    "of the same product from www2.census.gov and captures the exact archive "
    "selected by that listing. The DHS crosswalk (DOI 10.3886/E241367V1) row and "
    "equivalence to the retired HIFLD publication remain unverified. The "
    "DataLumos archive record is banned identification context only and is never "
    "fetched."
)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _canonical_source_key(release: census_latest.GeodatabaseRelease) -> str:
    return f"file_geodatabase/{release.source_file}"


def _processing_runtime() -> dict[str, str]:
    """Report runtime versions observed during processing without asserting Git identity."""
    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pyarrow": pa.__version__,
        "pyogrio": pyogrio.__version__,
        "gdal": ".".join(str(part) for part in pyogrio.__gdal_version__),
        "pyproj": pyproj.__version__,
        "proj": pyproj.proj_version_str,
        "httpx": httpx.__version__,
    }


def _reserve(current: int, upcoming: int) -> None:
    if current + upcoming > MAX_ARCHIVE_BYTES:
        raise ValueError("source_archive_capture_budget_exceeded")


def _require_accepted_transfer(receipt: dict, body: bytes,
                               release: census_latest.GeodatabaseRelease) -> None:
    """Apply the fresh-transfer acquisition policy to one transfer receipt.

    Both the live capture and the retained-transfer replay must accept the
    receipt's transfer completeness, HTTP status, final producer host,
    encoding and archive identity before any further validation; a matching
    archive hash alone does not establish receipt or source provenance.
    """
    source_host = str(urlsplit(release.source_url).hostname or "").lower()
    if (
        not receipt.get("transfer_complete")
        or not isinstance(receipt.get("status"), int)
        or not 200 <= receipt["status"] < 300
        or receipt.get("content_encoding") != "identity"
        or receipt.get("decoded_bytes_produced") != len(body)
        or receipt.get("wire_bytes_received") != len(body)
        or str(receipt.get("final_host") or "").lower() != source_host
        or not str(receipt.get("content_type") or "").lower().startswith("application/zip")
        or not body
        or len(body) > MAX_ARCHIVE_BYTES
        or receipt.get("requested_url") != release.source_url
        or receipt.get("final_url") != release.source_url
        or receipt.get("wire_sha256") != _sha256(body)
        or receipt.get("raw_sha256") != _sha256(body)
    ):
        raise ValueError("source_archive_transfer_or_identity_drift")


def _capture_archive(staging: StagingStorageResource, version: str | None,
                     client: httpx.Client,
                     release: census_latest.GeodatabaseRelease) -> tuple[bytes, dict, str]:
    """Stream one exact official archive, receipt it, and stage its canonical copy."""
    canonical_key = _canonical_source_key(release)
    wire = bytearray()
    attempt_observed_at = datetime.now(timezone.utc).isoformat()
    transfer_receipt_written = False
    try:
        with client.stream("GET", release.source_url, headers={"Accept-Encoding": "identity"}) as response:
            final_host = response.request.url.host
            result = capture_http_response(
                response,
                max_wire_bytes=MAX_ARCHIVE_BYTES,
                max_decoded_bytes=MAX_ARCHIVE_BYTES,
                reserve=lambda progress: _reserve(progress.wire_bytes_received, progress.reserve_bytes),
                retain_wire=wire.extend,
                retain_decoded=lambda _block: None,
            )
            receipt = {
                "method": "GET", "requested_url": release.source_url,
                "final_url": str(response.request.url), "final_host": final_host,
                "status": result.status_code,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "content_type": response.headers.get("content-type"),
                "last_modified": response.headers.get("last-modified"),
                "content_encoding": result.content_encoding,
                "content_length_header": result.content_length_header,
                "wire_bytes_received": result.wire_bytes_received,
                "wire_bytes_retained": result.wire_bytes_retained,
                "wire_sha256": result.wire_sha256,
                "decoded_bytes_produced": result.decoded_bytes_produced,
                "transfer_complete": result.complete,
                "transfer_error_code": result.error_code,
                "raw_archive_key": canonical_key,
                "raw_sha256": result.wire_sha256,
                "release_accepted": False,
            }
            body = bytes(wire)
            if version is None:
                identity = processing_identity(Path(__file__).parents[3])
                version = census_latest.release_version(
                    str(release.vintage), receipt["raw_sha256"], identity["code_commit"])
            staging.write(DATASET_SLUG, FILE_SLUG, version, canonical_key, body)
            staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json",
                          _json_bytes(receipt))
            transfer_receipt_written = True
            _require_accepted_transfer(receipt, body, release)
            return body, receipt, version
    except ValueError as exc:
        if not transfer_receipt_written:
            _retain_partial_transfer(staging, version or "unresolved", wire,
                                     attempt_observed_at, type(exc).__name__, release)
        raise
    except Exception as exc:
        _retain_partial_transfer(staging, version or "unresolved", wire,
                                 attempt_observed_at, type(exc).__name__, release)
        raise ValueError("source_archive_transport_error:" + type(exc).__name__) from exc


def _retain_partial_transfer(staging: StagingStorageResource, version: str, wire: bytearray,
                             observed_at: str, error_type: str,
                             release: census_latest.GeodatabaseRelease) -> None:
    partial = bytes(wire)
    partial_key = "raw/source_archive.partial.zip" if partial else None
    if partial_key:
        staging.write(DATASET_SLUG, FILE_SLUG, version, partial_key, partial)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.failure_receipt.json", _json_bytes({
        "method": "GET", "requested_url": release.source_url,
        "observed_at": observed_at,
        "wire_bytes_retained": len(partial),
        "wire_sha256": _sha256(partial),
        "partial_wire_key": partial_key,
        "transfer_error_type": error_type, "complete": False,
    }))


def _validate_archive(zip_path: Path,
                      release: census_latest.GeodatabaseRelease) -> dict[str, object]:
    """Require one intact bounded geodatabase archive with a safe roster."""
    try:
        with zipfile.ZipFile(zip_path) as source:
            entries = source.infolist()
            if not entries or len(entries) > MAX_ARCHIVE_MEMBERS:
                raise ValueError("source_archive_member_roster_drift")
            uncompressed_bytes = sum(item.file_size for item in entries)
            if uncompressed_bytes <= 0 or uncompressed_bytes > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("source_archive_expansion_limit_exceeded")
            if any(item.file_size > MAX_UNCOMPRESSED_BYTES for item in entries):
                raise ValueError("source_archive_member_limit_exceeded")
            for item in entries:
                parts = Path(item.filename).parts
                if Path(item.filename).is_absolute() or ".." in parts or item.filename.startswith(("/", "\\")):
                    raise ValueError("source_archive_unsafe_member_path")
            gdb_roots = {"/".join(item.filename.split("/")[:1]) for item in entries
                         if ".gdb/" in item.filename}
            if gdb_roots != {release.source_file[:-4]}:
                raise ValueError("source_archive_geodatabase_roster_drift")
            corrupt = source.testzip()
            if corrupt is not None:
                raise ValueError("source_archive_crc_failure")
            return {"member_count": len(entries),
                    "uncompressed_bytes": uncompressed_bytes}
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc


def _require_geodatabase_layout(zip_path: Path,
                                release: census_latest.GeodatabaseRelease) -> dict[str, object]:
    """Pin the single attribute-only producer layer and its physical schema."""
    try:
        layers = [str(row[0]) for row in pyogrio.list_layers(zip_path)]
        if layers != [LAYER_NAME]:
            raise ValueError("source_geodatabase_layer_roster_drift")
        info = pyogrio.read_info(zip_path, layer=LAYER_NAME, force_feature_count=True)
        feature_count = info.get("features")
        if (not isinstance(feature_count, int)
                or not FEATURE_COUNT_RANGE[0] <= feature_count <= FEATURE_COUNT_RANGE[1]):
            raise ValueError("source_feature_count_drift")
        if info.get("geometry_type") is not None or info.get("crs") is not None:
            raise ValueError("source_attribute_only_contract_drift")
        observed = [(str(name), str(kind))
                    for name, kind in zip(info["fields"], info["dtypes"])]
        if observed != EXPECTED_FIELD_KINDS:
            raise ValueError("source_schema_or_field_order_drift")
        return {"layers": layers, "feature_count": feature_count,
                "geometry_type": None, "crs": None,
                "field_kinds": observed}
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc


def _verify_native_id_roster(zip_path: Path, expected_count: int) -> dict[str, int]:
    """Require contiguous unique native OBJECTIDs 1..N over one narrow pass."""
    try:
        frame = pyogrio.read_dataframe(zip_path, layer=LAYER_NAME, columns=["MTFCC"],
                                       fid_as_index=True, use_arrow=True)
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc
    ids = frame.index.to_numpy()
    if (expected_count <= 0 or len(ids) != expected_count
            or np.any(np.sort(ids) != np.arange(1, expected_count + 1, dtype=ids.dtype))):
        raise ValueError("source_native_id_population_drift")
    return {"min": 1, "max": expected_count, "count": len(ids)}


def _arrow_widths(batch: pa.RecordBatch) -> dict[str, int | None]:
    widths: dict[str, int | None] = {}
    for field in batch.schema:
        width = None
        if field.metadata:
            raw = field.metadata.get(b"GDAL:OGR:width")
            if raw is not None:
                width = int(raw)
        widths[field.name] = width
    return widths


def _scan_native_values(zip_path: Path, expected_count: int) -> dict[str, object]:
    """Chunked full-population scan pinning values, domains and a record digest."""
    nulls = {name: 0 for name in FIELD_NAMES}
    domains: dict[str, dict[object, int]] = {name: {} for name in DOMAIN_FIELDS}
    violations = {name: 0 for name in VALUE_PATTERNS}
    violations["integral_doubles"] = 0
    violations["fullname_length"] = 0
    violations["fullname_max_length"] = 0
    digest = hashlib.sha256()
    rows = 0
    batch_index = 0
    try:
        with pyogrio.raw.open_arrow(zip_path, layer=LAYER_NAME, batch_size=SCAN_BATCH_ROWS) as opened:
            _metadata, stream = opened
            reader = pa.RecordBatchReader.from_stream(stream)
            for batch in reader:
                batch_index += 1
                if [field.name for field in batch.schema] != FIELD_NAMES:
                    raise ValueError("source_arrow_field_roster_drift")
                observed_widths = _arrow_widths(batch)
                if {name: observed_widths.get(name) for name in EXPECTED_FIELD_WIDTHS} != EXPECTED_FIELD_WIDTHS:
                    raise ValueError("source_arrow_field_width_drift")
                _validate_batch(batch, nulls, domains, violations, digest, batch_index)
                rows += batch.num_rows
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_scan_error:" + type(exc).__name__) from exc
    if rows != expected_count or batch_index == 0:
        raise ValueError("source_scan_row_count_drift")
    return {
        "scan_batches": batch_index,
        "scan_batch_rows": SCAN_BATCH_ROWS,
        "row_count": rows,
        "null_counts": {name: nulls[name] for name in FIELD_NAMES},
        "value_domains": {name: {str(key): count for key, count in sorted(
            domains[name].items(), key=lambda item: str(item[0]))} for name in DOMAIN_FIELDS},
        "value_violations": violations,
        "record_digest": digest.hexdigest(),
        "record_digest_scheme": SCAN_DIGEST_SCHEME,
    }


def _validate_batch(batch: pa.RecordBatch, nulls: dict[str, int],
                    domains: dict[str, dict[object, int]], violations: dict[str, int],
                    digest, batch_index: int) -> None:
    if [field.name for field in batch.schema] != FIELD_NAMES:
        raise ValueError("source_arrow_field_roster_drift")
    for name in FIELD_NAMES:
        column = batch.column(name)
        nulls[name] += column.null_count
        if name in VALUE_PATTERNS:
            matches = pc.match_substring_regex(column, VALUE_PATTERNS[name].pattern)
            violative = pc.sum(pc.if_else(pc.is_null(column), False, pc.invert(matches)))
            violations[name] += int(violative.as_py() or 0)
        if name in DOMAIN_FIELDS:
            for entry in pc.value_counts(column):
                key = entry["values"].as_py()
                if key is None:
                    continue
                domains[name][key] = domains[name].get(key, 0) + int(entry["counts"].as_py())
        if name in DOUBLE_FIELDS:
            non_null = pc.drop_null(column)
            if len(non_null):
                violations["integral_doubles"] += int(pc.sum(
                    pc.or_(pc.not_equal(non_null, pc.floor(non_null)),
                           pc.less(non_null, 0.0))).as_py() or 0)
        if name == "FULLNAME":
            lengths = pc.utf8_length(pc.fill_null(column, ""))
            maximum = pc.max(lengths).as_py()
            if maximum is not None and maximum > violations["fullname_max_length"]:
                violations["fullname_max_length"] = int(maximum)
            violations["fullname_length"] += int(pc.sum(
                pc.greater(lengths, MAX_FULLNAME_LENGTH)).as_py() or 0)
    for name in REQUIRED_NON_NULL_FIELDS:
        if nulls[name]:
            raise ValueError("source_required_value_null:" + name)
    if any(violations[name] for name in VALUE_PATTERNS):
        raise ValueError("source_value_pattern_drift")
    if violations["fullname_length"]:
        raise ValueError("source_fullname_length_drift")
    if violations["integral_doubles"]:
        raise ValueError("source_double_value_drift")
    digest.update(f"batch:{SCAN_BATCH_ROWS}:{batch_index}\n".encode())
    for name in FIELD_NAMES:
        digest.update(json.dumps(batch.column(name).to_pylist(), separators=(",", ":"),
                                 ensure_ascii=False, allow_nan=False).encode())


def _manifest(receipt: dict, identity: dict, replayed: bool,
              release: census_latest.GeodatabaseRelease,
              feature_count: int) -> dict[str, object]:
    return {
        "schema_version": 1,
        "title": "Address Ranges",
        "description": (
            "Draft lossless capture of the complete current Census TIGER national "
            "Address Ranges attribute-only FileGDB layer. Proposed source route for HIFLD "
            "inventory issue #408; DHS crosswalk row, historical HIFLD equivalence, schema/ID "
            "contract, baseline, rights and release acceptance remain unresolved."
        ),
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": release.source_url,
        "source_release": release.as_metadata(),
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "processing_identity": identity,
        "inputs": [{
            "input_id": f"census_tiger_address_range_geodatabase_{release.vintage}",
            "producer": PUBLISHER,
            "host": urlsplit(release.source_url).hostname,
            "role": "producer_publication",
            "canonical_url": release.source_url,
            "retrieval_url": receipt.get("final_url", release.source_url),
            "retrieved_at": receipt.get("observed_at"),
            "sha256": receipt["raw_sha256"],
            "bytes": receipt.get("wire_bytes_received"),
            "http_status": receipt.get("status"),
            "content_type": receipt.get("content_type"),
            "last_modified": receipt.get("last_modified"),
            "last_modified_meaning": (
                "HTTP Last-Modified on www2.census.gov; packaging evidence, not an "
                "independently published producer release date."
            ),
            "release_date_unknown_reason": (
                "The producer publishes this TIGER geodatabase edition without an explicit "
                "release date on the archive; the HTTP Last-Modified value is retained in the "
                "receipt and is not treated as a producer release date."
            ),
            "license": "Not adjudicated; source reuse terms and rights require maintainer review before release.",
            "attribution": (
                f"U.S. Census Bureau, TIGER national Address Range geodatabase "
                f"({release.vintage} edition); historical HIFLD attribution and reuse terms "
                "remain under review."
            ),
        }],
        "transformations": [
            {"name": "stage_lossless_source_archive", "parameters": {
                "canonical_object": _canonical_source_key(release),
                "recompression": "none", "extraction": "none",
                "attribute_only_layer": LAYER_NAME,
            }},
            {"name": "validate_native_population", "parameters": {
                "layer": LAYER_NAME, "row_count": feature_count,
                "native_objectid_roster": "contiguous 1..N verified",
                "value_domains_and_nulls": "recorded in metadata/native_capture.json",
                "schema_change": "none; native field names, order, widths and types pinned",
            }},
        ],
        "lineage": LINEAGE,
        "candidate_parquet_note": (
            "No prebuilt candidate parquet is produced by ingest; the catalog reads the "
            "staged canonical source (file_geodatabase) directly and any published "
            "conversion remains gated."
        ),
        "unsupported_output_formats": {
            "geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "Attribute-only source has no geometry; no display policy approved.",
        },
    }


def _capture_manifest(receipt: dict, integrity: dict, layout: dict, ids: dict,
                      scan: dict, replayed: bool,
                      release: census_latest.GeodatabaseRelease) -> dict[str, object]:
    return {
        "source_url": release.source_url,
        "source_host": urlsplit(release.source_url).hostname,
        "source_vintage": str(release.vintage),
        "source_release": release.as_metadata(),
        "archive_sha256": receipt["raw_sha256"], "archive_bytes": receipt.get("wire_bytes_received"),
        "archive_receipt": "raw/source_archive.receipt.json",
        "archive_last_modified": receipt.get("last_modified"),
        "archive_members_crc_checked": True,
        "archive_member_count": integrity["member_count"],
        "archive_uncompressed_bytes": integrity["uncompressed_bytes"],
        "geodatabase_root": release.source_file[:-4],
        "source_layer": LAYER_NAME,
        "source_layer_roster": layout["layers"],
        "attribute_only": True,
        "feature_count": int(layout["feature_count"]),
        "feature_count_range": list(FEATURE_COUNT_RANGE),
        "native_objectid": ids,
        "native_objectids_digest_scheme": OBJECTID_DIGEST_SCHEME,
        "scan": scan,
        **({"capture_replay": {
                "mode": "retained_transfer_replay",
                "original_retrieved_at": receipt.get("observed_at"),
                "new_network_retrieval_performed": False,
            }} if replayed else {"capture_mode": "live_http_capture",
                                 "new_network_retrieval_performed": True}),
        "refresh_semantics": (
            "Each live run discovers the producer's current national Address Ranges "
            "geodatabase and fails closed on any archive/identity drift; prior staged "
            "versions remain untouched."
        ),
        "release_accepted": False,
        "rights_contract_approved": False,
    }


def _validate_staged_population(staging: StagingStorageResource, version: str,
                                identity: dict, receipt: dict, replayed: bool,
                                release: census_latest.GeodatabaseRelease) -> dict[str, object]:
    """Validate the actual staged canonical bytes and return the capture evidence."""
    canonical_key = _canonical_source_key(release)
    staged_sha256 = staging.sha256_key(f"{DATASET_SLUG}/{FILE_SLUG}/{version}/{canonical_key}")
    if staged_sha256 != receipt["raw_sha256"]:
        raise ValueError("staged_source_archive_integrity_failure")
    with staging.get_local_version_dir(DATASET_SLUG, FILE_SLUG, version) as version_dir:
        zip_path = Path(version_dir) / canonical_key
        integrity = _validate_archive(zip_path, release)
        layout = _require_geodatabase_layout(zip_path, release)
        feature_count = int(layout["feature_count"])
        ids = _verify_native_id_roster(zip_path, feature_count)
        scan = _scan_native_values(zip_path, feature_count)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json",
                  _json_bytes(_manifest(receipt, identity, replayed, release, feature_count)))
    capture_manifest = _capture_manifest(receipt, integrity, layout, ids, scan, replayed, release)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/native_capture.json",
                  _json_bytes(capture_manifest))
    return {
        "feature_count": int(layout["feature_count"]),
        "archive_sha256": receipt["raw_sha256"],
        "archive_bytes": receipt.get("wire_bytes_received"),
        "record_digest": scan["record_digest"],
        "native_objectid": ids,
        "value_domains": scan["value_domains"],
        "null_counts": scan["null_counts"],
        "release_accepted": False,
    }


def capture(staging: StagingStorageResource, version: str | None = None,
            client: httpx.Client | None = None) -> dict[str, object]:
    """Discover the current release, capture it losslessly and validate its population."""
    owned = client is None
    client = client or httpx.Client(timeout=300, follow_redirects=True)
    release: census_latest.GeodatabaseRelease | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="address-ranges-discovery-") as temporary:
            release = census_latest.discover_geodatabase(
                client, Path(temporary), GEODATABASE_PRODUCT)
        _archive, receipt, version = _capture_archive(staging, version, client, release)
        identity = processing_identity(Path(__file__).parents[3])
        try:
            validate_construction_code_identity(
                Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
            )
        except Exception as exc:
            raise ValueError("source_code_identity_invalid:" + type(exc).__name__) from exc
        result = _validate_staged_population(
            staging, version, identity, receipt, replayed=False, release=release)
        return {**result, "version": version}
    except Exception as exc:
        staging.write(DATASET_SLUG, FILE_SLUG, version or "unresolved", "metadata/quarantine.json", _json_bytes({
            "status": "quarantined",
            "source_url": release.source_url if release is not None else None,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "failure_code": str(exc).split(":", 1)[0],
            "release_accepted": False,
        }))
        raise
    finally:
        if owned:
            client.close()


def replay_retained_capture(staging: StagingStorageResource, version: str, archive: bytes,
                            receipt_bytes: bytes) -> dict[str, object]:
    """Reprocess one retained transfer while preserving its original receipt.

    This path makes no new network request and does not invent a retrieval
    timestamp; it replays the byte-identical retained archive through the same
    validators so a completed transfer can be re-staged after a later step
    failed without re-downloading 1.75 GB. The retained receipt must satisfy
    the same fresh-transfer acquisition policy as a live capture before the
    replay proceeds; a matching archive hash alone does not establish receipt
    or source provenance.
    """
    try:
        receipt = json.loads(receipt_bytes)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("retained_capture_receipt_invalid") from exc
    if not isinstance(receipt, dict):
        raise ValueError("retained_capture_receipt_invalid")
    try:
        release = census_latest.geodatabase_release_from_receipt(receipt, GEODATABASE_PRODUCT)
    except ValueError as exc:
        raise ValueError("retained_capture_release_mismatch") from exc
    canonical_key = _canonical_source_key(release)
    if (receipt.get("raw_sha256") != _sha256(archive)
            or receipt.get("wire_bytes_retained") != len(archive)
            or receipt.get("raw_archive_key") != canonical_key
            or receipt.get("requested_url") != release.source_url
            or receipt.get("final_url") != release.source_url):
        raise ValueError("retained_capture_archive_receipt_mismatch")
    # Copy the original evidence bytes verbatim. This path is for a local
    # replay of an already observed transfer; it makes no new network request
    # and does not invent a retrieval timestamp.
    staging.write(DATASET_SLUG, FILE_SLUG, version, canonical_key, archive)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json", receipt_bytes)
    identity = processing_identity(Path(__file__).parents[3])
    try:
        _require_accepted_transfer(receipt, archive, release)
        validate_construction_code_identity(
            Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
        )
        result = {**_validate_staged_population(
            staging, version, identity, receipt, replayed=True, release=release),
            "version": version, "replayed": True}
        return result
    except Exception as exc:
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_url": release.source_url,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "failure_code": str(exc).split(":", 1)[0],
            "release_accepted": False,
            "replay": True,
        }))
        raise
