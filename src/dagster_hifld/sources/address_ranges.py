"""Candidate-only capture of the official Census TIGER national Address Ranges.

The retired HIFLD Open inventory row pins the producer's own download
(www2.census.gov TGRGDB24 ``tlgdb_2024_a_us_addr.gdb.zip``); the maintained
successor edition of the same national publication is the TGRGDB26 archive
``tlgdb_2026_us_addr.gdb.zip``, captured here losslessly as the staged
canonical source. The archive holds one attribute-only FileGDB layer,
``Address_Ranges``, whose native row identifiers are contiguous GDB OBJECTIDs.
The DataLumos record is banned identification context and is never fetched.
"""
from __future__ import annotations

import hashlib
import json
import platform
import re
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
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "address-ranges"
FILE_SLUG = DATASET_SLUG
SOURCE_VINTAGE = "2026"
SOURCE_URL = "https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_addr.gdb.zip"
SOURCE_FILE = "tlgdb_2026_us_addr.gdb.zip"
CANONICAL_SOURCE_KEY = f"file_geodatabase/{SOURCE_FILE}"
SOURCE_HOST = "www2.census.gov"
PUBLISHER = "U.S. Census Bureau, Geography Division"
# Archive identity observed live from the pinned producer URL on 2026-11-15
# (HTTP 200, identity encoding, 1,756,498,039 bytes); any drift fails closed.
EXPECTED_ARCHIVE_SHA256 = "73f3cd35d66a149684a6153c161ad6e94e7fb8a54530aae05f44e1e325ed72d4"
EXPECTED_ARCHIVE_BYTES = 1_756_498_039
EXPECTED_ARCHIVE_LAST_MODIFIED = "Wed, 29 Jul 2026 15:01:52 GMT"
MAX_ARCHIVE_BYTES = 2_100_000_000
MAX_UNCOMPRESSED_BYTES = 5_500_000_000
GDB_ROOT = "tlgdb_2026_us_addr.gdb"
LAYER_NAME = "Address_Ranges"
EXPECTED_FEATURE_COUNT = 37_730_671
EXPECTED_MEMBER_COUNT = 44
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
    "own download, the Census TIGER national Address Range geodatabase "
    "(TGRGDB24 tlgdb_2024_a_us_addr.gdb.zip). This pipeline captures the "
    "producer's currently maintained successor edition of the same national "
    "publication, TGRGDB26 tlgdb_2026_us_addr.gdb.zip (2026-07-29 directory "
    "stamp), directly from www2.census.gov. The DHS crosswalk "
    "(DOI 10.3886/E241367V1) row and equivalence to the retired HIFLD "
    "publication remain unverified. The DataLumos archive record is banned "
    "identification context only and is never fetched."
)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


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


def _require_accepted_transfer(receipt: dict, body: bytes) -> None:
    """Apply the fresh-transfer acquisition policy to one transfer receipt.

    Both the live capture and the retained-transfer replay must accept the
    receipt's transfer completeness, HTTP status, final producer host,
    encoding, headers and archive identity pins before any further
    validation; a matching archive hash alone does not establish receipt or
    source provenance.
    """
    if (
        not receipt.get("transfer_complete")
        or not isinstance(receipt.get("status"), int)
        or not 200 <= receipt["status"] < 300
        or receipt.get("content_encoding") != "identity"
        or receipt.get("decoded_bytes_produced") != len(body)
        or receipt.get("wire_bytes_received") != len(body)
        or str(receipt.get("final_host") or "").lower() != SOURCE_HOST
        or not str(receipt.get("content_type") or "").lower().startswith("application/zip")
        or len(body) != EXPECTED_ARCHIVE_BYTES
        or receipt.get("wire_sha256") != EXPECTED_ARCHIVE_SHA256
        or receipt.get("raw_sha256") != EXPECTED_ARCHIVE_SHA256
        or receipt.get("last_modified") != EXPECTED_ARCHIVE_LAST_MODIFIED
    ):
        raise ValueError("source_archive_transfer_or_identity_drift")


def _capture_archive(staging: StagingStorageResource, version: str, client: httpx.Client) -> tuple[bytes, dict]:
    """Stream one exact official archive, receipt it, and stage its canonical copy."""
    wire = bytearray()
    attempt_observed_at = datetime.now(timezone.utc).isoformat()
    transfer_receipt_written = False
    try:
        with client.stream("GET", SOURCE_URL, headers={"Accept-Encoding": "identity"}) as response:
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
                "method": "GET", "requested_url": SOURCE_URL,
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
                "raw_archive_key": CANONICAL_SOURCE_KEY,
                "raw_sha256": result.wire_sha256,
                "release_accepted": False,
            }
            body = bytes(wire)
            staging.write(DATASET_SLUG, FILE_SLUG, version, CANONICAL_SOURCE_KEY, body)
            staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json",
                          _json_bytes(receipt))
            transfer_receipt_written = True
            _require_accepted_transfer(receipt, body)
            return body, receipt
    except ValueError as exc:
        if not transfer_receipt_written:
            _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__)
        raise
    except Exception as exc:
        _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__)
        raise ValueError("source_archive_transport_error:" + type(exc).__name__) from exc


def _retain_partial_transfer(staging: StagingStorageResource, version: str, wire: bytearray,
                             observed_at: str, error_type: str) -> None:
    partial = bytes(wire)
    partial_key = "raw/source_archive.partial.zip" if partial else None
    if partial_key:
        staging.write(DATASET_SLUG, FILE_SLUG, version, partial_key, partial)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.failure_receipt.json", _json_bytes({
        "method": "GET", "requested_url": SOURCE_URL,
        "observed_at": observed_at,
        "wire_bytes_retained": len(partial),
        "wire_sha256": _sha256(partial),
        "partial_wire_key": partial_key,
        "transfer_error_type": error_type, "complete": False,
    }))


def _validate_archive(zip_path: Path) -> dict[str, object]:
    """Require one intact bounded geodatabase archive with the pinned roster."""
    try:
        with zipfile.ZipFile(zip_path) as source:
            entries = source.infolist()
            if len(entries) != EXPECTED_MEMBER_COUNT:
                raise ValueError("source_archive_member_roster_drift")
            if sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("source_archive_expansion_limit_exceeded")
            if any(item.file_size > MAX_UNCOMPRESSED_BYTES for item in entries):
                raise ValueError("source_archive_member_limit_exceeded")
            for item in entries:
                parts = Path(item.filename).parts
                if Path(item.filename).is_absolute() or ".." in parts or item.filename.startswith(("/", "\\")):
                    raise ValueError("source_archive_unsafe_member_path")
            gdb_roots = {"/".join(item.filename.split("/")[:1]) for item in entries
                         if ".gdb/" in item.filename}
            if gdb_roots != {GDB_ROOT}:
                raise ValueError("source_archive_geodatabase_roster_drift")
            corrupt = source.testzip()
            if corrupt is not None:
                raise ValueError("source_archive_crc_failure")
            return {"member_count": len(entries),
                    "uncompressed_bytes": sum(item.file_size for item in entries)}
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc


def _require_geodatabase_layout(zip_path: Path) -> dict[str, object]:
    """Pin the single attribute-only producer layer and its physical schema."""
    try:
        layers = [str(row[0]) for row in pyogrio.list_layers(zip_path)]
        if layers != [LAYER_NAME]:
            raise ValueError("source_geodatabase_layer_roster_drift")
        info = pyogrio.read_info(zip_path, layer=LAYER_NAME, force_feature_count=True)
        if info.get("features") != EXPECTED_FEATURE_COUNT:
            raise ValueError("source_feature_count_drift")
        if info.get("geometry_type") is not None or info.get("crs") is not None:
            raise ValueError("source_attribute_only_contract_drift")
        observed = [(str(name), str(kind))
                    for name, kind in zip(info["fields"], info["dtypes"])]
        if observed != EXPECTED_FIELD_KINDS:
            raise ValueError("source_schema_or_field_order_drift")
        return {"layers": layers, "feature_count": int(info["features"]),
                "geometry_type": None, "crs": None,
                "field_kinds": observed}
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc


def _verify_native_id_roster(zip_path: Path) -> dict[str, int]:
    """Require contiguous unique native OBJECTIDs 1..N over one narrow pass."""
    try:
        frame = pyogrio.read_dataframe(zip_path, layer=LAYER_NAME, columns=["MTFCC"],
                                       fid_as_index=True, use_arrow=True)
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc
    ids = frame.index.to_numpy()
    if (len(ids) != EXPECTED_FEATURE_COUNT
            or np.any(np.sort(ids) != np.arange(1, EXPECTED_FEATURE_COUNT + 1, dtype=ids.dtype))):
        raise ValueError("source_native_id_population_drift")
    return {"min": 1, "max": EXPECTED_FEATURE_COUNT, "count": len(ids)}


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


def _scan_native_values(zip_path: Path) -> dict[str, object]:
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
    if rows != EXPECTED_FEATURE_COUNT or batch_index == 0:
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


def _manifest(receipt: dict, identity: dict, replayed: bool) -> dict[str, object]:
    return {
        "schema_version": 1,
        "title": "Address Ranges",
        "description": (
            "Draft lossless capture of the complete current (TGRGDB26) Census TIGER national "
            "Address Ranges attribute-only FileGDB layer. Proposed source route for HIFLD "
            "inventory issue #408; DHS crosswalk row, historical HIFLD equivalence, schema/ID "
            "contract, baseline, rights and release acceptance remain unresolved."
        ),
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "processing_identity": identity,
        "inputs": [{
            "input_id": f"census_tiger_address_range_geodatabase_{SOURCE_VINTAGE}",
            "producer": PUBLISHER,
            "host": urlsplit(SOURCE_URL).hostname,
            "role": "producer_publication",
            "canonical_url": SOURCE_URL,
            "retrieval_url": receipt.get("final_url", SOURCE_URL),
            "retrieved_at": receipt.get("observed_at"),
            "sha256": receipt["raw_sha256"],
            "bytes": receipt.get("wire_bytes_received"),
            "http_status": receipt.get("status"),
            "content_type": receipt.get("content_type"),
            "last_modified": receipt.get("last_modified"),
            "last_modified_meaning": (
                "HTTP Last-Modified on www2.census.gov and the TGRGDB26 directory stamp; "
                "packaging evidence, not an independently published producer release date."
            ),
            "release_date_unknown_reason": (
                "The producer publishes the TGRGDB26 edition without an explicit release date "
                "on the archive; the HTTP Last-Modified value is retained in the receipt and is "
                "not treated as a producer release date."
            ),
            "license": "Not adjudicated; source reuse terms and rights require maintainer review before release.",
            "attribution": (
                f"U.S. Census Bureau, TIGER national Address Range geodatabase "
                f"({SOURCE_VINTAGE} edition); historical HIFLD attribution and reuse terms "
                "remain under review."
            ),
        }],
        "transformations": [
            {"name": "stage_lossless_source_archive", "parameters": {
                "canonical_object": CANONICAL_SOURCE_KEY,
                "recompression": "none", "extraction": "none",
                "attribute_only_layer": LAYER_NAME,
            }},
            {"name": "validate_native_population", "parameters": {
                "layer": LAYER_NAME, "row_count": EXPECTED_FEATURE_COUNT,
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
                      scan: dict, replayed: bool) -> dict[str, object]:
    return {
        "source_url": SOURCE_URL, "source_host": SOURCE_HOST,
        "source_vintage": SOURCE_VINTAGE,
        "archive_sha256": receipt["raw_sha256"], "archive_bytes": receipt.get("wire_bytes_received"),
        "archive_receipt": "raw/source_archive.receipt.json",
        "archive_last_modified": receipt.get("last_modified"),
        "archive_members_crc_checked": True,
        "archive_member_count": integrity["member_count"],
        "archive_uncompressed_bytes": integrity["uncompressed_bytes"],
        "geodatabase_root": GDB_ROOT,
        "source_layer": LAYER_NAME,
        "source_layer_roster": layout["layers"],
        "attribute_only": True,
        "feature_count": int(layout["feature_count"]),
        "expected_feature_count": EXPECTED_FEATURE_COUNT,
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
            "Each live run fetches the producer's current TGRGDB26 edition and fails closed "
            "on any archive/identity drift; prior staged versions remain untouched."
        ),
        "release_accepted": False,
        "rights_contract_approved": False,
    }


def _validate_staged_population(staging: StagingStorageResource, version: str,
                                identity: dict, receipt: dict, replayed: bool) -> dict[str, object]:
    """Validate the actual staged canonical bytes and return the capture evidence."""
    staged_sha256 = staging.sha256_key(f"{DATASET_SLUG}/{FILE_SLUG}/{version}/{CANONICAL_SOURCE_KEY}")
    if staged_sha256 != EXPECTED_ARCHIVE_SHA256:
        raise ValueError("staged_source_archive_integrity_failure")
    with staging.get_local_version_dir(DATASET_SLUG, FILE_SLUG, version) as version_dir:
        zip_path = Path(version_dir) / CANONICAL_SOURCE_KEY
        integrity = _validate_archive(zip_path)
        layout = _require_geodatabase_layout(zip_path)
        ids = _verify_native_id_roster(zip_path)
        scan = _scan_native_values(zip_path)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json",
                  _json_bytes(_manifest(receipt, identity, replayed)))
    capture_manifest = _capture_manifest(receipt, integrity, layout, ids, scan, replayed)
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


def capture(staging: StagingStorageResource, version: str,
            client: httpx.Client | None = None) -> dict[str, object]:
    """Capture the bounded official archive losslessly and validate its population."""
    owned = client is None
    client = client or httpx.Client(timeout=300, follow_redirects=True)
    try:
        _archive, receipt = _capture_archive(staging, version, client)
        identity = processing_identity(Path(__file__).parents[3])
        try:
            validate_construction_code_identity(
                Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
            )
        except Exception as exc:
            raise ValueError("source_code_identity_invalid:" + type(exc).__name__) from exc
        return _validate_staged_population(staging, version, identity, receipt, replayed=False)
    except Exception as exc:
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_url": SOURCE_URL,
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
    if (not isinstance(receipt, dict)
            or receipt.get("raw_sha256") != _sha256(archive)
            or receipt.get("wire_bytes_retained") != len(archive)
            or receipt.get("raw_archive_key") != CANONICAL_SOURCE_KEY
            or receipt.get("raw_sha256") != EXPECTED_ARCHIVE_SHA256):
        raise ValueError("retained_capture_archive_receipt_mismatch")
    # Copy the original evidence bytes verbatim. This path is for a local
    # replay of an already observed transfer; it makes no new network request
    # and does not invent a retrieval timestamp.
    staging.write(DATASET_SLUG, FILE_SLUG, version, CANONICAL_SOURCE_KEY, archive)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json", receipt_bytes)
    identity = processing_identity(Path(__file__).parents[3])
    try:
        _require_accepted_transfer(receipt, archive)
        validate_construction_code_identity(
            Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
        )
        result = _validate_staged_population(staging, version, identity, receipt, replayed=True)
        result["replayed"] = True
        return result
    except Exception as exc:
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_url": SOURCE_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "failure_code": str(exc).split(":", 1)[0],
            "release_accepted": False,
            "replay": True,
        }))
        raise
