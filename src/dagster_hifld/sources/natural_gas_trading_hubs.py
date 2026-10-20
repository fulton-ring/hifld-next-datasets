"""Historical source adapter for EIA's February 2020 natural-gas hub layer.

The EIA ZIP is an official, dated 2020 publication and matches the exact layer
title, description and 32-point product. It is useful for a reproducible
historical candidate. It does not prove an ongoing refresh, and the relationship
between this EIA publication and the later DOE/ORNL HIFLD archive remains under
review. All release gates therefore stay closed.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import BadZipFile, ZipFile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "natural-gas-trading-hubs"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://www.eia.gov/maps/map_data/NaturalGas_TradingHubs_US_EIA.zip"
SOURCE_TITLE = "Natural Gas Trading Hubs"
SOURCE_VINTAGE = "2020-02"
SOURCE_RELEASE_DATE = "2020-07-10"
SOURCE_HTTP_LAST_MODIFIED = "Thu, 09 Jul 2020 19:51:36 GMT"
EXPECTED_ZIP_BYTES = 30668
EXPECTED_ZIP_SHA256 = "c7be20d28af334567408c8c5cf0df7acff68d7b2f855118872db5e6d0cf1180b"
BASELINE_RELEASE_ID = "1704edb3-322d-469c-91da-92902fe76a2d"
BASELINE_COLLECTION_URL = (
    "https://storage.googleapis.com/hifld-next-datasets-prod/releases/"
    "1704edb3-322d-469c-91da-92902fe76a2d/hifld/natural-gas-trading-hubs/"
    "natural-gas-trading-hubs/v1.0.0/collection.json"
)
BASELINE_GEOPARQUET_SHA256 = "7f4c7c72de56d65e56c1ce016793c126555b078ceeeacc456a3e559c53b89b77"
EXPECTED_COUNT = 32
EXPECTED_MEMBERS = {
    "NaturalGas_TradingHubs_US_202002.cpg",
    "NaturalGas_TradingHubs_US_202002.dbf",
    "NaturalGas_TradingHubs_US_202002.prj",
    "NaturalGas_TradingHubs_US_202002.sbn",
    "NaturalGas_TradingHubs_US_202002.sbx",
    "NaturalGas_TradingHubs_US_202002.shp",
    "NaturalGas_TradingHubs_US_202002.shp.xml",
    "NaturalGas_TradingHubs_US_202002.shx",
}
EXPECTED_FIELDS = [
    {"name": "HubName", "ogr_type": "OFTString", "arrow_type": pa.string()},
    {"name": "Period", "ogr_type": "OFTInteger64", "arrow_type": pa.int32()},
    {"name": "Latitude", "ogr_type": "OFTReal", "arrow_type": pa.float64()},
    {"name": "Longitude", "ogr_type": "OFTReal", "arrow_type": pa.float64()},
]
MAX_SOURCE_BYTES = 1_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _capture_get(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    root.mkdir(parents=True, exist_ok=True)
    wire_path, body_path = root / f"{name}.wire", root / f"{name}.body"
    wire_path.unlink(missing_ok=True); body_path.unlink(missing_ok=True)
    wire_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    sizes = {"wire": 0, "body": 0}
    observed = datetime.now(timezone.utc).isoformat()
    def retain(path: Path, kind: str, digest, data: bytes) -> None:
        with path.open("ab") as stream: stream.write(data)
        sizes[kind] += len(data); digest.update(data)
    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        result = capture_http_response(
            response, max_wire_bytes=MAX_SOURCE_BYTES, max_decoded_bytes=MAX_SOURCE_BYTES,
            reserve=lambda _progress: None,
            retain_wire=lambda data: retain(wire_path, "wire", wire_hash, data),
            retain_decoded=lambda data: retain(body_path, "body", body_hash, data),
        )
        final_url = str(response.url)
    receipt = {
        "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": result.status_code, "complete_body": result.complete,
        "wire_bytes": result.wire_bytes_retained, "wire_sha256": result.wire_sha256,
        "bytes": result.decoded_bytes_retained, "sha256": result.decoded_sha256,
        "content_encoding": result.content_encoding, "content_length": result.content_length,
        "error_code": result.error_code,
    }
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    body = body_path.read_bytes(); wire = wire_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
            or len(body) != sizes["body"] or _sha(body) != receipt["sha256"]
            or len(wire) != sizes["wire"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture")
    return body, receipt


def _read_json_receipt(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != _canonical(value) + b"\n":
        raise ValueError("retained_capture_receipt_not_canonical")
    return value


def _validate_observed_at(value: object) -> None:
    if not isinstance(value, str):
        raise ValueError("retained_capture_timestamp_missing")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("retained_capture_timestamp_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("retained_capture_timestamp_not_timezone_aware")


def _load_retained_capture(root: Path) -> tuple[bytes, dict[str, object], dict[str, object]]:
    """Replay one immutable capture bundle, checking every retained byte and receipt."""
    expected_names = {
        "capture_manifest.json", "source_head.receipt.json", "eia_source_zip.body",
        "eia_source_zip.wire", "eia_source_zip.receipt.json",
    }
    if not root.is_dir() or root.is_symlink() or {path.name for path in root.iterdir()} != expected_names:
        raise ValueError("retained_capture_roster_mismatch")
    if any(path.is_symlink() or not path.is_file() for path in root.iterdir()):
        raise ValueError("retained_capture_nonregular_entry")
    manifest = _read_json_receipt(root / "capture_manifest.json")
    head_receipt_bytes = (root / "source_head.receipt.json").read_bytes()
    head = _read_json_receipt(root / "source_head.receipt.json")
    receipt = _read_json_receipt(root / "eia_source_zip.receipt.json")
    if manifest.get("complete") is not True or manifest.get("error_code") is not None:
        raise ValueError("retained_capture_incomplete")
    _validate_observed_at(manifest.get("captured_at"))
    if head.get("status") != 200 or head.get("url") != SOURCE_URL:
        raise ValueError("retained_head_receipt_identity_mismatch")
    _validate_observed_at(head.get("observed_at"))
    if (head.get("last_modified") != SOURCE_HTTP_LAST_MODIFIED
            or head.get("content_length") != str(EXPECTED_ZIP_BYTES)):
        raise ValueError("retained_head_receipt_metadata_mismatch")
    body = (root / "eia_source_zip.body").read_bytes()
    wire = (root / "eia_source_zip.wire").read_bytes()
    _validate_observed_at(receipt.get("observed_at"))
    if (receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
            or receipt.get("final_url") != SOURCE_URL or receipt.get("status") != 200
            or receipt.get("complete_body") is not True or receipt.get("error_code") is not None
            or receipt.get("content_encoding") not in (None, "identity")
            or receipt.get("content_length") not in (EXPECTED_ZIP_BYTES, str(EXPECTED_ZIP_BYTES))
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
            or body != wire):
        raise ValueError("retained_get_receipt_body_mismatch")
    if (len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256
            or manifest.get("receipts") != {"eia_source_zip": receipt}
            or manifest.get("source_head_receipt_sha256") != _sha(head_receipt_bytes)
            or manifest.get("source_feature_count") != EXPECTED_COUNT):
        raise ValueError("retained_capture_manifest_binding_mismatch")
    return body, receipt, head


def _incomplete_capture_is_retryable(root: Path, manifest: dict[str, object]) -> bool:
    """Recognize only a canonical failed attempt, never a complete/tampered seal."""
    if (manifest.get("complete") is not False
            or not isinstance(manifest.get("error_code"), str)
            or not manifest.get("error_code")
            or any(field in manifest for field in (
                "source_feature_count", "source_record_sha256", "source_geometry_sha256",
            ))):
        return False
    try:
        _validate_observed_at(manifest.get("captured_at"))
        head_path = root / "source_head.receipt.json"
        recorded_head_hash = manifest.get("source_head_receipt_sha256")
        return recorded_head_hash is None or (
            head_path.is_file() and _sha(head_path.read_bytes()) == recorded_head_hash
        )
    except (OSError, ValueError):
        return False


def _capture_attempt_directory(root: Path, *, allow_live_retry: bool) -> Path:
    """Choose a fresh live observation while preserving every earlier attempt."""
    if not allow_live_retry:
        return root
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            return root
        try:
            base_manifest = _read_json_receipt(root / "capture_manifest.json")
        except (OSError, ValueError, NotADirectoryError):
            return root
        if base_manifest.get("complete") is False:
            if not _incomplete_capture_is_retryable(root, base_manifest):
                return root
        else:
            # A completed prior capture must be strictly valid before refreshing.
            _load_retained_capture(root)
    attempts = sorted(root.parent.glob(f"{root.name}.attempt-*"))
    if attempts:
        latest = attempts[-1]
        if latest.is_symlink() or not latest.is_dir():
            return latest
        try:
            manifest = _read_json_receipt(latest / "capture_manifest.json")
        except (OSError, ValueError, NotADirectoryError):
            return latest
        if manifest.get("complete") is False:
            if not _incomplete_capture_is_retryable(latest, manifest):
                return latest
        else:
            # Never let a newer observation hide corruption in an older attempt.
            _load_retained_capture(latest)
    next_number = 1
    for attempt in attempts:
        suffix = attempt.name.removeprefix(f"{root.name}.attempt-")
        if suffix.isdigit():
            next_number = max(next_number, int(suffix) + 1)
    if not root.exists() and not attempts:
        return root
    return root.parent / f"{root.name}.attempt-{next_number:04d}"


def _reuse_original_capture(root: Path, fresh_root: Path, fresh_body: bytes) -> tuple[bytes, dict[str, object], dict[str, object], Path]:
    """Use first verified capture receipts when a live refresh confirms same bytes."""
    candidates = ([root] if root.exists() else []) + sorted(root.parent.glob(f"{root.name}.attempt-*"))
    for candidate in candidates:
        if candidate == fresh_root:
            continue
        if candidate.is_symlink() or not candidate.is_dir():
            raise ValueError("retained_capture_nonregular_entry")
        try:
            manifest = _read_json_receipt(candidate / "capture_manifest.json")
        except (OSError, ValueError, NotADirectoryError) as exc:
            raise ValueError("retained_capture_roster_mismatch") from exc
        if manifest.get("complete") is False:
            if not _incomplete_capture_is_retryable(candidate, manifest):
                raise ValueError("retained_capture_incomplete")
            continue
        original_body, receipt, head = _load_retained_capture(candidate)
        if original_body != fresh_body:
            raise ValueError("official_source_zip_changed_rebaseline_required")
        return original_body, receipt, head, candidate
    _body, receipt, head = _load_retained_capture(fresh_root)
    return fresh_body, receipt, head, fresh_root

def _acquire_capture(root: Path, *, allow_live_retry: bool = False) -> tuple[bytes, dict[str, object], dict[str, object], Path]:
    root = _capture_attempt_directory(root, allow_live_retry=allow_live_retry)
    root.parent.mkdir(parents=True, exist_ok=True)
    if root.exists():
        body, receipt, head = _load_retained_capture(root)
        return body, receipt, head, root
    root.mkdir(parents=False, exist_ok=False)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    head_receipt: dict[str, object] | None = None
    try:
        with httpx.Client(follow_redirects=False, timeout=60,
                          headers={"User-Agent": "hifld-next-datasets/natural-gas-trading-hubs-draft"}) as client:
            with client.stream("HEAD", SOURCE_URL, headers={"Accept-Encoding": "identity"}) as response:
                head_receipt = {"status": response.status_code, "url": str(response.url),
                                "last_modified": response.headers.get("last-modified"),
                                "content_length": response.headers.get("content-length"),
                                "observed_at": datetime.now(timezone.utc).isoformat()}
            (root / "source_head.receipt.json").write_bytes(_canonical(head_receipt) + b"\n")
            if (head_receipt["status"] != 200 or head_receipt["url"] != SOURCE_URL
                    or head_receipt["last_modified"] != SOURCE_HTTP_LAST_MODIFIED
                    or head_receipt["content_length"] != str(EXPECTED_ZIP_BYTES)):
                raise ValueError("official_source_http_metadata_drift")
            body, receipt = _capture_get(client, SOURCE_URL, root, "eia_source_zip")
            captures["eia_source_zip"] = (body, receipt)
        records, audit = _read_official_archive(body)
        (root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "error_code": None,
            "source_feature_count": len(records), "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_head_receipt_sha256": _sha((root / "source_head.receipt.json").read_bytes()),
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        }) + b"\n")
        body, receipt, head = _load_retained_capture(root)
        return body, receipt, head, root
    except Exception as exc:
        if not (root / "capture_manifest.json").exists():
            failed_receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
            failed_get_receipt = root / "eia_source_zip.receipt.json"
            if failed_get_receipt.is_file():
                try:
                    failed_receipts["eia_source_zip"] = _read_json_receipt(failed_get_receipt)
                except (OSError, ValueError):
                    pass
            (root / "capture_manifest.json").write_bytes(_canonical({
                "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                "error_code": type(exc).__name__,
                "source_head_receipt_sha256": (_sha((root / "source_head.receipt.json").read_bytes())
                                                if (root / "source_head.receipt.json").is_file() else None),
                "receipts": failed_receipts,
            }) + b"\n")
        raise


def _read_official_archive(body: bytes) -> tuple[list[dict[str, object]], dict[str, object]]:
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256:
        raise ValueError("official_source_zip_changed_rebaseline_required")
    try:
        from io import BytesIO
        archive = ZipFile(BytesIO(body))
    except BadZipFile as exc:
        raise ValueError("source_archive_not_a_zip") from exc
    names = [item.filename for item in archive.infolist()]
    if len(names) != len(set(names)) or set(names) != EXPECTED_MEMBERS:
        raise ValueError("source_archive_member_roster_drift")
    if any(name.startswith(("/", "\\")) or ".." in Path(name).parts for name in names):
        raise ValueError("unsafe_source_archive_path")
    xml = archive.read("NaturalGas_TradingHubs_US_202002.shp.xml")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise ValueError("source_metadata_xml_invalid") from exc
    publication_date = root.findtext(".//pubDate")
    purpose = " ".join((root.findtext(".//idPurp") or "").split())
    if publication_date != "2020-07-10T00:00:00" or "key natural gas trading hubs" not in purpose.lower():
        raise ValueError("source_vintage_or_product_metadata_drift")
    with tempfile.TemporaryDirectory(prefix="hifld-gas-hubs-") as temp:
        import pyogrio

        directory = Path(temp)
        for name in names:
            (directory / name).write_bytes(archive.read(name))
        shp = directory / "NaturalGas_TradingHubs_US_202002.shp"
        info = pyogrio.read_info(shp)
        field_names = list(info["fields"])
        physical = list(info["ogr_types"])
        expected_names = [field["name"] for field in EXPECTED_FIELDS]
        expected_types = [field["ogr_type"] for field in EXPECTED_FIELDS]
        if (info["features"] != EXPECTED_COUNT or info["geometry_type"] != "Point"
                or info["crs"] != "EPSG:4326" or field_names != expected_names
                or physical != expected_types or info.get("fid_column")):
            raise ValueError("source_geometry_schema_or_identifier_drift")
        frame = pyogrio.read_dataframe(shp, use_arrow=True)
    if len(frame) != EXPECTED_COUNT or list(frame.columns) != ["HubName", "Period", "Latitude", "Longitude", "geometry"]:
        raise ValueError("source_feature_population_or_schema_drift")
    records: list[dict[str, object]] = []
    for row in frame.itertuples(index=False, name=None):
        name, period, lat, lon, geom = row
        if not isinstance(name, str) or period != 202002 or geom is None or geom.geom_type != "Point":
            raise ValueError("source_attribute_or_geometry_invalid")
        if abs(geom.x - lon) > 1e-8 or abs(geom.y - lat) > 1e-8:
            raise ValueError("source_geometry_and_coordinate_attributes_disagree")
        records.append({"HubName": name, "Period": int(period), "Latitude": float(lat), "Longitude": float(lon),
                       "geometry_wkb": bytes(geom.wkb)})
    schema = [{"name": field["name"], "source_type": field["ogr_type"], "nullable": True,
               "parquet_type": str(field["arrow_type"])} for field in EXPECTED_FIELDS]
    geometry_hash = _sha(_canonical([[row["HubName"], row["geometry_wkb"].hex()] for row in records]))
    record_hash = _sha(_canonical([{**row, "geometry_wkb": row["geometry_wkb"].hex()} for row in records]))
    audit = {
        "feature_count": len(records), "source_schema": schema,
        "source_schema_sha256": _sha(_canonical(schema)), "source_record_sha256": record_hash,
        "source_geometry_sha256": geometry_hash, "source_crs": "EPSG:4326",
        "geometry_type": "Point", "null_geometry_count": 0, "invalid_geometry_count": 0,
        "native_id_field": None,
        "native_id_note": "The official shapefile has no stable feature-ID attribute. Record position is not an identifier; no substitute IDs are generated.",
        "source_release_date": SOURCE_RELEASE_DATE, "source_vintage": SOURCE_VINTAGE,
        "release_date_basis": "EIA Esri XML metadata pubDate; separately the ZIP Last-Modified is 2020-07-09.",
        "historical_hifld_comparison": {
            "release_id": BASELINE_RELEASE_ID, "collection_url": BASELINE_COLLECTION_URL,
            "geoparquet_sha256": BASELINE_GEOPARQUET_SHA256, "feature_count": 32,
            "result": "All four source attributes match the pinned HIFLD v1.0.0 baseline exactly for all rows; source points transformed EPSG:4326 to baseline EPSG:3857 match within 1 mm. Baseline adds derived bbox and stores Period as int64; EIA shapefile stores Period as int32.",
        },
        "release_accepted": False, "hifld_product_content_match_confirmed": True,
        "hifld_distribution_authority_confirmed": False,
    }
    return records, audit


def _parquet_bytes(records: list[dict[str, object]]) -> bytes:
    fields = [pa.field(item["name"], item["arrow_type"], nullable=True) for item in EXPECTED_FIELDS]
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    columns = {item["name"]: pa.array([row[item["name"]] for row in records], type=item["arrow_type"])
               for item in EXPECTED_FIELDS}
    columns["geometry"] = pa.array([row["geometry_wkb"] for row in records], type=pa.binary())
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar"
    }}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    import pyogrio

    root = Path(__file__).parents[3]
    identity = processing_identity(root)
    identity["gdal"] = pyogrio.__gdal_version_string__
    return identity


def capture_natural_gas_trading_hubs(storage: StagingStorageResource, *, version: str,
                                      capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for char in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_root = Path(capture_directory).absolute() if capture_directory else Path(
        Path("/tmp/hifld-native-reconstruction/174/captures") / version
    ).absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    requested_capture_root = capture_root
    body, receipt, head_receipt, capture_root = _acquire_capture(
        requested_capture_root, allow_live_retry=capture_directory is None
    )
    if capture_directory is None:
        body, receipt, head_receipt, capture_root = _reuse_original_capture(
            requested_capture_root, capture_root, body
        )
    captures = {"eia_source_zip": (body, receipt)}
    records, audit = _read_official_archive(body)
    capture_manifest = _read_json_receipt(capture_root / "capture_manifest.json")
    if (capture_manifest.get("source_record_sha256") != audit["source_record_sha256"]
            or capture_manifest.get("source_geometry_sha256") != audit["source_geometry_sha256"]
            or capture_manifest.get("source_feature_count") != audit["feature_count"]):
        raise ValueError("retained_capture_source_audit_binding_mismatch")

    identity = _code_identity()
    parquet_body = _parquet_bytes(records)
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    manifest = {
        "schema_version": 1, "approved": False, "title": SOURCE_TITLE,
        "description": "Official EIA natural gas trading hubs shapefile, February 2020 data period; exact DOE/ORNL HIFLD archive relationship and ongoing source maintenance remain unverified.",
        "publisher": "U.S. Energy Information Administration (EIA)", "agency": "U.S. Department of Energy",
        "source_url": SOURCE_URL, "license": None, "source_modified": None,
        "date_issued": SOURCE_RELEASE_DATE, "date_modified": None, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"], "inputs": [{
            "input_id": name, "producer": "U.S. Energy Information Administration", "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication", "canonical_url": SOURCE_URL, "retrieval_url": receipt["url"],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "release_date": SOURCE_RELEASE_DATE, "release_date_basis": "embedded XML pubDate, checked against dated HTTP Last-Modified",
            "license": "Not stated in the captured shapefile metadata; rights review pending.",
            "attribution": "U.S. Energy Information Administration (EIA), Natural Gas Trading Hubs source ZIP.",
        } for name, (_body, receipt) in sorted(captures.items())],
        "lineage": "The exact EIA ZIP names the product NaturalGas_TradingHubs_US_202002 and embeds Esri publication metadata dated 2020-07-10. Full comparison with pinned HIFLD GCS v1.0.0 release " + BASELINE_RELEASE_ID + " found all 32 rows, four native attributes and point positions equivalent (projected to the baseline CRS). This confirms the same historical product content; it does not establish an ongoing refresh or a separate ORNL authorization. The current EIA organization FeatureServer endpoint returns Token Required anonymously. No later source release date was found in the evidence reviewed.",
        "transformations": [{"operation": "EIA_SHAPEFILE_TO_NATIVE_GEOPARQUET_CANDIDATE",
            "parameters": {"attributes": "preserve all four shapefile fields and values in source order with explicit types",
                           "geometry": "point WKB EPSG:4326 from source shapefile; no coordinate repair or reproject",
                           "identifiers": "input has no stable ID attribute; do not generate substitute IDs",
                           "candidate_status": "unapproved historical candidate; release, source lineage, schema/ID, rights, freshness, and mapping gates remain closed"}}],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native source attributes and point geometries preserved; no synthetic IDs; candidate is historical and unapproved.",
            "objects": [{"path": f"geoparquet/{DATASET_SLUG}.parquet", "bytes": len(parquet_body), "sha256": _sha(parquet_body)}]}}},
        "unsupported_output_formats": {"geopackage": "No accepted publication contract.", "pmtiles": "No approved display schema."},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"publisher": "EIA", "official_source_zip_sha256": EXPECTED_ZIP_SHA256,
                  "observed_feature_order_preserved": True, "native_id_contract_accepted": False,
                  "hifld_product_content_match_confirmed": True, "hifld_distribution_authority_confirmed": False,
                  "schema_contract_accepted": False, "rights_accepted": False, "freshness_accepted": False,
                  "mapping_approved": False, "release_accepted": False})
    outputs: dict[str, bytes] = {
        f"geoparquet/{DATASET_SLUG}.parquet": parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "raw/eia_source_zip.body": body,
        "raw/eia_source_zip.wire": (capture_root / "eia_source_zip.wire").read_bytes(),
        "raw/eia_source_zip.receipt.json": _canonical(receipt) + b"\n",
        "raw/source_head.receipt.json": _canonical(head_receipt) + b"\n",
    }
    ready = {"approved": False, "version": version,
             "objects": {f"geoparquet/{DATASET_SLUG}.parquet": {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}},
             "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        expected = {name: {"bytes": len(data), "sha256": _sha(data)} for name, data in outputs.items()}
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                expected, {name: rec for name, (_b, rec) in captures.items()})
        return {"version": version, "feature_count": EXPECTED_COUNT, "source_null_geometry_count": 0, "reused": True,
                "candidate_ready": f"{prefix}/metadata/candidate_ready.json", "release_accepted": False}
    for name, data in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", data)
    descriptors = {name: {"bytes": len(data), "sha256": _sha(data)} for name, data in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"; snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: rec for name, (_b, rec) in captures.items()}, "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": 0,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json", "release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "freshness_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "capture_directory": str(capture_root)}
