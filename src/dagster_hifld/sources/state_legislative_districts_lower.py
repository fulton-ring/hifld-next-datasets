"""Candidate-only capture of the Census TIGER/Line 2026 State Legislative Districts - Lower layer."""
from __future__ import annotations

import hashlib
import io
import json
import os
import platform
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get,
    quarantine_incomplete_capture,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "state-legislative-districts-lower"
FILE_SLUG = DATASET_SLUG
SOURCE_VINTAGE = "2026"
LAYER_NAME = "State_Legislative_Districts_Lower"
SOURCE_URL = "https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_legislative.gdb.zip"
SOURCE_HOST = "www2.census.gov"
PUBLISHER = "U.S. Census Bureau, Geography Division"
GDB_ROOT = "tlgdb_2026_us_legislative.gdb"
EXPECTED_LAYER_ROSTER = (
    "Congressional_Districts",
    "State_Legislative_Districts_Upper",
    "State_Legislative_Districts_Lower",
)
EXPECTED_SHA256 = "70090d14175d60bf30d6b2b02e9c85bbb131d73898213deb6c9d41d8a99ec407"
EXPECTED_BYTES = 104_588_551
EXPECTED_FEATURE_COUNT = 4_879
EXPECTED_FIELDS = [
    "STATEFP", "SLDLST", "GEOID", "GEOIDFQ", "NAMELSAD", "LSAD", "LSY", "MTFCC",
    "FUNCSTAT", "ALAND", "AWATER", "INTPTLAT", "INTPTLON",
]
EXPECTED_OUTPUT_FIELDS = ["OBJECTID", *EXPECTED_FIELDS, "geometry"]
EXPECTED_CRS = "EPSG:4269"
EXPECTED_BOUNDS = [-179.231086, 17.831509, 179.859681, 71.439786]
EXPECTED_MTFCC_VALUES = ["G5220"]
EXPECTED_LSY_VALUES = ["2026"]
EXPECTED_GEOID_LENGTH = 5
MAX_SOURCE_BYTES = 160_000_000
MAX_UNCOMPRESSED_BYTES = 1_500_000_000
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
SOURCE_RELEASE_MODIFIED = "2026-11-09"


def _processing_runtime() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "httpx": httpx.__version__,
        "pyarrow": pa.__version__,
        "pyogrio": pyogrio.__version__,
        "gdal": ".".join(str(part) for part in pyogrio.__gdal_version__),
        "pyproj": pyproj.__version__,
        "proj": pyproj.proj_version_str,
        "shapely": shapely.__version__,
        "geos": shapely.geos_version_string,
    }


CODE_IDENTITY_SCHEME = "sha256-packaged-dagster_hifld-py-v1"
PROCESSING_SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROCESSING_SOURCE_PACKAGE = PROCESSING_SOURCE_ROOT / "dagster_hifld"


def processing_code_identity() -> str:
    """Hash packaged Dagster source files without requiring repository metadata."""
    files = sorted(PROCESSING_SOURCE_PACKAGE.rglob("*.py"))
    if not files or not PROCESSING_SOURCE_PACKAGE.is_dir():
        raise ValueError("processing_source_package_missing")
    digest = hashlib.sha256()
    for path in files:
        name = path.relative_to(PROCESSING_SOURCE_ROOT).as_posix().encode("utf-8")
        body = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def candidate_version() -> str:
    identity = _canonical({"source_sha256": EXPECTED_SHA256,
                           "code_identity_scheme": CODE_IDENTITY_SCHEME,
                           "code_sha256": processing_code_identity()})
    return "v" + _sha(identity)[:32]


def _quarantine_attempt(
    storage: StagingStorageResource,
    capture_root: Path,
    version: str,
    reason_code: str,
) -> str:
    """Retain all response bytes and a receipt before rejecting a bad capture."""
    body_path = capture_root / "source.body"
    wire_path = capture_root / "source.wire"
    receipt_path = capture_root / "source.receipt.json"
    body = body_path.read_bytes() if body_path.is_file() else b""
    wire = wire_path.read_bytes() if wire_path.is_file() else b""
    for path in (body_path, wire_path, receipt_path):
        path.touch(exist_ok=True)
    try:
        receipt = json.loads(receipt_path.read_text("utf-8"))
        if not isinstance(receipt, dict):
            raise ValueError("receipt_not_object")
    except (OSError, ValueError):
        receipt = {
            "method": "GET",
            "url": SOURCE_URL,
            "final_url": None,
            "status": None,
            "observed_at": None,
            "complete_body": False,
            "bytes": len(body),
            "sha256": _sha(body),
            "wire_bytes": len(wire),
            "wire_sha256": _sha(wire),
            "content_encoding": None,
            "content_length": None,
            "error_code": reason_code,
        }
        receipt_path.write_bytes(_canonical(receipt) + b"\n")
    return quarantine_incomplete_capture(
        storage, capture_root, DATASET_SLUG, version,
        {"source": (body, receipt)}, reason_code,
    )


def _capture(
    capture_directory: str | Path | None,
    staging_storage: StagingStorageResource,
    version: str,
) -> tuple[bytes, dict[str, object]]:
    temporary = tempfile.TemporaryDirectory(prefix="sldl-capture-") if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir():
                raise ValueError("source_capture_directory_invalid")
            if {p.name for p in root.iterdir()} != CAPTURE_FILES:
                raise ValueError("source_capture_roster_mismatch")
            body_path, wire_path, receipt_path = (root / "source.body", root / "source.wire", root / "source.receipt.json")
            if any(p.is_symlink() or not p.is_file() for p in (body_path, wire_path, receipt_path)):
                raise ValueError("source_capture_nonregular_file")
            body = body_path.read_bytes()
            wire = wire_path.read_bytes()
            receipt = json.loads(receipt_path.read_text("utf-8"))
            if (
                receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
                or receipt.get("final_url") != SOURCE_URL or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("content_encoding") != "identity"
            ):
                raise ValueError("source_capture_receipt_identity_mismatch")
        else:
            try:
                with httpx.Client(timeout=httpx.Timeout(300.0, connect=20.0), follow_redirects=False) as client:
                    body, receipt = capture_get(client, SOURCE_URL, root, "source", max_bytes=MAX_SOURCE_BYTES)
            except Exception as exc:
                quarantine = _quarantine_attempt(staging_storage, root, version, type(exc).__name__)
                raise ValueError(f"official_tiger_source_capture_failed; quarantined_at:{quarantine}") from exc

        if (
            receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
            or receipt.get("final_url") != SOURCE_URL or receipt.get("status") != 200
            or receipt.get("complete_body") is not True or receipt.get("bytes") != EXPECTED_BYTES
            or receipt.get("sha256") != EXPECTED_SHA256 or len(body) != EXPECTED_BYTES
            or receipt.get("content_encoding") != "identity"
            or receipt.get("wire_bytes") != EXPECTED_BYTES or receipt.get("wire_sha256") != EXPECTED_SHA256
            or receipt.get("content_length") not in (None, EXPECTED_BYTES)
            or _sha(body) != EXPECTED_SHA256
        ):
            quarantine = _quarantine_attempt(
                staging_storage, root, version, "official_tiger_archive_identity_or_release_drift",
            )
            raise ValueError(f"official_tiger_archive_identity_or_release_drift; quarantined_at:{quarantine}")
        return body, receipt
    finally:
        if temporary is not None:
            temporary.cleanup()


def _validate_archive(body: bytes) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            entries = archive.infolist()
            if not entries or sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("source_archive_expansion_limit_exceeded")
            for item in entries:
                raw = item.filename
                if "\\" in raw or raw.startswith("/") or any(part == ".." for part in raw.split("/")):
                    raise ValueError("source_archive_unsafe_member")
                stripped = raw.rstrip("/")
                if stripped and PurePosixPath(stripped).as_posix() != stripped:
                    raise ValueError("source_archive_unsafe_member")
            gdb_roots = {name.split("/")[0] for name in archive.namelist() if "/" in name}
            if gdb_roots != {GDB_ROOT}:
                raise ValueError("source_archive_geodatabase_roster_drift")
            corrupt = archive.testzip()
            if corrupt is not None:
                raise ValueError("source_archive_crc_failure:" + corrupt)
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc


def _extract_archive(body: bytes) -> Path:
    root = Path(tempfile.mkdtemp(prefix="sldl-extract-"))
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            archive.extractall(root)
        if not (root / GDB_ROOT).is_dir():
            raise ValueError("source_archive_geodatabase_missing")
        return root
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _native_layer(gdb: Path) -> tuple[pa.Table, list[int], dict]:
    try:
        layers = [str(row[0]) for row in pyogrio.list_layers(gdb)]
        if tuple(layers) != EXPECTED_LAYER_ROSTER:
            raise ValueError("source_layer_roster_drift")
        info = pyogrio.read_info(gdb, layer=LAYER_NAME, force_feature_count=True)
        if info.get("features") != EXPECTED_FEATURE_COUNT or info.get("geometry_type") != "MultiPolygon":
            raise ValueError("source_layer_population_or_geometry_drift")
        try:
            crs = pyproj.CRS.from_user_input(info.get("crs"))
        except Exception as exc:
            raise ValueError("source_crs_drift") from exc
        if not crs.equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_crs_drift")
        metadata, table = pyogrio.raw.read_arrow(gdb, layer=LAYER_NAME)
        if metadata.get("geometry_type") != "MultiPolygon" or not pyproj.CRS.from_user_input(metadata.get("crs")).equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_arrow_layer_metadata_drift")
        if metadata.get("geometry_name") != "SHAPE":
            raise ValueError("source_geometry_column_drift")
        actual = [(field.name, field.type) for field in table.schema]
        expected = [
            ("STATEFP", pa.string()), ("SLDLST", pa.string()), ("GEOID", pa.string()),
            ("GEOIDFQ", pa.string()), ("NAMELSAD", pa.string()), ("LSAD", pa.string()),
            ("LSY", pa.string()), ("MTFCC", pa.string()), ("FUNCSTAT", pa.string()),
            ("ALAND", pa.float64()), ("AWATER", pa.float64()),
            ("INTPTLAT", pa.string()), ("INTPTLON", pa.string()), ("SHAPE", pa.binary()),
        ]
        if actual != expected or table.num_rows != EXPECTED_FEATURE_COUNT:
            raise ValueError("source_schema_or_feature_count_drift")
        fid_frame = pyogrio.read_dataframe(gdb, layer=LAYER_NAME, fid_as_index=True, read_geometry=False, use_arrow=True)
        ids = [int(value) for value in fid_frame.index.tolist()]
        if len(ids) != EXPECTED_FEATURE_COUNT or len(set(ids)) != EXPECTED_FEATURE_COUNT or sorted(ids) != list(range(1, EXPECTED_FEATURE_COUNT + 1)):
            raise ValueError("source_native_id_population_drift")
        geoids = table.column("GEOID").to_pylist()
        if fid_frame["GEOID"].tolist() != geoids or len(set(geoids)) != EXPECTED_FEATURE_COUNT:
            raise ValueError("source_geoid_population_drift")
        return table, ids, {"layers": layers}
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc


def _validate_domain_and_ids(table: pa.Table) -> dict[str, object]:
    geoids = table.column("GEOID").to_pylist()
    statefps = table.column("STATEFP").to_pylist()
    sldlst = table.column("SLDLST").to_pylist()
    if any(len(value) != EXPECTED_GEOID_LENGTH for value in geoids):
        raise ValueError("source_geoid_length_drift")
    if any(not str(geoid).startswith(str(statefp)) for geoid, statefp in zip(geoids, statefps)):
        raise ValueError("source_geoid_state_prefix_drift")
    if len(set(zip(statefps, sldlst))) != len(geoids):
        raise ValueError("source_district_id_duplication_drift")
    for column, allowed in (("LSY", EXPECTED_LSY_VALUES), ("MTFCC", EXPECTED_MTFCC_VALUES)):
        if set(table.column(column).to_pylist()) != set(allowed):
            raise ValueError("source_domain_value_drift:" + column)
    if any(value is None for name in EXPECTED_FIELDS for value in table.column(name).to_pylist()):
        raise ValueError("source_attribute_null_drift")
    return {
        "state_fps": sorted(set(statefps)),
        "lsad_values": sorted(set(table.column("LSAD").to_pylist())),
        "funcstat_values": sorted(set(table.column("FUNCSTAT").to_pylist())),
    }


def _build_geoparquet(
    table: pa.Table,
    ids: list[int],
    *,
    expected_count: int = EXPECTED_FEATURE_COUNT,
    expected_bounds: list[float] | None = EXPECTED_BOUNDS,
) -> tuple[bytes, dict]:
    geometry_values = table.column("SHAPE").to_pylist()
    if len(geometry_values) != expected_count or len(ids) != expected_count or any(not isinstance(value, bytes) for value in geometry_values):
        raise ValueError("source_geometry_population_drift")
    xmin = ymin = float("inf")
    xmax = ymax = float("-inf")
    for raw in geometry_values:
        try:
            geometry = shapely.from_wkb(raw)
        except Exception as exc:
            raise ValueError("source_geometry_wkb_invalid") from exc
        if geometry.is_empty or geometry.geom_type != "MultiPolygon" or not geometry.is_valid:
            raise ValueError("source_geometry_type_or_validity_drift")
        bounds = geometry.bounds
        if len(bounds) != 4 or not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
            raise ValueError("source_geometry_coordinate_bounds_drift")
        xmin, ymin = min(xmin, bounds[0]), min(ymin, bounds[1])
        xmax, ymax = max(xmax, bounds[2]), max(ymax, bounds[3])
    observed_bounds = [xmin, ymin, xmax, ymax]
    if expected_bounds is not None and [round(value, 6) for value in observed_bounds] != [round(value, 6) for value in expected_bounds]:
        raise ValueError("source_geometry_coverage_extent_drift")

    columns = [pa.array(ids, type=pa.int64())]
    columns.extend(table.column(name) for name in EXPECTED_FIELDS)
    columns.append(table.column("SHAPE"))
    output = pa.Table.from_arrays(columns, names=EXPECTED_OUTPUT_FIELDS)
    geo = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["MultiPolygon"],
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
            "bbox": observed_bounds,
        }},
    }
    schema_metadata = dict(output.schema.metadata or {})
    schema_metadata[b"geo"] = _canonical(geo).strip()
    output = output.replace_schema_metadata(schema_metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(output, sink, compression="zstd", version="2.6")
    parquet = sink.getvalue().to_pybytes()
    _assert_full_parity(table, ids, parquet, expected_count=expected_count)
    return parquet, {
        "field_names": output.column_names, "arrow_schema": str(output.schema),
        "geometry_bbox": observed_bounds,
    }


def _assert_full_parity(source: pa.Table, ids: list[int], parquet: bytes, *, expected_count: int = EXPECTED_FEATURE_COUNT) -> None:
    output = pq.read_table(pa.BufferReader(parquet))
    if output.num_rows != expected_count or output.column_names != EXPECTED_OUTPUT_FIELDS:
        raise ValueError("native_output_population_or_field_roster_drift")
    if output.column("OBJECTID").to_pylist() != ids:
        raise ValueError("native_output_id_parity_drift")
    for name in EXPECTED_FIELDS:
        if output.column(name).to_pylist() != source.column(name).to_pylist():
            raise ValueError("native_output_attribute_parity_drift:" + name)
    if output.column("geometry").to_pylist() != source.column("SHAPE").to_pylist():
        raise ValueError("native_output_geometry_parity_drift")
    if len(set(output.column("OBJECTID").to_pylist())) != expected_count:
        raise ValueError("native_output_duplicate_ids")


def _source_records_sha256(table: pa.Table, ids: list[int]) -> str:
    columns = {name: table.column(name).to_pylist() for name in EXPECTED_FIELDS}
    records = [
        {"OBJECTID": identity,
         **{name: (columns[name][index].hex() if isinstance(columns[name][index], bytes) else columns[name][index]) for name in EXPECTED_FIELDS}}
        for index, identity in enumerate(ids)
    ]
    return _sha(_canonical(records))


def _source_schema_sha256(table: pa.Table) -> str:
    schema = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in table.schema if field.name != "SHAPE"
    ]
    return _sha(_canonical(schema))


def _ids_sha256(ids: list[int]) -> str:
    return _sha(_canonical(ids))


def _geoids_sha256(table: pa.Table) -> str:
    return _sha(_canonical(sorted(table.column("GEOID").to_pylist())))


def capture_state_legislative_districts_lower(
    staging_storage: StagingStorageResource,
    *,
    version: str,
    capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture the exact official 2026 legislative GDB and stage the SLDL layer."""
    body, receipt = _capture(capture_directory, staging_storage, version)
    _validate_archive(body)
    extracted = _extract_archive(body)
    try:
        gdb = extracted / GDB_ROOT
        table, ids, source_meta = _native_layer(gdb)
        domain_audit = _validate_domain_and_ids(table)
        parquet, output_schema = _build_geoparquet(table, ids)
    finally:
        shutil.rmtree(extracted, ignore_errors=True)

    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/raw/{PurePosixPath(SOURCE_URL).name}"
    parquet_relative = f"geoparquet/{FILE_SLUG}.parquet"
    parquet_key = f"{prefix}/{parquet_relative}"
    parquet_sha = _sha(parquet)
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": _processing_runtime(),
    }
    ids_rows = [{"OBJECTID": identity, "GEOID": table.column("GEOID")[index].as_py()} for index, identity in enumerate(ids)]
    supplied_claim = os.environ.get("HIFLD_CODE_COMMIT") or os.environ.get("GITHUB_SHA")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "supplied_code_commit_claim": supplied_claim,
        "title": "State Legislative Districts - Lower",
        "description": "2026 Census TIGER/Line national State Legislative Districts - Lower layer, extracted from the official nationwide File Geodatabase. Candidate only; the DHS crosswalk row, schema/ID contract, coverage, rights and release acceptance remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL,
        "source_layer": LAYER_NAME,
        "source_layer_roster": list(source_meta["layers"]),
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "date_issued": SOURCE_VINTAGE,
        "inputs": [{
            "input_id": "census_tigerline_2026_legislative_geodatabase",
            "producer": PUBLISHER,
            "host": urlsplit(SOURCE_URL).hostname,
            "canonical_url": SOURCE_URL,
            "retrieval_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"],
            "sha256": EXPECTED_SHA256,
            "bytes": EXPECTED_BYTES,
            "http_status": receipt["status"],
            "license": "https://www.usa.gov/government-works",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": "The producer identifies this as the 2026 TIGER/Line legislative vintage; the HTTP Last-Modified date (2026-11-09) is retained in the capture receipt and is not treated as the producer release date.",
        }],
        "transformations": [
            {"name": "extract_geodatabase_layer", "parameters": {"layer": LAYER_NAME, "field_subset": "none", "geometry_simplification": "none", "reprojection": "none"}},
            {"name": "write_geoparquet", "parameters": {"compression": "zstd", "geometry_encoding": "WKB", "native_crs": "EPSG:4269", "object_id": "native GDB FID"}},
        ],
        "lineage": "Direct extraction of the State_Legislative_Districts_Lower layer from the official U.S. Census Bureau TIGER/Line 2026 nationwide legislative geodatabase (current maintained vintage; supersedes the 2024-era release cited by the HIFLD inventory row). Equivalence to the archived HIFLD publication and the DHS crosswalk row remain unverified.",
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Source-faithful preservation of all 2026 native SLDL attributes, native FIDs, WKB geometries and EPSG:4269 coordinates in one pinned object; draft conversion only.",
                "objects": [{"path": parquet_relative, "sha256": parquet_sha, "bytes": len(parquet)}],
            }},
        },
    }
    try:
        validate_provenance(manifest)
    except Exception as exc:
        raise ValueError("source_manifest_provenance_invalid:" + type(exc).__name__) from exc
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if staging_storage.object_exists(manifest_key):
        prior = json.loads(staging_storage.read_key(manifest_key))
        validate_provenance(prior)
        expected_prior = json.loads(_canonical(manifest))
        if isinstance(prior.get("inputs"), list) and len(prior["inputs"]) == 1:
            expected_prior["inputs"][0]["retrieved_at"] = prior["inputs"][0].get("retrieved_at")
        if prior != expected_prior:
            raise ValueError("cached_source_manifest_identity_mismatch")
    else:
        write_immutable(staging_storage, manifest_key, _canonical(manifest) + b"\n")
    stable_receipt = {key: receipt.get(key) for key in (
        "method", "url", "final_url", "status", "complete_body", "wire_bytes",
        "wire_sha256", "bytes", "sha256", "content_encoding", "content_length",
    )}
    ids_sha = _ids_sha256(ids)
    audit_doc = {
        "source": {"url": SOURCE_URL, "vintage": SOURCE_VINTAGE, "release_modified": SOURCE_RELEASE_MODIFIED,
                   "archive_sha256": EXPECTED_SHA256, "archive_bytes": EXPECTED_BYTES},
        "processing_code": processing_code,
        "acquisition_receipt": stable_receipt,
        "capture_module_sha256": _sha(Path(__file__).read_bytes()),
        **domain_audit,
        "source_layer_roster": list(source_meta["layers"]),
        "feature_ids": ids_rows,
        "feature_count": len(ids), "ids_sha256": ids_sha,
        "source_id_field": "GEOID", "source_id_sha256": _geoids_sha256(table),
        "native_object_id_field": "OBJECTID (native GDB FID)",
        "source_schema_sha256": _source_schema_sha256(table),
        "native_geometry_type": "MultiPolygon", "native_crs": EXPECTED_CRS,
        "output": {"path": parquet_relative, "sha256": parquet_sha, "bytes": len(parquet), "schema": output_schema},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, archive_key, body)
    write_immutable(staging_storage, parquet_key, parquet)
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": len(ids), "source_population_complete": True,
        "source_id_field": "GEOID", "source_id_sha256": _geoids_sha256(table),
        "source_schema_sha256": _source_schema_sha256(table),
        "ids_sha256": ids_sha, "records_sha256": _source_records_sha256(table, ids),
        "source_archive_sha256": EXPECTED_SHA256, "source_archive_bytes": EXPECTED_BYTES,
        "native_parquet_sha256": parquet_sha,
        "source_capture_evidence_sha256": receipt["sha256"],
        "state_count": len(domain_audit["state_fps"]),
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_coverage_scope": "U.S. Census TIGER/Line 2026 State Legislative Districts - Lower, nationwide layer",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, parquet_key, manifest_key, audit_key],
    }
