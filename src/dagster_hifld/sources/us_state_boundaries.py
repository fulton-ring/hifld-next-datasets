"""Candidate-only acquisition of the official Census TIGER/Line US state boundaries."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import zipfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.sources.epa_frs_common import (
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    write_immutable,
)

try:  # geometry inspection needs geopandas; the runtime environment provides it
    import geopandas as gpd
except ImportError:  # pragma: no cover - runtime environment always provides it
    gpd = None

DATASET_SLUG = "us-state-boundaries-1"
FILE_SLUG = DATASET_SLUG
SOURCE_VINTAGE = "2023"
SOURCE_URL = "https://www2.census.gov/geo/tiger/TIGER2023/STATE/tl_2023_us_state.zip"
SOURCE_FILE = "tl_2023_us_state.zip"
PUBLISHER = "U.S. Census Bureau, Geography Division"
EXPECTED_SHA256 = "8ded0ef036e205e246ae4b03a66873f7e1eed50285b993702ce5aca674283a2e"
EXPECTED_BYTES = 9_947_818
EXPECTED_FEATURE_COUNT = 56
EXPECTED_FIELDS = [
    "REGION", "DIVISION", "STATEFP", "STATENS", "GEOID", "GEOIDFQ", "STUSPS", "NAME",
    "LSAD", "MTFCC", "FUNCSTAT", "ALAND", "AWATER", "INTPTLAT", "INTPTLON",
]
STRING_FIELDS = [name for name in EXPECTED_FIELDS if name not in ("ALAND", "AWATER")]
EXPECTED_CRS = "EPSG:4269"
EXPECTED_BOUNDS = [-179.231086, -14.601813, 179.859681, 71.439786]
EXPECTED_MTFCC = {"G4000"}
EXPECTED_FUNCSTAT = {"A"}
EXPECTED_REGION = {"1", "2", "3", "4", "9"}
EXPECTED_DIVISION = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
EXPECTED_LSAD = {"00"}
EXPECTED_GEOIDS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17",
    "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
    "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46",
    "47", "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69", "72", "78",
]
EXPECTED_GEOID_SHA256 = "da707aa61ecf29520f142ee144ef5e0417b9bc28b290e04eb38fe8036f9bfb15"
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 20_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def candidate_version(identity: Mapping[str, object]) -> str:
    """Bind the candidate key to the exact source and Git or packaged identity."""
    if not isinstance(identity.get("code_commit"), str) or not identity.get("code_identity_scheme"):
        raise ValueError("processing_identity_incomplete")
    return "v" + _sha(_canonical({
        "source_archive_sha256": EXPECTED_SHA256,
        "processing_identity": identity,
    }))[:32]


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
    body_path.write_bytes(body)
    wire_path.write_bytes(wire)
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
) -> tuple[bytes, dict[str, object], bytes, bytes]:
    temporary = tempfile.TemporaryDirectory(prefix="census-state-capture-") if capture_directory is None else None
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
                with httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0), follow_redirects=False) as client:
                    body, receipt = capture_get(client, SOURCE_URL, root, "source", max_bytes=MAX_SOURCE_BYTES)
                wire = (root / "source.wire").read_bytes()
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
        return body, receipt, wire, receipt_path.read_bytes()
    finally:
        if temporary is not None:
            temporary.cleanup()


def _inspect_archive_data(body: bytes) -> tuple[object, dict[str, object]]:
    expected_members = {
        "tl_2023_us_state.cpg", "tl_2023_us_state.dbf", "tl_2023_us_state.prj",
        "tl_2023_us_state.shp", "tl_2023_us_state.shp.ea.iso.xml",
        "tl_2023_us_state.shp.iso.xml", "tl_2023_us_state.shx",
    }
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = {Path(info.filename).name for info in archive.infolist() if not info.is_dir()}
        if names != expected_members or len(archive.infolist()) != len(expected_members):
            raise ValueError("tiger_archive_member_roster_drift")
        for info in archive.infolist():
            path = PurePosixPath(info.filename)
            if path.is_absolute() or ".." in path.parts or path.name != info.filename:
                raise ValueError("tiger_archive_unsafe_member")
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError("tiger_archive_crc_failure:" + bad_member)
        with tempfile.TemporaryDirectory(prefix="census-state-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / "tl_2023_us_state.shp"
            frame = gpd.read_file(path, engine="pyogrio")
    fields = [name for name in frame.columns if name != "geometry"]
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if len(frame) != EXPECTED_FEATURE_COUNT or frame["GEOID"].isna().any() or frame["GEOID"].duplicated().any():
        raise ValueError("tiger_source_population_or_id_drift")
    if set(frame["GEOID"].astype(str)) != set(EXPECTED_GEOIDS) or sorted(
        frame["GEOID"].astype(str).tolist()
    ) != sorted(EXPECTED_GEOIDS):
        raise ValueError("tiger_source_geoid_roster_drift")
    if frame["GEOID"].astype(str).str.len().ne(2).any() or not frame["GEOID"].astype(str).str.fullmatch(r"\d{2}").all():
        raise ValueError("tiger_source_geoid_structure_drift")
    if not (frame["GEOIDFQ"].astype(str) == "0400000US" + frame["GEOID"].astype(str)).all():
        raise ValueError("tiger_source_geoidfq_structure_drift")
    if not frame["STATENS"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("tiger_source_statens_structure_drift")
    if any(str(frame[column].dtype) != "object" for column in STRING_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    if any(str(frame[column].dtype) != "int64" for column in ("ALAND", "AWATER")):
        raise ValueError("tiger_source_area_physical_type_drift")
    for column, allowed in (
        ("MTFCC", EXPECTED_MTFCC), ("FUNCSTAT", EXPECTED_FUNCSTAT),
        ("REGION", EXPECTED_REGION), ("DIVISION", EXPECTED_DIVISION), ("LSAD", EXPECTED_LSAD),
    ):
        if frame[column].isna().any() or set(frame[column].astype(str).unique()) != allowed:
            raise ValueError("tiger_source_classification_drift:" + column)
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_source_geometry_quality_failure")
    if not set(geometry.geom_type) <= {"Polygon", "MultiPolygon"}:
        raise ValueError("tiger_source_geometry_type_drift")
    bounds = [float(value) for value in frame.total_bounds]
    if any(abs(a - b) > 1e-6 for a, b in zip(bounds, EXPECTED_BOUNDS)):
        raise ValueError("tiger_source_coverage_extent_drift")
    ids = sorted(frame["GEOID"].astype(str).tolist())
    id_digest = _sha(_canonical(ids))
    if id_digest != EXPECTED_GEOID_SHA256:
        raise ValueError("tiger_source_geoid_digest_drift")
    schema = [{"name": name, "type": "string" if name in STRING_FIELDS else "int64",
               "nullable": bool(frame[name].isna().any())} for name in EXPECTED_FIELDS]
    audit = {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS,
        "attribute_types": {name: ("string" if name in STRING_FIELDS else "int64") for name in EXPECTED_FIELDS},
        "source_id_field": "GEOID", "source_id_sha256": id_digest,
        "source_schema_sha256": _sha(_canonical(schema)), "source_crs": EXPECTED_CRS,
        "geometry_type": "Polygon/MultiPolygon", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()),
        "mtfcc_values": sorted(EXPECTED_MTFCC), "funcstat_values": sorted(EXPECTED_FUNCSTAT),
        "region_values": sorted(EXPECTED_REGION), "division_values": sorted(EXPECTED_DIVISION),
        "lsad_values": sorted(EXPECTED_LSAD),
    }
    return frame, audit


def _inspect_archive(body: bytes) -> dict[str, object]:
    """Validate a pinned archive and return its source summary."""
    return _inspect_archive_data(body)[1]


def _geoparquet(frame) -> bytes:
    """Encode native source columns and unmodified 2D polygon WKB."""
    names = [name for name in EXPECTED_FIELDS]
    arrays = []
    fields = []
    for name in names:
        if name in STRING_FIELDS:
            arrow_type = pa.string()
            values = frame[name].tolist()
            array = pa.array(values, type=arrow_type, safe=True, from_pandas=True)
        else:
            arrow_type = pa.int64()
            values = frame[name].tolist()
            array = pa.array(values, type=arrow_type, safe=True, from_pandas=True)
        arrays.append(array)
        fields.append(pa.field(name, arrow_type, nullable=bool(frame[name].isna().any())))

    geometry_types = sorted(set(frame.geometry.geom_type.astype(str)))
    if not geometry_types or not set(geometry_types) <= {"Polygon", "MultiPolygon"}:
        raise ValueError("tiger_output_geometry_type_drift")
    if any(shapely.get_coordinate_dimension(geometry) != 2 for geometry in frame.geometry.array):
        raise ValueError("tiger_output_non_2d_geometry")
    geometries = list(shapely.to_wkb(frame.geometry.array, byte_order=1, output_dimension=2))
    arrays.append(pa.array(geometries, type=pa.binary(), safe=True))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo_metadata = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "crs": frame.crs.to_json_dict(),
            }
        },
    }
    output = io.BytesIO()
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo_metadata)}))
    pq.write_table(table, output, compression="zstd", use_dictionary=False)
    readback = pq.read_table(pa.BufferReader(output.getvalue()))
    if readback.num_rows != len(frame) or readback.column_names != names + ["geometry"]:
        raise ValueError("tiger_output_schema_or_population_readback_drift")
    for name in names:
        expected = frame[name].tolist()
        if name in STRING_FIELDS:
            expected = [None if value is None or (isinstance(value, float) and value != value) else str(value)
                        for value in expected]
        else:
            expected = [None if value is None else int(value) for value in expected]
        if readback[name].to_pylist() != expected:
            raise ValueError("tiger_output_attribute_readback_drift:" + name)
    if readback["geometry"].to_pylist() != geometries:
        raise ValueError("tiger_output_native_wkb_readback_drift")
    geo = json.loads(readback.schema.metadata[b"geo"])
    if geo["columns"]["geometry"]["geometry_types"] != geometry_types:
        raise ValueError("tiger_output_geometry_metadata_drift")
    return output.getvalue()


def capture_us_state_boundaries(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
    identity: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Capture one exact official annual zip and stage it immutably for catalog."""
    identity = identity or processing_identity(Path(__file__).parents[3])
    validate_construction_code_identity(
        Path(__file__).parents[3], identity.get("code_commit"), identity.get("files", {}), identity=identity,
    )
    if version != candidate_version(identity):
        raise ValueError("candidate_version_identity_mismatch")
    body, receipt, wire, receipt_bytes = _capture(capture_directory, staging_storage, version)
    frame, audit = _inspect_archive_data(body)
    parquet = _geoparquet(frame)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    audit["native_geoparquet_bytes"] = len(parquet)
    audit["native_geoparquet_sha256"] = _sha(parquet)
    audit["native_geometry_type_counts"] = {
        name: int((frame.geometry.geom_type == name).sum())
        for name in sorted(set(frame.geometry.geom_type.astype(str)))
    }
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/raw/source.body"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    wire_key = f"{prefix}/raw/source.wire"
    receipt_raw_key = f"{prefix}/raw/source.receipt.json"
    write_immutable(staging_storage, wire_key, wire)
    write_immutable(staging_storage, receipt_raw_key, receipt_bytes)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != EXPECTED_BYTES or staging_storage.sha256_key(archive_key) != EXPECTED_SHA256:
        raise ValueError("staged_tiger_archive_integrity_failure")
    reused = before is not None
    code_commit = identity["code_commit"]
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "code_identity_scheme": identity["code_identity_scheme"],
        "title": "US State Boundaries",
        "description": "2023 Census TIGER/Line US state (and state equivalent) shapefile. Candidate only; the raw/published schema and ID contract await maintainer approval and no promotion is authorized.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": None,
        "date_issued": None,
        "inputs": [{
            "input_id": SOURCE_URL,
            "producer": PUBLISHER,
            "host": urlsplit(SOURCE_URL).hostname,
            "canonical_url": SOURCE_URL,
            "retrieval_url": receipt["final_url"],
            "retrieved_at": observed_at,
            "sha256": EXPECTED_SHA256,
            "bytes": EXPECTED_BYTES,
            "license": "https://www.usa.gov/government-works",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": "The producer identifies this as the 2023 TIGER/Line vintage; a precise publication date was not established. The vintage year is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP, wire response, and receipt under raw/; export its state features to GeoParquet without ID synthesis, schema widening, attribute mapping, geometry repair, or reprojection."],
        "lineage": "Official U.S. Census Bureau Geography Division TIGER/Line STATE product (producer publication), pinned to the 2023 vintage. No claim is made that this source replaces or matches a historical HIFLD baseline.",
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Preserves all source attributes, GEOID strings, nulls, and native Polygon/MultiPolygon WKB at EPSG:4269; this unapproved candidate performs no geometry repair, simplification, or reprojection.",
                "objects": [{"path": parquet_path, "bytes": len(parquet), "sha256": _sha(parquet)}],
            }},
        },
        "unsupported_output_formats": {
            "geopackage": "No approved export-field mapping or downstream schema contract.",
            "pmtiles": "No approved display transformation or generalization.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if staging_storage.object_exists(manifest_key):
        encoded_manifest = staging_storage.read_key(manifest_key)
        prior = json.loads(encoded_manifest)
        validate_provenance(prior)
        expected_prior = json.loads(_canonical(manifest))
        if isinstance(prior.get("inputs"), list) and len(prior["inputs"]) == 1:
            expected_prior["inputs"][0]["retrieved_at"] = prior["inputs"][0].get("retrieved_at")
        if prior != expected_prior:
            raise ValueError("cached_source_manifest_identity_mismatch")
    else:
        encoded_manifest = _canonical(manifest) + b"\n"
        write_immutable(staging_storage, manifest_key, encoded_manifest)
    stable_receipt = {key: receipt.get(key) for key in (
        "method", "url", "final_url", "status", "complete_body", "wire_bytes",
        "wire_sha256", "bytes", "sha256", "content_encoding", "content_length",
    )}
    audit_doc = {"source": {"url": SOURCE_URL, "vintage": SOURCE_VINTAGE, "archive_sha256": EXPECTED_SHA256,
                            "archive_bytes": EXPECTED_BYTES, "last_modified_observed": None},
                 "acquisition_receipt": {**stable_receipt, "observed_at": observed_at},
                 "processing_identity": identity, **audit}
    audit_doc.update({
        "baseline_accepted": False,
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    receipt_key = f"{prefix}/metadata/source_receipt.json"
    write_immutable(staging_storage, receipt_key, _canonical(receipt) + b"\n")
    identity_key = f"{prefix}/metadata/processing_identity.json"
    write_immutable(staging_storage, identity_key, _canonical(identity) + b"\n")
    geoparquet_key = f"{prefix}/{parquet_path}"
    write_immutable(staging_storage, geoparquet_key, parquet)
    candidate_ready_key = f"{prefix}/metadata/candidate_ready.json"
    ready = {
        "approved": False,
        "version": version,
        "objects": {parquet_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_archive_sha256": EXPECTED_SHA256,
        "source_population_complete": True,
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }
    write_immutable(staging_storage, candidate_ready_key, _canonical(ready) + b"\n")
    for key, expected_bytes, expected_sha in (
        (geoparquet_key, len(parquet), _sha(parquet)),
        (candidate_ready_key, len(_canonical(ready) + b"\n"), _sha(_canonical(ready) + b"\n")),
    ):
        snapshot = staging_storage.object_snapshot(key)
        if snapshot is None or snapshot.size != expected_bytes or staging_storage.sha256_key(key) != expected_sha:
            raise ValueError("staged_tiger_candidate_object_integrity_failure:" + key.rsplit("/", 1)[-1])
    validate_construction_code_identity(
        Path(__file__).parents[3], identity.get("code_commit"), identity.get("files", {}), identity=identity,
    )
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": EXPECTED_BYTES,
        "source_archive_sha256": EXPECTED_SHA256, "source_cache_reused": reused,
        "code_commit": code_commit,
        "code_identity_scheme": identity["code_identity_scheme"],
        "source_id_field": "GEOID",
        "source_coverage_scope": "All US states and state equivalents (DC, Puerto Rico, island areas), 2023 vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "candidate_ready": candidate_ready_key,
        "prebuilt_geoparquet": geoparquet_key,
        "prebuilt_geoparquet_sha256": _sha(parquet),
        "written_keys": [archive_key, wire_key, receipt_raw_key, manifest_key, audit_key, receipt_key, identity_key,
                         geoparquet_key, candidate_ready_key],
    }
