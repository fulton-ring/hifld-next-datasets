"""Candidate-only acquisition of the official Census TIGER/Line US county boundaries."""
from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.epa_frs_common import (
    capture_get,
    quarantine_incomplete_capture,
    write_immutable,
)

try:  # geometry inspection needs geopandas; the runtime environment provides it
    import geopandas as gpd
except ImportError:  # pragma: no cover - runtime environment always provides it
    gpd = None

DATASET_SLUG = "us-county-boundaries-1"
FILE_SLUG = DATASET_SLUG
SOURCE_VINTAGE = "2023"
SOURCE_URL = "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
SOURCE_FILE = "tl_2023_us_county.zip"
PUBLISHER = "U.S. Census Bureau, Geography Division"
EXPECTED_SHA256 = "692e12c30c83adcaabdbac0d3954fafa55e1c89a24b36d95e72e02dff938652e"
EXPECTED_BYTES = 83_451_409
EXPECTED_FEATURE_COUNT = 3235
EXPECTED_FIELDS = [
    "STATEFP", "COUNTYFP", "COUNTYNS", "GEOID", "GEOIDFQ", "NAME", "NAMELSAD",
    "LSAD", "CLASSFP", "MTFCC", "CSAFP", "CBSAFP", "METDIVFP", "FUNCSTAT",
    "ALAND", "AWATER", "INTPTLAT", "INTPTLON",
]
STRING_FIELDS = [name for name in EXPECTED_FIELDS if name not in ("ALAND", "AWATER")]
EXPECTED_CRS = "EPSG:4269"
EXPECTED_BOUNDS = [-179.231086, -14.601813, 179.859681, 71.439786]
EXPECTED_MTFCC = {"G4020"}
EXPECTED_CLASSFP = {"H1", "H4", "H5", "H6", "C7"}
EXPECTED_FUNCSTAT = {"A", "B", "C", "F", "G", "N", "S"}
EXPECTED_GEOID_SHA256 = "2543af0c3d664319369a412825a0ef0970f684b1db94163e711735239db312f2"
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 256 * 1024 * 1024
KNOWN_ARCHIVE_SHA256 = {
    "2023": ("692e12c30c83adcaabdbac0d3954fafa55e1c89a24b36d95e72e02dff938652e", 83_451_409),
    "2024": ("04e668d3502757c837c13444730547cd967f28a2c49aeffb873d1792ab2cb97b", 83_913_260),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def processing_commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parents[3], text=True).strip()


def candidate_version() -> str:
    identity = _canonical({"source_sha256": EXPECTED_SHA256, "code_commit": processing_commit()})
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
) -> tuple[bytes, dict[str, object]]:
    temporary = tempfile.TemporaryDirectory(prefix="census-county-capture-") if capture_directory is None else None
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


def _inspect_archive(body: bytes) -> dict[str, object]:
    expected_members = {
        "tl_2023_us_county.cpg", "tl_2023_us_county.dbf", "tl_2023_us_county.prj",
        "tl_2023_us_county.shp", "tl_2023_us_county.shp.ea.iso.xml",
        "tl_2023_us_county.shp.iso.xml", "tl_2023_us_county.shx",
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
        with tempfile.TemporaryDirectory(prefix="census-county-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / "tl_2023_us_county.shp"
            frame = gpd.read_file(path, engine="pyogrio")
    fields = [name for name in frame.columns if name != "geometry"]
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if len(frame) != EXPECTED_FEATURE_COUNT or frame["GEOID"].isna().any() or frame["GEOID"].duplicated().any():
        raise ValueError("tiger_source_population_or_id_drift")
    if frame["GEOID"].astype(str).str.len().ne(5).any() or not frame["GEOID"].astype(str).str.fullmatch(r"\d{5}").all():
        raise ValueError("tiger_source_geoid_structure_drift")
    if any(str(frame[column].dtype) != "object" for column in STRING_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    if any(str(frame[column].dtype) != "int64" for column in ("ALAND", "AWATER")):
        raise ValueError("tiger_source_area_physical_type_drift")
    for column, allowed in (("MTFCC", EXPECTED_MTFCC), ("CLASSFP", EXPECTED_CLASSFP), ("FUNCSTAT", EXPECTED_FUNCSTAT)):
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
    schema = [{"name": name, "type": "string" if name in STRING_FIELDS else "int64",
               "nullable": bool(frame[name].isna().any())} for name in EXPECTED_FIELDS]
    return {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS,
        "attribute_types": {name: ("string" if name in STRING_FIELDS else "int64") for name in EXPECTED_FIELDS},
        "source_id_field": "GEOID", "source_id_sha256": id_digest,
        "source_schema_sha256": _sha(_canonical(schema)), "source_crs": EXPECTED_CRS,
        "geometry_type": "Polygon/MultiPolygon", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()),
        "mtfcc_values": sorted(EXPECTED_MTFCC), "classfp_values": sorted(EXPECTED_CLASSFP),
        "funcstat_values": sorted(EXPECTED_FUNCSTAT),
    }


def capture_us_county_boundaries(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture one exact official annual zip and stage it immutably for catalog."""
    body, receipt = _capture(capture_directory, staging_storage, version)
    audit = _inspect_archive(body)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{SOURCE_FILE}"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != EXPECTED_BYTES or staging_storage.sha256_key(archive_key) != EXPECTED_SHA256:
        raise ValueError("staged_tiger_archive_integrity_failure")
    reused = before is not None
    code_commit = processing_commit()
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "title": "US County Boundaries",
        "description": "2023 Census TIGER/Line US county (and county equivalent) shapefile. Candidate only; the raw/published schema and ID contract await maintainer approval and no promotion is authorized.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": "2023-11-23",
        "date_issued": SOURCE_VINTAGE,
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
            "release_date_unknown_reason": "The producer identifies this as the 2023 TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified date is recorded separately and is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP and all sidecars; catalog the county Shapefile without ID synthesis, schema widening, or attribute mapping."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line COUNTY product (producer publication). The DHS crosswalk row matches the Census Bureau county series and this TIGER/Line lineage; the DataLumos archive item 240942 V1 is the archived HIFLD copy (pointing at the Census 2024 national geodatabase) and is retained only as a labeled fallback (HTTP 403 to automated retrieval on 2026-10-21).",
    }
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
                            "archive_bytes": EXPECTED_BYTES, "last_modified_observed": "2023-11-23T02:21:53Z"},
                 "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": EXPECTED_BYTES,
        "source_archive_sha256": EXPECTED_SHA256, "source_cache_reused": reused,
        "code_commit": code_commit,
        "source_id_field": "GEOID",
        "source_coverage_scope": "All US counties and county equivalents (states, DC, Puerto Rico, island areas), 2023 vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key],
    }
