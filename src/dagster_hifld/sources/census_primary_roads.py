"""Candidate-only acquisition of the official Census TIGER/Line primary roads."""
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import geopandas as gpd
import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.epa_frs_common import capture_get, write_immutable

DATASET_SLUG = "primary-roads-2"
FILE_SLUG = DATASET_SLUG
SOURCE_VINTAGE = "2025"
SOURCE_URL = "https://www2.census.gov/geo/tiger/TIGER2025/PRIMARYROADS/tl_2025_us_primaryroads.zip"
SOURCE_FILE = "tl_2025_us_primaryroads.zip"
PUBLISHER = "U.S. Census Bureau, Geography Division"
EXPECTED_SHA256 = "400453e97b9e6693dfecb7362ce7a6cf260d27050f7d84d2a024ba0710b94c07"
EXPECTED_BYTES = 38_379_400
EXPECTED_FEATURE_COUNT = 17_500
EXPECTED_FIELDS = ["LINEARID", "FULLNAME", "RTTYP", "MTFCC"]
EXPECTED_CRS = "EPSG:4269"
EXPECTED_BOUNDS = [-158.104182, 17.974171, -65.648659, 61.571228]
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 50_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def processing_commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parents[3], text=True).strip()


def candidate_version() -> str:
    identity = _canonical({"source_sha256": EXPECTED_SHA256, "code_commit": processing_commit()})
    return "v" + _sha(identity)[:32]


def _capture(capture_directory: str | Path | None) -> tuple[bytes, dict[str, object]]:
    if capture_directory is None:
        with tempfile.TemporaryDirectory(prefix="census-primary-roads-capture-") as tmp:
            with httpx.Client(timeout=httpx.Timeout(180.0, connect=20.0), follow_redirects=False) as client:
                body, receipt = capture_get(client, SOURCE_URL, Path(tmp), "source", max_bytes=MAX_SOURCE_BYTES)
    else:
        root = Path(capture_directory)
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
        raise ValueError("official_tiger_archive_identity_or_release_drift")
    return body, receipt


def _inspect_archive(body: bytes) -> dict[str, object]:
    expected_members = {
        "tl_2025_us_primaryroads.cpg", "tl_2025_us_primaryroads.dbf", "tl_2025_us_primaryroads.prj",
        "tl_2025_us_primaryroads.shp", "tl_2025_us_primaryroads.shp.ea.iso.xml",
        "tl_2025_us_primaryroads.shp.iso.xml", "tl_2025_us_primaryroads.shx",
    }
    with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
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
        with tempfile.TemporaryDirectory(prefix="census-primary-roads-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / "tl_2025_us_primaryroads.shp"
            frame = gpd.read_file(path, engine="pyogrio")
    fields = list(frame.columns[:-1])
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if len(frame) != EXPECTED_FEATURE_COUNT or frame["LINEARID"].isna().any() or frame["LINEARID"].duplicated().any():
        raise ValueError("tiger_source_population_or_id_drift")
    if frame["MTFCC"].isna().any() or set(frame["MTFCC"].unique()) != {"S1100"}:
        raise ValueError("tiger_primary_roads_classification_drift")
    if any(str(frame[column].dtype) != "object" for column in EXPECTED_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_source_geometry_quality_failure")
    if set(geometry.geom_type) != {"LineString"}:
        raise ValueError("tiger_source_geometry_type_drift")
    bounds = [float(value) for value in frame.total_bounds]
    if bounds != EXPECTED_BOUNDS:
        raise ValueError("tiger_source_coverage_extent_drift")
    ids = sorted(frame["LINEARID"].tolist())
    id_digest = _sha(_canonical(ids))
    schema = [{"name": name, "type": "string", "nullable": bool(frame[name].isna().any())} for name in EXPECTED_FIELDS]
    return {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS, "attribute_types": {name: "string" for name in EXPECTED_FIELDS},
        "source_id_field": "LINEARID", "source_id_sha256": id_digest,
        "source_schema_sha256": _sha(_canonical(schema)), "source_crs": EXPECTED_CRS,
        "geometry_type": "LineString", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()), "mtfcc_values": ["S1100"],
    }


def capture_census_primary_roads(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture one exact official annual zip and stage it immutably for catalog."""
    body, receipt = _capture(capture_directory)
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
        "title": "Primary Roads",
        "description": "2025 Census TIGER/Line national primary roads shapefile. Candidate only; crosswalk row and historical compatibility remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": "2025-09-23",
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
            "release_date_unknown_reason": "The producer identifies this as the 2025 TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified date is recorded separately and is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP and all sidecars; catalog the primary roads Shapefile without ID synthesis or attribute mapping."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line product. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified.",
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
    audit_doc = {"source": {"url": SOURCE_URL, "vintage": SOURCE_VINTAGE, "release_modified": "2025-09-23", "archive_sha256": EXPECTED_SHA256, "archive_bytes": EXPECTED_BYTES}, "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": EXPECTED_BYTES,
        "source_archive_sha256": EXPECTED_SHA256, "source_cache_reused": reused,
        "code_commit": code_commit,
        "source_id_field": "LINEARID", "source_coverage_scope": "U.S. nationwide Census TIGER primary roads, 2025 vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key],
    }
