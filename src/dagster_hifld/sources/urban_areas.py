"""Candidate-only acquisition of the official Census TIGER/Line 2026 Urban Areas.

Inventory entry ``urban-areas-1`` ("Urban Areas", agency U.S. Census Bureau).
The producer's maintained publication is the current TIGER/Line UAC20 national shapefile, resolved from the producer's annual listing at capture time. The project source policy requires the latest TIGER vintage, so a superseded edition is never fetched. The DataLumos project archive is banned identification context only and is never fetched.
"""
from __future__ import annotations

import hashlib
import json
import platform
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import geopandas as gpd
import httpx
import pyogrio
import pyproj
import shapely

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources import census_latest, census_tiger_common
from dagster_hifld.sources.capture_common import (
    capture_get,
    quarantine_incomplete_capture,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "urban-areas-1"
FILE_SLUG = DATASET_SLUG
LAYER = "UAC20"
PRODUCT = "uac20"
PUBLISHER = "U.S. Census Bureau, Geography Division"
# Broad sanity bounds; the producer publishes roughly 2,600 urban areas.
FEATURE_COUNT_RANGE = (1, 20_000)
EXPECTED_FIELDS = [
    "UACE20", "GEOID20", "GEOIDFQ20", "NAME20", "NAMELSAD20", "LSAD20",
    "MTFCC20", "FUNCSTAT20", "ALAND20", "AWATER20", "INTPTLAT20", "INTPTLON20",
]
INT_FIELDS = ("ALAND20", "AWATER20")
EXPECTED_CRS = "EPSG:4269"
EXPECTED_GEOID_LENGTH = 5
EXPECTED_CONSTANT_VALUES = {"MTFCC20": {"G3500"}, "FUNCSTAT20": {"S"}, "LSAD20": {"67"}}
EXPECTED_GEOMETRY_TYPES = {"Polygon", "MultiPolygon"}
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 90_000_000


def _processing_runtime() -> dict[str, str]:
    """Report runtime versions observed during extraction without asserting Git identity."""
    return {
        "python": platform.python_version(),
        "geopandas": gpd.__version__,
        "httpx": httpx.__version__,
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


def candidate_version(release: census_latest.ShapefileRelease, archive_sha256: str) -> str:
    return census_latest.release_version(str(release.vintage), archive_sha256, processing_code_identity())


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
            "url": "",
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
) -> tuple[bytes, dict[str, object], census_latest.ShapefileRelease]:
    body, receipt, _wire, _receipt_bytes, release = census_tiger_common.capture_tiger_shapefile(
        capture_directory, staging_storage, version,
        dataset_slug=DATASET_SLUG, layer=LAYER, product=PRODUCT,
        max_source_bytes=MAX_SOURCE_BYTES,
        client_timeout=httpx.Timeout(180.0, connect=20.0),
        capture_tmp_prefix="census-urban-areas-capture-",
        capture_get=capture_get,
    )
    return body, receipt, release


def _inspect_archive(body: bytes, release: census_latest.ShapefileRelease) -> dict[str, object]:
    expected_members = census_tiger_common.tiger_shapefile_members(release.source_file)
    import io

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
        with tempfile.TemporaryDirectory(prefix="census-urban-areas-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / census_tiger_common.tiger_shapefile_name(release.source_file)
            frame = gpd.read_file(path, engine="pyogrio")
    fields = list(frame.columns[:-1])
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if not (FEATURE_COUNT_RANGE[0] <= len(frame) <= FEATURE_COUNT_RANGE[1]):
        raise ValueError("tiger_source_population_drift")
    if frame["GEOID20"].isna().any() or frame["GEOID20"].duplicated().any() \
            or set(frame["GEOID20"].str.len()) != {EXPECTED_GEOID_LENGTH}:
        raise ValueError("tiger_source_id_roster_drift")
    for column, allowed in EXPECTED_CONSTANT_VALUES.items():
        if frame[column].isna().any() or set(frame[column].unique()) != allowed:
            raise ValueError("tiger_source_classification_value_drift:" + column)
    for column in ("UACE20", "NAME20", "NAMELSAD20"):
        if frame[column].isna().any() or not (frame[column].str.len() > 0).all():
            raise ValueError("tiger_source_required_attribute_drift:" + column)
    for column in ("INTPTLAT20", "INTPTLON20"):
        if frame[column].isna().any() or not (frame[column].str.len() > 0).all():
            raise ValueError("tiger_source_interior_point_drift:" + column)
    types = {name: str(frame[name].dtype) for name in EXPECTED_FIELDS}
    if any(types[name] != "int64" for name in INT_FIELDS) \
            or any(types[name] != "object" for name in EXPECTED_FIELDS if name not in INT_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    ids = sorted(frame["GEOID20"].tolist())
    id_digest = _sha(_canonical(ids))
    schema = [{"name": name, "type": "string", "nullable": bool(frame[name].isna().any())} for name in EXPECTED_FIELDS]
    schema_digest = _sha(_canonical(schema))
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_source_geometry_quality_failure")
    if set(geometry.geom_type) != EXPECTED_GEOMETRY_TYPES:
        raise ValueError("tiger_source_geometry_type_drift")
    bounds = [float(value) for value in frame.total_bounds]
    if not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
        raise ValueError("tiger_source_coverage_extent_drift")
    return {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS, "attribute_types": types,
        "source_id_field": "GEOID20", "source_id_sha256": id_digest,
        "source_schema_sha256": schema_digest, "source_crs": EXPECTED_CRS,
        "geometry_type": "Polygon/MultiPolygon", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()),
        "constant_value_counts": {name: frame[name].value_counts().to_dict() for name in EXPECTED_CONSTANT_VALUES},
    }


def capture_census_urban_areas(
    staging_storage: StagingStorageResource, *, version: str | None = None,
    capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture the newest official urban areas zip and stage it immutably for catalog."""
    body, receipt, release = _capture(capture_directory, staging_storage, version or "pending")
    version = version or candidate_version(release, receipt["sha256"])
    audit = _inspect_archive(body, release)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{release.source_file}"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != receipt["bytes"] or staging_storage.sha256_key(archive_key) != receipt["sha256"]:
        raise ValueError("staged_tiger_archive_integrity_failure")
    reused = before is not None
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": _processing_runtime(),
    }
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "title": "Urban Areas",
        "description": f"Census-delineated national urban areas from the maintained {release.vintage} TIGER/Line UAC20 national shapefile. Candidate only; crosswalk row and historical compatibility remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": release.source_url,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": "2026-10-14",
        "date_issued": str(release.vintage),
        "inputs": [{
            "input_id": release.source_url,
            "producer": PUBLISHER,
            "host": urlsplit(release.source_url).hostname,
            "canonical_url": release.source_url,
            "retrieval_url": receipt["final_url"],
            "retrieved_at": observed_at,
            "sha256": receipt["sha256"],
            "bytes": receipt["bytes"],
            "license": "https://www.usa.gov/government-works",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": f"The producer identifies this as the {release.vintage} TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified date is recorded separately and is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP and all sidecars; catalog the urban areas Shapefile without ID synthesis or attribute mapping."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line Urban Areas product (UAC20), resolved from the producer's current annual listing at capture time. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified. DataLumos is banned identification context only.",
        "source_release": release.as_metadata(),
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
    audit_doc = {"source": {"url": release.source_url, "vintage": str(release.vintage), "archive_sha256": receipt["sha256"], "archive_bytes": receipt["bytes"], "source_release": release.as_metadata()}, "processing_code": processing_code, "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": receipt["bytes"],
        "source_archive_sha256": receipt["sha256"], "source_cache_reused": reused,
        "source_vintage": str(release.vintage), "source_release": release.as_metadata(),
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": "GEOID20", "source_coverage_scope": "U.S., Puerto Rico and Island Areas Census 2020-delineated urban areas as of January 1, 2026 (TIGER/Line 2026 UAC20 vintage)",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key],
    }
