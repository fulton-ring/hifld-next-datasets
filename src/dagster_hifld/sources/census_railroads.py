"""Candidate-only acquisition of the official Census TIGER/Line national Rails."""
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import geopandas as gpd
import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources import census_latest, census_tiger_common
from dagster_hifld.sources.capture_common import (
    capture_get,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "railroads"
FILE_SLUG = DATASET_SLUG
LAYER = "RAILS"
PRODUCT = "rails"
PUBLISHER = "U.S. Census Bureau, Geography Division"
FEATURE_COUNT_RANGE = (50_000, 500_000)
EXPECTED_FIELDS = ["LINEARID", "FULLNAME", "MTFCC"]
EXPECTED_CRS = "EPSG:4269"
EXPECTED_MTFCC_VALUES = ["R1011"]
MAX_SOURCE_BYTES = 60_000_000

CODE_IDENTITY_SCHEME = census_tiger_common.CODE_IDENTITY_SCHEME
PROCESSING_SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROCESSING_SOURCE_PACKAGE = PROCESSING_SOURCE_ROOT / "dagster_hifld"


def processing_code_identity() -> str:
    """Hash packaged Dagster source files without requiring repository metadata."""
    return census_tiger_common.packaged_code_identity(
        PROCESSING_SOURCE_ROOT, PROCESSING_SOURCE_PACKAGE,
    )


def candidate_version(release: census_latest.ShapefileRelease, archive_sha256: str) -> str:
    return census_latest.release_version(str(release.vintage), archive_sha256, processing_code_identity())


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
        capture_tmp_prefix="census-railroads-capture-",
        capture_get=capture_get,
    )
    return body, receipt, release


def _inspect_archive(body: bytes, release: census_latest.ShapefileRelease) -> dict[str, object]:
    frame = census_tiger_common.read_tiger_shapefile(
        body,
        expected_members=census_tiger_common.tiger_shapefile_members(release.source_file),
        shapefile_name=census_tiger_common.tiger_shapefile_name(release.source_file),
        extract_tmp_prefix="census-railroads-",
        read_file=lambda path: gpd.read_file(path, engine="pyogrio"),
    )
    fields = list(frame.columns[:-1])
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if not (FEATURE_COUNT_RANGE[0] <= len(frame) <= FEATURE_COUNT_RANGE[1]) \
            or frame["LINEARID"].isna().any() or frame["LINEARID"].duplicated().any():
        raise ValueError("tiger_source_population_or_id_drift")
    if frame["MTFCC"].isna().any() or set(frame["MTFCC"].unique()) != set(EXPECTED_MTFCC_VALUES):
        raise ValueError("tiger_rails_classification_drift")
    if any(str(frame[column].dtype) != "object" for column in EXPECTED_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_source_geometry_quality_failure")
    if set(geometry.geom_type) != {"LineString"}:
        raise ValueError("tiger_source_geometry_type_drift")
    bounds = [float(value) for value in frame.total_bounds]
    if not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
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
        "invalid_geometry_count": int((~geometry.is_valid).sum()), "mtfcc_values": sorted(set(frame["MTFCC"].tolist())),
    }


def capture_census_railroads(
    staging_storage: StagingStorageResource, *, version: str | None = None,
    capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture the newest published official annual zip and stage it immutably for catalog."""
    body, receipt, release = _capture(capture_directory, staging_storage, version or "pending")
    version = version or candidate_version(release, receipt["sha256"])
    audit = _inspect_archive(body, release)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{release.source_file}"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != len(body) or staging_storage.sha256_key(archive_key) != receipt["sha256"]:
        raise ValueError("staged_tiger_archive_integrity_failure")
    reused = before is not None
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": census_tiger_common.processing_runtime(),
    }
    observed_at = receipt.get("observed_at")
    vintage = str(release.vintage)
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "title": "Railroads",
        "description": f"{vintage} Census TIGER/Line national Rails shapefile (railroad spur lines, rail yards, mass transit rail lines such as carlines, streetcar tracks, monorail or other mass transit rail, and special purpose rail lines such as cog rail lines, incline rail lines and trams). Candidate only; crosswalk row and historical compatibility remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": release.source_url,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": None,
        "date_issued": vintage,
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
            "release_date_unknown_reason": f"The producer identifies this as the {vintage} TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified date is recorded separately and is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP and all sidecars; catalog the Rails Shapefile without ID synthesis or attribute mapping."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line product, resolved from the producer's current annual listing at capture time. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified.",
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
    audit_doc = {"source": {"url": release.source_url, "vintage": vintage, "archive_sha256": receipt["sha256"], "archive_bytes": receipt["bytes"], "source_release": release.as_metadata()}, "processing_code": processing_code, "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": receipt["bytes"],
        "source_archive_sha256": receipt["sha256"], "source_cache_reused": reused,
        "source_vintage": vintage, "source_release": release.as_metadata(),
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": "LINEARID", "source_coverage_scope": f"U.S. nationwide Census TIGER rails, {vintage} vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key],
    }
