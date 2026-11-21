"""Candidate-only acquisition of the official current Census TIGER/Line CBSA file.

Inventory entry ``micropolitan-statistical-areas-3`` ("Micropolitan Statistical
Areas", agency U.S. Census Bureau). The producer's maintained publication is the
current TIGER/Line national CBSA file, resolved from the producer's annual
listing at capture time. The inventory defines this dataset as the producer's
own ``Core_Based_Statistical_Area`` layer restricted to micropolitan areas, i.e.
``MEMI = "2"``; the same selection is applied here to the producer's national
file. No projection, ID substitution, geometry repair or attribute mapping
occurs here. The DataLumos project archive is banned identification context only.
"""
from __future__ import annotations

import hashlib
import io
import json
import platform
import shutil
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

DATASET_SLUG = "micropolitan-statistical-areas-3"
FILE_SLUG = DATASET_SLUG
LAYER = "CBSA"
PRODUCT = "cbsa"
PUBLISHER = "U.S. Census Bureau, Geography Division"
# Broad sanity bounds; the producer publishes roughly 935 CBSAs, about 542 micropolitan.
FULL_FEATURE_COUNT_RANGE = (1, 20_000)
MICROPOLITAN_COUNT_RANGE = (1, 20_000)
EXPECTED_FIELDS = [
    "CSAFP", "CBSAFP", "GEOID", "GEOIDFQ", "NAME", "NAMELSAD", "LSAD",
    "MEMI", "MTFCC", "ALAND", "AWATER", "INTPTLAT", "INTPTLON",
]
INT_FIELDS = ("ALAND", "AWATER")
EXPECTED_CRS = "EPSG:4269"
EXPECTED_GEOID_LENGTH = 5
EXPECTED_CONSTANT_VALUES = {"MTFCC": {"G3110"}, "LSAD": {"M2"}, "MEMI": {"2"}}
# Producer-nullable CBSAFP/CSAFP: a micropolitan area belongs to no combined
# statistical area unless it is a component of one.
NULLABLE_FIELDS = ("CSAFP",)
EXPECTED_GEOMETRY_TYPES = {"Polygon", "MultiPolygon"}
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 45_000_000


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
        capture_tmp_prefix="census-micropolitan-capture-",
        capture_get=capture_get,
    )
    return body, receipt, release


def _schema_document(frame: gpd.GeoDataFrame) -> list[dict[str, object]]:
    return [
        {"name": name, "type": "string", "nullable": bool(frame[name].isna().any())}
        for name in EXPECTED_FIELDS
    ]


def _inspect_archive(body: bytes, release: census_latest.ShapefileRelease) -> dict[str, object]:
    member_stem = Path(release.source_file).stem
    expected_members = census_tiger_common.tiger_shapefile_members(release.source_file)
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
        with tempfile.TemporaryDirectory(prefix="census-micropolitan-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / census_tiger_common.tiger_shapefile_name(release.source_file)
            full = gpd.read_file(path, engine="pyogrio")
    fields = list(full.columns[:-1])
    if fields != EXPECTED_FIELDS or str(full.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if not (FULL_FEATURE_COUNT_RANGE[0] <= len(full) <= FULL_FEATURE_COUNT_RANGE[1]):
        raise ValueError("tiger_source_population_drift")
    if full["GEOID"].isna().any() or full["GEOID"].duplicated().any() \
            or set(full["GEOID"].str.len()) != {EXPECTED_GEOID_LENGTH}:
        raise ValueError("tiger_source_id_roster_drift")
    counts = full["MEMI"].value_counts().to_dict()
    if set(counts) != {"1", "2"} or not (
            MICROPOLITAN_COUNT_RANGE[0] <= counts.get("2", 0) <= MICROPOLITAN_COUNT_RANGE[1]) \
            or counts.get("1", 0) < 1:
        raise ValueError("tiger_source_memi_split_drift")
    subset = full[full["MEMI"] == "2"].reset_index(drop=True)
    for column, allowed in EXPECTED_CONSTANT_VALUES.items():
        if subset[column].isna().any() or set(subset[column].unique()) != allowed:
            raise ValueError("tiger_source_classification_value_drift:" + column)
    for column in ("CBSAFP", "GEOIDFQ", "NAME", "NAMELSAD"):
        if subset[column].isna().any() or not (subset[column].str.len() > 0).all():
            raise ValueError("tiger_source_required_attribute_drift:" + column)
    for column in ("INTPTLAT", "INTPTLON"):
        if subset[column].isna().any() or not (subset[column].str.len() > 0).all():
            raise ValueError("tiger_source_interior_point_drift:" + column)
    types = {name: str(subset[name].dtype) for name in EXPECTED_FIELDS}
    if any(types[name] != "int64" for name in INT_FIELDS) \
            or any(types[name] != "object" for name in EXPECTED_FIELDS if name not in INT_FIELDS):
        raise ValueError("tiger_source_attribute_physical_type_drift")
    ids = sorted(subset["GEOID"].tolist())
    id_digest = _sha(_canonical(ids))
    schema = _schema_document(subset)
    schema_digest = _sha(_canonical(schema))
    geometry = subset.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_source_geometry_quality_failure")
    if set(geometry.geom_type) != EXPECTED_GEOMETRY_TYPES:
        raise ValueError("tiger_source_geometry_type_drift")
    bounds = [float(value) for value in subset.total_bounds]
    if not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
        raise ValueError("tiger_source_coverage_extent_drift")
    return {
        "feature_count": len(subset), "attribute_fields": EXPECTED_FIELDS, "attribute_types": types,
        "source_id_field": "GEOID", "source_id_sha256": id_digest,
        "source_schema_sha256": schema_digest, "source_crs": EXPECTED_CRS,
        "geometry_type": "Polygon/MultiPolygon", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()),
        "null_value_counts": {name: int(subset[name].isna().sum()) for name in NULLABLE_FIELDS},
        "constant_value_counts": {name: subset[name].value_counts().to_dict() for name in EXPECTED_CONSTANT_VALUES},
        "full_file_feature_count": len(full),
        "full_file_memi_counts": {str(key): int(value) for key, value in counts.items()},
    }


def _stage_subset_shapefile(subset: gpd.GeoDataFrame, target_dir: Path, cpg_body: bytes, vintage: int) -> None:
    stem = DATASET_SLUG
    # GDAL defaults DBF_DATE_LAST_UPDATE to the write date; every same-version
    # replay regenerates the sidecars and compares bytes, so an unpinned date
    # would fail immutable_staged_object_changed on any later replay day. Pin
    # the staged DBF to the source release date to keep the output date-stable.
    subset.to_file(target_dir / f"{stem}.shp", engine="pyogrio", DBF_DATE_LAST_UPDATE=f"{vintage}-01-01")
    staged_names = {path.name for path in target_dir.iterdir()}
    required = {f"{stem}.shp", f"{stem}.shx", f"{stem}.dbf"}
    missing = sorted(required - staged_names)
    if missing:
        raise ValueError("staged_subset_shapefile_incomplete:" + ",".join(missing))
    cpg_path = target_dir / f"{stem}.cpg"
    if not cpg_path.exists():
        cpg_path.write_bytes(cpg_body)


def capture_census_micropolitan_statistical_areas(
    staging_storage: StagingStorageResource, *, version: str | None = None,
    capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture the newest official CBSA zip and stage its micropolitan subset for catalog."""
    body, receipt, release = _capture(capture_directory, staging_storage, version or "pending")
    version = version or candidate_version(release, receipt["sha256"])
    member_stem = Path(release.source_file).stem
    audit = _inspect_archive(body, release)
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        cpg_body = archive.read(f"{member_stem}.cpg")
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    provenance_key = f"{prefix}/provenance/{release.source_file}"
    write_immutable(staging_storage, provenance_key, body)
    if staging_storage.sha256_key(provenance_key) != receipt["sha256"]:
        raise ValueError("staged_tiger_archive_integrity_failure")
    shapefile_dir_key = f"{prefix}/shapefile"
    reused = staging_storage.object_exists(f"{shapefile_dir_key}/{DATASET_SLUG}.shp")
    staged_keys: list[str] = [provenance_key]
    with tempfile.TemporaryDirectory(prefix="census-micropolitan-stage-") as tmp:
        stage_dir = Path(tmp) / "shapefile"
        stage_dir.mkdir()
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            with tempfile.TemporaryDirectory(prefix="census-micropolitan-extract-") as extract_tmp:
                archive.extractall(extract_tmp)
                full = gpd.read_file(Path(extract_tmp) / census_tiger_common.tiger_shapefile_name(release.source_file), engine="pyogrio")
        subset = full[full["MEMI"] == "2"].reset_index(drop=True)
        _stage_subset_shapefile(subset, stage_dir, cpg_body, release.vintage)
        staged = gpd.read_file(stage_dir / f"{DATASET_SLUG}.shp", engine="pyogrio")
        if len(staged) != len(subset) \
                or sorted(staged["GEOID"].tolist()) != sorted(subset["GEOID"].tolist()) \
                or list(staged.columns[:-1]) != EXPECTED_FIELDS:
            raise ValueError("staged_subset_shapefile_identity_failure")
        for sidecar in sorted(stage_dir.iterdir()):
            key = f"{shapefile_dir_key}/{sidecar.name}"
            write_immutable(staging_storage, key, sidecar.read_bytes())
            staged_keys.append(key)
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
        "title": "Micropolitan Statistical Areas",
        "description": f"Micropolitan statistical areas (CBSAs associated with at least one urban cluster of "
                       f"population 10,000 to less than 50,000) selected from the maintained {release.vintage} "
                       f"TIGER/Line CBSA national file by the producer-defined MEMI=2 indicator. "
                       "Candidate only; crosswalk row and historical compatibility remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": release.source_url,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": None,
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
        "transformations": ["Preserve the complete source ZIP and all sidecars; select the producer-defined micropolitan subset MEMI=2 from the Core_Based_Statistical_Area national file without ID synthesis, attribute mapping, reprojection or geometry repair."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line national CBSA file, resolved from the producer's current annual listing at capture time. The inventory defines this dataset as the producer's Core_Based_Statistical_Area layer where MEMI=2; the same selection is applied to the resolved national file. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified. DataLumos is banned identification context only.",
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
        "source_id_field": "GEOID", "source_coverage_scope": "U.S. micropolitan statistical areas (MEMI=2) as of January 1, 2026 (TIGER/Line 2026 CBSA national file)",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [*staged_keys, manifest_key, audit_key],
    }
