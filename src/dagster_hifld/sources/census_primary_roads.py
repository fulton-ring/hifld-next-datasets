"""Candidate-only acquisition of the official Census TIGER/Line primary roads."""
from __future__ import annotations

import json
import io
import struct
import zipfile
import tempfile
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import geopandas as gpd
import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources import census_tiger_common, census_latest
from dagster_hifld.sources.epa_frs_common import (
    capture_get,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

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
MAX_SOURCE_BYTES = 50_000_000

SOURCE_STEM = Path(SOURCE_FILE).stem
EXPECTED_MEMBERS = {
    f"{SOURCE_STEM}.cpg", f"{SOURCE_STEM}.dbf", f"{SOURCE_STEM}.prj",
    f"{SOURCE_STEM}.shp", f"{SOURCE_STEM}.shp.ea.iso.xml",
    f"{SOURCE_STEM}.shp.iso.xml", f"{SOURCE_STEM}.shx",
}
SHAPEFILE_MEMBER = f"{SOURCE_STEM}.shp"


CODE_IDENTITY_SCHEME = census_tiger_common.CODE_IDENTITY_SCHEME
PROCESSING_SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROCESSING_SOURCE_PACKAGE = PROCESSING_SOURCE_ROOT / "dagster_hifld"


def processing_code_identity() -> str:
    """Hash packaged Dagster source files without requiring repository metadata."""
    return census_tiger_common.packaged_code_identity(
        PROCESSING_SOURCE_ROOT, PROCESSING_SOURCE_PACKAGE,
    )


def candidate_version() -> str:
    return census_tiger_common.candidate_version(EXPECTED_SHA256, processing_code_identity())


def _capture(
    capture_directory: str | Path | None,
    staging_storage: StagingStorageResource,
    version: str,
) -> tuple[bytes, dict[str, object]]:
    body, receipt, _wire, _receipt_bytes = census_tiger_common.capture_tiger_zip(
        capture_directory, staging_storage, version,
        dataset_slug=DATASET_SLUG, source_url=SOURCE_URL,
        max_source_bytes=MAX_SOURCE_BYTES,
        expected_bytes=EXPECTED_BYTES, expected_sha256=EXPECTED_SHA256,
        client_timeout=httpx.Timeout(180.0, connect=20.0),
        capture_tmp_prefix="census-primary-roads-capture-",
        capture_get=capture_get,
    )
    return body, receipt


def _inspect_archive(body: bytes, *, edition: str | None = None) -> dict[str, object]:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        if sum(info.file_size for info in archive.infolist()) > 250_000_000:
            raise ValueError("tiger_archive_expansion_budget_exceeded")
    stem = f"tl_{edition}_us_primaryroads" if edition else SOURCE_STEM
    members = {name.replace(SOURCE_STEM, stem) for name in EXPECTED_MEMBERS}
    frame = census_tiger_common.read_tiger_shapefile(
        body,
        expected_members=members,
        shapefile_name=f"{stem}.shp",
        extract_tmp_prefix="census-primary-roads-",
        read_file=lambda path: gpd.read_file(path, engine="pyogrio"),
    )
    # Check physical records independently of the decoder, which can omit rows.
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        dbf = archive.read(f'{stem}.dbf')
        shx = archive.read(f'{stem}.shx')
        shp = archive.read(f'{stem}.shp')
    if len(dbf) < 33 or len(shx) < 100 or len(shp) < 100:
        raise ValueError('tiger_native_population_truncated_header')
    declared, header_length, record_length = struct.unpack_from('<IHH', dbf, 4)
    dbf_end = header_length + declared * record_length
    if (header_length < 33 or (header_length - 33) % 32 or not record_length
            or header_length > len(dbf) or dbf[header_length-1] != 13
            or len(dbf) < dbf_end or dbf[dbf_end:] not in (b'', b'\x1a')):
        raise ValueError('tiger_native_population_dbf_length')
    if any(dbf[header_length + index * record_length] != 32 for index in range(declared)):
        raise ValueError('tiger_native_population_deleted_or_invalid_dbf_record')
    for data in (shx, shp):
        if (struct.unpack_from('>I', data)[0] != 9994
                or struct.unpack_from('>I', data, 24)[0] * 2 != len(data)
                or struct.unpack_from('<II', data, 28) != (1000, 3)):
            raise ValueError('tiger_native_population_shape_header')
    if (len(shx) - 100) % 8 or (len(shx) - 100) // 8 != declared or len(frame) != declared:
        raise ValueError('tiger_native_population_count_mismatch')
    next_offset = 100
    for index in range(declared):
        offset, length = struct.unpack_from('>II', shx, 100 + index * 8)
        offset, length = offset * 2, length * 2
        if (offset != next_offset or offset + 8 + length > len(shp)
                or struct.unpack_from('>II', shp, offset) != (index + 1, length // 2)):
            raise ValueError('tiger_native_population_shape_record_mismatch')
        next_offset = offset + 8 + length
    if next_offset != len(shp):
        raise ValueError('tiger_native_population_unindexed_shape_records')
    fields = list(frame.columns[:-1])
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("tiger_source_schema_or_crs_drift")
    if (edition is None and len(frame) != EXPECTED_FEATURE_COUNT) or len(frame) == 0 or frame["LINEARID"].isna().any() or frame["LINEARID"].duplicated().any():
        raise ValueError("tiger_source_population_or_id_drift")
    if not frame["LINEARID"].map(lambda value: isinstance(value, str) and value.isascii() and value.isdecimal()).all():
        raise ValueError("tiger_source_id_lexical_physical_type_drift")
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
    if edition is None and bounds != EXPECTED_BOUNDS:
        raise ValueError("tiger_source_coverage_extent_drift")
    ids = sorted(frame["LINEARID"].tolist())
    id_digest = _sha(_canonical(ids))
    schema = [{"name": name, "type": "string", "nullable": bool(frame[name].isna().any())} for name in EXPECTED_FIELDS]
    return {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS, "attribute_types": {name: "string" for name in EXPECTED_FIELDS},
        "dbf_declared_record_count": declared, "shx_record_count": (len(shx)-100)//8,
        "deleted_dbf_record_count": 0,
        "source_id_field": "LINEARID", "source_id_sha256": id_digest,
        "source_schema_sha256": _sha(_canonical(schema)), "source_crs": EXPECTED_CRS,
        "geometry_type": "LineString", "bounds": bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()), "mtfcc_values": ["S1100"],
    }


def capture_census_primary_roads(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
    release: dict | None = None, captured: tuple | None = None,
    discovery_evidence: dict[str, bytes] | None = None,
) -> dict[str, object]:
    """Capture one exact official annual zip and stage it immutably for catalog."""
    body, receipt = captured if captured is not None else _capture(capture_directory, staging_storage, version)
    source_url = release["source_url"] if release else SOURCE_URL
    source_file = release["source_file"] if release else SOURCE_FILE
    vintage = release["edition"] if release else SOURCE_VINTAGE
    archive_sha = _sha(body) if release else EXPECTED_SHA256
    archive_bytes = len(body) if release else EXPECTED_BYTES
    audit = _inspect_archive(body, edition=vintage if release else None)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{source_file}"
    before = staging_storage.object_snapshot(archive_key)
    reused = before is not None
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": census_tiger_common.processing_runtime(),
    }
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "title": "Primary Roads",
        "description": f"{vintage} Census TIGER/Line national primary roads shapefile. Candidate only; crosswalk row and historical compatibility remain unresolved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Census Bureau",
        "source_url": source_url,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "date_issued": vintage,
        "inputs": [{
            "input_id": source_url,
            "producer": PUBLISHER,
            "host": urlsplit(source_url).hostname,
            "canonical_url": source_url,
            "retrieval_url": receipt["final_url"],
            "retrieved_at": observed_at,
            "sha256": archive_sha,
            "bytes": archive_bytes,
            "license": "https://www.usa.gov/government-works",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": f"The producer identifies this as the {vintage} TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified date is recorded separately and is not treated as the source data release date.",
        }],
        "transformations": ["Preserve the complete source ZIP and all sidecars; catalog the primary roads Shapefile without ID synthesis or attribute mapping."],
        "lineage": "Official Census Bureau Geography Division TIGER/Line product. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified.",
    }
    if release:
        manifest["inputs"][0]["refresh"] = {
            "maintenance_status": "maintained", "source_release_id": vintage,
            "data_period": vintage, "update_cadence": "unknown",
            "unknown_reasons": {"update_cadence": "Annual edition directories are observed; a producer cadence statement was not captured."},
            "latest_checked_at": release["latest_checked_at"],
            "latest_evidence_input_id": release["listing_url"],
            "maintenance_evidence_input_id": release["annual_listing_url"],
            "selection_rule": release["selection_rule"],
            "source_state_fingerprint": archive_sha,
        }
        for index, url in enumerate((release["annual_listing_url"], release["listing_url"])):
            evidence_receipt = release["evidence_receipts"][index]
            manifest["inputs"].append({
                "input_id": url, "producer": PUBLISHER, "host": "www2.census.gov",
                "role": "producer_publication", "purpose": "methodology",
                "canonical_url": url, "retrieval_url": url,
                "retrieved_at": evidence_receipt["observed_at"],
                "sha256": evidence_receipt["sha256"], "bytes": evidence_receipt["bytes"],
                "license": "https://www.usa.gov/government-works", "attribution": PUBLISHER,
                "release_date_unknown_reason": "Live producer directory observation; not a dated data release.",
            })
    validate_provenance(manifest)
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if staging_storage.object_exists(manifest_key):
        encoded_manifest = staging_storage.read_key(manifest_key)
        prior = json.loads(encoded_manifest)
        validate_provenance(prior)
        expected_prior = json.loads(_canonical(manifest))
        if isinstance(prior.get("inputs"), list) and len(prior["inputs"]) == len(manifest["inputs"]):
            expected_prior["inputs"][0]["retrieved_at"] = prior["inputs"][0].get("retrieved_at")
            if release:
                expected_prior["inputs"][0]["refresh"]["latest_checked_at"] = prior["inputs"][0]["refresh"]["latest_checked_at"]
                expected_prior["inputs"][1:] = prior["inputs"][1:]
        if prior != expected_prior:
            raise ValueError("cached_source_manifest_identity_mismatch")
    else:
        encoded_manifest = _canonical(manifest) + b"\n"
        write_immutable(staging_storage, manifest_key, encoded_manifest)
    stable_receipt = {key: receipt.get(key) for key in (
        "method", "url", "final_url", "status", "complete_body", "wire_bytes",
        "wire_sha256", "bytes", "sha256", "content_encoding", "content_length",
    )}
    audit_doc = {"source": {"url": source_url, "vintage": vintage, "archive_sha256": archive_sha, "archive_bytes": archive_bytes}, "processing_code": processing_code, "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    audit_bytes = _canonical(audit_doc) + b"\n"
    write_immutable(staging_storage, audit_key, audit_bytes)
    if staging_storage.read_key(manifest_key) != encoded_manifest or staging_storage.read_key(audit_key) != audit_bytes:
        raise ValueError('current_tiger_metadata_readback_mismatch')
    discovery_keys = []
    if release:
        if not discovery_evidence:
            raise ValueError('current_tiger_discovery_evidence_required')
        evidence_prefix = f'{prefix}/metadata/discovery'
        if not staging_storage.object_exists(evidence_prefix + '/release.json'):
            for name, value in discovery_evidence.items():
                key = f'{evidence_prefix}/{name}'
                write_immutable(staging_storage, key, value)
                if staging_storage.read_key(key) != value:
                    raise ValueError('current_tiger_discovery_readback_mismatch')
        for name in discovery_evidence:
            key = f'{evidence_prefix}/{name}'
            if not staging_storage.object_exists(key):
                raise ValueError('current_tiger_discovery_evidence_incomplete')
            discovery_keys.append(key)
    # Expose geodata only after metadata and discovery evidence are complete.
    # The current sensor does not read ready markers and excludes hash versions.
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != archive_bytes or staging_storage.sha256_key(archive_key) != archive_sha:
        raise ValueError("staged_tiger_archive_integrity_failure")
    ready_key = f'{prefix}/metadata/candidate_ready.json'
    written_keys = [archive_key, manifest_key, audit_key, *discovery_keys]
    ready = {'version': version, 'source_archive_sha256': archive_sha, 'release_accepted': False,
             'source_population_complete': True,
             'objects': {key.removeprefix(prefix+'/'): _sha(staging_storage.read_key(key)) for key in written_keys}}
    write_immutable(staging_storage, ready_key, _canonical(ready) + b'\n')
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_edition": vintage, "latest_release_discovered": release is not None, "source_archive_bytes": archive_bytes,
        "source_archive_sha256": archive_sha, "source_cache_reused": reused,
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": "LINEARID", "source_coverage_scope": f"U.S. nationwide Census TIGER primary roads, {vintage} vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "capture_mode": "current_discovery" if release else "historical_pinned_reproduction",
        "written_keys": [*written_keys, ready_key],
    }


def capture_latest_census_primary_roads(staging_storage: StagingStorageResource, *, client=None) -> dict:
    """Discover and capture current producer roads; retain failures in quarantine.

    The historical pinned capture entrypoint remains available for explicitly
    supplied old receipts. This current path never applies its 2025 count/hash.
    """
    from_context = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(180.0, connect=20.0), follow_redirects=False)
    with tempfile.TemporaryDirectory(prefix='census-current-roads-') as tmp:
        root = Path(tmp)
        try:
            release = census_latest.discover_primary_roads(client, root)
            body, receipt = capture_get(client, release['source_url'], root, 'source', max_bytes=MAX_SOURCE_BYTES)
            if receipt.get('content_encoding') != 'identity' or not body:
                raise ValueError('current_tiger_archive_encoding_or_empty')
            # Recheck the selected directory after reading the entire ZIP. A new
            # annual publication or changed listing requires another capture.
            after = census_latest.discover_primary_roads(client, root / 'after')
            if any(release[key] != after[key] for key in ('edition', 'source_url', 'listing_sha256')):
                raise ValueError('current_tiger_listing_changed_during_capture')
            version = census_latest.release_version(release['edition'], receipt['sha256'], processing_code_identity())
            evidence = {f'{name}.{suffix}': (root / f'{name}.{suffix}').read_bytes()
                        for name in ('annual-listing', 'product-listing', 'source')
                        for suffix in ('body', 'wire', 'receipt.json')}
            evidence['release.json'] = _canonical(release) + b'\n'
            result = capture_census_primary_roads(staging_storage, version=version,
                                                   release=release, captured=(body, receipt), discovery_evidence=evidence)
            return result
        except Exception as exc:
            # Keep bounded successful and partial inputs, including discovery,
            # outside catalog paths. A failed check is never an unchanged result.
            location = f'_quarantine/{DATASET_SLUG}/' + _sha(_canonical({'error':type(exc).__name__, 'files':[p.name for p in root.rglob('*') if p.is_file()]}))[:16]
            # A fresh failed attempt needs its own immutable receipt namespace.
            location += '-' + uuid4().hex
            for path in root.rglob('*'):
                if path.is_file():
                    write_immutable(staging_storage, location + '/' + path.relative_to(root).as_posix(), path.read_bytes())
            write_immutable(staging_storage, location + '/failure.json', _canonical({'reason': str(exc), 'complete':False, 'release_accepted':False}))
            raise ValueError(f'current_tiger_capture_failed; quarantined_at:{location}') from exc
        finally:
            if from_context:
                client.close()
