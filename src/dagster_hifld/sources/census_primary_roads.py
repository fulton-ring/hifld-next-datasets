"""Candidate-only acquisition of the official Census TIGER/Line primary roads."""
from __future__ import annotations

import json
import io
import struct
import zipfile
import tempfile
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from xml.etree import ElementTree as ET

import geopandas as gpd
import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.source_release import (
    ReleaseSignal, decide_release, decide_unchanged_poll, load_release_state,
    probe_transport, record_release_state, release_state_record, unchanged_result,
)
from dagster_hifld.sources import census_tiger_common, census_latest
from dagster_hifld.sources.capture_common import (
    capture_get,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "primary-roads-2"
FILE_SLUG = DATASET_SLUG
LAYER = "PRIMARYROADS"
PRODUCT = "primaryroads"
PUBLISHER = "U.S. Census Bureau, Geography Division"
RELEASE_SELECTION_RULE = (
    "Newest exact TIGER20YY directory in the producer listing, requiring one "
    "national PRIMARYROADS ZIP. The edition is the release identity; a changed "
    "archive under the same edition is a same-release correction."
)
# Broad sanity bounds; the producer publishes roughly 17,000-18,000 primary roads.
FEATURE_COUNT_RANGE = (1, 100_000)
EXPECTED_FIELDS = ["LINEARID", "FULLNAME", "RTTYP", "MTFCC"]
EXPECTED_CRS = "EPSG:4269"
MAX_SOURCE_BYTES = 50_000_000

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
        capture_tmp_prefix="census-primary-roads-capture-",
        capture_get=capture_get,
    )
    return body, receipt, release


def _inspect_archive(body: bytes, release: census_latest.ShapefileRelease) -> dict[str, object]:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        if sum(info.file_size for info in archive.infolist()) > 250_000_000:
            raise ValueError("tiger_archive_expansion_budget_exceeded")
    stem = Path(release.source_file).stem
    frame = census_tiger_common.read_tiger_shapefile(
        body,
        expected_members=census_tiger_common.tiger_shapefile_members(release.source_file),
        shapefile_name=census_tiger_common.tiger_shapefile_name(release.source_file),
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
    if not (FEATURE_COUNT_RANGE[0] <= len(frame) <= FEATURE_COUNT_RANGE[1]) \
            or frame["LINEARID"].isna().any() or frame["LINEARID"].duplicated().any():
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
    if not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
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


def _current_native_metadata(body: bytes, stem: str, input_id: str) -> tuple[dict, dict, str | None]:
    """Retain exact native XML and project only definitions the producer supplied."""
    namespaces = {'gfc': 'http://www.isotc211.org/2005/gfc',
                  'gco': 'http://www.isotc211.org/2005/gco',
                  'gmd': 'http://www.isotc211.org/2005/gmd'}
    documents = {}
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for suffix in ('.shp.ea.iso.xml', '.shp.iso.xml'):
            name = stem + suffix
            if archive.getinfo(name).file_size > 1_000_000:
                raise ValueError('tiger_native_xml_metadata_budget_exceeded')
            documents[f'metadata/source/{name}'] = archive.read(name)
    catalogue = ET.fromstring(documents[f'metadata/source/{stem}.shp.ea.iso.xml'])
    definitions = {name: {'basis': 'unknown', 'source_fields': [name], 'input_ids': [input_id],
                         'unknown_reasons': {'definition': 'Native publisher XML omits this field definition.',
                                             'null_meaning': 'Native publisher XML does not establish null or sentinel meanings.'}}
                   for name in EXPECTED_FIELDS}
    seen = set()
    for attribute in catalogue.findall('.//gfc:FC_FeatureAttribute', namespaces):
        name = attribute.findtext('gfc:memberName/gco:LocalName', namespaces=namespaces)
        if name not in definitions:
            continue
        if name in seen:
            raise ValueError('tiger_duplicate_native_field_definition')
        seen.add(name)
        description = attribute.findtext('gfc:definition/gco:CharacterString', namespaces=namespaces)
        if description and description.strip():
            definitions[name].update({'basis': 'publisher', 'description': description.strip()})
            definitions[name]['unknown_reasons'].pop('definition')
        codes = {}
        for value in attribute.findall('gfc:listedValue/gfc:FC_ListedValue', namespaces):
            label = value.findtext('gfc:label/gco:CharacterString', namespaces=namespaces)
            meaning = value.findtext('gfc:definition/gco:CharacterString', namespaces=namespaces)
            if label and label.strip() and meaning and meaning.strip():
                if label in codes:
                    raise ValueError('tiger_duplicate_native_domain_code')
                codes[label] = meaning.strip()
        if codes:
            definitions[name]['codes'] = codes
    metadata = ET.fromstring(documents[f'metadata/source/{stem}.shp.iso.xml'])
    abstract = metadata.findtext('.//gmd:abstract/gco:CharacterString', namespaces=namespaces)
    return documents, definitions, abstract.strip() if abstract and abstract.strip() else None


def capture_census_primary_roads(
    staging_storage: StagingStorageResource, *, version: str | None = None,
    capture_directory: str | Path | None = None,
    release: census_latest.ShapefileRelease | None = None, captured: tuple | None = None,
    discovery_evidence: dict[str, bytes] | None = None,
    release_state_fingerprint: str | None = None,
) -> dict[str, object]:
    """Capture the newest official annual zip and stage it immutably for catalog."""
    if captured is not None:
        if release is None:
            raise ValueError("current_tiger_release_required")
        body, receipt = captured
    else:
        body, receipt, release = _capture(capture_directory, staging_storage, version or "pending")
    version = version or candidate_version(release, receipt["sha256"])
    source_url = release.source_url
    source_file = release.source_file
    vintage = str(release.vintage)
    archive_sha = receipt["sha256"]
    archive_bytes = receipt["bytes"]
    audit = _inspect_archive(body, release)
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
        "lineage": "Official Census Bureau Geography Division TIGER/Line product, resolved from the producer's current annual listing at capture time. The DHS crosswalk row and equivalence to the archived HIFLD publication remain unverified.",
        "source_release": release.as_metadata(),
    }
    documents, definitions, abstract = _current_native_metadata(body, Path(source_file).stem, source_url)
    manifest['documentation'] = [
        {'path': path, 'input_id': source_url,
         'purpose': 'schema' if path.endswith('.ea.iso.xml') else 'methodology',
         'media_type': 'application/xml', 'bytes': len(value), 'sha256': _sha(value)}
        for path, value in documents.items()]
    manifest['field_definitions'] = definitions
    manifest['coverage'] = {
        'intended_population': 'Census TIGER/Line national Primary Roads classified MTFCC S1100; excludes other road classes.',
        'served_population': 'All native records in the captured national Primary Roads archive; no source rows intentionally filtered.',
        'served_feature_count': audit['feature_count'],
        'observed_mtfcc_values': audit['mtfcc_values'],
        'coverage_accepted': False,
    }
    if abstract:
        manifest['coverage']['publisher_abstract'] = abstract
    manifest['source_relationship'] = {
        'relationship': 'unverified', 'evidence_input_ids': [source_url],
        'unknown_reason': 'Exact DHS crosswalk row and archived HIFLD feature/schema/coverage equivalence have not been established.'}
    refresh = {
        "maintenance_status": "maintained" if discovery_evidence else "unverified",
        "source_release_id": vintage,
        "data_period": vintage, "update_cadence": "unknown",
        "unknown_reasons": {"update_cadence": "Annual edition directories are observed; a producer cadence statement was not captured."},
        "selection_rule": RELEASE_SELECTION_RULE,
        "source_state_fingerprint": release_state_fingerprint or archive_sha,
    }
    if discovery_evidence:
        manifest["inputs"].append({
            "input_id": release.directory_url, "producer": PUBLISHER, "host": "www2.census.gov",
            "role": "producer_publication", "purpose": "methodology",
            "canonical_url": release.directory_url, "retrieval_url": release.directory_url,
            "retrieved_at": release.latest_checked_at,
            "sha256": release.listing_sha256, "bytes": release.listing_bytes,
            "license": "https://www.usa.gov/government-works", "attribution": PUBLISHER,
            "release_date_unknown_reason": "Live producer directory observation; not a dated data release.",
        })
        manifest["inputs"].append({
            "input_id": census_latest.ROOT, "producer": PUBLISHER, "host": "www2.census.gov",
            "role": "producer_publication", "purpose": "methodology",
            "canonical_url": census_latest.ROOT, "retrieval_url": census_latest.ROOT,
            "retrieved_at": release.latest_checked_at,
            "sha256": release.annual_listing_sha256, "bytes": 0,
            "license": "https://www.usa.gov/government-works", "attribution": PUBLISHER,
            "release_date_unknown_reason": "Live producer directory observation; not a dated data release.",
        })
        refresh["latest_checked_at"] = release.latest_checked_at
        refresh["latest_evidence_input_id"] = release.directory_url
        refresh["maintenance_evidence_input_id"] = census_latest.ROOT
    manifest["inputs"][0]["refresh"] = refresh
    validate_provenance(manifest)
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if staging_storage.object_exists(manifest_key):
        encoded_manifest = staging_storage.read_key(manifest_key)
        prior = json.loads(encoded_manifest)
        validate_provenance(prior)
        expected_prior = json.loads(_canonical(manifest))
        if isinstance(prior.get("inputs"), list) and len(prior["inputs"]) == len(manifest["inputs"]):
            expected_prior["inputs"][0]["retrieved_at"] = prior["inputs"][0].get("retrieved_at")
            if "latest_checked_at" in prior["inputs"][0].get("refresh", {}):
                expected_prior["inputs"][0]["refresh"]["latest_checked_at"] = prior["inputs"][0]["refresh"]["latest_checked_at"]
            if discovery_evidence:
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
    release_fields = {
        "release_id": vintage,
        "release_content_sha256": archive_sha,
        "release_selection_rule": RELEASE_SELECTION_RULE,
        "release_state_fingerprint": release_state_fingerprint or archive_sha,
    }
    audit_doc = {"source": {"url": source_url, "vintage": vintage, "archive_sha256": archive_sha, "archive_bytes": archive_bytes, "source_release": release.as_metadata()}, "processing_code": processing_code, "acquisition_receipt": stable_receipt, **audit, **release_fields}
    audit_key = f"{prefix}/metadata/source_audit.json"
    audit_bytes = _canonical(audit_doc) + b"\n"
    write_immutable(staging_storage, audit_key, audit_bytes)
    if staging_storage.read_key(manifest_key) != encoded_manifest or staging_storage.read_key(audit_key) != audit_bytes:
        raise ValueError('current_tiger_metadata_readback_mismatch')
    documentation_keys = []
    for path, value in documents.items():
        key = f'{prefix}/{path}'
        write_immutable(staging_storage, key, value)
        if staging_storage.read_key(key) != value:
            raise ValueError('current_tiger_native_documentation_readback_mismatch')
        documentation_keys.append(key)
    discovery_keys = []
    if discovery_evidence:
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
    written_keys = [archive_key, manifest_key, audit_key, *documentation_keys, *discovery_keys]
    ready = {'version': version, 'source_archive_sha256': archive_sha, 'release_accepted': False,
             'source_population_complete': True,
             'objects': {key.removeprefix(prefix+'/'): _sha(staging_storage.read_key(key)) for key in written_keys}}
    write_immutable(staging_storage, ready_key, _canonical(ready) + b'\n')
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_edition": vintage, "latest_release_discovered": True, "source_archive_bytes": archive_bytes,
        "source_archive_sha256": archive_sha, "source_cache_reused": reused,
        "source_vintage": vintage, "source_release": release.as_metadata(),
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": "LINEARID", "source_coverage_scope": f"U.S. nationwide Census TIGER primary roads, {vintage} vintage",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "capture_mode": "current_discovery",
        "written_keys": [*written_keys, ready_key],
        **release_fields,
    }


def capture_latest_census_primary_roads(staging_storage: StagingStorageResource, *, client=None) -> dict:
    """Discover and capture current producer roads; retain failures in quarantine."""
    from_context = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(180.0, connect=20.0), follow_redirects=False)
    with tempfile.TemporaryDirectory(prefix='census-current-roads-') as tmp:
        root = Path(tmp)
        try:
            release = census_latest.discover_shapefile(client, root, LAYER, PRODUCT)
            prior = load_release_state(staging_storage, DATASET_SLUG, FILE_SLUG)
            try:
                probe = probe_transport(client, release.source_url)
            except ValueError:
                # A publisher without HEAD support simply cannot supply a cheap
                # unchanged proof; capture and compare the archive instead.
                probe = None
            poll_signal = ReleaseSignal(
                release_id=str(release.vintage), release_date=None,
                last_edit=probe.last_modified if probe else None,
                poll_fingerprint=probe.fingerprint() if probe else None,
                content_sha256=None, selection_rule=RELEASE_SELECTION_RULE,
                evidence={'edition': str(release.vintage), 'source_url': release.source_url,
                          'listing_sha256': release.listing_sha256,
                          'last_modified': probe.last_modified if probe else None,
                          'etag': probe.etag if probe else None,
                          'content_length': probe.content_length if probe else None},
            )
            poll_decision = decide_unchanged_poll(prior, poll_signal)
            if poll_decision.action == 'unchanged':
                return unchanged_result(
                    prior, dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG,
                    source_edition=str(release.vintage), latest_release_discovered=True,
                    source_cache_reused=True,
                )
            body, receipt = capture_get(client, release.source_url, root, 'source', max_bytes=MAX_SOURCE_BYTES)
            if receipt.get('content_encoding') != 'identity' or not body:
                raise ValueError('current_tiger_archive_encoding_or_empty')
            # Recheck the selected directory after reading the entire ZIP. A new
            # annual publication or changed listing requires another capture.
            after = census_latest.discover_shapefile(client, root / 'after', LAYER, PRODUCT)
            if any(getattr(release, key) != getattr(after, key)
                   for key in ('vintage', 'source_url', 'listing_sha256')):
                raise ValueError('current_tiger_listing_changed_during_capture')
            signal = replace(poll_signal, content_sha256=_sha(body))
            version = census_latest.release_version(str(release.vintage), receipt['sha256'], processing_code_identity())
            same_candidate = prior is not None and version == prior['version']
            if same_candidate:
                action, note = 'unchanged', None
            else:
                decision = decide_release(prior, signal)
                action = decision.action if decision.action != 'unchanged' else 'new_release'
                note = decision.note if action == 'same_release_correction' else (
                    'Pipeline code identity changed; new immutable candidate for unchanged source content.'
                    if decision.action == 'unchanged' else None)
            evidence = {f'{name}.{suffix}': (root / f'{name}.{suffix}').read_bytes()
                        for name in ('annual-listing', f'{LAYER.lower()}-{release.vintage}-listing', 'source')
                        for suffix in ('body', 'wire', 'receipt.json')}
            evidence['release.json'] = _canonical(release.as_metadata()) + b'\n'
            result = capture_census_primary_roads(
                staging_storage, version=version, release=release, captured=(body, receipt),
                discovery_evidence=evidence,
                release_state_fingerprint=signal.content_fingerprint,
            )
            record_release_state(staging_storage, release_state_record(
                signal, dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, version=version,
                accepted_at=str(receipt['observed_at']), action=action,
                candidate_summary=result, note=note,
            ))
            return {**result, 'release_action': action, 'release_note': note,
                    'release_state_fingerprint': signal.state_fingerprint,
                    'unchanged': same_candidate}
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
