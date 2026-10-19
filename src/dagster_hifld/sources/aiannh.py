"""Faithful TIGER2023 AIANNH draft candidates; publication requires independent approval.

The inventory names TIGER2023. Later vintages and changed bytes at that URL are
quarantined, never silently substituted. These pins describe reviewed producer
bytes, not an accepted HIFLD published schema, baseline or migration decision.
"""
from __future__ import annotations

import collections
import hashlib
import io
import json
import struct
import sqlite3
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import fiona
import httpx
from pyproj import CRS
from shapely.geometry import mapping, shape

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'aiannh-areas-1'
VINTAGE = 2023
URL = 'https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip'
ARCHIVE_NAME = 'tl_2023_us_aiannh.zip'
PIN = '4155f58dfefcc40cfe61d741f760f882ebe6c6008537c0c7bc58075871af2f40'
ARCHIVE_BYTES = 9_086_993
MAX_ARCHIVE_BYTES = 20_000_000
MAX_EXPANDED_BYTES = 30_000_000
STEM = 'tl_2023_us_aiannh'
MEMBERS = {STEM + ext for ext in ('.cpg', '.dbf', '.prj', '.shp', '.shp.ea.iso.xml', '.shp.iso.xml', '.shx')}
SCHEMA = {
    'AIANNHCE': 'str:4', 'AIANNHNS': 'str:8', 'GEOID': 'str:5', 'GEOIDFQ': 'str:14',
    'NAME': 'str:100', 'NAMELSAD': 'str:100', 'LSAD': 'str:2', 'CLASSFP': 'str:2',
    'COMPTYP': 'str:1', 'AIANNHR': 'str:1', 'MTFCC': 'str:5', 'FUNCSTAT': 'str:1',
    'ALAND': 'int:14', 'AWATER': 'int:14', 'INTPTLAT': 'str:11', 'INTPTLON': 'str:12',
}
ID_FIELDS = ('AIANNHCE', 'AIANNHNS', 'GEOID', 'GEOIDFQ')
ATTRIBUTES_PIN = '9546e9917aba8f0da4bf63286458a7046982791ddab07343d9f8ee1c179ccfb2'
GEOMETRY_PIN = 'ce390bcfe6472cdb015137df8d74be3d825b4ae07365d5cbfcd14c03c8dc131f'
PROPOSAL = {
    'approved': False, 'status': 'draft_source_fidelity_only',
    'id_fields': ['GEOID'], 'coverage_fields': ['COMPTYP', 'CLASSFP', 'LSAD', 'MTFCC', 'AIANNHR'],
    'geometry': True, 'crs': 'EPSG:4269',
    'columns': {name: {'type': 'integer' if kind.startswith('int') else 'string', 'nullable': False}
                for name, kind in SCHEMA.items()},
}


class AiannhQuarantineError(ValueError):
    """Original bytes retained outside the catalogable candidate namespace."""


def _current_commit() -> str:
    result = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3],
                            capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else 'installed-package-without-git; exact-implementation-sha256-recorded'


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _immutable_stage(storage: StagingStorageResource, key: str, body: bytes) -> None:
    """No publication helper or overwrite; identical reruns are harmless."""
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise AiannhQuarantineError(f'staging_object_changed:{key}')
        return
    storage.write_key_if_unchanged(key, body, None)


def _population_audit(rows: list[dict], schema: dict, crs: str) -> dict:
    reasons = []
    if schema != {'properties': SCHEMA, 'geometry': 'Polygon'}:
        reasons.append('schema_drift')
    if not CRS.from_user_input(crs).equals(CRS.from_epsg(4269)):
        reasons.append('native_crs_drift')
    props = [r['properties'] for r in rows]
    if len(rows) != 862:
        reasons.append('incomplete_population')
    for r in props:
        if set(r) != set(SCHEMA):
            reasons.append('unexpected_or_missing_column')
            continue
        for name, kind in SCHEMA.items():
            value = r[name]
            expected_type = int if kind.startswith('int') else str
            if type(value) is not expected_type:
                reasons.append(f'type_or_null_loss:{name}')
            elif isinstance(value, str) and len(value) > int(kind.split(':')[1]):
                reasons.append(f'source_width_drift:{name}')
    for name in ('GEOID', 'GEOIDFQ'):
        values = [r.get(name) for r in props]
        if None in values or len(set(values)) != len(rows):
            reasons.append(f'missing_or_duplicate_id:{name}')
    complete = sorted(rows, key=lambda r: str(r['properties'].get('GEOID')))
    attributes = [[r['properties'].get('GEOID'), r['properties']] for r in complete]
    # Reference hashes were independently replayed from the retained original audit.
    attributes_sha = _sha(json.dumps(attributes, sort_keys=True, separators=(',', ':')).encode())
    geom = []
    counts = collections.Counter()
    invalid = empty = 0
    bounds = []
    for r in complete:
        g = shape(r['geometry']) if r['geometry'] else None
        geom.append([r['properties'].get('GEOID'), _sha(g.wkb) if g else None])
        counts[g.geom_type if g else None] += 1
        invalid += g is None or not g.is_valid
        empty += g is None or g.is_empty
        if g is not None and not g.is_empty:
            bounds.append(g.bounds)
    geometry_sha = _sha(json.dumps(geom, sort_keys=True, separators=(',', ':')).encode())
    if attributes_sha != ATTRIBUTES_PIN:
        reasons.append('attribute_id_or_coverage_drift')
    if geometry_sha != GEOMETRY_PIN or invalid or empty:
        reasons.append('geometry_drift_or_invalid')
    observed_bounds = [min(b[i] for b in bounds) if i < 2 else max(b[i] for b in bounds) for i in range(4)] if bounds else None
    if observed_bounds != [-174.235988, 18.91069, -67.035518, 71.340186]:
        reasons.append('coverage_bounds_drift')
    return dict(status='quarantined' if reasons else 'source_fidelity_pass', approved=False,
                reasons=sorted(set(reasons)), feature_count=len(rows), schema=schema, native_crs=crs,
                attributes_sha256=attributes_sha, geometry_sha256=geometry_sha,
                geometry_types=dict(counts), invalid_geometry_count=invalid, empty_geometry_count=empty,
                bounds=observed_bounds,
                identifiers={name: {'unique': len({r.get(name) for r in props}), 'nulls': sum(r.get(name) is None for r in props)} for name in ID_FIELDS},
                coverage={name: dict(collections.Counter(r.get(name) for r in props)) for name in PROPOSAL['coverage_fields']})


def inspect_archive(body: bytes, directory: Path) -> tuple[list[dict], dict, dict[str, bytes]]:
    """CRC and all physical members before the complete native row/geometry scan."""
    archive_path = directory / ARCHIVE_NAME
    archive_path.write_bytes(body)
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != MEMBERS:
            raise AiannhQuarantineError('unexpected_missing_or_duplicate_zip_member')
        if any(i.flag_bits & 1 for i in archive.infolist()) or sum(i.file_size for i in archive.infolist()) > MAX_EXPANDED_BYTES:
            raise AiannhQuarantineError('encrypted_or_oversized_zip')
        if archive.testzip() is not None:
            raise AiannhQuarantineError('zip_crc_failure')
        members = {name: archive.read(name) for name in names}
    with fiona.open(f'/vsizip/{{{archive_path.resolve()}}}/{STEM}.shp') as src:
        schema, crs = dict(src.schema), src.crs.to_string()
        rows = [{'type': 'Feature', 'properties': dict(f['properties']),
                 'geometry': mapping(shape(f['geometry'])) if f['geometry'] else None} for f in src]
    audit = _population_audit(rows, schema, crs)
    if struct.unpack_from('<I', members[STEM + '.dbf'], 4)[0] != len(rows):
        audit['reasons'].append('dbf_shp_count_mismatch')
        audit['status'] = 'quarantined'
    audit['members'] = {name: {'bytes': len(data), 'sha256': _sha(data)} for name, data in sorted(members.items())}
    return sorted(rows, key=lambda r: r['properties']['GEOID']), audit, members


def _quarantine(storage: StagingStorageResource, body: bytes, report: dict, artifacts: dict[str, bytes] | None = None) -> str:
    prefix = f'_quarantine/{SLUG}/{_sha(body)}/{uuid4().hex}'
    _immutable_stage(storage, f'{prefix}/response.body', body)
    _immutable_stage(storage, f'{prefix}/report.json', _json(report))
    for name, data in (artifacts or {}).items():
        _immutable_stage(storage, f'{prefix}/derivatives/{name}', data)
    return prefix


def stage_aiannh_candidate(storage: StagingStorageResource, *, vintage: int = VINTAGE,
                           archive_path: Path | None = None, code_commit: str | None = None) -> dict:
    """Acquire reviewed2023 only, retaining every raw byte and original identifier.

    archive_path explicitly replays a preserved capture; it never claims a live
    request. Default fetch is anonymous, bounded, timeout-limited, full HTTP200.
    A stable content+builder version preserves the first provenance on reruns.
    """
    if isinstance(storage, PublishedStorageResource) or not (storage.use_local or not storage.bucket):
        # This implementation is an explicitly local working draft. Operator
        # activation and a separately reviewed deployment are not implied.
        raise AiannhQuarantineError('aiannh_draft_requires_isolated_local_staging_storage')
    received_at = datetime.now(timezone.utc).isoformat()
    body = b''
    quarantine_artifacts = {}
    retrieval = {'mode': 'preserved_capture' if archive_path else 'anonymous_http',
                 'url': f'https://www2.census.gov/geo/tiger/TIGER{vintage}/AIANNH/tl_{vintage}_us_aiannh.zip'}
    try:
        if archive_path is not None:
            with archive_path.open('rb') as stream:
                body = stream.read(MAX_ARCHIVE_BYTES + 1)
            retrieval['capture_path'] = str(archive_path.resolve())
        else:
            if vintage != VINTAGE:
                raise AiannhQuarantineError('successor_vintage_requires_migration_decision')
            with httpx.Client(follow_redirects=False, timeout=60) as client:
                with client.stream('GET', URL) as response:
                    retrieval['http_status'] = response.status_code
                    retrieval['content_type'] = response.headers.get('content-type')
                    for block in response.iter_bytes():
                        body += block
                        if len(body) > MAX_ARCHIVE_BYTES:
                            raise AiannhQuarantineError('response_size_limit')
                    if response.status_code != 200:
                        raise AiannhQuarantineError(f'http_not_complete:{response.status_code}')
        if vintage != VINTAGE:
            raise AiannhQuarantineError('successor_vintage_requires_migration_decision')
        if len(body) != ARCHIVE_BYTES or _sha(body) != PIN:
            raise AiannhQuarantineError('changed_incomplete_or_unrecognized_source_bytes')
        with tempfile.TemporaryDirectory(prefix='aiannh-verify-') as tmp:
            rows, audit, members = inspect_archive(body, Path(tmp))
        if audit['status'] != 'source_fidelity_pass':
            raise AiannhQuarantineError(','.join(audit['reasons']))
        builder_sha = sha256_file(Path(__file__))
        implementation_sha = verifier_implementation_sha256()
        version = f'v0.0.0-draft-tiger2023-{PIN[:16]}-{implementation_sha[:16]}' 
        prefix = f'{SLUG}/{SLUG}/{version}'
        expected_names = {f'raw/{ARCHIVE_NAME}', f'geopackage/{SLUG}.gpkg',
                          'metadata/geopackage_source_fidelity.json', 'metadata/candidate.records.jsonl',
                          'metadata/aiannh_source_audit.json', 'metadata/proposed_source_contract.json',
                          'metadata/source_manifest.json', 'metadata/baseline_comparison.json'} | {
                              f'raw/members/{name}' for name in MEMBERS}
        records = b''.join(_json(r) for r in rows)
        ready_key = f'{prefix}/metadata/aiannh_candidate_ready.json'
        if storage.object_exists(ready_key):
            # Reuse only the complete exact inventory. Reprocessing never repairs
            # or overwrites a changed staged object, and no acceptance is implied.
            ready = json.loads(storage.read_key(ready_key))
            if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != _sha(records)
                    or ready.get('feature_count') != len(rows) or ready.get('approved') is not False
                    or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready'
                    or set(ready.get('objects', {})) != expected_names):
                raise AiannhQuarantineError('existing_ready_marker_conflict')
            for name, expected in ready['objects'].items():
                key = f'{prefix}/{name}'
                before = storage.object_snapshot(key)
                if before is None or _sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != before:
                    raise AiannhQuarantineError(f'existing_candidate_object_changed:{name}')
            validate_release_provenance(json.loads(storage.read_key(f'{prefix}/metadata/source_manifest.json')))
            return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                    'candidate_sha256': _sha(records), 'source_sha256': PIN, 'status': ready['status'],
                    'written_keys': sorted(f'{prefix}/{name}' for name in ready['objects'])}
        manifest_key = f'{prefix}/metadata/source_manifest.json'
        if storage.object_exists(manifest_key):
            manifest = json.loads(storage.read_key(manifest_key))
            if (manifest.get('builder_sha256') != builder_sha or manifest.get('implementation_sha256') != implementation_sha
                    or manifest['inputs'][0]['sha256'] != PIN):
                raise AiannhQuarantineError('existing_provenance_conflict')
        else:
            manifest = {'schema_version': 1, 'processor': 'HIFLD Next',
                        'processor_description': 'HIFLD Next draft AIANNH source adapter',
                        'code_commit': code_commit or _current_commit(), 'builder_sha256': builder_sha,
                        'implementation_sha256': implementation_sha,
                        'title': 'Census TIGER2023 AIANNH components — unapproved draft',
                        'approved': False, 'native_crs': 'EPSG:4269',
                        'unsupported_output_formats': {
                            'shapefile_zip': 'Do not regenerate a physical DBF/geometry derivative: original producer ZIP and every sidecar are retained losslessly under raw/. Source mixed Polygon/MultiPolygon representation must not be coerced by a regenerated driver.',
                            'pmtiles': 'Visualization simplification/precision and source-ID fidelity are not approved; raw native polygons are authoritative for this draft.'},
                        'refresh_policy': 'Fixed inventory-linked2023; retry monthly for availability and byte drift. New annual vintages require explicit migration decision.',
                        'transformations': [{'operation': 'decode_native_shapefile_to_sorted_typed_feature_jsonl',
                                             'sort_key': 'GEOID', 'geometry': 'No repair, projection, simplification or combined-M dissolve',
                                             'raw_preservation': 'Original ZIP plus every member unchanged'},
                                            {'operation': 'lossless_native_mixed_geometry_geopackage',
                                             'internal_fid': 'GeoPackage storage index only; no generated source ID or published mapping',
                                             'metadata': 'Fixed gpkg_contents.last_change to source release date for deterministic bytes'}],
                        'inputs': [{'input_id': f'tiger2023-aiannh-sha256:{PIN}', 'producer': 'U.S. Census Bureau, Geography Division',
                                    'host': 'www2.census.gov', 'role': 'producer_publication', 'canonical_url': URL,
                                    'retrieval_url': URL, 'release_date': '2023-11-22', 'retrieved_at': received_at,
                                    'retrieval': retrieval, 'sha256': PIN, 'bytes': len(body),
                                    'license': 'U.S. Census Bureau public data', 'attribution': 'U.S. Census Bureau, Geography Division',
                                    'lineage': 'TIGER2023/Boundary and Annexation Survey; exact inventory URL; DHS crosswalk remains unverified'}]}
        validate_release_provenance(manifest)
        with tempfile.TemporaryDirectory(prefix='aiannh-gpkg-') as tmp:
            directory = Path(tmp)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            geopackage = directory / f'{SLUG}.gpkg'
            with fiona.open(geopackage, 'w', driver='GPKG', layer=SLUG,
                            schema={'properties': SCHEMA, 'geometry': 'Unknown'}, crs='EPSG:4269') as dst:
                dst.writerecords(rows)
            # GeoPackage can store Polygon and MultiPolygon without coercing
            # every original Polygon into a MultiPolygon as the generic writer does.
            with sqlite3.connect(geopackage) as db:
                db.execute("UPDATE gpkg_contents SET last_change='2023-11-22T00:00:00.000Z'")
            derivative_records = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as src:
                if not CRS(src.crs).equals(CRS.from_epsg(4269)):
                    raise AiannhQuarantineError('geopackage_native_crs_changed')
                derivative_records.write_bytes(b''.join(_json({'type': 'Feature', 'properties': dict(f['properties']),
                    'geometry': mapping(shape(f['geometry'])) if f['geometry'] else None}) for f in src))
            fidelity = compare_derivative(source_records, derivative_records, directory / 'fidelity')
            if fidelity['status'] != 'pass':
                quarantine_artifacts = {'source_native.gpkg': geopackage.read_bytes(),
                                        'source_fidelity.json': _json(fidelity),
                                        'full_differences.jsonl': (directory / 'fidelity/baseline_differences.jsonl').read_bytes()}
                raise AiannhQuarantineError('geopackage_derivative_fidelity_failed')
            gpkg_bytes = geopackage.read_bytes()
            gpkg_report = (directory / 'fidelity/baseline_comparison.json').read_bytes()
        payloads = {f'raw/{ARCHIVE_NAME}': body,
                    f'geopackage/{SLUG}.gpkg': gpkg_bytes,
                    'metadata/geopackage_source_fidelity.json': gpkg_report,
                    'metadata/candidate.records.jsonl': records,
                    'metadata/aiannh_source_audit.json': _json(audit),
                    'metadata/proposed_source_contract.json': _json(PROPOSAL),
                    'metadata/source_manifest.json': _json(manifest),
                    'metadata/baseline_comparison.json': _json({'schema_version': 1, 'status': 'blocked', 'approved': False,
                          'reason_codes': ['missing_accepted_published_baseline_grain_mapping_contract'],
                          'candidate_sha256': _sha(records), 'acceptance': 'not_established',
                          'research_comparison': 'docs/dataset-sources/aiannh-areas-1/evidence/retry/comparison.json'})}
        payloads.update({f'raw/members/{name}': data for name, data in members.items()})
        if set(payloads) != expected_names:
            raise AiannhQuarantineError('incomplete_candidate_inventory')
        for name, value in payloads.items():
            _immutable_stage(storage, f'{prefix}/{name}', value)
        ready = {'status': 'working_draft_candidate_ready', 'approved': False, 'source_sha256': PIN,
                 'candidate_records_sha256': _sha(records), 'feature_count': len(rows), 'version': version,
                 'objects': {name: _sha(value) for name, value in sorted(payloads.items())}}
        _immutable_stage(storage, f'{prefix}/metadata/aiannh_candidate_ready.json', _json(ready))
        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                'candidate_sha256': _sha(records), 'source_sha256': PIN, 'status': ready['status'],
                'written_keys': sorted(f'{prefix}/{name}' for name in payloads)}
    except (ValueError, TypeError, KeyError, OverflowError, struct.error, OSError, RuntimeError, sqlite3.Error, zipfile.BadZipFile, fiona.errors.FionaError, httpx.HTTPError) as exc:
        report = {'status': 'quarantined', 'approved': False, 'reason': str(exc), 'vintage': vintage,
                  'received_at': received_at, 'retrieval': retrieval, 'sha256': _sha(body), 'bytes': len(body),
                  'bounded_capture_may_be_partial': len(body) > MAX_ARCHIVE_BYTES}
        key = _quarantine(storage, body, report, quarantine_artifacts)
        raise AiannhQuarantineError(f'{exc}; retained:{key}') from exc


def compare_derivative(source_records: Path, derivative_records: Path, output: Path) -> dict:
    """Shared full-row verifier for source fidelity; never a release baseline."""
    pin = sha256_file(source_records)
    reference = {'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
                 'source_archive_sha256': PIN,
                 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1', 'sha256': pin,
                                      'bytes': source_records.stat().st_size}}
    return compare_full_rows(source_records, derivative_records, PROPOSAL, output,
                             baseline_sha256=pin, candidate_sha256=sha256_file(derivative_records),
                             baseline_reference=reference)
