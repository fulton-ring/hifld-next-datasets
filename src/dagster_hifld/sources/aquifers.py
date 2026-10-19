"""Local working draft for the original USGS principal-aquifers publication.

This reconstructs the producer's static 2003 release, not a present-day aquifer
assessment. Byte pins and proposed fidelity checks do not approve an HIFLD ID
mapping, published baseline, schema migration, or release.
"""
from __future__ import annotations

import collections
import fcntl
import hashlib
import importlib.metadata
import io
import json
import platform
import re
import sqlite3
import stat
import struct
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import mapping, shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import (canonical_construction_bundle, load_first_construction, seal_first_construction)
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'aquifers'
URL = 'https://water.usgs.gov/GIS/dsdl/aquifers_us.zip'
LANDING = 'https://www.usgs.gov/mission-areas/water-resources/science/principal-aquifers-united-states'
ARCHIVE_NAME = 'aquifers_us.zip'
PIN = '3c597778c51325012aa5ff593751a6e4e662f43906e690fc3eb125c7073b1ebc'
ARCHIVE_BYTES = 7_789_912
MAX_BYTES = 12_000_000
MAX_EXPANDED_BYTES = 30_000_000
STEM = 'us_aquifers'
MEMBERS = frozenset(STEM + suffix for suffix in ('.sbn', '.sbx', '.shp', '.shp.xml', '.shx', '.dbf', '.prj'))
SCHEMA = {'OBJECTID_1': 'int32:9', 'ROCK_NAME': 'str:40', 'ROCK_TYPE': 'int32:9',
          'AQ_NAME': 'str:60', 'AQ_CODE': 'int32:9', 'Shape_Leng': 'float:19.11', 'Shape_Area': 'float:19.11'}
PROPOSAL = {'approved': False, 'id_fields': ['OBJECTID_1'], 'geometry': True,
            'coverage_fields': ['AQ_CODE', 'ROCK_TYPE'], 'crs': 'EPSG:4269',
            'columns': {name: {'type': 'integer' if kind.startswith('int') else 'decimal' if kind.startswith('float') else 'string',
                               'nullable': name == 'ROCK_NAME'} for name, kind in SCHEMA.items()}}


class AquifersQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    if len(body) > 128_000:
        raise AquifersQuarantineError('metadata_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise AquifersQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise AquifersQuarantineError('nonobject_or_error_json')
    return result


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise AquifersQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    require_local(storage)
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise AquifersQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict) -> None:
    if (receipt.get('requested_url') != URL or receipt.get('final_url') != URL
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise AquifersQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    moment = datetime.fromisoformat(receipt.get('fetched_at', ''))
    if moment.tzinfo is None:
        raise AquifersQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, archive_path: Path | None, receipt_path: Path | None) -> tuple[bytes, dict]:
    require_local(storage)
    if archive_path is not None:
        body = b''
        receipt = {'mode': 'preserved_capture', 'capture_path': str(archive_path)}
        try:
            with archive_path.open('rb') as stream:
                body = stream.read(MAX_BYTES + 1)
            if receipt_path is None:
                raise AquifersQuarantineError('preserved_capture_requires_original_receipt')
            with receipt_path.open('rb') as stream:
                receipt = strict_json(stream.read(128_001))
            validate_receipt(body, receipt)
            return body, receipt
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            location = quarantine(storage, body, receipt, str(exc))
            raise AquifersQuarantineError(f'{exc}; retained:{location}') from exc
    for attempt in range(3):
        body = b''
        receipt = {'requested_url': URL, 'fetched_at': datetime.now(timezone.utc).isoformat(), 'attempt': attempt + 1}
        try:
            with httpx.Client(follow_redirects=False, timeout=60) as client:
                with client.stream('GET', URL) as response:
                    receipt.update(final_url=str(response.url), status=response.status_code)
                    for block in response.iter_bytes(chunk_size=64 * 1024):
                        body += block
                        if len(body) > MAX_BYTES:
                            raise AquifersQuarantineError('response_size_limit')
                    receipt.update(bytes=len(body), sha256=sha(body))
                    validate_receipt(body, receipt)
                    content_length = response.headers.get('content-length')
                    if content_length is not None and int(content_length) != len(body):
                        raise AquifersQuarantineError('incomplete_content_length')
            return body, receipt
        except (httpx.HTTPError, OSError, ValueError, TypeError) as exc:
            receipt.update(bytes=len(body), sha256=sha(body), partial=True)
            location = quarantine(storage, body, receipt, str(exc))
            if attempt == 2:
                raise AquifersQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
    raise AssertionError('unreachable')


def processing_identity() -> dict:
    """Installed source, lock/project, Python, Python distributions and native engines."""
    root = Path(__file__).parents[3]
    sources = sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))
    return {'python': platform.python_version(), 'implementation': platform.python_implementation(),
            'platform': platform.platform(),
            'distributions': sorted((d.metadata['Name'], d.version) for d in importlib.metadata.distributions()),
            'gdal': fiona.__gdal_version__, 'geos': shapely.geos_version_string, 'proj': pyproj.proj_version_str,
            'files': {str(p.relative_to(root)): sha256_file(p) for p in sources + [root / 'uv.lock', root / 'pyproject.toml']},
            'verifier_sha256': verifier_implementation_sha256()}


def inspect_archive(body: bytes, directory: Path) -> tuple[list[dict], dict, dict[str, bytes]]:
    if len(body) != ARCHIVE_BYTES or sha(body) != PIN or not body.startswith(b'PK\x03\x04'):
        raise AquifersQuarantineError('changed_truncated_or_unknown_producer_archive')
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != MEMBERS:
            raise AquifersQuarantineError('unexpected_missing_duplicate_archive_member')
        if any(PurePosixPath(i.filename).name != i.filename or i.flag_bits & 1
               or stat.S_ISLNK(i.external_attr >> 16) for i in archive.infolist()):
            raise AquifersQuarantineError('unsafe_encrypted_or_symlink_archive_member')
        if sum(i.file_size for i in archive.infolist()) > MAX_EXPANDED_BYTES:
            raise AquifersQuarantineError('expanded_archive_size_limit')
        if archive.testzip() is not None:
            raise AquifersQuarantineError('zip_crc_failure')
        members = {name: archive.read(name) for name in names}
    for name, data in members.items():
        directory.joinpath(name).write_bytes(data)
    with fiona.open(directory / f'{STEM}.shp') as source:
        schema, crs = dict(source.schema), source.crs
        rows = [{'type': 'Feature', 'properties': dict(r['properties']),
                 'geometry': mapping(shape(r['geometry'])) if r['geometry'] else None} for r in source]
    if schema != {'properties': SCHEMA, 'geometry': 'Polygon'} or not pyproj.CRS(crs).equals(pyproj.CRS(4269)):
        raise AquifersQuarantineError('native_schema_or_crs_drift')
    if len(rows) != 4637 or struct.unpack_from('<I', members[f'{STEM}.dbf'], 4)[0] != len(rows):
        raise AquifersQuarantineError('incomplete_dbf_shp_population')
    ids = [r['properties'].get('OBJECTID_1') for r in rows]
    if any(type(v) is not int for v in ids) or len(set(ids)) != len(rows):
        raise AquifersQuarantineError('missing_invalid_or_duplicate_source_identifier')
    rows.sort(key=lambda r: r['properties']['OBJECTID_1'])
    for row in rows:
        if set(row['properties']) != set(SCHEMA):
            raise AquifersQuarantineError('unexpected_or_missing_source_columns')
        for name, kind in SCHEMA.items():
            value = row['properties'][name]
            expected = int if kind.startswith('int') else float if kind.startswith('float') else str
            if value is None and name == 'ROCK_NAME':
                continue
            if type(value) is not expected:
                raise AquifersQuarantineError(f'source_type_or_null_drift:{name}')
            if isinstance(value, int) and not -(2**31) <= value < 2**31:
                raise AquifersQuarantineError(f'source_integer_width_drift:{name}')
        if row['geometry'] is None:
            raise AquifersQuarantineError('missing_native_geometry')
        g = shape(row['geometry'])
        if g.is_empty or g.geom_type != 'Polygon':
            raise AquifersQuarantineError('invalid_empty_or_changed_native_geometry')
    invalid = {r['properties']['OBJECTID_1']: shapely.is_valid_reason(shape(r['geometry'])) for r in rows if not shape(r['geometry']).is_valid}
    if set(invalid) != {3,11,47,71,283,324,560,1271,1499,1803,2013,2169,2244,2309,2536,2548,2969,4186,4515,4620}:
        raise AquifersQuarantineError('native_invalid_geometry_population_drift')
    audit = {'invalid_geometry_count': len(invalid), 'native_invalid_geometries': invalid, 'source_quality': 'blocked_known_native_self_intersections_no_repair', 'approved': False, 'status': 'producer_source_fidelity_pass', 'feature_count': len(rows),
             'schema': schema, 'crs': 'EPSG:4269', 'bounds': shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
             'nulls': {name: sum(r['properties'][name] is None for r in rows) for name in SCHEMA},
             'identifiers': {'OBJECTID_1': {'unique': len(set(ids)), 'nulls': 0}},
             'coverage': {name: dict(collections.Counter(r['properties'][name] for r in rows)) for name in ('ROCK_TYPE', 'AQ_CODE')},
             'members': {name: {'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(members.items())}}
    return rows, audit, members


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def source_manifest(receipt: dict, fingerprint: str, commit: str) -> dict:
    """Deterministic producer attribution; caller validates original receipt first."""
    manifest = {'schema_version': 1, 'processor': 'HIFLD Next', 'approved': False, 'code_commit': commit,
        'processing_fingerprint': fingerprint, 'title': 'USGS 2003 Principal Aquifers — unapproved working draft',
        'publisher': 'U.S. Geological Survey', 'agency': 'U.S. Geological Survey', 'source_url': LANDING,
        'license': 'http://www.usa.gov/publicdomain/label/1.0/',
        'date_issued': '2003-10-01',
        'unsupported_output_formats': {
            'shapefile_zip': 'Original producer ZIP, DBF declarations, spatial indexes and metadata preserved under raw/. Regenerated ZIP omits producer .sbn/.sbx/.shp.xml sidecars and is not byte/physical-archive equivalent; output mapping is unapproved.',
            'pmtiles': 'Visualization requires reprojection and integer tile-coordinate quantization of native NAD83 double coordinates; full native coordinates/geometry fidelity cannot be preserved. No visualization mapping is approved.'},
        'inputs': [{'input_id': f'usgs-principal-aquifers-2003:{PIN}', 'producer': 'U.S. Geological Survey',
            'host': 'water.usgs.gov', 'role': 'producer_publication', 'canonical_url': URL,
            'retrieval_url': receipt['requested_url'], 'observed_final_url': receipt['final_url'],
            'retrieved_at': receipt['fetched_at'], 'release_date': '2003-10-01', 'sha256': PIN, 'bytes': ARCHIVE_BYTES,
            'license': 'http://www.usa.gov/publicdomain/label/1.0/', 'attribution': 'U.S. Geological Survey, 2003, Principal Aquifers of the 48 Conterminous United States, Hawaii, Puerto Rico, and the U.S. Virgin Islands, https://doi.org/10.5066/P9Y2HOUJ',
            'lineage': 'Original USGS producer release linked by the official principal-aquifers landing; HTTP Last-Modified2015 is packaging, not source currency.',
            'authority_evidence': LANDING}],
        'transformations': [{'operation': 'decode_original_shapefile', 'preserved': 'all source columns, physical DBF types/widths, all native Polygon coordinates, CRS, original archive and7sidecars', 'sort_key': 'OBJECTID_1', 'repair_or_projection': False},
            {'operation': 'typed_native_geopackage_and_arrow_parquet', 'gpkg_internal_fid': 'storage index only; never source or published ID', 'mapping_approval': 'not established'}]}
    validate_release_provenance(manifest)
    return manifest


def candidate_object_names() -> set[str]:
    return {
        f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg',
        'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl',
        'metadata/source_manifest.json', 'metadata/processing_identity.json',
        'metadata/aquifers_source_audit.json', 'metadata/proposed_source_contract.json',
        'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json',
        'metadata/baseline_comparison.json',
        'metadata/geopackage_fidelity/baseline_comparison.json',
        'metadata/geopackage_fidelity/baseline_differences.jsonl',
        'metadata/parquet_fidelity/baseline_comparison.json',
        'metadata/parquet_fidelity/baseline_differences.jsonl',
    } | {f'raw/members/{name}' for name in MEMBERS}


def local_candidate_path(storage: StagingStorageResource, version: str) -> Path:
    require_local(storage)
    root = Path(storage.local_dir).resolve()
    path = root / storage.build_target_location(SLUG, SLUG, version, '')
    if not path.resolve().is_relative_to(root):
        raise AquifersQuarantineError('candidate_path_outside_local_staging')
    return path


def candidate_arrow_schema(audit: dict) -> pa.Schema:
    schema = pa.schema([
        pa.field(name, pa.int32() if kind.startswith('int32') else
                 pa.float64() if kind.startswith('float') else pa.string(),
                 nullable=name == 'ROCK_NAME') for name, kind in SCHEMA.items()
    ] + [pa.field('geometry', pa.binary(), nullable=False)])
    geo = {'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {
        'encoding': 'WKB', 'geometry_types': ['Polygon'],
        'crs': pyproj.CRS(4269).to_json_dict(), 'bbox': audit['bounds']}}}
    return schema.with_metadata({b'geo': encode(geo)})


def geopackage_native_metadata(path: Path) -> tuple:
    """Exact declarations and per-row native headers, independently of GDAL CRS selection."""
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as database:
        declarations = tuple(database.execute(query).fetchall() for query in (
            'SELECT * FROM gpkg_spatial_ref_sys ORDER BY srs_id',
            'SELECT * FROM gpkg_contents ORDER BY table_name',
            'SELECT * FROM gpkg_geometry_columns ORDER BY table_name',
            'SELECT * FROM rtree_aquifers_geom ORDER BY id'))
        headers = []
        for fid, geometry in database.execute('SELECT fid,geom FROM aquifers ORDER BY fid'):
            if not isinstance(geometry, bytes) or len(geometry) < 8 or geometry[:3] != b'GP\x00':
                raise AquifersQuarantineError('cached_geopackage_invalid_native_header')
            envelope = (geometry[3] >> 1) & 7
            if envelope > 4:
                raise AquifersQuarantineError('cached_geopackage_invalid_native_envelope')
            size = 8 + (0, 32, 48, 48, 64)[envelope]
            if len(geometry) <= size:
                raise AquifersQuarantineError('cached_geopackage_truncated_native_header')
            headers.append((fid, geometry[:size]))
    return declarations, headers


def validate_cached_geopackage_metadata(path: Path, records: bytes) -> None:
    # Reproduce GDAL's exact declarations/envelopes from original native rows.
    # Its stored extrema can differ in their final bit from coordinate extrema;
    # no invented tolerance or geometry repair is used to reconcile the cache.
    with tempfile.TemporaryDirectory(prefix='aquifers-native-metadata-') as temporary:
        fresh = Path(temporary) / 'original.gpkg'
        with fiona.open(fresh, 'w', driver='GPKG', layer=SLUG, crs='EPSG:4269',
                        schema={'properties': SCHEMA, 'geometry': 'Polygon'}) as destination:
            destination.writerecords(json.loads(line) for line in records.splitlines())
        with sqlite3.connect(fresh) as database:
            database.execute("UPDATE gpkg_contents SET last_change='2003-10-01T00:00:00.000Z'")
        if geopackage_native_metadata(path) != geopackage_native_metadata(fresh):
            raise AquifersQuarantineError('cached_geopackage_native_crs_bounds_or_header_drift')
        # This immutable writer is byte-deterministic with the pinned effective
        # runtime. Reject unenumerated ancillary metadata, DDL or index changes too.
        if path.read_bytes() != fresh.read_bytes():
            raise AquifersQuarantineError('cached_geopackage_noncanonical_container_bytes')


def validate_cached_payloads(path: Path, records: bytes, audit: dict) -> None:
    """Fresh physical/semantic checks against the decoded pinned producer rows.

    The ready marker is an inventory, never authority for a changed derivative.
    Known native invalid geometries are preserved, not repaired to pass a check.
    """
    geopackage = path / 'geopackage' / f'{SLUG}.gpkg'
    validate_cached_geopackage_metadata(geopackage, records)
    if fiona.listlayers(geopackage) != [SLUG]:
        raise AquifersQuarantineError('cached_geopackage_unexpected_layers')
    expected_schema = {'properties': {
        name: 'int32' if kind.startswith('int32') else
        'float' if kind.startswith('float') else kind for name, kind in SCHEMA.items()
    }, 'geometry': 'Polygon'}
    with fiona.open(geopackage) as source:
        if dict(source.schema) != expected_schema or not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
            raise AquifersQuarantineError('cached_geopackage_physical_schema_or_crs_drift')
        decoded = b''.join(encode({'type': 'Feature', 'properties': dict(row['properties']),
            'geometry': mapping(shape(row['geometry'])) if row['geometry'] else None}) for row in source)
    if decoded != records:
        raise AquifersQuarantineError('cached_geopackage_original_row_or_geometry_mismatch')
    with sqlite3.connect(f'file:{geopackage}?mode=ro', uri=True) as database:
        columns = database.execute('PRAGMA table_info(aquifers)').fetchall()
    expected_columns = [('fid', 'INTEGER', 1, None, 1), ('geom', 'POLYGON', 0, None, 0)] + [
        (name, 'MEDIUMINT' if kind.startswith('int32') else 'REAL' if kind.startswith('float')
         else f'TEXT({kind.split(":")[1]})', 0, None, 0) for name, kind in SCHEMA.items()]
    if [column[1:] for column in columns] != expected_columns:
        raise AquifersQuarantineError('cached_geopackage_sql_physical_schema_drift')
    parquet = path / 'candidate/typed-source.parquet'
    table = pq.read_table(parquet)
    if not table.schema.equals(candidate_arrow_schema(audit), check_metadata=True):
        raise AquifersQuarantineError('cached_parquet_physical_schema_metadata_or_crs_drift')
    decoded = b''.join(encode({'type': 'Feature', 'properties': {name: row[name] for name in SCHEMA},
        'geometry': mapping(shapely.from_wkb(row['geometry'])) if row['geometry'] else None})
        for row in table.to_pylist())
    if decoded != records:
        raise AquifersQuarantineError('cached_parquet_original_row_or_geometry_mismatch')


def construction_record_key(version: str) -> str:
    return f'_construction-state/{SLUG}/{version}.json'


def construction_history_exists(storage: StagingStorageResource, version: str) -> bool:
    key = construction_record_key(version)
    path = Path(storage.local_dir) / storage._ensure_prefixed(key)
    return any(p.exists() or p.is_symlink() for p in
               [path, path.with_name(path.name + '.first-construction.json'),
                path.with_name(path.name + '.evidence')])


def original_construction(storage: StagingStorageResource, version: str, body: bytes,
                          identity: dict, *, receipt: dict | None = None,
                          manifest: dict | None = None, create: bool = False) -> tuple[dict, dict]:
    """Caller holds the common version lock; sealing follows private validation."""
    fingerprint = sha(encode(identity)); key = construction_record_key(version)
    if create:
        validate_receipt(body, receipt)
        receipt_key = f'_raw-captures/{SLUG}/{PIN}/receipts/{sha(encode(receipt))}.json'
        if not storage.object_exists(receipt_key) or storage.read_key(receipt_key) != encode(receipt):
            raise AquifersQuarantineError('original_construction_receipt_not_preserved')
        if not isinstance(manifest, dict) or manifest != source_manifest(receipt, fingerprint, manifest.get('code_commit')):
            raise AquifersQuarantineError('original_construction_manifest_semantic_drift')
        validate_construction_code_identity(Path(__file__).parents[3], manifest['code_commit'], identity['files'])
        bundle = canonical_construction_bundle(version, fingerprint,
                                               {'receipt': receipt, 'manifest': manifest})
        raw = seal_first_construction(storage, key, version, fingerprint, bundle)
    else:
        raw = load_first_construction(storage, key, version, fingerprint)
    value = strict_json(raw); payload = value['payload']
    if set(payload) != {'receipt', 'manifest'}:
        raise AquifersQuarantineError('original_construction_bundle_payload_drift')
    original_receipt, original_manifest = payload['receipt'], payload['manifest']
    validate_receipt(body, original_receipt)
    receipt_key = f'_raw-captures/{SLUG}/{PIN}/receipts/{sha(encode(original_receipt))}.json'
    if not storage.object_exists(receipt_key) or storage.read_key(receipt_key) != encode(original_receipt):
        raise AquifersQuarantineError('original_construction_receipt_not_preserved')
    if original_manifest != source_manifest(original_receipt, fingerprint, original_manifest.get('code_commit')):
        raise AquifersQuarantineError('original_construction_manifest_semantic_drift')
    validate_construction_code_identity(Path(__file__).parents[3], original_manifest['code_commit'], identity['files'])
    return original_receipt, original_manifest


def validate_cached_candidate(storage: StagingStorageResource, version: str, records: bytes,
                              body: bytes, members: dict, audit: dict, identity: dict) -> None:
    path = local_candidate_path(storage, version)
    prefix = f'{SLUG}/{SLUG}/{version}'
    marker = 'metadata/aquifers_candidate_ready.json'
    ready = strict_json(storage.read_key(f'{prefix}/{marker}'))
    names = candidate_object_names()
    if not isinstance(ready.get('objects'), dict):
        raise AquifersQuarantineError('existing_candidate_inventory_must_be_object')
    # These metadata products and snapshots belong to the normal downstream catalog.
    # Additional source files, derivatives, layers or self-added roster entries fail.
    authored_body=storage.read_key(f'{prefix}/metadata/source_manifest.json')
    fingerprint = sha(encode(identity))
    receipt = strict_json(storage.read_key(f'{prefix}/raw/retrieval.json'))
    validate_receipt(body, receipt)
    capture_receipt = f'_raw-captures/{SLUG}/{PIN}/receipts/{sha(encode(receipt))}.json'
    if not storage.object_exists(capture_receipt) or storage.read_key(capture_receipt) != encode(receipt):
        raise AquifersQuarantineError('cached_receipt_missing_original_capture')
    manifest = strict_json(storage.read_key(f'{prefix}/metadata/source_manifest.json'))
    commit = manifest.get('code_commit', '')
    try:
        validate_construction_code_identity(Path(__file__).parents[3], commit, identity['files'])
    except ValueError as exc:
        raise AquifersQuarantineError('cached_source_provenance_semantic_or_code_drift:' + str(exc)) from exc
    if manifest != source_manifest(receipt, fingerprint, commit):
        raise AquifersQuarantineError('cached_source_provenance_semantic_or_code_drift')
    original_receipt, original_manifest = original_construction(storage, version, body, identity)
    if receipt != original_receipt or manifest != original_manifest:
        raise AquifersQuarantineError('cached_source_provenance_original_construction_drift')
    authored_snapshot='metadata/source/version_'+sha(authored_body)+'_source_manifest.json'
    downstream = {authored_snapshot, 'metadata/resolved_provenance.json', 'metadata/data_dictionary.json', 'metadata/quality_manifest.json',
                  'metadata/source/source_manifest.json',
                  'metadata/source/data_dictionary.json', 'metadata/source/quality_manifest.json'}
    present = set()
    for item in path.rglob('*'):
        if item.is_symlink():
            raise AquifersQuarantineError('cached_candidate_contains_symlink')
        if item.is_file():
            present.add(item.relative_to(path).as_posix())
    if (set(ready) != {'status', 'approved', 'source_sha256', 'candidate_records_sha256',
                       'processing_fingerprint', 'version', 'objects'}
            or set(ready.get('objects', {})) != names or not (names | {marker}) <= present
            or present - names - {marker} - downstream or ready.get('version') != version
            or ready.get('status') != 'working_draft_candidate_ready'):
        raise AquifersQuarantineError('existing_candidate_inventory_conflict')
    for name in ('metadata/source/source_manifest.json',authored_snapshot):
        if name in present and storage.read_key(f'{prefix}/{name}')!=authored_body:
            raise AquifersQuarantineError('cached_authored_manifest_snapshot_drift')
    if 'metadata/resolved_provenance.json' in present:
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
        if resolved.get('inputs')!=strict_json(authored_body)['inputs'] or strict_json(storage.read_key(f'{prefix}/metadata/resolved_provenance.json'))!=resolved:
            raise AquifersQuarantineError('cached_resolved_provenance_drift')
    fingerprint = sha(encode(identity))
    if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
            or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
        raise AquifersQuarantineError('existing_candidate_identity_conflict')
    snapshots = {}
    for name, expected in ready['objects'].items():
        key = f'{prefix}/{name}'
        snapshots[key] = storage.object_snapshot(key)
        if snapshots[key] is None or sha(storage.read_key(key)) != expected:
            raise AquifersQuarantineError(f'existing_candidate_object_changed:{name}')
    expected_bytes = {f'raw/{ARCHIVE_NAME}': body, 'metadata/candidate.records.jsonl': records,
                      'metadata/processing_identity.json': encode(identity),
                      'metadata/aquifers_source_audit.json': encode(audit),
                      'metadata/proposed_source_contract.json': encode(PROPOSAL),
                      'metadata/baseline_comparison.json': encode({'schema_version': 1, 'status': 'blocked',
                          'approved': False, 'reason_codes': ['unapproved_baseline_and_source_published_id_schema_crs_mapping'],
                          'acceptance': 'not_established'})}
    expected_bytes.update({f'raw/members/{name}': value for name, value in members.items()})
    for name, expected in expected_bytes.items():
        if storage.read_key(f'{prefix}/{name}') != expected:
            raise AquifersQuarantineError(f'cached_original_semantics_changed:{name}')
    validate_cached_payloads(path, records, audit)
    for format_name in ('geopackage', 'parquet'):
        report_prefix = f'{prefix}/metadata/{format_name}_fidelity'
        report = strict_json(storage.read_key(f'{report_prefix}/baseline_comparison.json'))
        wrapper = strict_json(storage.read_key(f'{prefix}/metadata/{format_name}_source_fidelity.json'))
        population = {'rows': len(records.splitlines()), 'null_keys': 0,
                      'invalid_records': audit['invalid_geometry_count'], 'duplicate_keys': 0}
        expected_reference = {'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': sha(records), 'bytes': len(records)}}
        if (set(wrapper) != {'canonical_full_records_byte_identical', 'shared_verifier_artifact_prefix',
                             'shared_verifier', 'known_invalid_native_geometry_release_blocked'}
                or wrapper.get('shared_verifier_artifact_prefix') != f'metadata/{format_name}_fidelity/'
                or wrapper.get('shared_verifier') != report
                or wrapper.get('canonical_full_records_byte_identical') is not True
                or wrapper.get('known_invalid_native_geometry_release_blocked') is not True
                or report.get('baseline_sha256') != sha(records)
                or report.get('candidate_sha256') != sha(records)
                or report.get('schema_version') != 1
                or report.get('adapter') != 'typed-geojson-feature-jsonl-v1'
                or report.get('baseline') != expected_reference
                or report.get('populations') != {'baseline': population, 'candidate': population}
                or report.get('reason_codes') != ['invalid_schema_key_or_geometry']
                or report.get('acceptance') != 'not_established_by_comparison'
                or report.get('baseline_input_bytes') != len(records)
                or report.get('candidate_input_bytes') != len(records)
                or report.get('contract_sha256') != sha(json.dumps(PROPOSAL, sort_keys=True).encode())
                or report.get('comparator_sha256') != sha256_file(Path(compare_full_rows.__code__.co_filename))
                or report.get('status') != 'fail'
                or report.get('differences') != {'added': 0, 'removed': 0, 'attributes': 0,
                                                'geometry': 0, 'coverage_buckets': 0}):
            raise AquifersQuarantineError('cached_fidelity_claim_semantic_drift')
        sidecar = storage.read_key(f'{report_prefix}/baseline_differences.jsonl')
        expected_errors = [{'kind': 'invalid_record', 'side': side, 'row': index,
                            'errors': ['empty_or_invalid_geometry']}
                           for side in ('baseline', 'candidate')
                           for index, row in enumerate(records.splitlines(), 1)
                           if index in audit['native_invalid_geometries']
                           or str(index) in audit['native_invalid_geometries']]
        if ([strict_json(line) for line in sidecar.splitlines()] != expected_errors
                or report.get('sidecar') != {'path': 'baseline_differences.jsonl',
                                             'sha256': sha(sidecar), 'bytes': len(sidecar)}):
            raise AquifersQuarantineError('cached_fidelity_difference_artifact_drift')
    for key, snapshot in snapshots.items():
        if storage.object_snapshot(key) != snapshot:
            raise AquifersQuarantineError(f'cached_candidate_changed_during_validation:{key}')


def expose_completed_candidate(storage: StagingStorageResource, version: str,
                               payloads: dict[str, bytes], ready: dict) -> None:
    """No discoverable semver directory until every complete object is present."""
    target = local_candidate_path(storage, version)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.aquifers-build-', dir=target.parent) as temporary:
        build = Path(temporary)
        hidden_storage = StagingStorageResource(local_dir=str(build), use_local=True)
        for name, contents in payloads.items():
            immutable(hidden_storage, name, contents)
        immutable(hidden_storage, 'metadata/aquifers_candidate_ready.json', encode(ready))
        if target.exists() or target.is_symlink():
            raise AquifersQuarantineError('candidate_appeared_before_atomic_exposure')
        # Only complete private native bytes can establish the first outside witness.
        validate_cached_payloads(build, payloads['metadata/candidate.records.jsonl'],
                                 strict_json(payloads['metadata/aquifers_source_audit.json']))
        original_receipt, original_manifest = original_construction(
            storage, version, payloads[f'raw/{ARCHIVE_NAME}'],
            strict_json(payloads['metadata/processing_identity.json']),
            receipt=strict_json(payloads['raw/retrieval.json']),
            manifest=strict_json(payloads['metadata/source_manifest.json']), create=True)
        if (encode(original_receipt) != payloads['raw/retrieval.json'] or
                encode(original_manifest) != payloads['metadata/source_manifest.json']):
            raise AquifersQuarantineError('new_candidate_differs_from_original_construction')
        # A cooperating refresh holds this same per-version advisory lock from
        # cache inspection through rename. Existing versions are never replaced.
        if target.exists() or target.is_symlink():
            raise AquifersQuarantineError('candidate_appeared_before_atomic_exposure')
        build.rename(target)


def stage_aquifers_candidate(storage: StagingStorageResource, *, archive_path: Path | None = None,
                              receipt_path: Path | None = None) -> dict:
    require_local(storage)
    body, receipt = acquire(storage, archive_path, receipt_path)
    # Preserve the full capture before interpreting its contents, including rejected bytes.
    capture = f'_raw-captures/{SLUG}/{sha(body)}'
    immutable(storage, f'{capture}/{ARCHIVE_NAME}', body)
    immutable(storage, f'{capture}/receipts/{sha(encode(receipt))}.json', encode(receipt))
    lock = None
    cached_path = None
    try:
        with tempfile.TemporaryDirectory(prefix='aquifers-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            version = f'v0.0.0-draft-usgs2003-{PIN[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/aquifers_candidate_ready.json'
            candidate_path = local_candidate_path(storage, version)
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
            lock = (candidate_path.parent / f'.aquifers-lock-{version}').open('a+b')
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if candidate_path.exists() or candidate_path.is_symlink():
                cached_path = candidate_path
                validate_cached_candidate(storage, version, records, body, members, audit, identity)
                return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version,
                        'feature_count': len(rows), 'status': 'working_draft_candidate_ready',
                        'processing_fingerprint': fingerprint}
            original_manifest = None
            if construction_history_exists(storage, version):
                receipt, original_manifest = original_construction(storage, version, body, identity)
            geopackage = directory / f'{SLUG}.gpkg'
            with fiona.open(geopackage, 'w', driver='GPKG', layer=SLUG, crs='EPSG:4269',
                            schema={'properties': SCHEMA, 'geometry': 'Polygon'}) as destination:
                destination.writerecords(rows)
            with sqlite3.connect(geopackage) as db:
                db.execute("UPDATE gpkg_contents SET last_change='2003-10-01T00:00:00.000Z'")
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Polygon'}:
                    raise AquifersQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
                    raise AquifersQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if decoded.read_bytes() != records:
                raise AquifersQuarantineError('geopackage_complete_fidelity_failure')
            arrow_schema = pa.schema([pa.field(k, pa.int32() if v.startswith('int32') else pa.float64() if v.startswith('float') else pa.string(), nullable=k == 'ROCK_NAME') for k,v in SCHEMA.items()] + [pa.field('geometry', pa.binary(), nullable=False)])
            table = pa.Table.from_pylist([{**r['properties'], 'geometry': shape(r['geometry']).wkb} for r in rows], schema=arrow_schema)
            geo = {'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {'encoding': 'WKB', 'geometry_types': ['Polygon'], 'crs': pyproj.CRS(4269).to_json_dict(), 'bbox': audit['bounds']}}}
            table = table.replace_schema_metadata({b'geo': encode(geo)})
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            parquet_fidelity = compare_derivative(source_records, decoded, directory / 'parquet-comparison')
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise AquifersQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3], text=True).strip()
            manifest = original_manifest or source_manifest(receipt, fingerprint, commit)
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/aquifers_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
                'metadata/geopackage_fidelity/baseline_comparison.json': (directory / 'comparison/baseline_comparison.json').read_bytes(),
                'metadata/geopackage_fidelity/baseline_differences.jsonl': (directory / 'comparison/baseline_differences.jsonl').read_bytes(),
                'metadata/parquet_fidelity/baseline_comparison.json': (directory / 'parquet-comparison/baseline_comparison.json').read_bytes(),
                'metadata/parquet_fidelity/baseline_differences.jsonl': (directory / 'parquet-comparison/baseline_differences.jsonl').read_bytes(),
                'metadata/geopackage_source_fidelity.json': encode({'canonical_full_records_byte_identical': True, 'shared_verifier_artifact_prefix': 'metadata/geopackage_fidelity/', 'shared_verifier': fidelity, 'known_invalid_native_geometry_release_blocked': True}), 'metadata/parquet_source_fidelity.json': encode({'canonical_full_records_byte_identical': True, 'shared_verifier_artifact_prefix': 'metadata/parquet_fidelity/', 'shared_verifier': parquet_fidelity, 'known_invalid_native_geometry_release_blocked': True}),
                'metadata/baseline_comparison.json': encode({'schema_version':1, 'status':'blocked', 'approved':False,
                    'reason_codes':['unapproved_baseline_and_source_published_id_schema_crs_mapping'], 'acceptance':'not_established'})}
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            if set(payloads) != candidate_object_names():
                raise AquifersQuarantineError('incomplete_new_candidate_inventory')
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            expose_completed_candidate(storage, version, payloads, ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile,
            sqlite3.Error, fiona.errors.FionaError, shapely.errors.ShapelyError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        if cached_path is not None and (cached_path.exists() or cached_path.is_symlink()):
            destination = Path(storage.local_dir).resolve() / storage._ensure_prefixed(location) / 'candidate'
            cached_path.rename(destination)
        raise AquifersQuarantineError(f'{exc}; retained:{location}') from exc
    finally:
        if lock is not None:
            lock.close()
