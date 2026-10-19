"""Unaccepted ARC Regions snapshot: complete native data, no inferred edition.

The public NHQRedCrossGIS account's publisher/rights claims are preserved, not
approved. FY26/FY27 metadata conflict and all historical compatibility decisions
remain release gates. No coordinate repair, projection or replacement IDs.
"""
from __future__ import annotations

import collections
import fcntl
import hashlib
import importlib.metadata
import io
import json
import math
import os
import platform
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import fiona
from fiona.io import MemoryFile
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from shapely.geometry import mapping, shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG = 'american-red-cross-regions'
SERVICE = 'https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer'
LAYER = SERVICE + '/2'
ITEM = '8c6753c3c5814d05be65723cd4615d7c'
ITEM_URL = f'https://www.arcgis.com/sharing/rest/content/items/{ITEM}?f=json'
OWNER_URL = 'https://www.arcgis.com/sharing/rest/community/users/NHQRedCrossGIS?f=json'
PAGE_SIZE = 2000
EXPECTED_IDS = tuple(range(1, 48))
# Frozen only after complete original geometry is audited. Empty refuses staging.
GEOMETRY_SHA256 = 'a381c19c1f2f6ea0f0b3aaa8df603556b3f26ea67840ba05ae6d465a3698acd3'
FIELDS = [('OBJECTID', 'esriFieldTypeOID', False, None)] + [
    (name, 'esriFieldTypeString', True, 255) for name in
    ('RCODE', 'Region', 'DCODE', 'Division', 'Region_HQ_Address', 'Region_HQ_City',
     'Region_HQ_State', 'Region_HQ_Zip', 'Region_HQ_Phone', 'Time_Zone', 'FEMA_Region')
] + [('Acres', 'esriFieldTypeDouble', True, None), ('SQ_MI', 'esriFieldTypeDouble', True, None),
     ('Region_RCODE', 'esriFieldTypeString', True, 75),
     ('Shape__Area', 'esriFieldTypeDouble', True, None), ('Shape__Length', 'esriFieldTypeDouble', True, None)]
MAX_BODY = 64 * 1024 * 1024
FLOOR = 300 * 1024 * 1024
READY = 'metadata/arc_regions_ready.json'


class ArcRegionsError(ValueError):
    """A draft input/cache is incomplete, changed or untrusted."""


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ArcRegionsError('duplicate_json_member')
            result[key] = value
        return result
    value = json.loads(body, object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ArcRegionsError('nonfinite_json')))
    if type(value) is not dict or 'error' in value: raise ArcRegionsError('nonobject_or_error_response')
    return value


def query(**options):
    return LAYER + '/query?' + urlencode({'where': '1=1', 'f': 'json', **options})


URLS = {
    'service': SERVICE + '?f=json', 'item': ITEM_URL, 'owner': OWNER_URL,
    'layer': LAYER + '?f=json', 'count_before': query(returnCountOnly='true'),
    'ids_before': query(returnIdsOnly='true'),
    'features': query(outFields='*', returnGeometry='true', orderByFields='OBJECTID ASC', resultOffset=0, resultRecordCount=PAGE_SIZE),
    'tail': query(outFields='OBJECTID', returnGeometry='false', orderByFields='OBJECTID ASC', resultOffset=47, resultRecordCount=PAGE_SIZE),
    'count_after': query(returnCountOnly='true'), 'ids_after': query(returnIdsOnly='true'),
    'layer_after': LAYER + '?f=json', 'item_after': ITEM_URL,
}


def local_root(storage):
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ArcRegionsError('explicit_unprefixed_local_staging_required')
    root = Path(storage.local_dir)
    if '..' in root.parts: raise ArcRegionsError('unsafe_local_root')
    root = root.absolute()
    descriptors = [os.open(root.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    try:
        for part in root.parts[1:]:
            try: os.mkdir(part, mode=0o700, dir_fd=descriptors[-1])
            except FileExistsError: pass
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptors[-1]))
        if shutil.disk_usage(root).free < FLOOR: raise ArcRegionsError('local_storage_floor')
    finally:
        for descriptor in reversed(descriptors): os.close(descriptor)
    return root


def path_for(storage, key):
    root = local_root(storage)
    if (not isinstance(key, str) or PurePosixPath(key).is_absolute() or '..' in PurePosixPath(key).parts
            or PurePosixPath(key).as_posix() != key): raise ArcRegionsError('unsafe_object_key')
    target = root / key
    for part in [target, *target.parents]:
        if part.is_symlink(): raise ArcRegionsError('linked_object_path')
    return target


def immutable(storage, key, body):
    target = path_for(storage, key)
    if target.exists() and (not target.is_file() or target.stat().st_nlink != 1):
        raise ArcRegionsError('linked_or_special_object')
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        if shutil.disk_usage(local_root(storage)).free < FLOOR + len(body): raise ArcRegionsError('object_write_storage_reserve')
        storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise ArcRegionsError('immutable_object_changed')


def diagnostic(storage, pieces, reason):
    prefix = '_quarantine/' + SLUG + '/input-' + uuid4().hex
    for name, body in pieces.items(): immutable(storage, prefix + '/' + name, body)
    immutable(storage, prefix + '/report.json', encode({'approved': False, 'reason': reason,
        'objects': {name: {'bytes': len(body), 'sha256': digest(body)} for name, body in pieces.items()}}))
    return prefix


def read_input(path, limit):
    """Return actual consumed bytes AND an error, so prefixes are not invented/lost."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    chunks = []
    error = None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            return b'', 'nonregular_or_linked_capture'
        if before.st_size > limit: return b'', 'capture_size_limit_before_read'
        remaining = limit + 1
        try:
            while remaining:
                block = os.read(descriptor, min(1024 * 1024, remaining))
                if not block: break
                chunks.append(block); remaining -= len(block)
        except OSError as exc: error = type(exc).__name__
        after = os.fstat(descriptor)
        body = b''.join(chunks)
        if len(body) > limit: error = 'capture_growth_limit'
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_nlink) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink):
            error = 'capture_changed_during_read'
        if len(body) != before.st_size and error is None: error = 'capture_short_read'
        return body, error
    finally: os.close(descriptor)


def validate_receipt(name, body, receipt):
    if (receipt.get('requested_url') != URLS[name] or receipt.get('final_url') != URLS[name]
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or receipt.get('complete') is not True or type(receipt.get('bytes')) is not int
            or receipt['bytes'] != len(body) or receipt.get('sha256') != digest(body)):
        raise ArcRegionsError('invalid_or_incomplete_capture_receipt:' + name)
    stamp = datetime.fromisoformat(receipt.get('observed_at', '').replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() is None: raise ArcRegionsError('undated_capture')


def acquire(storage, capture_directory=None):
    local_root(storage); captures = {}
    for name, url in URLS.items():
        body = receipt_body = b''; pieces = {}; receipt = {}; result = None
        try:
            if capture_directory is not None:
                body, error = read_input(Path(capture_directory) / (name + '.body'), MAX_BODY if name == 'features' else 1024 * 1024)
                pieces['response.body'] = body
                immutable(storage, f'_raw/{SLUG}/{digest(body)}/{name}.body', body)
                if error: raise ArcRegionsError(error)
                receipt_body, error = read_input(Path(capture_directory) / (name + '.receipt.json'), 65536)
                pieces['original-receipt.prefix.body' if error else 'original-receipt.body'] = receipt_body
                immutable(storage, f'_raw/{SLUG}/receipts/{digest(receipt_body)}.body', receipt_body)
                if error: raise ArcRegionsError(error)
                receipt = strict_json(receipt_body)
            else:
                wire = bytearray(); decoded = bytearray()
                def reserve(progress):
                    if shutil.disk_usage(local_root(storage)).free < FLOOR + len(wire) + len(decoded) + progress.reserve_bytes:
                        raise ArcRegionsError('capture_reserve_before_pull_or_decode')
                with httpx.Client(timeout=60, follow_redirects=False, headers={'Accept-Encoding': 'identity'}) as client:
                    with client.stream('GET', url) as response:
                        try:
                            result = capture_http_response(response, max_wire_bytes=MAX_BODY, max_decoded_bytes=MAX_BODY if name == 'features' else 1024 * 1024,
                                reserve=reserve, retain_wire=wire.extend, retain_decoded=decoded.extend)
                        except (KeyboardInterrupt, SystemExit):
                            diagnostic(storage, {'wire.body': bytes(wire), 'response.body': bytes(decoded)}, 'capture_interrupted_no_retry')
                            raise
                        body = bytes(decoded)
                        pieces.update({'wire.body': bytes(wire), 'response.body': body,
                            'pending-wire.body': result.pending_wire, 'pending-decoded.body': result.pending_decoded})
                        if (len(wire) != result.wire_bytes_retained or digest(bytes(wire)) != result.wire_sha256
                                or len(body) != result.decoded_bytes_retained or digest(body) != result.decoded_sha256):
                            raise ArcRegionsError('capture_sink_readback_mismatch')
                        receipt = {'requested_url': url, 'final_url': str(response.url), 'status': response.status_code,
                            'observed_at': datetime.now(timezone.utc).isoformat(), 'bytes': len(body), 'sha256': digest(body),
                            'complete': result.complete, 'wire_bytes': result.wire_bytes_received, 'wire_sha256': result.received_wire_sha256,
                            'content_encoding': result.content_encoding, 'content_length': result.content_length,
                            'capture_error': result.error_code}
                        receipt_body = encode(receipt); pieces['receipt.json'] = receipt_body
                        immutable(storage, f'_raw/{SLUG}/{digest(body)}/{name}.body', body)
                        immutable(storage, f'_raw/{SLUG}/receipts/{digest(receipt_body)}.body', receipt_body)
                        if not result.complete: raise ArcRegionsError(result.error_code or 'incomplete_transfer')
            validate_receipt(name, body, receipt)
            captures[name] = (body, receipt, receipt_body)
        except (OSError, ValueError, TypeError, httpx.HTTPError) as exc:
            where = diagnostic(storage, pieces, str(exc))
            raise ArcRegionsError(f'{exc}; preserved:{where}') from exc
    return captures


def ring_population(geometries):
    result = []
    for geometry in geometries:
        polygons = list(geometry.geoms) if geometry.geom_type == 'MultiPolygon' else [geometry]
        result.extend(tuple(tuple(point) for point in ring.coords) for polygon in polygons for ring in [polygon.exterior, *polygon.interiors])
    return collections.Counter(result)


def inspect_captures(captures):
    if set(captures) != set(URLS): raise ArcRegionsError('capture_roster_drift')
    values = {}
    for name, (body, receipt, raw_receipt) in captures.items():
        validate_receipt(name, body, receipt)
        if strict_json(raw_receipt) != receipt: raise ArcRegionsError('receipt_bytes_mismatch')
        values[name] = strict_json(body)
    service, item, owner, layer = (values[name] for name in ['service', 'item', 'owner', 'layer'])
    if (service.get('serviceItemId') != ITEM or item.get('id') != ITEM or item.get('owner') != 'NHQRedCrossGIS'
            or item.get('url') != SERVICE or item.get('access') != 'public' or owner.get('username') != 'NHQRedCrossGIS'
            or layer.get('id') != 2 or layer.get('name') != 'Master_RC_Geo_Region_2027'):
        raise ArcRegionsError('publisher_account_or_layer_delegation_drift')
    if values['layer_after'] != layer or values['item_after'] != item: raise ArcRegionsError('metadata_changed_during_extraction')
    if (layer.get('geometryType') != 'esriGeometryPolygon' or layer.get('hasZ') is not False or layer.get('hasM') is not False
            or layer.get('spatialReference') != {'wkid': 102100, 'latestWkid': 3857}):
        raise ArcRegionsError('native_geometry_or_CRS_profile_drift')
    fields = [(f['name'], f['type'], f['nullable'], f.get('length')) for f in layer['fields']]
    if fields != FIELDS or layer.get('objectIdField') != 'OBJECTID': raise ArcRegionsError('native_field_declaration_drift')
    for name in ['count_before', 'count_after']:
        if type(values[name].get('count')) is not int or values[name]['count'] != len(EXPECTED_IDS):
            raise ArcRegionsError('coverage_count_drift')
    for name in ['ids_before', 'ids_after']:
        ids = values[name].get('objectIds')
        if (values[name].get('objectIdFieldName') != 'OBJECTID' or type(ids) is not list
                or any(type(v) is not int for v in ids) or tuple(sorted(ids)) != EXPECTED_IDS):
            raise ArcRegionsError('source_identifier_census_drift')
    features = values['features']; tail = values['tail']
    if features.get('exceededTransferLimit') is True or tail.get('exceededTransferLimit') is True or tail.get('features') != []:
        raise ArcRegionsError('incomplete_pagination')
    if (features.get('spatialReference') != layer['spatialReference'] or features.get('geometryType') != 'esriGeometryPolygon'
            or features.get('objectIdFieldName') != 'OBJECTID'):
        raise ArcRegionsError('response_geometry_profile_drift')
    # Query responses omit layer-only editable/nullable declarations. Retain both
    # complete raw schemas and compare the actual query declaration projection.
    expected_query_fields = [{k: v for k, v in field.items() if k not in {'nullable', 'editable'}} for field in layer['fields']]
    if features.get('fields') != expected_query_fields: raise ArcRegionsError('response_schema_drift')
    rows = features.get('features'); names = [f[0] for f in FIELDS]
    if type(rows) is not list or len(rows) != len(EXPECTED_IDS): raise ArcRegionsError('incomplete_rows')
    ids = []; raw_rings = []
    for row in rows:
        if set(row) != {'attributes', 'geometry'} or set(row['attributes']) != set(names): raise ArcRegionsError('feature_field_roster_drift')
        for name, kind, nullable, width in FIELDS:
            value = row['attributes'][name]
            if value is None:
                if not nullable: raise ArcRegionsError('null_required_field:' + name)
            elif kind == 'esriFieldTypeOID':
                if type(value) is not int or not 0 <= value < 2**63: raise ArcRegionsError('OID_type_or_range')
            elif kind == 'esriFieldTypeString':
                if type(value) is not str or len(value.encode('utf-16-le')) // 2 > width: raise ArcRegionsError('text_type_or_width:' + name)
            elif type(value) not in (int, float) or not math.isfinite(value): raise ArcRegionsError('double_type_or_nonfinite:' + name)
        ids.append(row['attributes']['OBJECTID'])
        geometry = row['geometry']
        if type(geometry) is not dict or set(geometry) != {'rings'}: raise ArcRegionsError('unexpected_geometry_member')
        for ring in geometry['rings']:
            if len(ring) < 4 or ring[0] != ring[-1]: raise ArcRegionsError('unclosed_ring')
            if any(type(point) is not list or len(point) != 2 or any(type(v) not in (int, float) or not math.isfinite(v) for v in point) for point in ring):
                raise ArcRegionsError('nonfinite_or_nonXY_coordinate')
            raw_rings.append(tuple(tuple(float(v) for v in point) for point in ring))
    if tuple(ids) != EXPECTED_IDS: raise ArcRegionsError('ordered_identifier_census_drift')
    # GDAL identifies ESRIJSON from the leading metadata; valid JSON member order
    # must not affect decoding. Rewrap only for that parser, retain original raw
    # wire bytes and verify every resulting attribute and ordered ring below.
    decoder_input = {'objectIdFieldName': 'OBJECTID', 'geometryType': features['geometryType'],
        'spatialReference': features['spatialReference'], 'fields': features['fields'], 'features': rows}
    with MemoryFile(json.dumps(decoder_input, allow_nan=False).encode()) as memory, memory.open(driver='ESRIJSON') as stream:
        converted = [(dict(row['properties']), shape(row['geometry'])) for row in stream]
    if len(converted) != len(rows) or any(props != row['attributes'] for (props, geom), row in zip(converted, rows)):
        raise ArcRegionsError('native_decoder_attribute_loss')
    geometries = [geom for props, geom in converted]
    if any(g.is_empty or g.geom_type not in {'Polygon', 'MultiPolygon'} for g in geometries): raise ArcRegionsError('missing_or_nonpolygon_geometry')
    if ring_population(geometries) != collections.Counter(raw_rings): raise ArcRegionsError('native_ring_coordinate_or_order_loss')
    signature = digest(encode([[row['attributes']['OBJECTID'], row['geometry']] for row in rows]))
    if not GEOMETRY_SHA256 or signature != GEOMETRY_SHA256: raise ArcRegionsError('geometry_coverage_drift_requires_review:' + signature)
    arrays = []; arrow_fields = []
    for name, kind, nullable, width in FIELDS:
        typ = pa.int64() if kind == 'esriFieldTypeOID' else pa.float64() if kind == 'esriFieldTypeDouble' else pa.string()
        arrays.append(pa.array([row['attributes'][name] for row in rows], type=typ))
        arrow_fields.append(pa.field(name, typ, nullable=nullable))
    arrays.append(pa.array([g.wkb for g in geometries], type=pa.binary())); arrow_fields.append(pa.field('geometry', pa.binary(), nullable=False))
    bounds = [min(g.bounds[i] for g in geometries) for i in (0, 1)] + [max(g.bounds[i] for g in geometries) for i in (2, 3)]
    metadata = {'geo': encode({'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {
        'encoding': 'WKB', 'geometry_types': sorted(set(g.geom_type for g in geometries)), 'crs': pyproj.CRS(3857).to_json_dict(), 'bbox': bounds}}}),
        'arc_source_fields': encode(layer['fields']), 'arc_encoding_gate': b'OID width unspecified by producer metadata; proposed int64 container encoding and all schema compatibility require approval.'}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(arrow_fields, metadata=metadata))
    audit = {'rows': len(rows), 'source_fields': layer['fields'], 'ids': ids, 'native_crs': 3857, 'bounds': bounds,
        'rings': len(raw_rings), 'vertices': sum(len(ring) for ring in raw_rings), 'geometry_sha256': signature,
        'invalid_geometry_ids': [oid for oid, geom in zip(ids, geometries) if not geom.is_valid],
        'edition_claims': {'service_title': item.get('title'), 'service_description': service.get('description'),
            'item_description': item.get('description'), 'effective_release_date': None, 'edition_accepted': False},
        'authority_accepted': False, 'producer_OID_width': 'unspecified', 'proposed_OID_container_encoding': 'int64; unapproved',
        'source_text_widths_retained_in_raw_and_schema_metadata': True}
    return table, audit


def processing_identity():
    root = Path(__file__).parents[3]
    files = sorted((root / 'src/dagster_hifld').rglob('*.py')) + [root / 'pyproject.toml', root / 'uv.lock']
    return {'files': {str(p.relative_to(root)): sha256_file(p) for p in files},
        'python': platform.python_version(), 'distributions': sorted((d.metadata['Name'], d.version) for d in importlib.metadata.distributions()),
        'gdal': fiona.__gdal_version__, 'geos': shapely.geos_version_string, 'proj': pyproj.proj_version_str,
        'verifier_sha256': verifier_implementation_sha256()}


def native_outputs(table, directory):
    """Deterministic complete native exports, checked against every source value."""
    stream = pa.BufferOutputStream(); pq.write_table(table, stream, compression='zstd'); parquet = stream.getvalue().to_pybytes()
    target = directory / 'regions.gpkg'
    schema = {'properties': {name: 'int64' if kind == 'esriFieldTypeOID' else 'float' if kind == 'esriFieldTypeDouble' else f'str:{width}'
        for name, kind, nullable, width in FIELDS}, 'geometry': 'Unknown'}
    with fiona.open(target, 'w', driver='GPKG', layer=SLUG, crs='EPSG:3857', schema=schema,
                    FID='__gpkg_fid', GEOMETRY_NAME='geometry') as output:
        for row in table.to_pylist():
            output.write({'type': 'Feature', 'properties': {name: row[name] for name, *_ in FIELDS},
                'geometry': mapping(shapely.from_wkb(row['geometry']))})
    # Local export metadata, not a claimed producer release/capture date.
    with sqlite3.connect(target) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")
        crs = db.execute('SELECT srs_id FROM gpkg_contents').fetchall()
        if crs != [(3857,)]: raise ArcRegionsError('GPKG_CRS_loss')
    exported = pyogrio.read_arrow(target)[1]
    if exported.column_names != table.column_names or exported.num_rows != table.num_rows:
        raise ArcRegionsError('GPKG_column_or_population_loss')
    for name in table.column_names:
        if exported.schema.field(name).type != table.schema.field(name).type or not exported.column(name).equals(table.column(name)):
            raise ArcRegionsError('GPKG_type_value_or_geometry_loss:' + name)
    if not pq.read_table(pa.BufferReader(parquet)).equals(table, check_metadata=True): raise ArcRegionsError('Parquet_loss')
    return target.read_bytes(), parquet


def manifest_for(captures, identity, audit, parquet, code_commit):
    metadata = strict_json(captures['item'][0])
    inputs = []
    for name in sorted(captures):
        body, receipt, raw_receipt = captures[name]
        inputs.append({'input_id': name + ':' + digest(body), 'producer': 'American Red Cross (publisher account NHQRedCrossGIS; selection unapproved)',
            'host': urlsplit(URLS[name]).hostname, 'role': 'producer_publication',
            'authority_scope': 'Publisher-account claim captured; independent source selection/authority acceptance is pending.',
            'canonical_url': URLS[name], 'retrieval_url': receipt['requested_url'], 'observed_final_url': receipt['final_url'],
            'retrieved_at': receipt['observed_at'], 'http_status': receipt['status'], 'sha256': digest(body), 'bytes': len(body),
            'license': metadata.get('licenseInfo') or 'Unknown; independent rights decision required',
            'attribution': metadata.get('accessInformation') or 'NHQRedCrossGIS; publisher attribution claim unaccepted',
            'release_date': None, 'release_date_unknown_reason': 'Conflicting FY26 January2026 and FY27 July2026 publisher metadata; effective edition not established.',
            'lineage': 'Candidate NHQRedCrossGIS service snapshot; not an archive-refresh substitution. Crosswalk and authority decision pending.'})
    result = {'schema_version': 1, 'processor': 'HIFLD Next', 'approved': False, 'source_accepted': False,
        'code_commit': code_commit, 'title': 'American Red Cross Regions — unaccepted native snapshot draft',
        'inputs': inputs, 'source_url': LAYER, 'agency': 'American Red Cross (inventory/captured publisher claim)',
        'publisher': 'NHQRedCrossGIS', 'license': metadata.get('licenseInfo') or 'Unresolved', 'date_issued': None,
        'processing_fingerprint': digest(encode(identity)), 'baseline_accepted': False, 'contract_approved': False,
        'release_decisions': ['publisher/source/crosswalk authority', 'effective FY26/FY27 edition and47-region coverage',
            'rights', 'historical baseline/IDs/schema/geometry mapping', 'OID physical width and proposed int64 container encoding',
            'native output compatibility and release'],
        'transformations': [{'operation': 'decode_complete_native_EsriJSON', 'projection_or_geometry_repair': False,
            'preserved': 'All17source fields, originalOBJECTID values, raw JSON and declarations, native3857ordered rings/XY; native Double→float64 and text→UTF8.',
            'unapproved_encoding': 'OID producer bit width unspecified; proposedint64. GPKG __gpkg_fid is a separate local storage ordinal; not a replacement source or published ID.',
            'generated_export_timestamp': '2000-01-01T00:00:00.000Z is deterministic local GPKG metadata, never producer release time.'}],
        'prebuilt_output_policy': {'schema_version': 1, 'formats': {'geoparquet': {
            'reason': 'Preserve full source-declared physical types and native geometry without generic conversion adding bbox/schema/nullability changes; mapping/release acceptance remains required.',
            'objects': [{'path': f'geoparquet/{SLUG}.parquet', 'bytes': len(parquet), 'sha256': digest(parquet)}]}}},
        'unsupported_output_formats': {'shapefile_zip': 'Long original attribute names exceed DBF limits; no destructive field mapping approved.',
            'pmtiles': 'Reprojection/quantization change native geometry and physical types; no visualization mapping approved.'}}
    validate_release_provenance(result)
    return result


def state_key(version): return f'_construction-state/{SLUG}/{version}.json'
def incident_key(version): return f'_construction-state/{SLUG}/incident-{version}/record.json'


def state_present(storage, version):
    key = state_key(version)
    return any(path_for(storage, name).exists() for name in [key, key + '.first-construction.json', key + '.evidence'])


def refuse_incident(storage, version):
    if path_for(storage, incident_key(version)).parent.exists(): raise ArcRegionsError('observed_trust_loss_requires_explicit_exact_restore_and_incident_review')


@contextmanager
def version_lock(storage, version):
    path = path_for(storage, f'_locks/{SLUG}-{version}.lock'); path.parent.mkdir(exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1: raise ArcRegionsError('unsafe_version_lock')
        fcntl.flock(descriptor, fcntl.LOCK_EX); yield
    finally: os.close(descriptor)


def observe_loss(storage, version, fingerprint, reason):
    """Durable refusal BEFORE detaching a cache; partial journal also blocks cold."""
    body = canonical_construction_bundle(version, fingerprint, {'approved': False, 'kind': 'observed_original_trust_loss', 'reason': reason})
    seal_first_construction(storage, incident_key(version), version, fingerprint, body)


def load_original(storage, version, fingerprint, captures, identity, audit, parquet):
    body = load_first_construction(storage, state_key(version), version, fingerprint)
    payload = strict_json(body)['payload']
    if set(payload) != {'manifest', 'captures', 'objects'} or set(payload['captures']) != set(URLS):
        raise ArcRegionsError('outside_payload_roster')
    original = {}
    for name, item in payload['captures'].items():
        if set(item) != {'body_key', 'receipt_key'}: raise ArcRegionsError('outside_capture_roster')
        expected = f'_raw/{SLUG}/{digest(captures[name][0])}/{name}.body'
        if item['body_key'] != expected: raise ArcRegionsError('outside_body_identity')
        body = storage.read_key(expected)
        if body != captures[name][0]: raise ArcRegionsError('outside_body_changed')
        key = item['receipt_key']
        if not isinstance(key, str) or not key.startswith(f'_raw/{SLUG}/receipts/') or not key.endswith('.body'):
            raise ArcRegionsError('outside_receipt_key')
        raw = path_for(storage, key).read_bytes()
        if key != f'_raw/{SLUG}/receipts/{digest(raw)}.body': raise ArcRegionsError('outside_receipt_changed')
        receipt = strict_json(raw); validate_receipt(name, body, receipt)
        original[name] = (body, receipt, raw)
    commit = payload['manifest']['code_commit']
    validate_construction_code_identity(Path(__file__).parents[3], commit, identity['files'])
    expected = manifest_for(original, identity, audit, parquet, commit)
    if encode(payload['manifest']) != encode(expected): raise ArcRegionsError('outside_manifest_changed')
    return payload, original


def payloads_for(captures, identity, audit, manifest, gpkg, parquet):
    result = {f'geopackage/{SLUG}.gpkg': gpkg, f'geoparquet/{SLUG}.parquet': parquet,
        'metadata/source_manifest.json': encode(manifest), 'metadata/arc_regions_source_audit.json': encode(audit),
        'metadata/processing_identity.json': encode(identity)}
    for name, (body, receipt, raw_receipt) in captures.items():
        result[f'raw/{name}.body'] = body; result[f'raw/{name}.receipt.json'] = raw_receipt
    return result


def inventory(root):
    result = {}
    for path in root.rglob('*'):
        info = path.lstat()
        if path.is_symlink() or (not path.is_dir() and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1)):
            raise ArcRegionsError('linked_or_special_candidate_object')
        if path.is_file(): result[str(path.relative_to(root))] = {'bytes': info.st_size, 'sha256': sha256_file(path)}
    return result


def validate_candidate(root, payloads, ready):
    expected = {**payloads, READY: encode(ready)}
    actual = inventory(root)
    allowed = {'metadata/quality_manifest.json', 'metadata/data_dictionary.json', 'metadata/geoparquet_layout.json',
        'metadata/resolved_provenance.json', 'metadata/source/source_manifest.json',
        'metadata/source/quality_manifest.json', 'metadata/source/data_dictionary.json',
        'metadata/source/version_' + digest(payloads['metadata/source_manifest.json']) + '_source_manifest.json'}
    if not set(expected) <= set(actual) or set(actual) - set(expected) - allowed: raise ArcRegionsError('candidate_closed_roster_drift')
    for name, body in expected.items():
        if actual[name] != {'bytes': len(body), 'sha256': digest(body)}: raise ArcRegionsError('candidate_value_or_container_drift:' + name)
    for name in ['metadata/source/source_manifest.json', 'metadata/source/version_' + digest(payloads['metadata/source_manifest.json']) + '_source_manifest.json']:
        if (root/name).exists() and (root/name).read_bytes() != payloads['metadata/source_manifest.json']:
            raise ArcRegionsError('authored_manifest_snapshot_drift')


def stage_arc_regions(storage, capture_directory=None):
    local_root(storage); captures = acquire(storage, capture_directory)
    try:
        table, audit = inspect_captures(captures); identity = processing_identity(); fingerprint = digest(encode(identity))
        body_identity = digest(encode({name: digest(value[0]) for name, value in captures.items()}))
        version = 'v0.0.0-draft-arc-' + body_identity[:16] + '-' + fingerprint[:16]
        prefix = f'{SLUG}/{SLUG}/{version}'; target = path_for(storage, prefix)
        # Reconstruct canonical exports once for strong byte/type/container validation.
        if shutil.disk_usage(local_root(storage)).free < FLOOR + 25_000_000: raise ArcRegionsError('native_export_storage_reserve')
        with tempfile.TemporaryDirectory(prefix='arc-native-') as directory:
            gpkg, parquet = native_outputs(table, Path(directory))
        with version_lock(storage, version):
            refuse_incident(storage, version)
            original = None
            if target.exists() or state_present(storage, version):
                try: original, captures = load_original(storage, version, fingerprint, captures, identity, audit, parquet)
                except (ValueError, OSError, TypeError, KeyError, RuntimeError) as exc:
                    # If journaling fails, do not detach anything. Partial incident
                    # directories remain visible to every later cold invocation.
                    observe_loss(storage, version, fingerprint, str(exc))
                    if target.exists():
                        retained = path_for(storage, '_quarantine/' + SLUG + '/version-' + uuid4().hex)
                        retained.parent.mkdir(parents=True, exist_ok=True); os.rename(target, retained)
                    raise ArcRegionsError('observed_original_state_loss') from exc
            commit = original['manifest']['code_commit'] if original else subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3], text=True).strip()
            validate_construction_code_identity(Path(__file__).parents[3], commit, identity['files'])
            manifest = manifest_for(captures, identity, audit, parquet, commit)
            payloads = payloads_for(captures, identity, audit, manifest, gpkg, parquet)
            objects = {name: {'bytes': len(body), 'sha256': digest(body)} for name, body in payloads.items()}
            ready = {'approved': False, 'version': version, 'processing_fingerprint': fingerprint, 'objects': objects}
            if original and original['objects'] != objects: raise ArcRegionsError('outside_original_object_contract_drift')
            if target.exists():
                try: validate_candidate(target, payloads, ready)
                except (ValueError, OSError, TypeError) as exc:
                    retained = path_for(storage, '_quarantine/' + SLUG + '/version-' + uuid4().hex)
                    retained.parent.mkdir(parents=True, exist_ok=True); os.rename(target, retained)
                    raise ArcRegionsError('whole_candidate_quarantined:' + str(exc)) from exc
                reused = True
            else:
                if shutil.disk_usage(local_root(storage)).free < FLOOR + sum(len(b) for b in payloads.values()):
                    raise ArcRegionsError('complete_private_candidate_storage_reserve')
                target.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.TemporaryDirectory(prefix='.arc-private-', dir=target.parent) as tmp:
                    private = Path(tmp)
                    for name, body in {**payloads, READY: encode(ready)}.items():
                        path = private/name; path.parent.mkdir(parents=True, exist_ok=True)
                        with path.open('xb') as stream:
                            written = stream.write(body)
                            if written != len(body): raise ArcRegionsError('private_short_write')
                            stream.flush(); os.fsync(stream.fileno())
                    validate_candidate(private, payloads, ready)
                    payload = {'manifest': manifest, 'objects': objects, 'captures': {name: {
                        'body_key': f'_raw/{SLUG}/{digest(value[0])}/{name}.body',
                        'receipt_key': f'_raw/{SLUG}/receipts/{digest(value[2])}.body'} for name, value in captures.items()}}
                    envelope = canonical_construction_bundle(version, fingerprint, payload)
                    if original:
                        anchored = load_first_construction(storage, state_key(version), version, fingerprint)
                        if anchored != envelope: raise ArcRegionsError('original_outside_binding_changed')
                    else: seal_first_construction(storage, state_key(version), version, fingerprint, envelope)
                    validate_candidate(private, payloads, ready)
                    os.rename(private, target)
                reused = False
        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'staging_prefix': prefix,
            'feature_count': table.num_rows, 'source_invalid_geometry_count': len(audit['invalid_geometry_ids']),
            'reused': reused, 'source_accepted': False, 'release_approved': False}
    except (ValueError, OSError, TypeError, KeyError, RuntimeError) as exc:
        diagnostic(storage, {'source-pointer.json': encode({name: {'body_sha256': digest(value[0]), 'receipt_sha256': digest(value[2])} for name, value in captures.items()})}, str(exc))
        raise
