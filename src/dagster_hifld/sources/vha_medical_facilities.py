"""Current VA health/Vet Center locator draft; not the historical NGA-enriched product.

Uses the public application's normal anonymous HEAD /v0/csrf_token session,
then complete, non-spatially-filtered POST searches. Tokens/cookies stay only in
memory. Two full stable sweeps witness completeness; no third-party input,
text normalization, generated IDs, geocoding or coordinate transformation.
Mixed dict/list/null source fields use explicitly declared logical JSON columns.
"""
from __future__ import annotations

import io
import json
import math
import shutil
import struct
import tempfile
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from time import sleep
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import (
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha

DATASET_SLUG = FILE_SLUG = 'veterans-health-administration-medical-facilities'
SEARCH_URL = 'https://api.va.gov/facilities_api/v2/va'
CSRF_URL = 'https://api.va.gov/v0/csrf_token'
LANDING_URL = 'https://www.va.gov/find-locations/'
PAGE_SIZE = 1000
MAX_BODY_BYTES = 16_000_000
EXPECTED_COUNTS = {'health': 1308, 'vet_center': 321}
EXPECTED_IDS_SHA256 = '660cfb8d35daad72616c224cdd7ae2e68982d42207d89ffedf0c24a00f943e13'
EXPECTED_GEOMETRY_SHA256 = 'cc2706343ad08870702960d5106b388b9aeebc40673b7294b6338b4aee004c19'
# Complete repeated 2026-11-15 capture: IDs, geometry, count and scope changes quarantine.
ATTRIBUTE_NAMES = tuple(sorted(('classification', 'distance', 'facilityType', 'id', 'lat', 'long',
    'mobile', 'name', 'operationalHoursSpecialInstructions', 'uniqueId', 'visn', 'website',
    'tmpCovidOnlineScheduling', 'access', 'address', 'feedback', 'hours', 'operatingStatus',
    'phone', 'services')))
JSON_FIELDS = {'access', 'address', 'feedback', 'hours', 'operatingStatus', 'phone',
               'services', 'operationalHoursSpecialInstructions'}
STATUSES = {'NORMAL', 'NOTICE', 'LIMITED', 'TEMPORARY_CLOSURE', 'VIRTUAL_CARE',
            'TEMPORARY_LOCATION', 'COMING_SOON', 'CLOSED'}
EXCLUDED_STATUSES = {'COMING_SOON', 'CLOSED'}
APPROVALS = {name: False for name in ('source_authority_accepted', 'source_release_accepted',
    'freshness_accepted', 'schema_contract_accepted', 'id_contract_accepted',
    'coverage_accepted', 'baseline_accepted', 'rights_accepted', 'mapping_approved', 'release_accepted')}


def search_params(kind: str, page: int) -> dict:
    return {'type': kind, 'page': page, 'per_page': PAGE_SIZE, 'mobile': False}


def bootstrap_session(client: httpx.Client) -> None:
    """Perform the VA application's ordinary anonymous session/CSRF bootstrap."""
    response = client.head(CSRF_URL)
    token = response.headers.get('X-CSRF-Token')
    if response.status_code != 204 or str(response.url) != CSRF_URL or not token or not client.cookies:
        raise ValueError('anonymous_session_bootstrap_failed')
    client.headers['X-CSRF-Token'] = token


def capture_page(client: httpx.Client, root: Path, name: str, kind: str, page: int) -> tuple:
    """Retain bounded original wire/decoded evidence; never retain session headers."""
    params = search_params(kind, page)
    observed = datetime.now(timezone.utc).isoformat()
    wire, decoded = bytearray(), bytearray()
    with client.stream('POST', SEARCH_URL, json=params, headers={
            'Accept-Encoding': 'identity', 'X-Key-Inflection': 'camel',
            'Source-App-Name': 'facilities'}) as response:
        captured = capture_http_response(response, max_wire_bytes=MAX_BODY_BYTES,
            max_decoded_bytes=MAX_BODY_BYTES, reserve=lambda _progress: None,
            retain_wire=wire.extend, retain_decoded=decoded.extend)
        final_url = str(response.url)
    body, original = bytes(decoded), bytes(wire)
    receipt = {'method': 'POST', 'url': SEARCH_URL, 'final_url': final_url,
        'request_json': params, 'observed_at': observed, 'status': captured.status_code,
        'complete_body': captured.complete, 'bytes': len(body), 'sha256': sha(body),
        'wire_bytes': len(original), 'wire_sha256': sha(original),
        'content_encoding': captured.content_encoding, 'error_code': captured.error_code}
    for suffix, value in (('body', body), ('wire', original), ('receipt.json', canonical(receipt) + b'\n')):
        (root / f'{name}.{suffix}').write_bytes(value)
    if not captured.complete or captured.status_code != 200 or final_url != SEARCH_URL:
        raise ValueError('incomplete_source_capture:' + name)
    return body, receipt, original


def page_plan():
    return [(f'{sweep}_{kind}_{page:04d}', kind, page)
            for sweep in ('before', 'after') for kind, count in EXPECTED_COUNTS.items()
            for page in range(1, math.ceil(count / PAGE_SIZE) + 1)]


def acquire(root: Path) -> dict:
    captured = {}
    with httpx.Client(timeout=90, follow_redirects=False, headers={
            'User-Agent': 'hifld-next-datasets/vha-medical-facilities-draft',
            'Origin': 'https://www.va.gov', 'Referer': LANDING_URL}) as client:
        bootstrap_session(client)
        for name, kind, page in page_plan():
            captured[name] = capture_page(client, root, name, kind, page)
            sleep(2.1)  # Below the producer's documented 30 requests/minute locator throttle.
    (root / 'capture-manifest.json').write_bytes(canonical({
        'objects': capture_roster_digests(root)}) + b'\n')
    return captured


def load_capture(root: Path) -> dict:
    manifest = json.loads((root / 'capture-manifest.json').read_bytes())
    objects = manifest['objects']
    expected_files = {f'{name}.{suffix}' for name, _, _ in page_plan()
                      for suffix in ('body', 'wire', 'receipt.json')}
    if not isinstance(objects, dict) or set(objects) != expected_files:
        raise ValueError('offline_capture_manifest_roster_invalid')
    validate_capture_roster(root, {**objects, 'capture-manifest.json': {
        'bytes': (root / 'capture-manifest.json').stat().st_size,
        'sha256': sha((root / 'capture-manifest.json').read_bytes())}})
    return {name: ((root / f'{name}.body').read_bytes(),
                   json.loads((root / f'{name}.receipt.json').read_bytes()),
                   (root / f'{name}.wire').read_bytes()) for name, _, _ in page_plan()}


def strict_document(body: bytes) -> dict:
    def unique_pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate_json_key')
            result[key] = value
        return result
    document = json.loads(body, object_pairs_hook=unique_pairs)
    if not isinstance(document, dict):
        raise ValueError('source_json_object_required')
    return document


def validate_pages(captures: dict) -> tuple[list[dict], dict]:
    if set(captures) != {name for name, _, _ in page_plan()}:
        raise ValueError('source_page_roster_incomplete')
    populations = {'before': [], 'after': []}
    for name, kind, page in page_plan():
        body, receipt, wire = captures[name]
        allowed_receipt_keys = {'method', 'url', 'final_url', 'request_json', 'observed_at', 'status',
            'complete_body', 'bytes', 'sha256', 'wire_bytes', 'wire_sha256', 'content_encoding', 'error_code'}
        if set(receipt) - allowed_receipt_keys:
            raise ValueError('source_receipt_unexpected_fields')
        if (receipt.get('status') != 200 or receipt.get('complete_body') is not True
                or receipt.get('method') != 'POST' or receipt.get('url') != SEARCH_URL
                or receipt.get('final_url') != SEARCH_URL or receipt.get('request_json') != search_params(kind, page)
                or receipt.get('bytes') != len(body) or receipt.get('sha256') != sha(body)
                or receipt.get('wire_bytes') != len(wire) or receipt.get('wire_sha256') != sha(wire)):
            raise ValueError('source_page_receipt_invalid:' + name)
        document = strict_document(body)
        count = EXPECTED_COUNTS[kind]
        pages = math.ceil(count / PAGE_SIZE)
        expected = {'currentPage': page, 'prevPage': page - 1 if page > 1 else None,
            'nextPage': page + 1 if page < pages else None, 'totalPages': pages, 'totalEntries': count}
        pagination = document.get('meta', {}).get('pagination')
        if (not isinstance(pagination, dict) or pagination != expected
                or any(type(pagination[key]) is not int for key, value in expected.items() if value is not None)):
            raise ValueError('source_pagination_or_count_drift:' + name)
        rows = document.get('data')
        if not isinstance(rows, list) or len(rows) != min(PAGE_SIZE, count - (page - 1) * PAGE_SIZE):
            raise ValueError('source_page_incomplete:' + name)
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'id', 'type', 'attributes'} or row['type'] != 'facility':
                raise ValueError('source_record_schema_drift')
            attrs = row['attributes']
            if not isinstance(attrs, dict) or set(attrs) != set(ATTRIBUTE_NAMES):
                raise ValueError('source_attribute_schema_drift')
            prefix = 'vha_' if kind == 'health' else 'vc_'
            facility_type = 'va_health_facility' if kind == 'health' else 'vet_center'
            if (not isinstance(row['id'], str) or not row['id'].startswith(prefix)
                    or attrs['id'] != row['id'] or not isinstance(attrs['uniqueId'], str)
                    or not attrs['uniqueId'] or row['id'] != prefix + attrs['uniqueId']):
                raise ValueError('source_native_identifier_invalid')
            if attrs['facilityType'] != facility_type or attrs['mobile'] is not False:
                raise ValueError('source_population_filter_drift')
            for field in ('name', 'visn'):
                if not isinstance(attrs[field], str):
                    raise ValueError('source_scalar_type_drift:' + field)
            for field in ('classification', 'website'):
                if attrs[field] is not None and not isinstance(attrs[field], str):
                    raise ValueError('source_scalar_type_drift:' + field)
            for field in ('distance', 'tmpCovidOnlineScheduling'):
                if attrs[field] is not None:
                    raise ValueError('source_null_field_drift:' + field)
            for field, limit in (('lat', 90), ('long', 180)):
                value = attrs[field]
                if value is not None and (type(value) is not float or not math.isfinite(value) or abs(value) > limit):
                    raise ValueError('source_coordinate_type_or_range_drift')
            for field in JSON_FIELDS:
                allowed = (dict, list) if field in {'address', 'feedback', 'services'} else (dict,)
                if field == 'operationalHoursSpecialInstructions':
                    allowed = (list, type(None))
                if not isinstance(attrs[field], allowed):
                    raise ValueError('source_logical_json_type_drift:' + field)
            status = attrs['operatingStatus']
            if not isinstance(status, dict) or status.get('code') not in STATUSES:
                raise ValueError('source_operating_status_drift')
            services = attrs['services']
            if isinstance(services, dict) and 'lastUpdated' in services:
                value = services['lastUpdated']
                if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                    raise ValueError('source_service_date_invalid')
            populations[name.split('_', 1)[0]].append(row)
    for sweep, rows in populations.items():
        ids = [row['id'] for row in rows]
        if len(ids) != sum(EXPECTED_COUNTS.values()) or len(set(ids)) != len(ids):
            raise ValueError('source_id_population_incomplete_or_duplicate:' + sweep)
        populations[sweep] = sorted(rows, key=lambda row: row['id'])
    if canonical(populations['before']) != canonical(populations['after']):
        raise ValueError('source_records_changed_during_capture')
    rows = populations['before']
    ids_sha = sha(canonical([row['id'] for row in rows]))
    geometry_sha = sha(canonical([[row['id'], row['attributes']['long'], row['attributes']['lat']] for row in rows]))
    if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
        raise ValueError('source_id_roster_drift')
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError('source_geometry_or_coverage_drift')
    excluded = {row['id']: row['attributes']['operatingStatus']['code'] for row in rows
                if row['attributes']['operatingStatus']['code'] in EXCLUDED_STATUSES}
    selected = [row for row in rows if row['id'] not in excluded]
    return selected, {'source_count': len(rows), 'feature_count': len(selected),
        'source_counts_by_type': EXPECTED_COUNTS, 'excluded_status_ids': excluded,
        'operating_status_counts': dict(Counter(row['attributes']['operatingStatus']['code'] for row in rows)),
        'null_geometry_count': sum(row['attributes']['lat'] is None or row['attributes']['long'] is None for row in selected),
        'source_id_sha256': ids_sha, 'source_geometry_sha256': geometry_sha,
        'source_record_sha256': sha(canonical(rows)), 'source_schema_sha256': sha(canonical(ATTRIBUTE_NAMES)),
        'source_release_date': None,
        'date_semantics': 'services.lastUpdated describes services, not coordinate or facility-record freshness; source release date/cadence unknown.',
        'source_population_complete': True, **APPROVALS}


def geoparquet(rows: list[dict]) -> bytes:
    columns = {'id': [row['id'] for row in rows], 'type': [row['type'] for row in rows]}
    fields = [pa.field('id', pa.string(), nullable=False), pa.field('type', pa.string(), nullable=False)]
    for name in ATTRIBUTE_NAMES:
        if name == 'id':
            continue  # Both native id occurrences are validated identical; full record retains both.
        values = [row['attributes'][name] for row in rows]
        metadata = None
        if name in JSON_FIELDS:
            values = [canonical(value).decode('utf-8') for value in values]
            kind = pa.string()
            metadata = {b'source_logical_encoding': b'JSON: decode to preserve dict/list/null and nested scalar types'}
        elif name in ('lat', 'long', 'distance'):
            kind = pa.float64()
        elif name == 'mobile':
            kind = pa.bool_()
        elif name == 'tmpCovidOnlineScheduling':
            kind = pa.null()
        else:
            kind = pa.string()
        fields.append(pa.field(name, kind, metadata=metadata))
        columns[name] = values
    fields.extend([pa.field('source_record_json', pa.string()), pa.field('geometry', pa.binary())])
    columns['source_record_json'] = [canonical(row).decode('utf-8') for row in rows]
    columns['geometry'] = [None if row['attributes']['lat'] is None or row['attributes']['long'] is None
        else struct.pack('<BIdd', 1, 1, row['attributes']['long'], row['attributes']['lat']) for row in rows]
    present = [row['attributes'] for row in rows if row['attributes']['lat'] is not None and row['attributes']['long'] is not None]
    column = {'encoding': 'WKB', 'geometry_types': ['Point'],
              'crs': CRS.from_user_input('OGC:CRS84').to_json_dict()}
    if present:
        column['bbox'] = [min(a['long'] for a in present), min(a['lat'] for a in present),
                          max(a['long'] for a in present), max(a['lat'] for a in present)]
    schema = pa.schema(fields, metadata={b'geo': canonical({'version': '1.1.0',
        'primary_column': 'geometry', 'columns': {'geometry': column}})})
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema), sink, compression='zstd')
    body = sink.getvalue().to_pybytes()
    for original, restored in zip(rows, pq.read_table(io.BytesIO(body)).to_pylist()):
        if canonical(json.loads(restored['source_record_json'])) != canonical(original):
            raise ValueError('native_record_roundtrip_failed')
        for name, value in original['attributes'].items():
            decoded = json.loads(restored[name]) if name in JSON_FIELDS else restored[name]
            if canonical(decoded) != canonical(value):
                raise ValueError('native_value_roundtrip_failed:' + name)
    return body


def capture_vha_medical_facilities(storage: StagingStorageResource, *,
        capture_directory: str | None = None, offline_capture_directory: str | None = None) -> dict:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError('explicit_local_staging_resource_required')
    base = Path(storage.local_dir).absolute()
    if '..' in base.parts or any(path.is_symlink() for path in (base, *base.parents)):
        raise ValueError('unsafe_staging_root')
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 1_000_000_000:
        raise ValueError('disk_floor_before_acquisition')
    parent = Path(capture_directory or (Path(tempfile.gettempdir()) / 'vha-medical-facilities-captures')).absolute()
    if '..' in parent.parts or any(path.is_symlink() for path in (parent, *parent.parents)):
        raise ValueError('unsafe_capture_root')
    root = parent / uuid4().hex
    root.mkdir(parents=True)
    captures = {}
    try:
        captures = load_capture(Path(offline_capture_directory)) if offline_capture_directory else acquire(root)
        for name, (body, receipt, wire) in captures.items():
            for suffix, value in (('body', body), ('wire', wire), ('receipt.json', canonical(receipt) + b'\n')):
                (root / f'{name}.{suffix}').write_bytes(value)
        (root / 'capture-manifest.json').write_bytes(canonical({
            'objects': capture_roster_digests(root, exclude={'capture-manifest.json'})}) + b'\n')
        rows, audit = validate_pages(captures)
    except Exception as exc:
        if not captures:
            for receipt in root.glob('*.receipt.json'):
                name = receipt.name.removesuffix('.receipt.json')
                captures[name] = ((root / f'{name}.body').read_bytes(), json.loads(receipt.read_bytes()), b'')
        quarantine_incomplete_capture(storage, root, DATASET_SLUG, 'invalid',
            {name: (body, receipt) for name, (body, receipt, _wire) in captures.items()},
            str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    fingerprint = sha(canonical({'records': audit['source_record_sha256'], 'identity': identity}))
    version = 'v' + fingerprint[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state = f'_construction-state/{DATASET_SLUG}/{version}.json'
    result = {'dataset_slug': DATASET_SLUG, 'file_slug': FILE_SLUG, 'version': version,
        'candidate_ready': prefix + '/metadata/candidate_ready.json',
        'feature_count': audit['feature_count'], 'source_count': audit['source_count'],
        'source_id_sha256': audit['source_id_sha256'], 'null_geometry_count': audit['null_geometry_count'],
        'source_population_complete': True, **APPROVALS}
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, version, fingerprint))['payload']
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, version,
            fingerprint, identity, audit['source_record_sha256'], audit['source_schema_sha256'],
            original['objects'], original['capture_receipts'])
        return {**result, 'reused': True}
    parquet = geoparquet(rows)
    parquet_path = f'geoparquet/{FILE_SLUG}.parquet'
    descriptor = {'bytes': len(parquet), 'sha256': sha(parquet)}
    inputs = [{'input_id': name, 'producer': 'Department of Veterans Affairs', 'host': 'api.va.gov',
        'role': 'producer_publication', 'canonical_url': SEARCH_URL, 'retrieval_url': SEARCH_URL,
        'observed_final_url': receipt['final_url'], 'retrieved_at': receipt['observed_at'],
        'http_status': receipt['status'], 'bytes': len(body), 'sha256': sha(body),
        'wire_bytes': len(wire), 'wire_sha256': sha(wire), 'release_date': None,
        'release_date_unknown_reason': audit['date_semantics'],
        'license': 'Public VA locator; redistribution rights not accepted',
        'attribution': 'U.S. Department of Veterans Affairs; current public locator',
        'request_method': 'POST', 'request_json': receipt['request_json']}
        for name, (body, receipt, wire) in sorted(captures.items())]
    manifest = {'schema_version': 1, 'approved': False,
        'title': 'VA health facilities and Vet Centers — current public locator draft',
        'description': 'Complete nonmobile health/Vet Center population from the current VA public locator. '
            'CLOSED/COMING_SOON records retained raw and explicitly excluded from the candidate. '
            'Original scalar values, native station IDs and WGS84 coordinates retained. '
            'Mixed dict/list/null columns explicitly encode logical JSON; full source record also retained. '
            'No historical NGA/ORNL/TechniGraphics enrichment or normalized text is reconstructed.',
        'agency': 'Department of Veterans Affairs', 'publisher': 'Department of Veterans Affairs',
        'source_url': LANDING_URL, 'source_release_date': None,
        'processor': 'HIFLD Next', 'code_commit': identity['code_commit'],
        'inputs': inputs, 'anonymous_session_bootstrap': {'method': 'HEAD', 'url': CSRF_URL,
            'status': 204, 'credentials_retained': False},
        'transformations': [{'operation': 'native_json_to_geoparquet_candidate', 'parameters': {
            'scope': 'health and vet_center; mobile=false; no spatial filter; exclude CLOSED/COMING_SOON only',
            'identifiers': 'root id and attributes.id validated identical and represented once as id; '
                'both original occurrences retained in source_record_json; uniqueId unchanged string',
            'logical_json_columns': sorted(JSON_FIELDS),
            'json_encoding': 'canonical JSON UTF-8 strings; decode to recover original dict/list/null and all nested types; '
                'scalar fields remain native Arrow strings/booleans/float64/null; no missing keys filled in full record',
            'geometry': 'WKB Point(long,lat), native numeric values, no transformation; WGS84 based on VA Facilities API documentation',
            'dates': audit['date_semantics']}}],
        'lineage': 'First-party current www.va.gov locator uses POST facilities_api/v2/va, which VA documents as '
            'the Lighthouse-backed health/Vet Center locator. Public application bundle implements normal anonymous '
            'HEAD /v0/csrf_token bootstrap. Independent 2026-11-15 national no-bbox extraction matched two global-bbox '
            'sweeps (1629 records). Historical HIFLD/NGA-enriched medical-facilities contract is not reconstructed. '
            'No archive/mirror is an input. Source cadence and record/coordinate update dates remain unknown.',
        'prebuilt_output_policy': {'schema_version': 1, 'formats': {'geoparquet': {
            'reason': 'All original scalar and logically encoded nested JSON values and native coordinates preserved.',
            'objects': [{'path': parquet_path, **descriptor}]}}},
        'unsupported_output_formats': {'geopackage': 'No approved mapping for mixed JSON fields',
            'shapefile_zip': 'No lossless approved mapping', 'pmtiles': 'No display transformation approved'},
        **APPROVALS}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {parquet_path: parquet, 'metadata/source_manifest.json': canonical(manifest) + b'\n',
        'metadata/source_audit.json': canonical(audit) + b'\n',
        'metadata/processing_identity.json': canonical(identity) + b'\n',
        'metadata/candidate_ready.json': canonical({'approved': False, 'version': version,
            'objects': {parquet_path: descriptor}, **result}) + b'\n'}
    for name, (body, receipt, wire) in captures.items():
        outputs.update({f'raw/{name}.body': body, f'raw/{name}.wire': wire,
            f'raw/{name}.receipt.json': canonical(receipt) + b'\n'})
    objects = {}
    for name, body in outputs.items():
        key = prefix + '/' + name
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != sha(body):
            raise ValueError('candidate_physical_readback_failed:' + name)
        objects[name] = {'bytes': len(body), 'sha256': sha(body)}
    seal_ready_candidate(storage, state, version, fingerprint, {'processing_identity': identity,
        'source_record_sha256': audit['source_record_sha256'],
        'source_schema_sha256': audit['source_schema_sha256'],
        'capture_receipts': {name: receipt for name, (_body, receipt, _wire) in captures.items()},
        'objects': objects})
    validate_construction_code_identity(Path(__file__).parents[3], identity['code_commit'],
        identity['files'], identity=identity)
    return {**result, 'reused': False, 'capture_directory': str(root)}
