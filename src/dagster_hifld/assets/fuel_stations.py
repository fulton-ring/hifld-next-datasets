"""Reviewable local draft pipeline for DOE/NLR's BTS-distributed stations.

The explicit normal ingest uses local review staging and the shared #417
publication gate. A separate raw/candidate graph supports bounded replay. All
raw ArcGIS fields, including OBJECTID and id, survive the candidate unchanged.
"""
from __future__ import annotations

import hashlib
import datetime
import json
import math
import shutil
import subprocess
import time
import uuid
import urllib.error
import http.client
import urllib.parse
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from dagster import (
    AssetIn, AssetKey, AssetSelection, ConfigurableResource,
    DefaultScheduleStatus, DynamicPartitionsDefinition, RunRequest,
    ScheduleDefinition, SkipReason, asset, define_asset_job,
)
from pyproj import CRS
from shapely import Point, to_wkb

from dagster_hifld.partitions import SEMVER_VERSION_RE
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.release_approval import verifier_implementation_sha256

SLUG = 'alternative-fueling-stations'
SERVICE = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/Alternative_Fueling_Stations/FeatureServer/0'
PROPOSED_SCHEMA_SHA256 = 'd7141e155260c80d2d87f80a3f62e7273b792154e1d77c85ff2ef5c99f25fdc3'  # Review draft pin, not an approved contract.
UNSUPPORTED_OUTPUT_FORMATS = {
    'shapefile_zip': 'Unapproved lossy format: 80 source fields include >10-character names/identifier columns and embedded JSON text beyond DBF width; no reviewed lossless name/type/null/value mapping.',
    'pmtiles': 'Unapproved derived representation: actual tippecanoe command permits density-based feature dropping and indexed FlatGeobuf assigns feature IDs by sequence; rendered per-zoom station-ID and coverage preservation are not verified. Canonical typed candidate/GeoPackage/Parquet remain available.',
}
FUEL_PARTITIONS = DynamicPartitionsDefinition(name='draft_alternative_fueling_stations_version')
_TYPES = {'esriFieldTypeOID': pa.int64(), 'esriFieldTypeInteger': pa.int32(),
          'esriFieldTypeDouble': pa.float64(), 'esriFieldTypeDate': pa.int64(),
          'esriFieldTypeString': pa.string()}


class FuelDraftStorage(ConfigurableResource):
    """Explicit local research storage; no cloud destination or approval switch."""
    capture_dir: str
    draft_dir: str


def _strict_json(raw):
    def members(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'Duplicate JSON member: {key}')
            result[key] = value
        return result
    def invalid_constant(value):
        raise ValueError(f'Non-JSON numeric constant: {value}')
    return json.loads(raw, object_pairs_hook=members, parse_constant=invalid_constant)


def _json(path: Path):
    return _strict_json(path.read_bytes())


def _require_review_staging(staging):
    if isinstance(staging, PublishedStorageResource):
        raise ValueError('Review draft refuses published storage before any capture or write')
    if not isinstance(staging, StagingStorageResource) or not staging.use_local or staging.bucket:
        raise ValueError('Review draft permits isolated local staging only')
    prefix = Path(staging.prefix)
    if prefix.is_absolute() or '..' in prefix.parts:
        raise ValueError('Review staging prefix must remain inside its local root')


def processing_identity():
    pipeline = verifier_implementation_sha256()
    versions = {'pyarrow': pa.__version__, 'pyogrio': pyogrio.__version__,
                'gdal': str(pyogrio.__gdal_version__)}
    fingerprint = hashlib.sha256(json.dumps({'installed_pipeline_sha256': pipeline,
        'versions': versions}, sort_keys=True).encode()).hexdigest()
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    commit = result.stdout.strip() if result.returncode == 0 else None
    dirty = subprocess.run(['git', '-C', str(root), 'diff', '--quiet', '--', 'src/dagster_hifld'], capture_output=True).returncode
    return {'builder_fingerprint': fingerprint, 'installed_pipeline_sha256': pipeline,
        'code_commit': commit, 'working_tree_modified': dirty != 0, 'versions': versions,
        'meaning': 'Original processing identity; replay does not invent a new upstream retrieval.'}


def _census(count, identifiers):
    if type(count) is not int or not 0 < count <= 250_000:
        raise ValueError('Invalid/beyond-bounds count witness')
    if not isinstance(identifiers, list) or len(identifiers) != count:
        raise ValueError('ID witness cardinality differs from count')
    if any(type(value) is not int or not -(2**63) <= value < 2**63 for value in identifiers):
        raise ValueError('ID witnesses must be exact 64-bit integers')
    if len(set(identifiers)) != count:
        raise ValueError('Duplicate ID witness')
    return identifiers


def _verify_data_receipt_scope(name, receipt, identifiers):
    """Request and observed terminal URL must identify this exact official feed."""
    if name in ('bts_layer', 'bts_layer_after'):
        path = urllib.parse.urlsplit(SERVICE).path
        expected = {'f': 'json'}
    elif name.startswith('bts_count'):
        path = urllib.parse.urlsplit(SERVICE).path + '/query'
        expected = {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'}
    elif name.startswith('bts_ids'):
        path = urllib.parse.urlsplit(SERVICE).path + '/query'
        expected = {'f': 'json', 'where': '1=1', 'returnIdsOnly': 'true'}
    elif name.startswith('page-'):
        page = int(name.removeprefix('page-'))
        group = sorted(identifiers)[page * 2000:(page + 1) * 2000]
        path = urllib.parse.urlsplit(SERVICE).path + '/query'
        expected = {'f': 'json', 'where': f'OBJECTID >= {group[0]} AND OBJECTID <= {group[-1]}',
            'outFields': '*', 'returnGeometry': 'true', 'orderByFields': 'OBJECTID ASC', 'resultRecordCount': '2000'}
    else:
        raise ValueError('Unrecognized official data receipt role')
    for field in ('url', 'final_url'):
        value = receipt.get(field)
        if not isinstance(value, str):
            raise ValueError(f'Unverified retrieval: missing observed {field} for {name}')
        parsed = urllib.parse.urlsplit(value)
        pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        params = dict(pairs)
        if (len(params) != len(pairs) or parsed.scheme != 'https'
                or parsed.hostname != 'services.arcgis.com' or parsed.port not in (None, 443)
                or parsed.username is not None or parsed.password is not None or parsed.fragment
                or parsed.path != path):
            raise ValueError(f'Unverified official data receipt origin/scope for {name}')
        if name in ('bts_layer', 'bts_layer_after') and params.get('f') == 'pjson':
            params['f'] = 'json'
        if params != expected:
            raise ValueError(f'Unverified official data receipt query scope for {name}')


def _raw_files(root):
    paths = list(root.glob('*.body')) + list(root.glob('*.receipt.json'))
    if (root / 'capture-status.json').is_file(): paths.append(root / 'capture-status.json')
    attempts = root / 'attempts'
    if attempts.exists(): paths.extend(p for p in attempts.rglob('*') if p.is_file())
    return sorted(set(paths))


def _preserve_for_review(capture, staging, version):
    _require_review_staging(staging)
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError('Explicit immutable semver version required')
    target = Path(staging.local_dir).resolve() / staging.prefix / 'quarantine' / SLUG / version / uuid.uuid4().hex
    target.mkdir(parents=True, exist_ok=False)
    (target / 'raw').mkdir()
    (target / 'status.json').write_text(json.dumps({'state': 'pending', 'version': version,
        'source_capture': str(capture), 'approval': None}, sort_keys=True))
    for path in _raw_files(capture):
        if path.is_symlink(): raise ValueError('Raw capture symlinks are not accepted')
        output = target / 'raw' / path.relative_to(capture)
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, output)
    return target


def _capture_json(destination, name, url, attempts=3):
    """Persist every bounded response, including partial/error bytes, before parsing."""
    folder = destination / 'attempts' / name
    folder.mkdir(parents=True, exist_ok=False)
    last_error = None
    for number in range(attempts):
        body_path = folder / f'{number:03d}.body'
        receipt = {'url': url, 'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'attempt': number + 1, 'status': 0, 'complete_body': False}
        response = None
        try:
            try:
                response = urllib.request.urlopen(url, timeout=30)
            except urllib.error.HTTPError as error:
                response = error
            receipt.update(status=response.status, final_url=response.geturl() if hasattr(response, 'geturl') else None,
                date=response.headers.get('Date'),
                content_length=response.headers.get('Content-Length'))
            with body_path.open('xb', buffering=0) as output:
                size = 0
                while True:
                    try:
                        block = response.read(min(64 * 1024, 12_000_001 - size))
                    except http.client.IncompleteRead as error:
                        output.write(error.partial)
                        raise
                    if not block: break
                    output.write(block); size += len(block)
                    if size > 12_000_000:
                        raise ValueError('Response exceeds bounded page size; partial bytes retained')
            declared = receipt['content_length']
            if declared is not None and (not declared.isdigit() or size != int(declared)):
                raise ValueError('HTTP body length differs from declared length; partial bytes retained')
            receipt['complete_body'] = True
            if receipt['status'] != 200:
                raise ValueError(f'HTTP response {receipt["status"]}')
            data = _json(body_path)
            if not isinstance(data, dict) or 'error' in data:
                raise ValueError('ArcGIS application error or nonobject response')
        except Exception as error:
            last_error = error
            receipt['error'] = f'{type(error).__name__}: {error}'
            if not body_path.exists(): body_path.write_bytes(b'')
        finally:
            if response is not None: response.close()
            receipt.update(bytes=body_path.stat().st_size, sha256=_digest(body_path))
            (folder / f'{number:03d}.receipt.json').write_text(json.dumps(receipt, sort_keys=True))
        if 'error' not in receipt:
            shutil.copyfile(body_path, destination / f'{name}.body')
            (destination / f'{name}.receipt.json').write_text(json.dumps(receipt, sort_keys=True))
            return data
        if number + 1 < attempts: time.sleep(0.05 * 2**number)
    raise ValueError(f'Capture failed after {attempts} bounded attempts for {name}: {last_error}')



def _digest(path: Path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _exact_point(geometry):
    if (set(geometry or {}) != {'x', 'y'}
            or not all(type(geometry[key]) in (int, float) and math.isfinite(geometry[key]) for key in ('x', 'y'))):
        raise ValueError('Missing/invalid point geometry')
    point = Point(geometry['x'], geometry['y'])
    if point.x != geometry['x'] or point.y != geometry['y']:
        raise ValueError('Point geometry would lose source numeric precision')
    return point


def capture_live(destination: Path) -> None:
    """Capture a bounded complete native-CRS feed, recording HTTP byte receipts.

    No NLR API key or registered AFDC download is needed for this official BTS
    distribution. Existing captures are never overwritten.
    """
    destination.mkdir(parents=True, exist_ok=False)

    def get(name: str, query: dict | None = None):
        url = SERVICE + ('/query?' + urllib.parse.urlencode(query) if query else '?f=pjson')
        return _capture_json(destination, name, url)

    try:
        get('bts_layer')
        count = get('bts_count', {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'})['count']
        ids = sorted(_census(count, get('bts_ids', {'f': 'json', 'where': '1=1', 'returnIdsOnly': 'true'})['objectIds']))
        for index, start in enumerate(range(0, count, 2000)):
            group = ids[start:start + 2000]
            get(f'page-{index:03d}', {'f': 'json', 'where': f'OBJECTID >= {group[0]} AND OBJECTID <= {group[-1]}',
                'outFields': '*', 'returnGeometry': 'true', 'orderByFields': 'OBJECTID ASC', 'resultRecordCount': 2000})
        get('bts_count_after', {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'})
        get('bts_ids_after', {'f': 'json', 'where': '1=1', 'returnIdsOnly': 'true'})
        get('bts_layer_after')
        inspect_capture(destination)
    except Exception as error:
        (destination / 'capture-status.json').write_text(json.dumps({'state': 'blocked',
            'reason': f'{type(error).__name__}: {error}', 'raw_preserved': True, 'approval': None}, sort_keys=True))
        raise
    (destination / 'capture-status.json').write_text(json.dumps({'state': 'complete_unapproved_draft',
        'raw_preserved': True, 'approval': None}, sort_keys=True))


def inspect_capture(root: Path) -> dict:
    """Fail closed on changed census/schema/edit date, truncation, IDs or geometry."""
    layer = _json(root / 'bts_layer.body')
    after = _json(root / 'bts_layer_after.body')
    if layer['fields'] != after['fields'] or layer['editingInfo'] != after['editingInfo']:
        raise ValueError('Source changed during extraction')
    if layer['geometryType'] != 'esriGeometryPoint' or layer.get('hasZ') or layer.get('hasM'):
        raise ValueError('Unsupported geometry dimensionality')
    expected = _json(root / 'bts_ids.body')['objectIds']
    ending = _json(root / 'bts_ids_after.body')['objectIds']
    count = _json(root / 'bts_count.body')['count']
    final_count = _json(root / 'bts_count_after.body')['count']
    _census(count, expected)
    _census(final_count, ending)
    if count != final_count or set(expected) != set(ending):
        raise ValueError('Changed or incomplete source ID census')
    paths = sorted(root.glob('page-*.body'))
    expected_pages = (count + 1999) // 2000
    if [p.name for p in paths] != [f'page-{n:03d}.body' for n in range(expected_pages)]:
        raise ValueError('Missing or extra page')
    fields = {f['name']: f for f in layer['fields']}
    if len(fields) != len(layer['fields']) or any(f['type'] not in _TYPES for f in fields.values()):
        raise ValueError('Unsupported or duplicate schema field')
    oid_field = layer['objectIdField']
    if oid_field != 'OBJECTID' or 'id' not in fields:
        raise ValueError('Missing or renamed station identifiers')
    seen, station_ids, files = set(), set(), []
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    for path in paths:
        page = _json(path)
        if 'error' in page or page.get('exceededTransferLimit'):
            raise ValueError('Error or truncated response')
        if page.get('fields') != layer['fields']:
            # ArcGIS query field definitions omit nullable/editable/defaultValue.
            signatures = lambda fs: [(f['name'], f['type']) for f in fs]
            if signatures(page.get('fields', [])) != signatures(layer['fields']):
                raise ValueError('Page schema differs')
        if page.get('spatialReference') != layer['extent']['spatialReference']:
            raise ValueError('Page CRS differs')
        if len(page['features']) > 2000:
            raise ValueError('Page exceeds requested limit')
        for feature in page['features']:
            attrs, geom = feature['attributes'], feature['geometry']
            if set(attrs) != set(fields):
                raise ValueError('Missing/extra raw attribute')
            _exact_point(geom)
            for name, value in attrs.items():
                field = fields[name]
                if value is None:
                    if not field['nullable']:
                        raise ValueError('Null non-nullable field')
                    continue
                kind = field['type']
                if kind == 'esriFieldTypeString' and not isinstance(value, str):
                    raise ValueError('String type loss')
                if kind in ('esriFieldTypeOID', 'esriFieldTypeInteger', 'esriFieldTypeDate') and type(value) is not int:
                    raise ValueError('Integer/date type loss')
                if kind in ('esriFieldTypeOID', 'esriFieldTypeDate') and not -(2**63) <= value < 2**63:
                    raise ValueError('64-bit integer overflow')
                if kind == 'esriFieldTypeInteger' and not -(2**31) <= value < 2**31:
                    raise ValueError('32-bit integer overflow')
                if kind == 'esriFieldTypeDouble' and (type(value) not in (int, float) or not math.isfinite(value)):
                    raise ValueError('Invalid double')
            oid, station_id = attrs['OBJECTID'], attrs['id']
            if oid in seen or station_id is None or station_id in station_ids:
                raise ValueError('Duplicate or missing raw ID')
            seen.add(oid); station_ids.add(station_id)
            bounds = [min(bounds[0], geom['x']), min(bounds[1], geom['y']), max(bounds[2], geom['x']), max(bounds[3], geom['y'])]
        files.append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)})
    if seen != set(expected):
        raise ValueError('Full extraction does not equal source census')
    for name in ['bts_layer','bts_layer_after','bts_ids','bts_ids_after','bts_count','bts_count_after']:
        path = root / f'{name}.body'
        files.append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)})
    receipts = []
    for record in files:
        receipt_path = root / record['name'].replace('.body', '.receipt.json')
        receipt = _json(receipt_path)
        _verify_data_receipt_scope(record['name'].removesuffix('.body'), receipt, expected)
        if (receipt.get('status') != 200 or receipt.get('sha256') != record['sha256']
                or receipt.get('bytes') != record['bytes']
                or receipt.get('complete_body', True) is not True):
            raise ValueError('HTTP receipt/checksum mismatch')
        receipts.append({'name': receipt_path.name, 'bytes': receipt_path.stat().st_size, 'sha256': _digest(receipt_path)})
    files.extend(receipts)
    if (root / 'capture-status.json').is_file():
        path = root / 'capture-status.json'
        files.append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)})
    attempts_dir = root / 'attempts'
    if attempts_dir.exists():
        files.extend({'name': str(path.relative_to(root)), 'bytes': path.stat().st_size, 'sha256': _digest(path)}
            for path in sorted(attempts_dir.rglob('*')) if path.is_file())
    return {'rows': len(seen), 'station_ids': len(station_ids), 'pages': len(paths), 'bounds': bounds,
            'source': SERVICE, 'editing_info': layer['editingInfo'], 'fields': layer['fields'],
            'crs': layer['extent']['spatialReference'], 'raw_files': files,
            'acceptance': 'unapproved_draft', 'promotion_allowed': False}


def build_candidate(capture: Path, destination: Path) -> dict:
    """Lossless native-schema GeoParquet draft, never a published projection."""
    audit = inspect_capture(capture)
    processing = processing_identity()
    fields = audit['fields']
    schema = pa.schema([pa.field(f['name'], _TYPES[f['type']], nullable=f['nullable']) for f in fields] + [pa.field('geometry', pa.binary(), nullable=False)])
    crs = CRS.from_epsg(audit['crs']['latestWkid']).to_json_dict()
    schema = schema.with_metadata({b'geo': json.dumps({'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {'encoding': 'WKB', 'geometry_types': ['Point'], 'crs': crs, 'bbox': audit['bounds']}}}, sort_keys=True).encode()})
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / 'candidate.parquet'
    request_path = destination / 'processing-request.json'
    request = {'processing': processing, 'raw_files': audit['raw_files']}
    if request_path.exists():
        original = _json(request_path)
        if (original['processing']['builder_fingerprint'] != processing['builder_fingerprint']
                or original['raw_files'] != audit['raw_files']):
            raise FileExistsError('Immutable processing request changed; use a new version')
    elif target.exists():
        raise FileExistsError('Existing candidate lacks its immutable processing request')
    else:
        with request_path.open('x') as output:
            output.write(json.dumps(request, sort_keys=True, indent=2) + '\n')
    provenance = destination / 'provenance.json'
    if target.exists():
        previous = _json(provenance)
        if (previous.get('processing', {}).get('builder_fingerprint') == processing['builder_fingerprint']
                and previous['raw_files'] == audit['raw_files'] and previous['candidate_sha256'] == _digest(target)):
            return previous
        raise FileExistsError('Immutable candidate version collision')
    temporary = destination / 'candidate.parquet.partial'
    with pq.ParquetWriter(temporary, schema, compression='zstd') as writer:
        for path in sorted(capture.glob('page-*.body')):
            rows = []
            for feature in _json(path)['features']:
                rows.append({**feature['attributes'], 'geometry': to_wkb(_exact_point(feature['geometry']), byte_order=1)})
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    temporary.replace(target)
    audit.update(processing=processing, original_retrieval_receipts=[{'name': p.name, 'url': _json(p).get('url'), 'final_url': _json(p).get('final_url'),
                    'observed_at': _json(p).get('observed_at'), 'http_date': _json(p).get('date', _json(p).get('headers', {}).get('date'))}
                    for p in sorted(capture.glob('*.receipt.json')) if p.name.startswith(('bts_', 'page-'))],
                 producer='DOE/NLR AFDC', distributor='USDOT_BTS official NTAD distribution',
                 candidate_sha256=_digest(target), candidate_bytes=target.stat().st_size,
                 proposed_mapping='identity for every raw field; epoch-ms dates remain int64; native EPSG:3857 geometry',
                 schema=[{'name': f.name, 'type': str(f.type), 'nullable': f.nullable} for f in schema])
    (destination / 'provenance.json').write_text(json.dumps(audit, sort_keys=True, indent=2) + '\n')
    return audit


@asset(key=AssetKey(['draft_raw', SLUG, SLUG]), partitions_def=FUEL_PARTITIONS, group_name='draft_bts')
def fuel_stations_raw(context, fuel_draft_storage: FuelDraftStorage) -> str:
    root = Path(fuel_draft_storage.capture_dir)
    retained = _preserve_for_review(root, StagingStorageResource(local_dir=fuel_draft_storage.draft_dir, use_local=True), context.partition_key)
    root = retained / 'raw'
    try:
        audit = inspect_capture(root)
    except Exception as error:
        (retained / 'status.json').write_text(json.dumps({'state': 'blocked', 'reason': str(error), 'raw_preserved': True, 'approval': None}))
        raise
    audit['processing'] = processing_identity()
    target = Path(fuel_draft_storage.draft_dir) / SLUG / context.partition_key / 'raw'
    target.mkdir(parents=True, exist_ok=True)
    for record in audit['raw_files']:
        source, output = root / record['name'], target / record['name']
        if output.exists() and _digest(output) != record['sha256']:
            raise ValueError('Immutable raw version collision')
        if not output.exists():
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, output)
    manifest = target / 'capture-manifest.json'
    if manifest.exists():
        previous = _json(manifest)
        if (previous.get('processing', {}).get('builder_fingerprint') != audit['processing']['builder_fingerprint']
                or previous['raw_files'] != audit['raw_files']):
            raise ValueError('Immutable raw processing version collision')
    else:
        manifest.write_text(json.dumps(audit, sort_keys=True, indent=2) + '\n')
    (retained / 'status.json').write_text(json.dumps({'state': 'complete_unapproved_draft', 'raw_preserved': True, 'approval': None}))
    context.add_output_metadata({'rows': audit['rows'], 'pages': audit['pages'], 'promotion_allowed': False})
    return str(target)


@asset(key=AssetKey(['draft_candidate', SLUG, SLUG]), partitions_def=FUEL_PARTITIONS, group_name='draft_bts', ins={'fuel_stations_raw': AssetIn(key=AssetKey(['draft_raw', SLUG, SLUG]))})
def fuel_stations_candidate(context, fuel_draft_storage: FuelDraftStorage, fuel_stations_raw: str) -> dict:
    target = Path(fuel_draft_storage.draft_dir) / SLUG / context.partition_key / 'candidate'
    manifest = build_candidate(Path(fuel_stations_raw), target)
    context.add_output_metadata({'rows': manifest['rows'], 'sha256': manifest['candidate_sha256'], 'promotion_allowed': False})
    return manifest


fuel_stations_draft_assets = [fuel_stations_raw, fuel_stations_candidate]


def verify_typed_geopackage(candidate: Path, bridge: Path) -> dict:
    """Recheck every field, physical type, nullability, value and native point."""
    parquet = pq.ParquetFile(candidate)
    rows = 0
    with pyogrio.open_arrow(bridge, layer=SLUG, use_pyarrow=True, batch_size=2000) as (metadata, reader):
        if CRS.from_user_input(metadata['crs']) != CRS.from_epsg(3857):
            raise ValueError('Typed bridge CRS changed')
        geom_name = metadata['geometry_name']
        expected_names = [n for n in parquet.schema_arrow.names if n != 'geometry']
        if list(metadata['fields']) != expected_names:
            raise ValueError('Typed bridge missing/extra/reordered source fields')
        for expected, actual in zip(parquet.iter_batches(batch_size=2000), reader, strict=True):
            if expected.num_rows != actual.num_rows:
                raise ValueError('Typed bridge row batches differ')
            for field in parquet.schema_arrow:
                name = geom_name if field.name == 'geometry' else field.name
                other = actual.schema.field(name)
                if field.type != other.type or (field.name != 'geometry' and field.nullable != other.nullable):
                    raise ValueError(f'Typed bridge physical schema changed: {field.name}')
                if not expected.column(field.name).equals(actual.column(name)):
                    raise ValueError(f'Typed bridge value/geometry changed: {field.name}')
            rows += actual.num_rows
    if rows != parquet.metadata.num_rows:
        raise ValueError('Typed bridge incomplete')
    return {'rows': rows, 'all_source_fields_types_nullability_values_equal': True,
        'all_native_geometry_wkb_equal': True, 'source_ids_substituted': False,
        'internal_driver_fid': 'GeoPackage format machinery only; original OBJECTID/id retained as independent named integer columns',
        'sha256': _digest(bridge)}


def write_typed_geopackage(candidate: Path, destination: Path, source_edit_ms: int) -> None:
    """Arrow stream bridge avoids GeoPandas nullable-integer coercion.

    GeoPackage's internal fid is format machinery, not a replacement source ID.
    Original OBJECTID/id columns remain present with declared integer widths and
    nullability; no published mapping is implied by this draft representation.
    """
    if destination.exists():
        verify_typed_geopackage(candidate, destination)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    parquet = pq.ParquetFile(candidate)
    reader = pa.RecordBatchReader.from_batches(parquet.schema_arrow, parquet.iter_batches(batch_size=2000))
    temporary = destination.with_suffix('.partial.gpkg')
    if temporary.exists(): temporary.unlink()
    previous = pyogrio.get_gdal_config_option('OGR_CURRENT_DATE')
    timestamp = datetime.datetime.fromtimestamp(source_edit_ms / 1000, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
    try:
        pyogrio.set_gdal_config_options({'OGR_CURRENT_DATE': timestamp})
        pyogrio.write_arrow(reader, temporary, driver='GPKG', layer=SLUG,
            geometry_name='geometry', geometry_type='Point', crs=CRS.from_epsg(3857).to_wkt())
    finally:
        pyogrio.set_gdal_config_options({'OGR_CURRENT_DATE': previous})
    verify_typed_geopackage(candidate, temporary)
    temporary.replace(destination)


def _stage_capture_payload(capture: Path, staging: StagingStorageResource, version: str) -> dict:
    """Stage native raw proof + lossless typed draft and canonical GeoJSON input.

    No publication mapping is applied. Generic downstream conversions remain
    draft candidates behind the #417 release boundary and require type review.
    """
    _require_review_staging(staging)
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError('Explicit immutable semver version required')
    audit = inspect_capture(capture)
    signature = [{k: f[k] for k in ('name', 'type', 'nullable')} for f in audit['fields']]
    schema_digest = hashlib.sha256(json.dumps(signature, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if schema_digest != PROPOSED_SCHEMA_SHA256:
        raise ValueError('Source schema drift quarantined; revise the review draft explicitly')
    root = Path(staging.local_dir).resolve() / staging.prefix / SLUG / SLUG / version
    root.mkdir(parents=True, exist_ok=True)
    for record in audit['raw_files']:
        output = root / 'raw_arcgis' / record['name']
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists() and _digest(output) != record['sha256']:
            raise ValueError('Immutable source staging collision')
        if not output.exists(): shutil.copyfile(capture / record['name'], output)
    candidate = build_candidate(capture, root / 'draft_candidate')
    write_typed_geopackage(root / 'draft_candidate' / 'candidate.parquet',
        root / 'geopackage' / (SLUG + '.gpkg'), audit['editing_info']['dataLastEditDate'])
    path = root / 'geojson' / (SLUG + '.geojson')
    path.parent.mkdir(exist_ok=True)
    partial = path.with_suffix('.partial')
    with partial.open('w') as output:
        output.write('{"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:3857"}},"features":[')
        first = True
        for page in sorted(capture.glob('page-*.body')):
            for feature in _json(page)['features']:
                if not first: output.write(',')
                first = False
                output.write(json.dumps({'type':'Feature', 'properties': feature['attributes'], 'geometry':{'type':'Point','coordinates':[feature['geometry']['x'],feature['geometry']['y']]}}, separators=(',', ':'), sort_keys=True, allow_nan=False))
        output.write(']}')
    if path.exists() and _digest(path) != _digest(partial):
        raise ValueError('Immutable GeoJSON candidate collision')
    if path.exists(): partial.unlink()
    else: partial.replace(path)
    metadata = root / 'metadata'; metadata.mkdir(exist_ok=True)
    manifest = {'title':'Alternative Fueling Stations', 'agency':'U.S. Department of Energy / National Laboratory of the Rockies',
        'publisher':'Bureau of Transportation Statistics (USDOT_BTS; official distributor)',
        'source_url':SERVICE, 'source_modified':audit['editing_info'], 'schema_version':'draft-unapproved-v1',
        'processor':candidate['processing'], 'code_commit':candidate['processing']['code_commit'],
        'original_retrieval_receipts':candidate['original_retrieval_receipts'],
        'inputs':[{'input_id':SERVICE, 'producer':candidate['producer'], 'distributor':candidate['distributor'],
            'original_retrieval_receipts':candidate['original_retrieval_receipts'],
            'raw_files':audit['raw_files'], 'crs':audit['crs'], 'fields':audit['fields']}],
        'transformations':[{'name':'raw ArcGIS point to native-CRS GeoJSON + typed GeoParquet + Arrow-stream typed GeoPackage', 'attribute_mapping':'identity; no omissions; epoch-ms dates retained as integers', 'acceptance':'unapproved'}],
        'lineage':{'producer':'DOE/NLR AFDC', 'distributor_item':'9010c04bcdf84c36abb74e98778af579','crosswalk':'unverified; DataLumos403 prevents inspecting the mapping'},
        'promotion_allowed':False, 'complete_rows':audit['rows'],
        'unsupported_output_formats':dict(UNSUPPORTED_OUTPUT_FORMATS)}
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2)+'\n').encode()
    manifest_path = metadata / 'source_manifest.json'
    if manifest_path.exists() and manifest_path.read_bytes() != manifest_bytes:
        raise ValueError('Immutable source manifest collision')
    if not manifest_path.exists(): manifest_path.write_bytes(manifest_bytes)
    return {'version': version, 'rows':audit['rows'], 'source':SERVICE, 'candidate_sha256':candidate['candidate_sha256'], 'promotion_allowed':False}


def stage_fuel_capture(capture: Path, staging: StagingStorageResource, version: str) -> dict:
    _require_review_staging(staging)
    retained = _preserve_for_review(capture, staging, version)
    try:
        result = _stage_capture_payload(retained / 'raw', staging, version)
    except Exception as error:
        (retained / 'status.json').write_text(json.dumps({'state': 'blocked', 'version': version,
            'reason': f'{type(error).__name__}: {error}', 'raw_preserved': True, 'approval': None}, sort_keys=True))
        raise
    (retained / 'status.json').write_text(json.dumps({'state': 'complete_unapproved_draft',
        'version': version, 'raw_preserved': True, 'approval': None}, sort_keys=True))
    return result


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='bts', compute_kind='http',
       config_schema={'capture_dir':str, 'version':str},
       description='Draft DOE/NLR AFDC feed via official USDOT_BTS distributor; all raw fields and IDs retained. Local research staging only; publication requires independent exact-release approval.')
def bts_alternative_fueling_stations(context, staging_storage: StagingStorageResource) -> dict:
    _require_review_staging(staging_storage)
    capture_dir = context.op_config['capture_dir']
    if capture_dir:
        result = stage_fuel_capture(Path(capture_dir), staging_storage, context.op_config['version'])
    else:
        version = context.op_config['version']
        if not SEMVER_VERSION_RE.fullmatch(version):
            raise ValueError('Explicit immutable semver version required')
        retained = Path(staging_storage.local_dir).resolve() / staging_storage.prefix / 'quarantine' / SLUG / version / uuid.uuid4().hex
        retained.mkdir(parents=True, exist_ok=False)
        state = retained / 'status.json'
        state.write_text(json.dumps({'state': 'pending_live_capture', 'version': version, 'approval': None}))
        capture = retained / 'capture'
        try:
            capture_live(capture)
            result = stage_fuel_capture(capture, staging_storage, version)
        except Exception as error:
            state.write_text(json.dumps({'state': 'blocked', 'version': version,
                'reason': f'{type(error).__name__}: {error}', 'raw_preserved': True, 'approval': None}, sort_keys=True))
            raise
        state.write_text(json.dumps({'state': 'complete_unapproved_draft', 'version': version, 'raw_preserved': True, 'approval': None}))
    context.add_output_metadata({'rows':result['rows'], 'version':result['version'], 'promotion_allowed':False})
    return result


fuel_stations_draft_refresh_job = define_asset_job(
    name='fuel_stations_draft_refresh_job',
    selection=AssetSelection.assets(bts_alternative_fueling_stations),
    tags={'hifld_review_draft': 'true', 'hifld_source_only': 'true'},
    description='Local source-only integrity poll; no catalog/publish selection or release approval.',
)


def _draft_refresh_request(context):
    scheduled = context.scheduled_execution_time
    if scheduled is None:
        return SkipReason('A dated schedule context is required for immutable draft versions')
    stamp = scheduled.astimezone(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return RunRequest(
        run_key=f'{SLUG}-draft-{stamp}',
        run_config={'ops': {bts_alternative_fueling_stations.op.name: {
            'config': {'capture_dir': '', 'version': f'v0.0.0-research-{stamp}'}}}},
        tags={'hifld_review_draft': 'true', 'hifld_source_only': 'true',
              'hifld_promotion_allowed': 'false'},
    )


fuel_stations_draft_refresh_schedule = ScheduleDefinition(
    name='fuel_stations_draft_refresh_schedule',
    job=fuel_stations_draft_refresh_job,
    cron_schedule='0 9 * * *',
    execution_timezone='America/New_York',
    execution_fn=_draft_refresh_request,
    default_status=DefaultScheduleStatus.STOPPED,
    description='Proposed daily local integrity poll at 09:00 America/New_York; not an official release cadence. Disabled until separately reviewed.',
)
