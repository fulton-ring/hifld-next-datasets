"""Reviewable local draft pipeline for Amtrak's BTS-distributed stations.

The explicit normal ingest uses local review staging and the shared #417
publication gate. A separate raw/candidate graph supports bounded replay. All
raw ArcGIS fields, including integer OBJECTID and string Code, survive unchanged.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import datetime
import json
import math
import platform
import re
import shutil
import subprocess
import time
import uuid
import urllib.error
import http.client
import urllib.parse
import urllib.request
from pathlib import Path
from email.utils import parsedate_to_datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster import (
    AssetKey, AssetSelection,
    DefaultScheduleStatus, RunRequest,
    ScheduleDefinition, SkipReason, asset, define_asset_job,
)
from pyproj import CRS
from shapely import Point, to_wkb

from dagster_hifld.partitions import SEMVER_VERSION_RE
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256

SLUG = 'amtrak-stations'
ITEM_ID = '1ed62a9f46304679aaa396bed4c8565a'
ITEM_URL = 'https://www.arcgis.com/sharing/rest/content/items/' + ITEM_ID + '?f=pjson'
SERVICE = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Amtrak_Stations/FeatureServer/0'
PROPOSED_SCHEMA_SHA256 = '5b45c633339cafafcc85d1d034581534da2b055611633a00b4aa60b3a85aa2fe'  # Review draft pin, not an approved contract.
_TYPES = {'esriFieldTypeOID': pa.int64(), 'esriFieldTypeInteger': pa.int32(),
          'esriFieldTypeDouble': pa.float64(), 'esriFieldTypeDate': pa.int64(),
          'esriFieldTypeString': pa.string()}
_OBSERVED_TIMESTAMP = re.compile(
    r'[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])T'
    r'(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,6})?'
    r'(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])'
)


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


def _arcgis_json(path: Path):
    """Replay uses the same application-error boundary as live extraction."""
    data = _json(path)
    if not isinstance(data, dict) or 'error' in data:
        raise ValueError(f'ArcGIS application error or nonobject response: {path.name}')
    return data


def _validate_observed_timestamp(value) -> None:
    """Validate original components; fromisoformat normalizes bad UTC minutes."""
    if not isinstance(value, str) or not _OBSERVED_TIMESTAMP.fullmatch(value):
        raise ValueError('Retrieval time must be an original ISO timestamp with valid timezone components')
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('Retrieval time must include timezone')


def _validated_retrieval_receipts(capture: Path, audit: dict) -> list[dict]:
    """Only validated canonical witnesses may make original retrieval claims."""
    pins = {record['name']: record for record in audit['raw_files']}
    result = []
    for name in audit['canonical_receipt_names']:
        path = capture / name
        if name not in pins or _digest(path) != pins[name]['sha256']:
            raise ValueError('Canonical retrieval receipt changed after validation')
        receipt = _json(path)
        _validate_observed_timestamp(receipt.get('observed_at'))
        http_date = receipt.get('date')
        if http_date is not None:
            if not isinstance(http_date, str):
                raise ValueError('Observed HTTP Date must retain its original string type')
            try:
                parsed = parsedate_to_datetime(http_date)
            except (TypeError, ValueError) as error:
                raise ValueError('Observed HTTP Date is invalid') from error
            if parsed.tzinfo is None:
                raise ValueError('Observed HTTP Date requires timezone')
        result.append({'name': name, 'url': receipt['url'], 'final_url': receipt['final_url'],
                       'observed_at': receipt['observed_at'], 'http_date': http_date})
    return result


def _require_review_staging(staging):
    if isinstance(staging, PublishedStorageResource):
        raise ValueError('Review draft refuses published storage before any capture or write')
    if not isinstance(staging, StagingStorageResource) or not staging.use_local or staging.bucket:
        raise ValueError('Review draft permits isolated local staging only')
    prefix = Path(staging.prefix)
    if prefix.is_absolute() or '..' in prefix.parts:
        raise ValueError('Review staging prefix must remain inside its local root')


def _project_identities(root: Path) -> dict:
    return {name: {'sha256': _digest(root / name)} if (root / name).is_file()
            else {'unavailable_reason': 'File is not present beside installed source'}
            for name in ('pyproject.toml', 'uv.lock')}


def processing_identity():
    pipeline = verifier_implementation_sha256()
    root = Path(__file__).resolve().parents[2]
    versions = {
        'python': platform.python_version(), 'python_implementation': platform.python_implementation(),
        'modules': {'pyarrow': pa.__version__, 'pyogrio': pyogrio.__version__,
                    'shapely': shapely.__version__, 'pyproj': pyproj.__version__},
        'native': {'gdal': str(pyogrio.__gdal_version__), 'geos': shapely.geos_version_string,
                   'proj': pyproj.proj_version_str},
        'effective_distributions': {name: importlib.metadata.version(name) for name in
                                    ('pyarrow', 'pyogrio', 'shapely', 'pyproj', 'numpy', 'dagster')},
        'installed_distribution_inventory': [list(item) for item in sorted({
            (distribution.metadata['Name'].lower().replace('_', '-'), distribution.version)
            for distribution in importlib.metadata.distributions() if distribution.metadata.get('Name')})],
        'project_identities': _project_identities(root),
    }
    fingerprint = hashlib.sha256(json.dumps({'installed_pipeline_sha256': pipeline,
        'versions': versions}, sort_keys=True).encode()).hexdigest()
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    commit = result.stdout.strip() if result.returncode == 0 else None
    dirty = subprocess.run(['git', '-C', str(root), 'status', '--porcelain', '--', 'src/dagster_hifld'], capture_output=True, text=True)
    return {'builder_fingerprint': fingerprint, 'installed_pipeline_sha256': pipeline,
        'code_commit': commit, 'working_tree_modified': bool(dirty.stdout.strip()), 'versions': versions,
        'meaning': 'Original processing identity; replay does not invent a new upstream retrieval.'}


def _census(count, identifiers):
    if type(count) is not int or not 0 < count <= 10_000:
        raise ValueError('Invalid/beyond-bounds count witness')
    if not isinstance(identifiers, list) or len(identifiers) != count:
        raise ValueError('ID witness cardinality differs from count')
    if any(type(value) is not int or not -(2**63) <= value < 2**63 for value in identifiers):
        raise ValueError('ID witnesses must be exact 64-bit integers')
    if len(set(identifiers)) != count:
        raise ValueError('Duplicate ID witness')
    return identifiers


def _raw_files(root):
    paths = list(root.glob('*.body')) + list(root.glob('*.receipt.json'))
    attempts = root / 'attempts'
    if attempts.exists(): paths.extend(p for p in attempts.rglob('*') if p.is_file())
    return sorted(set(paths))


def _preserve_for_review(capture, staging, version):
    _require_review_staging(staging)
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError('Explicit immutable semver version required')
    target = Path(staging.local_dir).resolve() / staging.prefix / 'quarantine' / SLUG / version / uuid.uuid4().hex
    target.mkdir(parents=True, exist_ok=False)
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
            receipt.update(status=response.status, date=response.headers.get('Date'),
                           final_url=response.geturl())
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


def capture_live(destination: Path, staging: StagingStorageResource) -> None:
    """Capture a bounded complete native-CRS feed, recording HTTP byte receipts.

    No API key or registered download is needed for this official BTS
    distribution. Existing captures are never overwritten.
    """
    _require_review_staging(staging)
    destination.mkdir(parents=True, exist_ok=False)

    def get(name: str, query: dict | None = None):
        url = SERVICE + ('/query?' + urllib.parse.urlencode(query) if query else '?f=pjson')
        return _capture_json(destination, name, url)

    _capture_json(destination, 'bts_item', ITEM_URL)
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
    _capture_json(destination, 'bts_item_after', ITEM_URL)
    inspect_capture(destination)


def inspect_capture(root: Path) -> dict:
    """Fail closed on changed census/schema/edit date, truncation, IDs or geometry."""
    # Check all terminal bodies before any of their fields can be interpreted.
    # Retained failed/partial attempts are evidence and are never candidate data.
    for path in sorted(root.glob('*.body')):
        _arcgis_json(path)
    item = _arcgis_json(root / 'bts_item.body')
    ending_item = _arcgis_json(root / 'bts_item_after.body')
    for current in (item, ending_item):
        if (current.get('id') != ITEM_ID or current.get('owner') != 'USDOT_BTS'
                or current.get('orgId') != 'xOi1kZaI0eWDREZv'
                or current.get('url') != SERVICE.rsplit('/', 1)[0]
                or current.get('access') != 'public'):
            raise ValueError('Producer/distribution identity changed or is not the pinned official item')
    if any(item.get(k) != ending_item.get(k) for k in ('modified', 'description', 'accessInformation', 'url')):
        raise ValueError('Source item changed during extraction')
    layer = _arcgis_json(root / 'bts_layer.body')
    after = _arcgis_json(root / 'bts_layer_after.body')
    stable_keys = ('fields', 'editingInfo', 'extent', 'geometryType', 'hasZ', 'hasM',
                   'serviceItemId', 'objectIdField')
    if any(layer.get(key) != after.get(key) for key in stable_keys):
        raise ValueError('Source changed during extraction')
    if layer.get('serviceItemId') != ITEM_ID:
        raise ValueError('Layer no longer belongs to official distribution item')
    if layer['geometryType'] != 'esriGeometryPoint' or layer.get('hasZ') or layer.get('hasM'):
        raise ValueError('Unsupported geometry dimensionality')
    if layer['extent']['spatialReference'] != {'wkid': 4326, 'latestWkid': 4326}:
        raise ValueError('Native source CRS drift')
    expected = _arcgis_json(root / 'bts_ids.body')['objectIds']
    ending = _arcgis_json(root / 'bts_ids_after.body')['objectIds']
    count = _arcgis_json(root / 'bts_count.body')['count']
    final_count = _arcgis_json(root / 'bts_count_after.body')['count']
    _census(count, expected)
    _census(final_count, ending)
    if count != final_count or set(expected) != set(ending):
        raise ValueError('Changed or incomplete source ID census')
    paths = sorted(root.glob('page-*.body'))
    expected_pages = (count + 1999) // 2000
    if [p.name for p in paths] != [f'page-{n:03d}.body' for n in range(expected_pages)]:
        raise ValueError('Missing or extra page')
    required_bodies = {p.name for p in paths} | {
        name + '.body' for name in ('bts_item', 'bts_item_after', 'bts_layer', 'bts_layer_after',
                                   'bts_ids', 'bts_ids_after', 'bts_count', 'bts_count_after')}
    if {p.name for p in root.glob('*.body')} != required_bodies:
        raise ValueError('Missing or extra terminal source response')
    fields = {f['name']: f for f in layer['fields']}
    if len(fields) != len(layer['fields']) or any(f['type'] not in _TYPES for f in fields.values()):
        raise ValueError('Unsupported or duplicate schema field')
    oid_field = layer['objectIdField']
    if oid_field != 'OBJECTID' or 'Code' not in fields:
        raise ValueError('Missing or renamed station identifiers')
    seen, station_ids, files = set(), set(), []
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    for path in paths:
        page = _arcgis_json(path)
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
            if set(geom or {}) != {'x', 'y'} or not all(type(geom[k]) in (int, float) and math.isfinite(geom[k]) for k in ('x', 'y')):
                raise ValueError('Missing/invalid point geometry')
            if not -180 <= geom['x'] <= 180 or not -90 <= geom['y'] <= 90:
                raise ValueError('Point outside native geographic domain')
            for name, value in attrs.items():
                field = fields[name]
                if value is None:
                    if not field['nullable']:
                        raise ValueError('Null non-nullable field')
                    continue
                kind = field['type']
                if kind == 'esriFieldTypeString' and not isinstance(value, str):
                    raise ValueError('String type loss')
                if kind == 'esriFieldTypeString' and field.get('length') is not None and len(value) > field['length']:
                    raise ValueError('String exceeds the pinned source physical declaration')
                if kind in ('esriFieldTypeOID', 'esriFieldTypeInteger', 'esriFieldTypeDate') and type(value) is not int:
                    raise ValueError('Integer/date type loss')
                if kind in ('esriFieldTypeOID', 'esriFieldTypeDate') and not -(2**63) <= value < 2**63:
                    raise ValueError('64-bit integer overflow')
                if kind == 'esriFieldTypeInteger' and not -(2**31) <= value < 2**31:
                    raise ValueError('32-bit integer overflow')
                if kind == 'esriFieldTypeDouble' and (type(value) not in (int, float) or not math.isfinite(value)):
                    raise ValueError('Invalid double')
                if kind == 'esriFieldTypeDouble' and type(value) is int and int(float(value)) != value:
                    raise ValueError('Native numeric value would round in the declared double representation')
            oid, station_id = attrs['OBJECTID'], attrs['Code']
            if oid in seen or not isinstance(station_id, str) or not station_id or station_id in station_ids:
                raise ValueError('Duplicate or missing raw ID')
            seen.add(oid); station_ids.add(station_id)
            bounds = [min(bounds[0], geom['x']), min(bounds[1], geom['y']), max(bounds[2], geom['x']), max(bounds[3], geom['y'])]
        files.append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)})
    if seen != set(expected):
        raise ValueError('Full extraction does not equal source census')
    for name in ['bts_item','bts_item_after','bts_layer','bts_layer_after','bts_ids','bts_ids_after','bts_count','bts_count_after']:
        path = root / f'{name}.body'
        files.append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)})
    receipts = []
    for record in files:
        receipt_path = root / record['name'].replace('.body', '.receipt.json')
        receipt = _json(receipt_path)
        if receipt.get('complete_body') is not True or receipt.get('status') != 200 or receipt.get('sha256') != record['sha256'] or receipt.get('bytes') != record['bytes']:
            raise ValueError('HTTP receipt/checksum mismatch')
        name = record['name'].removesuffix('.body')
        if name.startswith('bts_item'):
            expected_url = ITEM_URL
        elif name.startswith('bts_layer'):
            expected_url = SERVICE + '?f=pjson'
        else:
            query = {'f': 'json', 'where': '1=1'}
            if name.startswith('bts_count'):
                query['returnCountOnly'] = 'true'
            elif name.startswith('bts_ids'):
                query['returnIdsOnly'] = 'true'
            else:
                index = int(name.removeprefix('page-'))
                group = sorted(expected)[index * 2000:(index + 1) * 2000]
                query = {'f': 'json', 'where': f'OBJECTID >= {group[0]} AND OBJECTID <= {group[-1]}',
                         'outFields': '*', 'returnGeometry': 'true', 'orderByFields': 'OBJECTID ASC', 'resultRecordCount': 2000}
            expected_url = SERVICE + '/query?' + urllib.parse.urlencode(query)
        if receipt.get('url') != expected_url:
            raise ValueError('HTTP receipt does not name the pinned complete-extraction request')
        final = urllib.parse.urlsplit(receipt.get('final_url', ''))
        requested = urllib.parse.urlsplit(expected_url)
        if ((final.scheme, final.netloc, final.path, final.fragment) !=
                (requested.scheme, requested.netloc, requested.path, requested.fragment)
                or sorted(urllib.parse.parse_qsl(final.query, keep_blank_values=True)) !=
                sorted(urllib.parse.parse_qsl(requested.query, keep_blank_values=True))):
            raise ValueError('Observed final response URL is missing or left the exact official request')
        _validate_observed_timestamp(receipt.get('observed_at'))
        receipts.append({'name': receipt_path.name, 'bytes': receipt_path.stat().st_size, 'sha256': _digest(receipt_path)})
    files.extend(receipts)
    canonical_receipt_names = sorted(record['name'] for record in receipts)
    untrusted_diagnostics = []
    for path in sorted(root.glob('*.receipt.json')):
        if path.name not in canonical_receipt_names:
            record = {'name': path.name, 'bytes': path.stat().st_size, 'sha256': _digest(path)}
            files.append(record)
            untrusted_diagnostics.append({**record, 'classification': 'untrusted_diagnostic',
                'used_for_source_or_retrieval_claims': False,
                'meaning': 'Unrelated receipt bytes preserved only as diagnostics; contents are not trusted or interpreted'})
    attempts_dir = root / 'attempts'
    if attempts_dir.exists():
        files.extend({'name': str(path.relative_to(root)), 'bytes': path.stat().st_size, 'sha256': _digest(path)}
            for path in sorted(attempts_dir.rglob('*')) if path.is_file())
    return {'rows': len(seen), 'station_ids': len(station_ids), 'pages': len(paths), 'bounds': bounds,
            'source': SERVICE, 'official_item': item, 'editing_info': layer['editingInfo'], 'fields': layer['fields'],
            'crs': layer['extent']['spatialReference'], 'raw_files': files,
            'canonical_receipt_names': canonical_receipt_names, 'untrusted_diagnostics': untrusted_diagnostics,
            'acceptance': 'unapproved_draft', 'promotion_allowed': False}


def build_candidate(capture: Path, destination: Path) -> dict:
    """Build a typed native-CRS candidate in a raw/code-bound immutable namespace."""
    audit = inspect_capture(capture)
    processing = processing_identity()
    fields = audit['fields']
    schema = pa.schema([pa.field(f['name'], _TYPES[f['type']], nullable=f['nullable']) for f in fields] + [pa.field('geometry', pa.binary(), nullable=False)])
    crs = CRS.from_epsg(audit['crs']['latestWkid']).to_json_dict()
    schema = schema.with_metadata({b'geo': json.dumps({'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {'encoding': 'WKB', 'geometry_types': ['Point'], 'crs': crs, 'bbox': audit['bounds']}}}, sort_keys=True).encode()})
    semantics = {'raw_files': audit['raw_files'], 'builder_fingerprint': processing['builder_fingerprint'],
                 'proposed_schema_sha256': PROPOSED_SCHEMA_SHA256, 'mapping': 'all raw named fields unchanged; native Point WKB'}
    identity = hashlib.sha256(json.dumps(semantics, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / 'candidate.parquet'
    provenance = destination / 'provenance.json'
    obj = destination / 'objects' / identity / 'candidate.parquet'
    previous = None
    if target.exists():
        previous = _json(provenance)
        original_processing = previous.get('processing', {})
        # Replay retains original processing attribution if the installed builder is unchanged.
        if original_processing.get('builder_fingerprint') != processing['builder_fingerprint']:
            raise FileExistsError('Immutable candidate version collision')
        processing = {**processing, 'code_commit': original_processing.get('code_commit'),
                      'working_tree_modified': original_processing.get('working_tree_modified')}
        if not obj.is_file() or _digest(obj) != _digest(target):
            raise FileExistsError('Immutable candidate object missing or modified')
    else:
        obj.parent.mkdir(parents=True, exist_ok=False)
        temporary = obj.with_suffix('.parquet.partial')
        with pq.ParquetWriter(temporary, schema, compression='zstd') as writer:
            for path in sorted(capture.glob('page-*.body')):
                rows = [{**f['attributes'], 'geometry': to_wkb(Point(f['geometry']['x'], f['geometry']['y']), byte_order=1)}
                        for f in _json(path)['features']]
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
        temporary.replace(obj)
        with obj.open('rb') as source, target.open('xb') as output:
            shutil.copyfileobj(source, output)
    audit.update(processing=processing, candidate_namespace='objects/' + identity,
                 original_retrieval_receipts=_validated_retrieval_receipts(capture, audit),
                 producer='Amtrak', distributor='USDOT_BTS official NTAD distribution',
                 candidate_sha256=_digest(target), candidate_bytes=target.stat().st_size,
                 proposed_mapping='identity for every raw field; string Code and date text remain unchanged; native EPSG:4326 geometry',
                 schema=[{'name': f.name, 'type': str(f.type), 'nullable': f.nullable} for f in schema])
    if previous is not None:
        if previous != audit:
            raise FileExistsError('Immutable candidate provenance or input version collision')
        return previous
    with provenance.open('x') as output:
        output.write(json.dumps(audit, sort_keys=True, indent=2) + '\n')
    return audit


def verify_typed_geopackage(candidate: Path, bridge: Path) -> dict:
    """Recheck every field, physical type, nullability, value and native point."""
    parquet = pq.ParquetFile(candidate)
    rows = 0
    with pyogrio.open_arrow(bridge, layer=SLUG, use_pyarrow=True, batch_size=2000) as (metadata, reader):
        if CRS.from_user_input(metadata['crs']) != CRS.from_epsg(4326):
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
        'internal_driver_fid': 'GeoPackage format machinery only; original integer OBJECTID and string Code retained as separate named columns',
        'sha256': _digest(bridge)}


def write_typed_geopackage(candidate: Path, destination: Path, source_edit_ms: int) -> None:
    """Arrow stream bridge avoids GeoPandas nullable-integer coercion.

    GeoPackage's internal fid is format machinery, not a replacement source ID.
    Original OBJECTID/Code columns remain present with declared integer widths and
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
            geometry_name='geometry', geometry_type='Point', crs=CRS.from_epsg(4326).to_wkt())
    finally:
        pyogrio.set_gdal_config_options({'OGR_CURRENT_DATE': previous})
    verify_typed_geopackage(candidate, temporary)
    temporary.replace(destination)


def _provenance_inputs(capture: Path, audit: dict) -> tuple[list[dict], list[dict]]:
    """Describe each observed retained HTTP body without inventing a retrieval.

    Terminal copies and attempt originals have separate raw paths and identical
    response pins when they are aliases. Failed attempts remain evidence, never
    data used to construct the candidate. A pre-response transport failure has no
    observed HTTP body and is recorded separately rather than assigned a URL.
    """
    item = audit['official_item']
    license_text = item.get('licenseInfo')
    attribution = item.get('accessInformation')
    if not isinstance(license_text, str) or not license_text.strip() or not isinstance(attribution, str) or not attribution.strip():
        raise ValueError('Captured official item does not establish license and attribution provenance')
    inputs, unobserved = [], []
    for record in sorted(audit['raw_files'], key=lambda value: value['name']):
        if not record['name'].endswith('.body'):
            continue
        path = Path(record['name'])
        receipt_name = str(path.with_suffix('.receipt.json'))
        receipt = _json(capture / receipt_name)
        if (not isinstance(receipt, dict) or receipt.get('sha256') != record['sha256']
                or type(receipt.get('bytes')) is not int or receipt['bytes'] != record['bytes']):
            raise ValueError('Retained response attempt receipt/checksum mismatch')
        _validate_observed_timestamp(receipt.get('observed_at'))
        terminal_name = path.parts[1] if path.parts[0] == 'attempts' else path.stem
        terminal = _json(capture / (terminal_name + '.receipt.json'))
        if receipt.get('url') != terminal['url']:
            raise ValueError('Retained response lacks exact observed official request provenance')
        if type(receipt.get('status')) is not int:
            raise ValueError('Retained response lacks an observed HTTP status')
        if receipt.get('status') == 0 and record['bytes'] == 0 and not receipt.get('final_url'):
            unobserved.append({**record, 'receipt': receipt_name,
                               'meaning': 'Transport failed before an observed HTTP response; no retrieval URL invented'})
            continue
        if type(receipt.get('status')) is not int or not 100 <= receipt['status'] <= 599:
            raise ValueError('Retained response lacks an observed HTTP status')
        requested = urllib.parse.urlsplit(terminal['url'])
        observed = urllib.parse.urlsplit(receipt.get('final_url', ''))
        if (receipt.get('url') != terminal['url'] or
                (observed.scheme, observed.netloc, observed.path, observed.fragment) !=
                (requested.scheme, requested.netloc, requested.path, requested.fragment) or
                sorted(urllib.parse.parse_qsl(observed.query, keep_blank_values=True)) !=
                sorted(urllib.parse.parse_qsl(requested.query, keep_blank_values=True))):
            raise ValueError('Retained response lacks exact observed official request provenance')
        inputs.append({
            'input_id': 'raw_arcgis/' + record['name'],
            'producer': 'National Railroad Passenger Corporation (Amtrak)',
            'distributor': 'Bureau of Transportation Statistics (USDOT_BTS)',
            'host': observed.hostname, 'role': 'official_distributor',
            'canonical_url': receipt['url'], 'retrieval_url': receipt['final_url'],
            'retrieved_at': receipt['observed_at'], 'sha256': record['sha256'], 'bytes': record['bytes'],
            'license': license_text, 'attribution': attribution,
            'delegation_evidence': {'official_item': ITEM_URL, 'captured_item_sha256': _digest(capture / 'bts_item.body'),
                                    'published_producer_distributor_attribution': attribution,
                                    'meaning': 'Observed NTAD attribution; no dataset or release acceptance implied'},
            'release_date_unknown_reason': 'This individual HTTP response is an extraction/metadata witness, not an independently versioned producer release. The parent official source population statement is preserved separately.',
            'raw_path': 'raw_arcgis/' + record['name'], 'receipt_path': 'raw_arcgis/' + receipt_name,
            'receipt_sha256': _digest(capture / receipt_name), 'http_status': receipt['status'],
            'complete_body': receipt.get('complete_body') is True,
            'use': 'terminal_candidate_input' if path.parts[0] != 'attempts' else 'retained_response_attempt_evidence',
            'acceptance': 'unapproved_draft',
        })
    return inputs, unobserved


def _source_manifest(audit: dict, processing: dict, original_receipts: list, inputs: list, unobserved_attempts: list) -> dict:
    """Construct and validate full provenance before a version can be discovered."""
    manifest = {'title':'Amtrak Stations', 'agency':'National Railroad Passenger Corporation (Amtrak)',
        'publisher':'Bureau of Transportation Statistics (USDOT_BTS; official distributor)',
        'source_url':SERVICE, 'source_modified':audit['editing_info'], 'source_release_statement':audit['official_item']['description'], 'source_attribution':audit['official_item'].get('accessInformation'), 'schema_version':1,
        'processor':'HIFLD Next', 'processing_identity':processing, 'code_commit':processing['code_commit'],
        'original_retrieval_receipts':original_receipts,
        'inputs':inputs,
        'transformations':[{'name':'raw ArcGIS point to native-CRS GeoJSON + typed GeoParquet + Arrow-stream typed GeoPackage', 'attribute_mapping':'identity; no omissions; original DateModif text and native coordinate values retained', 'acceptance':'unapproved'}],
        'lineage':{'producer':'Amtrak', 'distributor_item':ITEM_ID,'crosswalk':'unverified; DataLumos403 prevents inspecting the mapping',
                   'raw_files':audit['raw_files'], 'source_crs':audit['crs'], 'source_fields':audit['fields'],
                   'unobserved_transport_attempts':unobserved_attempts,
                   'untrusted_diagnostics':audit['untrusted_diagnostics']},
        'promotion_allowed':False, 'complete_rows':audit['rows'],
        'unsupported_output_formats':{'shapefile_zip':'Unapproved lossy format: source fields include >10-character names and nullable string/identifier fields; regenerated DBF needs a reviewed mapping; no reviewed lossless name/type/null/value mapping.'}}
    # Structural correctness is implementation behavior, not an acceptance decision.
    validate_release_provenance(manifest)
    return manifest


def _stage_capture_payload(capture: Path, staging: StagingStorageResource, version: str) -> dict:
    """Stage native raw proof + lossless typed draft and canonical GeoJSON input.

    No publication mapping is applied. Generic downstream conversions remain
    draft candidates behind the #417 release boundary and require type review.
    """
    _require_review_staging(staging)
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError('Explicit immutable semver version required')
    audit = inspect_capture(capture)
    signature = audit['fields']
    schema_digest = hashlib.sha256(json.dumps(signature, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if schema_digest != PROPOSED_SCHEMA_SHA256:
        raise ValueError('Source schema drift quarantined; revise the review draft explicitly')
    # Raw quarantine is already durable. No canonical version directory exists
    # until every retained attempt and the complete manifest pass validation.
    inputs, unobserved_attempts = _provenance_inputs(capture, audit)
    original_receipts = _validated_retrieval_receipts(capture, audit)
    _source_manifest(audit, processing_identity(), original_receipts, inputs, unobserved_attempts)
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
        output.write('{"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:4326"}},"features":[')
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
    manifest = _source_manifest(audit, candidate['processing'], candidate['original_retrieval_receipts'], inputs, unobserved_attempts)
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2)+'\n').encode()
    manifest_path = metadata / 'source_manifest.json'
    if manifest_path.exists() and manifest_path.read_bytes() != manifest_bytes:
        raise ValueError('Immutable source manifest collision')
    if not manifest_path.exists(): manifest_path.write_bytes(manifest_bytes)
    return {'version': version, 'rows':audit['rows'], 'source':SERVICE, 'candidate_sha256':candidate['candidate_sha256'], 'promotion_allowed':False}


def stage_amtrak_capture(capture: Path, staging: StagingStorageResource, version: str) -> dict:
    _require_review_staging(staging)
    retained = _preserve_for_review(capture, staging, version)
    try:
        result = _stage_capture_payload(capture, staging, version)
    except Exception as error:
        (retained / 'status.json').write_text(json.dumps({'state': 'blocked', 'version': version,
            'reason': f'{type(error).__name__}: {error}', 'raw_preserved': True, 'approval': None}, sort_keys=True))
        raise
    (retained / 'status.json').write_text(json.dumps({'state': 'complete_unapproved_draft',
        'version': version, 'raw_preserved': True, 'approval': None}, sort_keys=True))
    return result


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='bts', compute_kind='http',
       config_schema={'capture_dir':str, 'version':str},
       description='Draft Amtrak feed via official USDOT_BTS distributor; all raw fields and IDs retained. Local research staging only; publication requires independent exact-release approval.')
def bts_amtrak_stations(context, staging_storage: StagingStorageResource) -> dict:
    _require_review_staging(staging_storage)
    capture_dir = context.op_config['capture_dir']
    if capture_dir:
        result = stage_amtrak_capture(Path(capture_dir), staging_storage, context.op_config['version'])
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
            capture_live(capture, staging_storage)
            result = stage_amtrak_capture(capture, staging_storage, version)
        except Exception as error:
            state.write_text(json.dumps({'state': 'blocked', 'version': version,
                'reason': f'{type(error).__name__}: {error}', 'raw_preserved': True, 'approval': None}, sort_keys=True))
            raise
        state.write_text(json.dumps({'state': 'complete_unapproved_draft', 'version': version, 'raw_preserved': True, 'approval': None}))
    context.add_output_metadata({'rows':result['rows'], 'version':result['version'], 'promotion_allowed':False})
    return result


amtrak_draft_refresh_job = define_asset_job(
    name='amtrak_draft_refresh_job',
    selection=AssetSelection.assets(bts_amtrak_stations),
    tags={'hifld_review_draft': 'true', 'hifld_source_only': 'true'},
    description='Local source-only integrity poll; no catalog/publish selection or release approval.',
)


def _draft_refresh_request(context):
    try:
        _require_review_staging(StagingStorageResource.from_env())
        _require_review_staging(context.resources.staging_storage)
    except ValueError as error:
        return SkipReason(str(error))
    scheduled = context.scheduled_execution_time
    if scheduled is None:
        return SkipReason('A dated schedule context is required for immutable draft versions')
    stamp = scheduled.astimezone(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return RunRequest(
        run_key=f'{SLUG}-draft-{stamp}',
        run_config={'ops': {bts_amtrak_stations.op.name: {
            'config': {'capture_dir': '', 'version': f'v0.0.0-research-{stamp}'}}}},
        tags={'hifld_review_draft': 'true', 'hifld_source_only': 'true',
              'hifld_promotion_allowed': 'false'},
    )


amtrak_draft_refresh_schedule = ScheduleDefinition(
    name='amtrak_draft_refresh_schedule',
    job=amtrak_draft_refresh_job,
    cron_schedule='0 8 * * 1',
    execution_timezone='America/New_York',
    execution_fn=_draft_refresh_request,
    required_resource_keys={'staging_storage'},
    default_status=DefaultScheduleStatus.STOPPED,
    description='Proposed weekly local integrity poll Monday at 08:00 America/New_York; not an official release cadence. Disabled until separately reviewed.',
)
