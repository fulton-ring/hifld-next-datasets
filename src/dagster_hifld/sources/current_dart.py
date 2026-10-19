"""Complete NCEI current DART layer draft, without source or ID acceptance.

NOAA's published hazards map delegates to this exact hosted layer. Every source
column, both OID populations, native coordinates and original responses survive.
Pinned drift quarantines; historical published IDs remain separately preserved.
"""
from __future__ import annotations

import collections
import hashlib
import importlib.metadata
import io
import fcntl
import os
import json
import platform
import math
from urllib.parse import urlsplit
import re
import sqlite3
import stat
import struct
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from contextlib import contextmanager, nullcontext
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
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file

FIDELITY_FILES = ('baseline_comparison.json','baseline_comparison.md','baseline_differences.jsonl','baseline_field_changes.jsonl')
SLUG = 'current-dart-deployments'
URL = 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0'
LANDING = 'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure'
URLS = {'ncei_dart': 'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure', 'ncei_map': 'https://www.ncei.noaa.gov/maps/hazards/?layers=4', 'map_store': 'https://www.ncei.noaa.gov/maps/hazards/assets/store-CeovILYQ.js', 'webmap': 'https://www.arcgis.com/sharing/rest/content/items/5cb326668189408e8c4c20ddd92aa2ed/data?f=pjson', 'item': 'https://www.arcgis.com/sharing/rest/content/items/764d3a99a4f8440ea05522815b5f5e19?f=pjson', 'layer': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0?f=pjson', 'count_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json', 'features': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=true&orderByFields=OBJECTID&f=json', 'ids_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json', 'count_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json'}
URLS['map_js'] = 'https://www.ncei.noaa.gov/maps/hazards/assets/index-Cyvq2L9n.js'
ARCHIVE_NAME = 'current-dart-original-features.json'
PIN = '00d93ba1dcc546476c11efdf71b1f0b6afd131325ba3ca3e32675285120d72f6'
ARCHIVE_BYTES = 16613
MAX_BYTES = 3_000_000
MEMBERS = frozenset()
NATIVE_FIELDS = [{'name': 'STATION', 'type': 'esriFieldTypeInteger', 'alias': 'Deployment ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Description', 'sqlType': 'sqlTypeOther', 'length': 80, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEPLOYED', 'type': 'esriFieldTypeString', 'alias': 'Deployed', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'DATA_URL', 'type': 'esriFieldTypeString', 'alias': 'Data URL', 'sqlType': 'sqlTypeOther', 'length': 90, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}]
NATIVE_SCHEMA = {'STATION': 'int32', 'DESCRIPTION': 'str:80', 'DEPLOYED': 'str:1', 'OBJECTID': 'int32', 'DATA_URL': 'str:90', 'OID': 'int32'}
SCHEMA = NATIVE_SCHEMA.copy()
PROPOSAL = {'approved':False,'id_fields':['STATION','OBJECTID','OID'],'geometry':True,
 'coverage_fields':['DEPLOYED'],'crs':'EPSG:3857',
 'columns':{name:{'type':'integer' if kind.startswith('int') else 'string','nullable':name!='OBJECTID'} for name,kind in SCHEMA.items()}}



class DartQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    if len(body) > MAX_BYTES:
        raise DartQuarantineError('response_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise DartQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise DartQuarantineError('nonobject_or_error_json')
    return result


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise DartQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise DartQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('url') != url or receipt.get('final_url') != url
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise DartQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp=receipt.get('fetched_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise DartQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    moment = datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        raise DartQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    require_local(storage)
    captures = {}
    for name,url in URLS.items():
        if capture_dir is not None:
            body = b''; receipt_body=b''; receipt = {'capture_path':str(capture_dir),'url':url}
            try:
                with (capture_dir / (name + '.body')).open('rb') as f: body = f.read(MAX_BYTES + 1)
                receipt_body=(capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body',receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body,receipt,url)
                if len(body)>MAX_BYTES: raise DartQuarantineError('capture_size_limit')
            except (OSError,ValueError,TypeError) as exc:
                location=quarantine(storage,body,receipt,str(exc))
                immutable(storage,f'{location}/original-receipt.body',receipt_body)
                raise DartQuarantineError(f'{exc}; retained:{location}') from exc
        else:
            for attempt in range(3):
                body=b'';receipt={'url':url,'fetched_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                try:
                    with httpx.Client(follow_redirects=False,timeout=45,headers={'Accept-Encoding':'identity'}) as client:
                        with client.stream('GET',url) as response:
                            receipt.update(final_url=str(response.url),status=response.status_code,
                                content_encoding=response.headers.get('content-encoding','identity'),
                                declared_wire_length=response.headers.get('content-length'))
                            for block in response.iter_bytes(chunk_size=65536):
                                body+=block
                                receipt['wire_bytes']=getattr(response,'num_bytes_downloaded',None)
                                if len(body)>MAX_BYTES: raise DartQuarantineError('response_size_limit')
                            receipt.update(bytes=len(body),sha256=sha(body))
                            validate_receipt(body,receipt,url)
                            receipt['wire_bytes']=getattr(response,'num_bytes_downloaded',None)
                            if (response.headers.get('content-length') is not None
                                    and int(response.headers['content-length'])!=receipt['wire_bytes']):
                                raise DartQuarantineError('incomplete_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(bytes=len(body),sha256=sha(body),partial=True)
                    location=quarantine(storage,body,receipt,str(exc))
                    if attempt==2: raise DartQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
        captures[name]=(body,receipt)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',body)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',encode(receipt))
    return captures


def provenance(captures: dict) -> dict:
    for name,url in URLS.items(): validate_receipt(*captures[name],url)
    if b'src="./assets/index-Cyvq2L9n.js"' not in captures['ncei_map'][0] or b'from"./store-CeovILYQ.js"' not in captures['map_js'][0]:
        raise DartQuarantineError('official_current_map_entry_or_store_import_changed')
    if b'5cb326668189408e8c4c20ddd92aa2ed' not in captures['map_store'][0]:
        raise DartQuarantineError('official_noaa_map_webmap_delegation_changed')
    webmap=strict_json(captures['webmap'][0])
    def find(items):
        for item in items:
            if item.get('id')==SLUG: return item
            nested=find(item.get('layers',[]))
            if nested: return nested
    delegated=find(webmap.get('operationalLayers',[]))
    if not delegated or delegated.get('url')!=URL or delegated.get('itemId')!='764d3a99a4f8440ea05522815b5f5e19':
        raise DartQuarantineError('official_noaa_webmap_layer_delegation_changed')
    item=strict_json(captures['item'][0])
    if item.get('owner')!='ncei_noaa' or item.get('url')!=URL.rsplit('/',1)[0]:
        raise DartQuarantineError('producer_account_or_service_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'NOAA National Centers for Environmental Information',
            'host':urlsplit(URLS[name]).hostname,
            'role':'producer_publication' if 'noaa.gov' in URLS[name] else 'official_distributor',
            'canonical_url':URLS[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['fetched_at'],'release_date':None,
            'release_date_unknown_reason':'No dated data release is declared; layer editingInfo and portal modified timestamps are operational metadata, not release dates.',
            'sha256':sha(body),'bytes':len(body),'license':'https://www.noaa.gov/disclaimer',
            'attribution':'NOAA NCEI / PMEL / NDBC',
            'lineage':'NOAA-hosted hazards map JavaScript selects webmap 5cb326668189408e8c4c20ddd92aa2ed; its exact current-dart-deployments layer delegates to ncei_noaa item764d3a99a4f8440ea05522815b5f5e19 layer0. No source, ID or contract acceptance.',
            'authority_evidence':URLS['ncei_map'],'distributor':'Esri ArcGIS Online for NOAA ncei_noaa','delegation_evidence':{'producer_map_url':URLS['ncei_map'],'producer_map_config_url':URLS['map_store'],'webmap_url':URLS['webmap']}})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Current DART Deployments — working draft',
        'publisher':'NOAA NCEI (ncei_noaa hosted service)','agency':'National Oceanic and Atmospheric Administration',
        'source_url':LANDING,'license':'https://www.noaa.gov/disclaimer','inputs':inputs,
        'transformations':[{'operation':'decode_native_esri_point_json','preserved':'all six original source attributes including STATION, OBJECTID and OID; declared int32 source columns, original EPSG3857 XY coordinates and every raw response','repair_or_projection':False,'mapping_approval':'not established'}],
        'unsupported_output_formats':{'shapefile_zip':'Regenerated DBF/index files and proposed ID mapping require acceptance; original JSON inputs retained losslessly.',
            'pmtiles':'Reprojection and quantization would alter native coordinates; visualization mapping unapproved.'}}
    validate_release_provenance(manifest)
    return manifest


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


def inspect_archive(body: bytes, directory: Path) -> tuple[list[dict],dict,dict]:
    if len(body)!=ARCHIVE_BYTES or sha(body)!=PIN:
        raise DartQuarantineError('source_drift_requires_review_new_pinned_capture')
    data=strict_json(body)
    fields=data.get('fields')
    normalized=[{k:f.get(k) for k in ('name','type','length') if k in f} for f in fields or []]
    expected=[{k:f.get(k) for k in ('name','type','length') if k in f} for f in NATIVE_FIELDS]
    if normalized!=expected or data.get('geometryType')!='esriGeometryPoint' or data.get('spatialReference')!={'wkid':102100,'latestWkid':3857} or data.get('exceededTransferLimit'):
        raise DartQuarantineError('schema_crs_geometry_or_transfer_limit_drift')
    rows=[]
    for feature in data.get('features',[]):
        props=feature.get('attributes',{});g=feature.get('geometry',{})
        if set(props)!=set(SCHEMA) or set(g)!={'x','y'}:
            raise DartQuarantineError('missing_or_extra_columns_or_point_dimensions')
        for key,value in props.items():
            kind=SCHEMA[key]
            if value is None:
                raise DartQuarantineError('new_null_requires_review')
            if kind.startswith('int'):
                if type(value) is not int or not -(2**31)<=value<2**31: raise DartQuarantineError('source_integer_type_width_drift')
            elif type(value) is not str or len(value)>int(kind.split(':')[1]): raise DartQuarantineError('source_string_type_width_drift')
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in g.values()):
            raise DartQuarantineError('invalid_point_coordinate')
        rows.append({'type':'Feature','properties':props,'geometry':{'type':'Point','coordinates':(g['x'],g['y'])}})
    rows.sort(key=lambda r:r['properties']['OBJECTID'])
    if len(rows)!=62 or any(len({r['properties'][k] for r in rows})!=len(rows) for k in ('STATION','OBJECTID','OID')):
        raise DartQuarantineError('source_population_or_identifier_drift')
    geometries=[shape(r['geometry']) for r in rows]
    if any(g.is_empty or not g.is_valid for g in geometries): raise DartQuarantineError('invalid_native_geometry')
    return rows,{'approved':False,'status':'producer_source_fidelity_pass','feature_count':len(rows),
        'schema':{'properties':NATIVE_SCHEMA,'geometry':'Point'},'source_field_declarations':NATIVE_FIELDS,
        'crs':'EPSG:3857','bounds':shapely.total_bounds(geometries).tolist(),
        'invalid_geometry_count':0,'nulls':{k:sum(r['properties'][k] is None for r in rows) for k in SCHEMA},
        'identifiers':{k:{'unique':len(rows),'nulls':0} for k in ('STATION','OBJECTID','OID')},
        'coverage':{'DEPLOYED':dict(collections.Counter(r['properties']['DEPLOYED'] for r in rows))},
        'declared_scope':'Complete producer current DART layer includes Y deployed and P planned sites worldwide; not restricted to US currently operational buoys.'},{}


def validate_complete_bundle(captures: dict) -> None:
    layer=strict_json(captures['layer'][0]);data=strict_json(captures['features'][0])
    if layer.get('fields')!=NATIVE_FIELDS or layer.get('name')!='Current DART Deployments' or layer.get('geometryType')!='esriGeometryPoint':
        raise DartQuarantineError('native_layer_metadata_drift')
    ids=[f['attributes']['OBJECTID'] for f in data.get('features',[])]
    for name in ('ids_before','ids_after'):
        observed=strict_json(captures[name][0])
        if observed.get('objectIdFieldName')!='OBJECTID' or len(observed.get('objectIds',[]))!=len(ids) or set(observed['objectIds'])!=set(ids):
            raise DartQuarantineError('incomplete_or_concurrently_changed_id_population')
    for name in ('count_before','count_after'):
        if strict_json(captures[name][0]).get('count')!=len(ids): raise DartQuarantineError('incomplete_or_concurrently_changed_count')


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int32() if v.startswith('int32') else pa.float64() if v.startswith('float') else pa.string(),nullable=k!='OBJECTID',metadata={b'esri_type':next(f['type'] for f in NATIVE_FIELDS if f['name']==k).encode()}) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(3857).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise DartQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    for part in target.relative_to(root).parts:
        current=current/part
        if current.is_symlink(): raise DartQuarantineError('linked_candidate_path')
    return target




def unapproved_baseline_comparison() -> dict:
    return {'schema_version':1,'status':'blocked','approved':False,
        'reason_codes':['unapproved_baseline_and_source_published_id_schema_crs_mapping'],
        'acceptance':'not_established'}


def fidelity_wrapper(report: dict) -> bytes:
    return encode({'canonical_full_records_byte_identical':True,'shared_verifier':report,
        'retained_shared_report_prefix':'metadata/source-fidelity',
        'known_invalid_native_geometry_release_blocked':False})


def expected_fidelity_payloads(rows: list[dict], directory: Path) -> dict[str,bytes]:
    # Actual native rows and typed output are reconciled independently before
    # using this exact original-to-original report as the expected full evidence.
    original=directory/'source.jsonl'
    original.write_bytes(b''.join(encode(row) for row in rows))
    replay=directory/'independent-replay.jsonl'
    replay.write_bytes(original.read_bytes())
    report=compare_derivative(original,replay,directory/'comparison')
    if report.get('status')!='pass':
        raise DartQuarantineError('original_source_evidence_replay_not_passed')
    return {'metadata/geopackage_source_fidelity.json':fidelity_wrapper(report),
        'metadata/parquet_source_fidelity.json':fidelity_wrapper(report),
        'metadata/baseline_comparison.json':encode(unapproved_baseline_comparison()),
        **{f'metadata/source-fidelity/{name}':(directory/'comparison'/name).read_bytes()
           for name in FIDELITY_FILES}}


def write_canonical_geopackage(destination: Path, rows: list[dict]) -> None:
    """Deterministic native container; no coordinate, ID, field or type changes."""
    with fiona.open(destination, 'w', driver='GPKG', layer=SLUG, crs='EPSG:3857',
                    schema={'properties': SCHEMA, 'geometry': 'Point'}) as stream:
        stream.writerecords(rows)
    with sqlite3.connect(destination) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")


def native_prebuilt_policy(body: bytes) -> dict:
    return {'schema_version':1,'formats':{'geoparquet':{
        'reason':'Preserve source Esri int32 physical widths, native CRS and full typed metadata; no inferred widening or extra bbox schema accepted.',
        'objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':sha(body),'bytes':len(body)}]}}}


def original_provenance_bundle(storage: StagingStorageResource, version: str,
                               captures: dict, manifest: dict, processing_files: dict, *, create: bool=False) -> tuple[dict,dict]:
    """Full immutable construction provenance outside self-pinned candidate caches.

    Caller holds the common version lock. Creation occurs only after the entire
    private candidate has been built and verified. Fresh observations have already
    been retained in the independent raw depot and never rewrite original receipts.
    """
    key=f'_candidate-construction/{SLUG}/{version}.json'
    if not storage.object_exists(key):
        if not create:
            raise DartQuarantineError('missing_original_construction_provenance_bundle')
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],processing_files)
        bundle={'schema_version':1,'version':version,'manifest':manifest,'captures':{
            name:{'body_key':f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',
                  'receipt_key':f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json'}
            for name,(body,receipt) in captures.items()}}
        immutable(storage,key,encode(bundle))
    bundle=strict_json(storage.read_key(key))
    if set(bundle)!={'schema_version','version','manifest','captures'} or bundle['schema_version']!=1 or bundle['version']!=version or set(bundle['captures'])!=set(URLS):
        raise DartQuarantineError('original_construction_bundle_roster_drift')
    original={}
    for name in URLS:
        item=bundle['captures'][name]
        if set(item)!={'body_key','receipt_key'}:
            raise DartQuarantineError('original_construction_capture_roster_drift')
        expected_body=f'_raw-captures/{SLUG}/{sha(captures[name][0])}/{name}.body'
        if item['body_key']!=expected_body:
            raise DartQuarantineError('original_construction_capture_key_drift')
        body=storage.read_key(expected_body)
        if body!=captures[name][0]:
            raise DartQuarantineError('original_construction_capture_bytes_changed')
        receipt_key=item['receipt_key']
        # Read only a canonical immutable raw-depot key, never an arbitrary path.
        prefix=f'_raw-captures/{SLUG}/{sha(body)}/'
        if not isinstance(receipt_key,str) or not receipt_key.startswith(prefix) or not re.fullmatch(r'[0-9a-f]{64}\.receipt\.json',receipt_key[len(prefix):]):
            raise DartQuarantineError('original_construction_receipt_key_drift')
        receipt_body=storage.read_key(receipt_key);receipt=strict_json(receipt_body)
        if receipt_key!=prefix+sha(receipt_body)+'.receipt.json' or receipt_body!=encode(receipt):
            raise DartQuarantineError('original_construction_receipt_bytes_changed')
        validate_receipt(body,receipt,URLS[name]);original[name]=(body,receipt)
    validate_complete_bundle(original)
    expected_manifest=provenance(original)
    expected_manifest.update(processing_fingerprint=manifest['processing_fingerprint'],
                             prebuilt_output_policy=manifest['prebuilt_output_policy'])
    actual=bundle['manifest'];validate_release_provenance(actual)
    if set(actual)!=set(expected_manifest) or any(actual[k]!=v for k,v in expected_manifest.items() if k!='code_commit'):
        raise DartQuarantineError('original_construction_manifest_drift')
    validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],processing_files)
    return original,actual


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    # The normal graph adds only these derivative files. Source-owned directories remain exact.
    manifest_body=(root/'metadata/source_manifest.json').read_bytes()
    authored_snapshot='metadata/source/version_'+sha(manifest_body)+'_source_manifest.json'
    allowed_derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json',
        'metadata/source/source_manifest.json','metadata/source/data_dictionary.json',
        'metadata/source/quality_manifest.json',authored_snapshot}
    for name in ['metadata/source/source_manifest.json',authored_snapshot]:
        if (root/name).exists() and (root/name).read_bytes()!=manifest_body:
            raise DartQuarantineError('cached_authored_manifest_snapshot_differs')
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise DartQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/current_dart_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise DartQuarantineError('existing_candidate_unrostered_or_missing_object')
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    validate_complete_bundle(cached)
    expected_manifest=provenance(cached)
    if any(cached[name]!=captures[name] for name in URLS):
        raise DartQuarantineError('cached_capture_differs_from_fresh_source')
    expected_manifest['prebuilt_output_policy']={'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve source Esri int32 physical widths, native CRS and full typed metadata; no inferred widening or extra bbox schema accepted.','objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':sha((root/'candidate/typed-source.parquet').read_bytes()),'bytes':(root/'candidate/typed-source.parquet').stat().st_size}]}}}
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(manifest)
    binding=strict_json(storage.read_key(f'_candidate-construction/{SLUG}/{ready["version"]}.json'))
    if binding.get('manifest')!=manifest:
        raise DartQuarantineError('cached_original_construction_manifest_changed')
    # code_commit identifies the original immutable construction, including later docs-only commits.
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise DartQuarantineError('cached_provenance_differs_from_original_capture')
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['features'][0],
        'raw/retrieval.json':encode(cached['features'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        'metadata/current_dart_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise DartQuarantineError('cached_source_or_normalized_records_differ')
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'Point'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(3857)):
            raise DartQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise DartQuarantineError('cached_geopackage_source_fidelity_failure')
    # Fiona selects one CRS declaration and ignores contradictory auxiliary SRS
    # metadata. Exact fresh reconstruction also binds every header, index and table.
    with tempfile.TemporaryDirectory(prefix='dart-cache-native-container-') as tmp:
        expected=Path(tmp)/'expected.gpkg'
        write_canonical_geopackage(expected,rows)
        if sha256_file(root/'geopackage'/f'{SLUG}.gpkg')!=sha256_file(expected):
            raise DartQuarantineError('cached_geopackage_canonical_container_differs')
    if (root/f'geoparquet/{SLUG}.parquet').read_bytes()!=(root/'candidate/typed-source.parquet').read_bytes():
        raise DartQuarantineError('native_parquet_prebuilt_differs')
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(typed_table(rows,audit),check_metadata=True):
        raise DartQuarantineError('cached_typed_parquet_source_fidelity_failure')
    with tempfile.TemporaryDirectory(prefix='dart-cache-source-evidence-') as tmp:
        expected=expected_fidelity_payloads(rows,Path(tmp))
        if any((root/name).read_bytes()!=body for name,body in expected.items()):
            raise DartQuarantineError('cached_source_fidelity_evidence_differs')




@contextmanager
def candidate_version_lock(storage: StagingStorageResource, prefix: str):
    """One stable lock inode for lookup, validation, invalid detach and install."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:
            raise DartQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict) -> None:
    """Install complete private output; an existing concurrent winner stays intact."""
    with candidate_version_lock(storage,prefix) as target:
        if target.exists() or target.is_symlink():
            raise DartQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for name,value in payloads.items(): immutable(private,name,value)
            immutable(private,'metadata/current_dart_candidate_ready.json',encode(ready))
            captures={name:(payloads[f'raw/captures/{name}.body'],strict_json(payloads[f'raw/captures/{name}.receipt.json'])) for name in URLS}
            manifest=strict_json(payloads['metadata/source_manifest.json'])
            original_captures,original_manifest=original_provenance_bundle(storage,ready['version'],captures,manifest,strict_json(payloads['metadata/processing_identity.json'])['files'],create=True)
            if original_captures!=captures or original_manifest!=manifest:
                raise DartQuarantineError('concurrent_original_construction_bundle_changed_retry_with_original')
            os.rename(tmp,target)


def quarantine_cached_candidate(storage: StagingStorageResource, prefix: str, reason: str, *, already_locked: bool=False) -> str | None:
    """Move only the invalid target observed under the common version lock."""
    context=nullcontext(local_candidate_path(storage,prefix)) if already_locked else candidate_version_lock(storage,prefix)
    with context as target:
        if not target.exists():
            return None
        destination=Path(storage.local_dir)/'_quarantine'/SLUG/('invalid-cache-'+uuid4().hex)
        destination.mkdir(parents=True)
        os.rename(target,destination/'candidate')
        (destination/'report.json').write_bytes(encode({'approved':False,
            'status':'quarantined_invalid_draft_cache','original_prefix':prefix,'reason':reason}))
        return str(destination)



def stage_current_dart_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['features']
    try:
        validate_complete_bundle(captures)
        manifest=provenance(captures)
        with tempfile.TemporaryDirectory(prefix='current_dart-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-dart-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/current_dart_candidate_ready.json'
            native_buffer=io.BytesIO();pq.write_table(typed_table(rows,audit),native_buffer,compression='zstd')
            manifest.update(processing_fingerprint=fingerprint,prebuilt_output_policy=native_prebuilt_policy(native_buffer.getvalue()))
            with candidate_version_lock(storage,prefix) as target:
                if target.exists():
                    try:
                        captures,manifest=original_provenance_bundle(storage,version,captures,manifest,identity['files'])
                        if not storage.object_exists(ready_key):
                            raise DartQuarantineError('concurrent_or_incomplete_existing_candidate:missing_candidate_ready')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', f'geoparquet/{SLUG}.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/current_dart_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'metadata/source-fidelity/{name}' for name in FIDELITY_FILES} | {f'raw/members/{name}' for name in MEMBERS} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
                        if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                            raise DartQuarantineError('existing_candidate_inventory_conflict')
                        if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
                                or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                            raise DartQuarantineError('existing_candidate_identity_conflict')
                        for name, expected in ready['objects'].items():
                            key = f'{prefix}/{name}'
                            snapshot = storage.object_snapshot(key)
                            if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                                raise DartQuarantineError(f'existing_candidate_object_changed:{name}')
                        reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members)
                        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                                'status': 'working_draft_candidate_ready', 'processing_fingerprint': fingerprint}
                    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,fiona.errors.FionaError) as exc:
                        retained=quarantine_cached_candidate(storage,prefix,str(exc),already_locked=True)
                        raise DartQuarantineError(f'{exc}; cached_candidate_retained:{retained}') from exc
                binding_key=f'_candidate-construction/{SLUG}/{version}.json'
                if storage.object_exists(binding_key):
                    captures,manifest=original_provenance_bundle(storage,version,captures,manifest,identity['files'])
                    body,receipt=captures['features']
            geopackage = directory / f'{SLUG}.gpkg'
            write_canonical_geopackage(geopackage,rows)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Point'}:
                    raise DartQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(3857)):
                    raise DartQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if decoded.read_bytes() != records:
                raise DartQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            parquet_fidelity = compare_derivative(source_records, decoded, directory / 'parquet-comparison')
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise DartQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            if native_prebuilt_policy(parquet.read_bytes())!=manifest['prebuilt_output_policy']:
                raise DartQuarantineError('native_prebuilt_bytes_changed_during_construction')
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),f'geoparquet/{SLUG}.parquet':parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/current_dart_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
                'metadata/geopackage_source_fidelity.json': fidelity_wrapper(fidelity),
                'metadata/parquet_source_fidelity.json': fidelity_wrapper(parquet_fidelity),
                'metadata/baseline_comparison.json': encode(unapproved_baseline_comparison())}
            payloads.update({f'metadata/source-fidelity/{name}':(directory/'comparison'/name).read_bytes()
                for name in FIDELITY_FILES})
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile, sqlite3.Error, fiona.errors.FionaError) as exc:
        # Cache failures are detached under the observation lock above. A
        # separate installation conflict must never quarantine a newer winner.
        location = quarantine(storage, body, receipt, str(exc))
        raise DartQuarantineError(f'{exc}; retained:{location}') from exc
