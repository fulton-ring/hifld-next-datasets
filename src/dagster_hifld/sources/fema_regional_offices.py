"""Complete producer-hosted FEMA office-point layer, local unapproved draft.

All original wire inputs, IDs, native types and EPSG4326 points are preserved.
Region polygons are a separate dataset; no geocoding or filter to ten offices.
"""
from __future__ import annotations
import collections
import base64
import io
import xml.etree.ElementTree as ET
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import sqlite3
import stat
import struct
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
SLUG='federal-emergency-management-agency-fema-regional-offices'
LAYER='fema_regional_offices'
SERVICE='https://gis.fema.gov/arcgis/rest/services/FEMA/RegHQs/FeatureServer'
BASE=SERVICE+'/0'
LICENSE='FEMA producer copyright/metadata retained; no source, rights, baseline, compatibility, contract or release acceptance.'
READY='metadata/fema_regional_offices_candidate_ready.json'
BATCH=100
MAX_ROWS=1000
MAX_CAPTURE_BYTES=12*1024*1024
FIELD_DEFINITIONS=[{'name': 'objectid', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'domain': None, 'editable': False, 'nullable': False, 'defaultValue': None, 'modelName': 'OBJECTID'}, {'name': 'city_name', 'type': 'esriFieldTypeString', 'alias': 'CITY_NAME', 'domain': None, 'editable': True, 'nullable': True, 'length': 40, 'defaultValue': None, 'modelName': 'CITY_NAME'}, {'name': 'state_name', 'type': 'esriFieldTypeString', 'alias': 'STATE_NAME', 'domain': None, 'editable': True, 'nullable': True, 'length': 25, 'defaultValue': None, 'modelName': 'STATE_NAME'}, {'name': 'type', 'type': 'esriFieldTypeString', 'alias': 'TYPE', 'domain': None, 'editable': True, 'nullable': True, 'length': 25, 'defaultValue': None, 'modelName': 'TYPE'}, {'name': 'zip', 'type': 'esriFieldTypeString', 'alias': 'ZIP', 'domain': None, 'editable': True, 'nullable': True, 'length': 13, 'defaultValue': None, 'modelName': 'ZIP'}, {'name': 'name', 'type': 'esriFieldTypeString', 'alias': 'Name', 'domain': None, 'editable': True, 'nullable': True, 'length': 40, 'defaultValue': None, 'modelName': 'Name'}, {'name': 'region_objectid', 'type': 'esriFieldTypeSmallInteger', 'alias': 'REGION OBJECTID', 'domain': None, 'editable': True, 'nullable': True, 'defaultValue': None, 'modelName': 'REGION_OBJECTID'}, {'name': 'region_number', 'type': 'esriFieldTypeSmallInteger', 'alias': 'REGION NUMBER', 'domain': None, 'editable': True, 'nullable': True, 'defaultValue': None, 'modelName': 'REGION_NUMBER'}, {'name': 'address1', 'type': 'esriFieldTypeString', 'alias': 'ADDRESS1', 'domain': None, 'editable': True, 'nullable': True, 'length': 50, 'defaultValue': None, 'modelName': 'ADDRESS1'}, {'name': 'address2', 'type': 'esriFieldTypeString', 'alias': 'ADDRESS2', 'domain': None, 'editable': True, 'nullable': True, 'length': 50, 'defaultValue': None, 'modelName': 'ADDRESS2'}, {'name': 'state', 'type': 'esriFieldTypeString', 'alias': 'STATE', 'domain': None, 'editable': True, 'nullable': True, 'length': 13, 'defaultValue': None, 'modelName': 'STATE'}, {'name': 'zip4', 'type': 'esriFieldTypeString', 'alias': 'ZIP +4', 'domain': None, 'editable': True, 'nullable': True, 'length': 13, 'defaultValue': None, 'modelName': 'ZIP4'}, {'name': 'website', 'type': 'esriFieldTypeString', 'alias': 'WEBSITE', 'domain': None, 'editable': True, 'nullable': True, 'length': 100, 'defaultValue': None, 'modelName': 'WEBSITE'}, {'name': 'telephone', 'type': 'esriFieldTypeString', 'alias': 'TELEPHONE', 'domain': None, 'editable': True, 'nullable': True, 'length': 15, 'defaultValue': None, 'modelName': 'TELEPHONE'}]
PINS={'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/geojson/federal-emergency-management-agency-fema-regional-offices-geojson.geojson', 'bytes': 7173, 'sha256': '0f8439ffcfc56171c17adb61345835f66d5dc945364e999cac8693397829fac0'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/geopackage/federal-emergency-management-agency-fema-regional-offices.gpkg', 'bytes': 102400, 'sha256': 'bc28a5954d466a74ad8920cad8718a59e3b02e79d0ff99324c310e39699f2f44'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/geoparquet/federal-emergency-management-agency-fema-regional-offices.parquet', 'bytes': 15912, 'sha256': 'ef61557aa4c166eeeee527db980f58e1299d28af6c629d48db318f1ee1785243'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/pmtiles/federal-emergency-management-agency-fema-regional-offices.pmtiles', 'bytes': 3850, 'sha256': 'bbcc30135a87bc89d628554adde5cc37f9e9fdf6cadc36ca9b6d34ef6b37b8b1'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/shapefile/federal-emergency-management-agency-fema-regional-offices.zip', 'bytes': 3170, 'sha256': '01f422a97621a5fb771808cfeb27d21d4f9dcf41a107d622f08fabb13bea3c16'}, 'baseline_collection': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/federal-emergency-management-agency-fema-regional-offices/federal-emergency-management-agency-fema-regional-offices/v1.0.0/collection.json', 'bytes': 12916, 'sha256': '340fa9498e39d64e407c5dcab1e84feafb23d82a01936162656f7fd8b644b1bc'}, 'baseline_metadata': {'url': 'https://storage.googleapis.com/storage/v1/b/hifld-next-datasets-prod/o?prefix=hifld%2Ffederal-emergency-management-agency-fema-regional-offices%2Ffederal-emergency-management-agency-fema-regional-offices%2Fv1.0.0%2F&maxResults=100', 'bytes': 19531, 'sha256': 'f959494f40a545f033d8dfaf7cdadade67ede957dc186e731371d675ad30444c'}}
FIELDS=tuple(f['name'] for f in FIELD_DEFINITIONS)+('geometry',)
TYPE_MAP={'esriFieldTypeOID':pa.int64(),'esriFieldTypeSmallInteger':pa.int16(),'esriFieldTypeString':pa.string()}
TYPES=tuple(TYPE_MAP[f['type']] for f in FIELD_DEFINITIONS)+(pa.binary(),)
INITIAL_COUNT=14

class IncidentWriteError(Exception):pass

class FemaOfficesQuarantineError(ValueError):
    """Untrusted/incomplete input or cache requires review; never release."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise FemaOfficesQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(FemaOfficesQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise FemaOfficesQuarantineError('json_object_required')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise FemaOfficesQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise FemaOfficesQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise FemaOfficesQuarantineError('linked_staging_object')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise FemaOfficesQuarantineError('immutable_capture_changed:'+key)

def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted((root/'src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}

def query_url(**params):
    return str(httpx.URL(BASE+'/query',params={'f':'json',**params}))

def request_plan(ids=None):
    urls={'authority_root':'https://gis.fema.gov/arcgis/rest/services?f=pjson',
        'authority_folder':'https://gis.fema.gov/arcgis/rest/services/FEMA?f=pjson',
        'authority_service':SERVICE+'?f=pjson','authority_xml':SERVICE+'/info/metadata',
        'metadata_before':BASE+'?f=pjson','count_before':query_url(where='1=1',returnCountOnly='true'),
        'ids_before':query_url(where='1=1',returnIdsOnly='true')}
    if ids is not None:
        for offset in range(0,len(ids),BATCH):
            urls[f'page_{offset:05}']=query_url(objectIds=','.join(map(str,ids[offset:offset+BATCH])),outFields='*',returnGeometry='true',orderByFields='objectid')
        urls.update(metadata_after=BASE+'?f=pjson',count_after=query_url(where='1=1',returnCountOnly='true'),ids_after=query_url(where='1=1',returnIdsOnly='true'))
        urls.update({name:pin['url'] for name,pin in PINS.items()})
    return urls


def retain_failure(storage,captures,reason,attempted=None):
    prefix=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,prefix+'/'+name+'.body',body);immutable(storage,prefix+'/'+name+'.receipt.json',encode(receipt))
    if attempted is not None:immutable(storage,prefix+'/attempted-inputs.json',encode({'approved':False,'receipt_metadata_validated':False,'objects':attempted}))
    immutable(storage,prefix+'/reason.json',encode({'approved':False,'reason':reason,'attempted_inputs':prefix+'/attempted-inputs.json' if attempted is not None else None}))
    return prefix


def fetch_one(url):
    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'complete_body':False}
    try:
        with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',url) as response:
            receipt.update(status=response.status_code,final_url=str(response.url),content_length=response.headers.get('content-length'),content_encoding=response.headers.get('content-encoding'),requested_accept_encoding='identity')
            try:
                for block in response.iter_bytes(65536):
                    body+=block
                    if len(body)>6*1024*1024:raise FemaOfficesQuarantineError('bounded_response_size_exceeded')
            finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):raise FemaOfficesQuarantineError('non200_or_incomplete_encoded_transport')
        receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
        return bytes(body),receipt
    except (httpx.HTTPError,OSError,ValueError) as exc:
        receipt.update(bytes=len(body),sha256=digest(body),error=str(exc))
        raise FemaOfficesFetchError(str(exc),bytes(body),receipt) from exc

class FemaOfficesFetchError(FemaOfficesQuarantineError):
    def __init__(self,message,body,receipt):
        super().__init__(message);self.body=body;self.receipt=receipt

def validate_capture(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=digest(body)):
        raise FemaOfficesQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

def source_ids(document):
    if set(document)!={'objectIdFieldName','objectIds'} or document['objectIdFieldName']!='objectid':raise FemaOfficesQuarantineError('missing_or_renamed_original_OID')
    ids=document['objectIds']
    if not isinstance(ids,list) or not ids or len(ids)>MAX_ROWS or any(type(v)is not int or not 0<v<=2**63-1 for v in ids) or len(set(ids))!=len(ids):raise FemaOfficesQuarantineError('invalid_duplicate_or_unbounded_source_ids')
    return sorted(ids)


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempted={};total=0
    root=Path(capture_dir).absolute() if capture_dir else None
    if root is not None and ('..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents])):raise FemaOfficesQuarantineError('unsafe_capture_root')
    def retain(name,body):
        key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body)
        attempted[name]={'body_key':key,'body_sha256':digest(body),'body_bytes':len(body),'raw_receipt_read':False}
        return key
    def read(name,url):
        nonlocal total
        if root is None:
            try:body,receipt=fetch_one(url)
            except FemaOfficesFetchError as exc:
                retain(name,exc.body);captures[name]=(exc.body,exc.receipt);raise
            retain(name,body);raw_receipt=encode(receipt)
        else:
            body=bounded_input(root/(name+'.body'),6*1024*1024)
            retain(name,body) # Durable body and diagnostic entry precede receipt open.
            raw_receipt=bounded_input(root/(name+'.receipt.json'),65536)
        receipt_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.raw-receipt-{digest(raw_receipt)}.body'
        immutable(storage,receipt_key,raw_receipt)
        attempted[name].update(raw_receipt_read=True,raw_receipt_key=receipt_key,raw_receipt_sha256=digest(raw_receipt),raw_receipt_bytes=len(raw_receipt))
        receipt=strict_json(raw_receipt);captures[name]=(body,receipt);total+=len(body)
        if total>MAX_CAPTURE_BYTES:raise FemaOfficesQuarantineError('cumulative_capture_budget_exceeded')
        validate_capture(name,body,receipt,url)
        if name in PINS and (len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']):raise FemaOfficesQuarantineError('pinned_baseline_candidate_changed:'+name)
    try:
        for name,url in request_plan().items():read(name,url)
        ids=source_ids(strict_json(captures['ids_before'][0]))
        for name,url in request_plan(ids).items():
            if name not in captures:read(name,url)
        if root is not None and {p.name for p in root.iterdir()}!={name+suffix for name in captures for suffix in ('.body','.receipt.json')}:raise FemaOfficesQuarantineError('capture_directory_extra_or_missing_object')
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise FemaOfficesQuarantineError(str(exc)+'; retained:'+retained) from exc


def validate_metadata(meta):
    if (meta.get('fields')!=FIELD_DEFINITIONS or meta.get('id')!=0 or meta.get('name')!='FEMA Regional Offices' or meta.get('objectIdField')!='objectid' or meta.get('geometryType')!='esriGeometryPoint'
        or meta.get('hasZ') is not False or meta.get('hasM') is not False or meta.get('extent',{}).get('spatialReference',{}).get('latestWkid')!=4326 or meta.get('maxRecordCount',0)<BATCH):
        raise FemaOfficesQuarantineError('source_schema_physical_types_geometry_or_layer_identity_drift')

def semantic_metadata(meta):
    # FEMA returns random authoringInfo.colorRamp values on repeat reads. Only
    # this display renderer is excluded; full unmodified wire bytes stay raw.
    return {k:v for k,v in meta.items() if k!='drawingInfo'}


def physical_value(field,value):
    if value is None:
        if field['nullable'] is not True:raise FemaOfficesQuarantineError('nonnull_source_identifier_missing')
        return None
    if field['type']=='esriFieldTypeString':
        if type(value)is not str or len(value)>field['length']:raise FemaOfficesQuarantineError('source_string_type_or_width_drift')
        return value # Preserve literal whitespace, leading-zero ZIPs and spelling.
    lo,hi=(1,2**63-1) if field['type']=='esriFieldTypeOID' else (-32768,32767)
    if type(value)is not int or not lo<=value<=hi:raise FemaOfficesQuarantineError('source_integer_physical_type_drift')
    return value


def inspect_captures(captures):
    ids=source_ids(strict_json(captures['ids_before'][0]));plan=request_plan(ids)
    if set(captures)!=set(plan):raise FemaOfficesQuarantineError('incomplete_or_extra_capture_roster')
    for name,url in plan.items():
        body,receipt=captures[name];validate_capture(name,body,receipt,url)
        if name in PINS and (len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']):raise FemaOfficesQuarantineError('pinned_baseline_candidate_changed:'+name)
    root=strict_json(captures['authority_root'][0]);folder=strict_json(captures['authority_folder'][0]);service=strict_json(captures['authority_service'][0])
    xml=ET.fromstring(captures['authority_xml'][0]);text=' '.join(xml.itertext())
    if ('FEMA' not in root.get('folders',[]) or {'name':'FEMA/RegHQs','type':'FeatureServer'} not in folder.get('services',[])
        or not any(layer.get('id')==0 and layer.get('name')=='FEMA Regional Offices' and layer.get('geometryType')=='esriGeometryPoint' for layer in service.get('layers',[]))
        or service.get('copyrightText')!='FEMA' or 'FEMA' not in text or 'RegHQs' not in text):raise FemaOfficesQuarantineError('official_FEMA_root_folder_service_or_metadata_authority_drift')
    before=strict_json(captures['metadata_before'][0]);after=strict_json(captures['metadata_after'][0]);validate_metadata(before);validate_metadata(after)
    if semantic_metadata(before)!=semantic_metadata(after) or source_ids(strict_json(captures['ids_after'][0]))!=ids:raise FemaOfficesQuarantineError('source_changed_during_complete_extraction')
    for name in ('count_before','count_after'):
        count=strict_json(captures[name][0])
        if set(count)!={'count'} or type(count['count'])is not int or count['count']!=len(ids):raise FemaOfficesQuarantineError('source_count_roster_or_completeness_mismatch')
    rows=[]
    for offset in range(0,len(ids),BATCH):
        page=strict_json(captures[f'page_{offset:05}'][0]);expected_ids=ids[offset:offset+BATCH]
        if page.get('objectIdFieldName')!='objectid' or page.get('exceededTransferLimit') is True or page.get('geometryType')!='esriGeometryPoint' or page.get('spatialReference',{}).get('latestWkid')!=4326:raise FemaOfficesQuarantineError('page_limit_geometry_crs_or_completeness_drift')
        if [(f['name'],f['type']) for f in page.get('fields',[])]!=[(f['name'],f['type']) for f in FIELD_DEFINITIONS]:raise FemaOfficesQuarantineError('query_field_roster_or_physical_types_drift')
        features=page.get('features')
        if not isinstance(features,list) or len(features)!=len(expected_ids) or [f.get('attributes',{}).get('objectid') for f in features]!=expected_ids:raise FemaOfficesQuarantineError('page_missing_duplicate_unrequested_or_reordered_IDs')
        for feature in features:
            attrs=feature['attributes']
            if set(attrs)!={f['name'] for f in FIELD_DEFINITIONS}:raise FemaOfficesQuarantineError('source_attribute_rename_missing_or_unexpected_column')
            row={f['name']:physical_value(f,attrs[f['name']]) for f in FIELD_DEFINITIONS};g=feature.get('geometry')
            if not isinstance(g,dict) or set(g)!={'x','y'} or any(type(g[v])not in(int,float) or not math.isfinite(g[v]) for v in ('x','y')) or not(-180<=g['x']<=180 and -90<=g['y']<=90):raise FemaOfficesQuarantineError('missing_changed_or_out_of_range_native_point')
            row['geometry']=shapely.Point(g['x'],g['y']).wkb;rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['nullable'],metadata={b'fema:esri_field':encode(f)}) for f in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary())]);table=pa.Table.from_pylist(rows,schema=schema)
    if table.num_rows!=len(ids) or len(set(table['objectid'].to_pylist()))!=table.num_rows:raise FemaOfficesQuarantineError('office_OID_missing_duplicate_or_coverage_loss')
    geometry=shapely.from_wkb(table['geometry'].to_pylist())
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise FemaOfficesQuarantineError('invalid_or_empty_native_geometry')
    audit={'approved':False,'feature_count':len(ids),'original_field_definitions':FIELD_DEFINITIONS,'source_original_OID':'objectid retained; OID width undeclared, Arrowint64 is a proposed lossless representation. No published extra id invented or mapped.',
        'physical_decode':'SmallInteger=int16, strings/nulls/literal whitespace retained, native4326 Point float64. No coordinate geocoding, rounding or repair.',
        'source_crs':'EPSG:4326','geometry':'Point','invalid_geometries':0,'office_type_counts':dict(sorted(collections.Counter(table['type'].to_pylist()).items())),
        'current_source_count':len(ids),'initial_verified_count':INITIAL_COUNT,'roster_change_from_initial_requires_review':len(ids)!=INITIAL_COUNT,
        'semantic_metadata_sha256':digest(encode(semantic_metadata(before))),'metadata_display_exclusion':'Only drawingInfo is excluded from semantic equality/version: random renderer colors; complete original wire metadata is preserved.',
        'release_day':'unknown; XML metadata creation 20201016, inventory dates and external portal timestamps do not establish record release/currency.',
        'coverage':'Complete FEMA RegHQs office POINT layer: 10 regional+3 area+1 national at initial observation. No filter to10, polygon substitution or geocoding.'}
    return table,audit


def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(4326).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def canonical_values(table):
    return table.to_pylist()


def records_hash(table):
    h=hashlib.sha256()
    for row in canonical_values(table):row['geometry']=row['geometry'].hex();h.update(encode(row))
    return h.hexdigest()

def write_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='geometry',geometry_type='Point',crs='EPSG:4326',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'geometry'})
    with sqlite3.connect(path) as db:db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")

def verify_outputs(root,table):
    for relative in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if not pq.read_table(root/relative).equals(expected_parquet(table),check_metadata=True):raise FemaOfficesQuarantineError('parquet_full_values_physical_types_ids_wkb_or_metadata_drift')
    meta,actual=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs']!='EPSG:4326' or tuple(actual.column_names)!=FIELDS or tuple(f.type for f in actual.schema)!=TYPES or canonical_values(actual)!=canonical_values(table):raise FemaOfficesQuarantineError('gpkg_full_values_physical_types_ids_or_native_geometry_loss')
    with tempfile.TemporaryDirectory(prefix='fema-offices-events-fresh-container-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise FemaOfficesQuarantineError('gpkg_full_container_srs_headers_sql_or_index_drift')
def manifest_for(captures,identity,parquet_pin):
    ids=source_ids(strict_json(captures['ids_before'][0]));urls=request_plan(ids);inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,urls[name]);baseline=name.startswith('baseline_')
        inputs.append({'input_id':name+':'+digest(body),'producer':'HIFLD Next published candidate' if baseline else 'Federal Emergency Management Agency','host':urlsplit(urls[name]).hostname,'role':'archive' if baseline else 'producer_publication',
            'canonical_url':urls[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
            'attribution':'Unapproved complete published snapshot' if baseline else 'FEMA RegHQs office-point service on producer government host; complete original layer0.',
            'release_date_unknown_reason':'No verified release day. Retrieval, XML metadata creation and external portal/inventory dates are not source record release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'FEMA Regional Offices — complete official office-point layer, unapproved draft','publisher':'Federal Emergency Management Agency','agency':'Federal Emergency Management Agency','source_url':BASE,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_native_office_point_extraction','parameters':{'authority':'FEMA government GIS root→FEMA folder→RegHQs service→layer0; no third-party account delegation inferred','completeness':'Before/after full OID rosters, counts and semantic metadata; exact ID-targeted batches','IDs':'Original objectid, region_objectid and region_number retained; published id kept separately, no surrogate','types':'Native SmallInteger int16 and original field declarations; undeclared-width OID represented losslessly as int64; strings/nulls/whitespace untouched','geometry':'Native4326 double Point x/y, no reprojection/geocoding/repair','metadata':'Complete wire preserved; only random display drawingInfo excluded from semantic fingerprint'}}],
        'unsupported_output_formats':{'shapefile_zip':'Source long field names/types cannot be mapped destructively without acceptance.','pmtiles':'Quantization changes native coordinates; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve every original physical source field/null/metadata and exact native WKB without generic bbox/typing additions. This capability grants no acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise FemaOfficesQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX);yield target

def construction_record_key(version):
    return f'_construction-state/{SLUG}/{version}.json'


def original_bundle(storage,version,captures,identity,manifest,*,create=False):
    """Caller holds common lock; create only after complete private validation.

    Complete first manifest/receipts are bound by the shared outside CAS/marker.
    Old unbound records remain evidence and cannot bootstrap this trust scope.
    """
    fingerprint=digest(encode(identity))
    payload={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt} for name,(body,receipt) in captures.items()}}
    envelope=canonical_construction_bundle(version,fingerprint,payload)
    legacy=f'_source-provenance/{SLUG}/{version}/bundle.json'
    if storage.object_exists(legacy) or storage.object_exists(legacy+'.first-construction.json'):
        raise FemaOfficesQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise FemaOfficesQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise FemaOfficesQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if name not in ('metadata_before','metadata_after') and body!=captures[name][0]:raise FemaOfficesQuarantineError('original_capture_bytes_differ')
        if name in ('metadata_before','metadata_after') and semantic_metadata(strict_json(body))!=semantic_metadata(strict_json(captures[name][0])):raise FemaOfficesQuarantineError('original_source_semantics_changed')
        original[name]=(body,item['receipt'])
    original_table,original_audit=inspect_captures(original)
    current_table,current_audit=inspect_captures(captures)
    if not original_table.equals(current_table,check_metadata=True) or original_audit!=current_audit:raise FemaOfficesQuarantineError('original_source_values_or_semantics_changed')
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise FemaOfficesQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

def source_payloads(captures,identity,audit,manifest,table):
    proof={'status':'pass','approved':False,'rows':table.num_rows,'source_records_sha256':records_hash(table),'method':'Complete original JSON→declared native physical decode→every output column/value/null/identifier/native WKB equality; whole deterministic GPKG container and exact Arrow field/schema metadata.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/fema_regional_offices_source_audit.json':encode(audit),'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),
        'metadata/baseline_comparison.json':encode(baseline_comparison(captures,table)),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}
    for name,(body,receipt) in captures.items():values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values

def reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin):
    root=Path(storage.local_dir).absolute()/prefix
    if not (root/READY).is_file():raise FemaOfficesQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table);names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise FemaOfficesQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise FemaOfficesQuarantineError('unknown_missing_or_linked_candidate_object')
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise FemaOfficesQuarantineError('source_owned_report_wrapper_placeholder_or_raw_provenance_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise FemaOfficesQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise FemaOfficesQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise FemaOfficesQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table)

def stage_fema_regional_offices(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    try:
        table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({'records':records_hash(table),'semantic_metadata':audit['semantic_metadata_sha256'],'baseline_pins':PINS,'other_original_bodies':{n:digest(b) for n,(b,_) in captures.items() if n not in ('metadata_before','metadata_after')}}))
        version=f'v0.0.0-draft-fema-offices-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir).absolute()
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            refuse_observed_loss(storage,version)
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise FemaOfficesQuarantineError('linked_candidate')
                    try:load_original_state(storage,version,captures,identity,table)
                    except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                        try:record_observed_loss(storage,version,fingerprint,str(loss))
                        except Exception as journal_error:raise IncidentWriteError('incident_write_failed_candidate_preserved') from journal_error
                        raise FemaOfficesQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
                    with tempfile.TemporaryDirectory(prefix='fema-offices-native-prebuilt-') as tmp:
                        fresh=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),fresh,compression='zstd');pin={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}
                    reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise FemaOfficesQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            if original_state_present(storage,version):
                try:load_original_state(storage,version,captures,identity,table)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                    try:record_observed_loss(storage,version,fingerprint,str(loss))
                    except Exception as journal_error:raise IncidentWriteError('incident_write_failed_partial_state_preserved') from journal_error
                    raise FemaOfficesQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
            with tempfile.TemporaryDirectory(prefix='.fema-offices-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geopackage','geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                write_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg');parquet=private/'geoparquet'/f'{SLUG}.parquet';pq.write_table(expected_parquet(table),parquet,compression='zstd');(private/'candidate/typed-source.parquet').write_bytes(parquet.read_bytes());verify_outputs(private,table)
                pin={'sha256':sha256_file(parquet),'bytes':parquet.stat().st_size};manifest=manifest_for(captures,identity,pin)
                # The entire private candidate is complete before first trust state
                # is installed. A failed metadata/container build cannot bind history.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                # An existing complete outside witness returns original receipts.
                # Rebuild only the private candidate from those exact originals.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise FemaOfficesQuarantineError(str(exc)+'; retained:'+retained) from exc

def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise FemaOfficesQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise FemaOfficesQuarantineError('incomplete_or_changed_bounded_input')
    return body


def incident_directory(version):
    return f'_construction-state/{SLUG}/incident-{version}'

def refuse_observed_loss(storage,version):
    directory=Path(storage.local_dir).absolute()/incident_directory(version)
    if directory.exists() and any(directory.rglob('*')):raise FemaOfficesQuarantineError('permanent_observed_original_trust_loss_requires_reviewed_operator_recovery')

def record_observed_loss(storage,version,fingerprint,reason):
    # Partial writes/pending files are themselves blocking negative evidence.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason})
    seal_first_construction(storage,key,version,fingerprint,body)

def original_state_present(storage,version):
    key=construction_record_key(version);base=Path(storage.local_dir).absolute()
    return (base/key).exists() or (base/(key+'.first-construction.json')).exists() or (base/(key+'.evidence')).exists()

def load_original_state(storage,version,captures,identity,table):
    with tempfile.TemporaryDirectory(prefix='fema-original-pin-') as tmp:
        path=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),path,compression='zstd');pin={'sha256':sha256_file(path),'bytes':path.stat().st_size}
    return original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))


def baseline_comparison(captures,source):
    """Full four open formats, all values/IDs/native geometry; PMTiles bytes only.

    Diagnostic proposed aliases never enter candidate schema or release approval.
    Original published bodies/IDs remain independent from the official source.
    """
    metadata=strict_json(captures['baseline_metadata'][0])
    if metadata.get('nextPageToken'):raise FemaOfficesQuarantineError('incomplete_published_provider_listing')
    for name,pin in PINS.items():
        if name in ('baseline_collection','baseline_metadata'):continue
        objects=[item for item in metadata.get('items',[]) if item.get('name')==urlsplit(pin['url']).path.split('/hifld-next-datasets-prod/')[1]]
        if len(objects)!=1 or int(objects[0].get('size',-1))!=pin['bytes'] or base64.b64encode(hashlib.md5(captures[name][0]).digest()).decode()!=objects[0].get('md5Hash'):raise FemaOfficesQuarantineError('published_provider_size_MD5_or_complete_object_drift')
    collection=strict_json(captures['baseline_collection'][0])
    # Keep complete provider/listing/collection bodies; their timestamps are not
    # promoted to producer release dates or accepted baseline decisions.
    baseline=pq.read_table(io.BytesIO(captures['baseline_geoparquet'][0]));records=[]
    for row in baseline.to_pylist():
        row.pop('bbox',None);row['geometry']=shapely.from_wkb(row['geometry']);records.append(row)
    formats={'geoparquet':records};schemas={'geoparquet':str(baseline.schema)}
    geo=strict_json(captures['baseline_geojson'][0])
    if geo.get('type')!='FeatureCollection':raise FemaOfficesQuarantineError('invalid_full_published_geojson')
    formats['geojson']=[dict(f['properties'],geometry=shapely.geometry.shape(f['geometry'])) for f in geo['features']]
    schemas['geojson']='Original JSON properties retained in full raw body; schema not inferred as native producer types.'
    with tempfile.TemporaryDirectory(prefix='fema-baseline-full-') as tmp:
        for name,ext in [('geopackage','.gpkg'),('shapefile','.zip')]:
            path=Path(tmp)/(name+ext);path.write_bytes(captures['baseline_'+name][0]);meta,table=pyogrio.read_arrow(path)
            if pyproj.CRS(meta['crs'])!=pyproj.CRS(4326):raise FemaOfficesQuarantineError('published_native_CRS_drift')
            rows=[]
            for row in table.to_pylist():
                geom=row.pop(meta.get('geometry_name') or 'wkb_geometry');row['geometry']=shapely.from_wkb(geom);rows.append(row)
            formats[name]=rows;schemas[name]=str(table.schema)
    summaries={};source_rows={row['objectid']:row for row in source.to_pylist()};aliases={'region_obj':'region_objectid','region_num':'region_number'}
    for name,rows in formats.items():
        if len(rows)!=14 or len({row.get('objectid') for row in rows})!=14 or {row.get('objectid') for row in rows}!=set(range(1,15)):raise FemaOfficesQuarantineError('published_complete_office_roster_drift')
        differences=[];equal_geometry=0
        for row in rows:
            oid=row['objectid'];current=source_rows.get(oid)
            if current is None:continue
            equal_geometry+=int(row['geometry'].wkb==current['geometry'])
            for field,value in row.items():
                if field in ('geometry','id'):continue
                native=aliases.get(field,field)
                if native in current and current[native]!=value:differences.append({'source_objectid':oid,'published_field':field,'source_field':native,'published':value,'source':current[native]})
        summaries[name]={'rows':len(rows),'published_identifiers':[{'objectid':row['objectid'],'id':row.get('id')} for row in rows],
            'schema':schemas[name],'geometry_WKB_exact_with_current_source':equal_geometry,'diagnostic_attribute_differences':differences,
            'columns':list(rows[0]),'compared_attribute_values':sum(sum(1 for field in row if field not in ('geometry','id') and aliases.get(field,field) in source_rows.get(row['objectid'],{})) for row in rows),'native_point_geometry':'EPSG:4326, no tolerance or approved transformation inferred'}
    return {'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_FEMA_source_baseline_column_names_int16_vs_int64_trailing_space_extra_published_ID_contract'],
        'complete_published_candidates':PINS,'formats':summaries,'pmtiles':'Complete original bytes/provider MD5 pinned; vector content not decoded or claimed native-lossless.',
        'diagnostic_aliases_unapproved':aliases,'current_source_objectids':source['objectid'].to_pylist(),'source_geometry_native4326_sha256':records_hash(source),
        'roster_change':set(source['objectid'].to_pylist())!=set(range(1,15)),'release_decision':'No baseline, source, tolerance, ID mapping, schema migration, contract or release accepted.'}
