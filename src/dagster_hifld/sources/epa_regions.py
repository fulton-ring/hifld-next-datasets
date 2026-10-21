"""Complete official EPA application-distributed region boundaries; local draft.

All original raw inputs, source FID/OBJECTID, native types and3857 rings stay literal.
The legacy SEGS composite and scale/EEZ products are distinct; no acceptance or silent substitution.
"""
from __future__ import annotations
from collections import Counter
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
import shutil
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
SLUG='epa-regions'
LAYER='epa_regions'
SERVICE='https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer'
BASE=SERVICE+'/0'
LICENSE='EPA default public-domain policy and original item access/rights metadata retained; no rights, source, baseline, compatibility, contract or release accepted.'
READY='metadata/epa_regions_candidate_ready.json'
BATCH=100
MAX_ROWS=100
MAX_CAPTURE_BYTES=32*1024*1024
MAX_RESPONSE_BYTES=24*1024*1024
SPACE_FLOOR=300_000_000
FIELD_DEFINITIONS=[{'name': 'FID', 'type': 'esriFieldTypeOID', 'actualType': 'int', 'alias': 'FID', 'sqlType': 'sqlTypeInteger', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeInteger', 'actualType': 'int', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeInteger', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'EPAREGION', 'type': 'esriFieldTypeString', 'actualType': 'nvarchar', 'alias': 'EPAREGION', 'sqlType': 'sqlTypeNVarchar', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'Shape_Leng', 'type': 'esriFieldTypeDouble', 'actualType': 'float', 'alias': 'Shape_Leng', 'sqlType': 'sqlTypeFloat', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'Shape_Le_1', 'type': 'esriFieldTypeDouble', 'actualType': 'float', 'alias': 'Shape_Le_1', 'sqlType': 'sqlTypeFloat', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'actualType': 'float', 'alias': 'Shape_Area', 'sqlType': 'sqlTypeFloat', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}]
PAGE_FIELDS=[{'name': 'FID', 'type': 'esriFieldTypeOID', 'alias': 'FID', 'sqlType': 'sqlTypeInteger', 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeInteger', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeInteger', 'domain': None, 'defaultValue': None}, {'name': 'EPAREGION', 'type': 'esriFieldTypeString', 'alias': 'EPAREGION', 'sqlType': 'sqlTypeNVarchar', 'length': 50, 'domain': None, 'defaultValue': None}, {'name': 'Shape_Leng', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Leng', 'sqlType': 'sqlTypeFloat', 'domain': None, 'defaultValue': None}, {'name': 'Shape_Le_1', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Le_1', 'sqlType': 'sqlTypeFloat', 'domain': None, 'defaultValue': None}, {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Area', 'sqlType': 'sqlTypeFloat', 'domain': None, 'defaultValue': None}]
URLS={'official_story': 'https://epa.maps.arcgis.com/sharing/rest/content/items/253b1156f708428aad3cbbd8fdf2ba5b/data?f=pjson', 'ids_after': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0/query?f=json&where=1%3D1&returnIdsOnly=true', 'metadata_after': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0?f=pjson', 'count_before': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0/query?f=json&where=1%3D1&returnCountOnly=true', 'ids_before': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0/query?f=json&where=1%3D1&returnIdsOnly=true', 'license_epa': 'https://edg.epa.gov/EPA_Data_License.html', 'page_00000': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0/query?f=json&objectIds=1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C10&outFields=%2A&returnGeometry=true&orderByFields=FID', 'metadata_before': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0?f=pjson', 'official_webmap': 'https://epa.maps.arcgis.com/sharing/rest/content/items/4bb38b536af04c76993bd62691278a24/data?f=pjson', 'count_after': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer/0/query?f=json&where=1%3D1&returnCountOnly=true', 'official_webmap_item': 'https://epa.maps.arcgis.com/sharing/rest/content/items/4bb38b536af04c76993bd62691278a24?f=pjson', 'source_metadata': 'https://epa.maps.arcgis.com/sharing/rest/content/items/fd4571953b1648f68be2478088a657c6/info/metadata/metadata.xml', 'source_service': 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Region/FeatureServer?f=pjson', 'official_application': 'https://www.epa.gov/uic/aquifer-exemptions-map', 'source_item': 'https://epa.maps.arcgis.com/sharing/rest/content/items/fd4571953b1648f68be2478088a657c6?f=pjson'}
FIELDS=tuple(f['name']for f in FIELD_DEFINITIONS)+('geometry',)
TYPE_MAP={'esriFieldTypeOID':pa.int32(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
TYPES=tuple(TYPE_MAP[f['type']]for f in FIELD_DEFINITIONS)+(pa.binary(),)
EXPECTED_IDENTITIES={1: (1, 'EPA Region 2'), 2: (2, 'EPA Region 3'), 3: (3, 'EPA Region 4'), 4: (4, 'EPA Region 5'), 5: (5, 'EPA Region 6'), 6: (6, 'EPA Region 7'), 7: (7, 'EPA Region 8'), 8: (8, 'EPA Region 1'), 9: (9, 'EPA Region 10'), 10: (10, 'EPA Region 9')}
INITIAL_COUNT=10
SOURCE_RECORDS_SHA256='eb927356b407a00a6918db9f04958dffa4e7708d52da74c4fd42ff648e3019c4'

class IncidentWriteError(Exception):pass

class EpaRegionsQuarantineError(ValueError):
    """Untrusted/incomplete input or cache requires review; never release."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise EpaRegionsQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(EpaRegionsQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise EpaRegionsQuarantineError('json_object_required')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise EpaRegionsQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise EpaRegionsQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise EpaRegionsQuarantineError('disk_floor_before_acquisition')
    if any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise EpaRegionsQuarantineError('linked_staging_object')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise EpaRegionsQuarantineError('immutable_capture_changed:'+key)

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
    names=['official_application','official_story','official_webmap','official_webmap_item','source_item','source_service','source_metadata','license_epa','metadata_before','count_before','ids_before']
    result={n:URLS[n]for n in names}
    if ids is not None:
        for offset in range(0,len(ids),BATCH):
            result[f'page_{offset:05}']=str(httpx.URL(BASE+'/query',params={'f':'json','objectIds':','.join(map(str,ids[offset:offset+BATCH])),'outFields':'*','returnGeometry':'true','orderByFields':'FID'}))
        result.update({n:URLS[n]for n in ['metadata_after','count_after','ids_after']})
    return result


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
                iterator=response.iter_bytes(65536)
                while True:
                    if shutil.disk_usage('/workspace').free<SPACE_FLOOR:raise EpaRegionsQuarantineError('disk_floor_before_next_read')
                    try:block=next(iterator)
                    except StopIteration:break
                    body+=block
                    if len(body)>MAX_RESPONSE_BYTES:raise EpaRegionsQuarantineError('bounded_response_size_exceeded')
            finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):raise EpaRegionsQuarantineError('non200_or_incomplete_encoded_transport')
        receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
        return bytes(body),receipt
    except (httpx.HTTPError,OSError,ValueError) as exc:
        receipt.update(bytes=len(body),sha256=digest(body),error=str(exc))
        raise EpaRegionsFetchError(str(exc),bytes(body),receipt) from exc

class EpaRegionsFetchError(EpaRegionsQuarantineError):
    def __init__(self,message,body,receipt):
        super().__init__(message);self.body=body;self.receipt=receipt

def validate_capture(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or type(receipt.get('bytes'))is not int or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=digest(body)):
        raise EpaRegionsQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

def source_ids(document):
    if set(document)!={'objectIdFieldName','objectIds'} or document['objectIdFieldName']!='FID':raise EpaRegionsQuarantineError('missing_or_renamed_original_OID')
    ids=document['objectIds']
    if not isinstance(ids,list) or not ids or len(ids)>MAX_ROWS or any(type(v)is not int or not 0<v<=2**31-1 for v in ids) or len(set(ids))!=len(ids):raise EpaRegionsQuarantineError('invalid_duplicate_or_unbounded_source_ids')
    return sorted(ids)


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempted={};total=0
    root=Path(capture_dir).absolute() if capture_dir else None
    if root is not None and ('..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents])):raise EpaRegionsQuarantineError('unsafe_capture_root')
    def retain(name,body):
        key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body)
        attempted[name]={'body_key':key,'body_sha256':digest(body),'body_bytes':len(body),'raw_receipt_read':False}
        return key
    def read(name,url):
        nonlocal total
        if root is None:
            try:body,receipt=fetch_one(url)
            except EpaRegionsFetchError as exc:
                retain(name,exc.body);captures[name]=(exc.body,exc.receipt);raise
            retain(name,body);raw_receipt=encode(receipt)
        else:
            body=bounded_input(root/(name+'.body'),MAX_RESPONSE_BYTES)
            retain(name,body) # Durable body and diagnostic entry precede receipt open.
            raw_receipt=bounded_input(root/(name+'.receipt.json'),65536)
        receipt_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.raw-receipt-{digest(raw_receipt)}.body'
        immutable(storage,receipt_key,raw_receipt)
        attempted[name].update(raw_receipt_read=True,raw_receipt_key=receipt_key,raw_receipt_sha256=digest(raw_receipt),raw_receipt_bytes=len(raw_receipt))
        receipt=strict_json(raw_receipt);captures[name]=(body,receipt);total+=len(body)
        if total>MAX_CAPTURE_BYTES:raise EpaRegionsQuarantineError('cumulative_capture_budget_exceeded')
        validate_capture(name,body,receipt,url)
    try:
        for name,url in request_plan().items():read(name,url)
        ids=source_ids(strict_json(captures['ids_before'][0]))
        for name,url in request_plan(ids).items():
            if name not in captures:read(name,url)
        if root is not None and {p.name for p in root.iterdir()}!={name+suffix for name in captures for suffix in ('.body','.receipt.json')}:raise EpaRegionsQuarantineError('capture_directory_extra_or_missing_object')
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise EpaRegionsQuarantineError(str(exc)+'; retained:'+retained) from exc


def validate_metadata(meta):
    if (encode(meta.get('fields'))!=encode(FIELD_DEFINITIONS)or type(meta.get('id'))is not int or meta['id']!=0 or meta.get('name')!='EPA_Region' or meta.get('objectIdField')!='FID' or meta.get('geometryType')!='esriGeometryPolygon'
        or meta.get('hasZ')is not False or meta.get('hasM')is not False or type(meta.get('extent',{}).get('spatialReference',{}).get('latestWkid'))is not int or meta.get('extent',{}).get('spatialReference',{}).get('latestWkid')!=3857 or type(meta.get('maxRecordCount'))is not int or meta['maxRecordCount']<BATCH):
        raise EpaRegionsQuarantineError('source_fields_identity_physical_types_or_native_CRS_drift')


def semantic_metadata(meta):
    # No renderer exclusion is needed: complete before/after metadata matched.
    return meta


def physical_value(field,value):
    if value is None:
        if field['nullable']is not True:raise EpaRegionsQuarantineError('nonnull_source_identifier_missing')
        return None
    typ=field['type']
    if typ=='esriFieldTypeString':
        if type(value)is not str or len(value)>field['length']:raise EpaRegionsQuarantineError('source_string_type_or_width_drift')
        return value
    if typ in ('esriFieldTypeOID','esriFieldTypeInteger'):
        if type(value)is not int or not-2**31<=value<2**31:raise EpaRegionsQuarantineError('source_SQL_integer32_type_or_range_drift')
        return value
    if type(value)not in(int,float)or not math.isfinite(value)or(type(value)is int and int(float(value))!=value):raise EpaRegionsQuarantineError('source_double_precision_or_type_drift')
    return float(value)


def authority(captures):
    application=captures['official_application'][0].decode('utf8')
    story=strict_json(captures['official_story'][0]);webmap=strict_json(captures['official_webmap'][0]);descriptor=strict_json(captures['official_webmap_item'][0]);item=strict_json(captures['source_item'][0]);service=strict_json(captures['source_service'][0])
    if ('https://storymaps.arcgis.com/stories/253b1156f708428aad3cbbd8fdf2ba5b'not in application or '4bb38b536af04c76993bd62691278a24'not in json.dumps(story)or descriptor.get('id')!='4bb38b536af04c76993bd62691278a24'
        or 'fd4571953b1648f68be2478088a657c6'not in json.dumps(webmap)or BASE not in json.dumps(webmap)or item.get('id')!='fd4571953b1648f68be2478088a657c6'or item.get('owner')!='goins.jake_EPAEXT'or item.get('url')!=SERVICE or item.get('access')!='public'
        or not any(type(x.get('id'))is int and x['id']==0 and x.get('name')=='EPA_Region'for x in service.get('layers',[]))):raise EpaRegionsQuarantineError('official_application_story_webmap_item_source_delegation_drift')
    ET.fromstring(captures['source_metadata'][0])
    return {'classification':'official agency application distribution; exact legacy SEGS continuation NOT established','producer':'U.S. Environmental Protection Agency','account':item['owner'],'source_item':item['id'],'current_government_application':URLS['official_application'],'licenseInfo':item.get('licenseInfo'),'accessInformation':item.get('accessInformation'),'source_url':item['url']}


def verify_original_rings(raw,geometry):
    rings=raw.get('rings')
    if set(raw)!={'rings'}or not isinstance(rings,list)or not rings:raise EpaRegionsQuarantineError('missing_or_changed_native_polygon_rings')
    def fingerprint(ring):
        if len(ring)<4 or ring[0]!=ring[-1]:raise EpaRegionsQuarantineError('unclosed_or_short_original_ring')
        if any(len(p)!=2 or any(type(v)not in(int,float)or not math.isfinite(v)for v in p)for p in ring):raise EpaRegionsQuarantineError('nonfinite_or_changed_original_coordinate_dimension')
        return digest(b''.join(struct.pack('<dd',*p)for p in ring))
    originals=Counter(fingerprint(r)for r in rings);actual=[]
    for poly in shapely.get_parts(geometry):
        if poly.geom_type!='Polygon':raise EpaRegionsQuarantineError('unexpected_source_geometry_kind')
        actual+=[list(poly.exterior.coords)]+[list(r.coords)for r in poly.interiors]
    if originals!=Counter(fingerprint(r)for r in actual):raise EpaRegionsQuarantineError('native_ring_vertices_precision_or_connectivity_changed')
    return len(rings)


def inspect_captures(captures):
    ids=source_ids(strict_json(captures['ids_before'][0]));plan=request_plan(ids)
    if set(captures)!=set(plan):raise EpaRegionsQuarantineError('incomplete_or_extra_capture_roster')
    for name,url in plan.items():validate_capture(name,*captures[name],url)
    delegation=authority(captures)
    before=strict_json(captures['metadata_before'][0]);after=strict_json(captures['metadata_after'][0]);validate_metadata(before);validate_metadata(after)
    if encode(before)!=encode(after)or source_ids(strict_json(captures['ids_after'][0]))!=ids:raise EpaRegionsQuarantineError('source_changed_during_complete_extraction')
    for name in ('count_before','count_after'):
        count=strict_json(captures[name][0])
        if set(count)!={'count'}or type(count['count'])is not int or count['count']!=len(ids)or len(ids)!=10:raise EpaRegionsQuarantineError('source_count_or_complete_roster_drift')
    rows=[];ring_count=0
    for offset in range(0,len(ids),BATCH):
        body=captures[f'page_{offset:05}'][0];page=strict_json(body);wanted=ids[offset:offset+BATCH]
        if page.get('objectIdFieldName')!='FID'or (page.get('exceededTransferLimit')is True or('exceededTransferLimit'in page and type(page['exceededTransferLimit'])is not bool)) or page.get('geometryType')!='esriGeometryPolygon'or encode(page.get('fields'))!=encode(PAGE_FIELDS)or encode(page.get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857}):raise EpaRegionsQuarantineError('page_fields_limit_or_native_geometry_CRS_drift')
        features=page.get('features')
        if not isinstance(features,list)or len(features)!=len(wanted)or[feature.get('attributes',{}).get('FID')for feature in features]!=wanted:raise EpaRegionsQuarantineError('page_missing_duplicate_unrequested_or_reordered_original_IDs')
        meta,geometries=pyogrio.read_arrow(io.BytesIO(body))
        if meta['crs']!='EPSG:3857'or geometries.num_rows!=len(wanted):raise EpaRegionsQuarantineError('native_geometry_decode_drift')
        geo_name=meta.get('geometry_name')or'wkb_geometry';decoded={r['FID']:r[geo_name]for r in geometries.to_pylist()}
        for feature in features:
            attrs=feature['attributes']
            if set(attrs)!={f['name']for f in FIELD_DEFINITIONS}:raise EpaRegionsQuarantineError('source_attribute_rename_missing_or_extra_column')
            row={f['name']:physical_value(f,attrs[f['name']])for f in FIELD_DEFINITIONS}
            if (row['OBJECTID'],row['EPAREGION'])!=EXPECTED_IDENTITIES.get(row['FID']):raise EpaRegionsQuarantineError('source_ID_region_association_drift_requires_review')
            row['geometry']=decoded[row['FID']];geometry=shapely.from_wkb(row['geometry']);ring_count+=verify_original_rings(feature.get('geometry',{}),geometry);rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['nullable'],metadata={b'epa:esri_field':encode(f),**({b'GDAL:OGR:width':str(f['length']).encode()}if 'length'in f else{})})for f in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary())]);table=pa.Table.from_pylist(rows,schema=schema)
    gs=shapely.from_wkb(table['geometry'].to_pylist())
    if not all(shapely.is_valid(gs))or any(shapely.is_empty(gs)):raise EpaRegionsQuarantineError('invalid_or_empty_native_geometry_requires_review')
    if int(shapely.get_num_coordinates(gs).sum())!=404106 or ring_count!=106:raise EpaRegionsQuarantineError('native_geometry_completeness_drift')
    audit={'approved':False,'feature_count':10,'crs':'EPSG:3857','original_field_definitions':FIELD_DEFINITIONS,'native_coordinates':404106,'original_rings':106,'invalid_geometries':0,'empty_geometries':0,'complete_original_106_ring_coordinate_bytes_preserved':True,'delegation':delegation,'source_semantic_metadata_sha256':digest(encode(before)),'source_record_sha256':records_hash(table),'release_date_unknown_reason':'Active official application distribution is observed; item creation/modification and XML creation do not establish source record release or currency.'}
    if audit['source_record_sha256']!=SOURCE_RECORDS_SHA256:raise EpaRegionsQuarantineError('source_snapshot_values_geometry_or_precision_changed_requires_review')
    return table,audit


def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Polygon','MultiPolygon'],'crs':pyproj.CRS(3857).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def canonical_values(table):
    return table.to_pylist()


def records_hash(table):
    h=hashlib.sha256()
    for row in canonical_values(table):row['geometry']=row['geometry'].hex();h.update(encode(row))
    return h.hexdigest()

def write_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='geometry',geometry_type='Unknown',crs='EPSG:3857',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'geometry'})
    with sqlite3.connect(path) as db:db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")

def verify_outputs(root,table):
    for relative in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if not pq.read_table(root/relative).equals(expected_parquet(table),check_metadata=True):raise EpaRegionsQuarantineError('parquet_full_values_physical_types_ids_wkb_or_metadata_drift')
    meta,actual=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs']!='EPSG:3857' or tuple(actual.column_names)!=FIELDS or tuple(f.type for f in actual.schema)!=TYPES or tuple(f.nullable for f in actual.schema)!=tuple(f.nullable for f in table.schema)or canonical_values(actual)!=canonical_values(table):raise EpaRegionsQuarantineError('gpkg_full_values_physical_types_ids_or_native_geometry_loss')
    with tempfile.TemporaryDirectory(prefix='epa-regions-events-fresh-container-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise EpaRegionsQuarantineError('gpkg_full_container_srs_headers_sql_or_index_drift')
def manifest_for(captures,identity,parquet_pin):
    ids=source_ids(strict_json(captures['ids_before'][0]));urls=request_plan(ids);inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,urls[name]);baseline=False
        inputs.append({'input_id':name+':'+digest(body),'producer':'HIFLD Next published candidate' if baseline else 'U.S. Environmental Protection Agency','host':urlsplit(urls[name]).hostname,'role':'archive' if baseline else 'producer_publication',
            'canonical_url':urls[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
            'attribution':'Unapproved complete published snapshot' if baseline else 'Official EPA application→StoryMap→webmap→EPA_Region layer0 distribution; exact SEGS continuation is unapproved.',
            'release_date_unknown_reason':'No verified release day. Current official application use, retrieval, XML creation and portal/inventory modification dates are not source record release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'EPA Regions — complete official application-distributed boundaries, unapproved draft','publisher':'EPA GeoPlatform account goins.jake_EPAEXT (official application distribution)','agency':'Environmental Protection Agency','source_url':BASE,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_native_EPA_region_polygon_extraction','parameters':{'authority':'Current EPA government application→StoryMap253b→webmap4bb→itemfd457→layer0, no source acceptance implied','completeness':'Before/after full OID rosters, counts and semantic metadata; exact ID-targeted batches','IDs':'Original FID/OBJECTID and their current region associations retained; all historical OBJECTID/id associations retained separately, never remapped or substituted','types':'Original actualTypeint/sqlTypeInteger int32 declarations, float64 source Double values, source text/width/nullability untouched; no numeric rounding','geometry':'Native3857 Polygon/MultiPolygon double rings; all106 ring sequences/404106coordinates exact, no reprojection/repair','metadata':'Complete original raw CAS/receipt context preserved; semantic field/data equality is type-sensitive; authority-view counters do not rebase first observations'}}],
        'unsupported_output_formats':{'shapefile_zip':'Source long field names/types cannot be mapped destructively without acceptance.','pmtiles':'Quantization changes native coordinates; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve every original physical source field/null/metadata and exact native WKB without generic bbox/typing additions. This capability grants no acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise EpaRegionsQuarantineError('linked_candidate_lock')
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
        raise EpaRegionsQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        # Validate all source and full manifest semantics BEFORE installing any
        # shared first-state record; never poison trust with a rejected claim.
        inspect_captures(captures)
        pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
        expected=manifest_for(captures,identity,{k:pin[k]for k in ('sha256','bytes')})
        if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):raise EpaRegionsQuarantineError('preseal_full_original_manifest_semantics_drift')
        validate_release_provenance(manifest);validate_output_policy(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise EpaRegionsQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise EpaRegionsQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if name in ('metadata_before','metadata_after') and encode(semantic_metadata(strict_json(body)))!=encode(semantic_metadata(strict_json(captures[name][0]))):raise EpaRegionsQuarantineError('original_source_semantics_changed')
        original[name]=(body,item['receipt'])
    original_table,original_audit=inspect_captures(original)
    current_table,current_audit=inspect_captures(captures)
    if not original_table.equals(current_table,check_metadata=True) or encode(original_audit)!=encode(current_audit):raise EpaRegionsQuarantineError('original_source_values_or_semantics_changed')
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    if encode({k:v for k,v in actual.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise EpaRegionsQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

def source_payloads(captures,identity,audit,manifest,table):
    proof={'status':'pass','approved':False,'rows':table.num_rows,'source_records_sha256':records_hash(table),'method':'Complete original JSON attributes/native source SQL types and every106 raw ring coordinate sequence→native WKB→every output value/ID/type/null and deterministic fullcontainer fidelity; no geometry or identifier substitution.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/epa_regions_source_audit.json':encode(audit),'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_generic_SEGS_to_official_application_source_ID_schema_geometry_precision_baseline_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}
    values['metadata/raw_input_refs.json']=encode({n:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{n}.body','receipt':rec}for n,(body,rec)in captures.items()})
    values['metadata/source_record_lineage.jsonl']=b''.join(encode({'original_FID':r['FID'],'original_OBJECTID':r['OBJECTID'],'original_EPAREGION':r['EPAREGION'],'source_page':'page_00000','source_geometry_JSON_path':f"features[{i}].geometry.rings"})for i,r in enumerate(table.to_pylist()))
    return values

def reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin,original_context=None):
    root=Path(storage.local_dir).absolute()/prefix
    if not (root/READY).is_file():raise EpaRegionsQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_context if original_context is not None else original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table);names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise EpaRegionsQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise EpaRegionsQuarantineError('unknown_missing_or_linked_candidate_object')
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise EpaRegionsQuarantineError('source_owned_report_wrapper_placeholder_or_raw_provenance_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise EpaRegionsQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise EpaRegionsQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise EpaRegionsQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table)

def stage_epa_regions(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    try:
        table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({'records':records_hash(table),'semantic_metadata':audit['source_semantic_metadata_sha256'],'source_delegation':audit['delegation']}))
        version=f'v0.0.0-draft-epa-regions-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir).absolute()
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            refuse_observed_loss(storage,version)
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise EpaRegionsQuarantineError('linked_candidate')
                    try:original_context=load_original_state(storage,version,captures,identity,table)
                    except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                        try:record_observed_loss(storage,version,fingerprint,str(loss))
                        except Exception as journal_error:raise IncidentWriteError('incident_write_failed_candidate_preserved') from journal_error
                        raise EpaRegionsQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
                    with tempfile.TemporaryDirectory(prefix='epa-regions-native-prebuilt-') as tmp:
                        fresh=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),fresh,compression='zstd');pin={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}
                    reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin,original_context)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise EpaRegionsQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            if original_state_present(storage,version):
                try:original_context=load_original_state(storage,version,captures,identity,table)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                    try:record_observed_loss(storage,version,fingerprint,str(loss))
                    except Exception as journal_error:raise IncidentWriteError('incident_write_failed_partial_state_preserved') from journal_error
                    raise EpaRegionsQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
            with tempfile.TemporaryDirectory(prefix='.epa-regions-building-',dir=target.parent) as tmp:
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
        retained=retain_failure(storage,captures,str(exc));raise EpaRegionsQuarantineError(str(exc)+'; retained:'+retained) from exc

def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise EpaRegionsQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise EpaRegionsQuarantineError('incomplete_or_changed_bounded_input')
    return body


def incident_directory(version):
    return f'_construction-state/{SLUG}/incident-{version}'

def refuse_observed_loss(storage,version):
    directory=Path(storage.local_dir).absolute()/incident_directory(version)
    if directory.exists():raise EpaRegionsQuarantineError('permanent_observed_original_trust_loss_requires_reviewed_operator_recovery')

def record_observed_loss(storage,version,fingerprint,reason):
    # Partial writes/pending files are themselves blocking negative evidence.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason})
    seal_first_construction(storage,key,version,fingerprint,body)

def original_state_present(storage,version):
    key=construction_record_key(version);base=Path(storage.local_dir).absolute()
    return (base/key).exists() or (base/(key+'.first-construction.json')).exists() or (base/(key+'.evidence')).exists()

def load_original_state(storage,version,captures,identity,table):
    with tempfile.TemporaryDirectory(prefix='epa-original-pin-') as tmp:
        path=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),path,compression='zstd');pin={'sha256':sha256_file(path),'bytes':path.stat().st_size}
    return original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))


