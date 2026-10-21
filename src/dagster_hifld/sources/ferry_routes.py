"""Complete actual official BTS NCFO routes draft, with unresolved producer vintage conflict.

Every source ID, attribute, declared type/null and native path coordinate is retained.
No baseline/source/contract or changed published mapping is approved.
"""
from __future__ import annotations

import collections
import math
import urllib.parse
import shutil
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import sqlite3
import stat
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _validate_cached_catalog_sidecars
from dagster_hifld.validation import sha256_file

SLUG = 'ferry-routes-1'
LAYER = 'BTS_NCFO_Ferry_Routes'
LANDING = 'https://geodata.bts.gov/datasets/usdot::ferry-routes/about'
ITEM = 'bc0f7dffb12d4ce9a47bf890d6c09804'
SERVICE = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Ferry_Routes/FeatureServer'
LAYER_URL = SERVICE+'/0'
LICENSE = 'https://www.arcgis.com/sharing/rest/content/items/'+ITEM+'?f=pjson'
URLS = {'bts_ntad':'https://www.bts.gov/ntad','official_landing':LANDING,
 'source_item':LICENSE,'source_layer_before':LAYER_URL+'?f=pjson',
 'source_count_before':LAYER_URL+'/query?where=1%3D1&returnCountOnly=true&f=json',
 'source_ids_before':LAYER_URL+'/query?where=1%3D1&returnIdsOnly=true&f=json'}
for first in range(1,965,100):
 URLS['source_page_'+str(first).zfill(4)]=LAYER_URL+'/query?'+urllib.parse.urlencode({'objectIds':','.join(map(str,range(first,min(first+100,965)))),'outFields':'*','returnGeometry':'true','returnTrueCurves':'true','returnZ':'true','returnM':'true','orderByFields':'OBJECTID ASC','f':'json'})
URLS.update(source_layer_after=LAYER_URL+'?f=pjson',source_count_after=LAYER_URL+'/query?where=1%3D1&returnCountOnly=true&f=json',source_ids_after=LAYER_URL+'/query?where=1%3D1&returnIdsOnly=true&f=json')
FIELD_DECLARATIONS = [{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'operator_id', 'type': 'esriFieldTypeString', 'alias': 'OPERATOR_ID', 'sqlType': 'sqlTypeOther', 'length': 3, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'segment_id', 'type': 'esriFieldTypeString', 'alias': 'segment_id', 'sqlType': 'sqlTypeOther', 'length': 4, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'seg_length', 'type': 'esriFieldTypeDouble', 'alias': 'seg_length', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'average_trip_time', 'type': 'esriFieldTypeInteger', 'alias': 'average_trip_time', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'segment_season_start', 'type': 'esriFieldTypeString', 'alias': 'segment_season_start', 'sqlType': 'sqlTypeOther', 'length': 5, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'segment_season_end', 'type': 'esriFieldTypeString', 'alias': 'segment_season_end', 'sqlType': 'sqlTypeOther', 'length': 5, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trips_per_year', 'type': 'esriFieldTypeInteger', 'alias': 'trips_per_year', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'route_rates_regulated', 'type': 'esriFieldTypeInteger', 'alias': 'route_rates_regulated', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'route_rate_regulator', 'type': 'esriFieldTypeString', 'alias': 'route_rate_regulator', 'sqlType': 'sqlTypeOther', 'length': 59, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'most_used_vessel_id', 'type': 'esriFieldTypeInteger', 'alias': 'most_used_vessel_id', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel1_id', 'type': 'esriFieldTypeInteger', 'alias': 'vessel1_id', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel2_id', 'type': 'esriFieldTypeInteger', 'alias': 'vessel2_id', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel3_id', 'type': 'esriFieldTypeInteger', 'alias': 'vessel3_id', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel4_id', 'type': 'esriFieldTypeString', 'alias': 'vessel4_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel5_id', 'type': 'esriFieldTypeString', 'alias': 'vessel5_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel6_id', 'type': 'esriFieldTypeString', 'alias': 'vessel6_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel7_id', 'type': 'esriFieldTypeString', 'alias': 'vessel7_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel8_id', 'type': 'esriFieldTypeString', 'alias': 'vessel8_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel9_id', 'type': 'esriFieldTypeString', 'alias': 'vessel9_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel10_id', 'type': 'esriFieldTypeString', 'alias': 'vessel10_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel11_id', 'type': 'esriFieldTypeString', 'alias': 'vessel11_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel12_id', 'type': 'esriFieldTypeString', 'alias': 'vessel12_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel13_id', 'type': 'esriFieldTypeString', 'alias': 'vessel13_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel14_id', 'type': 'esriFieldTypeString', 'alias': 'vessel14_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel15_id', 'type': 'esriFieldTypeString', 'alias': 'vessel15_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel16_id', 'type': 'esriFieldTypeString', 'alias': 'vessel16_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel17_id', 'type': 'esriFieldTypeString', 'alias': 'vessel17_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel18_id', 'type': 'esriFieldTypeString', 'alias': 'vessel18_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel19_id', 'type': 'esriFieldTypeString', 'alias': 'vessel19_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel20_id', 'type': 'esriFieldTypeString', 'alias': 'vessel20_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel21_id', 'type': 'esriFieldTypeString', 'alias': 'vessel21_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel22_id', 'type': 'esriFieldTypeString', 'alias': 'vessel22_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel23_id', 'type': 'esriFieldTypeString', 'alias': 'vessel23_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel24_id', 'type': 'esriFieldTypeString', 'alias': 'vessel24_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel25_id', 'type': 'esriFieldTypeString', 'alias': 'vessel25_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vessel26_id', 'type': 'esriFieldTypeString', 'alias': 'vessel26_id', 'sqlType': 'sqlTypeOther', 'length': 254, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'passengers', 'type': 'esriFieldTypeInteger', 'alias': 'passengers', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'vehicles', 'type': 'esriFieldTypeInteger', 'alias': 'vehicles', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'avg_daily_brd_pax', 'type': 'esriFieldTypeInteger', 'alias': 'avg_daily_brd_pax', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'avg_daily_brd_veh', 'type': 'esriFieldTypeInteger', 'alias': 'avg_daily_brd_veh', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'census_year', 'type': 'esriFieldTypeInteger', 'alias': 'census_year', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'data_year', 'type': 'esriFieldTypeInteger', 'alias': 'data_year', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'operator_name', 'type': 'esriFieldTypeString', 'alias': 'operator_name', 'sqlType': 'sqlTypeOther', 'length': 91, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'segment_name', 'type': 'esriFieldTypeString', 'alias': 'segment_name', 'sqlType': 'sqlTypeOther', 'length': 91, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'seg_terminal1_id', 'type': 'esriFieldTypeString', 'alias': 'seg_terminal1_id', 'sqlType': 'sqlTypeOther', 'length': 4, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'seg_terminal2_id', 'type': 'esriFieldTypeString', 'alias': 'seg_terminal2_id', 'sqlType': 'sqlTypeOther', 'length': 4, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_commuter_transit', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_COMMUTER_TRANSIT', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_pleasure', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_PLEASURE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_emergency', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_EMERGENCY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_roadway_conn', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_ROADWAY_CONN', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_lifeline', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_LIFELINE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_nps', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_NPS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'trip_purpose_other', 'type': 'esriFieldTypeInteger', 'alias': 'TRIP_PURPOSE_OTHER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'Shape__Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape__Length', 'sqlType': 'sqlTypeDouble', 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None}]
FIELDS = tuple(f['name'] for f in FIELD_DECLARATIONS)+('wkb_geometry',)
TYPE_MAP = {'esriFieldTypeOID':pa.int64(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
TYPES = tuple(TYPE_MAP[f['type']]for f in FIELD_DECLARATIONS)+(pa.binary(),)
SOURCE_FEATURES_SHA256 = '89c1fbe11427e17cc1f047d92ab2a0e921a39e714c540f2b23b0e664785eb2e8'
PARQUET_PIN = {'sha256': '75c492a658c2ffdc9f7b47d0cb3104b59b1443d473e294110ca870305ee1ef8c', 'bytes': 10140624}
READY = 'metadata/ferry_routes_candidate_ready.json'

class FerryRouteQuarantineError(ValueError):
    """Changed or incomplete source/cache requires review, never replacement."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise FerryRouteQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise FerryRouteQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if shutil.disk_usage(Path(storage.local_dir) if storage.local_dir and Path(storage.local_dir).exists() else Path('/workspace')).free<300_000_000: raise FerryRouteQuarantineError('local_space_guard')
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise FerryRouteQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir).absolute()
    if any(path.is_symlink() for path in [root,*root.parents]):
        raise FerryRouteQuarantineError('staging_path_symlink_refused')
    if root.exists():
        for path in root.rglob('*'):
            info=path.lstat()
            if stat.S_ISDIR(info.st_mode):continue
            if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise FerryRouteQuarantineError('staging_alias_or_special_file_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise FerryRouteQuarantineError('immutable_capture_changed:'+key)


def quarantine(storage,captures,reason):
    key=f'_quarantine/{SLUG}/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,key+'/'+name+'.body',body)
        immutable(storage,key+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,key+'/reason.json',encode({'approved':False,'reason':reason}))
    return key


def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}


def validate_receipt(name,body,receipt):
    url=URLS[name]
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body) or receipt.get('sha256')!=digest(body) or receipt.get('complete_body')is not True):
        raise FerryRouteQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'Bureau of Transportation Statistics, U.S. Department of Transportation',
        'host':urllib.parse.urlsplit(url).hostname,'role':'producer_publication'if name=='bts_ntad'else'official_distributor',
        'canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt.get('observed_at'),
        'distributor':'BTS (USDOT_BTS) through Esri ArcGIS Online','delegation_evidence':{'bts_page_url':'https://www.bts.gov/ntad','portal_dataset_url':LANDING,'publisher_account':'USDOT_BTS','service_item':ITEM},
        'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':'BTS National Transportation Atlas Database / National Census of Ferry Operators; acknowledges Census Bureau, USGS, NOAA and USACE.',
        'release_date_unknown_reason':'Producer item/Hub description claims NCFO2022 compilation2025-10-27, but layer description names NCFO2020 compilation2024-10-16 and all actual rows are census2020/data2019. Exact release and current-vintage reconciliation require producer/maintainer decision.'}


def manifest_for(captures,identity):
    if set(captures)!=set(URLS):raise FerryRouteQuarantineError('missing_extra_source_publication_roster')
    inputs=[validate_receipt(name,*captures[name])for name in URLS]
    if b'https://geodata.bts.gov/' not in captures['bts_ntad'][0] or ITEM.encode()not in captures['official_landing'][0]:
        raise FerryRouteQuarantineError('official_BTS_delegation_missing')
    item=strict_json(captures['source_item'][0])
    if item.get('id')!=ITEM or item.get('owner')!='USDOT_BTS' or item.get('orgId')!='xOi1kZaI0eWDREZv' or item.get('url')!=SERVICE or item.get('access')!='public':
        raise FerryRouteQuarantineError('official_publisher_item_service_delegation_drift')
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Ferry Routes — complete actual official NCFO2020 service draft, unresolved item-vintage conflict',
        'publisher':'Bureau of Transportation Statistics','agency':'U.S. Department of Transportation','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_native_Esri_polyline_extraction','parameters':{'roster':'Before/after964 original OBJECTIDs and55 exact original attribute declarations; ten explicit ID-bounded pages; no outSR, quantization or geometry precision request.',
            'physical_types':'Esri Integer retained int32, Double float64, String exact/null distinct. OID original declaration retained; Arrow int64 is an explicit storage representation, not approval of physical-width mapping.',
            'geometry':'Every original path and binary64 coordinate retained in WKB MultiLineString, including one-part path structure. No snapping, reprojection, repair or ID mapping.',
            'vintage':'Actual census_year2020/data_year2019, NOT claimed2022 contents. Producer item/layer conflict retained and unapproved.'}},
            {'operation':'native_geopackage_and_geoparquet','parameters':{'gpkg_storage_fid':'Separate internal storage index; original OBJECTID and all business IDs preserved, no substitute published ID.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Retain all source attributes, native integer widths, original IDs and exact native path coordinates; no generic bbox/schema expansion accepted.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**PARQUET_PIN}]}}},
        'unsupported_output_formats':{'shapefile_zip':'55 exact original field names and null/string/type distinctions have no approved destructive shapefile mapping.',
            'pmtiles':'Quantized visualization and ID/type mapping remain unapproved.'}}
    validate_release_provenance(manifest);return manifest


def bounded_input(path,limit):
    if '..'in path.parts or any(p.is_symlink()for p in[path.parent,*path.parent.parents]):raise FerryRouteQuarantineError('input_parent_alias_refused')
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb')as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise FerryRouteQuarantineError('unsafe_or_oversized_regular_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise FerryRouteQuarantineError('changed_or_incomplete_bounded_input')
    return body


def candidate_bytes(root,name):
    limit=32_000_000 if name.endswith(('.gpkg','.parquet'))else(15_000_000 if Path(name).name.startswith('source_page_')and name.endswith('.body')else 2_000_000)
    if Path(name).is_absolute()or '..'in Path(name).parts:raise FerryRouteQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,limit)


def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise FerryRouteQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names


def acquire(storage,capture_dir=None):
    local_only(storage);captures={}
    try:
        for name in URLS:
            if capture_dir is not None:
                body=bounded_input(Path(capture_dir)/(name+'.body'),15_000_000)
                # Retain attempted bytes before any receipt decoding. A malformed
                # local receipt is evidence, not permission to lose its body.
                key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
                immutable(storage,key,body)
                diagnostic={'url':URLS[name],'body_key':key,'bytes':len(body),
                    'sha256':digest(body),'complete_body':False,'receipt_validated':False}
                captures[name]=(body,diagnostic)
                if len(body)>15_000_000: raise FerryRouteQuarantineError('capture_body_limit')
                raw_receipt=bounded_input(Path(capture_dir)/(name+'.receipt.json'),65_536)
                receipt_key=key+'.receipt-original-'+digest(raw_receipt)+'.bin'
                immutable(storage,receipt_key,raw_receipt)
                diagnostic['original_receipt_key']=receipt_key
                if len(raw_receipt)>65_536:raise FerryRouteQuarantineError('capture_receipt_limit')
                receipt=strict_json(raw_receipt)
                captures[name]=(body,receipt)
            else:
                url=URLS[name]
                limit=15_000_000 if name.startswith('source_page_') else 2_000_000
                for attempt in range(3):
                    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                    try:
                        with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',url) as response:
                            receipt.update(status=response.status_code,final_url=str(response.url),
                                requested_accept_encoding='identity',content_encoding=response.headers.get('content-encoding'),
                                content_length=response.headers.get('content-length'))
                            try:
                                for block in response.iter_bytes(None):
                                    body+=block
                                    if len(body)>limit: raise FerryRouteQuarantineError('oversized_or_changed_source')
                            finally:
                                receipt['wire_bytes_downloaded']=getattr(response,'num_bytes_downloaded',None)
                            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                            # HTTP Content-Length measures the encoded transport body.
                            # iter_bytes transparently decodes Content-Encoding, even
                            # when the server ignores our identity request.
                            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):
                                raise FerryRouteQuarantineError('non200_or_incomplete_http_response')
                        captures[name]=(bytes(body),receipt);break
                    except (httpx.HTTPError,OSError,ValueError) as exc:
                        receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
                        quarantine(storage,{name:(bytes(body),receipt)},str(exc))
                        if attempt==2: raise FerryRouteQuarantineError('bounded_fetch_exhausted:'+str(exc)) from exc
        return captures
    except (OSError,ValueError,TypeError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise FerryRouteQuarantineError(f'{exc}; retained:{where}') from exc


def inspect_originals(captures):
    if set(captures)!=set(URLS):raise FerryRouteQuarantineError('missing_extra_source_publication_roster')
    docs={name:strict_json(body)for name,(body,_)in captures.items()if name not in ('bts_ntad','official_landing')}
    before=docs['source_layer_before'];after=docs['source_layer_after']
    for layer in [before,after]:
        if (encode(layer.get('fields'))!=encode(FIELD_DECLARATIONS) or layer.get('objectIdField')!='OBJECTID' or layer.get('geometryType')!='esriGeometryPolyline'
            or layer.get('hasM')is not False or layer.get('hasZ')is not False or encode(layer.get('spatialReference'))!=encode({'wkid':4326,'latestWkid':4326})
            or layer.get('serviceItemId')!=ITEM or layer.get('maxRecordCount',0)<100):
            raise FerryRouteQuarantineError('native_source_schema_types_crs_or_service_drift')
    if before.get('editingInfo')!=after.get('editingInfo'):raise FerryRouteQuarantineError('source_changed_during_extraction')
    for part in ['before','after']:
        count=docs['source_count_'+part];ids=docs['source_ids_'+part]
        roster=ids.get('objectIds')
        if (type(count.get('count'))is not int or count['count']!=964 or ids.get('objectIdFieldName')!='OBJECTID'
            or type(roster)is not list or len(roster)!=964 or any(type(value)is not int for value in roster)
            or len(set(roster))!=964 or sorted(roster)!=list(range(1,965))):
            raise FerryRouteQuarantineError('count_id_roster_completeness_or_coverage_drift')
    features=[]
    for first in range(1,965,100):
        page=docs['source_page_'+str(first).zfill(4)]
        if (page.get('exceededTransferLimit',False)is not False or page.get('objectIdFieldName')!='OBJECTID' or page.get('geometryType')!='esriGeometryPolyline'
            or encode(page.get('spatialReference'))!=encode({'wkid':4326,'latestWkid':4326}) or encode(page.get('fields'))!=encode([{k:v for k,v in field.items()if k not in('nullable','editable','isViewOverride')}for field in FIELD_DECLARATIONS])):
            raise FerryRouteQuarantineError('truncated_wrong_schema_or_coordinate_system_page')
        rows=page.get('features')
        if not isinstance(rows,list)or [x.get('attributes',{}).get('OBJECTID')for x in rows]!=list(range(first,min(first+100,965))):
            raise FerryRouteQuarantineError('missing_duplicate_extra_or_unordered_page_ids')
        features.extend(rows)
    if digest(encode(features))!=SOURCE_FEATURES_SHA256:raise FerryRouteQuarantineError('complete_original_attribute_geometry_or_coverage_values_drift')
    rows=[];vertices=0;parts=0
    for feature in features:
        if set(feature)!={'attributes','geometry'}or set(feature['attributes'])!=set(FIELDS[:-1]):raise FerryRouteQuarantineError('extra_missing_attribute_or_feature_members')
        row=dict(feature['attributes'])
        for field in FIELD_DECLARATIONS:
            name=field['name'];value=row[name];kind=field['type']
            if value is None:
                if not field['nullable']:raise FerryRouteQuarantineError('nonnull_source_identifier_missing')
            elif kind=='esriFieldTypeString':
                if not isinstance(value,str)or len(value)>field['length']:raise FerryRouteQuarantineError('source_string_type_or_declared_width_drift')
            elif kind in ('esriFieldTypeOID','esriFieldTypeInteger'):
                if type(value)is not int or(kind=='esriFieldTypeInteger'and not -2**31<=value<2**31):raise FerryRouteQuarantineError('source_integer_type_or_width_drift')
            elif type(value)not in(int,float)or not math.isfinite(value):raise FerryRouteQuarantineError('source_double_type_or_value_drift')
        geometry=feature['geometry']
        if not isinstance(geometry,dict)or set(geometry)!={'paths'}or not isinstance(geometry['paths'],list)or not geometry['paths']:raise FerryRouteQuarantineError('null_curve_or_changed_polyline_geometry')
        paths=geometry['paths']
        for path in paths:
            if len(path)<2 or any(not isinstance(p,list)or len(p)!=2 or any(type(v)not in(int,float)or not math.isfinite(v)for v in p)for p in path):raise FerryRouteQuarantineError('incomplete_or_changed_native_coordinates')
            vertices+=len(path)
        parts+=len(paths)
        row['wkb_geometry']=shapely.to_wkb(shapely.MultiLineString(paths),byte_order=1);rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['nullable'],metadata={b'original_esri_field':encode(f)})for f in FIELD_DECLARATIONS]+[pa.field('wkb_geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist(rows,schema=schema);validate_table(table)
    return table,{'original_fields':FIELD_DECLARATIONS,'feature_count':964,'all_source_attributes_ids_types_nulls_and_native_paths_retained':True,
        'all_original_attribute_geometry_values_sha256':SOURCE_FEATURES_SHA256,'native_vertices':vertices,'native_path_count':parts,
        'operator_count':len(set(table['operator_id'].to_pylist())),'census_year_counts':dict(collections.Counter(table['census_year'].to_pylist())),
        'data_year_counts':dict(collections.Counter(table['data_year'].to_pylist())),'source_layer_editing_info':before['editingInfo'],
        'item_description':docs['source_item']['description'],'layer_description':before['description'],
        'unresolved_producer_vintage_conflict':True,'scope':'Complete official NCFO responding-operator route universe, not every possible ferry route.'}


def validate_table(table):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=964:
        raise FerryRouteQuarantineError('native_schema_physical_types_or_completeness_drift')
    if table['OBJECTID'].to_pylist()!=list(range(1,965))or table['OBJECTID'].null_count or set(table['census_year'].to_pylist())!={2020}or set(table['data_year'].to_pylist())!={2019}:
        raise FerryRouteQuarantineError('original_ids_or_actual_vintage_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if set(shapely.get_type_id(geoms).tolist())!={5}or any(shapely.is_empty(geoms))or not all(shapely.is_valid(geoms)):
        raise FerryRouteQuarantineError('empty_invalid_or_changed_native_geometry')
    return table


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{'encoding':'WKB','geometry_types':['MultiLineString'],'crs':pyproj.CRS(4326).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_native_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='wkb_geometry',geometry_type='MultiLineString',crs='EPSG:4326',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path) as db: db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table):
    expected=expected_parquet_table(table)
    parquet=pq.read_table(root/'geoparquet/ferry-routes-1.parquet')
    if sha256_file(root/'geoparquet/ferry-routes-1.parquet')!=PARQUET_PIN['sha256'] or (root/'geoparquet/ferry-routes-1.parquet').stat().st_size!=PARQUET_PIN['bytes'] or not parquet.equals(expected,check_metadata=True): raise FerryRouteQuarantineError('cached_parquet_values_types_or_metadata_disagree_with_fresh_original')
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if gm['crs']!='EPSG:4326' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():
        raise FerryRouteQuarantineError('cached_geopackage_values_types_or_wkb_disagree_with_fresh_original')
    with tempfile.TemporaryDirectory(prefix='ferry-routes-canonical-gpkg-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise FerryRouteQuarantineError('cached_geopackage_full_container_disagrees_with_fresh_original')


def source_payloads(captures,identity,table,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/ferry_routes_source_audit.json':encode({'approved':False,'feature_count':964,'crs':'EPSG:4326','invalid_geometries':0,'native_multilinestrings':964,
            'complete_original_publication_audit':audit,'all_values_ids_declared_types_native_paths_roundtrip':True,
            'source_relationship':'Actual official2020/2019 NCFO feed; item claims2022. Current edition, source/published ID mapping and full schema/geometry/coverage compatibility remain unapproved.'}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['producer_item_layer_vintage_conflict_unresolved','baseline_source_schema_id_geometry_mapping_not_approved']})}
    for name,(body,receipt)in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,table,audit,captures,identity):
    root=Path(storage.local_dir)/prefix;actual=candidate_roster(root);ready=strict_json(candidate_bytes(root,READY))
    cached={name:(candidate_bytes(root,f'raw/captures/{name}.body'),strict_json(candidate_bytes(root,f'raw/captures/{name}.receipt.json'))) for name in URLS}
    if digest(encode([strict_json(cached[name][0])['features']for name in URLS if name.startswith('source_page_')]))!=digest(encode([strict_json(captures[name][0])['features']for name in URLS if name.startswith('source_page_')])): raise FerryRouteQuarantineError('cached_original_publication_changed')
    manifest=manifest_for(cached,identity)
    actual_manifest=strict_json(candidate_bytes(root,'metadata/source_manifest.json'))
    validate_release_provenance(actual_manifest)
    for key,value in manifest.items():
        if key!='code_commit' and actual_manifest.get(key)!=value: raise FerryRouteQuarantineError('cached_provenance_disagrees_with_original')
    pin_key=f'_source-provenance/{SLUG}/{digest(encode(identity))}/bundle.json'
    pinned=strict_json(storage.read_key(pin_key))
    if actual_manifest!=pinned['manifest']:raise FerryRouteQuarantineError('cached_provenance_disagrees_with_immutable_original_bundle')
    manifest=actual_manifest
    expected=source_payloads(cached,identity,table,audit,manifest)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg','geoparquet/ferry-routes-1.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1] or ready.get('approved') is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready['objects'])!=expected_names):
        raise FerryRouteQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if candidate_bytes(root,name)!=value: raise FerryRouteQuarantineError('cached_source_owned_bytes_drift:'+name)
    snapshot='metadata/source/version_'+digest(candidate_bytes(root,'metadata/source_manifest.json'))+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    if actual-(expected_names|{READY}|derivatives) or not (expected_names|{READY}).issubset(actual):
        raise FerryRouteQuarantineError('cached_unknown_or_missing_object')
    source_manifest_snapshots={f'{prefix}/metadata/source/source_manifest.json',f'{prefix}/{snapshot}'}
    found_source_manifest_snapshots={f'{prefix}/{name}' for name in actual}&source_manifest_snapshots
    if found_source_manifest_snapshots not in (set(),source_manifest_snapshots):
        raise FerryRouteQuarantineError('cached_catalog_source_snapshot_incomplete')
    _validate_cached_catalog_sidecars(storage,prefix,{f'{prefix}/{name}' for name in actual})
    for name,pin in ready['objects'].items():
        if digest(candidate_bytes(root,name))!=pin: raise FerryRouteQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists() and candidate_bytes(root,name)!=candidate_bytes(root,'metadata/source_manifest.json'): raise FerryRouteQuarantineError('cached_source_snapshot_drift')
    for name in actual-expected_names-{READY}:candidate_bytes(root,name)
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs'] or strict_json(candidate_bytes(root,'metadata/resolved_provenance.json'))!=resolved:
            raise FerryRouteQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table)



def construction_history(storage,fingerprint):
    """Independent first-use evidence must survive whole primary quarantine."""
    state=f'_construction-state/{SLUG}/{fingerprint}.json'
    evidence=f'_construction-evidence/{SLUG}/{fingerprint}/'
    return storage.object_exists(state) or bool(storage.list_prefix(evidence))


def original_provenance_bundle(storage,fingerprint,captures,identity,manifest,*,create=False):
    """Bind first construction through independent immutable state and full CAS.

    Neither loss/quarantine of the primary alias nor repeated retries can turn a
    previously used namespace into a fresh construction. Trust restoration is an
    explicit operator action using exact retained original evidence, never a
    reconstruction of trust from candidate labels or later observations.
    """
    key=f'_source-provenance/{SLUG}/{fingerprint}/bundle.json'
    state_key=f'_construction-state/{SLUG}/{fingerprint}.json'
    evidence_prefix=f'_construction-evidence/{SLUG}/{fingerprint}/'
    version=f'v0.0.0-draft-bts-ncfo2020-{fingerprint[:16]}'
    if not storage.object_exists(key):
        if construction_history(storage,fingerprint):
            raise FerryRouteQuarantineError('missing_original_provenance_bundle_after_first_construction')
        if not create:raise FerryRouteQuarantineError('missing_original_provenance_bundle')
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        bundle={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt}for name,(body,receipt)in captures.items()}}
        body=encode(bundle);evidence_key=evidence_prefix+digest(body)+'.json'
        # A complete verified private candidate precedes all trust records.
        # Preserve the complete original first, including partial write errors.
        immutable(storage,evidence_key,body)
        immutable(storage,state_key,encode({'schema_version':1,'version':version,'processing_fingerprint':fingerprint,
            'original_bundle_sha256':digest(body),'original_bundle_evidence_key':evidence_key}))
        immutable(storage,key,body)
    bundle_body=storage.read_key(key);bundle=strict_json(bundle_body)
    if set(bundle)!={'manifest','captures'}or set(bundle['captures'])!=set(URLS):
        raise FerryRouteQuarantineError('original_provenance_bundle_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        receipt=item['receipt'];expected=f"_raw-captures/{SLUG}/{receipt['sha256']}/{name}.body"
        if item['body_key']!=expected:raise FerryRouteQuarantineError('original_provenance_bundle_body_key_drift')
        original[name]=(storage.read_key(expected),receipt)
    expected_manifest=manifest_for(original,identity)
    for name,value in expected_manifest.items():
        if name!='code_commit'and bundle['manifest'].get(name)!=value:
            raise FerryRouteQuarantineError('original_provenance_bundle_manifest_drift')
    validate_release_provenance(bundle['manifest'])
    validate_construction_code_identity(Path(__file__).parents[3],bundle['manifest']['code_commit'],identity['files'])
    evidence_key=evidence_prefix+digest(bundle_body)+'.json'
    expected_state={'schema_version':1,'version':version,'processing_fingerprint':fingerprint,
        'original_bundle_sha256':digest(bundle_body),'original_bundle_evidence_key':evidence_key}
    if not storage.object_exists(state_key)or storage.read_key(state_key)!=encode(expected_state):
        raise FerryRouteQuarantineError('missing_or_invalid_first_construction_state')
    if not storage.object_exists(evidence_key)or storage.read_key(evidence_key)!=bundle_body:
        raise FerryRouteQuarantineError('missing_or_invalid_original_construction_evidence')
    return original,bundle['manifest']


def stage_ferry_routes(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    root=None
    try:
        identity=processing_identity();manifest=manifest_for(captures,identity)
        fingerprint=digest(encode(identity));version=f'v0.0.0-draft-bts-ncfo2020-{fingerprint[:16]}'
        prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir);root=base/prefix
        depot={}
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
            immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt));depot[name]=base/key
        table,audit=inspect_originals(captures)
        locks=base/'_locks';locks.mkdir(exist_ok=True);lockpath=locks/(SLUG+'.lock')
        with lockpath.open('a+b') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            binding=base/f'_source-provenance/{SLUG}/{fingerprint}'
            if root.exists():
                invalid_bundle=False
                try:
                    try:
                        captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest)
                        # Historical candidate reports must be derived from its
                        # original captures, not a later metadata observation.
                        table,audit=inspect_originals(captures)
                    except (ValueError,TypeError,KeyError,OSError,RuntimeError):
                        invalid_bundle=True
                        raise
                    reconcile_cache(storage,prefix,table,audit,captures,identity)
                except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
                    incident=uuid4().hex
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+incident)
                    target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    # A bad external anchor is evidence, not a basis for the
                    # next candidate. Preserve it intact beside the detached
                    # version while keeping genuine valid original bindings.
                    if invalid_bundle and binding.exists():
                        os.rename(binding,target.parent/('invalid-provenance-'+incident))
                    raise FerryRouteQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':964,'reused':True}
            # A previous valid construction may survive a rejected candidate.
            # Read its binding but never create an anchor until this private
            # reconstruction has passed the full native output verification.
            if binding.exists() or construction_history(storage,fingerprint):
                try:
                    captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest)
                    table,audit=inspect_originals(captures)
                except (ValueError,TypeError,KeyError,OSError,RuntimeError) as exc:
                    target=base/'_quarantine'/SLUG/('invalid-provenance-'+uuid4().hex)
                    target.parent.mkdir(parents=True,exist_ok=True)
                    if binding.exists():os.rename(binding,target)
                    raise FerryRouteQuarantineError('original_construction_trust_requires_exact_restore:'+str(exc)) from exc
            build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='candidate-',dir=build) as tmp:
                temp=Path(tmp)
                for part in ('geopackage','geoparquet','metadata','raw'): (temp/part).mkdir()
                write_native_gpkg(table,temp/'geopackage'/f'{SLUG}.gpkg')
                pq.write_table(expected_parquet_table(table),temp/'geoparquet/ferry-routes-1.parquet',compression='zstd')
                reconcile_outputs(temp,table)
                for name,value in source_payloads(captures,identity,table,audit,manifest).items():
                    (temp/name).parent.mkdir(parents=True,exist_ok=True);(temp/name).write_bytes(value)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p) for p in temp.rglob('*') if p.is_file()}}
                (temp/READY).write_bytes(encode(ready))
                # Only a fully verified private construction can establish a
                # first anchor. Existing caches can never recreate a lost one.
                original_provenance_bundle(storage,fingerprint,captures,identity,manifest,create=True)
                root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':964,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,shapely.errors.GEOSException) as exc:
        where=quarantine(storage,captures,str(exc))
        raise FerryRouteQuarantineError(f'{exc}; retained:{where}') from exc
