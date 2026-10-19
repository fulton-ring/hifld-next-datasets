"""Complete official NACEI 99-point North America draft; NOT the EIA US51 release.

No filtering, source substitution acceptance, casts, ID mapping or publication.
All native attributes/IDs/points and separate pinned US51 baselines survive.
"""
from __future__ import annotations
import collections
import shutil
from dataclasses import asdict
from dagster_hifld.http_capture import capture_http_response
import io
import fcntl
import hashlib
import importlib.metadata
import json
import math
import re
import os
import platform
import sqlite3
import stat
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
SLUG='border-crossings-natural-gas'
LAYER='natural_gas_border_crossings'
SERVICE='https://geoappext.nrcan.gc.ca/arcgis/rest/services/NACEI/energy_infrastructure_of_north_america_en/MapServer'
BASE=SERVICE+'/2'
AUTHORITY='https://open.canada.ca/data/en/dataset/e313db89-4219-4a5e-a543-772a86068710'
PROGRAM='https://www.eia.gov/Todayinenergy/detail.php?id=33952'
LICENSE='NACEI August2017 producer metadata retained; source/rights/grain/coverage/IDs/schema/baseline/contract/release unapproved.'
READY='metadata/natural_gas_border_crossings_candidate_ready.json'
BATCH=100
MAX_ROWS=1000
MAX_CAPTURE_BYTES=4*1024*1024
MAX_BODY=1024*1024
FREE_FLOOR=300_000_000
FIELD_DEFINITIONS=[{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'domain': None}, {'name': 'Country', 'type': 'esriFieldTypeString', 'alias': 'Country', 'length': 15, 'domain': None}, {'name': 'Pipeline', 'type': 'esriFieldTypeString', 'alias': 'Name of Pipeline', 'length': 150, 'domain': None}, {'name': 'Owner', 'type': 'esriFieldTypeString', 'alias': 'Owner Name (Company)', 'length': 150, 'domain': None}, {'name': 'Latitude', 'type': 'esriFieldTypeDouble', 'alias': 'Latitude', 'domain': None}, {'name': 'Longitude', 'type': 'esriFieldTypeDouble', 'alias': 'Longitude', 'domain': None}, {'name': 'City', 'type': 'esriFieldTypeString', 'alias': 'City (Site Name)', 'length': 150, 'domain': None}, {'name': 'County', 'type': 'esriFieldTypeString', 'alias': 'County', 'length': 50, 'domain': None}, {'name': 'StateProv', 'type': 'esriFieldTypeString', 'alias': 'State / Province / Territory', 'length': 50, 'domain': None}, {'name': 'ZipCode', 'type': 'esriFieldTypeString', 'alias': 'Zip Code / Postal Code', 'length': 7, 'domain': None}, {'name': 'Address', 'type': 'esriFieldTypeString', 'alias': 'Address', 'length': 150, 'domain': None}, {'name': 'FrmState', 'type': 'esriFieldTypeString', 'alias': 'State or Province From', 'length': 50, 'domain': None}, {'name': 'FrmCountry', 'type': 'esriFieldTypeString', 'alias': 'Country From', 'length': 15, 'domain': None}, {'name': 'ToState', 'type': 'esriFieldTypeString', 'alias': 'State or Province To', 'length': 50, 'domain': None}, {'name': 'ToCountry', 'type': 'esriFieldTypeString', 'alias': 'Country To', 'length': 15, 'domain': None}, {'name': 'NumPipes', 'type': 'esriFieldTypeDouble', 'alias': 'Number of Pipes', 'domain': None}, {'name': 'Diam_Inch', 'type': 'esriFieldTypeString', 'alias': 'Outside Diameter (inches)', 'length': 100, 'domain': None}, {'name': 'MaxOP_psi', 'type': 'esriFieldTypeString', 'alias': 'Maximum Operating Pressure (psi)', 'length': 100, 'domain': None}, {'name': 'Vol_MMcfd', 'type': 'esriFieldTypeString', 'alias': 'Volume (MMcf/d)', 'length': 100, 'domain': None}, {'name': 'Source', 'type': 'esriFieldTypeString', 'alias': 'Source Agency', 'length': 150, 'domain': None}, {'name': 'Period', 'type': 'esriFieldTypeInteger', 'alias': 'Reference Period', 'domain': None}, {'name': 'Diam_mm', 'type': 'esriFieldTypeString', 'alias': 'Outside Diameter (mm)', 'length': 100, 'domain': None}, {'name': 'MaxOP_kPa', 'type': 'esriFieldTypeString', 'alias': 'Maximum Operating Pressure (kPa)', 'length': 100, 'domain': None}, {'name': 'Vol_km3d', 'type': 'esriFieldTypeString', 'alias': 'Volume (km³/d)', 'length': 100, 'domain': None}]
PINS={'baseline_file_geodatabase': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/file_geodatabase/border-crossings-natural-gas-file_geodatabase.zip', 'bytes': 37712, 'sha256': '2a1ad4c5fcc4b186727a67b0d203a21789001af42112160a624d6bf4c1bfb8ae'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/geojson/border-crossings-natural-gas-geojson.geojson', 'bytes': 24919, 'sha256': 'c3e276270e3019e7b2dfad98c7f13d90f87d0eed07c5937881672a72ff5f9399'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/geopackage/border-crossings-natural-gas-geopackage.gpkg', 'bytes': 135168, 'sha256': '0d23b98d769167622a85159effde1dcd6e36b9fcd269c9eae74a5e1d59392949'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/geoparquet/border-crossings-natural-gas.parquet', 'bytes': 22080, 'sha256': 'daf25b473e7b23c1c515604f4d2911c61fc849945cf377643562fd726bb55370'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/pmtiles/border-crossings-natural-gas.pmtiles', 'bytes': 13925, 'sha256': '42c41960f41f9f3fbe6ed76a305361be05123c8789103294bdf0a91952e98d20'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/shapefile/border-crossings-natural-gas.zip', 'bytes': 5354, 'sha256': '422dfb523ed26ddd9a52899751c34a828f5a7e30f7f7ee7cef4ff7a8f77474e2'}, 'baseline_collection': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/border-crossings-natural-gas/border-crossings-natural-gas/v1.0.0/collection.json', 'bytes': 11401, 'sha256': '7aa599e74e3bc55135e5dc0c7c9a28df79cc07f6fb425fa7929ac065d0f6897a'}}
FIELDS=tuple(f['name'] for f in FIELD_DEFINITIONS)+('geometry',)
TYPE_MAP={'esriFieldTypeOID':pa.int64(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
TYPES=tuple(TYPE_MAP[f['type']] for f in FIELD_DEFINITIONS)+(pa.binary(),)
INITIAL_COUNT=99

class IncidentWriteError(Exception):pass

class NaturalGasCrossingsQuarantineError(ValueError):
    """Untrusted/incomplete input or cache requires review; never release."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise NaturalGasCrossingsQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(NaturalGasCrossingsQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise NaturalGasCrossingsQuarantineError('json_object_required')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise NaturalGasCrossingsQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise NaturalGasCrossingsQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise NaturalGasCrossingsQuarantineError('linked_staging_object')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise NaturalGasCrossingsQuarantineError('immutable_capture_changed:'+key)

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
    urls={'authority_catalog':AUTHORITY,'authority_program':PROGRAM,
        'metadata_before':BASE+'?f=pjson','count_before':query_url(where='1=1',returnCountOnly='true'),
        'ids_before':query_url(where='1=1',returnIdsOnly='true')}
    if ids is not None:
        for offset in range(0,len(ids),BATCH):
            urls[f'page_{offset:05}']=query_url(objectIds=','.join(map(str,ids[offset:offset+BATCH])),outFields='*',returnGeometry='true',orderByFields='OBJECTID')
        urls.update(metadata_after=BASE+'?f=pjson',count_after=query_url(where='1=1',returnCountOnly='true'),ids_after=query_url(where='1=1',returnIdsOnly='true'))
        urls.update({name:pin['url']for name,pin in PINS.items()})
    return urls

def retain_failure(storage,captures,reason,attempted=None):
    prefix=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,prefix+'/'+name+'.body',body);immutable(storage,prefix+'/'+name+'.receipt.json',encode(receipt))
    if attempted is not None:immutable(storage,prefix+'/attempted-inputs.json',encode({'approved':False,'receipt_metadata_validated':False,'objects':attempted}))
    immutable(storage,prefix+'/reason.json',encode({'approved':False,'reason':reason,'attempted_inputs':prefix+'/attempted-inputs.json' if attempted is not None else None}))
    return prefix


def fetch_one(storage,url):
    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'complete_body':False}
    key=f'_raw-wire/{SLUG}/{uuid4().hex}/wire.body';path=Path(storage.local_dir).absolute()/key
    path.parent.mkdir(parents=True,exist_ok=False)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    def retain_wire(block):
        view=memoryview(block)
        while view:
            size=os.write(fd,view)
            if size<=0:raise OSError('wire_short_write')
            view=view[size:]
        os.fsync(fd)
    def reserve(progress):
        if shutil.disk_usage(path.parent).free<FREE_FLOOR+progress.reserve_bytes:raise OSError('global300MBfloor')
    try:
        with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
            receipt.update(status=response.status_code,final_url=str(response.url))
            result=capture_http_response(response,max_wire_bytes=MAX_BODY,max_decoded_bytes=MAX_BODY,reserve=reserve,retain_wire=retain_wire,retain_decoded=body.extend)
            info=asdict(result)
            for field in ('pending_wire','pending_decoded'):
                pending=info.pop(field)
                if pending:immutable(storage,str(path.parent.relative_to(Path(storage.local_dir).absolute()))+'/'+field+'.body',pending)
            receipt['transfer']=info
            if path.stat().st_size!=result.wire_bytes_retained or sha256_file(path)!=result.wire_sha256 or len(body)!=result.decoded_bytes_retained or digest(body)!=result.decoded_sha256:raise NaturalGasCrossingsQuarantineError('wire_or_decode_sink_readback_mismatch')
            if not result.complete or response.status_code!=200:raise NaturalGasCrossingsQuarantineError('non200_or_incomplete_http_capture')
        receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
        return bytes(body),receipt
    except (httpx.HTTPError,OSError,ValueError)as exc:
        receipt.update(bytes=len(body),sha256=digest(body),error=type(exc).__name__)
        raise NaturalGasCrossingsFetchError(str(exc),bytes(body),receipt)from exc
    finally:
        os.close(fd)
        receipt['wire_evidence']={'key':key,'bytes':path.stat().st_size,'sha256':sha256_file(path)}
        immutable(storage,str(path.parent.relative_to(Path(storage.local_dir).absolute()))+'/receipt.json',encode(receipt))

class NaturalGasCrossingsFetchError(NaturalGasCrossingsQuarantineError):
    def __init__(self,message,body,receipt):
        super().__init__(message);self.body=body;self.receipt=receipt

def validate_capture(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or type(receipt.get('bytes'))is not int or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=digest(body)):
        raise NaturalGasCrossingsQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)
    moment=receipt.get('observed_at')
    if type(moment)is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',moment) or datetime.fromisoformat(moment).tzinfo is None:raise NaturalGasCrossingsQuarantineError('aware_observation_timestamp_required')

def source_ids(document):
    if set(document)!={'objectIdFieldName','objectIds'} or document['objectIdFieldName']!='OBJECTID':raise NaturalGasCrossingsQuarantineError('missing_or_renamed_original_OID')
    ids=document['objectIds']
    if not isinstance(ids,list) or not ids or len(ids)>MAX_ROWS or any(type(v)is not int or not 0<v<=2**63-1 for v in ids) or len(set(ids))!=len(ids):raise NaturalGasCrossingsQuarantineError('invalid_duplicate_or_unbounded_source_ids')
    return sorted(ids)


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempted={};total=0
    root=Path(capture_dir).absolute() if capture_dir else None
    if root is not None and ('..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents])):raise NaturalGasCrossingsQuarantineError('unsafe_capture_root')
    def retain(name,body):
        key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body)
        attempted[name]={'body_key':key,'body_sha256':digest(body),'body_bytes':len(body),'raw_receipt_read':False}
        return key
    def read(name,url):
        nonlocal total
        if root is None:
            try:body,receipt=fetch_one(storage,url)
            except NaturalGasCrossingsFetchError as exc:
                retain(name,exc.body);captures[name]=(exc.body,exc.receipt);raise
            retain(name,body);raw_receipt=encode(receipt)
        else:
            body=bounded_input(root/(name+'.body'),MAX_BODY)
            retain(name,body) # Durable body and diagnostic entry precede receipt open.
            raw_receipt=bounded_input(root/(name+'.receipt.json'),65536)
        receipt_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.raw-receipt-{digest(raw_receipt)}.body'
        immutable(storage,receipt_key,raw_receipt)
        attempted[name].update(raw_receipt_read=True,raw_receipt_key=receipt_key,raw_receipt_sha256=digest(raw_receipt),raw_receipt_bytes=len(raw_receipt))
        receipt=strict_json(raw_receipt);captures[name]=(body,receipt);total+=len(body)
        if total>MAX_CAPTURE_BYTES:raise NaturalGasCrossingsQuarantineError('cumulative_capture_budget_exceeded')
        validate_capture(name,body,receipt,url)
        if name in PINS and (len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']):raise NaturalGasCrossingsQuarantineError('pinned_baseline_candidate_changed:'+name)
    try:
        for name,url in request_plan().items():read(name,url)
        ids=source_ids(strict_json(captures['ids_before'][0]))
        for name,url in request_plan(ids).items():
            if name not in captures:read(name,url)
        if root is not None and {p.name for p in root.iterdir()}!={name+suffix for name in captures for suffix in ('.body','.receipt.json')}:raise NaturalGasCrossingsQuarantineError('capture_directory_extra_or_missing_object')
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise NaturalGasCrossingsQuarantineError(str(exc)+'; retained:'+retained) from exc


def validate_metadata(meta):
    expected_geometry={'name':'SHAPE','type':'esriFieldTypeGeometry','alias':'SHAPE','domain':None}
    if (encode(meta.get('fields'))!=encode(FIELD_DEFINITIONS[:1]+[expected_geometry]+FIELD_DEFINITIONS[1:]) or type(meta.get('id'))is not int or meta['id']!=2
        or meta.get('name')!='Natural Gas Pipeline' or meta.get('geometryType')!='esriGeometryPoint'
        or encode(meta.get('sourceSpatialReference'))!=encode({'wkid':4326,'latestWkid':4326})
        or meta.get('copyrightText')!='North American Cooperation on Energy Information (NACEI), August 2017'
        or type(meta.get('maxRecordCount'))is not int or meta['maxRecordCount']<BATCH):
        raise NaturalGasCrossingsQuarantineError('source_schema_physical_types_geometry_or_layer_identity_drift')

def semantic_metadata(meta):
    return meta # No source metadata field is silently excluded.

def physical_value(field,value):
    if value is None:
        if field['type']=='esriFieldTypeOID':raise NaturalGasCrossingsQuarantineError('missing_original_OID')
        return None # ArcGIS MapServer does not declare nullability; do not invent one.
    kind=field['type']
    if kind=='esriFieldTypeString':
        if type(value)is not str or len(value)>field['length']:raise NaturalGasCrossingsQuarantineError('source_string_type_or_width_drift')
    elif kind=='esriFieldTypeDouble':
        if type(value)not in(int,float)or not math.isfinite(value):raise NaturalGasCrossingsQuarantineError('source_double_type_drift')
        value=float(value)
    else:
        lo,hi=(1,2**63-1)if kind=='esriFieldTypeOID'else(-2**31,2**31-1)
        if type(value)is not int or not lo<=value<=hi:raise NaturalGasCrossingsQuarantineError('source_integer_physical_type_drift')
    return value

def inspect_captures(captures):
    ids=source_ids(strict_json(captures['ids_before'][0]));plan=request_plan(ids)
    if set(captures)!=set(plan):raise NaturalGasCrossingsQuarantineError('incomplete_or_extra_capture_roster')
    for name,url in plan.items():
        body,receipt=captures[name];validate_capture(name,body,receipt,url)
        if name in PINS and (len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']):raise NaturalGasCrossingsQuarantineError('pinned_baseline_candidate_changed:'+name)
    catalog=captures['authority_catalog'][0].decode('utf-8');program=captures['authority_program'][0].decode('utf-8')
    if BASE not in catalog or 'e313db89-4219-4a5e-a543-772a86068710'not in catalog or 'Natural Resources Canada'not in catalog or 'North American'not in program or 'Energy Information'not in program:
        raise NaturalGasCrossingsQuarantineError('official_joint_program_or_catalog_delegation_drift')
    before=strict_json(captures['metadata_before'][0]);after=strict_json(captures['metadata_after'][0]);validate_metadata(before);validate_metadata(after)
    if encode(semantic_metadata(before))!=encode(semantic_metadata(after)) or source_ids(strict_json(captures['ids_after'][0]))!=ids:raise NaturalGasCrossingsQuarantineError('source_changed_during_complete_extraction')
    for name in ('count_before','count_after'):
        count=strict_json(captures[name][0])
        if set(count)!={'count'} or type(count['count'])is not int or count['count']!=len(ids):raise NaturalGasCrossingsQuarantineError('source_count_roster_or_completeness_mismatch')
    rows=[]
    for offset in range(0,len(ids),BATCH):
        page=strict_json(captures[f'page_{offset:05}'][0]);expected_ids=ids[offset:offset+BATCH]
        if page.get('exceededTransferLimit') is True or page.get('geometryType')!='esriGeometryPoint' or page.get('spatialReference',{}).get('latestWkid')!=4326:raise NaturalGasCrossingsQuarantineError('page_limit_geometry_crs_or_completeness_drift')
        if encode(page.get('fields'))!=encode([{k:v for k,v in f.items()if k!='domain'}for f in FIELD_DEFINITIONS]):raise NaturalGasCrossingsQuarantineError('query_field_roster_or_physical_types_drift')
        features=page.get('features')
        if not isinstance(features,list) or len(features)!=len(expected_ids) or [f.get('attributes',{}).get('OBJECTID') for f in features]!=expected_ids:raise NaturalGasCrossingsQuarantineError('page_missing_duplicate_unrequested_or_reordered_IDs')
        for feature in features:
            attrs=feature['attributes']
            if set(attrs)!={f['name'] for f in FIELD_DEFINITIONS}:raise NaturalGasCrossingsQuarantineError('source_attribute_rename_missing_or_unexpected_column')
            row={f['name']:physical_value(f,attrs[f['name']]) for f in FIELD_DEFINITIONS};g=feature.get('geometry')
            if not isinstance(g,dict) or set(g)!={'x','y'} or any(type(g[v])not in(int,float) or not math.isfinite(g[v]) for v in ('x','y')) or not(-180<=g['x']<=180 and -90<=g['y']<=90):raise NaturalGasCrossingsQuarantineError('missing_changed_or_out_of_range_native_point')
            row['geometry']=shapely.Point(g['x'],g['y']).wkb;rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['type']!='esriFieldTypeOID',metadata={b'nacei:esri_field':encode(f)}) for f in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary())]);table=pa.Table.from_pylist(rows,schema=schema)
    if table.num_rows!=len(ids) or len(set(table['OBJECTID'].to_pylist()))!=table.num_rows:raise NaturalGasCrossingsQuarantineError('crossing_OID_missing_duplicate_or_coverage_loss')
    geometry=shapely.from_wkb(table['geometry'].to_pylist())
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise NaturalGasCrossingsQuarantineError('invalid_or_empty_native_geometry')
    audit={'approved':False,'feature_count':len(ids),'original_field_definitions':FIELD_DEFINITIONS,
        'source_original_OID':'OBJECTID retained, width not declared; int64 is a proposed lossless encoding, not an approval.',
        'physical_decode':'Integer=int32; Double=float64; string widths/literal blanks/nulls unchanged. SHAPE preserved as native WKB geometry; metadata raw retains original SHAPE declaration.',
        'source_crs':'EPSG:4326','geometry':'Point','invalid_geometries':0,
        'country_counts':dict(sorted(collections.Counter(table['Country'].to_pylist()).items())),
        'current_source_count':len(ids),'initial_verified_count':INITIAL_COUNT,'roster_change_from_initial_requires_review':len(ids)!=INITIAL_COUNT,
        'semantic_metadata_sha256':digest(encode(before)),
        'release_day':'August2017 source copyright reference, no exact producer release day or present-day currency asserted.',
        'coverage':'WHOLE North America NACEI layer: initially US51/Canada28/Mexico20. NOT the exact EIA US51 release. No filter, deduplication, coordinate repair, StateProv rename, Vol_MMcfd cast or published ID mapping.'}
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
        if not pq.read_table(root/relative).equals(expected_parquet(table),check_metadata=True):raise NaturalGasCrossingsQuarantineError('parquet_full_values_physical_types_ids_wkb_or_metadata_drift')
    meta,actual=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs']!='EPSG:4326' or tuple(actual.column_names)!=FIELDS or tuple(f.type for f in actual.schema)!=TYPES or canonical_values(actual)!=canonical_values(table):raise NaturalGasCrossingsQuarantineError('gpkg_full_values_physical_types_ids_or_native_geometry_loss')
    with tempfile.TemporaryDirectory(prefix='natural-gas-crossings-events-fresh-container-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise NaturalGasCrossingsQuarantineError('gpkg_full_container_srs_headers_sql_or_index_drift')
def manifest_for(captures,identity,parquet_pin):
    ids=source_ids(strict_json(captures['ids_before'][0]));urls=request_plan(ids);inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,urls[name]);baseline=name.startswith('baseline_')
        inputs.append({'input_id':name+':'+digest(body),'producer':'HIFLD Next published candidate' if baseline else 'Natural Resources Canada / joint NACEI','host':urlsplit(urls[name]).hostname,'role':'archive' if baseline else 'producer_publication',
            'canonical_url':urls[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
            'attribution':'Unapproved complete published snapshot' if baseline else 'NRCan government-hosted joint NACEI natural-gas border crossings; complete original layer2.',
            'release_date_unknown_reason':'August2017 source copyright is retained; exact release day and current currency unknown, retrieval dates are not release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'NACEI Natural Gas Border Crossings — complete North America99 draft; changed scope from EIA US51','publisher':'Natural Resources Canada / joint NACEI','agency':'Natural Resources Canada / joint NACEI','source_url':BASE,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_native_joint_north_america_point_extraction','parameters':{'authority':'OpenCanada dataset delegates exact NRCan layer2; EIA announcement verifies joint NACEI program. Exact EIA US51 service remains token-required; this is a changed-source draft proposal.','completeness':'Before/after full OID rosters, counts and semantic metadata; exact ID-targeted batches','IDs':'All original OBJECTIDs retained for all countries; published IDs kept separately, no joins or surrogates','types':'Original int32 Integer/float64 Double/string widths/metadata; OID unspecified-width int64 proposal; nullability undeclared except OID required; strings/nulls/whitespace untouched','geometry':'Native4326 double Point x/y, no reprojection/geocoding/repair','metadata':'Complete wire and full metadata preserved; before/after canonical type-sensitive semantic equality'}}],
        'unsupported_output_formats':{'shapefile_zip':'Source long field names/types cannot be mapped destructively without acceptance.','pmtiles':'Quantization changes native coordinates; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve every original physical source field/null/metadata and exact native WKB without generic bbox/typing additions. This capability grants no acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise NaturalGasCrossingsQuarantineError('linked_candidate_lock')
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
        raise NaturalGasCrossingsQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise NaturalGasCrossingsQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise NaturalGasCrossingsQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if name not in ('metadata_before','metadata_after') and body!=captures[name][0]:raise NaturalGasCrossingsQuarantineError('original_capture_bytes_differ')
        if name in ('metadata_before','metadata_after') and encode(semantic_metadata(strict_json(body)))!=encode(semantic_metadata(strict_json(captures[name][0]))):raise NaturalGasCrossingsQuarantineError('original_source_semantics_changed')
        original[name]=(body,item['receipt'])
    original_table,original_audit=inspect_captures(original)
    current_table,current_audit=inspect_captures(captures)
    if not original_table.equals(current_table,check_metadata=True) or encode(original_audit)!=encode(current_audit):raise NaturalGasCrossingsQuarantineError('original_source_values_or_semantics_changed')
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    if encode({k:v for k,v in actual.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise NaturalGasCrossingsQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

def source_payloads(captures,identity,audit,manifest,table):
    proof={'status':'pass','approved':False,'rows':table.num_rows,'source_records_sha256':records_hash(table),'method':'Complete original JSON→declared native physical decode→every output column/value/null/identifier/native WKB equality; whole deterministic GPKG container and exact Arrow field/schema metadata.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/natural_gas_border_crossings_source_audit.json':encode(audit),'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),
        'metadata/baseline_comparison.json':encode(baseline_comparison(captures,table)),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}
    for name,(body,receipt) in captures.items():values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values

def checked_ready_write(path,body):
    if path.write_bytes(body)!=len(body) or bounded_input(path,2*1024*1024)!=body:
        raise NaturalGasCrossingsQuarantineError('private_ready_incomplete_write')

def verify_private(root,version,capture_hash,captures,identity,audit,manifest,table):
    """Bind the complete source-owned inventory before first seal and exposure."""
    payloads=source_payloads(captures,identity,audit,manifest,table)
    names={*payloads,'candidate/typed-source.parquet',f'geopackage/{SLUG}.gpkg',f'geoparquet/{SLUG}.parquet'}
    actual={str(path.relative_to(root))for path in root.rglob('*')if path.is_file()}
    if actual!=names|{READY} or any(path.is_symlink()or(path.is_file()and path.stat().st_nlink!=1)for path in root.rglob('*')):
        raise NaturalGasCrossingsQuarantineError('private_candidate_inventory_drift')
    for name,body in payloads.items():
        if bounded_input(root/name,MAX_CAPTURE_BYTES)!=body:
            raise NaturalGasCrossingsQuarantineError('private_source_object_drift:'+name)
    ready={'version':version,'approved':False,'processing_fingerprint':digest(encode(identity)),
        'capture_fingerprint':capture_hash,'objects':{name:sha256_file(root/name)for name in names}}
    if bounded_input(root/READY,2*1024*1024)!=encode(ready):
        raise NaturalGasCrossingsQuarantineError('private_ready_or_inventory_drift')
    verify_outputs(root,table)

def reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin,capture_hash):
    root=Path(storage.local_dir).absolute()/prefix
    if not (root/READY).is_file():raise NaturalGasCrossingsQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table);names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if set(ready)!={'version','approved','processing_fingerprint','capture_fingerprint','objects'}or type(ready['objects'])is not dict or encode({k:v for k,v in ready.items()if k!='objects'})!=encode({'version':version,'approved':False,'processing_fingerprint':digest(encode(identity)),'capture_fingerprint':capture_hash})or set(ready['objects'])!=names:raise NaturalGasCrossingsQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise NaturalGasCrossingsQuarantineError('unknown_missing_or_linked_candidate_object')
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise NaturalGasCrossingsQuarantineError('source_owned_report_wrapper_placeholder_or_raw_provenance_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise NaturalGasCrossingsQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise NaturalGasCrossingsQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and encode(strict_json((root/'metadata/resolved_provenance.json').read_bytes()))!=encode(provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)):raise NaturalGasCrossingsQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table)

def stage_natural_gas_border_crossings(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    try:
        table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({'records':records_hash(table),'semantic_metadata':audit['semantic_metadata_sha256'],'baseline_pins':PINS,'other_original_bodies':{n:digest(b) for n,(b,_) in captures.items() if n not in ('metadata_before','metadata_after')}}))
        version=f'v0.0.0-draft-nacei-gas-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir).absolute()
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            refuse_observed_loss(storage,version)
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise NaturalGasCrossingsQuarantineError('linked_candidate')
                    try:load_original_state(storage,version,captures,identity,table)
                    except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                        try:record_observed_loss(storage,version,fingerprint,str(loss))
                        except Exception as journal_error:raise IncidentWriteError('incident_write_failed_candidate_preserved') from journal_error
                        raise NaturalGasCrossingsQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
                    with tempfile.TemporaryDirectory(prefix='natural-gas-crossings-native-prebuilt-') as tmp:
                        fresh=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),fresh,compression='zstd');pin={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}
                    reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin,capture_hash)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise NaturalGasCrossingsQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            if original_state_present(storage,version):
                try:load_original_state(storage,version,captures,identity,table)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                    try:record_observed_loss(storage,version,fingerprint,str(loss))
                    except Exception as journal_error:raise IncidentWriteError('incident_write_failed_partial_state_preserved') from journal_error
                    raise NaturalGasCrossingsQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
            with tempfile.TemporaryDirectory(prefix='.natural-gas-crossings-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geopackage','geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                write_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg');parquet=private/'geoparquet'/f'{SLUG}.parquet';pq.write_table(expected_parquet(table),parquet,compression='zstd');(private/'candidate/typed-source.parquet').write_bytes(parquet.read_bytes());verify_outputs(private,table)
                pin={'sha256':sha256_file(parquet),'bytes':parquet.stat().st_size};manifest=manifest_for(captures,identity,pin)
                # The entire private candidate is complete before first trust state
                # is installed. A failed metadata/container build cannot bind history.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():
                    (private/name).parent.mkdir(parents=True,exist_ok=True)
                    if (private/name).write_bytes(body)!=len(body)or(private/name).read_bytes()!=body:raise NaturalGasCrossingsQuarantineError('private_source_object_incomplete_write')
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                checked_ready_write(private/READY,encode(ready))
                verify_private(private,version,capture_hash,captures,identity,audit,manifest,table)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                # An existing complete outside witness returns original receipts.
                # Rebuild only the private candidate from those exact originals.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():
                    if (private/name).write_bytes(body)!=len(body)or(private/name).read_bytes()!=body:raise NaturalGasCrossingsQuarantineError('original_private_source_object_incomplete_write')
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY}
                checked_ready_write(private/READY,encode(ready))
                verify_private(private,version,capture_hash,captures,identity,audit,manifest,table)
                os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise NaturalGasCrossingsQuarantineError(str(exc)+'; retained:'+retained) from exc

def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise NaturalGasCrossingsQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise NaturalGasCrossingsQuarantineError('incomplete_or_changed_bounded_input')
    return body


def incident_directory(version):
    return f'_construction-state/{SLUG}/incident-{version}'

def refuse_observed_loss(storage,version):
    directory=Path(storage.local_dir).absolute()/incident_directory(version)
    if directory.exists() or directory.is_symlink():raise NaturalGasCrossingsQuarantineError('permanent_observed_original_trust_loss_requires_reviewed_operator_recovery')

def record_observed_loss(storage,version,fingerprint,reason):
    # Partial writes/pending files are themselves blocking negative evidence.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason})
    seal_first_construction(storage,key,version,fingerprint,body)

def original_state_present(storage,version):
    key=construction_record_key(version);base=Path(storage.local_dir).absolute()
    return (base/key).exists() or (base/(key+'.first-construction.json')).exists() or (base/(key+'.evidence')).exists()

def load_original_state(storage,version,captures,identity,table):
    with tempfile.TemporaryDirectory(prefix='gas-original-pin-') as tmp:
        path=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),path,compression='zstd');pin={'sha256':sha256_file(path),'bytes':path.stat().st_size}
    return original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))


def baseline_comparison(captures,source):
    baseline=pq.read_table(io.BytesIO(captures['baseline_geoparquet'][0]))
    if baseline.num_rows!=51:raise NaturalGasCrossingsQuarantineError('pinned_US51_baseline_count_drift')
    rows=source.to_pylist();us=[r for r in rows if r['Country']=='United States'];old=baseline.to_pylist()
    compare={name:{'US_source_multiset_equals_published':collections.Counter(r[name]for r in us)==collections.Counter(r[name]for r in old)}for name in ('Pipeline','City','FrmCountry','ToCountry','Period','Latitude','Longitude')}
    return {'schema_version':1,'approved':False,'status':'blocked','reason_codes':['exact_EIA_US51_service_requires_token','whole_NACEI_NorthAmerica99_source_and_coverage_selection_unapproved','native_schema_ID_datum_precision_rights_baseline_contract_unapproved'],
        'complete_published_candidates':PINS,'published_rows':51,'candidate_rows':source.num_rows,'country_counts':dict(collections.Counter(r['Country']for r in rows)),
        'diagnostic_US_component_only':compare,'US_component_is_not_candidate_filter':True,'source_original_OBJECTIDs':source['OBJECTID'].to_pylist(),
        'published_schema':str(baseline.schema),'source_schema':str(source.schema),
        'geometry':'Published native3857 vs source native4326: no coordinate tolerance, reprojection or correspondence accepted.',
        'IDs':'Published format-specific IDs retained in original complete bodies; no source/published ID mapping inferred.',
        'release_decision':'No source, grain, coverage, physical type, schema, ID, geometry, baseline, contract or release acceptance.'}
