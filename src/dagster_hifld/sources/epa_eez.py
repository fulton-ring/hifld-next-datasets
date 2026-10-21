"""Complete producer-hosted EPA EEZ region-boundary draft; no release accepted.

All5 original attributes, distinct IDs, literal contact information, physical types,
literal strings/nulls and native EPSG4269 Polygon/MultiPolygon WKB remain preserved.
"""
from __future__ import annotations
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import stat
import struct
import math
import io
from collections import Counter
from urllib.parse import urlparse, parse_qs, urlencode
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.sources.epa_frs_common import validate_staged_release_derivatives
from dagster_hifld.validation import sha256_file

SLUG='epa-region-boundaries-exclusive-economic-zone-eez'
LANDING='https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer'
SERVICE=LANDING
BASE=SERVICE+'/4'
LICENSE='https://edg.epa.gov/EPA_Data_License.html'
READY='metadata/epa_eez_candidate_ready.json'
SPACE_FLOOR=300_000_000
MAX_AUX_BYTES=3_000_000
MAX_BODY_BYTES=20_000_000
INITIAL_COUNT=10
SOURCE_RECORD_SHA256='a68eef177bea820407359b1a0281ce0912f3348586f9462e7384c7007c478700'
SOURCE_SPATIAL_REFERENCE={'falseM': -100000, 'xyTolerance': 1.7986440921511318e-05, 'mUnits': 10000, 'zUnits': 10000, 'latestWkid': 4269, 'zTolerance': 0.001, 'wkid': 4269, 'xyUnits': 1111948722.222222, 'mTolerance': 0.001, 'falseX': -400, 'falseY': -400, 'falseZ': -100000}
DISPLAY_SPATIAL_REFERENCE={'falseM': -100000, 'xyTolerance': 0.001, 'mUnits': 10000, 'zUnits': 10000, 'latestWkid': 3857, 'zTolerance': 0.001, 'wkid': 102100, 'xyUnits': 144148035.89861274, 'mTolerance': 0.001, 'falseX': -22041257.77387803, 'falseY': -30241100, 'falseZ': -100000}
NATIVE_CRS_JSON={'$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json', 'type': 'GeographicCRS', 'name': 'NAD83', 'datum': {'type': 'GeodeticReferenceFrame', 'name': 'North American Datum 1983', 'ellipsoid': {'name': 'GRS 1980', 'semi_major_axis': 6378137, 'inverse_flattening': 298.257222101}}, 'coordinate_system': {'subtype': 'ellipsoidal', 'axis': [{'name': 'Geodetic latitude', 'abbreviation': 'Lat', 'direction': 'north', 'unit': 'degree'}, {'name': 'Geodetic longitude', 'abbreviation': 'Lon', 'direction': 'east', 'unit': 'degree'}]}, 'scope': 'Geodesy.', 'area': 'North America - onshore and offshore: Canada - Alberta; British Columbia; Manitoba; New Brunswick; Newfoundland and Labrador; Northwest Territories; Nova Scotia; Nunavut; Ontario; Prince Edward Island; Quebec; Saskatchewan; Yukon. Puerto Rico. United States (USA) - Alabama; Alaska; Arizona; Arkansas; California; Colorado; Connecticut; Delaware; Florida; Georgia; Hawaii; Idaho; Illinois; Indiana; Iowa; Kansas; Kentucky; Louisiana; Maine; Maryland; Massachusetts; Michigan; Minnesota; Mississippi; Missouri; Montana; Nebraska; Nevada; New Hampshire; New Jersey; New Mexico; New York; North Carolina; North Dakota; Ohio; Oklahoma; Oregon; Pennsylvania; Rhode Island; South Carolina; South Dakota; Tennessee; Texas; Utah; Vermont; Virginia; Washington; West Virginia; Wisconsin; Wyoming. US Virgin Islands. British Virgin Islands.', 'bbox': {'south_latitude': 14.92, 'west_longitude': 167.65, 'north_latitude': 86.45, 'east_longitude': -40.73}, 'id': {'authority': 'EPSG', 'code': 4269}}
FIELD_DEFINITIONS=[{'nullable': True, 'domain': None, 'name': 'FIRST_CONTACT', 'length': 500, 'alias': 'Contact Info', 'type': 'esriFieldTypeString'}, {'nullable': True, 'domain': None, 'name': 'Shape_Length', 'alias': 'Shape_Length', 'type': 'esriFieldTypeDouble'}, {'nullable': True, 'domain': None, 'name': 'Shape_Area', 'alias': 'Shape_Area', 'type': 'esriFieldTypeDouble'}, {'nullable': False, 'domain': None, 'name': 'OBJECTID', 'alias': 'OBJECTID', 'type': 'esriFieldTypeOID'}, {'nullable': True, 'domain': None, 'name': 'Shape', 'alias': 'Shape', 'type': 'esriFieldTypeGeometry'}, {'nullable': True, 'domain': None, 'name': 'EPAREGION', 'length': 50, 'alias': 'EPAREGION', 'type': 'esriFieldTypeString'}]
PAGE_FIELDS=[{'name': 'FIRST_CONTACT', 'type': 'esriFieldTypeString', 'alias': 'Contact Info', 'length': 500}, {'name': 'Shape_Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Length'}, {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Area'}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID'}, {'name': 'EPAREGION', 'type': 'esriFieldTypeString', 'alias': 'EPAREGION', 'length': 50}]
REGION_IDENTITIES={1: 'Region 1', 2: 'Region 10', 3: 'Region 2', 4: 'Region 3', 5: 'Region 4', 6: 'Region 5', 7: 'Region 6', 8: 'Region 7', 9: 'Region 8', 10: 'Region 9'}
ATTRIBUTE_FIELDS=[d for d in FIELD_DEFINITIONS if d['type']!='esriFieldTypeGeometry']
# The producer layer metadata omits hasZ/hasM (neither dimension is advertised).
# Keep true curves and the producer's native CRS, while avoiding unsupported Z/M output switches.
URLS={'source_service':SERVICE+'?f=pjson','layer_before':BASE+'?f=pjson','license_epa':LICENSE,'count_before':BASE+'/query?'+urlencode({'where':'1=1','returnCountOnly':'true','f':'json'}),'ids_before':BASE+'/query?'+urlencode({'where':'1=1','returnIdsOnly':'true','f':'json'}),'page_00000':BASE+'/query?'+urlencode({'objectIds':'1,2,3,4,5,6,7,8,9,10','outFields':'*','returnGeometry':'true','returnTrueCurves':'true','outSR':4269,'orderByFields':'OBJECTID ASC','f':'json'}),'layer_after':BASE+'?f=pjson'}
URLS.update({'count_after':URLS['count_before'],'ids_after':URLS['ids_before']})

class EpaEezQuarantineError(ValueError):pass
class IncidentWriteError(Exception):pass


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise EpaEezQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise EpaEezQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise EpaEezQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise EpaEezQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise EpaEezQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise EpaEezQuarantineError('staging_alias_or_special_file_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise EpaEezQuarantineError('immutable_capture_changed:'+key)


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


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb')as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise EpaEezQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise EpaEezQuarantineError('changed_or_incomplete_original_input')
    return body


def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise EpaEezQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names


def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'


def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'


def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))


def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise EpaEezQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')


def record_observed_loss(storage,version,fingerprint,reason):
    # The reviewed shared protocol fsyncs each created directory and the final
    # records before returning. Any failure preserves the existing candidate;
    # even an incomplete incident directory blocks subsequent cold retries.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason,
        'operator_action':'Restore exact original witnesses and explicitly retain/review this negative incident; no automatic bootstrap.'})
    seal_first_construction(storage,key,version,fingerprint,body)


def write_private(root,name,body):
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise EpaEezQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise EpaEezQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise EpaEezQuarantineError('incomplete_private_write:'+name)


def validate_receipt(name,body,receipt):
    if(receipt.get('url')!=URLS[name]or receipt.get('final_url')!=URLS[name]or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise EpaEezQuarantineError('invalid_original_receipt:'+name)
    if type(receipt.get('observed_at'))is not str or datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise EpaEezQuarantineError('retrieval_timezone_required')
    return {'input_id':name+':'+digest(body),'producer':'U.S. Environmental Protection Agency Office of Environmental Information','host':httpx.URL(URLS[name]).host,'role':'producer_publication','canonical_url':URLS[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':'US EPA OEI SEGS; producer states 2011 TIGER/Line and NOAA U.S. Maritime Boundaries lineage, public access/use without restrictions. Rights acceptance remains pending.','release_date_unknown_reason':'Producer lineage2011 and service document title2012 are retained, not conflated with source release date. Retrieval, service availability, inventory and portal dates establish no current edition.'}


def retain_capture_fragment(path,body):
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        if os.write(descriptor,body)!=len(body):raise OSError('short_pending_capture_write')
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if bounded_input(path,len(body))!=body:raise OSError('pending_capture_readback_mismatch')


def preserve_transfer_result(path,result):
    transfer={key:value for key,value in vars(result).items()if key not in('pending_wire','pending_decoded')}
    for label in('wire','decoded'):
        pending=getattr(result,'pending_'+label)
        if pending:retain_capture_fragment(path/('pending_'+label+'.body'),pending)
        transfer['pending_'+label+'_artifact']={'bytes':len(pending),'sha256':digest(pending),'path':'pending_'+label+'.body'if pending else None}
        observed=path/(label+'.body')
        transfer['physical_'+label]={'bytes':observed.stat().st_size,'sha256':sha256_file(observed)}
    readback=(transfer['physical_wire']['bytes']==result.wire_bytes_retained
        and transfer['physical_wire']['sha256']==result.wire_sha256
        and transfer['physical_decoded']['bytes']==result.decoded_bytes_retained
        and transfer['physical_decoded']['sha256']==result.decoded_sha256)
    transfer['independent_physical_readback_matches_acknowledged_sinks']=readback
    retain_capture_fragment(path/'transfer.json',encode(transfer))
    return readback


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise EpaEezQuarantineError('unsafe_capture_root')
    try:
        for name,url in URLS.items():
            if root is not None:
                body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
                raw=bounded_input(root/(name+'.receipt.json'),65536)
            else:
                path=Path(storage.local_dir)/attempt/name;path.mkdir(parents=True)
                with(path/'wire.body').open('xb')as wire,(path/'decoded.body').open('xb')as decoded:
                    def retain(stream,block,target):
                        before=os.fstat(stream.fileno());written=stream.write(block)
                        stream.flush();os.fsync(stream.fileno())
                        if written!=len(block):raise OSError('short_capture_write')
                        reader=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                        try:
                            actual=os.fstat(reader)
                            if(not stat.S_ISREG(actual.st_mode)or actual.st_nlink!=1
                                or(actual.st_dev,actual.st_ino)!=(before.st_dev,before.st_ino)
                                or actual.st_size!=before.st_size+len(block)
                                or os.pread(reader,len(block),before.st_size)!=block):raise OSError('capture_block_readback_mismatch')
                        finally:os.close(reader)
                    def reserve(progress):
                        if shutil.disk_usage(path).free<SPACE_FLOOR+progress.reserve_bytes:raise OSError('disk_floor_before_read')
                    receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat()};result=None
                    try:
                        with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                            result=capture_http_response(response,max_wire_bytes=MAX_BODY_BYTES,max_decoded_bytes=MAX_BODY_BYTES,
                                reserve=reserve,retain_wire=lambda b:retain(wire,b,path/'wire.body'),retain_decoded=lambda b:retain(decoded,b,path/'decoded.body'))
                            receipt.update(final_url=str(response.url),status=response.status_code,complete_body=result.complete,
                                bytes=result.decoded_bytes_retained,sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code)
                    finally:
                        if result is not None:readback=preserve_transfer_result(path,result)
                        wire.flush();decoded.flush()
                receipt['complete_body']=result.complete and readback
                receipt['independent_sink_readback_matches']=readback
                body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
            immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
            receipt=strict_json(raw);validate_receipt(name,body,receipt);captures[name]=(body,receipt)
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise EpaEezQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def parquet_table(table):
    kinds=sorted({shapely.from_wkb(w).geom_type for w in table['Shape'].to_pylist()if w is not None})
    geo={'version':'1.1.0','primary_column':'Shape','columns':{'Shape':{'encoding':'WKB','geometry_types':kinds,'crs':NATIVE_CRS_JSON}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def validate_output(root,table):
    actual=pq.read_table(root/'geoparquet'/f'{SLUG}.parquet')
    if not actual.equals(parquet_table(table),check_metadata=True):raise EpaEezQuarantineError('full_typed_native_output_drift')


def manifest_for(captures,identity,output_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),'title':'EPA Region Boundaries Exclusive Economic Zone (EEZ) — complete current producer-hosted draft','publisher':'U.S. Environmental Protection Agency','agency':'Environmental Protection Agency','source_url':LANDING,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n])for n in URLS],
        'transformations':[{'operation':'complete_EPA_EEZ_only_extraction_native_source_CRS','parameters':{'source':'Exact EPA_Locations/MapServer/4, not ordinary region or alternate coastline layers; producer lineage2011 TIGER/Line and NOAA maritime boundaries retained without claiming modern data edition.','ids':'Original OBJECTID and literal EPAREGION are preserved; all historical format IDs retained separately, no approved join.','types':'Original Double binary64 and text literal values/nulls/width declarations preserved. OID candidate Int64 is proposed only: source physical bit width unspecified. Original Shape geometry declaration remains native field name.','geometry':'Official query explicitly requests producer-declared sourceSpatialReference4269; exact returned NAD83 rings/XY retained in WKB, no local projection/precision change/repair. Default service3857 display CRS remains separately documented. Polygon/MultiPolygon container grouping comes from GDAL EsriJSON decoding and is independently checked against every original ordered ring; no historical grouping/CRS mapping approved.','coverage':'Complete ten-region EEZ source, all original IDs and geometries; no partial substitution or silently extended coastline.','release':'Source, edition, rights, coverage, schema/IDs, geometry/baseline compatibility, contract and release acceptance remain false.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Pin complete original names/IDs/types/nulls/native NAD83 geometry; generic schema regeneration unapproved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Producer field widths/OID encoding and geometry container contract remain unapproved; faithful native GeoParquet is staged.','shapefile_zip':'Truncated names/widths/null mappings cannot silently replace original fields.','pmtiles':'Reprojection/quantization, dropped records and cartographic feature IDs remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise EpaEezQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(12_000_000 if name.endswith('.parquet')else MAX_AUX_BYTES))


def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/epa_eez_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values


def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(URLS):raise EpaEezQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise EpaEezQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise EpaEezQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')})
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise EpaEezQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old


def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False,storage=None):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise EpaEezQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise EpaEezQuarantineError('private_candidate_roster_drift')
    if cached:
        if storage is None:raise EpaEezQuarantineError('cached_candidate_requires_explicit_storage_context')
        _validate_cached_catalog(root,storage,version,names|{READY},roster)
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise EpaEezQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise EpaEezQuarantineError('candidate_ready_drift')
    validate_output(root,table)


def _catalog_document_bytes(value):
    return json.dumps(value,sort_keys=True,indent=2).encode('utf-8')


def _validate_catalog_sidecar_bytes(quality_body,dictionary_body,quality,dictionary):
    if quality_body!=_catalog_document_bytes(quality):raise EpaEezQuarantineError('cached_quality_manifest_drift')
    if dictionary_body!=_catalog_document_bytes(dictionary):raise EpaEezQuarantineError('cached_data_dictionary_drift')


def _validate_cached_catalog(root,storage,version,base_roster,roster):
    """Accept only source pins, exact catalog derivatives, and a valid release trio.

    Recompute the catalog from the pinned native GeoParquet. Catalog-owned
    quality and dictionary sidecars therefore cannot alter gate outcomes while
    the source cache claims to be unchanged.
    """
    prefix=f'{SLUG}/{SLUG}/{version}'
    actual_keys={storage._ensure_prefixed(f'{prefix}/{name}')for name in roster}
    try:release_names=validate_staged_release_derivatives(storage,prefix,actual_keys)
    except (OSError,UnicodeError,ValueError,TypeError)as exc:raise EpaEezQuarantineError('invalid_cached_release_derivatives')from exc
    release_names={key.removeprefix(storage._ensure_prefixed(prefix).rstrip('/')+'/')for key in release_names}
    catalog_names={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/resolved_provenance.json',
        'metadata/source/source_manifest.json','metadata/source/version_'+digest(candidate_bytes(root,'metadata/source_manifest.json'))+'_source_manifest.json'}
    present=(roster-base_roster)-release_names
    if present and present!=catalog_names:raise EpaEezQuarantineError('incomplete_or_unknown_cached_catalog_roster')
    expected_roster=set(base_roster)|release_names
    if present:
        expected_roster|=catalog_names
        source_manifest=candidate_bytes(root,'metadata/source_manifest.json')
        manifest_snapshot='metadata/source/version_'+digest(source_manifest)+'_source_manifest.json'
        if(candidate_bytes(root,'metadata/source/source_manifest.json')!=source_manifest
            or candidate_bytes(root,manifest_snapshot)!=source_manifest):raise EpaEezQuarantineError('cached_source_manifest_snapshot_drift')
        resolved=load_resolved_source_manifest(storage,SLUG,SLUG,version)
        projection=provenance_projection(resolved.metadata)
        if projection is None:raise EpaEezQuarantineError('cached_resolved_provenance_missing')
        if candidate_bytes(root,'metadata/resolved_provenance.json')!=_catalog_document_bytes(projection):
            raise EpaEezQuarantineError('cached_resolved_provenance_drift')
        try:summary=summarize_staged_catalog(storage,SLUG,SLUG,version,SLUG,source_metadata=resolved.metadata)
        except (OSError,UnicodeError,ValueError,TypeError)as exc:raise EpaEezQuarantineError('cached_catalog_recomputation_failed')from exc
        _validate_catalog_sidecar_bytes(candidate_bytes(root,'metadata/quality_manifest.json'),
            candidate_bytes(root,'metadata/data_dictionary.json'),summary.quality_manifest,summary.data_dictionary)
    if roster!=expected_roster:raise EpaEezQuarantineError('unknown_or_incomplete_cached_object_roster')


def stage_epa_eez(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    try:table,audit=inspect_captures(captures)
    except Exception as exc:
        where=quarantine(storage,captures,str(exc));raise EpaEezQuarantineError('source_drift_quarantined:'+where)from exc
    identity=processing_identity();fingerprint=digest(encode(identity));version='v0.0.0-draft-epa-eez-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise EpaEezQuarantineError('linked_lock')
        fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
        # A private deterministic output provides the pin before any trust seal.
        build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='candidate-',dir=build)as temporary:
            temp=Path(temporary);(temp/'geoparquet').mkdir();path=temp/'geoparquet'/f'{SLUG}.parquet';pq.write_table(parquet_table(table),path,compression='zstd');validate_output(temp,table)
            manifest=manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':sha256_file(path)})
            if root.exists()or state_present(storage,version):
                try:
                    captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest)
                    table,audit=inspect_captures(captures)
                except Exception as exc:
                    try:record_observed_loss(storage,version,fingerprint,type(exc).__name__)
                    except Exception as write:raise IncidentWriteError('incident_failure_candidate_preserved')from write
                    if root.exists():
                        q=base/'_quarantine'/SLUG/('observed-loss-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q)
                    raise EpaEezQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise EpaEezQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'source_null_geometry_count':audit['null_geometry_count'],'source_invalid_geometry_count':audit['invalid_geometry_count'],'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'source_null_geometry_count':audit['null_geometry_count'],'source_invalid_geometry_count':audit['invalid_geometry_count'],'reused':False}


def id_roster(body):
    document=strict_json(body);ids=document.get('objectIds')
    if document.get('objectIdFieldName')!='OBJECTID'or type(ids)is not list or len(ids)!=INITIAL_COUNT or any(type(x)is not int for x in ids)or sorted(ids)!=list(range(1,11)):raise EpaEezQuarantineError('complete_original_OID_roster_drift')
    return sorted(ids)


def verify_original_rings(raw,geometry):
    if type(raw)is not dict or set(raw)!={'rings'}or type(raw['rings'])is not list or not raw['rings']:raise EpaEezQuarantineError('unsupported_or_missing_original_rings')
    def fingerprint(ring):
        if type(ring)is not list or len(ring)<4 or encode(ring[0])!=encode(ring[-1]):raise EpaEezQuarantineError('unclosed_or_short_original_ring')
        if any(type(p)is not list or len(p)!=2 or any(type(v)not in(int,float)or not math.isfinite(v)or float(v)!=v for v in p)for p in ring):raise EpaEezQuarantineError('source_coordinate_dimension_or_precision_drift')
        return digest(b''.join(struct.pack('<dd',*p)for p in ring))
    original=Counter(fingerprint(r)for r in raw['rings']);actual=[]
    for polygon in shapely.get_parts(geometry):
        if polygon.geom_type!='Polygon':raise EpaEezQuarantineError('unexpected_decoded_geometry_kind')
        actual+=[list(map(list,polygon.exterior.coords))]+[list(map(list,r.coords))for r in polygon.interiors]
    if original!=Counter(fingerprint(r)for r in actual):raise EpaEezQuarantineError('original_ordered_ring_XY_or_precision_changed')
    return len(raw['rings']),sum(len(r)for r in raw['rings'])


def decode_page(page):
    if (page.get('geometryType')!='esriGeometryPolygon'or encode(page.get('fields'))!=encode(PAGE_FIELDS)
        or type(page.get('features'))is not list or ('exceededTransferLimit'in page and page['exceededTransferLimit']is not False)
        or encode(page.get('spatialReference'))!=encode({'wkid':4269,'latestWkid':4269})):raise EpaEezQuarantineError('page_schema_transfer_or_native_CRS_drift')
    # Keep driver-identifying headers before large feature arrays; raw wire bytes remain immutable.
    driver_document={k:v for k,v in page.items()if k!='features'};driver_document['features']=page['features']
    driver_bytes=json.dumps(driver_document,allow_nan=False,separators=(',',':')).encode()
    meta,decoded=pyogrio.read_arrow(io.BytesIO(driver_bytes),return_fids=True)
    if meta['crs']!='EPSG:4269'or decoded.num_rows!=len(page['features']):raise EpaEezQuarantineError('GDAL_native_population_or_CRS_drift')
    geometry_name=meta.get('geometry_name')or'wkb_geometry';fid=meta['fid_column'];geometries={r[fid]:r[geometry_name]for r in decoded.to_pylist()}
    if len(geometries)!=len(page['features']):raise EpaEezQuarantineError('duplicate_decoded_source_FID')
    rows=[];rings=vertices=0;seen=set()
    for feature in page['features']:
        attrs=feature.get('attributes');raw=feature.get('geometry')
        if type(attrs)is not dict or set(attrs)!={d['name']for d in ATTRIBUTE_FIELDS}:raise EpaEezQuarantineError('missing_extra_or_renamed_source_attribute')
        row={}
        for d in ATTRIBUTE_FIELDS:
            name=d['name'];value=attrs[name]
            if value is None:
                if d['nullable']is not True:raise EpaEezQuarantineError('missing_nonnull_source_id')
            elif d['type']=='esriFieldTypeString':
                if type(value)is not str or len(value)>d['length']:raise EpaEezQuarantineError('source_text_type_or_width_drift')
            elif d['type']=='esriFieldTypeOID':
                if type(value)is not int or not-2**63<=value<2**63:raise EpaEezQuarantineError('source_OID_type_or_candidate_width_drift')
            elif d['type']=='esriFieldTypeDouble':
                if type(value)not in(int,float)or not math.isfinite(value)or float(value)!=value:raise EpaEezQuarantineError('source_Double_type_or_precision_drift')
                value=float(value)
            else:raise EpaEezQuarantineError('unknown_source_attribute_type')
            row[name]=value
        oid=row['OBJECTID']
        if oid in seen or oid not in geometries:raise EpaEezQuarantineError('duplicate_or_unmatched_original_OBJECTID')
        seen.add(oid);row['Shape']=geometries[oid]
        if raw is None:
            if row['Shape']is not None:raise EpaEezQuarantineError('invented_source_geometry')
        else:
            if row['Shape']is None:raise EpaEezQuarantineError('dropped_source_geometry')
            a,b=verify_original_rings(raw,shapely.from_wkb(row['Shape']));rings+=a;vertices+=b
        rows.append(row)
    types={'esriFieldTypeString':pa.string(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeOID':pa.int64(),'esriFieldTypeGeometry':pa.binary()}
    schema=pa.schema([pa.field(d['name'],types[d['type']],nullable=d['nullable'],metadata={b'original_esri_field':encode(d)})for d in FIELD_DEFINITIONS]);rows.sort(key=lambda r:r['OBJECTID']);table=pa.Table.from_pylist(rows,schema=schema)
    return table,{'rings':rings,'vertices':vertices}


def inspect_captures(captures):
    if set(captures)!=set(URLS):raise EpaEezQuarantineError('incomplete_or_extra_capture_roster')
    for name,(body,receipt)in captures.items():validate_receipt(name,body,receipt)
    service=strict_json(captures['source_service'][0]);before=strict_json(captures['layer_before'][0]);after=strict_json(captures['layer_after'][0])
    if encode(before)!=encode(after):raise EpaEezQuarantineError('source_metadata_changed_during_complete_extraction')
    if not any(type(x.get('id'))is int and x['id']==4 and x.get('name')=='EPA Region Boundaries with Exclusive Economic Zone'for x in service.get('layers',[])):raise EpaEezQuarantineError('exact_EEZ_layer_delegation_drift')
    if ('2011 TIGER/Line and NOAA U.S. Maritime Boundaries'not in before.get('description','')or 'Office of Environmental Information'not in before.get('description','')):raise EpaEezQuarantineError('producer_EEZ_lineage_drift')
    if(type(before.get('id'))is not int or before['id']!=4 or before.get('name')!='EPA Region Boundaries with Exclusive Economic Zone'or before.get('geometryType')!='esriGeometryPolygon'or encode(before.get('fields'))!=encode(FIELD_DEFINITIONS)
        or encode(before.get('sourceSpatialReference'))!=encode(SOURCE_SPATIAL_REFERENCE)or encode(before.get('extent',{}).get('spatialReference'))!=encode(DISPLAY_SPATIAL_REFERENCE)):raise EpaEezQuarantineError('source_physical_schema_or_native_CRS_drift')
    ids=id_roster(captures['ids_before'][0])
    if ids!=id_roster(captures['ids_after'][0]):raise EpaEezQuarantineError('source_ID_census_changed')
    for name in('count_before','count_after'):
        count=strict_json(captures[name][0]).get('count')
        if type(count)is not int or count!=INITIAL_COUNT:raise EpaEezQuarantineError('complete_source_count_drift')
    page=strict_json(captures['page_00000'][0]);table,audit=decode_page(page)
    if table['OBJECTID'].to_pylist()!=ids or {r['OBJECTID']:r['EPAREGION']for r in table.to_pylist()}!=REGION_IDENTITIES:raise EpaEezQuarantineError('original_region_ID_coverage_drift')
    record_hash=digest(encode(page['features']))
    if record_hash!=SOURCE_RECORD_SHA256:raise EpaEezQuarantineError('source_record_or_native_geometry_drift_requires_review')
    geometry=shapely.from_wkb(table['Shape'].to_pylist());audit.update(feature_count=table.num_rows,field_count=5,source_CRS=4269,service_display_CRS=3857,null_geometry_count=table['Shape'].null_count,invalid_geometry_count=sum(g is not None and(not g.is_valid or g.is_empty)for g in geometry),invalid_geometry_OBJECTIDs=[oid for oid,g in zip(ids,geometry)if g is not None and(not g.is_valid or g.is_empty)],null_geometry_OBJECTIDs=[oid for oid,g in zip(ids,geometry)if g is None],original_OBJECTIDs=ids,source_record_sha256=record_hash,producer_lineage='2011 TIGER/Line and NOAA maritime boundaries; no modern replacement inferred.',release_date_established=False,approved=False)
    return table,audit
