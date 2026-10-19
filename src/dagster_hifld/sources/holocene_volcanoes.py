"""Complete Smithsonian GVP Holocene WFS snapshot; changed successor, unapproved.

All source feature IDs and Volcano_Number values remain distinct. Schema-declared
int/short/decimal types and literal strings/nulls survive in canonical GeoParquet.
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
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal
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
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG='historical-holocene-volcano-locations'
LANDING='https://volcano.si.edu/volcanolist_holocene.cfm'
SERVICE='https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows'
LICENSE='https://volcano.si.edu/gvp_termsofuse.cfm'
READY='metadata/holocene_volcanoes_candidate_ready.json'
SPACE_FLOOR=300_000_000
MAX_AUX_BYTES=3_000_000
MAX_BODY_BYTES=4_000_000
class CollegeQuarantineError(ValueError):pass
class IncidentWriteError(Exception):pass
URLS={'gvp': 'https://volcano.si.edu/volcanolist_holocene.cfm', 'gvp_services': 'https://volcano.si.edu/database/webservices.cfm', 'wfs_schema': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=describefeaturetype&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes', 'wfs_hits': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes&resultType=hits', 'wfs_all': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes&outputFormat=application/json', 'wfs_all_after': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes&outputFormat=application/json', 'wfs_hits_after': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes&resultType=hits', 'wfs_schema_after': 'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=describefeaturetype&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes', 'gvp_terms': 'https://volcano.si.edu/gvp_termsofuse.cfm', 'gvp_cite': 'https://volcano.si.edu/gvp_cite.cfm'}
FIELD_DEFINITIONS=[{'maxOccurs': '1', 'minOccurs': '1', 'name': 'Volcano_Number', 'nillable': 'false', 'type': 'xsd:int'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Volcano_Name', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '1', 'name': 'Volcanic_Landform', 'nillable': 'false', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Primary_Volcano_Type', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Last_Eruption_Year', 'nillable': 'true', 'type': 'xsd:int'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Country', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Region', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Subregion', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Geological_Summary', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Latitude', 'nillable': 'true', 'type': 'xsd:decimal'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Longitude', 'nillable': 'true', 'type': 'xsd:decimal'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Elevation', 'nillable': 'true', 'type': 'xsd:short'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Tectonic_Setting', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Geologic_Epoch', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Evidence_Category', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Primary_Photo_Link', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Primary_Photo_Caption', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Primary_Photo_Credit', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'Major_Rock_Type', 'nillable': 'true', 'type': 'xsd:string'}, {'maxOccurs': '1', 'minOccurs': '0', 'name': 'GeoLocation', 'nillable': 'true', 'type': 'gml:GeometryPropertyType'}]
INITIAL_COUNT=1214
INITIAL_SOURCE_ID_SHA256='c98034db2f2be24b40ab3ef24049f1df4e8d9b5a7194e0e9f4d18f5905f399f6'


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise CollegeQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise CollegeQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise CollegeQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise CollegeQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise CollegeQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise CollegeQuarantineError('staging_alias_or_special_file_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise CollegeQuarantineError('immutable_capture_changed:'+key)


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
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise CollegeQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise CollegeQuarantineError('changed_or_incomplete_original_input')
    return body


def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise CollegeQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names


def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'


def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'


def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))


def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise CollegeQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')


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
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise CollegeQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise CollegeQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise CollegeQuarantineError('incomplete_private_write:'+name)


def validate_receipt(name,body,receipt):
    if (receipt.get('url')!=URLS[name] or receipt.get('final_url')!=URLS[name]
        or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body) or receipt.get('complete_body')is not True):
        raise CollegeQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str):raise CollegeQuarantineError('missing_retrieval_time')
    when=datetime.fromisoformat(receipt['observed_at'])
    if when.tzinfo is None:raise CollegeQuarantineError('retrieval_time_timezone_required')
    return {'input_id':name+':'+digest(body),'producer':'Smithsonian Institution Global Volcanism Program',
        'host':httpx.URL(URLS[name]).host,'role':'producer_publication','canonical_url':URLS[name],
        'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],
        'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
        'attribution':'Smithsonian Institution Global Volcanism Program, Volcanoes of the World5.4.0, DOI10.5479/si.GVP.VOTW5-2026.5.4.',
        'release_date_unknown_reason':'Producer citation dates VOTW5.4.0 to2026-08-07; separate WFS snapshot release day is not established. Retrieval is an observation, not publication.'}


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise CollegeQuarantineError('unsafe_capture_root')
    try:
        for name,url in URLS.items():
            if root is not None:
                body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
                raw=bounded_input(root/(name+'.receipt.json'),65536)
            else:
                path=Path(storage.local_dir)/attempt/name;path.mkdir(parents=True)
                with(path/'wire.body').open('xb')as wire,(path/'decoded.body').open('xb')as decoded:
                    def retain(stream,block):
                        if stream.write(block)!=len(block):raise OSError('short_capture_write')
                        stream.flush();os.fsync(stream.fileno())
                    def reserve(progress):
                        if shutil.disk_usage(path).free<SPACE_FLOOR+progress.reserve_bytes:raise OSError('disk_floor_before_read')
                    receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat()}
                    try:
                        with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                            result=capture_http_response(response,max_wire_bytes=MAX_BODY_BYTES,max_decoded_bytes=MAX_BODY_BYTES,
                                reserve=reserve,retain_wire=lambda b:retain(wire,b),retain_decoded=lambda b:retain(decoded,b))
                            receipt.update(final_url=str(response.url),status=response.status_code,complete_body=result.complete,
                                bytes=result.decoded_bytes_retained,sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code)
                    finally:
                        wire.flush();decoded.flush()
                body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
            immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
            receipt=strict_json(raw);validate_receipt(name,body,receipt);captures[name]=(body,receipt)
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise CollegeQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def decimal_json(body):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise CollegeQuarantineError('duplicate_source_key')
            out[k]=v
        return out
    return json.loads(body,parse_float=Decimal,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in()).throw(ValueError(x)))


def declarations(body):
    if b'<!DOCTYPE'in body or b'<!ENTITY'in body:raise CollegeQuarantineError('xml_entities_refused')
    root=ET.fromstring(body)
    return [dict(x.attrib)for x in root.findall('.//{http://www.w3.org/2001/XMLSchema}sequence/{http://www.w3.org/2001/XMLSchema}element')]


def decode_population(body,*,expected_count=INITIAL_COUNT):
    data=decimal_json(body)
    if (data.get('type')!='FeatureCollection'or data.get('crs')!={'type':'name','properties':{'name':'urn:ogc:def:crs:EPSG::4326'}}
        or any(type(data.get(k))is not int or data[k]!=expected_count for k in('totalFeatures','numberMatched','numberReturned'))
        or len(data.get('features',[]))!=expected_count):raise CollegeQuarantineError('incomplete_population_or_crs_drift')
    declarations_by_name={f['name']:f for f in FIELD_DEFINITIONS};fields=list(declarations_by_name);properties=fields[:-1]
    rows=[];seen=set();numbers=set();regions={}
    for feature in data['features']:
        fid=feature.get('id');attrs=feature.get('properties');geom=feature.get('geometry')
        if(set(feature)!={'type','id','geometry','geometry_name','properties'}or feature['type']!='Feature'
            or feature['geometry_name']!='GeoLocation'or not isinstance(fid,str)or not fid or fid in seen
            or type(attrs)is not dict or list(attrs)!=properties):raise CollegeQuarantineError('source_field_or_feature_id_drift')
        seen.add(fid);values=[fid]
        for name in properties:
            d=declarations_by_name[name];value=attrs[name];kind=d['type']
            if value is None:
                if d['nillable']!='true':raise CollegeQuarantineError('nonnullable_source_field:'+name)
            elif kind=='xsd:string':
                if type(value)is not str:raise CollegeQuarantineError('source_string_type:'+name)
            elif kind in('xsd:int','xsd:short'):
                limit=2**(31 if kind=='xsd:int'else 15)
                if type(value)is not int or not-limit<=value<limit:raise CollegeQuarantineError('source_integer_width:'+name)
            elif kind=='xsd:decimal':
                if type(value)not in(int,Decimal):raise CollegeQuarantineError('source_decimal_type:'+name)
                value=Decimal(value)
                if not value.is_finite()or value.as_tuple().exponent < -9 or len(value.as_tuple().digits)>29:raise CollegeQuarantineError('decimal_storage_bound_requires_review')
            else:raise CollegeQuarantineError('unexpected_source_type')
            values.append(value)
        number=attrs['Volcano_Number']
        if number in numbers:raise CollegeQuarantineError('duplicate_original_volcano_number')
        numbers.add(number)
        if type(geom)is not dict or set(geom)!={'type','coordinates'}or geom['type']!='Point'or len(geom['coordinates'])!=2:raise CollegeQuarantineError('invalid_native_point')
        x,y=geom['coordinates']
        if type(x)not in(int,Decimal)or type(y)not in(int,Decimal)or not Decimal(-180)<=x<=Decimal(180)or not Decimal(-90)<=y<=Decimal(90):raise CollegeQuarantineError('nonfinite_or_outside_native_coordinate')
        if Decimal(x)!=Decimal(attrs['Longitude'])or Decimal(y)!=Decimal(attrs['Latitude']):raise CollegeQuarantineError('native_geometry_attribute_disagreement')
        values.append(struct.pack('<BIdd',1,1,float(x),float(y)));rows.append(values)
        regions[attrs['Region']]=regions.get(attrs['Region'],0)+1
    schema=[pa.field('id',pa.string(),nullable=False,metadata={b'original_role':b'GeoJSON Feature.id; unstable server identity, not Volcano_Number'})]
    types={'xsd:int':pa.int32(),'xsd:short':pa.int16(),'xsd:decimal':pa.decimal128(38,9),'xsd:string':pa.string()}
    for d in FIELD_DEFINITIONS:
        schema.append(pa.field(d['name'],pa.binary()if d['name']=='GeoLocation'else types[d['type']],nullable=d['nillable']=='true',metadata={k.encode():v.encode()for k,v in d.items()}))
    rows.sort(key=lambda v:v[1]);table=pa.Table.from_arrays([pa.array([row[i]for row in rows],type=f.type)for i,f in enumerate(schema)],schema=pa.schema(schema))
    return table,{'count':expected_count,'regions':regions,'all_original_feature_ids':sorted(seen),'all_original_volcano_numbers':sorted(numbers),'crs':'EPSG:4326','decimal_storage':'Exact decimal128(38,9) within explicit drift bounds; no float coercion of source properties.'}


def inspect_captures(captures):
    if set(captures)!=set(URLS):raise CollegeQuarantineError('capture_roster_drift')
    for n,(b,r)in captures.items():validate_receipt(n,b,r)
    if b'webservices.cfm'not in captures['gvp'][0]or b'GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes'not in captures['gvp_services'][0]:raise CollegeQuarantineError('missing_official_machine_delegation')
    for n in('wfs_schema','wfs_schema_after'):
        if encode(declarations(captures[n][0]))!=encode(FIELD_DEFINITIONS):raise CollegeQuarantineError('source_schema_drift')
    for n in('wfs_hits','wfs_hits_after'):
        if int(ET.fromstring(captures[n][0]).attrib['numberOfFeatures'])!=INITIAL_COUNT:raise CollegeQuarantineError('source_hitcount_drift')
    before,audit=decode_population(captures['wfs_all'][0]);after,after_audit=decode_population(captures['wfs_all_after'][0])
    if not before.drop(['id']).equals(after.drop(['id']),check_metadata=True):raise CollegeQuarantineError('population_changed_during_extraction')
    if digest(('\n'.join(audit['all_original_feature_ids'])+'\n').encode())!=INITIAL_SOURCE_ID_SHA256:raise CollegeQuarantineError('unstable_original_feature_ids_changed_requires_review')
    audit['after_original_feature_ids']=after_audit['all_original_feature_ids'];audit['changed_server_feature_ids']=sum(x!=y for x,y in zip(before['id'].to_pylist(),after['id'].to_pylist()))
    return before,audit


def parquet_table(table):
    geo={'version':'1.1.0','primary_column':'GeoLocation','columns':{'GeoLocation':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_epsg(4326).to_json_dict()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def validate_output(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet';actual=pq.read_table(path)
    if not actual.equals(parquet_table(table),check_metadata=True):raise CollegeQuarantineError('full_typed_native_output_drift')
    for w in actual['GeoLocation'].to_pylist():
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise CollegeQuarantineError('invalid_native_output_geometry')


def manifest_for(captures,identity,output_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Historical Holocene Volcano Locations — complete Smithsonian GVP5.4.0 changed successor draft','publisher':'Smithsonian Institution Global Volcanism Program','agency':'Smithsonian Institution','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n])for n in URLS],
        'transformations':[{'operation':'complete_official_WFS_snapshot','parameters':{'source_ids':'Both original unstable Feature.id and stable Volcano_Number retained distinctly; no historical ID join accepted.','types':'xsd:int int32, xsd:short int16, exact xsd:decimal decimal128(38,9) with explicit bounds; original schema and raw lexical JSON retained.','geometry':'Original GeoJSON Point XY in EPSG4326, native binary64 WKB, no reprojection/repair.','relationship':'Changed current1214 Holocene roster, not automatically equivalent to historical1383-row baseline.','upstream_cited_release':'VOTW5.4.0;7Aug2026, as explicitly stated on producer landing/citation pages; WFS release remains unspecified.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve original int32/int16/exact decimals/string/null/source IDs and native WKB; destructive coercion blocked.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Exact decimal property representation has no approved GeoPackage conversion.','shapefile_zip':'Names, decimals, strings and nulls have no approved destructive DBF mapping.','pmtiles':'Quantized visualization and ID mapping remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise CollegeQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(8_000_000 if name.endswith('.parquet')else MAX_AUX_BYTES))


def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/holocene_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values


def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(URLS):raise CollegeQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise CollegeQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise CollegeQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')})
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise CollegeQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old


def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise CollegeQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise CollegeQuarantineError('private_candidate_roster_drift')
    if cached:
        allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        if roster-(names|{READY}|allowed):raise CollegeQuarantineError('unknown_cached_object')
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise CollegeQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise CollegeQuarantineError('candidate_ready_drift')
    validate_output(root,table)


def stage_holocene(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));version='v0.0.0-draft-gvp54-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise CollegeQuarantineError('linked_lock')
        fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
        # A private deterministic output provides the pin before any trust seal.
        build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='candidate-',dir=build)as temporary:
            temp=Path(temporary);(temp/'geoparquet').mkdir();path=temp/'geoparquet'/f'{SLUG}.parquet';pq.write_table(parquet_table(table),path,compression='zstd');validate_output(temp,table)
            manifest=manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':sha256_file(path)})
            if root.exists()or state_present(storage,version):
                try:captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest)
                except Exception as exc:
                    try:record_observed_loss(storage,version,fingerprint,type(exc).__name__)
                    except Exception as write:raise IncidentWriteError('incident_failure_candidate_preserved')from write
                    if root.exists():
                        q=base/'_quarantine'/SLUG/('observed-loss-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q)
                    raise CollegeQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise CollegeQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
