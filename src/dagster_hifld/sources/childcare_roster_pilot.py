"""NY OCFS / WA DCYF rosters and separately cached Census NAD83 geocoding results."""
from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely.geometry import Point

from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get, content_derived_version, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha

DATASET = 'childcare-government-roster-geocoding-pilot'
FILES = {'ny':'new-york-census-matched-results', 'wa':'washington-census-matched-results'}
BENCHMARKS = 'https://geocoding.geo.census.gov/geocoder/benchmarks'
BATCH = 'https://geocoding.geo.census.gov/geocoder/locations/addressbatch'
API_DOC = 'https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html'
CRS_DOC = 'https://www2.census.gov/geo/pdfs/maps-data/data/FAQ_for_Census_Bureau_Public_Geocoder.pdf'
SOURCES = {
    'ny': {'base':'https://data.ny.gov', 'id':'cb42-qumz',
           'producer':'NYS Office of Children and Family Services','owner':'NY Open Data',
           'id_field':'facility_id','type_field':'program_type',
           'types':['DCC','SDCC','SACC'],
           'schema':'49942c8a3f0cdd68f281ec4ebe8a540199290364f59c7fac0cce74d7cff5879e',
           'coverage':'OCFS center/school programs only; NYC day-care centers excluded; homes excluded.',
           'cadence':'Daily'},
    'wa': {'base':'https://data.wa.gov', 'id':'was8-3ni8', 'producer':'DCYF','owner':'DCYF Open Data',
           'id_field':'wacompassid','type_field':'facilitytypegeneric',
           'types':['CHILD CARE CENTER','SCHOOL-AGE PROGRAM','OUTDOOR NATURE BASED PROGRAM'],
           'schema':'0e0e7928ccec9761101b1e063e8bc88dfa6796ac90a610a07941310e7d506f9e',
           'coverage':'DCYF center/school/outdoor programs only; homes excluded. Native duplicate IDs retained.',
           'cadence':'Monthly'},
}
GATES = {name:False for name in ('source_release_accepted','freshness_accepted','schema_contract_accepted',
    'id_contract_accepted','rights_accepted','crs_accepted','coverage_accepted','mapping_approved','release_accepted')}
NOTE = ('Partial Census geocoding results, NOT a complete facility inventory or historical HIFLD reconstruction. '
        'All input rows and geocoding outcomes retained separately. Census points are NAD83 EPSG:4269 '
        'address-range interpolation, not verified building entrances. Agency coordinates remain unchanged '
        'attributes with unverified CRS; never blended. Public_AR_Current mutates; capture time is not an immutable benchmark edition.')


def _local(storage: StagingStorageResource, directory: str) -> Path:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or not storage.use_local or storage.bucket or storage.prefix or not storage.local_dir or not directory):
        raise ValueError('explicit_local_staging_and_capture_directory_required')
    root=Path(directory).absolute()
    for path in (root,Path(storage.local_dir).absolute()):
        if '..' in path.parts or any(p.is_symlink() for p in (path,*path.parents)):
            raise ValueError('unsafe_local_directory')
    root.mkdir(parents=True,exist_ok=True)
    return root


def _read_capture(root: Path, name: str, url: str, method: str='GET', request_sha: str | None=None):
    receipt=json.loads((root/f'{name}.receipt.json').read_bytes())
    body=(root/f'{name}.body').read_bytes();wire=(root/f'{name}.wire').read_bytes()
    if (receipt.get('method')!=method or receipt.get('url')!=url or receipt.get('final_url')!=url
            or receipt.get('status')!=200 or receipt.get('complete_body') is not True
            or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=sha(body)
            or receipt.get('wire_bytes')!=len(wire) or receipt.get('wire_sha256')!=sha(wire)
            or (method=='POST' and receipt.get('request_sha256')!=request_sha)):
        raise ValueError('frozen_capture_identity_or_bytes_changed:'+name)
    return body,receipt


def frozen_get(client: httpx.Client, url: str, root: Path, name: str):
    """Replay exact successful original GET bytes; never pretend a replay was fetched now."""
    if (root/f'{name}.receipt.json').exists():return _read_capture(root,name,url)
    return capture_get(client,url,root,name,max_bytes=24_000_000)


def _checkpoint(storage, root, state, stage):
    key=f'_childcare-checkpoints/{state}/{stage}-{sha(str(root).encode())}.json'
    if storage.object_snapshot(key) is not None:
        pins=json.loads(storage.read_key(key));validate_capture_roster(root,pins)
    else:
        pins=capture_roster_digests(root);write_immutable(storage,key,canonical(pins)+b'\n')
    return pins


def _quarantine(storage, root, state, error):
    captures={p.name.removesuffix('.receipt.json'):
        ((root/(p.name.removesuffix('.receipt.json')+'.body')).read_bytes(),json.loads(p.read_bytes()))
        for p in root.glob('*.receipt.json')}
    quarantine_incomplete_capture(storage,root,DATASET,state,captures,type(error).__name__)


def _metadata(state, meta):
    spec=SOURCES[state]
    columns=[(c['fieldName'],c['dataTypeName']) for c in meta['columns'] if not c['fieldName'].startswith(':')]
    if (meta.get('id')!=spec['id'] or meta.get('attribution')!=spec['producer']
            or meta.get('owner',{}).get('displayName')!=spec['owner']
            or sha(canonical(columns))!=spec['schema'] or not isinstance(meta.get('rowsUpdatedAt'),int)):
        raise ValueError('producer_native_schema_or_identity_drift')
    if meta.get('license') is not None:raise ValueError('source_license_changed_review_required')
    custom=meta.get('metadata',{}).get('custom_fields',{})
    cadence=(custom.get('Dataset Summary',{}) if state=='ny' else custom.get('Temporal',{})).get('Posting Frequency')
    if cadence!=spec['cadence']:raise ValueError('source_refresh_cadence_drift')
    return {'id':meta['id'],'attribution':meta['attribution'],'owner':meta['owner']['displayName'],
            'columns':columns,'rowsUpdatedAt':meta['rowsUpdatedAt'],'license':meta.get('license'),
            'description':meta.get('description'),'custom_fields':custom}


def validate_rows(state, rows, meta, expected_count):
    """Check exact population and native lexical IDs, preserving duplicate agency IDs."""
    spec=SOURCES[state];allowed={c['fieldName'] for c in meta['columns']}|{'_socrata_row_id'}
    if not rows or len(rows)!=expected_count:raise ValueError('roster_population_incomplete')
    native_ids=[];row_ids=[]
    for row in rows:
        native=row.get(spec['id_field']);technical=row.get('_socrata_row_id')
        if (not isinstance(row,dict) or set(row)-allowed or not isinstance(native,str) or not native
                or not isinstance(technical,str) or not technical
                or row.get(spec['type_field']) not in spec['types']):
            raise ValueError('roster_native_field_id_or_scope_drift')
        native_ids.append(native);row_ids.append(technical)
    if len(set(row_ids))!=len(rows):raise ValueError('duplicate_socrata_technical_row_id')
    return {'source_feature_count':len(rows),'unique_native_id_count':len(set(native_ids)),
            'duplicate_native_id_count':len(rows)-len(set(native_ids)),
            'native_id_sha256':sha(canonical(native_ids)),'technical_row_ids_sha256':sha(canonical(row_ids))}


def acquire_roster(storage: StagingStorageResource, state: str, capture_directory: str) -> dict:
    """Acquire <=10,000 filtered public center rows with before/after population witnesses."""
    spec=SOURCES[state];root=_local(storage,str(Path(capture_directory)/state/'roster'))
    _checkpoint_key=f'_childcare-checkpoints/{state}/roster-{sha(str(root).encode())}.json'
    if storage.object_snapshot(_checkpoint_key) is not None:
        validate_capture_roster(root,json.loads(storage.read_key(_checkpoint_key)))
    base=spec['base'];resource=f"{base}/resource/{spec['id']}.json"
    where=spec['type_field']+' in('+','.join("'"+v+"'" for v in spec['types'])+')'
    def query(**kwargs):return resource+'?'+urlencode({'$'+k:v for k,v in kwargs.items()})
    groups={'select':spec['type_field']+',count(*) as n','group':spec['type_field'],'limit':'100'}
    witnesses={'select':':id as _socrata_row_id,'+spec['id_field'],'where':where,'order':':id','limit':'10000'}
    urls=[('metadata_before',f"{base}/api/views/{spec['id']}.json"),('counts_before',query(**groups)),
          ('ids_before',query(**witnesses)),('roster',query(select='*,:id as _socrata_row_id',where=where,order=':id',limit='10000')),
          ('ids_after',query(**witnesses)),('counts_after',query(**groups)),
          ('metadata_after',f"{base}/api/views/{spec['id']}.json")]
    captures={}
    try:
        with httpx.Client(timeout=90,follow_redirects=False) as client:
            for name,url in urls:captures[name]=frozen_get(client,url,root,name)
        before=json.loads(captures['metadata_before'][0]);after=json.loads(captures['metadata_after'][0])
        if _metadata(state,before)!=_metadata(state,after):raise ValueError('source_changed_during_capture')
        counts=json.loads(captures['counts_before'][0])
        if counts!=json.loads(captures['counts_after'][0]):raise ValueError('source_count_drift')
        if any(set(c)!={spec['type_field'],'n'} or not c['n'].isdigit() for c in counts):raise ValueError('invalid_population_counts')
        expected=sum(int(c['n']) for c in counts if c[spec['type_field']] in spec['types'])
        if expected>10000:raise ValueError('bounded_pilot_population_limit')
        rows=json.loads(captures['roster'][0]);audit=validate_rows(state,rows,before,expected)
        ids=[{k:row[k] for k in ('_socrata_row_id',spec['id_field'])} for row in rows]
        if ids!=json.loads(captures['ids_before'][0]) or ids!=json.loads(captures['ids_after'][0]):
            raise ValueError('source_id_roster_drift')
        actual=Counter(row[spec['type_field']] for row in rows)
        if actual!={c[spec['type_field']]:int(c['n']) for c in counts if c[spec['type_field']] in spec['types']}:
            raise ValueError('source_category_count_drift')
        audit.update({'category_counts':counts,'excluded_category_count':sum(int(c['n']) for c in counts if c[spec['type_field']] not in spec['types']),
            'source_updated_at':datetime.fromtimestamp(before['rowsUpdatedAt'],timezone.utc).isoformat(),
            'coverage_note':spec['coverage'],'source_license':None,'agency_crs':None,
            'jurisdictions_inspected':2,'proposed_jurisdictions':56,'other_jurisdictions_uninspected':54,
            'source_schema_sha256':sha(canonical(_metadata(state,before)['columns']))})
        _checkpoint(storage,root,state,'roster')
    except Exception as exc:
        _quarantine(storage,root,state,exc);raise
    return {'state':state,'rows':rows,'metadata':before,'audit':audit,'capture_root':str(root),
            'captures':captures,'source_record_sha256':sha(canonical(rows))}


def _coordinate(x, y):
    try:x,y=float(x),float(y)
    except (ValueError,TypeError):return None
    if not (math.isfinite(x) and math.isfinite(y) and -180<=x<=180 and -90<=y<=90) or (x,y)==(0,0):return None
    return [x,y]


def classify_location(state: str, row: dict) -> tuple[str,list[str] | None]:
    """Never reverse suppressed addresses or replace usable agency coordinates."""
    if state=='ny':
        if row.get('address_omitted') not in (None,''):return 'withheld',None
        xy=_coordinate(row.get('longitude'),row.get('latitude'))
        active=row.get('facility_status') in ('License','Registration')
        address=[' '.join(str(row.get(k) or '').strip() for k in ('street_number','street_name')).strip(),
                 row.get('city'),row.get('state'),row.get('zip_code')]
    else:
        xy=_coordinate(row.get('physicallongitude'),row.get('physciallatitude'))
        active=row.get('latestoperatingstatus')=='Active'
        address=[row.get(k) for k in ('physicalstreetaddress','physicalcity','physicalstate','physicalzip')]
    if xy:return 'agency_location_preserved',None
    if not active:return 'inactive',None
    if any(not isinstance(v,str) or not v.strip() for v in address):return 'incomplete_address',None
    if re.search(r'\bP\s*\.?\s*O\s*\.?\s*BOX\b|\bPOST\s+OFFICE\s+BOX\b',address[0],re.I):return 'po_box',None
    if (not re.match(r'^\s*\d',address[0]) or address[2].strip().upper()!=state.upper()
            or not re.fullmatch(r'\d{5}(?:-\d{4})?',address[3].strip())):return 'incomplete_address',None
    return 'eligible',[v.strip() for v in address]


def parse_batch(body: bytes, requests: list[list[str]]) -> dict:
    """Account one result per technical request ID, including Tie and No_Match."""
    expected={r[0] for r in requests};result={}
    for fields in csv.reader(io.StringIO(body.decode('utf-8-sig'),newline=''),strict=True):
        if len(fields)<3 or fields[0] not in expected or fields[0] in result or fields[2] not in ('Match','No_Match','Tie'):
            raise ValueError('geocoder_unexpected_duplicate_id_or_status')
        coords=None
        if fields[2]=='Match':
            if len(fields)!=8:raise ValueError('geocoder_match_schema_drift')
            parts=fields[5].split(',')
            if len(parts)!=2 or (coords:=_coordinate(*parts)) is None:raise ValueError('geocoder_invalid_coordinates')
        result[fields[0]]={'status':fields[2],'coordinates':coords,'response_fields':fields}
    if set(result)!=expected:raise ValueError('geocoder_incomplete_response')
    return result


def _batch(client, root, requests):
    if not 0<len(requests)<=10000:raise ValueError('invalid_census_batch_size')
    out=io.StringIO(newline='');csv.writer(out,lineterminator='\n').writerows(requests)
    request_body=out.getvalue().encode();name='batch-'+sha(request_body)
    request_path=root/(name+'.request.csv')
    if request_path.exists() and request_path.read_bytes()!=request_body:raise ValueError('geocoder_request_tamper')
    request_path.write_bytes(request_body)
    if (root/f'{name}.receipt.json').exists():
        return _read_capture(root,name,BATCH,'POST',sha(request_body)),name
    # The wire multipart request is retained exactly, including benchmark and CSV.
    request=client.build_request('POST',BATCH,data={'benchmark':'4'},
        files={'addressFile':('addresses.csv',request_body,'text/csv')},headers={'Accept-Encoding':'identity'})
    wire_request=request.read();(root/(name+'.request.wire')).write_bytes(wire_request)
    (root/(name+'.request.headers.json')).write_bytes(canonical(dict(request.headers))+b'\n')
    wire_path=root/(name+'.wire');body_path=root/(name+'.body')
    wire_path.write_bytes(b'');body_path.write_bytes(b'')
    def retain(path,data):
        with path.open('ab') as f:f.write(data)
    receipt={'method':'POST','url':BATCH,'observed_at':datetime.now(timezone.utc).isoformat(),
             'request_sha256':sha(request_body),'wire_request_sha256':sha(wire_request),
             'benchmark':'4','benchmark_name':'Public_AR_Current','complete_body':False,'status':None,'final_url':None}
    response=None
    try:
        response=client.send(request,stream=True)
        result=capture_http_response(response,max_wire_bytes=5_000_000,max_decoded_bytes=5_000_000,
                reserve=lambda _:None,retain_wire=lambda b:retain(wire_path,b),retain_decoded=lambda b:retain(body_path,b))
        receipt.update(status=result.status_code,final_url=str(response.url),complete_body=result.complete,
                           content_encoding=result.content_encoding,content_length=result.content_length,error_code=result.error_code)
    finally:
        if response is not None:response.close()
        wire=wire_path.read_bytes();body=body_path.read_bytes()
        receipt.update(bytes=len(body),sha256=sha(body),wire_bytes=len(wire),wire_sha256=sha(wire))
        (root/(name+'.receipt.json')).write_bytes(canonical(receipt)+b'\n')
    return _read_capture(root,name,BATCH,'POST',sha(request_body)),name


def geocode_roster(storage: StagingStorageResource, artifact: dict, capture_directory: str, max_addresses: int) -> dict:
    """Build a matched-only spatial companion plus complete input/outcome ledgers."""
    if type(max_addresses) is not int or not 1<=max_addresses<=10000:raise ValueError('explicit_bounded_geocoding_limit_required')
    state=artifact['state'];spec=SOURCES[state];rows=artifact['rows']
    if sha(canonical(rows))!=artifact['source_record_sha256']:raise ValueError('input_roster_changed')
    root=_local(storage,str(Path(capture_directory)/state/('census-'+artifact['source_record_sha256'])))
    sealkey=f'_childcare-checkpoints/{state}/census-{sha(str(root).encode())}.json'
    if storage.object_snapshot(sealkey) is not None:validate_capture_roster(root,json.loads(storage.read_key(sealkey)))
    outcomes=[];requests=[];captures=dict(artifact['captures']);capture_roots={n:Path(artifact.get('capture_root','')) for n in captures}
    for i,row in enumerate(rows):
        status,address=classify_location(state,row);technical=f'technical-{i:08d}'
        if status=='eligible':
            status='requested' if len(requests)<max_addresses else 'deferred'
            if status=='requested':requests.append([technical,*address])
        outcomes.append({'technical_request_id':technical,'socrata_row_id':row['_socrata_row_id'],
                         'native_id':row[spec['id_field']],'status':status,'result':None})
    try:
        with httpx.Client(timeout=180,follow_redirects=False) as client:
            captures['census_benchmarks']=frozen_get(client,BENCHMARKS,root,'census_benchmarks')
            capture_roots['census_benchmarks']=root
            benchmarks=json.loads(captures['census_benchmarks'][0])
            if not any(b.get('id')=='4' and b.get('benchmarkName')=='Public_AR_Current' for b in benchmarks.get('benchmarks',[])):
                raise ValueError('Census_benchmark_identity_drift')
            results={}
            if requests:
                pair,name=_batch(client,root,requests);captures[name]=pair;capture_roots[name]=root
                results=parse_batch(pair[0],requests)
        for item in outcomes:
            if item['status']=='requested':
                item['result']=results[item['technical_request_id']];item['status']=item['result']['status']
        counts=dict(Counter(item['status'] for item in outcomes))
        if sum(counts.values())!=len(rows) or any(x['status'] in ('eligible','requested') for x in outcomes):raise ValueError('outcome_accounting_incomplete')
        _checkpoint(storage,root,state,'census')
    except Exception as exc:
        _quarantine(storage,root,state,exc);raise
    audit={**artifact['audit'],**GATES,'geocoding_outcomes':counts,'geocoding_requested':len(requests),
           'max_addresses':max_addresses,'matched_result_count':counts.get('Match',0),
           'complete_roster_count':len(rows),'census_geometry_crs_epsg':4269,'agency_geometry_crs':None,
           'national_complete':False,'technical_key_note':'technical-* correlates one capture row, never an agency/HIFLD/published facility ID.',
           'coverage_note':spec['coverage']+' '+NOTE}
    if not counts.get('Match'):raise ValueError('no_matched_points_for_spatial_companion; complete checkpoint retained')
    matched=[]
    for row,outcome in zip(rows,outcomes,strict=True):
        if outcome['status']=='Match':
            if any(k in row for k in ('geometry','source_json','technical_request_id','census_response_json')):raise ValueError('derived_column_collision')
            matched.append({**row,'source_json':canonical(row).decode(),'technical_request_id':outcome['technical_request_id'],
                'census_response_json':canonical(outcome['result']).decode(),'geometry':Point(*outcome['result']['coordinates']).wkb})
    table=pa.Table.from_pylist(matched)
    table=table.replace_schema_metadata({b'geo':canonical({'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{
        'encoding':'WKB','geometry_types':['Point'],'crs':CRS.from_epsg(4269).to_json_dict(),'edges':'planar'}}}),
        b'hifld:scope':NOTE.encode(),b'hifld:native_schema':canonical(artifact['metadata']['columns'])})
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd');parquet=sink.getvalue().to_pybytes()
    repo=Path(__file__).parents[3];identity=processing_identity(repo)
    validate_construction_code_identity(repo,identity['code_commit'],identity['files'],identity=identity)
    source_hash=sha(canonical({'roster':artifact['source_record_sha256'],'outcomes':outcomes,
                              'captures':{n:r['sha256'] for n,(_,r) in captures.items()},'limit':max_addresses}))
    schema_hash=sha(canonical(artifact['metadata']['columns']));geometry_hash=sha(b''.join(row['geometry'] for row in matched))
    version=content_derived_version(source_hash,geometry_hash,schema_hash,identity)
    prefix=f'{DATASET}/{FILES[state]}/{version}';state_key=f'_construction-state/{DATASET}/{FILES[state]}-{version}.json';fingerprint=sha(canonical(identity))
    result={**GATES,'state':state,'dataset_slug':DATASET,'file_slug':FILES[state],'version':version,'prefix':prefix,
            'feature_count':len(matched),'roster_count':len(rows),'outcome_count':sum(counts.values()),'capture_directory':str(root),
            'outcomes':counts,'candidate_ready':prefix+'/metadata/candidate_ready.json'}
    if storage.object_snapshot(state_key) is not None:
        payload=json.loads(load_first_construction(storage,state_key,version,fingerprint))['payload']
        verify_cached_candidate(storage,repo,state_key,prefix,version,fingerprint,identity,source_hash,schema_hash,payload['objects'],payload['capture_receipts'])
        return {**result,'reused':True}
    name=f'geoparquet/{FILES[state]}.parquet';descriptor={'bytes':len(parquet),'sha256':sha(parquet)}
    inputs=[]
    for n,(_,r) in captures.items():
        producer='U.S. Census Bureau' if n.startswith(('batch-','census_')) else spec['producer']
        inputs.append({'input_id':n,'producer':producer,'host':urlsplit(r['url']).hostname,'role':'producer_publication',
            'canonical_url':r['url'],'retrieval_url':r['url'],'observed_final_url':r['final_url'],
            'retrieved_at':r['observed_at'],'http_status':r['status'],'bytes':r['bytes'],'sha256':r['sha256'],
            'license':'Unaccepted; government public access does not establish full derivative rights.',
            'attribution':producer,'release_date_unknown_reason':'Mutable endpoint; source update metadata retained independently from fetch time.'})
    manifest={'schema_version':1,'approved':False,'title':state.upper()+' Census matched child-care address results (pilot)',
        'description':NOTE+' '+spec['coverage'],'publisher':spec['producer']+' and U.S. Census Bureau',
        'agency':spec['producer'],'source_url':spec['base']+'/resource/'+spec['id']+'.json','license':'Unknown/unaccepted for release.',
        'processor':'HIFLD Next','code_commit':identity['code_commit'],'inputs':inputs,
        'transformations':[{'operation':'filtered_public_center_roster_to_separate_Census_matched_point_companion',
            'parameters':{'max_addresses':max_addresses,'benchmark':'Public_AR_Current (4), mutable',
                          'crs':4269,'native_fields':'All returned JSON values preserved; exact raw bytes and Socrata schema retained.',
                          'identity':'No agency ID deduplication; technical request IDs are capture-row correlations only.',
                          'geometry':NOTE,'accounting':counts}}],
        'documentation_url':API_DOC,'lineage':NOTE,'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Explicit matched-only NAD83 Census result companion; whole roster and outcome ledger retained separately.',
            'objects':[{'path':name,**descriptor}]}}},
        'unsupported_output_formats':{'geopackage':'No approved mapping.','pmtiles':'No approved display transform.'}}
    validate_release_provenance(manifest)
    outputs={name:parquet,'metadata/source_manifest.json':canonical(manifest)+b'\n','metadata/source_audit.json':canonical(audit)+b'\n',
        'metadata/processing_identity.json':canonical(identity)+b'\n','metadata/complete_input_roster.json':canonical(rows)+b'\n',
        'metadata/geocoding_outcomes.json':canonical(outcomes)+b'\n','metadata/agency_schema.json':canonical(artifact['metadata'])+b'\n',
        'metadata/candidate_ready.json':canonical({**result,'approved':False,'objects':{name:descriptor}})+b'\n'}
    for n,(body,receipt) in captures.items():
        outputs.update({f'raw/{n}.body':body,f'raw/{n}.wire':(capture_roots[n]/f'{n}.wire').read_bytes(),
                        f'raw/{n}.receipt.json':canonical(receipt)+b'\n'})
    for path in root.glob('*.request.*'):outputs['raw/'+path.name]=path.read_bytes()
    objects={n:{'bytes':len(b),'sha256':sha(b)} for n,b in outputs.items()}
    for n,body in outputs.items():write_immutable(storage,prefix+'/'+n,body)
    seal_ready_candidate(storage,state_key,version,fingerprint,{'processing_identity':identity,'source_record_sha256':source_hash,
        'source_schema_sha256':schema_hash,'capture_receipts':{n:r for n,(_,r) in captures.items()},'objects':objects})
    verify_cached_candidate(storage,repo,state_key,prefix,version,fingerprint,identity,source_hash,schema_hash,objects,{n:r for n,(_,r) in captures.items()})
    return {**result,'reused':False}
