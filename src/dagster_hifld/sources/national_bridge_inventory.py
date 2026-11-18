"""FHWA National Bridge Inventory SNBI reconstruction draft; never release approval."""
from __future__ import annotations
import fcntl
import hashlib
import html
import importlib.metadata
import json
import os
import platform
import re
import shutil
import stat
import struct
import subprocess
import tempfile
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin
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
from dagster_hifld.sources.capture_common import _validate_cached_catalog_sidecars
from dagster_hifld.validation import sha256_file

SLUG='national-bridge-inventory-nbi'
PRODUCER='United States Department of Transportation, Federal Highway Administration'
INDEX_URL='https://www.fhwa.dot.gov/bridge/nbi/nbi_data.cfm'
SCHEMA_PAGE_URL='https://www.fhwa.dot.gov/bridge/snbi/schema.cfm'
PROGRAM='https://www.fhwa.dot.gov/bridge/nbi.cfm'
LICENSE='https://www.fhwa.dot.gov/webpolicies'
CROSSWALK_URL='https://www.fhwa.dot.gov/bridge/snbi/datacrosswalk.xlsx'
READY='metadata/nbi_candidate_ready.json'
SPACE_FLOOR=8_000_000_000
MAX_AUX_BYTES=32_000_000
MAX_BODY_BYTES=512*1024*1024
MAX_PARQUET_BYTES=4_000_000_000
MIN_STATES=50
STATE_TOKENS={'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA','colorado':'CO','connecticut':'CT','delaware':'DE','district of columbia':'DC','florida':'FL','georgia':'GA','hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA','kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD','massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS','missouri':'MO','montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH','new jersey':'NJ','new mexico':'NM','new york':'NY','north carolina':'NC','north dakota':'ND','ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA','puerto rico':'PR','rhode island':'RI','south carolina':'SC','south dakota':'SD','tennessee':'TN','texas':'TX','utah':'UT','vermont':'VT','virginia':'VA','washington':'WA','west virginia':'WV','wisconsin':'WI','wyoming':'WY','u.s. virgin islands':'VI','us virgin islands':'VI','guam':'GU'}
NATIVE_CRS_JSON={'$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json', 'type': 'GeographicCRS', 'name': 'WGS 84', 'datum': {'type': 'GeodeticReferenceFrame', 'name': 'World Geodetic System 1984', 'ellipsoid': {'name': 'WGS 84', 'semi_major_axis': 6378137, 'inverse_flattening': 298.257223563}}, 'coordinate_system': {'subtype': 'ellipsoidal', 'axis': [{'name': 'Geodetic latitude', 'abbreviation': 'Lat', 'direction': 'north', 'unit': 'degree'}, {'name': 'Geodetic longitude', 'abbreviation': 'Lon', 'direction': 'east', 'unit': 'degree'}]}, 'id': {'authority': 'EPSG', 'code': 4326}}
# Clean 1995 Coding Guide transitions from the official FHWA SNBI Data
# Crosswalk (datacrosswalk.xlsx). Tags not listed are SNBI fields with no
# single clean Coding Guide equivalent; they are published under their SNBI
# data tag rather than inventing a Coding Guide value.
CODING_GUIDE_ITEMS={
'BAP01':('72','Approach Roadway Alignment'),'BC01':('58','Deck'),'BC02':('59','Superstructure'),'BC03':('60','Substructure'),
'BC04':('62','Culvert'),'BC09':('61','Channel and Channel Protetion'),'BC12':('CAT10','Bridge Condition'),'BC13':('CAT23','Lowest Condition Code'),
'BCL01':('22','Owner'),'BCL02':('21','Maintenance Responsibility'),'BCL05':('20','Toll'),'BCS01':('CS1','Element Quantity Condition State One'),
'BCS02':('CS2','Element Quantity Condition State Two'),'BCS03':('CS3','Element Quantity Condition State Three'),'BCS04':('CS4','Element Quantity Condition State Four'),
'BE01':('EN','Element Number'),'BE02':('EPN','Element Parent Number'),'BE03':('TOTALQTY','Element Total Quantity'),
'BF01':('42A/42B','Type of Service'),'BF02':('42A/42B','Type of Service'),'BF03':('6A/7','Features Intersected / Facility Carried by Structure'),
'BG02':('49','Structure Length'),'BG03':('48','Length of Maximum Span'),'BG05':('52','Deck Width, Out-to-Out'),'BG06':('51','Bridge Roadway Width, Curb-to-Curb'),
'BG07':('50A','Left curb or sidewalk width'),'BG08':('50B','Right curb or sidewalk width'),'BG09':('32','Approach Roadway Width'),
'BG10':('33','Bridge Median'),'BG11':('34','Skew'),'BH01':('26','Functional Classification of Inventory Route'),'BH03':('104','Highway System of the Inventory Route'),
'BH05':('100','STRAHNET Highway Designation'),'BH07':('11','Kilometerpoint'),'BH08':('28A','Lanes on Structure'),'BH09':('29','Average Daily Traffic'),
'BH10':('109','Average Daily Truck Traffic'),'BH11':('30','Year of Average Daily Traffic'),'BH12':('10','Inventory Route, Minimum Vertical Clearance'),
'BH13':('53/54A/54B','Minimum Vertical Clearance Over Bridge Roadway / Reference Feature / Minimum Vertical Underclearance'),
'BH14':('55A/56','Reference Feature / Minimum Lateral Underclearance on Left'),'BH15':('55A/55B','Reference Feature / Minimum Lateral Underclearance on Right'),
'BH16':('47','Inventory Route, Total Horizontal Clearance'),'BH17':('19','Bypass, Detour Length'),'BID01':('8','Structure Number'),
'BIR01':('92A','Critical Feature Inspection- Fracture Critical Details'),'BIR03':('92B','Critical Feature Inspection - Underwater Inspection'),
'BL01':('1','State Code'),'BL02':('3','County (Parish) Code'),'BL03':('4','Place Code'),'BL04':('2','Highway Agency District'),'BL05':('16','Latitude'),
'BL06':('17','Longitude'),'BL07':('99','Border Bridge Structure Number'),'BL08':('98A','Neighboring State Code'),'BL10':('1','State Code'),'BL11':('9','Location'),
'BLR01':('31','Design Load'),'BLR04':('63','Method Used To Determine Operating Rating'),'BLR05':('66','Inventory Rating'),'BLR06':('64','Operating Rating'),
'BN02':('39/43B/116','Navigation Vertical Clearance / Type of Design/Construction / Minimum Navigation Vertical Clearance Vertical Lift Bridge'),
'BN03':('39/43B','Navigation Vertical Clearance / Type of Design/Construction'),'BN04':('40','Navigation Horizontal Clearance'),
'BRR02':('54A/54B','Reference Feature / Minimum Vertical Underclearance'),'BRR03':('55A/55B','Reference Feature / Minimum Lateral Underclearance on Right'),
'BRT02':('5D/5E','Route Number / Directional Suffix'),'BRT04':('5B','Route Signing Prefix'),'BRT05':('5C','Designated Level of Service'),
'BW01':('27','Year Built'),'BW02':('106','Year Reconstructed'),
}
# The 2026-09-03 CT and MI re-uploads serve an undeclared top-level DATA_YEAR
# stamp (for example '8/4/2026') on every record. It is a producer extra, not a
# schema v04 property; keep it as a string column rather than dropping it.
PRODUCER_EXTRA_FIELDS={'DATA_YEAR':{'description':'Producer data year stamp served in the 2026-09-03 CT and MI re-uploads','type':'string'}}
RELEASE_LINK=re.compile(r'nbi_data_(\d{4})\.cfm',re.I)
SCHEMA_LINK=re.compile(r'SNBI\.Schema\.v[0-9.]+\.json',re.I)


class NBIQuarantineError(ValueError):pass
class IncidentWriteError(NBIQuarantineError):pass


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body):
    return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise NBIQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise NBIQuarantineError('metadata_must_be_object_without_error')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NBIQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise NBIQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise NBIQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('staging_alias_or_special_file_refused')

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise NBIQuarantineError('immutable_capture_changed:'+key)

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
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise NBIQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise NBIQuarantineError('changed_or_incomplete_original_input')
    return body

def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names

def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'

def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'

def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))

def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise NBIQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')

def record_observed_loss(storage,version,fingerprint,reason):
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason,
        'operator_action':'Restore exact original witnesses and explicitly retain/review this negative incident; no automatic bootstrap.'})
    seal_first_construction(storage,key,version,fingerprint,body)

def write_private(root,name,body):
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise NBIQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise NBIQuarantineError('incomplete_private_write:'+name)

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

def parquet_table(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':NATIVE_CRS_JSON}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def validate_output(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet';actual=pq.read_table(path)
    if not actual.equals(parquet_table(table),check_metadata=True):raise NBIQuarantineError('full_typed_native_output_drift')
    for w in actual['geometry'].to_pylist():
        if w is None:continue
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise NBIQuarantineError('invalid_native_output_geometry')

def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise NBIQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(MAX_PARQUET_BYTES if name.endswith('.parquet')else MAX_AUX_BYTES))

def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/nbi_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values

def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(captures):raise NBIQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise NBIQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise NBIQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')},count=a.num_rows)
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise NBIQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old

def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False,storage=None):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'capture_fingerprint':source_fingerprint(captures),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise NBIQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise NBIQuarantineError('private_candidate_roster_drift')
    if cached:
        allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        if roster-(names|{READY}|allowed):raise NBIQuarantineError('unknown_cached_object')
        for name in roster-(names|{READY}):strict_json(candidate_bytes(root,name))
        snapshot_names={'metadata/source/source_manifest.json',
            'metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        catalog_sidecars={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/resolved_provenance.json',
            'metadata/source/quality_manifest.json','metadata/source/data_dictionary.json'}
        for name in('metadata/source/source_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'):
            if name in roster and candidate_bytes(root,name)!=encode(manifest):raise NBIQuarantineError('cached_source_snapshot_drift')
        if roster & snapshot_names and roster & snapshot_names != snapshot_names:
            raise NBIQuarantineError('cached_source_manifest_snapshot_incomplete')
        if roster & catalog_sidecars:
            if not{'metadata/quality_manifest.json','metadata/data_dictionary.json'}<=roster:
                raise NBIQuarantineError('cached_catalog_metadata_incomplete')
            if roster & snapshot_names != snapshot_names:
                raise NBIQuarantineError('cached_source_manifest_snapshot_incomplete')
        if roster & catalog_sidecars:
            if storage is None:raise NBIQuarantineError('catalog_validation_requires_explicit_storage_context')
            prefix=f'{SLUG}/{SLUG}/{version}'
            actual_keys={storage._ensure_prefixed(f'{prefix}/{name}') for name in roster}
            try:_validate_cached_catalog_sidecars(storage,prefix,actual_keys)
            except (OSError,UnicodeError,TypeError,ValueError) as exc:
                raise NBIQuarantineError('cached_catalog_sidecar_semantics_invalid') from exc
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise NBIQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise NBIQuarantineError('candidate_ready_drift')
    validate_output(root,table)


def discover_release(index_body):
    years=[int(value)for value in RELEASE_LINK.findall(index_body.decode('utf-8','replace'))]
    if not years:raise NBIQuarantineError('producer_release_index_has_no_nbi_data_year')
    year=max(years)
    return year,f'https://www.fhwa.dot.gov/bridge/nbi/nbi_data_{year}.cfm'

def discover_schema(schema_page_body):
    match=SCHEMA_LINK.search(schema_page_body.decode('utf-8','replace'))
    if not match:raise NBIQuarantineError('producer_schema_page_has_no_schema_link')
    return urljoin(SCHEMA_PAGE_URL,match.group(0))

def parse_release_page(body,year,release_url):
    text=body.decode('utf-8','replace');states={};corrections={};total=None
    for row in re.findall(r'<tr>(.*?)</tr>',text,re.S|re.I):
        name_match=re.search(r'<th[^>]*scope="row"[^>]*>(.*?)</th>',row,re.S|re.I)
        count_match=re.search(r'<td[^>]*class="txtright"[^>]*>(.*?)</td>',row,re.S|re.I)
        if not name_match or not count_match:continue
        name=html.unescape(re.sub(r'\s+',' ',re.sub(r'<[^>]+>',' ',name_match.group(1)))).strip()
        digits=re.sub(r'[^0-9]','',count_match.group(1))
        if not digits:raise NBIQuarantineError('producer_release_state_count_not_integer:'+name)
        count=int(digits)
        if name.lower().startswith('total'):
            total=count;continue
        hrefs=re.findall(r'href="([^"]+)"',row,re.I)
        json_href=next((href for href in hrefs if re.search(r'/JSON/[^/]+\.json$',href,re.I)),None)
        expected=STATE_TOKENS.get(name.casefold())
        if expected is None:
            if json_href is None:
                if count!=0:raise NBIQuarantineError('producer_release_state_has_no_json_file:'+name)
                continue
            token_match=re.search(r'/([A-Z]{2})(\d{2})\.json$',json_href,re.I)
            if token_match is None:raise NBIQuarantineError('producer_release_json_name_not_state_year:'+json_href)
            token=token_match.group(1).upper()
            if int(token_match.group(2))!=year%100:raise NBIQuarantineError('producer_release_json_year_mismatch:'+json_href)
            url=urljoin(release_url,json_href)
        else:
            if json_href is None and count==0:continue
            token=expected
            directory=json_href.rsplit('/',1)[0]if json_href else f'{year}/JSON'
            page_match=re.search(r'/([A-Z]{2})(\d{2})\.json$',json_href or '',re.I)
            if page_match and(page_match.group(1).upper()!=token or int(page_match.group(2))!=year%100):
                corrections[name]={'page_token':page_match.group(1).upper(),'used_token':token}
            url=urljoin(release_url,f'{directory}/{token}{year%100:02d}.json')
        if token in states:raise NBIQuarantineError('producer_release_duplicate_state:'+token)
        states[token]={'name':name,'count':count,'url':url}
    if total is None:raise NBIQuarantineError('producer_release_has_no_total_row')
    if sum(state['count']for state in states.values())!=total:raise NBIQuarantineError('producer_release_state_counts_do_not_sum_to_total')
    if len(states)<MIN_STATES:raise NBIQuarantineError('producer_release_state_roster_too_small')
    return {'year':year,'url':release_url,'states':states,'total':total,'link_corrections':corrections}

def release_urls(release,schema_url):
    urls={'index':INDEX_URL,'release':release['url'],'schema_page':SCHEMA_PAGE_URL,'schema':schema_url}
    urls.update({'state_'+token:state['url']for token,state in release['states'].items()})
    return urls

def context_from_captures(captures):
    missing={'index','release','schema_page','schema'}-set(captures)
    if missing:raise NBIQuarantineError('capture_roster_drift')
    year,release_url=discover_release(captures['index'][0])
    release=parse_release_page(captures['release'][0],year,release_url)
    schema_url=discover_schema(captures['schema_page'][0])
    schema=json.loads(captures['schema'][0])
    return release_urls(release,schema_url),release,schema

def schema_parts(schema):
    try:
        properties=schema['items']['properties']
    except (KeyError,TypeError)as exc:
        raise NBIQuarantineError('snbi_schema_missing_top_level_properties')from exc
    scalar={};nested={}
    for tag,spec in properties.items():
        kind=spec.get('type')
        if kind=='array':nested[tag]=spec
        elif kind in('integer','number','string'):scalar[tag]=spec
        else:raise NBIQuarantineError('snbi_schema_unsupported_field_type:'+tag)
    return schema['items'],scalar,nested

def arrow_type(spec,tag):
    kind=spec.get('type');mapping={'integer':pa.int64(),'number':pa.float64(),'string':pa.string(),'array':pa.string()}
    if kind not in mapping:raise NBIQuarantineError('snbi_schema_unsupported_field_type:'+tag)
    return mapping[kind]

def build_schema(schema):
    _,scalar,nested=schema_parts(schema);fields=[]
    for tag,spec in schema['items']['properties'].items():
        metadata={'snbi_tag':tag,'snbi_description':spec.get('description',''),'snbi_type':spec.get('type','')}
        if tag in CODING_GUIDE_ITEMS:
            metadata['coding_guide_item'],metadata['coding_guide_name']=CODING_GUIDE_ITEMS[tag]
        if tag in nested:metadata['snbi_relationship']='nested_array_json'
        fields.append(pa.field(tag,arrow_type(spec,tag),nullable=True,metadata={key:encode(value)for key,value in metadata.items()}))
    for tag,spec in PRODUCER_EXTRA_FIELDS.items():
        metadata={'snbi_tag':tag,'snbi_description':spec.get('description',''),'snbi_type':spec.get('type',''),'snbi_relationship':'producer_extra_top_level'}
        fields.append(pa.field(tag,arrow_type(spec,tag),nullable=True,metadata={key:encode(value)for key,value in metadata.items()}))
    fields.append(pa.field('geometry',pa.binary(),nullable=True,metadata={b'original_crs':b'EPSG:4326',b'original_geometry_type':b'SNBI B.L.05/B.L.06 point'}))
    return pa.schema(fields)

def validate_value(tag,value,spec,token,required=False):
    if value is None:
        if required:raise NBIQuarantineError('snbi_required_field_missing:'+token+':'+tag)
        return
    kind=spec.get('type')
    if kind=='integer':
        if type(value)is int:pass
        elif type(value)is float and value.is_integer():pass
        else:raise NBIQuarantineError('snbi_field_type_drift:'+token+':'+tag)
    elif kind=='number':
        if type(value)not in(int,float):raise NBIQuarantineError('snbi_field_type_drift:'+token+':'+tag)
    elif kind=='string':
        if type(value)is not str:raise NBIQuarantineError('snbi_field_type_drift:'+token+':'+tag)
    else:raise NBIQuarantineError('snbi_schema_unsupported_field_type:'+tag)

def validate_nested(tag,value,spec,token):
    if value is None:return
    if type(value)is not list:raise NBIQuarantineError('snbi_nested_field_not_array:'+token+':'+tag)
    item=spec.get('items')or{}
    if isinstance(item,list):item=next((value for value in item if isinstance(value,dict)),{})
    if not isinstance(item,dict):return
    properties=item.get('properties')or{}
    for index,record in enumerate(value):
        if type(record)is not dict:raise NBIQuarantineError('snbi_nested_item_not_object:'+token+':'+tag)
        for key,item_spec in properties.items():
            if key in record:
                if item_spec.get('type')=='array':validate_nested(key,record[key],item_spec,token)
                else:validate_value(key,record[key],item_spec,token)

def geometry_for(record,token):
    latitude=record.get('BL05');longitude=record.get('BL06')
    if latitude is None and longitude is None:return None,'blank',False,False
    if latitude is None or longitude is None:return None,'partial',False,False
    if type(latitude)not in(int,float)or type(longitude)not in(int,float):raise NBIQuarantineError('snbi_coordinate_not_number:'+token)
    latitude=float(latitude);longitude=float(longitude)
    zero=latitude==0 and longitude==0
    out_of_envelope=not(-90<=latitude<=90 and -180<=longitude<=180)
    return struct.pack('<BIdd',1,1,longitude,latitude),'present',zero,out_of_envelope

def parse_state_file(body,token,schema):
    try:records=json.loads(body)
    except ValueError as exc:raise NBIQuarantineError('state_file_not_json:'+token)from exc
    if type(records)is not list:raise NBIQuarantineError('state_file_not_array:'+token)
    item,scalar,nested=schema_parts(schema);properties=item['properties'];required=set(item.get('required')or[])
    columns={tag:[]for tag in scalar};nested_columns={tag:[]for tag in nested};extra_columns={tag:[]for tag in PRODUCER_EXTRA_FIELDS}
    ids=[];geometry=[];zero=blank=partial=out_of_envelope=non_ascii=0
    for record in records:
        if type(record)is not dict:raise NBIQuarantineError('state_record_not_object:'+token)
        unknown=set(record)-set(properties)-set(PRODUCER_EXTRA_FIELDS)
        if unknown:raise NBIQuarantineError('state_record_unknown_field:'+token+':'+sorted(unknown)[0])
        missing=required-set(record)
        if missing:raise NBIQuarantineError('state_record_required_field_missing:'+token+':'+sorted(missing)[0])
        for tag,spec in scalar.items():
            value=record.get(tag);validate_value(tag,value,spec,token,required=tag in required)
            if spec.get('type')=='integer' and type(value)is float:value=int(value)
            columns[tag].append(value)
            if type(value)is str:non_ascii+=sum(1 for char in value if ord(char)>127)
        for tag,spec in nested.items():
            value=record.get(tag);validate_nested(tag,value,spec,token)
            nested_columns[tag].append(None if value is None else json.dumps(value,separators=(',',':'),ensure_ascii=False,allow_nan=False))
        for tag,spec in PRODUCER_EXTRA_FIELDS.items():
            value=record.get(tag);validate_value(tag,value,spec,token)
            extra_columns[tag].append(value)
        ids.append((record['BL01'],record['BID01']))
        point,category,was_zero,was_out=geometry_for(record,token)
        if point is None:
            if category=='blank':blank+=1
            else:partial+=1
        else:
            if was_zero:zero+=1
            if was_out:out_of_envelope+=1
        geometry.append(point)
    arrays=[pa.array(columns[tag],type=arrow_type(scalar[tag],tag))for tag in scalar]
    arrays.extend(pa.array(nested_columns[tag],type=pa.string())for tag in nested)
    arrays.extend(pa.array(extra_columns[tag],type=arrow_type(PRODUCER_EXTRA_FIELDS[tag],tag))for tag in PRODUCER_EXTRA_FIELDS)
    arrays.append(pa.array(geometry,type=pa.binary()))
    table=pa.Table.from_arrays(arrays,schema=build_schema(schema))
    stats={'rows':len(records),'zero':zero,'blank':blank,'partial':partial,'out_of_envelope':out_of_envelope,'non_ascii':non_ascii,
        'nested_counts':{tag:sum(0 if record.get(tag)is None else len(record[tag])for record in records)for tag in nested},
        'producer_extra_counts':{tag:sum(1 for record in records if tag in record)for tag in PRODUCER_EXTRA_FIELDS}}
    return table,ids,stats

def inspect_captures(captures):
    urls,release,schema=context_from_captures(captures)
    if set(captures)!=set(urls):raise NBIQuarantineError('capture_roster_drift')
    for name in sorted(captures):validate_receipt(name,*captures[name],urls)
    tables=[];roster=[];per_state={};duplicates=Counter();nested_totals={tag:0 for tag in schema_parts(schema)[2]}
    extra_totals={tag:0 for tag in PRODUCER_EXTRA_FIELDS}
    for token,state in release['states'].items():
        table,ids,stats=parse_state_file(captures['state_'+token][0],token,schema)
        tables.append(table);roster.extend(ids);duplicates.update(ids);per_state[token]=stats
        for tag,count in stats['nested_counts'].items():nested_totals[tag]+=count
        for tag,count in stats['producer_extra_counts'].items():extra_totals[tag]+=count
    if sum(state['count']for state in release['states'].values())!=release['total']:raise NBIQuarantineError('producer_release_total_drift')
    table=pa.concat_tables(tables)
    page_count_drift={token:per_state[token]['rows']-state['count']for token,state in release['states'].items()if per_state[token]['rows']!=state['count']}
    extra=sum(value-1 for value in duplicates.values()if value>1)
    duplicate_keys=sorted(key for key,value in duplicates.items()if value>1)
    roster_sha=digest(('\n'.join(sorted(f'{state}|{number}'for state,number in duplicates))+'\n').encode())
    zero=sum(stats['zero']for stats in per_state.values());blank=sum(stats['blank']for stats in per_state.values())
    partial=sum(stats['partial']for stats in per_state.values());out_of_envelope=sum(stats['out_of_envelope']for stats in per_state.values())
    non_ascii=sum(stats['non_ascii']for stats in per_state.values())
    release_dates={}
    for token in release['states']:
        modified=captures['state_'+token][1].get('server_last_modified')
        if modified:release_dates[token]=parsedate_to_datetime(modified).date().isoformat()
    audit={'count':table.num_rows,'field_count':len(schema['items']['properties']),'column_count':table.num_columns,'producer_extra_fields':sorted(PRODUCER_EXTRA_FIELDS),'state_rows':{token:stats['rows']for token,stats in per_state.items()},
        'release_year':release['year'],'release_url':release['url'],'release_page_total':release['total'],
        'release_page_state_counts':{token:state['count']for token,state in release['states'].items()},
        'release_page_count_drift':page_count_drift,'release_page_total_drift':table.num_rows-release['total'],
        'snbi_schema_digest':digest(captures['schema'][0]),'snbi_schema_title':schema.get('title'),
        'composite_ID_sha256':roster_sha,'composite_ID_duplicate_count':extra,'composite_ID_duplicate_keys':[list(key)for key in duplicate_keys],
        'nested_item_counts':nested_totals,'producer_extra_field_counts':extra_totals,
        'zero_coordinate_count':zero,'blank_coordinate_count':blank,'partial_coordinate_count':partial,
        'out_of_envelope_coordinate_count':out_of_envelope,'non_ascii_character_count':non_ascii,
        'state_file_last_modified':release_dates,'release_date':max(release_dates.values())if release_dates else None,
        'crs':'EPSG:4326 (WGS 84) decimal degrees from SNBI B.L.05/B.L.06 and the producer GeoJSON spatial reference',
        'coordinate_basis':'Points are derived only from SNBI B.L.05/B.L.06 decimal-degree values. Missing or partially missing coordinates yield null geometry; zero and out-of-envelope values are preserved with counts, not repaired.',
        'release_basis':'The FHWA NBI data index lists the newest nbi_data_YYYY.cfm release; that release page supplies the per-state JSON links. Its per-state counts and total are advisory cross-checks: the producer files are authoritative, and every page-count difference is recorded in release_page_state_counts and release_page_count_drift. 2025 was the final Coding Guide ASCII year and 2026 is the first SNBI-format release.',
        'schema_basis':'One row per bridge from the producer SNBI JSON. All top-level SNBI fields keep their data tag, native JSON type and value; nested SNBI arrays are retained as JSON text. The official SNBI Data Crosswalk maps clean 1995 Coding Guide items to SNBI tags; SNBI fields without a clean Coding Guide equivalent keep their SNBI tag rather than inventing a value. The 2026 files differ from the published schema inside nested arrays (for example Routes.BF01 is present but not declared, and Elements.BE02 is declared required but often absent); nested values are preserved verbatim as served and are not fabricated or dropped. The 2026-09-03 CT and MI re-uploads also serve an undeclared top-level DATA_YEAR stamp, kept as a producer-extra string column rather than dropped.',
        'coding_guide_basis':'CODING_GUIDE_ITEMS documents all 71 clean 1995 Coding Guide transitions from the official crosswalk, including tags that appear only inside nested SNBI arrays. Only top-level SNBI tags receive coding_guide_item and coding_guide_name column metadata; nested-tag entries document the relationship and are not emitted as columns.',
        'archive_relationship':'The archived HIFLD inventory (DataLumos 239684) is research context only and is not fetched. This draft reconstructs from the FHWA producer publication.',
        'temporary_objectID_warning':'none: the SNBI JSON has no producer row identifier; the composite (BL01, BID01) pair is a proposed join key with duplicates counted, not merged or approved as unique.',
        'approved':False}
    return table,audit

def source_fingerprint(captures):
    return digest(encode({name:digest(body)for name,(body,receipt)in captures.items()}))

def validate_receipt(name,body,receipt,urls):
    if name not in urls:raise NBIQuarantineError('unknown_original_input:'+name)
    url=urls[name]
    if(receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise NBIQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str)or datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise NBIQuarantineError('original_retrieval_timezone_required')
    server_last_modified=receipt.get('server_last_modified')
    if server_last_modified is not None and(type(server_last_modified)is not str or not server_last_modified):raise NBIQuarantineError('invalid_server_last_modified:'+name)
    if name.startswith('state_'):
        release={'release_date':parsedate_to_datetime(server_last_modified).date().isoformat()}if server_last_modified else {'release_date_unknown_reason':'No server Last-Modified available for this state file.'}
    else:
        release={'release_date_unknown_reason':'FHWA documentation-page edit time is not a data release date; the release is established per state JSON file by its server Last-Modified.'}
    return {'input_id':name+':'+digest(body),'producer':PRODUCER,'host':httpx.URL(url).host,
        'role':'producer_publication','distributor':'FHWA first-party publication on www.fhwa.dot.gov (original producer; no mirror or portal intermediary)',
        'delegation_evidence':{'producer_program_url':PROGRAM,'release_index_url':INDEX_URL,'release_page_url':urls['release'],'schema_page_url':SCHEMA_PAGE_URL,'data_crosswalk_url':CROSSWALK_URL},
        'canonical_url':url,'retrieval_url':url,'observed_final_url':url,'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'server_last_modified':server_last_modified,'license':LICENSE,
        'attribution':'FHWA National Bridge Inventory SNBI state JSON; 23 U.S.C. 409 applies; complete original item descriptions, attribution and rights retained.',
        **release}

def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise NBIQuarantineError('unsafe_capture_root')
    def fetch(name,url):
        if root is not None:
            body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
            immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
            immutable(storage,f'{attempt}/{name}.body-reference.json',encode({'bytes':len(body),'sha256':digest(body),'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'}))
            raw=bounded_input(root/(name+'.receipt.json'),65536)
        else:
            path=Path(storage.local_dir)/attempt/name;path.mkdir(parents=True)
            with(path/'wire.body').open('xb')as wire,(path/'decoded.body').open('xb')as decoded:
                def retain(stream,block,target):
                    before=os.fstat(stream.fileno());written=stream.write(block);stream.flush();os.fsync(stream.fileno())
                    if written!=len(block):raise OSError('short_capture_write')
                    reader=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                    try:
                        actual=os.fstat(reader)
                        if(not stat.S_ISREG(actual.st_mode)or actual.st_nlink!=1 or(actual.st_dev,actual.st_ino)!=(before.st_dev,before.st_ino)
                            or actual.st_size!=before.st_size+len(block)or os.pread(reader,len(block),before.st_size)!=block):raise OSError('capture_block_readback_mismatch')
                    finally:os.close(reader)
                def reserve(progress):
                    if shutil.disk_usage(path).free<SPACE_FLOOR+progress.reserve_bytes:raise OSError('disk_floor_before_read')
                receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat()};result=None
                try:
                    with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                        result=capture_http_response(response,max_wire_bytes=MAX_BODY_BYTES,max_decoded_bytes=MAX_BODY_BYTES,reserve=reserve,
                            retain_wire=lambda b:retain(wire,b,path/'wire.body'),retain_decoded=lambda b:retain(decoded,b,path/'decoded.body'))
                        receipt.update(final_url=str(response.url),status=response.status_code,complete_body=result.complete,bytes=result.decoded_bytes_retained,
                            sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code,
                            server_last_modified=response.headers.get('last-modified'))
                finally:
                    if result is not None:readback=preserve_transfer_result(path,result)
                    wire.flush();decoded.flush()
            receipt['complete_body']=result.complete and readback;receipt['independent_sink_readback_matches']=readback
            body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
            immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
        immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
        receipt=strict_json(raw);captures[name]=(body,receipt)
        return body
    try:
        index_body=fetch('index',INDEX_URL);year,release_url=discover_release(index_body)
        release_body=fetch('release',release_url);release=parse_release_page(release_body,year,release_url)
        schema_page_body=fetch('schema_page',SCHEMA_PAGE_URL);schema_url=discover_schema(schema_page_body)
        fetch('schema',schema_url);urls=release_urls(release,schema_url)
        for token,state in release['states'].items():fetch('state_'+token,state['url'])
        for name in sorted(captures):validate_receipt(name,*captures[name],urls)
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise NBIQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def manifest_for(captures,identity,output_pin,*,count=None):
    urls,release,schema=context_from_captures(captures)
    if count is None:count=sum(len(json.loads(body))for name,(body,_)in captures.items()if name.startswith('state_'))
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':f'National Bridge Inventory (NBI) — FHWA {release["year"]} SNBI reconstruction draft','publisher':PRODUCER,'agency':'Federal Highway Administration','source_url':release['url'],'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(name,*captures[name],urls)for name in sorted(captures)],
        'transformations':[{'operation':'complete_snbi_json_reconstruction','parameters':{
            'grain':f'All {count} bridge records of the official FHWA {release["year"]} SNBI release ({len(release["states"])} state and territory JSON files). No filtering, deduplication, reordering or repair.',
            'source_ids':'Every SNBI top-level field keeps its producer data tag, JSON type and value. Nested SNBI arrays (Features, Routes, Elements, Inspections, PostingEvaluations, PostingStatuses, SpanSets, SubstructureSets, Works) are retained as JSON text. The undeclared top-level DATA_YEAR stamp served in the 2026-09-03 CT and MI re-uploads is kept as a producer-extra string column. The composite (BL01, BID01) pair is proposed as the join key; duplicates are counted in the audit, not silently merged.',
            'types':'Native JSON integers, numbers and strings are stored without coercion or trimming. The SNBI schema digest and title are recorded. The official SNBI Data Crosswalk maps clean 1995 Coding Guide items to SNBI tags; SNBI fields with no clean Coding Guide equivalent keep their SNBI tag rather than inventing a value. Release-page counts are advisory and their differences from the producer files are recorded in the audit.',
            'geometry':'Points are derived only from SNBI B.L.05/B.L.06 decimal degrees as EPSG:4326. Missing or partially missing coordinates yield null geometry; zero and out-of-envelope source values are preserved and counted.',
            'relationship':'2025 was the final Coding Guide ASCII year; 2026 is the first SNBI-format release. The archived HIFLD copy (DataLumos 239684) is research context only and is not an input. Source, schema/ID contract, coordinate basis, rights and publication acceptance remain pending human decisions.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve every top-level SNBI field at its native type plus nested SNBI arrays as JSON text and the B.L.05/B.L.06 point geometry; no generic conversion or schema mapping approved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Nested SNBI arrays, native JSON types and the proposed composite ID need an approved contract; no implicit conversion.','shapefile_zip':'Nested arrays, field widths and null semantics cannot be silently coerced.','pmtiles':'Projection, quantization and feature ID mappings remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def stage_national_bridge_inventory(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity))
    _,release,_=context_from_captures(captures);version=f'v0.0.0-draft-fhwa-snbi-{release["year"]}-'+source_fingerprint(captures)[:16]+'-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise NBIQuarantineError('linked_lock')
        fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
        build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='candidate-',dir=build)as temporary:
            temp=Path(temporary);(temp/'geoparquet').mkdir();path=temp/'geoparquet'/f'{SLUG}.parquet';pq.write_table(parquet_table(table),path,compression='zstd');validate_output(temp,table)
            manifest=manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':sha256_file(path)},count=table.num_rows)
            if root.exists()or state_present(storage,version):
                try:
                    captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest)
                    table,audit=inspect_captures(captures)
                except Exception as exc:
                    try:record_observed_loss(storage,version,fingerprint,type(exc).__name__)
                    except Exception as write:raise IncidentWriteError('incident_failure_candidate_preserved')from write
                    if root.exists():
                        q=base/'_quarantine'/SLUG/('observed-loss-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q)
                    raise NBIQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:
                        validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise NBIQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'capture_fingerprint':source_fingerprint(captures),'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
