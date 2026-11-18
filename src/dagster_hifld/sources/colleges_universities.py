"""Complete latest published NCES EDGE postsecondary edition, unapproved successor.

Preserves every native field, physical DBF declaration, original file FID, UNITID
and WKB point. This does not approve replacing ORNL enrichment or published IDs.
"""
from __future__ import annotations

import collections
import csv
import io
import shutil
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.gdal import with_geopackage_creation_time
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.sources.capture_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    capture_get, processing_identity as packaged_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG = 'colleges-and-universities'
LAYER = 'NCES_EDGE_2025_2026'
LANDING = 'https://nces.ed.gov/programs/edge/Geographic/SchoolLocations'
URL = 'https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip'
DOCUMENTATION = 'https://nces.ed.gov/programs/edge/docs/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf'
LICENSE = 'Rights review pending.'
STEM = 'EDGE_GEOCODE_POSTSECSCH_2526'
URLS = {'nces_landing':LANDING,'nces_geocode':URL,'nces_documentation':DOCUMENTATION}
PINS = {'nces_geocode': {'bytes': 2651816, 'sha256': '4325bd9d8ab69155a95381f4a09eb62ad75e7960c288af6c192f224ca49b5114'}}
DBF_FIELDS = [('UNITID', 'C', 8, 0), ('NAME', 'C', 93, 0), ('STREET', 'C', 66, 0), ('CITY', 'C', 23, 0), ('STATE', 'C', 2, 0), ('ZIP', 'C', 10, 0), ('STFIP', 'C', 2, 0), ('CNTY', 'C', 5, 0), ('NMCNTY', 'C', 200, 0), ('LOCALE', 'C', 2, 0), ('LAT', 'N', 11, 6), ('LON', 'N', 12, 6), ('CBSA', 'C', 5, 0), ('NMCBSA', 'C', 100, 0), ('CBSATYPE', 'C', 1, 0), ('CSA', 'C', 3, 0), ('NMCSA', 'C', 100, 0), ('CD', 'C', 4, 0), ('SLDL', 'C', 5, 0), ('SLDU', 'C', 5, 0), ('SCHOOLYEAR', 'C', 9, 0)]
SOURCE_FIELDS = ('OGC_FID',*(f[0] for f in DBF_FIELDS),'wkb_geometry')
FIELDS = SOURCE_FIELDS
TYPES = (pa.int64(),*(pa.float64() if f[1]=='N' else pa.string() for f in DBF_FIELDS),pa.binary())
STATE_COUNTS = {'AL': 84, 'GA': 166, 'AK': 11, 'AZ': 131, 'DC': 25, 'CA': 695, 'NM': 47, 'AR': 86, 'TN': 148, 'CO': 87, 'WA': 101, 'VA': 162, 'NE': 41, 'MA': 144, 'MN': 112, 'MD': 78, 'CT': 57, 'NY': 423, 'DE': 19, 'FL': 383, 'HI': 22, 'ID': 37, 'IL': 244, 'IN': 148, 'MI': 176, 'KY': 89, 'IA': 75, 'MO': 148, 'KS': 82, 'LA': 117, 'ME': 38, 'MS': 59, 'TX': 423, 'OK': 104, 'MT': 29, 'NV': 41, 'NH': 31, 'NJ': 158, 'NC': 173, 'ND': 26, 'OH': 282, 'OR': 70, 'PA': 321, 'RI': 23, 'SC': 91, 'SD': 24, 'UT': 63, 'VT': 16, 'WV': 67, 'WI': 86, 'WY': 11, 'AS': 1, 'GU': 3, 'MP': 1, 'PR': 153, 'FM': 1, 'PW': 1, 'VI': 2, 'MH': 2}
UNITID_SHA256 = 'c2bffe8bcdb71fcc225cf63038d7b93b0b7962d53ad3d025c9cb41dc320f5e5c'
INNER_MEMBERS = {'EDGE_GEOCODE_POSTSECSCH_2526.shx': {'bytes': 51604, 'sha256': '2988305ec949148f913ce38e501143a970606ca6a6eca408233e446eab8c4be9'}, 'EDGE_GEOCODE_POSTSECSCH_2526.cpg': {'bytes': 5, 'sha256': '3ad3031f5503a4404af825262ee8232cc04d4ea6683d42c5dd0a2f2a27ac9824'}, 'EDGE_GEOCODE_POSTSECSCH_2526.dbf': {'bytes': 4294852, 'sha256': 'fbceb32b5a3e84718548de92d05111866beb474b9e7481b451c09fc835e14189'}, 'EDGE_GEOCODE_POSTSECSCH_2526.prj': {'bytes': 145, 'sha256': 'a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4'}, 'EDGE_GEOCODE_POSTSECSCH_2526.sbn': {'bytes': 60252, 'sha256': '48e667644974d3f11e06616347a5fa6ac2c1fda1b16dd9db324a6c88f289a79e'}, 'EDGE_GEOCODE_POSTSECSCH_2526.sbx': {'bytes': 892, 'sha256': 'ade3e6ee79e14336e17c79fc0548a8f17aa47074702a05ccab2353764b90b562'}, 'EDGE_GEOCODE_POSTSECSCH_2526.shp': {'bytes': 180364, 'sha256': 'ea4c8f1bacd04e88ff0ebdfd88cdd2f5fd6424bd6e745b5c1c47d6c67da1fc70'}, 'EDGE_GEOCODE_POSTSECSCH_2526.shp.xml': {'bytes': 25454, 'sha256': 'f6077c8fba2dc0ea82cdcfafb0fd05eff3b88058b18137bd45539c72d35a274b'}}
OUTER_MEMBERS = {STEM+'.sas7bdat',STEM+'.TXT',STEM+'.xlsx','Shapefile.zip'}
PARQUET_PIN = {'sha256': '8ec93dcb509224d4e566380d320a6ac5dd32cdb57387af71a146ae2658581d0d', 'bytes': 605896}
READY = 'metadata/colleges_candidate_ready.json'

class CollegeQuarantineError(ValueError):
    """Changed or incomplete source/cache requires review, never replacement."""


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


SPACE_FLOOR=300_000_000
MAX_ARCHIVE_BYTES=16_000_000
MAX_AUX_BYTES=2_000_000

class IncidentWriteError(Exception):pass

class _PublicationLinks(HTMLParser):
    def __init__(self):
        super().__init__();self.hrefs=[];self.stack=[];self.tokens=[];self.main_seen=False

    def handle_starttag(self, tag, attrs):
        attributes=dict(attrs)
        muted=(self.stack[-1][1] if self.stack else False) or tag in {'head','script','style','noscript','header','footer','nav','aside'}
        main=(self.stack[-1][2] if self.stack else False) or tag=='main' or 'MainContent' in attributes.get('class','').split()
        self.main_seen=self.main_seen or main
        if tag=='a' and not muted:
            for key,value in attrs:
                if key=='href' and value is not None:
                    self.hrefs.append(value);self.tokens.append((main,'href',urljoin(LANDING,value)))
        if tag not in {'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'}:
            self.stack.append((tag,muted,main))

    def handle_endtag(self,tag):
        for index in range(len(self.stack)-1,-1,-1):
            if self.stack[index][0]==tag:
                del self.stack[index:];break

    def handle_data(self,data):
        if self.stack and self.stack[-1][1]:return
        text=' '.join(data.split())
        if text:self.tokens.append((self.stack[-1][2] if self.stack else False,'text',text))

    def substantive_evidence(self):
        # Preserve producer notes, definitions, tables and links. NCES injects
        # randomized F5 JavaScript; executable/chrome bytes are retained raw,
        # but do not define a new data release. Fragments support offline tests.
        return [(kind,value) for main,kind,value in self.tokens if main or not self.main_seen]


def discover_release(body):
    """Select actual official postsecondary anchors; comments are not links."""
    links=_PublicationLinks();links.feed(body.decode('utf8'))
    candidates=[];documentation=[]
    for href in links.hrefs:
        url=urljoin(LANDING,href);parts=urlsplit(url)
        if parts.scheme!='https' or parts.netloc!='nces.ed.gov' or parts.query or parts.fragment:continue
        match=re.fullmatch(r'/programs/edge/data/(EDGE_GEOCODE_POSTSECSCH_(\d{2})(\d{2}))\.zip',parts.path)
        if match and int(match[3])==(int(match[2])+1)%100:
            candidates.append((int(match[2]),url,match[1]))
        if parts.path=='/programs/edge/docs/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf':documentation.append(url)
    if not candidates or len(documentation)!=1:raise CollegeQuarantineError('published_postsecondary_release_or_documentation_missing_or_ambiguous')
    newest=max(year for year,_,_ in candidates)
    selected=[(url,stem) for year,url,stem in candidates if year==newest]
    if len(selected)!=1:raise CollegeQuarantineError('published_postsecondary_release_ambiguous')
    source,stem=selected[0]
    return {'schoolyear':f'{2000+newest}-{2001+newest}','source_url':source,'stem':stem,
            'documentation_url':documentation[0],
            'landing_evidence_sha256':digest(encode(links.substantive_evidence())),
            'selection_rule':'Largest adjacent school-year POSTSECSCH ZIP named by a real official SchoolLocations anchor; preserve substantive main-page text and links, excluding scripts/styles/chrome; no guessed URLs or older fallback.'}


def release_urls(release):
    if release is None:return URLS
    original={'nces_landing':LANDING,'nces_geocode':release['source_url'],'nces_documentation':release['documentation_url']}
    return {**original,**{name+'_after':url for name,url in original.items()}}


def acquire_latest(storage,client=None):
    """Bound and retain before/after publisher evidence, refusing source mutation."""
    local_only(storage);owned=client is None
    client=client or httpx.Client(timeout=httpx.Timeout(90,connect=20),follow_redirects=False)
    captures={}
    with tempfile.TemporaryDirectory(prefix='nces-current-capture-') as directory:
        root=Path(directory)
        def fetch(name,url):
            body,receipt=capture_get(client,url,root,name,max_bytes=MAX_ARCHIVE_BYTES if 'geocode' in name else MAX_AUX_BYTES)
            captures[name]=(body,receipt)
            if receipt['content_encoding']!='identity':raise CollegeQuarantineError('current_source_requires_identity_transfer')
            return body
        try:
            release=discover_release(fetch('nces_landing',LANDING))
            fetch('nces_geocode',release['source_url']);documentation=fetch('nces_documentation',release['documentation_url'])
            if not documentation.startswith(b'%PDF-'):raise CollegeQuarantineError('source_technical_documentation_is_not_pdf')
            after=discover_release(fetch('nces_landing_after',LANDING))
            if after!=release:raise CollegeQuarantineError('published_selection_changed_during_capture')
            for name in ('nces_geocode','nces_documentation'):
                if fetch(name+'_after',release_urls(release)[name])!=captures[name][0]:
                    raise CollegeQuarantineError('source_changed_during_capture:'+name)
            for name,(body,receipt) in captures.items():validate_receipt(name,body,receipt,release=release)
            return captures,release
        except (ValueError,OSError,TypeError,UnicodeError) as exc:
            for path in root.glob('*.receipt.json'):
                name=path.name.removesuffix('.receipt.json');body_path=root/(name+'.body')
                if body_path.exists() and name not in captures:
                    captures[name]=(body_path.read_bytes(),strict_json(path.read_bytes()))
            where=quarantine(storage,captures,str(exc))
            for wire in root.glob('*.wire'):immutable(storage,where+'/'+wire.name,wire.read_bytes())
            raise CollegeQuarantineError(f'{exc}; retained:{where}') from exc
        finally:
            if owned:client.close()

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
    return packaged_processing_identity(root)


def validate_receipt(name,body,receipt,*,release=None):
    url=release_urls(release)[name]
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int
        or receipt['status']!=200 or type(receipt.get('bytes')) is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body) or receipt.get('complete_body') is not True):
        raise CollegeQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'National Center for Education Statistics, U.S. Department of Education','host':'nces.ed.gov',
        'role':'producer_publication','canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
        'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],'sha256':digest(body),'bytes':len(body),
        'license':LICENSE,'attribution':'NCES Education Demographic and Geographic Estimates (EDGE), Postsecondary School Geocodes '+(release['schoolyear'] if release else '2025–2026')+'.',
        'release_date_unknown_reason':'The school-year edition and file metadata dates are not an exact producer publication date.'}


def manifest_for(captures,identity,*,release=None,table=None):
    if set(captures)!=set(release_urls(release)): raise CollegeQuarantineError('missing_extra_source_publication_roster')
    inputs=[validate_receipt(name,*captures[name],release=release) for name in release_urls(release)]
    if release:
        if any(discover_release(captures[name][0])!=release for name in ('nces_landing','nces_landing_after')):
            raise CollegeQuarantineError('official_source_landing_selection_changed')
        if any(captures[name][0]!=captures[name+'_after'][0] for name in ('nces_geocode','nces_documentation')):
            raise CollegeQuarantineError('source_changed_during_capture')
        if table is None:table,_=inspect_archives({},captures,release=release)
        payload=parquet_payload(table);parquet_pin={'bytes':len(payload),'sha256':digest(payload)}
    else:parquet_pin=PARQUET_PIN
    if not release and URL.encode() not in captures['nces_landing'][0] and b'/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip' not in captures['nces_landing'][0]:
        raise CollegeQuarantineError('official_source_landing_download_delegation_missing')
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],'code_identity_scheme':identity['code_identity_scheme'],
        'processing_code':{'scheme':identity['code_identity_scheme'],'identity':identity['code_commit']},
        'title':'Colleges and Universities — complete NCES EDGE '+(release['schoolyear'] if release else '2025–2026')+' unapproved successor draft',
        'publisher':'National Center for Education Statistics','agency':'U.S. Department of Education','source_url':LANDING,
        'documentation_url':release['documentation_url'] if release else DOCUMENTATION,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'decode_complete_original_nested_shapefile','parameters':{'fields':'All21 original names/physical DBF declarations preserved; no ORNL enrichment inferred','source_fid':'Original file-local OGC_FID retained separately; not a generated IPEDSID','UNITID':'Original DBF C8 string preserved; no approved IPEDSID mapping','crs':'EPSG:4326','geometry':'native WKB Point, no reproject/repair/quantization'}},
            {'operation':'native_geopackage_and_geoparquet','parameters':{'source_types':'DBF C remains string, LAT/LON N11/12,6 exposed float64; raw lexical values retained and independently compared','gpkg_fid':'separate storage index, not source/published identifier'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve complete native source field roster/Arrow types and original OGC_FID; no generic bbox expansion approved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**parquet_pin}]}}},
        'unsupported_output_formats':{'shapefile_zip':'Original nested shapefile retained losslessly. Additional OGC_FID column is not an approved published schema/field mapping.',
            'pmtiles':'Quantized visualization and published ID mapping remain unapproved.'}}
    if release:
        indexed={name:item for name,item in zip(release_urls(release),inputs)}
        for name,item in indexed.items():item['purpose']='data' if 'geocode' in name else 'methodology'
        indexed['nces_geocode']['refresh']={
            'maintenance_status':'maintained','maintenance_evidence_input_id':indexed['nces_landing']['input_id'],
            'source_release_id':release['schoolyear'],'data_period':release['schoolyear'],
            'update_cadence':'unknown','unknown_reasons':{'update_cadence':'Annual IPEDS survey and annual edition links do not alone establish a geocode publication schedule.'},
            'latest_checked_at':captures['nces_landing'][1]['observed_at'],
            'latest_evidence_input_id':indexed['nces_landing']['input_id'],'selection_rule':release['selection_rule'],
            'source_state_fingerprint':digest(encode({'archive':digest(captures['nces_geocode'][0]),
                'documentation':digest(captures['nces_documentation'][0]),'publication':release['landing_evidence_sha256']}))}
        manifest['coverage']={'schoolyear':release['schoolyear'],'feature_count':table.num_rows,
            'population':'Complete selected NCES EDGE IPEDS reporting-institution file; not every physical campus or present-day operating institution.',
            'jurisdiction_counts':dict(collections.Counter(table['STATE'].to_pylist())),
            'coordinate_observation_date':'Unknown; school year is the reporting edition, not a coordinate observation timestamp.'}
        manifest['source_relationship']={'relationship':'unverified','evidence_input_ids':[],
            'unknown_reason':'Exact ORNL enriched historical product joins, IDs and population have not been accepted as equivalent to these NCES reporting-institution points.'}
        docs=[('raw/captures/nces_landing.body',captures['nces_landing'][0],'nces_landing','methodology','text/html'),
              ('raw/captures/nces_documentation.body',captures['nces_documentation'][0],'nces_documentation','methodology','application/pdf'),
              ('raw/source_metadata.xml',source_xml(captures,release),'nces_geocode','schema','application/xml')]
        manifest['documentation']=[{'path':path,'input_id':indexed[name]['input_id'],'purpose':purpose,'media_type':mime,
                                    'bytes':len(body),'sha256':digest(body)} for path,body,name,purpose,mime in docs]
        manifest['field_definitions']={
            name:{'source_fields':[name],'input_ids':[indexed['nces_documentation']['input_id']],'basis':'publisher','description':description}
            for name,description in {
                'UNITID':'Native IPEDS reporting-institution identifier, retained as a DBF C8 string including leading zeroes; not an approved historical IPEDSID or OPEID mapping.',
                'SCHOOLYEAR':'NCES school-year reporting edition; not the publication date, coordinate observation date or an operating-status guarantee.',
                'LAT':'Native geographic latitude attribute in degrees, DBF N11,6; kept separately from full native point precision.',
                'LON':'Native geographic longitude attribute in degrees, DBF N12,6; kept separately from full native point precision.'}.items()}
        manifest.update(source_release_accepted=False,source_freshness_accepted=False,schema_contract_accepted=False,
                        id_contract_accepted=False,coverage_accepted=False,rights_accepted=False,mapping_approved=False,release_accepted=False)
    validate_release_provenance(manifest)
    return manifest


def source_xml(captures,release):
    with zipfile.ZipFile(io.BytesIO(captures['nces_geocode'][0])) as outer:
        with zipfile.ZipFile(io.BytesIO(outer.read('Shapefile.zip'))) as inner:
            return inner.read(release['stem']+'.shp.xml')


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb')as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise CollegeQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise CollegeQuarantineError('changed_or_incomplete_original_input')
    return body


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempted={};root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or not root.is_dir()or any(p.is_symlink()for p in [root,*root.parents])):raise CollegeQuarantineError('unsafe_capture_root')
    def retain(name,body):
        key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);attempted[name]={'body_key':key,'body_sha256':digest(body),'body_bytes':len(body),'raw_receipt_read':False}
    try:
        for name,url in URLS.items():
            limit=MAX_ARCHIVE_BYTES if name=='nces_geocode'else MAX_AUX_BYTES
            if root is not None:
                body=bounded_input(root/(name+'.body'),limit);retain(name,body)
                raw_receipt=bounded_input(root/(name+'.receipt.json'),65536)
            else:
                for attempt in range(3):
                    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1,'complete_body':False}
                    try:
                        with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                            receipt.update(status=response.status_code,final_url=str(response.url),requested_accept_encoding='identity',content_encoding=response.headers.get('content-encoding'),content_length=response.headers.get('content-length'))
                            try:
                                iterator=response.iter_bytes(65536)
                                while True:
                                    if shutil.disk_usage(storage.local_dir).free<SPACE_FLOOR:raise CollegeQuarantineError('disk_floor_before_next_read')
                                    try:block=next(iterator)
                                    except StopIteration:break
                                    body+=block
                                    if len(body)>limit:raise CollegeQuarantineError('oversized_or_changed_source')
                            finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
                            if response.status_code!=200 or(receipt['content_length']is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):raise CollegeQuarantineError('non200_or_incomplete_http_response')
                        body=bytes(body);receipt.update(bytes=len(body),sha256=digest(body),complete_body=True);retain(name,body);raw_receipt=encode(receipt);break
                    except(httpx.HTTPError,OSError,ValueError)as exc:
                        body=bytes(body);receipt.update(bytes=len(body),sha256=digest(body),complete_body=False);retain(name,body);quarantine(storage,{name:(body,receipt)},str(exc))
                        if attempt==2:raise CollegeQuarantineError('bounded_fetch_exhausted:'+str(exc))from exc
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.raw-receipt-{digest(raw_receipt)}.body';immutable(storage,key,raw_receipt);attempted[name].update(raw_receipt_read=True,raw_receipt_key=key,raw_receipt_sha256=digest(raw_receipt))
            receipt=strict_json(raw_receipt);captures[name]=(body,receipt);validate_receipt(name,body,receipt)
        if root is not None and{p.name for p in root.iterdir()}!={name+suffix for name in URLS for suffix in('.body','.receipt.json')}:raise CollegeQuarantineError('extra_or_missing_capture_file')
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}));return captures
    except(OSError,ValueError,TypeError)as exc:
        key=f'_quarantine/{SLUG}/attempt-{uuid4().hex}';immutable(storage,key+'/attempted-inputs.json',encode({'approved':False,'objects':attempted}));where=quarantine(storage,captures,str(exc));raise CollegeQuarantineError(f'{exc}; retained:{where}; attempt:{key}')from exc


def dbf_definition(body):
    if len(body)<33: raise CollegeQuarantineError('short_dbf_header')
    rows=int.from_bytes(body[4:8],'little');header=int.from_bytes(body[8:10],'little');record=int.from_bytes(body[10:12],'little')
    if header<33 or header>len(body) or (header-33)%32 or body[header-1]!=13 or len(body) not in (header+rows*record,header+rows*record+1):
        raise CollegeQuarantineError('incomplete_or_changed_dbf_records')
    fields=[]
    for index in range(32,header-1,32):
        field=body[index:index+32]
        fields.append((field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
    if fields!=DBF_FIELDS or record!=1+sum(field[2] for field in fields) or any(body[header+i*record]!=32 for i in range(rows)):
        raise CollegeQuarantineError('native_dbf_physical_declarations_or_deleted_rows_drift')
    return {'fields':fields,'row_count':rows,'record_bytes':record,'source_oid_declaration':'No DBF OID. OGC_FID is the file-local driver index, preserved separately.'}



def decode_dbf_rows(body):
    """Independent physical DBF decode checks every source attribute/precision."""
    definition=dbf_definition(body)
    header=int.from_bytes(body[8:10],'little');width=definition['record_bytes'];rows=[]
    for index in range(definition['row_count']):
        cursor=header+index*width+1;row={}
        for name,kind,length,decimals in DBF_FIELDS:
            raw=body[cursor:cursor+length];cursor+=length
            if kind=='C': row[name]=raw.rstrip(b' \0').decode('utf8')
            else:
                value=raw.strip().decode('ascii')
                if not re.fullmatch(r'-?\d+\.\d{6}',value): raise CollegeQuarantineError('native_dbf_numeric_precision_or_value_drift')
                row[name]=float(value)
        rows.append(row)
    return rows


def inspect_archives(paths,captures,*,release=None):
    body=captures['nces_geocode'][0]
    stem=release['stem'] if release else STEM
    if release is None and (len(body)!=PINS['nces_geocode']['bytes'] or digest(body)!=PINS['nces_geocode']['sha256']):
        raise CollegeQuarantineError('changed_truncated_or_unknown_nces_release')
    with zipfile.ZipFile(io.BytesIO(body)) as outer:
        check_zip(outer,{name.replace(STEM,stem) for name in OUTER_MEMBERS},64_000_000 if release else 15_000_000)
        nested=outer.read('Shapefile.zip')
        text=outer.read(stem+'.TXT').decode('utf8')
        outer_inventory={i.filename:{'bytes':i.file_size,'sha256':digest(outer.read(i.filename))} for i in outer.infolist()}
    with zipfile.ZipFile(io.BytesIO(nested)) as archive:
        check_zip(archive,{name.replace(STEM,stem) for name in INNER_MEMBERS},32_000_000 if release else 8_000_000)
        for name,pin in INNER_MEMBERS.items():
            value=archive.read(name.replace(STEM,stem))
            if release is None and (len(value)!=pin['bytes'] or digest(value)!=pin['sha256']): raise CollegeQuarantineError('original_nested_member_drift:'+name)
        nested_inventory={i.filename:{'bytes':i.file_size,'sha256':digest(archive.read(i.filename))} for i in archive.infolist()}
        dbf=dbf_definition(archive.read(stem+'.dbf'))
        physical_rows=decode_dbf_rows(archive.read(stem+'.dbf'))
        if archive.read(stem+'.cpg')!=b'UTF-8':raise CollegeQuarantineError('source_encoding_drift')
        if release:
            # Independent physical sidecar counts precede and constrain GDAL.
            shp,shx=archive.read(stem+'.shp'),archive.read(stem+'.shx')
            for component in (shp,shx):
                if (len(component)<100 or int.from_bytes(component[:4],'big')!=9994
                        or int.from_bytes(component[24:28],'big')*2!=len(component)
                        or int.from_bytes(component[28:32],'little')!=1000
                        or int.from_bytes(component[32:36],'little')!=1):
                    raise CollegeQuarantineError('source_point_header_or_declared_length_drift')
            shx_count=(len(shx)-100)//8
            if (len(shx)-100)%8 or shx_count!=dbf['row_count'] or len(shp)!=100+28*shx_count:
                raise CollegeQuarantineError('source_dbf_shx_point_count_disagreement')
            for index in range(shx_count):
                entry=shx[100+index*8:108+index*8];offset=100+index*28
                if (int.from_bytes(entry[:4],'big')*2!=offset or int.from_bytes(entry[4:],'big')!=10
                        or int.from_bytes(shp[offset:offset+4],'big')!=index+1
                        or int.from_bytes(shp[offset+4:offset+8],'big')!=10
                        or int.from_bytes(shp[offset+8:offset+12],'little')!=1):
                    raise CollegeQuarantineError('source_point_index_or_record_disagreement')
            ET.fromstring(archive.read(stem+'.shp.xml'))
    with tempfile.TemporaryDirectory(prefix='nces-original-') as tmp:
        path=Path(tmp)/'original-shapefile.zip';path.write_bytes(nested)
        meta,table=pyogrio.read_arrow('/vsizip/{'+str(path)+'}/'+stem+'.shp',return_fids=True)
    if (tuple(table.column_names)!=SOURCE_FIELDS or tuple(f.type for f in table.schema)!=TYPES or meta['crs']!='EPSG:4326'
        or meta['fid_column']!='OGC_FID' or meta['geometry_type']!='Point' or table.num_rows!=dbf['row_count']):
        raise CollegeQuarantineError('source_schema_types_crs_geometry_or_dbf_completeness_drift')
    if [{name:r[name] for name,_,_,_ in DBF_FIELDS} for r in table.to_pylist()]!=physical_rows:
        raise CollegeQuarantineError('driver_disagrees_with_independent_physical_dbf_decode')
    text_rows=list(csv.reader(io.StringIO(text),delimiter='|'))
    converted=[]
    for row in text_rows:
        if len(row)!=len(DBF_FIELDS):raise CollegeQuarantineError('source_text_column_count_drift')
        converted.append({name:float(value) if kind=='N' else value for (name,kind,_,_),value in zip(DBF_FIELDS,row)})
    if converted!=physical_rows: raise CollegeQuarantineError('complete_official_text_disagrees_with_shapefile')
    validate_table(table,release=release,expected_count=dbf['row_count'])
    audit={'original_dbf':dbf,'original_outer_members':outer_inventory,'original_nested_members':nested_inventory,'arrow_schema':str(table.schema),
        'all_original_attributes_ids_and_native_wkb_retained':True,'native_wkb_vs_six_decimal_coordinates':'Both preserved without snapping; maximum allowed half DBF decimal unit','independent_dbf_and_official_text_rows_compared':len(physical_rows),
        'state_counts':dict(collections.Counter(table['STATE'].to_pylist())),'source_relationship':'ChangedNCES2025–2026 successor; NOT exactORNL2020–2021 enriched HIFLD continuation.'}
    if release:
        audit['source_counts']={'dbf':dbf['row_count'],'shx':shx_count,'decoded':table.num_rows,'text':len(converted)}
        audit['schoolyear']=release['schoolyear']
        audit['publication_selection']=release
        audit['source_relationship']='NCES reporting-institution points; historical ORNL enriched product equivalence is unverified.'
    return table,audit


def check_zip(archive,expected,limit):
    infos=archive.infolist();names=[i.filename for i in infos]
    if len(names)!=len(expected) or set(names)!=expected:
        raise CollegeQuarantineError('missing_duplicate_extra_or_corrupt_archive_member')
    if sum(i.file_size for i in infos)>limit or any(PurePosixPath(i.filename).name!=i.filename or i.flag_bits&1 or stat.S_ISLNK(i.external_attr>>16) for i in infos):
        raise CollegeQuarantineError('unsafe_or_oversized_archive')
    if archive.testzip() is not None:raise CollegeQuarantineError('missing_duplicate_extra_or_corrupt_archive_member')


def validate_table(table,*,release=None,expected_count=None):
    count=expected_count if release else 6438
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=count or not count or any(table[n].null_count for n in FIELDS):
        raise CollegeQuarantineError('native_schema_physical_types_nullability_or_completeness_drift')
    ids=table['UNITID'].to_pylist()
    if len(set(ids))!=count or (release is None and hashlib.sha256(json.dumps(ids,separators=(',',':')).encode()).hexdigest()!=UNITID_SHA256):
        raise CollegeQuarantineError('missing_duplicate_changed_source_ids')
    if any(not isinstance(value,str) or not value.isascii() or not value.isdecimal() or len(value)>8 for value in ids):
        raise CollegeQuarantineError('nonlexical_native_unitid')
    if (table['OGC_FID'].to_pylist()!=list(range(count))
            or (release is None and dict(collections.Counter(table['STATE'].to_pylist()))!=STATE_COUNTS)
            or set(table['SCHOOLYEAR'].to_pylist())!={release['schoolyear'] if release else '2025-2026'}):
        raise CollegeQuarantineError('native_fid_year_or_national_territory_coverage_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if not all(shapely.is_valid(geoms)) or any(shapely.is_empty(geoms)) or set(shapely.get_type_id(geoms).tolist())!={0}:
        raise CollegeQuarantineError('missing_invalid_or_changed_native_point_geometry')
    if release is None and hashlib.sha256(b''.join(table['wkb_geometry'].to_pylist())).hexdigest()!='14193ccbebe31729370f18b5e912c6399dac38fd342fcfbb75483d8060a94893':
        raise CollegeQuarantineError('native_geometry_values_drift')
    # Native double WKB and six-decimal DBF coordinate text have distinct
    # producer precision. Preserve both; never snap geometry to attributes.
    if any(abs(x-y)>0.0000005 for x,y in zip(shapely.get_x(geoms).tolist(),table['LON'].to_pylist())) or any(abs(x-y)>0.0000005 for x,y in zip(shapely.get_y(geoms).tolist(),table['LAT'].to_pylist())):
        raise CollegeQuarantineError('native_geometry_outside_declared_coordinate_precision')


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(4326).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def parquet_payload(table):
    sink=pa.BufferOutputStream();pq.write_table(expected_parquet_table(table),sink,compression='zstd')
    return sink.getvalue().to_pybytes()


def write_native_gpkg(table,path,*,release=None):
    layer='NCES_EDGE_'+release['schoolyear'].replace('-','_') if release else LAYER
    with with_geopackage_creation_time('2026-01-01T00:00:00.000Z'):
        pyogrio.write_arrow(table,path,layer=layer,driver='GPKG',geometry_name='wkb_geometry',geometry_type='Point',crs='EPSG:4326',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path) as db: db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table,*,release=None):
    expected=expected_parquet_table(table)
    parquet=pq.read_table(root/'geoparquet/colleges-and-universities.parquet')
    pin=PARQUET_PIN
    if release:
        payload=parquet_payload(table);pin={'bytes':len(payload),'sha256':digest(payload)}
    if sha256_file(root/'geoparquet/colleges-and-universities.parquet')!=pin['sha256'] or (root/'geoparquet/colleges-and-universities.parquet').stat().st_size!=pin['bytes'] or not parquet.equals(expected,check_metadata=True): raise CollegeQuarantineError('cached_parquet_values_types_or_metadata_disagree_with_fresh_original')
    layer='NCES_EDGE_'+release['schoolyear'].replace('-','_') if release else LAYER
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=layer)
    if gm['crs']!='EPSG:4326' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():
        raise CollegeQuarantineError('cached_geopackage_values_types_or_wkb_disagree_with_fresh_original')
    with tempfile.TemporaryDirectory(prefix='colleges-canonical-gpkg-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh,release=release)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise CollegeQuarantineError('cached_geopackage_full_container_disagrees_with_fresh_original')


def source_payloads(captures,identity,table,audit,manifest,*,release=None):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/colleges_source_audit.json':encode({'approved':False,'feature_count':table.num_rows,'crs':'EPSG:4326','invalid_geometries':0,'points':table.num_rows,
            'archive_count':1,'original_publications':audit,'full_values_ids_physical_types_native_wkb_roundtrip':True,
            'source_relationship':'NCES '+(release['schoolyear'] if release else '2025–2026')+' reporting-institution successor; exact ORNL enriched equivalence and all mapping/release gates remain unapproved.'}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_schema_id_geometry_mapping_not_approved']})}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    if release:values['raw/source_metadata.xml']=source_xml(captures,release)
    return values


def candidate_bytes(root,name):
    path=root/name
    if '..'in Path(name).parts or Path(name).is_absolute()or any(p.is_symlink()for p in[path.parent,*path.parent.parents]):
        raise CollegeQuarantineError('candidate_read_path_alias_refused')
    limit=MAX_ARCHIVE_BYTES if name.startswith('raw/captures/nces_geocode') and name.endswith('.body') else(32_000_000 if name.endswith(('.gpkg','.parquet'))else MAX_AUX_BYTES)
    return bounded_input(path,limit)


def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise CollegeQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names


def reconcile_cache(storage,prefix,table,audit,captures,identity,original_context=None,*,release=None):
    root=Path(storage.local_dir)/prefix;actual=candidate_roster(root);ready=strict_json(candidate_bytes(root,READY))
    cached={name:(candidate_bytes(root,f'raw/captures/{name}.body'),strict_json(candidate_bytes(root,f'raw/captures/{name}.receipt.json'))) for name in release_urls(release)}
    if any(cached[name][0]!=captures[name][0] for name in (release_urls(release) if release else PINS)): raise CollegeQuarantineError('cached_original_publication_changed')
    manifest=manifest_for(cached,identity,release=release,table=table)
    actual_manifest=strict_json(candidate_bytes(root,'metadata/source_manifest.json'))
    validate_release_provenance(actual_manifest)
    for key,value in manifest.items():
        if key!='code_commit' and encode(actual_manifest.get(key))!=encode(value): raise CollegeQuarantineError('cached_provenance_disagrees_with_original')
    fingerprint=candidate_fingerprint(identity,captures,release)
    retained,original_manifest=original_context if original_context is not None else original_provenance_bundle(storage,fingerprint,captures,identity,manifest,release=release)
    if encode(actual_manifest)!=encode(original_manifest):raise CollegeQuarantineError('cached_provenance_disagrees_with_immutable_original_bundle')
    manifest=actual_manifest
    expected=source_payloads(cached,identity,table,audit,manifest,release=release)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg','geoparquet/colleges-and-universities.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1] or ready.get('approved') is not False or ready.get('processing_fingerprint')!=fingerprint or set(ready['objects'])!=expected_names):
        raise CollegeQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if candidate_bytes(root,name)!=value: raise CollegeQuarantineError('cached_source_owned_bytes_drift:'+name)
    snapshot='metadata/source/version_'+digest(candidate_bytes(root,'metadata/source_manifest.json'))+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    if actual-(expected_names|{READY}|derivatives) or not (expected_names|{READY}).issubset(actual):
        raise CollegeQuarantineError('cached_unknown_or_missing_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise CollegeQuarantineError('invalid_staged_release_derivatives') from exc
    for name,pin in ready['objects'].items():
        if digest(candidate_bytes(root,name))!=pin: raise CollegeQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists() and candidate_bytes(root,name)!=candidate_bytes(root,'metadata/source_manifest.json'): raise CollegeQuarantineError('cached_source_snapshot_drift')
    for name in actual-expected_names-{READY}:candidate_bytes(root,name)
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs'] or encode(strict_json(candidate_bytes(root,'metadata/resolved_provenance.json')))!=encode(resolved):
            raise CollegeQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table,release=release)



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


def candidate_fingerprint(identity,captures,release=None):
    return digest(encode({'processing':identity,'release':release,'evidence':{name:digest(body) for name,(body,_) in captures.items()
                        if name not in {'nces_landing','nces_landing_after'}}})) if release else digest(encode(identity))


def candidate_version(fingerprint,release=None):
    code=release['stem'].rsplit('_',1)[1] if release else '2526'
    return f'v0.0.0-draft-nces{code}-{fingerprint[:16]}'


def original_provenance_bundle(storage,fingerprint,captures,identity,manifest,*,create=False,release=None):
    version=candidate_version(fingerprint,release)
    legacy=f'_source-provenance/{SLUG}/{fingerprint}/bundle.json'
    if storage.object_exists(legacy):raise CollegeQuarantineError('legacy_unbound_provenance_requires_explicit_migration')
    payload={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt}for name,(body,receipt)in captures.items()}}
    envelope=canonical_construction_bundle(version,fingerprint,payload);key=construction_record_key(version)
    if create:
        table,_=inspect_archives({},captures,release=release);expected=manifest_for(captures,identity,release=release,table=table)
        if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):raise CollegeQuarantineError('preseal_full_original_manifest_drift')
        validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        body=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:body=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(body)['payload']
    if set(bundle)!={'manifest','captures'}or set(bundle['captures'])!=set(release_urls(release)):raise CollegeQuarantineError('original_provenance_bundle_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'}or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise CollegeQuarantineError('original_provenance_body_key_drift')
        original[name]=(storage.read_key(item['body_key']),item['receipt'])
    first,first_audit=inspect_archives({},original,release=release);current,current_audit=inspect_archives({},captures,release=release)
    if not first.equals(current,check_metadata=True)or encode(first_audit)!=encode(current_audit)or original['nces_documentation'][0]!=captures['nces_documentation'][0]:raise CollegeQuarantineError('original_source_or_documentation_changed')
    if release and candidate_fingerprint(identity,original,release)!=fingerprint:raise CollegeQuarantineError('original_source_evidence_fingerprint_changed')
    expected=manifest_for(original,identity,release=release,table=first);actual=bundle['manifest']
    if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in actual.items()if k!='code_commit'}):raise CollegeQuarantineError('original_manifest_or_retrieval_context_drift')
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity);return original,actual


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


def reconcile_private(root,table,audit,captures,identity,manifest,version,*,release=None):
    # Re-read the complete closed candidate, not only native data, before the
    # first outside seal and again before exposing a discoverable version.
    expected=source_payloads(captures,identity,table,audit,manifest,release=release)
    names={*expected,f'geopackage/{SLUG}.gpkg',f'geoparquet/{SLUG}.parquet'}
    actual=candidate_roster(root)
    if actual!=names|{READY}:raise CollegeQuarantineError('private_candidate_closed_roster_drift')
    for name,body in expected.items():
        if candidate_bytes(root,name)!=body:raise CollegeQuarantineError('private_candidate_source_bytes_drift:'+name)
    actual_manifest=strict_json(candidate_bytes(root,'metadata/source_manifest.json'))
    validate_release_provenance(actual_manifest);validate_output_policy(actual_manifest)
    ready={'version':version,'approved':False,'processing_fingerprint':candidate_fingerprint(identity,captures,release),
        'objects':{name:sha256_file(root/name)for name in names}}
    if candidate_bytes(root,READY)!=encode(ready):raise CollegeQuarantineError('private_candidate_ready_drift')
    reconcile_outputs(root,table,release=release)


def stage_colleges(storage,*,capture_dir=None,client=None):
    """Refresh the discovered producer edition, or explicitly replay pinned historical evidence."""
    local_only(storage)
    if capture_dir is None:captures,release=acquire_latest(storage,client)
    else:captures,release=acquire(storage,capture_dir),None
    checked_at=captures['nces_landing'][1]['observed_at']
    try:
        identity=processing_identity();fingerprint=candidate_fingerprint(identity,captures,release);version=candidate_version(fingerprint,release)
        prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir).absolute();root=base/prefix
        depot={}
        for name,(body,receipt)in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt));depot[name]=base/key
        table,audit=inspect_archives(depot,captures,release=release);manifest=manifest_for(captures,identity,release=release,table=table)
        locks=base/'_locks';locks.mkdir(exist_ok=True);lockpath=locks/(SLUG+'.lock')
        descriptor=os.open(lockpath,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        with os.fdopen(descriptor,'r+b')as lock:
            if os.fstat(lock.fileno()).st_nlink!=1:raise CollegeQuarantineError('linked_common_lock')
            fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
            original_context=None
            if root.exists():
                try:
                    try:original_context=original_provenance_bundle(storage,fingerprint,captures,identity,manifest,release=release)
                    except(ValueError,TypeError,KeyError,OSError,RuntimeError)as loss:
                        try:record_observed_loss(storage,version,fingerprint,str(loss))
                        except Exception as write:raise IncidentWriteError('incident_write_failed_candidate_preserved')from write
                        raise CollegeQuarantineError('observed_original_trust_loss:'+str(loss))from loss
                    captures,manifest=original_context;reconcile_cache(storage,prefix,table,audit,captures,identity,original_context,release=release)
                except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise CollegeQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc))from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True,'release_accepted':False,'latest_checked_at':checked_at}
            if state_present(storage,version):
                try:captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest,release=release)
                except(ValueError,TypeError,KeyError,OSError,RuntimeError)as loss:
                    try:record_observed_loss(storage,version,fingerprint,str(loss))
                    except Exception as write:raise IncidentWriteError('incident_write_failed_partial_state_preserved')from write
                    raise CollegeQuarantineError('observed_original_trust_loss:'+str(loss))from loss
            build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='candidate-',dir=build)as tmp:
                temp=Path(tmp)
                for part in('geopackage','geoparquet','metadata','raw'):(temp/part).mkdir()
                write_native_gpkg(table,temp/'geopackage'/f'{SLUG}.gpkg',release=release)
                (temp/'geoparquet/colleges-and-universities.parquet').write_bytes(parquet_payload(table))
                reconcile_outputs(temp,table,release=release)
                for name,value in source_payloads(captures,identity,table,audit,manifest,release=release).items():write_private(temp,name,value)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));reconcile_private(temp,table,audit,captures,identity,manifest,version,release=release)
                captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest,create=True,release=release)
                for name,value in source_payloads(captures,identity,table,audit,manifest,release=release).items():write_private(temp,name,value)
                ready['objects']={str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()and str(p.relative_to(temp))!=READY};write_private(temp,READY,encode(ready));reconcile_private(temp,table,audit,captures,identity,manifest,version,release=release);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False,'release_accepted':False,'latest_checked_at':checked_at}
    except(ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError)as exc:
        where=quarantine(storage,captures,str(exc));raise CollegeQuarantineError(f'{exc}; retained:{where}')from exc
