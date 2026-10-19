"""Complete official Census TIGER2026 elementary-school districts; gated local draft.

All 25 producer state publications, original DBF declarations/IDs/file-local FIDs,
and native Polygon/MultiPolygon coordinates are retained. No source, baseline,
schema mapping, territorial interpretation or release is accepted by this code.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import sqlite3
import stat
import subprocess
import tempfile
import zipfile
import shutil
import xml.etree.ElementTree as ET
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
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file
SLUG='elementary-school-districts'
LAYER='Elementary_School_Districts'
DIRECTORY='https://www2.census.gov/geo/tiger/TIGER2026/ELSD/'
LICENSE='TIGER/Line products are not copyrighted; U.S. Census Bureau acknowledgement required (captured producer ISO resourceConstraints).'
PINS={'tl_2026_04_elsd': {'bytes': 767817, 'sha256': '009bbc8b48c2ab6d6dfa74c82951b47d163bb0d7d1115541fb44704e51e2d9d4'}, 'tl_2026_06_elsd': {'bytes': 3726253, 'sha256': 'd053f3c2f3a461bfb6ffaa5cb0d66279e8c4cf4f6a24fc822459245577bcf247'}, 'tl_2026_09_elsd': {'bytes': 199327, 'sha256': '5c85b7f6df54a98fbb8a259038190a788f000a416952278141954f4f55793882'}, 'tl_2026_13_elsd': {'bytes': 27307, 'sha256': '4120a746403bd94ff4e73f11a0a48e149d8b47e5c0501926441cbf669dacc3a2'}, 'tl_2026_17_elsd': {'bytes': 1846603, 'sha256': '2f32ffdc274c919f7fb87a57bc28a223a376129eaa851fce03e06ffa07e76e37'}, 'tl_2026_21_elsd': {'bytes': 34408, 'sha256': '8f9b3b11fd7360dd25f279c6eeea30978a9884e9a77c4306a7b38617daaccfbb'}, 'tl_2026_23_elsd': {'bytes': 504615, 'sha256': '67a6af772226f75896589ddff35d37a571c603a0ed92cd3fc40f1c8972da5ac4'}, 'tl_2026_25_elsd': {'bytes': 392234, 'sha256': '67e83c41e64a6993e364c1902c3a46521260daf393310b03802c25743ee2cc21'}, 'tl_2026_26_elsd': {'bytes': 79550, 'sha256': '45b194b46d5ad3092521ee5c572a23147f241689aaf0c419de8eae582146f31f'}, 'tl_2026_27_elsd': {'bytes': 58267, 'sha256': '21fc8d1ff64f0d6f96d049134a58d7efad8c8ead9e85a4788b5e6e3a16c17a80'}, 'tl_2026_29_elsd': {'bytes': 436518, 'sha256': 'eaf45da038dcf9dee17223a8971c8258f21a9126aba8c1ae93aea2f16901ae6f'}, 'tl_2026_30_elsd': {'bytes': 2852330, 'sha256': 'c7923593d8190c216c0e0f9cf85eebe13c1bb03e05a9ebbe868232909399d21b'}, 'tl_2026_33_elsd': {'bytes': 415738, 'sha256': 'ed330db653aeed863cf81bf9e39d59cbc8b33d7530ba3e2d8380061f6bd18e58'}, 'tl_2026_34_elsd': {'bytes': 1768600, 'sha256': '80fd3ac9566f0a51faaa5063b2ac39e8e61cb9c5709cd1b01e16f9ce43c6b009'}, 'tl_2026_36_elsd': {'bytes': 41715, 'sha256': '189844fa31cd229449f272316c7dbf7ceafdd9f9f9c2f59e6faf553bc900200e'}, 'tl_2026_38_elsd': {'bytes': 152230, 'sha256': 'aa1e8daede776eef0b8394c594a4e2c8f1e4c04aa1389ec8f4cb1944ee4ea7c2'}, 'tl_2026_40_elsd': {'bytes': 548072, 'sha256': '0ff8959ee7622d5808e2d3886c715c84834c095cd665cce69f972d715b0e8ef0'}, 'tl_2026_41_elsd': {'bytes': 93288, 'sha256': '8d7c76cef173509dd72abc1e5faf6cdf9dba34a535d16fe0a4efdc09c08010cf'}, 'tl_2026_44_elsd': {'bytes': 37730, 'sha256': 'a6bd74e799586183cb1117a0fd3777a42eb5a136124070a2d5ed857ed39b891d'}, 'tl_2026_47_elsd': {'bytes': 300905, 'sha256': 'a8dbc8894b1c12be02653519e0a4f52ace3d0a46c9466d87aec66c89f0ffb0ff'}, 'tl_2026_48_elsd': {'bytes': 17575, 'sha256': '5ad225ff144ff07a34e8da4bd9d74952bcda305d4efbd5b23bb730d4b125b81e'}, 'tl_2026_50_elsd': {'bytes': 220479, 'sha256': 'e97fa265fcec0ce37f31156e93469b10b3cd81b34ae019d68aff4a9cc8572594'}, 'tl_2026_51_elsd': {'bytes': 17643, 'sha256': '148281081703b6f94aa98c5fb519142851779f10ab0399002cb1e3aae8d81645'}, 'tl_2026_55_elsd': {'bytes': 251542, 'sha256': '36c3562821aa6ee6141105125be034eb0fd1c76ea1fb805376ff57630d86a454'}, 'tl_2026_56_elsd': {'bytes': 30572, 'sha256': '8383b6511cde85daf88c577f9f5224ac43536da4151d49773626c09e78620a9c'}}
URLS={'census2026_elsd':DIRECTORY,**{name:DIRECTORY+name+'.zip' for name in PINS}}
SOURCE_FIELDS=('OGC_FID', 'STATEFP', 'ELSDLEA', 'GEOID', 'GEOIDFQ', 'NAME', 'LSAD', 'LOGRADE', 'HIGRADE', 'MTFCC', 'SDTYP', 'FUNCSTAT', 'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON', 'wkb_geometry')
FIELDS=('source_archive',*SOURCE_FIELDS)
TYPES=(pa.string(),pa.int64(),*[pa.string()]*11,pa.int64(),pa.int64(),pa.string(),pa.string(),pa.binary())
DBF_FIELDS=[('STATEFP', 'C', 2, 0), ('ELSDLEA', 'C', 5, 0), ('GEOID', 'C', 7, 0), ('GEOIDFQ', 'C', 16, 0), ('NAME', 'C', 100, 0), ('LSAD', 'C', 2, 0), ('LOGRADE', 'C', 2, 0), ('HIGRADE', 'C', 2, 0), ('MTFCC', 'C', 5, 0), ('SDTYP', 'C', 1, 0), ('FUNCSTAT', 'C', 1, 0), ('ALAND', 'N', 14, 0), ('AWATER', 'N', 14, 0), ('INTPTLAT', 'C', 11, 0), ('INTPTLON', 'C', 12, 0)]
STATE_COUNTS={'04': 102, '06': 517, '09': 44, '13': 1, '17': 371, '21': 4, '23': 74, '25': 64, '26': 27, '27': 8, '29': 71, '30': 233, '33': 90, '34': 168, '36': 14, '38': 20, '40': 91, '41': 9, '44': 5, '47': 15, '48': 1, '50': 56, '51': 1, '55': 43, '56': 1}
NULL_COUNTS={'OGC_FID': 0, 'STATEFP': 0, 'ELSDLEA': 0, 'GEOID': 0, 'GEOIDFQ': 0, 'NAME': 0, 'LSAD': 0, 'LOGRADE': 0, 'HIGRADE': 0, 'MTFCC': 0, 'SDTYP': 1988, 'FUNCSTAT': 0, 'ALAND': 0, 'AWATER': 0, 'INTPTLAT': 0, 'INTPTLON': 0, 'wkb_geometry': 0}
GEOIDS_SHA256='a26ea567e3ae3bf8671d8622d394c6c81e349c4f15235aca80a4555836808697'
MEMBER_SUFFIXES=('.cpg','.dbf','.prj','.shp','.shp.ea.iso.xml','.shp.iso.xml','.shx')
PARQUET_PIN={'sha256': 'b969423641e1d492ab1f4e7b5d4c226cc314b2d79a056df46deabf0163285622', 'bytes': 18613579}
READY='metadata/elsd_candidate_ready.json'
SOURCE_RECORDS_SHA256='898df6fdde5f35c7ccd62707e60c5b7b92544e582d93b2b9f5427d404233fe4f'
COUNT=2030
class ElementaryDistrictQuarantineError(ValueError):
    """Unknown or incomplete source/candidate stays outside discovery."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ElementaryDistrictQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise ElementaryDistrictQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if shutil.disk_usage(Path(storage.local_dir) if storage.local_dir and Path(storage.local_dir).exists() else Path('/workspace')).free<300_000_000: raise ElementaryDistrictQuarantineError('local_space_guard')
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ElementaryDistrictQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    if any(path.is_symlink() for path in [root,*root.parents]):
        raise ElementaryDistrictQuarantineError('staging_path_symlink_refused')
    if root.exists() and any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):
        raise ElementaryDistrictQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise ElementaryDistrictQuarantineError('immutable_capture_changed:'+key)


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


def acquire(storage,capture_dir=None):
    local_only(storage);captures={}
    try:
        for name in URLS:
            if capture_dir is not None:
                with (Path(capture_dir)/(name+'.body')).open('rb') as stream: body=stream.read(15_000_001)
                # Retain attempted bytes before any receipt decoding. A malformed
                # local receipt is evidence, not permission to lose its body.
                key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
                immutable(storage,key,body)
                diagnostic={'url':URLS[name],'body_key':key,'bytes':len(body),
                    'sha256':digest(body),'complete_body':False,'receipt_validated':False}
                captures[name]=(body,diagnostic)
                if len(body)>15_000_000: raise ElementaryDistrictQuarantineError('capture_body_limit')
                with (Path(capture_dir)/(name+'.receipt.json')).open('rb') as stream:
                    raw_receipt=stream.read(65_537)
                receipt_key=key+'.receipt-original-'+digest(raw_receipt)+'.bin'
                immutable(storage,receipt_key,raw_receipt)
                diagnostic['original_receipt_key']=receipt_key
                if len(raw_receipt)>65_536:raise ElementaryDistrictQuarantineError('capture_receipt_limit')
                receipt=strict_json(raw_receipt)
                captures[name]=(body,receipt)
            else:
                url=URLS[name]
                limit=15_000_000
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
                                    if len(body)>limit: raise ElementaryDistrictQuarantineError('oversized_or_changed_source')
                            finally:
                                receipt['wire_bytes_downloaded']=getattr(response,'num_bytes_downloaded',None)
                            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                            # HTTP Content-Length measures the encoded transport body.
                            # iter_bytes transparently decodes Content-Encoding, even
                            # when the server ignores our identity request.
                            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):
                                raise ElementaryDistrictQuarantineError('non200_or_incomplete_http_response')
                        captures[name]=(bytes(body),receipt);break
                    except (httpx.HTTPError,OSError,ValueError) as exc:
                        receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
                        quarantine(storage,{name:(bytes(body),receipt)},str(exc))
                        if attempt==2: raise ElementaryDistrictQuarantineError('bounded_fetch_exhausted:'+str(exc)) from exc
        return captures
    except (OSError,ValueError,TypeError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise ElementaryDistrictQuarantineError(f'{exc}; retained:{where}') from exc


def dbf_definition(body):
    if len(body)<33: raise ElementaryDistrictQuarantineError('short_dbf_header')
    rows=int.from_bytes(body[4:8],'little');header=int.from_bytes(body[8:10],'little');record=int.from_bytes(body[10:12],'little')
    if header<33 or (header-33)%32 or body[header-1]!=13 or len(body) not in (header+rows*record,header+rows*record+1):
        raise ElementaryDistrictQuarantineError('incomplete_or_changed_dbf_records')
    fields=[]
    for index in range(32,header-1,32):
        field=body[index:index+32]
        fields.append((field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
    if fields!=DBF_FIELDS or record!=1+sum(field[2] for field in fields) or any(body[header+i*record]!=32 for i in range(rows)):
        raise ElementaryDistrictQuarantineError('native_dbf_physical_declarations_or_deleted_rows_drift')
    return {'fields':fields,'row_count':rows,'record_bytes':record,'source_oid_declaration':'No DBF OID. OGC_FID is the file-local driver index, preserved separately.'}


def decode_dbf_rows(body):
    """Independent physical DBF decode checks every source attribute/precision."""
    definition=dbf_definition(body)
    header=int.from_bytes(body[8:10],'little');width=definition['record_bytes'];rows=[]
    for index in range(definition['row_count']):
        cursor=header+index*width+1;row={}
        for name,kind,length,decimals in DBF_FIELDS:
            raw=body[cursor:cursor+length];cursor+=length
            if kind=='C':
                decoded=raw.rstrip(b' \0').decode('utf8')
                row[name]=decoded if decoded else None
            else:
                value=raw.strip().decode('ascii')
                if not re.fullmatch(r'-?\d+',value): raise ElementaryDistrictQuarantineError('native_dbf_numeric_null_or_nonintegral_drift')
                row[name]=int(value)
        rows.append(row)
    return rows


def inspect_archives(paths,captures):
    directory=captures['census2026_elsd'][0].decode('utf8')
    listed=re.findall(r'href="(tl_2026_\d{2}_elsd.zip)"',directory)
    if len(listed)!=25 or sorted(listed)!=[name+'.zip' for name in PINS]:
        raise ElementaryDistrictQuarantineError('official_directory_completeness_roster_drift')
    tables=[];audit={}
    for name in PINS:
        body=captures[name][0]
        if len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']:
            raise ElementaryDistrictQuarantineError('changed_truncated_or_unknown_census_publication:'+name)
        with zipfile.ZipFile(paths[name]) as archive:
            infos=archive.infolist();names=[i.filename for i in infos]
            if set(names)!={name+suffix for suffix in MEMBER_SUFFIXES} or len(names)!=7 or archive.testzip() is not None:
                raise ElementaryDistrictQuarantineError('missing_duplicate_or_corrupt_shapefile_member')
            if any(PurePosixPath(i.filename).name!=i.filename or i.flag_bits&1 or stat.S_ISLNK(i.external_attr>>16) for i in infos) or sum(i.file_size for i in infos)>30000000:
                raise ElementaryDistrictQuarantineError('unsafe_archive_member_or_expansion')
            dbf=dbf_definition(archive.read(name+'.dbf'))
            physical_rows=decode_dbf_rows(archive.read(name+'.dbf'))
            if archive.read(name+'.cpg')!=b'UTF-8': raise ElementaryDistrictQuarantineError('encoding_drift')
            xml=ET.fromstring(archive.read(name+'.shp.iso.xml'))
            ns={'gmd':'http://www.isotc211.org/2005/gmd','gco':'http://www.isotc211.org/2005/gco'}
            citations=[]
            for citation in xml.findall('.//gmd:CI_Citation',ns):
                title=citation.findtext('./gmd:title/gco:CharacterString',namespaces=ns)
                if title and title.startswith('TIGER/Line Shapefile, 2026, State,'):
                    citations.append({'title':title,'dates':[{'type':d.find('./gmd:dateType/gmd:CI_DateTypeCode',ns).get('codeListValue'),'value':''.join(d.find('./gmd:date',ns).itertext()).strip()} for d in citation.findall('./gmd:date/gmd:CI_Date',ns)]})
            if len(citations)!=1: raise ElementaryDistrictQuarantineError('producer_citation_missing')
            rights=[' '.join(''.join(x.itertext()).split()) for x in xml.findall('.//gmd:resourceConstraints',ns)]
            if not any('not copyrighted' in text and 'acknowledgement' in text for text in rights): raise ElementaryDistrictQuarantineError('producer_rights_evidence_missing')
            inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}','sha256':digest(archive.read(i.filename))} for i in infos}
        meta,table=pyogrio.read_arrow('/vsizip/{'+str(paths[name])+'}/'+name+'.shp',return_fids=True)
        if (tuple(table.column_names)!=SOURCE_FIELDS or tuple(f.type for f in table.schema)!=TYPES[1:] or meta['crs']!='EPSG:4269'
            or meta['fid_column']!='OGC_FID' or meta['geometry_type']!='Polygon' or table.num_rows!=dbf['row_count']):
            raise ElementaryDistrictQuarantineError('source_schema_types_crs_geometry_or_dbf_row_completeness_drift')
        if table['OGC_FID'].to_pylist()!=list(range(table.num_rows)) or any(v!=name[8:10] for v in table['STATEFP'].to_pylist()):
            raise ElementaryDistrictQuarantineError('file_local_fid_or_state_coverage_drift')
        if [{k:r[k] for k,_,_,_ in DBF_FIELDS} for r in table.to_pylist()]!=physical_rows:
            raise ElementaryDistrictQuarantineError('driver_attribute_values_disagree_with_independent_physical_dbf_decode')
        table=table.add_column(0,'source_archive',pa.array([name]*table.num_rows))
        tables.append(table)
        audit[name]={'dbf':dbf,'members':inventory,'producer_citation':citations[0],'producer_resource_constraints':rights,'driver_arrow_schema':str(table.schema),'full_source_GEOIDs':table['GEOID'].to_pylist()}
    combined=pa.concat_tables(tables)
    validate_table(combined)
    return combined,audit


def validate_table(table):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=COUNT:
        raise ElementaryDistrictQuarantineError('native_schema_physical_types_or_completeness_drift')
    if any(table[name].null_count!=NULL_COUNTS.get(name,0) for name in FIELDS):
        raise ElementaryDistrictQuarantineError('source_nullability_drift')
    rows=table.to_pylist()
    if dict(collections.Counter(table['STATEFP'].to_pylist()))!=STATE_COUNTS:
        raise ElementaryDistrictQuarantineError('complete_producer_state_publication_coverage_drift')
    ids=table['GEOID'].to_pylist()
    if len(set(ids))!=COUNT or digest(encode(sorted(ids)))!=GEOIDS_SHA256:
        raise ElementaryDistrictQuarantineError('missing_duplicate_changed_source_GEOID')
    if any(r['GEOID']!=r['STATEFP']+r['ELSDLEA'] or r['GEOIDFQ']!='9500000US'+r['GEOID'] or r['MTFCC']!='G5400' for r in rows):
        raise ElementaryDistrictQuarantineError('literal_elementary_district_identifier_or_type_drift')
    for name in PINS:
        fids=[r['OGC_FID']for r in rows if r['source_archive']==name]
        if fids!=list(range(STATE_COUNTS[name[8:10]])):
            raise ElementaryDistrictQuarantineError('original_file_local_FID_or_archive_roster_drift')
    if record_hash(table)!=SOURCE_RECORDS_SHA256:raise ElementaryDistrictQuarantineError('complete_original_attribute_values_and_native_WKB_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if (not all(shapely.is_valid(geoms)) or any(shapely.is_empty(geoms))
        or dict(collections.Counter(map(int,shapely.get_type_id(geoms))))!={3:1930,6:100}
        or int(shapely.get_num_coordinates(geoms).sum())!=1455682):
        raise ElementaryDistrictQuarantineError('missing_invalid_empty_or_changed_native_geometry')


def validate_receipt(name,body,receipt):
    url=URLS[name]
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int
        or receipt['status']!=200 or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body) or receipt.get('complete_body')is not True):
        raise ElementaryDistrictQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'U.S. Census Bureau','host':'www2.census.gov',
        'role':'producer_publication','canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
        'retrieved_at':receipt.get('observed_at',receipt.get('fetched_at')),'http_status':receipt['status'],'sha256':digest(body),'bytes':len(body),
        'license':LICENSE,'attribution':'U.S. Census Bureau, TIGER/Line2026 elementary-school-district state publications.',
        'release_date_unknown_reason':'Producer ISO citation gives publication year2026, creation2026-09 and lastUpdate2026-10; exact publication day not established. Boundaries explicitly in operation January1,2026; fetch/edit/HTTP Last-Modified are not release days.'}


def manifest_for(captures,identity):
    if set(captures)!=set(URLS):
        raise ElementaryDistrictQuarantineError('missing_extra_or_incomplete_publication_roster')
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Elementary School Districts — complete CensusTIGER2026 unapproved successor draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':DIRECTORY,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n])for n in URLS],
        'transformations':[{'operation':'decode_all_25_original_state_shapefiles','parameters':{
            'source_identifiers':'STATEFP, ELSDLEA, GEOID, GEOIDFQ and file-local OGC_FID retained exactly; no published-ID substitution',
            'source_archive':'literal original ZIP stem disambiguates repeated file-local FIDs; extra provenance column remains unapproved',
            'raw_dbf':'all original DBF names/types/widths/null conventions retained in raw archives/audit; blank C fields decode to source-driver NULL, leading characters retained',
            'geometry':'native EPSG4269 Polygon/MultiPolygon WKB, no repair, wrapping, simplification or reprojection',
            'coverage':'all25 state publications in exact official2026ELSD directory; elementary districts only, no unified/secondary districts or fabricated territories'}},
            {'operation':'deterministic_native_geopackage_and_geoparquet','parameters':{
                'ALAND_AWATER':'original N14,0 integer values retained as int64, never float',
                'gpkg_storage_fid':'separate storage-only primary key, not a new source/published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Additional provenance column/native widths cannot be silently truncated or renamed; all25complete original shapefiles remain raw.',
            'pmtiles':'Projection/quantization would change native geometry; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Retain exact original physical/Arrow values and native geometry; no generic schema/bbox/type migration accepted by this capability.',
            'objects':[{'path':f'geoparquet/{SLUG}.parquet',**PARQUET_PIN}]}}}}
    validate_release_provenance(manifest)
    return manifest


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{
        'encoding':'WKB','geometry_types':['Polygon','MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict(),
        'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_native_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='wkb_geometry',geometry_type='Unknown',crs='EPSG:4269',
        layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path)as db:
        db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet'
    if sha256_file(path)!=PARQUET_PIN['sha256'] or path.stat().st_size!=PARQUET_PIN['bytes'] or not pq.read_table(path).equals(expected_parquet_table(table),check_metadata=True):
        raise ElementaryDistrictQuarantineError('native_parquet_full_values_types_WKB_or_metadata_drift')
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if gm['crs']!='EPSG:4269'or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():
        raise ElementaryDistrictQuarantineError('native_geopackage_full_values_types_or_WKB_drift')
    with tempfile.TemporaryDirectory(prefix='elsd-canonical-gpkg-')as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise ElementaryDistrictQuarantineError('geopackage_full_container_SRS_envelope_SQL_or_rtree_drift')


def source_payloads(captures,identity,table,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/elsd_source_audit.json':encode({'approved':False,'feature_count':COUNT,'crs':'EPSG:4269','invalid_geometries':0,'empty_geometries':0,
            'polygons':1930,'multipolygons':100,'native_coordinates':1455682,'source_state_counts':STATE_COUNTS,
            'complete_original_publication_audit':audit,'all_values_ids_declared_types_native_WKB_roundtrip':True}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked',
            'reason_codes':['source_baseline_ID_schema_type_geometry_coverage_mapping_not_approved']})}
    for name,(body,receipt)in captures.items():
        values['raw/captures/'+name+'.body']=body
        values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def original_bundle(storage,key,version,identity,*,create=None):
    fingerprint=digest(encode(identity))
    if create is None:body=load_first_construction(storage,key,version,fingerprint)
    else:
        captures,manifest=create
        # All source claims, pinned originals and code semantics precede trust.
        expected_manifest=manifest_for(captures,identity)
        if encode({k:v for k,v in manifest.items()if k!='code_commit'})!=encode({k:v for k,v in expected_manifest.items()if k!='code_commit'}):
            raise ElementaryDistrictQuarantineError('preseal_original_manifest_semantic_drift')
        if any(len(captures[n][0])!=PINS[n]['bytes']or digest(captures[n][0])!=PINS[n]['sha256']for n in PINS):
            raise ElementaryDistrictQuarantineError('preseal_original_publication_pin_drift')
        listed=re.findall(r'href="(tl_2026_\d{2}_elsd.zip)"',captures['census2026_elsd'][0].decode('utf8'))
        if len(listed)!=25 or sorted(listed)!=[n+'.zip'for n in PINS]:
            raise ElementaryDistrictQuarantineError('preseal_original_directory_roster_drift')
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        payload={'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{n}.body','receipt':receipt}for n,(body,receipt)in captures.items()}}
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,payload))
    payload=strict_json(body)['payload']
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(URLS):raise ElementaryDistrictQuarantineError('original_construction_bundle_roster_drift')
    original={}
    for name,item in payload['captures'].items():
        receipt=item['receipt'];expected=f"_raw-captures/{SLUG}/{receipt['sha256']}/{name}.body"
        if item['body_key']!=expected:raise ElementaryDistrictQuarantineError('original_construction_body_key_drift')
        original[name]=(storage.read_key(expected),receipt)
    manifest=payload['manifest'];expected=manifest_for(original,identity)
    if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):raise ElementaryDistrictQuarantineError('original_construction_manifest_drift')
    validate_release_provenance(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
    return original,manifest


def reconcile_cache(storage,prefix,table,audit,captures,identity,manifest):
    root=Path(storage.local_dir)/prefix;ready=strict_json((root/READY).read_bytes());expected=source_payloads(captures,identity,table,audit,manifest)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg',f'geoparquet/{SLUG}.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1]or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity))or set(ready['objects'])!=expected_names):raise ElementaryDistrictQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if (root/name).read_bytes()!=value:raise ElementaryDistrictQuarantineError('cached_source_owned_bytes_drift:'+name)
    actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()};snapshot='metadata/source/version_'+digest((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    if actual-(expected_names|{READY}|derivatives)or not(expected_names|{READY}).issubset(actual):raise ElementaryDistrictQuarantineError('cached_unknown_or_missing_object')
    for name,pin in ready['objects'].items():
        if sha256_file(root/name)!=pin:raise ElementaryDistrictQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists()and(root/name).read_bytes()!=(root/'metadata/source_manifest.json').read_bytes():raise ElementaryDistrictQuarantineError('cached_source_snapshot_drift')
    if(root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs']or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved:raise ElementaryDistrictQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table)


def stage_elementary_districts(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);root=None
    try:
        identity=processing_identity();manifest=manifest_for(captures,identity)
        fingerprint=digest(encode(identity));version='v0.0.0-draft-census2026-'+fingerprint[:16];prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir);root=base/prefix
        for name,(body,receipt)in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        table,audit=inspect_archives({n:base/f"_raw-captures/{SLUG}/{digest(captures[n][0])}/{n}.body"for n in PINS},captures)
        record=f'_construction-state/{SLUG}/{fingerprint}.json';record_path=base/record
        locks=base/'_locks';locks.mkdir(exist_ok=True)
        lock_fd=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        with os.fdopen(lock_fd,'r+b')as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode)or os.fstat(lock.fileno()).st_nlink!=1:raise ElementaryDistrictQuarantineError('aliased_common_lock')
            fcntl.flock(lock,fcntl.LOCK_EX)
            has_history=record_path.exists()or Path(str(record_path)+'.first-construction.json').exists()or Path(str(record_path)+'.evidence').exists()
            if root.exists():
                try:
                    captures,manifest=original_bundle(storage,record,version,identity);table,audit=inspect_archives({n:base/f"_raw-captures/{SLUG}/{digest(captures[n][0])}/{n}.body"for n in PINS},captures)
                    reconcile_cache(storage,prefix,table,audit,captures,identity,manifest)
                except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise ElementaryDistrictQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc))from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'approved':False,'reused':True}
            if has_history:
                captures,manifest=original_bundle(storage,record,version,identity);table,audit=inspect_archives({n:base/f"_raw-captures/{SLUG}/{digest(captures[n][0])}/{n}.body"for n in PINS},captures)
            root.parent.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='.private-elsd-',dir=root.parent)as tmp:
                private=Path(tmp);values=source_payloads(captures,identity,table,audit,manifest)
                for name,body in values.items():path=private/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
                (private/'geopackage').mkdir();(private/'geoparquet').mkdir();write_native_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg');pq.write_table(expected_parquet_table(table),private/'geoparquet'/f'{SLUG}.parquet',compression='zstd',version='2.6')
                reconcile_outputs(private,table)
                roster={str(p.relative_to(private)):sha256_file(p)for p in private.rglob('*')if p.is_file()}
                (private/READY).write_bytes(encode({'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':roster}))
                # Complete private file roster and bytes precede the shared
                # write-once first-state CAS/marker/alias protocol.
                if not all(sha256_file(private/name)==pin for name,pin in roster.items()):raise ElementaryDistrictQuarantineError('private_candidate_bytes_changed_before_first_state')
                original,original_manifest=original_bundle(storage,record,version,identity,create=(captures,manifest))
                if encode(original_manifest)!=encode(manifest)or any(original[n][0]!=captures[n][0]or encode(original[n][1])!=encode(captures[n][1])for n in URLS):raise ElementaryDistrictQuarantineError('first_construction_winner_differs')
                if root.exists():raise ElementaryDistrictQuarantineError('concurrent_candidate_winner_preserved')
                os.rename(private,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'approved':False,'reused':False}
    except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
        where=quarantine(storage,captures,str(exc));raise ElementaryDistrictQuarantineError(f'{exc}; retained:{where}')from exc


def record_hash(table):
    value=hashlib.sha256()
    for row in table.to_pylist():
        row['wkb_geometry']=row['wkb_geometry'].hex()
        value.update(encode(row))
    return value.hexdigest()
