"""Complete Census TIGER2026 consolidated cities, unapproved successor draft.

All seven state publications are required. Original physical DBF declarations,
file-local FIDs, identifiers, field widths and native WKB are retained. Nothing
here accepts the different inventory edition, schema or published-ID mapping.
"""
from __future__ import annotations

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
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG = 'consolidated-cities'
LAYER = 'Consolidated_City'
DIRECTORY = 'https://www2.census.gov/geo/tiger/TIGER2026/CONCITY/'
LICENSE = 'https://www.census.gov/about/policies/quality/guidelines/copyright.html'
PINS = {'tl_2026_09_concity': {'bytes': 21198, 'sha256': '8753ff394c81e89db39c7e92b05f652c7f063209db9c3b1c6cec3bcd1e469359'}, 'tl_2026_13_concity': {'bytes': 70989, 'sha256': '7b1aef4ed64ac5fff90e73f24b9a2d6b3310889d2328e316705637b24a2901ac'}, 'tl_2026_18_concity': {'bytes': 40552, 'sha256': '317ac029de739b6bba242b763a97a2bc54b2384f2905177c5a69034ef9923622'}, 'tl_2026_20_concity': {'bytes': 20322, 'sha256': 'e7d5aab415b5fd5aa95cc42f2c0a1e95f117bdd3a78d7368e83479ac4cf0b964'}, 'tl_2026_21_concity': {'bytes': 33841, 'sha256': '30abb05506f8c9d4557f0272f5688d57462443e830db013c2e0091cfc59df255'}, 'tl_2026_30_concity': {'bytes': 77912, 'sha256': 'b32b8fbd032f66750cfcf753b6ee363ccf5b97d4fd854b9587049c22acdb52eb'}, 'tl_2026_47_concity': {'bytes': 57134, 'sha256': 'c4239eb3cb9853606a539c043c6c8db2459bf5badb670a65bc7c20e9c1b98c05'}}
SOURCE_FIELDS = ('OGC_FID','STATEFP','CONCTYFP','CONCTYNS','GEOID','GEOIDFQ','NAME','NAMELSAD','LSAD','CLASSFP','MTFCC','FUNCSTAT','ALAND','AWATER','INTPTLAT','INTPTLON','wkb_geometry')
FIELDS = ('source_archive',*SOURCE_FIELDS)
TYPES = (pa.string(),pa.int64(),*[pa.string()]*11,pa.int64(),pa.int64(),pa.string(),pa.string(),pa.binary())
DBF_FIELDS = [('STATEFP','C',2,0),('CONCTYFP','C',5,0),('CONCTYNS','C',8,0),('GEOID','C',7,0),('GEOIDFQ','C',16,0),('NAME','C',100,0),('NAMELSAD','C',100,0),('LSAD','C',2,0),('CLASSFP','C',2,0),('MTFCC','C',5,0),('FUNCSTAT','C',1,0),('ALAND','N',14,0),('AWATER','N',14,0),('INTPTLAT','C',11,0),('INTPTLON','C',12,0)]
GEOIDS = ('0947500','1303436','1304200','1836000','2028410','2148003','3011390','4752004')
MEMBER_SUFFIXES = ('.cpg','.dbf','.prj','.shp','.shp.ea.iso.xml','.shp.iso.xml','.shx')
READY = 'metadata/concity_candidate_ready.json'

class ConsolidatedCityQuarantineError(ValueError):
    """Changed or incomplete source/cache requires review, never replacement."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ConsolidatedCityQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise ConsolidatedCityQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise ConsolidatedCityQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    if any(path.is_symlink() for path in [root,*root.parents]):
        raise ConsolidatedCityQuarantineError('staging_path_symlink_refused')
    if root.exists() and any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):
        raise ConsolidatedCityQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise ConsolidatedCityQuarantineError('immutable_capture_changed:'+key)


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
    url=DIRECTORY if name=='census2026_concity' else DIRECTORY+name+'.zip'
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int
        or receipt['status']!=200 or type(receipt.get('bytes')) is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body) or receipt.get('complete_body') is not True):
        raise ConsolidatedCityQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'U.S. Census Bureau','host':'www2.census.gov',
        'role':'producer_publication','canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
        'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],'sha256':digest(body),'bytes':len(body),
        'license':LICENSE,'attribution':'U.S. Census Bureau, TIGER/Line 2026 consolidated city state publications.',
        'release_date_unknown_reason':'Producer ISO metadata states publication year2026, creation2026-09, lastUpdate2026-10, not an exact publication day. HTTP Last-Modified is packaging evidence only.'}


def manifest_for(captures,identity):
    if set(captures)!={'census2026_concity',*PINS}:
        raise ConsolidatedCityQuarantineError('missing_extra_or_incomplete_publication_roster')
    inputs=[validate_receipt(name,*captures[name]) for name in ['census2026_concity',*PINS]]
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Consolidated Cities — complete Census TIGER2026 unapproved successor draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':DIRECTORY,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'decode_all_seven_original_shapefiles','parameters':{'original_dbf_fields':'unchanged names, physical declarations retained in source audit','source_fid':'OGC_FID is the original file-local GDAL feature index; not a DBF field or replacement published ID','source_archive':'literal original archive stem distinguishes repeated file-local FIDs; not a generated row ID','crs':'EPSG:4269','repair':False,'reproject':False}},
            {'operation':'typed_native_geopackage_and_geoparquet','parameters':{'geometry':'preserve seven Polygon and one MultiPolygon WKB without wrapping','dbf_numeric_fields':'ALAND/AWATER N14,0 exposed/output int64, no float conversion','gpkg_fid':'independent storage index, not a source/published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Additional provenance column and field widths cannot be silently truncated or renamed to DBF. Original seven complete shapefiles remain available as raw source.',
            'pmtiles':'Native coordinates would be reprojected and quantized; no visualization mapping approved.'}}
    validate_release_provenance(manifest)
    return manifest


def acquire(storage,capture_dir=None):
    local_only(storage);captures={}
    try:
        for name in ['census2026_concity',*PINS]:
            if capture_dir is not None:
                body=(Path(capture_dir)/(name+'.body')).read_bytes()
                receipt=strict_json((Path(capture_dir)/(name+'.receipt.json')).read_bytes())
                captures[name]=(body,receipt)
            else:
                url=DIRECTORY if name=='census2026_concity' else DIRECTORY+name+'.zip'
                limit=262144 if name=='census2026_concity' else PINS[name]['bytes']
                for attempt in range(3):
                    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                    try:
                        with httpx.Client(follow_redirects=False,timeout=90) as client,client.stream('GET',url) as response:
                            receipt.update(status=response.status_code,final_url=str(response.url))
                            for block in response.iter_bytes(65536):
                                body+=block
                                if len(body)>limit: raise ConsolidatedCityQuarantineError('oversized_or_changed_source')
                            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                            if response.status_code!=200 or (response.headers.get('content-length') is not None and int(response.headers['content-length'])!=len(body)):
                                raise ConsolidatedCityQuarantineError('non200_or_incomplete_http_response')
                        captures[name]=(bytes(body),receipt);break
                    except (httpx.HTTPError,OSError,ValueError) as exc:
                        receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
                        quarantine(storage,{name:(bytes(body),receipt)},str(exc))
                        if attempt==2: raise ConsolidatedCityQuarantineError('bounded_fetch_exhausted:'+str(exc)) from exc
        return captures
    except (OSError,ValueError,TypeError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise ConsolidatedCityQuarantineError(f'{exc}; retained:{where}') from exc


def dbf_definition(body):
    if len(body)<33: raise ConsolidatedCityQuarantineError('short_dbf_header')
    rows=int.from_bytes(body[4:8],'little');header=int.from_bytes(body[8:10],'little');record=int.from_bytes(body[10:12],'little')
    if header<33 or (header-33)%32 or body[header-1]!=13 or len(body) not in (header+rows*record,header+rows*record+1):
        raise ConsolidatedCityQuarantineError('incomplete_or_changed_dbf_records')
    fields=[]
    for index in range(32,header-1,32):
        field=body[index:index+32]
        fields.append((field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
    if fields!=DBF_FIELDS or record!=1+sum(field[2] for field in fields) or any(body[header+i*record]!=32 for i in range(rows)):
        raise ConsolidatedCityQuarantineError('native_dbf_physical_declarations_or_deleted_rows_drift')
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
                if not re.fullmatch(r'-?\d+',value): raise ConsolidatedCityQuarantineError('native_dbf_numeric_null_or_nonintegral_drift')
                row[name]=int(value)
        rows.append(row)
    return rows


def inspect_archives(paths,captures):
    directory=captures['census2026_concity'][0].decode('utf8')
    listed=re.findall(r'href="(tl_2026_\d{2}_concity.zip)"',directory)
    if len(listed)!=7 or sorted(listed)!=[name+'.zip' for name in PINS]:
        raise ConsolidatedCityQuarantineError('official_directory_completeness_roster_drift')
    tables=[];audit={}
    for name in PINS:
        body=captures[name][0]
        if len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']:
            raise ConsolidatedCityQuarantineError('changed_truncated_or_unknown_census_publication:'+name)
        with zipfile.ZipFile(paths[name]) as archive:
            infos=archive.infolist();names=[i.filename for i in infos]
            if set(names)!={name+suffix for suffix in MEMBER_SUFFIXES} or len(names)!=7 or archive.testzip() is not None:
                raise ConsolidatedCityQuarantineError('missing_duplicate_or_corrupt_shapefile_member')
            if any(PurePosixPath(i.filename).name!=i.filename or i.flag_bits&1 or stat.S_ISLNK(i.external_attr>>16) for i in infos) or sum(i.file_size for i in infos)>5000000:
                raise ConsolidatedCityQuarantineError('unsafe_archive_member_or_expansion')
            dbf=dbf_definition(archive.read(name+'.dbf'))
            physical_rows=decode_dbf_rows(archive.read(name+'.dbf'))
            if archive.read(name+'.cpg')!=b'UTF-8': raise ConsolidatedCityQuarantineError('encoding_drift')
            xml=ET.fromstring(archive.read(name+'.shp.iso.xml'))
            ns={'gmd':'http://www.isotc211.org/2005/gmd','gco':'http://www.isotc211.org/2005/gco'}
            citations=[]
            for citation in xml.findall('.//gmd:CI_Citation',ns):
                title=citation.findtext('./gmd:title/gco:CharacterString',namespaces=ns)
                if title and title.startswith('TIGER/Line Shapefile, 2026, State,'):
                    citations.append({'title':title,'dates':[{'type':d.find('./gmd:dateType/gmd:CI_DateTypeCode',ns).get('codeListValue'),'value':''.join(d.find('./gmd:date',ns).itertext()).strip()} for d in citation.findall('./gmd:date/gmd:CI_Date',ns)]})
            if len(citations)!=1: raise ConsolidatedCityQuarantineError('producer_citation_missing')
            rights=[' '.join(''.join(x.itertext()).split()) for x in xml.findall('.//gmd:resourceConstraints',ns)]
            if not any('not copyrighted' in text and 'acknowledgement' in text for text in rights): raise ConsolidatedCityQuarantineError('producer_rights_evidence_missing')
            inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}','sha256':digest(archive.read(i.filename))} for i in infos}
        meta,table=pyogrio.read_arrow('/vsizip/{'+str(paths[name])+'}/'+name+'.shp',return_fids=True)
        if (tuple(table.column_names)!=SOURCE_FIELDS or tuple(f.type for f in table.schema)!=TYPES[1:] or meta['crs']!='EPSG:4269'
            or meta['fid_column']!='OGC_FID' or meta['geometry_type']!='Polygon' or table.num_rows!=dbf['row_count']):
            raise ConsolidatedCityQuarantineError('source_schema_types_crs_geometry_or_dbf_row_completeness_drift')
        if table['OGC_FID'].to_pylist()!=list(range(table.num_rows)) or any(v!=name[8:10] for v in table['STATEFP'].to_pylist()):
            raise ConsolidatedCityQuarantineError('file_local_fid_or_state_coverage_drift')
        if [{k:r[k] for k,_,_,_ in DBF_FIELDS} for r in table.to_pylist()]!=physical_rows:
            raise ConsolidatedCityQuarantineError('driver_attribute_values_disagree_with_independent_physical_dbf_decode')
        table=table.add_column(0,'source_archive',pa.array([name]*table.num_rows))
        tables.append(table)
        audit[name]={'dbf':dbf,'members':inventory,'producer_citation':citations[0],'producer_resource_constraints':rights,'driver_arrow_schema':str(table.schema),'full_source_GEOIDs':table['GEOID'].to_pylist()}
    combined=pa.concat_tables(tables)
    validate_table(combined)
    return combined,audit


def validate_table(table):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=8 or any(table[n].null_count for n in FIELDS):
        raise ConsolidatedCityQuarantineError('native_schema_physical_types_nullability_or_completeness_drift')
    if sorted(table['GEOID'].to_pylist())!=list(GEOIDS): raise ConsolidatedCityQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    rows=table.to_pylist()
    if any(r['GEOID']!=r['STATEFP']+r['CONCTYFP'] or r['GEOIDFQ']!='1700000US'+r['GEOID'] for r in rows):
        raise ConsolidatedCityQuarantineError('source_identifier_value_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if not all(shapely.is_valid(geoms)) or any(shapely.is_empty(geoms)) or sorted(shapely.get_type_id(geoms).tolist())!=[3]*7+[6]:
        raise ConsolidatedCityQuarantineError('missing_invalid_or_changed_native_geometry')


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{'encoding':'WKB','geometry_types':['Polygon','MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_native_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='wkb_geometry',geometry_type='Unknown',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path) as db: db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table):
    expected=expected_parquet_table(table)
    parquet=pq.read_table(root/'candidate/typed-source.parquet')
    if not parquet.equals(expected,check_metadata=True): raise ConsolidatedCityQuarantineError('cached_parquet_values_types_or_metadata_disagree_with_fresh_original')
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if gm['crs']!='EPSG:4269' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():
        raise ConsolidatedCityQuarantineError('cached_geopackage_values_types_or_wkb_disagree_with_fresh_original')
    with tempfile.TemporaryDirectory(prefix='concity-canonical-gpkg-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise ConsolidatedCityQuarantineError('cached_geopackage_full_container_disagrees_with_fresh_original')


def source_payloads(captures,identity,table,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/concity_source_audit.json':encode({'approved':False,'feature_count':8,'crs':'EPSG:4269','invalid_geometries':0,'polygons':7,'multipolygons':1,
            'archive_count':7,'original_publications':audit,'full_values_ids_physical_types_native_wkb_roundtrip':True,
            'source_relationship':'Changed2026 successor, not exact2024 GDB continuation; all schema/ID/type/geometry mappings remain unapproved.'}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_schema_id_geometry_mapping_not_approved']})}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,table,audit,captures,identity):
    root=Path(storage.local_dir)/prefix;ready=strict_json((root/READY).read_bytes())
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in ['census2026_concity',*PINS]}
    if any(cached[name][0]!=captures[name][0] for name in PINS): raise ConsolidatedCityQuarantineError('cached_original_publication_changed')
    manifest=manifest_for(cached,identity)
    actual_manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(actual_manifest)
    for key,value in manifest.items():
        if key!='code_commit' and actual_manifest.get(key)!=value: raise ConsolidatedCityQuarantineError('cached_provenance_disagrees_with_original')
    manifest=actual_manifest
    expected=source_payloads(cached,identity,table,audit,manifest)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1] or ready.get('approved') is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready['objects'])!=expected_names):
        raise ConsolidatedCityQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if (root/name).read_bytes()!=value: raise ConsolidatedCityQuarantineError('cached_source_owned_bytes_drift:'+name)
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    snapshot='metadata/source/version_'+digest((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    if actual-(expected_names|{READY}|derivatives) or not (expected_names|{READY}).issubset(actual):
        raise ConsolidatedCityQuarantineError('cached_unknown_or_missing_object')
    for name,pin in ready['objects'].items():
        if sha256_file(root/name)!=pin: raise ConsolidatedCityQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists() and (root/name).read_bytes()!=(root/'metadata/source_manifest.json').read_bytes(): raise ConsolidatedCityQuarantineError('cached_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs'] or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved:
            raise ConsolidatedCityQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table)


def stage_consolidated_cities(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    root=None
    try:
        identity=processing_identity();manifest=manifest_for(captures,identity)
        fingerprint=digest(encode(identity));version=f'v0.0.0-draft-census2026-{fingerprint[:16]}'
        prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir);root=base/prefix
        depot={}
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
            immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt));depot[name]=base/key
        table,audit=inspect_archives(depot,captures)
        locks=base/'_locks';locks.mkdir(exist_ok=True);lockpath=locks/(SLUG+'.lock')
        with lockpath.open('a+b') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if root.exists():
                try: reconcile_cache(storage,prefix,table,audit,captures,identity)
                except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise ConsolidatedCityQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':8,'reused':True}
            build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='candidate-',dir=build) as tmp:
                temp=Path(tmp)
                for part in ('geopackage','candidate','metadata','raw'): (temp/part).mkdir()
                write_native_gpkg(table,temp/'geopackage'/f'{SLUG}.gpkg')
                pq.write_table(expected_parquet_table(table),temp/'candidate/typed-source.parquet',compression='zstd')
                reconcile_outputs(temp,table)
                for name,value in source_payloads(captures,identity,table,audit,manifest).items():
                    (temp/name).parent.mkdir(parents=True,exist_ok=True);(temp/name).write_bytes(value)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p) for p in temp.rglob('*') if p.is_file()}}
                (temp/READY).write_bytes(encode(ready));root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':8,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise ConsolidatedCityQuarantineError(f'{exc}; retained:{where}') from exc
