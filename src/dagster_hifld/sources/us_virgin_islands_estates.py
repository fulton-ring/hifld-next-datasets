"""Complete Census TIGER2026 U.S. Virgin Islands estates, unapproved draft.

The single estate publication is required. Original physical DBF declarations,
file-local FIDs, identifiers, field widths and native WKB are retained. Nothing
here accepts a mirror, the retired HIFLD copy or any replacement ID mapping.
"""
from __future__ import annotations

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
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.gdal import with_geopackage_creation_time
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import (
    STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as packaged_processing_identity,
    validate_staged_release_derivatives,
)
from dagster_hifld.validation import sha256_file

SLUG = 'us-virgin-islands-estates'
LAYER = 'Estate'
DIRECTORY = 'https://www2.census.gov/geo/tiger/TIGER2026/ESTATE/'
LICENSE = 'https://www.census.gov/about/policies/quality/guidelines/copyright.html'
PINS = {'tl_2026_78_estate': {'bytes': 659951, 'sha256': '47b8a7dd9a52a634ed6e8bfe1c7d1b28453c9994d399b6f39a1f71772e7e6f5a'}}
SOURCE_FIELDS = ('OGC_FID','STATEFP','COUNTYFP','ESTATEFP','ESTATENS','GEOID','GEOIDFQ','NAME','NAMELSAD','LSAD','CLASSFP','MTFCC','FUNCSTAT','ALAND','AWATER','INTPTLAT','INTPTLON','wkb_geometry')
FIELDS = ('source_archive',*SOURCE_FIELDS)
TYPES = (pa.string(),pa.int64(),*[pa.string()]*12,pa.int64(),pa.int64(),pa.string(),pa.string(),pa.binary())
DBF_FIELDS = [('STATEFP','C',2,0),('COUNTYFP','C',3,0),('ESTATEFP','C',5,0),('ESTATENS','C',8,0),('GEOID','C',10,0),('GEOIDFQ','C',19,0),('NAME','C',100,0),('NAMELSAD','C',100,0),('LSAD','C',2,0),('CLASSFP','C',2,0),('MTFCC','C',5,0),('FUNCSTAT','C',1,0),('ALAND','N',14,0),('AWATER','N',14,0),('INTPTLAT','C',11,0),('INTPTLON','C',12,0)]
GEOIDS = ('7801000620','7801000800','7801001400','7801001450','7801002500','7801003540','7801003630','7801003800','7801005540','7801005580','7801005590','7801006175','7801006225','7801006260','7801007500','7801007650','7801007850','7801007910','7801007915','7801007940','7801008070','7801008085','7801008600','7801009300','7801009400','7801009500','7801009600','7801010770','7801010800','7801011525','7801011750','7801011900','7801011950','7801012050','7801012500','7801012750','7801012800','7801013700','7801014600','7801014700','7801014840','7801014870','7801020000','7801022325','7801022330','7801022400','7801023200','7801023500','7801024410','7801024470','7801024500','7801024600','7801026450','7801026600','7801027250','7801027300','7801027525','7801027600','7801029825','7801029850','7801030300','7801034850','7801035150','7801035450','7801035650','7801036050','7801037700','7801038000','7801041600','7801043500','7801044250','7801044300','7801045050','7801045160','7801045175','7801045225','7801045525','7801045575','7801046100','7801046150','7801046175','7801046320','7801046330','7801046350','7801046380','7801047000','7801047600','7801047750','7801048030','7801048045','7801048130','7801048140','7801048150','7801048190','7801048600','7801049000','7801049700','7801049930','7801049960','7801050550','7801051710','7801053400','7801053970','7801054000','7801054200','7801054250','7801054300','7801054920','7801054930','7801054940','7801054970','7801054975','7801054980','7801055100','7801055200','7801055930','7801055960','7801056200','7801056860','7801056950','7801057050','7801057100','7801057250','7801057400','7801057425','7801057530','7801057560','7801057580','7801057650','7801057660','7801057670','7801057680','7801057690','7801057795','7801057805','7801057810','7801057840','7801057860','7801057880','7801057950','7801058500','7801059000','7801059550','7801059600','7801060850','7801061450','7801061700','7801062580','7801062730','7801062760','7801063050','7801063175','7801063300','7801063900','7801064070','7801064150','7801064300','7801064515','7801064530','7801064630','7801064660','7801064820','7801064870','7801064885','7801064915','7801064930','7801064980','7801064990','7801065020','7801065225','7801065450','7801065525','7801065750','7801065775','7801065830','7801065860','7801066030','7801066060','7801066750','7801067000','7801067100','7801067630','7801068000','7801068530','7801068560','7801069200','7801069600','7801070260','7801071300','7801071600','7801073655','7801073658','7801074000','7801074400','7801075100','7801075925','7801075950','7801076100','7801076200','7801076300','7801076500','7801076555','7801077100','7801077700','7801077760','7801078000','7801078050','7801078075','7801078100','7801078200','7801078250','7801079500','7801079700','7801079900','7801080400','7801080850','7801081900','7801081925','7801084100','7801084900','7801085075','7801085150','7801085250','7801085550','7801085630','7801085660','7801085820','7801085870','7801085900','7801085965','7801085975','7802000020','7802000060','7802003520','7802006265','7802006280','7802006500','7802007755','7802008300','7802008700','7802008900','7802010750','7802011870','7802013650','7802015500','7802019150','7802021900','7802024420','7802024440','7802024530','7802025350','7802028950','7802029300','7802034775','7802035100','7802035250','7802035750','7802035760','7802035780','7802038500','7802041700','7802043000','7802043400','7802045300','7802046125','7802046200','7802046360','7802046390','7802047230','7802047280','7802047700','7802048110','7802049875','7802053810','7802054550','7802054725','7802054950','7802054996','7802055600','7802056740','7802057200','7802057240','7802057330','7802057770','7802063600','7802064030','7802065085','7802065135','7802065610','7802065650','7802066765','7802068200','7802068650','7802068858','7802070230','7802076430','7802077650','7802078225','7802078230','7802078275','7802080475','7802084850','7802086100','7803000030','7803000080','7803001425','7803003670','7803005520','7803005560','7803006270','7803006600','7803007775','7803007950','7803008060','7803008100','7803008400','7803008450','7803008500','7803008550','7803009000','7803011650','7803012300','7803012400','7803013675','7803014820','7803014830','7803017300','7803023400','7803023435','7803026310','7803026320','7803026330','7803026340','7803026350','7803027850','7803029100','7803030400','7803030550','7803030650','7803031500','7803034700','7803034800','7803035890','7803040300','7803042450','7803042500','7803045550','7803045800','7803046365','7803047100','7803047235','7803047270','7803047950','7803048060','7803048120','7803048160','7803048180','7803048500','7803049750','7803050100','7803050575','7803050800','7803053950','7803054650','7803054825','7803054875','7803054885','7803054925','7803054990','7803054997','7803054998','7803055300','7803055800','7803055850','7803056100','7803056750','7803056910','7803057320','7803058150','7803058400','7803058750','7803058825','7803059200','7803062520','7803062800','7803063700','7803063950','7803064005','7803064010','7803064050','7803065070','7803065125','7803066200','7803066300','7803066900','7803067660','7803067800','7803068545','7803068800','7803072500','7803073550','7803073825','7803073850','7803076400','7803077150','7803077300','7803077715','7803077730','7803077995','7803078150','7803078600','7803080260','7803081975','7803082700','7803085850','7803086200')
MEMBER_SUFFIXES = ('.cpg','.dbf','.prj','.shp','.shp.ea.iso.xml','.shp.iso.xml','.shx')
READY = 'metadata/estate_candidate_ready.json'

class EstateQuarantineError(ValueError):
    """Changed or incomplete source/cache requires review, never replacement."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise EstateQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise EstateQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise EstateQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    if any(path.is_symlink() for path in [root,*root.parents]):
        raise EstateQuarantineError('staging_path_symlink_refused')
    if root.exists() and any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):
        raise EstateQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise EstateQuarantineError('immutable_capture_changed:'+key)


def quarantine(storage,captures,reason):
    key=f'_quarantine/{SLUG}/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,key+'/'+name+'.body',body)
        immutable(storage,key+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,key+'/reason.json',encode({'approved':False,'reason':reason}))
    return key


def processing_identity():
    return packaged_processing_identity(Path(__file__).parents[3])


def validate_receipt(name,body,receipt):
    url=DIRECTORY if name=='census2026_estate' else DIRECTORY+name+'.zip'
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int
        or receipt['status']!=200 or type(receipt.get('bytes')) is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body) or receipt.get('complete_body') is not True):
        raise EstateQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'U.S. Census Bureau','host':'www2.census.gov',
        'role':'producer_publication','canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
        'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],'sha256':digest(body),'bytes':len(body),
        'license':LICENSE,'attribution':'U.S. Census Bureau, TIGER/Line 2026 U.S. Virgin Islands estate publication.',
        'release_date_unknown_reason':'Producer ISO metadata states publication year2026 and a 2026 TIGER/Line citation; HTTP Last-Modified is packaging evidence only.'}


def manifest_for(captures,identity):
    if set(captures)!={'census2026_estate',*PINS}:
        raise EstateQuarantineError('missing_extra_or_incomplete_publication_roster')
    inputs=[validate_receipt(name,*captures[name]) for name in ['census2026_estate',*PINS]]
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],
        'title':'US Virgin Islands Estates — Census TIGER2026 unapproved draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':DIRECTORY,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'decode_the_single_original_shapefile','parameters':{'original_dbf_fields':'unchanged names, physical declarations retained in source audit','source_fid':'OGC_FID is the original file-local GDAL feature index; not a DBF field or replacement published ID','source_archive':'literal original archive stem distinguishes repeated file-local FIDs; not a generated row ID','crs':'EPSG:4269','repair':False,'reproject':False}},
            {'operation':'typed_native_geopackage_and_geoparquet','parameters':{'geometry':'preserve Polygon and MultiPolygon WKB without wrapping','dbf_numeric_fields':'ALAND/AWATER N14,0 exposed/output int64, no float conversion','gpkg_fid':'independent storage index, not a source/published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Additional provenance column and field widths cannot be silently truncated or renamed to DBF. Original complete shapefile remains available as raw source.',
            'pmtiles':'Native coordinates would be reprojected and quantized; no visualization mapping approved.'}}
    validate_release_provenance(manifest)
    return manifest


def acquire(storage,capture_dir=None):
    local_only(storage);captures={}
    try:
        for name in ['census2026_estate',*PINS]:
            if capture_dir is not None:
                body=(Path(capture_dir)/(name+'.body')).read_bytes()
                receipt=strict_json((Path(capture_dir)/(name+'.receipt.json')).read_bytes())
                captures[name]=(body,receipt)
            else:
                url=DIRECTORY if name=='census2026_estate' else DIRECTORY+name+'.zip'
                limit=262144 if name=='census2026_estate' else PINS[name]['bytes']
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
                                    if len(body)>limit: raise EstateQuarantineError('oversized_or_changed_source')
                            finally:
                                receipt['wire_bytes_downloaded']=getattr(response,'num_bytes_downloaded',None)
                            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                            # HTTP Content-Length measures the encoded transport body.
                            # iter_bytes transparently decodes Content-Encoding, even
                            # when the server ignores our identity request.
                            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):
                                raise EstateQuarantineError('non200_or_incomplete_http_response')
                        captures[name]=(bytes(body),receipt);break
                    except (httpx.HTTPError,OSError,ValueError) as exc:
                        receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
                        quarantine(storage,{name:(bytes(body),receipt)},str(exc))
                        if attempt==2: raise EstateQuarantineError('bounded_fetch_exhausted:'+str(exc)) from exc
        return captures
    except (OSError,ValueError,TypeError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise EstateQuarantineError(f'{exc}; retained:{where}') from exc


def dbf_definition(body):
    if len(body)<33: raise EstateQuarantineError('short_dbf_header')
    rows=int.from_bytes(body[4:8],'little');header=int.from_bytes(body[8:10],'little');record=int.from_bytes(body[10:12],'little')
    if header<33 or (header-33)%32 or body[header-1]!=13 or len(body) not in (header+rows*record,header+rows*record+1):
        raise EstateQuarantineError('incomplete_or_changed_dbf_records')
    fields=[]
    for index in range(32,header-1,32):
        field=body[index:index+32]
        fields.append((field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
    if fields!=DBF_FIELDS or record!=1+sum(field[2] for field in fields) or any(body[header+i*record]!=32 for i in range(rows)):
        raise EstateQuarantineError('native_dbf_physical_declarations_or_deleted_rows_drift')
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
                if not re.fullmatch(r'-?\d+',value): raise EstateQuarantineError('native_dbf_numeric_null_or_nonintegral_drift')
                row[name]=int(value)
        rows.append(row)
    return rows


def inspect_archives(paths,captures):
    directory=captures['census2026_estate'][0].decode('utf8')
    listed=re.findall(r'href="(tl_2026_\d{2}_estate.zip)"',directory)
    if len(listed)!=1 or sorted(listed)!=[name+'.zip' for name in PINS]:
        raise EstateQuarantineError('official_directory_completeness_roster_drift')
    tables=[];audit={}
    for name in PINS:
        body=captures[name][0]
        if len(body)!=PINS[name]['bytes'] or digest(body)!=PINS[name]['sha256']:
            raise EstateQuarantineError('changed_truncated_or_unknown_census_publication:'+name)
        with zipfile.ZipFile(paths[name]) as archive:
            infos=archive.infolist();names=[i.filename for i in infos]
            if set(names)!={name+suffix for suffix in MEMBER_SUFFIXES} or len(names)!=7 or archive.testzip() is not None:
                raise EstateQuarantineError('missing_duplicate_or_corrupt_shapefile_member')
            if any(PurePosixPath(i.filename).name!=i.filename or i.flag_bits&1 or stat.S_ISLNK(i.external_attr>>16) for i in infos) or sum(i.file_size for i in infos)>5000000:
                raise EstateQuarantineError('unsafe_archive_member_or_expansion')
            dbf=dbf_definition(archive.read(name+'.dbf'))
            physical_rows=decode_dbf_rows(archive.read(name+'.dbf'))
            if archive.read(name+'.cpg')!=b'UTF-8': raise EstateQuarantineError('encoding_drift')
            xml=ET.fromstring(archive.read(name+'.shp.iso.xml'))
            ns={'gmd':'http://www.isotc211.org/2005/gmd','gco':'http://www.isotc211.org/2005/gco'}
            citations=[]
            for citation in xml.findall('.//gmd:CI_Citation',ns):
                title=citation.findtext('./gmd:title/gco:CharacterString',namespaces=ns)
                if title and title.startswith('TIGER/Line Shapefile, 2026, State,'):
                    citations.append({'title':title,'dates':[{'type':d.find('./gmd:dateType/gmd:CI_DateTypeCode',ns).get('codeListValue'),'value':''.join(d.find('./gmd:date',ns).itertext()).strip()} for d in citation.findall('./gmd:date/gmd:CI_Date',ns)]})
            if len(citations)!=1: raise EstateQuarantineError('producer_citation_missing')
            rights=[' '.join(''.join(x.itertext()).split()) for x in xml.findall('.//gmd:resourceConstraints',ns)]
            if not any('not copyrighted' in text and 'acknowledgement' in text for text in rights): raise EstateQuarantineError('producer_rights_evidence_missing')
            inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}','sha256':digest(archive.read(i.filename))} for i in infos}
        meta,table=pyogrio.read_arrow('/vsizip/{'+str(paths[name])+'}/'+name+'.shp',return_fids=True)
        if (tuple(table.column_names)!=SOURCE_FIELDS or tuple(f.type for f in table.schema)!=TYPES[1:] or meta['crs']!='EPSG:4269'
            or meta['fid_column']!='OGC_FID' or meta['geometry_type']!='Polygon' or table.num_rows!=dbf['row_count']):
            raise EstateQuarantineError('source_schema_types_crs_geometry_or_dbf_row_completeness_drift')
        if table['OGC_FID'].to_pylist()!=list(range(table.num_rows)) or any(v!='78' for v in table['STATEFP'].to_pylist()):
            raise EstateQuarantineError('file_local_fid_or_state_coverage_drift')
        if [{k:r[k] for k,_,_,_ in DBF_FIELDS} for r in table.to_pylist()]!=physical_rows:
            raise EstateQuarantineError('driver_attribute_values_disagree_with_independent_physical_dbf_decode')
        table=table.add_column(0,'source_archive',pa.array([name]*table.num_rows))
        tables.append(table)
        audit[name]={'dbf':dbf,'members':inventory,'producer_citation':citations[0],'producer_resource_constraints':rights,'driver_arrow_schema':str(table.schema),'full_source_GEOIDs':table['GEOID'].to_pylist()}
    combined=pa.concat_tables(tables)
    validate_table(combined)
    return combined,audit


def validate_table(table):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=416 or any(table[n].null_count for n in FIELDS):
        raise EstateQuarantineError('native_schema_physical_types_nullability_or_completeness_drift')
    if sorted(table['GEOID'].to_pylist())!=list(GEOIDS): raise EstateQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    rows=table.to_pylist()
    if any(r['GEOID']!=r['STATEFP']+r['COUNTYFP']+r['ESTATEFP'] or r['GEOIDFQ']!='06V0000US'+r['GEOID'] for r in rows):
        raise EstateQuarantineError('source_identifier_value_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if not all(shapely.is_valid(geoms)) or any(shapely.is_empty(geoms)) or sorted(shapely.get_type_id(geoms).tolist())!=[3]*410+[6]*6:
        raise EstateQuarantineError('missing_invalid_or_changed_native_geometry')


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{'encoding':'WKB','geometry_types':['Polygon','MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_native_gpkg(table,path):
    with with_geopackage_creation_time('2026-01-01T00:00:00.000Z'):
        pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='wkb_geometry',geometry_type='Unknown',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path) as db: db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table):
    expected=expected_parquet_table(table)
    parquet=pq.read_table(root/'candidate/typed-source.parquet')
    if not parquet.equals(expected,check_metadata=True): raise EstateQuarantineError('cached_parquet_values_types_or_metadata_disagree_with_fresh_original')
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if gm['crs']!='EPSG:4269' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():
        raise EstateQuarantineError('cached_geopackage_values_types_or_wkb_disagree_with_fresh_original')
    with tempfile.TemporaryDirectory(prefix='estate-canonical-gpkg-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise EstateQuarantineError('cached_geopackage_full_container_disagrees_with_fresh_original')


def source_payloads(captures,identity,table,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/estate_source_audit.json':encode({'approved':False,'feature_count':416,'crs':'EPSG:4269','invalid_geometries':0,'polygons':410,'multipolygons':6,
            'archive_count':1,'original_publications':audit,'full_values_ids_physical_types_native_wkb_roundtrip':True,
            'source_relationship':'Census TIGER2026 producer publication of the retired HIFLD estates layer (crosswalk row points Census TIGERweb); TIGERweb layer 0 reports the same 416 estates; byte equivalence to the retired HIFLD release is unverified and all schema/ID/type/geometry mappings remain unapproved.'}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_schema_id_geometry_mapping_not_approved']})}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,table,audit,captures,identity):
    root=Path(storage.local_dir)/prefix;ready=strict_json((root/READY).read_bytes())
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in ['census2026_estate',*PINS]}
    if any(cached[name][0]!=captures[name][0] for name in PINS): raise EstateQuarantineError('cached_original_publication_changed')
    manifest=manifest_for(cached,identity)
    actual_manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(actual_manifest)
    for key,value in manifest.items():
        if key!='code_commit' and actual_manifest.get(key)!=value: raise EstateQuarantineError('cached_provenance_disagrees_with_original')
    pin_key=f'_source-provenance/{SLUG}/{digest(encode(identity))}/bundle.json'
    pinned=strict_json(storage.read_key(pin_key))
    if actual_manifest!=pinned['manifest']:raise EstateQuarantineError('cached_provenance_disagrees_with_immutable_original_bundle')
    manifest=actual_manifest
    expected=source_payloads(cached,identity,table,audit,manifest)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1] or ready.get('approved') is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready['objects'])!=expected_names):
        raise EstateQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if (root/name).read_bytes()!=value: raise EstateQuarantineError('cached_source_owned_bytes_drift:'+name)
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    snapshot='metadata/source/version_'+digest((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    if actual-(expected_names|{READY}|derivatives) or not (expected_names|{READY}).issubset(actual):
        raise EstateQuarantineError('cached_unknown_or_missing_object')
    try:
        validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc:
        raise EstateQuarantineError('cached_staged_release_documents_invalid') from exc
    for name,pin in ready['objects'].items():
        if sha256_file(root/name)!=pin: raise EstateQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists() and (root/name).read_bytes()!=(root/'metadata/source_manifest.json').read_bytes(): raise EstateQuarantineError('cached_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs'] or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved:
            raise EstateQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table)



def original_provenance_bundle(storage,fingerprint,captures,identity,manifest,*,create=False):
    """Pin first construction provenance outside the candidate's self-pinned cache."""
    key=f'_source-provenance/{SLUG}/{fingerprint}/bundle.json'
    if not storage.object_exists(key):
        if not create: raise EstateQuarantineError('missing_original_construction_bundle')
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        bundle={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt} for name,(body,receipt) in captures.items()}}
        immutable(storage,key,encode(bundle))
    bundle=strict_json(storage.read_key(key))
    if set(bundle)!= {'manifest','captures'} or set(bundle['captures'])!={'census2026_estate',*PINS}:
        raise EstateQuarantineError('original_provenance_bundle_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        receipt=item['receipt'];expected=f"_raw-captures/{SLUG}/{receipt['sha256']}/{name}.body"
        if item['body_key']!=expected:raise EstateQuarantineError('original_provenance_bundle_body_key_drift')
        original[name]=(storage.read_key(expected),receipt)
    expected_manifest=manifest_for(original,identity)
    for name,value in expected_manifest.items():
        if name!='code_commit' and bundle['manifest'].get(name)!=value:
            raise EstateQuarantineError('original_provenance_bundle_manifest_drift')
    validate_release_provenance(bundle['manifest'])
    validate_construction_code_identity(Path(__file__).parents[3],bundle['manifest']['code_commit'],identity['files'],identity=identity)
    return original,bundle['manifest']


def stage_us_virgin_islands_estates(storage,*,capture_dir=None):
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
                try:
                    captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest)
                    reconcile_cache(storage,prefix,table,audit,captures,identity)
                except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise EstateQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':416,'reused':True}
            if storage.object_exists(f'_source-provenance/{SLUG}/{fingerprint}/bundle.json'):
                captures,manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest)
            else:
                validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
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
                (temp/READY).write_bytes(encode(ready))
                original_captures,original_manifest=original_provenance_bundle(storage,fingerprint,captures,identity,manifest,create=True)
                if original_manifest!=manifest: raise EstateQuarantineError('concurrent_original_manifest_changed')
                root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':416,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise EstateQuarantineError(f'{exc}; retained:{where}') from exc
