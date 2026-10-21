"""Complete exact Census 2024 County_Subdivision; isolated unapproved draft.

The original ZIP is preserved either in the owned depot or an explicitly pinned,
read-only external capture depot. No source identifier or geometry is replaced.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
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
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import processing_identity as packaged_processing_identity
from dagster_hifld.validation import sha256_file

SLUG='county-subdivisions-1'
LAYER='County_Subdivision'
GDB='tlgdb_2024_a_us_substategeo.gdb'
URL='https://www2.census.gov/geo/tiger/TGRGDB24/'+GDB+'.zip'
SHA256='e2409cd4ddb1a9302b130096fbcf9925ca3a701f720b8991045602bcb99d6b35'
BYTES=736048475
COUNT=36492
LICENSE='https://www.census.gov/about/policies/quality/guidelines/copyright.html'
RAW_KEY=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
READY='metadata/cousub_candidate_ready.json'
FIELDS=('OBJECTID','COUSUBNS','GEOID','GEOIDFQ','NAMELSAD','CLASSFP','FUNCSTAT','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
TYPES=(pa.int32(),*[pa.string()]*6,pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())
MAX_RECEIPT_BYTES=65536
LIVE_BLOCK_BYTES=1024*1024
BATCH_SIZE=512
STATE_COUNTS={'01': 390, '02': 37, '04': 80, '05': 1095, '06': 404, '08': 209, '09': 174, '10': 27, '11': 1, '12': 316, '13': 586, '15': 44, '16': 170, '17': 1696, '18': 1012, '19': 1663, '20': 1525, '21': 493, '22': 576, '23': 529, '24': 290, '25': 357, '26': 1580, '27': 2759, '28': 410, '29': 1394, '30': 194, '31': 1182, '32': 71, '33': 260, '34': 569, '35': 130, '36': 1023, '37': 1041, '38': 1761, '39': 1607, '40': 305, '41': 212, '42': 2573, '44': 40, '45': 299, '46': 1328, '47': 844, '48': 862, '49': 93, '50': 256, '51': 552, '53': 242, '54': 225, '55': 1925, '56': 71, '60': 16, '66': 20, '69': 12, '72': 939, '78': 23}

class CountySubdivisionQuarantineError(ValueError):
    """Changed or incomplete inputs/caches require review and cannot promote."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body):return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise CountySubdivisionQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite_json')))
    if type(value) is not dict:raise CountySubdivisionQuarantineError('json_object_required')
    return value

def unaliased(path,*,file=False):
    path=Path(path)
    if '..' in path.parts or any(p.is_symlink() for p in [path,*path.parents]):raise CountySubdivisionQuarantineError('root_ancestor_or_object_alias')
    if file and (not path.is_file() or path.stat().st_nlink!=1):raise CountySubdivisionQuarantineError('capture_not_unique_regular_file')
    return path.absolute()

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:raise CountySubdivisionQuarantineError('explicit_unprefixed_local_staging_required')
    root=unaliased(storage.local_dir);root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise CountySubdivisionQuarantineError('linked_staging_object')
    return root

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise CountySubdivisionQuarantineError('immutable_capture_changed:'+key)

def validate_receipt(path,receipt):
    unaliased(path,file=True)
    if (receipt.get('url')!=URL or receipt.get('final_url')!=URL or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or type(receipt.get('bytes')) is not int or receipt['bytes']!=BYTES
        or receipt.get('sha256')!=SHA256 or path.stat().st_size!=BYTES or sha256_file(path)!=SHA256):raise CountySubdivisionQuarantineError('source_url_status_completeness_length_or_hash_drift')
    return {'input_id':'census2024-substategeo:'+SHA256,'producer':'U.S. Census Bureau','host':'www2.census.gov','role':'producer_publication',
        'canonical_url':URL,'retrieval_url':URL,'observed_final_url':URL,'retrieved_at':receipt.get('observed_at'),'http_status':200,'sha256':SHA256,'bytes':BYTES,
        'license':LICENSE,'attribution':'U.S. Census Bureau, TIGER/Line 2024 national substate geographic geodatabase, County_Subdivision.',
        'release_date_unknown_reason':'Producer edition2024. HTTP Last-Modified2024-08-08 is packaging evidence; an exact independently established publication day remains unresolved.'}

def observe_external_receipt(storage,archive_path,receipt_path):
    """Retain bounded exact consumed receipt bytes BEFORE trusting any claims."""
    path=unaliased(archive_path,file=True);receipt_file=unaliased(receipt_path,file=True)
    descriptor=os.open(receipt_file,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1:raise CountySubdivisionQuarantineError('receipt_regular_unlinked_descriptor_required')
        raw=stream.read(MAX_RECEIPT_BYTES+1);after=os.fstat(stream.fileno())
    fields=('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')
    stable=all(getattr(before,n)==getattr(after,n) for n in fields)
    diagnostic={'classification':'UNTRUSTED external capture/receipt observation; no HTTP/source validity or completeness claim',
        'archive_path':str(path),'archive_bytes_reported_by_stat':path.stat().st_size,
        'receipt_path':str(receipt_file),'receipt_bytes_reported_by_stat':before.st_size,'receipt_read_limit':MAX_RECEIPT_BYTES,
        'receipt_consumed_bytes':len(raw),'receipt_consumed_sha256':digest(raw),'receipt_descriptor_stable':stable,
        'complete_receipt_descriptor_read':stable and before.st_size<=MAX_RECEIPT_BYTES and len(raw)==before.st_size}
    key=f'_raw-untrusted-observations/{SLUG}/{digest(encode(diagnostic))}'
    immutable(storage,key+'/receipt.bin',raw);immutable(storage,key+'/observation.json',encode(diagnostic))
    try:
        if not diagnostic['complete_receipt_descriptor_read']:raise CountySubdivisionQuarantineError('external_receipt_oversized_short_or_changed')
        receipt=strict_json(raw);validate_receipt(path,receipt)
    except (ValueError,TypeError,KeyError,OSError) as exc:
        immutable(storage,key+'/rejection.json',encode({'approved':False,'error_type':type(exc).__name__,'reason':str(exc)[:512],'source_claims_validated':False}))
        raise CountySubdivisionQuarantineError('external_capture_receipt_rejected_after_observation:'+type(exc).__name__) from exc
    return path,receipt

def acquire(storage,archive_path=None,receipt_path=None):
    root=local_only(storage)
    if archive_path is not None:
        # Deliberately explicit read-only reference to a preserved complete raw
        # depot, NOT a copied staging object or claim of a new live observation.
        path,receipt=observe_external_receipt(storage,archive_path,receipt_path)
        pointer={'kind':'external_preserved_read_only_capture','absolute_path':str(path),'sha256':SHA256,'bytes':BYTES,'ownership':'caller/coordinator preserved original; adapter never writes or removes this file'}
    else:
        path=root/RAW_KEY;path.parent.mkdir(parents=True,exist_ok=True)
        if shutil.disk_usage(root).free<BYTES+300000000:raise CountySubdivisionQuarantineError('insufficient_space_for_lossless_live_capture')
        partial=path.parent/('capture-'+uuid4().hex+'.partial');receipt={'url':URL,'observed_at':datetime.now(timezone.utc).isoformat(),'complete_body':False}
        try:
            with httpx.Client(follow_redirects=False,timeout=180,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',URL) as response,partial.open('xb') as output:
                receipt.update(status=response.status_code,final_url=str(response.url),content_length=response.headers.get('content-length'),content_encoding=response.headers.get('content-encoding'),requested_accept_encoding='identity')
                size=0
                try:
                    blocks=iter(response.iter_bytes(LIVE_BLOCK_BYTES))
                    while True:
                        # Reserve the complete bounded decoded block BEFORE a
                        # network read. Every consumed block is then retained,
                        # including at most one block beyond the pinned size.
                        if shutil.disk_usage(root).free<300000000+LIVE_BLOCK_BYTES:raise CountySubdivisionQuarantineError('oversized_source_or_storage_floor')
                        try:block=next(blocks)
                        except StopIteration:break
                        output.write(block)
                        size+=len(block)
                        if size>BYTES:raise CountySubdivisionQuarantineError('oversized_source_or_storage_floor')
                finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
            receipt.update(bytes=partial.stat().st_size,sha256=sha256_file(partial),complete_body=True)
            if receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']:raise CountySubdivisionQuarantineError('encoded_transport_truncation')
            validate_receipt(partial,receipt)
            if path.exists():
                validate_receipt(path,receipt);partial.unlink() # verified identical refresh body only
            else:os.replace(partial,path)
        except (OSError,ValueError,httpx.HTTPError) as exc:
            receipt.update(complete_body=False,reason=str(exc),bytes=partial.stat().st_size if partial.exists() else 0,sha256=sha256_file(partial) if partial.exists() else None)
            immutable(storage,f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}.json',encode({'receipt':receipt,'preserved_partial_path':str(partial)}))
            raise CountySubdivisionQuarantineError('live_capture_failed_partial_retained:'+str(exc)) from exc
        pointer={'kind':'owned_content_addressed_capture','key':RAW_KEY,'sha256':SHA256,'bytes':BYTES}
    immutable(storage,f'_raw-capture-references/{SLUG}/{SHA256}/receipt-{digest(encode(receipt))}.json',encode({'capture':pointer,'receipt':receipt}))
    return path,receipt,pointer

def processing_identity():
    identity=packaged_processing_identity(Path(__file__).parents[3])
    # GDAL is a native decoding dependency not included in the shared runtime
    # version tuple; pin its build alongside the package identity.
    identity['pyogrio_gdal']=str(pyogrio.__gdal_version_string__)
    return identity

def uri(path):return '/vsizip/'+str(path)+'/'+GDB

def native_definition(path):
    with zipfile.ZipFile(path) as z:
        items=z.infolist()
        if len(items)!=84 or len({i.filename for i in items})!=84 or sum(i.file_size for i in items)!=1050730729:raise CountySubdivisionQuarantineError('complete_archive_inventory_drift')
        for item in items:
            parts=PurePosixPath(item.filename)
            if parts.is_absolute() or '..' in parts.parts or len(parts.parts)!=2 or parts.parts[0]!=GDB or item.flag_bits&1 or stat.S_ISLNK(item.external_attr>>16):raise CountySubdivisionQuarantineError('unsafe_archive_member')
        members={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in items}
    _,t=pyogrio.read_arrow(uri(path),sql='GetLayerDefinition '+LAYER)
    if t.num_rows!=1:raise CountySubdivisionQuarantineError('missing_native_definition')
    xml=t['FIELD_1'][0].as_py();root=ET.fromstring(xml);fields={f.findtext('Name'):{n.tag:n.text for n in f if n.tag!='Name'} for f in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    if set(fields)!=set(FIELDS) or root.findtext('OIDFieldName')!='OBJECTID' or root.findtext('HasZ')!='false' or root.findtext('HasM')!='false':raise CountySubdivisionQuarantineError('native_schema_or_geometry_declaration_drift')
    if fields['OBJECTID'].get('FieldType')!='esriFieldTypeOID' or fields['OBJECTID'].get('Length')!='4':raise CountySubdivisionQuarantineError('native_oid_width_drift')
    for n in ('ALAND','AWATER'):
        if fields[n].get('FieldType')!='esriFieldTypeDouble' or fields[n].get('Length')!='8':raise CountySubdivisionQuarantineError('native_area_type_drift')
    return {'members':members,'native_layer_definition':xml,'physical_fields':fields}

@contextmanager
def source_reader(path):
    with pyogrio.open_arrow(uri(path),layer=LAYER,return_fids=True,use_pyarrow=True,batch_size=BATCH_SIZE) as (meta,reader):
        if meta['crs']!='EPSG:4269' or meta['geometry_type']!='MultiPolygon' or meta['fid_column']!='OBJECTID':raise CountySubdivisionQuarantineError('source_crs_geometry_or_identifier_drift')
        yield meta,reader

def native_batch(batch):
    source_types=(pa.int64(),*TYPES[1:])
    if tuple(batch.schema.names)!=FIELDS or tuple(f.type for f in batch.schema)!=source_types:raise CountySubdivisionQuarantineError('source_columns_physical_types_or_order_drift')
    # FileGDB declares OID Length4. GDAL's generic int64 FID transport is safely
    # narrowed to its actual source physical int32 width without renumbering.
    return batch.set_column(0,pa.field('OBJECTID',pa.int32(),nullable=False),batch.column(0).cast(pa.int32(),safe=True))

def scan(reader,*,check_source=False):
    count=0;states={};classes={};functions={};nulls={n:0 for n in FIELDS};ids=set();geoids=set();h=hashlib.sha256();wkb_bytes=0;invalid=empty=0
    for batch in reader:
        if check_source:batch=native_batch(batch)
        if tuple(batch.schema.names)!=FIELDS or tuple(f.type for f in batch.schema)!=TYPES:raise CountySubdivisionQuarantineError('native_output_schema_or_types_changed')
        rows=batch.to_pylist()
        for r in rows:
            count+=1
            if r['OBJECTID']!=count or r['OBJECTID'] in ids or r['GEOID'] in geoids:raise CountySubdivisionQuarantineError('missing_duplicate_or_renumbered_source_id')
            ids.add(r['OBJECTID']);geoids.add(r['GEOID'])
            if not re.fullmatch(r'\d{10}',r['GEOID']) or not re.fullmatch(r'\d{8}',r['COUSUBNS']) or r['GEOIDFQ']!='0600000US'+r['GEOID']:raise CountySubdivisionQuarantineError('source_literal_identifier_drift')
            for n,v in r.items():nulls[n]+=v is None
            if r['SHAPE'] is None:raise CountySubdivisionQuarantineError('missing_native_geometry')
            wkb_bytes+=len(r['SHAPE']);r['SHAPE']=r['SHAPE'].hex();h.update(encode(r))
            state=r['GEOID'][:2];states[state]=states.get(state,0)+1
            for field,values in [('CLASSFP',classes),('FUNCSTAT',functions)]:values[r[field]]=values.get(r[field],0)+1
        geom=shapely.from_wkb(batch['SHAPE'].to_pylist());invalid+=int((~shapely.is_valid(geom)).sum());empty+=int(shapely.is_empty(geom).sum())
        if not all(shapely.get_type_id(geom)==6):raise CountySubdivisionQuarantineError('native_geometry_type_drift')
    if count!=COUNT or states!=STATE_COUNTS or any(nulls.values()) or invalid or empty:raise CountySubdivisionQuarantineError('incomplete_null_invalid_or_empty_population')
    return {'rows':count,'native_attributes':len(FIELDS)-1,'state_counts':states,'class_counts':classes,'function_counts':functions,'nulls':nulls,'records_sha256':h.hexdigest(),'native_wkb_bytes':wkb_bytes,'invalid_geometry_count':invalid,'empty_geometry_count':empty,'approved':False}

def source_audit(path):
    audit=native_definition(path)
    with source_reader(path) as (_,reader):audit.update(scan(reader,check_source=True))
    return audit

def geo_metadata():
    return {b'geo':encode({'version':'1.1.0','primary_column':'SHAPE','columns':{'SHAPE':{'encoding':'WKB','geometry_types':['MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict()}}})}

def write_parquet(path,out):
    with source_reader(path) as (_,reader):
        writer=None
        try:
            for batch in reader:
                batch=native_batch(batch);schema=batch.schema.with_metadata(geo_metadata())
                if writer is None:writer=pq.ParquetWriter(out,schema,compression='zstd')
                writer.write_batch(batch.replace_schema_metadata(geo_metadata()))
        finally:
            if writer is not None:writer.close()

def write_gpkg(path,out):
    with source_reader(path) as (_,reader):
        schema=reader.schema.set(0,pa.field('OBJECTID',pa.int32(),nullable=False))
        converted=pa.RecordBatchReader.from_batches(schema,(native_batch(batch) for batch in reader))
        pyogrio.write_arrow(converted,out,layer=LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'SHAPE'})
    with sqlite3.connect(out) as db:db.execute("UPDATE gpkg_contents SET last_change='2024-01-01T00:00:00.000Z'")

def stable_sqlite_sha256(path):
    """Hash SQLite content while ignoring only its two volatile change counters.

    SQLite's database-change counter (header bytes 24..27) and the matching
    version-valid-for value (bytes 92..95) can differ after equivalent writes.
    The remaining container bytes, including all GeoPackage SQL/SRS/RTree and
    feature data, remain part of the exact digest.
    """
    h=hashlib.sha256()
    with open(path,'rb') as stream:
        header=stream.read(100)
        if len(header)!=100 or header[:16]!=b'SQLite format 3\x00':
            raise CountySubdivisionQuarantineError('invalid_sqlite_container_header')
        page_size=int.from_bytes(header[16:18],'big')
        if page_size==1:page_size=65536
        if page_size<512 or page_size>65536 or page_size&(page_size-1) or os.fstat(stream.fileno()).st_size%page_size:
            raise CountySubdivisionQuarantineError('invalid_sqlite_container_page_layout')
        h.update(header[:24]);h.update(b'\x00'*4);h.update(header[28:92]);h.update(b'\x00'*4);h.update(header[96:100])
        while True:
            chunk=stream.read(1024*1024)
            if not chunk:break
            h.update(chunk)
    return h.hexdigest()

def verify_outputs(root,path,audit,*,canonical=True):
    parquet=root/'geoparquet'/f'{SLUG}.parquet';f=pq.ParquetFile(parquet)
    if f.schema_arrow.metadata!=geo_metadata():raise CountySubdivisionQuarantineError('native_parquet_metadata_drift')
    if scan(f.iter_batches(batch_size=BATCH_SIZE))!={k:v for k,v in audit.items() if k not in ('members','native_layer_definition','physical_fields')}:raise CountySubdivisionQuarantineError('parquet_all_values_ids_nulls_types_or_wkb_drift')
    with pyogrio.open_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER,use_pyarrow=True,batch_size=BATCH_SIZE) as (meta,reader):
        if meta['crs']!='EPSG:4269' or meta['geometry_type']!='MultiPolygon':raise CountySubdivisionQuarantineError('gpkg_crs_geometry_drift')
        if scan(reader)!={k:v for k,v in audit.items() if k not in ('members','native_layer_definition','physical_fields')}:raise CountySubdivisionQuarantineError('gpkg_all_values_ids_types_or_wkb_drift')
    if canonical:
        with tempfile.TemporaryDirectory(prefix='.cousub-canonical-',dir=root.parent) as tmp:
            fresh=Path(tmp)/'fresh.gpkg';write_gpkg(path,fresh)
            if stable_sqlite_sha256(fresh)!=stable_sqlite_sha256(root/'geopackage'/f'{SLUG}.gpkg'):raise CountySubdivisionQuarantineError('full_gpkg_native_container_srs_sql_or_rtree_drift')

def manifest_for(path,receipt,identity,pin):
    m={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'title':'County Subdivisions — exact Census2024 working draft','publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':URL,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(path,receipt)],
        'transformations':[{'operation':'extract_complete_native_FileGDB_layer','parameters':{'layer':LAYER,'all_source_identifiers':'preserved without renumbering','OID':'physical esriFieldTypeOID Length4 int32, safe decode from GDAL int64 transport','geometry':'native EPSG4269 MultiPolygon exact WKB; no repair/reprojection/simplification','areas':'native Double Float64 retained'}}],
        'unsupported_output_formats':{'shapefile_zip':'No accepted DBF schema/ID/geometry mapping. Original complete FileGDB is retained.','pmtiles':'No accepted reprojection/quantization or visualization mapping.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve native source physical types and exact geometry. No schema/bbox additions or release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**pin}]}}}}
    validate_output_policy(m);validate_release_provenance(m);return m

@contextmanager
def candidate_lock(root,prefix):
    target=root/prefix;target.parent.mkdir(parents=True,exist_ok=True);fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as stream:
        if os.fstat(stream.fileno()).st_nlink!=1:raise CountySubdivisionQuarantineError('linked_lock')
        fcntl.flock(stream,fcntl.LOCK_EX);yield target

def original_bundle(storage,key,version,path,identity,pin,*,create=None):
    fp=digest(encode(identity))
    if create is None:body=load_first_construction(storage,key,version,fp)
    else:
        receipt,pointer,manifest=create
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        body=seal_first_construction(storage,key,version,fp,canonical_construction_bundle(version,fp,{'receipt':receipt,'capture':pointer,'manifest':manifest}))
    payload=strict_json(body)['payload']
    if set(payload)!={'receipt','capture','manifest'}:raise CountySubdivisionQuarantineError('construction_bundle_roster_drift')
    pointer=payload['capture']
    if pointer.get('sha256')!=SHA256 or pointer.get('bytes')!=BYTES:raise CountySubdivisionQuarantineError('original_capture_binding_drift')
    original_path=unaliased(pointer['absolute_path'],file=True) if pointer.get('kind')=='external_preserved_read_only_capture' else Path(storage.local_dir)/RAW_KEY
    if pointer.get('kind') not in ('external_preserved_read_only_capture','owned_content_addressed_capture') or (pointer.get('kind')=='owned_content_addressed_capture' and pointer.get('key')!=RAW_KEY):raise CountySubdivisionQuarantineError('original_capture_pointer_drift')
    expected=manifest_for(original_path,payload['receipt'],identity,pin);actual=payload['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity)
    if encode({k:v for k,v in actual.items() if k!='code_commit'})!=encode({k:v for k,v in expected.items() if k!='code_commit'}):raise CountySubdivisionQuarantineError('original_manifest_receipt_or_processing_drift')
    return payload['receipt'],pointer,actual

def source_payloads(receipt,pointer,identity,audit,manifest):
    proof={'approved':False,'status':'pass','rows':COUNT,'native_record_hash':audit['records_sha256'],'all_native_values_types_ids_nulls_wkb':'full-population exact, bounded streaming; complete native GeoPackage container canonical byte check'}
    return {'raw/source_capture.json':encode(pointer),'raw/retrieval.json':encode(receipt),'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/cousub_source_audit.json':encode(audit),
        'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_ID_schema_type_geometry_coverage_contract_unapproved']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}

def reconcile_cache(storage,target,version,path,audit,identity,pin,original):
    receipt,pointer,manifest=original;expected=source_payloads(receipt,pointer,identity,audit,manifest);names={*expected,f'geopackage/{SLUG}.gpkg',f'geoparquet/{SLUG}.parquet'};ready=strict_json((target/READY).read_bytes())
    if set(ready)!={'version','approved','processing_fingerprint','source_sha256','objects'} or ready.get('source_sha256')!=SHA256 or ready.get('version')!=version or ready.get('approved') is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise CountySubdivisionQuarantineError('ready_identity_roster_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json';derived={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(target)) for p in target.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derived) or not (names|{READY}).issubset(actual):raise CountySubdivisionQuarantineError('unknown_or_missing_candidate_object')
    for n,b in expected.items():
        if (target/n).read_bytes()!=b:raise CountySubdivisionQuarantineError('source_owned_payload_or_wrapper_drift:'+n)
    for n,h in ready['objects'].items():
        if sha256_file(target/n)!=h:raise CountySubdivisionQuarantineError('candidate_object_bytes_drift:'+n)
    for n in ['metadata/source/source_manifest.json',snapshot]:
        if (target/n).exists() and (target/n).read_bytes()!=encode(manifest):raise CountySubdivisionQuarantineError('source_snapshot_drift')
    if (target/'metadata/resolved_provenance.json').exists() and strict_json((target/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise CountySubdivisionQuarantineError('resolved_provenance_drift')
    verify_outputs(target,path,audit)

def stage_county_subdivisions_candidate(storage,*,archive_path=None,receipt_path=None):
    root=local_only(storage);path,receipt,pointer=acquire(storage,archive_path,receipt_path);audit=source_audit(path);identity=processing_identity();fp=digest(encode(identity));version=f'v0.0.0-draft-census2024-{SHA256[:16]}-{fp[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';key=f'_construction-state/{SLUG}/{version}.json';incident=f'_construction-incidents/{SLUG}/{version}.json'
    with candidate_lock(root,prefix) as target:
        if target.exists():
            try:
                with tempfile.TemporaryDirectory(prefix='.cousub-pin-',dir=target.parent) as tmp:
                    fresh=Path(tmp)/'fresh.parquet';write_parquet(path,fresh);pin={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}
                original=original_bundle(storage,key,version,path,identity,pin);reconcile_cache(storage,target,version,path,audit,identity,pin,original)
            except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error) as exc:
                destination=root/'_quarantine'/SLUG/('invalid-'+uuid4().hex);destination.parent.mkdir(parents=True,exist_ok=True);os.rename(target,destination)
                immutable(storage,incident,encode({'version':version,'reason':'candidate_trust_or_fidelity_incident_requires_original_state'}))
                immutable(storage,str(destination.relative_to(root))+'.reason.json',encode({'reason':str(exc),'approved':False,'original_prefix':prefix}));raise CountySubdivisionQuarantineError('whole_candidate_detached_intact:'+str(exc)) from exc
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'reused':True}
        # Existing outside evidence/incident is checked before expensive outputs;
        # the shared seal refuses all partial-state bootstrap on subsequent use.
        state_path=root/key
        if (storage.object_exists(incident) or state_path.exists() or Path(str(state_path)+'.first-construction.json').exists() or Path(str(state_path)+'.evidence').exists()):load_first_construction(storage,key,version,fp)
        with tempfile.TemporaryDirectory(prefix='.cousub-building-',dir=target.parent) as tmp:
            private=Path(tmp)
            for part in ('raw','metadata','geopackage','geoparquet'):(private/part).mkdir()
            if shutil.disk_usage(root).free<300000000+audit['native_wkb_bytes']*3:raise CountySubdivisionQuarantineError('insufficient_space_for_bounded_native_candidate')
            write_gpkg(path,private/'geopackage'/f'{SLUG}.gpkg');parquet=private/'geoparquet'/f'{SLUG}.parquet';write_parquet(path,parquet);verify_outputs(private,path,audit)
            pin={'sha256':sha256_file(parquet),'bytes':parquet.stat().st_size};manifest=manifest_for(path,receipt,identity,pin)
            # Complete every source-owned report and ready manifest BEFORE the
            # first durable seal. A pre-seal metadata fault creates no trust.
            proposed=source_payloads(receipt,pointer,identity,audit,manifest)
            for n,b in proposed.items():(private/n).write_bytes(b)
            ready={'version':version,'approved':False,'processing_fingerprint':fp,'source_sha256':SHA256,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
            (private/READY).write_bytes(encode(ready))
            original=original_bundle(storage,key,version,path,identity,pin,create=(receipt,pointer,manifest));old_receipt,old_pointer,manifest=original
            # Existing trusted construction keeps its ORIGINAL receipts/HEAD;
            # current observations remain separately retained in the raw depot.
            for n,b in source_payloads(old_receipt,old_pointer,identity,audit,manifest).items():(private/n).write_bytes(b)
            ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY}
            (private/READY).write_bytes(encode(ready));os.rename(private,target)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'reused':False}
