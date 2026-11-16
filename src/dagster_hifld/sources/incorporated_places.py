"""Exact Census 2024 substate-GDB Incorporated Places; unapproved draft.

Incorporated places are the legal city/town/village/borough entities reported
through the Boundary and Annexation Survey. This adapter reconstructs the
complete original Incorporated_Place layer from the same pinned producer
FileGDB publication (TIGER 2024 national substate geographic database) without
replacing any source identifier, physical declaration or native coordinate.
The historical edition is explicitly pinned; a new annual release needs a new
reviewed pin/contract. Repeated observations of this edition never assert new
boundaries, and CDP/consolidated-city layers are never included here.
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
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as shared_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.validation import sha256_file

SLUG='incorporated-places'
LAYER='Incorporated_Place'
GDB='tlgdb_2024_a_us_substategeo.gdb'
URL='https://www2.census.gov/geo/tiger/TGRGDB24/'+GDB+'.zip'
SHA256='e2409cd4ddb1a9302b130096fbcf9925ca3a701f720b8991045602bcb99d6b35'
BYTES=736048475
COUNT=19728
LICENSE='https://www.census.gov/about/policies/quality/guidelines/copyright.html'
RAW_KEY=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
READY='metadata/incplace_candidate_ready.json'
FIELDS=('OBJECTID','PLACENS','GEOID','GEOIDFQ','NAMELSAD','CLASSFP','FUNCSTAT','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
TYPES=(pa.int64(),*[pa.string()]*6,pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())
STATE_COUNTS={'01': 463, '02': 149, '04': 91, '05': 500, '06': 482, '08': 272, '09': 30, '10': 57, '11': 1, '12': 411, '13': 537, '16': 200, '17': 1294, '18': 566, '19': 940, '20': 626, '21': 419, '22': 304, '23': 23, '24': 157, '25': 58, '26': 533, '27': 855, '28': 300, '29': 948, '30': 127, '31': 528, '32': 19, '33': 13, '34': 323, '35': 105, '36': 595, '37': 552, '38': 355, '39': 925, '40': 599, '41': 241, '42': 1013, '44': 8, '45': 271, '46': 310, '47': 345, '48': 1228, '49': 255, '50': 40, '51': 227, '53': 281, '54': 231, '55': 607, '56': 99, '60': 77, '69': 135, '78': 3}
CLASSFP_CODES=('C1','C2','C5','C6','C7','C8','C9')
FUNCSTAT_CODES=('A','B','F','I','N')

class IncorporatedPlaceQuarantineError(ValueError):
    """Changed source, incomplete input or untrusted cache; no implicit acceptance."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise IncorporatedPlaceQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(IncorporatedPlaceQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise IncorporatedPlaceQuarantineError('json_object_required')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise IncorporatedPlaceQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir)
    if storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise IncorporatedPlaceQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise IncorporatedPlaceQuarantineError('linked_staging_object')

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise IncorporatedPlaceQuarantineError('immutable_capture_changed:'+key)

def validate_receipt(path,receipt):
    if (receipt.get('url')!=URL or receipt.get('final_url')!=URL or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or type(receipt.get('bytes')) is not int or receipt['bytes']!=BYTES
        or receipt.get('sha256')!=SHA256 or path.stat().st_size!=BYTES or sha256_file(path)!=SHA256):
        raise IncorporatedPlaceQuarantineError('original_url_status_complete_length_or_hash_drift')
    return {'input_id':'census2024-substategeo:'+SHA256,'producer':'U.S. Census Bureau','host':'www2.census.gov',
        'role':'producer_publication','canonical_url':URL,'retrieval_url':URL,'observed_final_url':URL,
        'retrieved_at':receipt.get('observed_at'),'http_status':200,'sha256':SHA256,'bytes':BYTES,
        'license':LICENSE,'attribution':'U.S. Census Bureau, TIGER/Line 2024 substate geographic database, Incorporated_Place layer.',
        'release_date_unknown_reason':'Edition2024. Producer HTTP Last-Modified2024-08-08 is packaging evidence, not an independently established publication day.'}

def acquire(storage,archive_path=None,receipt_path=None):
    """One lossless depot copy; never expand other GDB layers or copy ZIP per version."""
    local_only(storage);target=Path(storage.local_dir)/RAW_KEY;target.parent.mkdir(parents=True,exist_ok=True)
    if archive_path is not None:
        source=Path(archive_path);receipt=strict_json(Path(receipt_path).read_bytes());validate_receipt(source,receipt)
        if source.resolve()!=target.resolve():
            if target.exists():
                if sha256_file(target)!=SHA256:raise IncorporatedPlaceQuarantineError('existing_raw_depot_changed')
            else:
                if shutil.disk_usage(target.parent).free<BYTES+300_000_000:raise IncorporatedPlaceQuarantineError('insufficient_space_for_lossless_raw_capture')
                partial=target.parent/('capture-'+uuid4().hex+'.partial')
                shutil.copyfile(source,partial)
                if sha256_file(partial)!=SHA256:raise IncorporatedPlaceQuarantineError('copy_changed')
                os.replace(partial,target)
    else:
        # Each refresh observes the producer anew. Existing immutable raw is not
        # evidence of a successful live observation. Changed/partial bodies stay.
        partial=target.parent/('capture-'+uuid4().hex+'.partial');receipt={'url':URL,'observed_at':datetime.now(timezone.utc).isoformat(),'complete_body':False}
        if shutil.disk_usage(target.parent).free<BYTES+300_000_000:raise IncorporatedPlaceQuarantineError('insufficient_space_for_live_lossless_capture')
        try:
            with httpx.Client(follow_redirects=False,timeout=180,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',URL) as response,partial.open('xb') as output:
                receipt.update(status=response.status_code,final_url=str(response.url),content_length=response.headers.get('content-length'),content_encoding=response.headers.get('content-encoding'),requested_accept_encoding='identity')
                size=0
                try:
                    for block in response.iter_bytes(1024*1024):
                        size+=len(block)
                        if size>BYTES or shutil.disk_usage(target.parent).free<250_000_000:raise IncorporatedPlaceQuarantineError('oversized_source_or_capacity_guard')
                        output.write(block)
                finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
            receipt.update(bytes=partial.stat().st_size,sha256=sha256_file(partial),complete_body=True)
            if receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']:raise IncorporatedPlaceQuarantineError('short_encoded_transport')
            validate_receipt(partial,receipt)
            if target.exists():
                validate_receipt(target,receipt)
                # A verified exact duplicate is not a historical release. This
                # live receipt remains separately immutable in the depot.
                partial.unlink()
            else:os.replace(partial,target)
        except (OSError,ValueError,httpx.HTTPError) as exc:
            receipt.update(complete_body=False,reason=str(exc),bytes=partial.stat().st_size if partial.exists() else 0,sha256=sha256_file(partial) if partial.exists() else None)
            immutable(storage,f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}.json',encode({'receipt':receipt,'preserved_partial_key':str(partial.relative_to(storage.local_dir))}))
            raise IncorporatedPlaceQuarantineError('live_capture_failed_and_partial_retained:'+str(exc)) from exc
    validate_receipt(target,receipt)
    immutable(storage,RAW_KEY+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
    return target,receipt

def processing_identity():
    root=Path(__file__).parents[3]
    identity=shared_processing_identity(root)
    # The shared package identity records source/dependency files and generic
    # runtime versions. Bind the native GDAL build used to decode this FileGDB
    # as well, so a linked-library change alters the candidate fingerprint.
    identity['pyogrio_gdal']=str(pyogrio.__gdal_version_string__)
    return identity

def inspect_archive(path):
    with zipfile.ZipFile(path) as archive:
        infos=archive.infolist()
        if len(infos)!=84 or len({i.filename for i in infos})!=84 or sum(i.file_size for i in infos)!=1050730729:raise IncorporatedPlaceQuarantineError('archive_member_inventory_or_expansion_drift')
        for item in infos:
            parts=PurePosixPath(item.filename)
            if parts.is_absolute() or '..' in parts.parts or len(parts.parts)!=2 or parts.parts[0]!=GDB or item.flag_bits&1 or stat.S_ISLNK(item.external_attr>>16):raise IncorporatedPlaceQuarantineError('unsafe_archive_member')
        # The complete pinned ZIP SHA binds every member; GDAL opens only the
        # exact Incorporated_Place layer virtually. No unrelated layer is
        # unpacked or substituted (CDP and consolidated-city layers stay separate).
        members={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in infos}
    uri=f'/vsizip/{path}/{GDB}'
    metadata,table=pyogrio.read_arrow(uri,layer=LAYER,return_fids=True)
    _,definition=pyogrio.read_arrow(uri,sql='GetLayerDefinition '+LAYER)
    if definition.num_rows!=1:raise IncorporatedPlaceQuarantineError('missing_native_layer_definition')
    xml=definition['FIELD_1'][0].as_py();root=ET.fromstring(xml)
    fields={f.findtext('Name'):{n.tag:n.text for n in f if n.tag!='Name'} for f in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    if set(fields)!=set(FIELDS) or root.findtext('OIDFieldName')!='OBJECTID' or root.findtext('HasZ')!='false' or root.findtext('HasM')!='false':raise IncorporatedPlaceQuarantineError('native_layer_definition_drift')
    if fields['OBJECTID'].get('FieldType')!='esriFieldTypeOID' or fields['OBJECTID'].get('Length')!='4':raise IncorporatedPlaceQuarantineError('native_oid_width_drift')
    for name in ('ALAND','AWATER'):
        if fields[name].get('FieldType')!='esriFieldTypeDouble' or fields[name].get('Length')!='8':raise IncorporatedPlaceQuarantineError('native_area_type_drift')
    validate_table(table,metadata)
    audit={'approved':False,'feature_count':COUNT,'crs':'EPSG:4269','geometry':'MultiPolygon','invalid_geometries':0,'empty_geometries':0,'state_counts':STATE_COUNTS,
        'members':members,'native_layer_definition':xml,'physical_fields':fields,'arrow_schema':str(table.schema),
        'source_id':'Original OBJECTID esriFieldTypeOID Length4, exposed by GDAL return_fids as int64. All values retained; widening/ID compatibility unapproved.',
        'coverage':'All19728 incorporated places in the exact2024 national substate GDB, including American Samoa, Northern Mariana Islands and USVI. Census_Designated_Place and Consolidated_City are separate layers, never included.'}
    return table,audit

def validate_table(table,metadata):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=COUNT or any(table[n].null_count for n in FIELDS):raise IncorporatedPlaceQuarantineError('schema_physical_types_nullability_or_completeness_drift')
    if metadata['crs']!='EPSG:4269' or metadata['geometry_type']!='MultiPolygon' or metadata['fid_column']!='OBJECTID':raise IncorporatedPlaceQuarantineError('native_crs_geometry_or_identifier_drift')
    rows=table.to_pylist()
    if [r['OBJECTID'] for r in rows]!=list(range(1,COUNT+1)) or len({r['GEOID'] for r in rows})!=COUNT:raise IncorporatedPlaceQuarantineError('missing_duplicate_or_changed_source_id')
    counts={}
    for row in rows:
        if not re.fullmatch(r'\d{7}',row['GEOID']) or not re.fullmatch(r'\d{8}',row['PLACENS']) or row['GEOIDFQ']!='1600000US'+row['GEOID'] or row['CLASSFP'] not in CLASSFP_CODES or row['FUNCSTAT'] not in FUNCSTAT_CODES:raise IncorporatedPlaceQuarantineError('literal_id_or_place_class_drift')
        state=row['GEOID'][:2];counts[state]=counts.get(state,0)+1
    if counts!=STATE_COUNTS:raise IncorporatedPlaceQuarantineError('national_state_territory_coverage_drift')
    geometry=shapely.from_wkb(table['SHAPE'].to_pylist())
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)) or not all(shapely.get_type_id(geometry)==6):raise IncorporatedPlaceQuarantineError('invalid_empty_or_changed_native_geometry')

def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'SHAPE','columns':{'SHAPE':{'encoding':'WKB','geometry_types':['MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['SHAPE'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def write_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'SHAPE'})
    with sqlite3.connect(path) as db:db.execute("UPDATE gpkg_contents SET last_change='2024-01-01T00:00:00.000Z'")

def record_hash(table):
    h=hashlib.sha256()
    for row in table.to_pylist():
        row['SHAPE']=row['SHAPE'].hex();h.update(encode(row))
    return h.hexdigest()

def verify_outputs(root,table):
    expected=expected_parquet(table)
    for path in (root/'candidate/typed-source.parquet',root/'geoparquet'/f'{SLUG}.parquet'):
        if not pq.read_table(path).equals(expected,check_metadata=True):raise IncorporatedPlaceQuarantineError('native_parquet_full_values_types_wkb_or_metadata_loss')
    meta,actual=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs']!='EPSG:4269' or tuple(actual.column_names)!=FIELDS or tuple(f.type for f in actual.schema)!=TYPES or actual.to_pylist()!=table.to_pylist():raise IncorporatedPlaceQuarantineError('native_gpkg_full_values_types_or_wkb_loss')
    with tempfile.TemporaryDirectory(prefix='incplace-canonical-container-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise IncorporatedPlaceQuarantineError('full_gpkg_container_srs_envelope_sql_or_rtree_drift')

def manifest_for(path,receipt,identity,parquet_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'title':'Incorporated Places — exact2024 source, unapproved draft','publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':URL,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(path,receipt)],
        'transformations':[{'operation':'decode_exact_original_filegdb_layer','parameters':{'layer':LAYER,'all_source_columns_and_original_OBJECTID':'preserved','source_OID_physical_width':4,'GDAL_Arrow_OID_representation':'int64, proposed compatibility widening remains unapproved','geometry':'native EPSG4269 MultiPolygon coordinates without repair, reprojection or simplification','area_types':'native Double preserved'}}],
        'unsupported_output_formats':{'shapefile_zip':'DBF limits can truncate source names/widths or alter source OID schema; no mapping approved. Complete original FileGDB remains retained.','pmtiles':'Reprojection/quantization changes native geometry; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve exact source physical/Arrow metadata and native geometry; generic bbox/schema additions are unapproved. This capability pin grants no acceptance.',
            'objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_output_policy(manifest);validate_release_provenance(manifest);return manifest

@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise IncorporatedPlaceQuarantineError('linked_lock')
        fcntl.flock(held,fcntl.LOCK_EX);yield target

def construction_bundle(storage,version,path,receipt,identity,manifest,*,create=False):
    key=f'_source-provenance/{SLUG}/{version}/bundle.json'
    started=key+'.first-construction.json'
    if not storage.object_exists(key):
        if storage.object_exists(started):raise IncorporatedPlaceQuarantineError('missing_original_binding_after_first_construction')
        if not create:raise IncorporatedPlaceQuarantineError('missing_original_construction_binding')
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        immutable(storage,started,encode({'version':version,'body_key':RAW_KEY,'processing_fingerprint':digest(encode(identity))}))
        immutable(storage,key,encode({'manifest':manifest,'body_key':RAW_KEY,'receipt':receipt}))
    if not storage.object_exists(started) or strict_json(storage.read_key(started))!={'version':version,'body_key':RAW_KEY,'processing_fingerprint':digest(encode(identity))}:raise IncorporatedPlaceQuarantineError('missing_or_invalid_first_construction_marker')
    bundle=strict_json(storage.read_key(key))
    if set(bundle)!={'manifest','body_key','receipt'} or bundle['body_key']!=RAW_KEY:raise IncorporatedPlaceQuarantineError('original_binding_roster_or_body_key_drift')
    original=bundle['receipt'];pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(path,original,identity,{k:pin[k] for k in ('sha256','bytes')})
    actual=bundle['manifest'];validate_release_provenance(actual);validate_output_policy(actual)
    validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity)
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise IncorporatedPlaceQuarantineError('original_construction_manifest_or_receipt_drift')
    return original,actual

def source_payloads(receipt,identity,audit,manifest,table):
    proof={'status':'pass','method':'all_original_rows_attributes_types_and_native_WKB_exact_equality; full_deterministic_GPKG_container_and_exact_Arrow_metadata',
        'rows':COUNT,'records_sha256':record_hash(table),'crs':'EPSG:4269','approved':False}
    return {'raw/source_capture.json':encode({'key':RAW_KEY,'sha256':SHA256,'bytes':BYTES,'complete_original_members':84}),
        'raw/retrieval.json':encode(receipt),'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/incplace_source_audit.json':encode(audit),
        'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['published_baseline_and_ID_schema_physical_type_contract_acceptance_pending']}),
        'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}

def reconcile_cache(storage,prefix,version,path,table,audit,receipt,identity,parquet_pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise IncorporatedPlaceQuarantineError('incomplete_existing_candidate_missing_ready')
    expected_manifest=manifest_for(path,receipt,identity,parquet_pin)
    original,manifest=construction_bundle(storage,version,path,receipt,identity,expected_manifest)
    expected=source_payloads(original,identity,audit,manifest,table)
    names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved') is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise IncorporatedPlaceQuarantineError('ready_identity_or_source_owned_roster_drift')
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise IncorporatedPlaceQuarantineError('unknown_missing_or_aliased_candidate_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise IncorporatedPlaceQuarantineError('invalid_staged_release_derivatives') from exc
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise IncorporatedPlaceQuarantineError('source_owned_report_wrapper_placeholder_or_provenance_drift:'+name)
    for name,pin in ready['objects'].items():
        if sha256_file(root/name)!=pin:raise IncorporatedPlaceQuarantineError('candidate_object_bytes_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise IncorporatedPlaceQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists():
        expected_resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
        if strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=expected_resolved:raise IncorporatedPlaceQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table)

def stage_incorporated_places_candidate(storage,*,archive_path=None,receipt_path=None):
    local_only(storage);path,receipt=acquire(storage,archive_path,receipt_path)
    table,audit=inspect_archive(path);identity=processing_identity();fingerprint=digest(encode(identity))
    version=f'v0.0.0-draft-census2024-{SHA256[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
    # Construction stays private and is validated before the first immutable
    # provenance binding is created. Existing/missing bindings are never rebuilt.
    with candidate_lock(base,prefix) as target:
        if target.exists() or target.is_symlink():
            try:
                if target.is_symlink():raise IncorporatedPlaceQuarantineError('linked_candidate')
                native_pin={'sha256':sha256_file(target/'geoparquet'/f'{SLUG}.parquet'),'bytes':(target/'geoparquet'/f'{SLUG}.parquet').stat().st_size}
                # Never trust a repinned native capability: derive it afresh.
                with tempfile.TemporaryDirectory(prefix='incplace-fresh-parquet-') as tmp:
                    fresh=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),fresh,compression='zstd')
                    if native_pin!={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}:raise IncorporatedPlaceQuarantineError('cached_prebuilt_pin_disagrees_with_original')
                reconcile_cache(storage,prefix,version,path,table,audit,receipt,identity,native_pin)
            except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error) as exc:
                destination=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);destination.parent.mkdir(parents=True,exist_ok=True);os.rename(target,destination)
                immutable(storage,str(destination.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}))
                raise IncorporatedPlaceQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'reused':True}
        binding=f'_source-provenance/{SLUG}/{version}/bundle.json'
        with tempfile.TemporaryDirectory(prefix='.incplace-building-',dir=target.parent) as tmp:
            private=Path(tmp)
            for part in ('candidate','geopackage','geoparquet','metadata','raw'):(private/part).mkdir()
            write_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg')
            parquet=private/'geoparquet'/f'{SLUG}.parquet';pq.write_table(expected_parquet(table),parquet,compression='zstd')
            shutil.copyfile(parquet,private/'candidate/typed-source.parquet');verify_outputs(private,table)
            native_pin={'sha256':sha256_file(parquet),'bytes':parquet.stat().st_size};manifest=manifest_for(path,receipt,identity,native_pin)
            original,manifest=construction_bundle(storage,version,path,receipt,identity,manifest,create=not storage.object_exists(binding))
            for name,body in source_payloads(original,identity,audit,manifest,table).items():(private/name).write_bytes(body)
            ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'source_sha256':SHA256,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
            (private/READY).write_bytes(encode(ready));os.rename(private,target)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':COUNT,'reused':False}
