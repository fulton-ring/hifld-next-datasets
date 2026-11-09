"""Lossless FDIC location records and explicitly unapproved point interpretation.

Native JSON string/integer/float/null/ABSENT distinctions survive unchanged.
Homogeneous derivatives are unsupported pending an accepted encoding contract.
"""
from __future__ import annotations
import collections
import fcntl
import gzip
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import re
import shutil
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime,timezone
from pathlib import Path,PurePosixPath
from urllib.parse import urlsplit,parse_qs
from uuid import uuid4
import requests
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle,load_first_construction,seal_first_construction
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.release_approval import validate_release_provenance,verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource,StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.fdic_locations_spec import SLUG,PINS,SOURCE_COUNT,SOURCE_INDEX,SOURCE_AUDIT
READY='metadata/fdic_locations_ready.json'
HTTP_CHUNK_LIMIT=1_000_000
GEOJSON_KEY='geojson/'+SLUG+'.geojson'
class FdicQuarantineError(ValueError):
    """Untrusted source, schema, state or cached payload stays unpublished."""


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result: raise FdicQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    value = json.loads(body, object_pairs_hook=unique,
                       parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if type(value) is not dict or 'error' in value:
        raise FdicQuarantineError('expected_nonerror_json_object')
    return value


def local_root(storage):
    # Reject publication/remote targets before source reads or any mutation.
    if (isinstance(storage, PublishedStorageResource) or not isinstance(storage, StagingStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise FdicQuarantineError('explicit_unprefixed_local_staging_required')
    path = Path(storage.local_dir)
    if '..' in path.parts: raise FdicQuarantineError('unsafe_local_root')
    path = path.absolute()
    descriptors = [os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    try:
        for part in path.parts[1:]:
            try: os.mkdir(part, mode=0o700, dir_fd=descriptors[-1])
            except FileExistsError: pass
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        if shutil.disk_usage(path).free < 300_000_000:
            raise FdicQuarantineError('local_space_guard')
    finally:
        for descriptor in reversed(descriptors): os.close(descriptor)
    return path


def safe_tree(path):
    if path.is_symlink(): raise FdicQuarantineError('linked_candidate_root')
    for item in path.rglob('*'):
        info = item.lstat()
        if item.is_symlink() or (not item.is_dir() and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1)):
            raise FdicQuarantineError('linked_or_special_candidate_object')


def checked_path(storage, key):
    root = local_root(storage); path = root/key
    if PurePosixPath(key).is_absolute() or '..' in PurePosixPath(key).parts:
        raise FdicQuarantineError('unsafe_local_object_key')
    for item in [path,*path.parents]:
        if item.is_symlink(): raise FdicQuarantineError('linked_local_object_ancestor')
    return path


def immutable(storage, key, body):
    local_root(storage)
    path = Path(storage.local_dir)/key
    for ancestor in [path, *path.parents]:
        if ancestor.is_symlink(): raise FdicQuarantineError('linked_capture_path')
    if path.exists() and (not path.is_file() or path.stat().st_nlink != 1):
        raise FdicQuarantineError('linked_capture_object')
    snapshot = storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise FdicQuarantineError('immutable_capture_changed')


def retain_diagnostic(storage, pieces, reason):
    root = checked_path(storage,'_quarantine/'+SLUG+'/input-'+uuid4().hex)
    root.mkdir(parents=True)
    inventory = {}
    for name, body in pieces.items():
        path = root/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
        inventory[name] = {'bytes':len(body), 'sha256':digest(body), 'trust':'untrusted_diagnostic'}
    (root/'incident.json').write_bytes(encode({'approved':False, 'reason':reason, 'objects':inventory}))
    return str(root)


def bounded_regular_read(path, limit):
    """Reject oversized/special inputs before allocation and bound raced growth."""
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise FdicQuarantineError('linked_or_special_source_capture')
    if observed.st_size > limit: raise FdicQuarantineError('source_capture_size_limit')
    descriptor = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        current = os.fstat(descriptor)
        if (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_nlink) != (observed.st_dev,observed.st_ino,observed.st_size,observed.st_mtime_ns,1):
            raise FdicQuarantineError('source_capture_changed_before_read')
        with os.fdopen(os.dup(descriptor),'rb') as stream: body = stream.read(limit+1)
        after = os.fstat(descriptor)
        if len(body) > limit: raise FdicQuarantineError('source_capture_size_limit')
        if (after.st_size,after.st_mtime_ns,after.st_nlink) != (current.st_size,current.st_mtime_ns,1) or len(body) != current.st_size:
            raise FdicQuarantineError('source_capture_changed_during_read')
        return body
    finally: os.close(descriptor)


def processing_identity():
    root = Path(__file__).parents[3]
    return {'python':platform.python_version(), 'implementation':platform.python_implementation(),
            'platform':platform.platform(), 'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
            'gdal':pyogrio.__gdal_version_string__, 'geos':shapely.geos_version_string, 'proj':pyproj.proj_version_str,
            'verifier_sha256':verifier_implementation_sha256(),
            'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']}}


@contextmanager
def version_lock(storage, version):
    folder = checked_path(storage,'_locks'); folder.mkdir(exist_ok=True)
    if folder.is_symlink(): raise FdicQuarantineError('linked_version_lock_directory')
    descriptor = os.open(folder/(SLUG+'-'+version+'.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1: raise FdicQuarantineError('linked_version_lock')
        fcntl.flock(descriptor,fcntl.LOCK_EX); local_root(storage); yield
    finally: os.close(descriptor)


def observed_trust_loss(storage, version, fp, *, candidate=None, restored_manifest=None):
    """Remember an OBSERVED missing/invalid witness before detaching its cache.

    This is a refusal journal, not authority to recreate the original context.
    Exact trusted outside records must be restored; a cached manifest is merely
    an additional observed claim against which that restoration is checked.
    Unobserved simultaneous loss of every outside record cannot be detected.
    """
    key = '_construction-incidents/'+SLUG+'/'+version+'/incident.json'
    folder = checked_path(storage,'_construction-incidents/'+SLUG+'/'+version)
    path = checked_path(storage,key)
    if not folder.exists() and candidate is None: return False
    if not folder.exists():
        # Persist refusal existence first. Even an interrupted body write leaves
        # a fail-closed incident; never reconstruct authority from this marker.
        folder.mkdir(parents=True)
        descriptor = os.open(folder.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        claim = None; manifest_path = candidate/'metadata/source_manifest.json'
        try:
            descriptor = os.open(manifest_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                info = os.fstat(descriptor)
                if stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 2_000_000:
                    with os.fdopen(os.dup(descriptor),'rb') as stream: claim = digest(stream.read())
            finally: os.close(descriptor)
        except FileNotFoundError: pass
        immutable(storage,key,encode({'schema_version':1,'version':version,'processing_fingerprint':fp,
            'status':'observed_original_construction_trust_loss','approved':False,
            'observed_cached_manifest_sha256':claim}))
    try: body = bounded_regular_read(path,128_000)
    except FileNotFoundError as exc: raise FdicQuarantineError('incomplete_observed_trust_loss_journal') from exc
    value = strict_json(body); claim = value.get('observed_cached_manifest_sha256')
    if claim is not None and (type(claim) is not str or re.fullmatch('[0-9a-f]{64}',claim) is None):
        raise FdicQuarantineError('invalid_observed_trust_loss_claim')
    expected = {'schema_version':1,'version':version,'processing_fingerprint':fp,
        'status':'observed_original_construction_trust_loss','approved':False,'observed_cached_manifest_sha256':claim}
    if body != encode(expected): raise FdicQuarantineError('invalid_observed_trust_loss_journal')
    if restored_manifest is not None and claim is not None and digest(encode(restored_manifest)) != claim:
        raise FdicQuarantineError('restored_original_manifest_differs_from_observed_candidate_claim')
    return True


def acquire(capture_directory,storage=None):
    root=Path(capture_directory).absolute(); captures={};pieces={}
    if '..' in root.parts or any(p.is_symlink()for p in [root,*root.parents]):
        raise FdicQuarantineError('unsafe_capture_directory')
    try:
        for name,pin in PINS.items():
            pieces={}
            receipt_bytes=bounded_regular_read(root/(name+'.receipt.json'),128_000)
            pieces[name+'.receipt.json']=receipt_bytes
            receipt=strict_json(receipt_bytes)
            path=root/(name+'.body');body=bounded_regular_read(path,pin['bytes'])
            pieces[name+'.body']=body
            validate_capture(name,body,receipt)
            captures[name]=(path,receipt)
        return captures
    except (ValueError,OSError,TypeError,KeyError,OverflowError)as exc:
        if storage is not None:
            location=retain_diagnostic(storage,pieces,str(exc))
            raise FdicQuarantineError(str(exc)+';untrusted_diagnostic:'+location)from exc
        raise


def validate_capture(name,body,receipt):
    pin=PINS[name]
    if (len(body)!=pin['bytes'] or digest(body)!=pin['sha256']
        or receipt.get('requested_url')!=pin['requested_url']
        or receipt.get('observed_final_url')!=pin['observed_final_url']
        or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body)or receipt.get('complete')is not True):
        raise FdicQuarantineError('incomplete_or_changed_pinned_capture:'+name)
    for timestamp in [receipt['started_at'],receipt['observed_at']]:
        if type(timestamp)is not str or re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})',timestamp)is None:
            raise FdicQuarantineError('strict_aware_ISO_capture_time_required')
        if int(timestamp[11:13])>23 or int(timestamp[14:16])>59 or int(timestamp[17:19])>59:
            raise FdicQuarantineError('invalid_capture_time_components')
        if not timestamp.endswith('Z')and(int(timestamp[-5:-3])>23 or int(timestamp[-2:])>59):
            raise FdicQuarantineError('invalid_capture_timezone_components')
    start=datetime.fromisoformat(receipt['started_at']);end=datetime.fromisoformat(receipt['observed_at'])
    if start.utcoffset()is None or end.utcoffset()is None or start>end:
        raise FdicQuarantineError('invalid_capture_observation_interval')


def body_for(name,captures):
    body=bounded_regular_read(captures[name][0],PINS[name]['bytes'])
    validate_capture(name,body,captures[name][1]);return body


def persist_response_stream(storage,response,path,limit):
    """Persist every delivered bounded chunk BEFORE rejecting a size overflow.

    Floor checks precede each read. A failed capture can exceed its limit by at
    most one 1 MiB chunk; it is explicitly incomplete and never a source claim.
    """
    local_root(storage)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    count=0
    try:
        while True:
            local_root(storage) # No new network bytes consumed below the floor.
            chunk=response.raw.read(HTTP_CHUNK_LIMIT)
            if not chunk:break
            position=0
            while position<len(chunk):
                written=os.write(descriptor,chunk[position:])
                if written<=0:raise OSError('failed_capture_evidence_write')
                position+=written
            os.fsync(descriptor)
            count+=len(chunk)
            if len(chunk)>HTTP_CHUNK_LIMIT:raise FdicQuarantineError('transport_violated_bounded_read_contract_after_preservation')
            if count>limit:raise FdicQuarantineError('live_response_size_drift_incomplete_prefix_preserved')
        return count
    finally:os.close(descriptor)

def native_audit(rows):
    fields=sorted(set().union(*(set(row)for row in rows)))
    counts=collections.Counter(tuple(sorted(row))for row in rows)
    return {'count':len(rows),'index':SOURCE_INDEX,'field_roster':fields,
        'field_roster_counts':{str(k):v for k,v in counts.items()},
        'native_primitive_types':{name:dict(collections.Counter(type(row[name]).__name__ if name in row else 'ABSENT'for row in rows))for name in fields},
        'BKCLASS_counts':dict(collections.Counter(row.get('BKCLASS')for row in rows)),
        'MAINOFF_counts':dict(collections.Counter(str(row.get('MAINOFF'))for row in rows)),
        'RUNDATE_counts':dict(collections.Counter(str(row.get('RUNDATE'))for row in rows)),
        'zero_coordinate_pairs':sum(row.get('LATITUDE')==0 and row.get('LONGITUDE')==0 for row in rows),
        'null_or_absent_coordinate_fields':sum(row.get('LATITUDE')is None or row.get('LONGITUDE')is None for row in rows),
        'native_leading_zero_ZIP_count':sum(type(row.get('ZIP'))is str and row['ZIP'].startswith('0')for row in rows),
        'canonical_all_raw_source_record_sha256':digest(encode(rows))}


def page_records(name,captures):
    value=strict_json(body_for(name,captures));meta=value.get('meta',{});total=value.get('totals',{}).get('count')
    if (type(meta.get('total'))is not int or meta['total']!=SOURCE_COUNT or type(total)is not int or total!=SOURCE_COUNT or meta.get('index',{}).get('name')!=SOURCE_INDEX):
        raise FdicQuarantineError('source_census_or_index_drift')
    parameters=parse_qs(urlsplit(PINS[name]['requested_url']).query)
    # FDIC does not echo sort_by/sort_order. Verify actual monotonic IDs below;
    # do not manufacture a metadata claim that the producer does not emit.
    echoed={'filters':'','limit':parameters['limit'][0],'offset':parameters['offset'][0]}
    if 'fields'in parameters:echoed['fields']=parameters['fields'][0]
    if encode(meta.get('parameters'))!=encode(echoed):raise FdicQuarantineError('source_query_echo_drift')
    offset=int(parameters['offset'][0]);limit=int(parameters['limit'][0]);records=value.get('data')
    if type(records)is not list or len(records)!=min(limit,SOURCE_COUNT-offset):raise FdicQuarantineError('incomplete_or_excess_source_page')
    if any(set(record)!={'data','score'}or type(record['data'])is not dict for record in records):raise FdicQuarantineError('source_record_envelope_drift')
    return [record['data']for record in records],meta['index']


def decode_native(captures):
    populations={};indexes=[]
    for kind in ['census-before','page','census-after']:
        rows=[]
        for offset in range(0,SOURCE_COUNT,10000):
            part,index=page_records('source-'+kind+'-'+str(offset).zfill(6),captures);rows.extend(part);indexes.append(index)
        identifiers=[]
        for row in rows:
            if type(row.get('UNINUM'))is not int or type(row.get('ID'))is not str or not row['ID']:raise FdicQuarantineError('missing_or_changed_native_identifier_type')
            identifiers.append(row['UNINUM'])
        if identifiers!=sorted(identifiers)or len(set(identifiers))!=SOURCE_COUNT or len({row['ID']for row in rows})!=SOURCE_COUNT:
            raise FdicQuarantineError('duplicate_or_incomplete_source_identifier_census')
        populations[kind]=rows
    if any(encode(index)!=encode(indexes[0])for index in indexes):raise FdicQuarantineError('observed_source_index_drift')
    source=populations['page'];ids=[{'UNINUM':row['UNINUM'],'ID':row['ID']}for row in source]
    if encode(ids)!=encode(populations['census-before'])or encode(ids)!=encode(populations['census-after']):raise FdicQuarantineError('observed_before_after_ID_census_drift')
    audit=native_audit(source)
    if encode(audit)!=encode(SOURCE_AUDIT):raise FdicQuarantineError('native_schema_primitive_presence_values_or_coverage_drift')
    return source,audit,indexes[0]


def interpretable_coordinates(latitude,longitude):
    return (type(latitude)in[int,float]and type(longitude)in[int,float]
        and math.isfinite(latitude)and math.isfinite(longitude)
        and -90<=latitude<=90 and -180<=longitude<=180)


def point_collection(rows):
    """Literal properties are unchanged; Point/CRS interpretation is proposed."""
    features=[];zeros=0;uninterpretable=0
    for row in rows:
        latitude=row.get('LATITUDE');longitude=row.get('LONGITUDE')
        usable=interpretable_coordinates(latitude,longitude)
        if usable:geometry={'type':'Point','coordinates':[longitude,latitude]};zeros+=latitude==0 and longitude==0
        else:geometry=None;uninterpretable+=1
        features.append({'type':'Feature','properties':row,'geometry':geometry})
    quality={'complete_population':len(rows),'zero_coordinate_pairs':zeros,'uninterpretable_coordinates':uninterpretable,
        'quality_check_passed':zeros==0 and uninterpretable==0,'producer_declared_CRS':None,
        'proposed_geometry_CRS':'EPSG:4326 for draft GeoJSON only; NOT producer-declared or approved',
        'coordinate_policy':'Every literal property retained, including integer zeros; no fill, geocode, clipping or repair.'}
    return {'type':'FeatureCollection','features':features},quality


def full_comparison(captures,rows,quality):
    baseline=pq.read_table(captures['baseline-parquet'][0]);old=baseline.to_pylist()
    expected_names=(set(SOURCE_AUDIT['field_roster'])-{'ID'})|{'geometry','bbox','OBJECTID','source_ID'}
    if len(old)!=79542 or set(baseline.column_names)!=expected_names:raise FdicQuarantineError('historical_baseline_schema_or_count_drift')
    current={row['UNINUM']:row for row in rows};seen=set();links=[];differences=collections.Counter();retained=0;equal_geometry=0;header=0
    for before in old:
        key=before['UNINUM'];after=current.get(key);is_header=before['OBJECTID']==30001 and before['NAME']=='NAME';header+=is_header
        if key is not None:
            if type(key)is not int or key in seen:raise FdicQuarantineError('historical_native_key_duplicate_or_type_drift')
            seen.add(key)
        changed=[];same=False
        if after is not None:
            retained+=1
            for name in SOURCE_AUDIT['field_roster']:
                old_name='source_ID'if name=='ID'else name
                if encode({'present':name in after,'value':after.get(name)})!=encode({'present':old_name in before,'value':before.get(old_name)}):changed.append(name);differences[name]+=1
            if before['geometry']is not None and interpretable_coordinates(after['LATITUDE'],after['LONGITUDE']):
                same=shapely.to_wkb(shapely.Point(after['LONGITUDE'],after['LATITUDE']))==before['geometry']
            equal_geometry+=same
        links.append({'historical_original_ids':{name:before[name]for name in ['OBJECTID','source_ID','UNINUM','CERT','FI_UNINUM']},
            'current_original_ids':{name:after[name]for name in ['ID','UNINUM','CERT','FI_UNINUM']}if after is not None else None,
            'diagnostic_UNINUM_join_NOT_approved':after is not None,'historical_header_preserved':is_header,
            'changed_typed_values_or_key_presence':changed,'literal_proposed_point_WKB_equal':bool(same)})
    if header!=1 or len(seen)!=79541:raise FdicQuarantineError('historical_identifier_or_header_drift')
    lineage=gzip.compress(encode({'scope':'Complete original ID populations; exact-integer UNINUM diagnostic join only; no approved ID/date/type/geometry mapping.',
        'rows':links,'source_only_original_ids':[{name:row[name]for name in ['ID','UNINUM','CERT','FI_UNINUM']}for row in rows if row['UNINUM']not in seen]}),mtime=0)
    report={'schema_version':1,'approved':False,'status':'blocked','acceptance':'not_established',
        'populations':{'source':len(rows),'baseline':len(old),'retained_UNINUM':retained,'source_only':len(set(current)-seen),'baseline_only_with_header':len(old)-retained},
        'full_retained_rows_compared':retained,'full_typed_field_change_counts':dict(differences),'literal_point_WKB_equal_count':equal_geometry,
        'historical_header_rows_preserved':header,'historical_zero_coordinate_pairs_preserved':5766,'source_quality':quality,
        'reason_codes':['current2026_vs_historical2024_release_selection_unapproved','source_baseline_contract_rights_unapproved','variant_primitives_absence_null_dates_IDs_unapproved',
            'proposed_Point_and_CRS_not_producer_declared_or_approved','803_source_zero_coordinate_pairs_not_repaired'],
        'full_identifier_lineage_evidence':{'path':'candidate/full_identifier_lineage.json.gz','bytes':len(lineage),'sha256':digest(lineage)},
        'baseline_schema':str(baseline.schema),'native_primitive_schema':SOURCE_AUDIT['native_primitive_types'],
        'scope':'All79542Parquetrows/all78028source records/allretainedtypedfields/literalpointWKB; all5fullhistoricalformats pinned/preserved, not declared equivalent.'}
    return report,lineage


def manifest_for(captures,identity,geojson):
    inputs=[]
    for name,pin in PINS.items():
        archive=name.startswith('baseline-')or name=='gcs-objects';receipt=captures[name][1]
        inputs.append({'input_id':name+':'+pin['sha256'],'producer':'FDIC'if not archive else'Prior HIFLD Next reference, unapproved baseline',
            'host':urlsplit(pin['requested_url']).hostname,'role':'archive'if archive else'producer_publication','canonical_url':pin['requested_url'],'retrieval_url':pin['requested_url'],
            'observed_final_url':pin['observed_final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'bytes':pin['bytes'],'sha256':pin['sha256'],
            'license':'FDIC attribution retained; exact rights/release acceptance pending.'if not archive else'Preserved prior candidate; no new rights/acceptance inferred.',
            'attribution':'FDIC BankFind Suite locations product; HIFLD Next processes draft reconstruction only.'if not archive else'Prior HIFLD Next artifact; every original ID/schema/geometry preserved.',
            'release_date_unknown_reason':'APIindexcreation is indexing metadata. LiteralRUNDATE10/16/2026 retained; no preciseproducerpublicationtimestamp established.'if not archive else'Exact historical baseline and Excel-date interpretation remain unapproved.'})
    unsupported='Native mixed JSON string/integer types and ABSENT/null distinctions need a reviewed encoding contract; no homogeneous coercion is approved.'
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'title':'FDIC Insured Banks — complete official locations draft',
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_FDIC_locations_native_record_preservation','parameters':{'record_count':SOURCE_COUNT,'source_index':SOURCE_INDEX,
            'attributes':'All original primitives and key-presence/null distinctions retained; no stringification, numeric coercion, ID replacement or date parsing.',
            'envelope':'Search score/query/index envelope retained in complete raw responses; not invented as main attributes.',
            'coherence':'Complete before/full/after unique ID census and index equality, no transaction guarantee.'}},
            {'operation':'PROPOSED_Point_interpretation','parameters':{'geometry':'Point from unchanged literal LONGITUDE/LATITUDE; uninterpretable geometry null, allrows retained.',
                'producer_declared_CRS':None,'proposed_GeoJSON_CRS':'EPSG:4326','approved':False,'quality':'803integerzero pairs and2out-of-range interpretations blocked; all literal coordinates retained, no swap/geocode/repair.'}}],
        'unsupported_output_formats':{'geopackage':unsupported,'geoparquet':unsupported,'shapefile_zip':unsupported+' Full original field names also exceed Shapefile limits.',
            'pmtiles':'No approvedcartographicID/field/type/geometrycontract; native types/absence must not be normalized.'},
        'preserved_native_output':{'path':GEOJSON_KEY,'bytes':len(geojson),'sha256':digest(geojson)},
        'literal_geojson_catalog':{'schema_version':1,'path':GEOJSON_KEY,'bytes':len(geojson),'sha256':digest(geojson)},
        'native_schema_authority':'Complete source primitive/presence audit; generic catalog inferred types are exploratory metadata, not a homogeneous contract.'}
    validate_release_provenance(manifest);return manifest


def source_payloads(captures,identity,audit,index,quality,comparison,lineage,manifest,geojson):
    return {GEOJSON_KEY:geojson,'metadata/source_manifest.json':encode(manifest),'metadata/baseline_comparison.json':encode(comparison),'candidate/full_identifier_lineage.json.gz':lineage,
        'candidate/native_schema_and_coverage.json':encode({'approved':False,'source_index':index,'full_native_audit':audit,'full_quality':quality,'geometry_is_unapproved_interpretation':True}),
        'candidate/processing_identity.json':encode(identity),'candidate/raw_input_inventory.json':encode({name:{'raw_body_key':f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body',
            'receipt':captures[name][1],'bytes':pin['bytes'],'sha256':pin['sha256']}for name,pin in PINS.items()})}


def original_context(storage,key,version,fp,identity,geojson):
    payload=strict_json(load_first_construction(storage,key,version,fp))['payload']
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(PINS):raise FdicQuarantineError('original_context_closed_roster_drift')
    original={}
    for name,claim in payload['captures'].items():
        pin=PINS[name];raw=f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body';receipt_key=raw+'.receipt-'+digest(encode(claim.get('receipt')))+'.json'
        if set(claim)!={'raw_body_key','raw_receipt_key','receipt'}or claim['raw_body_key']!=raw or claim['raw_receipt_key']!=receipt_key:raise FdicQuarantineError('original_raw_capture_identity_drift')
        if bounded_regular_read(checked_path(storage,receipt_key),128_000)!=encode(claim['receipt']):raise FdicQuarantineError('original_receipt_missing_or_changed')
        path=checked_path(storage,raw);validate_capture(name,bounded_regular_read(path,pin['bytes']),claim['receipt']);original[name]=(path,claim['receipt'])
    expected=manifest_for(original,identity,geojson);manifest=payload['manifest'];validate_release_provenance(manifest);expected['code_commit']=manifest.get('code_commit')
    if encode(expected)!=encode(manifest):raise FdicQuarantineError('original_full_manifest_changed')
    validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files']);return original,manifest


def reconcile_cache(storage,prefix,expected,version,fp):
    root=checked_path(storage,prefix);safe_tree(root);ready_body=bounded_regular_read(root/READY,128_000)
    canonical={'version':version,'processing_fingerprint':fp,'approved':False,'objects':{name:{'sha256':digest(body),'bytes':len(body)}for name,body in expected.items()}}
    if ready_body!=encode(canonical):raise FdicQuarantineError('repinned_noncanonical_or_changed_ready_receipt')
    for name,body in expected.items():
        if bounded_regular_read(root/name,len(body))!=body:raise FdicQuarantineError('fresh_complete_source_owned_payload_disagrees:'+name)
    snapshot='metadata/source/version_'+digest(expected['metadata/source_manifest.json'])+'_source_manifest.json'
    downstream={'metadata/source/source_manifest.json',snapshot,'metadata/source/data_dictionary.json','metadata/source/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/quality_manifest.json','metadata/resolved_provenance.json'}
    actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()};owned=set(expected)
    if actual-(owned|{READY}|downstream)or not(owned|{READY}).issubset(actual):raise FdicQuarantineError('unrostered_or_missing_candidate_object')
    authored_catalog_pair={'metadata/source/quality_manifest.json','metadata/source/data_dictionary.json'}
    source_authored_catalog=authored_catalog_pair.intersection(owned)
    if source_authored_catalog and source_authored_catalog!=authored_catalog_pair:
        raise FdicQuarantineError('incomplete_authored_catalog_roster')
    if source_authored_catalog and not source_authored_catalog.issubset(actual):
        raise FdicQuarantineError('missing_authored_catalog_snapshot')
    catalog_paths={
        'metadata/quality_manifest.json':'quality_manifest',
        'metadata/source/quality_manifest.json':'quality_manifest',
        'metadata/data_dictionary.json':'data_dictionary',
        'metadata/source/data_dictionary.json':'data_dictionary',
    }
    catalog_present=actual.intersection(catalog_paths)
    if catalog_present:
        if not {'metadata/quality_manifest.json','metadata/data_dictionary.json'}.issubset(actual):
            raise FdicQuarantineError('incomplete_catalog_sidecar_pair')
        for name in ['metadata/source/source_manifest.json',snapshot]:
            if name not in actual:raise FdicQuarantineError('missing_authored_manifest_snapshot:'+name)
            if(root/name).read_bytes()!=expected['metadata/source_manifest.json']:raise FdicQuarantineError('authored_manifest_snapshot_changed')
        resolved_metadata=load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata
        resolved=provenance_projection(resolved_metadata)
        provenance_path=root/'metadata/resolved_provenance.json'
        if (resolved is not None) != (provenance_path.relative_to(root).as_posix() in actual):
            raise FdicQuarantineError('resolved_provenance_presence_mismatch')
        if resolved is not None and encode(strict_json(bounded_regular_read(provenance_path,20_000_000)))!=encode(resolved):
            raise FdicQuarantineError('resolved_provenance_changed')
        summary=summarize_staged_catalog(storage,SLUG,SLUG,version,SLUG,source_metadata=resolved_metadata)
        canonical_catalog={'quality_manifest':summary.quality_manifest,'data_dictionary':summary.data_dictionary}
        for name in sorted(catalog_present):
            actual_catalog=strict_json(bounded_regular_read(root/name,20_000_000))
            if encode(actual_catalog)!=encode(canonical_catalog[catalog_paths[name]]):
                raise FdicQuarantineError('catalog_sidecar_semantic_drift:'+name)


def capture_live_observation(storage,captures):
    folder=checked_path(storage,'_observations/'+SLUG+'/'+uuid4().hex);folder.mkdir(parents=True);observed=dict(captures)
    names=[]
    for prefix in ['source-census-before-','source-page-','source-census-after-']:names.extend(sorted(name for name in PINS if name.startswith(prefix)))
    for name in names:
        pin=PINS[name];path=folder/(name+'.body');progress={'requested_url':pin['requested_url'],'started_at':datetime.now(timezone.utc).isoformat(),
            'complete':False,'trust':'untrusted_capture_in_progress','body_path':path.name,'bounded_read_chunk_limit':HTTP_CHUNK_LIMIT}
        failed=folder/(name+'.failed-receipt.json');failed.write_bytes(encode(progress))
        try:
            with requests.get(pin['requested_url'],headers={'Accept-Encoding':'identity','User-Agent':'HIFLD-Next-FDIC-draft'},stream=True,timeout=120)as response:
                progress.update(observed_final_url=response.url,status=response.status_code);count=persist_response_stream(storage,response,path,pin['bytes'])
                if response.headers.get('Content-Encoding','identity').lower()not in['identity','']:raise FdicQuarantineError('unexpected_live_content_encoding')
                if response.headers.get('Content-Length')is not None and int(response.headers['Content-Length'])!=count:raise FdicQuarantineError('incomplete_live_transport')
                body=bounded_regular_read(path,pin['bytes']);receipt={**progress,'observed_at':datetime.now(timezone.utc).isoformat(),'bytes':count,'wire_bytes':count,'sha256':digest(body),'complete':True}
            validate_capture(name,body,receipt);(folder/(name+'.receipt.json')).write_bytes(encode(receipt));observed[name]=(path,receipt);failed.unlink()
        except BaseException as exc:
            progress.update(observed_at=datetime.now(timezone.utc).isoformat(),error=str(exc),complete=False,trust='untrusted_failed_capture',
                persisted_bytes=path.stat().st_size if path.exists()else 0,persisted_sha256=sha256_file(path)if path.exists()else None);failed.write_bytes(encode(progress));raise
    return observed


def stage_fdic_locations(storage,capture_directory,refresh_official=False):
    root=local_root(storage);captures=acquire(capture_directory,storage)
    if refresh_official:captures=capture_live_observation(storage,captures)
    identity=processing_identity();fp=digest(encode(identity));core=digest(encode({name:pin['sha256']for name,pin in PINS.items()}))
    version='v0.0.0-draft-fdic-locations-'+core[:12]+'-'+fp[:16];prefix=SLUG+'/'+SLUG+'/'+version;target=checked_path(storage,prefix);key='_construction-state/'+SLUG+'/'+version+'.json'
    for name,pin in PINS.items():
        raw=f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body';immutable(storage,raw,body_for(name,captures));receipt=captures[name][1];immutable(storage,raw+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
    rows,audit,index=decode_native(captures);collection,quality=point_collection(rows);geojson=encode(collection);comparison,lineage=full_comparison(captures,rows,quality);manifest=manifest_for(captures,identity,geojson)
    with version_lock(storage,version):
        state=root/key;has_history=state.exists()or state.with_name(state.name+'.first-construction.json').exists()or state.with_name(state.name+'.evidence').exists()
        if target.exists():
            can_detach=True
            try:
                try:captures,manifest=original_context(storage,key,version,fp,identity,geojson);observed_trust_loss(storage,version,fp,restored_manifest=manifest)
                except(ValueError,OSError,TypeError,KeyError,RuntimeError):
                    try:observed_trust_loss(storage,version,fp,candidate=target)
                    except(ValueError,OSError,TypeError,KeyError,RuntimeError):can_detach=False;raise
                    raise
                expected=source_payloads(captures,identity,audit,index,quality,comparison,lineage,manifest,geojson);reconcile_cache(storage,prefix,expected,version,fp)
            except(ValueError,OSError,TypeError,KeyError,RuntimeError)as exc:
                if not can_detach:raise FdicQuarantineError('trust_loss_journal_failed_candidate_retained_untrusted:'+str(exc))from exc
                rejected=checked_path(storage,'_quarantine/'+SLUG+'/invalid-candidate-'+uuid4().hex);rejected.parent.mkdir(parents=True,exist_ok=True);os.rename(target,rejected)
                raise FdicQuarantineError('invalid_candidate_preserved_outside_discovery:'+str(rejected)+':'+str(exc))from exc
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':len(rows),'reused':True,**quality,'source_release_approved':False}
        if has_history or observed_trust_loss(storage,version,fp):
            captures,manifest=original_context(storage,key,version,fp,identity,geojson);observed_trust_loss(storage,version,fp,restored_manifest=manifest)
        else:validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        expected=source_payloads(captures,identity,audit,index,quality,comparison,lineage,manifest,geojson);validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files']);building=checked_path(storage,'_building/'+SLUG);building.mkdir(parents=True,exist_ok=True)
        temporary=Path(tempfile.mkdtemp(prefix='candidate-',dir=building))
        try:
            for name,body in expected.items():
                path=temporary/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
            if encode(strict_json((temporary/GEOJSON_KEY).read_bytes()))!=geojson:raise FdicQuarantineError('written_GeoJSON_not_lossless')
            objects={name:{'sha256':sha256_file(temporary/name),'bytes':(temporary/name).stat().st_size}for name in expected}
            (temporary/READY).write_bytes(encode({'version':version,'processing_fingerprint':fp,'approved':False,'objects':objects}))
            bundle=canonical_construction_bundle(version,fp,{'manifest':manifest,'captures':{name:{'raw_body_key':f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body',
                'raw_receipt_key':f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body.receipt-'+digest(encode(captures[name][1]))+'.json','receipt':captures[name][1]}for name,pin in PINS.items()}})
            if seal_first_construction(storage,key,version,fp,bundle)!=bundle:raise FdicQuarantineError('concurrent_original_context_differs')
            target.parent.mkdir(parents=True,exist_ok=True);os.rename(temporary,target)
        except BaseException:
            if temporary.exists():
                rejected=checked_path(storage,'_quarantine/'+SLUG+'/failed-private-'+uuid4().hex);rejected.parent.mkdir(parents=True,exist_ok=True);os.rename(temporary,rejected)
            raise
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':len(rows),'reused':False,**quality,'source_release_approved':False}
