"""Explicit unapproved historical USGS 2003 survey reconstruction; never polls.

The only candidate input package is the USGS-owned "Active Mines and Mineral
Processing Plants in the United States in 2003" distribution (MRDATA
mineplant), the producer's own historical 2003 Minerals Information Team survey
publication documenting the assigned construction-minerals scope of the retired
National Atlas construction-minerals layer; it is a historical source, not a
current or maintained census. The existing published HIFLD objects are pinned
comparison context only; they are never candidate inputs. No third-party
preservation masters are fetched or compared. No default fetching, archive
polling, USGS release authority claim, datum correction, uppercase conversion,
identifier mapping or production release is performed.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import json
import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from fiona.io import MemoryFile, ZipMemoryFile
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.epa_frs_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as packaged_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.sources.usgs_mineplant import (MINEPLANT_DBF_FIELDS, MINEPLANT_MEMBERS,
    MINEPLANT_PRJ, MINEPLANT_RECORDS, MineplantQuarantineError, crosscheck_mineplant_txt,
    decode_mineplant_dbf, decode_mineplant_zip, decode_points, verify_mineplant_geometry_values)
SLUG='construction-minerals-operations'
PACKAGE='usgs-mrdata-mineplant-2003-v1'
READY='metadata/construction_minerals_candidate_ready.json'
LICENSE='USGS metadata for the mineplant successor and the retired National Atlas layer states no use restrictions (National Minerals Information Center acknowledgment appreciated). No source selection, authority, datum, schema/ID mapping, contract or release acceptance.'
class MineralsQuarantineError(ValueError): pass

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise MineralsQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(MineralsQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise MineralsQuarantineError('json_object_required')
    return value

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise MineralsQuarantineError('immutable_capture_changed:'+key)

def processing_identity():
    return packaged_processing_identity(Path(__file__).parents[3])

def retain_failure(storage,captures,reason,attempted=None):
    prefix=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,prefix+'/'+name+'.body',body);immutable(storage,prefix+'/'+name+'.receipt.json',encode(receipt))
    if attempted is not None:immutable(storage,prefix+'/attempted-inputs.json',encode({'approved':False,'receipt_metadata_validated':False,'objects':attempted}))
    immutable(storage,prefix+'/reason.json',encode({'approved':False,'reason':reason,'attempted_inputs':prefix+'/attempted-inputs.json' if attempted is not None else None}))
    return prefix

def validate_capture(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=digest(body)):
        raise MineralsQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise MineralsQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX);yield target

def construction_record_key(version):
    return f'_construction-state/{SLUG}/{version}.json'

def original_bundle(storage,version,captures,identity,manifest,*,create=False):
    """Caller holds common lock; create only after complete private validation.

    Complete first manifest/receipts are bound by the shared outside CAS/marker.
    Old unbound records remain evidence and cannot bootstrap this trust scope.
    """
    fingerprint=digest(encode(identity))
    payload={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt} for name,(body,receipt) in captures.items()}}
    envelope=canonical_construction_bundle(version,fingerprint,payload)
    legacy=f'_source-provenance/{SLUG}/{version}/bundle.json'
    if storage.object_exists(legacy) or storage.object_exists(legacy+'.first-construction.json'):
        raise MineralsQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise MineralsQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise MineralsQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if body!=captures[name][0]:raise MineralsQuarantineError('original_capture_bytes_differ')
        original[name]=(body,item['receipt'])
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity)
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise MineralsQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

PINS={'usgs_mineplant_zip': {'url': 'https://mrdata.usgs.gov/mineplant/mineplant.zip', 'bytes': 513191, 'sha256': '6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e'}, 'usgs_mineplant_metadata': {'url': 'https://mrdata.usgs.gov/metadata/mineplant.xml', 'bytes': 26366, 'sha256': '419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0'}, 'legacy_metadata': {'url': 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml', 'bytes': 19769, 'sha256': 'd68776d0c1c487d7b131c4abc0643c7e0ac9fbeac656144b585ca3c6044c3e94'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geojson/construction-minerals-operations-geojson.geojson', 'bytes': 275487, 'sha256': 'd8f5de1e147ae977d1a780a0940996805808c67623edb00d4ba38aa20e7f7e6d'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geopackage/construction-minerals-operations-geopackage.gpkg', 'bytes': 270336, 'sha256': '9d18391d85ac6689596689011821799b6723b3cb9a5b633a6b4c13018a31b305'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geoparquet/construction-minerals-operations.parquet', 'bytes': 76683, 'sha256': 'd5189e3387bce01681b454d035f56365e83f3d54bba34a3cbb2d388f22e4361d'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/pmtiles/construction-minerals-operations.pmtiles', 'bytes': 85733, 'sha256': '1d9b649236652f2ab62a7cf834be0d199f08fdd6f705073309bc4d2be62b32c6'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/shapefile/construction-minerals-operations.zip', 'bytes': 47909, 'sha256': '3e0f83e7853f0e1d17c68603a23290cc881460e98634ae34888f002e8bde8da5'}, 'baseline_version': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/releases/1704edb3-322d-469c-91da-92902fe76a2d/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/collection.json', 'bytes': 12652, 'sha256': '217f441c7acf0d3763c46a22375fa2fafaf885cd6f5d5d9bcb63aaa918dc25d6'}, 'baseline_current_metadata': {'url': 'https://storage.googleapis.com/storage/v1/b/hifld-next-datasets-prod/o?prefix=hifld%2Fconstruction-minerals-operations%2Fconstruction-minerals-operations%2Fv1.0.0%2F&maxResults=100', 'bytes': 16676, 'sha256': '4f4b3e2b34aee2174790c05bc70bedd844ea9f993b0e15b3f1aac017d1ff29c7'}}

# Only USGS-owned objects may feed candidate values; every other pinned object
# is comparison context and is labeled as such in the source manifest and audit.
INPUT_NAMES=frozenset({'usgs_mineplant_zip','usgs_mineplant_metadata','legacy_metadata'})

CONSTRUCTION_COMMODITIES=('Ball Clay','Cement','Common Clay and Shale','Dimension Stone','Gypsum','Mica','Perlite','Pumice')
CONSTRUCTION_COMMODITY_COUNTS={'Ball Clay':58,'Cement':115,'Common Clay and Shale':262,'Dimension Stone':154,'Gypsum':62,'Mica':24,'Perlite':74,'Pumice':16}
CONSTRUCTION_ROWS=765
CONSTRUCTION_STATES=46
# Physical declarations of the retired National Atlas layer, retained because the
# pinned published comparison objects use the original atlas schema and MINOP2X020 IDs.
DBF_FIELDS=[('MINOP2X020','N',3,0),('COMMODITY','C',21,0),('SITE_NAME','C',46,0),('COMPANY_NA','C',51,0),('STATE_LOCA','C',14,0),('COUNTY','C',16,0),('LATITUDE','N',9,3),('LONGITUDE','N',11,3),('PLANT_MIN','C',10,0)]
SOURCE_NAMES=['source_fid','source_shp_record_number']+[f[0] for f in MINEPLANT_DBF_FIELDS]+['geometry']

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise MineralsQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir)
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise MineralsQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise MineralsQuarantineError('linked_staging_object')


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise MineralsQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise MineralsQuarantineError('incomplete_or_changed_bounded_input')
    return body


def acquire(storage,capture_dir,package_id):
    local_only(storage)
    if package_id!=PACKAGE or not capture_dir:raise MineralsQuarantineError('explicit_unapproved_usgs_successor_package_selection_required_no_default_fetch')
    root=Path(capture_dir);captures={};attempted={}
    if '..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents]):raise MineralsQuarantineError('unsafe_input_package_root')
    try:
        expected={name+suffix for name in PINS for suffix in ('.body','.receipt.json')}
        if {p.name for p in root.iterdir()}!=expected:raise MineralsQuarantineError('incomplete_or_extra_explicit_package_roster')
        for name,pin in PINS.items():
            paths=[root/(name+suffix) for suffix in ('.body','.receipt.json')]
            if any(not p.is_file() or p.is_symlink() or p.stat().st_nlink!=1 for p in paths):raise MineralsQuarantineError('linked_or_nonregular_package_input')
            if paths[0].stat().st_size>2*1024*1024 or paths[1].stat().st_size>65536:raise MineralsQuarantineError('package_input_exceeds_bound')
            body=bounded_input(paths[0],2*1024*1024)
            # Preserve the full bounded attempted input BEFORE interpreting any
            # receipt. These CAS bytes are evidence, not validated provenance.
            body_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
            immutable(storage,body_key,body)
            attempted[name]={'body_key':body_key,'body_sha256':digest(body),'body_bytes':len(body),'raw_receipt_read':False,'canonical_expected_url':pin['url']}
            raw_receipt=bounded_input(paths[1],65536)
            receipt_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.raw-receipt-{digest(raw_receipt)}.body'
            immutable(storage,receipt_key,raw_receipt)
            attempted[name].update(raw_receipt_read=True,raw_receipt_key=receipt_key,raw_receipt_sha256=digest(raw_receipt),raw_receipt_bytes=len(raw_receipt))
            receipt=strict_json(raw_receipt);captures[name]=(body,receipt)
            validate_capture(name,body,receipt,pin['url'])
            if len(body)!=pin['bytes'] or digest(body)!=pin['sha256']:raise MineralsQuarantineError('unreviewed_package_bytes_require_explicit_new_package:'+name)
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise MineralsQuarantineError(str(exc)+'; retained:'+retained) from exc


    if len(rows)!=len(geometry):raise MineralsQuarantineError('usgs_record_geometry_alignment_drift')
    for row,wkb in zip(rows,geometry):
        point=shapely.from_wkb(wkb)
        if (point.x,point.y)!=(float(row['LONGITUDE']),float(row['LATITUDE'])):raise MineralsQuarantineError('usgs_geometry_attribute_value_drift')


def construction_rows(rows):
    subset=[row for row in rows if row['COMMODITY'] in CONSTRUCTION_COMMODITIES]
    if len(subset)!=CONSTRUCTION_ROWS:raise MineralsQuarantineError('construction_subset_completeness_drift')
    if dict(collections.Counter(row['COMMODITY'] for row in subset))!=CONSTRUCTION_COMMODITY_COUNTS:raise MineralsQuarantineError('construction_subset_commodity_count_drift')
    if len({row['STATE_LOCA'] for row in subset})!=CONSTRUCTION_STATES:raise MineralsQuarantineError('construction_subset_state_coverage_drift')
    return subset


def inspect_captures(captures):
    if set(captures)!=set(PINS):raise MineralsQuarantineError('package_capture_roster_drift')
    for name,(body,receipt) in captures.items():
        validate_capture(name,body,receipt,PINS[name]['url'])
        if digest(body)!=PINS[name]['sha256'] or len(body)!=PINS[name]['bytes']:raise MineralsQuarantineError('pinned_package_drift:'+name)
    members=decode_mineplant_zip(captures['usgs_mineplant_zip'][0])
    if members['mineplant.prj'].decode('ascii')!=MINEPLANT_PRJ+'\n':raise MineralsQuarantineError('usgs_native_prj_drift')
    rows=decode_mineplant_dbf(members['mineplant.dbf'])
    crosscheck_mineplant_txt(members['mineplant.txt'],rows)
    geometry=decode_points(members['mineplant.shp'],members['mineplant.shx'],MINEPLANT_RECORDS)
    verify_mineplant_geometry_values(rows,geometry)
    subset=construction_rows(rows)
    fields=[pa.field('source_fid',pa.int64(),nullable=False,metadata={b'identity':b'Original zero-based USGS mineplant SHP/DBF physical record index of the construction subset row, not a published or replacement feature ID'}),pa.field('source_shp_record_number',pa.int32(),nullable=False,metadata={b'identity':b'Original USGS mineplant SHP record-header number'})]
    for name,kind,width,scale in MINEPLANT_DBF_FIELDS:fields.append(pa.field(name,pa.string() if kind=='C' else pa.decimal128(width,scale),nullable=False,metadata={b'original_DBF_physical_declaration':encode({'type':kind,'width':width,'decimals':scale})}))
    fields.append(pa.field('geometry',pa.binary(),nullable=False))
    table=pa.Table.from_pylist([{**{f[0]:row[f[0]] for f in MINEPLANT_DBF_FIELDS},'source_fid':row['source_fid'],'source_shp_record_number':row['source_shp_record_number'],'geometry':geometry[row['source_fid']]} for row in subset],schema=pa.schema(fields))
    baseline=pq.read_table(pa.BufferReader(captures['baseline_geoparquet'][0]));published=strict_json(captures['baseline_geojson'][0])
    if baseline.num_rows!=766 or len(published.get('features',[]))!=766 or set(baseline.column_names)!={'geometry','OBJECTID','bbox',*[f[0] for f in DBF_FIELDS]}:raise MineralsQuarantineError('complete_published_baseline_schema_or_coverage_drift')
    pubrows=baseline.to_pylist();geo={f['properties']['MINOP2X020']:f for f in published['features']}
    if set(geo)!=set(range(1,767)) or {r['MINOP2X020'] for r in pubrows}!=set(geo):raise MineralsQuarantineError('published_ID_roster_drift')
    with MemoryFile(captures['baseline_geopackage'][0]) as mem:
        with mem.open() as collection: gpkg_ids={int(f.properties['MINOP2X020']):int(f.id) for f in collection}
    with ZipMemoryFile(captures['baseline_shapefile'][0]) as mem:
        with mem.open() as collection: shp_ids={int(f.properties['MINOP2X020']):int(f.id) for f in collection}
    if set(gpkg_ids)!=set(geo) or set(shp_ids)!=set(geo):raise MineralsQuarantineError('published_native_file_ID_roster_drift')
    identity=[]
    for row in sorted(pubrows,key=lambda r:r['MINOP2X020']):
        f=geo[row['MINOP2X020']]
        if f['properties']['OBJECTID']!=row['OBJECTID']:raise MineralsQuarantineError('published_crossformat_ID_drift')
        identity.append({'published_MINOP2X020':row['MINOP2X020'],'published_OBJECTID':row['OBJECTID'],'published_geojson_FID':f['properties']['FID'],'published_geojson_feature_id':f['id'],'published_geopackage_storage_fid':gpkg_ids[row['MINOP2X020']],'published_shapefile_fid':shp_ids[row['MINOP2X020']]})
    published_ids=pa.Table.from_pylist(identity,schema=pa.schema([pa.field(n,pa.int64(),nullable=False) for n in identity[0]]))
    # IDs coexist in separate tables. This numeric equality is diagnostic, not an approved join or source-ID replacement.
    audit={'approved':False,'package_id':PACKAGE,'classification':'USGS-owned historical 2003 National Minerals Information Team survey publication documenting the assigned scope of the retired National Atlas construction-minerals layer; unapproved draft reconstruction, not the original layer and not a current census','count':CONSTRUCTION_ROWS,'states':CONSTRUCTION_STATES,'commodities':dict(collections.Counter(row['COMMODITY'] for row in subset)),
           'original_DBF_fields':MINEPLANT_DBF_FIELDS,'native_crs_claim':MINEPLANT_PRJ,'contradictory_original_USGS_metadata_datum':'North American Datum of 1983','datum_resolution':'UNAPPROVED; coordinates unchanged as provided; each product PRJ and metadata claim as provided only; no correction/transform',
           'physical_numeric_decode':'Exact DBF base10 decimal values preserved (stored exponents vary up to the declared 5-decimal scale); SHP point doubles equal the DBF values; no binary floating substitution','source_ids':'original USGS mineplant id plus original mineplant SHP/DBF record indices and record numbers, all distinct and unreplaced','published_identifiers':'complete published MINOP2X020/OBJECTID and GeoJSON FID/featureid separately retained as comparison context, no mapping approved','release':'USGS mineplant FGDC publication 2005 of the 2003 Minerals Information Team survey, the same survey the retired layer publication 200512 documents; exact release day unknown'}
    return table,published_ids,audit


def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_wkt(MINEPLANT_PRJ).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'candidate_scope':PACKAGE.encode()})


def parquet_bytes(table):
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd');return sink.getvalue().to_pybytes()


def verify_outputs(root,table,published_ids):
    expected=parquet_bytes(expected_parquet(table))
    for name in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if (root/name).read_bytes()!=expected or not pq.read_table(root/name).equals(expected_parquet(table),check_metadata=True):raise MineralsQuarantineError('full_native_source_values_IDs_decimal_types_geometry_metadata_or_parquet_bytes_drift')
    if (root/'candidate/published-identifiers.parquet').read_bytes()!=parquet_bytes(published_ids):raise MineralsQuarantineError('complete_separate_published_identifiers_drift')


def manifest_for(captures,identity,parquet_pin):
    inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,PINS[name]['url'])
        if name in INPUT_NAMES:
            role='producer_publication';use='candidate_input'
            producer='U.S. Geological Survey (National Minerals Information Center historical 2003 survey distribution)' if name!='legacy_metadata' else 'U.S. Geological Survey (National Atlas legacy metadata for the retired construction-minerals layer)'
            attribution='USGS MRDATA mineplant historical 2003 survey package and USGS National Atlas legacy metadata; the pinned USGS metadata establishes the exact assigned construction-minerals dataset family and its 200512 publication. No contract or release acceptance.'
        else:
            role='archive';use='comparison_only_retained_prior_research'
            producer='HIFLD Next existing published candidate, acceptance unresolved'
            attribution='Existing published HIFLD v1 objects retained solely to preserve the published identifier roster and comparison context. Never a pipeline input.'
        inputs.append({'input_id':name+':'+digest(body),'producer':producer,'host':urlsplit(receipt['url']).hostname,'role':role,'pipeline_use':use,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':attribution,'release_date_unknown_reason':'USGS mineplant FGDC publication year 2005 and National Atlas layer publication 200512 have no exact release day; retrieval and portal-edit times are not source release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],'title':'Construction Minerals Operations — historical USGS 2003 survey reconstruction (unapproved draft)','publisher':'U.S. Geological Survey Mineral Resources Program (MRDATA historical 2003 survey publication)','agency':'U.S. Geological Survey','source_url':'https://mrdata.usgs.gov/mineplant/','license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'explicit_historical_usgs_survey_native_lossless_reconstruction','parameters':{'package_id':PACKAGE,'input_selection':'explicit local USGS package required, no default fetch or polling; only USGS-owned objects are candidate inputs','assigned_scope':'construction minerals operations as documented by the pinned USGS National Atlas metadata: the eight USGS commodity classes ball clay, cement, common clay and shale, dimension stone, gypsum, mica, perlite and pumice, excluding construction sand and gravel and crushed stone','subset_selection':'rows of the USGS mineplant file whose COMMODITY equals one of the eight assigned classes; pinned completeness 765 rows in 46 states','geometry':'original USGS mineplant SHP coordinates and own NAD27 PRJ as provided; SHP point doubles equal the DBF values; contradictory NAD83 claim of the retained USGS National Atlas legacy metadata retained and unresolved','IDs':'USGS mineplant id and original mineplant SHP/DBF record indices preserved; complete published identifiers separately retained, no approved cross-package mapping'}}],'unsupported_output_formats':{'shapefile_zip':'No accepted source/published schema mapping; the original USGS mineplant package is retained losslessly as raw evidence, not a newly approved output.','geopackage':'DBF decimal128 precision/scale and full Arrow physical schema cannot be silently coerced to GPKG REAL/INTEGER; no migration approved.','pmtiles':'Reprojection/quantization and datum choice are unapproved; native coordinates retained.'},'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all source decimal physical declarations, original IDs, strings, native WKB and metadata, no generic conversion or schema expansion. This is capability only, not input/contract/release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def source_payloads(captures,identity,audit,manifest,table,published_ids):
    proof={'status':'pass','approved':False,'rows':CONSTRUCTION_ROWS,'method':'All USGS mineplant DBF records and physical declarations decoded and cross-checked against the producer TXT; construction subset selected; SHP/SHX native points decoded and tied to DBF values; all765values/IDs/decimaltypes/nativeWKB/schema metadata and exact deterministic Parquet bytes verified; full original USGS package retained. No source authority/datum/compatibility acceptance.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/construction_minerals_source_audit.json':encode(audit),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_historical_usgs_input_authority_datum_schema_ID_mapping_and_baseline_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b'','candidate/published-identifiers.parquet':parquet_bytes(published_ids)}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,version,table,published_ids,audit,captures,identity,pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise MineralsQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table,published_ids);names={*expected,'candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise MineralsQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise MineralsQuarantineError('unknown_missing_or_linked_candidate_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise MineralsQuarantineError('invalid_staged_release_derivatives') from exc
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise MineralsQuarantineError('source_owned_report_raw_provenance_or_published_ID_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise MineralsQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise MineralsQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise MineralsQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table,published_ids)


def stage_construction_minerals(storage,*,capture_dir=None,package_id=None):
    local_only(storage);captures=acquire(storage,capture_dir,package_id)
    try:
        table,published_ids,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({name:digest(body) for name,(body,_) in captures.items()}))
        version=f'v0.0.0-draft-usgs-mineplant-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise MineralsQuarantineError('linked_candidate')
                    body=parquet_bytes(expected_parquet(table));pin={'sha256':digest(body),'bytes':len(body)}
                    reconcile_cache(storage,prefix,version,table,published_ids,audit,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise MineralsQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':CONSTRUCTION_ROWS,'reused':True,'package_id':PACKAGE,'accepted':False}
            with tempfile.TemporaryDirectory(prefix='.minerals-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                body=parquet_bytes(expected_parquet(table));(private/f'geoparquet/{SLUG}.parquet').write_bytes(body);(private/'candidate/typed-source.parquet').write_bytes(body);pin={'sha256':digest(body),'bytes':len(body)}
                manifest=manifest_for(captures,identity,pin)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'package_id':PACKAGE,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():
                    if (private/name).read_bytes()!=body:raise MineralsQuarantineError('incomplete_private_source_payload:'+name)
                # Validate source semantics + exact Git identity before the first seal;
                # the complete private candidate exists and has been verified first.
                validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY};(private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':CONSTRUCTION_ROWS,'reused':False,'package_id':PACKAGE,'accepted':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise MineralsQuarantineError(str(exc)+'; retained:'+retained) from exc
