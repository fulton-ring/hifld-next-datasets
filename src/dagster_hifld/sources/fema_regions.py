"""Faithful draft of the ten official FEMA Region polygon boundaries.

Direct FEMA producer publication; no source/baseline/contract/release acceptance.
All original IDs, field names/types, native mixed polygon geometry and raw bytes
are preserved. Historical publication compatibility is a separate hard gate.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import importlib.metadata
import io
import json
import math
import os
import platform
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import requests
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4
import fiona
from fiona.io import MemoryFile
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from shapely.geometry import shape
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle,load_first_construction,seal_first_construction
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance,verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource,StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _validate_cached_catalog_sidecars
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.fema_regions_spec import SLUG,PINS,FIELDS,QUERY_FIELDS,SOURCE_ATTRIBUTES,SOURCE_GEOMETRY
READY='metadata/fema_regions_ready.json'
LAYER='fema_regions_native'
class FemaQuarantineError(ValueError):
    """Incomplete, changed or untrusted inputs/caches cannot be promoted."""


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()

def digest(body):
    return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result: raise FemaQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    value = json.loads(body, object_pairs_hook=unique,
                       parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if type(value) is not dict or 'error' in value:
        raise FemaQuarantineError('expected_nonerror_json_object')
    return value

def local_root(storage):
    # Reject publication/remote targets before source reads or any mutation.
    if (isinstance(storage, PublishedStorageResource) or not isinstance(storage, StagingStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise FemaQuarantineError('explicit_unprefixed_local_staging_required')
    path = Path(storage.local_dir)
    if '..' in path.parts: raise FemaQuarantineError('unsafe_local_root')
    path = path.absolute()
    descriptors = [os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    try:
        for part in path.parts[1:]:
            try: os.mkdir(part, mode=0o700, dir_fd=descriptors[-1])
            except FileExistsError: pass
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        if shutil.disk_usage(path).free < 300_000_000:
            raise FemaQuarantineError('local_space_guard')
    finally:
        for descriptor in reversed(descriptors): os.close(descriptor)
    return path

def safe_tree(path):
    if path.is_symlink(): raise FemaQuarantineError('linked_candidate_root')
    for item in path.rglob('*'):
        info = item.lstat()
        if item.is_symlink() or (not item.is_dir() and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1)):
            raise FemaQuarantineError('linked_or_special_candidate_object')

def checked_path(storage, key):
    root = local_root(storage); path = root/key
    if PurePosixPath(key).is_absolute() or '..' in PurePosixPath(key).parts:
        raise FemaQuarantineError('unsafe_local_object_key')
    for item in [path,*path.parents]:
        if item.is_symlink(): raise FemaQuarantineError('linked_local_object_ancestor')
    return path

def immutable(storage, key, body):
    local_root(storage)
    path = Path(storage.local_dir)/key
    for ancestor in [path, *path.parents]:
        if ancestor.is_symlink(): raise FemaQuarantineError('linked_capture_path')
    if path.exists() and (not path.is_file() or path.stat().st_nlink != 1):
        raise FemaQuarantineError('linked_capture_object')
    snapshot = storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise FemaQuarantineError('immutable_capture_changed')

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
        raise FemaQuarantineError('linked_or_special_source_capture')
    if observed.st_size > limit: raise FemaQuarantineError('source_capture_size_limit')
    descriptor = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        current = os.fstat(descriptor)
        if (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_nlink) != (observed.st_dev,observed.st_ino,observed.st_size,observed.st_mtime_ns,1):
            raise FemaQuarantineError('source_capture_changed_before_read')
        with os.fdopen(os.dup(descriptor),'rb') as stream: body = stream.read(limit+1)
        after = os.fstat(descriptor)
        if len(body) > limit: raise FemaQuarantineError('source_capture_size_limit')
        if (after.st_size,after.st_mtime_ns,after.st_nlink) != (current.st_size,current.st_mtime_ns,1) or len(body) != current.st_size:
            raise FemaQuarantineError('source_capture_changed_during_read')
        return body
    finally: os.close(descriptor)

def processing_identity():
    root = Path(__file__).parents[3]
    return {'python':platform.python_version(), 'implementation':platform.python_implementation(),
            'platform':platform.platform(), 'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
            'gdal':pyogrio.__gdal_version_string__, 'geos':shapely.geos_version_string, 'proj':pyproj.proj_version_str,
            'verifier_sha256':verifier_implementation_sha256(),
            'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']}}

def parquet_bytes(table):
    stream = pa.BufferOutputStream(); pq.write_table(table,stream,compression='zstd')
    return stream.getvalue().to_pybytes()

@contextmanager
def version_lock(storage, version):
    folder = checked_path(storage,'_locks'); folder.mkdir(exist_ok=True)
    if folder.is_symlink(): raise FemaQuarantineError('linked_version_lock_directory')
    descriptor = os.open(folder/(SLUG+'-'+version+'.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1: raise FemaQuarantineError('linked_version_lock')
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
    except FileNotFoundError as exc: raise FemaQuarantineError('incomplete_observed_trust_loss_journal') from exc
    value = strict_json(body); claim = value.get('observed_cached_manifest_sha256')
    if claim is not None and (type(claim) is not str or re.fullmatch('[0-9a-f]{64}',claim) is None):
        raise FemaQuarantineError('invalid_observed_trust_loss_claim')
    expected = {'schema_version':1,'version':version,'processing_fingerprint':fp,
        'status':'observed_original_construction_trust_loss','approved':False,'observed_cached_manifest_sha256':claim}
    if body != encode(expected): raise FemaQuarantineError('invalid_observed_trust_loss_journal')
    if restored_manifest is not None and claim is not None and digest(encode(restored_manifest)) != claim:
        raise FemaQuarantineError('restored_original_manifest_differs_from_observed_candidate_claim')
    return True


def acquire(capture_directory,storage=None):
    root=Path(capture_directory).absolute(); captures={};pieces={}
    if '..' in root.parts or any(p.is_symlink()for p in [root,*root.parents]):
        raise FemaQuarantineError('unsafe_capture_directory')
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
            raise FemaQuarantineError(str(exc)+';untrusted_diagnostic:'+location)from exc
        raise


def validate_capture(name,body,receipt):
    pin=PINS[name]
    if (len(body)!=pin['bytes'] or digest(body)!=pin['sha256']
        or receipt.get('requested_url')!=pin['requested_url']
        or receipt.get('observed_final_url')!=pin['observed_final_url']
        or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body)or receipt.get('complete')is not True):
        raise FemaQuarantineError('incomplete_or_changed_pinned_capture:'+name)
    for timestamp in [receipt['started_at'],receipt['observed_at']]:
        if type(timestamp)is not str or re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})',timestamp)is None:
            raise FemaQuarantineError('strict_aware_ISO_capture_time_required')
        if int(timestamp[11:13])>23 or int(timestamp[14:16])>59 or int(timestamp[17:19])>59:
            raise FemaQuarantineError('invalid_capture_time_components')
        if not timestamp.endswith('Z')and(int(timestamp[-5:-3])>23 or int(timestamp[-2:])>59):
            raise FemaQuarantineError('invalid_capture_timezone_components')
    start=datetime.fromisoformat(receipt['started_at']);end=datetime.fromisoformat(receipt['observed_at'])
    if start.utcoffset()is None or end.utcoffset()is None or start>end:
        raise FemaQuarantineError('invalid_capture_observation_interval')


def body_for(name,captures):
    body=bounded_regular_read(captures[name][0],PINS[name]['bytes'])
    validate_capture(name,body,captures[name][1]);return body


def native_rings(geometry):
    if geometry.geom_type=='Polygon':
        return [tuple(geometry.exterior.coords)]+[tuple(r.coords)for r in geometry.interiors]
    if geometry.geom_type=='MultiPolygon':return [r for polygon in geometry.geoms for r in native_rings(polygon)]
    raise FemaQuarantineError('unexpected_native_geometry_type')


def validate_snapshot(captures):
    def obj(name):return strict_json(body_for(name,captures))
    before=obj('official-layer');after=obj('official-layer-after')
    if encode(before)!=encode(after) or encode(before.get('fields'))!=encode(FIELDS) or before.get('id')!=2 or before.get('name')!='FEMA Regions':
        raise FemaQuarantineError('official_layer_schema_or_observed_snapshot_drift')
    if (before.get('geometryType')!='esriGeometryPolygon' or before.get('objectIdField')!='objectid'
        or before.get('hasZ')is not False or before.get('hasM')is not False
        or before.get('extent',{}).get('spatialReference',{}).get('wkid')!=4326):
        raise FemaQuarantineError('native_geometry_crs_or_dimensions_drift')
    for name in ['source-count','source-count-after']:
        if encode(obj(name))!=encode({'count':10}):raise FemaQuarantineError('complete_population_count_drift')
    for name in ['source-ids','source-ids-after']:
        values=obj(name)
        if values.get('objectIdFieldName')!='objectid'or encode(values.get('objectIds'))!=encode(list(range(11,21))):
            raise FemaQuarantineError('source_identifier_census_drift')
    attrs=[]
    for name in ['source-attributes','source-attributes-after']:
        values=obj(name)
        if values.get('exceededTransferLimit')is True or encode(values.get('fields'))!=encode(QUERY_FIELDS):
            raise FemaQuarantineError('incomplete_attribute_query_or_physical_schema_drift')
        attrs.append([f['attributes']for f in values['features']])
    if encode(attrs[0])!=encode(attrs[1])or encode(attrs[0])!=encode([SOURCE_ATTRIBUTES[x]for x in range(11,21)]):
        raise FemaQuarantineError('observed_attribute_snapshot_drift')
    return before


def decode_native(captures):
    metadata=validate_snapshot(captures);rows=[];audit=[]
    for oid in range(11,21):
        name='source-oid-'+str(oid);body=body_for(name,captures);value=strict_json(body)
        if (value.get('exceededTransferLimit')is True or encode(value.get('fields'))!=encode(QUERY_FIELDS)
            or value.get('spatialReference',{}).get('wkid')!=4326 or len(value.get('features',[]))!=1):
            raise FemaQuarantineError('incomplete_native_per_identifier_query')
        feature=value['features'][0];attributes=feature['attributes']
        if encode(attributes)!=encode(SOURCE_ATTRIBUTES[oid])or set(feature['geometry'])!={'rings'}:
            raise FemaQuarantineError('source_attribute_or_geometry_encoding_drift')
        raw=feature['geometry']['rings']
        if any(len(p)!=2 or any(type(n)not in[int,float]or not math.isfinite(n)for n in p)for r in raw for p in r):
            raise FemaQuarantineError('native_coordinate_dimensions_or_nonfinite_value')
        with MemoryFile(body,ext='.json')as memory:
            with memory.open(driver='ESRIJSON')as layer:
                record=next(iter(layer));geometry=shape(record['geometry'])
                if layer.crs.to_epsg()!=4326:raise FemaQuarantineError('decoded_native_crs_disagrees')
        original=[tuple(tuple(p)for p in ring)for ring in raw]
        if collections.Counter(original)!=collections.Counter(native_rings(geometry)):
            raise FemaQuarantineError('source_ordered_ring_or_vertex_loss')
        wkb=shapely.to_wkb(geometry);spec=SOURCE_GEOMETRY[oid]
        if (len(raw)!=spec['rings']or sum(map(len,raw))!=spec['vertices']
            or geometry.geom_type!=spec['source_geometry_type']or digest(wkb)!=spec['native_WKB_sha256']):
            raise FemaQuarantineError('native_geometry_or_coverage_drift')
        rows.append({**attributes,'geometry':wkb})
        audit.append({'objectid':oid,'region':attributes['region'],'cnt_region':attributes['cnt_region'],
            'rings':len(raw),'vertices':sum(map(len,raw)),'ordered_native_rings_and_vertices_exact':True,
            'geometry_type':geometry.geom_type,'native_WKB_sha256':digest(wkb),'valid':geometry.is_valid,'bounds':list(geometry.bounds)})
    return native_table(rows,[r['geometry_type']for r in audit]),audit,metadata


def native_table(rows,geometry_types):
    fields=[]
    for field in FIELDS:
        arrow={'esriFieldTypeOID':pa.int32(),'esriFieldTypeInteger':pa.int32(),
            'esriFieldTypeString':pa.string(),'esriFieldTypeDouble':pa.float64()}[field['type']]
        fields.append(pa.field(field['name'],arrow,nullable=field['nullable']))
    fields.append(pa.field('geometry',pa.binary(),nullable=True))
    table=pa.Table.from_pylist(rows,schema=pa.schema(fields))
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB',
        'geometry_types':sorted(set(geometry_types)),'crs':pyproj.CRS(4326).to_json_dict()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def compare_published(captures,table,audit):
    baseline=pq.read_table(pa.BufferReader(body_for('baseline-parquet',captures)))
    if len(baseline)!=10 or set(baseline.column_names)!={'geometry','objectid','region','Shape__Are','Shape__Len','cnt_region','website','id','bbox'}:
        raise FemaQuarantineError('historical_published_schema_or_population_drift')
    old={r['objectid']:r for r in baseline.to_pylist()}
    if set(old)!=set(range(11,21))or sorted(r['id']for r in old.values())!=list(range(1,11)):
        raise FemaQuarantineError('historical_original_identifier_drift')
    mapping={'objectid':'objectid','region':'region','Shape__Area':'Shape__Are','Shape__Length':'Shape__Len','cnt_region':'cnt_region','website':'website'}
    comparisons=[]
    for row,detail in zip(table.to_pylist(),audit):
        before=old[row['objectid']]
        if any(row[k]!=before[v]for k,v in mapping.items()):raise FemaQuarantineError('historical_attribute_relationship_drift')
        comparisons.append({'original_source_objectid':row['objectid'],'original_published_id':before['id'],
            'all6_attribute_values_match_under_UNAPPROVED_diagnostic_name_mapping':True,
            'source_geometry_type':detail['geometry_type'],'published_geometry_type':shapely.from_wkb(before['geometry']).geom_type,
            'ordered_native_WKB_equals_published':row['geometry']==before['geometry'],
            'source_WKB_sha256':digest(row['geometry']),'published_WKB_sha256':digest(before['geometry'])})
    return {'schema_version':1,'approved':False,'status':'blocked','acceptance':'not_established',
        'reason_codes':['source_release_baseline_contract_rights_unapproved','published_ID_mapping_unapproved',
            'physical_int32_int64_and_full_area_length_field_names_unapproved','two_Polygon_MultiPolygon_publication_wrappings_unapproved'],
        'populations':{'native_rows':10,'published_rows':10},'all_rows':comparisons,
        'historical_schema':str(baseline.schema),'source_schema':str(table.schema),
        'source_quality':{'complete_population':10,'invalid_geometries':sum(not r['valid']for r in audit)},
        'other_complete_published_format_pins':{n:PINS[n]for n in PINS if n.startswith('baseline-')},
        'scope':'Full Parquet values/IDs/schema/geometry comparison. All five complete published format bytes preserved/pinned; GeoJSON/GPKG/Shapefile audit is a separate reproducible full input audit, PMTiles is a cartographic tile archive, not a lossless row baseline.'}


def write_geopackage(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='geometry',geometry_type='Unknown',
        crs='EPSG:4326',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'geometry'})
    with sqlite3.connect(path)as db:db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")


def reconcile_native(root,table):
    actual=pq.read_table(root/'geoparquet'/f'{SLUG}.parquet')
    if not actual.equals(table,check_metadata=True):raise FemaQuarantineError('native_full_Parquet_rows_types_ids_geometry_changed')
    metadata,physical=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    geometry_name=metadata['geometry_name']
    if not pyproj.CRS(metadata['crs']).equals(pyproj.CRS(4326))or len(physical)!=10:
        raise FemaQuarantineError('native_GPKG_crs_or_census_changed')
    for name in table.column_names:
        observed=physical[geometry_name if name=='geometry'else name]
        if observed.type!=table[name].type or not observed.equals(table[name]):
            raise FemaQuarantineError('native_GPKG_full_field_type_null_value_or_WKB_changed:'+name)
    with tempfile.TemporaryDirectory(prefix='fema-fresh-native-')as directory:
        fresh=Path(directory)/'fresh.gpkg';write_geopackage(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise FemaQuarantineError('fresh_complete_GPKG_bytes_disagree')


def manifest_for(captures,identity,parquet):
    inputs=[]
    for name in PINS:
        receipt=captures[name][1];archive=name.startswith('baseline-');pin=PINS[name]
        inputs.append({'input_id':name+':'+pin['sha256'],
            'producer':'FEMA'if not archive else'Prior HIFLD Next publication of FEMA-attributed data; baseline not accepted',
            'host':urlsplit(pin['requested_url']).hostname,'role':'archive'if archive else'producer_publication',
            'canonical_url':pin['requested_url'],'retrieval_url':pin['requested_url'],'observed_final_url':pin['observed_final_url'],
            'retrieved_at':receipt['observed_at'],'http_status':200,'bytes':pin['bytes'],'sha256':pin['sha256'],
            'license':'Original FEMA service credit retained; exact rights/release acceptance pending.'if not archive else'Preserved prior-publication reference; no new rights or acceptance inferred.',
            'attribution':'FEMA producer publication via gis.fema.gov; HIFLD Next processes reconstruction only.'if not archive else'Prior HIFLD Next artifact, original FEMA attribution and all source/published IDs retained.',
            'release_date_unknown_reason':'No record release/edit timestamp is exposed by the live service. Metadata creation2020-10-16 and observation time are not release dates.'if not archive else'Pinned historical publication candidate; its presence or storage modification time is not baseline acceptance.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'title':'FEMA Regions — faithful official-source draft',
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'decode_complete_native_ESRIJSON','parameters':{'source_geometry_member':'geometry','source_layer_geometry_field':'shape',
            'canonical_geometry_column':'geometry','native_CRS':'EPSG:4326','source_ID':'objectid retained exactly; separate prior id not substituted',
            'physical_types':'esriFieldTypeOID/Integer→int32, Double→float64, String→UTF8; original declarations/length/nullability retained in sidecar',
            'geometry':'all ordered rings/vertices retained; no projection, densification, coordinate rounding, orientation change, Polygon wrapping or repair',
            'coherence':'before/after full schema/count/ID/attribute observations equal; no transactional snapshot/atomicity claimed'}},
            {'operation':'native_candidate_construction','parameters':{'contract_approved':False,'published_schema_mapping_approved':False,'source_release_approved':False}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Retain every native ID/field/type and mixed Polygon/MultiPolygon geometry without historical schema/ID conversion.',
            'objects':[{'path':'geoparquet/'+SLUG+'.parquet','bytes':len(parquet),'sha256':digest(parquet)}]}}},
        'unsupported_output_formats':{'shapefile_zip':'Native Shape__Area/Shape__Length names cannot be retained in Shapefile; no approved truncation/type/ID mapping.',
            'pmtiles':'No approved cartographic field/ID/geometry contract; preserve native lossless outputs and all original baseline PMTiles bytes.'}}
    validate_release_provenance(manifest);return manifest


def source_payloads(captures,identity,table,audit,metadata,comparison,manifest):
    parquet=parquet_bytes(table)
    return {'geoparquet/'+SLUG+'.parquet':parquet,
        'metadata/source_manifest.json':encode(manifest),'metadata/baseline_comparison.json':encode(comparison),
        'candidate/native_schema_and_coverage.json':encode({'schema_version':1,'approved':False,'source_layer_metadata':metadata,
            'full_population_native_audit':audit,'full_native_schema':str(table.schema),'original_input_pins':PINS,
            'published_ID_compatibility':'Every original id is retained in the complete baseline archive/report; no extra main-table published id mapping is approved.'}),
        'candidate/processing_identity.json':encode(identity),
        'candidate/raw_input_inventory.json':encode({n:{'raw_body_key':f'_raw-captures/{SLUG}/{p["sha256"]}/{n}.body',
            'receipt':captures[n][1],'bytes':p['bytes'],'sha256':p['sha256']}for n,p in PINS.items()})}


def original_context(storage,key,version,fp,captures,identity,table):
    raw=load_first_construction(storage,key,version,fp);payload=json.loads(raw)['payload']
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(PINS):
        raise FemaQuarantineError('original_context_closed_roster_drift')
    original={}
    for name,claim in payload['captures'].items():
        pin=PINS[name];expected=f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body'
        receipt_key=expected+'.receipt-'+digest(encode(claim.get('receipt')))+'.json'
        if set(claim)!={'raw_body_key','raw_receipt_key','receipt'}or claim['raw_body_key']!=expected or claim['raw_receipt_key']!=receipt_key:
            raise FemaQuarantineError('original_raw_capture_identity_drift')
        if bounded_regular_read(checked_path(storage,receipt_key),128_000)!=encode(claim['receipt']):
            raise FemaQuarantineError('original_receipt_outside_bundle_missing_or_changed')
        path=checked_path(storage,expected);body=bounded_regular_read(path,pin['bytes'])
        validate_capture(name,body,claim['receipt']);original[name]=(path,claim['receipt'])
    expected=manifest_for(original,identity,parquet_bytes(table));manifest=payload['manifest']
    validate_release_provenance(manifest)
    expected['code_commit']=manifest.get('code_commit')
    if encode(expected)!=encode(manifest):
        raise FemaQuarantineError('original_full_construction_manifest_changed')
    validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
    return original,manifest


def reconcile_cache(storage,prefix,expected,table,version,fp):
    root=checked_path(storage,prefix);safe_tree(root);ready_body=bounded_regular_read(root/READY,128_000);ready=strict_json(ready_body)
    owned=set(expected)|{'geopackage/'+SLUG+'.gpkg'}
    if (set(ready)!={'version','processing_fingerprint','approved','objects'}or ready['version']!=version
        or ready['processing_fingerprint']!=fp or ready['approved']is not False or set(ready['objects'])!=owned):
        raise FemaQuarantineError('ready_identity_or_closed_roster_changed')
    for name,body in expected.items():
        if (root/name).stat().st_size!=len(body)or sha256_file(root/name)!=digest(body):
            raise FemaQuarantineError('full_source_owned_payload_changed:'+name)
    canonical={'version':version,'processing_fingerprint':fp,'approved':False,
        'objects':{n:{'sha256':sha256_file(root/n),'bytes':(root/n).stat().st_size}for n in owned}}
    if ready_body!=encode(canonical):raise FemaQuarantineError('repinned_noncanonical_or_changed_ready_receipt')
    manifest=strict_json(expected['metadata/source_manifest.json']);snapshot='metadata/source/version_'+digest(expected['metadata/source_manifest.json'])+'_source_manifest.json'
    downstream={'metadata/source/source_manifest.json',snapshot,'metadata/source/data_dictionary.json','metadata/source/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/quality_manifest.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json'}
    actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()}
    if actual-(owned|{READY}|downstream)or not(owned|{READY}).issubset(actual):
        raise FemaQuarantineError('unrostered_or_missing_candidate_object')
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists()and (root/name).read_bytes()!=expected['metadata/source_manifest.json']:
            raise FemaQuarantineError('authored_manifest_snapshot_changed')
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
        if resolved.get('inputs')!=manifest['inputs']or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved:
            raise FemaQuarantineError('resolved_provenance_changed')
    # Catalog metadata is downstream-owned, but it still must be recomputed
    # from this exact native candidate before a source-cache hit is trusted.
    _validate_cached_catalog_sidecars(storage,prefix,set(storage.list_prefix(prefix)))
    reconcile_native(root,table)


HTTP_CHUNK_LIMIT=1_000_000


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
            if len(chunk)>HTTP_CHUNK_LIMIT:raise FemaQuarantineError('transport_violated_bounded_read_contract_after_preservation')
            if count>limit:raise FemaQuarantineError('live_response_size_drift_incomplete_prefix_preserved')
        return count
    finally:os.close(descriptor)


def capture_live_observation(storage,captures):
    """Anonymous bounded current extraction; no baseline refetch/overwrite."""
    folder=checked_path(storage,'_observations/'+SLUG+'/'+uuid4().hex);folder.mkdir(parents=True)
    names=['official-layer','source-count','source-ids','source-attributes']+['source-oid-'+str(x)for x in range(11,21)]+['official-layer-after','source-count-after','source-ids-after','source-attributes-after']
    observed=dict(captures)
    for name in names:
        pin=PINS[name];start=datetime.now(timezone.utc).isoformat();path=folder/(name+'.body')
        progress={'requested_url':pin['requested_url'],'started_at':start,'complete':False,
            'trust':'untrusted_capture_in_progress','body_path':path.name,'bounded_read_chunk_limit':HTTP_CHUNK_LIMIT}
        # Establish untrusted status before any read, including interruption.
        (folder/(name+'.failed-receipt.json')).write_bytes(encode(progress))
        try:
            with requests.get(pin['requested_url'],headers={'Accept-Encoding':'identity','User-Agent':'HIFLD-Next-FEMA-regions-draft'},stream=True,timeout=120)as response:
                progress.update(observed_final_url=response.url,status=response.status_code)
                count=persist_response_stream(storage,response,path,pin['bytes'])
                if response.headers.get('Content-Encoding','identity').lower()not in['identity','']:
                    raise FemaQuarantineError('unexpected_live_content_encoding')
                declared=response.headers.get('Content-Length')
                if declared is not None and int(declared)!=count:raise FemaQuarantineError('incomplete_live_transport')
                body=bounded_regular_read(path,pin['bytes'])
                receipt={'requested_url':pin['requested_url'],'observed_final_url':response.url,'started_at':start,
                    'observed_at':datetime.now(timezone.utc).isoformat(),'status':response.status_code,
                    'bytes':len(body),'wire_bytes':count,'sha256':digest(body),'complete':True,
                    'headers':{k:v for k,v in response.headers.items()if k.lower()in['content-type','content-length','etag','last-modified','date','content-encoding']}}
            validate_capture(name,body,receipt)
            (folder/(name+'.receipt.json')).write_bytes(encode(receipt));observed[name]=(path,receipt)
            (folder/(name+'.failed-receipt.json')).unlink()
        except BaseException as exc:
            # Do not call guarded storage helpers here: that could refuse to
            # preserve a consumed chunk just because the floor was crossed.
            progress.update(observed_at=datetime.now(timezone.utc).isoformat(),error=str(exc),
                trust='untrusted_failed_capture',complete=False,persisted_bytes=path.stat().st_size if path.exists() else 0,
                persisted_sha256=sha256_file(path)if path.exists()else None)
            (folder/(name+'.failed-receipt.json')).write_bytes(encode(progress))
            raise
    return observed


def stage_fema_regions(storage,capture_directory,refresh_official=False):
    root=local_root(storage);captures=acquire(capture_directory,storage)
    if refresh_official:captures=capture_live_observation(storage,captures)
    identity=processing_identity();fp=digest(encode(identity))
    core=digest(encode({n:p['sha256']for n,p in PINS.items()}))
    version='v0.0.0-draft-fema-regions-'+core[:12]+'-'+fp[:16]
    prefix=SLUG+'/'+SLUG+'/'+version;target=checked_path(storage,prefix);key='_construction-state/'+SLUG+'/'+version+'.json'
    # The full original captures are immutable outside versioned candidates;
    # historical raw payloads and complete original receipts are never replaced.
    for name,pin in PINS.items():
        raw=f'_raw-captures/{SLUG}/{pin["sha256"]}/{name}.body'
        immutable(storage,raw,body_for(name,captures))
        receipt=captures[name][1];immutable(storage,raw+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
    table,audit,metadata=decode_native(captures);comparison=compare_published(captures,table,audit)
    parquet=parquet_bytes(table);manifest=manifest_for(captures,identity,parquet)
    with version_lock(storage,version):
        state=root/key;has_history=state.exists()or state.with_name(state.name+'.first-construction.json').exists()or state.with_name(state.name+'.evidence').exists()
        if target.exists():
            can_detach=True
            try:
                try:
                    captures,manifest=original_context(storage,key,version,fp,captures,identity,table)
                    observed_trust_loss(storage,version,fp,restored_manifest=manifest)
                except (ValueError,OSError,TypeError,KeyError,RuntimeError):
                    try:observed_trust_loss(storage,version,fp,candidate=target)
                    except (ValueError,OSError,TypeError,KeyError,RuntimeError):can_detach=False;raise
                    raise
                expected=source_payloads(captures,identity,table,audit,metadata,comparison,manifest)
                reconcile_cache(storage,prefix,expected,table,version,fp)
            except (ValueError,OSError,TypeError,KeyError,RuntimeError,sqlite3.Error)as exc:
                if not can_detach:raise FemaQuarantineError('trust_loss_journal_failed_candidate_retained_untrusted:'+str(exc))from exc
                rejected=checked_path(storage,'_quarantine/'+SLUG+'/invalid-candidate-'+uuid4().hex)
                rejected.parent.mkdir(parents=True,exist_ok=True);os.rename(target,rejected)
                raise FemaQuarantineError('invalid_candidate_preserved_outside_discovery:'+str(rejected)+':'+str(exc))from exc
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':10,'reused':True,
                'source_invalid_geometry_count':sum(not r['valid']for r in audit),'source_release_approved':False}
        if has_history or observed_trust_loss(storage,version,fp):
            captures,manifest=original_context(storage,key,version,fp,captures,identity,table)
            observed_trust_loss(storage,version,fp,restored_manifest=manifest)
        else:validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        expected=source_payloads(captures,identity,table,audit,metadata,comparison,manifest)
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        building=checked_path(storage,'_building/'+SLUG);building.mkdir(parents=True,exist_ok=True)
        temporary=Path(tempfile.mkdtemp(prefix='candidate-',dir=building))
        try:
            for name,body in expected.items():
                path=temporary/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
            (temporary/'geopackage').mkdir();write_geopackage(table,temporary/'geopackage'/f'{SLUG}.gpkg')
            reconcile_native(temporary,table)
            objects={str(p.relative_to(temporary)):{'sha256':sha256_file(p),'bytes':p.stat().st_size}for p in temporary.rglob('*')if p.is_file()}
            (temporary/READY).write_bytes(encode({'version':version,'processing_fingerprint':fp,'approved':False,'objects':objects}))
            bundle=canonical_construction_bundle(version,fp,{'manifest':manifest,'captures':{n:{
                'raw_body_key':f'_raw-captures/{SLUG}/{p["sha256"]}/{n}.body',
                'raw_receipt_key':f'_raw-captures/{SLUG}/{p["sha256"]}/{n}.body.receipt-'+digest(encode(captures[n][1]))+'.json',
                'receipt':captures[n][1]}for n,p in PINS.items()}})
            if seal_first_construction(storage,key,version,fp,bundle)!=bundle:
                raise FemaQuarantineError('concurrent_original_context_differs')
            target.parent.mkdir(parents=True,exist_ok=True);os.rename(temporary,target)
        except BaseException:
            if temporary.exists():
                rejected=checked_path(storage,'_quarantine/'+SLUG+'/failed-private-'+uuid4().hex)
                rejected.parent.mkdir(parents=True,exist_ok=True);os.rename(temporary,rejected)
            raise
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':10,'reused':False,
        'source_invalid_geometry_count':sum(not r['valid']for r in audit),'source_release_approved':False}
