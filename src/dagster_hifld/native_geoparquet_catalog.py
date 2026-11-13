"""Opt-in native GeoParquet catalog fallback, never dataset/source acceptance.

Only a single explicitly pinned prebuilt object is supported. Existing canonical
raw sources retain precedence. Memory is limited by Arrow batches plus a bounded
sample; Arrow may allocate one decoded batch before the byte ceiling is checked.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import stat
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path, PurePosixPath

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyproj import CRS
import shapely
from shapely.errors import GEOSException

from dagster_hifld.output_policy import prebuilt_output_spec
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS
from dagster_hifld.validation import sha256_file

SAMPLE_ROWS=5000
SAMPLE_BYTES=64*1024*1024
BATCH_ROWS=128
BATCH_BYTES=128*1024*1024
GEOMETRY_TYPES={'Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection'}


def _pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError('native_geoparquet_duplicate_metadata_member')
        result[key]=value
    return result


def _json(body):
    try:return json.loads(body,object_pairs_hook=_pairs,parse_constant=lambda v:(_ for _ in ()).throw(ValueError(v)))
    except(UnicodeError,RecursionError)as exc:raise ValueError('native_geoparquet_invalid_geo_JSON')from exc


def _roster(storage,prefix):
    directory=storage._ensure_prefixed(f'{prefix}/geoparquet/')
    return {key for key in storage.list_prefix(f'{prefix}/geoparquet')if key.startswith(directory)}


def _local_root(storage):
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts:raise ValueError('native_geoparquet_unsafe_local_root')
    for path in [*reversed(root.parents),root]:
        try:info=path.lstat()
        except FileNotFoundError:continue
        if not stat.S_ISDIR(info.st_mode):raise ValueError('native_geoparquet_aliased_local_root')
    return root


def _safe_local_key(storage,key,*,required=True):
    root=_local_root(storage)
    if PurePosixPath(key).is_absolute()or PurePosixPath(key).as_posix()!=key or any(p in('..','.')for p in PurePosixPath(key).parts):
        raise ValueError('native_geoparquet_unsafe_local_key')
    path=root/key
    for parent in reversed(path.parents):
        if parent==root or root in parent.parents:
            if parent.is_symlink():raise ValueError('native_geoparquet_aliased_local_path')
    try:info=path.lstat()
    except FileNotFoundError:
        if required:raise ValueError('native_geoparquet_missing_pinned_local_file')
        return path
    if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise ValueError('native_geoparquet_not_single_regular_file')
    return path


def _descriptor(storage,prefix,metadata):
    spec=prebuilt_output_spec(metadata,'geoparquet')
    if spec is None:return None
    if len(spec['objects'])!=1:raise ValueError('native_geoparquet_catalog_requires_single_pinned_object')
    obj=spec['objects'][0];key=storage._ensure_prefixed(f"{prefix}/{obj['path']}")
    if _roster(storage,prefix)!={key}:raise ValueError('native_geoparquet_complete_roster_changed')
    if storage.use_local or not storage.bucket:_safe_local_key(storage,key)
    snapshot=storage.object_snapshot(key)
    if snapshot is None or snapshot.size!=obj['bytes']:raise ValueError('native_geoparquet_pinned_size_changed')
    return obj,key,snapshot


def _manifest_snapshots(storage,metadata):
    layers=metadata.get('manifest_layers',[])
    if type(layers)is not list or len(layers)>3:raise ValueError('native_geoparquet_invalid_manifest_layers')
    pins=[]
    for layer in layers:
        if type(layer)is not dict:raise ValueError('native_geoparquet_invalid_manifest_layer')
        key=layer.get('manifest_key');expected=layer.get('sha256')
        if(type(key)is not str or not re.fullmatch(r'[A-Za-z0-9_./-]{1,1024}',key)
            or PurePosixPath(key).is_absolute()or PurePosixPath(key).as_posix()!=key
            or any(p in('..','.')for p in PurePosixPath(key).parts)
            or type(expected)is not str or not re.fullmatch('[0-9a-f]{64}',expected)):
            raise ValueError('native_geoparquet_unsafe_manifest_identity')
        if storage.use_local or not storage.bucket:_safe_local_key(storage,storage._ensure_prefixed(key))
        snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size>2*1024*1024:raise ValueError('native_geoparquet_missing_or_oversized_source_manifest')
        body=storage.read_key_range(key,0,snapshot.size,expected_snapshot=snapshot)
        if hashlib.sha256(body).hexdigest()!=expected:raise ValueError('native_geoparquet_source_manifest_identity_changed')
        pins.append((key,snapshot,expected))
    return pins


def _check_manifests(storage,pins):
    for key,snapshot,expected in pins:
        body=storage.read_key_range(key,0,snapshot.size,expected_snapshot=snapshot)
        if hashlib.sha256(body).hexdigest()!=expected:raise ValueError('native_geoparquet_source_manifest_identity_changed')


@contextmanager
def pinned_native_path(storage:StagingStorageResource,prefix:str,metadata:dict):
    """Materialize at most one exact object, with stable complete roster/bytes.

    This is integrity of an explicitly proposed input, not approval. Remote
    sensor/catalog reads hash real downloaded bytes and never trust a caller's
    SHA metadata as proof. They can require one full object read per validation.
    """
    selected=_descriptor(storage,prefix,metadata)
    if selected is None:
        yield None
        return
    obj,key,snapshot=selected
    manifests=_manifest_snapshots(storage,metadata)
    with tempfile.TemporaryDirectory(prefix='hifld-native-parquet-')as temporary:
        if storage.use_local or not storage.bucket:
            path=_safe_local_key(storage,key)
        else:
            path=Path(temporary)/'input.parquet'
            storage.download_key_to(key,path)
        def unchanged():
            if storage.use_local or not storage.bucket:_safe_local_key(storage,key)
            _check_manifests(storage,manifests)
            if(path.stat().st_size!=obj['bytes']or sha256_file(path)!=obj['sha256']
                or storage.object_snapshot(key)!=snapshot or _roster(storage,prefix)!={key}):
                raise ValueError('native_geoparquet_pinned_bytes_or_roster_changed')
        unchanged()
        yield path
        unchanged()


def geometry_metadata(schema):
    """Validate one WKB geometry column. Omitted CRS means normative CRS84."""
    if len(schema.names)!=len(set(schema.names)):raise ValueError('native_geoparquet_duplicate_columns')
    raw=(schema.metadata or{}).get(b'geo')
    if raw is None or len(raw)>2*1024*1024:raise ValueError('native_geoparquet_missing_or_oversized_geo_metadata')
    geo=_json(raw)
    if type(geo)is not dict or geo.get('version')not in('1.0.0','1.1.0'):raise ValueError('native_geoparquet_unsupported_geo_version')
    primary=geo.get('primary_column');columns=geo.get('columns')
    if type(primary)is not str or primary not in schema.names or type(columns)is not dict or set(columns)!={primary}:
        raise ValueError('native_geoparquet_requires_one_explicit_primary_geometry_column')
    column=columns[primary]
    if type(column)is not dict or column.get('encoding')!='WKB' or not(pa.types.is_binary(schema.field(primary).type)or pa.types.is_large_binary(schema.field(primary).type)):
        raise ValueError('native_geoparquet_requires_binary_WKB')
    kinds=column.get('geometry_types')
    if(type(kinds)is not list or any(type(v)is not str or v.removesuffix(' Z')not in GEOMETRY_TYPES for v in kinds)or len(kinds)!=len(set(kinds))):
        raise ValueError('native_geoparquet_invalid_declared_geometry_types')
    if column.get('edges','planar')!='planar':raise ValueError('native_geoparquet_spherical_edges_not_supported')
    if 'crs'in column and column['crs']is None:raise ValueError('native_geoparquet_explicit_null_crs_unknown')
    crs_value=column.get('crs','OGC:CRS84')
    if 'crs'in column and type(crs_value)is not dict:raise ValueError('native_geoparquet_crs_requires_PROJJSON')
    try:crs=CRS.from_user_input(crs_value)
    except Exception as exc:raise ValueError('native_geoparquet_unrecognized_crs')from exc
    bbox=column.get('bbox')
    if bbox is not None:
        if(type(bbox)is not list or len(bbox)not in(4,6)or any(type(v)not in(int,float)or not math.isfinite(v)for v in bbox)
            or any(a>b for a,b in zip(bbox[:len(bbox)//2],bbox[len(bbox)//2:]))):raise ValueError('native_geoparquet_invalid_declared_bbox')
    return primary,column,crs


def has_other_source(storage,prefix):
    """Preserve canonical/legacy/tabular precedence, including invalid sources."""
    if(type(prefix)is not str or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*',prefix)
       or any(p in('..','.')for p in prefix.split('/'))):raise ValueError('native_geoparquet_unsafe_version_prefix')
    if storage.use_local or not storage.bucket:_local_root(storage)
    root=storage._ensure_prefixed(prefix).rstrip('/')+'/'
    return any(key.startswith(root)and key[len(root):].split('/',1)[0]in(CANONICAL_SOURCE_FORMAT_DIRS|{'unknown'})
               for key in storage.list_prefix(prefix))


def resolve_native_metadata(storage,prefix):
    if storage.use_local or not storage.bucket:
        dataset,file,version=prefix.split('/')
        for key in [f'{dataset}/metadata/source_manifest.json',f'{dataset}/{file}/metadata/source_manifest.json',f'{prefix}/metadata/source_manifest.json']:
            _safe_local_key(storage,storage._ensure_prefixed(key),required=False)
    return load_resolved_source_manifest(storage,*prefix.split('/')).metadata


def discover_prebuilt_native(storage,prefix,metadata=None):
    """Pin/format eligibility only; never quality, source or contract approval."""
    if has_other_source(storage,prefix):return False
    if metadata is None:metadata=resolve_native_metadata(storage,prefix)
    if prebuilt_output_spec(metadata,'geoparquet')is None:return False
    with pinned_native_path(storage,prefix,metadata)as path:
        geometry_metadata(pq.ParquetFile(path).schema_arrow)
    return True


def _text(value):
    if isinstance(value,bytes):return value.hex()
    if isinstance(value,(Decimal,date,datetime,time)):return str(value)
    if isinstance(value,(list,tuple,dict)):return repr(value)
    return str(value)


def _category(dtype):
    if pa.types.is_decimal(dtype):return 'decimal'
    if pa.types.is_integer(dtype):return 'integer'
    if pa.types.is_floating(dtype):return 'float'
    if pa.types.is_boolean(dtype):return 'boolean'
    if pa.types.is_date(dtype):return 'date'
    if pa.types.is_timestamp(dtype):return 'timestamp'
    if pa.types.is_time(dtype):return 'time'
    if pa.types.is_binary(dtype)or pa.types.is_large_binary(dtype)or pa.types.is_fixed_size_binary(dtype):return 'binary'
    if pa.types.is_struct(dtype):return 'struct'
    if pa.types.is_list(dtype)or pa.types.is_large_list(dtype)or pa.types.is_fixed_size_list(dtype):return 'list'
    if pa.types.is_map(dtype):return 'map'
    return 'string'


def _dictionary(sample,schema,primary,name,metadata):
    columns=[]
    for field in schema:
        values=sample[field.name];category='geometry'if field.name==primary else _category(field.type)
        entry={'name':field.name,'type':category,'nativeArrowType':str(field.type),'nullable':field.nullable,
            'numNullValues':values.null_count,'statisticsScope':'bounded_arrow_sample',
            'originalArrowFieldMetadata':{k.hex():v.hex()for k,v in(field.metadata or{}).items()}}
        if field.name!=primary:
            nonnull=values.drop_null();examples=nonnull.slice(0,5).to_pylist();entry['exampleValues']=[_text(v)for v in examples]
            try:entry['numUniqueValues']=int(pc.count_distinct(values).as_py())
            except pa.ArrowException:entry['numUniqueValues']=None
            if pa.types.is_integer(field.type)or pa.types.is_floating(field.type):
                bounds=pc.min_max(values).as_py()
                # Arrow returns Python ints for native integer ranges. Never
                # route IDs through float, even for dictionary-only statistics.
                entry.update(min=bounds['min'],max=bounds['max'],nativeBitWidth=field.type.bit_width)
                for bound in ('min','max'):
                    if isinstance(entry[bound],float)and not math.isfinite(entry[bound]):entry[bound]=None
            if pa.types.is_decimal(field.type):
                bounds=pc.min_max(values).as_py()
                entry.update(precision=field.type.precision,scale=field.type.scale,
                    min=None if bounds['min']is None else str(bounds['min']),max=None if bounds['max']is None else str(bounds['max']),
                    minValue=None if bounds['min']is None else str(bounds['min']),maxValue=None if bounds['max']is None else str(bounds['max']))
        columns.append(entry)
    # Existing metadata/provenance projection stays centralized. It receives an
    # empty frame so it cannot infer/overwrite any native Arrow field types.
    return columns


def _finite_geometry_coordinates(geometry):
    if not np.isfinite(shapely.get_coordinates(geometry)).all():return False,None
    parts=[geometry];z_bounds=None
    while parts:
        part=parts.pop()
        if part.geom_type in {'GeometryCollection','MultiPoint','MultiLineString','MultiPolygon'}:
            parts.extend(part.geoms)
        elif part.has_z and not part.is_empty:
            z=shapely.get_coordinates(part,include_z=True)[:,2]
            if not np.isfinite(z).all():return False,None
            current=[float(z.min()),float(z.max())]
            z_bounds=current if z_bounds is None else[min(z_bounds[0],current[0]),max(z_bounds[1],current[1])]
    return True,z_bounds


def summarize_native_path(path:Path,name:str,metadata:dict):
    """Full batched population/geometry quality, bounded typed dictionary sample."""
    parquet=pq.ParquetFile(path);schema=parquet.schema_arrow;primary,geo,crs=geometry_metadata(schema)
    samples=[];sample_rows=0;sample_bytes=0;sample_closed=False;rows=0;nulls=0;empty=0;invalid=0;unparseable=0;types=set();bounds=None;z_bounds=None;nonfinite=False
    for batch in parquet.iter_batches(batch_size=BATCH_ROWS,use_threads=False):
        if batch.nbytes>BATCH_BYTES:raise ValueError('native_geoparquet_decoded_batch_byte_limit')
        rows+=batch.num_rows
        for field,array in zip(schema,batch.columns):
            if not field.nullable and array.null_count:raise ValueError('native_geoparquet_nonnullable_field_has_null')
        if not sample_closed and sample_rows<SAMPLE_ROWS and sample_bytes<SAMPLE_BYTES:
            count=min(batch.num_rows,SAMPLE_ROWS-sample_rows)
            while count and sample_bytes+batch.slice(0,count).nbytes>SAMPLE_BYTES:count//=2
            if count:
                part=batch.slice(0,count);samples.append(part);sample_rows+=count;sample_bytes+=part.nbytes
            if count<min(batch.num_rows,SAMPLE_ROWS-sample_rows+count):sample_closed=True
        for wkb in batch.column(schema.get_field_index(primary)).to_pylist():
            if wkb is None:nulls+=1;continue
            try:geometry=shapely.from_wkb(wkb)
            except(GEOSException,ValueError,TypeError,OverflowError):unparseable+=1;invalid+=1;continue
            if geometry is None:unparseable+=1;invalid+=1;continue
            if geometry.is_empty:empty+=1;continue
            finite,current_z=_finite_geometry_coordinates(geometry)
            kind=geometry.geom_type+(' Z'if geometry.has_z or current_z is not None else'');types.add(kind)
            if not finite:invalid+=1;nonfinite=True;continue
            if not geometry.is_valid:invalid+=1
            current=geometry.bounds
            bounds=list(current)if bounds is None else[min(bounds[0],current[0]),min(bounds[1],current[1]),max(bounds[2],current[2]),max(bounds[3],current[3])]
            if current_z is not None:
                z_bounds=current_z if z_bounds is None else[min(z_bounds[0],current_z[0]),max(z_bounds[1],current_z[1])]
    if rows!=parquet.metadata.num_rows:raise ValueError('native_geoparquet_incomplete_population_read')
    if geo['geometry_types']and not types.issubset(set(geo['geometry_types'])):raise ValueError('native_geoparquet_declared_geometry_types_mismatch')
    declared=geo.get('bbox')
    if declared is not None and bounds is not None and not nonfinite:
        actual=bounds if len(declared)==4 else None if z_bounds is None else[bounds[0],bounds[1],z_bounds[0],bounds[2],bounds[3],z_bounds[1]]
        if actual!=declared:raise ValueError('native_geoparquet_declared_bbox_mismatch')
    sample=pa.Table.from_batches(samples,schema=schema)if samples else pa.Table.from_batches([],schema=schema)
    names=[name for name in schema.names if name!=primary]
    quality={'version':'v1','feature_count':rows,'bounds':None if nonfinite else bounds,'geometry_type':next(iter(types)).removesuffix(' Z')if len(types)==1 else'Mixed'if types else None,
        'invalid_geometry_count':invalid,'null_geometry_count':nulls,'empty_geometry_count':empty,'unparseable_geometry_count':unparseable,
        'quality_check_passed':rows>0 and invalid==0 and nulls==0 and empty==0,
        'columns_hash':hashlib.sha256(json.dumps(names,sort_keys=True).encode()).hexdigest(),
        'catalog_mode':'native_geoparquet_full_population','spatial_status':'spatial'if types else'all_null_geometry',
        'sampled_feature_count':sample_rows,'sampled_bytes':sample_bytes,'population_quality_scope':'all_rows',
        'native_crs':crs.to_json_dict(),'crs_source':'explicit_PROJJSON'if'crs'in geo else'GeoParquet_normative_OGC_CRS84',
        'native_geometry_column':primary,'native_geo_metadata':geo,'originalArrowSchema':str(schema)}
    serialized_schema=schema.serialize().to_pybytes()
    if len(serialized_schema)>4*1024*1024:raise ValueError('native_geoparquet_schema_metadata_byte_limit')
    dictionary={'name':name,'columns':_dictionary(sample,schema,primary,name,metadata),'sampledFeatureCount':sample_rows,
        'featureCount':rows,'nativeGeometryColumn':primary,'originalArrowSchema':str(schema),
        'nativeArrowSchemaIPCBase64':base64.b64encode(serialized_schema).decode()}
    return quality,dictionary
