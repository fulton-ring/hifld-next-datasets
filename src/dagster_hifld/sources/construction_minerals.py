"""Explicit unapproved USGS producer-successor reconstruction; never polls.

The only candidate input package is the USGS-owned "Active Mines and Mineral
Processing Plants in the United States in 2003" distribution (MRDATA
mineplant), the producer's own successor product of the same 2003 Minerals
Information Team survey as the retired National Atlas construction-minerals
layer. The retained third-party preservation masters of that retired layer and
the existing published HIFLD objects are pinned comparison context only; they
are never candidate inputs. No default fetching, archive polling, USGS release
authority claim, datum correction, uppercase conversion, identifier mapping or
production release is performed.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import io
import json
import math
import os
import re
import stat
import struct
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
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

PINS={'usgs_mineplant_zip': {'url': 'https://mrdata.usgs.gov/mineplant/mineplant.zip', 'bytes': 513191, 'sha256': '6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e'}, 'usgs_mineplant_metadata': {'url': 'https://mrdata.usgs.gov/metadata/mineplant.xml', 'bytes': 26366, 'sha256': '419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0'}, 'legacy_metadata': {'url': 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml', 'bytes': 19769, 'sha256': 'd68776d0c1c487d7b131c4abc0643c7e0ac9fbeac656144b585ca3c6044c3e94'}, 'stanford_publicxml': {'url': 'https://purl.stanford.edu/ds401wt4784.xml', 'bytes': 22726, 'sha256': '7e25e868315f45e54bc7d3bb29a1c64ede4b814071007838154c3c9e2a82d45f'}, 'minop2x020.dbf': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.dbf', 'bytes': 139734, 'sha256': 'c5f3ad0795515c7f3080445f7c759d2285724b5df268394b226fedae665dc90a'}, 'minop2x020.prj': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.prj', 'bytes': 145, 'sha256': 'a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4'}, 'minop2x020.shp': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shp', 'bytes': 21548, 'sha256': '4f35b155c9ac1379223184e5156523c21cfed821e2b29a1cb0b374d75b283b53'}, 'minop2x020.shp.xml': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shp.xml', 'bytes': 188838, 'sha256': 'b8d44bb1c9854cc7ceca85a25ea573d38a4efdc4b903e22b6b44de95ac12ba4a'}, 'minop2x020.shx': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shx', 'bytes': 6228, 'sha256': 'b40ede8c63beec7d78a607ad90e3d768ec70e54caa08d77cbac46b3eec0e9548'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geojson/construction-minerals-operations-geojson.geojson', 'bytes': 275487, 'sha256': 'd8f5de1e147ae977d1a780a0940996805808c67623edb00d4ba38aa20e7f7e6d'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geopackage/construction-minerals-operations-geopackage.gpkg', 'bytes': 270336, 'sha256': '9d18391d85ac6689596689011821799b6723b3cb9a5b633a6b4c13018a31b305'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geoparquet/construction-minerals-operations.parquet', 'bytes': 76683, 'sha256': 'd5189e3387bce01681b454d035f56365e83f3d54bba34a3cbb2d388f22e4361d'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/pmtiles/construction-minerals-operations.pmtiles', 'bytes': 85733, 'sha256': '1d9b649236652f2ab62a7cf834be0d199f08fdd6f705073309bc4d2be62b32c6'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/shapefile/construction-minerals-operations.zip', 'bytes': 47909, 'sha256': '3e0f83e7853f0e1d17c68603a23290cc881460e98634ae34888f002e8bde8da5'}, 'baseline_version': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/releases/1704edb3-322d-469c-91da-92902fe76a2d/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/collection.json', 'bytes': 12652, 'sha256': '217f441c7acf0d3763c46a22375fa2fafaf885cd6f5d5d9bcb63aaa918dc25d6'}, 'baseline_current_metadata': {'url': 'https://storage.googleapis.com/storage/v1/b/hifld-next-datasets-prod/o?prefix=hifld%2Fconstruction-minerals-operations%2Fconstruction-minerals-operations%2Fv1.0.0%2F&maxResults=100', 'bytes': 16676, 'sha256': '4f4b3e2b34aee2174790c05bc70bedd844ea9f993b0e15b3f1aac017d1ff29c7'}}

# Only USGS-owned objects may feed candidate values; every other pinned object
# is comparison context and is labeled as such in the source manifest and audit.
INPUT_NAMES=frozenset({'usgs_mineplant_zip','usgs_mineplant_metadata','legacy_metadata'})
COMPARISON_NAMES=frozenset(PINS)-INPUT_NAMES

MINEPLANT_MEMBERS={'mineplant.dbf':1655861,'mineplant.shp':190080,'mineplant.shx':54380,'mineplant.txt':670146,'mineplant.prj':368}
MINEPLANT_DBF_FIELDS=[('id','N',8,0),('COMMODITY','C',30,0),('SITE_NAME','C',50,0),('COMPANY_NA','C',50,0),('STATE_LOCA','C',40,0),('COUNTY','C',40,0),('LATITUDE','N',10,5),('LONGITUDE','N',10,5),('PLANT_MIN','C',5,0)]
MINEPLANT_RECORDS=6785
CONSTRUCTION_COMMODITIES=('Ball Clay','Cement','Common Clay and Shale','Dimension Stone','Gypsum','Mica','Perlite','Pumice')
CONSTRUCTION_COMMODITY_COUNTS={'Ball Clay':58,'Cement':115,'Common Clay and Shale':262,'Dimension Stone':154,'Gypsum':62,'Mica':24,'Perlite':74,'Pumice':16}
CONSTRUCTION_ROWS=765
CONSTRUCTION_STATES=46
# Physical declarations of the retired National Atlas layer, retained because the
# pinned published comparison objects and the pinned USGS successor-fidelity
# comparison both depend on the original atlas schema and MINOP2X020 IDs.
DBF_FIELDS=[('MINOP2X020','N',3,0),('COMMODITY','C',21,0),('SITE_NAME','C',46,0),('COMPANY_NA','C',51,0),('STATE_LOCA','C',14,0),('COUNTY','C',16,0),('LATITUDE','N',9,3),('LONGITUDE','N',11,3),('PLANT_MIN','C',10,0)]
SOURCE_NAMES=['source_fid','source_shp_record_number']+[f[0] for f in MINEPLANT_DBF_FIELDS]+['geometry']

MINEPLANT_PRJ='GEOGCS["GCS_North_American_1927",DATUM["D_North_American_1927",SPHEROID["Clarke_1866", 6378206.4, 294.9786982138982, AUTHORITY["EPSG","7008"]],TOWGS84[-9.0, 151.0, 185.0, 0.0, 0.0, 0.0, 0.0],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8900"]],UNIT["degree", 0.017453292519943295],AXIS["Lon", EAST],AXIS["Lat", NORTH],AUTHORITY["EPSG","4267"]]'
PRESERVED_PRJ='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'


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


def decode_mineplant_zip(body):
    try:archive=zipfile.ZipFile(io.BytesIO(body))
    except (OSError,ValueError,zipfile.BadZipFile,EOFError) as exc:raise MineralsQuarantineError('invalid_usgs_package_container') from exc
    names=archive.namelist()
    if len(names)!=len(set(names)) or set(names)!=set(MINEPLANT_MEMBERS) or any(archive.getinfo(name).file_size!=MINEPLANT_MEMBERS[name] for name in names):raise MineralsQuarantineError('usgs_package_member_roster_or_size_drift')
    return {name:archive.read(name) for name in MINEPLANT_MEMBERS}


def decode_mineplant_dbf(body):
    if len(body)<321:raise MineralsQuarantineError('incomplete_usgs_dbf')
    count,header,width=struct.unpack_from('<IHH',body,4);fields=[];offset=32
    while offset+32<=header and body[offset]!=13:
        desc=body[offset:offset+32];fields.append((desc[:11].split(b'\0')[0].decode('ascii'),chr(desc[11]),desc[16],desc[17]));offset+=32
    if fields!=MINEPLANT_DBF_FIELDS or count!=MINEPLANT_RECORDS or width!=244 or header!=321 or body[offset]!=13 or len(body) not in (header+count*width,header+count*width+1):raise MineralsQuarantineError('missing_renamed_extra_ID_type_width_count_or_DBF_completeness_drift')
    if len(body)==header+count*width+1 and body[-1]!=26:raise MineralsQuarantineError('unexpected_dbf_tail')
    rows=[];seen=set()
    for fid in range(count):
        rec=body[header+fid*width:header+(fid+1)*width]
        if rec[:1]!=b' ':raise MineralsQuarantineError('deleted_or_unknown_dbf_record')
        offset=1;row={'source_fid':fid,'source_shp_record_number':fid+1}
        for name,kind,size,scale in fields:
            raw=rec[offset:offset+size];offset+=size
            if kind=='C':
                if any(c>127 or c<32 for c in raw):raise MineralsQuarantineError('undeclared_nonASCII_source_encoding')
                row[name]=raw.rstrip(b' ').decode('ascii')
            else:
                text=raw.decode('ascii').strip()
                if not re.fullmatch(r'-?\d+(?:\.\d+)?',text):raise MineralsQuarantineError('null_nonfinite_or_unparseable_DBF_number')
                value=Decimal(text)
                if value.as_tuple().exponent<-scale:raise MineralsQuarantineError('declared_DBF_scale_drift')
                row[name]=value
        ident=int(row['id'])
        if ident<1 or ident in seen:raise MineralsQuarantineError('missing_duplicate_or_nonpositive_usgs_mineplant_id')
        seen.add(ident);rows.append(row)
    return rows


def crosscheck_mineplant_txt(body,rows):
    lines=body.decode('ascii').splitlines()
    if len(lines)!=len(rows)+1 or lines[0].split('\t')!=['ID','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN']:raise MineralsQuarantineError('usgs_txt_roster_or_header_drift')
    def norm(value): return '' if value.strip().upper()=='NULL' else value.rstrip()
    for line,row in zip(lines[1:],rows):
        cells=line.split('\t')
        if len(cells)!=9 or int(cells[0])!=int(row['id']) or norm(cells[1])!=norm(row['COMMODITY']) or norm(cells[2])!=norm(row['SITE_NAME']) or norm(cells[3])!=norm(row['COMPANY_NA']) or norm(cells[4])!=norm(row['STATE_LOCA']) or norm(cells[5])!=norm(row['COUNTY']) or Decimal(cells[6])!=row['LATITUDE'] or Decimal(cells[7])!=row['LONGITUDE'] or norm(cells[8])!=norm(row['PLANT_MIN']):raise MineralsQuarantineError('usgs_txt_dbf_value_drift')


def decode_points(body,index,count):
    if len(body)<100 or len(index)!=100+8*count:raise MineralsQuarantineError('incomplete_SHP_SHX')
    for data in (body,index):
        if struct.unpack_from('>i',data)[0]!=9994 or struct.unpack_from('>i',data,24)[0]*2!=len(data) or struct.unpack_from('<ii',data,28)!=(1000,1):raise MineralsQuarantineError('native_geometry_header_type_or_length_drift')
    out=[];offset=100
    for fid in range(count):
        if offset+28>len(body):raise MineralsQuarantineError('incomplete_point_records')
        record,words=struct.unpack_from('>ii',body,offset);typ,x,y=struct.unpack_from('<idd',body,offset+8)
        if record!=fid+1 or words!=10 or typ!=1 or struct.unpack_from('>ii',index,100+8*fid)!=(offset//2,10) or not all(math.isfinite(v) for v in (x,y)):raise MineralsQuarantineError('SHP_SHX_record_ID_offset_type_or_coordinate_drift')
        out.append(shapely.Point(x,y).wkb);offset+=28
    if offset!=len(body):raise MineralsQuarantineError('extra_point_records')
    geometry=shapely.from_wkb(out)
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise MineralsQuarantineError('invalid_empty_native_point')
    bounds=shapely.total_bounds(geometry).tolist()
    if list(struct.unpack_from('<4d',body,36))!=bounds or index[36:68]!=body[36:68]:raise MineralsQuarantineError('native_bounds_or_index_drift')
    return out


def decode_dbf(body):
    """Decode the pinned third-party preservation master of the retired atlas layer.

    Comparison-only: this decodes the retired National Atlas layer schema for
    the successor-fidelity proof and never feeds candidate values.
    """
    if len(body)<321:raise MineralsQuarantineError('incomplete_dbf')
    count,header,width=struct.unpack_from('<IHH',body,4);fields=[];offset=32
    while offset+32<=header and body[offset]!=13:
        desc=body[offset:offset+32];fields.append((desc[:11].split(b'\0')[0].decode('ascii'),chr(desc[11]),desc[16],desc[17]));offset+=32
    if fields!=DBF_FIELDS or count!=766 or width!=182 or header!=321 or body[offset]!=13 or len(body) not in (header+count*width,header+count*width+1):raise MineralsQuarantineError('missing_renamed_extra_ID_type_width_count_or_DBF_completeness_drift')
    if len(body)==header+count*width+1 and body[-1]!=26:raise MineralsQuarantineError('unexpected_dbf_tail')
    rows=[]
    for fid in range(count):
        rec=body[header+fid*width:header+(fid+1)*width]
        if rec[:1]!=b' ':raise MineralsQuarantineError('deleted_or_unknown_dbf_record')
        offset=1;row={'source_fid':fid,'source_shp_record_number':fid+1}
        for name,kind,size,scale in fields:
            raw=rec[offset:offset+size];offset+=size
            if kind=='C':
                if any(c>127 or c<32 for c in raw):raise MineralsQuarantineError('undeclared_nonASCII_source_encoding')
                row[name]=raw.rstrip(b' ').decode('ascii')
            else:
                text=raw.decode('ascii').strip()
                if not re.fullmatch(r'-?\d+(?:\.\d+)?',text):raise MineralsQuarantineError('null_nonfinite_or_unparseable_DBF_number')
                value=Decimal(text)
                if value.as_tuple().exponent!=-scale:raise MineralsQuarantineError('declared_DBF_scale_drift')
                row[name]=value
        rows.append(row)
    if [int(r['MINOP2X020']) for r in rows]!=list(range(1,767)):raise MineralsQuarantineError('missing_duplicate_reordered_original_MINOP2X020')
    return rows


def verify_mineplant_geometry_values(rows,geometry):
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


def successor_comparison(captures,rows):
    """Prove the USGS successor contains the complete assigned dataset.

    Comparison-only: the pinned third-party preservation masters of the retired
    National Atlas layer are used exclusively to verify that every one of the
    766 retired-layer operations is present in the selected USGS successor
    package. They never feed candidate values, and any drift fails closed.
    """
    atlas=decode_dbf(captures['minop2x020.dbf'][0])
    if len(decode_points(captures['minop2x020.shp'][0],captures['minop2x020.shx'][0],766))!=len(atlas):raise MineralsQuarantineError('atlas_comparison_master_geometry_roster_drift')
    def norm(text): return '' if text.strip().upper()=='NULL' else text.rstrip()
    def exact(row): return (norm(row['SITE_NAME']),row['COMPANY_NA'].rstrip(),row['STATE_LOCA'].rstrip(),row['COUNTY'].rstrip(),row['COMMODITY'].rstrip(),row['LATITUDE'],row['LONGITUDE'],row['PLANT_MIN'].rstrip())
    def reduced(row): return (norm(row['SITE_NAME']),row['STATE_LOCA'].rstrip(),row['COUNTY'].rstrip(),row['COMMODITY'].rstrip(),row['LATITUDE'],row['LONGITUDE'],row['PLANT_MIN'].rstrip())
    index={}
    for row in rows:index.setdefault(exact(row),[]).append(row)
    matches=[];truncated=[];unmatched=[]
    for row in atlas:
        candidates=index.get(exact(row))
        if candidates:
            matches.append((row,candidates[0]));continue
        prefix=row['COMPANY_NA'].rstrip()[:50]
        near=[r for r in rows if reduced(r)==reduced(row) and r['COMPANY_NA'].rstrip()==prefix]
        if len(near)!=1:unmatched.append(str(row['MINOP2X020']));continue
        matches.append((row,near[0]));truncated.append({'atlas_MINOP2X020':str(row['MINOP2X020']),'atlas_COMPANY_NA':row['COMPANY_NA'],'usgs_mineplant_COMPANY_NA':near[0]['COMPANY_NA'],'reason':'the 51-character National Atlas company value exceeds the USGS successor 50-character DBF field width; all other fields and coordinates are equal'})
    consumed=collections.Counter(id(r) for _,r in matches)
    duplicate_pairs=[]
    for row,r in matches:
        if consumed[id(r)]>1 and all(p['usgs_mineplant_id']!=str(r['id']) for p in duplicate_pairs):
            duplicate_pairs.append({'atlas_MINOP2X020_pair':sorted(str(a['MINOP2X020']) for a,b in matches if b is r),'usgs_mineplant_id':str(r['id']),'reason':'the retired layer recorded this single operation as two identical records; the USGS successor carries it once'})
    if unmatched or len(matches)!=766 or len(consumed)!=len(rows) or any(count<1 for count in consumed.values()) or len(duplicate_pairs)>1 or len(truncated)>1:
        raise MineralsQuarantineError('usgs_successor_does_not_contain_complete_assigned_dataset:unmatched='+str(len(unmatched))+';duplicate_pairs='+str(len(duplicate_pairs))+';truncated='+str(len(truncated)))
    return {'approved':False,'comparison_only':True,'purpose':'prove the selected USGS successor package contains the complete assigned National Atlas construction-minerals dataset','retired_layer_source':'pinned third-party preservation masters of the retired National Atlas layer; comparison context only, never a pipeline input','atlas_records':766,'usgs_successor_construction_records':len(rows),'exact_field_value_matches':766-len(truncated),'atlas_duplicate_collapsed_cases':duplicate_pairs,'company_width_truncation_cases':truncated,'unmatched_atlas_records':unmatched,'coordinate_relationship':'LATITUDE/LONGITUDE values are numerically identical in both USGS products; no datum transformation was applied by the producer','datum_claims_unresolved':{'usgs_mineplant_prj':'North American Datum of 1927','atlas_preserved_prj':'WGS 1984','atlas_metadata':'North American Datum of 1983','resolution':'UNAPPROVED; coordinates unchanged, each product PRJ as provided only'}}


def inspect_captures(captures):
    if set(captures)!=set(PINS):raise MineralsQuarantineError('package_capture_roster_drift')
    for name,(body,receipt) in captures.items():
        validate_capture(name,body,receipt,PINS[name]['url'])
        if digest(body)!=PINS[name]['sha256'] or len(body)!=PINS[name]['bytes']:raise MineralsQuarantineError('pinned_package_drift:'+name)
    public=ET.fromstring(captures['stanford_publicxml'][0]);masters=[]
    for file in public.iter('file'):
        if file.attrib.get('role')!='master':continue
        name=file.attrib['id'];masters.append(name)
        if name not in PINS:raise MineralsQuarantineError('unexpected_preservation_master')
        body=captures[name][0]
        if len(body)!=int(file.attrib['size']) or any(hashlib.new(c.attrib['type'].lower().replace('-',''),body).hexdigest()!=c.text for c in file.findall('checksum')):raise MineralsQuarantineError('preservation_manifest_size_or_checksum_mismatch')
    if sorted(masters)!=sorted('minop2x020.'+s for s in ('dbf','prj','shp','shp.xml','shx')):raise MineralsQuarantineError('missing_preservation_master')
    members=decode_mineplant_zip(captures['usgs_mineplant_zip'][0])
    if members['mineplant.prj'].decode('ascii')!=MINEPLANT_PRJ:raise MineralsQuarantineError('usgs_native_prj_drift')
    rows=decode_mineplant_dbf(members['mineplant.dbf'])
    crosscheck_mineplant_txt(members['mineplant.txt'],rows)
    geometry=decode_points(members['mineplant.shp'],members['mineplant.shx'],MINEPLANT_RECORDS)
    verify_mineplant_geometry_values(rows,geometry)
    subset=construction_rows(rows)
    fields=[pa.field('source_fid',pa.int64(),nullable=False,metadata={b'identity':b'Original zero-based USGS mineplant SHP/DBF physical record index of the construction subset row, not a published or replacement feature ID'}),pa.field('source_shp_record_number',pa.int32(),nullable=False,metadata={b'identity':b'Original USGS mineplant SHP record-header number'})]
    for name,kind,width,scale in MINEPLANT_DBF_FIELDS:fields.append(pa.field(name,pa.string() if kind=='C' else pa.decimal128(width,scale),nullable=False,metadata={b'original_DBF_physical_declaration':encode({'type':kind,'width':width,'decimals':scale})}))
    fields.append(pa.field('geometry',pa.binary(),nullable=False))
    table=pa.Table.from_pylist([{**{f[0]:row[f[0]] for f in MINEPLANT_DBF_FIELDS},'source_fid':row['source_fid'],'source_shp_record_number':row['source_shp_record_number'],'geometry':geometry[row['source_fid']]} for row in subset],schema=pa.schema(fields))
    comparison=successor_comparison(captures,subset)
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
    audit={'approved':False,'package_id':PACKAGE,'classification':'USGS-owned producer successor of the retired National Atlas construction-minerals layer; unapproved draft reconstruction, not the original layer and not an authenticated historical archive of it','count':CONSTRUCTION_ROWS,'states':CONSTRUCTION_STATES,'commodities':dict(collections.Counter(row['COMMODITY'] for row in subset)),
           'original_DBF_fields':MINEPLANT_DBF_FIELDS,'native_crs_claim':MINEPLANT_PRJ,'contradictory_original_USGS_metadata_datum':'North American Datum of 1983','atlas_preserved_prj_claim':'WGS 1984','datum_resolution':'UNAPPROVED; coordinates numerically identical in both USGS products and unchanged; each product PRJ as provided only; no correction/transform',
           'physical_numeric_decode':'Exact DBF base10 decimal values preserved (stored exponents vary up to the declared 5-decimal scale); SHP point doubles equal the DBF values; no binary floating substitution','source_ids':'original USGS mineplant id plus original mineplant SHP/DBF record indices and record numbers, all distinct and unreplaced','published_identifiers':'complete published MINOP2X020/OBJECTID and GeoJSON FID/featureid separately retained as comparison context, no mapping approved','release':'USGS mineplant FGDC publication 2005 from the same National Minerals Information Center survey as the retired layer publication 200512; exact release day unknown'}
    return table,published_ids,audit,comparison


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
            producer='U.S. Geological Survey (National Minerals Information Center successor distribution of the 2003 survey)' if name!='legacy_metadata' else 'U.S. Geological Survey (National Atlas legacy metadata for the retired construction-minerals layer)'
            attribution='USGS MRDATA mineplant successor package and USGS National Atlas legacy metadata; the pinned USGS metadata establishes the exact assigned construction-minerals dataset family and its 200512 publication. No contract or release acceptance.'
        elif name.startswith('minop2x020.') or name=='stanford_publicxml':
            role='archive';use='comparison_only_retained_prior_research'
            producer='Stanford Libraries preservation'
            attribution='Third-party preservation masters of the retired National Atlas layer; retained solely as pinned comparison evidence proving the USGS successor contains the complete assigned dataset. Never a pipeline input.'
        else:
            role='archive';use='comparison_only_retained_prior_research'
            producer='HIFLD Next existing published candidate, acceptance unresolved'
            attribution='Existing published HIFLD v1 objects retained solely to preserve the published identifier roster and comparison context. Never a pipeline input.'
        inputs.append({'input_id':name+':'+digest(body),'producer':producer,'host':urlsplit(receipt['url']).hostname,'role':role,'pipeline_use':use,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':attribution,'release_date_unknown_reason':'USGS mineplant FGDC publication year 2005 and National Atlas layer publication 200512 have no exact release day; retrieval and portal-edit times are not source release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],'title':'Construction Minerals Operations — USGS producer-successor reconstruction (unapproved draft)','publisher':'U.S. Geological Survey Mineral Resources Program (MRDATA successor product)','agency':'U.S. Geological Survey','source_url':'https://mrdata.usgs.gov/mineplant/','license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'explicit_usgs_producer_successor_native_lossless_reconstruction','parameters':{'package_id':PACKAGE,'input_selection':'explicit local USGS package required, no default fetch or polling; only USGS-owned objects are candidate inputs','assigned_scope':'construction minerals operations as documented by the pinned USGS National Atlas metadata: the eight USGS commodity classes ball clay, cement, common clay and shale, dimension stone, gypsum, mica, perlite and pumice, excluding construction sand and gravel and crushed stone','subset_selection':'rows of the USGS mineplant file whose COMMODITY equals one of the eight assigned classes; pinned completeness 765 rows in 46 states','geometry':'original USGS mineplant SHP coordinates and own NAD27 PRJ as provided; SHP point doubles equal the DBF values; contradictory NAD83/WGS84 claims of the retired atlas layer retained and unresolved','successor_fidelity':'pinned comparison masters prove all 766 retired-layer operations are present: exact field equality except one 51-to-50 character company width truncation and one atlas duplicate collapsed to a single USGS record; coordinates numerically identical','IDs':'USGS mineplant id and original mineplant SHP/DBF record indices preserved; complete published identifiers separately retained, no approved cross-package mapping'}}],'unsupported_output_formats':{'shapefile_zip':'No accepted source/published schema mapping; the original USGS mineplant package is retained losslessly as raw evidence, not a newly approved output.','geopackage':'DBF decimal128 precision/scale and full Arrow physical schema cannot be silently coerced to GPKG REAL/INTEGER; no migration approved.','pmtiles':'Reprojection/quantization and datum choice are unapproved; native coordinates retained.'},'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all source decimal physical declarations, original IDs, strings, native WKB and metadata, no generic conversion or schema expansion. This is capability only, not input/contract/release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def source_payloads(captures,identity,audit,manifest,table,published_ids,comparison):
    proof={'status':'pass','approved':False,'rows':CONSTRUCTION_ROWS,'method':'All USGS mineplant DBF records and physical declarations decoded and cross-checked against the producer TXT; construction subset selected; SHP/SHX native points decoded and tied to DBF values; all765values/IDs/decimaltypes/nativeWKB/schema metadata and exact deterministic Parquet bytes verified; full original USGS package retained. No source authority/datum/compatibility acceptance.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/construction_minerals_source_audit.json':encode(audit),'metadata/successor_comparison.json':encode(comparison),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_usgs_successor_input_authority_datum_schema_ID_mapping_and_baseline_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b'','candidate/published-identifiers.parquet':parquet_bytes(published_ids)}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,version,table,published_ids,audit,comparison,captures,identity,pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise MineralsQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table,published_ids,comparison);names={*expected,'candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
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
        table,published_ids,audit,comparison=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({name:digest(body) for name,(body,_) in captures.items()}))
        version=f'v0.0.0-draft-usgs-mineplant-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise MineralsQuarantineError('linked_candidate')
                    body=parquet_bytes(expected_parquet(table));pin={'sha256':digest(body),'bytes':len(body)}
                    reconcile_cache(storage,prefix,version,table,published_ids,audit,comparison,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise MineralsQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':CONSTRUCTION_ROWS,'reused':True,'package_id':PACKAGE,'accepted':False}
            with tempfile.TemporaryDirectory(prefix='.minerals-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                body=parquet_bytes(expected_parquet(table));(private/f'geoparquet/{SLUG}.parquet').write_bytes(body);(private/'candidate/typed-source.parquet').write_bytes(body);pin={'sha256':digest(body),'bytes':len(body)}
                manifest=manifest_for(captures,identity,pin)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'package_id':PACKAGE,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison).items():
                    if (private/name).read_bytes()!=body:raise MineralsQuarantineError('incomplete_private_source_payload:'+name)
                # Validate source semantics + exact Git identity before the first seal;
                # the complete private candidate exists and has been verified first.
                validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY};(private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':CONSTRUCTION_ROWS,'reused':False,'package_id':PACKAGE,'accepted':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise MineralsQuarantineError(str(exc)+'; retained:'+retained) from exc
