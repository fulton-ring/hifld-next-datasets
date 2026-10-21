"""Explicit, unapproved Stanford historical preservation-package reconstruction.

No default fetching, archive polling, USGS authority claim, datum correction,
uppercase conversion, identifier mapping, or production release is performed.
"""
from __future__ import annotations
import collections
import base64
import pyogrio
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
SLUG='crushed-stone-operations'
PACKAGE='stanford-cs268nn2622-preservation-as-provided-v1'
READY='metadata/crushed_stone_candidate_ready.json'
LICENSE='USGS National Atlas metadata states no use restrictions; Stanford preserved package metadata retained. No source selection, authority, mapping, datum, contract or release acceptance.'
class IncidentWriteError(Exception): pass

class CrushedStoneQuarantineError(ValueError): pass

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise CrushedStoneQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(CrushedStoneQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise CrushedStoneQuarantineError('json_object_required')
    return value

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise CrushedStoneQuarantineError('immutable_capture_changed:'+key)

def processing_identity():
    root=Path(__file__).parents[3]
    return packaged_processing_identity(root)

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
        raise CrushedStoneQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise CrushedStoneQuarantineError('linked_candidate_lock')
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
        raise CrushedStoneQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise CrushedStoneQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise CrushedStoneQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if body!=captures[name][0]:raise CrushedStoneQuarantineError('original_capture_bytes_differ')
        original[name]=(body,item['receipt'])
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity)
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise CrushedStoneQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

PINS={'legacy_metadata': {'url': 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/crstonx020.xml', 'bytes': 19892, 'sha256': '0dbf1190282e425aa0b72179e5d163a716126e19daa50c6795378e784864ac24'}, 'stanford_publicxml': {'url': 'https://purl.stanford.edu/cs268nn2622.xml', 'bytes': 20891, 'sha256': '1faf3ba4b6baa4be16983d01e622e89795c602b259bef831d9c8d45c3499745e'}, 'crstonx020.dbf': {'url': 'https://stacks.stanford.edu/file/druid:cs268nn2622/crstonx020.dbf', 'bytes': 4838954, 'sha256': '8276ce1cdc493f36cc3b333c7894ecd6ba1c7c6885015aa7552805ce68aafe78'}, 'crstonx020.prj': {'url': 'https://stacks.stanford.edu/file/druid:cs268nn2622/crstonx020.prj', 'bytes': 167, 'sha256': '84cf5b9a3c1a444f83ac30ff76f8c5035990223027ba69b2f4d628388adcbd65'}, 'crstonx020.shp': {'url': 'https://stacks.stanford.edu/file/druid:cs268nn2622/crstonx020.shp', 'bytes': 65172, 'sha256': '7e364d47d9bea71788492826789b84e98b9e481043aee7b193632946a0916cf0'}, 'crstonx020.shp.xml': {'url': 'https://stacks.stanford.edu/file/druid:cs268nn2622/crstonx020.shp.xml', 'bytes': 167184, 'sha256': 'cecaa98f28a7de59f1ebe933c2da8091d27e7631b48ac11c0a84161632cc69a9'}, 'crstonx020.shx': {'url': 'https://stacks.stanford.edu/file/druid:cs268nn2622/crstonx020.shx', 'bytes': 18692, 'sha256': '4618d05729ddfe8eaa347b87eacf50290522c145c2e405d3c7b6fec9e7290ad8'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/geojson/crushed-stone-operations-geojson.geojson', 'bytes': 275487, 'sha256': '2ddcf6a267fdaf2d1063fee1dab2deece39ccf6a8cece79221c49e63910ab595'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/geopackage/crushed-stone-operations-Crushed_Stone_Operations.gpkg', 'bytes': 249856, 'sha256': 'becb8e86f616c5579e555464f01ea2e64b8089ff749e9b762237b2935ef1bf82'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/geoparquet/crushed-stone-operations.parquet', 'bytes': 79396, 'sha256': '6e4fa965ef0164f0d46c855ad517cb2440aa512cdbf852dd0c7643ea3aa8dd4e'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/pmtiles/crushed-stone-operations.pmtiles', 'bytes': 85710, 'sha256': '6d30d4a1f199d729ea4471c6e954c66e8c526ff96d5bc9bfdff9f9ee1eb4df58'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/shapefile/crushed-stone-operations.zip', 'bytes': 47739, 'sha256': 'b2cb4ae2bde74b58e7c06ee19e87dd4583a07af2d1ff971bccea6b3b2165163c'}, 'baseline_file_geodatabase': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/file_geodatabase/crushed-stone-operations-file_geodatabase.zip', 'bytes': 74138, 'sha256': '9820c939b832e7612390a0a7741722e84e04a3666e1ea782729a6dd97d1ab903'}, 'baseline_collection': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/crushed-stone-operations/crushed-stone-operations/v1.0.0/collection.json', 'bytes': 9923, 'sha256': '306ee7d79fcbbd17ea102068015d410b54757486798e72039f26d9ae4cba068f'}, 'baseline_metadata': {'url': 'https://storage.googleapis.com/storage/v1/b/hifld-next-datasets-prod/o?prefix=hifld%2Fcrushed-stone-operations%2Fcrushed-stone-operations%2Fv1.0.0%2F&maxResults=100', 'bytes': 17197, 'sha256': 'b82cdebfc4951b9949134066980ac2e9157b6ac938f12995875df7f8bc075ae3'}}

DBF_FIELDS=[('CRSTONX020', 'N', 11, 0), ('COMP_NAME', 'C', 254, 0), ('OPER_NAME', 'C', 254, 0), ('COUNTY', 'C', 254, 0), ('STATE', 'C', 254, 0), ('FIPS', 'C', 254, 0), ('STATE_FIPS', 'C', 254, 0), ('LATITUDE', 'N', 19, 4), ('LONGITUDE', 'N', 19, 4), ('OPER_TYPE', 'C', 254, 0), ('COMMODITY', 'C', 254, 0)]
SOURCE_NAMES=[f[0] for f in DBF_FIELDS]+['geometry']


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise CrushedStoneQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise CrushedStoneQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise CrushedStoneQuarantineError('linked_staging_object')


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise CrushedStoneQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise CrushedStoneQuarantineError('incomplete_or_changed_bounded_input')
    return body


def acquire(storage,capture_dir,package_id):
    local_only(storage)
    if package_id!=PACKAGE or not capture_dir:raise CrushedStoneQuarantineError('explicit_unapproved_preservation_package_selection_required_no_default_fetch')
    root=Path(capture_dir);captures={};attempted={}
    if '..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents]):raise CrushedStoneQuarantineError('unsafe_input_package_root')
    try:
        expected={name+suffix for name in PINS for suffix in ('.body','.receipt.json')}
        if {p.name for p in root.iterdir()}!=expected:raise CrushedStoneQuarantineError('incomplete_or_extra_explicit_package_roster')
        for name,pin in PINS.items():
            paths=[root/(name+suffix) for suffix in ('.body','.receipt.json')]
            if any(not p.is_file() or p.is_symlink() or p.stat().st_nlink!=1 for p in paths):raise CrushedStoneQuarantineError('linked_or_nonregular_package_input')
            if paths[0].stat().st_size>8*1024*1024 or paths[1].stat().st_size>65536:raise CrushedStoneQuarantineError('package_input_exceeds_bound')
            body=bounded_input(paths[0],8*1024*1024)
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
            if len(body)!=pin['bytes'] or digest(body)!=pin['sha256']:raise CrushedStoneQuarantineError('unreviewed_package_bytes_require_explicit_new_package:'+name)
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise CrushedStoneQuarantineError(str(exc)+'; retained:'+retained) from exc


def decode_dbf(body):
    if len(body)<385:raise CrushedStoneQuarantineError('incomplete_dbf')
    count,header,width=struct.unpack_from('<IHH',body,4);fields=[];offset=32
    while offset+32<=header and body[offset]!=13:
        desc=body[offset:offset+32];fields.append((desc[:11].split(b'\0')[0].decode('ascii'),chr(desc[11]),desc[16],desc[17]));offset+=32
    if fields!=DBF_FIELDS or count!=2324 or width!=2082 or header!=385 or body[offset]!=13 or body[0]!=3 or body[29]!=87 or len(body) not in (header+count*width,header+count*width+1):raise CrushedStoneQuarantineError('missing_renamed_extra_ID_type_width_count_or_DBF_completeness_drift')
    if len(body)==header+count*width+1 and body[-1]!=26:raise CrushedStoneQuarantineError('unexpected_dbf_tail')
    rows=[]
    for fid in range(count):
        rec=body[header+fid*width:header+(fid+1)*width]
        if rec[:1]!=b' ':raise CrushedStoneQuarantineError('deleted_or_unknown_dbf_record')
        offset=1;row={}
        for name,kind,size,scale in fields:
            raw=rec[offset:offset+size];offset+=size
            if kind=='C':
                if any(c>127 or c<32 for c in raw):raise CrushedStoneQuarantineError('undeclared_nonASCII_source_encoding')
                row[name]=raw.rstrip(b' ').decode('ascii')
            else:
                text=raw.decode('ascii').strip()
                if not re.fullmatch(r'-?\d+(?:\.\d+)?',text):raise CrushedStoneQuarantineError('null_nonfinite_or_unparseable_DBF_number')
                value=Decimal(text)
                if value.as_tuple().exponent!=-scale:raise CrushedStoneQuarantineError('declared_DBF_scale_drift')
                row[name]=value
        rows.append(row)
    if [int(r['CRSTONX020']) for r in rows]!=list(range(1,2325)):raise CrushedStoneQuarantineError('missing_duplicate_reordered_original_CRSTONX020')
    return rows


def decode_points(body,index):
    if len(body)<100 or len(index)!=100+8*2324:raise CrushedStoneQuarantineError('incomplete_SHP_SHX')
    for data in (body,index):
        if struct.unpack_from('>i',data)[0]!=9994 or struct.unpack_from('>i',data,24)[0]*2!=len(data) or struct.unpack_from('<ii',data,28)!=(1000,1):raise CrushedStoneQuarantineError('native_geometry_header_type_or_length_drift')
    out=[];offset=100
    for fid in range(2324):
        if offset+28>len(body):raise CrushedStoneQuarantineError('incomplete_point_records')
        record,words=struct.unpack_from('>ii',body,offset);typ,x,y=struct.unpack_from('<idd',body,offset+8)
        if record!=fid+1 or words!=10 or typ!=1 or struct.unpack_from('>ii',index,100+8*fid)!=(offset//2,10) or not all(math.isfinite(v) for v in (x,y)):raise CrushedStoneQuarantineError('SHP_SHX_record_ID_offset_type_or_coordinate_drift')
        out.append(shapely.Point(x,y).wkb);offset+=28
    if offset!=len(body):raise CrushedStoneQuarantineError('extra_point_records')
    geometry=shapely.from_wkb(out)
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise CrushedStoneQuarantineError('invalid_empty_native_point')
    bounds=shapely.total_bounds(geometry).tolist()
    if list(struct.unpack_from('<4d',body,36))!=bounds or index[36:68]!=body[36:68]:raise CrushedStoneQuarantineError('native_bounds_or_index_drift')
    return out


def inspect_captures(captures):
    if set(captures)!=set(PINS):raise CrushedStoneQuarantineError('package_capture_roster_drift')
    for name,(body,receipt) in captures.items():
        validate_capture(name,body,receipt,PINS[name]['url'])
        if digest(body)!=PINS[name]['sha256'] or len(body)!=PINS[name]['bytes']:raise CrushedStoneQuarantineError('pinned_package_drift:'+name)
    public=ET.fromstring(captures['stanford_publicxml'][0]);masters=[]
    for file in public.iter('file'):
        if file.attrib.get('role')!='master':continue
        name=file.attrib['id'];masters.append(name)
        if name not in PINS:raise CrushedStoneQuarantineError('unexpected_preservation_master')
        body=captures[name][0]
        if len(body)!=int(file.attrib['size']) or any(hashlib.new(c.attrib['type'].lower().replace('-',''),body).hexdigest()!=c.text for c in file.findall('checksum')):raise CrushedStoneQuarantineError('preservation_manifest_size_or_checksum_mismatch')
    if sorted(masters)!=sorted('crstonx020.'+ext for ext in ('dbf','prj','shp','shp.xml','shx')):raise CrushedStoneQuarantineError('missing_preservation_master')
    official=ET.fromstring(captures['legacy_metadata'][0]);own=ET.fromstring(captures['crstonx020.shp.xml'][0])
    if not any('Crushed Stone Operations' in str(e.text) for e in official.findall('.//title')) or '200506' not in [e.text for e in official.findall('.//pubdate')] or '2002' not in [e.text for e in official.findall('.//caldate')]:raise CrushedStoneQuarantineError('producer_historical_dataset_family_or_release_identity_drift')
    if captures['crstonx020.prj'][0].decode('ascii')!=PRESERVED_PRJ or any('North American Datum of 1983' not in [e.text for e in meta.findall('.//horizdn')] for meta in (official,own)):raise CrushedStoneQuarantineError('native_package_datum_claim_or_metadata_drift')
    rows=decode_dbf(captures['crstonx020.dbf'][0]);geometry=decode_points(captures['crstonx020.shp'][0],captures['crstonx020.shx'][0])
    for row,wkb in zip(rows,geometry):row['geometry']=wkb
    if any(row['COMMODITY']!='CRUSHED STONE' for row in rows):raise CrushedStoneQuarantineError('wrong_commodity_family_or_partial_source_substitution')
    fields=[pa.field(name,pa.string() if kind=='C' else pa.decimal128(width,scale),nullable=False,metadata={b'original_DBF_physical_declaration':encode({'type':kind,'width':width,'decimals':scale,'ldid':87,'nullability':'not expressed in dBASEIII; all original current records nonblank, no inferred null substitution'})}) for name,kind,width,scale in DBF_FIELDS]
    fields.append(pa.field('geometry',pa.binary(),nullable=False));table=pa.Table.from_pylist(rows,schema=pa.schema(fields))
    published=baseline_audit(captures)
    audit={'approved':False,'package_id':PACKAGE,'classification':'Explicit Stanford historical preservation as provided, not authenticated original USGS archive/current source feed','count':2324,'states':sorted({row['STATE'] for row in rows}),'commodities':dict(collections.Counter(row['COMMODITY'] for row in rows)),
        'original_DBF_fields':DBF_FIELDS,'native_crs_claim':PRESERVED_PRJ,'native_crs':'EPSG:4269','datum_claim':'Own preserved PRJ and historical USGS/package metadata agree on NAD83; package-original authentication remains unresolved, no transformation accepted.',
        'physical_numeric_decode':'Exact DBF base10 decimal(width,scale); original record bytes/header/LDID87 retained. All current character bytes are ASCII within the declared Windows1252 code page; decode right-padding only, no case/leading-zero normalization.',
        'source_ids':'Original CRSTONX020 decimal(11,0)1..2324 retained, never equated to published MINOP2X020/OBJECTID/id. Intrinsic zero-based physical order and SHP record header numbers retained in a separate lineage sidecar, not invented main IDs.',
        'published_identifiers':'All published feature IDs, system/storage IDs and producer fields independently retained; complete766MINOP construction-family baseline is NOT a reviewed continuation of2324CRSTON crushed-stone source.',
        'release':'Historical USGS metadata publication200506 and active2002>50000tons/non-government/non-portable; no exact release day or current coverage claimed; Stanford preservation is a distinct unapproved input package.'}
    return table,published,audit



def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_wkt(PRESERVED_PRJ).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'candidate_scope':PACKAGE.encode()})


PRESERVED_PRJ='GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'

def parquet_bytes(table):
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd');return sink.getvalue().to_pybytes()


def verify_outputs(root,table,published_ids):
    expected=parquet_bytes(expected_parquet(table))
    for name in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if (root/name).read_bytes()!=expected or not pq.read_table(root/name).equals(expected_parquet(table),check_metadata=True):raise CrushedStoneQuarantineError('full_native_source_values_IDs_decimal_types_geometry_metadata_or_parquet_bytes_drift')
    if (root/'metadata/published-identifiers.json').read_bytes()!=encode(published_ids):raise CrushedStoneQuarantineError('complete_separate_published_identifiers_drift')


def manifest_for(captures,identity,parquet_pin):
    inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,PINS[name]['url'])
        role='producer_publication' if name=='legacy_metadata' else 'archive'
        inputs.append({'input_id':name+':'+digest(body),'producer':'U.S. Geological Survey (historical dataset metadata only)' if name=='legacy_metadata' else 'Stanford Libraries preservation' if name.startswith('crstonx020.') or name=='stanford_publicxml' else 'HIFLD Next existing published candidate, acceptance unresolved','host':urlsplit(receipt['url']).hostname,'role':role,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':'Stanford cs268nn2622 preservation-as-provided; USGS National Atlas CRSTON crushed-stone historical family; existing766MINOP construction-family published candidates retained independently. No delegation, package-original authentication, identity correction or source selection accepted.','release_date_unknown_reason':'Official source publication month200506(active2002,production>50000tons,non-government/non-portable), no exact release day; Stanford package preservation and baseline fetch times are not source release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],'title':'Crushed Stone Operations — explicit historical2324-record preservation candidate; published766MINOP identity mismatch unapproved','publisher':'Stanford Libraries preservation (not established agency distribution)','agency':'US Geological Survey historical dataset family; input authority unresolved','source_url':'https://purl.stanford.edu/cs268nn2622','license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'explicit_preservation_package_native_lossless_reconstruction','parameters':{'package_id':PACKAGE,'input_selection':'explicit local package required, no default fetch or polling; acceptance pending','geometry':'original SHP coordinates and own PRJ as provided; own PRJ and historical metadata agree on NAD83; package originality/selection remains unapproved','container':'A new deterministic ZIP of unchanged preservation masters is raw-only archival evidence, not the missing USGS archive or a canonical/output Shapefile. Native GeoParquet is the sole candidate input/output capability.', 'attributes':'Declared LDID87/all current data ASCII, right-padding decode only; case, leading-zero FIPS, widths and exact decimal values preserved, original DBF sidecars retained','IDs':'all native IDs distinct; complete published IDs separately retained, no approved cross-package mapping'}}],'unsupported_output_formats':{'shapefile_zip':'No accepted source/published schema mapping; preserved original five master sidecars remain lossless raw evidence, not a newly approved output.','geopackage':'DBF decimal128 precision/scale and full Arrow physical schema cannot be silently coerced to GPKG REAL/INTEGER; no migration approved.','pmtiles':'Reprojection/quantization and datum choice are unapproved; native coordinates retained.'},'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all source decimal physical declarations, original IDs, strings, native WKB and metadata, no generic conversion or schema expansion. This is capability only, not input/contract/release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def preservation_transport_zip(captures):
    # New deterministic container, NEVER described as the missing original USGS
    # archive. Each member is an unchanged Stanford preservation master.
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(n for n in captures if n.startswith('crstonx020.')):
            info=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));info.create_system=3;info.external_attr=0o100600<<16;archive.writestr(info,captures[name][0])
    return stream.getvalue()


def source_payloads(captures,identity,audit,manifest,table,published_ids):
    proof={'status':'pass','approved':False,'rows':2324,'method':'All original DBF records/physical declarations and SHP/SHX native points decoded; all2324values/IDs/decimaltypes/nativeWKB/schema metadata and exact deterministic Parquet bytes verified; full original masters retained. No source authenticity/datum/compatibility acceptance.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/crushed_stone_source_audit.json':encode(audit),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['published766MINOP_construction_family_vs2324CRSTON_crushed_stone_identity_coverage_schema_ID_source_selection_break'],'complete_published_audit':published_ids,'source_rows':2324,'source_fields':DBF_FIELDS,'source_CRS':'EPSG:4269','no_cross_dataset_ID_join_or_approved_transformation':True}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b'','metadata/published-identifiers.json':encode(published_ids), 'metadata/source-record-lineage.json':encode({'approved':False,'source_record_header_numbers':list(range(1,2325)),'zero_based_physical_order':list(range(2324)),'relationship':'Intrinsic original SHP record order/header numbers only; not generated replacement IDs, not a baseline join.'})}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
        # Original masters/transport remain raw-only evidence. Native opted-in
        # GeoParquet is the sole canonical candidate; no Shapefile format
        # mapping or legacy-discovery workaround is granted here.
        if name.startswith('crstonx020.'):values['raw/archive/'+name]=body
    values['raw/archive/preservation-package.zip']=preservation_transport_zip(captures)
    return values


def ready_metadata(version,captures,identity,objects):
    """Closed source-owned wrapper; a draft marker cannot add acceptance claims."""
    return {'version':version,'approved':False,'processing_fingerprint':digest(encode(identity)),
        'capture_fingerprint':digest(encode({name:digest(body) for name,(body,_) in captures.items()})),
        'package_id':PACKAGE,'objects':objects}


def validate_ready_metadata(body,version,captures,identity,objects):
    if body!=encode(ready_metadata(version,captures,identity,objects)):
        raise CrushedStoneQuarantineError('ready_identity_or_closed_source_inventory_drift')


def reconcile_cache(storage,prefix,version,table,published_ids,audit,captures,identity,pin):
    root=Path(storage.local_dir).absolute()/prefix
    if not (root/READY).is_file():raise CrushedStoneQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table,published_ids);names={*expected,'candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    objects={name:digest(body) for name,body in expected.items()}
    objects.update({'candidate/typed-source.parquet':pin['sha256'],f'geoparquet/{SLUG}.parquet':pin['sha256']})
    # Exact canonical bytes bind every field/type/closed roster to the validated
    # original captures and native objects, including package selection. Python
    # JSON/dict equality must not equate a boolean with numeric zero or one.
    validate_ready_metadata((root/READY).read_bytes(),version,captures,identity,objects)
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise CrushedStoneQuarantineError('unknown_missing_or_linked_candidate_object')
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise CrushedStoneQuarantineError('source_owned_report_raw_provenance_or_published_ID_drift:'+name)
    for name,sha in objects.items():
        if sha256_file(root/name)!=sha:raise CrushedStoneQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise CrushedStoneQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise CrushedStoneQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table,published_ids)


def stage_crushed_stone(storage,*,capture_dir=None,package_id=None):
    local_only(storage);captures=acquire(storage,capture_dir,package_id)
    try:
        table,published_ids,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({name:digest(body) for name,(body,_) in captures.items()}))
        version=f'v0.0.0-draft-stanford-preservation-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir).absolute()
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            refuse_observed_loss(storage,version)
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise CrushedStoneQuarantineError('linked_candidate')
                    try:load_original_state(storage,version,captures,identity,table)
                    except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                        try:record_observed_loss(storage,version,fingerprint,str(loss))
                        except Exception as journal_error:raise IncidentWriteError('incident_write_failed_candidate_preserved') from journal_error
                        raise CrushedStoneQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
                    body=parquet_bytes(expected_parquet(table));pin={'sha256':digest(body),'bytes':len(body)}
                    reconcile_cache(storage,prefix,version,table,published_ids,audit,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise CrushedStoneQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':2324,'reused':True,'package_id':PACKAGE,'accepted':False}
            if original_state_present(storage,version):
                try:load_original_state(storage,version,captures,identity,table)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as loss:
                    try:record_observed_loss(storage,version,fingerprint,str(loss))
                    except Exception as journal_error:raise IncidentWriteError('incident_write_failed_partial_state_preserved') from journal_error
                    raise CrushedStoneQuarantineError('observed_original_trust_loss:'+str(loss)) from loss
            with tempfile.TemporaryDirectory(prefix='.crushed-stone-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                body=parquet_bytes(expected_parquet(table));(private/f'geoparquet/{SLUG}.parquet').write_bytes(body);(private/'candidate/typed-source.parquet').write_bytes(body);pin={'sha256':digest(body),'bytes':len(body)}
                manifest=manifest_for(captures,identity,pin)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready=ready_metadata(version,captures,identity,{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()})
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():
                    if (private/name).read_bytes()!=body:raise CrushedStoneQuarantineError('incomplete_private_source_payload:'+name)
                # Validate source semantics + exact Git identity before the first seal;
                # the complete private candidate exists and has been verified first.
                validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).write_bytes(body)
                ready=ready_metadata(version,captures,identity,{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY});(private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':2324,'reused':False,'package_id':PACKAGE,'accepted':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise CrushedStoneQuarantineError(str(exc)+'; retained:'+retained) from exc

def incident_directory(version):
    return f'_construction-state/{SLUG}/incident-{version}'

def refuse_observed_loss(storage,version):
    directory=Path(storage.local_dir).absolute()/incident_directory(version)
    if directory.exists() and any(directory.rglob('*')):raise CrushedStoneQuarantineError('permanent_observed_original_trust_loss_requires_reviewed_operator_recovery')

def record_observed_loss(storage,version,fingerprint,reason):
    # Partial writes/pending files are themselves blocking negative evidence.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason})
    seal_first_construction(storage,key,version,fingerprint,body)

def original_state_present(storage,version):
    key=construction_record_key(version);base=Path(storage.local_dir).absolute()
    return (base/key).exists() or (base/(key+'.first-construction.json')).exists() or (base/(key+'.evidence')).exists()


def load_original_state(storage,version,captures,identity,table):
    body=parquet_bytes(expected_parquet(table));pin={'sha256':digest(body),'bytes':len(body)}
    return original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))


def baseline_audit(captures):
    """All five open published formats, IDs/native schema/geometry; NO source join."""
    metadata=strict_json(captures['baseline_metadata'][0])
    if metadata.get('nextPageToken'):raise CrushedStoneQuarantineError('incomplete_published_provider_listing')
    for name,pin in PINS.items():
        if name not in {'baseline_'+fmt for fmt in ('geojson','geopackage','geoparquet','pmtiles','shapefile','file_geodatabase')}:continue
        objects=[o for o in metadata.get('items',[]) if o.get('name')==urlsplit(pin['url']).path.split('/hifld-next-datasets-prod/')[1]]
        if len(objects)!=1 or int(objects[0].get('size',-1))!=pin['bytes'] or base64.b64encode(hashlib.md5(captures[name][0]).digest()).decode()!=objects[0].get('md5Hash'):raise CrushedStoneQuarantineError('published_size_MD5_or_complete_object_drift')
    common=['MINOP2X020','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN']
    q=pq.read_table(pa.BufferReader(captures['baseline_geoparquet'][0]));formats={'geoparquet':(q.to_pylist(),str(q.schema),'EPSG:3857','geometry',None)}
    geo=strict_json(captures['baseline_geojson'][0])
    if geo.get('type')!='FeatureCollection':raise CrushedStoneQuarantineError('published_geojson_incomplete_or_invalid')
    formats['geojson']=([dict(f['properties'],original_geojson_feature_id=f.get('id'),geometry=shapely.geometry.shape(f['geometry']).wkb) for f in geo['features']],'Original JSON property scalar types preserved in complete raw body; no physical DBF width inferred.','EPSG:4326','geometry','original_geojson_feature_id')
    physical_dbf=[]
    with tempfile.TemporaryDirectory(prefix='crushed-stone-baselines-') as tmp:
        for name,ext in [('geopackage','.gpkg'),('shapefile','.zip'),('file_geodatabase','.zip')]:
            path=Path(tmp)/(name+ext);path.write_bytes(captures['baseline_'+name][0]);read_path=path
            if name=='file_geodatabase':
                with zipfile.ZipFile(path) as archive:
                    roots={n.split('/')[0] for n in archive.namelist()}
                    if len(roots)!=1 or not next(iter(roots)).endswith('.gdb'):raise CrushedStoneQuarantineError('published_complete_GDB_container_drift')
                read_path='/vsizip/'+str(path)+'/'+next(iter(roots))
            elif name=='shapefile':
                with zipfile.ZipFile(path) as archive:
                    names=[n for n in archive.namelist() if n.endswith('.dbf')]
                    if len(names)!=1:raise CrushedStoneQuarantineError('published_DBF_roster_drift')
                    body=archive.read(names[0]);header=struct.unpack_from('<H',body,8)[0]
                    for offset in range(32,header-1,32):
                        desc=body[offset:offset+32];physical_dbf.append({'name':desc[:11].split(b'\0')[0].decode('ascii'),'type':chr(desc[11]),'width':desc[16],'decimals':desc[17]})
            meta,table=pyogrio.read_arrow(read_path,return_fids=True)
            if pyproj.CRS(meta['crs'])!=pyproj.CRS(3857):raise CrushedStoneQuarantineError('published_native_CRS_drift')
            formats[name]=(table.to_pylist(),str(table.schema),meta['crs'],meta['geometry_name'] or 'wkb_geometry',meta['fid_column'])
    report={};reference={row['MINOP2X020']:row for row in q.to_pylist()}
    if len(reference)!=766 or set(reference)!=set(range(1,767)):raise CrushedStoneQuarantineError('published_MINOP_construction_family_ID_roster_drift')
    for name,(rows,schema,crs,geometry,fid_column) in formats.items():
        if len(rows)!=766 or any(not set(common).issubset(row) for row in rows) or {row['MINOP2X020'] for row in rows}!=set(range(1,767)) or len({row['MINOP2X020'] for row in rows})!=766:raise CrushedStoneQuarantineError('published_complete_family_schema_ID_or_coverage_drift')
        if any(any(row[key]!=reference[row['MINOP2X020']][key] for key in common) for row in rows):raise CrushedStoneQuarantineError('published_crossformat_full_attribute_drift')
        ids=[key for key in ('OBJECTID','MINOP2X020','id','FID',fid_column) if key is not None and key in rows[0]];ids=list(dict.fromkeys(ids))
        geometries=shapely.from_wkb([row[geometry] for row in rows]);invalid=int((~shapely.is_valid(geometries)).sum())
        if invalid or any(shapely.is_empty(geometries)):raise CrushedStoneQuarantineError('published_invalid_or_empty_geometry')
        report[name]={'rows':len(rows),'original_schema':schema,'native_crs':crs,'original_identifier_columns':ids,'original_identifier_values':[{key:row[key] for key in ids} for row in rows],
            'driver_original_system_fid_column':fid_column,'all9_original_attribute_values_equal_to_published_parquet':True,'attribute_values_compared':len(rows)*9,
            'native_geometry_WKB_exact_with_published_parquet':sum(row[geometry]==reference[row['MINOP2X020']]['geometry'] for row in rows) if crs=='EPSG:3857' else None,
            'geometry_comparison_scope':'Within complete published formats only; no source join, reprojection, distance or tolerance. GeoJSON4326 is kept separate from3857 geometry.'}
    return {'approved':False,'classification':'All published candidates contain766MINOP construction-minerals records, whereas proposed historical source has2324CRSTON crushed-stone records; no compatible continuation or IDmapping accepted.',
        'complete_formats':report,'original_shapefile_DBF_physical_declarations':physical_dbf,'published_commodities':dict(collections.Counter(row['COMMODITY'] for row in reference.values())),
        'pmtiles':'Complete bytes/provider MD5 pinned; content not decoded or claimed lossless.','baseline_decision':'Blocked; dataset-identity/coverage/schema/source-selection decision required, not an accepted breaking correction.'}
