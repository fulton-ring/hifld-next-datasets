"""Explicit, unapproved USGS producer-successor reconstruction; never polls.

The only candidate inputs are the USGS-owned "Active Mines and Mineral
Processing Plants in the United States in 2003" distribution (MRDATA
mineplant) plus its FGDC metadata and the USGS National Atlas legacy FGDC
metadata of the retired Nonferrous Metal Mines layer (``minop6x020``,
publication 200512).  The mineplant product is the producer's own successor
publication of the same 2003 Minerals Information Team survey.  The retained
published HIFLD copy of the retired layer is pinned comparison context only
and is never a candidate input.  No default fetching, archive polling, USGS
release authority claim, datum correction, uppercase conversion, identifier
mapping or production release is performed.
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
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
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
SLUG='nonferrous-metal-mines'
PACKAGE='usgs-mrdata-mineplant-2003-nonferrous-metal-mines-v1'
READY='metadata/nonferrous_metal_mines_candidate_ready.json'
LICENSE='USGS metadata for the mineplant successor and the retired National Atlas layer states no use restrictions (National Minerals Information Center acknowledgment appreciated). No source selection, authority, datum, schema/ID mapping, contract or release acceptance.'
class NonferrousMinesQuarantineError(ValueError): pass

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise NonferrousMinesQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(NonferrousMinesQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise NonferrousMinesQuarantineError('json_object_required')
    return value

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise NonferrousMinesQuarantineError('immutable_capture_changed:'+key)

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
        raise NonferrousMinesQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise NonferrousMinesQuarantineError('linked_candidate_lock')
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
        raise NonferrousMinesQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise NonferrousMinesQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise NonferrousMinesQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if body!=captures[name][0]:raise NonferrousMinesQuarantineError('original_capture_bytes_differ')
        original[name]=(body,item['receipt'])
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'],identity=identity)
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise NonferrousMinesQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

# Only USGS-owned objects may feed candidate values; the retained published
# HIFLD copy of the retired layer is comparison context labeled as such in the
# source manifest and audit.
PINS={'usgs_mineplant_zip': {'url': 'https://mrdata.usgs.gov/mineplant/mineplant.zip', 'bytes': 513191, 'sha256': '6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e'}, 'usgs_mineplant_metadata': {'url': 'https://mrdata.usgs.gov/metadata/mineplant.xml', 'bytes': 26366, 'sha256': '419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0'}, 'legacy_metadata': {'url': 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop6x020.xml', 'bytes': 18443, 'sha256': '663509feaae108d27fbe98818384adf5cb23d122f3a0cb069af7ce25f031218a'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/nonferrous-metal-mines/nonferrous-metal-mines/v1.0.0/geoparquet/nonferrous-metal-mines.parquet', 'bytes': 21301, 'sha256': '1de4144906a419909377f307f3567bd3e42a23460e04739cbcc93a42925f7898'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/nonferrous-metal-mines/nonferrous-metal-mines/v1.0.0/geojson/nonferrous-metal-mines-geojson.geojson', 'bytes': 46876, 'sha256': 'a49dbed8cf33a3d53c854df47a19031d399f36392c6e686eedfc2bbcc7ea8056'}}
INPUT_NAMES=frozenset({'usgs_mineplant_zip','usgs_mineplant_metadata','legacy_metadata'})
COMPARISON_NAMES=frozenset(PINS)-INPUT_NAMES

MINEPLANT_MEMBERS={'mineplant.dbf':1655861,'mineplant.shp':190080,'mineplant.shx':54380,'mineplant.txt':670146,'mineplant.prj':368}
MINEPLANT_DBF_FIELDS=[('id','N',8,0),('COMMODITY','C',30,0),('SITE_NAME','C',50,0),('COMPANY_NA','C',50,0),('STATE_LOCA','C',40,0),('COUNTY','C',40,0),('LATITUDE','N',10,5),('LONGITUDE','N',10,5),('PLANT_MIN','C',5,0)]
MINEPLANT_RECORDS=6785
MINEPLANT_METADATA_TITLE='Active Mines and Mineral Processing Plants in the United States in 2003'
MINEPLANT_METADATA_ATTRS=('COMMODITY','ID','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN')
MINEPLANT_METADATA_METD='20201230'
# Producer scope of the retired National Atlas Nonferrous Metal Mines layer,
# pinned from the surviving USGS legacy FGDC metadata (publication 200512).
NONFERROUS_COMMODITIES=('Beryllium','Copper','Germanium','Gold','Lead','Silver','Zinc')
NONFERROUS_COMMODITY_COUNTS={'Beryllium':1,'Copper':22,'Germanium':2,'Gold':36,'Lead':11,'Silver':38,'Zinc':13}
MINES_ROLES=('M','M/P')
NONFERROUS_ROWS=123
NONFERROUS_STATES=13
ATLAS_TITLE='Nonferrous Metal Mines - Direct Download'
ATLAS_PUBDATE='200512'
ATLAS_PROCSTEP_DATE='200511'
ATLAS_METD='20060713'
ATLAS_ONLINK='http://dds.cr.usgs.gov/pub/data/nationalatlas/minop6x020_nt00023.tar.gz'
ATLAS_ID_FIELD='Minop6x020'
ATLAS_ID_RANGE=('1','123')
ATLAS_PLANT_MIN_DOMAIN=('M','M/P')
ATLAS_LATITUDE_RANGE=('31.429','68.070')
ATLAS_LONGITUDE_RANGE=('-162.865','-83.407')
ATLAS_EXCLUDED_COMMODITIES=('Iron','Cobalt','Molybdenum','Nickel','Rhenium')
# Physical layout of the pinned published HIFLD copy of the retired layer
# (comparison context only).
BASELINE_ROWS=136
BASELINE_FIELD_MAP=[('COMMODITY','COMMODITY'),('SITE_NAME','SITE_NAME'),('COMPANY_NAME','COMPANY_NA'),('STATE','STATE_LOCA'),('COUNTY','COUNTY'),('LATITUDE','LATITUDE'),('LONGITUDE','LONGITUDE'),('PLANT_MINE','PLANT_MIN')]
BASELINE_SCHEMA_NAMES={'geometry','MINERAL_OPERATIONS_ID','COMMODITY','SITE_NAME','COMPANY_NAME','STATE','COUNTY','LATITUDE','LONGITUDE','PLANT_MINE','bbox'}
BASELINE_GEOJSON_PROPS={'OBJECTID','MINERAL_OPERATIONS_ID','COMMODITY','SITE_NAME','COMPANY_NAME','STATE','COUNTY','LATITUDE','LONGITUDE','PLANT_MINE'}

MINEPLANT_PRJ='GEOGCS["GCS_North_American_1927",DATUM["D_North_American_1927",SPHEROID["Clarke_1866", 6378206.4, 294.9786982138982, AUTHORITY["EPSG","7008"]],TOWGS84[-9.0, 151.0, 185.0, 0.0, 0.0, 0.0, 0.0],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]],UNIT["degree", 0.017453292519943295],AXIS["Lon", EAST],AXIS["Lat", NORTH],AUTHORITY["EPSG","4267"]]'


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise NonferrousMinesQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir)
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise NonferrousMinesQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise NonferrousMinesQuarantineError('linked_staging_object')


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>limit:raise NonferrousMinesQuarantineError('unsafe_or_oversize_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    fields=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
    if len(body)>limit or len(body)!=before.st_size or fields(before)!=fields(after):raise NonferrousMinesQuarantineError('incomplete_or_changed_bounded_input')
    return body


def acquire(storage,capture_dir,package_id):
    local_only(storage)
    if package_id!=PACKAGE or not capture_dir:raise NonferrousMinesQuarantineError('explicit_unapproved_usgs_successor_package_selection_required_no_default_fetch')
    root=Path(capture_dir);captures={};attempted={}
    if '..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents]):raise NonferrousMinesQuarantineError('unsafe_input_package_root')
    try:
        expected={name+suffix for name in PINS for suffix in ('.body','.receipt.json')}
        if {p.name for p in root.iterdir()}!=expected:raise NonferrousMinesQuarantineError('incomplete_or_extra_explicit_package_roster')
        for name,pin in PINS.items():
            paths=[root/(name+suffix) for suffix in ('.body','.receipt.json')]
            if any(not p.is_file() or p.is_symlink() or p.stat().st_nlink!=1 for p in paths):raise NonferrousMinesQuarantineError('linked_or_nonregular_package_input')
            if paths[0].stat().st_size>2*1024*1024 or paths[1].stat().st_size>65536:raise NonferrousMinesQuarantineError('package_input_exceeds_bound')
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
            if len(body)!=pin['bytes'] or digest(body)!=pin['sha256']:raise NonferrousMinesQuarantineError('unreviewed_package_bytes_require_explicit_new_package:'+name)
        immutable(storage,f'_acquisition-attempts/{SLUG}/{uuid4().hex}.json',encode({'approved':False,'receipt_metadata_validated':True,'objects':attempted}))
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc),attempted);raise NonferrousMinesQuarantineError(str(exc)+'; retained:'+retained) from exc


def decode_mineplant_zip(body):
    try:archive=zipfile.ZipFile(io.BytesIO(body))
    except (OSError,ValueError,zipfile.BadZipFile,EOFError) as exc:raise NonferrousMinesQuarantineError('invalid_usgs_package_container') from exc
    names=archive.namelist()
    if len(names)!=len(set(names)) or set(names)!=set(MINEPLANT_MEMBERS) or any(archive.getinfo(name).file_size!=MINEPLANT_MEMBERS[name] for name in names):raise NonferrousMinesQuarantineError('usgs_package_member_roster_or_size_drift')
    return {name:archive.read(name) for name in MINEPLANT_MEMBERS}


def decode_mineplant_dbf(body):
    if len(body)<321:raise NonferrousMinesQuarantineError('incomplete_usgs_dbf')
    count,header,width=struct.unpack_from('<IHH',body,4);fields=[];offset=32
    while offset+32<=header and body[offset]!=13:
        desc=body[offset:offset+32];fields.append((desc[:11].split(b'\0')[0].decode('ascii'),chr(desc[11]),desc[16],desc[17]));offset+=32
    if fields!=MINEPLANT_DBF_FIELDS or count!=MINEPLANT_RECORDS or width!=244 or header!=321 or body[offset]!=13 or len(body) not in (header+count*width,header+count*width+1):raise NonferrousMinesQuarantineError('missing_renamed_extra_ID_type_width_count_or_DBF_completeness_drift')
    if len(body)==header+count*width+1 and body[-1]!=26:raise NonferrousMinesQuarantineError('unexpected_dbf_tail')
    rows=[];seen=set()
    for fid in range(count):
        rec=body[header+fid*width:header+(fid+1)*width]
        if rec[:1]!=b' ':raise NonferrousMinesQuarantineError('deleted_or_unknown_dbf_record')
        offset=1;row={'source_fid':fid,'source_shp_record_number':fid+1}
        for name,kind,size,scale in fields:
            raw=rec[offset:offset+size];offset+=size
            if kind=='C':
                if any(c>127 or c<32 for c in raw):raise NonferrousMinesQuarantineError('undeclared_nonASCII_source_encoding')
                row[name]=raw.rstrip(b' ').decode('ascii')
            else:
                text=raw.decode('ascii').strip()
                if not re.fullmatch(r'-?\d+(?:\.\d+)?',text):raise NonferrousMinesQuarantineError('null_nonfinite_or_unparseable_DBF_number')
                value=Decimal(text)
                if value.as_tuple().exponent<-scale:raise NonferrousMinesQuarantineError('declared_DBF_scale_drift')
                row[name]=value
        ident=int(row['id'])
        if ident<1 or ident in seen:raise NonferrousMinesQuarantineError('missing_duplicate_or_nonpositive_usgs_mineplant_id')
        seen.add(ident);rows.append(row)
    return rows


def crosscheck_mineplant_txt(body,rows):
    lines=body.decode('ascii').splitlines()
    if len(lines)!=len(rows)+1 or lines[0].split('\t')!=['ID','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN']:raise NonferrousMinesQuarantineError('usgs_txt_roster_or_header_drift')
    def norm(value): return '' if value.strip().upper()=='NULL' else value.rstrip()
    for line,row in zip(lines[1:],rows):
        cells=line.split('\t')
        if len(cells)!=9 or int(cells[0])!=int(row['id']) or norm(cells[1])!=norm(row['COMMODITY']) or norm(cells[2])!=norm(row['SITE_NAME']) or norm(cells[3])!=norm(row['COMPANY_NA']) or norm(cells[4])!=norm(row['STATE_LOCA']) or norm(cells[5])!=norm(row['COUNTY']) or Decimal(cells[6])!=row['LATITUDE'] or Decimal(cells[7])!=row['LONGITUDE'] or norm(cells[8])!=norm(row['PLANT_MIN']):raise NonferrousMinesQuarantineError('usgs_txt_dbf_value_drift')


def decode_points(body,index,count):
    if len(body)<100 or len(index)!=100+8*count:raise NonferrousMinesQuarantineError('incomplete_SHP_SHX')
    for data in (body,index):
        if struct.unpack_from('>i',data)[0]!=9994 or struct.unpack_from('>i',data,24)[0]*2!=len(data) or struct.unpack_from('<ii',data,28)!=(1000,1):raise NonferrousMinesQuarantineError('native_geometry_header_type_or_length_drift')
    out=[];offset=100
    for fid in range(count):
        if offset+28>len(body):raise NonferrousMinesQuarantineError('incomplete_point_records')
        record,words=struct.unpack_from('>ii',body,offset);typ,x,y=struct.unpack_from('<idd',body,offset+8)
        if record!=fid+1 or words!=10 or typ!=1 or struct.unpack_from('>ii',index,100+8*fid)!=(offset//2,10) or not all(math.isfinite(v) for v in (x,y)):raise NonferrousMinesQuarantineError('SHP_SHX_record_ID_offset_type_or_coordinate_drift')
        out.append(shapely.Point(x,y).wkb);offset+=28
    if offset!=len(body):raise NonferrousMinesQuarantineError('extra_point_records')
    geometry=shapely.from_wkb(out)
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise NonferrousMinesQuarantineError('invalid_empty_native_point')
    bounds=shapely.total_bounds(geometry).tolist()
    if list(struct.unpack_from('<4d',body,36))!=bounds or index[36:68]!=body[36:68]:raise NonferrousMinesQuarantineError('native_bounds_or_index_drift')
    return out


def verify_mineplant_geometry_values(rows,geometry):
    if len(rows)!=len(geometry):raise NonferrousMinesQuarantineError('usgs_record_geometry_alignment_drift')
    for row,wkb in zip(rows,geometry):
        point=shapely.from_wkb(wkb)
        if (point.x,point.y)!=(float(row['LONGITUDE']),float(row['LATITUDE'])):raise NonferrousMinesQuarantineError('usgs_geometry_attribute_value_drift')


def seven_commodity_rows(rows):
    """All-roles rows of the seven producer nonferrous commodity classes."""
    subset=[row for row in rows if row['COMMODITY'] in NONFERROUS_COMMODITIES]
    counts=dict(collections.Counter(row['COMMODITY'] for row in subset))
    if any(counts.get(commodity,0)<NONFERROUS_COMMODITY_COUNTS[commodity] for commodity in NONFERROUS_COMMODITIES):raise NonferrousMinesQuarantineError('nonferrous_seven_commodity_operations_drift')
    return subset


def mines_rows(rows):
    subset=[row for row in seven_commodity_rows(rows) if row['PLANT_MIN'] in MINES_ROLES]
    if len(subset)!=NONFERROUS_ROWS:raise NonferrousMinesQuarantineError('nonferrous_mines_subset_completeness_drift')
    if dict(collections.Counter(row['COMMODITY'] for row in subset))!=NONFERROUS_COMMODITY_COUNTS:raise NonferrousMinesQuarantineError('nonferrous_mines_subset_commodity_count_drift')
    if len({row['STATE_LOCA'] for row in subset})!=NONFERROUS_STATES:raise NonferrousMinesQuarantineError('nonferrous_mines_subset_state_coverage_drift')
    latitudes=[row['LATITUDE'] for row in subset];longitudes=[row['LONGITUDE'] for row in subset]
    if (min(latitudes),max(latitudes))!=(Decimal(ATLAS_LATITUDE_RANGE[0]),Decimal(ATLAS_LATITUDE_RANGE[1])) or (min(longitudes),max(longitudes))!=(Decimal(ATLAS_LONGITUDE_RANGE[0]),Decimal(ATLAS_LONGITUDE_RANGE[1])):raise NonferrousMinesQuarantineError('nonferrous_mines_subset_coordinate_range_drift')
    return subset


def atlas_scope(legacy_metadata_body,subset):
    """Pin the producer scope of the retired layer from the surviving USGS FGDC.

    Comparison-only authority anchor: proves the selected USGS successor subset
    matches the retired layer's declared entity-point count, identifier range,
    plant/mine domain, commodity classes and coordinate ranges.  It never feeds
    candidate values.
    """
    root=ET.fromstring(legacy_metadata_body)
    def text(path): return (root.findtext(path) or '').strip()
    if text('.//citation//title')!=ATLAS_TITLE or text('.//citation//pubdate')!=ATLAS_PUBDATE:raise NonferrousMinesQuarantineError('atlas_legacy_metadata_title_or_pubdate_drift')
    if text('.//onlink')!=ATLAS_ONLINK:raise NonferrousMinesQuarantineError('atlas_legacy_original_download_link_drift')
    if text('.//procstep//procdate')!=ATLAS_PROCSTEP_DATE or text('.//metd')!=ATLAS_METD:raise NonferrousMinesQuarantineError('atlas_legacy_metadata_process_or_metadata_date_drift')
    if text('.//horizdn')!='North American Datum of 1983':raise NonferrousMinesQuarantineError('atlas_legacy_metadata_datum_drift')
    declared=int(text('.//ptvctcnt'))
    if declared!=NONFERROUS_ROWS:raise NonferrousMinesQuarantineError('atlas_declared_entity_point_count_drift')
    id_attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip()==ATLAS_ID_FIELD),None)
    if id_attr is None or [e.text.strip() for e in id_attr.findall('.//rdommin')]+[e.text.strip() for e in id_attr.findall('.//rdommax')]!=list(ATLAS_ID_RANGE):raise NonferrousMinesQuarantineError('atlas_legacy_identifier_range_drift')
    plant_attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()=='plant_min'),None)
    domain=tuple(e.text.strip() for e in plant_attr.findall('.//edomv')) if plant_attr is not None else ()
    if domain!=ATLAS_PLANT_MIN_DOMAIN:raise NonferrousMinesQuarantineError('atlas_legacy_plant_mine_domain_drift')
    if next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()=='commodity'),None) is None:raise NonferrousMinesQuarantineError('atlas_legacy_commodity_attribute_missing')
    themekeys=[e.text.strip() for e in root.findall('.//themekey')]
    covered={key for key in themekeys if key in NONFERROUS_COMMODITIES}
    if covered!=set(NONFERROUS_COMMODITIES) or any(commodity in themekeys for commodity in ATLAS_EXCLUDED_COMMODITIES):raise NonferrousMinesQuarantineError('atlas_legacy_commodity_class_scope_drift')
    if 'extract nonferrous metals' not in (root.findtext('.//enttypd') or ''):raise NonferrousMinesQuarantineError('atlas_legacy_entity_description_drift')
    def declared_range(label):
        attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()==label),None)
        if attr is None:raise NonferrousMinesQuarantineError('atlas_legacy_'+label+'_attribute_missing')
        values=[e.text.strip() for e in attr.findall('.//rdommin')]+[e.text.strip() for e in attr.findall('.//rdommax')]
        if len(values)!=2:raise NonferrousMinesQuarantineError('atlas_legacy_'+label+'_range_incomplete')
        return tuple(values)
    latitude_range=declared_range('latitude');longitude_range=declared_range('longitude')
    if (latitude_range,longitude_range)!=(ATLAS_LATITUDE_RANGE,ATLAS_LONGITUDE_RANGE):raise NonferrousMinesQuarantineError('atlas_legacy_declared_coordinate_range_drift')
    latitudes=[row['LATITUDE'] for row in subset];longitudes=[row['LONGITUDE'] for row in subset]
    if (Decimal(latitude_range[0]),Decimal(latitude_range[1]))!=(min(latitudes),max(latitudes)) or (Decimal(longitude_range[0]),Decimal(longitude_range[1]))!=(min(longitudes),max(longitudes)):raise NonferrousMinesQuarantineError('atlas_declared_coordinate_ranges_do_not_match_selected_subset')
    return {'approved':False,'comparison_only':True,'purpose':'pin the producer scope of the retired National Atlas nonferrous-metal-mines layer','atlas_layer':ATLAS_TITLE,'atlas_pubdate':ATLAS_PUBDATE,'atlas_process_date':ATLAS_PROCSTEP_DATE,'atlas_metadata_date':ATLAS_METD,'atlas_original_download':ATLAS_ONLINK,'declared_entity_points':declared,'declared_plant_mine_domain':list(ATLAS_PLANT_MIN_DOMAIN),'declared_commodity_classes':list(NONFERROUS_COMMODITIES),'excluded_commodity_classes':list(ATLAS_EXCLUDED_COMMODITIES),'declared_latitude_range':list(ATLAS_LATITUDE_RANGE),'declared_longitude_range':list(ATLAS_LONGITUDE_RANGE),'subset_matches_declared_scope':True,'source':'U.S. Geological Survey National Geospatial Technical Operations Center legacy metadata WAF; retained producer metadata, not a data input'}


def mineplant_metadata_scope(body):
    root=ET.fromstring(body)
    title=(root.findtext('.//citation//title') or '').strip()
    if title!=MINEPLANT_METADATA_TITLE:raise NonferrousMinesQuarantineError('usgs_successor_metadata_title_drift')
    labels=tuple((attr.findtext('attrlabl') or '').strip() for attr in root.findall('.//attr'))
    if labels!=MINEPLANT_METADATA_ATTRS:raise NonferrousMinesQuarantineError('usgs_successor_metadata_attribute_roster_drift')
    if (root.findtext('.//metd') or '').strip()!=MINEPLANT_METADATA_METD:raise NonferrousMinesQuarantineError('usgs_successor_metadata_date_drift')
    if (root.findtext('.//accconst') or '').strip()!='None':raise NonferrousMinesQuarantineError('usgs_successor_metadata_access_constraint_drift')
    return {'title':title,'metadata_date':MINEPLANT_METADATA_METD,'attribute_roster':list(MINEPLANT_METADATA_ATTRS)}


def published_comparison(captures,subset,seven):
    """Prove the candidate subset against the retained published HIFLD copy.

    Comparison-only: the pinned published copy of the retired HIFLD layer is
    used to show that every one of its operations is present in the selected
    USGS successor package and that the candidate equals the producer-defined
    mines scope.  It never feeds candidate values, and any drift fails closed.
    """
    baseline=pq.read_table(pa.BufferReader(captures['baseline_geoparquet'][0]))
    published=strict_json(captures['baseline_geojson'][0])
    if baseline.num_rows!=BASELINE_ROWS or len(published.get('features',[]))!=BASELINE_ROWS or set(baseline.column_names)!=BASELINE_SCHEMA_NAMES:raise NonferrousMinesQuarantineError('published_baseline_schema_or_coverage_drift')
    pubrows=baseline.to_pylist()
    features={int(f['properties']['MINERAL_OPERATIONS_ID']):f for f in published['features']}
    if sorted(features)!=list(range(1,BASELINE_ROWS+1)) or {int(r['MINERAL_OPERATIONS_ID']) for r in pubrows}!=set(features):raise NonferrousMinesQuarantineError('published_identifier_roster_drift')
    for feature in published['features']:
        props=feature['properties']
        if set(props)!=BASELINE_GEOJSON_PROPS or type(props['OBJECTID']) is not int or type(feature['id']) is not int:raise NonferrousMinesQuarantineError('published_geojson_identity_or_property_drift')
    for row in pubrows:
        ident=int(row['MINERAL_OPERATIONS_ID']);props=features[ident]['properties']
        if int(props['OBJECTID'])!=ident or features[ident]['id']!=ident:raise NonferrousMinesQuarantineError('published_identifier_identity_drift')
        if props['COMMODITY']!=row['COMMODITY'] or props['SITE_NAME']!=row['SITE_NAME'] or props['COMPANY_NAME']!=row['COMPANY_NAME'] or props['STATE']!=row['STATE'] or props['COUNTY']!=row['COUNTY'] or float(props['LATITUDE'])!=float(row['LATITUDE']) or float(props['LONGITUDE'])!=float(row['LONGITUDE']) or props['PLANT_MINE']!=row['PLANT_MINE']:raise NonferrousMinesQuarantineError('published_crossformat_value_drift')
    identity=[{'published_MINERAL_OPERATIONS_ID':ident,'published_OBJECTID':features[ident]['properties']['OBJECTID'],'published_geojson_feature_id':features[ident]['id']} for ident in sorted(features)]
    published_ids=pa.Table.from_pylist(identity,schema=pa.schema([pa.field('published_MINERAL_OPERATIONS_ID',pa.int64(),nullable=False),pa.field('published_OBJECTID',pa.int64(),nullable=False),pa.field('published_geojson_feature_id',pa.int64(),nullable=False)]))
    def baseline_key(row): return tuple((Decimal(repr(float(row[source]))) if source in ('LATITUDE','LONGITUDE') else row[source]) for source,_ in BASELINE_FIELD_MAP)
    def successor_key(row): return tuple((Decimal(repr(float(row[successor]))) if successor in ('LATITUDE','LONGITUDE') else row[successor]) for _,successor in BASELINE_FIELD_MAP)
    index={}
    for row in seven:index.setdefault(successor_key(row),[]).append(row)
    matches=[];unmatched=[]
    for row in pubrows:
        candidates=index.get(baseline_key(row))
        if candidates:matches.append((row,candidates[0]));continue
        unmatched.append(int(row['MINERAL_OPERATIONS_ID']))
    if unmatched or len(matches)!=BASELINE_ROWS:raise NonferrousMinesQuarantineError('published_copy_operations_missing_from_usgs_successor:'+str(unmatched))
    consumed=collections.Counter(id(r) for _,r in matches)
    if len(consumed)>len(seven) or any(count!=1 for count in consumed.values()):raise NonferrousMinesQuarantineError('published_copy_match_ambiguity')
    matched_ids={id(r) for _,r in matches}
    successor_plants_not_published=[{'usgs_mineplant_id':str(row['id']),'COMMODITY':row['COMMODITY'],'SITE_NAME':row['SITE_NAME'],'COMPANY_NA':row['COMPANY_NA'],'STATE_LOCA':row['STATE_LOCA'],'COUNTY':row['COUNTY'],'LATITUDE':float(row['LATITUDE']),'LONGITUDE':float(row['LONGITUDE']),'PLANT_MIN':row['PLANT_MIN'],'reason':'present in the USGS successor seven-commodity operations but absent from the retained published HIFLD mines copy; it is a processing-only facility (Plant_min P)'} for row in seven if id(row) not in matched_ids]
    if len(successor_plants_not_published)!=len(seven)-BASELINE_ROWS or any(item['PLANT_MIN']!='P' for item in successor_plants_not_published):raise NonferrousMinesQuarantineError('successor_unpublished_operations_drift')
    mines_index={}
    for row in subset:mines_index.setdefault(successor_key(row),[]).append(row)
    subset_matches=[];subset_unmatched=[]
    for row in pubrows:
        if row['PLANT_MINE'] not in MINES_ROLES:continue
        candidates=mines_index.get(baseline_key(row))
        if candidates:subset_matches.append((row,candidates[0]));continue
        subset_unmatched.append(int(row['MINERAL_OPERATIONS_ID']))
    expected_mines_published=[row for row in pubrows if row['PLANT_MINE'] in MINES_ROLES]
    if subset_unmatched or len(subset_matches)!=NONFERROUS_ROWS or len(expected_mines_published)!=NONFERROUS_ROWS or len({id(r) for _,r in subset_matches})!=NONFERROUS_ROWS:raise NonferrousMinesQuarantineError('candidate_subset_does_not_equal_published_mines_scope:'+str(subset_unmatched))
    baseline_plant_only=[{'published_MINERAL_OPERATIONS_ID':int(row['MINERAL_OPERATIONS_ID']),'COMMODITY':row['COMMODITY'],'SITE_NAME':row['SITE_NAME'],'COMPANY_NAME':row['COMPANY_NAME'],'STATE':row['STATE'],'COUNTY':row['COUNTY'],'LATITUDE':float(row['LATITUDE']),'LONGITUDE':float(row['LONGITUDE']),'PLANT_MINE':row['PLANT_MINE'],'reason':'the retained published HIFLD copy includes this processing-only facility (Plant_min P); the producer mines layer documented by the pinned USGS metadata covers facilities that extract, or extract and process, nonferrous metals'} for row in pubrows if row['PLANT_MINE']=='P']
    if len(baseline_plant_only)!=BASELINE_ROWS-NONFERROUS_ROWS:raise NonferrousMinesQuarantineError('published_baseline_role_composition_drift')
    comparison={'approved':False,'comparison_only':True,'purpose':'prove the selected USGS successor package contains every operation of the retained published HIFLD nonferrous-metal-mines copy','published_copy_source':'retained published HIFLD v1 objects; comparison context only, never a pipeline input','published_rows':BASELINE_ROWS,'published_identifier_roster':'MINERAL_OPERATIONS_ID 1..136 contiguous; separate table retained, no approved cross-package mapping','published_plant_only_cases':baseline_plant_only,'successor_operations_without_published_row':successor_plants_not_published,'coordinate_relationship':'LATITUDE/LONGITUDE values are numerically identical in both USGS products; no datum transformation was applied by the producer','datum_claims_unresolved':{'usgs_mineplant_prj':'North American Datum of 1927','retained_atlas_metadata':'North American Datum of 1983','resolution':'UNAPPROVED; coordinates unchanged, each product PRJ as provided only'}}
    return comparison,published_ids


def inspect_captures(captures):
    if set(captures)!=set(PINS):raise NonferrousMinesQuarantineError('package_capture_roster_drift')
    for name,(body,receipt) in captures.items():
        validate_capture(name,body,receipt,PINS[name]['url'])
        if digest(body)!=PINS[name]['sha256'] or len(body)!=PINS[name]['bytes']:raise NonferrousMinesQuarantineError('pinned_package_drift:'+name)
    metadata_scope=mineplant_metadata_scope(captures['usgs_mineplant_metadata'][0])
    members=decode_mineplant_zip(captures['usgs_mineplant_zip'][0])
    if members['mineplant.prj'].decode('ascii')!=MINEPLANT_PRJ+'\n':raise NonferrousMinesQuarantineError('usgs_native_prj_drift')
    rows=decode_mineplant_dbf(members['mineplant.dbf'])
    crosscheck_mineplant_txt(members['mineplant.txt'],rows)
    geometry=decode_points(members['mineplant.shp'],members['mineplant.shx'],MINEPLANT_RECORDS)
    verify_mineplant_geometry_values(rows,geometry)
    subset=mines_rows(rows);seven=seven_commodity_rows(rows)
    fields=[pa.field('source_fid',pa.int64(),nullable=False,metadata={b'identity':b'Original zero-based USGS mineplant SHP/DBF physical record index of the nonferrous-mines subset row, not a published or replacement feature ID'}),pa.field('source_shp_record_number',pa.int32(),nullable=False,metadata={b'identity':b'Original USGS mineplant SHP record-header number'})]
    for name,kind,width,scale in MINEPLANT_DBF_FIELDS:fields.append(pa.field(name,pa.string() if kind=='C' else pa.decimal128(width,scale),nullable=False,metadata={b'original_DBF_physical_declaration':encode({'type':kind,'width':width,'decimals':scale})}))
    fields.append(pa.field('geometry',pa.binary(),nullable=False))
    table=pa.Table.from_pylist([{**{f[0]:row[f[0]] for f in MINEPLANT_DBF_FIELDS},'source_fid':row['source_fid'],'source_shp_record_number':row['source_shp_record_number'],'geometry':geometry[row['source_fid']]} for row in subset],schema=pa.schema(fields))
    scope=atlas_scope(captures['legacy_metadata'][0],subset)
    comparison,published_ids=published_comparison(captures,subset,seven)
    audit={'approved':False,'package_id':PACKAGE,'classification':'USGS-owned producer successor of the retired National Atlas nonferrous-metal-mines layer; unapproved draft reconstruction, not the original layer and not an authenticated historical archive of it','count':NONFERROUS_ROWS,'states':NONFERROUS_STATES,'commodities':dict(collections.Counter(row['COMMODITY'] for row in subset)),'mineplant_metadata':metadata_scope,
           'original_DBF_fields':MINEPLANT_DBF_FIELDS,'native_crs_claim':MINEPLANT_PRJ,'contradictory_original_USGS_metadata_datum':'North American Datum of 1983','retained_atlas_metadata_datum':'North American Datum of 1983','datum_resolution':'UNAPPROVED; coordinates numerically identical in both USGS products and unchanged; each product PRJ as provided only; no correction/transform',
           'physical_numeric_decode':'Exact DBF base10 decimal values preserved (stored exponents vary up to the declared 5-decimal scale); SHP point doubles equal the DBF values; no binary floating substitution','source_ids':'original USGS mineplant id plus original mineplant SHP/DBF record indices and record numbers, all distinct and unreplaced','published_identifiers':'complete published MINERAL_OPERATIONS_ID/OBJECTID and GeoJSON feature ids separately retained as comparison context, no mapping approved','release':'USGS mineplant FGDC publication 2005 from the same National Minerals Information Center survey as the retired layer publication 200512; exact release day unknown'}
    return table,published_ids,audit,comparison,scope


def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_wkt(MINEPLANT_PRJ).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'candidate_scope':PACKAGE.encode()})


def parquet_bytes(table):
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd');return sink.getvalue().to_pybytes()


def verify_outputs(root,table,published_ids):
    expected=parquet_bytes(expected_parquet(table))
    for name in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if (root/name).read_bytes()!=expected or not pq.read_table(root/name).equals(expected_parquet(table),check_metadata=True):raise NonferrousMinesQuarantineError('full_native_source_values_IDs_decimal_types_geometry_metadata_or_parquet_bytes_drift')
    if (root/'candidate/published-identifiers.parquet').read_bytes()!=parquet_bytes(published_ids):raise NonferrousMinesQuarantineError('complete_separate_published_identifiers_drift')


def manifest_for(captures,identity,parquet_pin):
    inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name];validate_capture(name,body,receipt,PINS[name]['url'])
        if name in INPUT_NAMES:
            role='producer_publication';use='candidate_input'
            producer='U.S. Geological Survey (National Minerals Information Center successor distribution of the 2003 survey)' if name!='legacy_metadata' else 'U.S. Geological Survey (National Atlas legacy metadata for the retired nonferrous-metal-mines layer)'
            attribution='USGS MRDATA mineplant successor package and USGS National Atlas legacy metadata; the pinned USGS metadata establishes the exact assigned nonferrous-metal-mines dataset family and its 200512 publication. No contract or release acceptance.'
        else:
            role='archive';use='comparison_only_retained_prior_research'
            producer='HIFLD Next existing published candidate, acceptance unresolved'
            attribution='Existing published HIFLD v1 objects retained solely to prove the published operation roster and comparison context. Never a pipeline input.'
        inputs.append({'input_id':name+':'+digest(body),'producer':producer,'host':urlsplit(receipt['url']).hostname,'role':role,'pipeline_use':use,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':attribution,'release_date_unknown_reason':'USGS mineplant FGDC publication year 2005 and National Atlas layer publication 200512 have no exact release day; retrieval and portal-edit times are not source release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':identity['code_commit'],'title':'Nonferrous Metal Mines — USGS producer-successor reconstruction (unapproved draft)','publisher':'U.S. Geological Survey Mineral Resources Program (MRDATA successor product)','agency':'U.S. Geological Survey','source_url':'https://mrdata.usgs.gov/mineplant/','license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'explicit_usgs_producer_successor_native_lossless_reconstruction','parameters':{'package_id':PACKAGE,'input_selection':'explicit local USGS package required, no default fetch or polling; only USGS-owned objects are candidate inputs','assigned_scope':'nonferrous metal mines as documented by the pinned USGS National Atlas metadata: facilities that extract, or extract and process, the seven USGS commodity classes beryllium, copper, germanium, gold, lead, silver and zinc','subset_selection':'rows of the USGS mineplant file whose COMMODITY equals one of the seven assigned classes and whose PLANT_MIN is M or M/P; pinned completeness 123 rows in 13 states, equal to the 123 entity points, identifier range 1..123 and declared coordinate ranges of the pinned metadata','geometry':'original USGS mineplant SHP coordinates and own NAD27 PRJ as provided; SHP point doubles equal the DBF values; contradictory NAD27/NAD83 claims of the two USGS metadata products retained and unresolved','successor_fidelity':'retained published HIFLD copy proves all 136 of its operations are present in the USGS successor seven-commodity rows; the candidate equals the producer-defined mines scope (123 rows); the 13 published processing-only facilities and the successor operations without a published row are retained as documented comparison context','IDs':'USGS mineplant id and original mineplant SHP/DBF record indices preserved; complete published identifiers separately retained, no approved cross-package mapping'}}],'unsupported_output_formats':{'shapefile_zip':'No accepted source/published schema mapping; the original USGS mineplant package is retained losslessly as raw evidence, not a newly approved output.','geopackage':'DBF decimal128 precision/scale and full Arrow physical schema cannot be silently coerced to GPKG REAL/INTEGER; no migration approved.','pmtiles':'Reprojection/quantization and datum choice are unapproved; native coordinates retained.'},'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all source decimal physical declarations, original IDs, strings, native WKB and metadata, no generic conversion or schema expansion. This is capability only, not input/contract/release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def source_payloads(captures,identity,audit,manifest,table,published_ids,comparison,scope):
    proof={'status':'pass','approved':False,'rows':NONFERROUS_ROWS,'method':'All USGS mineplant DBF records and physical declarations decoded and cross-checked against the producer TXT; nonferrous-mines subset selected; SHP/SHX native points decoded and tied to DBF values; all 123 values/IDs/decimal types/native WKB/schema metadata and exact deterministic Parquet bytes verified; full original USGS package retained. No source authority/datum/compatibility acceptance.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/nonferrous_metal_mines_source_audit.json':encode(audit),'metadata/published_comparison.json':encode(comparison),'metadata/atlas_scope.json':encode(scope),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_usgs_successor_input_authority_datum_schema_ID_mapping_and_baseline_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b'','candidate/published-identifiers.parquet':parquet_bytes(published_ids)}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,version,table,published_ids,audit,comparison,scope,captures,identity,pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise NonferrousMinesQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table,published_ids,comparison,scope);names={*expected,'candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise NonferrousMinesQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise NonferrousMinesQuarantineError('unknown_missing_or_linked_candidate_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise NonferrousMinesQuarantineError('invalid_staged_release_derivatives') from exc
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise NonferrousMinesQuarantineError('source_owned_report_raw_provenance_or_published_ID_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise NonferrousMinesQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise NonferrousMinesQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise NonferrousMinesQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table,published_ids)


def stage_nonferrous_metal_mines(storage,*,capture_dir=None,package_id=None):
    local_only(storage);captures=acquire(storage,capture_dir,package_id)
    try:
        table,published_ids,audit,comparison,scope=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({name:digest(body) for name,(body,_) in captures.items()}))
        version=f'v0.0.0-draft-usgs-mineplant-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise NonferrousMinesQuarantineError('linked_candidate')
                    body=parquet_bytes(expected_parquet(table));pin={'sha256':digest(body),'bytes':len(body)}
                    reconcile_cache(storage,prefix,version,table,published_ids,audit,comparison,scope,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise NonferrousMinesQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':NONFERROUS_ROWS,'reused':True,'package_id':PACKAGE,'accepted':False}
            with tempfile.TemporaryDirectory(prefix='.nonferrous-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                body=parquet_bytes(expected_parquet(table));(private/f'geoparquet/{SLUG}.parquet').write_bytes(body);(private/'candidate/typed-source.parquet').write_bytes(body);pin={'sha256':digest(body),'bytes':len(body)}
                manifest=manifest_for(captures,identity,pin)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison,scope).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'package_id':PACKAGE,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison,scope).items():
                    if (private/name).read_bytes()!=body:raise NonferrousMinesQuarantineError('incomplete_private_source_payload:'+name)
                # Validate source semantics + exact Git identity before the first seal;
                # the complete private candidate exists and has been verified first.
                validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids,comparison,scope).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY};(private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':NONFERROUS_ROWS,'reused':False,'package_id':PACKAGE,'accepted':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise NonferrousMinesQuarantineError(str(exc)+'; retained:'+retained) from exc
