"""Explicit, unapproved Stanford historical preservation-package reconstruction.

No default fetching, archive polling, USGS authority claim, datum correction,
uppercase conversion, identifier mapping, or production release is performed.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import re
import stat
import struct
import subprocess
import tempfile
import xml.etree.ElementTree as ET
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
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
SLUG='construction-minerals-operations'
PACKAGE='stanford-ds401wt4784-preservation-as-provided-v1'
READY='metadata/construction_minerals_candidate_ready.json'
LICENSE='USGS National Atlas metadata states no use restrictions; Stanford preserved package metadata retained. No source selection, authority, mapping, datum, contract or release acceptance.'
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
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted((root/'src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}

def retain_failure(storage,captures,reason):
    prefix=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,prefix+'/'+name+'.body',body);immutable(storage,prefix+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,prefix+'/reason.json',encode({'approved':False,'reason':reason}))
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
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
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
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise MineralsQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

PINS={'legacy_metadata': {'url': 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml', 'bytes': 19769, 'sha256': 'd68776d0c1c487d7b131c4abc0643c7e0ac9fbeac656144b585ca3c6044c3e94'}, 'stanford_publicxml': {'url': 'https://purl.stanford.edu/ds401wt4784.xml', 'bytes': 22726, 'sha256': '7e25e868315f45e54bc7d3bb29a1c64ede4b814071007838154c3c9e2a82d45f'}, 'minop2x020.dbf': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.dbf', 'bytes': 139734, 'sha256': 'c5f3ad0795515c7f3080445f7c759d2285724b5df268394b226fedae665dc90a'}, 'minop2x020.prj': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.prj', 'bytes': 145, 'sha256': 'a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4'}, 'minop2x020.shp': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shp', 'bytes': 21548, 'sha256': '4f35b155c9ac1379223184e5156523c21cfed821e2b29a1cb0b374d75b283b53'}, 'minop2x020.shp.xml': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shp.xml', 'bytes': 188838, 'sha256': 'b8d44bb1c9854cc7ceca85a25ea573d38a4efdc4b903e22b6b44de95ac12ba4a'}, 'minop2x020.shx': {'url': 'https://stacks.stanford.edu/file/druid:ds401wt4784/minop2x020.shx', 'bytes': 6228, 'sha256': 'b40ede8c63beec7d78a607ad90e3d768ec70e54caa08d77cbac46b3eec0e9548'}, 'baseline_geojson': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geojson/construction-minerals-operations-geojson.geojson', 'bytes': 275487, 'sha256': 'd8f5de1e147ae977d1a780a0940996805808c67623edb00d4ba38aa20e7f7e6d'}, 'baseline_geopackage': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geopackage/construction-minerals-operations-geopackage.gpkg', 'bytes': 270336, 'sha256': '9d18391d85ac6689596689011821799b6723b3cb9a5b633a6b4c13018a31b305'}, 'baseline_geoparquet': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/geoparquet/construction-minerals-operations.parquet', 'bytes': 76683, 'sha256': 'd5189e3387bce01681b454d035f56365e83f3d54bba34a3cbb2d388f22e4361d'}, 'baseline_pmtiles': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/pmtiles/construction-minerals-operations.pmtiles', 'bytes': 85733, 'sha256': '1d9b649236652f2ab62a7cf834be0d199f08fdd6f705073309bc4d2be62b32c6'}, 'baseline_shapefile': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/shapefile/construction-minerals-operations.zip', 'bytes': 47909, 'sha256': '3e0f83e7853f0e1d17c68603a23290cc881460e98634ae34888f002e8bde8da5'}, 'baseline_version': {'url': 'https://storage.googleapis.com/hifld-next-datasets-prod/releases/1704edb3-322d-469c-91da-92902fe76a2d/hifld/construction-minerals-operations/construction-minerals-operations/v1.0.0/collection.json', 'bytes': 12652, 'sha256': '217f441c7acf0d3763c46a22375fa2fafaf885cd6f5d5d9bcb63aaa918dc25d6'}, 'baseline_current_metadata': {'url': 'https://storage.googleapis.com/storage/v1/b/hifld-next-datasets-prod/o?prefix=hifld%2Fconstruction-minerals-operations%2Fconstruction-minerals-operations%2Fv1.0.0%2F&maxResults=100', 'bytes': 16676, 'sha256': '4f4b3e2b34aee2174790c05bc70bedd844ea9f993b0e15b3f1aac017d1ff29c7'}}

DBF_FIELDS=[('MINOP2X020','N',3,0),('COMMODITY','C',21,0),('SITE_NAME','C',46,0),('COMPANY_NA','C',51,0),('STATE_LOCA','C',14,0),('COUNTY','C',16,0),('LATITUDE','N',9,3),('LONGITUDE','N',11,3),('PLANT_MIN','C',10,0)]
SOURCE_NAMES=['source_fid','source_shp_record_number']+[f[0] for f in DBF_FIELDS]+['geometry']


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise MineralsQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir)
    if '..' in root.parts or storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise MineralsQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise MineralsQuarantineError('linked_staging_object')


def acquire(storage,capture_dir,package_id):
    local_only(storage)
    if package_id!=PACKAGE or not capture_dir:raise MineralsQuarantineError('explicit_unapproved_preservation_package_selection_required_no_default_fetch')
    root=Path(capture_dir);captures={}
    if '..' in root.parts or not root.is_dir() or any(p.is_symlink() for p in [root,*root.parents]):raise MineralsQuarantineError('unsafe_input_package_root')
    try:
        expected={name+suffix for name in PINS for suffix in ('.body','.receipt.json')}
        if {p.name for p in root.iterdir()}!=expected:raise MineralsQuarantineError('incomplete_or_extra_explicit_package_roster')
        for name,pin in PINS.items():
            paths=[root/(name+suffix) for suffix in ('.body','.receipt.json')]
            if any(not p.is_file() or p.is_symlink() or p.stat().st_nlink!=1 for p in paths):raise MineralsQuarantineError('linked_or_nonregular_package_input')
            if paths[0].stat().st_size>2*1024*1024 or paths[1].stat().st_size>65536:raise MineralsQuarantineError('package_input_exceeds_bound')
            body=paths[0].read_bytes();receipt=strict_json(paths[1].read_bytes());captures[name]=(body,receipt)
            validate_capture(name,body,receipt,pin['url'])
            if len(body)!=pin['bytes'] or digest(body)!=pin['sha256']:raise MineralsQuarantineError('unreviewed_package_bytes_require_explicit_new_package:'+name)
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc));raise MineralsQuarantineError(str(exc)+'; retained:'+retained) from exc


def decode_dbf(body):
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


def decode_points(body,index):
    if len(body)<100 or len(index)!=100+8*766:raise MineralsQuarantineError('incomplete_SHP_SHX')
    for data in (body,index):
        if struct.unpack_from('>i',data)[0]!=9994 or struct.unpack_from('>i',data,24)[0]*2!=len(data) or struct.unpack_from('<ii',data,28)!=(1000,1):raise MineralsQuarantineError('native_geometry_header_type_or_length_drift')
    out=[];offset=100
    for fid in range(766):
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
    rows=decode_dbf(captures['minop2x020.dbf'][0]);geometry=decode_points(captures['minop2x020.shp'][0],captures['minop2x020.shx'][0])
    for row,wkb in zip(rows,geometry):row['geometry']=wkb
    fields=[pa.field('source_fid',pa.int64(),nullable=False,metadata={b'identity':b'Original zero-based SHP physical record index, not a published or replacement feature ID'}),pa.field('source_shp_record_number',pa.int32(),nullable=False,metadata={b'identity':b'Original SHP record-header number'})]
    for name,kind,width,scale in DBF_FIELDS:fields.append(pa.field(name,pa.string() if kind=='C' else pa.decimal128(width,scale),nullable=False,metadata={b'original_DBF_physical_declaration':encode({'type':kind,'width':width,'decimals':scale})}))
    fields.append(pa.field('geometry',pa.binary(),nullable=False))
    table=pa.Table.from_pylist(rows,schema=pa.schema(fields))
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
    audit={'approved':False,'package_id':PACKAGE,'classification':'third-party historical preservation candidate as provided; not authenticated original agency bytes/current feed','count':766,'states':len({r['STATE_LOCA'] for r in rows}),'commodities':dict(collections.Counter(r['COMMODITY'] for r in rows)),
           'original_DBF_fields':DBF_FIELDS,'native_crs_claim':captures['minop2x020.prj'][0].decode('ascii'),'contradictory_original_USGS_metadata_datum':'North American Datum of 1983','datum_resolution':'UNAPPROVED; coordinates unchanged, package PRJ as provided only; no correction/transform',
           'physical_numeric_decode':'Exact DBF base10 decimal(width,scale), original text/record bytes retained in raw sidecars; no binary floating substitution','source_ids':'original implicit source_fid0..765, SHP record numbers1..766, MINOP2X020 decimal(3,0)1..766 all separate','published_identifiers':'complete published MINOP2X020/OBJECTID and GeoJSON FID/featureid separately retained, no mapping approved','release':'USGS metadata publication200512(active2003); Stanford preservation is a distinct package, original download authentication unresolved'}
    return table,published_ids,audit


def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_wkt(PRESERVED_PRJ).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'candidate_scope':PACKAGE.encode()})


PRESERVED_PRJ='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'

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
        role='producer_publication' if name=='legacy_metadata' else 'archive'
        inputs.append({'input_id':name+':'+digest(body),'producer':'U.S. Geological Survey (historical dataset metadata only)' if name=='legacy_metadata' else 'Stanford Libraries preservation' if name.startswith('minop2x020.') or name=='stanford_publicxml' else 'HIFLD Next existing published candidate, acceptance unresolved','host':urlsplit(receipt['url']).hostname,'role':role,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':'Stanford ds401wt4784 preservation-as-provided; USGS National Atlas exact dataset family metadata; baseline retained independently. No delegation or package-original authentication established.','release_date_unknown_reason':'Official source publication month200512(active2003), no exact release day; Stanford package preservation and baseline fetch times are not source release dates.'})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),'title':'Construction Minerals Operations — explicit unapproved Stanford historical preservation reconstruction','publisher':'Stanford Libraries preservation (not established agency distribution)','agency':'US Geological Survey historical dataset family; input authority unresolved','source_url':'https://purl.stanford.edu/ds401wt4784','license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'explicit_preservation_package_native_lossless_reconstruction','parameters':{'package_id':PACKAGE,'input_selection':'explicit local package required, no default fetch or polling; acceptance pending','geometry':'original SHP coordinates and own PRJ as provided; contradictory NAD83 metadata retained and unresolved','attributes':'ASCII right-padding decode only; mixed case, width and decimal values preserved, original DBF sidecars retained','IDs':'all native IDs distinct; complete published IDs separately retained, no approved cross-package mapping'}}],'unsupported_output_formats':{'shapefile_zip':'No accepted source/published schema mapping; preserved original five master sidecars remain lossless raw evidence, not a newly approved output.','geopackage':'DBF decimal128 precision/scale and full Arrow physical schema cannot be silently coerced to GPKG REAL/INTEGER; no migration approved.','pmtiles':'Reprojection/quantization and datum choice are unapproved; native coordinates retained.'},'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all source decimal physical declarations, original IDs, strings, native WKB and metadata, no generic conversion or schema expansion. This is capability only, not input/contract/release acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def source_payloads(captures,identity,audit,manifest,table,published_ids):
    proof={'status':'pass','approved':False,'rows':766,'method':'All original DBF records/physical declarations and SHP/SHX native points decoded; all766values/IDs/decimaltypes/nativeWKB/schema metadata and exact deterministic Parquet bytes verified; full original masters retained. No source authenticity/datum/compatibility acceptance.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/construction_minerals_source_audit.json':encode(audit),'metadata/parquet_source_fidelity.json':encode(proof),'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_preservation_input_source_authority_datum_schema_ID_mapping_and_baseline_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b'','candidate/published-identifiers.parquet':parquet_bytes(published_ids)}
    for name,(body,receipt) in captures.items():values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def reconcile_cache(storage,prefix,version,table,published_ids,audit,captures,identity,pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise MineralsQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table,published_ids);names={*expected,'candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise MineralsQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise MineralsQuarantineError('unknown_missing_or_linked_candidate_object')
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
        version=f'v0.0.0-draft-stanford-preservation-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
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
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':766,'reused':True,'package_id':PACKAGE,'accepted':False}
            with tempfile.TemporaryDirectory(prefix='.minerals-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                body=parquet_bytes(expected_parquet(table));(private/f'geoparquet/{SLUG}.parquet').write_bytes(body);(private/'candidate/typed-source.parquet').write_bytes(body);pin={'sha256':digest(body),'bytes':len(body)}
                manifest=manifest_for(captures,identity,pin)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'package_id':PACKAGE,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids)
                # Validate source semantics + exact Git identity before the first seal;
                # the complete private candidate exists and has been verified first.
                validate_release_provenance(manifest);validate_output_policy(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                for name,body in source_payloads(captures,identity,audit,manifest,table,published_ids).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY};(private/READY).write_bytes(encode(ready));verify_outputs(private,table,published_ids);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':766,'reused':False,'package_id':PACKAGE,'accepted':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise MineralsQuarantineError(str(exc)+'; retained:'+retained) from exc
