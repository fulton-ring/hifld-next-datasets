"""Distinct, unapproved current-USGS uranium/vanadium deposit subset.

This United States v3.0 product does not replace the parked worldwide MRDS
inventory. Preserve all native DBF fields, native per-file FIDs and WGS84
Point geometry; no source/contract/rights/baseline/release acceptance is implied.
"""
from __future__ import annotations

import io
import json
import math
from pathlib import Path
import struct
import tempfile
from urllib.parse import urlsplit
from uuid import uuid4
import zipfile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (candidate_prefix, capture_get, content_derived_version,
    processing_identity, quarantine_incomplete_capture, seal_ready_candidate,
    verify_cached_candidate, write_immutable)
from dagster_hifld.sources.esri_common import canonical, sha

DATASET_SLUG = 'usgs-critical-mineral-deposits'
FILE_SLUG = 'uranium-vanadium-deposits'
ITEM_ID = '6464de5bd34ec179a83d9e6c'
METADATA_URL = f'https://www.sciencebase.gov/catalog/item/{ITEM_ID}?format=json'
TITLE = 'Critical mineral deposits of the United States (ver. 3.0, September 2026)'
DOI = '10.5066/P9K1HBNT'
ARCHIVE_NAME = 'critical_mineral_deposits_v3.zip'
ARCHIVE_SHA256 = 'e11c52c07c7fd6f0fef41e865c797e813683e2687252b37c2a79e418e8f9ca77'
ARCHIVE_BYTES = 272122
ARCHIVE_MD5 = 'cd0368daa43c1534f57e52213bc2d5d3'
STEM = 'critical_mineral_deposits_v3/critical_mineral_deposits_v3'
MEMBER_SIZES = {STEM+ext:size for ext,size in {'.cpg':5,'.dbf':5873997,'.prj':145,'.sbn':12620,'.sbx':644,'.shp':35688,'.shp.xml':12860,'.shx':10268}.items()}
DBF_FIELDS = [('Deposit_na','C',254,0),('Lat_WGS84','F',19,11),('Long_WGS84','F',19,11),
    ('State','C',254,0),('MinSystem','C',254,0),('DepType','C',254,0),('CritMin','C',254,0),('DepCat','C',254,0),('Rank','N',10,0),
    *[(name,'C',254,0) for name in ['Prduction1','Prduction2','Prduction3','Resources1','Resources2','Resources3','Resources4','Comments1','Comments2','Comments3','References','Links']]]
EXPECTED_SOURCE_COUNT = 1271
EXPECTED_SELECTED_COUNT = 97
EXPECTED_URANIUM_COUNT = 81
EXPECTED_VANADIUM_COUNT = 38
EXPECTED_BOTH_COUNT = 22
EXPECTED_SOURCE_RECORD_SHA256 = 'eda7ff77eac5af73db068eaeba0d050b8d444aef454ca02c78b722838e2bca8d'
EXPECTED_SELECTED_RECORD_SHA256 = 'a3af277c7761c486b22d9335458261df850fed57c3c6ca50d2431d6a63cba606'
EXPECTED_SELECTED_ID_SHA256 = '5da5d2bc4b7bd7a31d3ee4fe9504a49e8ef52c6ac83698a9f81aad6f55a06c52'
REPO_ROOT = Path(__file__).parents[3]


def commodity_tokens(value: str | None) -> set[str]:
    """Split only producer comma-delimited whole commodity names."""
    return {token.strip().casefold() for token in (value or '').split(',') if token.strip()}


def parse_metadata(body: bytes) -> dict:
    """Require this producer's current v3 release and its original ZIP descriptor."""
    doc = json.loads(body)
    dates = {d.get('type'):d.get('dateString') for d in doc.get('dates',[])}
    publishers = [c.get('name') for c in doc.get('contacts',[]) if c.get('type') == 'Publisher']
    if (doc.get('id') != ITEM_ID or doc.get('title') != TITLE or DOI not in doc.get('citation','')
            or 'U.S. Geological Survey' not in publishers
            or dates.get('Publication') != '2023-07-18' or dates.get('lastRevision') != '2026-09-09'):
        raise ValueError('usgs_producer_release_identity_drift')
    files = [f for f in doc.get('files',[]) if f.get('name') == ARCHIVE_NAME]
    if len(files) != 1:
        raise ValueError('usgs_original_archive_descriptor_missing_or_duplicate')
    file = files[0]
    url = file.get('downloadUri','')
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.netloc != 'www.sciencebase.gov'
            or parsed.path != f'/catalog/file/get/{ITEM_ID}' or not parsed.query or parsed.fragment
            or file.get('size') != ARCHIVE_BYTES
            or file.get('checksum') != {'type':'MD5','value':ARCHIVE_MD5}):
        raise ValueError('usgs_original_archive_url_size_or_checksum_drift')
    return {'download_url':url,'publication_date':dates['Publication'],'last_revision':dates['lastRevision'],'title':doc['title']}


def record_sha(table: pa.Table) -> str:
    """Hash all logical native values and exact WKB, including per-file FIDs."""
    return sha(canonical([{**r,'wkb_geometry':r['wkb_geometry'].hex()} for r in table.to_pylist()]))


def select_deposits(table: pa.Table) -> tuple[pa.Table, dict]:
    """Validate the complete native population before applying whole-token selection."""
    expected = [('OGC_FID',pa.int64()), *[(name,pa.float64() if kind=='F' else pa.int64() if kind=='N' else pa.string()) for name,kind,_,_ in DBF_FIELDS], ('wkb_geometry',pa.binary())]
    if table.num_rows != EXPECTED_SOURCE_COUNT or [(f.name,f.type) for f in table.schema] != expected:
        raise ValueError('native_population_schema_or_physical_type_drift')
    if table['OGC_FID'].to_pylist() != list(range(EXPECTED_SOURCE_COUNT)):
        raise ValueError('native_per_file_fid_missing_duplicate_or_reordered')
    geometries = table['wkb_geometry'].to_pylist()
    if any(g is None or len(g)!=21 for g in geometries):
        raise ValueError('native_point_geometry_missing_or_changed')
    points = shapely.from_wkb(geometries)
    if (not bool(shapely.is_valid(points).all()) or not bool((shapely.get_type_id(points)==0).all())
            or bool(shapely.has_z(points).any()) or bool(shapely.is_empty(points).any())):
        raise ValueError('native_point_geometry_type_or_validity_drift')
    bounds = shapely.total_bounds(points).tolist()
    if not all(math.isfinite(v) for v in bounds) or not (-180<=bounds[0]<=bounds[2]<=180 and -90<=bounds[1]<=bounds[3]<=90):
        raise ValueError('native_point_geometry_coverage_drift')
    if record_sha(table) != EXPECTED_SOURCE_RECORD_SHA256:
        raise ValueError('whole_source_record_or_geometry_drift')
    tokens = [commodity_tokens(value) for value in table['CritMin'].to_pylist()]
    uranium = sum('uranium' in names for names in tokens)
    vanadium = sum('vanadium' in names for names in tokens)
    both = sum({'uranium','vanadium'} <= names for names in tokens)
    selected = table.filter(pa.array([bool({'uranium','vanadium'} & names) for names in tokens]))
    ids_sha = sha(canonical(selected['OGC_FID'].to_pylist()))
    if ((uranium,vanadium,both,selected.num_rows) != (EXPECTED_URANIUM_COUNT,EXPECTED_VANADIUM_COUNT,EXPECTED_BOTH_COUNT,EXPECTED_SELECTED_COUNT)
            or ids_sha != EXPECTED_SELECTED_ID_SHA256 or record_sha(selected) != EXPECTED_SELECTED_RECORD_SHA256):
        raise ValueError('commodity_selection_population_identifier_or_value_drift')
    return selected, {'source_feature_count':table.num_rows,'feature_count':selected.num_rows,
        'uranium_count':uranium,'vanadium_count':vanadium,'both_count':both,
        'source_record_sha256':record_sha(table),'selected_record_sha256':record_sha(selected),
        'source_id_sha256':sha(canonical(table['OGC_FID'].to_pylist())), 'selected_id_sha256':ids_sha,
        'source_schema_sha256':sha(canonical([(f.name,str(f.type)) for f in table.schema])),
        'source_geometry_sha256':sha(canonical([g.hex() for g in geometries])),
        'source_bounds':bounds,'crs_epsg':4326,'native_fid_column':'OGC_FID',
        'native_fid_scope':'original shapefile feature index; not a global or cross-release agency ID',
        'null_geometry_count':0,'invalid_geometry_count':0,'population_quality_scope':'all_rows'}


def decode_archive(body: bytes) -> tuple[pa.Table, dict]:
    """Check the entire ZIP/DBF/SHP/SHX and cross-check the native Arrow reader."""
    if len(body) != ARCHIVE_BYTES or sha(body) != ARCHIVE_SHA256:
        raise ValueError('complete_usgs_archive_bytes_drift')
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        entries = [i for i in archive.infolist() if not i.is_dir()]
        if (len(entries)!=len(MEMBER_SIZES) or {i.filename:i.file_size for i in entries} != MEMBER_SIZES
                or len({i.filename for i in archive.infolist()}) != len(archive.infolist())):
            raise ValueError('native_archive_member_roster_or_size_drift')
        parts = {name:archive.read(name) for name in MEMBER_SIZES}
    dbf = parts[STEM+'.dbf']
    count,header,width = struct.unpack_from('<IHH',dbf,4)
    fields=[]
    for offset in range(32,header-1,32):
        field=dbf[offset:offset+32]
        fields.append((field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
    if (fields!=DBF_FIELDS or count!=EXPECTED_SOURCE_COUNT or header!=32+32*len(DBF_FIELDS)+1
            or width!=1+sum(f[2] for f in DBF_FIELDS) or dbf[header-1]!=13
            or len(dbf)!=header+count*width+1 or dbf[-1]!=26
            or any(dbf[header+i*width]!=32 for i in range(count))):
        raise ValueError('native_dbf_schema_population_deleted_record_or_completeness_drift')
    source_crs=read_source_crs(parts[STEM+'.prj'])
    if parts[STEM+'.cpg'] != b'UTF-8':
        raise ValueError('native_source_encoding_or_crs_drift')
    shp,shx = parts[STEM+'.shp'],parts[STEM+'.shx']
    if len(shp)!=100+count*28 or len(shx)!=100+count*8:
        raise ValueError('native_shp_shx_population_incomplete')
    for component in (shp,shx):
        if struct.unpack_from('>i',component)[0]!=9994 or struct.unpack_from('>i',component,24)[0]*2!=len(component) or struct.unpack_from('<ii',component,28)!=(1000,1):
            raise ValueError('native_shapefile_header_or_geometry_type_drift')
    native_wkb=[]
    for fid in range(count):
        offset=100+fid*28
        number,words=struct.unpack_from('>ii',shp,offset)
        typ,x,y=struct.unpack_from('<idd',shp,offset+8)
        if (number!=fid+1 or words!=10 or typ!=1 or not all(math.isfinite(v) for v in (x,y))
                or struct.unpack_from('>ii',shx,100+fid*8)!=(offset//2,10)):
            raise ValueError('native_shp_record_identifier_geometry_or_shx_offset_drift')
        native_wkb.append(struct.pack('<BIdd',1,1,x,y))
    with tempfile.TemporaryDirectory(prefix='usgs-critical-minerals-') as temporary:
        path=Path(temporary)/ARCHIVE_NAME
        path.write_bytes(body)
        metadata,table=pyogrio.read_arrow('/vsizip/'+str(path)+'/'+STEM+'.shp',return_fids=True)
    if (metadata['fid_column']!='OGC_FID' or metadata['geometry_type']!='Point'
            or pyproj.CRS.from_user_input(metadata['crs'])!=pyproj.CRS.from_epsg(4326)
            or table['wkb_geometry'].to_pylist()!=native_wkb):
        raise ValueError('native_reader_fid_crs_or_exact_geometry_mismatch')
    selected,audit=select_deposits(table)
    return selected,{**audit,'native_crs':source_crs.to_json_dict()}


def read_source_crs(body: bytes) -> pyproj.CRS:
    """Retain the original WGS84 WKT axes without reprojection or normalization."""
    try:
        crs=pyproj.CRS.from_wkt(body.decode())
    except (pyproj.exceptions.CRSError,UnicodeDecodeError) as exc:
        raise ValueError('native_source_crs_invalid') from exc
    if crs.to_epsg()!=4326:
        raise ValueError('native_source_crs_drift')
    return crs


def capture_deposits(storage: StagingStorageResource, *, capture_directory: str | None = None) -> dict:
    """Acquire the full current producer release and seal its unapproved U/V subset."""
    if (not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError('explicit_local_staging_resource_required')
    base=Path(storage.local_dir).absolute()
    if '..' in base.parts or any(p.is_symlink() for p in (base,*base.parents)):
        raise ValueError('unsafe_staging_root')
    attempt=(Path(capture_directory) if capture_directory else Path('/tmp/hifld-native-reconstruction/352-current-usgs'))/uuid4().hex
    attempt=attempt.absolute()
    if '..' in attempt.parts or any(p.is_symlink() for p in (attempt,*attempt.parents)):
        raise ValueError('unsafe_capture_directory')
    attempt.mkdir(parents=True,exist_ok=True)
    captures={}
    try:
        with httpx.Client(follow_redirects=False,timeout=90,headers={'User-Agent':'hifld-next-datasets/current-usgs-uv-draft'}) as client:
            captures['metadata_before']=capture_get(client,METADATA_URL,attempt,'metadata_before',max_bytes=2*1024*1024)
            release=parse_metadata(captures['metadata_before'][0])
            captures['archive']=capture_get(client,release['download_url'],attempt,'archive',max_bytes=1024*1024)
            selected,audit=decode_archive(captures['archive'][0])
            captures['metadata_after']=capture_get(client,METADATA_URL,attempt,'metadata_after',max_bytes=2*1024*1024)
            if parse_metadata(captures['metadata_after'][0])!=release:
                raise ValueError('source_release_changed_during_capture')
    except Exception as exc:
        # The shared capture helper persists bounded partial HTTP evidence too.
        for path in attempt.glob('*.receipt.json'):
            name=path.name.removesuffix('.receipt.json')
            if name not in captures and (attempt/f'{name}.body').is_file() and (attempt/f'{name}.wire').is_file():
                captures[name]=((attempt/f'{name}.body').read_bytes(),json.loads(path.read_bytes()))
        if captures:
            quarantine_incomplete_capture(storage,attempt,DATASET_SLUG,'unapproved',captures,str(exc))
        raise
    identity=processing_identity(REPO_ROOT)
    identity['gdal']=pyogrio.__gdal_version_string__
    audit={**audit,'release':release,'native_dbf_fields':DBF_FIELDS,'source_population_complete':True,
        'freshness_accepted':False,'schema_contract_accepted':False,'id_contract_accepted':False,
        'rights_accepted':False,'mapping_approved':False,'release_accepted':False}
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{
        'encoding':'WKB','geometry_types':['Point'],'crs':audit['native_crs'],'edges':'planar'}}}
    selected=selected.replace_schema_metadata({**(selected.schema.metadata or {}),b'geo':canonical(geo)})
    sink=pa.BufferOutputStream()
    pq.write_table(selected,sink,compression='zstd')
    parquet=sink.getvalue().to_pybytes()
    relative=f'geoparquet/{FILE_SLUG}.parquet'
    descriptor={'bytes':len(parquet),'sha256':sha(parquet)}
    version=content_derived_version(audit['source_record_sha256'],audit['source_geometry_sha256'],audit['source_schema_sha256'],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    state_key=f'_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}.json'
    fingerprint=sha(canonical(identity))
    result={'dataset_slug':DATASET_SLUG,'file_slug':FILE_SLUG,'version':version,
        'feature_count':selected.num_rows,'source_feature_count':audit['source_feature_count'],
        'uranium_count':audit['uranium_count'],'vanadium_count':audit['vanadium_count'],'both_count':audit['both_count'],
        'source_population_complete':True,'source_id_sha256':audit['source_id_sha256'],
        'source_schema_sha256':audit['source_schema_sha256'],'candidate_ready':prefix+'/metadata/candidate_ready.json',
        'null_geometry_count':0,'invalid_geometry_count':0,'release_accepted':False}
    if storage.object_snapshot(state_key) is not None:
        payload=json.loads(load_first_construction(storage,state_key,version,fingerprint))['payload']
        verify_cached_candidate(storage,REPO_ROOT,state_key,prefix,version,fingerprint,identity,
            audit['source_record_sha256'],audit['source_schema_sha256'],payload['objects'],payload['capture_receipts'])
        return {**result,'reused':True}
    manifest={
        'schema_version':1,'approved':False,'title':'Current USGS uranium and vanadium deposits (United States)',
        'description':'Unapproved whole-token uranium/vanadium subset of the complete USGS critical mineral deposits v3.0 release. Distinct from the parked worldwide MRDS dataset; no MRDS equivalence or coverage claim.',
        'publisher':'U.S. Geological Survey','agency':'U.S. Geological Survey','source_url':METADATA_URL,
        'license':None,'source_modified':release['last_revision'],'date_issued':release['publication_date'],
        'date_modified':release['last_revision'],'processor':'HIFLD Next','code_commit':identity['code_commit'],
        'inputs':[{'input_id':name,'producer':'U.S. Geological Survey','host':urlsplit(receipt['url']).hostname,
            'role':'producer_publication','canonical_url':receipt['url'],'retrieval_url':receipt['url'],
            'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':receipt['status'],
            'bytes':receipt['bytes'],'sha256':receipt['sha256'],'release_date':release['last_revision'],
            'license':'USGS ScienceBase data release; rights acceptance pending.','attribution':TITLE+'; DOI '+DOI}
            for name,(_,receipt) in sorted(captures.items())],
        'transformations':[{'operation':'whole_token_commodity_subset','parameters':{
            'field':'CritMin','delimiter':',','tokens':['uranium','vanadium'],'comparison':'strip and casefold each complete comma-delimited token',
            'source_population':audit['source_feature_count'],'selected_population':selected.num_rows,'attributes':'all 21 native DBF fields, names, physical types and logical values unchanged',
            'identifiers':'OGC_FID is the native reader per-file feature identifier; preserve unchanged; not a global or cross-release agency ID',
            'geometry':'original WGS84 Point coordinates, exact WKB; no repair or reprojection'}}],
        'lineage':'USGS ScienceBase item '+ITEM_ID+', DOI '+DOI+', v3.0 September 2026. User-authorized separate current-USGS candidate; original worldwide uranium-and-vanadium-deposits MRDS inventory remains parked. ZIP, native sidecars and HTTP captures retained losslessly; no mirrors or archive copies are inputs.',
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Native whole-token selected attributes, per-file FIDs and exact source geometry; all publication approvals remain false.',
            'objects':[{'path':relative,**descriptor}]}}},
        'unsupported_output_formats':{'shapefile_zip':'No approved public mapping.','geopackage':'No approved public mapping.','pmtiles':'No approved display transformation.'}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs={relative:parquet,'metadata/source_manifest.json':canonical(manifest)+b'\n',
        'metadata/source_audit.json':canonical(audit)+b'\n','metadata/processing_identity.json':canonical(identity)+b'\n',
        'metadata/candidate_ready.json':canonical({'approved':False,'version':version,'objects':{relative:descriptor},
            'freshness_accepted':False,'schema_contract_accepted':False,'id_contract_accepted':False,
            'rights_accepted':False,'mapping_approved':False,'release_accepted':False})+b'\n'}
    for name,(body,receipt) in captures.items():
        outputs[f'raw/{name}.body']=body
        outputs[f'raw/{name}.wire']=(attempt/f'{name}.wire').read_bytes()
        outputs[f'raw/{name}.receipt.json']=canonical(receipt)+b'\n'
    descriptors={name:{'bytes':len(body),'sha256':sha(body)} for name,body in outputs.items()}
    for name,body in outputs.items():
        key=f'{prefix}/{name}'
        write_immutable(storage,key,body)
        before=storage.object_snapshot(key)
        if before is None or before.size!=len(body) or storage.sha256_key(key)!=sha(body) or storage.object_snapshot(key)!=before:
            raise ValueError('candidate_physical_readback_failed:'+name)
    validate_construction_code_identity(REPO_ROOT,identity['code_commit'],identity['files'],identity=identity)
    seal_ready_candidate(storage,state_key,version,fingerprint,{'processing_identity':identity,
        'source_record_sha256':audit['source_record_sha256'],'source_schema_sha256':audit['source_schema_sha256'],
        'capture_receipts':{name:receipt for name,(_,receipt) in captures.items()},'objects':descriptors})
    return {**result,'capture_directory':str(attempt)}
