"""Faithful local draft of the exact inventory's Census 2026 national-GDB MSA layer.

The pipeline pins the current Census TIGER/Line national geodatabase edition,
re-published annually; the 2026 object supersedes the retired 2024 ``_a_``
edition named by the inventory row. Metropolitan Statistical Areas are the
``Core_Based_Statistical_Area`` features with ``MEMI`` equal to ``1``; the
complementary micropolitan selection is validated but never staged. An edition
label is not proof of a release date or approved compatibility. No projection,
ID substitution, geometry repair or area-type migration occurs here.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.gdal import with_geopackage_creation_time
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.capture_common import (
    processing_identity as package_processing_identity, seal_ready_candidate, verify_cached_candidate)
from dagster_hifld.validation import sha256_file

SLUG = 'metropolitan-statistical-areas'
LAYER = 'Core_Based_Statistical_Area'
GDB = 'tlgdb_2026_us_nationgeo.gdb'
URL = 'https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_nationgeo.gdb.zip'
SHA256 = '93b8c6da72cf68832d69b3e72a766d6dc7df89f0d886ed2f965da8f6d0c1d354'
BYTES = 273107184
FIELDS = ('OBJECTID','CSAFP','CBSAFP','GEOID','GEOIDFQ','NAME','NAMELSAD','LSAD','MEMI','MTFCC','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
# Producer OID is esriFieldTypeOID Length4. This int64 is the GDAL Arrow
# return_fids/output representation, not a native-width declaration/approval.
# CSAFP is producer-nullable and observed empty on exactly 112 of the 393
# metropolitan features (CBSAs belonging to no CSA); every other field is fully
# populated on the selection.
TYPES = (pa.int64(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())
# Measured against the pinned 2026 archive: the layer holds 935 CBSAs, the
# metropolitan (MEMI=1) selection is 393 features, the complementary
# micropolitan (MEMI=2) rows are 542, and CSAFP is null on 353 layer rows
# (112 metropolitan, 241 micropolitan).
TOTAL_ROWS = 935
METROPOLITAN_ROWS = 393
MICROPOLITAN_ROWS = 542
NULLS = {'CSAFP': 112}
LAYER_NULLS = {'CSAFP': 353}
# Producer OBJECTIDs (sorted) of the metropolitan selection in the pinned
# archive; metro and micro rows interleave inside the layer, so the exact
# membership also guards the MEMI=1 selection against silent substitution.
METRO_OBJECTIDS = (1,2,5,7,8,10,14,15,16,19,20,22,25,27,28,29,30,33,34,36,38,39,43,44,47,49,50,53,55,56,58,60,70,73,76,77,79,80,81,83,84,89,94,95,98,99,101,104,106,107,112,114,115,117,121,122,123,125,126,131,134,136,137,138,144,148,151,154,155,157,158,165,169,170,174,178,181,182,183,184,188,189,193,194,195,196,199,200,204,207,209,212,216,217,220,222,223,224,225,226,229,233,236,239,240,242,243,246,248,249,250,251,255,256,257,258,264,265,269,270,271,273,275,276,278,280,284,293,294,296,298,299,300,301,302,304,305,306,311,316,319,323,324,326,331,333,338,342,344,345,346,347,348,349,357,359,360,366,367,374,375,377,378,379,384,385,388,390,396,397,399,401,402,403,408,409,412,416,418,419,420,421,422,424,425,426,429,431,433,434,443,445,446,447,451,452,454,459,460,461,463,467,470,471,474,478,479,480,483,485,487,490,496,503,504,505,508,510,513,515,519,521,523,524,525,527,528,530,531,532,537,541,542,543,544,547,548,554,555,556,558,562,564,577,580,583,584,590,591,593,595,596,597,598,599,600,603,604,606,607,608,609,610,611,613,614,616,618,622,624,628,629,637,638,640,642,644,653,655,661,662,667,672,676,677,678,679,681,683,684,686,688,691,694,703,705,706,707,708,709,711,712,713,723,727,731,732,733,737,743,744,746,748,751,753,754,755,762,767,771,776,778,779,783,784,785,786,787,788,789,790,791,792,795,800,802,810,815,817,820,821,826,828,829,831,832,834,837,838,839,848,851,852,853,855,856,857,858,861,864,866,868,871,872,877,878,883,884,886,888,890,891,894,895,896,897,902,903,904,905,908,909,911,912,920,921,922,924,930,931,933,934,935)
BASELINE_ROOT = 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/metropolitan-statistical-areas/tl_2024_us_cbsa_metropolitan/v1.0.0'
BASELINE_COLLECTION = {'name':'collection.json','url':BASELINE_ROOT+'/collection.json','bytes':8883,'generation':'1791506395935828','md5':'7a4b9f4ba7911d8b2a21e888035d64bc','sha256':'2ba7ca223d7e70cfc00e361496cb2bd4b8d0837ea3dee64dac1c30667f7ab063'}
BASELINE_ASSETS = {
    'geopackage-099d15d31cd4': {'name':'tl_2024_us_cbsa_metropolitan.gpkg','format':'geopackage','url':BASELINE_ROOT+'/geopackage/tl_2024_us_cbsa_metropolitan.gpkg','bytes':31645696,'generation':'1791506395831146','md5':'6068f10b0bac2a4db666665d024bd4e0','collection_checksum':'d501106068f10b0bac2a4db666665d024bd4e0','sha256':'7bee46440dcf2d6cdba394ff7cfa044a624c1f6d5eee79ed95c4e2d51536d5e4'},
    'geoparquet-8f0bbf5d1822': {'name':'tl_2024_us_cbsa_metropolitan.parquet','format':'geoparquet','url':BASELINE_ROOT+'/geoparquet/tl_2024_us_cbsa_metropolitan.parquet','bytes':12724212,'generation':'1791506395813016','md5':'d3d0395fe708deaa8c217c15b3b284f8','collection_checksum':'d50110d3d0395fe708deaa8c217c15b3b284f8','sha256':'77f0a79939d6427f63ea16f414c168fa86b1b570d01c6cdfe2549417aa516576'},
    'pmtiles-8e45458b1646': {'name':'tl_2024_us_cbsa_metropolitan.pmtiles','format':'pmtiles','url':BASELINE_ROOT+'/pmtiles/tl_2024_us_cbsa_metropolitan.pmtiles','bytes':5088752,'generation':'1791506395884381','md5':'30c35d2c497c3e204c5fe1ab1d5de4d3','collection_checksum':'d5011030c35d2c497c3e204c5fe1ab1d5de4d3','sha256':'ef5a4c5c9eb50381534215293408a86062cf52bd906b59ab20f36a4c9e7a3eea'},
    'shapefile-4fbedf63da90': {'name':'tl_2024_us_cbsa_metropolitan.zip','format':'shapefile_zip','url':BASELINE_ROOT+'/shapefile/tl_2024_us_cbsa_metropolitan.zip','bytes':19885949,'generation':'1791506395966861','md5':'3ba1b58efffeb380bb991c660c86c012','collection_checksum':'d501103ba1b58efffeb380bb991c660c86c012','sha256':'3468341b05dc481f8f5da15022ad45d1c8e724c6e2fa2841bf6ba6df7f84ae4a'},
}

class MsaQuarantineError(ValueError):
    """A changed or incomplete input requires review, never implicit replacement."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result = {}
        for key,value in items:
            if key in result:
                raise MsaQuarantineError('duplicate_json_key')
            result[key] = value
        return result
    value = json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value:
        raise MsaQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise MsaQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    for path in [root,*root.parents]:
        if path.is_symlink():
            raise MsaQuarantineError('staging_path_symlink_refused')
    if root.exists():
        for path in root.rglob('*'):
            if path.is_symlink() or (path.is_file() and path.stat().st_nlink!=1):
                raise MsaQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise MsaQuarantineError('immutable_capture_changed:'+key)


def quarantine(storage,body,receipt,reason):
    key = f'_quarantine/{SLUG}/{uuid4().hex}'
    immutable(storage,key+'/response.body',body)
    immutable(storage,key+'/receipt.json',encode(receipt))
    immutable(storage,key+'/reason.json',encode({'approved':False,'reason':reason}))
    return key


def manifest_for(body,receipt,identity):
    # This validator runs before canonical output is created, even on cache reuse.
    if (receipt.get('url') != URL or receipt.get('final_url') != URL
        or type(receipt.get('status')) is not int or receipt['status'] != 200
        or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
        or receipt.get('sha256') != digest(body) or receipt.get('complete_body') is not True):
        raise MsaQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch')
    manifest = {'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Metropolitan Statistical Areas — exact Census 2026 GDB working draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':URL,
        'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
        'processing_fingerprint':digest(encode(identity)),
        'inputs':[{'input_id':'census2026-national-geodatabase:'+SHA256,
            'producer':'U.S. Census Bureau','host':'www2.census.gov','role':'producer_publication',
            'canonical_url':URL,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],
            'sha256':digest(body),'bytes':len(body),'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
            'attribution':'U.S. Census Bureau, 2026 TIGER/Line national geographic geodatabase, Core_Based_Statistical_Area layer, MEMI=1 selection.',
            'edition':'2026','refresh_cadence':'Census re-publishes the TIGER/Line geodatabase national files annually; the advertised current product is built from, not a retired historical edition.',
            'release_evidence':{'landing_page':'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geodatabase-file.html',
                'advertised_current_product':'2026 TIGER/Line Geodatabases',
                'file_last_modified':'2026-07-29T16:00:43Z',
                'file_last_modified_meaning':'HTTP Last-Modified on www2.census.gov and TGRGDB26 directory date; packaging evidence, not an independently published producer release date.'},
            'release_date_unknown_reason':'The producer release date is not independently established; the observed 2026-07-29 publication timestamp and the advertised current 2026 TIGER/Line Geodatabases product are recorded as evidence. Earlier Census pages linking a 2023 metadata document for this inventory remain an unresolved lineage discrepancy.'}],
        'transformations':[{'operation':'decode_exact_original_openfilegdb_layer',
            'parameters':{'layer':LAYER,'preserve_intrinsic_fid_as':'OBJECTID','producer_oid_declaration':{'type':'esriFieldTypeOID','length_bytes':4,'nullable':False,'required':True,'editable':False},'native_crs':'EPSG:4269','repair':False,'reproject':False}},
            {'operation':'select_metropolitan_area_rows','parameters':{'field':'MEMI','equals':'1','layer_rows':TOTAL_ROWS,'selection_rows':METROPOLITAN_ROWS,'micropolitan_rows':MICROPOLITAN_ROWS,'excluded_rows_never_staged':True}},
            {'operation':'typed_native_geopackage_and_geoparquet','parameters':{'area_types':'float64','gpkg_fid':'independent storage index, not a source or published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Producer GDB field declarations and intrinsic identifiers cannot be silently remapped to DBF; no mapping approved.',
            'pmtiles':'Native coordinates would be reprojected and quantized; no visualization mapping approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity():
    """Bind the cached candidate to the shared, validator-supported code identity."""
    return package_processing_identity(Path(__file__).parents[3])


def acquire(storage,archive_path=None,receipt_path=None):
    local_only(storage)
    if archive_path is not None:
        body=b''; receipt={}
        try:
            with Path(archive_path).open('rb') as stream:
                body=stream.read(BYTES+1)
            if receipt_path is None:
                raise MsaQuarantineError('preserved_input_requires_original_receipt')
            receipt = strict_json(Path(receipt_path).read_bytes())
            return body,receipt
        except (ValueError,OSError,TypeError) as exc:
            location=quarantine(storage,body,receipt,str(exc))
            raise MsaQuarantineError(f'{exc}; retained:{location}') from exc
    for attempt in range(3):
        body=bytearray();receipt={'url':URL,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
        try:
            with httpx.Client(follow_redirects=False,timeout=120) as client, client.stream('GET',URL) as response:
                receipt.update(status=response.status_code,final_url=str(response.url))
                for block in response.iter_bytes(1024*1024):
                    body += block
                    if len(body)>BYTES:
                        raise MsaQuarantineError('oversized_or_changed_source')
                receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                if response.headers.get('content-length') is not None and int(response.headers['content-length']) != len(body):
                    raise MsaQuarantineError('incomplete_http_response')
                if response.status_code != 200:
                    raise MsaQuarantineError('non200_http_response')
                return bytes(body),receipt
        except (httpx.HTTPError,OSError,ValueError) as exc:
            receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
            location=quarantine(storage,bytes(body),receipt,str(exc))
            if attempt==2:
                raise MsaQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc


def inspect_archive(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        entries=archive.infolist();names=[i.filename for i in entries]
        if len(entries)!=64 or len(set(names))!=64:
            raise MsaQuarantineError('missing_duplicate_or_changed_archive_inventory')
        for entry in entries:
            path=PurePosixPath(entry.filename)
            if (path.is_absolute() or '..' in path.parts or len(path.parts)!=2 or path.parts[0]!=GDB
                or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr>>16)):
                raise MsaQuarantineError('unsafe_encrypted_or_changed_archive_member')
        if sum(i.file_size for i in entries)>600_000_000 or archive.testzip() is not None:
            raise MsaQuarantineError('zip_expansion_or_crc_failure')
        inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in entries}
    metadata,table=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',layer=LAYER,return_fids=True)
    validate_layer(table,metadata)
    selection=select_metropolitan_rows(table)
    validate_selection(selection)
    metadata['native_layer_definition']=read_native_definition(archive_path)
    metadata['layer_selection']={'field':'MEMI','equals':'1','layer_rows':table.num_rows,
        'selection_rows':selection.num_rows,'micropolitan_rows':MICROPOLITAN_ROWS}
    return metadata,selection,inventory


def select_metropolitan_rows(table):
    return table.filter(pc.equal(table['MEMI'],'1'))


def read_native_definition(archive_path):
    """Retain producer declarations separately from GDAL's Arrow exposure."""
    _,definition=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',sql=f'GetLayerDefinition {LAYER}')
    if definition.num_rows!=1 or 'FIELD_1' not in definition.column_names:
        raise MsaQuarantineError('missing_original_layer_definition')
    xml=definition['FIELD_1'][0].as_py()
    root=ET.fromstring(xml)
    fields={field.findtext('Name'):{node.tag:node.text for node in field if node.tag!='Name'} for field in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    if set(fields)!=set(FIELDS) or root.findtext('OIDFieldName')!='OBJECTID' or root.findtext('HasZ')!='false' or root.findtext('HasM')!='false':
        raise MsaQuarantineError('native_layer_definition_drift')
    oid=fields['OBJECTID']
    if any(oid.get(k)!=v for k,v in {'FieldType':'esriFieldTypeOID','Length':'4','IsNullable':'false','Required':'true','Editable':'false'}.items()):
        raise MsaQuarantineError('native_oid_declaration_drift')
    for name in ('ALAND','AWATER'):
        if fields[name].get('FieldType')!='esriFieldTypeDouble' or fields[name].get('Length')!='8':
            raise MsaQuarantineError('native_double_area_declaration_drift')
    widths={'CSAFP':'3','CBSAFP':'5','GEOID':'5','GEOIDFQ':'14','NAME':'100','NAMELSAD':'100','LSAD':'2','MEMI':'1','MTFCC':'5','INTPTLAT':'11','INTPTLON':'12'}
    for name,width in widths.items():
        if fields[name].get('FieldType')!='esriFieldTypeString' or fields[name].get('Length')!=width:
            raise MsaQuarantineError('native_string_field_declaration_drift:'+name)
    return {'definition_xml':xml,'fields':fields,'driver_oid_representation':'Arrow return_fids int64, not producer OID width',
        'draft_output_oid_representation':'int64 attribute preserving every source value; widening/mapping not approved'}


def validate_layer(table,metadata):
    """Guard the complete Core_Based_Statistical_Area layer before selection."""
    if (tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES
        or metadata['geometry_type']!='MultiPolygon' or metadata['crs']!='EPSG:4269'
        or metadata['fid_column']!='OBJECTID' or table.num_rows!=TOTAL_ROWS):
        raise MsaQuarantineError('native_layer_schema_physical_types_crs_or_completeness_drift')
    if any(table.column(name).null_count!=LAYER_NULLS.get(name,0) for name in FIELDS):
        raise MsaQuarantineError('native_layer_nullability_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows]
    if ids!=list(range(1,TOTAL_ROWS+1)):
        raise MsaQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    memi={value:sum(r['MEMI']==value for r in rows) for value in ('1','2')}
    if memi!={'1':METROPOLITAN_ROWS,'2':MICROPOLITAN_ROWS}:
        raise MsaQuarantineError('metropolitan_micropolitan_distribution_drift')
    geoids=[r['GEOID'] for r in rows]
    if len(set(geoids))!=TOTAL_ROWS or any(not re.fullmatch(r'\d{5}',v or '') for v in geoids):
        raise MsaQuarantineError('missing_duplicate_changed_source_geoids')
    if any(r['LSAD'] not in ('M1','M2') or r['MTFCC']!='G3110' for r in rows):
        raise MsaQuarantineError('changed_lsad_or_mtfcc_values')
    geometries=shapely.from_wkb(table['SHAPE'].to_pylist())
    if any(shapely.is_empty(geometries)) or not all(shapely.is_valid(geometries)) or not all(shapely.get_type_id(geometries)==6):
        raise MsaQuarantineError('missing_invalid_or_changed_native_geometry')


def validate_selection(table):
    """Guard the staged MEMI=1 metropolitan selection exactly."""
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=METROPOLITAN_ROWS:
        raise MsaQuarantineError('selection_schema_or_row_count_drift')
    if any(table.column(name).null_count!=NULLS.get(name,0) for name in FIELDS):
        raise MsaQuarantineError('selection_nullability_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows]
    if sorted(ids)!=list(METRO_OBJECTIDS):
        raise MsaQuarantineError('selection_objectid_membership_drift')
    if any(r['MEMI']!='1' or r['LSAD']!='M1' or r['MTFCC']!='G3110' for r in rows):
        raise MsaQuarantineError('selection_area_type_drift')
    geoids=[r['GEOID'] for r in rows]
    if len(set(geoids))!=METROPOLITAN_ROWS or any(not re.fullmatch(r'\d{5}',v or '') for v in geoids):
        raise MsaQuarantineError('missing_duplicate_changed_selection_geoids')
    if any(r['CBSAFP']!=r['GEOID'] or not re.fullmatch(r'310M700US\d{5}',r['GEOIDFQ'] or '') for r in rows):
        raise MsaQuarantineError('selection_identifier_construction_drift')
    geometries=shapely.from_wkb(table['SHAPE'].to_pylist())
    if any(shapely.is_empty(geometries)) or not all(shapely.is_valid(geometries)) or not all(shapely.get_type_id(geometries)==6):
        raise MsaQuarantineError('missing_invalid_or_changed_selection_geometry')


def table_values(table):
    return [{**{k:r[k] for k in FIELDS if k!='SHAPE'},'SHAPE':r['SHAPE'].hex()} for r in table.to_pylist()]


def write_native_gpkg(table,path):
    # Pin GDAL creation time so SQLite transaction headers are reproducible;
    # the ambient date must not enter the container bytes (CI caught a cached
    # container that disagreed with a fresh rebuild of the same rows).
    with with_geopackage_creation_time('2026-01-01T00:00:00.000Z'):
        pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'SHAPE'})
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def gpkg_geometry_declarations(path):
    with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as db:
        return (db.execute('SELECT table_name,column_name,geometry_type_name,srs_id,z,m FROM gpkg_geometry_columns ORDER BY table_name').fetchall(),
            db.execute('SELECT table_name,data_type,srs_id,min_x,min_y,max_x,max_y FROM gpkg_contents ORDER BY table_name').fetchall())


def expected_parquet_table(table):
    """Exact schema/geometry declarations generated from fresh native records."""
    geometry=table['SHAPE'].to_pylist()
    geo={'version':'1.1.0','primary_column':'SHAPE','columns':{'SHAPE':{'encoding':'WKB','geometry_types':['MultiPolygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(geometry)).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def reconcile_cached_native_outputs(root,table):
    """Derive fidelity from pinned original rows, never from a repinned receipt."""
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    pt=pq.read_table(root/'candidate/typed-source.parquet')
    expected=expected_parquet_table(table)
    # GDAL's container-envelope serialization can differ by the last binary bit
    # from the WKB coordinate extremum. Recompute its exact deterministic output;
    # do not invent a coordinate tolerance or trust the cached envelope.
    with tempfile.TemporaryDirectory(prefix='msa-gpkg-metadata-') as temporary:
        fresh=Path(temporary)/'fresh.gpkg'
        write_native_gpkg(table,fresh)
        if gpkg_geometry_declarations(root/'geopackage'/f'{SLUG}.gpkg')!=gpkg_geometry_declarations(fresh):
            raise MsaQuarantineError('cached_geopackage_geometry_declarations_disagree_with_fresh_original')
        # This is an immutable canonical cache, not an arbitrary equivalent GPKG.
        # Exact deterministic reconstruction also binds complete SRS authority/WKT,
        # geometry headers/flags/envelopes, RTree/indexes and unknown SQL metadata.
        if sha256_file(root/'geopackage'/f'{SLUG}.gpkg')!=sha256_file(fresh):
            raise MsaQuarantineError('cached_geopackage_container_bytes_disagree_with_fresh_original')
    if not pt.schema.equals(expected.schema,check_metadata=True):
        raise MsaQuarantineError('cached_parquet_geometry_or_physical_schema_metadata_disagrees_with_fresh_original')
    if (gm['geometry_name']!='SHAPE' or gm['geometry_type']!='MultiPolygon' or gm['crs']!='EPSG:4269' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES
        or tuple(pt.column_names)!=FIELDS or tuple(f.type for f in pt.schema)!=TYPES
        or table_values(gt)!=table_values(table) or table_values(pt)!=table_values(table)):
        raise MsaQuarantineError('cached_outputs_disagree_with_fresh_original_native_records')


def acquire_published_baseline(storage, baseline_dir=None):
    """Pin the existing public GCS baseline and every full artifact losslessly."""
    local=Path(baseline_dir) if baseline_dir else None
    if local is not None and (local.is_symlink() or not local.is_dir()):
        raise MsaQuarantineError('invalid_explicit_baseline_capture_directory')
    specs=[('collection',BASELINE_COLLECTION),*[(key,value) for key,value in BASELINE_ASSETS.items()]]
    blobs={};receipts={}
    for key,spec in specs:
        if key=='collection':
            filename='collection.json'
        else:
            filename=spec['name']
        path=local/filename if local is not None else None
        if path is not None:
            body=path.read_bytes()
            if key=='collection':
                receipt=strict_json((local/'collection.receipt.json').read_bytes())
            else:
                receipt_map=strict_json((local/'asset-receipts.json').read_bytes())
                receipt=receipt_map[key]
        else:
            url=spec['url']+'?generation='+str(spec['generation'])
            pieces=[];size=0
            with httpx.Client(follow_redirects=False,timeout=60) as client, client.stream('GET',url,headers={'Accept-Encoding':'identity'}) as response:
                receipt={'url':url,'final_url':str(response.url),'status':response.status_code,
                    'observed_at':datetime.now(timezone.utc).isoformat(),
                    'content_length':response.headers.get('content-length'),'generation':response.headers.get('x-goog-generation'),
                    'etag':response.headers.get('etag'),'x_goog_hash':response.headers.get('x-goog-hash')}
                for block in response.iter_bytes(1024*1024):
                    size+=len(block)
                    if size>spec['bytes']:
                        raise MsaQuarantineError('published_baseline_oversized_response:'+key)
                    pieces.append(block)
            body=b''.join(pieces)
            # The live receipt must carry the same computed digest the pin
            # comparison requires; otherwise every default ingest (no preserved
            # baseline capture) would fail on collection.json before staging.
            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
        md5=hashlib.md5(body).hexdigest()
        if (len(body)!=spec['bytes'] or digest(body)!=spec['sha256'] or md5!=spec['md5']
            or receipt.get('status')!=200 or receipt.get('complete_body') is not True
            or receipt.get('bytes')!=spec['bytes'] or receipt.get('sha256')!=spec['sha256']
            or receipt.get('generation')!=spec['generation'] or receipt.get('content_length')!=str(spec['bytes'])
            or receipt.get('etag','').strip('\"')!=spec['md5']):
            raise MsaQuarantineError('published_baseline_pin_or_capture_drift:'+key)
        if key=='collection':
            collection=strict_json(body)
            assets=collection.get('assets',{})
            if (collection.get('hifld:feature_count')!=METROPOLITAN_ROWS or collection.get('hifld:agency')!='Census Bureau'
                or collection.get('hifld:geometry_type')!='MultiPolygon' or set(assets)!=set(BASELINE_ASSETS)):
                raise MsaQuarantineError('published_baseline_collection_asset_roster_changed')
            for asset_key,asset_spec in BASELINE_ASSETS.items():
                item=assets[asset_key]
                if item.get('href')!=asset_spec['url'] or item.get('file:size')!=asset_spec['bytes'] or item.get('file:checksum')!=asset_spec['collection_checksum']:
                    raise MsaQuarantineError('published_baseline_collection_pin_changed:'+asset_key)
        else:
            # The captured evidence receipt pins GCS generation and MD5 independently.
            if receipt.get('generation')!=spec['generation'] or receipt.get('bytes')!=spec['bytes'] or receipt.get('sha256')!=spec['sha256'] or receipt.get('etag','').strip('\"')!=spec['md5']:
                raise MsaQuarantineError('published_baseline_receipt_pin_changed:'+key)
        raw_key=f'_raw-captures/{SLUG}/baseline/{spec["generation"]}/{filename}'
        immutable(storage,raw_key,body)
        # Every live GET stamps a fresh observed_at, so receipt bytes are not
        # reproducible; retain each receipt under a content-addressed key (like
        # the original capture receipt). A fixed name would make any rerun with
        # unchanged inputs fail its own immutable capture. The body key plus the
        # byte/generation/hash pins above still quarantine every real drift.
        receipt_body=encode(receipt)
        receipt_key=raw_key+'.receipt-'+digest(receipt_body)+'.json'
        immutable(storage,receipt_key,receipt_body)
        blobs[key]=body;receipts[key]={'url':spec['url'],'generation':spec['generation'],'bytes':len(body),
            'md5':md5,'sha256':digest(body),'capture_key':raw_key,'receipt_key':receipt_key,'receipt_sha256':digest(receipt_body)}
    return blobs,receipts


def compare_published_baseline(source_table, baseline_blobs):
    """Compare all published vector formats to the native source, without approving mappings."""
    source_rows={row['GEOID']:row for row in source_table.to_pylist()}
    source_ids=set(source_rows)
    formats={}
    with tempfile.TemporaryDirectory(prefix='msa-baseline-') as temporary:
        root=Path(temporary)
        for key,spec in BASELINE_ASSETS.items():
            if spec['format']=='pmtiles':
                formats[key]={'format':'pmtiles','full_object_pinned':True,
                    'native_id_schema_geometry_comparison':'not_applicable_to_tiled_display_artifact',
                    'sha256':digest(baseline_blobs[key]),'bytes':len(baseline_blobs[key])}
                continue
            path=root/spec['name'];path.write_bytes(baseline_blobs[key])
            if spec['format']=='geoparquet':
                table=pq.read_table(path);meta={'crs':'EPSG:4269','geometry_type':'MultiPolygon'}
            elif spec['format']=='shapefile_zip':
                meta,table=pyogrio.read_arrow(f'/vsizip/{path}/tl_2024_us_cbsa_metropolitan.shp',return_fids=True)
            else:
                meta,table=pyogrio.read_arrow(path,return_fids=True)
            geometry=next((name for name in ('geom','geometry','wkb_geometry') if name in table.column_names),None)
            if geometry is None or 'GEOID' not in table.column_names:
                raise MsaQuarantineError('published_baseline_missing_geometry_or_geoid:'+key)
            rows=table.to_pylist();by_id={row['GEOID']:row for row in rows};ids=set(by_id)
            if len(rows)!=METROPOLITAN_ROWS or len(ids)!=len(rows):
                raise MsaQuarantineError('published_baseline_feature_or_id_population_drift:'+key)
            type_names=[{'name':name,'type':str(table.schema.field(name).type),'nullable':table.schema.field(name).nullable,
                'width':(table.schema.field(name).metadata or {}).get(b'GDAL:OGR:width',b'').decode() or None}
                for name in table.column_names if name!=geometry]
            published_geometries=[shapely.from_wkb(row[geometry]) for row in rows]
            invalid=sum(not shapely.is_valid(geom) for geom in published_geometries)
            common=sorted(source_ids&ids)
            transformer=pyproj.Transformer.from_crs(meta.get('crs') or 'EPSG:4326','EPSG:4269',always_xy=True)
            exact_geometry_matches=0
            for geoid in common:
                source_geom=shapely.from_wkb(source_rows[geoid]['SHAPE'])
                published_geom=shapely.from_wkb(by_id[geoid][geometry])
                projected=shapely.transform(published_geom,transformer.transform,interleaved=False)
                if shapely.equals(source_geom,projected):
                    exact_geometry_matches+=1
            published_fields=[field['name'] for field in type_names]
            source_fields=[name for name in FIELDS if name not in ('OBJECTID','SHAPE')]
            formats[key]={'format':spec['format'],'bytes':len(baseline_blobs[key]),'sha256':digest(baseline_blobs[key]),
                'feature_count':len(rows),'geometry_type':meta.get('geometry_type'),'crs':meta.get('crs'),
                'fields':type_names,'source_fields_missing':sorted(set(source_fields)-set(published_fields)),
                'published_fields_unmapped':sorted(set(published_fields)-set(FIELDS)),
                'source_GEOID_count':len(source_ids),'published_GEOID_count':len(ids),
                'matching_GEOID_count':len(common),'source_GEOIDs_missing':sorted(source_ids-ids),
                'published_GEOIDs_unmapped':sorted(ids-source_ids),
                'source_OBJECTID_equal_published_OBJECTID_by_GEOID':sum(source_rows[g]['OBJECTID']==by_id[g].get('OBJECTID') for g in common) if common and 'OBJECTID' in by_id[common[0]] else None,
                'source_and_published_geometry_topologically_equal_after_crs_transform_count':exact_geometry_matches,
                'published_invalid_geometry_count':invalid}
    return {'baseline_collection':BASELINE_COLLECTION,'baseline_artifacts':{key:{k:v for k,v in spec.items() if k!='url'} for key,spec in BASELINE_ASSETS.items()},
        'baseline_receipts':{},'formats':formats,'comparison_status':'breaking_change_review_required',
        'source_schema_contract_approved':False,'source_id_contract_approved':False,'baseline_approved':False,'release_approved':False,
        'reason_codes':['native_schema_field_and_type_drift','published_OBJECTID_mapping_absent',
            'published_geometries_differ_from_native_source']}


def stage_msa_candidate(storage,*,archive_path=None,receipt_path=None,baseline_dir=None):
    local_only(storage)
    body,receipt=acquire(storage,archive_path,receipt_path)
    try:
        identity=processing_identity()
        manifest=manifest_for(body,receipt,identity)
        if len(body)!=BYTES or digest(body)!=SHA256:
            raise MsaQuarantineError('changed_truncated_or_unknown_census_publication')
        capture=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        immutable(storage,capture,body)
        immutable(storage,capture+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        fingerprint=digest(encode(identity))
        version=f'v0.0.0-draft-census2026-{SHA256[:16]}-{fingerprint[:16]}'
        prefix=f'{SLUG}/{SLUG}/{version}'
        base=Path(storage.local_dir);root=base/prefix
        # Decode the exact immutable retained original; no external-path substitution.
        metadata,table,inventory=inspect_archive(base/capture)
        baseline_blobs,baseline_receipts=acquire_published_baseline(storage,baseline_dir)
        baseline_comparison=compare_published_baseline(table,baseline_blobs)
        baseline_comparison["baseline_receipts"]=baseline_receipts
        ready_path=root/'metadata/msa_candidate_ready.json'
        source_values=table_values(table)
        source_record_sha256=digest(encode(source_values))
        source_schema_sha256=digest(encode({'arrow_schema':str(table.schema),
            'fields':metadata['native_layer_definition']['fields'],
            'layer_definition_xml':metadata['native_layer_definition']['definition_xml'],
            'geometry_type':metadata['geometry_type'],'crs':metadata['crs'],
            'layer_selection':metadata['layer_selection']}))
        state_key=f'_construction-state/{SLUG}/{version}.json'
        if root.exists() or storage.object_snapshot(state_key) is not None:
            if not root.is_dir() or not ready_path.is_file() or storage.object_snapshot(state_key) is None:
                raise MsaQuarantineError('existing_candidate_missing_first_construction_or_candidate')
            ready=strict_json(ready_path.read_bytes())
            if ready.get('version')!=version or ready.get('processing_fingerprint')!=fingerprint or ready.get('source_sha256')!=SHA256 or ready.get('approved') is not False:
                raise MsaQuarantineError('immutable_candidate_identity_conflict')
            stored=json.loads(load_first_construction(storage,state_key,version,fingerprint))
            trusted_objects=stored.get('payload',{}).get('objects')
            if not isinstance(trusted_objects,dict):
                raise MsaQuarantineError('first_construction_object_roster_missing')
            verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
                fingerprint,identity,source_record_sha256,source_schema_sha256,trusted_objects,{})
            ready_objects=ready.get('objects')
            if not isinstance(ready_objects,dict) or ready_objects!={k:v for k,v in trusted_objects.items() if k!='metadata/msa_candidate_ready.json'}:
                raise MsaQuarantineError('candidate_ready_roster_changed')
            reconcile_cached_native_outputs(root,table)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':METROPOLITAN_ROWS,'reused':True}
        building=base/'_building'/SLUG
        building.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='candidate-',dir=building) as temporary:
            temp=Path(temporary)
            for part in ('geopackage','candidate','metadata','raw'):
                (temp/part).mkdir()
            gpkg=temp/'geopackage'/f'{SLUG}.gpkg'
            write_native_gpkg(table,gpkg)
            gm,gt=pyogrio.read_arrow(gpkg,layer=LAYER)
            if tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gm['crs']!='EPSG:4269' or table_values(gt)!=table_values(table):
                raise MsaQuarantineError('native_geopackage_value_physical_type_or_wkb_loss')
            table=expected_parquet_table(table)
            parquet=temp/'candidate'/'typed-source.parquet'
            pq.write_table(table,parquet,compression='zstd')
            if not pq.read_table(parquet).equals(table,check_metadata=True):
                raise MsaQuarantineError('typed_parquet_roundtrip_loss')
            audit={'approved':False,'feature_count':METROPOLITAN_ROWS,'invalid_geometries':0,'crs':'EPSG:4269','driver_arrow_schema':str(table.schema),'producer_native_layer_definition':metadata['native_layer_definition'],'members':inventory,
                'intrinsic_source_id':'Producer OBJECTID esriFieldTypeOID Length4, required/nonnullable/noneditable; GDAL return_fids and draft output expose int64. Every original value preserved; widening/mapping remains unapproved. Independent GPKG storage FID not published/source ID',
                'layer_selection':metadata['layer_selection'],
                'coverage_GEOIDs':sorted(table['GEOID'].to_pylist()),'full_value_type_id_and_native_wkb_roundtrip':{'geopackage':True,'parquet':True},
                'published_compatibility':'Complete pinned HIFLD v1.0.0 baseline comparison is recorded separately. Field/type/OBJECTID/CRS/geometry drift is present; no mapping or release is approved.'}
            payloads={'raw/source_capture.json':{'key':capture,'sha256':SHA256,'bytes':BYTES,'complete_original_gdb_members':64},'raw/retrieval.json':receipt,
                'metadata/source_manifest.json':manifest,'metadata/processing_identity.json':identity,'metadata/msa_source_audit.json':audit,
                # Preserve the measured, fully pinned baseline comparison. A
                # review gate may block acceptance, but the baseline is present;
                # do not replace its observed diagnostics with an availability
                # placeholder that contradicts the retained receipts.
                'metadata/baseline_comparison.json':baseline_comparison}
            for name,value in payloads.items():
                (temp/name).write_bytes(encode(value))
            expected_outputs={str(p.relative_to(temp)):p.read_bytes() for p in temp.rglob('*') if p.is_file()}
            descriptors={name:{'bytes':len(value),'sha256':digest(value)} for name,value in expected_outputs.items()}
            ready={'version':version,'approved':False,'source_sha256':SHA256,'processing_fingerprint':fingerprint,
                'source_record_sha256':source_record_sha256,'source_schema_sha256':source_schema_sha256,'objects':descriptors}
            (temp/'metadata/msa_candidate_ready.json').write_bytes(encode(ready))
            outputs={str(p.relative_to(temp)):p.read_bytes() for p in temp.rglob('*') if p.is_file()}
            output_descriptors={name:{'bytes':len(body),'sha256':digest(body)} for name,body in outputs.items()}
            root.parent.mkdir(parents=True,exist_ok=True)
            # All source/provenance/output checks completed in a hidden build tree.
            os.rename(temp,root)
        for name,body in outputs.items():
            key=f'{prefix}/{name}'
            snapshot=storage.object_snapshot(key)
            if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=digest(body) or storage.object_snapshot(key)!=snapshot:
                raise MsaQuarantineError('candidate_output_physical_readback_failed:'+name)
        payload={'processing_identity':identity,'source_record_sha256':source_record_sha256,
            'source_schema_sha256':source_schema_sha256,'capture_receipts':{},'objects':output_descriptors}
        seal_ready_candidate(storage,state_key,version,fingerprint,payload)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':METROPOLITAN_ROWS,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        # The full original is already pinned in raw-captures when valid. Avoid a
        # second 272 MB copy per policy/schema failure; retain exact pointer+receipt.
        raw_key=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        retained=raw_key if digest(body)==SHA256 and storage.object_exists(raw_key) else None
        location=quarantine(storage,b'' if retained else body,{'original_receipt':receipt,'retained_full_body_key':retained,'body_sha256':digest(body),'body_bytes':len(body)},str(exc))
        raise MsaQuarantineError(f'{exc}; retained:{location}') from exc
