"""Faithful local draft of the exact inventory's Census 2024 national-GDB Metdiv layer.

An edition label is not proof of a release date or approved compatibility. No
projection, ID substitution, geometry repair or area-type migration occurs here.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
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
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.epa_frs_common import seal_ready_candidate, verify_cached_candidate
from dagster_hifld.validation import sha256_file

SLUG = 'metropolitan-divisions-2'
LAYER = 'Metropolitan_Division'
GDB = 'tlgdb_2024_a_us_nationgeo.gdb'
URL = 'https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_nationgeo.gdb.zip'
SHA256 = '68f517f0ef6ab6bcbc2e89102fa960fd1c7c7a130039a1aee2f475319917aeb4'
BYTES = 271963133
FIELDS = ('OBJECTID','GEOID','GEOIDFQ','NAMELSAD','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
# Producer OID is esriFieldTypeOID Length4. This int64 is the GDAL Arrow
# return_fids/output representation, not a native-width declaration/approval.
TYPES = (pa.int64(),pa.string(),pa.string(),pa.string(),pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())
BASELINE_ROOT = 'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/metropolitan-divisions-2/metropolitan-divisions-2/v1.0.0'
BASELINE_COLLECTION = {'name':'collection.json','url':BASELINE_ROOT+'/collection.json','bytes':11853,'generation':'1791506395736945','md5':'69f72f5fb13cd92ebb074dbbddb7c95e','sha256':'e09863343cd1987f04b5bbc6a647b41caa62f0bd902a95af1e88b33904df6ffb'}
BASELINE_ASSETS = {
    'geojson-0749bd938d45': {'name':'metropolitan-divisions-2-geojson.geojson','format':'geojson','url':BASELINE_ROOT+'/geojson/metropolitan-divisions-2-geojson.geojson','bytes':11077874,'generation':'1791506395653226','md5':'107f99bde54531e0a75295827973835c','collection_checksum':'d50110107f99bde54531e0a75295827973835c','sha256':'a489c19d48e577e320e5b4876b3d3841c62916cd327e105aaea98b5f67cd5159'},
    'geopackage-4fa3a34f37f9': {'name':'metropolitan-divisions-2.gpkg','format':'geopackage','url':BASELINE_ROOT+'/geopackage/metropolitan-divisions-2.gpkg','bytes':4083712,'generation':'1791506395633023','md5':'4a12e64ce0773a78eb69dfd7e027d51d','collection_checksum':'d501104a12e64ce0773a78eb69dfd7e027d51d','sha256':'875e029cadc1d3561d3f2e5a59698ec05a3c9a18676d8fb0676527d71c6451bf'},
    'geoparquet-7f2db7d7a5a2': {'name':'metropolitan-divisions-2.parquet','format':'geoparquet','url':BASELINE_ROOT+'/geoparquet/metropolitan-divisions-2.parquet','bytes':2049123,'generation':'1791506395638985','md5':'da8d1dfb759652967541933de5245abb','collection_checksum':'d50110da8d1dfb759652967541933de5245abb','sha256':'ac12e07861a18292eaa914adb0ccf528065c357bdeca495032426dca15aa6f36'},
    'pmtiles-fd4fb2ad1789': {'name':'metropolitan-divisions-2.pmtiles','format':'pmtiles','url':BASELINE_ROOT+'/pmtiles/metropolitan-divisions-2.pmtiles','bytes':1042831,'generation':'1791506395689927','md5':'a3b482e7fc7a0bd9aa2fe69f0c29c3d9','collection_checksum':'d50110a3b482e7fc7a0bd9aa2fe69f0c29c3d9','sha256':'4da5380819dd3053e3b1f94331b731b5ccd24da939bdf1692410f187149b5ec5'},
    'shapefile-cae93a700e72': {'name':'metropolitan-divisions-2.zip','format':'shapefile_zip','url':BASELINE_ROOT+'/shapefile/metropolitan-divisions-2.zip','bytes':2594263,'generation':'1791506395690370','md5':'eb01995b75c9c4c5fecdd030354865b1','collection_checksum':'d50110eb01995b75c9c4c5fecdd030354865b1','sha256':'ae1a8f2d71f9c2d0f686bee643ef5cd5a52f69a52df888c7a334af36217b5392'},
}

class MetdivQuarantineError(ValueError):
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
                raise MetdivQuarantineError('duplicate_json_key')
            result[key] = value
        return result
    value = json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value:
        raise MetdivQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise MetdivQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    for path in [root,*root.parents]:
        if path.is_symlink():
            raise MetdivQuarantineError('staging_path_symlink_refused')
    if root.exists():
        for path in root.rglob('*'):
            if path.is_symlink() or (path.is_file() and path.stat().st_nlink!=1):
                raise MetdivQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise MetdivQuarantineError('immutable_capture_changed:'+key)


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
        raise MetdivQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch')
    manifest = {'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Metropolitan Divisions — exact Census 2024 GDB working draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':URL,
        'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
        'processing_fingerprint':digest(encode(identity)),
        'inputs':[{'input_id':'census2024-national-geodatabase:'+SHA256,
            'producer':'U.S. Census Bureau','host':'www2.census.gov','role':'producer_publication',
            'canonical_url':URL,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],
            'sha256':digest(body),'bytes':len(body),'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
            'attribution':'U.S. Census Bureau, 2024 TIGER/Line national geographic geodatabase, Metropolitan_Division layer.',
            'release_date_unknown_reason':'2024 edition label is explicit; actual producer release date is not established. HTTP Last-Modified is packaging evidence only. Inventory links a 2023 metadata document, an unresolved lineage discrepancy.'}],
        'transformations':[{'operation':'decode_exact_original_openfilegdb_layer',
            'parameters':{'layer':LAYER,'preserve_intrinsic_fid_as':'OBJECTID','producer_oid_declaration':{'type':'esriFieldTypeOID','length_bytes':4,'nullable':False,'required':True,'editable':False},'driver_and_draft_output_oid':'int64; width representation change remains unapproved','native_crs':'EPSG:4269','repair':False,'reproject':False}},
            {'operation':'typed_native_geopackage_and_geoparquet','parameters':{'area_types':'float64','gpkg_fid':'independent storage index, not a source or published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Producer GDB field declarations and intrinsic identifiers cannot be silently remapped to DBF; no mapping approved.',
            'pmtiles':'Native coordinates would be reprojected and quantized; no visualization mapping approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity():
    root = Path(__file__).parents[3]
    # The shared cache verifier binds the first-construction identity to its
    # creating commit, so retain it alongside the source manifest provenance.
    return {'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        # The shared first-construction witness accepts canonical JSON types;
        # use lists instead of tuples so the identity can be sealed losslessly.
        'distributions':sorted([d.metadata['Name'],d.version] for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}


def acquire(storage,archive_path=None,receipt_path=None):
    local_only(storage)
    if archive_path is not None:
        body=b''; receipt={}
        try:
            with Path(archive_path).open('rb') as stream:
                body=stream.read(BYTES+1)
            if receipt_path is None:
                raise MetdivQuarantineError('preserved_input_requires_original_receipt')
            receipt = strict_json(Path(receipt_path).read_bytes())
            return body,receipt
        except (ValueError,OSError,TypeError) as exc:
            location=quarantine(storage,body,receipt,str(exc))
            raise MetdivQuarantineError(f'{exc}; retained:{location}') from exc
    for attempt in range(3):
        body=bytearray();receipt={'url':URL,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
        try:
            with httpx.Client(follow_redirects=False,timeout=120) as client, client.stream('GET',URL) as response:
                receipt.update(status=response.status_code,final_url=str(response.url))
                for block in response.iter_bytes(1024*1024):
                    body += block
                    if len(body)>BYTES:
                        raise MetdivQuarantineError('oversized_or_changed_source')
                receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                if response.headers.get('content-length') is not None and int(response.headers['content-length']) != len(body):
                    raise MetdivQuarantineError('incomplete_http_response')
                if response.status_code != 200:
                    raise MetdivQuarantineError('non200_http_response')
                return bytes(body),receipt
        except (httpx.HTTPError,OSError,ValueError) as exc:
            receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
            location=quarantine(storage,bytes(body),receipt,str(exc))
            if attempt==2:
                raise MetdivQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc


def inspect_archive(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        entries=archive.infolist();names=[i.filename for i in entries]
        if len(entries)!=71 or len(set(names))!=71:
            raise MetdivQuarantineError('missing_duplicate_or_changed_archive_inventory')
        for entry in entries:
            path=PurePosixPath(entry.filename)
            if (path.is_absolute() or '..' in path.parts or len(path.parts)!=2 or path.parts[0]!=GDB
                or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr>>16)):
                raise MetdivQuarantineError('unsafe_encrypted_or_changed_archive_member')
        if sum(i.file_size for i in entries)>600_000_000 or archive.testzip() is not None:
            raise MetdivQuarantineError('zip_expansion_or_crc_failure')
        inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in entries}
    metadata,table=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',layer=LAYER,return_fids=True)
    validate_table(table,metadata)
    metadata['native_layer_definition']=read_native_definition(archive_path)
    return metadata,table,inventory



def read_native_definition(archive_path):
    """Retain producer declarations separately from GDAL's Arrow exposure."""
    _,definition=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',sql=f'GetLayerDefinition {LAYER}')
    if definition.num_rows!=1 or 'FIELD_1' not in definition.column_names:
        raise MetdivQuarantineError('missing_original_layer_definition')
    xml=definition['FIELD_1'][0].as_py()
    root=ET.fromstring(xml)
    fields={field.findtext('Name'):{node.tag:node.text for node in field if node.tag!='Name'} for field in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    if set(fields)!=set(FIELDS) or root.findtext('OIDFieldName')!='OBJECTID' or root.findtext('HasZ')!='false' or root.findtext('HasM')!='false':
        raise MetdivQuarantineError('native_layer_definition_drift')
    oid=fields['OBJECTID']
    if any(oid.get(k)!=v for k,v in {'FieldType':'esriFieldTypeOID','Length':'4','IsNullable':'false','Required':'true','Editable':'false'}.items()):
        raise MetdivQuarantineError('native_oid_declaration_drift')
    for name in ('ALAND','AWATER'):
        if fields[name].get('FieldType')!='esriFieldTypeDouble' or fields[name].get('Length')!='8':
            raise MetdivQuarantineError('native_double_area_declaration_drift')
    return {'definition_xml':xml,'fields':fields,'driver_oid_representation':'Arrow return_fids int64, not producer OID width',
        'draft_output_oid_representation':'int64 attribute preserving every source value; widening/ID mapping not approved'}


def validate_table(table,metadata):
    if (tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES
        or metadata['geometry_type']!='MultiPolygon' or metadata['crs']!='EPSG:4269'
        or metadata['fid_column']!='OBJECTID' or table.num_rows!=37):
        raise MetdivQuarantineError('native_schema_physical_types_crs_or_completeness_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows];geoids=[r['GEOID'] for r in rows]
    if ids!=list(range(1,38)) or len(set(geoids))!=37 or any(not re.fullmatch(r'\d{10}',v or '') for v in geoids):
        raise MetdivQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    if any(table.column(name).null_count for name in FIELDS):
        raise MetdivQuarantineError('native_nullability_drift')
    geometries=shapely.from_wkb(table['SHAPE'].to_pylist())
    if any(shapely.is_empty(geometries)) or not all(shapely.is_valid(geometries)) or not all(shapely.get_type_id(geometries)==6):
        raise MetdivQuarantineError('missing_invalid_or_changed_native_geometry')


def table_values(table):
    return [{**{k:r[k] for k in FIELDS if k!='SHAPE'},'SHAPE':r['SHAPE'].hex()} for r in table.to_pylist()]





def write_native_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'SHAPE'})
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2024-01-01T00:00:00.000Z'")


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
    with tempfile.TemporaryDirectory(prefix='metdiv-gpkg-metadata-') as temporary:
        fresh=Path(temporary)/'fresh.gpkg'
        write_native_gpkg(table,fresh)
        if gpkg_geometry_declarations(root/'geopackage'/f'{SLUG}.gpkg')!=gpkg_geometry_declarations(fresh):
            raise MetdivQuarantineError('cached_geopackage_geometry_declarations_disagree_with_fresh_original')
        # This is an immutable canonical cache, not an arbitrary equivalent GPKG.
        # Exact deterministic reconstruction also binds complete SRS authority/WKT,
        # geometry headers/flags/envelopes, RTree/indexes and unknown SQL metadata.
        if sha256_file(root/'geopackage'/f'{SLUG}.gpkg')!=sha256_file(fresh):
            raise MetdivQuarantineError('cached_geopackage_container_bytes_disagree_with_fresh_original')
    if not pt.schema.equals(expected.schema,check_metadata=True):
        raise MetdivQuarantineError('cached_parquet_geometry_or_physical_schema_metadata_disagrees_with_fresh_original')
    if (gm['geometry_name']!='SHAPE' or gm['geometry_type']!='MultiPolygon' or gm['crs']!='EPSG:4269' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES
        or tuple(pt.column_names)!=FIELDS or tuple(f.type for f in pt.schema)!=TYPES
        or table_values(gt)!=table_values(table) or table_values(pt)!=table_values(table)):
        raise MetdivQuarantineError('cached_outputs_disagree_with_fresh_original_native_records')


def acquire_published_baseline(storage, baseline_dir=None):
    """Pin the existing public GCS baseline and every full artifact losslessly."""
    local=Path(baseline_dir) if baseline_dir else None
    if local is not None and (local.is_symlink() or not local.is_dir()):
        raise MetdivQuarantineError('invalid_explicit_baseline_capture_directory')
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
                        raise MetdivQuarantineError('published_baseline_oversized_response:'+key)
                    pieces.append(block)
            body=b''.join(pieces)
            receipt.update(bytes=len(body),complete_body=True)
        md5=hashlib.md5(body).hexdigest()
        if (len(body)!=spec['bytes'] or digest(body)!=spec['sha256'] or md5!=spec['md5']
            or receipt.get('status')!=200 or receipt.get('complete_body') is not True
            or receipt.get('bytes')!=spec['bytes'] or receipt.get('sha256')!=spec['sha256']
            or receipt.get('generation')!=spec['generation'] or receipt.get('content_length')!=str(spec['bytes'])
            or receipt.get('etag','').strip('\"')!=spec['md5']):
            raise MetdivQuarantineError('published_baseline_pin_or_capture_drift:'+key)
        if key=='collection':
            collection=strict_json(body)
            assets=collection.get('assets',{})
            if (collection.get('hifld:feature_count')!=37 or collection.get('hifld:agency')!='U.S. Census Bureau'
                or collection.get('hifld:geometry_type')!='MultiPolygon' or set(assets)!=set(BASELINE_ASSETS)):
                raise MetdivQuarantineError('published_baseline_collection_asset_roster_changed')
            for asset_key,asset_spec in BASELINE_ASSETS.items():
                item=assets[asset_key]
                if item.get('href')!=asset_spec['url'] or item.get('file:size')!=asset_spec['bytes'] or item.get('file:checksum')!=asset_spec['collection_checksum']:
                    raise MetdivQuarantineError('published_baseline_collection_pin_changed:'+asset_key)
        else:
            # The captured evidence receipt pins GCS generation and MD5 independently.
            if receipt.get('generation')!=spec['generation'] or receipt.get('bytes')!=spec['bytes'] or receipt.get('sha256')!=spec['sha256'] or receipt.get('etag','').strip('\"')!=spec['md5']:
                raise MetdivQuarantineError('published_baseline_receipt_pin_changed:'+key)
        raw_key=f'_raw-captures/{SLUG}/baseline/{spec["generation"]}/{filename}'
        immutable(storage,raw_key,body)
        immutable(storage,raw_key+'.receipt.json',encode(receipt))
        blobs[key]=body;receipts[key]={'url':spec['url'],'generation':spec['generation'],'bytes':len(body),
            'md5':md5,'sha256':digest(body),'capture_key':raw_key,'receipt_sha256':digest(encode(receipt))}
    return blobs,receipts


def compare_published_baseline(source_table, baseline_blobs):
    """Compare all published vector formats to the native source, without approving mappings."""
    source_rows={row['GEOID']:row for row in source_table.to_pylist()}
    source_ids=set(source_rows)
    formats={}
    with tempfile.TemporaryDirectory(prefix='metdiv-baseline-') as temporary:
        root=Path(temporary)
        for key,spec in BASELINE_ASSETS.items():
            if spec['format']=='pmtiles':
                formats[key]={'format':'pmtiles','full_object_pinned':True,
                    'native_id_schema_geometry_comparison':'not_applicable_to_tiled_display_artifact',
                    'sha256':digest(baseline_blobs[key]),'bytes':len(baseline_blobs[key])}
                continue
            path=root/spec['name'];path.write_bytes(baseline_blobs[key])
            if spec['format']=='geoparquet':
                table=pq.read_table(path);meta={'crs':'EPSG:3857','geometry_type':'MultiPolygon'}
            elif spec['format']=='shapefile_zip':
                meta,table=pyogrio.read_arrow(f'/vsizip/{path}/metropolitan-divisions-2.shp',return_fids=True)
            else:
                meta,table=pyogrio.read_arrow(path,return_fids=True)
            geometry=next((name for name in ('geom','geometry','wkb_geometry') if name in table.column_names),None)
            if geometry is None or 'GEOID' not in table.column_names:
                raise MetdivQuarantineError('published_baseline_missing_geometry_or_geoid:'+key)
            rows=table.to_pylist();by_id={row['GEOID']:row for row in rows};ids=set(by_id)
            if len(rows)!=37 or len(ids)!=len(rows):
                raise MetdivQuarantineError('published_baseline_feature_or_id_population_drift:'+key)
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
                'source_OBJECTID_equal_published_OBJECTID_by_GEOID':sum(source_rows[g]['OBJECTID']==by_id[g].get('OBJECTID') for g in common) if 'OBJECTID' in by_id[common[0]] else None,
                'source_and_published_geometry_topologically_equal_after_crs_transform_count':exact_geometry_matches,
                'published_invalid_geometry_count':invalid}
    return {'baseline_collection':BASELINE_COLLECTION,'baseline_artifacts':{key:{k:v for k,v in spec.items() if k!='url'} for key,spec in BASELINE_ASSETS.items()},
        'baseline_receipts':{},'formats':formats,'comparison_status':'breaking_change_review_required',
        'source_schema_contract_approved':False,'source_id_contract_approved':False,'baseline_approved':False,'release_approved':False,
        'reason_codes':['native_schema_field_and_type_drift','published_OBJECTID_mapping_differs_by_GEOID',
            'published_geometries_and_CRS_differ','published_baseline_reports_invalid_geometry']}


def stage_metdiv_candidate(storage,*,archive_path=None,receipt_path=None,baseline_dir=None):
    local_only(storage)
    body,receipt=acquire(storage,archive_path,receipt_path)
    try:
        identity=processing_identity()
        manifest=manifest_for(body,receipt,identity)
        if len(body)!=BYTES or digest(body)!=SHA256:
            raise MetdivQuarantineError('changed_truncated_or_unknown_census_publication')
        capture=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        immutable(storage,capture,body)
        immutable(storage,capture+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        fingerprint=digest(encode(identity))
        version=f'v0.0.0-draft-census2024-{SHA256[:16]}-{fingerprint[:16]}'
        prefix=f'{SLUG}/{SLUG}/{version}'
        base=Path(storage.local_dir);root=base/prefix
        # Decode the exact immutable retained original; no external-path substitution.
        metadata,table,inventory=inspect_archive(base/capture)
        baseline_blobs,baseline_receipts=acquire_published_baseline(storage,baseline_dir)
        baseline_comparison=compare_published_baseline(table,baseline_blobs)
        baseline_comparison["baseline_receipts"]=baseline_receipts
        ready_path=root/'metadata/metdiv_candidate_ready.json'
        source_values=table_values(table)
        source_record_sha256=digest(encode(source_values))
        source_schema_sha256=digest(encode({'arrow_schema':str(table.schema),
            'fields':metadata['native_layer_definition']['fields'],
            'layer_definition_xml':metadata['native_layer_definition']['definition_xml'],
            'geometry_type':metadata['geometry_type'],'crs':metadata['crs']}))
        state_key=f'_construction-state/{SLUG}/{version}.json'
        if root.exists() or storage.object_snapshot(state_key) is not None:
            if not root.is_dir() or not ready_path.is_file() or storage.object_snapshot(state_key) is None:
                raise MetdivQuarantineError('existing_candidate_missing_first_construction_or_candidate')
            ready=strict_json(ready_path.read_bytes())
            if ready.get('version')!=version or ready.get('processing_fingerprint')!=fingerprint or ready.get('source_sha256')!=SHA256 or ready.get('approved') is not False:
                raise MetdivQuarantineError('immutable_candidate_identity_conflict')
            stored=json.loads(load_first_construction(storage,state_key,version,fingerprint))
            trusted_objects=stored.get('payload',{}).get('objects')
            if not isinstance(trusted_objects,dict):
                raise MetdivQuarantineError('first_construction_object_roster_missing')
            verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
                fingerprint,identity,source_record_sha256,source_schema_sha256,trusted_objects,{})
            ready_objects=ready.get('objects')
            if not isinstance(ready_objects,dict) or ready_objects!={k:v for k,v in trusted_objects.items() if k!='metadata/metdiv_candidate_ready.json'}:
                raise MetdivQuarantineError('candidate_ready_roster_changed')
            reconcile_cached_native_outputs(root,table)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':37,'reused':True}
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
                raise MetdivQuarantineError('native_geopackage_value_physical_type_or_wkb_loss')
            table=expected_parquet_table(table)
            parquet=temp/'candidate'/'typed-source.parquet'
            pq.write_table(table,parquet,compression='zstd')
            if not pq.read_table(parquet).equals(table,check_metadata=True):
                raise MetdivQuarantineError('typed_parquet_roundtrip_loss')
            audit={'approved':False,'feature_count':37,'invalid_geometries':0,'crs':'EPSG:4269','driver_arrow_schema':str(table.schema),'producer_native_layer_definition':metadata['native_layer_definition'],'members':inventory,
                'intrinsic_source_id':'Producer OBJECTID esriFieldTypeOID Length4, required/nonnullable/noneditable; GDAL return_fids and draft output expose int64. Every original value preserved; widening/mapping remains unapproved. Independent GPKG storage FID not published/source ID',
                'coverage_GEOIDs':sorted(table['GEOID'].to_pylist()),'full_value_type_id_and_native_wkb_roundtrip':{'geopackage':True,'parquet':True},
                'published_compatibility':'Complete pinned HIFLD v1.0.0 baseline comparison is recorded separately. Field/type/OBJECTID/CRS/geometry drift is present; no mapping or release is approved.'}
            payloads={'raw/source_capture.json':{'key':capture,'sha256':SHA256,'bytes':BYTES,'complete_original_gdb_members':71},'raw/retrieval.json':receipt,
                'metadata/source_manifest.json':manifest,'metadata/processing_identity.json':identity,'metadata/metdiv_source_audit.json':audit,
                'metadata/baseline_comparison.json':{'schema_version':1,'approved':False,'status':'blocked','reason_codes':['complete_published_baseline_unavailable','source_schema_id_geometry_contract_unapproved']}}
            for name,value in payloads.items():
                (temp/name).write_bytes(encode(value))
            expected_outputs={str(p.relative_to(temp)):p.read_bytes() for p in temp.rglob('*') if p.is_file()}
            descriptors={name:{'bytes':len(value),'sha256':digest(value)} for name,value in expected_outputs.items()}
            ready={'version':version,'approved':False,'source_sha256':SHA256,'processing_fingerprint':fingerprint,
                'source_record_sha256':source_record_sha256,'source_schema_sha256':source_schema_sha256,'objects':descriptors}
            (temp/'metadata/metdiv_candidate_ready.json').write_bytes(encode(ready))
            outputs={str(p.relative_to(temp)):p.read_bytes() for p in temp.rglob('*') if p.is_file()}
            output_descriptors={name:{'bytes':len(body),'sha256':digest(body)} for name,body in outputs.items()}
            root.parent.mkdir(parents=True,exist_ok=True)
            # All source/provenance/output checks completed in a hidden build tree.
            os.rename(temp,root)
        for name,body in outputs.items():
            key=f'{prefix}/{name}'
            snapshot=storage.object_snapshot(key)
            if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=digest(body) or storage.object_snapshot(key)!=snapshot:
                raise MetdivQuarantineError('candidate_output_physical_readback_failed:'+name)
        payload={'processing_identity':identity,'source_record_sha256':source_record_sha256,
            'source_schema_sha256':source_schema_sha256,'capture_receipts':{},'objects':output_descriptors}
        seal_ready_candidate(storage,state_key,version,fingerprint,payload)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':37,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        # The full original is already pinned in raw-captures when valid. Avoid a
        # second 272 MB copy per policy/schema failure; retain exact pointer+receipt.
        raw_key=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        retained=raw_key if digest(body)==SHA256 and storage.object_exists(raw_key) else None
        location=quarantine(storage,b'' if retained else body,{'original_receipt':receipt,'retained_full_body_key':retained,'body_sha256':digest(body),'body_bytes':len(body)},str(exc))
        raise MetdivQuarantineError(f'{exc}; retained:{location}') from exc
