"""Faithful local draft of the exact inventory's Census 2024 national-GDB CSA layer.

An edition label is not proof of a release date or approved compatibility. No
projection, ID substitution, geometry repair or area-type migration occurs here.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
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

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity as packaged_processing_identity
from dagster_hifld.validation import sha256_file

SLUG = 'combined-statistical-areas-3'
LAYER = 'Combined_Statistical_Area'
GDB = 'tlgdb_2024_a_us_nationgeo.gdb'
URL = 'https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_nationgeo.gdb.zip'
SHA256 = '68f517f0ef6ab6bcbc2e89102fa960fd1c7c7a130039a1aee2f475319917aeb4'
BYTES = 271963133
FIELDS = ('OBJECTID','GEOID','GEOIDFQ','NAMELSAD','LSAD','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
# Producer OID is esriFieldTypeOID Length4. This int64 is the GDAL Arrow
# return_fids/output representation, not a native-width declaration/approval.
TYPES = (pa.int64(),pa.string(),pa.string(),pa.string(),pa.string(),pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())

class CSAQuarantineError(ValueError):
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
                raise CSAQuarantineError('duplicate_json_key')
            result[key] = value
        return result
    value = json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value:
        raise CSAQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise CSAQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    for path in [root,*root.parents]:
        if path.is_symlink():
            raise CSAQuarantineError('staging_path_symlink_refused')
    if root.exists():
        for path in root.rglob('*'):
            if path.is_symlink() or (path.is_file() and path.stat().st_nlink!=1):
                raise CSAQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise CSAQuarantineError('immutable_capture_changed:'+key)


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
        raise CSAQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch')
    manifest = {'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'working_tree_status':identity['working_tree_status'],
        'title':'Combined Statistical Areas — exact Census 2024 GDB working draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':URL,
        'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
        'processing_fingerprint':digest(encode(identity)),
        'inputs':[{'input_id':'census2024-national-geodatabase:'+SHA256,
            'producer':'U.S. Census Bureau','host':'www2.census.gov','role':'producer_publication',
            'canonical_url':URL,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],
            'sha256':digest(body),'bytes':len(body),'license':'https://www.census.gov/about/policies/quality/guidelines/copyright.html',
            'attribution':'U.S. Census Bureau, 2024 TIGER/Line national geographic geodatabase, Combined_Statistical_Area layer.',
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
    identity = packaged_processing_identity(root)
    # Bind the native GDB reader too; it affects decoding but is not among the
    # common helper's core Python/geospatial runtime version fields.
    identity['gdal'] = pyogrio.__gdal_version_string__
    return identity


def acquire(storage,archive_path=None,receipt_path=None):
    local_only(storage)
    if archive_path is not None:
        body=b''; receipt={}
        try:
            with Path(archive_path).open('rb') as stream:
                body=stream.read(BYTES+1)
            if receipt_path is None:
                raise CSAQuarantineError('preserved_input_requires_original_receipt')
            receipt = strict_json(Path(receipt_path).read_bytes())
            return body,receipt
        except (ValueError,OSError,TypeError) as exc:
            location=quarantine(storage,body,receipt,str(exc))
            raise CSAQuarantineError(f'{exc}; retained:{location}') from exc
    # This draft performs one bounded live observation per run. A failed or
    # changed response is quarantined for review rather than silently retried.
    for attempt in range(1):
        body=bytearray();receipt={'url':URL,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
        try:
            with httpx.Client(follow_redirects=False,timeout=180,headers={'Accept-Encoding':'identity'}) as client, client.stream('GET',URL) as response:
                receipt.update(status=response.status_code,final_url=str(response.url),
                    content_length=response.headers.get('content-length'),
                    content_encoding=response.headers.get('content-encoding'),
                    requested_accept_encoding='identity')
                for block in response.iter_bytes(1024*1024):
                    body += block
                    if len(body)>BYTES:
                        raise CSAQuarantineError('oversized_or_changed_source')
                receipt.update(bytes=len(body),sha256=digest(body),complete_body=True,
                    wire_bytes_downloaded=response.num_bytes_downloaded)
                if response.headers.get('content-length') is not None and int(response.headers['content-length']) != len(body):
                    raise CSAQuarantineError('incomplete_http_response')
                if response.status_code != 200:
                    raise CSAQuarantineError('non200_http_response')
                return bytes(body),receipt
        except (httpx.HTTPError,OSError,ValueError) as exc:
            receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
            location=quarantine(storage,bytes(body),receipt,str(exc))
            raise CSAQuarantineError(f'bounded_fetch_failed:{exc}; retained:{location}') from exc


def inspect_archive(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        entries=archive.infolist();names=[i.filename for i in entries]
        if len(entries)!=71 or len(set(names))!=71:
            raise CSAQuarantineError('missing_duplicate_or_changed_archive_inventory')
        for entry in entries:
            path=PurePosixPath(entry.filename)
            if (path.is_absolute() or '..' in path.parts or len(path.parts)!=2 or path.parts[0]!=GDB
                or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr>>16)):
                raise CSAQuarantineError('unsafe_encrypted_or_changed_archive_member')
        if sum(i.file_size for i in entries)>600_000_000 or archive.testzip() is not None:
            raise CSAQuarantineError('zip_expansion_or_crc_failure')
        inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in entries}
    metadata,table=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',layer=LAYER,return_fids=True)
    validate_table(table,metadata)
    metadata['native_layer_definition']=read_native_definition(archive_path)
    return metadata,table,inventory



def read_native_definition(archive_path):
    """Retain producer declarations separately from GDAL's Arrow exposure."""
    _,definition=pyogrio.read_arrow(f'/vsizip/{archive_path}/{GDB}',sql=f'GetLayerDefinition {LAYER}')
    if definition.num_rows!=1 or 'FIELD_1' not in definition.column_names:
        raise CSAQuarantineError('missing_original_layer_definition')
    xml=definition['FIELD_1'][0].as_py()
    root=ET.fromstring(xml)
    fields={field.findtext('Name'):{node.tag:node.text for node in field if node.tag!='Name'} for field in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    if set(fields)!=set(FIELDS) or root.findtext('OIDFieldName')!='OBJECTID' or root.findtext('HasZ')!='false' or root.findtext('HasM')!='false':
        raise CSAQuarantineError('native_layer_definition_drift')
    oid=fields['OBJECTID']
    if any(oid.get(k)!=v for k,v in {'FieldType':'esriFieldTypeOID','Length':'4','IsNullable':'false','Required':'true','Editable':'false'}.items()):
        raise CSAQuarantineError('native_oid_declaration_drift')
    for name in ('ALAND','AWATER'):
        if fields[name].get('FieldType')!='esriFieldTypeDouble' or fields[name].get('Length')!='8':
            raise CSAQuarantineError('native_double_area_declaration_drift')
    return {'definition_xml':xml,'fields':fields,'driver_oid_representation':'Arrow return_fids int64, not producer OID width',
        'draft_output_oid_representation':'int64 attribute preserving every source value; widening/ID mapping not approved'}


def validate_table(table,metadata):
    if (tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES
        or metadata['geometry_type']!='MultiPolygon' or metadata['crs']!='EPSG:4269'
        or metadata['fid_column']!='OBJECTID' or table.num_rows!=184):
        raise CSAQuarantineError('native_schema_physical_types_crs_or_completeness_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows];geoids=[r['GEOID'] for r in rows]
    if ids!=list(range(1,185)) or len(set(geoids))!=184 or any(not re.fullmatch(r'\d{3}',v or '') for v in geoids):
        raise CSAQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    if any(table.column(name).null_count for name in FIELDS):
        raise CSAQuarantineError('native_nullability_drift')
    geometries=shapely.from_wkb(table['SHAPE'].to_pylist())
    if any(shapely.is_empty(geometries)) or not all(shapely.is_valid(geometries)) or not all(shapely.get_type_id(geometries)==6):
        raise CSAQuarantineError('missing_invalid_or_changed_native_geometry')


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
    if not pt.schema.equals(expected.schema,check_metadata=True):
        raise CSAQuarantineError('cached_parquet_geometry_or_physical_schema_metadata_disagrees_with_fresh_original')
    # GDAL's container-envelope serialization can differ by the last binary bit
    # from the WKB coordinate extremum. Recompute its exact deterministic output;
    # do not invent a coordinate tolerance or trust the cached envelope.
    with tempfile.TemporaryDirectory(prefix='csa-gpkg-metadata-') as temporary:
        fresh=Path(temporary)/'fresh.gpkg'
        write_native_gpkg(table,fresh)
        if gpkg_geometry_declarations(root/'geopackage'/f'{SLUG}.gpkg')!=gpkg_geometry_declarations(fresh):
            raise CSAQuarantineError('cached_geopackage_geometry_declarations_disagree_with_fresh_original')
        # This is an immutable canonical cache, not an arbitrary equivalent GPKG.
        # Exact deterministic reconstruction also binds complete SRS authority/WKT,
        # geometry headers/flags/envelopes, RTree/indexes and unknown SQL metadata.
        if sha256_file(root/'geopackage'/f'{SLUG}.gpkg')!=sha256_file(fresh):
            raise CSAQuarantineError('cached_geopackage_container_bytes_disagree_with_fresh_original')
    if (gm['geometry_name']!='SHAPE' or gm['geometry_type']!='MultiPolygon' or gm['crs']!='EPSG:4269' or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES
        or tuple(pt.column_names)!=FIELDS or tuple(f.type for f in pt.schema)!=TYPES
        or table_values(gt)!=table_values(table) or table_values(pt)!=table_values(table)):
        raise CSAQuarantineError('cached_outputs_disagree_with_fresh_original_native_records')


def stage_csa_candidate(storage,*,archive_path=None,receipt_path=None):
    local_only(storage)
    body,receipt=acquire(storage,archive_path,receipt_path)
    try:
        if len(body)!=BYTES or digest(body)!=SHA256:
            raise CSAQuarantineError('changed_truncated_or_unknown_census_publication')
        identity=processing_identity()
        manifest=manifest_for(body,receipt,identity)
        if (identity.get('code_identity_scheme') == 'sha256-packaged-dagster-hifld-v1'
                or identity.get('working_tree_status') == 'clean'):
            validate_construction_code_identity(Path(__file__).parents[3], identity['code_commit'],
                                                identity['files'], identity=identity)
        else:
            raise CSAQuarantineError('construction_code_identity_requires_clean_or_packaged_runtime')
        capture=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        immutable(storage,capture,body)
        immutable(storage,capture+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        fingerprint=digest(encode(identity))
        version=f'v0.0.0-draft-census2024-{SHA256[:16]}-{fingerprint[:16]}'
        prefix=f'{SLUG}/{SLUG}/{version}'
        base=Path(storage.local_dir);root=base/prefix
        # Decode the exact immutable retained original; no external-path substitution.
        metadata,table,inventory=inspect_archive(base/capture)
        ready_path=root/'metadata/csa_candidate_ready.json'
        if root.exists():
            if not ready_path.is_file():
                raise CSAQuarantineError('incomplete_existing_candidate')
            ready=strict_json(ready_path.read_bytes())
            if ready.get('version')!=version or ready.get('processing_fingerprint')!=fingerprint or ready.get('source_sha256')!=SHA256 or ready.get('approved') is not False:
                raise CSAQuarantineError('immutable_candidate_identity_conflict')
            expected={f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet','raw/source_capture.json','raw/retrieval.json','metadata/source_manifest.json','metadata/processing_identity.json','metadata/csa_source_audit.json','metadata/baseline_comparison.json'}
            actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}-{'metadata/csa_candidate_ready.json'}
            # Catalog/format assets own subsequent files; source-owned directories
            # and the immutable receipt roster remain closed, never self-authorizing.
            source_actual={name for name in actual if name.startswith(('candidate/','raw/','geopackage/')) or name in expected}
            downstream={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json',f'geoparquet/{SLUG}.parquet'}
            if set(ready['objects'])!=expected or source_actual!=expected or actual-expected-downstream or any(p.is_symlink() for p in root.rglob('*')):
                raise CSAQuarantineError('immutable_candidate_inventory_conflict')
            for name,pin in ready['objects'].items():
                if sha256_file(root/name)!=pin:
                    raise CSAQuarantineError('immutable_candidate_bytes_changed:'+name)
            cached_manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
            validate_release_provenance(cached_manifest)
            if cached_manifest.get('approved') is not False or cached_manifest.get('processing_fingerprint')!=fingerprint or cached_manifest['inputs'][0]['sha256']!=SHA256 or cached_manifest['inputs'][0]['bytes']!=BYTES or len(cached_manifest['inputs'])!=1 or cached_manifest['inputs'][0]['retrieval_url']!=URL or cached_manifest['inputs'][0].get('observed_final_url')!=URL or encode(strict_json((root/'metadata/processing_identity.json').read_bytes()))!=encode(identity):
                raise CSAQuarantineError('cached_provenance_or_processing_identity_changed')
            reconcile_cached_native_outputs(root,table)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':184,'reused':True}
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
                raise CSAQuarantineError('native_geopackage_value_physical_type_or_wkb_loss')
            table=expected_parquet_table(table)
            parquet=temp/'candidate'/'typed-source.parquet'
            pq.write_table(table,parquet,compression='zstd')
            if not pq.read_table(parquet).equals(table,check_metadata=True):
                raise CSAQuarantineError('typed_parquet_roundtrip_loss')
            audit={'approved':False,'feature_count':184,'invalid_geometries':0,'crs':'EPSG:4269','driver_arrow_schema':str(table.schema),'producer_native_layer_definition':metadata['native_layer_definition'],'members':inventory,
                'intrinsic_source_id':'Producer OBJECTID esriFieldTypeOID Length4, required/nonnullable/noneditable; GDAL return_fids and draft output expose int64. Every original value preserved; widening/mapping remains unapproved. Independent GPKG storage FID not published/source ID',
                'coverage_GEOIDs':sorted(table['GEOID'].to_pylist()),'full_value_type_id_and_native_wkb_roundtrip':{'geopackage':True,'parquet':True},
                'published_compatibility':'Not accepted. Published id differs from every source OBJECTID; source doubles differ from published integer areas, source fields differ, metadata2023 vs GDB2024 lineage unresolved.'}
            payloads={'raw/source_capture.json':{'key':capture,'sha256':SHA256,'bytes':BYTES,'complete_original_gdb_members':71},'raw/retrieval.json':receipt,
                'metadata/source_manifest.json':manifest,'metadata/processing_identity.json':identity,'metadata/csa_source_audit.json':audit,
                'metadata/baseline_comparison.json':{'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_schema_id_geometry_mapping_not_approved']}}
            for name,value in payloads.items():
                (temp/name).write_bytes(encode(value))
            ready={'version':version,'approved':False,'source_sha256':SHA256,'processing_fingerprint':fingerprint,
                'objects':{str(p.relative_to(temp)):sha256_file(p) for p in temp.rglob('*') if p.is_file()}}
            (temp/'metadata/csa_candidate_ready.json').write_bytes(encode(ready))
            root.parent.mkdir(parents=True,exist_ok=True)
            # All source/provenance/output checks completed in a hidden build tree.
            os.rename(temp,root)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':184,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        # The full original is already pinned in raw-captures when valid. Avoid a
        # second272MB copy per policy/schema failure; retain exact pointer+receipt.
        raw_key=f'_raw-captures/{SLUG}/{SHA256}/original.gdb.zip'
        retained=raw_key if digest(body)==SHA256 and storage.object_exists(raw_key) else None
        location=quarantine(storage,b'' if retained else body,{'original_receipt':receipt,'retained_full_body_key':retained,'body_sha256':digest(body),'body_bytes':len(body)},str(exc))
        raise CSAQuarantineError(f'{exc}; retained:{location}') from exc
