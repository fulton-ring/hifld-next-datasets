"""Faithful local draft of the current Census TIGER national-GDB MSA layer.

The producer listing selects the current vintage; an edition label is not proof
of a release date or approved compatibility. Metropolitan Statistical Areas are
the ``Core_Based_Statistical_Area`` features with ``MEMI`` equal to ``1``; the
complementary micropolitan selection is validated but never staged. No
projection, ID substitution, geometry repair or area-type migration occurs here.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
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
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.gdal import with_geopackage_creation_time
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import (
    processing_identity as package_processing_identity, seal_ready_candidate, verify_cached_candidate)
from dagster_hifld.validation import sha256_file

SLUG = 'metropolitan-statistical-areas'
LAYER = 'Core_Based_Statistical_Area'
# The producer publishes one national geographic FileGDB per TIGER vintage; the
# newest published edition is resolved from the producer listing at capture
# time, never a pinned year.
GEODATABASE_PRODUCT = 'nationgeo'
LICENSE = 'https://www.census.gov/about/policies/quality/guidelines/copyright.html'
# Measured on the 2026 published product: the layer holds 935 CBSAs, the
# metropolitan (MEMI=1) selection 393 and the micropolitan (MEMI=2) remainder
# 542. The ranges admit routine delineation change and fail closed on grossly
# wrong or empty populations.
LAYER_COUNT_RANGE = (700, 1500)
METROPOLITAN_RANGE = (300, 700)
MICROPOLITAN_RANGE = (400, 900)
MAX_SOURCE_BYTES = 400_000_000
MAX_UNCOMPRESSED_BYTES = 1_500_000_000
MAX_ARCHIVE_MEMBERS = 200
FIELDS = ('OBJECTID','CSAFP','CBSAFP','GEOID','GEOIDFQ','NAME','NAMELSAD','LSAD','MEMI','MTFCC','ALAND','AWATER','INTPTLAT','INTPTLON','SHAPE')
# Producer OID is esriFieldTypeOID Length4. This int64 is the GDAL Arrow
# return_fids/output representation, not a native-width declaration/approval.
TYPES = (pa.int64(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.string(),pa.float64(),pa.float64(),pa.string(),pa.string(),pa.binary())
# Producer-nullable field on layer and selection; every other field must be
# fully populated.
NULLABLE_FIELDS = ('CSAFP',)

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


def manifest_for(body,receipt,identity,release):
    # This validator runs before canonical output is created, even on cache reuse.
    if (receipt.get('url') != release.source_url or receipt.get('final_url') != release.source_url
        or type(receipt.get('status')) is not int or receipt['status'] != 200
        or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
        or receipt.get('sha256') != digest(body) or receipt.get('complete_body') is not True):
        raise MsaQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch')
    manifest = {'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'working_tree_status':identity['working_tree_status'],
        'title':f'Metropolitan Statistical Areas — {release.vintage} Census TIGER/Line national GDB working draft',
        'publisher':'U.S. Census Bureau','agency':'U.S. Census Bureau','source_url':release.source_url,
        'source_release':release.as_metadata(),
        'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),
        'inputs':[{'input_id':f'census{release.vintage}-national-geodatabase:'+digest(body),
            'producer':'U.S. Census Bureau','host':'www2.census.gov','role':'producer_publication',
            'canonical_url':release.source_url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],
            'sha256':digest(body),'bytes':len(body),'license':LICENSE,
            'attribution':f'U.S. Census Bureau, {release.vintage} TIGER/Line national geographic geodatabase, Core_Based_Statistical_Area layer, MEMI=1 selection.',
            'release_date_unknown_reason':f'The producer identifies this as the {release.vintage} TIGER/Line vintage; the directory listing and HTTP Last-Modified values are retained as discovery evidence and are not treated as the producer release date. Earlier Census pages linking a 2023 metadata document for this inventory remain an unresolved lineage discrepancy.'}],
        'transformations':[{'operation':'decode_exact_original_openfilegdb_layer',
            'parameters':{'layer':LAYER,'preserve_intrinsic_fid_as':'OBJECTID','producer_oid_declaration':{'type':'esriFieldTypeOID','length_bytes':4,'nullable':False,'required':True,'editable':False},'driver_and_draft_output_oid':'int64; width representation change remains unapproved','native_crs':'EPSG:4269','repair':False,'reproject':False}},
            {'operation':'select_metropolitan_area_rows','parameters':{'field':'MEMI','equals':'1','excluded_rows_never_staged':True}},
            {'operation':'typed_native_geopackage_and_geoparquet','parameters':{'area_types':'float64','gpkg_fid':'independent storage index, not a source or published identifier'}}],
        'unsupported_output_formats':{'shapefile_zip':'Producer GDB field declarations and intrinsic identifiers cannot be silently remapped to DBF; no mapping approved.',
            'pmtiles':'Native coordinates would be reprojected and quantized; no visualization mapping approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity():
    root = Path(__file__).parents[3]
    identity = package_processing_identity(root)
    # Bind the native GDB reader too; it affects decoding but is not among the
    # common helper's core Python/geospatial runtime version fields.
    identity['gdal'] = pyogrio.__gdal_version_string__
    return identity


def acquire(storage,archive_path=None,receipt_path=None,release=None):
    local_only(storage)
    if archive_path is not None:
        body=b''; receipt={}
        try:
            with Path(archive_path).open('rb') as stream:
                body=stream.read(MAX_SOURCE_BYTES+1)
            if len(body)>MAX_SOURCE_BYTES:
                raise MsaQuarantineError('oversized_or_changed_source')
            if receipt_path is None:
                raise MsaQuarantineError('preserved_input_requires_original_receipt')
            receipt = strict_json(Path(receipt_path).read_bytes())
            release = release or census_latest.geodatabase_release_from_receipt(receipt, GEODATABASE_PRODUCT)
            if receipt.get('url') != release.source_url:
                raise MsaQuarantineError('receipt_url_does_not_match_release')
            return body,receipt,release
        except (ValueError,OSError,TypeError) as exc:
            location=quarantine(storage,body,receipt,str(exc))
            raise MsaQuarantineError(f'{exc}; retained:{location}') from exc
    # This draft performs one bounded live observation per run. A failed or
    # changed response is quarantined for review rather than silently retried.
    for attempt in range(1):
        body=bytearray();receipt={'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
        try:
            with tempfile.TemporaryDirectory(prefix='msa-discovery-') as discovery_root, \
                    httpx.Client(follow_redirects=False,timeout=180,headers={'Accept-Encoding':'identity'}) as client:
                release = release or census_latest.discover_geodatabase(client, Path(discovery_root), GEODATABASE_PRODUCT)
                receipt.update(url=release.source_url)
                receipt['source_release']=release.as_receipt_evidence()
                with client.stream('GET',release.source_url) as response:
                    receipt.update(status=response.status_code,final_url=str(response.url),
                        content_length=response.headers.get('content-length'),
                        content_encoding=response.headers.get('content-encoding'),
                        requested_accept_encoding='identity')
                    for block in response.iter_bytes(1024*1024):
                        body += block
                        if len(body)>MAX_SOURCE_BYTES:
                            raise MsaQuarantineError('oversized_or_changed_source')
                    receipt.update(bytes=len(body),sha256=digest(body),complete_body=True,
                        wire_bytes_downloaded=response.num_bytes_downloaded)
                    if response.headers.get('content-length') is not None and int(response.headers['content-length']) != len(body):
                        raise MsaQuarantineError('incomplete_http_response')
                    if response.status_code != 200:
                        raise MsaQuarantineError('non200_http_response')
                    return bytes(body),receipt,release
        except (httpx.HTTPError,OSError,ValueError) as exc:
            receipt.update(url=release.source_url if release is not None else None,
                bytes=len(body),sha256=digest(body),complete_body=False)
            location=quarantine(storage,bytes(body),receipt,str(exc))
            raise MsaQuarantineError(f'bounded_fetch_failed:{exc}; retained:{location}') from exc


def inspect_archive(archive_path,release):
    gdb_root=release.source_file[:-4]
    with zipfile.ZipFile(archive_path) as archive:
        entries=archive.infolist();names=[i.filename for i in entries]
        if not entries or len(entries)>MAX_ARCHIVE_MEMBERS or len(set(names))!=len(entries):
            raise MsaQuarantineError('missing_duplicate_or_changed_archive_inventory')
        for entry in entries:
            path=PurePosixPath(entry.filename)
            if (path.is_absolute() or '..' in path.parts or len(path.parts)!=2 or path.parts[0]!=gdb_root
                or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr>>16)):
                raise MsaQuarantineError('unsafe_encrypted_or_changed_archive_member')
        expanded=sum(i.file_size for i in entries)
        if expanded<=0 or expanded>MAX_UNCOMPRESSED_BYTES or archive.testzip() is not None:
            raise MsaQuarantineError('zip_expansion_or_crc_failure')
        inventory={i.filename:{'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in entries}
    metadata,table=pyogrio.read_arrow(f'/vsizip/{archive_path}/{gdb_root}',layer=LAYER,return_fids=True)
    validate_layer(table,metadata)
    selection=select_metropolitan_rows(table)
    validate_selection(selection)
    metadata['native_layer_definition']=read_native_definition(archive_path,release)
    metadata['layer_selection']={'field':'MEMI','equals':'1','layer_rows':table.num_rows,
        'selection_rows':selection.num_rows,'micropolitan_rows':table.num_rows-selection.num_rows}
    return metadata,selection,inventory


def select_metropolitan_rows(table):
    return table.filter(pc.equal(table['MEMI'],'1'))


def read_native_definition(archive_path,release):
    """Retain producer declarations separately from GDAL's Arrow exposure."""
    _,definition=pyogrio.read_arrow(f'/vsizip/{archive_path}/{release.source_file[:-4]}',sql=f'GetLayerDefinition {LAYER}')
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
        'draft_output_oid_representation':'int64 attribute preserving every source value; widening/ID mapping not approved'}


def validate_layer(table,metadata):
    """Guard the complete Core_Based_Statistical_Area layer before selection."""
    if (tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES
        or metadata['geometry_type']!='MultiPolygon' or metadata['crs']!='EPSG:4269'
        or metadata['fid_column']!='OBJECTID'
        or not LAYER_COUNT_RANGE[0]<=table.num_rows<=LAYER_COUNT_RANGE[1]):
        raise MsaQuarantineError('native_layer_schema_physical_types_crs_or_completeness_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows]
    if ids!=list(range(1,len(rows)+1)):
        raise MsaQuarantineError('missing_duplicate_changed_source_ids_or_coverage')
    for name in FIELDS:
        count=table.column(name).null_count
        if (name in NULLABLE_FIELDS and count==table.num_rows) or (name not in NULLABLE_FIELDS and count):
            raise MsaQuarantineError('native_layer_nullability_drift')
    memi={value:sum(r['MEMI']==value for r in rows) for value in ('1','2')}
    if (not METROPOLITAN_RANGE[0]<=memi['1']<=METROPOLITAN_RANGE[1]
        or not MICROPOLITAN_RANGE[0]<=memi['2']<=MICROPOLITAN_RANGE[1]
        or memi['1']+memi['2']!=table.num_rows):
        raise MsaQuarantineError('metropolitan_micropolitan_distribution_drift')
    geoids=[r['GEOID'] for r in rows]
    if len(set(geoids))!=table.num_rows or any(not re.fullmatch(r'\d{5}',v or '') for v in geoids):
        raise MsaQuarantineError('missing_duplicate_changed_source_geoids')
    if any(r['LSAD'] not in ('M1','M2') or r['MTFCC']!='G3110' for r in rows):
        raise MsaQuarantineError('changed_lsad_or_mtfcc_values')
    geometries=shapely.from_wkb(table['SHAPE'].to_pylist())
    if any(shapely.is_empty(geometries)) or not all(shapely.is_valid(geometries)) or not all(shapely.get_type_id(geometries)==6):
        raise MsaQuarantineError('missing_invalid_or_changed_native_geometry')


def validate_selection(table):
    """Guard the staged MEMI=1 metropolitan selection."""
    if (tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES
        or not METROPOLITAN_RANGE[0]<=table.num_rows<=METROPOLITAN_RANGE[1]):
        raise MsaQuarantineError('selection_schema_or_row_count_drift')
    for name in FIELDS:
        count=table.column(name).null_count
        if (name in NULLABLE_FIELDS and count==table.num_rows) or (name not in NULLABLE_FIELDS and count):
            raise MsaQuarantineError('selection_nullability_drift')
    rows=table.to_pylist();ids=[r['OBJECTID'] for r in rows]
    if len(set(ids))!=len(ids):
        raise MsaQuarantineError('selection_objectid_membership_drift')
    if any(r['MEMI']!='1' or r['LSAD']!='M1' or r['MTFCC']!='G3110' for r in rows):
        raise MsaQuarantineError('selection_area_type_drift')
    geoids=[r['GEOID'] for r in rows]
    if len(set(geoids))!=table.num_rows or any(not re.fullmatch(r'\d{5}',v or '') for v in geoids):
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


def stage_msa_candidate(storage,*,archive_path=None,receipt_path=None):
    local_only(storage)
    body,receipt,release=acquire(storage,archive_path,receipt_path)
    try:
        if not body or len(body)>MAX_SOURCE_BYTES:
            raise MsaQuarantineError('changed_truncated_or_unknown_census_publication')
        archive_sha=digest(body)
        identity=processing_identity()
        manifest=manifest_for(body,receipt,identity,release)
        capture=f'_raw-captures/{SLUG}/{archive_sha}/original.gdb.zip'
        immutable(storage,capture,body)
        immutable(storage,capture+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        fingerprint=digest(encode(identity))
        version=census_latest.release_version(str(release.vintage),archive_sha,fingerprint)
        prefix=f'{SLUG}/{SLUG}/{version}'
        base=Path(storage.local_dir);root=base/prefix
        # Decode the exact immutable retained original; no external-path substitution.
        metadata,table,inventory=inspect_archive(base/capture,release)
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
            if ready.get('version')!=version or ready.get('processing_fingerprint')!=fingerprint or ready.get('source_sha256')!=archive_sha or ready.get('approved') is not False:
                raise MsaQuarantineError('immutable_candidate_identity_conflict')
            stored=json.loads(load_first_construction(storage,state_key,version,fingerprint))
            trusted_objects=stored.get('payload',{}).get('objects')
            if not isinstance(trusted_objects,dict):
                raise MsaQuarantineError('first_construction_object_roster_missing')
            # Re-derive the expected provenance from the retained original receipt;
            # a repinned discovery claim cannot reuse this candidate.
            original_receipt=strict_json((root/'raw/retrieval.json').read_bytes())
            original_release=census_latest.geodatabase_release_from_receipt(original_receipt,GEODATABASE_PRODUCT)
            expected_manifest=manifest_for(body,original_receipt,identity,original_release)
            if (original_release.source_url!=release.source_url
                    or strict_json((root/'metadata/source_manifest.json').read_bytes())!=expected_manifest
                    or encode(strict_json((root/'metadata/processing_identity.json').read_bytes()))!=encode(identity)):
                raise MsaQuarantineError('cached_provenance_or_processing_identity_changed')
            verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
                fingerprint,identity,source_record_sha256,source_schema_sha256,trusted_objects,{})
            ready_objects=ready.get('objects')
            if not isinstance(ready_objects,dict) or ready_objects!={k:v for k,v in trusted_objects.items() if k!='metadata/msa_candidate_ready.json'}:
                raise MsaQuarantineError('candidate_ready_roster_changed')
            reconcile_cached_native_outputs(root,table)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
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
            audit={'approved':False,'feature_count':table.num_rows,'invalid_geometries':0,'crs':'EPSG:4269','driver_arrow_schema':str(table.schema),'producer_native_layer_definition':metadata['native_layer_definition'],'members':inventory,
                'source_release':release.as_metadata(),
                'intrinsic_source_id':'Producer OBJECTID esriFieldTypeOID Length4, required/nonnullable/noneditable; GDAL return_fids and draft output expose int64. Every original value preserved; widening/mapping remains unapproved. Independent GPKG storage FID not published/source ID',
                'layer_selection':metadata['layer_selection'],
                'coverage_GEOIDs':sorted(table['GEOID'].to_pylist()),'full_value_type_id_and_native_wkb_roundtrip':{'geopackage':True,'parquet':True},
                'published_compatibility':'No source schema, ID or geometry mapping is approved for the published inventory; the baseline comparison below blocks acceptance until that review completes.'}
            payloads={'raw/source_capture.json':{'key':capture,'sha256':archive_sha,'bytes':len(body),'complete_original_gdb_members':len(inventory),'source_release':release.as_metadata()},'raw/retrieval.json':receipt,
                'metadata/source_manifest.json':manifest,'metadata/processing_identity.json':identity,'metadata/msa_source_audit.json':audit,
                # Baseline acceptance remains blocked; record the gate instead of
                # inventing a comparison this run did not execute.
                'metadata/baseline_comparison.json':{'schema_version':1,'approved':False,'status':'blocked','reason_codes':['baseline_source_schema_id_geometry_mapping_not_approved']}}
            for name,value in payloads.items():
                (temp/name).write_bytes(encode(value))
            descriptors={str(p.relative_to(temp)):{'bytes':len(p.read_bytes()),'sha256':sha256_file(p)} for p in temp.rglob('*') if p.is_file()}
            ready={'version':version,'approved':False,'source_sha256':archive_sha,'processing_fingerprint':fingerprint,
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
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,zipfile.BadZipFile,sqlite3.Error,shapely.errors.GEOSException,ET.ParseError) as exc:
        # The full original is already pinned in raw-captures when valid. Avoid a
        # second copy of the multi-hundred-MB GDB per policy/schema failure; retain
        # the exact pointer plus receipt.
        raw_key=f'_raw-captures/{SLUG}/{digest(body)}/original.gdb.zip'
        retained=raw_key if storage.object_exists(raw_key) else None
        location=quarantine(storage,b'' if retained else body,{'original_receipt':receipt,'retained_full_body_key':retained,'body_sha256':digest(body),'body_bytes':len(body)},str(exc))
        raise MsaQuarantineError(f'{exc}; retained:{location}') from exc
