"""Faithful complete delegated FWS CBRS Generalized Units local draft.

Layer 4 of the official FWS-linked USGS CBRS Mapper service. Native API fields,
identifiers and NAD83 coordinates are retained; Esri rings are converted to
Polygon/MultiPolygon by signed-area classification without repair. Declared
Single-precision acreage fields serialized by the endpoint as integral JSON
numbers are represented as float64 per the layer's declared type, with the
exact producer serialization retained in raw captures. Publication date and
all source/baseline/contract acceptance remain unresolved.
"""
from __future__ import annotations

import collections
import hashlib
import json
import fcntl
import os
import re
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import Polygon, mapping, shape

from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as packaged_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'generalized-units-1'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
_ITEM = 'https://www.arcgis.com/sharing/rest/content/items/a60c25b4b2e44969b3164742901058a2?f=pjson'
_LAYER = 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/4'
_QUERY = _LAYER + '/query'
URLS = {'fws_data': LANDING, 'official_item': _ITEM, 'layer': _LAYER + '?f=pjson',
    'count': _QUERY + '?where=1%3D1&returnCountOnly=true&f=json',
    'ids': _QUERY + '?where=1%3D1&returnIdsOnly=true&f=json',
    'page0000': _QUERY + '?where=1%3D1&outFields=%2A&returnGeometry=true&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=json',
    'count_after': _QUERY + '?where=1%3D1&returnCountOnly=true&f=json',
    'ids_after': _QUERY + '?where=1%3D1&returnIdsOnly=true&f=json'}
URL = URLS['page0000']
PIN = '19869fa685f62e9fee7a606138485ce602f0ece578431489aa56aa2b5f3298ea'
ARCHIVE_NAME = 'native-generalized-units-page0000.json'
MAX_BYTES = 64_000_000
LAYER_ID = 4
LAYER_NAME = 'Generalized Units'
EXPECTED_COUNT = 942
FIDELITY_FILES = ('baseline_comparison.json','baseline_comparison.md','baseline_differences.jsonl','baseline_field_changes.jsonl')
SCHEMA = {'OBJECTID': 'int64', 'Unit': 'str:8', 'Name': 'str:254', 'Unit_Type': 'str:25',
    'Fast_Acres': 'float', 'Wet_Acres': 'float', 'Shore_Mile': 'float',
    'Shape_Length': 'float', 'Shape_Area': 'float'}
# Declared esriFieldTypeSingle members that the REST endpoint serializes as
# integral JSON numbers for many records; adapted to their declared float type.
SINGLE_FLOAT_FIELDS = ('Fast_Acres', 'Wet_Acres', 'Shore_Mile')
NATIVE_FIELDS = [{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID'},
    {'name': 'Unit', 'type': 'esriFieldTypeString', 'alias': 'Unit', 'length': 8},
    {'name': 'Name', 'type': 'esriFieldTypeString', 'alias': 'Name', 'length': 254},
    {'name': 'Unit_Type', 'type': 'esriFieldTypeString', 'alias': 'Unit_Type', 'length': 25},
    {'name': 'Fast_Acres', 'type': 'esriFieldTypeSingle', 'alias': 'Fast_Acres'},
    {'name': 'Wet_Acres', 'type': 'esriFieldTypeSingle', 'alias': 'Wet_Acres'},
    {'name': 'Shore_Mile', 'type': 'esriFieldTypeSingle', 'alias': 'Shore_Mile'},
    {'name': 'Shape_Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Length'},
    {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Area'}]
EXPECTED_NULLS = {name: 0 for name in SCHEMA}
EXPECTED_COVERAGE = {'System Unit': 592, 'Otherwise Protected Area': 350}
PROPOSAL = {'approved':False,'id_fields':['OBJECTID'],'geometry':True,
 'coverage_fields':['Unit_Type'],'crs':'EPSG:4269',
 'columns':{name:{'type':'integer' if kind.startswith('int') else 'decimal' if kind.startswith('float') else 'string','nullable':bool(EXPECTED_NULLS[name])} for name,kind in SCHEMA.items()}}


class CBRSQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes, max_bytes: int = 4_000_000) -> dict:
    if len(body) > max_bytes:
        raise CBRSQuarantineError('metadata_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CBRSQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise CBRSQuarantineError('nonobject_or_error_json')
    return result


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise CBRSQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise CBRSQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('requested_url') != url or receipt.get('final_url') != url
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('byte_length')) is not int or receipt['byte_length'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise CBRSQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp=receipt.get('observed_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise CBRSQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    moment = datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        raise CBRSQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    require_local(storage)
    captures = {}
    for name,url in URLS.items():
        if capture_dir is not None:
            body = b''; receipt_body=b''; receipt = {'capture_path':str(capture_dir),'requested_url':url}
            try:
                with (capture_dir / (name + '.body')).open('rb') as f: body = f.read(MAX_BYTES + 1)
                receipt_body=(capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body',receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body,receipt,url)
                if len(body)>MAX_BYTES: raise CBRSQuarantineError('capture_size_limit')
            except (OSError,ValueError,TypeError) as exc:
                location=quarantine(storage,body,receipt,str(exc))
                immutable(storage,f'{location}/original-receipt.body',receipt_body)
                raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
        else:
            for attempt in range(3):
                response=None
                body=b'';receipt={'requested_url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                try:
                    with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'}) as client:
                        with client.stream('GET',url) as response:
                            receipt.update(final_url=str(response.url),status=response.status_code,
                                content_encoding=response.headers.get('content-encoding','identity'),
                                content_length_header=response.headers.get('content-length'))
                            for block in response.iter_bytes(chunk_size=65536):
                                body+=block
                                if len(body)>MAX_BYTES: raise CBRSQuarantineError('response_size_limit')
                            receipt.update(byte_length=len(body),sha256=sha(body),wire_byte_length=response.num_bytes_downloaded)
                            validate_receipt(body,receipt,url)
                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=response.num_bytes_downloaded:
                                raise CBRSQuarantineError('incomplete_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(byte_length=len(body),sha256=sha(body),partial=True)
                    if response is not None:receipt['wire_byte_length']=response.num_bytes_downloaded
                    location=quarantine(storage,body,receipt,str(exc))
                    if attempt==2: raise CBRSQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
        captures[name]=(body,receipt)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',body)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',encode(receipt))
    return captures


def provenance(captures: dict, identity: dict) -> dict:
    if set(captures)!=set(URLS): raise CBRSQuarantineError('capture_roster_drift')
    for name,url in URLS.items(): validate_receipt(*captures[name],url)
    if b'gis1.wim.usgs.gov' not in captures['fws_data'][0] or b'a60c25b4b2e44969b3164742901058a2' not in captures['fws_data'][0]:
        raise CBRSQuarantineError('official_delegation_changed')
    item=strict_json(captures['official_item'][0])
    if item.get('url')!='https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer':
        raise CBRSQuarantineError('delegated_service_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'U.S. Fish and Wildlife Service',
            'host':URLS[name].split('/')[2],'role':'producer_publication' if name=='fws_data' else 'official_distributor',
            'canonical_url':URLS[name],'retrieval_url':receipt['requested_url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['observed_at'],'release_date':None,
            'release_date_unknown_reason':'The FWS mapper page notes data last modified 2024-12-19, but the layer and service expose no editing date; portal modification and fetch timestamps are not release dates.',
            'sha256':sha(body),'bytes':len(body),'license':LANDING,
            'license_scope':'FWS CBRS mapper official maps control; digital representations are not definitive property determinations. Item accessInformation/licenseInfo retained verbatim in raw official_item; release acceptance pending.',
            'attribution':'U.S. Fish and Wildlife Service; delegated USGS CBRS Mapper distribution.',
            'lineage':'FWS official maps-and-data links exact ArcGIS item and USGS CBRS Mapper; exact service layer4 Generalized Units.',
            'authority_evidence':LANDING,'delegation_evidence':{'url':LANDING,'scope':'Exact FWS-linked USGS CBRS MapServer via ArcGIS itema60c25b4b2e44969b3164742901058a2'}})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'title':'Generalized Units — faithful complete API working draft',
        'publisher':'U.S. Fish and Wildlife Service via delegated USGS CBRS Mapper','agency':'U.S. Fish and Wildlife Service',
        'source_url':LANDING,'license':LANDING,'inputs':inputs,
        'license_review_required':'Preserved full official item constraints; maintainer acceptance required.',
        'transformations':[{'operation':'decode_native_Esri_JSON_rings_to_Polygon_MultiPolygon',
            'preserved':'All9 source attributes, original OBJECTID JSON integers under proposed int64 proxy (native OID width not supplied), declared-Single acreage values adapted to float64 per declared field type (exact producer JSON retained in raw page capture), all native EPSG4269 coordinates; Esri clockwise shells and counter-clockwise holes classified by signed area into smallest containing shells; no field renames, ID generation, reprojection or geometry repair.',
            'repair_or_projection':False,'mapping_approval':'not established; baseline equivalence, ID contract and release require explicit decisions'},
            {'operation':'single_precision_serialization_normalization',
            'preserved':'Fast_Acres, Wet_Acres and Shore_Mile are declared esriFieldTypeSingle; the endpoint serializes integral values as JSON integers, so adapted records represent them as float64 while raw captures retain the exact producer bytes.',
            'repair_or_projection':False,'mapping_approval':'not established'}],
        'unsupported_output_formats':{'shapefile_zip':'Field roster truncation and Single-precision serialization decisions are unapproved.',
            'pmtiles':'Reprojection/quantization changes geometry; no visualization contract approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity() -> dict:
    """Use the shared Git-or-packaged construction identity and validate its claim."""
    root = Path(__file__).parents[3]
    identity = packaged_processing_identity(root)
    validate_construction_code_identity(
        root, identity['code_commit'], identity['files'], identity=identity)
    return {**identity, 'gdal': fiona.__gdal_version__,
            'verifier_sha256': verifier_implementation_sha256()}


def _ring_area(ring: list[list[float]]) -> float:
    # Translate to the first vertex to avoid cancellation for small rings.
    x0, y0 = ring[0]
    return sum((ring[i][0]-x0)*(ring[i+1][1]-y0)-(ring[i+1][0]-x0)*(ring[i][1]-y0)
               for i in range(len(ring)-1))/2


def _esri_polygons(rings) -> dict:
    """Convert Esri clockwise-shell rings without dissolving or repairing them."""
    if not isinstance(rings, list) or not rings:
        raise CBRSQuarantineError('geometry_layout_drift')
    shells, holes = [], []
    for ring in rings:
        if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                or any(not isinstance(c, list) or len(c) != 2 for c in ring)):
            raise CBRSQuarantineError('geometry_layout_drift')
        (shells if _ring_area(ring) < 0 else holes).append(ring)
    if not shells:
        raise CBRSQuarantineError('esri_shell_missing')
    assigned: dict[int, list] = {index: [] for index in range(len(shells))}
    members = [Polygon(ring) for ring in shells]
    for hole in holes:
        hole_polygon = Polygon(hole)
        containers = [(polygon.area, index) for index, polygon in enumerate(members)
                      if polygon.contains(hole_polygon)]
        if not containers:
            raise CBRSQuarantineError('esri_ring_nesting_ambiguous')
        assigned[min(containers)[1]].append(hole)
    polygons = [Polygon(shells[index], assigned[index]) for index in range(len(shells))]
    for polygon in polygons:
        if polygon.is_empty or not polygon.is_valid:
            raise CBRSQuarantineError('invalid_native_geometry')
    converted = polygons[0] if len(polygons) == 1 else shapely.MultiPolygon(polygons)
    return mapping(converted)


def inspect_source(captures: dict) -> tuple[list[dict], dict]:
    page=strict_json(captures['page0000'][0],MAX_BYTES);layer=strict_json(captures['layer'][0])
    if sha(captures['page0000'][0])!=PIN: raise CBRSQuarantineError('pinned_source_content_drift_requires_review')
    if layer.get('id')!=LAYER_ID or layer.get('name')!=LAYER_NAME or layer.get('geometryType')!='esriGeometryPolygon':
        raise CBRSQuarantineError('layer_identity_or_geometry_drift')
    if page.get('exceededTransferLimit') or [f['name'] for f in page.get('fields',[])]!=list(SCHEMA):
        raise CBRSQuarantineError('partial_response_or_field_schema_drift')
    declared=[{k:v for k,v in f.items() if k!='domain'} for f in layer['fields'] if f['type']!='esriFieldTypeGeometry']
    if declared!=NATIVE_FIELDS: raise CBRSQuarantineError('layer_page_schema_disagreement')
    if page.get('spatialReference',{}).get('wkid')!=4269 or layer.get('sourceSpatialReference',{}).get('wkid')!=4269:
        raise CBRSQuarantineError('native_crs_drift')
    before=strict_json(captures['ids'][0]);after=strict_json(captures['ids_after'][0])
    if before!=after or before.get('objectIdFieldName')!='OBJECTID': raise CBRSQuarantineError('mid_capture_identifier_drift')
    ids=before.get('objectIds',[])
    if len(ids)!=EXPECTED_COUNT or len(set(ids))!=EXPECTED_COUNT or any(type(i)is not int for i in ids): raise CBRSQuarantineError('identifier_population_drift')
    if any(strict_json(captures[n][0])!={'count':EXPECTED_COUNT} for n in ['count','count_after']): raise CBRSQuarantineError('count_or_mid_capture_drift')
    features=page.get('features',[])
    if len(features)!=EXPECTED_COUNT or [f['attributes'].get('OBJECTID') for f in features]!=sorted(ids):
        raise CBRSQuarantineError('incomplete_duplicate_or_unordered_feature_population')
    rows=[]
    for feature in features:
        attrs=feature['attributes']
        if set(attrs)!=set(SCHEMA): raise CBRSQuarantineError('unexpected_or_missing_source_columns')
        for name,kind in SCHEMA.items():
            v=attrs[name];expected=int if kind.startswith('int') else str if kind.startswith('str:') else (int,float)
            if v is not None and not isinstance(v,expected): raise CBRSQuarantineError('physical_type_drift:'+name)
            if v is not None and kind.startswith('int') and not -(2**63)<=v<2**63: raise CBRSQuarantineError('OID_width_drift')
            if v is not None and kind.startswith('str:') and len(v)>int(kind[4:]): raise CBRSQuarantineError('string_width_drift:'+name)
        properties={name:(float(attrs[name]) if name in SINGLE_FLOAT_FIELDS and isinstance(attrs[name],int) else attrs[name])
                    for name in SCHEMA}
        geometry=_esri_polygons((feature.get('geometry') or {}).get('rings'))
        geometry_type=shape(geometry).geom_type
        if geometry_type not in ('Polygon','MultiPolygon'): raise CBRSQuarantineError('invalid_native_geometry')
        rows.append({'type':'Feature','properties':properties,'geometry':geometry})
    nulls={name:sum(r['properties'][name] is None for r in rows) for name in SCHEMA}
    if nulls!=EXPECTED_NULLS: raise CBRSQuarantineError('nullability_drift')
    coverage=collections.Counter(r['properties']['Unit_Type'] for r in rows)
    if dict(coverage)!=EXPECTED_COVERAGE: raise CBRSQuarantineError('unit_type_coverage_drift')
    geometries=[shape(r['geometry']) for r in rows]
    audit={'approved':False,'status':'producer_source_fidelity_pass','feature_count':EXPECTED_COUNT,
       'invalid_geometry_count':0,'native_schema':NATIVE_FIELDS,'crs':'EPSG:4269',
       'bounds':shapely.total_bounds(geometries).tolist(),
       'nulls':nulls,'identifiers':{'OBJECTID':{'unique':EXPECTED_COUNT,'nulls':0}},
       'geometry_types':dict(collections.Counter(g.geom_type for g in geometries)),
       'coverage':dict(coverage),
       'single_float_serialization':{name:{'declared':'esriFieldTypeSingle','json_integers':sum(isinstance(f['attributes'][name],int) for f in features)} for name in SINGLE_FLOAT_FIELDS},
       'source_release_date':None,
       'source_release_date_unknown_reason':'The layer and service expose no editing date; the mapper page data note is not a feed release date.'}
    return rows,audit


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_capture_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int64() if v.startswith('int') else pa.string() if v.startswith('str:') else pa.float64(),nullable=bool(EXPECTED_NULLS[k])) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['MultiPolygon','Polygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_geopackage(path: Path, rows: list[dict]) -> None:
    with fiona.open(path,'w',driver='GPKG',SPATIAL_INDEX='NO',layer=SLUG,crs='EPSG:4269',schema={'properties':SCHEMA,'geometry':'Unknown'}) as destination:
        destination.writerecords(rows)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='1970-01-01T00:00:00.000Z'")


def validate_gpkg_metadata(path: Path, rows: list[dict]) -> None:
    # Reconstruct source-derived declarations, including redundant CRS, bounds,
    # SRS authority/WKT and the SRS header of every original geometry BLOB.
    with tempfile.TemporaryDirectory(prefix='generalized-units-metadata-') as tmp:
        reference=Path(tmp)/'reference.gpkg';write_geopackage(reference,rows)
        with sqlite3.connect(f'file:{reference}?mode=ro',uri=True) as expected, sqlite3.connect(f'file:{path}?mode=ro',uri=True) as actual:
            for table in ['gpkg_contents','gpkg_geometry_columns','gpkg_spatial_ref_sys',SLUG]:
                quoted='"'+table+'"'
                if actual.execute('SELECT * FROM '+quoted+' ORDER BY 1').fetchall()!=expected.execute('SELECT * FROM '+quoted+' ORDER BY 1').fetchall():
                    raise CBRSQuarantineError('geopackage_metadata_or_geometry_header_drift:'+table)
                if actual.execute('PRAGMA table_info('+quoted+')').fetchall()!=expected.execute('PRAGMA table_info('+quoted+')').fetchall():
                    raise CBRSQuarantineError('geopackage_physical_table_schema_drift:'+table)


def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise CBRSQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    for part in target.relative_to(root).parts:
        current=current/part
        if current.is_symlink(): raise CBRSQuarantineError('linked_candidate_path')
    return target


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    manifest_body=(root/'metadata/source_manifest.json').read_bytes()
    authored_snapshot='metadata/source/version_'+sha(manifest_body)+'_source_manifest.json'
    allowed_derivatives={f'geopackage/{SLUG}.gpkg','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json',
        'metadata/source/source_manifest.json','metadata/source/data_dictionary.json',
        'metadata/source/quality_manifest.json',authored_snapshot}|STAGED_RELEASE_DERIVATIVE_NAMES
    for relative in ('metadata/source/source_manifest.json',authored_snapshot):
        snapshot=root/relative
        if snapshot.exists() and snapshot.read_bytes()!=manifest_body:
            raise CBRSQuarantineError('cached_authored_manifest_snapshot_differs')
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise CBRSQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{f'metadata/{SLUG}_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise CBRSQuarantineError('existing_candidate_unrostered_or_missing_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise CBRSQuarantineError('invalid_staged_release_derivatives') from exc
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    expected_manifest=provenance(cached, identity)
    expected_manifest['processing_fingerprint']=sha(encode(identity))
    if any(cached[name][0]!=captures[name][0] for name in URLS):
        raise CBRSQuarantineError('cached_capture_differs_from_fresh_source')
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(manifest)
    resolved_file=root/'metadata/resolved_provenance.json'
    if resolved_file.exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if strict_json(resolved_file.read_bytes())!=resolved:
            raise CBRSQuarantineError('cached_resolved_provenance_differs')
    if set(manifest)!=set(expected_manifest): raise CBRSQuarantineError('cached_provenance_field_roster_drift')
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise CBRSQuarantineError('cached_provenance_differs_from_original_capture')
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['page0000'][0],
        'raw/retrieval.json':encode(cached['page0000'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        f'metadata/{SLUG}_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise CBRSQuarantineError('cached_source_or_normalized_records_differ')
    validate_gpkg_metadata(root/'geopackage'/f'{SLUG}.gpkg',rows)
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v=='int32' else 'int' if v=='int64' else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'Unknown'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(4269)):
            raise CBRSQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise CBRSQuarantineError('cached_geopackage_source_fidelity_failure')
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(typed_table(rows,audit),check_metadata=True):
        raise CBRSQuarantineError('cached_typed_parquet_source_fidelity_failure')
    with tempfile.TemporaryDirectory(prefix='generalized-units-evidence-') as tmp:
        directory=Path(tmp);original=directory/'original.jsonl';candidate=directory/'candidate.jsonl'
        original.write_bytes(expected_bytes['metadata/candidate.records.jsonl'])
        candidate.write_bytes(b''.join(encode(r) for r in decoded))
        report=compare_derivative(original,candidate,directory/'comparison')
        if report.get('status')!='pass': raise CBRSQuarantineError('fresh_source_fidelity_comparison_failed')
        evidence=fidelity_payloads(report,rows,original.read_bytes(),directory/'comparison')
        for name,body in evidence.items():
            if (root/name).read_bytes()!=body:
                raise CBRSQuarantineError('cached_complete_source_fidelity_evidence_drift:'+name)


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict) -> None:
    """Install one fully built local version atomically; partial builds are never discoverable."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    lock=target.parent/('.'+target.name+'.lock')
    descriptor=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1: raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise CBRSQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for name,value in payloads.items(): immutable(private,name,value)
            immutable(private,f'metadata/{SLUG}_candidate_ready.json',encode(ready))
            os.rename(tmp,target)


@contextmanager
def candidate_lock(storage: StagingStorageResource, prefix: str):
    """Serialize lookup, validation and invalid-cache quarantine with installation."""
    target=local_candidate_path(storage,prefix);target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target


def quarantine_cached_candidate_held(storage: StagingStorageResource, target: Path, prefix: str, reason: str) -> str:
    """Caller holds candidate_lock; preserve every byte before releasing it."""
    destination=Path(storage.local_dir)/'_quarantine'/SLUG/('invalid-cache-'+uuid4().hex)
    destination.mkdir(parents=True)
    os.rename(target,destination/'candidate')
    (destination/'report.json').write_bytes(encode({'approved':False,'status':'quarantined_invalid_draft_cache','original_prefix':prefix,'reason':reason}))
    return str(destination)


def fidelity_payloads(fidelity: dict, rows: list[dict], records: bytes, comparison: Path) -> dict[str, bytes]:
    """One deterministic evidence definition for construction and fresh cache replay."""
    parquet_fidelity={'status':'pass','method':'complete_decoded_source_records_byte_equality_and_exact_arrow_schema',
        'rows':len(rows),'source_records_sha256':sha(records),'shared_schema_geometry_validation_reference':fidelity}
    payloads={
        'metadata/geopackage_source_fidelity.json':encode({'canonical_full_records_byte_identical':True,'shared_verifier':fidelity,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/parquet_source_fidelity.json':encode({'canonical_full_records_byte_identical':True,'shared_verifier':parquet_fidelity,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'status':'blocked','approved':False,
            'reason_codes':['unapproved_baseline_field_id_geometry_and_single_precision_serialization_differences'],'acceptance':'not_established'})}
    payloads.update({f'metadata/source-fidelity/{name}':(comparison/name).read_bytes() for name in FIDELITY_FILES})
    return payloads


def stage_generalized_units_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['page0000']
    try:
        identity = processing_identity()
        manifest=provenance(captures, identity)
        with tempfile.TemporaryDirectory(prefix='generalized-units-source-') as tmp:
            directory = Path(tmp)
            rows, audit = inspect_source(captures)
            records = b''.join(encode(r) for r in rows)
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-generalizedunits-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/{SLUG}_candidate_ready.json'
            with candidate_lock(storage,prefix) as target:
                if target.exists():
                    try:
                        if not storage.object_exists(ready_key):raise CBRSQuarantineError('existing_candidate_missing_ready_marker')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', f'metadata/{SLUG}_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'metadata/source-fidelity/{name}' for name in FIDELITY_FILES} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
                        if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                            raise CBRSQuarantineError('existing_candidate_inventory_conflict')
                        if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
                                or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                            raise CBRSQuarantineError('existing_candidate_identity_conflict')
                        for name, expected in ready['objects'].items():
                            key = f'{prefix}/{name}'
                            snapshot = storage.object_snapshot(key)
                            if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                                raise CBRSQuarantineError(f'existing_candidate_object_changed:{name}')
                        reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity)
                        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                                'status': 'working_draft_candidate_ready', 'processing_fingerprint': fingerprint}
                    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,fiona.errors.FionaError) as exc:
                        quarantine_cached_candidate_held(storage,target,prefix,str(exc))
                        raise
            geopackage = directory / f'{SLUG}.gpkg'
            write_geopackage(geopackage, rows)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v=='int32' else 'int' if v=='int64' else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Unknown'}:
                    raise CBRSQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
                    raise CBRSQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if fidelity.get('status')!='pass' or decoded.read_bytes() != records:
                raise CBRSQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise CBRSQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            manifest.update(code_commit=identity['code_commit'],
                            code_identity_scheme=identity['code_identity_scheme'],
                            processing_fingerprint=fingerprint)
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), f'metadata/{SLUG}_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
            }
            payloads.update(fidelity_payloads(fidelity,rows,records,directory/'comparison'))
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, sqlite3.Error, fiona.errors.FionaError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc