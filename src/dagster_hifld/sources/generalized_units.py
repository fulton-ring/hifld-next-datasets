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
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.sources.esri_geometry import esri_polygon_wkb
from dagster_hifld.sources.arcgis_layer import (
    layer_capture_roster, capture_witness, fetch_capture_retry, load_replay,
)
from shapely.geometry import Polygon, mapping, shape

from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.capture_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as packaged_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'generalized-units-1'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
_ITEM = 'https://www.arcgis.com/sharing/rest/content/items/a60c25b4b2e44969b3164742901058a2?f=pjson'
_LAYER = 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/4'
ARCHIVE_NAME = 'native-generalized-units-page0000.json'
MAX_BYTES = 64_000_000
LAYER_ID = 4
LAYER_NAME = 'Generalized Units'
PAGE_SIZE = 1000
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
PROPOSAL = {'approved':False,'id_fields':['OBJECTID'],'geometry':True,
 'coverage_fields':['Unit_Type'],'crs':'EPSG:4269',
 'columns':{name:{'type':'integer' if kind.startswith('int') else 'decimal' if kind.startswith('float') else 'string','nullable':False} for name,kind in SCHEMA.items()}}


def capture_roster(ids: list[int]) -> dict[str, str]:
    aliases = {'layer_before': 'layer', 'count_before': 'count', 'ids_before': 'ids'}
    return {aliases.get(name, name): url for name, url in layer_capture_roster(
        ids, layer_url=_LAYER, service_url=_LAYER.rsplit('/', 1)[0], page_size=PAGE_SIZE,
        extra_documents=(('fws_data', LANDING), ('official_item', _ITEM)),
        page_namer=lambda index, _: f'page{index:04d}')}


def capture_urls(captures: dict) -> dict[str, str]:
    ids = strict_json(captures['ids'][0]).get('objectIds')
    if not isinstance(ids, list) or not ids or any(type(oid) is not int for oid in ids):
        raise CBRSQuarantineError('identifier_roster_invalid')
    capture_witness({'count_before': captures['count'], 'ids_before': captures['ids'],
                     'count_after': captures['count'], 'ids_after': captures['ids']})
    return capture_roster(sorted(ids))


def population_witness(captures: dict, captured_ids=None) -> list[int]:
    return capture_witness({**captures, 'count_before': captures['count'],
                            'ids_before': captures['ids']}, captured_ids=captured_ids)


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


def validate_receipt(body: bytes, receipt: dict, url: str, wire: bytes) -> None:
    if (receipt.get('requested_url') != url or receipt.get('final_url') != url
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('byte_length')) is not int or receipt['byte_length'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise CBRSQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    if (receipt.get('complete_body') is not True
            or type(receipt.get('wire_bytes')) is not int or receipt['wire_bytes'] != len(wire)
            or receipt.get('wire_sha256') != sha(wire)):
        raise CBRSQuarantineError('capture_receipt_wire_hash_length_mismatch')
    timestamp=receipt.get('observed_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise CBRSQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    moment = datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        raise CBRSQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    require_local(storage)
    captures = {}
    with tempfile.TemporaryDirectory(prefix='generalized-units-capture-') as tmp, httpx.Client(timeout=90) as client:
        root = capture_dir if capture_dir is not None else Path(tmp)
        budget = [0]
        try:
            roster = capture_roster([])
            discovery = [(name, roster[name]) for name in (
                'fws_data', 'official_item', 'service_before', 'layer', 'count', 'ids')]
            pending = discovery
            while pending:
                for name, url in pending:
                    if capture_dir is None:
                        body, receipt = fetch_capture_retry(client, url, root, name, budget,
                            max_capture_bytes=MAX_BYTES * 4, max_body_bytes=MAX_BYTES)
                        receipt.update(requested_url=url, byte_length=len(body))
                        (root / f'{name}.receipt.json').write_bytes(encode(receipt))
                    else:
                        body, receipt = load_replay(root, name)
                        receipt = strict_json((root / f'{name}.receipt.json').read_bytes())
                        budget[0] += len(body) + receipt['wire_bytes']
                        if len(body) > MAX_BYTES or budget[0] > MAX_BYTES * 4:
                            raise CBRSQuarantineError('capture_size_limit')
                    wire = (root / f'{name}.wire').read_bytes()
                    validate_receipt(body, receipt, url, wire)
                    captures[name] = (body, receipt, wire)
                if pending is discovery:
                    roster = capture_urls(captures)
                    pending = [(name, url) for name, url in roster.items() if name not in captures]
                else:
                    pending = []
            population_witness(captures)
        except (httpx.HTTPError, OSError, ValueError, TypeError, KeyError) as exc:
            location = quarantine(storage, b'', {'capture_path': str(root)}, str(exc))
            for path in root.iterdir() if root.exists() else ():
                if path.is_file() and not path.is_symlink():
                    immutable(storage, f'{location}/evidence/{path.name}', path.read_bytes())
            raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
        for path in root.iterdir():
            if path.is_file() and not path.is_symlink():
                body = path.read_bytes()
                immutable(storage, f'_raw-captures/{SLUG}/{sha(body)}/{path.name}', body)
    return captures


def provenance(captures: dict, identity: dict) -> dict:
    urls = capture_urls(captures)
    if set(captures)!=set(urls): raise CBRSQuarantineError('capture_roster_drift')
    for name,url in urls.items():
        body, receipt, wire = captures[name]
        validate_receipt(body, receipt, url, wire)
    if b'gis1.wim.usgs.gov' not in captures['fws_data'][0] or b'a60c25b4b2e44969b3164742901058a2' not in captures['fws_data'][0]:
        raise CBRSQuarantineError('official_delegation_changed')
    item=strict_json(captures['official_item'][0])
    if item.get('url')!='https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer':
        raise CBRSQuarantineError('delegated_service_changed')
    inputs=[]
    for name,(body,receipt,wire) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'U.S. Fish and Wildlife Service',
            'host':urls[name].split('/')[2],'role':'producer_publication' if name=='fws_data' else 'official_distributor',
            'canonical_url':urls[name],'retrieval_url':receipt['requested_url'],'observed_final_url':receipt['final_url'],
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


def _esri_polygons(rings) -> dict:
    """Convert Esri rings through the shared GDAL converter without repair."""
    if not isinstance(rings, list) or not rings:
        raise CBRSQuarantineError('geometry_layout_drift')
    for ring in rings:
        if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                or any(not isinstance(c, list) or len(c) != 2 for c in ring)):
            raise CBRSQuarantineError('geometry_layout_drift')
    try:
        converted = esri_polygon_wkb({"rings": rings}, allow_zm=False)
    except ValueError as exc:
        code = str(exc)
        if code == "source_polygon_shell_missing":
            raise CBRSQuarantineError('esri_shell_missing') from exc
        if code == "source_hole_ownership_ambiguous":
            raise CBRSQuarantineError('esri_ring_nesting_ambiguous') from exc
        raise
    geometry = shapely.from_wkb(converted)
    if geometry.is_empty or not geometry.is_valid:
        raise CBRSQuarantineError('invalid_native_geometry')
    return mapping(geometry)


def inspect_source(captures: dict) -> tuple[list[dict], dict]:
    layer=strict_json(captures['layer'][0])
    urls = capture_urls(captures)
    if set(captures) != set(urls): raise CBRSQuarantineError('capture_roster_drift')
    for name in ('layer', 'service'):
        before_name = 'service_before' if name == 'service' else name
        if strict_json(captures[before_name][0]) != strict_json(captures[name + '_after'][0]):
            raise CBRSQuarantineError('mid_capture_metadata_drift')
    features = []
    ids = population_witness(captures)
    for index, name in enumerate(name for name in urls if name.startswith('page')):
        page = strict_json(captures[name][0], MAX_BYTES)
        if page.get('exceededTransferLimit') or [f['name'] for f in page.get('fields',[])] != list(SCHEMA):
            raise CBRSQuarantineError('partial_response_or_field_schema_drift')
        if [(f['name'], f.get('type')) for f in page['fields']] != [(f['name'], f['type']) for f in NATIVE_FIELDS]:
            raise CBRSQuarantineError('layer_page_schema_disagreement')
        if page.get('spatialReference',{}).get('wkid') != 4269:
            raise CBRSQuarantineError('native_crs_drift')
        page_features = page.get('features', [])
        if [f['attributes'].get('OBJECTID') for f in page_features] != ids[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]:
            raise CBRSQuarantineError('incomplete_duplicate_or_unordered_feature_population')
        features.extend(page_features)
    population_witness(captures, [f['attributes'].get('OBJECTID') for f in features])
    if layer.get('id')!=LAYER_ID or layer.get('name')!=LAYER_NAME or layer.get('geometryType')!='esriGeometryPolygon':
        raise CBRSQuarantineError('layer_identity_or_geometry_drift')
    declared=[{k:v for k,v in f.items() if k!='domain'} for f in layer['fields'] if f['type']!='esriFieldTypeGeometry']
    if declared!=NATIVE_FIELDS: raise CBRSQuarantineError('layer_page_schema_disagreement')
    if layer.get('sourceSpatialReference',{}).get('wkid')!=4269:
        raise CBRSQuarantineError('native_crs_drift')
    before=strict_json(captures['ids'][0]);after=strict_json(captures['ids_after'][0])
    if any(doc.get('objectIdFieldName')!='OBJECTID' for doc in (before, after)):
        raise CBRSQuarantineError('native_identifier_field_drift')
    rows=[]
    for feature in features:
        attrs=feature['attributes']
        if set(attrs)!=set(SCHEMA): raise CBRSQuarantineError('unexpected_or_missing_source_columns')
        for name,kind in SCHEMA.items():
            v=attrs[name];expected=int if kind.startswith('int') else str if kind.startswith('str:') else (int,float)
            if v is None and not PROPOSAL['columns'][name]['nullable']:
                raise CBRSQuarantineError('nullability_drift:'+name)
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
    coverage=collections.Counter(r['properties']['Unit_Type'] for r in rows)
    geometries=[shape(r['geometry']) for r in rows]
    audit={'approved':False,'status':'producer_source_fidelity_pass','feature_count':len(rows),
       'source_id_sha256':sha(encode(ids)), 'capture_completeness':'pass',
       'invalid_geometry_count':0,'native_schema':NATIVE_FIELDS,'crs':'EPSG:4269',
       'bounds':shapely.total_bounds(geometries).tolist(),
       'nulls':nulls,'identifiers':{'OBJECTID':{'unique':len(ids),'nulls':0}},
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
            'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int64() if v.startswith('int') else pa.string() if v.startswith('str:') else pa.float64(),nullable=PROPOSAL['columns'][k]['nullable']) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
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
    cached = {}
    for name in captures:
        body, receipt = load_replay(root / 'raw/captures', name)
        receipt = strict_json((root / f'raw/captures/{name}.receipt.json').read_bytes())
        cached[name] = (body, receipt, (root / f'raw/captures/{name}.wire').read_bytes())
    expected_manifest=provenance(cached, identity)
    expected_manifest['processing_fingerprint']=sha(encode(identity))
    if any(cached[name][0]!=captures[name][0] for name in captures):
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
    body,receipt,_wire = captures['page0000']
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
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', f'metadata/{SLUG}_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'metadata/source-fidelity/{name}' for name in FIDELITY_FILES} | {f'raw/captures/{name}{suffix}' for name in captures for suffix in ('.body','.receipt.json','.wire')}
                        if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                            raise CBRSQuarantineError('existing_candidate_inventory_conflict')
                        if (ready.get('source_sha256') != sha(body) or ready.get('candidate_records_sha256') != sha(records)
                                or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                            raise CBRSQuarantineError('existing_candidate_identity_conflict')
                        for name, expected in ready['objects'].items():
                            key = f'{prefix}/{name}'
                            snapshot = storage.object_snapshot(key)
                            if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                                raise CBRSQuarantineError(f'existing_candidate_object_changed:{name}')
                        reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity)
                        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows), 'source_id_sha256': audit['source_id_sha256'],
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
            payloads.update({f'raw/captures/{name}.wire':value[2] for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':sha(body),
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows), 'source_id_sha256':audit['source_id_sha256'],
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, sqlite3.Error, fiona.errors.FionaError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
