"""Faithful complete delegated FWS CBRS Map Panels local draft.

Native API fields, identifiers, date integers and NAD83 coordinates are retained.
Publication date and all source/baseline/contract acceptance remain unresolved.
"""
from __future__ import annotations

import collections
import hashlib
import importlib.metadata
import io
import fcntl
import os
import json
import platform
import re
import sqlite3
import stat
import struct
import subprocess
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import mapping, shape

from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'cbrs-map-panels-1'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
URLS = {'fws_data': 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data', 'official_item': 'https://www.arcgis.com/sharing/rest/content/items/a60c25b4b2e44969b3164742901058a2?f=pjson', 'layer': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5?f=pjson', 'count': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query?where=1%3D1&returnCountOnly=true&f=json', 'ids': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query?where=1%3D1&returnIdsOnly=true&f=json', 'page0000': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query?where=1%3D1&outFields=%2A&returnGeometry=true&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=json', 'count_after': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_after': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query?where=1%3D1&returnIdsOnly=true&f=json'}
URL = URLS['page0000']
PIN = '9c4162886e09b2d10152cf83c059fb26dc7825836c4f3ac5ed8b1e5fda074787'
ARCHIVE_NAME = 'native-map-panels-page0000.json'
MAX_BYTES = 4_000_000
FIDELITY_FILES = ('baseline_comparison.json','baseline_comparison.md','baseline_differences.jsonl','baseline_field_changes.jsonl')
SCHEMA = {'OBJECTID': 'int64', 'Panel_No': 'str:10', 'State': 'str:2', 'Map_Date': 'int64', 'Title': 'str:75', 'Title_2': 'str:75', 'Title_3': 'str:75', 'Title_4': 'str:75', 'Map_Link': 'str:150', 'Effective_Date': 'int64', 'Historical_Link': 'str:500', 'Shape_Length': 'float', 'Shape_Area': 'float'}
NATIVE_FIELDS = [{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID'}, {'name': 'Panel_No', 'type': 'esriFieldTypeString', 'alias': 'Panel_No', 'length': 10}, {'name': 'State', 'type': 'esriFieldTypeString', 'alias': 'State', 'length': 2}, {'name': 'Map_Date', 'type': 'esriFieldTypeDate', 'alias': 'Map_Date', 'length': 8}, {'name': 'Title', 'type': 'esriFieldTypeString', 'alias': 'Title', 'length': 75}, {'name': 'Title_2', 'type': 'esriFieldTypeString', 'alias': 'Title_2', 'length': 75}, {'name': 'Title_3', 'type': 'esriFieldTypeString', 'alias': 'Title_3', 'length': 75}, {'name': 'Title_4', 'type': 'esriFieldTypeString', 'alias': 'Title_4', 'length': 75}, {'name': 'Map_Link', 'type': 'esriFieldTypeString', 'alias': 'Map_Link', 'length': 150}, {'name': 'Effective_Date', 'type': 'esriFieldTypeDate', 'alias': 'Effective_Date', 'length': 8}, {'name': 'Historical_Link', 'type': 'esriFieldTypeString', 'alias': 'Change_Link', 'length': 500}, {'name': 'Shape_Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Length'}, {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Area'}]
EXPECTED_NULLS = {'OBJECTID': 0, 'Panel_No': 0, 'State': 0, 'Map_Date': 0, 'Title': 0, 'Title_2': 114, 'Title_3': 170, 'Title_4': 243, 'Map_Link': 0, 'Effective_Date': 0, 'Historical_Link': 0, 'Shape_Length': 0, 'Shape_Area': 0}
PROPOSAL = {'approved':False,'id_fields':['OBJECTID'],'geometry':True,
 'coverage_fields':['State'],'crs':'EPSG:4269',
 'columns':{name:{'type':'integer' if kind.startswith('int') else 'decimal' if kind.startswith('float') else 'string','nullable':bool(EXPECTED_NULLS[name])} for name,kind in SCHEMA.items()}}


class CBRSQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    if len(body) > 4_000_000:
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
                    with httpx.Client(follow_redirects=False,timeout=45,headers={'Accept-Encoding':'identity'}) as client:
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


def provenance(captures: dict) -> dict:
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
            'release_date_unknown_reason':'Map panel dates are per-record dates, not a documented feed publication date; portal modification and fetch timestamps are not release dates.',
            'sha256':sha(body),'bytes':len(body),'license':LANDING,
            'license_scope':'FWS CBRS mapper official maps control; digital representations are not definitive property determinations. Item accessInformation/licenseInfo retained verbatim in raw official_item; release acceptance pending.',
            'attribution':'U.S. Fish and Wildlife Service; delegated USGS CBRS Mapper distribution.',
            'lineage':'FWS official maps-and-data links exact ArcGIS item and USGS CBRS Mapper; exact service layer5 Map Panels.',
            'authority_evidence':LANDING,'delegation_evidence':{'url':LANDING,'scope':'Exact FWS-linked USGS CBRS MapServer via ArcGIS itema60c25b4b2e44969b3164742901058a2'}})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'CBRS Map Panels — faithful complete API working draft',
        'publisher':'U.S. Fish and Wildlife Service via delegated USGS CBRS Mapper','agency':'U.S. Fish and Wildlife Service',
        'source_url':LANDING,'license':LANDING,'inputs':inputs,
        'license_review_required':'Preserved full official item constraints; maintainer acceptance required.',
        'transformations':[{'operation':'decode_native_Esri_JSON','preserved':'All13 source attributes, original OBJECTID JSON integers under proposed int64 proxy (native OID width not supplied), date millisecond int64 representations, all native EPSG4269 coordinate rings; no field renames, ID generation, date conversion or geometry repair.',
            'repair_or_projection':False,'mapping_approval':'not established; baseline truncated names/string dates/null-empty representations require explicit decisions'}],
        'unsupported_output_formats':{'shapefile_zip':'Long field names truncate and native millisecond dates lose types; no destructive mapping approved.',
            'pmtiles':'Reprojection/quantization changes geometry; no visualization contract approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity() -> dict:
    """Installed source, lock/project, Python, Python distributions and native engines."""
    root = Path(__file__).parents[3]
    sources = sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))
    return {'python': platform.python_version(), 'implementation': platform.python_implementation(),
            'platform': platform.platform(),
            'distributions': sorted((d.metadata['Name'], d.version) for d in importlib.metadata.distributions()),
            'gdal': fiona.__gdal_version__, 'geos': shapely.geos_version_string, 'proj': pyproj.proj_version_str,
            'files': {str(p.relative_to(root)): sha256_file(p) for p in sources + [root / 'uv.lock', root / 'pyproject.toml']},
            'verifier_sha256': verifier_implementation_sha256()}


def inspect_source(captures: dict) -> tuple[list[dict], dict, dict]:
    page=strict_json(captures['page0000'][0]);layer=strict_json(captures['layer'][0])
    if sha(captures['page0000'][0])!=PIN: raise CBRSQuarantineError('pinned_source_content_drift_requires_review')
    if layer.get('id')!=5 or layer.get('name')!='CBRS Map Panels' or layer.get('geometryType')!='esriGeometryPolygon':
        raise CBRSQuarantineError('layer_identity_or_geometry_drift')
    if page.get('exceededTransferLimit') or page.get('fields')!=NATIVE_FIELDS:
        raise CBRSQuarantineError('partial_response_or_field_schema_drift')
    declared=[{k:v for k,v in f.items() if k!='domain'} for f in layer['fields'] if f['type']!='esriFieldTypeGeometry']
    if declared!=NATIVE_FIELDS: raise CBRSQuarantineError('layer_page_schema_disagreement')
    if page.get('spatialReference',{}).get('wkid')!=4269 or layer.get('sourceSpatialReference',{}).get('wkid')!=4269:
        raise CBRSQuarantineError('native_crs_drift')
    before=strict_json(captures['ids'][0]);after=strict_json(captures['ids_after'][0])
    if before!=after or before.get('objectIdFieldName')!='OBJECTID': raise CBRSQuarantineError('mid_capture_identifier_drift')
    ids=before.get('objectIds',[])
    if len(ids)!=574 or len(set(ids))!=574 or any(type(i)is not int for i in ids): raise CBRSQuarantineError('identifier_population_drift')
    if any(strict_json(captures[n][0])!={'count':574} for n in ['count','count_after']): raise CBRSQuarantineError('count_or_mid_capture_drift')
    features=page.get('features',[])
    if len(features)!=574 or [f['attributes'].get('OBJECTID') for f in features]!=sorted(ids):
        raise CBRSQuarantineError('incomplete_duplicate_or_unordered_feature_population')
    rows=[]
    for feature in features:
        attrs=feature['attributes']
        if set(attrs)!=set(SCHEMA): raise CBRSQuarantineError('unexpected_or_missing_source_columns')
        for name,kind in SCHEMA.items():
            v=attrs[name];expected=int if kind.startswith('int') else float if kind.startswith('float') else str
            if v is not None and type(v)is not expected: raise CBRSQuarantineError('physical_type_drift:'+name)
            if v is not None and kind.startswith('int') and not -(2**63)<=v<2**63: raise CBRSQuarantineError('OID_width_drift')
            if v is not None and kind.startswith('str:') and len(v)>int(kind[4:]): raise CBRSQuarantineError('string_width_drift:'+name)
        rings=feature.get('geometry',{}).get('rings',[])
        if len(rings)!=1 or len(rings[0])<4 or rings[0][0]!=rings[0][-1] or any(len(c)!=2 for c in rings[0]):
            raise CBRSQuarantineError('geometry_layout_drift')
        geometry={'type':'Polygon','coordinates':rings}
        g=shape(geometry)
        if not g.is_valid or g.is_empty: raise CBRSQuarantineError('invalid_native_geometry')
        rows.append({'type':'Feature','properties':attrs,'geometry':geometry})
    nulls={name:sum(r['properties'][name] is None for r in rows) for name in SCHEMA}
    if nulls!=EXPECTED_NULLS: raise CBRSQuarantineError('nullability_drift')
    audit={'approved':False,'status':'producer_source_fidelity_pass','feature_count':574,'invalid_geometry_count':0,
       'native_schema':NATIVE_FIELDS,'crs':'EPSG:4269','bounds':shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
       'nulls':nulls,'identifiers':{'OBJECTID':{'unique':574,'nulls':0}},
       'coverage':dict(collections.Counter(r['properties']['State'] for r in rows)),
       'source_release_date':None,'source_release_date_unknown_reason':'Per-map dates do not establish feed publication date.'}
    return rows,audit,{}


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_capture_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int32() if v=='int32' else pa.int64() if v=='int64' else pa.float64() if v.startswith('float') else pa.string(),nullable=bool(EXPECTED_NULLS[k])) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Polygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_geopackage(path: Path, rows: list[dict]) -> None:
    with fiona.open(path,'w',driver='GPKG',SPATIAL_INDEX='NO',layer=SLUG,crs='EPSG:4269',schema={'properties':SCHEMA,'geometry':'Polygon'}) as destination:
        destination.writerecords(rows)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='1970-01-01T00:00:00.000Z'")


def validate_gpkg_metadata(path: Path, rows: list[dict]) -> None:
    # Reconstruct source-derived declarations, including redundant CRS, bounds,
    # SRS authority/WKT and the SRS header of every original geometry BLOB.
    with tempfile.TemporaryDirectory(prefix='map-panels-metadata-') as tmp:
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
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    # The normal graph adds only these derivative files. Source-owned directories remain exact.
    allowed_derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json'}
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise CBRSQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/cbrs_map_panels_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise CBRSQuarantineError('existing_candidate_unrostered_or_missing_object')
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    expected_manifest=provenance(cached)
    expected_manifest['processing_fingerprint']=sha(encode(identity))
    if any(cached[name][0]!=captures[name][0] for name in URLS):
        raise CBRSQuarantineError('cached_capture_differs_from_fresh_source')
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(manifest)
    # code_commit identifies the original immutable construction, including later docs-only commits.
    if set(manifest)!=set(expected_manifest): raise CBRSQuarantineError('cached_provenance_field_roster_drift')
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise CBRSQuarantineError('cached_provenance_differs_from_original_capture')
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['page0000'][0],
        'raw/retrieval.json':encode(cached['page0000'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        'metadata/cbrs_map_panels_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise CBRSQuarantineError('cached_source_or_normalized_records_differ')
    validate_gpkg_metadata(root/'geopackage'/f'{SLUG}.gpkg',rows)
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v=='int32' else 'int' if v=='int64' else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'Polygon'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(4269)):
            raise CBRSQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise CBRSQuarantineError('cached_geopackage_source_fidelity_failure')
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(typed_table(rows,audit),check_metadata=True):
        raise CBRSQuarantineError('cached_typed_parquet_source_fidelity_failure')
    # Re-run the shared comparator against the freshly decoded original and cached
    # derivative, binding every report field, sidecar, Markdown and wrapper byte.
    with tempfile.TemporaryDirectory(prefix='map-panels-evidence-') as tmp:
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
    # Cooperating refresh processes cannot replace an existing version or race its first commit.
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
            immutable(private,'metadata/cbrs_map_panels_candidate_ready.json',encode(ready))
            os.rename(tmp,target)
    # Keep the tiny lock inode: unlinking permits a third process to bypass a waiter.



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
            'reason_codes':['unapproved_baseline_field_truncation_date_type_empty_null_id_and_geometry_differences'],'acceptance':'not_established'})}
    payloads.update({f'metadata/source-fidelity/{name}':(comparison/name).read_bytes() for name in FIDELITY_FILES})
    return payloads


def stage_cbrs_map_panels_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['page0000']
    try:
        manifest=provenance(captures)
        with tempfile.TemporaryDirectory(prefix='cbrs_map_panels-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_source(captures)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-cbrspanels-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/cbrs_map_panels_candidate_ready.json'
            with candidate_lock(storage,prefix) as target:
                if target.exists():
                    try:
                        if not storage.object_exists(ready_key):raise CBRSQuarantineError('existing_candidate_missing_ready_marker')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/cbrs_map_panels_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'metadata/source-fidelity/{name}' for name in FIDELITY_FILES} | {f'raw/members/{name}' for name in members} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
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
                        reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members)
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
                if dict(source.schema) != {'properties': {k: ('int32' if v=='int32' else 'int' if v=='int64' else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Polygon'}:
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
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3], text=True).strip()
            manifest.update(code_commit=commit,processing_fingerprint=fingerprint)
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/cbrs_map_panels_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
            }
            payloads.update(fidelity_payloads(fidelity,rows,records,directory/'comparison'))
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile, sqlite3.Error, fiona.errors.FionaError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
