"""Bounded draft capture of the official USACE National Inventory of Dams service.

This source is the USACE producer service directly linked from the Esri Federal
Data distribution item. It captures the layer's complete current population
without asserting that NID includes every qualifying dam. Historical DataLumos
V1 equivalence, schema/baseline acceptance, rights, freshness and release remain
pending; every candidate and publication gate is unapproved.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import shutil
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.http_capture import DECODE_BLOCK_BYTES
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "national-inventory-of-dams-nid-1"
PUBLISHER = "U.S. Army Corps of Engineers (USACE), National Inventory of Dams"
SERVICE_URL = "https://geospatial.sec.usace.army.mil/dls/rest/services/NID/National_Inventory_of_Dams_Public_Service/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "National Inventory of Dams"
PAGE_SIZE = 2_000
MAX_RESPONSE_BYTES = 12_000_000
MAX_CAPTURE_PHYSICAL_BYTES = 250_000_000
MIN_DISK_FREE_BYTES = 400_000_000
MAX_CAPTURE_MANIFEST_BYTES = 2_000_000
MAX_RECEIPT_BYTES = 64_000
EXPECTED_EXTENT = (-176.6873, 13.3605, 144.7061, 71.266667)

# Observed from the USACE producer layer metadata, not from the Esri cache.
# Pin names, physical types and string/blob lengths; source values remain untouched.
EXPECTED_FIELD_SIGNATURES = (
    ('OBJECTID', 'esriFieldTypeOID', None), ('NIDID', 'esriFieldTypeString', 8),
    ('OTHER_STRUCTURE_ID', 'esriFieldTypeString', 100), ('FEDERAL_ID', 'esriFieldTypeString', 100),
    ('DATA_UPDATED', 'esriFieldTypeDate', 8), ('NAME', 'esriFieldTypeString', 255),
    ('OTHER_NAMES', 'esriFieldTypeString', 1000), ('OWNER_TYPES', 'esriFieldTypeString', 1000),
    ('PRIMARY_OWNER_TYPE', 'esriFieldTypeString', 1000), ('STATE_FED_AGENCY', 'esriFieldTypeString', 255),
    ('NUMBER_ASSOCIATED_STRUCTURES', 'esriFieldTypeInteger', None), ('DESIGNER_NAMES', 'esriFieldTypeString', 1000),
    ('NON_FED_ON_FED', 'esriFieldTypeString', 20), ('STATE_REGULATED', 'esriFieldTypeString', 20),
    ('STATE_JURISDICTION', 'esriFieldTypeString', 20), ('STATE_REGULATORY_AGENCY', 'esriFieldTypeString', 500),
    ('STATE_PERMITTING', 'esriFieldTypeString', 20), ('STATE_INSPECTION', 'esriFieldTypeString', 20),
    ('STATE_ENFORCEMENT', 'esriFieldTypeString', 20), ('PRIMARY_SOURCE_AGENCY', 'esriFieldTypeString', 255),
    ('LATITUDE', 'esriFieldTypeDouble', None), ('LONGITUDE', 'esriFieldTypeDouble', None),
    ('STATE', 'esriFieldTypeString', 500), ('COUNTYSTATE', 'esriFieldTypeString', 500),
    ('CITY', 'esriFieldTypeString', 256), ('DISTANCE', 'esriFieldTypeDouble', None),
    ('RIVER_OR_STREAM', 'esriFieldTypeString', 255), ('CONGDIST', 'esriFieldTypeString', 500),
    ('CONG_REPRESNTATIVE', 'esriFieldTypeString', 100), ('FED_AGENCY_OWNERS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_FUNDINGS', 'esriFieldTypeString', 500), ('FED_AGENCY_DESIGNERS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_CONSTRUCTIONS', 'esriFieldTypeString', 500), ('FED_AGENCY_REGULATORIES', 'esriFieldTypeString', 500),
    ('FED_AGENCY_INSPECTIONS', 'esriFieldTypeString', 500), ('FED_AGENCY_OPERATIONS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_OTHERS', 'esriFieldTypeString', 500), ('PRIMARY_PURPOSE', 'esriFieldTypeString', 256),
    ('PURPOSES', 'esriFieldTypeString', 1000), ('PRIMARY_DAM_TYPE', 'esriFieldTypeString', 100),
    ('DAM_TYPES', 'esriFieldTypeString', 500), ('CORE_TYPES', 'esriFieldTypeString', 500),
    ('FOUNDATIONS', 'esriFieldTypeString', 500), ('DAM_HEIGHT', 'esriFieldTypeDouble', None),
    ('HYDRAULIC_HEIGHT', 'esriFieldTypeDouble', None), ('STRUCTURAL_HEIGHT', 'esriFieldTypeDouble', None),
    ('NID_HEIGHT', 'esriFieldTypeInteger', None), ('DAM_LENGTH', 'esriFieldTypeDouble', None),
    ('DAM_VOLUME', 'esriFieldTypeInteger', None), ('YEAR_COMPLETED', 'esriFieldTypeInteger', None),
    ('NID_STORAGE', 'esriFieldTypeDouble', None), ('MAX_STORAGE', 'esriFieldTypeDouble', None),
    ('NORMAL_STORAGE', 'esriFieldTypeDouble', None), ('SURFACE_AREA', 'esriFieldTypeDouble', None),
    ('DRAINAGE_AREA', 'esriFieldTypeDouble', None), ('MAX_DISCHARGE', 'esriFieldTypeDouble', None),
    ('SPILLWAY_TYPE', 'esriFieldTypeString', 256), ('SPILLWAY_WIDTH', 'esriFieldTypeInteger', None),
    ('NUMBER_OF_LOCKS', 'esriFieldTypeInteger', None), ('LENGTH_OF_LOCKS', 'esriFieldTypeDouble', None),
    ('WIDTH_OF_LOCKS', 'esriFieldTypeDouble', None), ('YEARS_MODIFIED', 'esriFieldTypeString', 500),
    ('OUTLET_GATES', 'esriFieldTypeString', 500), ('ASSOCIATED_STRUCTURES', 'esriFieldTypeString', 1000),
    ('LAST_INSPECTION_DATE', 'esriFieldTypeDate', 8), ('INSPECTION_FREQUENCY', 'esriFieldTypeInteger', None),
    ('HAZARD_POTENTIAL', 'esriFieldTypeString', 1000), ('CONDITION_ASSESSMENT', 'esriFieldTypeString', 20),
    ('CONDITION_ASSESS_DATE', 'esriFieldTypeDate', 8), ('EAP_PREPARED', 'esriFieldTypeString', 255),
    ('EAP_LAST_REV_DATE', 'esriFieldTypeDate', 8), ('SE_ANNO_CAD_DATA', 'esriFieldTypeBlob', None),
    ('FORMER_NAMES', 'esriFieldTypeString', 500), ('IS_ASSOCIATED_STRUCTURE', 'esriFieldTypeString', 3),
    ('AIANNH', 'esriFieldTypeString', 100), ('FEDERALLY_REGULATED_DAM', 'esriFieldTypeString', 3),
    ('BUILT_UNDER_SEC_OF_AG_AUTH', 'esriFieldTypeString', 3), ('NRCS_WATERSHED_DAM_AUTH', 'esriFieldTypeString', 100),
    ('OPERATIONAL_STATUS', 'esriFieldTypeString', 100), ('OPERATIONAL_STATUS_DATE', 'esriFieldTypeDate', 8),
    ('WEBSITE_URL', 'esriFieldTypeString', 150),
)
EXPECTED_FIELD_NAMES = [item[0] for item in EXPECTED_FIELD_SIGNATURES]
ARROW_TYPES = {
    'esriFieldTypeOID': pa.int64(), 'esriFieldTypeInteger': pa.int64(),
    'esriFieldTypeDouble': pa.float64(), 'esriFieldTypeDate': pa.int64(),
    'esriFieldTypeString': pa.string(), 'esriFieldTypeBlob': pa.binary(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_json(body: bytes | str) -> object:
    def object_without_duplicate_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('usace_nid_duplicate_json_key')
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=object_without_duplicate_keys,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('invalid_json_constant')))


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_nlink)


def _capture_file_limit(name: str) -> int:
    return MAX_RECEIPT_BYTES if name.endswith('.receipt.json') else MAX_RESPONSE_BYTES


def _bounded_capture_file(path: Path, limit: int, expected_identity=None) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit
                or (expected_identity is not None and _stat_identity(before) != expected_identity)):
            raise ValueError('usace_nid_replay_artifact_unsafe_or_oversized')
        body = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if (len(body) != before.st_size or len(body) > limit
            or _stat_identity(before) != _stat_identity(after)):
        raise ValueError('usace_nid_replay_artifact_changed_or_incomplete')
    return body


def _bounded_capture_roster(root: Path, expected_names: set[str], *, include_manifest: bool = False) -> dict[str, bytes]:
    if not root.is_dir() or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError('usace_nid_replay_root_invalid')
    if any(not isinstance(name, str) or name in {'', '.', '..'} or Path(name).name != name
           for name in expected_names):
        raise ValueError('usace_nid_replay_manifest_roster_invalid')
    expected_total = set(expected_names)
    if include_manifest:
        expected_total.add('capture_manifest.json')
    entries = list(root.iterdir())
    if {path.name for path in entries} != expected_total:
        raise ValueError('usace_nid_replay_capture_roster_mismatch')
    stat_by_name = {}
    total_bytes = 0
    for name in sorted(expected_total):
        path = root / name
        info = path.lstat()
        limit = MAX_CAPTURE_MANIFEST_BYTES if name == 'capture_manifest.json' else _capture_file_limit(name)
        if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise ValueError('usace_nid_replay_artifact_unsafe_or_oversized:' + name)
        total_bytes += info.st_size
        stat_by_name[name] = _stat_identity(info)
    if total_bytes > MAX_CAPTURE_PHYSICAL_BYTES:
        raise ValueError('usace_nid_replay_aggregate_size_exceeded')
    return {name: _bounded_capture_file(root / name,
                 MAX_CAPTURE_MANIFEST_BYTES if name == 'capture_manifest.json' else _capture_file_limit(name),
                 stat_by_name[name]) for name in sorted(expected_total)}


def _validate_local_directory_path(path: Path, requested: str | None = None) -> None:
    if requested is not None and '..' in Path(requested).parts:
        raise ValueError('usace_nid_path_traversal_refused')
    if '..' in path.parts or any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError('usace_nid_symlink_path_refused')


def _query(params: Mapping[str, object]) -> str:
    return LAYER_URL + '/query?' + urlencode(params)


def _field_signature(field: Mapping[str, object]) -> tuple[object, object, object]:
    return field.get('name'), field.get('type'), field.get('length')


def _is_native_4269(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    codes = [value[key] for key in ('wkid', 'latestWkid') if key in value]
    return bool(codes) and all(type(code) is int and code == 4269 for code in codes)


def _validate(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: _strict_json(body) for name, (body, _) in captures.items()}
    service, layer = docs['service_before'], docs['layer_before']
    layers = service.get('layers') if isinstance(service, dict) else None
    if (not isinstance(layers, list) or len(layers) != 1
            or layers[0].get('id') != LAYER_ID or layers[0].get('name') != LAYER_NAME
            or layers[0].get('type') != 'Feature Layer'):
        raise ValueError('official_usace_nid_service_identity_drift')
    if (layer.get('id') != LAYER_ID or layer.get('name') != LAYER_NAME
            or layer.get('type') != 'Feature Layer' or layer.get('geometryType') != 'esriGeometryPoint'
            or layer.get('maxRecordCount') != PAGE_SIZE or layer.get('objectIdField') != 'OBJECTID'):
        raise ValueError('usace_nid_layer_identity_or_limit_drift')
    if layer.get('hasZ') is True or layer.get('hasM') is True:
        raise ValueError('usace_nid_geometry_dimensions_drift')
    extent = layer.get('extent')
    if not isinstance(extent, dict) or any(
            type(extent.get(key)) not in (int, float)
            or not math.isfinite(extent[key])
            or abs(extent[key] - expected) > 1e-6
            for key, expected in zip(('xmin', 'ymin', 'xmax', 'ymax'), EXPECTED_EXTENT)):
        raise ValueError('usace_nid_coverage_extent_drift')
    sr = layer.get('extent', {}).get('spatialReference', {})
    if not _is_native_4269(sr):
        raise ValueError('usace_nid_native_crs_drift')
    fields = layer.get('fields')
    if not isinstance(fields, list) or tuple(_field_signature(field) for field in fields) != EXPECTED_FIELD_SIGNATURES:
        raise ValueError('usace_nid_field_schema_drift')
    if docs['service_after'] != service or docs['layer_after'] != layer:
        raise ValueError('usace_nid_metadata_changed_during_capture')
    count = docs['count_before'].get('count')
    if type(count) is not int or count <= 0 or docs['count_after'].get('count') != count:
        raise ValueError('usace_nid_population_count_changed_or_invalid')
    rosters = [docs[name].get('objectIds') for name in ('ids_before', 'ids_seed', 'ids_after')]
    if any(not isinstance(ids, list) or any(type(i) is not int for i in ids)
           or len(ids) != count or len(set(ids)) != count for ids in rosters):
        raise ValueError('usace_nid_source_oid_roster_invalid')
    source_ids = sorted(rosters[0])
    if any(sorted(ids) != source_ids for ids in rosters[1:]):
        raise ValueError('usace_nid_source_oid_population_changed')
    page_names = [f'page_{i:05d}' for i in range((count + PAGE_SIZE - 1) // PAGE_SIZE)]
    expected_capture_names = {'service_before', 'layer_before', 'count_before', 'ids_before', 'ids_seed',
                              'service_after', 'layer_after', 'count_after', 'ids_after', *page_names}
    if set(captures) != expected_capture_names:
        raise ValueError('usace_nid_capture_roster_incomplete_or_unexpected')
    features = []
    invalid_geometry_count = 0
    null_geometry_count = 0
    for offset, name in enumerate(page_names):
        page = docs[name]
        if 'error' in page or page.get('exceededTransferLimit') is True:
            raise ValueError('usace_nid_page_error_or_transfer_limit')
        page_sr = page.get('spatialReference', {})
        if not _is_native_4269(page_sr):
            raise ValueError('usace_nid_page_native_crs_drift')
        rows = page.get('features')
        expected_ids = source_ids[offset * PAGE_SIZE:(offset + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError('usace_nid_page_population_incomplete')
        if [row.get('attributes', {}).get('OBJECTID') for row in rows] != expected_ids:
            raise ValueError('usace_nid_page_oid_order_or_completeness_drift')
        for row in rows:
            attrs = row.get('attributes')
            if not isinstance(attrs, dict) or list(attrs) != EXPECTED_FIELD_NAMES:
                raise ValueError('usace_nid_record_attribute_roster_drift')
            geometry = row.get('geometry')
            if geometry is None:
                null_geometry_count += 1
            else:
                valid = (isinstance(geometry, dict) and set(geometry) <= {'x', 'y', 'spatialReference'}
                         and all(type(geometry.get(k)) in (int, float) and math.isfinite(geometry[k])
                                 for k in ('x', 'y'))
                         and -180 <= geometry.get('x', 999) <= 180
                         and -90 <= geometry.get('y', 999) <= 90)
                geom_sr = geometry.get('spatialReference', {}) if isinstance(geometry, dict) else {}
                valid = valid and (not geom_sr or _is_native_4269(geom_sr))
                if not valid:
                    invalid_geometry_count += 1
        features.extend(rows)
    features.sort(key=lambda row: row['attributes']['OBJECTID'])
    record_sha = _sha(_canonical(features))
    geometry_sha = _sha(_canonical([[row['attributes']['OBJECTID'], row.get('geometry')] for row in features]))
    field_schema = [_field_signature(field) for field in fields]
    audit = {
        'source_feature_count': count,
        'source_objectid_sha256': _sha(_canonical(source_ids)),
        'source_record_sha256': record_sha,
        'source_geometry_sha256': geometry_sha,
        'source_schema_sha256': _sha(_canonical(field_schema)),
        'source_fields': field_schema,
        'source_crs': 'EPSG:4269',
        'source_spatial_reference': sr,
        'source_extent': extent,
        'null_geometry_count': null_geometry_count,
        'invalid_geometry_count': invalid_geometry_count,
        'source_release_date': None,
        'release_date_unknown_reason': 'The USACE service does not expose a verified release date; retrieval/metadata times are not release dates.',
        'nid_scope_note': 'Current USACE NID selection as published; USACE says qualifying dams are not yet all represented. This capture does not claim a census of all dams.',
        'historical_crosswalk': 'DataLumos project 239785 V1 not yet compared to the current producer schema.',
    }
    return fields, features, audit


def _parquet(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns, schema_fields = {}, []
    for field in fields:
        name, kind = field['name'], field['type']
        if kind not in ARROW_TYPES:
            raise ValueError('usace_nid_unsupported_source_type:' + str(kind))
        if name in {'geometry', 'source_geometry'}:
            raise ValueError('usace_nid_reserved_source_column')
        typ = ARROW_TYPES[kind]
        values = [row['attributes'].get(name) for row in features]
        if kind == 'esriFieldTypeBlob':
            try:
                values = [None if value is None else base64.b64decode(value, validate=True) for value in values]
            except (TypeError, ValueError) as exc:
                raise ValueError('usace_nid_blob_encoding_drift') from exc
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(name, typ, nullable=field.get('nullable', True)))
    native_type = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64()),
                             pa.field('spatial_reference', pa.string()), pa.field('raw_json', pa.string())])
    native, wkb = [], []
    for row in features:
        geom = row.get('geometry')
        if geom is None:
            native.append(None); wkb.append(None)
            continue
        if not isinstance(geom, dict) or set(geom) - {'x', 'y', 'spatialReference'}:
            raise ValueError('usace_nid_geometry_shape_drift')
        sr = geom.get('spatialReference', {'wkid': 4269})
        if (type(geom.get('x')) not in (int, float) or type(geom.get('y')) not in (int, float)
                or not math.isfinite(geom['x']) or not math.isfinite(geom['y'])
                or not -180 <= geom['x'] <= 180 or not -90 <= geom['y'] <= 90
                or (sr and not _is_native_4269(sr))):
            raise ValueError('usace_nid_geometry_value_or_crs_drift')
        native.append({'x': geom['x'], 'y': geom['y'], 'spatial_reference': _canonical(sr).decode(),
                       'raw_json': _canonical(geom).decode()})
        wkb.append(Point(geom['x'], geom['y']).wkb)
    columns['source_geometry'] = pa.array(native, type=native_type)
    schema_fields.append(pa.field('source_geometry', native_type, nullable=True))
    columns['geometry'] = pa.array(wkb, type=pa.binary())
    schema_fields.append(pa.field('geometry', pa.binary(), nullable=True))
    geo = {'version': '1.1.0', 'primary_column': 'geometry', 'columns': {'geometry': {
        'encoding': 'WKB', 'geometry_types': ['Point'], 'crs': pyproj.CRS.from_epsg(4269).to_json_dict(),
        'edges': 'planar'}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b'geo': _canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression='zstd')
    return sink.getvalue().to_pybytes()


def _capture_urls(ids: list[int] | None = None) -> dict[str, str]:
    url_ids = _query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
    urls = {
        'service_before': SERVICE_URL + '?f=json', 'layer_before': LAYER_URL + '?f=json',
        'count_before': _query({'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}),
        'ids_before': url_ids, 'ids_seed': url_ids,
        'service_after': SERVICE_URL + '?f=json', 'layer_after': LAYER_URL + '?f=json',
        'count_after': _query({'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}),
        'ids_after': url_ids,
    }
    if ids is not None:
        for offset in range(0, len(ids), PAGE_SIZE):
            page_ids = ids[offset:offset + PAGE_SIZE]
            name = f'page_{offset // PAGE_SIZE:05d}'
            params = {'objectIds': ','.join(map(str, page_ids)), 'outFields': '*', 'returnGeometry': 'true',
                      'outSR': '4269', 'orderByFields': 'OBJECTID ASC', 'f': 'json'}
            urls[name] = _query(params)
    return urls


def _read_replay(capture_root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    manifest_path = capture_root / 'capture_manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError('usace_nid_replay_manifest_missing')
    manifest_body = _bounded_capture_file(manifest_path, MAX_CAPTURE_MANIFEST_BYTES)
    manifest = _strict_json(manifest_body)
    if not isinstance(manifest, dict):
        raise ValueError('usace_nid_replay_manifest_invalid')
    receipts = manifest.get('receipts')
    files = manifest.get('files')
    if manifest.get('complete') is not True or not isinstance(receipts, dict) or not isinstance(files, dict):
        raise ValueError('usace_nid_replay_manifest_invalid')
    if any(not isinstance(name, str) or name in {'', '.', '..'} or Path(name).name != name for name in receipts):
        raise ValueError('usace_nid_replay_name_invalid')
    bound_capture_files = {f'{name}{suffix}' for name in receipts
                           for suffix in ('.body', '.wire', '.receipt.json')}
    if set(files) != bound_capture_files:
        raise ValueError('usace_nid_replay_file_receipt_roster_mismatch')
    # Stat every body, wire and receipt, including their aggregate size, before
    # reading any response artifact; the manifest itself was read under its own cap.
    bounded_files = _bounded_capture_roster(capture_root, set(files), include_manifest=True)
    if bounded_files['capture_manifest.json'] != manifest_body:
        raise ValueError('usace_nid_replay_manifest_changed')
    # Verify descriptor-bound body/wire/receipt bytes before decoding source JSON.
    for filename, descriptor in files.items():
        body = bounded_files[filename]
        if descriptor != {'bytes': len(body), 'sha256': _sha(body)}:
            raise ValueError('usace_nid_replay_manifest_blob_mismatch')
    captures = {}
    for name, receipt in receipts.items():
        if not isinstance(name, str) or name in {'', '.', '..'} or Path(name).name != name:
            raise ValueError('usace_nid_replay_name_invalid')
        body, wire = bounded_files[f'{name}.body'], bounded_files[f'{name}.wire']
        stored_receipt = _strict_json(bounded_files[f'{name}.receipt.json'])
        if (stored_receipt != receipt or receipt.get('complete_body') is not True or receipt.get('status') != 200
                or receipt.get('method') != 'GET' or receipt.get('final_url') != receipt.get('url')
                or receipt.get('bytes') != len(body) or receipt.get('sha256') != _sha(body)
                or receipt.get('wire_bytes') != len(wire) or receipt.get('wire_sha256') != _sha(wire)):
            raise ValueError('usace_nid_replay_receipt_invalid:' + name)
        captures[name] = (body, receipt)
    return captures


def _validate_capture_identity(captures: Mapping[str, tuple[bytes, Mapping[str, object]]],
                               expected_urls: Mapping[str, str]) -> None:
    if set(expected_urls) != set(captures):
        raise ValueError('usace_nid_replay_capture_set_mismatch')
    for name, (_body, receipt) in captures.items():
        if (receipt.get('method') != 'GET' or receipt.get('status') != 200
                or receipt.get('complete_body') is not True
                or receipt.get('url') != expected_urls[name]
                or receipt.get('final_url') != expected_urls[name]):
            raise ValueError('usace_nid_capture_request_identity_mismatch:' + name)


def _candidate_result(version: str, audit: Mapping[str, object], prefix: str, reused: bool,
                      capture_directory: str | None = None) -> dict[str, object]:
    result = {'dataset_slug': DATASET_SLUG, 'file_slug': FILE_SLUG, 'version': version,
              'feature_count': audit['source_feature_count'], 'source_null_geometry_count': audit['null_geometry_count'],
              'source_invalid_geometry_count': audit['invalid_geometry_count'], 'candidate_ready': prefix + '/metadata/candidate_ready.json',
              'source_release_accepted': False, 'freshness_accepted': False, 'schema_contract_accepted': False,
              'id_contract_accepted': False, 'rights_accepted': False, 'mapping_approved': False,
              'release_accepted': False, 'reused': reused}
    if capture_directory is not None:
        result['capture_directory'] = capture_directory
    return result


def capture_national_inventory_of_dams(storage: StagingStorageResource, *, version: str,
                                       capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError('explicit_local_staging_resource_required')
    staging_root = Path(storage.local_dir).absolute()
    _validate_local_directory_path(staging_root, storage.local_dir)
    staging_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging_root).free < MIN_DISK_FREE_BYTES:
        raise ValueError('disk_floor_before_acquisition')
    capture_root = (Path(capture_directory).absolute() if capture_directory else
                    Path('/tmp/hifld-native-reconstruction/165/captures') / f'{version}-{uuid4().hex}')
    _validate_local_directory_path(capture_root, capture_directory)
    replay = capture_directory is not None and (capture_root / 'capture_manifest.json').is_file()
    if not replay:
        capture_root.mkdir(parents=True, exist_ok=False)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}

    def get(client: httpx.Client, name: str, url: str):
        def reserve(progress):
            existing = 0
            for path in capture_root.iterdir():
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError('usace_nid_capture_path_unsafe')
                if path.name not in {f'{name}.body', f'{name}.wire'}:
                    existing += info.st_size
            required = existing + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes
            if required > MAX_CAPTURE_PHYSICAL_BYTES:
                raise ValueError('usace_nid_total_capture_budget_exceeded')
            if shutil.disk_usage(capture_root).free < MIN_DISK_FREE_BYTES:
                raise ValueError('disk_floor_before_capture_copy')
        return capture_get(client, url, capture_root, name, max_bytes=MAX_RESPONSE_BYTES,
                           max_wire_block_bytes=DECODE_BLOCK_BYTES,
                           include_observed_wire=True, reserve=reserve)

    try:
        if replay:
            captures = _read_replay(capture_root)
        else:
            with httpx.Client(follow_redirects=False, timeout=60,
                              headers={'User-Agent': 'hifld-next-datasets/nid-draft', 'Accept-Encoding': 'identity'}) as client:
                ids_url = _query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
                seed_body, seed_receipt = get(client, 'ids_seed', ids_url)
                seed_ids = _strict_json(seed_body).get('objectIds')
                if (not isinstance(seed_ids, list) or any(type(i) is not int for i in seed_ids)
                        or len(set(seed_ids)) != len(seed_ids)):
                    raise ValueError('usace_nid_seed_oid_roster_invalid')
                captures['ids_seed'] = (seed_body, seed_receipt)
                for name in ('service_before', 'layer_before', 'count_before', 'ids_before'):
                    captures[name] = get(client, name, _capture_urls()[name])
                count = _strict_json(captures['count_before'][0]).get('count')
                if type(count) is not int or count != len(seed_ids):
                    raise ValueError('usace_nid_seed_count_mismatch')
                ids = sorted(seed_ids)
                urls = _capture_urls(ids)
                for name in sorted(k for k in urls if k.startswith('page_')):
                    captures[name] = get(client, name, urls[name])
                for name in ('service_after', 'layer_after', 'count_after', 'ids_after'):
                    captures[name] = get(client, name, urls[name])
        fields, features, audit = _validate(captures)
        expected_urls = _capture_urls(sorted(_strict_json(captures['ids_seed'][0])['objectIds']))
        _validate_capture_identity(captures, expected_urls)
        retained_names = {f'{name}{suffix}' for name in captures
                          for suffix in ('.body', '.wire', '.receipt.json')}
        retained_bodies = _bounded_capture_roster(capture_root, retained_names)
        retained_files = {name: {'bytes': len(body), 'sha256': _sha(body)}
                          for name, body in retained_bodies.items()}
        manifest_body = _canonical({'complete': True, 'source_feature_count': audit['source_feature_count'],
                                    'receipts': {name: receipt for name, (_, receipt) in sorted(captures.items())},
                                    'files': retained_files}) + b'\n'
        if len(manifest_body) > MAX_CAPTURE_MANIFEST_BYTES or sum(len(body) for body in retained_bodies.values()) + len(manifest_body) > MAX_CAPTURE_PHYSICAL_BYTES:
            raise ValueError('usace_nid_total_capture_budget_exceeded')
        with (capture_root / 'capture_manifest.json').open('xb') as stream:
            stream.write(manifest_body)
    except Exception as exc:
        if not replay:
            (capture_root / 'capture_failure.json').write_bytes(_canonical({'complete': False, 'error_code': type(exc).__name__,
                                                                              'captured_names': sorted(captures)}) + b'\n')
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    capture_manifest_body = _bounded_capture_file(capture_root / 'capture_manifest.json', MAX_CAPTURE_MANIFEST_BYTES)
    capture_manifest = _strict_json(capture_manifest_body)
    sealed_files = capture_manifest.get('files') if isinstance(capture_manifest, dict) else None
    if not isinstance(sealed_files, dict):
        raise ValueError('usace_nid_capture_manifest_files_missing')

    def read_sealed_file(filename: str) -> bytes:
        if filename not in sealed_files:
            raise ValueError('usace_nid_capture_manifest_file_missing:' + filename)
        body = _bounded_capture_file(capture_root / filename, _capture_file_limit(filename))
        if sealed_files[filename] != {'bytes': len(body), 'sha256': _sha(body)}:
            raise ValueError('usace_nid_capture_manifest_file_changed:' + filename)
        return body

    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    rel = f'geoparquet/{DATASET_SLUG}.parquet'
    candidate = _parquet(fields, features)
    record = {'bytes': len(candidate), 'sha256': _sha(candidate)}
    manifest = {
        'schema_version': 1, 'approved': False, 'title': LAYER_NAME,
        'description': ('Official USACE NID producer FeatureServer layer 0. Capture preserves all 81 source fields, source OBJECTID values, and native NAD83 point coordinates. NID is criterion-bounded and USACE says not all qualifying dams are yet included. DataLumos V1 crosswalk, historical compatibility, release date, rights, and approval remain pending.'),
        'publisher': PUBLISHER, 'agency': 'U.S. Army Corps of Engineers', 'source_url': LAYER_URL,
        'license': None, 'source_modified': None, 'date_issued': None, 'date_modified': None,
        'processor': 'HIFLD Next', 'code_commit': identity['code_commit'],
        'inputs': [{'input_id': name, 'producer': PUBLISHER, 'host': urlsplit(receipt['url']).hostname,
                    'role': 'producer_publication', 'canonical_url': SERVICE_URL if name.startswith('service_') else LAYER_URL,
                    'retrieval_url': receipt['url'], 'observed_final_url': receipt['final_url'],
                    'retrieved_at': receipt['observed_at'], 'http_status': receipt['status'], 'bytes': receipt['bytes'],
                    'sha256': receipt['sha256'], 'license': 'pending_rights_review', 'attribution': 'USACE credit required; terms review pending.',
                    'release_date': None, 'release_date_unknown_reason': audit['release_date_unknown_reason']}
                   for name, (_, receipt) in sorted(captures.items())],
        'transformations': [{'operation': 'native_esri_json_to_geoparquet_candidate',
                             'parameters': {'source_layer': LAYER_ID, 'source_fields': 'all 81 source attributes retained in producer order and physical types',
                                            'source_identity': 'source OBJECTID retained; NIDID values preserved but uniqueness/stability not asserted',
                                            'geometry': 'native EPSG:4269 point coordinates retained in source_geometry and WKB; no reprojection or repair',
                                            'candidate_status': 'unapproved; historical contract, freshness, rights, and release approval pending'}}],
        'lineage': 'USACE National Inventory of Dams producer service directly linked from Esri Federal Data NGDA distribution item a4c195b7a6b74f278ff43e5d60c6915d. Esri cache is not used as the capture authority.',
        'prebuilt_output_policy': {'schema_version': 1, 'formats': {'geoparquet': {'reason': 'Faithful full-source draft; acceptance unresolved.', 'objects': [{'path': rel, **record}]}}},
        'unsupported_output_formats': {'geopackage': 'No accepted historical field contract.', 'pmtiles': 'No approved display mapping.'},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    version_key = content_derived_version(audit['source_record_sha256'], audit['source_geometry_sha256'],
                                          audit['source_schema_sha256'], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version_key)
    outputs = {rel: candidate, 'metadata/source_manifest.json': _canonical(manifest) + b'\n',
               'metadata/source_audit.json': _canonical(audit) + b'\n',
               'metadata/processing_identity.json': _canonical(identity) + b'\n',
               'metadata/candidate_ready.json': _canonical({'approved': False, 'version': version_key,
                   'objects': {rel: record}, 'source_release_accepted': False, 'freshness_accepted': False,
                   'schema_contract_accepted': False, 'id_contract_accepted': False, 'rights_accepted': False,
                   'mapping_approved': False, 'release_accepted': False}) + b'\n'}
    for name, (body, receipt) in captures.items():
        outputs[f'raw/captures/{name}.body'] = read_sealed_file(f'{name}.body')
        outputs[f'raw/captures/{name}.wire'] = read_sealed_file(f'{name}.wire')
        outputs[f'raw/captures/{name}.receipt.json'] = read_sealed_file(f'{name}.receipt.json')
    outputs['metadata/capture_manifest.json'] = capture_manifest_body
    additional_bytes = sum(len(body) for body in outputs.values())
    if shutil.disk_usage(staging_root).free < MIN_DISK_FREE_BYTES + additional_bytes:
        raise ValueError('disk_floor_before_candidate_staging')
    state_key = f'_construction-state/{DATASET_SLUG}/{version_key}.json'
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = _strict_json(load_first_construction(storage, state_key, version_key, fingerprint))
        payload = bundle.get('payload')
        if not isinstance(payload, dict):
            raise ValueError('usace_nid_cached_first_candidate_invalid')
        verify_cached_candidate(storage, repo_root, state_key, prefix, version_key, fingerprint,
                                identity, audit['source_record_sha256'], audit['source_schema_sha256'],
                                payload.get('objects', {}), payload.get('capture_receipts', {}))
        return _candidate_result(version_key, audit, prefix, True)
    for name, body in outputs.items():
        write_immutable(storage, f'{prefix}/{name}', body)
    descriptors = {name: {'bytes': len(body), 'sha256': _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f'{prefix}/{name}'; snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor['bytes'] or storage.sha256_key(key) != descriptor['sha256'] or storage.object_snapshot(key) != snap:
            raise ValueError('usace_nid_candidate_readback_failed:' + name)
    payload = {'processing_identity': identity, 'source_record_sha256': audit['source_record_sha256'],
               'source_schema_sha256': audit['source_schema_sha256'],
               'capture_receipts': {name: receipt for name, (_, receipt) in captures.items()}, 'objects': descriptors}
    seal_ready_candidate(storage, state_key, version_key, fingerprint, payload)
    validate_construction_code_identity(repo_root, identity['code_commit'], identity['files'])
    return _candidate_result(version_key, audit, prefix, False, str(capture_root))
