"""Pinned opt-in RFC7946 catalog without Fiona/Pandas type homogenization.

The complete input has a 128MiB encoded-byte ceiling. The standard-library JSON
tree may consume substantially more memory; this is not a hard RSS quota.
Dictionary examples are bounded, while presence/types/geometry scan every row.
No source data is rewritten and no schema/source/release decision is made.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import tempfile
from collections import Counter
from pathlib import Path

import shapely
from shapely.geometry import shape
from pyproj import CRS
from shapely.errors import GEOSException

from dagster_hifld.literal_geojson_policy import LITERAL_GEOJSON_FIELD, literal_geojson_spec
from dagster_hifld.native_geoparquet_catalog import (
    _safe_local_key, _manifest_snapshots, _check_manifests,
)
from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_PRECEDENCE
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file

SAMPLE_ROWS = 5000
EXAMPLE_BYTES = 64 * 1024
TOTAL_EXAMPLE_BYTES = 4 * 1024 * 1024
MAX_PROPERTIES = 4096


def _encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True, allow_nan=False).encode()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('literal_geojson_duplicate_JSON_member')
        result[key] = value
    return result


class _JSONFloat(float):
    """Keep exact decimal/exponent lexemes alongside decoded binary64 examples."""
    def __new__(cls, text):
        value = super().__new__(cls, text)
        value.lexeme = text
        return value


def _float(text):
    value = _JSONFloat(text)
    if not math.isfinite(value):
        raise ValueError('literal_geojson_nonfinite_number')
    return value


def _kind(value):
    if isinstance(value, _JSONFloat):
        return 'float'
    return {type(None): 'null', bool: 'boolean', int: 'integer', float: 'float',
            str: 'string', list: 'array', dict: 'object'}[type(value)]


def _typed(value):
    kind = _kind(value)
    if isinstance(value, _JSONFloat):
        return {'jsonType': kind, 'numericLexeme': value.lexeme, 'decodedBinary64': float(value)}
    if type(value) is list:
        return {'jsonType': kind, 'value': [_typed(item) for item in value]}
    if type(value) is dict:
        return {'jsonType': kind, 'value': {key: _typed(item) for key,item in value.items()}}
    return {'jsonType': kind, 'value': value}


def resolve_literal_metadata(storage, prefix, supplied=None):
    """Actual authored profile wins; a caller cannot erase or replace its pin."""
    if (type(prefix) is not str or re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*', prefix) is None):
        raise ValueError('literal_geojson_unsafe_version_prefix')
    dataset, file, version = prefix.split('/')
    if storage.use_local or not storage.bucket:
        for key in [f'{dataset}/metadata/source_manifest.json', f'{dataset}/{file}/metadata/source_manifest.json',
                    f'{prefix}/metadata/source_manifest.json']:
            _safe_local_key(storage, storage._ensure_prefixed(key), required=False)
    actual = load_resolved_source_manifest(storage, dataset, file, version, max_layer_bytes=2*1024*1024).metadata
    spec = literal_geojson_spec(actual)
    proposed = literal_geojson_spec(supplied or {})
    if proposed is not None and (spec is None or _encode(proposed) != _encode(spec)):
        raise ValueError('literal_geojson_supplied_profile_not_authored')
    if spec is None:
        return None
    if not actual.get('manifest_layers'):
        raise ValueError('literal_geojson_requires_authored_manifest_pin')
    return actual


def _roster(storage, prefix):
    directory = storage._ensure_prefixed(prefix + '/geojson/')
    return {key for key in storage.list_prefix(prefix + '/geojson') if key.startswith(directory)}


def _selection(storage, prefix, metadata):
    spec = literal_geojson_spec(metadata)
    key = storage._ensure_prefixed(prefix + '/' + spec['path'])
    # Explicit profile selects its canonical GeoJSON. It cannot mask a higher
    # precedence source, an ambiguous sibling or a legacy competing source.
    before = set(CANONICAL_SOURCE_FORMAT_PRECEDENCE[:CANONICAL_SOURCE_FORMAT_PRECEDENCE.index('geojson')])
    root = storage._ensure_prefixed(prefix).rstrip('/') + '/'
    if any(k.startswith(root) and k[len(root):].split('/', 1)[0] in before
           for k in storage.list_prefix(prefix)):
        raise ValueError('literal_geojson_higher_precedence_canonical_source_present')
    if _roster(storage, prefix) != {key}:
        raise ValueError('literal_geojson_complete_object_roster_changed')
    if storage.use_local or not storage.bucket:
        _safe_local_key(storage, key)
    snapshot = storage.object_snapshot(key)
    if snapshot is None or snapshot.size != spec['bytes']:
        raise ValueError('literal_geojson_pinned_size_changed')
    return spec, key, snapshot


def summarize_literal_geojson(storage, prefix, name, metadata):
    spec, key, snapshot = _selection(storage, prefix, metadata)
    manifests = _manifest_snapshots(storage, metadata)
    with tempfile.TemporaryDirectory(prefix='hifld-literal-geojson-') as temporary:
        if storage.use_local or not storage.bucket:
            path = _safe_local_key(storage, key)
        else:
            path = Path(temporary) / 'input.geojson'
            storage.download_key_to(key, path)
        def unchanged():
            if storage.use_local or not storage.bucket:
                _safe_local_key(storage, key)
            _check_manifests(storage, manifests)
            if (path.stat().st_size != spec['bytes'] or sha256_file(path) != spec['sha256']
                    or storage.object_snapshot(key) != snapshot or _roster(storage, prefix) != {key}):
                raise ValueError('literal_geojson_pinned_bytes_or_roster_changed')
        unchanged()
        with path.open('rb') as stream:
            body = stream.read(spec['bytes'] + 1)
        if len(body) != spec['bytes'] or hashlib.sha256(body).hexdigest() != spec['sha256']:
            raise ValueError('literal_geojson_read_pin_changed')
        try:
            data = json.loads(body.decode('utf-8'), object_pairs_hook=_pairs, parse_float=_float,
                              parse_constant=lambda value: (_ for _ in ()).throw(ValueError('literal_geojson_nonfinite_number')))
        except (UnicodeError, RecursionError, json.JSONDecodeError) as exc:
            raise ValueError('literal_geojson_malformed_or_truncated_JSON') from exc
        del body
        try:
            quality, dictionary = _summarize(data, name, spec)
        except (UnicodeError, RecursionError) as exc:
            raise ValueError('literal_geojson_unsupported_Unicode_or_nesting') from exc
        unchanged()
        return quality, dictionary


def _geometry(value):
    if type(value) is not dict:
        raise ValueError('literal_geometry_must_be_object')
    kind = value.get('type')
    if kind == 'GeometryCollection':
        if set(value) != {'type', 'geometries'} or type(value['geometries']) is not list:
            raise ValueError('literal_geometry_collection_members')
        for item in value['geometries']:
            _geometry(item)
        return
    depths = {'Point': 0, 'MultiPoint': 1, 'LineString': 1, 'MultiLineString': 2, 'Polygon': 2, 'MultiPolygon': 3}
    if kind not in depths or set(value) != {'type', 'coordinates'}:
        raise ValueError('literal_geometry_type_or_members')
    def positions(coordinates, depth):
        if type(coordinates) is not list:
            raise ValueError('literal_coordinates_must_be_arrays')
        if not coordinates:
            return
        if depth:
            for item in coordinates:
                positions(item, depth - 1)
        elif len(coordinates) not in (2, 3) or any(type(x) not in (int, float, _JSONFloat) or not math.isfinite(x) for x in coordinates):
            raise ValueError('literal_positions_require_finite_numbers')
    positions(value['coordinates'], depths[kind])
    polygons = [value['coordinates']] if kind == 'Polygon' else value['coordinates'] if kind == 'MultiPolygon' else []
    for polygon in polygons:
        for ring in polygon:
            if len(ring) < 4 or ring[0] != ring[-1]:
                raise ValueError('literal_polygon_ring_not_closed')


def _summarize(data, name, spec):
    root_metadata = spec.get('root_metadata', {})
    if (type(data) is not dict or data.get('type') != 'FeatureCollection'
            or set(data) - {'type', 'features', 'bbox', *root_metadata}
            or any(data.get(key) != value for key, value in root_metadata.items())
            or type(data.get('features')) is not list):
        raise ValueError('literal_geojson_requires_RFC7946_FeatureCollection_no_legacy_CRS_or_foreign_root_members')
    if 'crs' in root_metadata:
        try:
            declared_crs = CRS.from_user_input(root_metadata['crs']['properties']['name'])
        except Exception as exc:
            raise ValueError('literal_geojson_pinned_CRS_is_not_recognized') from exc
        if (not declared_crs.is_geographic or not declared_crs.axis_info
                or any(axis.unit_name.lower() != 'degree' for axis in declared_crs.axis_info)):
            raise ValueError('literal_geojson_pinned_CRS_must_be_geographic_degrees')
        crs = declared_crs.to_json_dict()
        crs_source = 'producer_declared_CRS_pinned_in_source_manifest'
        coordinate_order = 'source_serialized_x_y_retained_without_axis_order_reinterpretation_or_swap'
    else:
        crs = CRS.from_user_input('OGC:CRS84').to_json_dict()
        crs_source = 'GeoJSON_RFC7946_normative_OGC_CRS84_interpretation_not_producer_authority_or_acceptance'
        coordinate_order = 'RFC7946_longitude_latitude'
    rows = data['features']; fields = {}; ids = {'present': 0, 'null': 0, 'types': Counter(), 'examples': []}
    properties_null = 0; nulls = 0; empty = 0; invalid = 0; unparseable = 0; kinds = set(); bounds = None
    examples_bytes = 0; property_hash = hashlib.sha256(); id_hash = hashlib.sha256(); property_null_hash = hashlib.sha256()
    def observe(state, value, index):
        nonlocal examples_bytes
        state['present'] += 1; state['types'][_kind(value)] += 1
        if value is None:
            state['null'] += 1
        if index < SAMPLE_ROWS and len(state['examples']) < 5:
            example = {'jsonType': _kind(value), 'value': value, 'nativeJSONValue': _typed(value)}
            encoded = _encode(example)
            if len(encoded) <= EXAMPLE_BYTES and examples_bytes + len(encoded) <= TOTAL_EXAMPLE_BYTES:
                if all(_encode(existing) != _encode(example) for existing in state['examples']):
                    state['examples'].append(example); examples_bytes += len(encoded)
    for index, feature in enumerate(rows):
        if (type(feature) is not dict or feature.get('type') != 'Feature'
                or not {'type', 'properties', 'geometry'} <= set(feature)
                or set(feature) - {'type', 'properties', 'geometry', 'id', 'bbox'}):
            raise ValueError('literal_geojson_invalid_or_unsupported_Feature_members')
        props = feature['properties']
        if props is None:
            properties_null += 1; props = {}
        if type(props) is not dict:
            raise ValueError('literal_geojson_properties_must_be_object_or_null')
        property_hash.update(_encode(_typed(props)) + b'\n')
        property_null_hash.update(b'null\n' if feature['properties'] is None else b'object\n')
        for field, value in props.items():
            if field not in fields:
                if len(fields) >= MAX_PROPERTIES:
                    raise ValueError('literal_geojson_property_count_limit')
                fields[field] = {'present': 0, 'null': 0, 'types': Counter(), 'examples': []}
            observe(fields[field], value, index)
        id_hash.update(_encode({'present': 'id' in feature, **({'value': _typed(feature['id'])} if 'id' in feature else {})}) + b'\n')
        if 'id' in feature:
            if type(feature['id']) not in (str, int, float, _JSONFloat, type(None)):
                raise ValueError('literal_geojson_unsupported_Feature_id_type')
            observe(ids, feature['id'], index)
        raw = feature['geometry']
        if raw is None:
            nulls += 1
            if 'bbox' in feature:raise ValueError('literal_geojson_bbox_on_null_geometry')
            continue
        try:
            _geometry(raw); geometry = shape(raw)
            if geometry.is_empty:
                empty += 1
                if 'bbox' in feature:raise ValueError('literal_bbox_on_empty_geometry')
                continue
            current = list(geometry.bounds)
            if len(current) != 4 or not all(math.isfinite(x) for x in current):
                raise ValueError('literal_nonfinite_geometry')
        except (GEOSException, ValueError, TypeError, OverflowError, RecursionError):
            invalid += 1; unparseable += 1
            continue
        if not shapely.is_valid(geometry):
            invalid += 1
        kinds.add(geometry.geom_type)
        if 'bbox' in feature and _bbox(feature['bbox']) != current:
            raise ValueError('literal_geojson_Feature_bbox_mismatch')
        bounds = current if bounds is None else [min(bounds[0],current[0]), min(bounds[1],current[1]), max(bounds[2],current[2]), max(bounds[3],current[3])]
    if 'bbox' in data and (_bbox(data['bbox']) != bounds or unparseable):
        raise ValueError('literal_geojson_population_bbox_mismatch')
    total = len(rows)
    def descriptor(state):
        return {'statisticsScope': 'full_population', 'presentCount': state['present'],
            'numMissingValues': total - state['present'], 'numNullValues': state['null'],
            'nativeJSONTypeCounts': dict(sorted(state['types'].items())),
            'nativeJSONTypes': sorted(state['types']), 'typedExamples': state['examples'],
            'examplesScope': 'first_5000_features_with_byte_limits'}
    columns = [{'name': field, 'type': 'json', **descriptor(state)} for field,state in fields.items()]
    quality = {'version': 'v1', 'feature_count': total, 'bounds': bounds, 'geometry_type': next(iter(kinds)) if len(kinds)==1 else 'Mixed' if kinds else None,
        'invalid_geometry_count': invalid, 'null_geometry_count': nulls, 'empty_geometry_count': empty,
        'unparseable_geometry_count': unparseable, 'quality_check_passed': total > 0 and not(nulls or empty or invalid),
        'columns_hash': hashlib.sha256(_encode(list(fields))).hexdigest(),
        'catalog_mode': 'literal_geojson_full_population', 'spatial_status': 'spatial' if kinds else 'all_null_geometry',
        'sampled_feature_count': min(total, SAMPLE_ROWS), 'population_quality_scope': 'all_rows',
        'native_crs': crs, 'crs_source': crs_source, 'coordinate_order': coordinate_order,
        'source_root_metadata': root_metadata,
        'native_geometry_member': 'Feature.geometry', 'properties_null_count': properties_null,
        'literal_input': dict(spec)}
    dictionary = {'name': name, 'columns': columns, 'featureCount': total,
        'sampledFeatureCount': min(total,SAMPLE_ROWS), 'nativeGeometryMember': 'Feature.geometry',
        'featureIDMember': {'member':'Feature.id', **descriptor(ids)},
        'propertiesNullCount': properties_null, 'exampleBytes': examples_bytes,
        'nativePropertyPopulationSHA256': property_hash.hexdigest(),
        'nativePropertiesContainerPopulationSHA256': property_null_hash.hexdigest(),
        'nativeFeatureIDPopulationSHA256': id_hash.hexdigest(), 'literalInput': dict(spec),
        'sourceRootMetadata': root_metadata, 'nativeCRS': crs, 'coordinateOrder': coordinate_order,
        'numericInterpretation': 'Typed float examples expose decoded binary64; nativeJSONValue retains exact fractional/exponent lexemes, including nested values. Population hashes use those typed lexemes. Original bytes remain pinned and unchanged.',
        'schemaInterpretation': 'Literal JSON types and field presence; no homogeneous Arrow/SQL schema or mapping accepted.'}
    return quality, dictionary


def _bbox(value):
    if (type(value) is not list or len(value) != 4
            or any(type(v) not in (int,float,_JSONFloat) or not math.isfinite(v) for v in value)
            or value[0] > value[2] or value[1] > value[3]):
        raise ValueError('literal_geojson_requires_finite_2D_bbox')
    return value
