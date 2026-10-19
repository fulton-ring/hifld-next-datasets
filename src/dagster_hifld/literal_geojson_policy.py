"""Explicit integrity profile for a literal JSON catalog, never acceptance."""
from __future__ import annotations

import re
from pathlib import PurePosixPath

LITERAL_GEOJSON_FIELD = 'literal_geojson_catalog'
MAX_LITERAL_GEOJSON_BYTES = 128 * 1024 * 1024


def literal_geojson_spec(metadata: dict) -> dict | None:
    if LITERAL_GEOJSON_FIELD not in metadata:
        return None
    spec = metadata[LITERAL_GEOJSON_FIELD]
    if (type(spec) is not dict or set(spec) not in (
                {'schema_version', 'path', 'bytes', 'sha256'},
                {'schema_version', 'path', 'bytes', 'sha256', 'root_metadata'})
            or type(spec['schema_version']) is not int or spec['schema_version'] != 1
            or type(spec['bytes']) is not int or not 0 < spec['bytes'] <= MAX_LITERAL_GEOJSON_BYTES
            or type(spec['sha256']) is not str or re.fullmatch('[0-9a-f]{64}', spec['sha256']) is None):
        raise ValueError('literal_geojson_invalid_versioned_profile')
    path = spec['path']
    if (type(path) is not str or not re.fullmatch(r'geojson/[A-Za-z0-9_][A-Za-z0-9_.-]*\.geojson', path)
            or PurePosixPath(path).as_posix() != path):
        raise ValueError('literal_geojson_unsafe_or_unsupported_object_path')
    if 'root_metadata' in spec:
        root = spec['root_metadata']
        if type(root) is not dict or set(root) - {'name', 'crs'} or not root:
            raise ValueError('literal_geojson_invalid_pinned_root_metadata')
        if 'name' in root and (type(root['name']) is not str or not root['name'] or len(root['name']) > 256):
            raise ValueError('literal_geojson_invalid_pinned_root_name')
        if 'crs' in root:
            crs = root['crs']
            if (type(crs) is not dict or set(crs) != {'type', 'properties'} or crs.get('type') != 'name'
                    or type(crs.get('properties')) is not dict or set(crs['properties']) != {'name'}
                    or type(crs['properties']['name']) is not str
                    or re.fullmatch(r'urn:ogc:def:crs:EPSG::[1-9][0-9]{0,5}', crs['properties']['name']) is None):
                raise ValueError('literal_geojson_invalid_pinned_named_epsg_crs')
    return spec
