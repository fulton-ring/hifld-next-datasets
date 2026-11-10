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
    if (type(spec) is not dict or set(spec) != {'schema_version', 'path', 'bytes', 'sha256'}
            or type(spec['schema_version']) is not int or spec['schema_version'] != 1
            or type(spec['bytes']) is not int or not 0 < spec['bytes'] <= MAX_LITERAL_GEOJSON_BYTES
            or type(spec['sha256']) is not str or re.fullmatch('[0-9a-f]{64}', spec['sha256']) is None):
        raise ValueError('literal_geojson_invalid_versioned_profile')
    path = spec['path']
    if (type(path) is not str or not re.fullmatch(r'geojson/[A-Za-z0-9_][A-Za-z0-9_.-]*\.geojson', path)
            or PurePosixPath(path).as_posix() != path):
        raise ValueError('literal_geojson_unsafe_or_unsupported_object_path')
    return spec
