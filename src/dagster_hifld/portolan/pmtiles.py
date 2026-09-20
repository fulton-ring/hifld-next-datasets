"""Bounded PMTiles v3 metadata inspection for Portolan publication."""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable

_HEADER_SIZE = 127
_MAGIC = b"PMTiles"
_VERSION = 3
_METADATA_OFFSET = 24
_METADATA_LENGTH = 32
_INTERNAL_COMPRESSION = 97
_COMPRESSION_NONE = 1
_COMPRESSION_GZIP = 2
_MAX_METADATA_BYTES = 16 * 1024 * 1024

RangeReader = Callable[[int, int], bytes]


def extract_vector_layer_ids(read_range: RangeReader, size_bytes: int) -> tuple[str, ...]:
    """Read only PMTiles metadata and return its declared vector-layer IDs."""
    if size_bytes < _HEADER_SIZE:
        raise ValueError("PMTiles archive is shorter than its v3 header.")
    header = _read_exact(read_range, 0, _HEADER_SIZE)
    if header[:7] != _MAGIC:
        raise ValueError("PMTiles archive has invalid magic bytes.")
    if header[7] != _VERSION:
        raise ValueError(f"Unsupported PMTiles version: {header[7]}.")
    metadata_offset = int.from_bytes(
        header[_METADATA_OFFSET : _METADATA_OFFSET + 8], "little"
    )
    metadata_length = int.from_bytes(
        header[_METADATA_LENGTH : _METADATA_LENGTH + 8], "little"
    )
    if metadata_length == 0:
        raise ValueError("PMTiles archive has no metadata block.")
    if metadata_length > _MAX_METADATA_BYTES:
        raise ValueError("PMTiles metadata block exceeds the configured size limit.")
    metadata_end = metadata_offset + metadata_length
    if metadata_offset < _HEADER_SIZE or metadata_end > size_bytes:
        raise ValueError("PMTiles metadata range is outside the archive.")
    metadata = _read_exact(read_range, metadata_offset, metadata_length)
    compression = header[_INTERNAL_COMPRESSION]
    if compression == _COMPRESSION_GZIP:
        try:
            metadata = gzip.decompress(metadata)
        except OSError as error:
            raise ValueError("PMTiles gzip metadata could not be decompressed.") from error
    elif compression != _COMPRESSION_NONE:
        raise ValueError(f"Unsupported PMTiles internal compression: {compression}.")
    try:
        decoded = json.loads(metadata)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("PMTiles metadata is not valid JSON.") from error
    if not isinstance(decoded, dict):
        raise TypeError("PMTiles metadata must be a JSON object.")
    vector_layers = decoded.get("vector_layers")
    if not isinstance(vector_layers, list) or not vector_layers:
        raise ValueError("PMTiles metadata has no vector_layers entries.")
    seen: set[str] = set()
    layer_ids: list[str] = []
    for index, layer in enumerate(vector_layers):
        if not isinstance(layer, dict):
            raise TypeError(f"PMTiles vector_layers[{index}] is not an object.")
        layer_id = layer.get("id")
        if not isinstance(layer_id, str) or not layer_id.strip():
            raise ValueError(f"PMTiles vector_layers[{index}] has no non-empty id.")
        if layer_id not in seen:
            seen.add(layer_id)
            layer_ids.append(layer_id)
    return tuple(layer_ids)


def _read_exact(read_range: RangeReader, offset: int, length: int) -> bytes:
    data = read_range(offset, length)
    if not isinstance(data, bytes) or len(data) != length:
        raise ValueError("PMTiles range read returned fewer bytes than requested.")
    return data
