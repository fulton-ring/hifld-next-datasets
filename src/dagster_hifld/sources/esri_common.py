"""Shared Esri source formatting primitives.

One canonical-JSON encoder (plain and newline-terminated forms), the
Esri-field-to-Arrow type maps, the Esri field-roster signature validator and
one sha256 helper for the source-capture modules. Digest and schema-sha bytes
are part of the recorded-provenance contract: these bodies are identical to the
copies they replace, and the returned values must not change.
"""
from __future__ import annotations

import hashlib
import json
from typing import Mapping

import pyarrow as pa


def canonical(value: object) -> bytes:
    """Canonical JSON encoding: sorted keys, compact separators, UTF-8, no NaN."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def canonical_line(value: object) -> bytes:
    """Canonical JSON encoding terminated with a newline (manifest/receipt form)."""
    return canonical(value) + b"\n"


def arrow_type(field: Mapping[str, object]) -> pa.DataType:
    """Arrow type for an Esri field descriptor; unknown or missing types degrade to string."""
    return {
        "esriFieldTypeOID": pa.int64(),
        "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(),
        "esriFieldTypeSingle": pa.float32(),
        "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.int64(),
    }.get(field.get("type"), pa.string())


def native_arrow_type(kind: str) -> pa.DataType:
    """Arrow type for a native Esri field type string; unknown types fail closed."""
    if kind in {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("unsupported_esri_field_type:" + kind)


def field_signature(fields: object) -> list[tuple[str, str, int | None]]:
    if not isinstance(fields, list) or not fields or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_schema_missing")
    result = []
    for field in fields:
        name, kind, length = field.get("name"), field.get("type"), field.get("length")
        if not isinstance(name, str) or not isinstance(kind, str) or (length is not None and type(length) is not int):
            raise ValueError("source_schema_invalid")
        result.append((name, kind, length))
    if len({field[0] for field in result}) != len(result):
        raise ValueError("source_schema_invalid")
    return result


def sha(body: bytes) -> str:
    """Hex sha256 digest of ``body``."""
    return hashlib.sha256(body).hexdigest()
