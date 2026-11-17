"""Shared Esri source formatting primitives.

One canonical-JSON encoder, one Esri-field-to-Arrow type map and one sha256
helper for the source-capture modules. Digest and schema-sha bytes are part of
the recorded-provenance contract: these bodies are identical to the copies they
replace, and the returned values must not change.
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


def sha(body: bytes) -> str:
    """Hex sha256 digest of ``body``."""
    return hashlib.sha256(body).hexdigest()
