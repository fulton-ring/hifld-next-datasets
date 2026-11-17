"""Focused checks for the shared Esri source formatting helpers.

The helpers replaced byte-identical copies across ``sources/``; these tests
pin the observable behavior the copies guaranteed: canonical-JSON bytes, the
extended Esri-field Arrow map with the string fallback, and hex sha256.
"""
from __future__ import annotations

import hashlib
import json
import unittest

import pyarrow as pa

from dagster_hifld.sources import capture_common, esri_common


class CanonicalJsonTest(unittest.TestCase):
    def test_sorted_compact_utf8_bytes(self):
        self.assertEqual(
            esri_common.canonical({"b": "é", "a": 1, "list": [3, 1]}),
            '{"a":1,"b":"é","list":[3,1]}'.encode("utf-8"),
        )

    def test_key_order_and_whitespace_do_not_change_the_bytes(self):
        self.assertEqual(
            esri_common.canonical({"a": 1, "b": 2}),
            esri_common.canonical({"b": 2, "a": 1}),
        )

    def test_nan_is_refused_not_silently_encoded(self):
        with self.assertRaises(ValueError):
            esri_common.canonical({"value": float("nan")})

    def test_non_ascii_is_not_escaped(self):
        self.assertNotIn(b"\\u", esri_common.canonical({"name": "ü"}))

    def test_matches_the_capture_common_copy(self):
        value = {"z": 1, "a": {"nested": [True, None, "é"]}, "d": 4.5}
        self.assertEqual(esri_common.canonical(value), capture_common._canonical(value))
        self.assertEqual(
            esri_common.canonical(value),
            json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False).encode("utf-8"),
        )


class ArrowTypeTest(unittest.TestCase):
    def test_extended_esri_field_type_map(self):
        cases = {
            "esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
        }
        for esri_type, expected in cases.items():
            with self.subTest(esri_type=esri_type):
                self.assertEqual(esri_common.arrow_type({"type": esri_type, "name": "F"}), expected)

    def test_unknown_type_falls_back_to_string(self):
        self.assertEqual(esri_common.arrow_type({"type": "esriFieldTypeGeometry"}), pa.string())

    def test_missing_type_falls_back_to_string(self):
        self.assertEqual(esri_common.arrow_type({"name": "F"}), pa.string())

    def test_string_type_argument_falls_back_to_string(self):
        self.assertEqual(esri_common.arrow_type({"type": 4}), pa.string())


class ShaTest(unittest.TestCase):
    def test_known_vector(self):
        self.assertEqual(
            esri_common.sha(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        self.assertEqual(esri_common.sha(b""), hashlib.sha256(b"").hexdigest())

    def test_matches_hashlib_sha256_hexdigest(self):
        body = bytes(range(256))
        self.assertEqual(esri_common.sha(body), hashlib.sha256(body).hexdigest())


if __name__ == "__main__":
    unittest.main()
