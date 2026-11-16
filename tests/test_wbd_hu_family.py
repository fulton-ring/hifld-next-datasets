"""Parity coverage for the shared WBD hydrologic-unit family machinery.

The two keyset-window WBD adapters (2-digit and 12-digit) must behave as one
implementation over ``dagster_hifld.sources.wbd_hu_family`` with different
level pins and policies, and the offset-segment checkpoint adapter (8-digit
HU subbasin) must keep its deliberately distinct acquisition policy. These
tests pin the parity points refactor issue #924 calls out: leading-zero HUC
codes, schema/CRS handling, native ring ownership, malformed geometry,
ID-roster drift, split/refused page windows, corrupt replay, and the
8-digit's distinct receipt/checkpoint policy.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources import wbd_2_digit_hu as two
from dagster_hifld.sources import wbd_12_digit_hu as twelve
from dagster_hifld.sources import wbd_hu8_subbasin as hu8
from shapely import from_wkb
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources import wbd_hu_family as family
from dagster_hifld.sources.usace_obstruction_lines import _canonical
from tests.test_wbd_2_digit_hu import (
    _documents as _two_documents,
    _seal_manifest as _seal_two_manifest,
    _write_capture_root as _two_write_capture_root,
    patches as _patches, relaxed_pins as _two_relaxed_pins,
)
from tests.test_wbd_12_digit_hu import (
    _documents as _twelve_documents,
    _write_capture_root as _twelve_write_capture_root,
    relaxed_pins as _twelve_relaxed_pins,
)


def _seal_manifest(root: Path, documents: dict, *, page_parts: dict | None = None,
                   page_refusals: list[str] | None = None) -> None:
    """Seal a capture manifest with correct digests (generic across levels)."""
    import hashlib

    def digest(body: bytes) -> str:
        return hashlib.sha256(body).hexdigest()

    manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                "receipts": {}, "artifact_roster": {},
                "page_parts": page_parts or {}, "page_refusals": page_refusals or []}
    for name in sorted(documents):
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt_raw = (root / f"{name}.receipt.json").read_bytes()
        manifest["receipts"][name] = json.loads(receipt_raw)
        for suffix, blob in (("body", body), ("wire", wire),
                             ("receipt.json", receipt_raw)):
            manifest["artifact_roster"][f"{name}.{suffix}"] = {
                "bytes": len(blob), "sha256": digest(blob)}
    (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")


class WbdHuFamilyParityTests(unittest.TestCase):
    def _tmpdir(self) -> str:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-family-parity-")
        self.addCleanup(temporary.cleanup)
        return temporary.name

    def test_shared_primitives_are_one_implementation(self):
        # Ring orientation and hole assignment are GDAL's through the shared
        # native Esri polygon conversion (issue #997); the only remaining gate
        # mode is the producer-refused attributes-only window.
        self.assertEqual(
            twelve.GATE_MODES, frozenset({family.REFUSED_GEOMETRY_GATE_MODE}))
        self.assertEqual(two.GATE_MODES, frozenset())
        self.assertIs(two._EPSG3857_PROJJSON, twelve._EPSG3857_PROJJSON)
        self.assertIs(two.DIGEST_SEPARATOR, twelve.DIGEST_SEPARATOR)
        self.assertIs(two.METADATA_NAMES, twelve.METADATA_NAMES)

    def test_native_ring_ownership_prefers_smallest_containing_exterior(self):
        from shapely import from_wkb

        big = [[0.0, 0.0], [0.0, 100.0], [100.0, 100.0], [100.0, 0.0], [0.0, 0.0]]
        small = [[150.0, 150.0], [150.0, 170.0], [170.0, 170.0], [170.0, 150.0],
                 [150.0, 150.0]]
        hole = [[155.0, 155.0], [165.0, 155.0], [165.0, 165.0], [155.0, 165.0],
                [155.0, 155.0]]
        outside = [[-50.0, -50.0], [-50.0, -40.0], [-40.0, -40.0], [-40.0, -50.0],
                   [-50.0, -50.0]]
        wkb = to_wkb({"rings": [big, small, hole, outside]}, "esriGeometryPolygon",
                     has_z=False, has_m=False)
        self.assertEqual(wkb[:5], b"\x01\x06\x00\x00\x00")
        decoded = from_wkb(wkb)
        self.assertEqual(len(decoded.geoms), 3)
        owning = [polygon for polygon in decoded.geoms if len(polygon.interiors) == 1]
        self.assertEqual(len(owning), 1)
        self.assertLess(owning[0].area, 500.0)

    def test_leading_zero_huc_codes_are_retained_verbatim(self):
        documents = _two_documents()
        documents["page_0000"]["features"][0]["attributes"]["huc2"] = "07"
        documents["page_0000"]["features"][0]["attributes"]["name"] = "Zero Region"
        documents["page_0000"]["features"][0]["attributes"]["tnmid"] = "{07X}"
        sink = Path(self._tmpdir()) / "two.parquet"
        with _two_relaxed_pins():
            root = _two_write_capture_root(Path(self._tmpdir()) / "two", documents)
            two._validate_stream(root, parquet_sink=sink)
        table = pq.read_table(sink)
        self.assertEqual(table.column("huc2")[0].as_py(), "07")
        twelve_documents = _twelve_documents()
        twelve_documents["page_0000"]["features"][0]["attributes"]["huc12"] = "070200040301"
        twelve_sink = Path(self._tmpdir()) / "twelve.parquet"
        with _twelve_relaxed_pins():
            twelve_root = _twelve_write_capture_root(Path(self._tmpdir()) / "twelve",
                                                     twelve_documents)
            twelve._validate_stream(twelve_root, parquet_sink=twelve_sink)
        twelve_table = pq.read_table(twelve_sink)
        self.assertEqual(twelve_table.column("huc12")[0].as_py(), "070200040301")

    def test_schema_crs_and_malformed_geometry_parity(self):
        for module, label in ((two, "two"), (twelve, "twelve")):
            with self.subTest(module=label):
                schema = module._native_schema()
                self.assertEqual(schema.field("geometry").type, pa.binary())
                self.assertIn(b"geo", schema.metadata)
                self.assertEqual(schema.field("objectid").type, pa.int64())
                # GDAL retains zero-area and self-intersecting rings verbatim
                # as OGC-invalid geometry and assembles an empty ring list as
                # an error (issue #997 shared converter).
                self.assertFalse(from_wkb(to_wkb({"rings": [[[1.0, 2.0]] * 4]},
                                                 "esriGeometryPolygon",
                                                 has_z=False, has_m=False)).is_valid)
                with self.assertRaisesRegex(ValueError, "invalid_esri_polygon_rings"):
                    to_wkb({"rings": []}, "esriGeometryPolygon", has_z=False, has_m=False)
                self.assertFalse(from_wkb(to_wkb(
                    {"rings": [[[0.0, 0.0], [2.0, 0.0], [0.0, 2.0], [3.0, 2.0], [0.0, 0.0]]]},
                    "esriGeometryPolygon", has_z=False, has_m=False)).is_valid)
                with self.assertRaisesRegex(ValueError, "source_page_0000_page_crs_drift"):
                    documents = (_two_documents() if module is two else _twelve_documents())
                    documents["page_0000"]["spatialReference"] = {"wkid": 4326}
                    writer = _two_write_capture_root if module is two \
                        else _twelve_write_capture_root
                    relaxed = _two_relaxed_pins if module is two else _twelve_relaxed_pins
                    with relaxed():
                        module._validate_stream(
                            writer(Path(self._tmpdir()) / ("crs-" + label), documents),
                            parquet_sink=None)

    def test_id_roster_drift_messages_match_across_adapters(self):
        document = {"objectIdFieldName": "objectid", "objectIds": [1, 2, 3]}
        with _two_relaxed_pins(EXPECTED_COUNT=2):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift:probe"):
                two._validate_roster_document(document, "probe")
        with _twelve_relaxed_pins(EXPECTED_COUNT=2):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift:probe"):
                twelve._validate_roster_document(document, "probe")

    def test_split_refused_windows_follow_each_level_policy(self):
        roster = [11, 12, 501, 100_003]
        refusal = b'{"error": {"code": 500, "message": "Error performing query operation", ' \
                  b'"details": []}}'
        served = b'{"features": []}'

        def fake_capture(_client, url, root, name, max_bytes=48_000_000):
            body = refusal if name.endswith(("a", "b")) or len(name) == 9 else served
            wire = b"wire:" + body[:8]
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                       "complete_body": True, "bytes": len(body),
                       "sha256": family._sha(body), "wire_bytes": len(wire),
                       "wire_sha256": family._sha(wire), "content_encoding": None,
                       "content_length": None, "error_code": None, "error_type": None}
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(wire)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        root = Path(self._tmpdir()) / "captures"
        (root / "two").mkdir(parents=True)
        (root / "twelve").mkdir(parents=True)
        with _patches(two, _capture=fake_capture):
            with self.assertRaisesRegex(ValueError, "source_page_window_refused_after_bisection"):
                two._capture_page_window(None, roster, 0, root / "two")
        with _patches(twelve, _capture=fake_capture, SINGLE_REFUSAL_RETRY_SECONDS=0.0):
            parts, refusals = twelve._capture_page_window(None, roster, 0, root / "twelve")
        self.assertIn("page_0000aa_attrs", parts)
        self.assertNotIn("page_0000a", parts)
        self.assertEqual(refusals[0], "page_0000_refusal")
        self.assertGreater(len(refusals), 1)

    def test_corrupt_replay_artifact_is_refused_for_both_adapters(self):
        for module, documents, writer, relaxed, label in (
                (two, _two_documents(), _two_write_capture_root, _two_relaxed_pins, "two"),
                (twelve, _twelve_documents(), _twelve_write_capture_root,
                 _twelve_relaxed_pins, "twelve")):
            with self.subTest(module=label):
                with relaxed():
                    root = writer(Path(self._tmpdir()) / ("replay-" + label), documents)
                    _seal = (_seal_two_manifest if module is two else _seal_manifest)
                    _seal(root, documents)
                victim = root / "count_before.body"
                victim.write_bytes(victim.read_bytes() + b" ")
                with relaxed(EXPECTED_ID_SHA256="", DISK_FLOOR_BYTES=1):
                    with self.assertRaisesRegex(
                            ValueError, "retained_capture_artifact_digest_mismatch"):
                        module._read_replay(root)

    def test_hu8_keeps_the_distinct_receipt_and_checkpoint_policy(self):
        root = Path(self._tmpdir()) / "hu8"
        root.mkdir()
        receipt = {"method": "GET", "url": "https://a.example/x", "final_url": "https://b/y",
                   "status": 200, "complete_body": True, "bytes": 3,
                   "sha256": family._sha(b"abc"), "wire_bytes": 3,
                   "wire_sha256": family._sha(b"xyz")}
        (root / "seg_00000_001.body").write_bytes(b"abc")
        (root / "seg_00000_001.wire").write_bytes(b"xyz")
        (root / "seg_00000_001.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        with self.assertRaisesRegex(hu8.WbdHu8SubbasinError, "retained_capture_receipt_invalid"):
            hu8._read_capture(root, "seg_00000_001")
        state = {"segments": [{"offset": 0, "count": 1, "name": "seg_00000_001",
                               "features": 1}], "complete": False}
        (root / "capture_state.json").write_bytes(_canonical(state) + b"\n")
        self.assertEqual(hu8._load_state(root)["segments"], state["segments"])


if __name__ == "__main__":
    unittest.main()
