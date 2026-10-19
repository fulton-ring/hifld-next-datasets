"""Focused identity and release-gate checks for the frozen BTS IPCD draft."""

from dagster_hifld.assets.bts_ipcd import (
    bts_intermodal_passenger_connectivity_database_ipcd,
    bts_intermodal_passenger_connectivity_database_ipcd_complete,
    bts_ipcd_frozen_snapshot_schedule,
)
from dagster_hifld.sources.bts_ipcd import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_GEOMETRY_SHA256,
    EXPECTED_ID_SHA256,
    EXPECTED_RECORD_SHA256,
    EXPECTED_SCHEMA_SHA256,
    ITEM_ID,
    LAYER_ID,
    LAYER_NAME,
    SERVICE_URL,
    _canonical,
    _load_retained_capture,
    _arrow_type,
    _sha,
)
import pyarrow as pa
from dagster import DefaultScheduleStatus
import unittest
import tempfile
import json
from pathlib import Path


class TestBtsIpcd(unittest.TestCase):
    def test_bts_ipcd_asset_is_explicit_and_snapshot_schedule_is_stopped(self):
        self.assertEqual(DATASET_SLUG, "intermodal-passenger-connectivity-database-ipcd")
        self.assertEqual(ITEM_ID, "25d9314da4ec44088f1db9e9e11a19d6")
        self.assertEqual(LAYER_ID, 0)
        self.assertEqual(LAYER_NAME, "Intermodal Passenger Connectivity Database (IPCD)")
        self.assertTrue(SERVICE_URL.endswith("NTAD_Intermodal_Passenger_Connectivity_Database/FeatureServer"))
        self.assertEqual(bts_intermodal_passenger_connectivity_database_ipcd.key.path, ["ingest", DATASET_SLUG, DATASET_SLUG])
        self.assertEqual(bts_ipcd_frozen_snapshot_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertTrue(bts_intermodal_passenger_connectivity_database_ipcd_complete.check_specs)

    def test_frozen_ipcd_population_baselines_are_complete_and_pinned(self):
        self.assertEqual(EXPECTED_COUNT, 15488)
        self.assertTrue(all(len(value) == 64 for value in (
            EXPECTED_ID_SHA256, EXPECTED_RECORD_SHA256, EXPECTED_GEOMETRY_SHA256,
            EXPECTED_SCHEMA_SHA256,
        )))

    def test_esri_physical_types_preserve_date_and_numeric_widths(self):
        self.assertEqual(_arrow_type({"type": "esriFieldTypeOID"}), pa.int64())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeInteger"}), pa.int32())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeDouble"}), pa.float64())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeDate"}), pa.int64())

    def test_retained_capture_replay_preserves_original_receipt_and_time(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body, wire = b'{"features":[]}', b'wire-bytes'
            receipt = {
                "url": "https://example.invalid/source", "final_url": "https://example.invalid/source",
                "status": 200, "complete_body": True, "error_code": None,
                "bytes": len(body), "sha256": _sha(body),
                "wire_bytes": len(wire), "wire_sha256": _sha(wire),
                "observed_at": "2026-10-19T22:20:54.063734+00:00",
            }
            (root / "page_000.body").write_bytes(body)
            (root / "page_000.wire").write_bytes(wire)
            (root / "page_000.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            captured_at = "2026-10-19T22:20:54.063734+00:00"
            (root / "capture_manifest.json").write_bytes(_canonical({
                "complete": True, "captured_at": captured_at, "receipts": {"page_000": receipt}
            }) + b"\n")
            captures, observed_capture_time = _load_retained_capture(root)
            self.assertEqual(observed_capture_time, captured_at)
            self.assertEqual(captures["page_000"], (body, receipt))

    def test_retained_capture_replay_rejects_changed_wire_body(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body, wire = b'{"features":[]}', b'original-wire'
            receipt = {
                "url": "https://example.invalid/source", "final_url": "https://example.invalid/source",
                "status": 200, "complete_body": True, "error_code": None,
                "bytes": len(body), "sha256": _sha(body),
                "wire_bytes": len(wire), "wire_sha256": _sha(wire),
            }
            (root / "page_000.body").write_bytes(body)
            (root / "page_000.wire").write_bytes(b"changed-wire")
            (root / "page_000.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            (root / "capture_manifest.json").write_bytes(_canonical({
                "complete": True, "receipts": {"page_000": receipt}
            }) + b"\n")
            with self.assertRaisesRegex(ValueError, "retained_capture_integrity_failed"):
                _load_retained_capture(root)
