import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from dagster_hifld.sources import natural_gas_trading_hubs as hubs
from dagster_hifld.sources.natural_gas_trading_hubs import _parquet_bytes


class NaturalGasTradingHubsCandidateTests(unittest.TestCase):
    def test_native_fields_and_geometry_are_preserved_without_synthetic_id(self):
        rows = [{
            "HubName": "Henry Hub", "Period": 202002,
            "Latitude": 29.957, "Longitude": -91.15,
            "geometry_wkb": Point(-91.15, 29.957).wkb,
        }]
        result = pq.read_table(pa.BufferReader(_parquet_bytes(rows)))
        self.assertEqual(result.column_names, ["HubName", "Period", "Latitude", "Longitude", "geometry"])
        self.assertEqual(result.schema.field("HubName").type, pa.string())
        self.assertEqual(result.schema.field("Period").type, pa.int32())
        self.assertEqual(result.schema.field("Latitude").type, pa.float64())
        self.assertEqual(result.schema.field("Longitude").type, pa.float64())
        self.assertEqual(result["HubName"].to_pylist(), ["Henry Hub"])
        self.assertEqual(result["Period"].to_pylist(), [202002])
        self.assertEqual(result["geometry"].to_pylist(), [rows[0]["geometry_wkb"]])

    def test_retained_capture_replay_preserves_receipts_and_rejects_tampering(self):
        body = b"test archive bytes"
        digest = hashlib.sha256(body).hexdigest()
        observed = "2020-07-10T12:34:56+00:00"
        receipt = {"method": "GET", "url": hubs.SOURCE_URL, "final_url": hubs.SOURCE_URL,
                   "observed_at": observed, "status": 200, "complete_body": True,
                   "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                   "sha256": digest, "content_encoding": "identity", "content_length": str(len(body)),
                   "error_code": None}
        head = {"status": 200, "url": hubs.SOURCE_URL, "last_modified": "test-date",
                "content_length": str(len(body)), "observed_at": observed}
        head_bytes = hubs._canonical(head) + b"\n"
        manifest = {"captured_at": observed, "complete": True, "error_code": None,
                    "source_feature_count": 1, "source_head_receipt_sha256": hashlib.sha256(head_bytes).hexdigest(),
                    "receipts": {"eia_source_zip": receipt}}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "capture"
            root.mkdir()
            (root / "eia_source_zip.body").write_bytes(body)
            (root / "eia_source_zip.wire").write_bytes(body)
            (root / "eia_source_zip.receipt.json").write_bytes(hubs._canonical(receipt) + b"\n")
            (root / "source_head.receipt.json").write_bytes(head_bytes)
            (root / "capture_manifest.json").write_bytes(hubs._canonical(manifest) + b"\n")
            with (patch.object(hubs, "EXPECTED_ZIP_BYTES", len(body)),
                  patch.object(hubs, "EXPECTED_ZIP_SHA256", digest),
                  patch.object(hubs, "SOURCE_HTTP_LAST_MODIFIED", "test-date"),
                  patch.object(hubs, "EXPECTED_COUNT", 1)):
                with patch.object(hubs.httpx, "Client", side_effect=AssertionError("replay refetched source")):
                    replay_body, replay_receipt, replay_head = hubs._acquire_capture(root)
                self.assertEqual(replay_body, body)
                self.assertEqual(replay_receipt["observed_at"], observed)
                self.assertEqual(replay_head["observed_at"], observed)
                (root / "eia_source_zip.body").write_bytes(body + b"tamper")
                with self.assertRaisesRegex(ValueError, "retained_get_receipt_body_mismatch"):
                    hubs._load_retained_capture(root)
                (root / "eia_source_zip.body").write_bytes(body)
                (root / "unexpected.txt").write_text("rogue")
                with self.assertRaisesRegex(ValueError, "retained_capture_roster_mismatch"):
                    hubs._load_retained_capture(root)


if __name__ == "__main__":
    unittest.main()
