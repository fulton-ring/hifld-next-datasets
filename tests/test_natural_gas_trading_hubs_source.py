import hashlib
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
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
                   "sha256": digest, "content_encoding": "identity", "content_length": len(body),
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
                    replay_body, replay_receipt, replay_head, replay_root = hubs._acquire_capture(root)
                self.assertEqual(replay_body, body)
                self.assertEqual(replay_root, root)
                self.assertEqual(replay_receipt["observed_at"], observed)
                self.assertEqual(replay_head["observed_at"], observed)
                (root / "eia_source_zip.body").write_bytes(body + b"tamper")
                with self.assertRaisesRegex(ValueError, "retained_get_receipt_body_mismatch"):
                    hubs._load_retained_capture(root)
                (root / "eia_source_zip.body").write_bytes(body)
                (root / "unexpected.txt").write_text("rogue")
                with self.assertRaisesRegex(ValueError, "retained_capture_roster_mismatch"):
                    hubs._load_retained_capture(root)

    def test_live_retry_preserves_failed_attempt_and_recovers_in_new_directory(self):
        body = b"healthy retry archive bytes"
        modified = "test-date"
        request_url = httpx.URL(hubs.SOURCE_URL)

        class FakeClient:
            def __init__(self, healthy):
                self.healthy = healthy

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @contextmanager
            def stream(self, method, _url, **_kwargs):
                if not self.healthy:
                    response = httpx.Response(503, request=httpx.Request(method, request_url))
                elif method == "HEAD":
                    response = httpx.Response(200, headers={
                        "last-modified": modified, "content-length": str(len(body)),
                    }, request=httpx.Request(method, request_url))
                else:
                    response = httpx.Response(200, headers={
                        "content-length": str(len(body)), "content-encoding": "identity",
                    }, stream=httpx.ByteStream(body), request=httpx.Request(method, request_url))
                yield response

        class ClientSequence:
            calls = 0

            def __new__(cls, *args, **kwargs):
                cls.calls += 1
                return FakeClient(healthy=cls.calls > 1)

        record_audit = {"source_record_sha256": "a" * 64,
                        "source_geometry_sha256": "b" * 64, "feature_count": 1}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "attempt"
            with (patch.object(hubs.httpx, "Client", ClientSequence),
                  patch.object(hubs, "EXPECTED_ZIP_BYTES", len(body)),
                  patch.object(hubs, "EXPECTED_ZIP_SHA256", hashlib.sha256(body).hexdigest()),
                  patch.object(hubs, "SOURCE_HTTP_LAST_MODIFIED", modified),
                  patch.object(hubs, "EXPECTED_COUNT", 1),
                  patch.object(hubs, "_read_official_archive", return_value=([{"fixture": True}], record_audit))):
                with self.assertRaisesRegex(ValueError, "official_source_http_metadata_drift"):
                    hubs._acquire_capture(root, allow_live_retry=True)
                self.assertTrue((root / "source_head.receipt.json").is_file())
                failed_manifest = hubs._read_json_receipt(root / "capture_manifest.json")
                self.assertFalse(failed_manifest["complete"])
                failed_head = hubs._read_json_receipt(root / "source_head.receipt.json")
                self.assertEqual(failed_head["status"], 503)
                with patch.object(hubs.httpx, "Client", side_effect=AssertionError("explicit replay fetched")):
                    with self.assertRaisesRegex(ValueError, "retained_capture_roster_mismatch"):
                        hubs._acquire_capture(root)

                replay_body, _receipt, _head, recovered_root = hubs._acquire_capture(
                    root, allow_live_retry=True
                )
                self.assertEqual(replay_body, body)
                self.assertEqual(recovered_root, Path(temp) / "attempt.attempt-0001")
                self.assertTrue((root / "source_head.receipt.json").is_file())
                self.assertEqual(
                    hubs._read_json_receipt(root / "source_head.receipt.json")["status"], 503
                )
                fresh_body, fresh_receipt, _fresh_head, fresh_root = hubs._acquire_capture(
                    root, allow_live_retry=True
                )
                self.assertEqual(fresh_root, Path(temp) / "attempt.attempt-0002")
                self.assertEqual(fresh_body, body)
                self.assertNotEqual(fresh_receipt["observed_at"], _receipt["observed_at"])
                reused_body, reused_receipt, _reused_head, reused_root = hubs._reuse_original_capture(
                    root, fresh_root, fresh_body
                )
                self.assertEqual(reused_body, body)
                self.assertEqual(reused_root, recovered_root)
                self.assertEqual(reused_receipt["observed_at"], _receipt["observed_at"])
                self.assertTrue((fresh_root / "eia_source_zip.receipt.json").is_file())

                class PartialGetClient:
                    def __enter__(self):
                        return self

                    def __exit__(self, *_args):
                        return False

                    @contextmanager
                    def stream(self, method, _url, **_kwargs):
                        if method == "HEAD":
                            response = httpx.Response(200, headers={
                                "last-modified": modified, "content-length": str(len(body)),
                            }, request=httpx.Request(method, request_url))
                        else:
                            partial = b"partial GET bytes"
                            response = httpx.Response(503, headers={
                                "content-length": str(len(partial)), "content-encoding": "identity",
                            }, stream=httpx.ByteStream(partial), request=httpx.Request(method, request_url))
                        yield response

                partial_root = Path(temp) / "partial-get"
                with patch.object(hubs.httpx, "Client", return_value=PartialGetClient()):
                    with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                        hubs._acquire_capture(partial_root, allow_live_retry=True)
                partial_manifest = hubs._read_json_receipt(partial_root / "capture_manifest.json")
                self.assertIn("eia_source_zip.body", partial_manifest["retained_files"])
                self.assertIn("eia_source_zip.wire", partial_manifest["retained_files"])
                self.assertIn("eia_source_zip.receipt.json", partial_manifest["retained_files"])
                self.assertTrue(hubs._incomplete_capture_is_retryable(partial_root, partial_manifest))
                retained_body = (partial_root / "eia_source_zip.body").read_bytes()
                (partial_root / "eia_source_zip.body").write_bytes(retained_body + b"changed")
                self.assertFalse(hubs._incomplete_capture_is_retryable(partial_root, partial_manifest))
                (partial_root / "eia_source_zip.body").write_bytes(retained_body)
                (partial_root / "unexpected.extra").write_bytes(b"extra")
                self.assertFalse(hubs._incomplete_capture_is_retryable(partial_root, partial_manifest))


if __name__ == "__main__":
    unittest.main()
