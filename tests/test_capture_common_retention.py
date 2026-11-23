"""Failure-retention helper and quarantine behavior (issue #1225, batch 1)."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common as common


def _receipt(name, body, wire, **overrides):
    receipt = {
        "method": "GET", "url": f"https://producer.test/{name}",
        "final_url": f"https://producer.test/{name}",
        "observed_at": "2026-11-23T00:00:00+00:00", "status": 200,
        "complete_body": True, "wire_bytes": len(wire),
        "wire_sha256": hashlib.sha256(wire).hexdigest(),
        "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
        "content_encoding": None, "content_length": None, "error_code": None,
    }
    receipt.update(overrides)
    return receipt


def _write_capture(root, name, body=b"body-bytes", wire=b"wire-bytes", **receipt_overrides):
    (root / f"{name}.body").write_bytes(body)
    (root / f"{name}.wire").write_bytes(wire)
    (root / f"{name}.receipt.json").write_bytes(
        common._canonical(_receipt(name, body, wire, **receipt_overrides)) + b"\n")


class RetainFailedCaptureTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.capture = self.root / "capture"
        self.capture.mkdir()
        (self.root / "staging").mkdir()
        self.storage = StagingStorageResource(
            use_local=True, local_dir=str(self.root / "staging"), prefix="")

    def retain(self, exc):
        """The caller contract: retain evidence, then re-raise the original."""
        try:
            raise exc
        except Exception as original:
            common.retain_failed_capture(
                self.storage, self.capture, "dataset", "v1", original, error=ValueError)
            raise

    def _failure(self):
        keys = [key for key in self.storage.list_prefix("_quarantine/dataset")
                if key.endswith("metadata/failure.json")]
        self.assertEqual(len(keys), 1)
        base = keys[0][: -len("metadata/failure.json")]
        return base, json.loads(self.storage.read_key(keys[0]))

    def test_missing_wire_is_recorded_and_original_error_propagates(self):
        _write_capture(self.capture, "good")
        _write_capture(self.capture, "no-wire")
        (self.capture / "no-wire.wire").unlink()
        original = OSError("upstream reset while paging")
        with self.assertRaises(OSError) as raised:
            self.retain(original)
        self.assertIs(raised.exception, original)
        base, doc = self._failure()
        self.assertEqual(doc["reason_code"], "OSError")
        self.assertEqual(doc["captured_names"], ["good"])
        self.assertEqual(doc["not_retained"],
                         [{"name": "no-wire",
                           "reason": "retained_capture_file_missing:no-wire:wire"}])
        self.assertEqual(self.storage.read_key(base + "raw/good.body"), b"body-bytes")
        self.assertEqual(self.storage.read_key(base + "raw/good.wire"), b"wire-bytes")
        self.assertIsNone(self.storage.object_snapshot(base + "raw/no-wire.body"))

    def test_truncated_receipt_is_recorded_not_replacing_original_error(self):
        _write_capture(self.capture, "truncated")
        (self.capture / "truncated.receipt.json").write_bytes(b'{"method": "GET"')
        original = ValueError("source_capture_incomplete:eia_zip")
        with self.assertRaises(ValueError) as raised:
            self.retain(original)
        self.assertIs(raised.exception, original)
        self.assertNotIsInstance(raised.exception, json.JSONDecodeError)
        _, doc = self._failure()
        self.assertEqual(doc["captured_names"], [])
        self.assertEqual(doc["not_retained"],
                         [{"name": "truncated",
                           "reason": "retained_capture_receipt_unreadable:truncated"}])

    def test_quarantine_holds_good_capture_and_lists_the_rest(self):
        _write_capture(self.capture, "good")
        _write_capture(self.capture, "bad-receipt")
        (self.capture / "bad-receipt.receipt.json").write_bytes(b'{"bytes": 1')
        _write_capture(self.capture, "bad-size")
        (self.capture / "bad-size.body").write_bytes(b"body-bytes-tampered")
        _write_capture(self.capture, "no-wire")
        (self.capture / "no-wire.wire").unlink()
        original = RuntimeError("fetch_failed:ids_seed")
        with self.assertRaises(RuntimeError) as raised:
            self.retain(original)
        self.assertIs(raised.exception, original)
        base, doc = self._failure()
        self.assertEqual(doc["reason_code"], "RuntimeError")
        self.assertEqual(doc["captured_names"], ["good"])
        self.assertEqual(doc["not_retained"], [
            {"name": "bad-receipt",
             "reason": "retained_capture_receipt_unreadable:bad-receipt"},
            {"name": "bad-size", "reason": "retained_capture_evidence_mismatch:bad-size"},
            {"name": "no-wire", "reason": "retained_capture_file_missing:no-wire:wire"},
        ])
        self.assertEqual(self.storage.read_key(base + "raw/good.body"), b"body-bytes")
        self.assertIsNone(self.storage.object_snapshot(base + "raw/bad-size.body"))

    def test_partial_attempt_with_consistent_evidence_is_retained(self):
        _write_capture(self.capture, "partial", complete_body=False,
                       error_code="incomplete_source_capture")
        base = common.retain_failed_capture(self.storage, self.capture, "dataset", "v1",
                                            OSError("connection reset"), error=ValueError)
        doc = json.loads(self.storage.read_key(base + "/metadata/failure.json"))
        self.assertEqual(doc["captured_names"], ["partial"])
        # An empty not_retained list is omitted to keep the pre-migration
        # failure.json contract for adapters not yet on issue #1225.
        self.assertNotIn("not_retained", doc)

    def test_helper_failure_is_recorded_and_does_not_replace_original(self):
        _write_capture(self.capture, "good")
        broken = Mock(spec=StagingStorageResource)
        broken.object_snapshot.return_value = None
        broken.write_key_if_unchanged.side_effect = OSError("disk full")
        original = OSError("upstream reset while paging")
        with self.assertRaises(OSError) as raised:
            try:
                raise original
            except Exception as exc:
                base = common.retain_failed_capture(
                    broken, self.capture, "dataset", "v1", exc, error=ValueError)
                self.assertIsNone(base)
                raise
        self.assertIs(raised.exception, original)
        marker = json.loads((self.capture / "capture_retention_failed.json").read_bytes())
        self.assertEqual(marker["error_type"], "OSError")

    def test_empty_capture_root_is_still_recorded(self):
        base = common.retain_failed_capture(self.storage, self.capture, "dataset", "v1",
                                            ValueError("source_capture_incomplete"),
                                            error=ValueError)
        doc = json.loads(self.storage.read_key(base + "/metadata/failure.json"))
        self.assertEqual(doc, {"reason_code": "ValueError", "captured_names": []})

    def test_missing_capture_root_is_recorded_not_raised(self):
        base = common.retain_failed_capture(self.storage, self.root / "absent", "dataset", "v1",
                                            ValueError("boom"), error=ValueError)
        doc = json.loads(self.storage.read_key(base + "/metadata/failure.json"))
        self.assertEqual(doc["not_retained"],
                         [{"name": "*", "reason": "capture_root_unreadable:FileNotFoundError"}])


class QuarantineMissingWireTest(unittest.TestCase):
    def test_quarantine_records_missing_wire_instead_of_raising(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        capture = root / "capture"
        capture.mkdir()
        (root / "staging").mkdir()
        storage = StagingStorageResource(
            use_local=True, local_dir=str(root / "staging"), prefix="")
        _write_capture(capture, "good")
        _write_capture(capture, "no-wire")
        (capture / "no-wire.wire").unlink()
        values = {name: ((capture / f"{name}.body").read_bytes(),
                          json.loads((capture / f"{name}.receipt.json").read_bytes()))
                  for name in ("good", "no-wire")}
        base = common.quarantine_incomplete_capture(
            storage, capture, "dataset", "v1", values, "OSError")
        doc = json.loads(storage.read_key(base + "/metadata/failure.json"))
        self.assertEqual(doc["captured_names"], ["good"])
        self.assertEqual(doc["not_retained"],
                         [{"name": "no-wire",
                           "reason": "retained_capture_wire_missing:no-wire"}])


if __name__ == "__main__":
    unittest.main()
