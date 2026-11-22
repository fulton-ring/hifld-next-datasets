"""Document acquisition keeps receipt ledgers and native volume headroom."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dagster_hifld.sources.arcgis_adaptive import capture_document_group
from dagster_hifld.sources import pad_us_protection_mechanism_category as pad


class AdaptiveDocumentTests(unittest.TestCase):
    def test_receipt_budget_retains_last_document_and_stops_before_next_request(self):
        calls = []
        captures = {}

        def fetch(client, url, root, name):
            calls.append(name)
            return name.encode(), {"bytes": 2, "wire_bytes": 3}

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_capture_exceeds_scratch_budget"):
                capture_document_group(None, Path(tmp), [("first", "a"), ("second", "b"), ("third", "c")],
                    fetch=fetch, captures=captures, retained_bytes=1, max_total_bytes=10)
        self.assertEqual(calls, ["first", "second"])
        self.assertEqual(set(captures), {"first", "second"})

    def test_document_transport_selection_and_source_validation_keep_order(self):
        calls = []
        validated = []
        producer, docs = object(), object()

        def fetch(client, url, root, name):
            calls.append((client, name))
            return name.encode(), {}

        with tempfile.TemporaryDirectory() as tmp:
            captures, total = capture_document_group(producer, Path(tmp), [("license", "a"), ("layer", "b")],
                fetch=fetch, client_for=lambda name: docs if name == "license" else producer,
                validate=lambda name, body: validated.append((name, body)))
        self.assertEqual(calls, [(docs, "license"), (producer, "layer")])
        self.assertEqual(validated, [("license", b"license"), ("layer", b"layer")])
        self.assertEqual(set(captures), {"license", "layer"})
        self.assertEqual(total, 0)

    def test_pad_capacity_budget_preserves_existing_disk_floor_and_retained_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "page.body").write_bytes(b"body")
            (root / "page.wire").write_bytes(b"wire")
            (root / "page.receipt.json").write_bytes(b"receipt")
            with patch.object(pad, "MIN_FREE_BYTES", 10), \
                 patch.object(pad.shutil, "disk_usage", return_value=SimpleNamespace(free=20)):
                self.assertEqual(pad._capture_capacity_budget(root), 18)
            with patch.object(pad, "MIN_FREE_BYTES", 10), \
                 patch.object(pad.shutil, "disk_usage", return_value=SimpleNamespace(free=9)):
                with self.assertRaisesRegex(pad.PadUsProtectionError, "disk_floor_before_acquisition"):
                    pad._capture_capacity_budget(root)
