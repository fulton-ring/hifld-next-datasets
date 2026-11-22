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

    def test_pad_zero_headroom_refuses_before_retaining_response_blocks(self):
        from test_pad_us_protection_mechanism_category import (
            fixture_mode, make_responder, _fixture_rows, _fixture_layer_fields,
        )
        schema_sha = pad._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, make_responder(_fixture_rows())), \
                 patch.object(pad.shutil, "disk_usage",
                              return_value=SimpleNamespace(free=pad.MIN_FREE_BYTES)):
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                    pad.fetch_pad_us_protection_mechanism_category_pages(root)
            self.assertTrue((root / "service_doc.receipt.json").exists())
            self.assertEqual(sum(path.stat().st_size for path in root.iterdir()
                                 if path.name.endswith((".body", ".wire"))), 0)

    def test_adaptive_pages_keep_receipts_and_read_bodies_from_disk(self):
        from dagster_hifld.sources.arcgis_adaptive import SliceCaptureSpec, capture_offset_pages
        from dagster_hifld.sources.arcgis_capture import RetainedCaptureView

        def capture(client, url, root, name, **policy):
            body = name.encode()
            (root / f"{name}.body").write_bytes(body)
            return body, {"bytes": len(body), "wire_bytes": 0}

        spec = SliceCaptureSpec(byte_budget=1000,
            page_name=lambda offset, count: f"page_{offset}_{count}",
            page_url=lambda offset, count: f"https://producer.invalid/{offset}/{count}",
            capture=capture, producer_error=lambda body: False,
            page_attempts=1, page_delay=0, doc_attempts=1, doc_delay=0)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            captures = capture_offset_pages(spec, None, root, 4, page_size=2)
            self.assertIsInstance(captures, RetainedCaptureView)
            self.assertEqual(set(captures), {"page_0_2", "page_2_2"})
            self.assertEqual(captures["page_0_2"][0], b"page_0_2")
            (root / "page_0_2.body").write_bytes(b"loaded from disk")
            self.assertEqual(captures["page_0_2"][0], b"loaded from disk")
            self.assertEqual(captures.receipts["page_2_2"], {"bytes": 8, "wire_bytes": 0})
