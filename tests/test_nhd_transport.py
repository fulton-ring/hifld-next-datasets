"""Retained failure and producer-host rescue contracts for NHD transport."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from dagster_hifld.sources import arcgis_layer, nhd_transport


class TransportError(ValueError):
    pass


class NhdTransportTests(unittest.TestCase):
    def test_document_retry_retains_every_previous_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempts = []

            def fetch(client, url, root, name, **kwargs):
                attempts.append(name)
                (root / f"{name}.body").write_bytes(str(len(attempts)).encode())
                (root / f"{name}.receipt.json").write_text('{}')
                if len(attempts) < 3:
                    raise ValueError("incomplete_source_capture:reset")
                return b"3", {}

            result = nhd_transport.capture_document(
                None, "url", root, "doc", [0], attempts=3, base_delay=0,
                max_body_bytes=100, check_budget=Mock(), retain_budget=lambda budget: None,
                fetch=fetch, sleep=Mock(), retry_http_errors=True)
            self.assertEqual(result, (b"3", {}))
            self.assertEqual((root / "doc.attempt-01.body").read_bytes(), b"1")
            self.assertEqual((root / "doc.attempt-02.body").read_bytes(), b"2")
            self.assertEqual((root / "doc.body").read_bytes(), b"3")

    def test_waterbody_doc_retry_witnesses_do_not_enter_candidate_inputs(self):
        from dagster_hifld.sources import nhd_waterbody, nhd_waterbody_large_scale
        for source in (nhd_waterbody, nhd_waterbody_large_scale):
            with self.subTest(source=source.__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                attempts = []

                def fetch(client, url, root, name, **kwargs):
                    attempts.append(name)
                    (root / f"{name}.body").write_bytes(b"{}")
                    (root / f"{name}.wire").write_bytes(b"{}")
                    receipt = {"method": "GET", "url": url, "final_url": url,
                               "status": 200, "complete_body": len(attempts) > 1}
                    (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
                    if len(attempts) == 1:
                        raise ValueError("incomplete_source_capture:doc")
                    return b"{}", receipt

                with patch.object(source, "capture_get", side_effect=fetch), \
                        patch.object(source, "DOC_RETRY_BASE_DELAY_SECONDS", 0):
                    source._capture_doc(None, "url", root, "doc", [0])
                self.assertEqual(list(source._load_receipt_map(root)), ["doc"])
                evidence = list(root.glob("doc.error-*.receipt.json"))
                self.assertEqual(len(evidence), 1)
                self.assertFalse(json.loads(evidence[0].read_text())["complete_body"])

    def test_document_http_retry_policy_preserves_area_exception_behavior(self):
        for retry_http, expected in [(False, 1), (True, 2)]:
            with self.subTest(retry_http=retry_http), tempfile.TemporaryDirectory() as directory:
                fetch = Mock(side_effect=httpx.ReadError("reset"))
                with self.assertRaises(httpx.ReadError):
                    nhd_transport.capture_document(
                        None, "url", Path(directory), "doc", [0], attempts=2, base_delay=0,
                        max_body_bytes=100, check_budget=Mock(), retain_budget=lambda budget: None,
                        fetch=fetch, sleep=Mock(), retry_http_errors=retry_http)
                self.assertEqual(fetch.call_count, expected)

    def test_page_body_limit_is_witnessed_and_returns_to_bisection_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "page.receipt.json").write_text('{"error_code":"wire_limit_exceeded"}')
            (root / "page.body").write_bytes(b"partial")
            fetch = Mock(side_effect=ValueError("incomplete_source_capture:page"))
            with self.assertRaisesRegex(TransportError, "source_page_body_limit:page"):
                nhd_transport.fetch_keyset_page(
                    None, root, [0], "page", "url", attempts=3, base_delay=0,
                    max_body_bytes=100, check_budget=Mock(), retain_budget=lambda budget: None,
                    fetch=fetch, witness=arcgis_layer.witness_aside,
                    producer_error=arcgis_layer.producer_error_doc, sleep=Mock(), error_type=TransportError)
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(len(list(root.glob("page.error-*.body"))), 1)
            self.assertFalse((root / "page.body").exists())

    def test_budget_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            budget = [0]

            def fetch(client, url, root, name, **kwargs):
                kwargs["before_retain"]("wire", b"over")

            sleep = Mock()
            with self.assertRaisesRegex(TransportError, "source_capture_budget_exceeded"):
                nhd_transport.capture_document(
                    None, "url", Path(directory), "doc", budget, attempts=3, base_delay=0,
                    max_body_bytes=100, check_budget=lambda budget: nhd_transport.budget_check(budget, 1, TransportError),
                    retain_budget=lambda budget: nhd_transport.budget_guard(budget, 1, TransportError),
                    fetch=fetch, sleep=sleep, retry_http_errors=True)
            self.assertEqual(budget, [4])
            sleep.assert_not_called()

    def rescue(self, root, fetch):
        pages, calls = [], []
        result = nhd_transport.rescue_single_record(
            "primary", root, [0], 6, pages, lambda: calls.append("persist"),
            attrs_url="attrs", geometry_hosts=lambda oid: (
                ("primary", "primary", f"primary/{oid}"), ("alias", "alias", f"alias/{oid}")),
            capture_attrs=lambda *args: ({"OBJECTID": 7}, {}),
            retained_or_fetch=lambda *args, **kwargs: (b'{"features":[{"attributes":{"OBJECTID":7}}]}', {}),
            page_name_for=lambda last, size: "accepted", attempts=2, base_delay=0,
            max_body_bytes=100, check_budget=Mock(), retain_budget=lambda budget: None,
            fetch=fetch, witness=arcgis_layer.witness_aside,
            producer_error=arcgis_layer.producer_error_doc, sleep=Mock(), error_type=TransportError)
        return result, pages, calls

    def test_alias_rescue_preserves_served_bytes_and_original_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            urls = []
            body = b'{ "features" : [{"attributes":{"OBJECTID":7}}] }\n'

            def fetch(client, url, root, name, **kwargs):
                urls.append(url)
                served = body if client == "alias" else b'{"error":{"code":500}}'
                receipt = {"observed_at": "then", "url": url}
                (root / f"{name}.body").write_bytes(served)
                (root / f"{name}.wire").write_bytes(served)
                (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
                return served, receipt

            result, pages, calls = self.rescue(root, fetch)
            self.assertEqual(result, (7, True))
            self.assertEqual(urls, ["primary/7", "alias/7"])
            self.assertEqual((root / "accepted.body").read_bytes(), body)
            self.assertEqual(json.loads((root / "accepted.receipt.json").read_text())["url"], "alias/7")
            self.assertEqual(pages[0]["rescued_host"], "alias")
            self.assertEqual(calls, ["persist"])
            self.assertEqual(len(list(root.glob("refused_probe_000000007_geo_primary.error-*.body"))), 1)

    def test_both_hosts_refusing_never_append_an_accepted_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def fetch(client, url, root, name, **kwargs):
                body = b'{"error":{"code":500}}'
                (root / f"{name}.body").write_bytes(body)
                return body, {}

            result, pages, calls = self.rescue(root, fetch)
            self.assertEqual(result, (7, False))
            self.assertEqual(pages, [])
            self.assertEqual(calls, [])
            self.assertEqual(len(list(root.glob("*.error-*.body"))), 4)


if __name__ == "__main__":
    unittest.main()
