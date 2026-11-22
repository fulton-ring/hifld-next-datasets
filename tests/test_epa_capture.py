"""Offline behavior checks for the shared EPA acquisition owner."""
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
from dagster_hifld.sources.capture_common import capture_get
from dagster_hifld.sources.epa_capture import (
    OffsetCaptureSpec, capture_id_windows, capture_offset_pages,
)


class EPACaptureTests(unittest.TestCase):
    def offset_spec(self, **overrides):
        return OffsetCaptureSpec(
            table_url="https://example.invalid/FeatureServer/2", page_size=2,
            page_name=lambda number: f"page_{number:03d}", error=ValueError,
            query={"where": "1=1", "outFields": "OBJECTID", "returnGeometry": "false"},
            **overrides,
        )

    def test_offset_capture_is_lazy_and_retains_refused_page_before_validation(self):
        calls = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = RetainedCaptureView(root)

            def respond(request):
                offset = int(parse_qs(request.url.query.decode())["resultOffset"][0])
                calls.append(offset)
                rows = [{"attributes": {"OBJECTID": offset + 7}}]
                if offset == 0:
                    rows.append({"attributes": {"OBJECTID": 20}})
                return httpx.Response(200, stream=httpx.ByteStream(
                    json.dumps({"features": rows, "exceededTransferLimit": True}).encode()))

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                def request(name, url, params):
                    target = str(client.build_request("GET", url, params=params).url)
                    evidence[name] = capture_get(client, target, root, name, max_bytes=10000)
                    return json.loads(evidence[name][0])

                pages = capture_offset_pages(self.offset_spec(), 3, request=request)
                self.assertEqual(calls, [])
                first = next(pages)
                self.assertEqual(calls, [0])
                self.assertEqual([row["attributes"]["OBJECTID"] for row in first.features], [7, 20])
                with self.assertRaisesRegex(ValueError, "source_final_page_exceeded_transfer_limit"):
                    next(pages)
                self.assertEqual(calls, [0, 2])
                self.assertIn("page_001", evidence)
                self.assertTrue((root / "page_001.wire").is_file())
                self.assertTrue(evidence["page_001"][1]["complete_body"])

    def test_full_final_transfer_limit_preserves_layer_policy(self):
        page = {"features": [{}, {}], "exceededTransferLimit": True}
        self.assertEqual(len(list(capture_offset_pages(
            self.offset_spec(), 2, request=lambda *_: page))), 1)
        with self.assertRaisesRegex(ValueError, "source_final_page_exceeded_transfer_limit"):
            list(capture_offset_pages(self.offset_spec(reject_full_final_limit=True), 2,
                                      request=lambda *_: page))

    def test_incomplete_page_retains_protocol_evidence_and_stops_before_next_request(self):
        retained = []
        with self.assertRaisesRegex(ValueError, "source_page_row_count_incomplete"):
            list(capture_offset_pages(
                self.offset_spec(), 5, request=lambda *_: {"features": []},
                retain=lambda name, params, response: retained.append((name, params))))
        self.assertEqual([name for name, _ in retained], ["page_000"])

    def test_numeric_id_windows_preserve_gaps_and_stop_at_native_maximum(self):
        calls = []

        def respond(request):
            where = parse_qs(request.url.query.decode())["where"][0]
            calls.append(where)
            low, high = (int(part.split()[-1]) for part in where.split("AND"))
            return httpx.Response(200, json={"objectIds": [i for i in [2, 99, 205]
                                                          if low < i <= high]})

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            ids = list(capture_id_windows(
                maximum_id=205, window_size=100,
                table_url="https://example.invalid/FeatureServer/2",
                request=lambda url, params: client.get(url, params=params),
                payload=lambda response: response.json(), error=ValueError))
        self.assertEqual(ids, [2, 99, 205])
        self.assertEqual(calls, ["OBJECTID > 0 AND OBJECTID <= 100",
                                 "OBJECTID > 100 AND OBJECTID <= 200",
                                 "OBJECTID > 200 AND OBJECTID <= 205"])

    def test_numeric_id_window_rejects_duplicates_order_and_out_of_bounds(self):
        for ids, reason in [([2, 2], "duplicate_or_unordered"),
                            ([5, 2], "order_or_bounds_changed"),
                            ([101], "order_or_bounds_changed"),
                            ([True], "source_id_window_invalid")]:
            with self.subTest(ids=ids), self.assertRaisesRegex(ValueError, reason):
                list(capture_id_windows(
                    maximum_id=100, window_size=100, table_url="https://example.invalid/2",
                    request=lambda *_: {"objectIds": ids}, payload=lambda value: value,
                    error=ValueError))

    def test_invalid_page_and_window_bounds_fail_before_requests(self):
        request = lambda *_: self.fail("invalid bounds attempted a request")
        with self.assertRaisesRegex(ValueError, "source_page_size_invalid"):
            spec = OffsetCaptureSpec(table_url="https://example.invalid/2", page_size=0,
                                     query={}, page_name=str, error=ValueError)
            list(capture_offset_pages(spec, 2, request=request))
        with self.assertRaisesRegex(ValueError, "source_id_window_size_invalid"):
            list(capture_id_windows(maximum_id=2, window_size=0,
                                    table_url="https://example.invalid/2", request=request,
                                    payload=lambda value: value, error=ValueError))
