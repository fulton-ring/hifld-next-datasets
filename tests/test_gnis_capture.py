"""Shared GNIS acquisition preserves checkpoints and producer witnesses."""
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import httpx

from dagster_hifld.sources import arcgis_adaptive
from dagster_hifld.sources import gnis_historical_cultural_political_points as source
from test_gnis_historical_cultural_political_points import (
    FIXTURE_OBJECTIDS, JSONStream, _fixture_rows, make_responder,
)


class GnisCaptureTests(unittest.TestCase):
    def test_shared_capture_persists_pages_and_replays_without_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with httpx.Client(transport=make_responder(_fixture_rows())) as client:
                result = arcgis_adaptive.capture_adaptive_layer(source._capture_spec(), root,
                                                         client=client)
                self.assertFalse(client.is_closed)
            self.assertEqual(result.roster, list(FIXTURE_OBJECTIDS))
            state = json.loads((root / "capture_state.json").read_bytes())
            self.assertTrue(state["complete"])
            self.assertEqual(sum(page["count"] for page in state["pages"]),
                             len(FIXTURE_OBJECTIDS))
            retained = {path.name: path.read_bytes() for path in root.glob("*.body")}

            def offline(request):
                self.fail(f"retained capture unexpectedly fetched {request.url}")

            with httpx.Client(transport=httpx.MockTransport(offline)) as client:
                replay = arcgis_adaptive.capture_adaptive_layer(source._capture_spec(), root,
                                                         client=client)
                self.assertFalse(client.is_closed)
            self.assertEqual(replay.roster, result.roster)
            self.assertEqual(replay.pages, result.pages)
            self.assertEqual(retained,
                             {path.name: path.read_bytes() for path in root.glob("*.body")})

    def test_closing_roster_drift_retains_evidence_without_marking_complete(self):
        delegate = make_responder(_fixture_rows())
        roster_requests = 0

        def respond(request):
            nonlocal roster_requests
            if request.url.params.get("returnIdsOnly") == "true":
                roster_requests += 1
                if roster_requests == 3:
                    return httpx.Response(200, stream=JSONStream({
                        "objectIdFieldName": "OBJECTID",
                        "objectIds": list(FIXTURE_OBJECTIDS[:-1]),
                    }))
            return delegate.handle_request(request)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                with self.assertRaisesRegex(source.GnisHistoricalCulturalPoliticalPointsError,
                                            "source_id_population_drift:ids_after"):
                    arcgis_adaptive.capture_adaptive_layer(source._capture_spec(), root,
                                                    client=client)
                self.assertFalse(client.is_closed)
            self.assertTrue((root / "ids_after.receipt.json").exists())
            self.assertTrue(list(root.glob("page_*.body")))
            state = json.loads((root / "capture_state.json").read_bytes())
            self.assertFalse(state["complete"])

    def test_invalid_discovery_wraps_error_and_closes_owned_client(self):
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=JSONStream(b"invalid json"))))
        spec = replace(source._capture_spec(), default_client=lambda: client)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(source.GnisHistoricalCulturalPoliticalPointsError,
                                        "source_capture_failed:JSONDecodeError"):
                arcgis_adaptive.capture_adaptive_layer(spec, root)
            self.assertTrue(client.is_closed)
            self.assertTrue((root / "ids_seed.receipt.json").exists())
            self.assertFalse((root / "capture_state.json").exists())

    def test_capture_budget_is_enforced_before_streamed_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = replace(source._capture_spec(), byte_budget=1,
                           doc_attempts=1, doc_delay=0)
            with httpx.Client(transport=make_responder(_fixture_rows())) as client:
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                    arcgis_adaptive.capture_adaptive_layer(spec, root, client=client)
            self.assertTrue((root / "service_before.receipt.json").exists())
            self.assertLessEqual(sum(path.stat().st_size for path in root.iterdir()
                                     if path.name.endswith((".body", ".wire"))), 1)
