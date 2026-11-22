"""Real request-loop regressions for discovery before a feature population exists."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_airport_area, usace_navigation_marks_areas
from test_usace_airport_area import _base_captures
import test_usace_navigation_marks_areas as navigation_fixtures


class IencSeedDiscoveryTests(unittest.TestCase):
    def _closing_drift_after_real_pages(self, source, entrypoint, captures):
        ids = json.loads(captures['ids_seed'][0])['objectIds']
        captures['count_after'] = (json.dumps({'count': len(ids) + 1}).encode(), {})
        requests = []
        planned = [('ids_seed', source._query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})),
                   *source._capture_roster(ids)]
        responses = {}
        for name, url in planned:
            responses.setdefault(str(httpx.URL(url)), []).append(captures[name][0])
        counts = {}
        def respond(request):
            requests.append(request)
            url = str(request.url)
            index = counts.get(url, 0)
            counts[url] = index + 1
            return httpx.Response(200, stream=httpx.ByteStream(responses[url][index]))
        real_client = httpx.Client
        def client_factory(*args, **kwargs):
            return real_client(*args, transport=httpx.MockTransport(respond), **kwargs)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(local_dir=str(root / 'stage'))
            capture_root = root / 'captures'
            with patch.object(source.httpx, 'Client', side_effect=client_factory):
                with self.assertRaisesRegex(ValueError, 'source_count_or_id_roster_drift'):
                    entrypoint(storage, version='seed-discovery', capture_directory=str(capture_root))
            pages = [request for request in requests if request.url.params.get('objectIds')]
            self.assertEqual(len(pages), 1)
            self.assertEqual(pages[0].url.params['objectIds'], ','.join(map(str, ids)))
            retained = list(capture_root.rglob('page_000.body'))
            self.assertEqual(len(retained), 1)
            self.assertEqual(len(json.loads(retained[0].read_bytes())['features']), len(ids))
            self.assertTrue(retained[0].with_suffix('.wire').is_file())
            receipt = json.loads(retained[0].with_suffix('.receipt.json').read_bytes())
            self.assertTrue(receipt['complete_body'])
            self.assertTrue(storage.list_prefix('_quarantine/' + source.DATASET_SLUG))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])

    def test_airport_seed_is_discovered_before_pages_and_closing_drift_is_refused(self):
        self._closing_drift_after_real_pages(usace_airport_area,
            usace_airport_area.capture_usace_airport_area, _base_captures(list(range(1, 73))))

    def test_navigation_marks_seed_is_discovered_before_pages_and_closing_drift_is_refused(self):
        fixture = navigation_fixtures.UsaceNavigationMarksAreasTests('test_validates_complete_population_and_keeps_leading_zero_ids')
        self._closing_drift_after_real_pages(usace_navigation_marks_areas,
            usace_navigation_marks_areas.capture_usace_navigation_marks_areas, fixture._capture())
