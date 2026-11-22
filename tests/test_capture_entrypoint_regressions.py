"""Native entrypoints retain complete feature evidence before refusing closing drift."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fuds_public_properties as fuds
from dagster_hifld.sources import wreck_areas_usace_ienc, wrecks_usace_ienc
import test_fuds_public_properties as fuds_fixtures
import test_wreck_areas_usace_ienc as areas_fixtures
import test_wrecks_usace_ienc as wrecks_fixtures


class CaptureEntrypointRegressions(unittest.TestCase):
    def _capture_closing_drift(self, source, entrypoint, documents, plan, page_name, error):
        requests = []
        responses = {}
        for name, url in plan:
            responses.setdefault(str(httpx.URL(url)), []).append(source._canonical(documents[name]))
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
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            capture_root = Path(tmp) / 'captures'
            with patch.object(source.httpx, 'Client', side_effect=client_factory):
                with self.assertRaisesRegex(ValueError, error):
                    entrypoint(storage, version='native-entrypoint', capture_directory=str(capture_root))
            pages = [r for r in requests if r.url.params.get('objectIds') or 'resultOffset' in r.url.params]
            self.assertEqual(len(pages), 1)
            if page_name == 'features':
                self.assertEqual(pages[0].url.params['objectIds'], ','.join(map(str, documents['ids_before']['objectIds'])))
            else:
                self.assertEqual(pages[0].url.params['resultOffset'], '0')
                self.assertEqual(pages[0].url.params['resultRecordCount'], str(source.PAGE_SIZE))
            retained = list(capture_root.rglob(page_name + '.body'))
            self.assertEqual(len(retained), 1)
            self.assertEqual(json.loads(retained[0].read_bytes()), documents[page_name])
            self.assertTrue(retained[0].with_suffix('.wire').is_file())
            receipt = json.loads(retained[0].with_suffix('.receipt.json').read_bytes())
            self.assertTrue(receipt['complete_body'])
            self.assertTrue(storage.list_prefix('_quarantine/' + source.DATASET_SLUG))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])

    def _wreck(self, source, entrypoint, fixture):
        documents = fixture._documents()
        ids = documents['ids_before']['objectIds']
        documents['count_after']['count'] += 1
        seed_url = source._query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
        plan = [('ids_before', seed_url), *[(name, url) for name, url in source._capture_urls(ids) if name != 'ids_before']]
        self._capture_closing_drift(source, entrypoint, documents, plan, 'features', 'source_count_or_id_roster_drift')

    def test_wreck_areas_live_capture_uses_one_native_full_id_query(self):
        self._wreck(wreck_areas_usace_ienc, wreck_areas_usace_ienc.capture_wreck_areas_usace_ienc,
                    areas_fixtures.WreckAreasUsaceIencTests())

    def test_wrecks_live_capture_uses_one_native_full_id_query(self):
        self._wreck(wrecks_usace_ienc, wrecks_usace_ienc.capture_wrecks_usace_ienc,
                    wrecks_fixtures.WrecksUsaceIencTests())

    def test_fuds_live_capture_preserves_residual_per_response_budget(self):
        fields = fuds_fixtures._fields()
        ids = fuds_fixtures.IDS
        documents = fuds_fixtures._documents(ids, fuds_fixtures._features(), fields, len(ids))
        documents['count_after']['count'] += 1
        seed_url = fuds.LAYER_URL + '/query?' + urlencode({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
        plan = [('ids_seed', seed_url), *fuds._feature_urls(ids)]
        native_validate = fuds._validate_captures
        schema_sha = fuds._sha(fuds._canonical([fuds._plain_field(field) for field in fields]))
        with patch.object(fuds, 'MAX_TOTAL_CAPTURE_BYTES', 32_000), patch.object(fuds, 'capture_page', wraps=fuds.capture_page) as fetch:
            with patch.object(fuds, '_validate_captures', side_effect=lambda captures: native_validate(captures, expected_schema_sha256=schema_sha)):
                self._capture_closing_drift(fuds, fuds.capture_fuds_public_properties, documents, plan, 'page_000', 'source_count_drift')
            caps = [call.kwargs['page_max_bytes'] for call in fetch.call_args_list]
            self.assertEqual(caps[0], fuds.MAX_RESPONSE_BYTES)
            self.assertTrue(all(0 < cap < 16_000 for cap in caps[1:]))
            self.assertTrue(all(before > after for before, after in zip(caps[1:], caps[2:])))
