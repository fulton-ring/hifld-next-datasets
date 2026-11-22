import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.sources import arcgis_layer
from dagster_hifld.sources.capture_common import capture_get
import httpx


class LayerAcquisitionTests(unittest.TestCase):
    def run_capture(self, root, *, after_ids=(1, 2), page_ids=(1, 2), budget=100_000):
        requests = []
        def serve(request):
            requests.append(str(request.url))
            params = request.url.params
            if params.get('returnIdsOnly'):
                data = {'objectIds': [1, 2] if len(requests) == 1 else list(after_ids)}
            elif params.get('returnCountOnly'):
                data = {'count': 2}
            elif params.get('objectIds'):
                data = {'features': [{'attributes': {'OBJECTID': oid}} for oid in page_ids]}
            else:
                data = {}
            return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()))
        with httpx.Client(transport=httpx.MockTransport(serve)) as client:
            result = arcgis_layer.capture_layer('https://example.test/FeatureServer/0', root,
                                                budget, 2, client=client)
        return result, requests

    def test_discovery_pages_and_witnesses_return_lazy_retained_bodies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result, requests = self.run_capture(root)
            self.assertEqual(len(requests), 9)
            self.assertEqual(json.loads(result['page_000'][0])['features'][0]['attributes']['OBJECTID'], 1)
            self.assertEqual(result.body_paths['page_000'], root / 'page_000.body')
            (root / 'page_000.body').write_bytes(b'changed')
            self.assertEqual(result['page_000'][0], b'changed')

    def test_torn_page_population_retains_evidence_and_refuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                self.run_capture(root, page_ids=(1, 1))
            self.assertTrue((root / 'page_000.receipt.json').is_file())
            self.assertTrue((root / 'ids_after.body').is_file())

    def test_budget_refuses_before_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'budget'):
                self.run_capture(Path(tmp), budget=1)
            self.assertEqual(list(Path(tmp).iterdir()), [])
