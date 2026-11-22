import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.sources import arcgis_layer
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

    def test_seed_validation_failure_keeps_lazy_prefix_for_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            view = arcgis_layer.RetainedCaptureView(root)
            def reject(body):
                self.assertEqual(json.loads(body), {"objectIds": [1]})
                raise ValueError("source_seed_contract")
            def serve(request):
                return httpx.Response(200, stream=httpx.ByteStream(b'{"objectIds": [1]}'))
            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                with self.assertRaisesRegex(ValueError, "source_seed_contract"):
                    arcgis_layer.capture_layer('https://example.test/FeatureServer/0', root,
                        100_000, 2, client=client, captures=view, validate_seed=reject)
            self.assertEqual(list(view), ['ids_before'])
            self.assertTrue((root / 'ids_before.receipt.json').is_file())

    def test_non_page_named_population_and_probe_are_distinguished_by_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = 'https://example.test/FeatureServer/0'
            def roster(ids):
                planned = arcgis_layer.layer_capture_roster(ids, layer_url=base,
                    service_url=base.rsplit('/', 1)[0], page_size=2)
                return [(('features' if name == 'page_000' else name), url)
                        for name, url in planned]
            def serve(request):
                query = request.url.params
                if query.get('returnIdsOnly'):
                    doc = {'objectIds': [1]}
                elif query.get('returnCountOnly'):
                    doc = {'count': 1}
                elif query.get('outFields'):
                    doc = {'features': [{'attributes': {'OBJECTID': 1}}]}
                else:
                    doc = {}
                return httpx.Response(200, stream=httpx.ByteStream(json.dumps(doc).encode()))
            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                view = arcgis_layer.capture_layer(base, root, 100_000, 2,
                    client=client, roster=roster)
            self.assertIn('features', view)

    def test_changed_closing_witness_refuses_with_retained_documents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                self.run_capture(root, after_ids=(1, 3))
            self.assertTrue((root / 'ids_after.body').is_file())

    def test_metadata_after_validated_seed_precedes_metadata_selected_page_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = 'https://example.test/FeatureServer/0'
            view = arcgis_layer.RetainedCaptureView(root)
            requests = []

            def serve(request):
                requests.append(str(request.url))
                query = request.url.params
                if query.get('returnIdsOnly'):
                    doc = {'objectIds': [1]}
                elif query.get('returnCountOnly'):
                    doc = {'count': 1}
                elif query.get('outFields'):
                    doc = {'features': [{'attributes': {'OBJECTID': 1}}]}
                else:
                    doc = {'advancedQueryCapabilities': {'supportsPagination': True}}
                return httpx.Response(200, stream=httpx.ByteStream(json.dumps(doc).encode()))

            def roster(ids):
                layer = json.loads(view['layer_before'][0])
                self.assertTrue(layer['advancedQueryCapabilities']['supportsPagination'])
                return arcgis_layer.layer_capture_roster(ids, layer_url=base,
                    service_url=base.rsplit('/', 1)[0], page_size=2)

            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                arcgis_layer.capture_layer(base, root, 100_000, 2, client=client,
                    captures=view, roster=roster,
                    after_seed_documents=[('layer_before', base + '?f=pjson')])
            self.assertIn('returnIdsOnly=true', requests[0])
            self.assertEqual(requests[1], base + '?f=pjson')
            self.assertEqual(len(requests), 9)
            self.assertEqual(list(view)[:2], ['ids_before', 'layer_before'])

    def test_invalid_seed_never_fetches_following_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            view = arcgis_layer.RetainedCaptureView(root)
            requests = []

            def serve(request):
                requests.append(str(request.url))
                return httpx.Response(200, stream=httpx.ByteStream(b'{"objectIds": null}'))

            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                with self.assertRaisesRegex(ValueError, 'source_seed_id_population_drift'):
                    arcgis_layer.capture_layer('https://example.test/FeatureServer/0',
                        root, 100_000, 2, client=client, captures=view,
                        after_seed_documents=[('layer_before', 'https://example.test/metadata')])
            self.assertEqual(len(requests), 1)
            self.assertEqual(list(view), ['ids_before'])
            self.assertTrue((root / 'ids_before.receipt.json').is_file())
