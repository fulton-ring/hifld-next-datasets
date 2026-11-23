import json
import tempfile
import unittest
from pathlib import Path

import httpx
from dagster_hifld.sources import arcgis_capture_sequence as common
from dagster_hifld.sources.capture_common import capture_get


class CaptureSequenceTests(unittest.TestCase):
    def test_invalid_derived_plan_retains_discovery_without_requesting_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seen = []
            def respond(request):
                seen.append(str(request.url))
                return httpx.Response(200, stream=httpx.ByteStream(b'{"objectIds":[1]}'))
            captures = {}
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                def fetch(name, url):
                    return capture_get(client, url, root, name)
                with self.assertRaisesRegex(ValueError, 'capture_roster_name_duplicate'):
                    common.capture_request_sequence(captures,
                        [('ids', 'https://example.test/ids')],
                        lambda captures: [('page', 'https://example.test/one'),
                                          ('page', 'https://example.test/two')], fetch)
            self.assertEqual(seen, ['https://example.test/ids'])
            self.assertEqual(json.loads(captures['ids'][0]), {'objectIds': [1]})
            self.assertTrue((root / 'ids.receipt.json').is_file())

    def test_cursor_records_failed_request_and_enforces_receipt_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def respond(request):
                return httpx.Response(200, stream=httpx.ByteStream(b'{}'))
            cursor = common.LayerCaptureFetch(max_body_bytes=100, max_capture_bytes=3)
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                    cursor.fetch(client, 'https://example.test/page', root, 'page')
            retained = sum(p.stat().st_size for p in root.iterdir() if p.suffix in {'.body', '.wire'})
            self.assertLessEqual(retained, 3)
            self.assertEqual(cursor.current_name, 'page')
            self.assertEqual(cursor.current_url, 'https://example.test/page')
            self.assertTrue((root / 'page.receipt.json').is_file())

    def test_source_steps_validate_before_issuing_the_next_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seen = []
            def respond(request):
                seen.append(str(request.url))
                return httpx.Response(200, stream=httpx.ByteStream(b'{"id":9}'))
            def steps():
                body, receipt = yield ('layer', 'https://example.test/layer')
                if json.loads(body)['id'] != 1:
                    raise ValueError('source_layer_identity_drift')
                yield ('page', 'https://example.test/page')
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                with self.assertRaisesRegex(ValueError, 'source_layer_identity_drift'):
                    common.capture_request_steps(steps(),
                        lambda name, url: capture_get(client, url, root, name))
            self.assertEqual(seen, ['https://example.test/layer'])
            self.assertTrue((root / 'layer.receipt.json').is_file())

    def test_retry_retains_refused_native_receipt_before_success(self):
        from dagster_hifld.resources import StagingStorageResource
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'capture'; root.mkdir()
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            statuses = iter((503, 200))
            def respond(request):
                return httpx.Response(next(statuses), stream=httpx.ByteStream(b'{}'))
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                body, receipt = common.retry_capture_request(
                    lambda attempt: capture_get(client, 'https://example.test/page', root, 'page'),
                    attempts=2, schedule=(), retryable=lambda exc, attempt: True,
                    preserve_failed=lambda exc, attempt: common.quarantine_retry_response(
                        storage, root, 'fixture', 'v1', 'page', type(exc).__name__))
            self.assertEqual(receipt['status'], 200)
            self.assertEqual(body, b'{}')
            evidence = storage.list_prefix('_quarantine/fixture')
            retained = [key for key in evidence if key.endswith('page.receipt.json')]
            self.assertEqual(len(retained), 1)
            self.assertEqual(json.loads(storage.read_key(retained[0]))['status'], 503)

    def test_ordinary_adapters_use_owned_request_drivers(self):
        import ast
        source_root = Path(common.__file__).parent
        names = ('bts_intermodal_family', 'bts_pipeline_terminals',
                 'epa_tsca', 'final_critical_habitat', 'proposed_critical_habitat',
                 'national_transit_map_agencies', 'national_transit_map_routes',
                 'national_transit_map_stops', 'fema_nss_shelter_locations',
                 'fema_regional_offices', 'generalized_units', 'historical_tsunami_events',
                 'natural_gas_border_crossings', 'significant_volcanic_events',
                 'red_cross_headquarters', 'usace_civil_works_divisions')
        owners = {'capture_layer', 'capture_request_sequence', 'capture_request_steps'}
        sinks = {'capture_get', 'fetch_capture_retry', 'fetch_one', '_fetch', '_request',
                 'capture_one', 'capture_with_retry', '_acquire_document', 'get', 'read'}
        for name in names:
            with self.subTest(source=name):
                tree = ast.parse((source_root / f'{name}.py').read_text())
                calls = {n.func.id for n in ast.walk(tree)
                         if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
                self.assertTrue(calls & owners)
                violations = [n.lineno for n in ast.walk(tree)
                    if isinstance(n, (ast.For, ast.While, ast.ListComp, ast.DictComp))
                    and any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                            and c.func.id in sinks for c in ast.walk(n))]
                self.assertEqual(violations, [])

    def test_retained_view_reads_a_source_preserved_response_path(self):
        from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            refused = root / 'ids.error-123.body'
            refused.write_bytes(b'{"objectIds":null}')
            view = RetainedCaptureView(root)
            view['ids'] = (refused, {'bytes': refused.stat().st_size})
            self.assertEqual(view['ids'][0], refused.read_bytes())
            self.assertEqual(view.body_paths['ids'], refused)
