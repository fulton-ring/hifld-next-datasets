"""Offline retry policy and retained evidence contracts shared by ArcGIS families."""
import ast
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from dagster_hifld.sources import arcgis_layer as source


class Body(httpx.SyncByteStream):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        yield self.data


class ArcgisRetryTests(unittest.TestCase):
    def test_refusal_predicate_preserves_family_integer_code_policy(self):
        self.assertTrue(source.producer_error_doc(b'{"error":null}'))
        for body in (b'{"error":null}', b'{"error":{"code":true}}', b'{"error":{"code":"500"}}'):
            self.assertFalse(source.producer_error_doc(body, max_bytes=4096, require_int_code=True))
        self.assertTrue(source.producer_error_doc(b'{"error":{"code":500}}', max_bytes=4096,
                                                   require_int_code=True))
        self.assertFalse(source.producer_error_doc(b'{"error":{"code":500}}', max_bytes=4,
                                                    require_int_code=True))

    def test_final_complete_error_body_returns_for_family_bisection(self):
        body = b'{"error":{"code":500}}'
        def respond(request):
            return httpx.Response(200, stream=Body(body), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            root = Path(tmp)
            result, receipt = source.capture_with_retry(client, 'https://example.org/query', root, 'page',
                                                       attempts=2, base_delay=0, max_bytes=1024)
            self.assertEqual(result, body)
            self.assertTrue(receipt['complete_body'])
            self.assertEqual((root / 'page.attempt-01.body').read_bytes(), body)
            self.assertEqual((root / 'page.body').read_bytes(), body)

    def test_resume_never_overwrites_prior_attempt_evidence(self):
        responses = iter([(500, b'first'), (200, b'{}')])
        def respond(request):
            status, body = next(responses)
            return httpx.Response(status, stream=Body(body), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            root = Path(tmp)
            for suffix in ('body', 'wire', 'receipt.json'):
                (root / f'page.attempt-01.{suffix}').write_bytes(b'previous-session')
            source.capture_with_retry(client, 'https://example.org/query', root, 'page',
                                      attempts=2, base_delay=0, max_bytes=1024)
            self.assertEqual((root / 'page.attempt-01.body').read_bytes(), b'previous-session')
            self.assertEqual((root / 'page.attempt-02.body').read_bytes(), b'first')
            self.assertEqual((root / 'page.body').read_bytes(), b'{}')

    def test_retained_budget_is_checked_again_before_retry(self):
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(500, stream=Body(b'failed'), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded_before_request'):
                source.capture_with_retry(client, 'https://example.org/query', root, 'page', attempts=3,
                                          base_delay=0, max_bytes=65536, max_total_bytes=131072)
            self.assertEqual(len(calls), 1)
            self.assertEqual((root / 'page.attempt-01.body').read_bytes(), b'failed')
            self.assertEqual(json.loads((root / 'page.attempt-01.receipt.json').read_bytes())['status'], 500)

    def test_strict_status_policy_retains_final_failure(self):
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(503, stream=Body(b'unavailable'), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'source_endpoint_retries_exhausted:page'):
                source.capture_with_retry(client, 'https://example.org/query', root, 'page', attempts=2,
                                          base_delay=0, max_bytes=65536, retryable_statuses=(503,),
                                          return_final_error=False, preserve_final_failure=True)
            self.assertEqual(len(calls), 2)
            self.assertEqual((root / 'page.attempt-01.body').read_bytes(), b'unavailable')
            self.assertEqual((root / 'page.attempt-02.body').read_bytes(), b'unavailable')

    def test_non_json_document_can_be_captured(self):
        body = b'<html>agency metadata</html>'
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=Body(body), request=request))) as client:
            result, _receipt = source.capture_with_retry(client, 'https://example.org/item', Path(tmp),
                                                        'inport_item', attempts=2, base_delay=0, max_bytes=1024,
                                                        retryable_statuses=(500,), return_final_error=False)
            self.assertEqual(result, body)


class RetryOwnershipTests(unittest.TestCase):
    def test_migrated_adapters_have_no_local_retry_or_evidence_bodies(self):
        root = Path(source.__file__).parent
        names = (
            'gnis_historical_cultural_political_points', 'gnis_historical_hydrographic_points',
            'gnis_historical_physical_points', 'gnis_other_hydrographic_features',
            'incorporated_places_civil_gnis', 'landforms_gnis', 'populated_places_gnis',
            'unincorporated_places_census_gnis', 'us_landslide_regions', 'tmas_stations',
            'wbd_hu8_subbasin', 'shipping_fairways', 'pad_us_manager_name', 'nfhl/capture',
            'railroad_bridges', 'runways', 'nfhl_lomas', 'railroad_grade_crossings',
            'nhd_area_large_scale', 'nhd_waterbody', 'nhd_waterbody_large_scale', 'wbd_hu_family',
            'roads_usace_ienc', 'secondary_roads_census',
        )
        forbidden = {'capture_with_retry', 'producer_error_doc', 'preserve_failed_attempt'}
        for name in names:
            with self.subTest(module=name):
                tree = ast.parse((root / (name + '.py')).read_text())
                duplicates = [node.name for node in ast.walk(tree)
                              if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                              and node.name.lstrip('_') in forbidden]
                self.assertEqual(duplicates, [])
                if name in {'roads_usace_ienc', 'secondary_roads_census'}:
                    capture = next(node for node in tree.body
                                   if isinstance(node, ast.FunctionDef) and node.name == '_capture')
                    self.assertFalse(any(isinstance(node, (ast.For, ast.While))
                                         for node in ast.walk(capture)))
