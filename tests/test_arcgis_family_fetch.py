"""Family retry adapters declare policies while shared owners retain network evidence."""
import ast
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.sources import arcgis_layer as shared


FETCH_ADAPTERS = '''coastlines_usace_ienc hf_areas_usace_ienc land_region_areas_usace_ienc
land_surface_areas_usace_ienc lock_basins_usace_ienc lock_gate_lines_usace_ienc mexico_us_border
usace_conveyor_areas usace_crane_locations usace_data_quality_areas usace_depth_areas
usace_dumping_ground_areas usace_floating_docks usace_hulke_areas usace_lakes
usace_landmark_areas usace_navigation_lights usace_pipeline_areas usace_pontoon_areas'''.split()
BRACKET_ADAPTERS = ['boem_ocs_lease_blocks', 'submerged_land_act_boundary', 'usace_civil_works_divisions']


class FamilyFetchTests(unittest.TestCase):
    def test_each_family_keeps_failed_attempt_and_promotes_success_with_native_receipt(self):
        for name in FETCH_ADAPTERS:
            with self.subTest(adapter=name), tempfile.TemporaryDirectory() as tmp:
                module = importlib.import_module('dagster_hifld.sources.' + name)
                status = 429 if name == 'usace_depth_areas' else 500
                responses = iter([(status, b'refused'), (200, b'{"count":1}')])
                calls = []
                def serve(request):
                    calls.append(str(request.url))
                    code, body = next(responses)
                    return httpx.Response(code, headers={'Retry-After': '0'}, stream=httpx.ByteStream(body))
                root = Path(tmp)
                budget = [0]
                with httpx.Client(transport=httpx.MockTransport(serve)) as client, patch.object(shared.time, 'sleep'):
                    args = (client, 'https://producer.test/layer', root, 'page')
                    body, receipt = module._fetch_retry(*args) if name == 'usace_navigation_lights' else module._fetch_retry(*args, budget)
                self.assertEqual(body, b'{"count":1}')
                self.assertEqual(len(calls), 2)
                self.assertEqual((root / 'page.attempt1.body').read_bytes(), b'refused')
                self.assertEqual((root / 'page.attempt1.wire').read_bytes(), b'refused')
                previous = json.loads((root / 'page.attempt1.receipt.json').read_bytes())
                self.assertEqual(previous['status'], status)
                self.assertTrue(previous['complete_body'])
                self.assertEqual(previous['sha256'], shared._sha(b'refused'))
                self.assertEqual(receipt['status'], 200)
                self.assertEqual(json.loads((root / 'page.receipt.json').read_bytes()), receipt)
                if name == 'usace_depth_areas':
                    self.assertEqual(previous['retry_after'], '0')
                if name != 'usace_navigation_lights':
                    self.assertEqual(budget[0], 2 * (len(b'refused') + len(body)))

    def test_retry_after_refuses_permanent_errors_and_unbounded_waits(self):
        module = importlib.import_module('dagster_hifld.sources.usace_depth_areas')
        for status, delay, error in [(404, '0', 'incomplete_source_capture'),
                                      (429, '61', 'retry_after_exceeds_bounded_wait'),
                                      (429, 'nan', 'retry_after_exceeds_bounded_wait')]:
            with self.subTest(status=status, delay=delay), tempfile.TemporaryDirectory() as tmp:
                calls = []
                def serve(request):
                    calls.append(request)
                    return httpx.Response(status, headers={'Retry-After': delay}, stream=httpx.ByteStream(b'refused'))
                with httpx.Client(transport=httpx.MockTransport(serve)) as client, patch.object(shared.time, 'sleep') as wait:
                    with self.assertRaisesRegex(ValueError, error):
                        module._fetch_retry(client, 'https://producer.test/layer', Path(tmp), 'page', [0])
                self.assertEqual(len(calls), 1)
                wait.assert_not_called()
                self.assertEqual((Path(tmp) / 'page.attempt1.body').read_bytes(), b'refused')

    def test_bracket_refusals_keep_witnesses_and_complete_body_predicate(self):
        for name in BRACKET_ADAPTERS:
            with self.subTest(adapter=name), tempfile.TemporaryDirectory() as tmp:
                module = importlib.import_module('dagster_hifld.sources.' + name)
                responses = iter([b'{"objectIds":null}', b'{"objectIds":[1]}'])
                def serve(request):
                    return httpx.Response(200, stream=httpx.ByteStream(next(responses)))
                root = Path(tmp)
                with httpx.Client(transport=httpx.MockTransport(serve)) as client, patch.object(shared.time, 'sleep'):
                    body, receipt = module._acquire_document(client, root, 'ids_before', 'https://producer.test/query')
                self.assertEqual(json.loads(body), {'objectIds': [1]})
                refused = list(root.glob('ids_before.error-*.body'))
                self.assertEqual(len(refused), 1)
                self.assertEqual(refused[0].read_bytes(), b'{"objectIds":null}')
                self.assertEqual(receipt['status'], 200)

    def test_bracket_transport_failure_is_not_retried(self):
        for name in BRACKET_ADAPTERS:
            with self.subTest(adapter=name), tempfile.TemporaryDirectory() as tmp:
                module = importlib.import_module('dagster_hifld.sources.' + name)
                calls = []
                def serve(request):
                    calls.append(request)
                    return httpx.Response(503, stream=httpx.ByteStream(b'refused'))
                with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                    with self.assertRaisesRegex(ValueError, 'incomplete_source_capture:ids_before'):
                        module._acquire_document(client, Path(tmp), 'ids_before', 'https://producer.test/query')
                self.assertEqual(len(calls), 1)
                self.assertEqual((Path(tmp) / 'ids_before.body').read_bytes(), b'refused')

    def test_assigned_fetch_and_retry_wrappers_have_no_transport_or_retry_implementation(self):
        root = Path(shared.__file__).parent
        for name in FETCH_ADAPTERS + BRACKET_ADAPTERS:
            with self.subTest(adapter=name):
                tree = ast.parse((root / (name + '.py')).read_text())
                functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                             and n.name in {'_fetch', '_fetch_retry', '_acquire_document'}]
                self.assertTrue(functions)
                for function in functions:
                    self.assertFalse(any(isinstance(n, (ast.For, ast.While, ast.Try, ast.With))
                                         for n in ast.walk(function)), function.name)
