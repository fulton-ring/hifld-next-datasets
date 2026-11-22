"""Local-road retries retain failures while native source validation owns HTTP-200 errors."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.sources import local_roads_census as source
from dagster_hifld.sources import arcgis_layer as shared


class LocalRoadRetryTests(unittest.TestCase):
    def test_real_failure_then_success_preserves_failed_attempt(self):
        replies = iter([(500, b'refused'), (200, b'{"count":6}')])
        calls = []
        def serve(request):
            calls.append(request)
            status, body = next(replies)
            return httpx.Response(status, stream=httpx.ByteStream(body))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with httpx.Client(transport=httpx.MockTransport(serve)) as client, patch.object(shared.time, 'sleep'):
                body, receipt = source._capture(client, 'https://producer.test/query', root, 'count_before', max_bytes=10000)
            self.assertEqual(body, b'{"count":6}')
            self.assertEqual(receipt['status'], 200)
            self.assertEqual(len(calls), 2)
            self.assertEqual((root / 'count_before.attempt-01.body').read_bytes(), b'refused')
            self.assertEqual((root / 'count_before.attempt-01.wire').read_bytes(), b'refused')
            prior = json.loads((root / 'count_before.attempt-01.receipt.json').read_bytes())
            self.assertEqual(prior['status'], 500)
            self.assertEqual(prior['sha256'], source._sha(b'refused'))

    def test_complete_200_error_is_returned_without_retry(self):
        calls = []
        def serve(request):
            calls.append(request)
            return httpx.Response(200, stream=httpx.ByteStream(b'{"error":{"code":500}}'))
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(serve)) as client:
            body, _receipt = source._capture(client, 'https://producer.test/query', Path(tmp), 'page', max_bytes=10000)
            self.assertEqual(json.loads(body), {'error': {'code': 500}})
            self.assertEqual(len(calls), 1)


class LocalRoadRequestStepTests(unittest.TestCase):
    def test_mid_page_failure_resumes_native_windows_and_always_refetches_closing_group(self):
        from test_local_roads_census import _documents, _receipt_for, PATCH, COUNT
        from dagster_hifld.sources.esri_common import canonical

        calls = []
        failed_page = 'page_0002'
        documents = None
        def capture(_client, url, root, name, *, max_bytes):
            calls.append(name)
            if name == failed_page:
                raise ValueError('incomplete_source_capture:' + name)
            body = canonical(documents[name])
            receipt = _receipt_for(name, url, body)
            (root / (name + '.body')).write_bytes(body)
            (root / (name + '.wire')).write_bytes(body)
            (root / (name + '.receipt.json')).write_bytes(canonical(receipt))
            return body, receipt

        with tempfile.TemporaryDirectory() as tmp, patch.multiple(source, **PATCH), \
                patch.object(source, '_TRANSIENT_RETRY_DELAYS', (0.0, 0.0)), \
                patch.object(shared, 'capture_get', side_effect=capture):
            root = Path(tmp)
            documents = _documents()
            with self.assertRaisesRegex(ValueError, 'incomplete_source_capture:page_0002'):
                source._capture_attempt(root)
            progress = json.loads((root / 'capture_manifest.json').read_bytes())
            self.assertEqual(progress['error_code'], 'ValueError')
            self.assertEqual(set(progress['receipts']), set(source._before_names(COUNT)) | {'page_0000', 'page_0001'})
            self.assertEqual(calls.count('page_0002'), 3)

            failed_page = None
            calls.clear()
            # The retained count_before, rather than a hypothetical fresh answer,
            # still determines exactly which numeric ID windows are resumed.
            documents['count_before'] = {'count': 999}
            receipts = source._capture_attempt(root)
            self.assertEqual(calls, ['page_0002', *source._after_names(COUNT)])
            self.assertEqual(set(receipts), source._capture_names(COUNT))
            checkpoint = (root / 'capture_manifest.json').read_bytes()
            saved = json.loads(checkpoint)
            self.assertNotIn('error_code', saved)
            self.assertEqual(set(saved['receipts']), set(source._before_names(COUNT)) | {'page_0000', 'page_0001', 'page_0002'})

            calls.clear()
            source._capture_attempt(root)
            self.assertEqual(calls, list(source._after_names(COUNT)))
            self.assertEqual((root / 'capture_manifest.json').read_bytes(), checkpoint)

    def test_closing_failure_does_not_add_a_page_error_checkpoint(self):
        from test_local_roads_census import _documents, _receipt_for, PATCH
        from dagster_hifld.sources.esri_common import canonical

        documents = None
        def capture(_client, url, root, name, *, max_bytes):
            if name == 'count_after':
                raise ValueError('incomplete_source_capture:' + name)
            body = canonical(documents[name])
            receipt = _receipt_for(name, url, body)
            (root / (name + '.body')).write_bytes(body)
            (root / (name + '.wire')).write_bytes(body)
            (root / (name + '.receipt.json')).write_bytes(canonical(receipt))
            return body, receipt
        with tempfile.TemporaryDirectory() as tmp, patch.multiple(source, **PATCH), \
                patch.object(source, '_TRANSIENT_RETRY_DELAYS', ()), \
                patch.object(shared, 'capture_get', side_effect=capture):
            root = Path(tmp)
            documents = _documents()
            with self.assertRaisesRegex(ValueError, 'incomplete_source_capture:count_after'):
                source._capture_attempt(root)
            progress = json.loads((root / 'capture_manifest.json').read_bytes())
            self.assertNotIn('error_code', progress)
            self.assertTrue(all(not name.endswith('_after') and not name.startswith('roster_after_')
                                for name in progress['receipts']))
