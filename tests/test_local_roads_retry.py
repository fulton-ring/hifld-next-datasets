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
