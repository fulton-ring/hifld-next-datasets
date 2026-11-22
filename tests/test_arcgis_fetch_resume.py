"""Resumed fetch retries never replace an earlier attempt's physical evidence."""
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from dagster_hifld.sources import arcgis_layer as shared


class FetchResumeEvidenceTests(unittest.TestCase):
    def test_resume_uses_ordinal_after_highest_partial_attempt_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = {'page.attempt1.body': b'previous-body',
                        'page.attempt3.wire': b'previous-wire',
                        'page.attempt7.receipt.json': b'partial-receipt'}
            for name, body in previous.items():
                (root / name).write_bytes(body)
            responses = iter([(500, b'transient'), (200, b'{"count":1}')])
            calls = []
            def serve(request):
                calls.append(str(request.url))
                status, body = next(responses)
                return httpx.Response(status, stream=httpx.ByteStream(body))
            budget = [len(previous['page.attempt1.body']) + len(previous['page.attempt3.wire'])]
            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                body, receipt = shared.fetch_capture_retry(client, 'https://producer.test/query', root, 'page',
                    budget, max_capture_bytes=10000000, max_body_bytes=10000)
            self.assertEqual(body, b'{"count":1}')
            self.assertEqual(len(calls), 2)
            for name, old_body in previous.items():
                self.assertEqual((root / name).read_bytes(), old_body, name)
            self.assertEqual((root / 'page.attempt8.body').read_bytes(), b'transient')
            self.assertEqual((root / 'page.attempt8.wire').read_bytes(), b'transient')
            refused = json.loads((root / 'page.attempt8.receipt.json').read_bytes())
            self.assertEqual(refused['status'], 500)
            self.assertEqual(refused['sha256'], shared._sha(b'transient'))
            self.assertEqual(json.loads((root / 'page.receipt.json').read_bytes()), receipt)
            self.assertEqual(receipt['status'], 200)
            self.assertFalse((root / 'page.attempt9.body').exists())

    def test_resumed_exhaustion_keeps_all_new_and_prior_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'page.attempt4.receipt.json').write_bytes(b'prior-receipt')
            with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
                    503, stream=httpx.ByteStream(b'unavailable')))) as client:
                with self.assertRaisesRegex(ValueError, 'incomplete_source_capture:page.attempt6'):
                    shared.fetch_capture_retry(client, 'https://producer.test/query', root, 'page', [0],
                        max_capture_bytes=10000000, max_body_bytes=10000)
            self.assertEqual((root / 'page.attempt4.receipt.json').read_bytes(), b'prior-receipt')
            for ordinal in (5, 6):
                self.assertEqual((root / f'page.attempt{ordinal}.body').read_bytes(), b'unavailable')
                self.assertEqual(json.loads((root / f'page.attempt{ordinal}.receipt.json').read_bytes())['status'], 503)
