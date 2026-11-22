"""Acquisition-wide disk limits remain binding through custom capture callbacks."""
import gzip
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from dagster_hifld.sources import arcgis_layer
from dagster_hifld.sources.capture_common import capture_get


class CaptureLayerBudgetTests(unittest.TestCase):
    def capture(self, root, budget, *, compressed=False, retry=False, large_seed=False):
        seen = []
        def serve(request):
            seen.append(str(request.url))
            query = request.url.params
            if retry and len(seen) == 1:
                return httpx.Response(500, stream=httpx.ByteStream(b'transient'))
            if query.get('returnIdsOnly'):
                document = {'objectIds': [1]}
            elif query.get('returnCountOnly'):
                document = {'count': 1}
            elif query.get('objectIds'):
                document = {'features': [{'attributes': {'OBJECTID': 1}}]}
            else:
                document = {}
            body = json.dumps(document).encode()
            if large_seed and len(seen) == 1:
                body += b' ' * 5000
            wire = gzip.compress(body, mtime=0) if compressed else body
            headers = {'Content-Encoding': 'gzip'} if compressed else {}
            return httpx.Response(200, headers=headers, stream=httpx.ByteStream(wire))
        def fetch(client, url, root, name):
            if retry:
                return arcgis_layer.capture_with_retry(client, url, root, name,
                    max_bytes=10000, attempts=2, base_delay=0)
            return capture_get(client, url, root, name, max_bytes=10000)
        with httpx.Client(transport=httpx.MockTransport(serve)) as client:
            result = arcgis_layer.capture_layer('https://example.test/FeatureServer/0', root,
                budget, 2, client=client, fetch=fetch)
        return result, seen

    @staticmethod
    def retained(root):
        return sum(path.stat().st_size for path in root.iterdir()
                   if path.name.endswith(('.body', '.wire')))

    def test_custom_fetch_cannot_bypass_total_disk_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                self.capture(root, 1)
            self.assertLessEqual(self.retained(root), 1)
            receipt = json.loads((root / 'ids_before.receipt.json').read_bytes())
            self.assertFalse(receipt['complete_body'])
            self.assertEqual(receipt['wire_bytes'], 0)

    def test_tiny_exact_budget_accepts_complete_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.capture(root / 'baseline', 100000)
            size = self.retained(root / 'baseline')
            result, seen = self.capture(root / 'bounded', size)
            self.assertEqual(len(seen), 9)
            self.assertEqual(self.retained(root / 'bounded'), size)
            self.assertIn('ids_after', result)

    def test_gzip_decoded_block_is_refused_before_disk_overshoot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                self.capture(root, 100, compressed=True, large_seed=True)
            self.assertLessEqual(self.retained(root), 100)
            receipt = json.loads((root / 'ids_before.receipt.json').read_bytes())
            self.assertFalse(receipt['complete_body'])
            self.assertGreater(receipt['wire_bytes'], 0)
            self.assertEqual(receipt['bytes'], 0)
            self.assertEqual((root / 'ids_before.body').read_bytes(), b'')

    def test_failed_retry_evidence_is_charged_and_exact_budget_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.capture(root / 'baseline', 100000, retry=True)
            size = self.retained(root / 'baseline')
            self.capture(root / 'exact', size, retry=True)
            self.assertEqual(self.retained(root / 'exact'), size)
            self.assertEqual((root / 'exact/ids_before.attempt-01.wire').read_bytes(), b'transient')
            with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                self.capture(root / 'refused', size - 1, retry=True)
            self.assertLessEqual(self.retained(root / 'refused'), size - 1)
            self.assertEqual((root / 'refused/ids_before.attempt-01.wire').read_bytes(), b'transient')

    def test_budget_scope_resets_after_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                self.capture(root / 'refused', 1)
            self.capture(root / 'next', 100000)
            with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
                    200, stream=httpx.ByteStream(b'outside')))) as client:
                body, _receipt = capture_get(client, 'https://example.test/outside', root / 'outside',
                                             'document', max_bytes=10000)
            self.assertEqual(body, b'outside')
