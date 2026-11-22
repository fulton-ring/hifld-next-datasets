"""Shared WBD window refusal and serializer fallback contracts."""
import json
import tempfile
import unittest
from functools import partial
from pathlib import Path

from dagster_hifld.sources import wbd_capture


class WbdCaptureTests(unittest.TestCase):
    def capture(self, replies, calls):
        def fetch(client, url, root, name, **kwargs):
            calls.append((name, url))
            body = replies(name)
            receipt = {'url': url, 'bytes': len(body), 'wire_bytes': len(body)}
            for suffix, value in (('.body', body), ('.wire', body),
                                  ('.receipt.json', json.dumps(receipt).encode())):
                (root / (name + suffix)).write_bytes(value)
            return body, receipt
        return fetch

    def test_recursive_single_refusal_keeps_each_ancestor_and_exact_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = []
            with self.assertRaisesRegex(ValueError, '^source_page_window_refused_single_feature:page_0000aa:objectid=11$'):
                wbd_capture.capture_window_receipts(None, [11, 20, 33], 0, root,
                    policy=wbd_capture.WindowPolicy(page_size=3, byte_budget=100_000),
                    window_url=lambda ids, index, path: 'https://example.test/' + path,
                    capture=self.capture(lambda name: b'refused', calls),
                    is_refusal=lambda body: body == b'refused',
                    at_floor=wbd_capture.fail_single_feature)
            self.assertEqual([name for name, _ in calls], ['page_0000', 'page_0000a', 'page_0000aa'])
            self.assertTrue(all((root / (name + '_refusal.body')).is_file() for name, _ in calls))

    def test_double_serializer_refusal_keeps_pbf_evidence_then_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = []
            with self.assertRaisesRegex(ValueError, '^source_page_window_refused_after_bisection:page_0000$'):
                wbd_capture.capture_window_receipts(None, [11], 0, root,
                    policy=wbd_capture.WindowPolicy(page_size=1, byte_budget=100_000),
                    window_url=lambda ids, index, path: 'https://example.test/json',
                    capture=self.capture(lambda name: b'refused', calls),
                    is_refusal=lambda body: body == b'refused',
                    at_floor=partial(wbd_capture.capture_protobuf_fallback,
                        pbf_url=lambda path: 'https://example.test/pbf',
                        is_pbf_refusal=lambda body: True))
            self.assertEqual(calls[-1], ('page_0000_pbf', 'https://example.test/pbf'))
            self.assertTrue((root / 'page_0000_refusal.body').is_file())
            self.assertTrue((root / 'page_0000_pbf_refusal.body').is_file())

    def test_refusal_collision_preserves_preexisting_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'page_0000_refusal.body').write_bytes(b'original refusal')
            with self.assertRaisesRegex(ValueError, '^source_refusal_evidence_name_collision:page_0000_refusal.body$'):
                wbd_capture.capture_window_receipts(None, [11], 0, root,
                    policy=wbd_capture.WindowPolicy(page_size=1, byte_budget=100_000,
                                                     refuse_evidence_collision=True),
                    window_url=lambda ids, index, path: 'https://example.test/json',
                    capture=self.capture(lambda name: b'new refusal', []),
                    is_refusal=lambda body: True, at_floor=wbd_capture.fail_refused_window)
            self.assertEqual((root / 'page_0000_refusal.body').read_bytes(), b'original refusal')
            self.assertEqual((root / 'page_0000.body').read_bytes(), b'new refusal')

    def test_streaming_window_budget_refuses_before_excess_bytes_are_retained(self):
        import httpx
        from dagster_hifld.sources.capture_common import capture_get

        def serve(request):
            return httpx.Response(200, stream=httpx.ByteStream(b'{"features": []}'))

        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(serve)) as client:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, '^source_capture_budget_exceeded$'):
                wbd_capture.capture_window_receipts(client, [11], 0, root,
                    policy=wbd_capture.WindowPolicy(page_size=1, byte_budget=12),
                    window_url=lambda ids, index, path: 'https://example.test/json',
                    capture=partial(capture_get, max_bytes=100),
                    is_refusal=lambda body: False, at_floor=wbd_capture.fail_refused_window)
            self.assertLessEqual(sum(path.stat().st_size for pattern in ('*.body', '*.wire')
                                     for path in root.glob(pattern)), 12)
            receipt = json.loads((root / 'page_0000.receipt.json').read_bytes())
            self.assertFalse(receipt['complete_body'])

    def test_singleton_retries_preserve_every_refusal_before_attrs_or_pbf(self):
        import httpx
        from dagster_hifld.sources.capture_common import capture_get

        for serializer in ('attrs', 'pbf'):
            with self.subTest(serializer=serializer), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                attempts = []
                fallback = b'{"features": []}' if serializer == 'attrs' else b'native protobuf'

                def serve(request):
                    if request.url.path.endswith('/json'):
                        attempts.append(len(attempts) + 1)
                        body = json.dumps({'error': {'code': 500, 'attempt': attempts[-1]}}).encode()
                    else:
                        body = fallback
                    return httpx.Response(200, stream=httpx.ByteStream(body))

                at_floor = (partial(wbd_capture.capture_attributes_fallback,
                                    attrs_url=lambda oid: 'https://example.test/attrs')
                            if serializer == 'attrs' else
                            partial(wbd_capture.capture_protobuf_fallback,
                                    pbf_url=lambda path: 'https://example.test/pbf',
                                    is_pbf_refusal=lambda body: body != fallback))
                with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                    parts, refusals = wbd_capture.capture_window_receipts(client, [11], 0, root,
                        policy=wbd_capture.WindowPolicy(page_size=1, byte_budget=100_000,
                                                        single_refusal_retries=2),
                        window_url=lambda ids, index, path: 'https://example.test/json',
                        capture=partial(capture_get, max_bytes=1000),
                        is_refusal=lambda body: body.startswith(b'{"error"'), at_floor=at_floor)
                evidence = sorted(root.glob('page_0000.error-*.receipt.json'))
                self.assertEqual(len(evidence), 2)
                self.assertEqual(attempts, [1, 2, 3])
                self.assertEqual(set(parts), {'page_0000_refusal', 'page_0000_' + serializer})
                self.assertEqual(refusals, ['page_0000_refusal'])
                self.assertEqual(json.loads((root / 'page_0000_refusal.body').read_bytes())['error']['attempt'], 3)
                for index, path in enumerate(evidence, 1):
                    name = path.name.removesuffix('.receipt.json')
                    body = (root / (name + '.body')).read_bytes()
                    receipt = json.loads(path.read_bytes())
                    self.assertEqual(json.loads(body)['error']['attempt'], index)
                    self.assertEqual((root / (name + '.wire')).read_bytes(), body)
                    self.assertEqual(receipt['url'], 'https://example.test/json')
                    self.assertEqual(receipt['status'], 200)
                    self.assertTrue(receipt['complete_body'])
                self.assertEqual((root / ('page_0000_' + serializer + '.body')).read_bytes(), fallback)
                self.assertEqual(parts['page_0000_' + serializer]['url'], 'https://example.test/' + serializer)

    def test_retry_witness_ordinals_skip_existing_partial_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'page_0000.error-1.body').write_bytes(b'earlier failure')
            (root / 'page_0000.error-4.wire').write_bytes(b'earlier partial wire')
            replies = iter((b'refused', b'served'))
            parts, refusals = wbd_capture.capture_window_receipts(None, [11], 0, root,
                policy=wbd_capture.WindowPolicy(page_size=1, byte_budget=100_000,
                                                single_refusal_retries=1),
                window_url=lambda ids, index, path: 'https://example.test/json',
                capture=self.capture(lambda name: next(replies), []),
                is_refusal=lambda body: body == b'refused', at_floor=wbd_capture.fail_refused_window)
            self.assertEqual(set(parts), {'page_0000'})
            self.assertEqual(refusals, [])
            self.assertEqual((root / 'page_0000.error-1.body').read_bytes(), b'earlier failure')
            self.assertEqual((root / 'page_0000.error-4.wire').read_bytes(), b'earlier partial wire')
            self.assertEqual((root / 'page_0000.error-5.body').read_bytes(), b'refused')
            self.assertTrue((root / 'page_0000.error-5.receipt.json').is_file())
