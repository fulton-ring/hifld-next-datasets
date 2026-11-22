"""Contracts for native request identities and raw staging acquisition."""
import json
import unittest
from unittest.mock import patch

import httpx

from dagster_hifld.sources.arcgis_raw_capture import (
    RawCapturePolicy, capture_exact_id_population, request_raw, read_staged_response, population_snapshot,
)


class RawCaptureTests(unittest.TestCase):
    def request(self, serve, *, policy=None, method='GET', budget=None):
        objects = {}
        client = httpx.Client(transport=httpx.MockTransport(serve))
        self.addCleanup(client.close)
        result = request_raw(client, 'https://agency.test/FeatureServer/0/query',
            {'objectIds': '1,2', 'f': 'json'}, 'page_0000000', budget or [0],
            write=objects.__setitem__, encode=lambda value: json.dumps(value).encode(),
            policy=policy or RawCapturePolicy(1000, 10_000_000), method=method)
        return result, objects

    def test_post_preserves_body_receipt_and_query_identity(self):
        seen = []
        def serve(request):
            seen.append((request.method, str(request.url), request.content))
            return httpx.Response(200, stream=httpx.ByteStream(b'{"features":[]}'))
        result, objects = self.request(serve, method='POST', policy=RawCapturePolicy(
            1000, 10_000_000, body_suffix='body', extended_receipt=True, exact_status=True,
            preserve_transport=True))
        self.assertEqual(seen, [('POST', 'https://agency.test/FeatureServer/0/query', b'objectIds=1%2C2&f=json')])
        self.assertEqual(objects['raw/page_0000000.request.body'], seen[0][2])
        receipt = json.loads(objects['raw/page_0000000.receipt.json'])
        self.assertEqual(receipt['request_body_bytes'], len(seen[0][2]))
        self.assertEqual(receipt['final_url'], seen[0][1])
        self.assertEqual(result, {'features': []})

    def test_retry_preserves_failed_raw_response_before_success(self):
        attempts = []
        def serve(request):
            attempts.append(request)
            return httpx.Response(503 if len(attempts) == 1 else 200,
                stream=httpx.ByteStream(b'{"features":[]}'))
        with patch('dagster_hifld.sources.arcgis_raw_capture.time.sleep'):
            _, objects = self.request(serve, policy=RawCapturePolicy(1000, 10_000_000, attempts=3))
        self.assertEqual(json.loads(objects['raw/page_0000000.attempt_1.receipt.json'])['status'], 503)
        self.assertEqual(json.loads(objects['raw/page_0000000.receipt.json'])['attempt'], 2)
        self.assertEqual(len(attempts), 2)

    def test_failed_status_preserves_response_then_refuses(self):
        objects = {}
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(
                403, stream=httpx.ByteStream(b'{"error":{}}')))) as client:
            with self.assertRaisesRegex(ValueError, 'incomplete_source_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(1000, 10_000_000))
        self.assertEqual(objects['raw/page.json'], b'{"error":{}}')

    def test_validation_failure_does_not_capture_closing_witnesses(self):
        calls = []
        def invalid(offset, requested, result):
            raise ValueError('source_page_invalid')
        with self.assertRaisesRegex(ValueError, 'source_page_invalid'):
            capture_exact_id_population([2, 7], 1,
                fetch_page=lambda o, ids: calls.append(('page', ids)) or {},
                validate_page=invalid, close_witnesses=lambda: calls.append(('after',)))
        self.assertEqual(calls, [('page', [2])])

    def test_population_uses_exact_slices_and_closes_after_last_page(self):
        calls = []
        def fetch(offset, ids):
            calls.append(('page', ids))
            return {'features': [{'attributes': {'OBJECTID': i}} for i in ids]}
        features, pages, after = capture_exact_id_population([2, 7, 9], 2,
            fetch_page=fetch, validate_page=lambda o, ids, r: r['features'],
            close_witnesses=lambda: calls.append(('after',)) or 'witness')
        self.assertEqual(calls, [('page', [2, 7]), ('page', [9]), ('after',)])
        self.assertEqual(pages[-1], {'offset': 2, 'requested_ids': [9], 'returned_ids': [9], 'count': 1})
        self.assertEqual(len(features), 3)
        self.assertEqual(after, 'witness')

    def test_transport_exception_retains_post_request_and_failure_receipt(self):
        objects = {}
        def fail(request):
            raise httpx.ConnectError('fixture failure', request=request)
        with httpx.Client(transport=httpx.MockTransport(fail)) as client:
            with self.assertRaisesRegex(ValueError, 'source_transport_error:ConnectError'):
                request_raw(client, 'https://agency.test/query', {'objectIds': '9'}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(), method='POST',
                    policy=RawCapturePolicy(1000, 10_000_000, body_suffix='body',
                        extended_receipt=True, exact_status=True, preserve_transport=True))
        receipt_key = next(key for key in objects if key.endswith('.receipt.json'))
        receipt = json.loads(objects[receipt_key])
        self.assertFalse(receipt['complete'])
        self.assertEqual(receipt['error_type'], 'ConnectError')
        self.assertEqual(objects[receipt_key.removesuffix('.receipt.json') + '.request.body'], b'objectIds=9')

    def test_budget_refusal_never_reads_body(self):
        objects = {}
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(
                200, stream=httpx.ByteStream(b'{"features":[]}')))) as client:
            with self.assertRaisesRegex(ValueError, 'incomplete_source_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(1000, 1))
        receipt = json.loads(objects['raw/page.receipt.json'])
        self.assertEqual(receipt['error_code'], 'reserve_error')
        self.assertEqual(objects['raw/page.json'], b'')

    def test_arcgis_error_body_is_staged_before_refusal(self):
        objects = {}
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(
                200, stream=httpx.ByteStream(b'{"error":{}}')))) as client:
            with self.assertRaisesRegex(ValueError, 'invalid_arcgis_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(1000, 10_000_000))
        self.assertEqual(objects['raw/page.json'], b'{"error":{}}')

    def test_replay_checks_request_bytes_and_keeps_original_receipt(self):
        _, objects = self.request(lambda r: httpx.Response(200, stream=httpx.ByteStream(b'{}')),
            method='POST', policy=RawCapturePolicy(1000, 10_000_000, body_suffix='body',
                extended_receipt=True, exact_status=True))
        read = lambda key: objects[key.removeprefix('old/')]
        args = (read, 'old', 'page_0000000', 'POST',
                'https://agency.test/FeatureServer/0/query', {'objectIds': '1,2', 'f': 'json'})
        retained = read_staged_response(*args)
        self.assertEqual(retained[3], objects['raw/page_0000000.receipt.json'])
        objects['raw/page_0000000.request.body'] = b'objectIds=wrong'
        with self.assertRaisesRegex(ValueError, 'retained_capture_request_mismatch'):
            read_staged_response(*args)
        receipt = json.loads(objects['raw/page_0000000.receipt.json'])
        receipt['sha256'] = 'bad'
        objects['raw/page_0000000.receipt.json'] = json.dumps(receipt).encode()
        with self.assertRaisesRegex(ValueError, 'retained_capture_receipt_mismatch'):
            read_staged_response(*args)

    def test_snapshot_preserves_original_order(self):
        calls = []
        count = lambda: calls.append('count') or 2
        ids = lambda: calls.append('ids') or [4, 9]
        self.assertEqual(population_snapshot(count, ids, ids_first=True), (2, [4, 9]))
        self.assertEqual(calls, ['ids', 'count'])

    def test_oversized_wire_block_is_observed_but_never_staged_beyond_budget(self):
        objects = {}
        block = b'x' * 2_100_000
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(
                200, stream=httpx.ByteStream(block)))) as client:
            with self.assertRaisesRegex(ValueError, 'incomplete_source_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(4_000_000, 2_000_000))
        receipt = json.loads(objects['raw/page.receipt.json'])
        self.assertEqual(receipt['wire_bytes_received'], len(block))
        self.assertEqual(receipt['wire_bytes'], 0)
        self.assertEqual(receipt['bytes'], 0)
        self.assertEqual(objects['raw/page.wire'], b'')
        self.assertFalse(receipt['complete'])

    def test_refused_retry_keeps_aggregate_retained_prefix_under_budget(self):
        objects = {}
        attempts = []
        def serve(request):
            attempts.append(request)
            return httpx.Response(503 if len(attempts) == 1 else 200,
                stream=httpx.ByteStream(b'busy' if len(attempts) == 1 else b'x' * 1_600_000))
        budget = [0]
        with httpx.Client(transport=httpx.MockTransport(serve)) as client, patch(
                'dagster_hifld.sources.arcgis_raw_capture.time.sleep'):
            with self.assertRaisesRegex(ValueError, 'incomplete_source_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', budget,
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(2_000_000, 2_000_000, attempts=3))
        retained = sum(len(data) for key, data in objects.items() if key.endswith(('.json', '.wire'))
                       and not key.endswith('.receipt.json'))
        self.assertEqual(retained, budget[0])
        self.assertLessEqual(retained, 2_000_000)
        self.assertEqual(objects['raw/page.attempt_1.json'], b'busy')
        receipt = json.loads(objects['raw/page.attempt_2.receipt.json'])
        self.assertFalse(receipt['complete'])
        self.assertEqual(receipt['wire_bytes_received'], 1_600_000)
        self.assertEqual(receipt['wire_bytes'], 1_600_000)
        self.assertEqual(receipt['bytes'], 0)

    def test_refused_decoded_expansion_keeps_only_bounded_wire_evidence(self):
        import gzip
        objects = {}
        decoded = b'x' * 2000
        wire = gzip.compress(decoded)
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(
                200, stream=httpx.ByteStream(wire), headers={'content-encoding': 'gzip'}))) as client:
            with self.assertRaisesRegex(ValueError, 'incomplete_source_response:page'):
                request_raw(client, 'https://agency.test/query', {}, 'page', [0],
                    write=objects.__setitem__, encode=lambda v: json.dumps(v).encode(),
                    policy=RawCapturePolicy(1000, 10_000_000))
        receipt = json.loads(objects['raw/page.receipt.json'])
        self.assertEqual(objects['raw/page.wire'], wire)
        self.assertEqual(objects['raw/page.json'], b'')
        self.assertEqual(receipt['decoded_bytes_produced'], len(decoded))
        self.assertEqual(receipt['bytes'], 0)
