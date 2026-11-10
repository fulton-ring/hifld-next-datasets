"""Tiny real HTTPX streams: lossless partial wire and bounded decoding."""
from __future__ import annotations

import gzip
import hashlib
import unittest
import zlib

import httpx

from dagster_hifld.http_capture import DECODE_BLOCK_BYTES, capture_http_response


class DeliveredStream(httpx.SyncByteStream):
    def __init__(self, blocks, error=None):
        self.blocks = blocks
        self.error = error
        self.reads = 0

    def __iter__(self):
        for block in self.blocks:
            self.reads += 1
            yield block
        self.reads += 1
        if self.error is not None:
            raise self.error


class HTTPCaptureTests(unittest.TestCase):
    def capture(self, blocks, *, headers=None, error=None, reserve=None, wire_cap=8*DECODE_BLOCK_BYTES,
                decoded_cap=8*DECODE_BLOCK_BYTES, status=200, wire_sink=None, decoded_sink=None):
        wire, decoded, reservations = bytearray(), bytearray(), []
        stream = DeliveredStream(blocks, error)

        def budget(progress):
            reservations.append(progress)
            if reserve is not None:
                reserve(progress, stream)

        transport = httpx.MockTransport(lambda request: httpx.Response(status, headers=headers, stream=stream))
        with httpx.Client(transport=transport) as client, client.stream('GET', 'https://source.example.invalid/data') as response:
            result = capture_http_response(response, max_wire_bytes=wire_cap, max_decoded_bytes=decoded_cap,
                reserve=budget, retain_wire=wire.extend if wire_sink is None else wire_sink,
                retain_decoded=decoded.extend if decoded_sink is None else decoded_sink)
        return result, bytes(wire), bytes(decoded), reservations, stream

    def assert_capture(self, result, wire, decoded):
        self.assertEqual(result.wire_bytes_received, len(wire))
        self.assertEqual(result.wire_bytes_retained, len(wire))
        self.assertEqual(result.httpx_bytes_downloaded, len(wire))
        self.assertEqual(result.decoded_bytes_produced, len(decoded))
        self.assertEqual(result.decoded_bytes_retained, len(decoded))
        self.assertEqual(result.wire_sha256, hashlib.sha256(wire).hexdigest())
        self.assertEqual(result.received_wire_sha256, result.wire_sha256)
        self.assertEqual(result.decoded_sha256, hashlib.sha256(decoded).hexdigest())
        self.assertEqual(result.produced_decoded_sha256, result.decoded_sha256)
        self.assertEqual(result.pending_wire, b'')
        self.assertEqual(result.pending_decoded, b'')

    def test_identity_and_empty_are_actual_complete_bodies_not_status_acceptance(self):
        for blocks in [[], [b''], [b'first', b'second']]:
            with self.subTest(blocks=blocks):
                expected = b''.join(blocks)
                r, wire, body, _, _ = self.capture(blocks, headers={'Content-Length': str(len(expected))}, status=403)
                self.assertTrue(r.complete)
                self.assertEqual(r.status_code, 403)
                self.assertTrue(r.stream_exhausted)
                self.assertIsNone(r.decoder_eof)
                self.assertEqual(wire, expected)
                self.assertEqual(body, expected)
                self.assert_capture(r, wire, body)

    def test_real_partial_identity_and_gzip_wire_survive_transport_error(self):
        cases = [(b'partial', {}), (gzip.compress(b'a'*1000)[:18], {'Content-Encoding': 'gzip'})]
        for delivered, headers in cases:
            with self.subTest(headers=headers):
                r, wire, body, _, _ = self.capture([delivered], headers=headers, error=httpx.ReadError('interrupted'))
                self.assertFalse(r.complete)
                self.assertEqual(r.error_code, 'transport_error')
                self.assertEqual(r.error_type, 'ReadError')
                self.assertFalse(r.stream_exhausted)
                self.assertEqual(wire, delivered)
                self.assert_capture(r, wire, body)

    def test_gzip_and_zlib_deflate_wire_content_length_and_bounded_decode(self):
        original = b'abcd' * (DECODE_BLOCK_BYTES//2 + 23)
        for encoding, compress in [('gzip', gzip.compress), ('deflate', zlib.compress)]:
            with self.subTest(encoding=encoding):
                wire = compress(original)
                r, retained, body, reservations, _ = self.capture([wire[:7], wire[7:]],
                    headers={'Content-Encoding': encoding, 'Content-Length': str(len(wire))})
                self.assertTrue(r.complete)
                self.assertTrue(r.decoder_eof)
                self.assertEqual(body, original)
                self.assertEqual(retained, wire)
                self.assertTrue(all(p.reserve_bytes <= DECODE_BLOCK_BYTES for p in reservations))
                self.assert_capture(r, retained, body)

    def test_complete_decode_exact_output_block_boundary(self):
        for length in [DECODE_BLOCK_BYTES, 2*DECODE_BLOCK_BYTES]:
            with self.subTest(length=length):
                compressed = gzip.compress(b'x'*length)
                r, wire, body, _, _ = self.capture([compressed], headers={'Content-Encoding': 'gzip'})
                self.assertTrue(r.complete)
                self.assertEqual(len(body), length)
                self.assert_capture(r, wire, body)

    def test_limits_preserve_entire_delivered_block_without_clipping(self):
        for wire_cap, decoded_cap, code in [(36, 100, 'wire_limit_exceeded'), (100, 36, 'decoded_limit_exceeded')]:
            with self.subTest(code=code):
                delivered = b'x'*68
                r, wire, body, _, stream = self.capture([delivered, b'not read'], wire_cap=wire_cap, decoded_cap=decoded_cap)
                self.assertEqual(wire, delivered)
                self.assertEqual(body, delivered if code == 'decoded_limit_exceeded' else b'')
                self.assertEqual(r.error_code, code)
                self.assertEqual(stream.reads, 1)
                self.assert_capture(r, wire, body)

    def test_gzip_expansion_limit_retains_wire_and_only_one_bounded_output_overshoot(self):
        compressed = gzip.compress(b'x'*(3*DECODE_BLOCK_BYTES))
        r, wire, body, _, _ = self.capture([compressed], headers={'Content-Encoding': 'gzip'}, decoded_cap=9)
        self.assertEqual(r.error_code, 'decoded_limit_exceeded')
        self.assertEqual(wire, compressed)
        self.assertEqual(len(body), DECODE_BLOCK_BYTES)
        self.assert_capture(r, wire, body)

    def test_reserve_runs_before_first_and_every_next_including_EOF(self):
        for fail_at in [0, 1, 2]:
            def budget(progress, stream):
                if progress.phase == 'wire':
                    self.assertEqual(stream.reads, progress.wire_bytes_received // 3)
                    if stream.reads == fail_at:
                        raise OSError('space reserve')
            with self.subTest(fail_at=fail_at):
                r, wire, body, _, stream = self.capture([b'one', b'two'], reserve=budget)
                self.assertEqual(r.error_code, 'reserve_error')
                self.assertEqual(stream.reads, fail_at)
                self.assertEqual(wire, b'onetwo'[:3*fail_at])
                self.assert_capture(r, wire, body)

    def test_decode_budget_rejects_before_allocation_but_after_raw_retention(self):
        def budget(progress, stream):
            if progress.phase == 'decode':
                raise OSError('reserve')
        for headers, wire in [({}, b'actual raw'), ({'Content-Encoding':'gzip'}, gzip.compress(b'expanded'))]:
            with self.subTest(headers=headers):
                r, retained, body, _, _ = self.capture([wire], headers=headers, reserve=budget)
                self.assertEqual(r.error_code, 'reserve_error')
                self.assertEqual(retained, wire)
                self.assertEqual(body, b'')
                self.assert_capture(r, retained, body)

    def test_truncated_CRC_trailing_and_concatenated_members_reject_with_wire_retained(self):
        original = gzip.compress(b'payload'*100)
        bad_crc = original[:-8] + bytes([original[-8] ^ 1]) + original[-7:]
        cases = [([original[:-1]], 'compressed_stream_incomplete'),
                 ([bad_crc], 'decode_error'), ([bad_crc[:-8], bad_crc[-8:]], 'decode_error'),
                 ([original+b'trailing'], 'compressed_trailing_data'),
                 ([original, gzip.compress(b'second member')], 'compressed_trailing_data')]
        for blocks, code in cases:
            with self.subTest(code=code, blocks=len(blocks)):
                r, wire, body, _, _ = self.capture(blocks, headers={'Content-Encoding':'gzip'})
                self.assertFalse(r.complete)
                self.assertEqual(r.error_code, code)
                self.assertEqual(wire, b''.join(blocks))
                self.assert_capture(r, wire, body)

    def test_content_length_mismatch_and_trailer_transport_error(self):
        for length in [0, 5]:
            r, wire, body, _, _ = self.capture([b'abc'], headers={'Content-Length':str(length)})
            self.assertEqual(r.error_code, 'content_length_mismatch')
            self.assert_capture(r, wire, body)
        full = gzip.compress(b'body')
        r, wire, body, _, _ = self.capture([full], headers={'Content-Encoding':'gzip'}, error=httpx.ReadError('after trailer'))
        self.assertEqual(r.error_code, 'transport_error')
        self.assertTrue(r.decoder_eof)
        self.assertFalse(r.stream_exhausted)
        self.assert_capture(r, wire, body)

    def test_invalid_representation_headers_fail_before_first_read(self):
        for headers in [{'Content-Encoding':'br'}, {'Content-Encoding':'gzip, deflate'},
                        {'Content-Length':'-1'}, {'Content-Length':'1, 1'}, {'Content-Length':'1.0'}]:
            with self.subTest(headers=headers):
                r, wire, body, _, stream = self.capture([b'not consumed'], headers=headers)
                self.assertFalse(r.complete)
                self.assertEqual(stream.reads, 0)
                self.assert_capture(r, wire, body)

    def test_raw_deflate_is_explicitly_unsupported_representation(self):
        compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        raw = compressor.compress(b'plain') + compressor.flush()
        r, wire, body, _, _ = self.capture([raw], headers={'Content-Encoding':'deflate'})
        self.assertEqual(r.error_code, 'decode_error')
        self.assertEqual(wire, raw)
        self.assert_capture(r, wire, body)

    def test_sink_failures_return_current_pending_block_not_fictitious_empty_capture(self):
        def fail(block):
            raise OSError('disk failure')
        r, _, _, _, _ = self.capture([b'actual delivered'], wire_sink=fail)
        self.assertEqual(r.error_code, 'wire_sink_error')
        self.assertEqual(r.wire_bytes_received, 16)
        self.assertEqual(r.wire_bytes_retained, 0)
        self.assertEqual(r.pending_wire, b'actual delivered')
        self.assertEqual(r.received_wire_sha256, hashlib.sha256(r.pending_wire).hexdigest())
        r, wire, _, _, _ = self.capture([b'actual delivered'], decoded_sink=fail)
        self.assertEqual(r.error_code, 'decoded_sink_error')
        self.assertEqual(r.pending_decoded, wire)
        self.assertEqual(r.decoded_bytes_retained, 0)
        self.assertEqual(r.decoded_bytes_produced, len(wire))

    def test_already_decoded_response_cannot_claim_wire_capture(self):
        response = httpx.Response(200, content=b'already read')
        r = capture_http_response(response, max_wire_bytes=100, max_decoded_bytes=100,
            reserve=lambda progress: None, retain_wire=lambda block: self.fail('not wire'),
            retain_decoded=lambda block: self.fail('not wire'))
        self.assertEqual(r.error_code, 'wire_stream_unavailable')
        self.assertFalse(r.complete)
        self.assertEqual(r.wire_bytes_received, 0)

    def test_interruptions_propagate_with_already_retained_wire(self):
        wire = bytearray()
        with self.assertRaises(KeyboardInterrupt):
            self.capture([b'prior'], decoded_sink=lambda block: (_ for _ in ()).throw(KeyboardInterrupt()), wire_sink=wire.extend)
        self.assertEqual(wire, b'prior')

    def test_limits_and_callbacks_validate_before_read(self):
        response = httpx.Response(200, stream=DeliveredStream([b'x']))
        for value in [-1, True, 1.5, None]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                capture_http_response(response, max_wire_bytes=value, max_decoded_bytes=1,
                    reserve=lambda progress: None, retain_wire=lambda block: None, retain_decoded=lambda block: None)
        with self.assertRaises(TypeError):
            capture_http_response(response, max_wire_bytes=1, max_decoded_bytes=1,
                reserve=None, retain_wire=lambda block: None, retain_decoded=lambda block: None)


if __name__ == '__main__':
    unittest.main()
