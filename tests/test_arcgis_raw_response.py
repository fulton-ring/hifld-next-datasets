"""Failure contracts for the shared native dual-sink request owner."""
import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.sources import arcgis_raw_capture as raw
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView


class RawResponseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.writes = {}
        self.receipts = []
        self.diagnostics = []

    def write(self, key, body):
        self.writes[key] = body
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)

    def file_request(self, client=None, **kwargs):
        return raw.capture_file_request(
            'https://example.test/query', 'page', self.root, self.root / 'request',
            policy=raw.FileRequestPolicy(4096, 0, 'raw', kwargs.pop('receipt_first', False)),
            read=lambda path, limit: path.read_bytes(), encode=lambda value: json.dumps(value).encode(),
            parse_receipt=json.loads, validate=lambda body, receipt: None,
            write=self.write, write_receipt=self.receipts.append, client=client, **kwargs)

    def memory_request(self, client=None, **kwargs):
        return raw.capture_memory_request(
            'https://example.test/query', 'page',
            policy=raw.MemoryRequestPolicy(4096, 4096, 0, 'raw', ValueError),
            space_root=lambda: self.root, write=self.write,
            encode=lambda value: json.dumps(value).encode(),
            read=lambda path, limit: (path.read_bytes(), None), parse_receipt=json.loads,
            validate=lambda body, receipt: None,
            diagnostic=lambda pieces, reason: self.diagnostics.append((dict(pieces), reason)) or 'refusal',
            client=client, **kwargs)

    def client(self, body, **kwargs):
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                yield body
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Stream(), **kwargs)))
        self.addCleanup(client.close)
        return client

    def test_file_identity_and_gzip_have_independent_wire_and_decoded_custody(self):
        for compressed in (False, True):
            with self.subTest(compressed=compressed):
                if (self.root / 'request').exists():
                    import shutil
                    shutil.rmtree(self.root / 'request')
                body = b'{"features":[]}'
                wire = gzip.compress(body) if compressed else body
                path, receipt = self.file_request(self.client(wire, headers={
                    'content-encoding': 'gzip' if compressed else 'identity'}))
                self.assertEqual(path.read_bytes(), body)
                self.assertEqual((self.root / 'request/wire.body').read_bytes(), wire)
                self.assertTrue(receipt['complete_body'])
                self.assertTrue(receipt['independent_sink_readback_matches'])
                view = RetainedCaptureView(self.root)
                view['page'] = (path, receipt)
                self.assertEqual(view['page'], (body, receipt))

    def test_file_failed_readback_retains_unacknowledged_block(self):
        body = b'{"features":[]}'
        with patch('os.pread', return_value=b'corrupted'):
            _, receipt = self.file_request(self.client(body))
        self.assertFalse(receipt['complete_body'])
        self.assertEqual((self.root / 'request/pending_wire.body').read_bytes(), body)
        transfer = json.loads((self.root / 'request/transfer.json').read_bytes())
        self.assertFalse(transfer['independent_physical_readback_matches_acknowledged_sinks'])
        self.assertEqual(transfer['physical_wire']['sha256'], hashlib.sha256(body).hexdigest())

    def test_file_space_refusal_happens_before_body_pull(self):
        pulls = []
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                pulls.append(True)
                yield b'body'
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())))
        self.addCleanup(client.close)
        with patch('shutil.disk_usage') as disk:
            disk.return_value.free = 0
            _, receipt = self.file_request(client)
        self.assertFalse(receipt['complete_body'])
        self.assertEqual(pulls, [])
        self.assertTrue((self.root / 'request/transfer.json').is_file())

    def test_file_replay_custody_order_preserves_native_receipt_policy(self):
        replay = self.root / 'replay'
        replay.mkdir()
        (replay / 'page.body').write_bytes(b'original')
        with self.assertRaises(FileNotFoundError):
            self.file_request(replay_root=replay)
        self.assertEqual(list(self.writes.values()), [b'original'])
        self.writes.clear()
        with self.assertRaises(FileNotFoundError):
            self.file_request(replay_root=replay, receipt_first=True)
        self.assertEqual(self.writes, {})

    def test_pending_fragment_refuses_symlinks_and_readback_mismatch(self):
        target = self.root / 'fragment'
        with self.assertRaisesRegex(OSError, 'pending_capture_readback_mismatch'):
            raw._retain_file_fragment(target, b'original', lambda path, limit: b'wrong')
        self.assertEqual(target.read_bytes(), b'original')
        link = self.root / 'link'
        link.symlink_to(target)
        with self.assertRaises(OSError):
            raw._retain_file_fragment(link, b'replacement', lambda path, limit: path.read_bytes())
        self.assertEqual(target.read_bytes(), b'original')

    def test_memory_gzip_retains_native_receipt_and_original_wire_hash(self):
        body = b'{"features":[]}'
        wire = gzip.compress(body)
        actual, receipt, original = self.memory_request(self.client(wire, headers={'content-encoding': 'gzip'}))
        self.assertEqual(actual, body)
        self.assertTrue(receipt['complete'])
        self.assertEqual(receipt['wire_sha256'], hashlib.sha256(wire).hexdigest())
        self.assertEqual(json.loads(original), receipt)
        self.assertIn(body, self.writes.values())
        self.assertIn(original, self.writes.values())

    def test_memory_transport_failure_retains_prefix_and_receipt_before_diagnostic(self):
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                yield b'prefix'
                raise httpx.ReadError('dropped')
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())))
        self.addCleanup(client.close)
        with self.assertRaisesRegex(ValueError, 'preserved:refusal'):
            self.memory_request(client)
        self.assertIn(b'prefix', self.writes.values())
        pieces, reason = self.diagnostics[0]
        self.assertEqual(pieces['response.body'], b'prefix')
        self.assertFalse(json.loads(pieces['receipt.json'])['complete'])
        self.assertIn(json.loads(pieces['receipt.json'])['capture_error'], reason)

    def test_memory_interrupt_preserves_prefix_once_and_propagates(self):
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                yield b'prefix'
                raise KeyboardInterrupt()
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())))
        self.addCleanup(client.close)
        with self.assertRaises(KeyboardInterrupt):
            self.memory_request(client)
        self.assertEqual(self.diagnostics, [({'wire.body': b'prefix', 'response.body': b'prefix'}, 'capture_interrupted_no_retry')])
        self.assertEqual(self.writes, {})

    def test_memory_malformed_replay_receipt_retains_original_bytes(self):
        replay = self.root / 'replay'
        replay.mkdir()
        (replay / 'page.body').write_bytes(b'original')
        (replay / 'page.receipt.json').write_bytes(b'not JSON')
        with self.assertRaisesRegex(ValueError, 'preserved:refusal'):
            self.memory_request(replay_root=replay)
        self.assertIn(b'original', self.writes.values())
        self.assertIn(b'not JSON', self.writes.values())
        self.assertEqual(self.diagnostics[0][0]['original-receipt.body'], b'not JSON')
    def test_oversized_transport_block_has_bounded_file_evidence(self):
        delivered = b'x' * 8192
        self.file_request(self.client(delivered))
        request = self.root / 'request'
        wire = (request / 'wire.body').read_bytes()
        pending = (request / 'pending_wire.body').read_bytes()
        self.assertLessEqual(len(wire) + len(pending), 4096)
        transfer = json.loads((request / 'transfer.json').read_bytes())
        self.assertEqual(transfer['wire_bytes_received'], len(delivered))
        evidence = transfer['pending_wire_artifact']
        self.assertEqual(evidence['bytes'], len(delivered))
        self.assertEqual(evidence['sha256'], hashlib.sha256(delivered).hexdigest())
        self.assertEqual(evidence['retained_bytes'], len(pending))
        self.assertTrue(evidence['truncated'])

    def test_gzip_expansion_has_bounded_file_evidence(self):
        self.file_request(self.client(gzip.compress(b'x' * 8192), headers={'content-encoding':'gzip'}))
        request = self.root / 'request'
        retained = sum((request / name).stat().st_size for name in ('decoded.body', 'pending_decoded.body'))
        self.assertLessEqual(retained, 4096)
        transfer = json.loads((request / 'transfer.json').read_bytes())
        self.assertFalse(transfer['complete'])
        self.assertTrue(transfer['pending_decoded_artifact']['truncated'])

    def test_oversized_memory_block_has_bounded_diagnostic_evidence(self):
        delivered = b'x' * 8192
        with self.assertRaisesRegex(ValueError, 'preserved:refusal'):
            self.memory_request(self.client(delivered))
        pieces, _ = self.diagnostics[0]
        self.assertLessEqual(len(pieces['wire.body']) + len(pieces['pending-wire.body']), 4096)
        observation = json.loads(pieces['pending-wire.observation.json'])
        self.assertEqual(observation['bytes'], len(delivered))
        self.assertEqual(observation['sha256'], hashlib.sha256(delivered).hexdigest())

    def test_gzip_expansion_has_bounded_memory_diagnostic_evidence(self):
        with self.assertRaisesRegex(ValueError, 'preserved:refusal'):
            self.memory_request(self.client(gzip.compress(b'x' * 8192), headers={'content-encoding':'gzip'}))
        pieces, _ = self.diagnostics[0]
        self.assertLessEqual(len(pieces['response.body']) + len(pieces['pending-decoded.body']), 4096)
        self.assertIn('pending-decoded.observation.json', pieces)

    def test_real_dialysis_acquisition_refuses_four_megabyte_block_at_native_limit(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import dialysis_centers
        delivered = b'x' * 4_000_000
        client = self.client(delivered)
        with patch.object(dialysis_centers.httpx, 'Client', return_value=client):
            with self.assertRaises(ValueError):
                dialysis_centers.acquire(StagingStorageResource(local_dir=str(self.root)))
        wire = next(self.root.glob('_acquisition-attempts/*/*/*/wire.body'))
        pending = wire.with_name('pending_wire.body')
        self.assertLessEqual(wire.stat().st_size + pending.stat().st_size, dialysis_centers.MAX_BODY_BYTES)
        transfer = json.loads(wire.with_name('transfer.json').read_bytes())
        self.assertEqual(transfer['wire_bytes_received'], len(delivered))
        self.assertEqual(transfer['pending_wire_artifact']['sha256'], hashlib.sha256(delivered).hexdigest())
        self.assertFalse(transfer['complete'])

    def test_prior_prefix_survives_later_wire_refusal_with_bounded_pending(self):
        prefix, refused = b'admitted', b'x' * 8192
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                yield prefix
                yield refused
        def client():
            value = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())))
            self.addCleanup(value.close)
            return value
        _, receipt = self.file_request(client())
        request = self.root / 'request'
        self.assertFalse(receipt['complete_body'])
        self.assertEqual((request / 'wire.body').read_bytes(), prefix)
        self.assertEqual((request / 'decoded.body').read_bytes(), prefix)
        self.assertEqual((request / 'pending_wire.body').stat().st_size, 4096 - len(prefix))
        with self.assertRaisesRegex(ValueError, 'preserved:refusal'):
            self.memory_request(client())
        pieces, _ = self.diagnostics[0]
        self.assertEqual(pieces['wire.body'], prefix)
        self.assertEqual(pieces['response.body'], prefix)
        self.assertEqual(len(pieces['pending-wire.body']), 4096 - len(prefix))
        observation = json.loads(pieces['pending-wire.observation.json'])
        self.assertEqual(observation['sha256'], hashlib.sha256(refused).hexdigest())
