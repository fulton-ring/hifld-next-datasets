"""WBD singleton retry witnesses are raw evidence, never native page parts."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_12_digit_hu as source
from dagster_hifld.sources import wbd_hu_family as family
from dagster_hifld.sources.esri_common import canonical
from tests.test_wbd_12_digit_hu import (
    _documents, _encode, _TEST_ROSTER, _write_capture_root, relaxed_pins,
)
from tests.test_wbd_hu_family import _seal_manifest


def _write_response(root, name, url, body, *, wire=None):
    wire = body if wire is None else wire
    receipt = {'method': 'GET', 'url': url, 'final_url': url,
               'observed_at': '2026-11-22T12:00:00+00:00', 'status': 200,
               'complete_body': True, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
               'wire_bytes': len(wire), 'wire_sha256': hashlib.sha256(wire).hexdigest(),
               'content_encoding': None, 'content_length': None, 'error_code': None}
    (root / (name + '.body')).write_bytes(body)
    (root / (name + '.wire')).write_bytes(wire)
    (root / (name + '.receipt.json')).write_bytes(canonical(receipt) + b'\n')


class WbdRetryEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def fixture(self, suffix='', *, name='page_0002.error-1'):
        root = self.base / ('capture' + suffix)
        docs = _documents()
        with relaxed_pins():
            _write_capture_root(root, docs)
            bound = name.rsplit('.error-', 1)[0]
            # Invalid-name cases still carry a correctly formed receipt; name
            # admission must refuse them before interpreting this fallback URL.
            url = family.page_part_url(source._spec(), _TEST_ROSTER, 2, 'page_0002')
            if bound != 'page_9999':
                url = family.page_part_url(source._spec(), _TEST_ROSTER, int(bound[5:9]), bound)
            _write_response(root, name, url, b'{"error":{"code":500,"attempt":1}}')
        _seal_manifest(root, {**docs, name: None})
        return root, docs, name, url

    def test_attempts_replay_and_hash_without_becoming_population_parts(self):
        root, _docs, name, _url = self.fixture()
        with relaxed_pins():
            captures, parts, refusals = source._read_replay(root)
            self.assertIn(name, captures)
            self.assertEqual(parts, {})
            self.assertEqual(refusals, [])
            self.assertEqual(captures[name][0], b'{"error":{"code":500,"attempt":1}}')
            audit, _derived = source._validate_stream(root, parquet_sink=None, page_parts=parts)
            self.assertEqual(audit['source_feature_count'], 5)
            identity = family._capture_identity(captures, root)
            self.assertEqual(identity[name]['decoded_sha256'], hashlib.sha256(captures[name][0]).hexdigest())

    def test_resealed_forged_attempts_fail_native_admission(self):
        for case in ('non_singleton', 'unknown_window', 'served_body', 'wrong_url',
                     'wire_binding', 'missing_wire', 'symlink', 'extra_artifact', 'disabled',
                     'malformed_name', 'oversized', 'manifest_receipt'):
            with self.subTest(case=case):
                name = ('page_0000.error-1' if case == 'non_singleton' else
                        'page_9999.error-1' if case == 'unknown_window' else
                        'page_0002.error-bad' if case == 'malformed_name' else 'page_0002.error-1')
                root, docs, name, url = self.fixture(case, name=name)
                body = (root / (name + '.body')).read_bytes()
                if case == 'served_body':
                    _write_response(root, name, url, b'{"features":[]}')
                elif case == 'wrong_url':
                    _write_response(root, name, 'https://example.test/wrong/query', body)
                elif case == 'wire_binding':
                    _write_response(root, name, url, body, wire=b'forged wire')
                elif case == 'oversized':
                    _write_response(root, name, url, body + b' ' * 4096)
                elif case == 'symlink':
                    (root / (name + '.wire')).unlink()
                    (root / (name + '.wire')).symlink_to(root / (name + '.body'))
                _seal_manifest(root, {**docs, name: None})
                if case == 'missing_wire':
                    (root / (name + '.wire')).unlink()
                elif case == 'extra_artifact':
                    (root / 'unexpected.body').write_bytes(b'not a capture')
                if case in ('missing_wire', 'extra_artifact'):
                    manifest = json.loads((root / 'capture_manifest.json').read_bytes())
                    manifest['artifact_roster'] = capture_roster_digests(root, exclude={'capture_manifest.json'})
                    (root / 'capture_manifest.json').write_bytes(canonical(manifest) + b'\n')
                if case == 'manifest_receipt':
                    manifest = json.loads((root / 'capture_manifest.json').read_bytes())
                    manifest['receipts'][name]['observed_at'] = '2026-11-23T00:00:00+00:00'
                    (root / 'capture_manifest.json').write_bytes(canonical(manifest) + b'\n')
                expected = {
                    'non_singleton': 'retained_capture_attempt_window_invalid:',
                    'unknown_window': 'retained_capture_attempt_window_invalid:',
                    'served_body': 'retained_capture_attempt_document_drift:',
                    'wrong_url': 'retained_capture_request_identity_mismatch:',
                    'wire_binding': 'retained_capture_wire_body_mismatch:',
                    'missing_wire': 'retained_capture_attempt_window_invalid:',
                    'symlink': 'capture_roster_nonregular_path:',
                    'extra_artifact': 'retained_capture_roster_or_names_invalid',
                    'disabled': 'retained_capture_attempt_window_invalid:',
                    'malformed_name': 'retained_capture_attempt_name_invalid:',
                    'oversized': 'retained_capture_attempt_size_limit:',
                    'manifest_receipt': 'retained_capture_manifest_receipt_mismatch:',
                }[case]
                extra = {'SINGLE_REFUSAL_RETRIES': 0} if case == 'disabled' else {}
                with relaxed_pins(**extra), self.assertRaisesRegex(
                        ValueError, '^' + expected):
                    source._read_replay(root)

    def transport(self, *, fail_attrs=False):
        docs = _documents()
        real_client = httpx.Client
        calls = []
        urls = dict(source._capture_roster(_TEST_ROSTER))
        bodies = {urls[name]: _encode(doc) for name, doc in docs.items()}
        attrs_url = source._record_attrs_url(_TEST_ROSTER[-1])
        page = docs['page_0002']
        attrs = {key: page[key] for key in ('displayFieldName', 'fieldAliases', 'fields')}
        attrs['features'] = [{'attributes': page['features'][0]['attributes']}]
        bodies[attrs_url] = _encode(attrs)

        class TornAttrs(httpx.SyncByteStream):
            def __iter__(self):
                yield b'partial attributes'
                raise httpx.ReadError('interrupted attributes response')

        def serve(request):
            if str(request.url) == attrs_url and fail_attrs:
                return httpx.Response(200, stream=TornAttrs())
            if str(request.url) == urls['page_0002']:
                calls.append(str(request.url))
                body = json.dumps({'error': {'code': 500, 'attempt': len(calls)}}).encode()
            else:
                body = bodies[str(request.url)]
            return httpx.Response(200, stream=httpx.ByteStream(body))

        return (lambda **kwargs: real_client(transport=httpx.MockTransport(serve), **kwargs)), calls, urls

    def test_candidate_upload_and_cache_bind_every_intermediate_refusal(self):
        with relaxed_pins(SINGLE_REFUSAL_RETRY_SECONDS=0.0, DISK_FLOOR_BYTES=1):
            factory, calls, urls = self.transport()
            storage = StagingStorageResource(local_dir=str(self.base / 'staging'), use_local=True)
            with patch.object(source.httpx, 'Client', side_effect=factory):
                result = source.capture_wbd_12_digit_hu(storage, version='retry-evidence',
                    capture_directory=str(self.base / 'live'))
            root = Path(result['capture_directory'])
            manifest = json.loads((root / 'capture_manifest.json').read_bytes())
            self.assertEqual(calls, [urls['page_0002']] * 3)
            self.assertEqual(manifest['page_parts']['page_0002'], ['page_0002_attrs'])
            self.assertEqual(manifest['page_refusals'], ['page_0002_refusal'])
            evidence = sorted(name for name in manifest['receipts'] if '.error-' in name)
            self.assertEqual(evidence, ['page_0002.error-1', 'page_0002.error-2'])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            audit = json.loads(storage.read_key(prefix + '/metadata/source_audit.json'))
            for ordinal, name in enumerate(evidence, 1):
                body = (root / (name + '.body')).read_bytes()
                self.assertEqual(json.loads(body)['error']['attempt'], ordinal)
                for suffix in ('body', 'wire'):
                    self.assertEqual(storage.read_key(prefix + '/raw/' + name + '.' + suffix),
                                     (root / (name + '.' + suffix)).read_bytes())
                self.assertEqual(json.loads(storage.read_key(prefix + '/raw/' + name + '.receipt.json')),
                                 manifest['receipts'][name])
                self.assertIn(name, audit['source_capture_evidence'])
            with patch.object(source.httpx, 'Client', side_effect=AssertionError('network forbidden')):
                replay = source.capture_wbd_12_digit_hu(storage, version='replay', capture_directory=str(root))
            self.assertTrue(replay['reused'])
            self.assertEqual(replay['version'], result['version'])

    def test_failed_attrs_fallback_quarantines_intermediate_refusal_triplets(self):
        with relaxed_pins(SINGLE_REFUSAL_RETRY_SECONDS=0.0, DISK_FLOOR_BYTES=1):
            factory, calls, urls = self.transport(fail_attrs=True)
            storage = StagingStorageResource(local_dir=str(self.base / 'staging'), use_local=True)
            with patch.object(source.httpx, 'Client', side_effect=factory), self.assertRaisesRegex(
                    ValueError, 'incomplete_source_capture:page_0002_attrs'):
                source.capture_wbd_12_digit_hu(storage, version='failed',
                    capture_directory=str(self.base / 'live'))
            self.assertEqual(calls, [urls['page_0002']] * 3)
            root = next((self.base / 'live' / 'failed').iterdir())
            manifest = json.loads((root / 'capture_manifest.json').read_bytes())
            self.assertFalse(manifest['complete'])
            quarantined = storage.list_prefix('_quarantine/' + source.DATASET_SLUG + '/')
            for ordinal in (1, 2):
                name = f'page_0002.error-{ordinal}'
                for suffix in ('body', 'wire', 'receipt.json'):
                    key = next(key for key in quarantined if key.endswith('/raw/' + name + '.' + suffix))
                    self.assertEqual(storage.read_key(key), (root / (name + '.' + suffix)).read_bytes())
            self.assertFalse(storage.list_prefix(source.DATASET_SLUG + '/'))
