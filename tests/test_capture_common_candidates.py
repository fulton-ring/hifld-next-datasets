"""Candidate upload/seal behavior shared by the GNIS, NHD, PAD and WBD adapters."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common as common


class CandidateLifecycleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.capture = self.root / 'capture'
        self.capture.mkdir()
        (self.root / 'staging').mkdir()
        self.storage = StagingStorageResource(local_dir=str(self.root / 'staging'))
        self.prefix = 'synthetic/source/v1'
        self.state = '_construction-state/synthetic/v1.json'
        self.receipts = {'page-1': {'url': 'https://producer.test/page-1', 'observed_at': 'original'}}
        (self.capture / 'page-1.wire').write_bytes(b'wire-page')

    def stage(self, **kwargs):
        common.stage_candidate_objects(
            self.storage, self.prefix, self.state, 'v1', {'code_commit': 'synthetic'},
            {'metadata/candidate_ready.json': b'{"approved":false}\n'},
            self.capture, self.receipts,
            kwargs.pop('read_capture', lambda root, name: (b'decoded-page', self.receipts[name])),
            source_record_sha256='a' * 64, source_schema_sha256='b' * 64, **kwargs)

    def test_seal_pins_uploaded_bytes_original_receipts_and_streamed_file_descriptor(self):
        descriptor = {'bytes': 4, 'sha256': hashlib.sha256(b'file').hexdigest()}
        self.storage.write_key(f'{self.prefix}/geoparquet/native.parquet', b'file')
        self.stage(descriptors={'geoparquet/native.parquet': descriptor})
        bundle = json.loads(self.storage.read_key(self.state))
        payload = bundle['payload']
        self.assertEqual(payload['capture_receipts'], self.receipts)
        self.assertEqual(payload['objects']['geoparquet/native.parquet'], descriptor)
        for relative, body in {
            'raw/page-1.body': b'decoded-page', 'raw/page-1.wire': b'wire-page',
            'raw/page-1.receipt.json': common._canonical(self.receipts['page-1']) + b'\n',
            'metadata/candidate_ready.json': b'{"approved":false}\n',
        }.items():
            self.assertEqual(self.storage.read_key(f'{self.prefix}/{relative}'), body)
            self.assertEqual(payload['objects'][relative],
                             {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()})

    def test_reuse_dispatch_does_not_skip_verification_or_rebuild_after_failure(self):
        verify = Mock(return_value={'reused': True})
        self.assertIsNone(common.reuse_sealed_candidate(self.storage, self.state, verify))
        verify.assert_not_called()
        self.stage()
        self.assertEqual(common.reuse_sealed_candidate(self.storage, self.state, verify), {'reused': True})
        verify.side_effect = ValueError('tampered sealed inventory')
        with self.assertRaisesRegex(ValueError, 'tampered sealed inventory'):
            common.reuse_sealed_candidate(self.storage, self.state, verify)

    def test_receipt_validation_failure_never_seals_partial_upload(self):
        def refuse(root, name):
            raise ValueError('receipt_mismatch')
        with self.assertRaisesRegex(ValueError, 'receipt_mismatch'):
            self.stage(read_capture=refuse)
        self.assertIsNone(self.storage.object_snapshot(self.state))

    def test_immutable_conflict_does_not_replace_original_or_seal(self):
        key = f'{self.prefix}/raw/page-1.body'
        self.storage.write_key(key, b'original')
        with self.assertRaisesRegex(ValueError, 'immutable_staged_object_changed'):
            self.stage()
        self.assertEqual(self.storage.read_key(key), b'original')
        self.assertIsNone(self.storage.object_snapshot(self.state))

    def test_failed_extra_evidence_admission_prevents_seal(self):
        def refuse(inventory):
            raise ValueError('failed evidence changed')
        with self.assertRaisesRegex(ValueError, 'failed evidence changed'):
            self.stage(retain_extra=refuse)
        self.assertIsNone(self.storage.object_snapshot(self.state))

    def test_conflicting_second_seal_preserves_first_receipt(self):
        self.stage()
        original = self.storage.read_key(self.state)
        self.receipts['page-1']['observed_at'] = 'later'
        with self.assertRaisesRegex(ValueError, 'immutable_staged_object_changed'):
            self.stage()
        self.assertEqual(self.storage.read_key(self.state), original)

    def test_repeated_identical_stage_keeps_first_seal(self):
        self.stage()
        original = self.storage.read_key(self.state)
        self.stage()
        self.assertEqual(self.storage.read_key(self.state), original)

    def test_large_file_upload_rejects_bad_readback(self):
        path = self.capture / 'native.parquet'
        path.write_bytes(b'native bytes')
        with self.assertRaisesRegex(ValueError, 'candidate_parquet_readback_mismatch'):
            common.upload_candidate_file(
                self.storage, f'{self.prefix}/native.parquet', path,
                lambda storage, key: {'bytes': 0, 'sha256': 'wrong'}, ValueError)

    def test_refused_evidence_mutation_is_rejected_before_admission(self):
        for suffix in ('body', 'wire', 'receipt.json'):
            (self.capture / f'failed.{suffix}').write_bytes(b'failed attempt')
        inventory = {}

        def mutate(root, names):
            (root / f'{names[0]}.body').write_bytes(b'changed attempt bytes')

        with self.assertRaisesRegex(ValueError, 'retained_capture_changed_during_admission:failed'):
            common.upload_refused_candidate_evidence(
                self.storage, self.prefix, self.capture, self.receipts,
                inventory, mutate, ValueError)
        self.assertEqual(inventory, {})

    def test_checkpoint_preserves_original_receipts_and_rejects_different_source(self):
        self.stage()
        fingerprint, payload = common.load_candidate_checkpoint(
            self.storage, self.state, 'v1', {'code_commit': 'synthetic'},
            'a' * 64, 'b' * 64, ValueError)
        self.assertEqual(fingerprint, common._sha(common._canonical({'code_commit': 'synthetic'})))
        self.assertEqual(payload['capture_receipts'], self.receipts)
        class SourceError(ValueError):
            pass
        with self.assertRaisesRegex(SourceError, 'cached_first_candidate_identity_or_source_changed'):
            common.load_candidate_checkpoint(
                self.storage, self.state, 'v1', {'code_commit': 'synthetic'},
                'different source', 'b' * 64, SourceError)

    def test_checkpoint_rejects_non_candidate_envelope(self):
        identity = {'code_commit': 'synthetic'}
        fingerprint = common._sha(common._canonical(identity))
        common.seal_ready_candidate(self.storage, self.state, 'v1', fingerprint,
                                    {'processing_identity': identity})
        with self.assertRaisesRegex(ValueError, 'cached_first_candidate_identity_or_source_changed'):
            common.load_candidate_checkpoint(
                self.storage, self.state, 'v1', identity, 'a' * 64, 'b' * 64, ValueError)
