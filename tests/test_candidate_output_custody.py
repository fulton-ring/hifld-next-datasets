"""Custody policies for native ArcGIS candidate uploads."""
import hashlib
import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources import capture_common as common


@dataclass(frozen=True)
class Snapshot:
    size: int
    generation: int


class MemoryStorage:
    def __init__(self):
        self.objects = {}
        self.events = []
        self.generation = 0
        self.change_on_hash = False
        self.bad_hash = False
        self.hide_written = False

    def object_snapshot(self, key):
        self.events.append(('snapshot', key))
        body = self.objects.get(key)
        return None if body is None or self.hide_written else Snapshot(len(body), self.generation)

    def write_key_if_unchanged(self, key, body, snapshot):
        self.events.append(('write', key))
        self.objects[key] = body
        self.generation += 1

    def read_key(self, key):
        return self.objects[key]

    def object_exists(self, key):
        return key in self.objects

    def upload_local_file(self, path, key):
        self.write_key_if_unchanged(key, Path(path).read_bytes(), None)

    def sha256_key(self, key):
        self.events.append(('hash', key))
        if self.change_on_hash:
            self.generation += 1
        return 'wrong' if self.bad_hash else hashlib.sha256(self.objects[key]).hexdigest()


class CandidateOutputCustodyTests(unittest.TestCase):
    def test_two_pass_candidates_finish_writes_before_readback(self):
        for mode in ('per-object', 'after-all'):
            with self.subTest(mode=mode):
                storage = MemoryStorage()
                result = common.write_candidate_outputs(
                    storage, 'candidate', {'b': b'B', 'a': b'A'}, readback_order=mode)
                self.assertEqual(list(result), ['b', 'a'])
                events = [event for event in storage.events if event[0] != 'snapshot']
                expected = [('write', 'candidate/b'), ('hash', 'candidate/b'),
                            ('write', 'candidate/a'), ('hash', 'candidate/a')]
                if mode == 'after-all':
                    expected = [expected[0], expected[2], expected[1], expected[3]]
                self.assertEqual(events, expected)
                self.assertEqual(result['a'], {'bytes': 1, 'sha256': hashlib.sha256(b'A').hexdigest()})

    def test_snapshot_stability_is_a_native_policy(self):
        storage = MemoryStorage()
        storage.change_on_hash = True
        with self.assertRaisesRegex(ValueError, 'candidate_object_readback_failed:a'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'A'})
        storage = MemoryStorage()
        storage.change_on_hash = True
        self.assertIn('a', common.write_candidate_outputs(
            storage, 'candidate', {'a': b'A'}, snapshot_check='content'))

    def test_readback_failure_keeps_native_error_class_and_prefix(self):
        class NativeError(ValueError):
            pass
        storage = MemoryStorage()
        storage.bad_hash = True
        with self.assertRaisesRegex(NativeError, 'native_readback_failed:a'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'A'},
                error_type=NativeError, error_prefix='native_readback_failed:')

    def test_missing_snapshot_refuses_candidate(self):
        storage = MemoryStorage()
        storage.hide_written = True
        with self.assertRaisesRegex(ValueError, 'candidate_object_readback_failed:a'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'A'})

    def test_existing_immutable_conflict_never_overwrites(self):
        storage = MemoryStorage()
        storage.objects['candidate/a'] = b'original'
        with self.assertRaisesRegex(ValueError, 'immutable_staged_object_changed:candidate/a'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'changed'})
        self.assertEqual(storage.objects['candidate/a'], b'original')
        self.assertFalse(any(event[0] == 'write' for event in storage.events))

    def test_two_pass_write_failure_performs_no_readback(self):
        storage = MemoryStorage()
        def writer(target, key, body):
            target.write_key_if_unchanged(key, body, None)
            if key.endswith('/b'):
                raise ValueError('writer refused')
        with self.assertRaisesRegex(ValueError, 'writer refused'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'A', 'b': b'B'},
                writer=writer, readback_order='after-all')
        self.assertFalse(any(event[0] == 'hash' for event in storage.events))

    def test_observed_descriptor_and_legacy_write_only_policies_are_preserved(self):
        storage = MemoryStorage()
        def writer(target, key, body):
            target.write_key_if_unchanged(key, b'stored', None)
        result = common.write_candidate_outputs(storage, 'candidate', {'a': b'input'},
            writer=writer, snapshot_check='observed')
        self.assertEqual(result['a']['sha256'], hashlib.sha256(b'stored').hexdigest())
        self.assertEqual(result['a']['bytes'], len(b'stored'))
        storage = MemoryStorage()
        common.write_candidate_outputs(storage, 'candidate', {'a': b'A'}, snapshot_check='none')
        self.assertEqual(storage.events, [('snapshot', 'candidate/a'), ('write', 'candidate/a')])

    def test_supplied_construction_fingerprint_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            from dagster_hifld.resources import StagingStorageResource
            root = Path(temporary).resolve()
            storage = StagingStorageResource(local_dir=str(root))
            common.seal_candidate_objects(storage, '_construction-state/native/v1.json', 'v1',
                {'code_commit': 'synthetic'}, {}, {}, source_record_sha256='a' * 64,
                source_schema_sha256='b' * 64, fingerprint='c' * 64)
            bundle = json.loads(storage.read_key('_construction-state/native/v1.json'))
            self.assertEqual(bundle['processing_fingerprint'], 'c' * 64)

    def test_declared_inventory_readback_refuses_size_before_hash(self):
        storage = MemoryStorage()
        storage.objects['candidate/native'] = b'wrong size'
        with self.assertRaisesRegex(ValueError, 'candidate_output_physical_readback_failed:native'):
            common.verify_candidate_outputs(storage, 'candidate', {
                'native': {'bytes': 1, 'sha256': 'a' * 64}})
        self.assertFalse(any(event[0] == 'hash' for event in storage.events))

    def test_streamed_upload_refuses_missing_or_different_native_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'native.parquet'
            path.write_bytes(b'bytes')
            expected = {'bytes': 5, 'sha256': hashlib.sha256(b'bytes').hexdigest()}
            for missing in (False, True):
                with self.subTest(missing=missing):
                    storage = MemoryStorage()
                    storage.hide_written = missing
                    storage.bad_hash = not missing
                    with self.assertRaisesRegex(ValueError, 'candidate_object_readback_failed:native.parquet'):
                        common.upload_candidate_local_file(
                            storage, 'candidate', 'native.parquet', path, expected)

    def test_existing_file_sha_policy_preserves_original_and_rejects_bad_upload(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'native.parquet'
            path.write_bytes(b'bytes')
            expected = hashlib.sha256(b'bytes').hexdigest()
            storage = MemoryStorage()
            storage.objects['candidate/native.parquet'] = b'original'
            with self.assertRaisesRegex(ValueError, 'immutable_staged_object_changed:candidate/native.parquet'):
                common.write_candidate_file_by_sha(
                    storage, 'candidate', 'native.parquet', path, expected)
            self.assertEqual(storage.objects['candidate/native.parquet'], b'original')
            storage = MemoryStorage()
            storage.bad_hash = True
            with self.assertRaisesRegex(ValueError, 'candidate_object_readback_failed:native.parquet'):
                common.write_candidate_file_by_sha(
                    storage, 'candidate', 'native.parquet', path, expected)

    def test_native_file_outputs_are_streamed_without_path_read_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            from dagster_hifld.resources import StagingStorageResource
            root = Path(temporary).resolve()
            native = root / 'native.parquet'
            native.write_bytes(b'parquet bytes')
            stage = root / 'stage'
            storage = StagingStorageResource(local_dir=str(stage))
            with patch.object(Path, 'read_bytes', side_effect=AssertionError('buffered native file')):
                inventory = common.write_candidate_outputs(
                    storage, 'candidate', {'native.parquet': native, 'metadata.json': b'{}'},
                    readback_order='after-all')
            self.assertEqual(inventory['native.parquet'], {
                'bytes': len(b'parquet bytes'), 'sha256': hashlib.sha256(b'parquet bytes').hexdigest()})
            self.assertEqual(storage.read_key('candidate/native.parquet'), b'parquet bytes')

    def test_budget_reservation_refuses_before_any_object_io(self):
        storage = MemoryStorage()
        def refuse(size):
            self.assertEqual(size, 1)
            raise ValueError('native_budget_exceeded')
        with self.assertRaisesRegex(ValueError, 'native_budget_exceeded'):
            common.write_candidate_outputs(storage, 'candidate', {'a': b'A'}, reserve=refuse)
        self.assertEqual(storage.events, [])

    def test_transport_cleanup_only_releases_hash_verified_decoded_copy(self):
        for valid in (False, True):
            with self.subTest(valid=valid), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                storage = MemoryStorage()
                (root / 'page.wire').write_bytes(b'wire')
                (root / 'page.body').write_bytes(b'body' if valid else b'forged')
                receipt = {'bytes': 4, 'sha256': hashlib.sha256(b'body').hexdigest()}
                descriptors = {'raw/page.wire': {}, 'raw/page.receipt.json': {}}
                reserve = []
                def stage():
                    common.write_candidate_transport_evidence(storage, 'candidate', root,
                        descriptors, {'page': (None, receipt)}, lambda name: b'exact receipt', reserve.append)
                if valid:
                    stage()
                    self.assertFalse((root / 'page.body').exists())
                else:
                    with self.assertRaisesRegex(ValueError, 'capture_decoded_body_changed:page'):
                        stage()
                    self.assertTrue((root / 'page.body').exists())
                self.assertEqual(reserve, [4, len(b'exact receipt')])
                self.assertEqual(storage.objects['candidate/raw/page.receipt.json'], b'exact receipt')

    def test_native_immutable_and_ready_error_policies_are_preserved(self):
        class NativeError(ValueError):
            pass
        storage = MemoryStorage()
        storage.objects['candidate/a'] = b'original'
        with self.assertRaisesRegex(NativeError, 'native_conflict:a'):
            common.write_immutable(storage, 'candidate/a', b'changed', error_type=NativeError,
                                   error_name='a', error_prefix='native_conflict:')
        with self.assertRaisesRegex(NativeError, 'candidate_ready_state_changed'):
            common.write_candidate_ready_state(storage, 'candidate/a', b'changed', NativeError)
        self.assertEqual(storage.objects['candidate/a'], b'original')

    def test_nfhl_single_object_retains_full_key_error_name(self):
        storage = MemoryStorage()
        storage.bad_hash = True
        with self.assertRaisesRegex(ValueError, 'candidate_output_physical_readback_failed:native/object'):
            common.stage_candidate_object(storage, 'native/object', body=b'bytes')
        self.assertEqual(storage.objects['native/object'], b'bytes')

    def test_filegdb_custody_streams_paths_and_retains_cas(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources.arcgis_filegdb import stage_filegdb_archive
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            archive = root / 'source.zip'
            archive.write_bytes(b'archive')
            gdb = root / 'native.gdb'
            gdb.mkdir()
            (gdb / 'table.gdbtable').write_bytes(b'native table')
            storage = StagingStorageResource(local_dir=str(root / 'stage'))
            with patch.object(Path, 'read_bytes', side_effect=AssertionError('buffered archive/member')):
                objects = stage_filegdb_archive(storage, 'candidate', archive, gdb, 'native.gdb')
            self.assertEqual([item['path'] for item in objects], [
                'file_geodatabase/source_filegdb.zip', 'file_geodatabase/native.gdb/table.gdbtable'])
            self.assertEqual(storage.read_key('candidate/file_geodatabase/source_filegdb.zip'), b'archive')
            race_storage = StagingStorageResource(local_dir=str(root / 'race'))
            original = StagingStorageResource.copy_key_to_if_unchanged
            def race(source, target, key, destination_key, **kwargs):
                target.write_key_if_unchanged(destination_key, b'competing writer', None)
                return original(source, target, key, destination_key, **kwargs)
            with patch.object(StagingStorageResource, 'copy_key_to_if_unchanged', race):
                with self.assertRaises(RuntimeError):
                    stage_filegdb_archive(race_storage, 'candidate', archive, gdb, 'native.gdb')
            self.assertEqual(race_storage.read_key('candidate/file_geodatabase/source_filegdb.zip'), b'competing writer')

    def test_create_only_reservation_preserves_existing_immutable_objects(self):
        storage = MemoryStorage()
        storage.objects['candidate/a'] = b'A'
        def refuse():
            raise ValueError('native_create_reserve')
        common.write_immutable(storage, 'candidate/a', b'A', before_create=refuse)
        with self.assertRaisesRegex(ValueError, 'native_create_reserve'):
            common.write_immutable(storage, 'candidate/b', b'B', before_create=refuse)
        self.assertNotIn('candidate/b', storage.objects)

    def test_filegdb_source_replacement_after_hash_is_not_admitted(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources.arcgis_filegdb import stage_filegdb_archive
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            archive = root / 'source.zip'
            archive.write_bytes(b'original')
            gdb = root / 'native.gdb'
            gdb.mkdir()
            storage = StagingStorageResource(local_dir=str(root / 'stage'))
            hash_file = common.sha256_file
            def replace_after_hash(path):
                digest = hash_file(path)
                if path == archive:
                    path.write_bytes(b'replaced')
                return digest
            with patch.object(common, 'sha256_file', replace_after_hash):
                with self.assertRaisesRegex(ValueError, 'immutable_staged_object_changed:'):
                    stage_filegdb_archive(storage, 'candidate', archive, gdb, 'native.gdb')
            self.assertEqual(storage.list_prefix('candidate'), [])

    def test_cache_descriptor_native_field_policy_and_error_class(self):
        class NativeError(ValueError):
            pass
        storage = MemoryStorage()
        storage.objects['candidate/a'] = b'A'
        descriptor = {'bytes': 1, 'sha256': hashlib.sha256(b'A').hexdigest(), 'native_field': 'retained'}
        common.verify_cached_objects(storage, 'candidate', {'a': descriptor},
                                     descriptor_check='fields', error_type=NativeError)
        with self.assertRaisesRegex(ValueError, 'cached_candidate_object_integrity_failed:a'):
            common.verify_cached_objects(storage, 'candidate', {'a': descriptor})
        with self.assertRaisesRegex(NativeError, 'cached_candidate_object_integrity_failed:a'):
            common.verify_cached_objects(storage, 'candidate', {'a': None},
                                         descriptor_check='fields', error_type=NativeError)

    def test_cache_transport_keeps_exact_original_receipt_encoding(self):
        storage = MemoryStorage()
        storage.objects['candidate/raw/page.wire'] = b'wire'
        raw_receipt = b'{ "original": "bytes" }\n'
        receipt = {'raw_receipt_sha256': hashlib.sha256(raw_receipt).hexdigest()}
        storage.objects['candidate/raw/page.receipt.json'] = raw_receipt
        observed = []
        common.verify_cached_transport_receipts(storage, 'candidate', {'page': receipt},
            lambda wire, value, name: observed.append((wire, value, name)), receipt_encoding='sha256')
        self.assertEqual(observed, [(b'wire', receipt, 'page')])
        storage.objects['candidate/raw/page.receipt.json'] = b'changed'
        with self.assertRaisesRegex(ValueError, 'cached_original_capture_changed:page'):
            common.verify_cached_transport_receipts(storage, 'candidate', {'page': receipt},
                lambda wire, value, name: None, receipt_encoding='sha256')

    def test_ienc_quarantine_keeps_exact_evidence_and_native_prefix(self):
        from types import SimpleNamespace
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import (hf_areas_usace_ienc, land_region_areas_usace_ienc,
                                          lock_basins_usace_ienc, lock_gate_lines_usace_ienc)
        receipt = {'observed_at': 'original', 'source_note': 'retained ñ'}
        for adapter in (hf_areas_usace_ienc, land_region_areas_usace_ienc,
                        lock_basins_usace_ienc, lock_gate_lines_usace_ienc):
            with self.subTest(adapter=adapter.__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                (root / 'page.wire').write_bytes(b'wire')
                storage = StagingStorageResource(local_dir=str(root / 'stage'))
                with patch.object(common, 'uuid4', return_value=SimpleNamespace(hex='fixed')):
                    prefix = adapter.quarantine_incomplete_capture(
                        storage, root, 'native', 'v1', {'page': (b'body', receipt)}, 'native_failure')
                self.assertEqual(prefix, '_quarantine/native/v1-fixed')
                self.assertEqual(storage.read_key(prefix + '/raw/page.body'), b'body')
                self.assertEqual(storage.read_key(prefix + '/raw/page.wire'), b'wire')
                self.assertEqual(storage.read_key(prefix + '/raw/page.receipt.json'),
                                 common._canonical(receipt) + b'\n')
                self.assertEqual(json.loads(storage.read_key(prefix + '/metadata/failure.json')),
                                 {'reason_code': 'native_failure', 'captured_names': ['page']})
