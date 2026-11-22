"""Atomic visibility and outside-custody ordering for private candidate installation."""
import tempfile
import unittest
import os
import fcntl
from unittest.mock import patch
from pathlib import Path

from dagster_hifld.sources import candidate_transaction as transaction


class PrivateCandidateTransactionTests(unittest.TestCase):
    def test_checked_short_write_never_reaches_anchor_or_public_target(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); private = base / 'private'; private.mkdir()
            target = base / 'target'; anchors = []
            write = os.write
            with patch.object(transaction.os, 'write', side_effect=lambda fd, body: write(fd, body[:1])):
                with self.assertRaisesRegex(ValueError, 'incomplete_private_write:native.bin'):
                    transaction.install_private_candidate(
                        private, target, [('native.bin', b'whole-native-artifact')],
                        write_one=lambda root, name, body: transaction.write_checked_private_file(
                            root, name, body, readback=lambda root, name: (root / name).read_bytes(),
                            error_type=ValueError), anchor=lambda: anchors.append(True))
            self.assertEqual(anchors, [])
            self.assertFalse(target.exists())
            self.assertEqual((private / 'native.bin').read_bytes(), b'w')

    def test_lock_refuses_linked_inode_and_keeps_existing_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / 'version.lock'; path.write_bytes(b'custody')
            os.link(path, root / 'alias.lock')
            with self.assertRaisesRegex(ValueError, 'unsafe_lock'):
                with transaction.lock_path(path, error_type=ValueError, unsafe_code='unsafe_lock'):
                    self.fail('linked lock must not be acquired')
            self.assertEqual(path.read_bytes(), b'custody')
            self.assertEqual(path.stat().st_nlink, 2)

    def test_exception_releases_lock_without_unlinking_its_inode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'version.lock'
            with self.assertRaisesRegex(ValueError, 'private_failed'):
                with transaction.lock_path(path, error_type=ValueError, unsafe_code='unsafe_lock'):
                    inode = path.stat().st_ino
                    descriptor = os.open(path, os.O_RDWR)
                    try:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    finally:
                        os.close(descriptor)
                    raise ValueError('private_failed')
            self.assertEqual(path.stat().st_ino, inode)
            descriptor = os.open(path, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)

    def test_failed_anchor_keeps_private_artifacts_and_never_exposes_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            private = base / 'private'; private.mkdir()
            target = base / 'versions' / 'candidate'
            calls = []
            def write(root, name, body):
                self.assertFalse(target.exists())
                transaction.write_exclusive_private_file(
                    root, name, body, error_type=ValueError, short_write_code='short')
            def validate(root):
                self.assertEqual((root / 'native.bin').read_bytes(), b'\x00native\xff')
                calls.append('validate')
            def anchor():
                calls.append('anchor')
                raise ValueError('outside_custody_failed')
            with self.assertRaisesRegex(ValueError, 'outside_custody_failed'):
                transaction.install_private_candidate(
                    private, target, [('native.bin', b'\x00native\xff')], write_one=write,
                    validate_before=validate, anchor=anchor, validate_after=validate)
            self.assertEqual(calls, ['validate', 'anchor'])
            self.assertFalse(target.exists())
            self.assertEqual((private / 'native.bin').read_bytes(), b'\x00native\xff')

    def test_post_anchor_readback_failure_does_not_rename(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); private = base / 'private'; private.mkdir()
            target = base / 'target'; calls = []
            def anchor():
                calls.append('anchor')
                (private / 'native.bin').write_bytes(b'tampered')
            def validate(root):
                calls.append('validate')
                if (root / 'native.bin').read_bytes() != b'original':
                    raise ValueError('private_readback_changed')
            with self.assertRaisesRegex(ValueError, 'private_readback_changed'):
                transaction.install_private_candidate(
                    private, target, [('native.bin', b'original')],
                    write_one=lambda root, name, body: (root / name).write_bytes(body),
                    validate_before=validate, anchor=anchor, validate_after=validate)
            self.assertEqual(calls, ['validate', 'anchor', 'validate'])
            self.assertFalse(target.exists())

    def test_ready_is_written_last_and_native_artifacts_survive_exactly(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); private = base / 'private'; private.mkdir()
            target = base / 'target'; calls = []
            bodies = {'native.gpkg': b'SQLite format 3\x00\xff', 'native.parquet': b'PAR1\x00native\xffPAR1'}
            def write(root, name, body):
                calls.append(('write', name))
                (root / name).write_bytes(body)
            def ready(root):
                self.assertEqual({p.name for p in root.iterdir()}, set(bodies))
                return 'ready.json', b'{"approved":false}\n'
            def validate(root):
                self.assertFalse(target.exists())
                self.assertEqual({name: (root / name).read_bytes() for name in bodies}, bodies)
                calls.append(('validate', None))
            transaction.install_private_candidate(
                private, target, bodies.items(), write_one=write, ready=ready,
                validate_before=validate, anchor=lambda: calls.append(('anchor', None)),
                validate_after=validate)
            self.assertFalse(private.exists())
            self.assertEqual({name: (target / name).read_bytes() for name in bodies}, bodies)
            self.assertEqual(calls, [('write', 'native.gpkg'), ('write', 'native.parquet'),
                                     ('write', 'ready.json'), ('validate', None), ('anchor', None),
                                     ('validate', None)])


if __name__ == '__main__':
    unittest.main()
