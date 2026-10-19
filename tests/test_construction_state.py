import hashlib
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from contextlib import chdir
from unittest.mock import patch

from dagster_hifld import construction_state as state
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

KEY = '_construction-state/synthetic/v1.json'
FP = 'a' * 64
VERSION = 'v1'


def bundle(receipt='original'):
    return state.canonical_construction_bundle(VERSION, FP, {'receipt': receipt, 'capture_sha256': 'b' * 64})


def inventory(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}


class ConstructionStateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.storage = StagingStorageResource(local_dir=str(self.root))

    def seal(self, body=None):
        return state.seal_first_construction(self.storage, KEY, VERSION, FP, body or bundle())

    def load(self):
        return state.load_first_construction(self.storage, KEY, VERSION, FP)

    def assert_code(self, code, call):
        with self.assertRaises(state.ConstructionStateError) as result:
            call()
        self.assertEqual(result.exception.code, code)

    def test_lexical_root_and_ancestor_symlinks_reject_seal_and_load(self):
        outside=self.root/'outside'; outside.mkdir()
        child=outside/'child'; child.mkdir()
        (outside/'prior-sentinel.body').write_bytes(b'preserve outside source')
        for destination in (outside,child):
            state.seal_first_construction(StagingStorageResource(local_dir=str(destination)),KEY,VERSION,FP,bundle())
        before=inventory(outside)
        alias=self.root/'root-link'; alias.symlink_to(outside,target_is_directory=True)
        ancestor=self.root/'ancestor-link'; ancestor.symlink_to(outside,target_is_directory=True)
        for lexical in (alias,ancestor/'child'):
            for relative in (False,True):
                with self.subTest(path=lexical,relative=relative), chdir(self.root):
                    selected=str(lexical.relative_to(self.root)) if relative else str(lexical)
                    storage=StagingStorageResource(local_dir=selected)
                    self.assert_code('construction_invalid_storage_root',lambda:state.seal_first_construction(storage,KEY,VERSION,FP,bundle('new receipt')))
                    self.assert_code('construction_invalid_storage_root',lambda:state.load_first_construction(storage,KEY,VERSION,FP))
                    self.assertEqual(before,inventory(outside))
        # A hard-linked ordinary file is not a directory/root either.
        os.link(outside/'prior-sentinel.body',self.root/'not-a-directory')
        invalid=StagingStorageResource(local_dir=str(self.root/'not-a-directory'))
        self.assert_code('construction_invalid_storage_root',lambda:state.seal_first_construction(invalid,KEY,VERSION,FP,bundle()))
        self.assertEqual(before,inventory(outside))

    def test_published_or_nonstorage_resource_rejected_before_any_directory_creation(self):
        for resource in (PublishedStorageResource(local_dir=str(self.root/'must-not-exist')),
                         PublishedStorageResource(bucket='synthetic',use_local=False),None,object()):
            self.assert_code('construction_staging_resource_required',lambda:state.seal_first_construction(resource,KEY,VERSION,FP,bundle()))
            self.assert_code('construction_staging_resource_required',lambda:state.load_first_construction(resource,KEY,VERSION,FP))
        self.assertEqual(list(self.root.iterdir()),[])

    def test_original_receipts_immutable_and_prefix_supported(self):
        original = self.seal(); before = inventory(self.root)
        self.assertEqual(self.seal(bundle('newer observation')), original)
        self.assertEqual(self.load(), original)
        self.assertEqual(before, inventory(self.root))
        prefixed = StagingStorageResource(local_dir=str(self.root), prefix='tenant')
        self.assertEqual(state.seal_first_construction(prefixed, KEY, VERSION, FP, bundle()), original)
        self.assertTrue((self.root/'tenant'/KEY).is_file())

    def test_missing_alias_after_quarantine_blocks_every_refresh_and_exact_restore_works(self):
        original = self.seal(); alias = self.root/KEY
        alias.rename(self.root/'quarantined-original.body')
        before = inventory(self.root)
        for _ in range(3):
            self.assert_code('construction_missing_original_state', lambda: self.seal(bundle('newer')))
            self.assert_code('construction_missing_original_state', self.load)
        self.assertEqual(before, inventory(self.root))
        # Explicit trusted operator restoration, never performed by the helper.
        alias.write_bytes(original)
        self.assertEqual(self.load(), original)

    def test_marker_missing_or_forged_never_bootstraps(self):
        self.seal(); marker = self.root/(KEY+'.first-construction.json')
        original = marker.read_bytes(); marker.unlink()
        self.assert_code('construction_missing_original_state', self.seal)
        marker.write_bytes(original)
        for changes in ({'original_bundle_sha256': '0'*64}, {'original_bundle_evidence_key':'../source'},
                        {'schema_version': True}, {'version':'other'}, {'processing_fingerprint':'c'*64}):
            value=json.loads(original); value.update(changes)
            marker.write_bytes(state._encode(value)); before=inventory(self.root)
            self.assert_code('construction_marker_mismatch', self.seal)
            self.assertEqual(before,inventory(self.root))

    def test_interruption_after_cas_or_marker_retains_evidence_and_blocks_second_refresh(self):
        install=state._install_once
        for boundary in (1,2):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                storage=StagingStorageResource(local_dir=directory);calls=[]
                def interrupted(root,key,body):
                    install(root,key,body);calls.append(key)
                    if len(calls)==boundary: raise KeyboardInterrupt()
                with patch.object(state,'_install_once',interrupted):
                    with self.assertRaises(KeyboardInterrupt):
                        state.seal_first_construction(storage,KEY,VERSION,FP,bundle())
                before=inventory(Path(directory));self.assertTrue(before)
                for _ in range(2):
                    self.assert_code('construction_missing_original_state',lambda:state.seal_first_construction(storage,KEY,VERSION,FP,bundle('newer')))
                self.assertEqual(before,inventory(Path(directory)))
                evidence=Path(directory)/(KEY+'.evidence')/(hashlib.sha256(bundle()).hexdigest()+'.json')
                self.assertEqual(evidence.read_bytes(),bundle())

    def test_marker_write_oserror_retains_cas_and_rejects_newer_rebind(self):
        install=state._install_once
        def fail_marker(root,key,body):
            if key.endswith('.first-construction.json'): raise OSError('injected marker failure')
            install(root,key,body)
        with patch.object(state,'_install_once',fail_marker):
            self.assert_code('construction_state_io_failed',self.seal)
        before=inventory(self.root)
        self.assertEqual(len(before),1)
        for _ in range(2):
            self.assert_code('construction_missing_original_state',lambda:self.seal(bundle('newer docs HEAD receipt')))
            self.assert_code('construction_missing_original_state',self.load)
        self.assertEqual(before,inventory(self.root))

    def test_missing_cas_with_valid_alias_and_marker_blocks(self):
        original=self.seal()
        evidence=self.root/(KEY+'.evidence')/(hashlib.sha256(original).hexdigest()+'.json')
        evidence.unlink();before=inventory(self.root)
        for call in (self.load,self.seal):
            self.assert_code('construction_original_evidence_mismatch',call)
        self.assertEqual(before,inventory(self.root))

    def test_partial_first_cas_write_and_storage_failure_remain_discoverable(self):
        original_fsync=os.fsync
        def fail_file_fsync(descriptor):
            if stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError('injected file durability failure')
            original_fsync(descriptor)
        with patch.object(state.os,'fsync',side_effect=fail_file_fsync):
            self.assert_code('construction_state_io_failed',self.seal)
        evidence=self.root/(KEY+'.evidence')
        pending=list(evidence.glob('.pending-*'))
        self.assertEqual(len(pending),1)
        self.assertEqual(pending[0].read_bytes(),bundle())
        before=inventory(self.root)
        self.assert_code('construction_missing_original_state',self.seal)
        self.assertEqual(before,inventory(self.root))

    def test_concurrent_marker_winner_preserved_with_both_complete_bundles(self):
        install=state._install_once;winner=bundle('concurrent winner');loser=bundle('loser')
        fired=[]
        def racing(root,key,body):
            if key==KEY+'.first-construction.json' and not fired:
                fired.append(True)
                digest=hashlib.sha256(winner).hexdigest();evidence=KEY+'.evidence/'+digest+'.json'
                install(root,evidence,winner)
                install(root,key,state._encode({'schema_version':1,'version':VERSION,'processing_fingerprint':FP,
                    'original_bundle_sha256':digest,'original_bundle_evidence_key':evidence}))
                install(root,KEY,winner)
            install(root,key,body)
        with patch.object(state,'_install_once',racing):
            self.assert_code('construction_concurrent_winner_differs',lambda:self.seal(loser))
        self.assertEqual(self.load(),winner)
        self.assertEqual(len(list((self.root/(KEY+'.evidence')).glob('*.json'))),2)
        before=inventory(self.root);self.assertEqual(self.seal(loser),winner)
        self.assertEqual(before,inventory(self.root))

    def test_invalid_body_identity_paths_remote_and_sizes_fail_without_writes(self):
        for key in ('candidate/v1.json','_construction-state/../x.json','/_construction-state/x.json',
                    '_construction-state//x.json','_construction-state/link\\x.json','_construction-state/x.txt'):
            self.assert_code('construction_unsafe_record_key',lambda:state.seal_first_construction(self.storage,key,VERSION,FP,bundle()))
        for version in ('..','/v1','v1/other',False):
            self.assert_code('construction_invalid_version',lambda:state.seal_first_construction(self.storage,KEY,version,FP,bundle()))
        self.assert_code('construction_invalid_fingerprint',lambda:state.seal_first_construction(self.storage,KEY,VERSION,'bad',bundle()))
        for body in (b'{}', b'{"schema_version":1,"schema_version":1}', bundle()+b'\n', b'{"x":NaN}', b'\xff'):
            with self.assertRaises(state.ConstructionStateError):self.seal(body)
        with patch.object(state,'_MAX_BODY',10):
            self.assert_code('construction_invalid_record_body',self.seal)
        remote=StagingStorageResource(bucket='synthetic',use_local=False)
        self.assert_code('construction_remote_protocol_unsupported',lambda:state.seal_first_construction(remote,KEY,VERSION,FP,bundle()))
        for payload in ({1:'nonstring key'}, {'tuple':(1,2)}, {'nonfinite':float('nan')}):
            self.assert_code('construction_invalid_bundle',lambda:state.canonical_construction_bundle(VERSION,FP,payload))
        self.assertEqual(inventory(self.root),{})

    def test_linked_alias_parents_and_changed_cas_reject_without_touching_targets(self):
        self.seal();alias=self.root/KEY;original=alias.read_bytes()
        target=self.root/'outside.body';target.write_bytes(b'preserve original')
        alias.unlink();alias.symlink_to(target)
        self.assert_code('construction_state_io_failed',self.load)
        self.assertEqual(target.read_bytes(),b'preserve original')
        alias.unlink();os.link(target,alias)
        self.assert_code('construction_unsafe_record_object',self.load)
        alias.unlink();alias.write_bytes(original)
        evidence=self.root/(KEY+'.evidence')/(hashlib.sha256(original).hexdigest()+'.json')
        evidence.write_bytes(b'changed')
        self.assert_code('construction_original_evidence_mismatch',self.load)
        with tempfile.TemporaryDirectory() as directory:
            parent=self.root/'_construction-state'/'linked';parent.symlink_to(directory,target_is_directory=True)
            self.assert_code('construction_state_io_failed',lambda:state.seal_first_construction(self.storage,'_construction-state/linked/v1.json',VERSION,FP,bundle()))
            self.assertEqual(list(Path(directory).iterdir()),[])


if __name__=='__main__': unittest.main()
