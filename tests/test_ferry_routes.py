"""Complete real original-source behavior; no fixtures imply real acceptance."""
import copy
import fcntl
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa

from dagster_hifld.sources import ferry_routes as f

CAPTURE=os.environ.get('FERRY_ROUTES_TEST_CAPTURE_DIR')


class FerryTransportTests(unittest.TestCase):
    def fetch(self,headers):
        original=httpx.Client;raw=b'complete bounded encoded input';encoded=gzip.compress(raw)
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip',**headers(len(encoded))},stream=httpx.ByteStream(encoded)))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            return f.acquire(f.StagingStorageResource(use_local=True,local_dir=tmp))
    def test_gzip_wire_length_is_not_decoded_length(self):
        captures=self.fetch(lambda size:{'Content-Length':str(size)})
        for body,receipt in captures.values():
            self.assertEqual(body,b'complete bounded encoded input');self.assertEqual(int(receipt['content_length']),receipt['wire_bytes_downloaded']);self.assertNotEqual(receipt['bytes'],receipt['wire_bytes_downloaded'])
    def test_incomplete_wire_fails_and_preserves_diagnostics(self):
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,'non200_or_incomplete_http_response'):self.fetch(lambda size:{'Content-Length':str(size+1)})
    def test_chunked_complete_input(self):self.assertEqual(len(self.fetch(lambda size:{})),19)
    def test_malformed_local_receipt_retains_body_and_exact_original_receipt(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);source=root/'inputs';source.mkdir();body=b'original attempted body';bad=b'{malformed exact original receipt\n'
            (source/'bts_ntad.body').write_bytes(body);(source/'bts_ntad.receipt.json').write_bytes(bad)
            staging=root/'staging';storage=f.StagingStorageResource(use_local=True,local_dir=str(staging))
            with self.assertRaises(f.FerryRouteQuarantineError):f.acquire(storage,source)
            retained=list((staging/'_quarantine').rglob('bts_ntad.body'));self.assertEqual(len(retained),1);self.assertEqual(retained[0].read_bytes(),body)
            receipt=json.loads(retained[0].with_suffix('.receipt.json').read_text())
            self.assertIs(receipt['complete_body'],False);self.assertEqual((staging/receipt['original_receipt_key']).read_bytes(),bad)
            self.assertEqual((source/'bts_ntad.receipt.json').read_bytes(),bad)
    def test_live_partial_stream_preserved_for_each_bounded_attempt(self):
        class BrokenStream(httpx.SyncByteStream):
            def __iter__(self):
                yield b'partial original wire body'
                raise httpx.ReadError('test only interrupted stream')
        original=httpx.Client
        transport=httpx.MockTransport(lambda request:httpx.Response(200,stream=BrokenStream()))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'bounded_fetch_exhausted'):f.acquire(f.StagingStorageResource(use_local=True,local_dir=tmp))
            bodies=list(Path(tmp).glob('_quarantine/**/bts_ntad.body'));self.assertEqual(len(bodies),3)
            for body in bodies:
                self.assertEqual(body.read_bytes(),b'partial original wire body')
                receipt=json.loads(body.with_suffix('.receipt.json').read_text());self.assertIs(receipt['complete_body'],False)
                self.assertEqual(receipt['sha256'],f.digest(body.read_bytes()))
    def test_remote_published_or_alias_staging_refused(self):
        with self.assertRaises(f.FerryRouteQuarantineError):f.local_only(f.PublishedStorageResource(local_dir='/tmp'))


@unittest.skipUnless(CAPTURE,'Set FERRY_ROUTES_TEST_CAPTURE_DIR to complete unchanged19-slot official capture')
class FerryOriginalCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name)
        storage=f.StagingStorageResource(use_local=True,local_dir=str(cls.base/'original'))
        cls.output=f.stage_ferry_routes(storage,capture_dir=CAPTURE);cls.prefix=f'{f.SLUG}/{f.SLUG}/{cls.output["version"]}'
        cls.captures={n:((Path(CAPTURE)/(n+'.body')).read_bytes(),json.loads((Path(CAPTURE)/(n+'.receipt.json')).read_text()))for n in f.URLS}
        cls.table,cls.audit=f.inspect_originals(cls.captures)
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.path=Path(self.directory.name)/'staging';shutil.copytree(self.base/'original',self.path)
        self.storage=f.StagingStorageResource(use_local=True,local_dir=str(self.path));self.root=self.path/self.prefix
    def refresh(self):return f.stage_ferry_routes(self.storage,capture_dir=CAPTURE)
    def repin(self,name):
        path=self.root/f.READY;ready=json.loads(path.read_text());ready['objects'][name]=f.sha256_file(self.root/name);path.write_bytes(f.encode(ready))
    def reject(self,reason):
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,reason):self.refresh()
        self.assertFalse(self.root.exists());self.assertTrue(list((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*')))
    def changed_doc(self,name,mutate):
        captures=dict(self.captures);doc=json.loads(captures[name][0]);mutate(doc);captures[name]=(f.encode(doc),captures[name][1]);return captures
    def test_complete_original964_rows55_attributes714082_vertices(self):
        self.assertEqual(self.table.num_rows,964);self.assertEqual(len(self.table.column_names),56);self.assertEqual(self.audit['native_vertices'],714082)
        self.assertEqual(self.table['operator_id'].type,pa.string());self.assertEqual(self.table['average_trip_time'].type,pa.int32())
        self.assertEqual(self.table['vessel4_id'].type,pa.string());self.assertEqual(self.audit['operator_count'],160)
        self.assertEqual(self.table['OBJECTID'].to_pylist(),list(range(1,965)));self.assertEqual(set(self.table['census_year'].to_pylist()),{2020})
        f.reconcile_outputs(self.root,self.table);self.assertTrue(self.refresh()['reused'])
    def test_missing_duplicate_renamed_or_retyped_identifier_rejected(self):
        for table in [self.table.drop(['OBJECTID']),self.table.set_column(0,'OBJECTID',pa.array([1]*964)),self.table.set_column(0,'OBJECTID',pa.array([str(v)for v in range(1,965)]))]:
            with self.assertRaises(f.FerryRouteQuarantineError):f.validate_table(table)
    def test_schema_extra_column_and_integer_width_drift_rejected(self):
        for table in [self.table.append_column('invented',pa.array([1]*964)),self.table.set_column(4,'average_trip_time',self.table['average_trip_time'].cast(pa.int64())),self.table.slice(1)]:
            with self.assertRaises(f.FerryRouteQuarantineError):f.validate_table(table)
    def test_full_native_coordinate_and_attribute_drift_rejected(self):
        for mutate in [lambda j:j['features'][0]['geometry']['paths'][0][0].__setitem__(0,0.0),lambda j:j['features'][0]['attributes'].__setitem__('operator_id','000')]:
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'complete_original_attribute_geometry_or_coverage_values_drift'):f.inspect_originals(self.changed_doc('source_page_0001',mutate))
    def test_transfer_limit_missing_rows_and_incomplete_count_rejected(self):
        for name,mutate in [('source_page_0001',lambda j:j.__setitem__('exceededTransferLimit',True)),('source_page_0001',lambda j:j['features'].pop()),('source_count_after',lambda j:j.__setitem__('count',963))]:
            with self.assertRaises(f.FerryRouteQuarantineError):f.inspect_originals(self.changed_doc(name,mutate))
    def test_before_after_all_id_witness_requires_exact_integer_physical_types(self):
        for part in ['before','after']:
            for malformed in [True,1.0]:
                def mutate(doc):doc['objectIds'][doc['objectIds'].index(1)]=malformed
                with self.subTest(part=part,value=malformed),self.assertRaisesRegex(f.FerryRouteQuarantineError,'count_id_roster'):
                    f.inspect_originals(self.changed_doc('source_ids_'+part,mutate))
    def test_actual_vintage_and_moving_snapshot_rejected(self):
        captures=self.changed_doc('source_layer_after',lambda j:j['editingInfo'].__setitem__('dataLastEditDate',0))
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,'changed_during_extraction'):f.inspect_originals(captures)
        with self.assertRaises(f.FerryRouteQuarantineError):f.validate_table(self.table.set_column(self.table.schema.get_field_index('census_year'),'census_year',pa.array([2022]*964,type=pa.int32())))
    def test_native_container_auxiliary_crs_forgery_fails_after_self_repin(self):
        name=f'geopackage/{f.SLUG}.gpkg'
        with sqlite3.connect(self.root/name)as db:db.execute('UPDATE gpkg_contents SET srs_id=3857')
        self.repin(name);self.reject('full_container')
    def test_false_source_audit_and_approved_placeholder_cannot_self_repin(self):
        name='metadata/baseline_comparison.json';j=json.loads((self.root/name).read_text());j['approved']=True;(self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('source_owned_bytes_drift')
    def test_missing_ready_and_unknown_object_wholly_quarantined(self):
        (self.root/f.READY).unlink();self.reject('invalid_cache_moved_intact')
    def bundle(self):return next((self.path/'_source-provenance').rglob('bundle.json'))
    def test_missing_original_anchor_never_bootstrapped_from_existing_cache(self):
        bundle=self.bundle();original=bundle.read_bytes();bundle.unlink()
        state=self.state().read_bytes();evidence=self.evidence().read_bytes()
        self.reject('missing_original_provenance_bundle_after_first_construction')
        for attempt in range(2):
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):self.refresh()
            self.assertFalse(self.root.exists());self.assertFalse(bundle.exists())
            self.assertEqual(self.state().read_bytes(),state);self.assertEqual(self.evidence().read_bytes(),evidence)
        self.restore_primary()
        self.assertEqual(bundle.read_bytes(),original)
        self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
    def test_malformed_external_anchor_and_candidate_preserved_outside_discovery(self):
        self.bundle().write_bytes(b'{"manifest":{}}');self.reject('original_provenance_bundle_roster_drift')
        self.assertTrue(list((self.path/'_quarantine').rglob('invalid-provenance-*/bundle.json')))
    def test_original_code_coforged_with_nonexistent_or_wrong_real_commit_rejected(self):
        name='metadata/source_manifest.json';j=json.loads((self.root/name).read_text());j['code_commit']='4800e4f5df550bfadd3631585268ce68a0fa25d9'
        (self.root/name).write_bytes(f.encode(j));self.repin(name);p=self.bundle();anchor=json.loads(p.read_text());anchor['manifest']=j;p.write_bytes(f.encode(anchor));self.reject('construction')
    def test_common_lock_covers_full_validation(self):
        original=f.reconcile_cache
        def checking(*args,**kwargs):
            with (self.path/'_locks'/f'{f.SLUG}.lock').open('a+b')as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*args,**kwargs)
        with patch.object(f,'reconcile_cache',checking):self.assertTrue(self.refresh()['reused'])
    def test_no_anchor_created_before_private_native_output_verifies(self):
        with tempfile.TemporaryDirectory()as tmp,patch.object(f,'reconcile_outputs',side_effect=f.FerryRouteQuarantineError('test_only_native_construction_failure')):
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'test_only_native_construction_failure'):f.stage_ferry_routes(f.StagingStorageResource(use_local=True,local_dir=tmp),capture_dir=CAPTURE)
            self.assertFalse(list(Path(tmp).glob('_source-provenance/**/bundle.json')))
            self.assertFalse(list(Path(tmp).glob('_construction-state/**/*.json')))
            self.assertFalse(list(Path(tmp).glob('_construction-evidence/**/*.json')))
            self.assertFalse(list(Path(tmp).glob(f'{f.SLUG}/{f.SLUG}/v*')))

    def test_metadata_only_refresh_keeps_original_audit_and_retains_new_observation(self):
        captures=dict(self.captures)
        item=json.loads(captures['source_item'][0]);item['description']+=' [TEST ONLY: metadata observation, no changed source rows.]'
        body=f.encode(item);receipt=dict(captures['source_item'][1]);receipt.update(bytes=len(body),sha256=f.digest(body))
        captures['source_item']=(body,receipt)
        before={str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()}
        with patch.object(f,'acquire',return_value=captures):result=self.refresh()
        self.assertTrue(result['reused'])
        self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()})
        self.assertEqual((self.path/f'_raw-captures/{f.SLUG}/{f.digest(body)}/source_item.body').read_bytes(),body)

    def state(self):return next((self.path/'_construction-state'/f.SLUG).glob('*.json'))
    def evidence(self):
        marker=json.loads(self.state().read_text());return self.path/marker['original_bundle_evidence_key']
    def restore_primary(self):
        marker=json.loads(self.state().read_text());body=self.evidence().read_bytes()
        self.assertEqual(f.digest(body),marker['original_bundle_sha256'])
        fingerprint=marker['processing_fingerprint'];path=self.path/f'_source-provenance/{f.SLUG}/{fingerprint}/bundle.json'
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
    def test_missing_first_state_never_bootstrapped_after_primary_quarantine(self):
        state=self.state();original=state.read_bytes();state.unlink()
        self.reject('missing_or_invalid_first_construction_state')
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):self.refresh()
        self.assertFalse(state.exists());self.assertFalse(self.root.exists())
        state.parent.mkdir(parents=True,exist_ok=True);state.write_bytes(original);self.restore_primary()
        self.assertFalse(self.refresh()['reused'])
    def test_forged_primary_cannot_replace_original_CAS_evidence(self):
        primary=self.bundle();body=json.loads(primary.read_text());body['manifest']['code_commit']='0'*40;primary.write_bytes(f.encode(body))
        self.reject('construction')
        marker_before=self.state().read_bytes()
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):self.refresh()
        self.assertEqual(self.state().read_bytes(),marker_before)
        self.restore_primary();self.assertFalse(self.refresh()['reused'])
    def test_invalid_first_state_kept_permanent_and_repeat_rejected(self):
        state=self.state();marker=json.loads(state.read_text());marker['original_bundle_sha256']='0'*64;state.write_bytes(f.encode(marker));bad=state.read_bytes()
        self.reject('missing_or_invalid_first_construction_state')
        with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):self.refresh()
        self.assertEqual(state.read_bytes(),bad);self.assertFalse(self.root.exists())
    def test_first_primary_write_failure_cannot_create_new_history_on_retry(self):
        original=f.immutable
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);storage=f.StagingStorageResource(use_local=True,local_dir=tmp)
            def failure(store,key,body):
                if key.endswith('/bundle.json'):raise OSError('test_only_primary_write_failed_after_independent_history')
                return original(store,key,body)
            with patch.object(f,'immutable',side_effect=failure):
                with self.assertRaisesRegex(f.FerryRouteQuarantineError,'primary_write_failed'):f.stage_ferry_routes(storage,capture_dir=CAPTURE)
            records={str(p.relative_to(root)):p.read_bytes()for base in ['_construction-state','_construction-evidence']for p in(root/base).rglob('*.json')}
            self.assertEqual(len(records),2)
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):f.stage_ferry_routes(storage,capture_dir=CAPTURE)
            self.assertEqual(records,{str(p.relative_to(root)):p.read_bytes()for base in ['_construction-state','_construction-evidence']for p in(root/base).rglob('*.json')})
            self.assertFalse(list(root.glob(f'{f.SLUG}/{f.SLUG}/v*')))

    def test_missing_CAS_evidence_never_recreated_from_primary_or_candidate(self):
        evidence=self.evidence();original=evidence.read_bytes();evidence.unlink();state=self.state().read_bytes()
        self.reject('missing_or_invalid_original_construction_evidence')
        for attempt in range(2):
            with self.assertRaisesRegex(f.FerryRouteQuarantineError,'after_first_construction'):self.refresh()
            self.assertFalse(evidence.exists());self.assertFalse(self.root.exists());self.assertEqual(self.state().read_bytes(),state)
        evidence.parent.mkdir(parents=True,exist_ok=True);evidence.write_bytes(original);self.restore_primary()
        self.assertFalse(self.refresh()['reused'])
