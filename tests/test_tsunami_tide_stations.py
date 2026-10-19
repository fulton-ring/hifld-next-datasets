"""Actual complete source and failure behavior; no real acceptance implied."""
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
from dagster_hifld.sources import tsunami_tide_stations as f
CAPTURE=os.environ.get("TSUNAMI_TIDE_TEST_CAPTURE_DIR")

class TideTransportTests(unittest.TestCase):
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
        with self.assertRaisesRegex(f.TideStationQuarantineError,'non200_or_incomplete_http_response'):self.fetch(lambda size:{'Content-Length':str(size+1)})
    def test_chunked_complete_input(self):self.assertEqual(len(self.fetch(lambda size:{})),18)
    def test_malformed_local_receipt_retains_body_and_exact_original_receipt(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);source=root/'inputs';source.mkdir();body=b'original attempted body';bad=b'{malformed exact original receipt\n'
            (source/'bts_ntad.body').write_bytes(body);(source/'bts_ntad.receipt.json').write_bytes(bad)
            staging=root/'staging';storage=f.StagingStorageResource(use_local=True,local_dir=str(staging))
            with self.assertRaises(f.TideStationQuarantineError):f.acquire(storage,source)
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
            with self.assertRaisesRegex(f.TideStationQuarantineError,'bounded_fetch_exhausted'):f.acquire(f.StagingStorageResource(use_local=True,local_dir=tmp))
            bodies=list(Path(tmp).glob('_quarantine/**/bts_ntad.body'));self.assertEqual(len(bodies),3)
            for body in bodies:
                self.assertEqual(body.read_bytes(),b'partial original wire body')
                receipt=json.loads(body.with_suffix('.receipt.json').read_text());self.assertIs(receipt['complete_body'],False)
                self.assertEqual(receipt['sha256'],f.digest(body.read_bytes()))
    def test_remote_published_or_alias_staging_refused(self):
        with self.assertRaises(f.TideStationQuarantineError):f.local_only(f.PublishedStorageResource(local_dir='/tmp'))

@unittest.skipUnless(CAPTURE,'Set TSUNAMI_TIDE_TEST_CAPTURE_DIR to full original official capture')
class TideOriginalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name);storage=f.StagingStorageResource(use_local=True,local_dir=str(cls.base/'original'));cls.output=f.stage_tide_stations(storage,capture_dir=CAPTURE);cls.prefix=f'{f.SLUG}/{f.SLUG}/{cls.output["version"]}';C=Path(CAPTURE);cls.caps={n:((C/(n+'.body')).read_bytes(),json.loads((C/(n+'.receipt.json')).read_text()))for n in f.URLS};cls.table,cls.audit=f.inspect_originals(cls.caps)
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.tempdir=tempfile.TemporaryDirectory();self.addCleanup(self.tempdir.cleanup);self.path=Path(self.tempdir.name)/'staging';shutil.copytree(self.base/'original',self.path);self.storage=f.StagingStorageResource(use_local=True,local_dir=str(self.path));self.root=self.path/self.prefix
    def refresh(self):return f.stage_tide_stations(self.storage,capture_dir=CAPTURE)
    def changed_doc(self,name,mutate):
        caps=dict(self.caps);j=json.loads(caps[name][0]);mutate(j);body=f.encode(j);receipt=dict(caps[name][1]);receipt.update(bytes=len(body),sha256=f.digest(body));caps[name]=(body,receipt);return caps
    def repin(self,name):
        path=self.root/f.READY;j=json.loads(path.read_text());j['objects'][name]=f.sha256_file(self.root/name);path.write_bytes(f.encode(j))
    def record(self):return next((self.path/'_construction-state'/f.SLUG).glob('*.json'))
    def marker(self):return Path(str(self.record())+'.first-construction.json')
    def reject(self,reason):
        with self.assertRaisesRegex(f.TideStationQuarantineError,reason):self.refresh()
        self.assertFalse(self.root.exists());self.assertTrue(list((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*')))
    def test_complete374_original_fields_ids_native_points_and_reuse(self):
        self.assertEqual(self.table.num_rows,374);self.assertEqual(len(self.table.column_names),11);self.assertEqual(self.table['STAT_OBJID'].to_pylist(),f.ORIGINAL_IDS);self.assertEqual(self.table['STAT'].type,pa.int32());self.assertEqual(self.table['OID'].type,pa.int32());self.assertEqual(self.table['STAT_ST'].null_count,22);self.assertEqual(self.table['STAT_URL_REAL'].null_count,26);f.reconcile_outputs(self.root,self.table);self.assertTrue(self.refresh()['reused'])
    def test_missing_renamed_duplicate_and_retyped_ids_rejected(self):
        for tab in [self.table.drop(['STAT']),self.table.rename_columns(['renamed']+list(self.table.column_names)[1:]),self.table.set_column(0,'STAT',pa.array([1]*374,type=pa.int32())),self.table.set_column(0,'STAT',self.table['STAT'].cast(pa.int64()))]:
            with self.assertRaises(f.TideStationQuarantineError):f.validate_table(tab)
    def test_unexpected_column_and_incomplete_rows_rejected(self):
        for tab in [self.table.append_column('invented',pa.array([1]*374)),self.table.slice(1)]:
            with self.assertRaises(f.TideStationQuarantineError):f.validate_table(tab)
    def test_geometry_and_literal_attribute_drift_rejected(self):
        for mutate in [lambda j:j['features'][0]['geometry'].__setitem__('x',0.0),lambda j:j['features'][0]['attributes'].__setitem__('STAT_NAME','changed')]:
            with self.assertRaisesRegex(f.TideStationQuarantineError,'complete_original_attribute_geometry_or_coverage'):f.inspect_originals(self.changed_doc('source_page_0000',mutate))
    def test_before_after_bool_float_and_duplicate_witness_rejected(self):
        for part in ['before','after']:
            for value in [True,1630.0,None]:
                with self.subTest(part=part,value=value),self.assertRaisesRegex(f.TideStationQuarantineError,'count_id_roster'):
                    f.inspect_originals(self.changed_doc('source_ids_'+part,lambda j:j['objectIds'].__setitem__(0,value)))
    def test_pagination_transfer_limit_missing_rows_and_count_drift(self):
        for name,mutate in [('source_page_0000',lambda j:j.__setitem__('exceededTransferLimit',True)),('source_page_0000',lambda j:j['features'].pop()),('source_count_after',lambda j:j.__setitem__('count',373))]:
            with self.assertRaises(f.TideStationQuarantineError):f.inspect_originals(self.changed_doc(name,mutate))
    def test_schema_crs_and_snapshot_drift_rejected(self):
        for mutate in [lambda j:j['editingInfo'].__setitem__('dataLastEditDate',0),lambda j:j['fields'][0].__setitem__('nullable',False),lambda j:j['spatialReference'].__setitem__('latestWkid',4326)]:
            with self.assertRaises(f.TideStationQuarantineError):f.inspect_originals(self.changed_doc('source_layer_after',mutate))
    def test_official_webmap_layer_delegation_drift_rejected(self):
        caps=self.changed_doc('source_item',lambda j:j.__setitem__('owner','third_party'))
        with self.assertRaisesRegex(f.TideStationQuarantineError,'publisher_service'):f.manifest_for(caps,f.processing_identity())
    def test_native_gpkg_forgery_cannot_self_repin(self):
        name=f'geopackage/{f.SLUG}.gpkg'
        with sqlite3.connect(self.root/name)as db:db.execute('UPDATE gpkg_contents SET srs_id=4326')
        self.repin(name);self.reject('full_container')
    def test_false_source_report_and_approval_placeholder_cannot_self_repin(self):
        name='metadata/baseline_comparison.json';j=json.loads((self.root/name).read_text());j['approved']=True;(self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('source_owned_bytes')
    def test_missing_ready_whole_candidate_preserved(self):
        (self.root/f.READY).unlink();self.reject('invalid_cache_moved_intact')
    def test_primary_missing_three_retries_never_rebase_and_trusted_restore(self):
        record=self.record();original=record.read_bytes();marker=self.marker();mark=marker.read_bytes();j=json.loads(mark);evidence=self.path/j['original_bundle_evidence_key'];body=evidence.read_bytes();record.unlink();self.reject('construction_missing_original_state')
        for attempt in range(2):
            with self.assertRaisesRegex(f.TideStationQuarantineError,'construction_missing_original_state'):self.refresh()
            self.assertFalse(record.exists());self.assertFalse(self.root.exists());self.assertEqual(marker.read_bytes(),mark);self.assertEqual(evidence.read_bytes(),body)
        record.write_bytes(original);self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
    def test_missing_marker_rejects_without_automatic_repair(self):
        marker=self.marker();original=marker.read_bytes();marker.unlink();self.reject('construction_missing_original_state')
        with self.assertRaisesRegex(f.TideStationQuarantineError,'construction_missing_original_state'):self.refresh()
        self.assertFalse(marker.exists());marker.write_bytes(original);self.assertFalse(self.refresh()['reused'])
    def test_dual_original_and_candidate_coforge_fails_git_identity(self):
        record=self.record();envelope=json.loads(record.read_text());envelope['payload']['manifest']['code_commit']='0'*40;record.write_bytes(f.encode(envelope));name='metadata/source_manifest.json';j=json.loads((self.root/name).read_text());j['code_commit']='0'*40;(self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('construction')
    def test_metadata_only_observation_preserved_without_original_rewrite(self):
        caps=self.changed_doc('source_item',lambda j:j.__setitem__('description',j['description']+' [TEST ONLY newer metadata]'));before={str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()}
        with patch.object(f,'acquire',return_value=caps):self.assertTrue(self.refresh()['reused'])
        self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()});body=caps['source_item'][0];self.assertEqual((self.path/f'_raw-captures/{f.SLUG}/{f.digest(body)}/source_item.body').read_bytes(),body)
    def test_no_first_state_before_private_native_validation(self):
        with tempfile.TemporaryDirectory()as tmp,patch.object(f,'reconcile_outputs',side_effect=f.TideStationQuarantineError('test_only_private_failure')):
            with self.assertRaisesRegex(f.TideStationQuarantineError,'test_only_private_failure'):f.stage_tide_stations(f.StagingStorageResource(use_local=True,local_dir=tmp),capture_dir=CAPTURE)
            self.assertFalse(list(Path(tmp).glob('_construction-state/**/*')))
    def test_common_lock_covers_whole_cache_validation(self):
        original=f.reconcile_cache
        def checking(*args,**kwargs):
            with(self.path/'_locks'/(f.SLUG+'.lock')).open('a+b')as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*args,**kwargs)
        with patch.object(f,'reconcile_cache',checking):self.assertTrue(self.refresh()['reused'])
