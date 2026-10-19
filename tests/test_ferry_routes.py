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
        self.bundle().unlink();self.reject('missing_original_provenance_bundle');self.assertFalse(list((self.path/'_source-provenance').rglob('bundle.json')))
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
            self.assertFalse(list(Path(tmp).glob(f'{f.SLUG}/{f.SLUG}/v*')))
