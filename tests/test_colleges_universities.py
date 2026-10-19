"""Actual-source regression probes; complete original capture is opt-in, never synthesized."""
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
from dagster_hifld.sources import colleges_universities as c

CAPTURE=os.environ.get('COLLEGES_TEST_CAPTURE_DIR')

class CollegeTransportTests(unittest.TestCase):
    def fetch(self,headers):
        original=httpx.Client
        raw=b'bounded complete original'
        encoded=gzip.compress(raw)
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip',**headers(len(encoded))},stream=httpx.ByteStream(encoded)))
        with tempfile.TemporaryDirectory() as tmp,patch.object(c.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            store=c.StagingStorageResource(use_local=True,local_dir=tmp)
            return c.acquire(store)
    def test_gzip_content_length_measures_wire_not_decoded_bytes(self):
        captures=self.fetch(lambda size:{'Content-Length':str(size)})
        self.assertEqual(set(captures),set(c.URLS))
        for body,receipt in captures.values():
            self.assertEqual(body,b'bounded complete original')
            self.assertEqual(int(receipt['content_length']),receipt['wire_bytes_downloaded'])
            self.assertNotEqual(receipt['bytes'],receipt['wire_bytes_downloaded'])
    def test_gzip_short_wire_is_rejected(self):
        with self.assertRaisesRegex(c.CollegeQuarantineError,'non200_or_incomplete_http_response'):
            self.fetch(lambda size:{'Content-Length':str(size+1)})
    def test_chunked_gzip_complete_response(self):
        self.assertEqual(len(self.fetch(lambda size:{})),3)
    def test_remote_output_storage_refused(self):
        with self.assertRaises(c.CollegeQuarantineError):c.local_only(c.PublishedStorageResource(local_dir='/tmp'))

@unittest.skipUnless(CAPTURE,'Set COLLEGES_TEST_CAPTURE_DIR to complete unmodified NCES capture')
class CollegeOriginalCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name)
        original=c.StagingStorageResource(use_local=True,local_dir=str(cls.base/'original'))
        cls.result=c.stage_colleges(original,capture_dir=CAPTURE)
        cls.prefix=f'{c.SLUG}/{c.SLUG}/{cls.result["version"]}'
        cls.captures={name:((Path(CAPTURE)/(name+'.body')).read_bytes(),json.loads((Path(CAPTURE)/(name+'.receipt.json')).read_text()))for name in c.URLS}
        cls.table,cls.audit=c.inspect_archives({},cls.captures)
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.path=Path(self.directory.name)/'staging';shutil.copytree(self.base/'original',self.path)
        self.storage=c.StagingStorageResource(use_local=True,local_dir=str(self.path));self.root=self.path/self.prefix
    def refresh(self):return c.stage_colleges(self.storage,capture_dir=CAPTURE)
    def repin(self,name):
        p=self.root/c.READY;j=json.loads(p.read_text());j['objects'][name]=c.sha256_file(self.root/name);p.write_bytes(c.encode(j))
    def reject(self,reason):
        with self.assertRaisesRegex(c.CollegeQuarantineError,reason):self.refresh()
        self.assertFalse(self.root.exists())
        self.assertTrue(list((self.path/'_quarantine'/c.SLUG).glob('invalid-candidate-*')))
    def test_complete_original_full_rows_id_types_geometry_and_roundtrip(self):
        self.assertEqual(self.table.num_rows,6438);self.assertEqual(self.table['UNITID'].type,pa.string())
        self.assertEqual(len(set(self.table['STATE'].to_pylist())),59)
        self.assertEqual(self.audit['independent_dbf_and_official_text_rows_compared'],6438)
        c.reconcile_outputs(self.root,self.table)
        self.assertTrue(self.refresh()['reused'])
    def test_missing_and_duplicate_identifiers_rejected(self):
        for name,values in [('UNITID',[self.table['UNITID'][0].as_py()]*6438),('OGC_FID',[0]*6438)]:
            table=self.table.set_column(self.table.schema.get_field_index(name),name,pa.array(values,type=self.table[name].type))
            with self.assertRaises(c.CollegeQuarantineError):c.validate_table(table)
    def test_schema_physical_type_and_coverage_drift_rejected(self):
        for table in [self.table.drop(['UNITID']),self.table.append_column('extra',pa.array([1]*6438)),self.table.set_column(1,'UNITID',pa.array([int(x)for x in self.table['UNITID'].to_pylist()])),self.table.slice(1)]:
            with self.assertRaises(c.CollegeQuarantineError):c.validate_table(table)
    def test_native_wkb_value_drift_rejected(self):
        values=self.table['wkb_geometry'].to_pylist();values[0]=values[1]
        table=self.table.set_column(self.table.schema.get_field_index('wkb_geometry'),'wkb_geometry',pa.array(values))
        with self.assertRaisesRegex(c.CollegeQuarantineError,'native_geometry_values_drift'):c.validate_table(table)
    def test_truncated_and_changed_complete_zip_rejected(self):
        for value in [self.captures['nces_geocode'][0][:-1],b'not a complete source']:
            captures=dict(self.captures);captures['nces_geocode']=(value,self.captures['nces_geocode'][1])
            with self.assertRaisesRegex(c.CollegeQuarantineError,'changed_truncated'):c.inspect_archives({},captures)
    def test_whole_container_auxiliary_crs_mutation_rejected_after_self_repin(self):
        name=f'geopackage/{c.SLUG}.gpkg'
        with sqlite3.connect(self.root/name) as db:db.execute("UPDATE gpkg_contents SET srs_id=3857")
        self.repin(name);self.reject('full_container')
    def test_self_repinned_false_source_report_rejected(self):
        name='metadata/colleges_source_audit.json';j=json.loads((self.root/name).read_text());j['feature_count']=1;(self.root/name).write_bytes(c.encode(j));self.repin(name);self.reject('source_owned_bytes_drift')
    def test_self_repinned_unapproved_placeholder_cannot_approve(self):
        name='metadata/baseline_comparison.json';j=json.loads((self.root/name).read_text());j['approved']=True;(self.root/name).write_bytes(c.encode(j));self.repin(name);self.reject('source_owned_bytes_drift')
    def test_self_repinned_construction_commit_cannot_rewrite_original(self):
        name='metadata/source_manifest.json';j=json.loads((self.root/name).read_text());j['code_commit']='0'*40;(self.root/name).write_bytes(c.encode(j));self.repin(name);self.reject('immutable_original_bundle')
    def test_missing_ready_marker_moves_whole_cache_out_of_discovery(self):
        (self.root/c.READY).unlink();self.reject('invalid_cache_moved_intact')
    def test_unknown_object_moves_whole_cache_out_of_discovery(self):
        (self.root/'invented_ids.csv').write_text('ID\n0\n');self.reject('unknown_or_missing_object')
    def test_lock_covers_validation_and_quarantine(self):
        original=c.reconcile_cache
        def checking(*args,**kwargs):
            with (self.path/'_locks'/f'{c.SLUG}.lock').open('a+b') as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*args,**kwargs)
        with patch.object(c,'reconcile_cache',checking):self.assertTrue(self.refresh()['reused'])
