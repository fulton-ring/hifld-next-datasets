"""Actual-source regression probes; complete original capture is opt-in, never synthesized."""
import copy
import fcntl
import struct
import zipfile
import io
import hashlib
import gzip
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
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
    def bundle_path(self):return self.path/c.construction_record_key(self.result['version'])
    def state_backup(self):return {p:p.read_bytes()for p in(self.path/'_construction-state').rglob('*')if p.is_file()}
    def restore_fixture_state(self,backup,label):
        for p,body in backup.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
        incident=self.path/c.incident_directory(self.result['version'])
        if incident.exists():incident.rename(self.path.parent/('operator-retained-fixture-'+label))
    def test_missing_all_original_witnesses_blocks_repeated_cold_bootstrap(self):
        backup=self.state_backup()
        for p in backup:p.unlink()
        self.reject('observed_original_trust_loss')
        for _ in range(2):
            with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
        for p,body in backup.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
        with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
        self.restore_fixture_state(backup,'missing-all');self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
    def test_malformed_primary_retained_with_negative_incident(self):
        backup=self.state_backup();bundle=self.bundle_path();body=b'{"malformed":"synthetic anchor"}\n';bundle.write_bytes(body)
        self.reject('construction_noncanonical_record_body');self.assertEqual(bundle.read_bytes(),body)
        with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
        self.restore_fixture_state(backup,'malformed');self.assertFalse(self.refresh()['reused'])
    def test_no_first_state_before_complete_private_native_construction(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage=c.StagingStorageResource(use_local=True,local_dir=tmp)
            with patch.object(c,'reconcile_outputs',side_effect=c.CollegeQuarantineError('synthetic_private_failure')),self.assertRaisesRegex(c.CollegeQuarantineError,'synthetic_private_failure'):c.stage_colleges(storage,capture_dir=CAPTURE)
            self.assertFalse((Path(tmp)/'_construction-state').exists());self.assertFalse(list(Path(tmp).glob(f'{c.SLUG}/{c.SLUG}/*')))
    def test_candidate_and_primary_coforge_cannot_rebind_shared_first_context(self):
        bundle=self.bundle_path();value=json.loads(bundle.read_bytes());value['payload']['manifest']['code_commit']='0'*40;bundle.write_bytes(c.canonical_construction_bundle(value['version'],value['processing_fingerprint'],value['payload']))
        self.reject('construction_marker_mismatch')
        for _ in range(2):
            with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
    def test_failed_and_partial_negative_journals_preserve_candidate(self):
        bundle=self.bundle_path();body=bundle.read_bytes();bundle.unlink();before={str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()}
        with patch.object(c,'record_observed_loss',side_effect=OSError('synthetic incident IO failure')),self.assertRaisesRegex(c.IncidentWriteError,'candidate_preserved'):self.refresh()
        self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()});bundle.write_bytes(body)
        incident=self.path/c.incident_directory(self.result['version']);incident.mkdir()
        for _ in range(2):
            with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
            self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()})
    def test_directory_fsync_failure_preserves_candidate_and_partial_incident_blocks_retry(self):
        bundle=self.bundle_path();body=bundle.read_bytes();bundle.unlink();before={str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()};real=os.fsync;calls=[]
        def fail_directory(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):calls.append(os.readlink('/proc/self/fd/'+str(fd)));raise OSError('synthetic directory fsync failure')
            return real(fd)
        with patch('os.fsync',side_effect=fail_directory),self.assertRaisesRegex(c.IncidentWriteError,'candidate_preserved'):self.refresh()
        self.assertTrue(calls);self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()});bundle.write_bytes(body)
        for _ in range(2):
            with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):self.refresh()
            self.assertEqual(before,{str(p.relative_to(self.root)):p.read_bytes()for p in self.root.rglob('*')if p.is_file()})
    def test_short_private_manifest_and_ready_writes_never_expose_bad_candidate(self):
        real=Path.write_bytes
        for name,occurrence,first_sealed in [('source_manifest.json',1,False),('source_manifest.json',2,True),(Path(c.READY).name,1,False)]:
            with self.subTest(name=name,occurrence=occurrence),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);storage=c.StagingStorageResource(local_dir=tmp);seen=[]
                def short(path,body):
                    if '_building'in path.parts and path.name==name:
                        seen.append(len(body))
                        if len(seen)==occurrence:return real(path,body[:17])
                    return real(path,body)
                with patch.object(Path,'write_bytes',short),self.assertRaisesRegex(c.CollegeQuarantineError,'incomplete_private_write'):c.stage_colleges(storage,capture_dir=CAPTURE)
                self.assertEqual(len(seen),occurrence);self.assertFalse(list(root.glob(f'{c.SLUG}/{c.SLUG}/*')));self.assertTrue((root/'_raw-captures'/c.SLUG).exists())
                original=[q for q in(root/'_construction-state').rglob('*.first-construction.json')if q.is_file()]if(root/'_construction-state').exists()else[]
                self.assertEqual(bool(original),first_sealed)
                if first_sealed:
                    witnesses={q:q.read_bytes()for q in(root/'_construction-state').rglob('*')if q.is_file()};self.assertFalse(c.stage_colleges(storage,capture_dir=CAPTURE)['reused']);self.assertTrue(c.stage_colleges(storage,capture_dir=CAPTURE)['reused']);self.assertEqual(witnesses,{q:q.read_bytes()for q in witnesses})

    def test_complete_private_metadata_ready_and_closed_roster_reread_before_seal(self):
        real=c.write_private
        for target in('metadata/colleges_source_audit.json',c.READY,'invented-source.csv'):
            with self.subTest(target=target),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);storage=c.StagingStorageResource(local_dir=tmp);seen=[]
                def corrupt(private,name,body):
                    real(private,name,body)
                    if name==c.READY and not seen:seen.append(name);(private/target).write_bytes(b'{}')
                with patch.object(c,'write_private',corrupt),self.assertRaisesRegex(c.CollegeQuarantineError,'private_candidate_'):c.stage_colleges(storage,capture_dir=CAPTURE)
                self.assertTrue(seen);self.assertFalse((root/'_construction-state').exists());self.assertFalse(list(root.glob(f'{c.SLUG}/{c.SLUG}/*')));self.assertTrue((root/'_raw-captures'/c.SLUG).exists())

    def test_bad_Git_claim_fails_before_first_shared_seal(self):
        identity=c.processing_identity();manifest=c.manifest_for(self.captures,identity);manifest['code_commit']='0'*40
        with tempfile.TemporaryDirectory()as tmp:
            storage=c.StagingStorageResource(local_dir=tmp);c.local_only(storage)
            with self.assertRaisesRegex(ValueError,'construction_invalid_commit'):c.original_provenance_bundle(storage,c.digest(c.encode(identity)),self.captures,identity,manifest,create=True)
            self.assertFalse((Path(tmp)/'_construction-state').exists())

def small_dbf():
    # Two explicit synthetic structural rows; never a national source substitute.
    rows=[]
    for index in range(2):
        row={name:''for name,kind,width,decimals in c.DBF_FIELDS};row.update(UNITID='000001'+str(index),NAME='  Synthetic College '+str(index),STREET='  10 Example Rd',CITY='Example',STATE='AS'if index==0 else'MP',ZIP='00123-000'+str(index),STFIP='60'if index==0 else'69',CNTY='60010'if index==0 else'69110',LOCALE='11',LAT=38.123456+index,LON=-77.123456-index,SCHOOLYEAR='2025-2026');rows.append(row)
    header=32+len(c.DBF_FIELDS)*32+1;record=1+sum(f[2]for f in c.DBF_FIELDS);body=bytearray(header);body[0]=3;body[4:8]=len(rows).to_bytes(4,'little');body[8:10]=header.to_bytes(2,'little');body[10:12]=record.to_bytes(2,'little')
    for index,(name,kind,width,decimals)in enumerate(c.DBF_FIELDS):
        start=32+32*index;body[start:start+len(name)]=name.encode();body[start+11]=ord(kind);body[start+16]=width;body[start+17]=decimals
    body[-1]=13
    for row in rows:
        body+=b' '
        for name,kind,width,decimals in c.DBF_FIELDS:
            value=(format(row[name],'.6f')if kind=='N'else row[name]).encode();assert len(value)<=width;body+=value.rjust(width,b' ')if kind=='N'else value.ljust(width,b' ')
    return bytes(body)+b'\x1a',rows

class CollegeStructuralTests(unittest.TestCase):
    def test_two_synthetic_binary_DBF_rows_keep_leading_characters_and_precision(self):
        body,rows=small_dbf();self.assertEqual(c.dbf_definition(body)['row_count'],2);self.assertEqual(c.decode_dbf_rows(body),rows);self.assertTrue(rows[0]['NAME'].startswith('  '));self.assertEqual(rows[0]['ZIP'],'00123-0000')
    def test_small_DBF_corrupt_headers_types_and_truncation_fail(self):
        body,_=small_dbf()
        for changed in [body[:-4],body[:32]+b'X'+body[33:],body[:43]+b'N'+body[44:]]:
            with self.assertRaises(c.CollegeQuarantineError):c.dbf_definition(changed)
    def test_two_synthetic_native_rows_roundtrip_all_original_types_and_unsnapped_WKB(self):
        _,rows=small_dbf();values=[]
        for index,row in enumerate(rows):values.append({'OGC_FID':index,**row,'wkb_geometry':shapely.to_wkb(shapely.Point(row['LON']+0.00000025,row['LAT']+0.00000025))})
        table=pa.Table.from_pylist(values,schema=pa.schema([pa.field(n,t)for n,t in zip(c.FIELDS,c.TYPES)]))
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);(root/'geopackage').mkdir();(root/'geoparquet').mkdir();gp=root/'geopackage'/f'{c.SLUG}.gpkg';pqfile=root/'geoparquet'/f'{c.SLUG}.parquet';c.write_native_gpkg(table,gp);pq.write_table(c.expected_parquet_table(table),pqfile,compression='zstd');pin={'bytes':pqfile.stat().st_size,'sha256':c.sha256_file(pqfile)}
            with patch.object(c,'PARQUET_PIN',pin):c.reconcile_outputs(root,table)
            actual=pq.read_table(pqfile);self.assertEqual(actual['UNITID'].to_pylist(),['0000010','0000011']);self.assertEqual(actual['NAME'].to_pylist(),[r['NAME']for r in rows]);self.assertEqual(actual['wkb_geometry'].to_pylist(),table['wkb_geometry'].to_pylist())
    def test_small_zip_roster_crc_and_unsafe_member_validation(self):
        for name in ['one.txt','../one.txt']:
            out=io.BytesIO()
            with zipfile.ZipFile(out,'w')as z:z.writestr(name,b'explicit synthetic structural member')
            with zipfile.ZipFile(io.BytesIO(out.getvalue()))as z:
                if name=='one.txt':c.check_zip(z,{'one.txt'},100)
                else:
                    with self.assertRaises(c.CollegeQuarantineError):c.check_zip(z,{name},100)
    def test_private_write_checks_truthful_count_and_false_full_count_with_readback(self):
        real=Path.write_bytes;body=b'explicit synthetic complete private metadata'
        for truthful in(True,False):
            with self.subTest(truthful=truthful),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp)
                def short(path,value):
                    count=real(path,value[:5]);return count if truthful else len(value)
                with patch.object(Path,'write_bytes',short),self.assertRaisesRegex(c.CollegeQuarantineError,'incomplete_private_write'):c.write_private(root,'metadata/example.json',body)
                self.assertEqual((root/'metadata/example.json').read_bytes(),body[:5])
    def test_negative_incident_uses_directory_durable_shared_records(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage=c.StagingStorageResource(local_dir=tmp);c.local_only(storage);real=os.fsync;calls=[]
            def sync(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):calls.append(os.readlink('/proc/self/fd/'+str(fd)))
                return real(fd)
            with patch('os.fsync',side_effect=sync):c.record_observed_loss(storage,'vsynthetic','a'*64,'synthetic missing original')
            directory=Path(tmp)/c.incident_directory('vsynthetic');self.assertIn(str(directory),calls);self.assertIn(str(directory.parent),calls)
            value=c.strict_json(c.load_first_construction(storage,c.incident_directory('vsynthetic')+'/record.json','vsynthetic','a'*64));self.assertEqual(value['payload']['kind'],'observed_original_trust_loss')
            with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):c.refuse_observed_loss(storage,'vsynthetic')
    def test_negative_incident_directory_and_body_fsync_faults_leave_blocking_evidence(self):
        for directory_fault in(True,False):
            with self.subTest(directory_fault=directory_fault),tempfile.TemporaryDirectory()as tmp:
                storage=c.StagingStorageResource(local_dir=tmp);c.local_only(storage);real=os.fsync;seen=[]
                def fail(fd):
                    if stat.S_ISDIR(os.fstat(fd).st_mode)==directory_fault:seen.append(fd);raise OSError('synthetic incident fsync failure')
                    return real(fd)
                with patch('os.fsync',side_effect=fail),self.assertRaises((OSError,ValueError)):c.record_observed_loss(storage,'vsynthetic','a'*64,'synthetic missing original')
                self.assertTrue(seen)
                # The first parent fsync can fail before the incident directory
                # exists; later faults leave a permanent partial incident.
                if not directory_fault:
                    with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):c.refuse_observed_loss(storage,'vsynthetic')

    def test_permanent_empty_incident_refuses_cold_retry_without_history_repair(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage=c.StagingStorageResource(local_dir=tmp);c.local_only(storage);directory=Path(tmp)/c.incident_directory('vsynthetic');directory.mkdir(parents=True)
            for _ in range(2):
                with self.assertRaisesRegex(c.CollegeQuarantineError,'permanent_observed_original_trust_loss'):c.refuse_observed_loss(storage,'vsynthetic')
    def test_first_body_and_exact_malformed_receipt_are_durable(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();body=b'explicit synthetic raw body';bad=b'{malformed synthetic receipt';(inputs/'nces_landing.body').write_bytes(body);(inputs/'nces_landing.receipt.json').write_bytes(bad);storage=c.StagingStorageResource(local_dir=str(root/'staging'))
            with self.assertRaises(ValueError):c.acquire(storage,inputs)
            depot=root/'staging/_raw-captures'/c.SLUG/c.digest(body);self.assertEqual((depot/'nces_landing.body').read_bytes(),body);self.assertEqual(next(depot.glob('*.raw-receipt-*.body')).read_bytes(),bad)
    def test_unreadable_first_receipt_keeps_body_only_attempt_diagnostic(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();body=b'explicit synthetic raw body';(inputs/'nces_landing.body').write_bytes(body);p=inputs/'nces_landing.receipt.json';p.write_bytes(b'{}');p.chmod(0)
            try:
                with self.assertRaises(ValueError):c.acquire(c.StagingStorageResource(local_dir=str(root/'staging')),inputs)
            finally:p.chmod(0o600)
            self.assertEqual((root/'staging/_raw-captures'/c.SLUG/c.digest(body)/'nces_landing.body').read_bytes(),body);attempt=json.loads(next((root/'staging/_quarantine'/c.SLUG).glob('attempt-*/attempted-inputs.json')).read_bytes());self.assertFalse(attempt['objects']['nces_landing']['raw_receipt_read'])

    def test_real_HTTPX_consumed_oversize_chunk_retained_before_rejection(self):
        body=b'x'*37;real=httpx.Client
        def response(request):return httpx.Response(200,headers={'Content-Length':'37'},stream=httpx.ByteStream(body))
        with tempfile.TemporaryDirectory()as tmp,patch.object(c,'MAX_AUX_BYTES',36),patch.object(c.httpx,'Client',lambda **kw:real(transport=httpx.MockTransport(response),**kw)),self.assertRaisesRegex(ValueError,'oversized_or_changed_source'):
            try:c.acquire(c.StagingStorageResource(local_dir=tmp))
            finally:
                self.assertEqual((Path(tmp)/'_raw-captures'/c.SLUG/c.digest(body)/'nces_landing.body').read_bytes(),body)
                receipts=[json.loads(p.read_bytes())for p in(Path(tmp)/'_quarantine').rglob('nces_landing.receipt.json')];self.assertEqual(len(receipts),3);self.assertTrue(all(r['bytes']==37 and r['complete_body']is False for r in receipts))
    def test_floor_precedes_next_read_and_retains_consumed_prefix(self):
        real=httpx.Client;seen=[];first=b'x'*65536
        class Stream(httpx.SyncByteStream):
            def __iter__(self):
                seen.append(1);yield first
                seen.append(2);yield b'y'*65536
        def response(request):return httpx.Response(200,stream=Stream())
        free=[SimpleNamespace(free=c.SPACE_FLOOR+1)]*2+[SimpleNamespace(free=c.SPACE_FLOOR-1)]*12
        with tempfile.TemporaryDirectory()as tmp,patch.object(c.shutil,'disk_usage',side_effect=free),patch.object(c.httpx,'Client',lambda **kw:real(transport=httpx.MockTransport(response),**kw)),self.assertRaisesRegex(ValueError,'disk_floor_before_next_read'):
            try:c.acquire(c.StagingStorageResource(local_dir=tmp))
            finally:self.assertEqual(seen,[1]);self.assertEqual((Path(tmp)/'_raw-captures'/c.SLUG/c.digest(first)/'nces_landing.body').read_bytes(),first)
