"""Complete real Census fixtures test draft mechanics, not dataset acceptance."""
import copy
import fcntl
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from dagster_hifld.sources import elementary_school_districts as f

INPUTS=Path(__file__).parents[1]/'docs/dataset-sources/elementary-school-districts/evidence/inputs'

class ElementaryTransportTests(unittest.TestCase):
    def fetch(self,headers):
        original=httpx.Client;raw=b'complete bounded encoded input';encoded=gzip.compress(raw)
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip',**headers(len(encoded))},stream=httpx.ByteStream(encoded)))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            return f.acquire(f.StagingStorageResource(use_local=True,local_dir=tmp))
    def test_gzip_wire_length_is_not_decoded_length(self):
        caps=self.fetch(lambda n:{'Content-Length':str(n)})
        self.assertEqual(len(caps),26)
        for body,r in caps.values():
            self.assertEqual(body,b'complete bounded encoded input')
            self.assertEqual(int(r['content_length']),r['wire_bytes_downloaded'])
            self.assertNotEqual(r['bytes'],r['wire_bytes_downloaded'])
    def test_incomplete_wire_fails_and_retains_attempt(self):
        with self.assertRaisesRegex(ValueError,'non200_or_incomplete_http_response'):self.fetch(lambda n:{'Content-Length':str(n+1)})
    def test_chunked_complete_input(self):
        self.assertEqual(len(self.fetch(lambda n:{})),26)
    def test_malformed_local_receipt_retains_exact_body_and_receipt(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);source=root/'inputs';source.mkdir();body=b'original attempted body';bad=b'{bad exact receipt\n'
            (source/'census2026_elsd.body').write_bytes(body);(source/'census2026_elsd.receipt.json').write_bytes(bad)
            staging=root/'staging';storage=f.StagingStorageResource(use_local=True,local_dir=str(staging))
            with self.assertRaises(ValueError):f.acquire(storage,source)
            path=next((staging/'_quarantine').rglob('census2026_elsd.body'))
            self.assertEqual(path.read_bytes(),body);r=json.loads(path.with_suffix('.receipt.json').read_bytes())
            self.assertFalse(r['complete_body']);self.assertEqual((staging/r['original_receipt_key']).read_bytes(),bad)
            self.assertEqual((source/'census2026_elsd.receipt.json').read_bytes(),bad)
    def test_live_partial_stream_retains_three_attempts(self):
        class Broken(httpx.SyncByteStream):
            def __iter__(self):
                yield b'partial original bytes'
                raise httpx.ReadError('test interrupted stream')
        original=httpx.Client
        transport=httpx.MockTransport(lambda request:httpx.Response(200,stream=Broken()))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):f.acquire(f.StagingStorageResource(local_dir=tmp))
            bodies=list(Path(tmp).glob('_quarantine/**/census2026_elsd.body'));self.assertEqual(len(bodies),3)
            self.assertTrue(all(p.read_bytes()==b'partial original bytes'for p in bodies))
    def test_published_cloud_prefixed_and_aliased_storage_refused(self):
        for storage in [f.PublishedStorageResource(local_dir='/tmp'),f.StagingStorageResource(bucket='never'),f.StagingStorageResource(local_dir='/tmp',prefix='unexpected')]:
            with self.assertRaises(ValueError):f.local_only(storage)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);(root/'real').mkdir();(root/'alias').symlink_to(root/'real')
            with self.assertRaises(ValueError):f.local_only(f.StagingStorageResource(local_dir=str(root/'alias')))

class ElementaryOriginalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name)
        cls.original=cls.base/'original';storage=f.StagingStorageResource(local_dir=str(cls.original))
        cls.output=f.stage_elementary_districts(storage,capture_dir=INPUTS)
        cls.prefix=f'{f.SLUG}/{f.SLUG}/{cls.output["version"]}'
        cls.caps={n:((INPUTS/(n+'.body')).read_bytes(),f.strict_json((INPUTS/(n+'.receipt.json')).read_bytes()))for n in f.URLS}
        cls.table,cls.audit=f.inspect_archives({n:INPUTS/(n+'.body')for n in f.PINS},cls.caps)
        cls.identity=f.processing_identity()
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.tempdir=tempfile.TemporaryDirectory();self.addCleanup(self.tempdir.cleanup)
        self.path=Path(self.tempdir.name)/'staging';shutil.copytree(self.original,self.path)
        self.storage=f.StagingStorageResource(local_dir=str(self.path));self.root=self.path/self.prefix
        self.prior=self.path/'prior-accepted';self.prior.write_bytes(b'accepted bytes stay unchanged')
    def refresh(self):return f.stage_elementary_districts(self.storage,capture_dir=INPUTS)
    def repin(self,name):
        path=self.root/f.READY;j=json.loads(path.read_bytes());j['objects'][name]=f.sha256_file(self.root/name);path.write_bytes(f.encode(j))
    def record(self):
        return next(p for p in(self.path/'_construction-state'/f.SLUG).glob('*.json')if not p.name.endswith('.first-construction.json'))
    def reject(self,reason):
        with self.assertRaisesRegex(ValueError,reason):self.refresh()
        self.assertFalse(self.root.exists())
        self.assertTrue(list((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*')))
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_complete_2030_records_all_ids_native_types_nulls_geometry_and_reuse(self):
        self.assertEqual(self.table.num_rows,2030);self.assertEqual(len(self.audit),25)
        self.assertEqual(self.table['SDTYP'].null_count,1988)
        self.assertEqual(len(set(self.table['GEOID'].to_pylist())),2030)
        self.assertEqual(tuple(x.type for x in self.table.schema),f.TYPES)
        self.assertEqual(self.table['ELSDLEA'].type,pa.string())
        self.assertEqual(self.table['ALAND'].type,pa.int64())
        f.reconcile_outputs(self.root,self.table);self.assertTrue(self.refresh()['reused'])
    def test_terminal_schema_preserves_original_fields_and_complete_lineage_sidecar(self):
        parquet=pq.read_table(self.root/'geoparquet'/f'{f.SLUG}.parquet')
        _,gpkg=pyogrio.read_arrow(self.root/'geopackage'/f'{f.SLUG}.gpkg',layer=f.LAYER)
        self.assertEqual(tuple(parquet.column_names),f.SOURCE_FIELDS)
        self.assertEqual(tuple(gpkg.column_names),f.SOURCE_FIELDS)
        self.assertNotIn('source_archive',parquet.column_names)
        self.assertEqual(parquet['OGC_FID'].to_pylist(),self.table['OGC_FID'].to_pylist())
        self.assertLess(len(set(parquet['OGC_FID'].to_pylist())),2030)
        lineage=[json.loads(b)for b in(self.root/'metadata/source_record_lineage.jsonl').read_bytes().splitlines()]
        self.assertEqual(len(lineage),2030)
        self.assertEqual(len({r['original_identifiers']['GEOID']for r in lineage}),2030)
        for record,row in zip(lineage,self.table.to_pylist()):
            self.assertEqual(record['source_archive'],row['source_archive']+'.zip')
            self.assertEqual(record['source_member'],row['source_archive']+'.dbf')
            self.assertEqual(record['original_file_local_OGC_FID'],row['OGC_FID'])
            self.assertEqual(record['original_identifiers'],{n:row[n]for n in ['STATEFP','ELSDLEA','GEOID','GEOIDFQ']})
            self.assertEqual(record['original_input_sha256'],f.PINS[row['source_archive']]['sha256'])
        name='metadata/source_record_lineage.jsonl'
        records=lineage.copy();records[0]=dict(records[0]);records[0]['source_archive']='invented.zip'
        (self.root/name).write_bytes(b''.join(f.encode(r)for r in records));self.repin(name);self.reject('source_owned_bytes')

    def test_missing_renamed_retyped_ids_duplicate_coverage_and_unexpected_fields(self):
        for table in [self.table.drop(['GEOID']),self.table.rename_columns(['wrong']+self.table.column_names[1:]),self.table.slice(1),
                      self.table.set_column(4,'GEOID',pa.array(['0000000']*2030)),self.table.append_column('extra',pa.array([1]*2030)),
                      self.table.set_column(13,'ALAND',self.table['ALAND'].cast(pa.float64()))]:
            with self.subTest(columns=table.column_names),self.assertRaises(ValueError):f.validate_table(table)
    def test_complete_attribute_and_geometry_drift_detected(self):
        values=self.table['NAME'].to_pylist();values[0]='changed'
        with self.assertRaisesRegex(ValueError,'attribute_values'):f.validate_table(self.table.set_column(6,'NAME',pa.array(values)))
        geoms=self.table['wkb_geometry'].to_pylist();geoms[0]=geoms[1]
        with self.assertRaisesRegex(ValueError,'attribute_values'):f.validate_table(self.table.set_column(17,'wkb_geometry',pa.array(geoms,type=pa.binary())))
    def test_receipt_url_status_bool_length_and_time_fail_before_candidate(self):
        for change in [{'status':True},{'bytes':True},{'final_url':'https://example.invalid/'},{'complete_body':1},{'observed_at':'bad'}]:
            caps=copy.deepcopy(self.caps);caps['tl_2026_04_elsd'][1].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):f.manifest_for(caps,self.identity)
    def test_changed_truncated_missing_publication_and_directory_roster(self):
        for name in ['tl_2026_04_elsd','census2026_elsd']:
            caps=copy.deepcopy(self.caps);body,receipt=caps[name]
            body=body[:-1]if name!='census2026_elsd'else body.replace(b'tl_2026_04_elsd.zip',b'tl_2026_99_elsd.zip')
            receipt.update(bytes=len(body),sha256=f.digest(body));caps[name]=(body,receipt)
            with self.assertRaises(ValueError):f.inspect_archives({n:INPUTS/(n+'.body')for n in f.PINS},caps)
        caps=dict(self.caps);caps.pop('tl_2026_04_elsd')
        with self.assertRaises(ValueError):f.manifest_for(caps,self.identity)
    def test_physical_dbf_leading_characters_preserved_and_schema_drift_refused(self):
        with zipfile.ZipFile(INPUTS/'tl_2026_04_elsd.body')as z:body=bytearray(z.read('tl_2026_04_elsd.dbf'))
        h=int.from_bytes(body[8:10],'little');cursor=h+1
        for name,kind,width,_ in f.DBF_FIELDS:
            if name=='NAME':body[cursor:cursor+width]=b' leading original name'+b' '*(width-22);break
            cursor+=width
        self.assertEqual(f.decode_dbf_rows(bytes(body))[0]['NAME'],' leading original name')
        body[32+11]=ord('N')
        with self.assertRaisesRegex(ValueError,'physical_declarations'):f.dbf_definition(bytes(body))
    def test_forged_native_container_and_source_reports_cannot_self_repin(self):
        name=f'geopackage/{f.SLUG}.gpkg'
        with sqlite3.connect(self.root/name)as db:db.execute('UPDATE gpkg_contents SET srs_id=3857')
        self.repin(name);self.reject('full_container')
    def test_false_approval_placeholder_is_quarantined(self):
        name='metadata/baseline_comparison.json';j=json.loads((self.root/name).read_bytes());j['approved']=True
        (self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('source_owned_bytes')
    def test_missing_ready_preserves_entire_invalid_candidate(self):
        (self.root/f.READY).unlink();before={str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()}
        self.reject('invalid_cache_moved_intact')
        q=next((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))
        self.assertEqual(before,{str(p.relative_to(q)):f.sha256_file(p)for p in q.rglob('*')if p.is_file()})
    def test_missing_primary_three_retries_cannot_rebase_trusted_restore_recovers(self):
        record=self.record();original=record.read_bytes();marker=Path(str(record)+'.first-construction.json')
        saved=marker.read_bytes();evidence=self.path/json.loads(saved)['original_bundle_evidence_key'];saved_evidence=evidence.read_bytes()
        record.unlink();self.reject('construction_missing_original_state')
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.refresh()
            self.assertFalse(self.root.exists());self.assertFalse(record.exists())
            self.assertEqual(marker.read_bytes(),saved);self.assertEqual(evidence.read_bytes(),saved_evidence)
        record.write_bytes(original);self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
    def test_observed_whole_history_loss_stays_refused_until_exact_trusted_restore(self):
        folder=self.record().parent
        saved={p.relative_to(folder):p.read_bytes()for p in folder.rglob('*')if p.is_file()}
        original_manifest=(self.root/'metadata/source_manifest.json').read_bytes()
        raw={p.relative_to(self.path):f.sha256_file(p)for p in(self.path/'_raw-captures').rglob('*')if p.is_file()}
        shutil.rmtree(folder);self.reject('construction_missing_original_state')
        incident=self.path/f'_construction-incidents/{f.SLUG}/{self.output["version"]}/incident.json'
        body=incident.read_bytes();self.assertEqual(json.loads(body)['observed_cached_manifest_sha256'],f.digest(original_manifest))
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.refresh()
            self.assertFalse(self.root.exists());self.assertFalse(folder.exists());self.assertEqual(incident.read_bytes(),body)
        for name,value in saved.items():path=folder/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(value)
        self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
        self.assertEqual((self.root/'metadata/source_manifest.json').read_bytes(),original_manifest)
        self.assertEqual(raw,{p.relative_to(self.path):f.sha256_file(p)for p in(self.path/'_raw-captures').rglob('*')if p.is_file()})
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_partial_incident_write_retains_untrusted_candidate_and_refuses_retry(self):
        shutil.rmtree(self.record().parent)
        before={str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()}
        original=f.immutable
        def fail_incident(storage,key,body):
            if key.startswith('_construction-incidents/'):raise OSError('synthetic_incident_write_failure')
            return original(storage,key,body)
        with patch.object(f,'immutable',side_effect=fail_incident):
            with self.assertRaisesRegex(ValueError,'trust_loss_journal_failed_candidate_retained_untrusted'):self.refresh()
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):self.refresh()
            self.assertTrue(self.root.exists())
            self.assertEqual(before,{str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()})
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_relative_root_observed_incident_preserves_exact_restored_context(self):
        import os
        previous=Path.cwd()
        try:
            os.chdir(self.path.parent)
            self.storage=f.StagingStorageResource(local_dir=self.path.name)
            self.test_observed_whole_history_loss_stays_refused_until_exact_trusted_restore()
        finally:os.chdir(previous)
    def test_incident_canonical_schema_types_and_claim_are_strict(self):
        fingerprint=f.digest(f.encode(self.identity))
        f.observed_trust_loss(self.storage,self.output['version'],fingerprint,candidate=self.root)
        path=self.path/f'_construction-incidents/{f.SLUG}/{self.output["version"]}/incident.json';original=path.read_bytes()
        for key,value in [('schema_version',True),('schema_version',1.0),('approved',0),('observed_cached_manifest_sha256','wrong')]:
            bad=json.loads(original);bad[key]=value;path.write_bytes(f.encode(bad))
            with self.assertRaises(ValueError):f.observed_trust_loss(self.storage,self.output['version'],fingerprint)
        path.write_bytes(original)
        with self.assertRaisesRegex(ValueError,'restored_original_manifest_differs'):f.observed_trust_loss(self.storage,self.output['version'],fingerprint,restored_manifest={'synthetic':'different'})
    def test_marker_bool_or_float_version_fails_without_self_repair(self):
        marker=Path(str(self.record())+'.first-construction.json');original=marker.read_bytes()
        for value in [True,1.0]:
            with self.subTest(schema_version=value):
                j=json.loads(original);j['schema_version']=value;marker.write_bytes(f.encode(j));self.reject('construction')
                with self.assertRaisesRegex(ValueError,'construction'):self.refresh()
                marker.write_bytes(original);self.assertFalse(self.refresh()['reused'])
    def test_original_and_candidate_coforge_cannot_change_construction_claim(self):
        record=self.record();j=json.loads(record.read_bytes());j['payload']['manifest']['code_commit']='0'*40
        record.write_bytes(f.encode(j));name='metadata/source_manifest.json';j=json.loads((self.root/name).read_bytes());j['code_commit']='0'*40
        (self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('construction')
    def test_common_lock_covers_full_cache_verification(self):
        original=f.reconcile_cache
        def check(*a,**kw):
            with(self.path/'_locks'/(f.SLUG+'.lock')).open('a+b')as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*a,**kw)
        with patch.object(f,'reconcile_cache',side_effect=check):self.assertTrue(self.refresh()['reused'])
    def test_failed_private_output_leaves_no_candidate_or_trust_state(self):
        with tempfile.TemporaryDirectory()as tmp,patch.object(f,'reconcile_outputs',side_effect=ValueError('test_private_failure')):
            with self.assertRaisesRegex(ValueError,'test_private_failure'):f.stage_elementary_districts(f.StagingStorageResource(local_dir=tmp),capture_dir=INPUTS)
            self.assertFalse([p for p in Path(tmp).glob('_construction-state/**/*')if p.is_file()])
            self.assertFalse(list(Path(tmp).glob(f'{f.SLUG}/{f.SLUG}/v*')))
            self.assertEqual(len(list(Path(tmp).glob('_raw-captures/**/*.body'))),26)
    def test_invalid_first_code_and_manifest_claims_create_no_trust_records(self):
        manifest=f.manifest_for(self.caps,self.identity)
        for key,value in [('code_commit','0'*40),('title','forged original title')]:
            bad=copy.deepcopy(manifest);bad[key]=value
            with tempfile.TemporaryDirectory()as tmp:
                storage=f.StagingStorageResource(local_dir=tmp)
                with self.assertRaisesRegex(ValueError,'construction_invalid_commit|preseal_original_manifest_semantic_drift'):
                    f.original_bundle(storage,'_construction-state/test/first.json','vtest',self.identity,create=(self.caps,bad))
                self.assertFalse([p for p in Path(tmp).glob('_construction-state/**/*')if p.is_file()])

if __name__=='__main__':unittest.main()
