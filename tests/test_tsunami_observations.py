"""Real complete native source audit plus explicitly synthetic bounded behavior fixtures."""
from pathlib import Path
from unittest import TestCase,mock
from datetime import datetime,timedelta
import copy,gzip,io,json,os,fcntl,sqlite3,tempfile,zipfile

import pyarrow.parquet as pq
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import tsunami_observations as s

CAPTURES=Path(__file__).parents[1]/'docs/dataset-sources/historical-tsunami-observations/evidence/captures'
class CompleteObservationSourceTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packs={n:(CAPTURES/(n+'.zip')).read_bytes() for n in s.ARCHIVE_NAMES};cls.http=s.unpack_captures(cls.packs)
    def test_all_34495_records_ids_native_types_and_original_wire_survive(self):
        s.validate_complete_bundle(self.http);manifest=s.provenance(self.http);self.assertEqual(len(manifest['inputs']),82)
        rows,audit,diagnostics=s.decode_rows(self.http);table=s.typed_table(rows,audit)
        self.assertEqual(len(table),34495);self.assertEqual(audit['identifiers']['ID']['unique'],34495);self.assertEqual(audit['identifiers']['ObjectId']['unique'],34495);self.assertEqual(audit['identifiers']['TSEVENT_ID']['unique'],2397)
        for field in s.NATIVE_FIELDS:
            self.assertEqual(str(table.schema.field(field['name']).type),{'int16':'int16','int32':'int32','float32':'float','float64':'double','string':'string'}[s.KIND[field['type']]])
            self.assertEqual(table.schema.field(field['name']).nullable,field['nullable'])
        self.assertEqual(rows[0]['properties']['YEAR'],-1610);self.assertEqual(rows[0]['properties']['DATE_STRING'],'-1610/??/??')
        self.assertEqual(audit['numeric_physical_decode']['changed_wire_representation_count'],42421)
        self.assertEqual(len(gzip.decompress(diagnostics).splitlines()),42421)
    def test_missing_renamed_ids_extra_columns_and_physical_type_loss_rejected(self):
        original=json.loads(self.http['source_http:page_000'][0])
        def missing(j):del j['features'][0]['attributes']['ID']
        def renamed(j):j['features'][0]['attributes']['OBJECTID']=j['features'][0]['attributes'].pop('ObjectId')
        def extra(j):j['features'][0]['attributes']['invented_id']='1'
        def width(j):j['features'][0]['attributes']['YEAR']=40000
        def integer(j):j['features'][0]['attributes']['ID']=1.0
        def single(j):j['features'][0]['attributes']['RUNUP_HT']='12.3'
        def geom(j):j['features'][0]['geometry']['z']=0
        for mutate in (missing,renamed,extra,width,integer,single,geom):
            with self.subTest(mutation=mutate.__name__):
                j=copy.deepcopy(original);mutate(j);changed=dict(self.http);changed['source_http:page_000']=(s.encode(j),self.http['source_http:page_000'][1])
                with self.assertRaises(s.ObservationQuarantineError):s.decode_rows(changed)
    def test_pagination_truncation_id_census_transfer_and_authority_fail_closed(self):
        for name,change in [('source_http:ids_after',lambda j:j['objectIds'].pop()),('source_http:count_after',lambda j:j.update(count=34494)),('source_http:page_068',lambda j:j.update(exceededTransferLimit=True))]:
            with self.subTest(name=name):
                j=json.loads(self.http[name][0]);change(j);changed=dict(self.http);body=s.encode(j);changed[name]=(body,self.http[name][1]);pins=dict(s.SOURCE_PAGE_PINS)
                if name.endswith('page_068'):pins['page_068']=s.sha(body)
                with mock.patch.object(s,'SOURCE_PAGE_PINS',pins):
                    with self.assertRaises(s.ObservationQuarantineError):s.validate_complete_bundle(changed)
        changed=dict(self.http);j=json.loads(changed['authority_http:source_item'][0]);j['owner']='an_unrelated_mirror';changed['authority_http:source_item']=(s.encode(j),changed['authority_http:source_item'][1])
        with self.assertRaisesRegex(s.ObservationQuarantineError,'official_noaa_owner'):s.provenance(changed)

class SyntheticObservationBehaviorTests(TestCase):
    """Two REAL row values in synthetic complete HTTP responses; never source approval."""
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.storage=StagingStorageResource(local_dir=self.tmp.name,use_local=True)
        source=(CAPTURES/'source_http.zip').read_bytes();self.authority=(CAPTURES/'authority_http.zip').read_bytes()
        with zipfile.ZipFile(io.BytesIO(source)) as z:
            self.original={n:(z.read(n+'.body'),json.loads(z.read(n+'.receipt.json'))) for n in ['layer_before','count_before','ids_before','page_000','ids_after','count_after','layer_after']}
        page=json.loads(self.original['page_000'][0]);page['features']=page['features'][:2];page['exceededTransferLimit']=False;self.page=s.encode(page)
        self.patches=[mock.patch.object(s,'FEATURE_COUNT',2),mock.patch.object(s,'PAGE_SIZE',2),mock.patch.object(s,'SOURCE_PAGE_PINS',{'page_000':s.sha(self.page)}),mock.patch.object(s,'PIN',s.sha(self.page))]
        for p in self.patches:p.start();self.addCleanup(p.stop)
        self.capture=Path(self.tmp.name)/'synthetic-captures';self.write_fixture(self.capture,False)
    def write_fixture(self,directory,newer):
        directory.mkdir();(directory/'authority_http.zip').write_bytes(self.authority)
        ids=[f['attributes']['ObjectId'] for f in json.loads(self.page)['features']]
        with zipfile.ZipFile(directory/'source_http.zip','w',compression=zipfile.ZIP_DEFLATED) as z:
            for n,url in s.source_urls().items():
                body,receipt=copy.deepcopy(self.original[n])
                if n=='page_000':body=self.page
                if n.startswith('count'):body=s.encode({'count':2})
                if n.startswith('ids'):body=s.encode({'objectIdFieldName':'ObjectId','objectIds':ids})
                receipt.update(url=url,final_url=url,bytes=len(body),sha256=s.sha(body))
                if newer:receipt['fetched_at']=(datetime.fromisoformat(receipt['fetched_at'])+timedelta(hours=3)).isoformat()
                z.writestr(n+'.body',body);z.writestr(n+'.receipt.json',s.encode(receipt))
    def stage(self,capture=None):return s.stage_tsunami_observations_candidate(self.storage,capture_dir=capture or self.capture)
    def root(self,out):return Path(self.tmp.name)/s.SLUG/s.SLUG/out['version']
    def snap(self,root):return {str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    def test_native_synthetic_behavior_idempotent_with_original_widths(self):
        out=self.stage();root=self.root(out);before=self.snap(root);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)
        table=pq.read_table(root/'candidate/typed-source.parquet');self.assertEqual(str(table.schema.field('YEAR').type),'int16');self.assertEqual(str(table.schema.field('RUNUP_HT').type),'float');self.assertEqual(len(table),2)
    def test_newer_receipts_whole_quarantine_and_original_cold_recovery(self):
        out=self.stage();root=self.root(out);before=self.snap(root);new=Path(self.tmp.name)/'synthetic-newer';self.write_fixture(new,True)
        self.assertEqual(self.stage(new),out);self.assertEqual(self.snap(root),before)
        (root/'metadata/observations_candidate_ready.json').unlink()
        with self.assertRaisesRegex(s.ObservationQuarantineError,'missing_candidate_ready'):self.stage(new)
        self.assertFalse(root.exists());self.assertEqual(self.stage(new),out);self.assertEqual(self.snap(root),before)
    def test_missing_anchor_second_attempt_cannot_bootstrap_after_detach(self):
        out=self.stage();root=self.root(out);key,marker,_=s.trust_keys(out['version']);original=self.storage.read_key(key);first=self.storage.read_key(marker);Path(self.tmp.name,key).unlink()
        with self.assertRaisesRegex(s.ObservationQuarantineError,'missing_original_construction'):self.stage()
        self.assertFalse(root.exists());self.assertFalse(Path(self.tmp.name,key).exists())
        with self.assertRaisesRegex(s.ObservationQuarantineError,'missing_original_construction'):self.stage()
        self.assertFalse(root.exists());self.assertFalse(Path(self.tmp.name,key).exists());self.assertEqual(self.storage.read_key(marker),first)
        self.storage.write_key(key,original);self.assertEqual(self.stage(),out)
    def test_missing_marker_and_both_missing_records_never_recreate_from_cache(self):
        out=self.stage();root=self.root(out);key,marker,_=s.trust_keys(out['version']);original=self.storage.read_key(key);first=self.storage.read_key(marker)
        Path(self.tmp.name,key).unlink();Path(self.tmp.name,marker).unlink()
        with self.assertRaisesRegex(s.ObservationQuarantineError,'missing_original_construction'):self.stage()
        self.assertFalse(root.exists())
        with self.assertRaisesRegex(s.ObservationQuarantineError,'previous_candidate_trust_failure'):self.stage()
        self.assertFalse(Path(self.tmp.name,key).exists());self.assertFalse(Path(self.tmp.name,marker).exists())
        self.storage.write_key(key,original);self.storage.write_key(marker,first);self.assertEqual(self.stage(),out)
    def test_coforged_bundle_marker_and_candidate_cannot_invent_git_identity(self):
        out=self.stage();root=self.root(out);key,marker,_=s.trust_keys(out['version']);bundle=json.loads(self.storage.read_key(key));bundle['manifest']['code_commit']='0'*40;b=s.encode(bundle);self.storage.write_key(key,b);first=json.loads(self.storage.read_key(marker));first['original_bundle_sha256']=s.sha(b);self.storage.write_key(marker,s.encode(first))
        with self.assertRaisesRegex(s.ObservationQuarantineError,'construction_invalid_commit'):self.stage()
        self.assertFalse(root.exists())
    def test_repinned_native_aux_crs_or_unknown_table_quarantined_whole(self):
        out=self.stage();root=self.root(out);gp=root/'geopackage'/f'{s.SLUG}.gpkg'
        with sqlite3.connect(gp) as db:db.execute('CREATE TABLE unexpected_identifiers (invented INTEGER)')
        marker=root/'metadata/observations_candidate_ready.json';j=json.loads(marker.read_bytes());j['objects'][f'geopackage/{s.SLUG}.gpkg']=s.sha(gp.read_bytes());marker.write_bytes(s.encode(j));before=self.snap(root)
        with self.assertRaisesRegex(s.ObservationQuarantineError,'whole_native_geopackage_bytes_changed'):self.stage()
        self.assertFalse(root.exists());self.assertTrue(any(self.snap(q)==before for q in (Path(self.tmp.name)/'_quarantine'/s.SLUG).glob('invalid-cache-*/candidate')))
    def test_common_validation_lock_and_losing_install_preserve_winner(self):
        out=self.stage();root=self.root(out);original=s.reconcile_cached_candidate
        def validate(*args,**kwargs):
            fd=os.open(root.parent/('.'+root.name+'.lock'),os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            finally:os.close(fd)
            return original(*args,**kwargs)
        with mock.patch.object(s,'reconcile_cached_candidate',side_effect=validate):self.stage()
    def test_private_failure_does_not_pin_first_construction_and_no_remote_write(self):
        with mock.patch.object(s,'write_canonical_geopackage',side_effect=OSError('synthetic_writer_failure')):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'synthetic_writer_failure'):self.stage()
        self.assertFalse((Path(self.tmp.name)/'_candidate-first-construction').exists())
        for store in (PublishedStorageResource(local_dir=self.tmp.name,use_local=True),StagingStorageResource(bucket='not-real',use_local=False)):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'draft_requires'):s.stage_tsunami_observations_candidate(store,capture_dir=self.capture)
