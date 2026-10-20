"""Small synthetic default mechanics; complete real observations are explicit opt-in.

Fixture authority strings exercise parser/role guards only. They are not real
HTTP, producer delegation, observation values, rights or acceptance evidence.
"""
from pathlib import Path
from unittest import TestCase, mock, skipUnless
from datetime import datetime, timedelta
import copy, gzip, io, json, os, fcntl, sqlite3, tempfile, zipfile

import pyarrow.parquet as pq
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.sources import tsunami_observations as s

FIXTURE = Path(__file__).parent / 'fixtures/tsunami_observations/two_rows.json'
FULL_CAPTURES = os.environ.get('HIFLD_TSUNAMI_OBSERVATIONS_CAPTURE_DIR')

@skipUnless(FULL_CAPTURES, 'Complete 34,495-row audit requires explicit preserved HIFLD_TSUNAMI_OBSERVATIONS_CAPTURE_DIR; no live refetch.')
class CompleteObservationSourceTests(TestCase):
    @classmethod
    def setUpClass(cls):
        capture = Path(FULL_CAPTURES)
        cls.packs = {n: (capture / (n + '.zip')).read_bytes() for n in s.ARCHIVE_NAMES}
        cls.http = s.unpack_captures(cls.packs)
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



class SyntheticCaptureFixture(TestCase):
    """Invented two-row native interface; actual decoder/Git/state/cache remain active."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = StagingStorageResource(local_dir=self.tmp.name, use_local=True)
        fixture = json.loads(FIXTURE.read_text())
        self.assertTrue(fixture['classification'].startswith('EXPLICIT_SYNTHETIC'))
        page = fixture['page']
        page['fields'] = copy.deepcopy(fixture['layer_metadata']['fields'])
        self.page = s.encode(page)
        self.patches = [mock.patch.object(s, 'FEATURE_COUNT', 2),
                        mock.patch.object(s, 'PAGE_SIZE', 2),
                        mock.patch.object(s, 'SOURCE_PAGE_PINS', {'page_000': s.sha(self.page)}),
                        mock.patch.object(s, 'PIN', s.sha(self.page))]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)
        ids = [f['attributes']['ObjectId'] for f in page['features']]
        bodies = {'layer_before': fixture['layer_metadata'], 'layer_after': fixture['layer_metadata'],
                  'count_before': {'count': 2}, 'count_after': {'count': 2},
                  'ids_before': {'objectIdFieldName': 'ObjectId', 'objectIds': ids},
                  'ids_after': {'objectIdFieldName': 'ObjectId', 'objectIds': ids}, 'page_000': page}
        self.original = {name: (s.encode(body), self.receipt(s.encode(body), s.source_urls()[name]))
                         for name, body in bodies.items()}
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as output:
            for name, body in fixture['authority_bodies'].items():
                raw = body.encode() if isinstance(body, str) else s.encode(body)
                receipt = self.receipt(raw, s.AUTHORITY_URLS[name], s.KNOWN_FINAL_URLS.get(name))
                output.writestr(name + '.body', raw)
                output.writestr(name + '.receipt.json', s.encode(receipt))
        self.authority = archive.getvalue()
        self.capture = Path(self.tmp.name) / 'synthetic-captures'
        self.write_fixture(self.capture, False)

    @staticmethod
    def receipt(body, url, final=None):
        return {'url': url, 'final_url': final or url, 'status': 200,
                'bytes': len(body), 'sha256': s.sha(body), 'fetched_at': '2000-01-01T00:00:00+00:00',
                'EXPLICIT_SYNTHETIC': 'No actual HTTP observation or producer approval is claimed.'}
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


class SyntheticObservationBehaviorTests(SyntheticCaptureFixture):
    """Default full behavior tests use only explicitly invented bounded responses."""
    def test_missing_renamed_ids_extra_columns_and_physical_type_loss_rejected(self):
        http=s.unpack_captures({n:(self.capture/(n+'.zip')).read_bytes() for n in s.ARCHIVE_NAMES})
        original=json.loads(http['source_http:page_000'][0])
        def missing(j):del j['features'][0]['attributes']['ID']
        def renamed(j):j['features'][0]['attributes']['OBJECTID']=j['features'][0]['attributes'].pop('ObjectId')
        def extra(j):j['features'][0]['attributes']['invented_id']='1'
        def width(j):j['features'][0]['attributes']['YEAR']=40000
        def integer(j):j['features'][0]['attributes']['ID']=1.0
        def single(j):j['features'][0]['attributes']['RUNUP_HT']='12.3'
        def geom(j):j['features'][0]['geometry']['z']=0
        for mutate in (missing,renamed,extra,width,integer,single,geom):
            with self.subTest(mutation=mutate.__name__):
                j=copy.deepcopy(original);mutate(j);changed=dict(http);changed['source_http:page_000']=(s.encode(j),http['source_http:page_000'][1])
                with self.assertRaises(s.ObservationQuarantineError):s.decode_rows(changed)

    def test_pagination_truncation_id_census_transfer_and_authority_fail_closed(self):
        http=s.unpack_captures({n:(self.capture/(n+'.zip')).read_bytes() for n in s.ARCHIVE_NAMES})
        for name,change in [('source_http:ids_after',lambda j:j['objectIds'].pop()),('source_http:count_after',lambda j:j.update(count=1)),('source_http:page_000',lambda j:j.update(exceededTransferLimit=True))]:
            with self.subTest(name=name):
                j=json.loads(http[name][0]);change(j);changed=dict(http);body=s.encode(j);changed[name]=(body,http[name][1]);pins=dict(s.SOURCE_PAGE_PINS)
                if name.endswith('page_000'):pins['page_000']=s.sha(body)
                with mock.patch.object(s,'SOURCE_PAGE_PINS',pins):
                    with self.assertRaises(s.ObservationQuarantineError):s.validate_complete_bundle(changed)
        changed=dict(http);j=json.loads(changed['authority_http:source_item'][0]);j['owner']='an_unrelated_mirror';changed['authority_http:source_item']=(s.encode(j),changed['authority_http:source_item'][1])
        with self.assertRaisesRegex(s.ObservationQuarantineError,'official_noaa_owner'):s.provenance(changed)

    def test_native_synthetic_behavior_idempotent_with_original_widths(self):
        out=self.stage();root=self.root(out);before=self.snap(root);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)
        table=pq.read_table(root/'candidate/typed-source.parquet');self.assertEqual(str(table.schema.field('YEAR').type),'int16');self.assertEqual(str(table.schema.field('RUNUP_HT').type),'float');self.assertEqual(len(table),2)

        self.assertEqual(table.column('DATE_STRING').to_pylist(),['-1610/??/??','2000/01/02']);self.assertEqual(table.column('AREA').to_pylist(),['0007','0007'])
        self.assertEqual(table.column('ObjectId').to_pylist(),[1001,1002]);self.assertEqual(table.column('ID').to_pylist(),[5001,5002]);self.assertEqual(table.column('TSEVENT_ID').to_pylist(),[9001,9001])
        self.assertEqual(table.column('RUNUP_HT').null_count,1);self.assertEqual(json.loads(self.page)['features'][0]['attributes']['RUNUP_HT'],0.1)
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

    def test_common_validation_lock_covers_full_native_cache_validation(self):
        out=self.stage();root=self.root(out);original=s.reconcile_cached_candidate
        def validate(*args,**kwargs):
            fd=os.open(root.parent/('.'+root.name+'.lock'),os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            finally:os.close(fd)
            return original(*args,**kwargs)
        with mock.patch.object(s,'reconcile_cached_candidate',side_effect=validate):self.stage()

    def test_install_loser_preserves_complete_concurrent_winner(self):
        original=s.commit_complete_candidate;winner={}
        def install_then_lose(storage,prefix,payloads,ready,packs,manifest,identity):
            original(storage,prefix,payloads,ready,packs,manifest,identity)
            root=Path(storage.local_dir)/prefix;winner.update(root=root,before=self.snap(root))
            return original(storage,prefix,payloads,ready,packs,manifest,identity)
        with mock.patch.object(s,'commit_complete_candidate',side_effect=install_then_lose):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'concurrent_or_incomplete_existing_candidate'):self.stage()
        self.assertTrue(winner['root'].exists());self.assertEqual(self.snap(winner['root']),winner['before'])
        out=self.stage();self.assertEqual(self.root(out),winner['root']);self.assertEqual(self.snap(winner['root']),winner['before'])

    def test_first_marker_requires_canonical_integer_schema_and_retains_each_rejection(self):
        out=self.stage();root=self.root(out);original_candidate=self.snap(root)
        key,marker,_=s.trust_keys(out['version']);original=self.storage.read_key(marker)
        decoded=json.loads(original)
        variants=[s.encode({**decoded,'schema_version':True}),
                  s.encode({**decoded,'schema_version':1.0}),original+b'\n',
                  s.encode({**decoded,'extra':'unapproved'}),
                  s.encode({**decoded,'original_bundle_sha256':'0'*64})]
        for bad in variants:
            with self.subTest(marker=bad):
                self.storage.write_key(marker,bad)
                for attempt in range(2):
                    with self.assertRaisesRegex(s.ObservationQuarantineError,'durable_marker_binding_changed'):
                        self.stage()
                    self.assertFalse(root.exists())
                    self.assertEqual(self.storage.read_key(marker),bad)
                    retained=f'_quarantine/{s.SLUG}/construction-markers/{s.sha(bad)}.json'
                    self.assertEqual(self.storage.read_key(retained),bad)
                self.storage.write_key(marker,original)  # Explicit operator restoration only.
                self.assertEqual(self.stage(),out)
                self.assertEqual(self.snap(root),original_candidate)
        self.assertEqual(self.stage(),out)
        self.assertEqual(self.storage.read_key(marker),original)

    def test_interrupted_live_extraction_preserves_all_partial_bytes_and_existing_candidate(self):
        out=self.stage();root=self.root(out);before=self.snap(root);fragment=b'{"incomplete_original_http":'
        response=mock.MagicMock();response.url=next(iter(s.source_urls().values()));response.status_code=200
        response.headers={};response.num_bytes_downloaded=len(fragment)
        def interrupted(*args,**kwargs):
            yield fragment
            raise s.httpx.ReadError('synthetic_interrupted_body')
        response.iter_bytes.side_effect=interrupted;response.__enter__.return_value=response
        client=mock.MagicMock();client.__enter__.return_value=client;client.stream.return_value=response
        with mock.patch.object(s.httpx,'Client',return_value=client):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'bounded_original_http_retry_exhausted'):s.stage_tsunami_observations_candidate(self.storage)
        self.assertEqual(self.snap(root),before);self.assertEqual(client.stream.call_count,3)
        retained=[p for p in (Path(self.tmp.name)/'_quarantine'/s.SLUG).rglob('*') if p.is_file() and p.read_bytes()==fragment]
        self.assertEqual(len(retained),3)
        archives=list((Path(self.tmp.name)/'_quarantine'/s.SLUG/'acquisitions').rglob('*.zip'));self.assertEqual(len(archives),1)
        with zipfile.ZipFile(archives[0]) as z:self.assertIsNone(z.testzip())

    def test_private_failure_does_not_pin_first_construction_and_no_remote_write(self):
        with mock.patch.object(s,'write_canonical_geopackage',side_effect=OSError('synthetic_writer_failure')):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'synthetic_writer_failure'):self.stage()
        self.assertFalse((Path(self.tmp.name)/'_candidate-first-construction').exists())
        for store in (PublishedStorageResource(local_dir=self.tmp.name,use_local=True),StagingStorageResource(bucket='not-real',use_local=False)):
            with self.assertRaisesRegex(s.ObservationQuarantineError,'draft_requires'):s.stage_tsunami_observations_candidate(store,capture_dir=self.capture)


class ObservationRootGuardTests(SyntheticCaptureFixture):
    def snap(self,path):return {str(p.relative_to(path)):s.sha(p.read_bytes()) for p in path.rglob('*') if p.is_file()}

    def test_absolute_and_relative_root_and_ancestor_aliases_reject_before_capture_and_all_writes(self):
        with tempfile.TemporaryDirectory() as d:
            base=Path(d);outside=base/'outside';outside.mkdir();(outside/'child').mkdir();(outside/'prior.body').write_bytes(b'prior output fixture')
            alias=base/'alias';alias.symlink_to(outside,target_is_directory=True);before=self.snap(outside);cwd=Path.cwd()
            try:
                os.chdir(base)
                for selected in [alias,alias/'child',Path('alias'),Path('alias/child')]:
                    storage=StagingStorageResource(local_dir=str(selected),use_local=True)
                    for operation in [lambda:s.require_local(storage),lambda:s.acquire(storage,self.capture),
                        lambda:s.stage_tsunami_observations_candidate(storage,capture_dir=self.capture),
                        lambda:s.quarantine(storage,b'failed input',{},'synthetic guard probe'),
                        lambda:s.immutable(storage,'metadata/probe.json',b'no redirected write')]:
                        with self.subTest(selected=str(selected),operation=operation):
                            with self.assertRaisesRegex(s.ObservationQuarantineError,'linked_local_staging_root_or_ancestor'):operation()
                            self.assertEqual(self.snap(outside),before)
            finally:os.chdir(cwd)

    def test_empty_prefix_and_canonical_object_keys_are_required(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d,use_local=True)
            for key in ['', '.', '../outside', '/outside','valid/../outside','valid//alias','valid/./alias','valid\\alias']:
                with self.subTest(key=key):
                    with self.assertRaisesRegex(s.ObservationQuarantineError,'unsafe_local_candidate_prefix'):s.immutable(storage,key,b'blocked')
            for prefix in ['namespace','/','../escape']:
                storage=StagingStorageResource(local_dir=d,use_local=True,prefix=prefix)
                with self.assertRaisesRegex(s.ObservationQuarantineError,'draft_requires_empty_storage_prefix'):s.acquire(storage,self.capture)
            self.assertEqual(list(Path(d).iterdir()),[])

    def test_real_relative_root_acquisition_and_nested_aliases_preserve_lexical_boundary(self):
        with tempfile.TemporaryDirectory() as d:
            base=Path(d);cwd=Path.cwd()
            try:
                os.chdir(base);storage=StagingStorageResource(local_dir='real',use_local=True)
                packs=s.acquire(storage,self.capture)
                self.assertEqual(set(packs),set(s.ARCHIVE_NAMES));self.assertEqual(s.require_local(storage),base/'real')
                outside=base/'outside';outside.mkdir();(outside/'prior').write_bytes(b'unchanged')
                (base/'real/alias').symlink_to(outside,target_is_directory=True);before=self.snap(outside)
                with self.assertRaisesRegex(s.ObservationQuarantineError,'linked_candidate_path'):s.immutable(storage,'alias/escape',b'blocked')
                self.assertEqual(self.snap(outside),before)
            finally:os.chdir(cwd)
