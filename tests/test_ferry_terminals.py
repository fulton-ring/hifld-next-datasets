"""Complete real845-row NCFO source plus adversarial draft-pipeline behavior."""
from pathlib import Path
from unittest import TestCase,mock
import unittest
from datetime import datetime,timedelta
import copy,fcntl,gzip,json,os,sqlite3,tempfile

import httpx
import pyarrow.parquet as pq
from dagster import build_schedule_context
from dagster import config as dagster_config
from dagster._core.instance import DagsterInstance
from dagster_hifld.assets.bts_ferry_terminals import bts_ferry_terminals_draft_refresh_schedule
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.catalog import summarize_staged_catalog,write_catalog_metadata
from dagster_hifld.source_manifest import load_resolved_source_manifest,snapshot_source_metadata
from dagster_hifld.sources import ferry_terminals as s

_CAPTURE_DIR_ENV='HIFLD_FERRY_TERMINALS_CAPTURE_DIR'
_CAPTURE_DIR=Path(os.environ[_CAPTURE_DIR_ENV]).resolve() if os.environ.get(_CAPTURE_DIR_ENV) else None
_REPO_ROOT=Path(__file__).resolve().parents[1]
C=_CAPTURE_DIR
def _capture_dir_is_external():
    if C is None or not C.is_dir():return False
    try:C.relative_to(_REPO_ROOT);return False
    except ValueError:return True
@unittest.skipUnless(_capture_dir_is_external(), 'set HIFLD_FERRY_TERMINALS_CAPTURE_DIR to the complete capture directory restored outside Git')
class FerryTerminalsTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.storage=StagingStorageResource(local_dir=self.tmp.name,use_local=True)
    def stage(self,capture=C):return s.stage_ferry_terminals_candidate(self.storage,capture_dir=capture)
    def root(self,out):return Path(self.tmp.name)/s.SLUG/s.SLUG/out['version']
    def snap(self,root):return {str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    def test_complete_real_native_nulls_repeated_string_ids_and_idempotency(self):
        out=self.stage();root=self.root(out);before=self.snap(root);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)
        t=pq.read_table(root/'candidate/typed-source.parquet');self.assertEqual(len(t),845);self.assertEqual(str(t.schema.field('OBJECTID').type),'int32');self.assertEqual(str(t.schema.field('PARKING').type),'int32');self.assertEqual(str(t.schema.field('LATITUDE').type),'double');self.assertFalse(t.schema.field('OBJECTID').nullable)
        self.assertEqual(t.column('PARKING').null_count,7);self.assertEqual(len(set(t.column('TERMINAL_ID').to_pylist())),569);self.assertEqual(len(set(t.column('OPERATOR_ID').to_pylist())),161);self.assertEqual(len(set(t.column('OBJECTID').to_pylist())),845)
        self.assertEqual(sum(x=='' for x in t.column('TERMINAL_OPERATED_BY').to_pylist()),301)
        self.assertEqual((root/f'geoparquet/{s.SLUG}.parquet').read_bytes(),(root/'candidate/typed-source.parquet').read_bytes())
        manifest=json.loads((root/'metadata/source_manifest.json').read_bytes());self.assertFalse(manifest['approved']);self.assertEqual(len(manifest['inputs']),9)
    def test_missing_renamed_ids_type_loss_extra_columns_geometry_and_truncation(self):
        original=json.loads((C/'features.body').read_bytes())
        def missing(j):del j['features'][0]['attributes']['TERMINAL_ID']
        def rename(j):j['features'][0]['attributes']['terminal_id']=j['features'][0]['attributes'].pop('TERMINAL_ID')
        def number(j):j['features'][0]['attributes']['TERMINAL_ID']=228
        def integer(j):j['features'][0]['attributes']['OBJECTID']=1.0
        def duplicate(j):j['features'][0]['attributes']['OBJECTID']=j['features'][1]['attributes']['OBJECTID']
        def extra(j):j['features'][0]['attributes']['NEW']='unexpected'
        def geom(j):j['features'][0]['geometry']['z']=0
        def short(j):j['features'].pop()
        def transfer(j):j['exceededTransferLimit']=True
        for mutate in (missing,rename,number,integer,duplicate,extra,geom,short,transfer):
            with self.subTest(mutate=mutate.__name__):
                j=copy.deepcopy(original);mutate(j);b=s.encode(j)
                with mock.patch.object(s,'PIN',s.sha(b)),mock.patch.object(s,'ARCHIVE_BYTES',len(b)):
                    with self.assertRaises(s.FerryQuarantineError):s.inspect_archive(b,Path(self.tmp.name))
    def test_leading_zero_string_and_all_source_nullable_values_are_not_coerced(self):
        j=json.loads((C/'features.body').read_bytes());j['features'][0]['attributes']['TERMINAL_ID']='0228';b=s.encode(j)
        with mock.patch.object(s,'PIN',s.sha(b)),mock.patch.object(s,'ARCHIVE_BYTES',len(b)):
            rows,audit,_=s.inspect_archive(b,Path(self.tmp.name));table=s.typed_table(rows,audit)
        self.assertEqual(table.column('TERMINAL_ID')[0].as_py(),'0228');self.assertEqual(table.column('PARKING').null_count,7)
    def test_complete_population_and_official_delegation_are_required(self):
        captures={n:((C/(n+'.body')).read_bytes(),json.loads((C/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        s.provenance(captures);s.validate_complete_bundle(captures)
        for n in ('count_before','count_after','ids_before','ids_after'):
            changed=copy.deepcopy(captures);j=json.loads(changed[n][0])
            if n.startswith('ids'):j['objectIds'].pop()
            else:j['count']-=1
            changed[n]=(s.encode(j),changed[n][1])
            with self.assertRaises(s.FerryQuarantineError):s.validate_complete_bundle(changed)
        changed=copy.deepcopy(captures);j=json.loads(changed['item'][0]);j['owner']='an_unrelated_mirror';b=s.encode(j);receipt=changed['item'][1];receipt.update(bytes=len(b),sha256=s.sha(b));changed['item']=(b,receipt)
        with self.assertRaisesRegex(s.FerryQuarantineError,'producer_account'):s.provenance(changed)
    def cache_mutation(self,mutate,reason):
        out=self.stage();root=self.root(out);mutate(root);p=root/'metadata/ferry_terminals_candidate_ready.json';j=json.loads(p.read_bytes())
        for n in j['objects']:
            if (root/n).exists():j['objects'][n]=s.sha((root/n).read_bytes())
        p.write_bytes(s.encode(j));before=self.snap(root)
        with self.assertRaisesRegex(s.FerryQuarantineError,reason):self.stage()
        self.assertFalse(root.exists());self.assertTrue(any(self.snap(p)==before for p in (Path(self.tmp.name)/'_quarantine'/s.SLUG).glob('invalid-cache-*/candidate')));self.assertEqual(self.stage(),out)
    def test_self_repinned_entire_native_container_crs_is_not_authority(self):
        def alter(root):
            with sqlite3.connect(root/'geopackage'/f'{s.SLUG}.gpkg') as db:db.execute('UPDATE gpkg_contents SET srs_id=3857')
        self.cache_mutation(alter,'canonical_container_differs')
    def test_self_repinned_false_report_and_extra_identifier_file_quarantined(self):
        for label in ('report','extra'):
            with self.subTest(label=label):
                if label=='report':self.cache_mutation(lambda root:(root/'metadata/source-fidelity/baseline_comparison.md').write_text('invented PASS'),'source_fidelity_evidence_differs')
                else:self.cache_mutation(lambda root:(root/'candidate/invented-id.json').write_text('{}'),'unrostered')
    def test_cataloged_cache_canonical_metadata_is_recomputed_and_quarantined(self):
        def catalog(root,out):
            snapshot_source_metadata(self.storage,s.SLUG,s.SLUG,out['version'])
            resolved=load_resolved_source_manifest(self.storage,s.SLUG,s.SLUG,out['version'])
            summary=summarize_staged_catalog(self.storage,s.SLUG,s.SLUG,out['version'],s.SLUG,source_metadata=resolved.metadata)
            write_catalog_metadata(self.storage,s.SLUG,s.SLUG,out['version'],summary.quality_manifest,summary.data_dictionary)
        cases=(
            ('quality',lambda root,out:write_catalog_metadata(self.storage,s.SLUG,s.SLUG,out['version'],{'quality_check_passed':True,'feature_count':1},json.loads((root/'metadata/data_dictionary.json').read_bytes())),'cached_catalog_metadata_drift:quality'),
            ('dictionary',lambda root,out:write_catalog_metadata(self.storage,s.SLUG,s.SLUG,out['version'],json.loads((root/'metadata/quality_manifest.json').read_bytes()),{'name':s.SLUG,'columns':[]}),'cached_catalog_metadata_drift:dictionary'),
            ('missing_provenance',lambda root,out:(root/'metadata/resolved_provenance.json').unlink(),'cached_catalog_metadata_drift:provenance'),
            ('tampered_snapshot',lambda root,out:(root/'metadata/source/quality_manifest.json').write_bytes(b'{}'),'cached_catalog_source_snapshot_differs:source_quality'),
        )
        for label,mutate,reason in cases:
            with self.subTest(label):
                out=self.stage();root=self.root(out);catalog(root,out);mutate(root,out);before=self.snap(root)
                with self.assertRaisesRegex(s.FerryQuarantineError,reason):self.stage()
                self.assertFalse(root.exists())
                quarantined=(Path(self.tmp.name)/'_quarantine'/s.SLUG).glob('invalid-cache-*/candidate')
                self.assertTrue(any(self.snap(path)==before for path in quarantined))
                self.assertTrue(self.stage()['version']==out['version'])

    def test_missing_ready_moves_whole_cache_and_restores_original_newer_receipts(self):
        out=self.stage();root=self.root(out);before=self.snap(root);new=Path(self.tmp.name)/'newer';new.mkdir()
        for n in s.URLS:
            body=(C/(n+'.body')).read_bytes();r=json.loads((C/(n+'.receipt.json')).read_bytes());r['fetched_at']=(datetime.fromisoformat(r['fetched_at'])+timedelta(hours=2)).isoformat();(new/(n+'.body')).write_bytes(body);(new/(n+'.receipt.json')).write_bytes(s.encode(r))
        self.assertEqual(self.stage(new),out);self.assertEqual(self.snap(root),before)
        (root/'metadata/ferry_terminals_candidate_ready.json').unlink()
        with self.assertRaisesRegex(s.FerryQuarantineError,'missing_candidate_ready'):self.stage(new)
        self.assertFalse(root.exists());self.assertEqual(self.stage(new),out);self.assertEqual(self.snap(root),before)
    def test_missing_anchor_and_coforged_wrong_source_commit_fail_whole_cache(self):
        out=self.stage();root=self.root(out);anchor=Path(self.tmp.name)/'_candidate-construction'/s.SLUG/(out['version']+'.json');original=anchor.read_bytes();anchor.unlink()
        with self.assertRaisesRegex(s.FerryQuarantineError,'missing_original_construction'):self.stage()
        self.assertFalse(root.exists());self.assertFalse(anchor.exists());anchor.write_bytes(original);self.assertEqual(self.stage(),out)
        bundle=json.loads(original);bundle['manifest']['code_commit']='0'*40;anchor.write_bytes(s.encode(bundle));(root/'metadata/source_manifest.json').write_bytes(s.encode(bundle['manifest']));ready=root/'metadata/ferry_terminals_candidate_ready.json';j=json.loads(ready.read_bytes());j['objects']['metadata/source_manifest.json']=s.sha(s.encode(bundle['manifest']));ready.write_bytes(s.encode(j))
        with self.assertRaisesRegex(s.FerryQuarantineError,'construction_invalid_commit'):self.stage()
        self.assertFalse(root.exists())
    def test_missing_anchor_twice_never_rebases_original_receipts(self):
        out=self.stage();root=self.root(out);before=self.snap(root)
        anchor=Path(self.tmp.name)/'_candidate-construction'/s.SLUG/(out['version']+'.json')
        marker=anchor.with_name(anchor.name+'.first-construction.json')
        original=anchor.read_bytes();original_marker=marker.read_bytes();anchor.unlink()
        with self.assertRaisesRegex(s.FerryQuarantineError,'missing_original_construction_bundle_after_first_construction'):self.stage()
        self.assertFalse(root.exists());new=Path(self.tmp.name)/'explicit-synthetic-later-receipts';new.mkdir()
        for n in s.URLS:
            body=(C/(n+'.body')).read_bytes();receipt=json.loads((C/(n+'.receipt.json')).read_bytes())
            receipt['fetched_at']=(datetime.fromisoformat(receipt['fetched_at'])+timedelta(hours=3)).isoformat()
            (new/(n+'.body')).write_bytes(body);(new/(n+'.receipt.json')).write_bytes(s.encode(receipt))
        for _ in range(2):
            with self.assertRaisesRegex(s.FerryQuarantineError,'missing_original_construction_bundle_after_first_construction'):self.stage(new)
            self.assertFalse(root.exists());self.assertFalse(anchor.exists());self.assertEqual(marker.read_bytes(),original_marker)
        anchor.write_bytes(original)
        self.assertEqual(self.stage(new),out);self.assertEqual(self.snap(root),before)
        self.assertEqual(anchor.read_bytes(),original);self.assertEqual(marker.read_bytes(),original_marker)

    def test_missing_or_malformed_first_marker_requires_exact_trusted_restore(self):
        out=self.stage();root=self.root(out);before=self.snap(root)
        anchor=Path(self.tmp.name)/'_candidate-construction'/s.SLUG/(out['version']+'.json')
        marker=anchor.with_name(anchor.name+'.first-construction.json');original=marker.read_bytes()
        for case in ('missing','malformed'):
            if case=='missing':marker.unlink()
            else:marker.write_bytes(b'{}')
            for _ in range(2):
                with self.assertRaisesRegex(s.FerryQuarantineError,'missing_or_invalid_first_construction_marker'):self.stage()
                self.assertFalse(root.exists())
            marker.write_bytes(original);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)

    def test_first_marker_schema_version_requires_exact_json_integer(self):
        out=self.stage();root=self.root(out);before=self.snap(root)
        anchor=Path(self.tmp.name)/'_candidate-construction'/s.SLUG/(out['version']+'.json')
        marker=anchor.with_name(anchor.name+'.first-construction.json');original=marker.read_bytes()
        for value in (True,1.0):
            changed=json.loads(original);changed['schema_version']=value;marker.write_bytes(s.encode(changed))
            for _ in range(2):
                with self.assertRaisesRegex(s.FerryQuarantineError,'missing_or_invalid_first_construction_marker'):self.stage()
                self.assertFalse(root.exists())
            marker.write_bytes(original);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)

    def test_interrupted_private_native_write_has_no_construction_records(self):
        with mock.patch.object(s,'write_canonical_geopackage',side_effect=OSError('test private native interruption')):
            with self.assertRaisesRegex(s.FerryQuarantineError,'private native interruption'):self.stage()
        self.assertFalse(list((Path(self.tmp.name)/'_candidate-construction').rglob('*.json')))
        self.assertFalse(list((Path(self.tmp.name)/s.SLUG).glob('*/*/metadata/ferry_terminals_candidate_ready.json')))

    def test_partial_first_marker_write_retains_original_and_blocks_new_context(self):
        original_write=s.immutable
        def interrupt(storage,key,body):
            if key.startswith('_candidate-construction/') and key.endswith('.first-construction.json'):
                raise OSError('injected first marker write interruption')
            return original_write(storage,key,body)
        with mock.patch.object(s,'immutable',side_effect=interrupt):
            with self.assertRaisesRegex(s.FerryQuarantineError,'first marker write interruption'):self.stage()
        base=Path(self.tmp.name)/'_candidate-construction'/s.SLUG
        evidence=list(base.glob('*.json.evidence/*.json'));self.assertEqual(len(evidence),1)
        body=evidence[0].read_bytes();bundle=json.loads(body);version=bundle['version']
        anchor=base/(version+'.json');marker=base/(version+'.json.first-construction.json')
        self.assertFalse(anchor.exists());self.assertFalse(marker.exists())
        for _ in range(2):
            with self.assertRaisesRegex(s.FerryQuarantineError,'partial_original_construction_evidence_requires_trusted_restore'):self.stage()
            self.assertEqual(evidence[0].read_bytes(),body);self.assertFalse(anchor.exists());self.assertFalse(marker.exists())
        # Explicit fixture/operator restoration from the preserved exact original.
        marker.write_bytes(s.encode({'schema_version':1,'version':version,
            'processing_fingerprint':bundle['manifest']['processing_fingerprint'],
            'original_bundle_sha256':s.sha(body),
            'original_bundle_evidence_key':str(evidence[0].relative_to(Path(self.tmp.name)))}))
        anchor.write_bytes(body);out=self.stage();self.assertEqual(out['version'],version)
        self.assertEqual((self.root(out)/'metadata/source_manifest.json').read_bytes(),s.encode(bundle['manifest']))

    def test_missing_or_corrupt_referenced_original_evidence_is_not_complete(self):
        out=self.stage();root=self.root(out);before=self.snap(root)
        anchor=Path(self.tmp.name)/'_candidate-construction'/s.SLUG/(out['version']+'.json')
        marker=json.loads(anchor.with_name(anchor.name+'.first-construction.json').read_bytes())
        evidence=Path(self.tmp.name)/marker['original_bundle_evidence_key'];original=evidence.read_bytes()
        for case in ('missing','corrupt'):
            if case=='missing':evidence.unlink()
            else:evidence.write_bytes(b'corrupted original evidence')
            for _ in range(2):
                with self.assertRaisesRegex(s.FerryQuarantineError,'original_construction_evidence_missing_or_changed'):self.stage()
                self.assertFalse(root.exists())
            evidence.write_bytes(original);self.assertEqual(self.stage(),out);self.assertEqual(self.snap(root),before)

    def test_private_install_and_cache_share_common_lock_and_preserve_winner(self):
        out=self.stage();root=self.root(out);original=s.reconcile_cached_candidate;observed=[]
        def validate(*args,**kwargs):
            fd=os.open(root.parent/('.'+root.name+'.lock'),os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                observed.append(True)
            finally:os.close(fd)
            return original(*args,**kwargs)
        with mock.patch.object(s,'reconcile_cached_candidate',side_effect=validate):self.stage()
        self.assertEqual(observed,[True])
    def test_losing_install_never_detaches_a_complete_concurrent_winner(self):
        original=s.commit_complete_candidate;winning=[]
        def lose(storage,prefix,payloads,ready):
            original(storage,prefix,payloads,ready);path=s.local_candidate_path(storage,prefix);winning.append((path,self.snap(path)))
            raise s.FerryQuarantineError('concurrent_or_incomplete_existing_candidate')
        with mock.patch.object(s,'commit_complete_candidate',side_effect=lose):
            with self.assertRaisesRegex(s.FerryQuarantineError,'concurrent_or_incomplete'):self.stage()
        self.assertEqual(self.snap(winning[0][0]),winning[0][1]);self.stage()
    def test_live_compressed_wire_counter_retains_complete_decoded_source(self):
        original=httpx.Client
        def handle(request):
            name=next(n for n,u in s.URLS.items() if u==str(request.url));body=(C/(name+'.body')).read_bytes();wire=gzip.compress(body)
            return httpx.Response(200,stream=httpx.ByteStream(wire),headers={'Content-Encoding':'gzip','Content-Length':str(len(wire))},request=request)
        with mock.patch.object(s.httpx,'Client',side_effect=lambda **kwargs:original(transport=httpx.MockTransport(handle),**kwargs)):got=s.acquire(self.storage,None)
        self.assertEqual(len(got),9);self.assertEqual(got['features'][0],(C/'features.body').read_bytes())


class FerryTerminalsMechanicsTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.storage=StagingStorageResource(local_dir=self.tmp.name,use_local=True)

    def test_partial_download_bounded_retries_preserve_original_partial_bytes(self):
        original=httpx.Client;calls=[]
        def handle(request):
            calls.append(str(request.url));return httpx.Response(200,stream=httpx.ByteStream(b'partial'),headers={'Content-Length':'100'},request=request)
        with mock.patch.object(s.httpx,'Client',side_effect=lambda **kwargs:original(transport=httpx.MockTransport(handle),**kwargs)):
            with self.assertRaisesRegex(s.FerryQuarantineError,'bounded_fetch_exhausted'):s.acquire(self.storage,None)
        self.assertEqual(len(calls),3);bodies=list((Path(self.tmp.name)/'_quarantine'/s.SLUG).rglob('response.body'));self.assertEqual(len(bodies),3);self.assertTrue(all(p.read_bytes()==b'partial' for p in bodies))

    def test_published_and_remote_storage_reject_unapproved_draft(self):
        for storage in (PublishedStorageResource(local_dir=self.tmp.name,use_local=True),StagingStorageResource(bucket='not-real',use_local=False)):
            with self.assertRaisesRegex(s.FerryQuarantineError,'draft_requires'):s.stage_ferry_terminals_candidate(storage,capture_dir=C)

    def test_scheduled_refresh_run_request_config_validates_against_job(self):
        """Asset jobs name nodes by asset key, so a schedule must not emit op config under the asset function name."""
        repo=defs.get_repository_def();job=repo.get_job('bts_ferry_terminals_draft_refresh')
        self.assertEqual([n.name for n in job.graph.nodes],['ingest__ferry_terminals__ferry_terminals'])
        with DagsterInstance.ephemeral() as instance:
            context=build_schedule_context(instance=instance,repository_def=repo,
                resources={'staging_storage':StagingStorageResource(local_dir=self.tmp.name,use_local=True)})
            result=bts_ferry_terminals_draft_refresh_schedule(context)
        requests=result if isinstance(result,list) else [result]
        self.assertTrue(requests)
        schema=job.run_config_schema.config_type
        for request in requests:
            verdict=dagster_config.validate_config(schema,request.run_config or {})
            self.assertTrue(verdict.success,f'{verdict.errors}; run_config={request.run_config}')
