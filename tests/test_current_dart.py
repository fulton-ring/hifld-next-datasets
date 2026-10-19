"""Real complete NOAA capture and adversarial implementation checks; no acceptance."""
from pathlib import Path
from unittest import TestCase, mock
import copy
import fcntl
import gzip
import json
import os
import sqlite3
import tempfile

import httpx
import pyarrow.parquet as pq
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import current_dart as source

CAPTURES=Path(__file__).parents[1]/'docs/dataset-sources/current-dart-deployments/evidence/captures'

class CurrentDartTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.storage=StagingStorageResource(local_dir=self.tmp.name,use_local=True)
    def stage(self):return source.stage_current_dart_candidate(self.storage,capture_dir=CAPTURES)
    def root(self,result):return Path(self.tmp.name)/source.SLUG/source.SLUG/result['version']
    def snapshot(self,root):return {str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    def test_complete_actual_source_and_idempotent_typed_native_outputs(self):
        first=self.stage();root=self.root(first);before=self.snapshot(root)
        self.assertEqual(first,self.stage());self.assertEqual(before,self.snapshot(root))
        table=pq.read_table(root/'candidate/typed-source.parquet');self.assertEqual(len(table),62)
        self.assertEqual(str(table.schema.field('OBJECTID').type),'int32');self.assertEqual(str(table.schema.field('OID').type),'int32')
        self.assertEqual(table.schema.field('OBJECTID').nullable,False)
        self.assertEqual(len(set(table.column('STATION').to_pylist())),62)
        self.assertEqual((root/f'geoparquet/{source.SLUG}.parquet').read_bytes(),(root/'candidate/typed-source.parquet').read_bytes())
        manifest=json.loads((root/'metadata/source_manifest.json').read_bytes());self.assertFalse(manifest['approved']);self.assertEqual(len(manifest['inputs']),12)
    def test_real_source_missing_or_changed_types_ids_geometry_and_transfer_limit_rejected(self):
        actual=json.loads((CAPTURES/'features.body').read_bytes())
        def remove_id(j):del j['features'][0]['attributes']['OID']
        def duplicate_id(j):j['features'][0]['attributes']['STATION']=j['features'][1]['attributes']['STATION']
        def lost_type(j):j['features'][0]['attributes']['OID']=1.0
        def width(j):j['features'][0]['attributes']['OID']=2**40
        def extra(j):j['features'][0]['attributes']['NEW']=1
        def geom(j):j['features'][0]['geometry']['z']=0
        def incomplete(j):j['features'].pop()
        def transfer(j):j['exceededTransferLimit']=True
        for change in (remove_id,duplicate_id,lost_type,width,extra,geom,incomplete,transfer):
            with self.subTest(change=change.__name__):
                j=copy.deepcopy(actual);change(j);body=source.encode(j)
                with mock.patch.object(source,'PIN',source.sha(body)),mock.patch.object(source,'ARCHIVE_BYTES',len(body)):
                    with self.assertRaises(source.DartQuarantineError):source.inspect_archive(body,Path(self.tmp.name))
    def test_before_after_ids_counts_and_delegation_fail_closed(self):
        captures={n:((CAPTURES/(n+'.body')).read_bytes(),json.loads((CAPTURES/(n+'.receipt.json')).read_bytes())) for n in source.URLS}
        source.validate_complete_bundle(captures);source.provenance(captures)
        for name in ('ids_before','ids_after','count_before','count_after'):
            with self.subTest(name=name):
                altered=copy.deepcopy(captures);j=json.loads(altered[name][0])
                if name.startswith('ids'):j['objectIds'].pop()
                else:j['count']-=1
                altered[name]=(source.encode(j),altered[name][1])
                with self.assertRaises(source.DartQuarantineError):source.validate_complete_bundle(altered)
        altered=copy.deepcopy(captures);j=json.loads(altered['webmap'][0])
        # A responding mirror with another layer URL is not NOAA's exact delegation.
        def change(items):
            for item in items:
                if item.get('id')==source.SLUG:item['url']='https://example.com/mirror'
                change(item.get('layers',[]))
        change(j['operationalLayers']);body=source.encode(j);receipt=copy.deepcopy(altered['webmap'][1]);receipt.update(bytes=len(body),sha256=source.sha(body));altered['webmap']=(body,receipt)
        with self.assertRaisesRegex(source.DartQuarantineError,'delegation_changed'):source.provenance(altered)
    def test_current_noaa_page_entry_and_config_import_chain_required(self):
        captures={n:((CAPTURES/(n+'.body')).read_bytes(),json.loads((CAPTURES/(n+'.receipt.json')).read_bytes())) for n in source.URLS}
        for name,old,new in [('ncei_map',b'index-Cyvq2L9n.js',b'index-NEW.js'),('map_js',b'from"./store-CeovILYQ.js"',b'from"./store-NEW.js"')]:
            with self.subTest(name=name):
                changed=copy.deepcopy(captures);body=changed[name][0].replace(old,new)
                self.assertNotEqual(body,changed[name][0]);receipt=changed[name][1];receipt.update(bytes=len(body),sha256=source.sha(body));changed[name]=(body,receipt)
                with self.assertRaisesRegex(source.DartQuarantineError,'official_current_map_entry_or_store_import_changed'):source.provenance(changed)
    def mutate_cache(self,mutator,reason):
        result=self.stage();root=self.root(result);mutator(root)
        marker=root/'metadata/current_dart_candidate_ready.json';j=json.loads(marker.read_bytes())
        for key in j['objects']:
            if (root/key).exists():j['objects'][key]=source.sha((root/key).read_bytes())
        marker.write_bytes(source.encode(j));before=self.snapshot(root)
        with self.assertRaisesRegex(source.DartQuarantineError,reason):self.stage()
        self.assertFalse(root.exists())
        matches=[p for p in (Path(self.tmp.name)/'_quarantine'/source.SLUG).glob('invalid-cache-*/candidate') if self.snapshot(p)==before]
        self.assertEqual(len(matches),1);self.assertEqual(self.stage(),result)
    def test_self_repinned_whole_native_geopackage_metadata_quarantined(self):
        def mutate(root):
            connection=sqlite3.connect(root/'geopackage'/f'{source.SLUG}.gpkg')
            try:connection.execute('UPDATE gpkg_contents SET srs_id=4326');connection.commit()
            finally:connection.close()
        self.mutate_cache(mutate,'canonical_container_differs')
    def test_self_repinned_source_evidence_quarantined(self):
        def mutate(root):(root/'metadata/source-fidelity/baseline_comparison.md').write_text('forged PASS')
        self.mutate_cache(mutate,'source_fidelity_evidence_differs')
    def test_self_repinned_construction_commit_quarantined(self):
        def mutate(root):
            p=root/'metadata/source_manifest.json';j=json.loads(p.read_bytes());j['code_commit']='a'*40;p.write_bytes(source.encode(j))
        self.mutate_cache(mutate,'original_construction_manifest_changed')
    def test_missing_marker_detaches_whole_version_without_reconstruction(self):
        result=self.stage();root=self.root(result);(root/'metadata/current_dart_candidate_ready.json').unlink()
        with mock.patch.object(source,'write_canonical_geopackage',side_effect=AssertionError('must not reconstruct incomplete cache')):
            with self.assertRaisesRegex(source.DartQuarantineError,'missing_candidate_ready'):self.stage()
        self.assertFalse(root.exists());self.assertEqual(self.stage(),result)
    def test_cache_validation_holds_common_kernel_lock(self):
        result=self.stage();root=self.root(result);original=source.reconcile_cached_candidate;observed=[]
        def check(*args,**kwargs):
            descriptor=os.open(root.parent/('.'+root.name+'.lock'),os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                observed.append(True)
            finally:os.close(descriptor)
            return original(*args,**kwargs)
        with mock.patch.object(source,'reconcile_cached_candidate',side_effect=check):self.stage()
        self.assertEqual(observed,[True])
    def test_losing_install_preserves_concurrent_complete_winner(self):
        original=source.commit_complete_candidate;winning=[]
        def install_then_lose(storage,prefix,payloads,ready):
            original(storage,prefix,payloads,ready);winning.append((source.local_candidate_path(storage,prefix),self.snapshot(source.local_candidate_path(storage,prefix))))
            raise source.DartQuarantineError('concurrent_or_incomplete_existing_candidate')
        with mock.patch.object(source,'commit_complete_candidate',side_effect=install_then_lose):
            with self.assertRaisesRegex(source.DartQuarantineError,'concurrent_or_incomplete'):self.stage()
        self.assertEqual(self.snapshot(winning[0][0]),winning[0][1]);self.stage()
    def test_published_storage_or_remote_backend_cannot_run_unapproved_source(self):
        for storage in (PublishedStorageResource(local_dir=self.tmp.name,use_local=True),StagingStorageResource(bucket='not-real',use_local=False)):
            with self.assertRaisesRegex(source.DartQuarantineError,'draft_requires'):source.stage_current_dart_candidate(storage,capture_dir=CAPTURES)
    def test_live_wire_length_uses_encoded_counter_not_decoded_length(self):
        original=httpx.Client
        def handle(request):
            name=next(n for n,u in source.URLS.items() if u==str(request.url));body=(CAPTURES/(name+'.body')).read_bytes();encoded=gzip.compress(body)
            return httpx.Response(200,stream=httpx.ByteStream(encoded),headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},request=request)
        with mock.patch.object(source.httpx,'Client',side_effect=lambda **kwargs:original(transport=httpx.MockTransport(handle),**kwargs)):
            captures=source.acquire(self.storage,None)
        self.assertEqual(len(captures),len(source.URLS));self.assertTrue(all(r['wire_bytes']!=len(body) for body,r in captures.values()))
    def test_partial_or_wrong_wire_length_is_retained_and_bounded(self):
        original=httpx.Client;calls=[]
        def handle(request):
            calls.append(str(request.url));return httpx.Response(200,stream=httpx.ByteStream(b'partial'),headers={'Content-Length':'100'},request=request)
        with mock.patch.object(source.httpx,'Client',side_effect=lambda **kwargs:original(transport=httpx.MockTransport(handle),**kwargs)):
            with self.assertRaisesRegex(source.DartQuarantineError,'bounded_fetch_exhausted'):source.acquire(self.storage,None)
        self.assertEqual(len(calls),3)
        retained=list((Path(self.tmp.name)/'_quarantine'/source.SLUG).rglob('response.body'));self.assertEqual(len(retained),3);self.assertTrue(all(p.read_bytes()==b'partial' for p in retained))
