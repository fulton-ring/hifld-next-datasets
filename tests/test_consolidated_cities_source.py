"""Complete retained original inputs test draft mechanics; no real acceptance."""
import copy
import gzip
import zlib
from contextlib import contextmanager
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
import pyarrow.parquet as pq
from dagster_hifld.sources import consolidated_cities as c
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.assets.census_consolidated_cities import concity_source_integrity_schedule
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS

_CAPTURE_DIRECTORY=os.environ.get('HIFLD_CONCITY_CAPTURE_DIR')
INPUTS=Path(_CAPTURE_DIRECTORY) if _CAPTURE_DIRECTORY else None

def captures():
    return {n:((INPUTS/(n+'.body')).read_bytes(),c.strict_json((INPUTS/(n+'.receipt.json')).read_bytes())) for n in ['census2026_concity',*c.PINS]}

@unittest.skipUnless(_CAPTURE_DIRECTORY and Path(_CAPTURE_DIRECTORY).is_dir(),
                     "set HIFLD_CONCITY_CAPTURE_DIR to an externally restored issue 41 archive")
class ConsolidatedCityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.capture=captures()
        cls.identity=c.processing_identity()
        cls.table,cls.audit=c.inspect_archives({n:INPUTS/(n+'.body') for n in c.PINS},cls.capture)

    def test_live_transport_complete_gzip_uses_wire_length_and_identity_request(self):
        real_client=httpx.Client
        seen=[]
        class WireBody(httpx.SyncByteStream):
            def __init__(self,body):self.body=body
            def __iter__(self):yield self.body
        def respond(request):
            seen.append(request.headers['accept-encoding'])
            name='census2026_concity' if str(request.url)==c.DIRECTORY else request.url.path.rsplit('/',1)[1][:-4]
            body=self.capture[name][0];wire=gzip.compress(body,mtime=0)
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire))},stream=WireBody(wire))
        def client(**kwargs):return real_client(transport=httpx.MockTransport(respond),**kwargs)
        with tempfile.TemporaryDirectory() as d,patch.object(c.httpx,'Client',side_effect=client):
            actual=c.acquire(StagingStorageResource(local_dir=d))
            self.assertEqual(set(actual),set(self.capture));self.assertEqual(seen,['identity']*8)
            for name,(body,receipt) in actual.items():
                self.assertEqual(body,self.capture[name][0]);self.assertEqual(receipt['content_encoding'],'gzip')
                self.assertEqual(int(receipt['content_length']),receipt['wire_bytes_downloaded'])
                self.assertEqual(receipt['bytes'],len(body));self.assertNotEqual(receipt['bytes'],receipt['wire_bytes_downloaded'])
                self.assertTrue(receipt['complete_body'])

    def test_live_transport_truncated_gzip_preserves_every_attempt_and_prior_capture(self):
        real_client=httpx.Client;seen=[]
        partial=self.capture['tl_2026_09_concity'][0][:-9]
        class WireBody(httpx.SyncByteStream):
            def __init__(self,body):self.body=body
            def __iter__(self):yield self.body
        def respond(request):
            name='census2026_concity' if str(request.url)==c.DIRECTORY else request.url.path.rsplit('/',1)[1][:-4]
            seen.append(name)
            body=self.capture[name][0];wire=gzip.compress(body,mtime=0)
            if name=='tl_2026_09_concity':wire=gzip.compress(partial,mtime=0)
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire)+(13 if name=='tl_2026_09_concity' else 0))},stream=WireBody(wire))
        def client(**kwargs):return real_client(transport=httpx.MockTransport(respond),**kwargs)
        with tempfile.TemporaryDirectory() as d,patch.object(c.httpx,'Client',side_effect=client):
            root=Path(d);prior=root/'prior-accepted';prior.write_bytes(b'unchanged')
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):c.acquire(StagingStorageResource(local_dir=d))
            self.assertEqual(seen,['census2026_concity']+['tl_2026_09_concity']*3)
            receipts=list((root/'_quarantine').rglob('tl_2026_09_concity.receipt.json'));self.assertEqual(len(receipts),3)
            for path in receipts:
                receipt=c.strict_json(path.read_bytes());self.assertFalse(receipt['complete_body'])
                self.assertNotEqual(int(receipt['content_length']),receipt['wire_bytes_downloaded'])
                self.assertEqual(path.with_name('tl_2026_09_concity.body').read_bytes(),partial)
            self.assertTrue(any(path.read_bytes()==self.capture['census2026_concity'][0] for path in (root/'_quarantine').rglob('census2026_concity.body')))
            self.assertEqual(prior.read_bytes(),b'unchanged');self.assertFalse((root/c.SLUG).exists())


    def test_live_transport_missing_content_length_and_wire_counter_remain_supported(self):
        class PlainResponse:
            status_code=200;headers={}
            def __init__(self,url,body):self.url=url;self.body=body
            def iter_bytes(self,size):yield self.body
        class PlainClient:
            def __init__(self,**kwargs):pass
            def __enter__(self):return self
            def __exit__(self,*args):pass
            @contextmanager
            def stream(inner,method,url):
                name='census2026_concity' if url==c.DIRECTORY else url.rsplit('/',1)[1][:-4]
                yield PlainResponse(url,self.capture[name][0])
        with tempfile.TemporaryDirectory() as d,patch.object(c.httpx,'Client',PlainClient):
            result=c.acquire(StagingStorageResource(local_dir=d))
            self.assertEqual(len(result),8)
            self.assertTrue(all(r['complete_body'] and r['wire_bytes_downloaded'] is None for _,r in result.values()))


    def test_complete_original_rows_types_identifiers_and_dbf_declarations(self):
        self.assertEqual(self.table.num_rows,8)
        self.assertEqual(sorted(self.table['GEOID'].to_pylist()),list(c.GEOIDS))
        self.assertEqual(self.table['OGC_FID'].to_pylist(),[0,0,1,0,0,0,0,0])
        self.assertEqual(tuple(f.type for f in self.table.schema),c.TYPES)
        self.assertEqual(len(self.audit),7)
        for audit in self.audit.values():self.assertEqual(audit['dbf']['fields'],c.DBF_FIELDS)
        for changed in [self.table.slice(0,7),self.table.drop(['GEOID']),self.table.append_column('extra',pa.array([1]*8)),self.table.set_column(13,'ALAND',pa.array([1.0]*8)),self.table.set_column(5,'GEOID',pa.array(['0000000']*8)),self.table.set_column(17,'wkb_geometry',pa.array([None]*8,type=pa.binary()))]:
            with self.assertRaises(ValueError):c.validate_table(changed)


    def test_strict_json_and_provenance_preflight_precede_canonical_writes(self):
        for body in [b'[]',b'null',b'{"error":{}}',b'{"x":1,"x":2}']:
            with self.assertRaises(ValueError):c.strict_json(body)
        for mutation in [{'observed_at':'not-a-date'},{'observed_at':'2026-10-19T00:00:00+00:99'},{'observed_at':'2026-10-19T00:00:00'},{'observed_at':True},{'url':'https://example.invalid/x'},{'final_url':'https://example.invalid/x'},{'status':True},{'status':403},{'bytes':True},{'sha256':'0'*64},{'complete_body':False}]:
            bad=copy.deepcopy(self.capture);bad['tl_2026_09_concity'][1].update(mutation)
            with tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d);prior=Path(d)/'prior';prior.write_bytes(b'keep')
                with patch.object(c,'acquire',return_value=bad),patch.object(c,'processing_identity',return_value=self.identity),self.assertRaises(ValueError):c.stage_consolidated_cities(storage)
                self.assertFalse((Path(d)/c.SLUG).exists());self.assertEqual(prior.read_bytes(),b'keep')
                self.assertTrue(list((Path(d)/'_quarantine').rglob('reason.json')))

    def test_changed_archive_or_listing_quarantines_all_without_candidate(self):
        for target in ['tl_2026_09_concity','census2026_concity']:
            bad=copy.deepcopy(self.capture);body,receipt=bad[target]
            body=body[:-1] if target!='census2026_concity' else body.replace(b'tl_2026_09_concity.zip',b'tl_2026_99_concity.zip')
            receipt.update(bytes=len(body),sha256=c.digest(body));bad[target]=(body,receipt)
            with tempfile.TemporaryDirectory() as d:
                with patch.object(c,'acquire',return_value=bad),patch.object(c,'processing_identity',return_value=self.identity),self.assertRaises(ValueError):c.stage_consolidated_cities(StagingStorageResource(local_dir=d))
                self.assertFalse((Path(d)/c.SLUG).exists())
                self.assertTrue(any(p.read_bytes()==body for p in (Path(d)/'_quarantine').rglob(target+'.body')))

    def test_atomic_failure_and_keyboard_interrupt_never_expose_partial_candidate(self):
        for failure in [OSError('injected'),KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value=self.identity):
                with patch.object(c.pq,'write_table',side_effect=failure),self.assertRaises(KeyboardInterrupt if isinstance(failure,KeyboardInterrupt) else ValueError):
                    c.stage_consolidated_cities(StagingStorageResource(local_dir=d),capture_dir=INPUTS)
                self.assertFalse((Path(d)/c.SLUG).exists())
                self.assertFalse(list((Path(d)/'_building').rglob('*.gpkg')))
                self.assertEqual(len(list((Path(d)/'_raw-captures').rglob('*.body'))),8)

    def test_idempotency_and_missing_ready_cache_is_moved_intact(self):
        with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value=self.identity):
            storage=StagingStorageResource(local_dir=d);a=c.stage_consolidated_cities(storage,capture_dir=INPUTS);b=c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            self.assertFalse(a['reused']);self.assertTrue(b['reused'])
            root=Path(d)/c.SLUG/c.SLUG/a['version'];(root/c.READY).unlink();before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with self.assertRaisesRegex(ValueError,'invalid_cache_moved_intact'):c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            self.assertFalse(root.exists());q=next((Path(d)/'_quarantine'/c.SLUG).glob('invalid-candidate-*'))
            self.assertEqual(before,{str(p.relative_to(q)):p.read_bytes() for p in q.rglob('*') if p.is_file()})

    def test_repinned_cache_reports_rows_schema_ids_and_geometry_never_authorize_truth(self):
        with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value=self.identity):
            storage=StagingStorageResource(local_dir=d);result=c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            root=Path(d)/c.SLUG/c.SLUG/result['version'];saved=Path(d)/'_saved';shutil.copytree(root,saved)
            cases=['placeholder','audit','id','geometry','parquet_crs','gpkg_srs','gpkg_rtree','unknown','resolved','code_commit']
            for case in cases:
                if root.exists():shutil.rmtree(root)
                shutil.copytree(saved,root)
                if case=='placeholder':(root/'metadata/baseline_comparison.json').write_bytes(c.encode({'schema_version':1,'approved':True,'status':'pass'}))
                elif case=='audit':
                    report=c.strict_json((root/'metadata/concity_source_audit.json').read_bytes());report['feature_count']=1;(root/'metadata/concity_source_audit.json').write_bytes(c.encode(report))
                elif case in ('id','geometry','parquet_crs'):
                    table=pq.read_table(root/'candidate/typed-source.parquet')
                    if case=='id':table=table.set_column(5,'GEOID',pa.array(['9999999']*8))
                    elif case=='geometry':table=table.set_column(17,'wkb_geometry',pa.array([c.shapely.to_wkb(c.shapely.Point(0,0))]*8))
                    else:
                        geo=json.loads(table.schema.metadata[b'geo']);geo['columns']['wkb_geometry']['crs']=c.pyproj.CRS(3857).to_json_dict();table=table.replace_schema_metadata({b'geo':c.encode(geo)})
                    pq.write_table(table,root/'candidate/typed-source.parquet')
                elif case in ('gpkg_srs','gpkg_rtree'):
                    with sqlite3.connect(root/'geopackage'/f'{c.SLUG}.gpkg') as db:
                        if case=='gpkg_srs':db.execute('UPDATE gpkg_contents SET srs_id=4326')
                        else:db.execute(f'UPDATE "rtree_{c.LAYER}_wkb_geometry" SET minx=-999')
                elif case=='unknown':(root/'metadata/unknown.json').write_text('{}')
                elif case=='code_commit':
                    report=c.strict_json((root/'metadata/source_manifest.json').read_bytes());report['code_commit']='0'*40;(root/'metadata/source_manifest.json').write_bytes(c.encode(report))
                else:(root/'metadata/resolved_provenance.json').write_text('{}')
                ready=c.strict_json((root/c.READY).read_bytes())
                ready['objects']={name:c.sha256_file(root/name) for name in ready['objects']};(root/c.READY).write_bytes(c.encode(ready))
                with self.subTest(case=case),self.assertRaisesRegex(ValueError,'invalid_cache_moved_intact'):
                    c.stage_consolidated_cities(storage,capture_dir=INPUTS)
                self.assertFalse(root.exists())
                self.assertEqual(len(list((Path(d)/'_raw-captures').rglob('*.body'))),8)


    def test_actual_git_commit_bundle_forgery_missing_anchor_and_trusted_recovery(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            out=c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            root=Path(d)/c.SLUG/c.SLUG/out['version']
            bundle=next((Path(d)/'_source-provenance'/c.SLUG).glob('*/bundle.json'))
            original_bundle=bundle.read_bytes()
            original_manifest=(root/'metadata/source_manifest.json').read_bytes()
            original_native={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            raw={str(p.relative_to(Path(d))):p.read_bytes() for p in (Path(d)/'_raw-captures').rglob('*') if p.is_file()}
            for case in ('zeros','wrong_reachable','missing','malformed'):
                if case in ('zeros','wrong_reachable'):
                    manifest=json.loads(original_manifest)
                    manifest['code_commit']='0'*40 if case=='zeros' else '6969d05028352eefd20348a738a829034af9882f'
                    (root/'metadata/source_manifest.json').write_bytes(c.encode(manifest))
                    value=json.loads(original_bundle);value['manifest']=manifest;bundle.write_bytes(c.encode(value))
                    ready=c.strict_json((root/c.READY).read_bytes());ready['objects']['metadata/source_manifest.json']=c.sha256_file(root/'metadata/source_manifest.json');(root/c.READY).write_bytes(c.encode(ready))
                elif case=='missing':bundle.unlink()
                else:bundle.write_bytes(b'{}')
                with self.subTest(case=case),self.assertRaisesRegex(ValueError,'invalid_cache_moved_intact'):
                    c.stage_consolidated_cities(storage,capture_dir=INPUTS)
                self.assertFalse(root.exists())
                bundle.write_bytes(original_bundle)
                rebuilt=c.stage_consolidated_cities(storage,capture_dir=INPUTS)
                self.assertFalse(rebuilt['reused'])
                self.assertEqual({str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()},original_native)
            self.assertEqual(raw,{str(p.relative_to(Path(d))):p.read_bytes() for p in (Path(d)/'_raw-captures').rglob('*') if p.is_file()})

    def test_interrupted_build_never_bootstraps_original_binding(self):
        with tempfile.TemporaryDirectory() as d,patch.object(c,'write_native_gpkg',side_effect=OSError('test private build interruption')):
            with self.assertRaisesRegex(ValueError,'private build interruption'):
                c.stage_consolidated_cities(StagingStorageResource(local_dir=d),capture_dir=INPUTS)
            self.assertFalse(list((Path(d)/'_source-provenance').rglob('bundle.json')))
            self.assertFalse(list((Path(d)/c.SLUG).glob('*/*/metadata/concity_candidate_ready.json')))


class ConsolidatedCityDefaultTests(unittest.TestCase):
    """Capture-independent safeguards remain enabled without external evidence."""
    def test_live_transport_stream_error_retains_decoded_partial_bytes_and_wire_count(self):
            real_client=httpx.Client;partial=b'partial Census directory response'
            compressor=zlib.compressobj(wbits=31);wire=compressor.compress(partial)+compressor.flush(zlib.Z_SYNC_FLUSH)
            class BrokenWire(httpx.SyncByteStream):
                def __iter__(self):
                    yield wire
                    raise httpx.RemoteProtocolError('injected premature encoded-body EOF')
            def client(**kwargs):
                return real_client(transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire)+10)},stream=BrokenWire())),**kwargs)
            with tempfile.TemporaryDirectory() as d,patch.object(c.httpx,'Client',side_effect=client):
                root=Path(d)
                with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):c.acquire(StagingStorageResource(local_dir=d))
                receipts=list((root/'_quarantine').rglob('census2026_concity.receipt.json'));self.assertEqual(len(receipts),3)
                for path in receipts:
                    receipt=c.strict_json(path.read_bytes());self.assertFalse(receipt['complete_body'])
                    self.assertEqual(receipt['wire_bytes_downloaded'],len(wire));self.assertEqual(receipt['bytes'],len(partial))
                    self.assertEqual(path.with_name('census2026_concity.body').read_bytes(),partial)
                self.assertFalse((root/c.SLUG).exists())

    def test_refuses_published_storage_or_cloud_before_acquisition(self):
            for storage in [PublishedStorageResource(local_dir='/tmp/never-concity-output'),StagingStorageResource(bucket='never')]:
                with patch.object(c,'acquire') as get,self.assertRaises(ValueError):c.stage_consolidated_cities(storage)
                get.assert_not_called()

    def test_registration_and_stopped_schedule(self):
            self.assertIn((c.SLUG,c.SLUG),SUPPORTED_DATASET_FILE_PAIRS)
            self.assertEqual(concity_source_integrity_schedule.default_status.value,'STOPPED')
if __name__=='__main__':unittest.main()
