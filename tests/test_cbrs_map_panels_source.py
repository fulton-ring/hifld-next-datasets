"""Real complete captures establish draft fidelity; synthetic negatives grant no approval."""
import copy
import gzip
import os

import httpx
import json
import sqlite3
import struct
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.assets.fws_map_panels import fws_cbrs_map_panels, fws_cbrs_map_panels_draft_refresh_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import cbrs_map_panels as s

_CAPTURE_ENV = 'HIFLD_CBRS_MAP_PANELS_TEST_CAPTURE_DIR'
CAPTURE=Path(os.environ[_CAPTURE_ENV]) if os.environ.get(_CAPTURE_ENV) else None
def captures(): return {n:((CAPTURE/(n+'.body')).read_bytes(),json.loads((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}

@unittest.skipUnless(os.environ.get(_CAPTURE_ENV), 'complete original CBRS captures opt-in required; see docs/dataset-sources/cbrs-map-panels-1/README.md')
class PanelsTests(unittest.TestCase):
    def test_refresh_reuses_candidate_after_registered_catalog_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=StagingStorageResource(local_dir=tmp)
            result=s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            version=result['version']
            snapshot_source_metadata(store,s.SLUG,s.SLUG,version)
            resolved=load_resolved_source_manifest(store,s.SLUG,s.SLUG,version)
            summary=summarize_staged_catalog(store,s.SLUG,s.SLUG,version,s.SLUG,
                                             source_metadata=resolved.metadata)
            write_catalog_metadata(store,s.SLUG,s.SLUG,version,
                                   summary.quality_manifest,summary.data_dictionary)
            root=Path(tmp,s.SLUG,s.SLUG,version)
            for relative in ('metadata/quality_manifest.json','metadata/data_dictionary.json',
                             'metadata/resolved_provenance.json','metadata/source/source_manifest.json'):
                self.assertTrue((root/relative).is_file(),relative)
            digest=s.sha((root/'metadata/source_manifest.json').read_bytes())
            self.assertTrue((root/'metadata/source'/f'version_{digest}_source_manifest.json').is_file())
            self.assertEqual(s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE),result)

    def test_complete_original(self):
        rows,audit,_=s.inspect_source(captures())
        self.assertEqual(len(rows),574);self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(len({r['properties']['OBJECTID'] for r in rows}),574)
        self.assertEqual(len(s.provenance(captures(), s.processing_identity())['inputs']),8)
        self.assertEqual(s.typed_table(rows,audit).schema.field('Map_Date').type.bit_width,64)


    def test_receipt_and_provenance_preflight(self):
        for name in s.URLS:
            for field,value in [('requested_url','https://example.com'),('final_url','https://example.com'),('status',True),('byte_length',True),('sha256','0'*64),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19')]:
                bundle=captures();bundle[name][1][field]=value
                with self.subTest(name=name,field=field),tempfile.TemporaryDirectory() as tmp,patch.object(s,'acquire',return_value=bundle),self.assertRaises(ValueError):
                    try:s.stage_cbrs_map_panels_candidate(StagingStorageResource(local_dir=tmp),capture_dir=CAPTURE)
                    finally:self.assertFalse(Path(tmp,s.SLUG).exists())
        with tempfile.TemporaryDirectory() as tmp,patch.object(s,'validate_release_provenance',side_effect=ValueError('injected malformed provenance')),self.assertRaises(ValueError):
            try:s.stage_cbrs_map_panels_candidate(StagingStorageResource(local_dir=tmp),capture_dir=CAPTURE)
            finally:self.assertFalse(Path(tmp,s.SLUG).exists())

    def test_count_ids_partial_fields_types_and_geometry_drift(self):
        for mode in ['count','ids','partial','fields','type','id','geometry','column','crs']:
            bundle=captures();page=json.loads(bundle['page0000'][0]);name='page0000'
            if mode=='count':name='count_after';obj={'count':573}
            elif mode=='ids':name='ids_after';obj=json.loads(bundle[name][0]);obj['objectIds'].pop()
            else:
                obj=page
                if mode=='partial':obj['exceededTransferLimit']=True
                if mode=='fields':obj['fields'][0]['name']='newOID'
                if mode=='type':obj['features'][0]['attributes']['Map_Date']='2026-01-01'
                if mode=='id':obj['features'][0]['attributes']['OBJECTID']=obj['features'][1]['attributes']['OBJECTID']
                if mode=='geometry':obj['features'][0]['geometry']['rings']=[]
                if mode=='column':obj['features'][0]['attributes']['surprise']=1
                if mode=='crs':obj['spatialReference']['wkid']=3857
            body=s.encode(obj);bundle[name]=(body,bundle[name][1])
            with self.subTest(mode=mode),patch.object(s,'PIN',s.sha(bundle['page0000'][0])),self.assertRaises(ValueError):s.inspect_source(bundle)

    def test_atomic_interrupt_never_discoverable_and_prior_preserved(self):
        for failure in [OSError('disk'),KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as tmp,patch.object(s.os,'rename',side_effect=failure),self.assertRaises(type(failure)):
                try:s.commit_complete_candidate(StagingStorageResource(local_dir=tmp),'a/a/v0.0.0-fixture',{'geopackage/a.gpkg':b'candidate'},{})
                finally:self.assertFalse(Path(tmp,'a/a/v0.0.0-fixture').exists())
        with tempfile.TemporaryDirectory() as tmp:
            store=StagingStorageResource(local_dir=tmp);s.commit_complete_candidate(store,'a/a/v0.0.0-fixture',{'geopackage/a.gpkg':b'prior'},{})
            with self.assertRaises(ValueError):s.commit_complete_candidate(store,'a/a/v0.0.0-fixture',{'geopackage/a.gpkg':b'new'},{})
            self.assertEqual(Path(tmp,'a/a/v0.0.0-fixture/geopackage/a.gpkg').read_bytes(),b'prior')

    def test_fresh_original_cache_and_metadata_self_repin_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=StagingStorageResource(local_dir=tmp);result=s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            root=Path(tmp,s.SLUG,s.SLUG,result['version']);readyfile=root/'metadata/cbrs_map_panels_candidate_ready.json';ready=readyfile.read_bytes()
            self.assertEqual(result,s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE))
            gp=root/'geopackage'/f'{s.SLUG}.gpkg';original=gp.read_bytes();key=f'geopackage/{s.SLUG}.gpkg'
            pristine=Path(tmp,'pristine');shutil.copytree(root,pristine)
            queries=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_geometry_columns SET srs_id=4326", "UPDATE gpkg_contents SET min_x=0", "UPDATE gpkg_spatial_ref_sys SET definition='WRONG' WHERE srs_id=4269", "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=3857 WHERE srs_id=4269", 'UPDATE "'+s.SLUG+'" SET Map_Date=1 WHERE OBJECTID=21']
            for query in queries:
                if not root.exists():shutil.copytree(pristine,root)
                gp.write_bytes(original)
                with sqlite3.connect(gp) as db:db.execute(query)
                inventory=json.loads(ready);inventory['objects'][key]=s.sha(gp.read_bytes());readyfile.write_bytes(s.encode(inventory))
                with self.subTest(query=query),self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
                self.assertFalse(root.exists())
                retained=list(Path(tmp,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate/geopackage/*.gpkg'))
                self.assertTrue(retained)
            shutil.copytree(pristine,root)
            gp.write_bytes(original)
            with sqlite3.connect(gp) as db:
                column=db.execute('SELECT column_name FROM gpkg_geometry_columns').fetchone()[0]
                blob=bytearray(db.execute('SELECT '+column+' FROM "'+s.SLUG+'" WHERE OBJECTID=21').fetchone()[0])
                struct.pack_into('<i' if blob[3]&1 else '>i',blob,4,3857)
                db.execute('UPDATE "'+s.SLUG+'" SET '+column+'=? WHERE OBJECTID=21',(bytes(blob),))
            inventory=json.loads(ready);inventory['objects'][key]=s.sha(gp.read_bytes());readyfile.write_bytes(s.encode(inventory))
            with self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            self.assertFalse(root.exists());shutil.copytree(pristine,root)
            sidecar=root/'metadata/source-fidelity/baseline_differences.jsonl';sidecar.write_bytes(b'forged accepted report')
            inventory=json.loads(ready);inventory['objects']['metadata/source-fidelity/baseline_differences.jsonl']=s.sha(sidecar.read_bytes());readyfile.write_bytes(s.encode(inventory))
            with self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            self.assertFalse(root.exists());shutil.copytree(pristine,root)
            gp.write_bytes(original);readyfile.write_bytes(ready)
            pqfile=root/'candidate/typed-source.parquet';originalpq=pqfile.read_bytes();table=pq.read_table(pqfile);meta=dict(table.schema.metadata);geo=json.loads(meta[b'geo']);geo['columns']['geometry']['crs']={'type':'GeographicCRS','name':'WRONG'};meta[b'geo']=s.encode(geo);pq.write_table(table.replace_schema_metadata(meta),pqfile)
            inventory=json.loads(ready);inventory['objects']['candidate/typed-source.parquet']=s.sha(pqfile.read_bytes());readyfile.write_bytes(s.encode(inventory))
            with self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            self.assertFalse(root.exists());shutil.copytree(pristine,root)
            pqfile.write_bytes(originalpq);readyfile.write_bytes(ready)
            self.assertEqual(result,s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE))

    def test_complete_evidence_replay_and_missing_marker_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=StagingStorageResource(local_dir=tmp);result=s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            root=Path(tmp,s.SLUG,s.SLUG,result['version']);pristine=Path(tmp,'pristine');shutil.copytree(root,pristine)
            readyfile=root/'metadata/cbrs_map_panels_candidate_ready.json'
            modes=['missing_ready','population','blocked_baseline','markdown','gp_wrapper','pq_wrapper','field_sidecar']
            for mode in modes:
                if not root.exists():shutil.copytree(pristine,root)
                ready=json.loads(readyfile.read_bytes())
                if mode=='missing_ready':readyfile.unlink()
                else:
                    relative={'population':'metadata/source-fidelity/baseline_comparison.json','blocked_baseline':'metadata/baseline_comparison.json','markdown':'metadata/source-fidelity/baseline_comparison.md','gp_wrapper':'metadata/geopackage_source_fidelity.json','pq_wrapper':'metadata/parquet_source_fidelity.json','field_sidecar':'metadata/source-fidelity/baseline_field_changes.jsonl'}[mode]
                    path=root/relative
                    if mode in ['markdown','field_sidecar']:path.write_bytes(b'forged accepted population')
                    else:
                        doc=json.loads(path.read_bytes())
                        if mode=='population':
                            doc['populations']['baseline']['rows']=1;doc['populations']['candidate']['rows']=1;doc['retained_rows_compared']=1;doc['baseline']['approved']=True
                        elif mode=='blocked_baseline':doc.update(status='pass',approved=True,acceptance='approved')
                        elif mode=='gp_wrapper':doc['shared_verifier']['baseline']['approved']=True
                        else:doc['shared_verifier']['rows']=1
                        path.write_bytes(s.encode(doc))
                    ready['objects'][relative]=s.sha(path.read_bytes());readyfile.write_bytes(s.encode(ready))
                before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
                old=set(Path(tmp,'_quarantine',s.SLUG).glob('invalid-cache-*'))
                with self.subTest(mode=mode),self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
                self.assertFalse(root.exists())
                new=set(Path(tmp,'_quarantine',s.SLUG).glob('invalid-cache-*'))-old;self.assertEqual(len(new),1)
                retained=new.pop()/'candidate'
                self.assertEqual(before,{str(p.relative_to(retained)):s.sha(p.read_bytes()) for p in retained.rglob('*') if p.is_file()})
            shutil.copytree(pristine,root)
            self.assertEqual(result,s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE))

    def test_concurrent_install_winner_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);install=s.commit_complete_candidate
            def competing(store,prefix,payloads,ready):
                install(store,prefix,payloads,ready)
                raise s.CBRSQuarantineError('simulated_concurrent_install_winner')
            with patch.object(s,'commit_complete_candidate',side_effect=competing),self.assertRaisesRegex(ValueError,'simulated_concurrent_install_winner'):
                s.stage_cbrs_map_panels_candidate(storage,capture_dir=CAPTURE)
            versions=list(Path(tmp,s.SLUG,s.SLUG).glob('v*'));self.assertEqual(len(versions),1)
            before={str(p.relative_to(versions[0])):s.sha(p.read_bytes()) for p in versions[0].rglob('*') if p.is_file()}
            result=s.stage_cbrs_map_panels_candidate(storage,capture_dir=CAPTURE)
            self.assertEqual(result['feature_count'],574)
            self.assertEqual(before,{str(p.relative_to(versions[0])):s.sha(p.read_bytes()) for p in versions[0].rglob('*') if p.is_file()})
            self.assertFalse(list(Path(tmp,'_quarantine',s.SLUG).glob('invalid-cache-*')))

    def test_bad_receipt_original_preserved_no_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw=Path(tmp,'capture');raw.mkdir();body=(CAPTURE/'fws_data.body').read_bytes();bad=b'{bad receipt'
            (raw/'fws_data.body').write_bytes(body);(raw/'fws_data.receipt.json').write_bytes(bad);stage=Path(tmp,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),raw)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())


class PanelsMechanicsTests(unittest.TestCase):
    def test_registry_schedule_and_remote_rejected(self):
        self.assertIn(fws_cbrs_map_panels,hifld_ingest_assets)
        self.assertEqual(fws_cbrs_map_panels_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        for storage in [PublishedStorageResource(local_dir='/tmp/no-write'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_live_partial_attempts_retained(self):
        import httpx
        class Response:
            status_code=200;url=s.URLS['fws_data'];headers={};num_bytes_downloaded=7
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def iter_bytes(self,**kwargs):yield b'partial';raise httpx.ReadError('reset')
        class Client:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def stream(self,*args):return Response()
        with tempfile.TemporaryDirectory() as tmp,patch.object(s.httpx,'Client',return_value=Client()):
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(StagingStorageResource(local_dir=tmp),None)
            self.assertEqual(len(list(Path(tmp).rglob('response.body'))),3)
            self.assertFalse(Path(tmp,s.SLUG).exists())

    def test_compressed_transport_wire_count_complete_truncated_partial(self):
        # Use real HTTPX decoding/accounting even though the deterministic server
        # intentionally ignores Accept-Encoding: identity.
        OriginalClient=httpx.Client
        decoded=bytes(range(256))*800;wire=gzip.compress(decoded,mtime=0)
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):
                yield wire[:len(wire)*3//4]
                raise httpx.ReadError('reset after compressed prefix')
        for mode in ['complete','truncated','partial']:
            requests=[]
            def serve(request):
                requests.append(request)
                self.assertEqual(request.headers['accept-encoding'],'identity')
                stream=Interrupted() if mode=='partial' else httpx.ByteStream(wire if mode=='complete' else wire[:-6])
                return httpx.Response(200,headers={'content-encoding':'gzip','content-length':str(len(wire))},stream=stream)
            def client(**kwargs):return OriginalClient(transport=httpx.MockTransport(serve),**kwargs)
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp,patch.object(s,'URLS',{'probe':s.LANDING}),patch.object(s.httpx,'Client',side_effect=client):
                store=StagingStorageResource(local_dir=tmp)
                if mode=='complete':
                    result=s.acquire(store,None);body,receipt=result['probe']
                    self.assertEqual(body,decoded);self.assertEqual(receipt['byte_length'],len(decoded));self.assertEqual(receipt['wire_byte_length'],len(wire))
                    self.assertNotEqual(receipt['byte_length'],receipt['wire_byte_length']);self.assertEqual(len(requests),1)
                else:
                    with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(store,None)
                    bodies=list(Path(tmp).rglob('response.body'));self.assertEqual(len(bodies),3);self.assertEqual(len(requests),3)
                    for path in bodies:
                        partial=path.read_bytes();self.assertTrue(partial and decoded.startswith(partial))
                        receipt=json.loads((path.parent/'receipt.json').read_bytes());self.assertTrue(receipt['partial']);self.assertLess(receipt['wire_byte_length'],len(wire))
                    self.assertFalse(Path(tmp,s.SLUG).exists())

    def test_json_error_duplicate_nonfinite(self):
        for body in [b'[]',b'{"error":{}}',b'{"x":1,"x":2}',b'{"x":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

if __name__=='__main__':unittest.main()
