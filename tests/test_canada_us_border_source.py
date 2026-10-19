"""Complete authentic producer release fixtures test draft mechanics, not acceptance."""
import copy
import gzip
import os
import fcntl
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely.geometry import shape

from dagster_hifld.assets.ibc import ibc_boundary_draft_refresh_schedule, ibc_canada_us_border
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import _iter_staged_version_paths
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import snapshot_source_metadata
from dagster_hifld.sources import canada_us_border as s

CAPTURE=Path(__file__).parents[1]/'docs/dataset-sources/canada-and-us-border/evidence/captures'


class BoundaryTests(unittest.TestCase):
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'ibc-zip.body').read_bytes(),Path(d))
            self.assertEqual(len(rows),30);self.assertEqual(len(members),11)
            self.assertEqual(audit['invalid_geometry_count'],0)
            self.assertEqual({r['properties']['SectionNum'] for r in rows},set(range(30)))
            self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(30)})
            self.assertTrue(all(shape(r['geometry']).is_valid for r in rows))
            with self.assertRaisesRegex(ValueError,'changed_truncated'):
                s.inspect_archive((CAPTURE/'ibc-zip.body').read_bytes()[:-1],Path(d))

    def test_registry_schedule_and_no_published_destination(self):
        self.assertEqual(sum(a is ibc_canada_us_border for a in hifld_ingest_assets),1)
        self.assertEqual(ibc_boundary_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(ibc_boundary_draft_refresh_schedule.cron_schedule,'0 11 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_all_capture_receipts_preflight_before_candidate(self):
        for name in s.URLS:
            for field,value in [('url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at',None),('bytes',True),('sha256','0'*64),('status',True)]:
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d:
                    capture=Path(d,'capture');shutil.copytree(CAPTURE,capture)
                    receipt=capture/(name+'.receipt.json');j=json.loads(receipt.read_text());j[field]=value;receipt.write_text(json.dumps(j))
                    stage=Path(d,'stage')
                    with self.assertRaises((ValueError,TypeError)):
                        s.stage_canada_us_border_candidate(StagingStorageResource(local_dir=str(stage)),capture_dir=capture)
                    self.assertFalse((stage/s.SLUG).exists())
                    self.assertTrue(list(stage.rglob('report.json')))

    def test_strict_json_rejects_error_array_duplicate_nested(self):
        for body in [b'[]',b'{"error":{}}',b'{"a":1,"a":2}',b'{"a":{"b":1,"b":2}}',b'{"a":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

    def test_partial_responses_preserved_every_attempt(self):
        class Response:
            status_code=200;url=s.URL;headers={}
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def iter_bytes(self,**kw):yield b'partial';raise httpx.ReadError('reset')
        class Client:
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def stream(self,*a):return Response()
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',return_value=Client()) as c:
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(c.call_count,3);self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
            self.assertFalse(Path(d,s.SLUG).exists())

    def test_full_staging_fidelity_idempotency_cache_drift(self):
        identity=s.processing_identity()
        with tempfile.TemporaryDirectory() as d,patch.object(s,'processing_identity',return_value=identity):
            storage=StagingStorageResource(local_dir=d)
            result=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            root=Path(d,s.SLUG,s.SLUG,result['version'])
            before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
            self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE),result)
            self.assertEqual(before,{str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()})
            table=pq.read_table(root/'candidate/typed-source.parquet');self.assertEqual(len(table),30)
            self.assertEqual(str(table.schema.field('SectionNum').type),'int32')
            with tempfile.TemporaryDirectory() as original:
                rows,_,_=s.inspect_archive((CAPTURE/'ibc-zip.body').read_bytes(),Path(original))
            native={r['properties']['SectionNum']:r for r in rows}
            for row in table.to_pylist():
                expected=native[row['SectionNum']]
                self.assertEqual({k:row[k] for k in s.SCHEMA},expected['properties'])
                self.assertEqual(row['geometry'],shape(expected['geometry']).wkb)
            with fiona.open(root/'geopackage'/f'{s.SLUG}.gpkg') as gp:
                for row in gp:
                    expected=native[row['properties']['SectionNum']]
                    self.assertEqual(dict(row['properties']),expected['properties'])
                    self.assertEqual(shape(row.geometry).wkb,shape(expected['geometry']).wkb)
            manifest=json.loads((root/'metadata/source_manifest.json').read_text());s.validate_release_provenance(manifest)
            self.assertEqual(len(manifest['inputs']),4)
            changed=copy.deepcopy(identity);changed['gdal']='changed-runtime'
            with patch.object(s,'processing_identity',return_value=changed):
                self.assertNotEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)['version'],result['version'])
            path=root/'candidate/typed-source.parquet';path.write_bytes(b'corrupted')
            with self.assertRaisesRegex(ValueError,'existing_candidate_object_changed'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)

    def test_schema_id_geometry_mutations_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            rows,_,_=s.inspect_archive((CAPTURE/'ibc-zip.body').read_bytes(),Path(d))
        class Stream:
            schema={'properties':s.NATIVE_SCHEMA,'geometry':'LineString'}
            crs='EPSG:4269'
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def __iter__(self):return iter(self.rows)
        class Row(dict):
            def __init__(self,r):super().__init__(properties={k:v for k,v in r['properties'].items() if k!='__source_fid'},geometry=r['geometry']);self.id=r['properties']['__source_fid']
        for mode in ['missing','duplicate','type','extra','geometry']:
            mutated=[Row(r) for r in rows]
            if mode=='missing':del mutated[0]['properties']['SectionNum']
            if mode=='duplicate':mutated[0]['properties']['SectionNum']=mutated[1]['properties']['SectionNum']
            if mode=='type':mutated[0]['properties']['SectionNum']=float(mutated[0]['properties']['SectionNum'])
            if mode=='extra':mutated[0]['properties']['unexpected']='x'
            if mode=='geometry':mutated[0]['geometry']=None
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive((CAPTURE/'ibc-zip.body').read_bytes(),Path(d))

    def test_cache_repin_cannot_replace_fresh_source_reconciliation(self):
        for kind in ['parquet_value','parquet_type','gpkg_value','records','extra_file','extra_ready']:
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                output=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                root=Path(d,s.SLUG,s.SLUG,output['version']);marker=root/'metadata/canada_us_border_candidate_ready.json'
                ready=json.loads(marker.read_text());target=None
                if kind.startswith('parquet'):
                    target=root/'candidate/typed-source.parquet';table=pq.read_table(target)
                    if kind=='parquet_value':table=table.set_column(table.schema.get_field_index('SectionEng'),table.schema.field('SectionEng'),pa.array(['changed']*30))
                    else:table=table.set_column(table.schema.get_field_index('SectionNum'),'SectionNum',table.column('SectionNum').cast(pa.int64()))
                    pq.write_table(table,target)
                elif kind=='gpkg_value':
                    target=root/'geopackage'/f'{s.SLUG}.gpkg'
                    with fiona.open(target) as f:schema=f.schema;crs=f.crs;rows=[{'properties':dict(r['properties']),'geometry':r.geometry} for r in f]
                    rows[0]['properties']['SectionEng']='changed';target.unlink()
                    with fiona.open(target,'w',driver='GPKG',layer=s.SLUG,crs=crs,schema=schema) as f:f.writerecords(rows)
                elif kind=='records':
                    target=root/'metadata/candidate.records.jsonl';target.write_bytes(target.read_bytes().replace(b'West',b'East'))
                else:
                    target=root/'candidate/extra.parquet';target.write_bytes(b'extra')
                    if kind=='extra_file':target=None
                if target is not None:ready['objects'][str(target.relative_to(root))]=s.sha(target.read_bytes())
                marker.write_text(json.dumps(ready))
                raw=(root/'raw'/s.ARCHIVE_NAME).read_bytes()
                with self.assertRaisesRegex(ValueError,'fidelity|differ|roster|schema|inventory'):
                    s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                retained=list(Path(d,'_quarantine').glob(f'{s.SLUG}/invalid-cache-*/candidate/raw/{s.ARCHIVE_NAME}'))
                self.assertEqual(len(retained),1);self.assertEqual(retained[0].read_bytes(),raw)
                self.assertFalse(root.exists())
                self.assertTrue(list(Path(d,'_quarantine').rglob('report.json')))

    def test_interrupted_private_build_never_discoverable(self):
        original=s.immutable
        for failure in [OSError('injected disk failure'),KeyboardInterrupt('injected interruption')]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                def injected(target,key,body):
                    if Path(target.local_dir).name.startswith('.candidate-build-') and key=='candidate/typed-source.parquet':raise failure
                    return original(target,key,body)
                with patch.object(s,'immutable',side_effect=injected),self.assertRaises(s.BoundaryQuarantineError if isinstance(failure,OSError) else KeyboardInterrupt):
                    s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                self.assertEqual(list(_iter_staged_version_paths(storage)),[])
                self.assertTrue(list(Path(d,'_raw-captures').rglob('ibc-zip.body')))
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))

    def test_invalid_offset_and_original_bad_receipt_bytes_retained(self):
        for original in [b'{"url":"x","url":"y"}',b'[]']:
            with self.subTest(original=original),tempfile.TemporaryDirectory() as d:
                capture=Path(d,'capture');shutil.copytree(CAPTURE,capture)
                (capture/'ibc-zip.receipt.json').write_bytes(original)
                storage=StagingStorageResource(local_dir=str(Path(d,'stage')))
                with self.assertRaises(ValueError):s.stage_canada_us_border_candidate(storage,capture_dir=capture)
                self.assertTrue(any(p.read_bytes()==original for p in Path(d,'stage','_quarantine').rglob('original-receipt.body')))
        body=(CAPTURE/'ibc-zip.body').read_bytes();receipt=json.loads((CAPTURE/'ibc-zip.receipt.json').read_text())
        for timestamp in ['2026-10-19T01:02:03+00:99','2026-10-19T01:02:03+24:00','2026-10-19T01:02:03+01:60','2026-10-19T25:02:03+00:00']:
            with self.subTest(timestamp=timestamp),self.assertRaises(ValueError):s.validate_receipt(body,{**receipt,'observed_at':timestamp},s.URL)
        with tempfile.TemporaryDirectory() as d:
            captures=s.acquire(StagingStorageResource(local_dir=d),CAPTURE);manifest=s.provenance(captures)
        for item in manifest['inputs']:
            self.assertEqual(item['license'],'https://open.canada.ca/en/open-government-licence-canada' if item['input_id'].startswith('ibc-zip:') else s.TERMS)


    def test_native_container_metadata_self_repin_rejected_and_retained(self):
        mutations={
            'contents_crs':"UPDATE gpkg_contents SET srs_id=4326",
            'authority':"UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269",
            'definition':"UPDATE gpkg_spatial_ref_sys SET definition=(SELECT definition FROM gpkg_spatial_ref_sys WHERE srs_id=4326) WHERE srs_id=4269",
            'stored_bounds':"UPDATE gpkg_contents SET min_x=0",
            'ancillary_table':"CREATE TABLE contradictory_metadata (value TEXT)",
        }
        for label,sql in mutations.items():
            with self.subTest(label=label),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                out=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                root=Path(d,s.SLUG,s.SLUG,out['version'])
                target=root/'geopackage'/f'{s.SLUG}.gpkg'
                with sqlite3.connect(target) as db:db.execute(sql)
                marker=root/'metadata/canada_us_border_candidate_ready.json'
                ready=json.loads(marker.read_text());ready['objects'][f'geopackage/{s.SLUG}.gpkg']=s.sha(target.read_bytes())
                marker.write_text(json.dumps(ready))
                before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
                published=Path(d,'published');published.mkdir();(published/'accepted-fixture').write_bytes(b'synthetic-prior')
                with self.assertRaisesRegex(ValueError,'cached_geopackage_canonical_container_differs'):
                    s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                self.assertFalse(root.exists())
                retained=list(Path(d,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate'))
                self.assertEqual(len(retained),1)
                self.assertEqual(before,{str(p.relative_to(retained[0])):s.sha(p.read_bytes()) for p in retained[0].rglob('*') if p.is_file()})
                self.assertEqual((published/'accepted-fixture').read_bytes(),b'synthetic-prior')
                self.assertEqual(list(_iter_staged_version_paths(storage)),[])
                restored=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                self.assertEqual(restored,out)

    def test_repeat_catalog_snapshots_allow_refresh_and_bind_authored_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            out=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            prefix=f"{s.SLUG}/{s.SLUG}/{out['version']}"
            snapshot_source_metadata(storage,s.SLUG,s.SLUG,out['version'])
            for name in ['data_dictionary','quality_manifest']:
                storage.write_key(f'{prefix}/metadata/{name}.json',b'{}')
            snapshot_source_metadata(storage,s.SLUG,s.SLUG,out['version'])
            for name in ['data_dictionary','quality_manifest']:
                self.assertTrue(storage.object_exists(f'{prefix}/metadata/source/{name}.json'))
            self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE),out)
            storage.write_key(f'{prefix}/metadata/source/source_manifest.json',b'{}')
            with self.assertRaisesRegex(ValueError,'cached_authored_manifest_snapshot_differs'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(Path(d,prefix).exists())


    def test_missing_ready_version_is_retained_outside_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            out=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            root=Path(d,s.SLUG,s.SLUG,out['version'])
            (root/'metadata/canada_us_border_candidate_ready.json').unlink()
            before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
            with self.assertRaisesRegex(ValueError,'concurrent_or_incomplete_existing_candidate'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(root.exists());self.assertEqual(list(_iter_staged_version_paths(storage)),[])
            retained=list(Path(d,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate'))
            self.assertEqual(len(retained),1)
            self.assertEqual(before,{str(p.relative_to(retained[0])):s.sha(p.read_bytes()) for p in retained[0].rglob('*') if p.is_file()})
            self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE),out)

    def test_full_source_reports_and_unapproved_placeholder_cannot_self_repin(self):
        names=['metadata/geopackage_source_fidelity.json','metadata/parquet_source_fidelity.json',
            'metadata/baseline_comparison.json','metadata/source-fidelity/baseline_comparison.json',
            'metadata/source-fidelity/baseline_comparison.md','metadata/source-fidelity/baseline_differences.jsonl']
        for name in names:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                out=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                root=Path(d,s.SLUG,s.SLUG,out['version']);target=root/name
                if name.endswith('.json'):
                    value=json.loads(target.read_text())
                    if name.endswith('_source_fidelity.json'):
                        value['shared_verifier']['populations']['baseline']['rows']=1
                        value['shared_verifier']['baseline']['approved']=True
                    else:value.update(status='pass',approved=True,acceptance='approved')
                    target.write_bytes(s.encode(value))
                else:target.write_bytes(b'forged evidence')
                marker=root/'metadata/canada_us_border_candidate_ready.json'
                ready=json.loads(marker.read_text());ready['objects'][name]=s.sha(target.read_bytes());marker.write_text(json.dumps(ready))
                before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
                with self.assertRaisesRegex(ValueError,'cached_source_fidelity_evidence_differs'):
                    s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
                self.assertFalse(root.exists());self.assertEqual(list(_iter_staged_version_paths(storage)),[])
                retained=list(Path(d,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate'))
                self.assertEqual(len(retained),1)
                self.assertEqual(before,{str(p.relative_to(retained[0])):s.sha(p.read_bytes()) for p in retained[0].rglob('*') if p.is_file()})


    def test_cache_observation_lock_and_concurrent_install_winner(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);install=s.commit_complete_candidate
            def competing(store,prefix,payloads,ready):
                install(store,prefix,payloads,ready)
                raise s.BoundaryQuarantineError('simulated_concurrent_install_winner')
            with patch.object(s,'commit_complete_candidate',side_effect=competing),self.assertRaisesRegex(ValueError,'simulated_concurrent_install_winner'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            versions=list(Path(d,s.SLUG,s.SLUG).glob('v*'));self.assertEqual(len(versions),1)
            root=versions[0];before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
            reconcile=s.reconcile_cached_candidate
            def check_held(store,prefix,*args):
                target=s.local_candidate_path(store,prefix)
                fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(fd)
                return reconcile(store,prefix,*args)
            with patch.object(s,'reconcile_cached_candidate',side_effect=check_held):
                self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)['feature_count'],30)
            self.assertEqual(before,{str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()})
            self.assertFalse(list(Path(d,'_quarantine',s.SLUG).glob('invalid-cache-*')))
            (root/'metadata/canada_us_border_candidate_ready.json').unlink()
            with patch.object(s,'commit_complete_candidate') as commit,self.assertRaisesRegex(ValueError,'missing_candidate_ready'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            commit.assert_not_called();self.assertFalse(root.exists())

    def test_original_commit_cannot_self_repin_and_docs_refresh_retains_original(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            out=s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            root=Path(d,s.SLUG,s.SLUG,out['version'])
            manifest=root/'metadata/source_manifest.json';original=manifest.read_bytes()
            bundle=Path(d,'_source-provenance',s.SLUG,out['version'],'bundle.json')
            original_bundle=bundle.read_bytes()
            # A subsequent docs-only HEAD does not rewrite the real construction.
            with patch.object(s.subprocess,'check_output',return_value='f'*40+'\n'):
                self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE),out)
            self.assertEqual(manifest.read_bytes(),original)
            forged=json.loads(original);forged['code_commit']='0'*40;manifest.write_bytes(s.encode(forged))
            marker=root/'metadata/canada_us_border_candidate_ready.json';ready=json.loads(marker.read_text())
            ready['objects']['metadata/source_manifest.json']=s.sha(manifest.read_bytes());marker.write_bytes(s.encode(ready))
            before={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
            published=Path(d,'published');published.mkdir();(published/'prior').write_bytes(b'local-fixture')
            with self.assertRaisesRegex(ValueError,'cached_original_construction_manifest_changed'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(root.exists());self.assertEqual(list(_iter_staged_version_paths(storage)),[])
            retained=list(Path(d,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate'))
            self.assertEqual(len(retained),1)
            self.assertEqual(before,{str(p.relative_to(retained[0])):s.sha(p.read_bytes()) for p in retained[0].rglob('*') if p.is_file()})
            with patch.object(s.subprocess,'check_output',return_value='f'*40+'\n'):
                self.assertEqual(s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE),out)
            self.assertEqual(manifest.read_bytes(),original);self.assertEqual(bundle.read_bytes(),original_bundle)
            self.assertEqual((published/'prior').read_bytes(),b'local-fixture')
            # Existing unbound caches cannot bless themselves as original history.
            bundle.unlink()
            with self.assertRaisesRegex(ValueError,'missing_original_construction_provenance_bundle'):
                s.stage_canada_us_border_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(root.exists());self.assertFalse(bundle.exists())

    def test_encoded_wire_length_and_partial_diagnostics(self):
        actual_client=httpx.Client
        bodies={url:(CAPTURE/(name+'.body')).read_bytes() for name,url in s.URLS.items()}
        seen=[]
        def success(request):
            seen.append(request.headers.get('accept-encoding'))
            raw=gzip.compress(bodies[str(request.url)])
            return httpx.Response(200,headers={'content-encoding':'gzip','content-length':str(len(raw))},
                stream=httpx.ByteStream(raw),request=request)
        with tempfile.TemporaryDirectory() as d:
            transport=httpx.MockTransport(success)
            with patch.object(s.httpx,'Client',side_effect=lambda **kw:actual_client(transport=transport,**kw)):
                captured=s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(captured),4);self.assertEqual(seen,['identity']*4)
            for name,url in s.URLS.items():
                body,receipt=captured[name];self.assertEqual(body,bodies[url])
                self.assertEqual(receipt['wire_bytes'],len(gzip.compress(body)))
                self.assertEqual(receipt['content_encoding'],'gzip')
        def wrong_length(request):
            raw=gzip.compress(bodies[str(request.url)])
            return httpx.Response(200,headers={'content-encoding':'gzip','content-length':str(len(raw)+1)},
                stream=httpx.ByteStream(raw),request=request)
        with tempfile.TemporaryDirectory() as d:
            transport=httpx.MockTransport(wrong_length)
            with patch.object(s.httpx,'Client',side_effect=lambda **kw:actual_client(transport=transport,**kw)),self.assertRaisesRegex(ValueError,'incomplete_content_length'):
                s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(list(Path(d,'_quarantine').rglob('response.body'))),3)
            for path in Path(d,'_quarantine').rglob('response.body'):self.assertEqual(path.read_bytes(),bodies[s.URL])
