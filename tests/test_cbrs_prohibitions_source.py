"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import gzip
import json
import os
import shutil
import tempfile
import sqlite3
import struct
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.fws_prohibitions import fws_cbrs_prohibitions_draft_refresh_schedule, fws_cbrs_prohibitions
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import cbrs_prohibitions as s

CAPTURE=Path(os.environ.get('HIFLD_CBRS_PROHIBITIONS_CAPTURE_DIR','__full_capture_optin_not_configured__'))
REAL_CAPTURE=unittest.skipUnless(os.environ.get('HIFLD_CBRS_PROHIBITIONS_CAPTURE_DIR'),'Complete producer capture is an external artifact; set HIFLD_CBRS_PROHIBITIONS_CAPTURE_DIR')

class CBRSTests(unittest.TestCase):
    @REAL_CAPTURE
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),1693);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['CBRS_Type'],{'System Unit':1129,'Otherwise Protected Area':564})
        self.assertEqual(audit['nulls']['SU_Date'],564)
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(1693)})

    def test_synthetic_native_date_null_fid_and_geometry_roundtrip(self):
        """Two invented rows exercise native serialization; no producer acceptance."""
        import pyarrow.parquet as pq
        from datetime import date
        from shapely.geometry import shape
        from shapely import from_wkb
        polygon={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,0]]]}
        multi={'type':'MultiPolygon','coordinates':[[[[2,0],[3,0],[3,1],[2,0]]]]}
        rows=[{'properties':{'Unit':'SYNTH-001','CBRS_Type':'System Unit','FI_Date':'2000-01-02','SU_Date':'2001-02-03','SHAPE_Leng':1.125,'SHAPE_Area':0.5,'__source_fid':'0'},'geometry':polygon},
              {'properties':{'Unit':'SYNTH-002','CBRS_Type':'Otherwise Protected Area','FI_Date':'2000-01-02','SU_Date':None,'SHAPE_Leng':2.25,'SHAPE_Area':0.5,'__source_fid':'1'},'geometry':multi}]
        table=s.typed_table(rows,{'bounds':[0,0,3,1]})
        self.assertEqual(str(table.schema.field('FI_Date').type),'date32[day]')
        self.assertEqual(str(table.schema.field('SU_Date').type),'date32[day]')
        self.assertEqual(table['SU_Date'].null_count,1)
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'synthetic.parquet';pq.write_table(table,path);result=pq.read_table(path)
        self.assertEqual(result.schema,table.schema)
        self.assertEqual(result['__source_fid'].to_pylist(),['0','1'])
        self.assertEqual(result['FI_Date'].to_pylist(),[date(2000,1,2)]*2)
        for actual,original in zip(result.to_pylist(),rows):
            self.assertEqual(actual['geometry'],shape(original['geometry']).wkb)
            self.assertEqual(from_wkb(actual['geometry']).geom_type,original['geometry']['type'])
            for name in ['Unit','CBRS_Type','SHAPE_Leng','SHAPE_Area']:
                self.assertEqual(actual[name],original['properties'][name])

    def test_synthetic_receipt_type_and_origin_guards(self):
        body=b'synthetic bounded observation only'
        valid={'requested_url':s.URL,'final_url':s.URL,'status':200,'byte_length':len(body),'sha256':s.sha(body),'observed_at':'2026-10-19T00:00:00+00:00'}
        s.validate_receipt(body,valid,s.URL)
        for field,value in [('status',True),('byte_length',True),('requested_url','https://example.invalid/mirror'),('final_url','https://example.invalid/redirect'),('sha256','0'*64),('observed_at','2026-10-19T00:00:00+00:99')]:
            with self.subTest(field=field),self.assertRaises(ValueError):s.validate_receipt(body,{**valid,field:value},s.URL)

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_prohibitions for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_prohibitions_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_prohibitions_draft_refresh_schedule.cron_schedule,'0 13 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_prohibitions_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    @REAL_CAPTURE
    def test_all_receipts_preflight_strict_timestamp_url_pins(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        self.assertEqual(len(s.provenance(captures)['inputs']),4)
        for name in s.URLS:
            for field,value in [('requested_url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19T00:00:00+99:00'),('observed_at',None),('byte_length',True),('sha256','0'*64),('status',True)]:
                changed={n:(body,dict(receipt)) for n,(body,receipt) in captures.items()};changed[name][1][field]=value
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=changed),self.assertRaises((ValueError,TypeError)):
                    try:s.stage_cbrs_prohibitions_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
                    finally:self.assertFalse(Path(d,s.SLUG).exists())

    def test_strict_json_error_array_nested_duplicate_nonfinite(self):
        for body in [b'[]',b'{"error":{}}',b'{"a":1,"a":2}',b'{"a":{"b":1,"b":2}}',b'{"a":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

    def test_bad_receipt_exact_bytes_retained_and_no_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            capture=Path(d,'capture');capture.mkdir();body=b'synthetic untrusted archive observation, not producer data'
            (capture/'official_zip.body').write_bytes(body);bad=b'{"malformed":'
            (capture/'official_zip.receipt.json').write_bytes(bad);stage=Path(d,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())

    def test_partial_each_attempt_retained(self):
        class Partial(httpx.SyncByteStream):
            def __iter__(self):yield b'partial';raise httpx.ReadError('reset')
        factory=httpx.Client
        def response(request):return httpx.Response(200,stream=Partial())
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:factory(transport=httpx.MockTransport(response),**kw)) as client:
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(client.call_count,3)
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')],[b'partial']*3)
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('wire.body')],[b'partial']*3)
            self.assertFalse(Path(d,s.SLUG).exists())

    def test_atomic_interrupt_and_oserror_never_expose_version(self):
        for failure in [OSError('disk'),KeyboardInterrupt()]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                target='cbrs-prohibitions-1/cbrs-prohibitions-1/v0.0.0-draft-fixture'
                with patch.object(s.os,'rename',side_effect=failure),self.assertRaises(type(failure)):
                    s.commit_complete_candidate(storage,target,{'geopackage/test.gpkg':b'fixture'}, {})
                self.assertFalse(Path(d,target).exists())
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))

    def test_live_process_interrupt_retains_acknowledged_prefix_without_retry(self):
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):yield b'actual-consumed-prefix';raise KeyboardInterrupt()
        factory=httpx.Client
        def response(request):return httpx.Response(200,stream=Interrupted())
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:factory(transport=httpx.MockTransport(response),**kw)) as client:
            with self.assertRaises(KeyboardInterrupt):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(client.call_count,1)
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')],[b'actual-consumed-prefix'])
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('wire.body')],[b'actual-consumed-prefix'])
            receipt=json.loads(next(Path(d).rglob('receipt.json')).read_bytes())
            self.assertIs(receipt['capture_complete'],False)
            self.assertEqual(receipt['capture_error_type'],'KeyboardInterrupt')
            self.assertFalse(Path(d,s.SLUG).exists())

    def test_archive_pin_and_zip_roster_changes_fail_before_decode(self):
        body=(CAPTURE/'official_zip.body').read_bytes() if os.environ.get('HIFLD_CBRS_PROHIBITIONS_CAPTURE_DIR') else b'PK\x03\x04synthetic unapproved invalid archive'
        for changed in [body[:-1],body[:1000],b'error']:
            with tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open') as opened,self.assertRaisesRegex(ValueError,'changed_truncated'):
                s.inspect_archive(changed,Path(d))
            opened.assert_not_called()

    @REAL_CAPTURE
    def test_missing_extra_type_id_and_geometry_drift(self):
        body=(CAPTURE/'official_zip.body').read_bytes()
        with tempfile.TemporaryDirectory() as d:
            rows,_,_=s.inspect_archive(body,Path(d))
        class Row(dict):
            def __init__(self,r):
                super().__init__(properties={k:v for k,v in r['properties'].items() if k!='__source_fid'},geometry={'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]})
                self.id=r['properties']['__source_fid']
        class Stream:
            schema={'properties':s.NATIVE_SCHEMA,'geometry':'Polygon'};crs='EPSG:4269'
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def __iter__(self):return iter(self.rows)
        for mode in ['missing','extra','type','null','id','geometry','coverage','date']:
            mutated=[Row(r) for r in rows]
            if mode=='missing':del mutated[0]['properties']['SHAPE_Leng']
            if mode=='extra':mutated[0]['properties']['surprise']=1
            if mode=='type':mutated[0]['properties']['SHAPE_Leng']=20
            if mode=='null':mutated[0]['properties']['SHAPE_Leng']=None
            if mode=='id':mutated[0].id=mutated[1].id
            if mode=='geometry':mutated[0]['geometry']=None
            if mode=='coverage':mutated[0]['properties']['CBRS_Type']='Unreviewed'
            if mode=='date':mutated[0]['properties']['FI_Date']='1983-13-01'
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive(body,Path(d))

    def test_atomic_target_does_not_replace_existing_release(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'prior'}, {'status':'fixture'})
            with self.assertRaises(ValueError):s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'new'}, {})
            self.assertEqual(Path(d,prefix,'geopackage/a.gpkg').read_bytes(),b'prior')

    @REAL_CAPTURE
    def test_cache_metadata_guards_original_gpkg_and_native_date_parquet(self):
        import fiona
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rows,audit,_=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),root)
            gp=root/'source.gpkg'
            s.write_geopackage(gp,rows)
            s.validate_geopackage_metadata(gp,rows)
            original=gp.read_bytes()
            changes=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_geometry_columns SET srs_id=4326", "UPDATE gpkg_contents SET min_x=0", "UPDATE gpkg_spatial_ref_sys SET definition='undefined' WHERE srs_id=4269", "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269", "UPDATE gpkg_geometry_columns SET z=1", "CREATE TABLE unapproved_extra_identifiers (OBJECTID INTEGER)"]
            for sql in changes:
                gp.write_bytes(original)
                with sqlite3.connect(gp) as db:db.execute(sql)
                with self.subTest(sql=sql),self.assertRaisesRegex(ValueError,'auxiliary_metadata_or_header_drift'):
                    s.validate_geopackage_metadata(gp,rows)
            gp.write_bytes(original)
            with sqlite3.connect(gp) as db:
                db.create_function('ST_IsEmpty',1,lambda x:False);db.create_function('ST_MinX',1,lambda x:0);db.create_function('ST_MaxX',1,lambda x:0);db.create_function('ST_MinY',1,lambda x:0);db.create_function('ST_MaxY',1,lambda x:0)
                fid,blob=db.execute('SELECT fid,geom FROM "'+s.SLUG+'" LIMIT 1').fetchone()
                changed=blob[:4]+struct.pack('<i',4326)+blob[8:]
                db.execute('UPDATE "'+s.SLUG+'" SET geom=? WHERE fid=?',(changed,fid))
            with self.assertRaisesRegex(ValueError,'auxiliary_metadata_or_header_drift'):
                s.validate_geopackage_metadata(gp,rows)
            gp.write_bytes(original)
            with sqlite3.connect(gp) as db:blob=db.execute('SELECT geom FROM "'+s.SLUG+'" ORDER BY fid LIMIT 1').fetchone()[0]
            self.assertEqual((blob[3]>>1)&7,1)
            data=bytearray(original);offset=data.find(blob[:64]);self.assertGreaterEqual(offset,0)
            data[offset+8:offset+16]=struct.pack('<d' if blob[3]&1 else '>d',0.0);gp.write_bytes(data)
            with self.assertRaisesRegex(ValueError,'complete_container_bytes'):s.validate_geopackage_metadata(gp,rows)
            table=s.typed_table(rows,audit)
            self.assertEqual(str(table.schema.field('FI_Date').type),'date32[day]')
            self.assertEqual(str(table.schema.field('SU_Date').type),'date32[day]')
            self.assertEqual(table.column('SU_Date').null_count,564)

    @REAL_CAPTURE
    def test_native_prebuilt_policy_binds_existing_artifact_without_whole_format_ban(self):
        from dagster_hifld.output_policy import validate_output_policy
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        manifest=s.provenance(captures)
        self.assertNotIn('geoparquet',manifest['unsupported_output_formats'])
        self.assertEqual(set(manifest['unsupported_output_formats']),{'shapefile_zip','pmtiles'})
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'native.parquet'
            with self.assertRaises(FileNotFoundError):s.native_prebuilt_policy(path)
            path.write_bytes(b'synthetic native-policy fixture only')
            manifest['prebuilt_output_policy']=s.native_prebuilt_policy(path)
            validate_output_policy(manifest)
            self.assertEqual(manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'],[{'path':f'geoparquet/{s.SLUG}.parquet','sha256':s.sha(path.read_bytes()),'bytes':path.stat().st_size}])
            manifest['unsupported_output_formats']['geoparquet']='Inherited whole-format prohibition must still win'
            with self.assertRaises(ValueError):validate_output_policy(manifest)

    def test_common_lock_preserves_concurrent_install_winner(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            started=threading.Event();finished=threading.Event();outcome=[]
            def installer():
                started.set()
                try:s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'loser'}, {})
                except ValueError as exc:outcome.append(str(exc))
                finally:finished.set()
            with s.candidate_lock(storage,prefix) as target:
                thread=threading.Thread(target=installer);thread.start();self.assertTrue(started.wait(1))
                self.assertFalse(finished.wait(0.1))
                target.mkdir();(target/'winner').write_bytes(b'valid-concurrent-winner')
            thread.join(3);self.assertFalse(thread.is_alive())
            self.assertTrue(outcome and 'concurrent_or_incomplete_existing_candidate' in outcome[0])
            self.assertEqual((Path(d)/prefix/'winner').read_bytes(),b'valid-concurrent-winner')

    def tiny_construction(self,root,version='v0.0.0-draft-synthetic-history'):
        """Opaque synthetic protocol data, never a real native source build."""
        capture=root/'synthetic-captures';capture.mkdir()
        bodies={'official_zip':b'explicitly synthetic opaque fixture', 'fws_data':b'synthetic page',
            'fws_download':b'/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip',
            'fws_prohibitions_metadata':b'synthetic 20241219 CBRS_Prohibitions'}
        for name,body in bodies.items():
            (capture/(name+'.body')).write_bytes(body)
            (capture/(name+'.receipt.json')).write_bytes(s.encode({'requested_url':s.URLS[name],'final_url':s.URLS[name],'status':200,'byte_length':len(body),'sha256':s.sha(body),'observed_at':'2026-10-19T00:00:00+00:00','fixture':'synthetic only'}))
        storage=StagingStorageResource(local_dir=str(root/'staging'));captures=s.acquire(storage,capture)
        repo=Path(__file__).parents[1];commit=s.subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
        identity={'files':{'pyproject.toml':s.sha(s.subprocess.check_output(['git','show',commit+':pyproject.toml'],cwd=repo))}}
        native=root/'synthetic.parquet';native.write_bytes(b'opaque synthetic prebuilt bytes, not a native dataset')
        manifest=s.provenance(captures);manifest.update(processing_fingerprint=s.sha(s.encode(identity)),prebuilt_output_policy=s.native_prebuilt_policy(native))
        payloads={'metadata/source_manifest.json':s.encode(manifest),'geoparquet/'+s.SLUG+'.parquet':native.read_bytes(),'raw/retrieval.json':s.encode(captures['official_zip'][1])}
        payloads.update({f'raw/captures/{name}.receipt.json':s.encode(receipt) for name,(_,receipt) in captures.items()})
        ready={'version':version,'objects':{name:s.sha(value) for name,value in payloads.items()}}
        prefix=f'{s.SLUG}/{s.SLUG}/{version}'
        return storage,capture,captures,identity,payloads,ready,prefix

    def test_external_first_construction_preserves_original_receipts_and_commit(self):
        with tempfile.TemporaryDirectory() as d:
            storage,capture,captures,identity,payloads,ready,prefix=self.tiny_construction(Path(d));version=ready['version']
            s.commit_complete_candidate(storage,prefix,payloads,ready,captures=captures,identity=identity)
            key=s.construction_bundle_key(version);original=storage.read_key(key)
            manifest=s.strict_json(payloads['metadata/source_manifest.json'])
            for name,(_,receipt) in captures.items():
                (capture/(name+'.receipt.json')).write_bytes(s.encode({**receipt,'observed_at':'2026-10-20T00:00:00+00:00'}))
            later=s.acquire(storage,capture);shutil.rmtree(Path(storage.local_dir,prefix))
            later_payloads=dict(payloads);later_manifest=s.provenance(later);later_manifest.update(processing_fingerprint=s.sha(s.encode(identity)),prebuilt_output_policy=manifest['prebuilt_output_policy']);later_payloads['metadata/source_manifest.json']=s.encode(later_manifest)
            s.commit_complete_candidate(storage,prefix,later_payloads,ready,captures=later,identity=identity)
            self.assertEqual(storage.read_key(key),original)
            self.assertEqual(s.strict_json(later_payloads['metadata/source_manifest.json']),manifest)
            self.assertEqual(s.strict_json(later_payloads['raw/retrieval.json']),captures['official_zip'][1])
            wrong_identity={'files':{'pyproject.toml':'0'*64}}
            with self.assertRaisesRegex(ValueError,'construction_processing_blob_hash_mismatch'):s.validate_construction_commit(manifest,wrong_identity)

    def test_shared_history_loss_repeated_cold_refusal_and_explicit_recovery(self):
        for mode in ['alias','all_witnesses','marker_bool','marker_float','marker_noncanonical','cas_only']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d:
                storage,_,captures,identity,payloads,ready,prefix=self.tiny_construction(Path(d));version=ready['version']
                s.commit_complete_candidate(storage,prefix,payloads,ready,captures=captures,identity=identity)
                state=Path(storage.local_dir,'_construction-state');trusted={str(p.relative_to(state)):p.read_bytes() for p in state.rglob('*') if p.is_file()};target=Path(storage.local_dir,prefix);candidate=s.tree_inventory(target);key=Path(storage.local_dir,s.construction_bundle_key(version));marker=Path(str(key)+'.first-construction.json')
                if mode=='alias':key.unlink()
                elif mode=='all_witnesses':shutil.rmtree(state)
                elif mode=='cas_only':key.unlink();marker.unlink()
                else:
                    changed=s.strict_json(marker.read_bytes());changed['schema_version']=True if mode=='marker_bool' else 1.0 if mode=='marker_float' else 1
                    marker.write_bytes(s.encode(changed)+(b'\n' if mode=='marker_noncanonical' else b''))
                with s.candidate_lock(storage,prefix),self.assertRaisesRegex(s.CBRSQuarantineError,'observed_construction_history_loss'):
                    s.preflight_construction_history_held(storage,version,identity)
                self.assertFalse(target.exists());bad=list(Path(storage.local_dir,'_quarantine',s.SLUG).glob('invalid-cache-*/candidate'));self.assertEqual(len(bad),1);self.assertEqual(s.tree_inventory(bad[0]),candidate)
                for retry in range(2):
                    with patch.object(s,'seal_first_construction',wraps=s.seal_first_construction) as seal,self.assertRaisesRegex(s.CBRSQuarantineError,'observed_construction_history_loss_requires_explicit_trusted_recovery'):
                        s.commit_complete_candidate(storage,prefix,dict(payloads),dict(ready),captures=captures,identity=identity)
                    seal.assert_not_called()
                # Explicit trusted operator fixture restoration, never automatic repair.
                if state.exists():shutil.rmtree(state)
                for name,body in trusted.items():p=state/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
                shutil.rmtree(s.construction_incident_path(storage,version));os.rename(bad[0],target)
                with s.candidate_lock(storage,prefix):s.preflight_construction_history_held(storage,version,identity)
                original,_=s.load_construction_bundle(storage,version,s.strict_json(payloads['metadata/source_manifest.json']),identity,captures)
                self.assertEqual(original,s.strict_json(payloads['metadata/source_manifest.json']))

    def test_source_uses_shared_canonical_reachable_git_binding(self):
        with tempfile.TemporaryDirectory() as d:
            _,_,_,identity,payloads,_,_=self.tiny_construction(Path(d));manifest=s.strict_json(payloads['metadata/source_manifest.json'])
            s.validate_construction_commit(manifest,identity)
            for changed in ['0'*40,'f'*40,manifest['code_commit'].upper()]:
                with self.subTest(commit=changed),self.assertRaises(ValueError):s.validate_construction_commit({**manifest,'code_commit':changed},identity)
            wrong={'files':{'pyproject.toml':'0'*64}}
            with self.assertRaisesRegex(ValueError,'construction_processing_blob_hash_mismatch'):s.validate_construction_commit(manifest,wrong)

    def test_private_failure_precedes_any_first_state_seal(self):
        for key in ['metadata/source_manifest.json','metadata/cbrs_prohibitions_candidate_ready.json']:
            for failure in [OSError('fixture disk'),KeyboardInterrupt()]:
                with self.subTest(key=key,error=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                    storage,_,captures,identity,payloads,ready,prefix=self.tiny_construction(Path(d));original=s.immutable
                    def fail_private(resource,name,body):
                        if '.candidate-build-' in resource.local_dir and name==key:raise failure
                        return original(resource,name,body)
                    with patch.object(s,'immutable',side_effect=fail_private),patch.object(s,'seal_first_construction',wraps=s.seal_first_construction) as seal,self.assertRaises(type(failure)):
                        s.commit_complete_candidate(storage,prefix,payloads,ready,captures=captures,identity=identity)
                    seal.assert_not_called();self.assertFalse(Path(storage.local_dir,prefix).exists());self.assertFalse(Path(storage.local_dir,'_construction-state').exists());self.assertFalse(list(Path(storage.local_dir).rglob('.candidate-build-*')))

    def test_incident_write_failure_keeps_complete_candidate_in_place(self):
        with tempfile.TemporaryDirectory() as d:
            storage,_,captures,identity,payloads,ready,prefix=self.tiny_construction(Path(d));version=ready['version'];s.commit_complete_candidate(storage,prefix,payloads,ready,captures=captures,identity=identity)
            target=Path(storage.local_dir,prefix);before=s.tree_inventory(target);Path(storage.local_dir,s.construction_bundle_key(version)).unlink();original=s.immutable
            def fail_journal(resource,key,body):
                if key.startswith('_construction-incidents/'):raise OSError('synthetic incident write failure')
                return original(resource,key,body)
            with s.candidate_lock(storage,prefix),patch.object(s,'immutable',side_effect=fail_journal),self.assertRaisesRegex(OSError,'incident write failure'):s.preflight_construction_history_held(storage,version,identity)
            self.assertEqual(s.tree_inventory(target),before);self.assertFalse(list(Path(storage.local_dir).rglob('invalid-cache-*')))

    def test_lexical_root_aliases_reject_before_any_writes(self):
        for ancestor in [False,True]:
            with self.subTest(ancestor=ancestor),tempfile.TemporaryDirectory() as d:
                root=Path(d);outside=root/'outside';outside.mkdir();(outside/'sentinel').write_bytes(b'unchanged');alias=root/'alias';alias.symlink_to(outside,target_is_directory=True)
                selected=alias/'child' if ancestor else alias
                with self.assertRaisesRegex(ValueError,'linked_or_non_directory_staging_root'):s.acquire(StagingStorageResource(local_dir=str(selected)),root/'no-capture')
                self.assertEqual(s.tree_inventory(outside),{'sentinel':{'sha256':s.sha(b'unchanged'),'bytes':9}})

    def test_parent_components_and_nonempty_prefix_reject_before_acquisition(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);outside=root/'outside';outside.mkdir();(outside/'sentinel').write_bytes(b'unchanged');alias=root/'alias';alias.symlink_to(outside,target_is_directory=True)
            resources=[StagingStorageResource(local_dir=str(alias/'..'/'stage')),StagingStorageResource(local_dir=str(root/'stage'),prefix='../outside'),StagingStorageResource(local_dir=str(root/'stage'),prefix='alias')]
            for resource in resources:
                with self.subTest(resource=resource),self.assertRaises(ValueError):s.acquire(resource,root/'no-capture')
            self.assertEqual(s.tree_inventory(outside),{'sentinel':{'sha256':s.sha(b'unchanged'),'bytes':9}});self.assertFalse((root/'stage').exists())

    def test_receipt_io_and_size_failure_keep_only_consumed_bytes(self):
        for mode in ['unreadable','oversize','symlink']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d:
                root=Path(d);capture=root/'capture';capture.mkdir();body=b'synthetic retained before receipt I/O';(capture/'official_zip.body').write_bytes(body);receipt=capture/'official_zip.receipt.json';receipt.write_bytes(b'x'*65538 if mode=='oversize' else b'{}')
                if mode=='unreadable':receipt.chmod(0)
                if mode=='symlink':receipt.unlink();receipt.symlink_to(root/'nonexistent')
                stage=root/'stage'
                try:
                    with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
                finally:
                    if mode=='unreadable':receipt.chmod(0o600)
                self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('official_zip.body')))
                diagnostics=[s.strict_json(p.read_bytes()) for p in stage.rglob('receipt.json')];self.assertEqual(len(diagnostics),1);self.assertEqual(diagnostics[0]['raw_receipt_read'],mode=='oversize')
                if mode=='oversize':self.assertEqual(next(stage.rglob('original-receipt.prefix.body')).read_bytes(),b'x'*65537)
                else:self.assertFalse(list(stage.rglob('original-receipt.body')))

    def test_compressed_wire_length_and_decoded_body_limit(self):
        url='https://www.fws.gov/test-transport-fixture';decoded=b'complete-decoded-body-'*10000;wire=gzip.compress(decoded)
        def response(request):
            self.assertEqual(request.headers['accept-encoding'],'identity')
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire))},stream=httpx.ByteStream(wire))
        factory=httpx.Client
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s.httpx,'Client',side_effect=lambda **kwargs:factory(transport=httpx.MockTransport(response),**kwargs)):
            captures=s.acquire(StagingStorageResource(local_dir=d),None)
            body,receipt=captures['fixture'];self.assertEqual(body,decoded)
            self.assertEqual(receipt['wire_byte_length'],len(wire));self.assertNotEqual(len(body),len(wire))
        def wrong_length(request):
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire)+1)},stream=httpx.ByteStream(wire))
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s.httpx,'Client',side_effect=lambda **kwargs:factory(transport=httpx.MockTransport(wrong_length),**kwargs)):
            with self.assertRaisesRegex(ValueError,'incomplete_content_length'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
            self.assertTrue(all(p.read_bytes()==decoded for p in Path(d).rglob('response.body')))
        class PartialGzip(httpx.SyncByteStream):
            def __iter__(self):
                yield wire[:-8]
                raise httpx.ReadError('interrupted gzip trailer')
        def partial_response(request):
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(wire))},stream=PartialGzip())
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s.httpx,'Client',side_effect=lambda **kwargs:factory(transport=httpx.MockTransport(partial_response),**kwargs)):
            with self.assertRaisesRegex(ValueError,'transport_error'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
            self.assertTrue(all(p.read_bytes()==decoded for p in Path(d).rglob('response.body')))
            receipts=[json.loads(p.read_text()) for p in Path(d).rglob('receipt.json')]
            self.assertTrue(all(x['partial'] and x['wire_byte_length']==len(wire)-8 for x in receipts))
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s,'MAX_BYTES',100),patch.object(s.httpx,'Client',side_effect=lambda **kwargs:factory(transport=httpx.MockTransport(response),**kwargs)):
            with self.assertRaisesRegex(ValueError,'response_size_limit'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertTrue(all(p.read_bytes()==decoded for p in Path(d).rglob('response.body')))

    def test_local_read_error_retains_actual_consumed_prefix(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);capture=root/'capture';capture.mkdir();(capture/'official_zip.body').write_bytes(b'synthetic prefix retained');stage=root/'stage'
            original=s.os.read;calls=[0]
            def interrupted(fd,limit):
                calls[0]+=1
                if calls[0]==1:return original(fd,5)
                raise OSError('synthetic local read interruption')
            with patch.object(s.os,'read',side_effect=interrupted),self.assertRaisesRegex(ValueError,'capture_input_read_error'):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
            self.assertTrue(any(p.read_bytes()==b'synth' for p in stage.rglob('official_zip.body')))
            diagnostic=s.strict_json(next(stage.rglob('receipt.json')).read_bytes());self.assertTrue(diagnostic['raw_body_read']);self.assertFalse(diagnostic['raw_body_complete']);self.assertFalse(diagnostic['raw_receipt_read'])

    def test_live_delivered_overflow_and_reserve_before_next(self):
        url='https://www.fws.gov/synthetic-transport';body=b'x'*68;factory=httpx.Client
        def response(request):return httpx.Response(200,stream=httpx.ByteStream(body))
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s,'MAX_BYTES',36),patch.object(s.httpx,'Client',side_effect=lambda **kw:factory(transport=httpx.MockTransport(response),**kw)):
            with self.assertRaisesRegex(ValueError,'response_size_limit'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')],[body]*3)
        delivered=[]
        class Blocks(httpx.SyncByteStream):
            def __iter__(self):
                delivered.append(1);yield b'a'*s.LIVE_BLOCK_BYTES
                delivered.append(2);yield b'b'*s.LIVE_BLOCK_BYTES
        def blocks(request):return httpx.Response(200,stream=Blocks())
        for allow in [0,1,2]:
            delivered.clear();calls=[0]
            def available(storage):
                calls[0]+=1
                return 10**12 if calls[0]%(allow+1) else 0
            with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s,'available_capture_bytes',side_effect=available),patch.object(s.httpx,'Client',side_effect=lambda **kw:factory(transport=httpx.MockTransport(blocks),**kw)):
                with self.assertRaisesRegex(ValueError,'capture_disk_reserve_before_next_block'):s.acquire(StagingStorageResource(local_dir=d),None)
                self.assertEqual(delivered,[1]*3 if allow else [])
                self.assertEqual([p.read_bytes() for p in Path(d).rglob('wire.body')],[b'a'*s.LIVE_BLOCK_BYTES if allow else b'']*3)
                self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')],[b'a'*s.LIVE_BLOCK_BYTES if allow==2 else b'']*3)

    def test_shared_capture_incomplete_and_sink_mismatch_cannot_become_success(self):
        from dataclasses import replace
        original=s.capture_http_response;factory=httpx.Client;body=b'synthetic complete transport only';url='https://www.fws.gov/synthetic-capture-boundary'
        def response(request):return httpx.Response(200,stream=httpx.ByteStream(body))
        for mode in ['incomplete','sink_mismatch']:
            def changed(*args,**kwargs):
                result=original(*args,**kwargs)
                return replace(result,complete=False,error_code='synthetic_forced_incomplete') if mode=='incomplete' else replace(result,wire_sha256='0'*64)
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s,'capture_http_response',side_effect=changed),patch.object(s.httpx,'Client',side_effect=lambda **kw:factory(transport=httpx.MockTransport(response),**kw)):
                with self.assertRaisesRegex(ValueError,'synthetic_forced_incomplete' if mode=='incomplete' else 'capture_sink_readback_mismatch'):s.acquire(StagingStorageResource(local_dir=d),None)
                self.assertEqual([p.read_bytes() for p in Path(d).rglob('wire.body')],[body]*3)
                self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')],[body]*3)
                self.assertTrue(all(s.strict_json(p.read_bytes())['capture_complete'] is False for p in Path(d).rglob('receipt.json')))
                self.assertFalse(Path(d,s.SLUG).exists())

if __name__=='__main__':unittest.main()
