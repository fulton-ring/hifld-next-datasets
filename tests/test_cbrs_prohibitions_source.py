"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import gzip
import json
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

CAPTURE=Path(__file__).parents[1]/'docs/dataset-sources/cbrs-prohibitions-1/evidence/captures'

class CBRSTests(unittest.TestCase):
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),1693);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['CBRS_Type'],{'System Unit':1129,'Otherwise Protected Area':564})
        self.assertEqual(audit['nulls']['SU_Date'],564)
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(1693)})

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_prohibitions for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_prohibitions_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_prohibitions_draft_refresh_schedule.cron_schedule,'0 13 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_prohibitions_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

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
            capture=Path(d,'capture');capture.mkdir();body=(CAPTURE/'official_zip.body').read_bytes()
            (capture/'official_zip.body').write_bytes(body);bad=b'{"malformed":'
            (capture/'official_zip.receipt.json').write_bytes(bad);stage=Path(d,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())

    def test_partial_each_attempt_retained(self):
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

    def test_atomic_interrupt_and_oserror_never_expose_version(self):
        for failure in [OSError('disk'),KeyboardInterrupt()]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                target='cbrs-prohibitions-1/cbrs-prohibitions-1/v0.0.0-draft-fixture'
                with patch.object(s.os,'rename',side_effect=failure),self.assertRaises(type(failure)):
                    s.commit_complete_candidate(storage,target,{'geopackage/test.gpkg':b'fixture'}, {})
                self.assertFalse(Path(d,target).exists())
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))

    def test_archive_pin_and_zip_roster_changes_fail_before_decode(self):
        body=(CAPTURE/'official_zip.body').read_bytes()
        for changed in [body[:-1],body[:1000],b'error']:
            with tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open') as opened,self.assertRaisesRegex(ValueError,'changed_truncated'):
                s.inspect_archive(changed,Path(d))
            opened.assert_not_called()

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

    def test_external_first_construction_preserves_original_receipts_and_commit(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        repo=Path(__file__).parents[1];commit=s.subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
        name='pyproject.toml';identity={'files':{name:s.sha(s.subprocess.check_output(['git','show',f'{commit}:{name}'],cwd=repo))}}
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);s.acquire(storage,CAPTURE)
            native=Path(d)/'fixture.parquet';native.write_bytes(b'policy-only native fixture')
            manifest=s.provenance(captures);manifest.update(processing_fingerprint=s.sha(s.encode(identity)),prebuilt_output_policy=s.native_prebuilt_policy(native))
            version='v0.0.0-draft-construction-unit-fixture';payloads={'metadata/source_manifest.json':s.encode(manifest)};ready={'version':version}
            with s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                s.bind_first_construction_held(storage,version,payloads,ready,captures,identity)
            bundle_key=s.construction_bundle_key(version);original_bundle=storage.read_key(bundle_key)
            changed=copy.deepcopy(captures)
            for _,receipt in changed.values():receipt['observed_at']='2026-10-20T01:02:03+00:00'
            later=s.provenance(changed);later.update(processing_fingerprint=s.sha(s.encode(identity)),prebuilt_output_policy=manifest['prebuilt_output_policy'],code_commit='f'*40)
            later_payload={'metadata/source_manifest.json':s.encode(later)}
            with s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                s.bind_first_construction_held(storage,version,later_payload,ready,changed,identity)
            self.assertEqual(storage.read_key(bundle_key),original_bundle)
            self.assertEqual(s.strict_json(later_payload['metadata/source_manifest.json']),manifest)
            self.assertEqual(s.strict_json(later_payload['raw/retrieval.json']),captures['official_zip'][1])
            Path(d,bundle_key).unlink()
            with self.assertRaisesRegex(ValueError,'missing_original_construction_bundle'):
                s.load_construction_bundle(storage,version,manifest,identity,captures)
            forged=s.strict_json(original_bundle);forged['manifest']['code_commit']='0'*40
            Path(d,bundle_key).write_bytes(s.encode(forged))
            with self.assertRaisesRegex(ValueError,'original_construction_commit_not_in_repository'):
                s.load_construction_bundle(storage,version,manifest,identity,captures)
            forged=s.strict_json(original_bundle);forged['manifest']['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]['bytes']+=1
            Path(d,bundle_key).write_bytes(s.encode(forged))
            with self.assertRaisesRegex(ValueError,'original_construction_manifest_drift'):
                s.load_construction_bundle(storage,version,manifest,identity,captures)
            Path(d,bundle_key).write_bytes(original_bundle)
            wrong_identity={'files':{name:'0'*64}}
            with self.assertRaisesRegex(ValueError,'original_construction_commit_processing_files_differ'):
                s.validate_construction_commit(manifest,wrong_identity)

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
            with self.assertRaisesRegex(ValueError,'interrupted gzip trailer'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
            self.assertTrue(all(p.read_bytes()==decoded for p in Path(d).rglob('response.body')))
            receipts=[json.loads(p.read_text()) for p in Path(d).rglob('receipt.json')]
            self.assertTrue(all(x['partial'] and x['wire_byte_length']==len(wire)-8 for x in receipts))
        with tempfile.TemporaryDirectory() as d,patch.object(s,'URLS',{'fixture':url}),patch.object(s,'MAX_BYTES',100),patch.object(s.httpx,'Client',side_effect=lambda **kwargs:factory(transport=httpx.MockTransport(response),**kwargs)):
            with self.assertRaisesRegex(ValueError,'response_size_limit'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertTrue(all(p.read_bytes()==decoded[:101] for p in Path(d).rglob('response.body')))

if __name__=='__main__':unittest.main()
