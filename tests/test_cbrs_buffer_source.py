"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import gzip
import threading
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.fws import fws_cbrs_buffer_draft_refresh_schedule, fws_cbrs_buffer_zone
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import cbrs_buffer as s

CAPTURE=Path(__file__).parents[1]/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures'

class CBRSTests(unittest.TestCase):
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),1145);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['BUFF_DIST'],{20.0:1138,0.0:7})
        self.assertEqual(len(audit['zero_buffer_width_fids']),7)
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(1145)})

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_buffer_zone for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_buffer_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_buffer_draft_refresh_schedule.cron_schedule,'0 12 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_buffer_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_all_receipts_preflight_strict_timestamp_url_pins(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        self.assertEqual(len(s.provenance(captures)['inputs']),4)
        for name in s.URLS:
            for field,value in [('requested_url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19T00:00:00+99:00'),('observed_at',None),('byte_length',True),('sha256','0'*64),('status',True)]:
                changed={n:(body,dict(receipt)) for n,(body,receipt) in captures.items()};changed[name][1][field]=value
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=changed),self.assertRaises((ValueError,TypeError)):
                    try:s.stage_cbrs_buffer_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
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
                target='cbrs-buffer-zone-1/cbrs-buffer-zone-1/v0.0.0-draft-fixture'
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
        for mode in ['missing','extra','type','null','id','geometry','coverage']:
            mutated=[Row(r) for r in rows]
            if mode=='missing':del mutated[0]['properties']['BUFF_DIST']
            if mode=='extra':mutated[0]['properties']['surprise']=1
            if mode=='type':mutated[0]['properties']['BUFF_DIST']=20
            if mode=='null':mutated[0]['properties']['BUFF_DIST']=None
            if mode=='id':mutated[0].id=mutated[1].id
            if mode=='geometry':mutated[0]['geometry']=None
            if mode=='coverage':mutated[0]['properties']['BUFF_DIST']=10.0
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive(body,Path(d))

    def test_atomic_target_does_not_replace_existing_release(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'prior'}, {'status':'fixture'})
            with self.assertRaises(ValueError):s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'new'}, {})
            self.assertEqual(Path(d,prefix,'geopackage/a.gpkg').read_bytes(),b'prior')

    def test_every_auxiliary_geopackage_byte_is_bound(self):
        rows=[{'type':'Feature','properties':{'BUFF_DIST':20.0,'Shape_Leng':1.0,'Shape_Area':0.5,'__source_fid':'0'},
               'geometry':{'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]}}]
        mutations=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_spatial_ref_sys SET definition='contradictory' WHERE srs_id=4269",
                   "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269",
                   "CREATE TABLE unreviewed_metadata(value TEXT)", "UPDATE gpkg_contents SET min_x=-180",
                   "UPDATE rtree_cbrs-buffer-zone-1_geom SET minx=-180"]
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'original.gpkg';s.write_native_geopackage(rows,path);original=path.read_bytes()
            s.verify_canonical_geopackage(path,rows)
            for sql in mutations:
                with self.subTest(sql=sql):
                    path.write_bytes(original)
                    with sqlite3.connect(path) as db:
                        if sql.startswith('UPDATE rtree_'):
                            table=db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'rtree_%_geom' AND type='table'").fetchone()[0]
                            db.execute('UPDATE "'+table+'" SET minx=-180')
                        else:db.execute(sql)
                    with self.assertRaisesRegex(ValueError,'container_bytes_disagree'):s.verify_canonical_geopackage(path,rows)
            path.write_bytes(original);data=bytearray(original);data[-1]^=1;path.write_bytes(data)
            with self.assertRaisesRegex(ValueError,'container_bytes_disagree'):s.verify_canonical_geopackage(path,rows)

    def test_rejected_cache_moves_intact_out_of_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix=s.SLUG+'/'+s.SLUG+'/v0.0.0-fixture'
            root=Path(d)/prefix;(root/'geopackage').mkdir(parents=True);(root/'geopackage/corrupted.gpkg').write_bytes(b'corrupt')
            (root/'raw').mkdir();(root/'raw/original.zip').write_bytes(b'preserved original')
            q=s.quarantine_rejected_cache(storage,prefix,'test')
            self.assertFalse(root.exists());destination=Path(d)/q/root.name
            self.assertEqual((destination/'geopackage/corrupted.gpkg').read_bytes(),b'corrupt')
            self.assertEqual((destination/'raw/original.zip').read_bytes(),b'preserved original')

    def test_complete_fidelity_report_sidecars_wrappers_and_blocker_are_bound(self):
        rows=[{'type':'Feature','properties':{'BUFF_DIST':20.0,'Shape_Leng':1.0,'Shape_Area':0.5,'__source_fid':'0'},
               'geometry':{'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]}}]
        records=b''.join(s.encode(r) for r in rows);fingerprint='synthetic-contract-fixture-not-real-source-approval'
        expected=s.fidelity_payloads(records,fingerprint)
        self.assertEqual(len(expected),7)
        report=json.loads(expected['metadata/source_fidelity/baseline_comparison.json'])
        self.assertFalse(report['baseline']['approved']);self.assertEqual(report['populations']['candidate']['rows'],1)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for name,body in expected.items():(root/name).parent.mkdir(parents=True,exist_ok=True);(root/name).write_bytes(body)
            s.validate_fidelity_payloads(root,records,fingerprint)
            for name,body in expected.items():
                with self.subTest(name=name):
                    (root/name).write_bytes(body+b'forged claim')
                    with self.assertRaisesRegex(ValueError,'cached_source_fidelity_evidence'):s.validate_fidelity_payloads(root,records,fingerprint)
                    (root/name).write_bytes(body)
            # Independent fresh replay must derive the entire same report and all sidecars.
            s._FIDELITY_PAYLOAD_CACHE.clear();self.assertEqual(s.fidelity_payloads(records,fingerprint),expected)

    def test_compressed_wire_length_and_incomplete_wire_response(self):
        original_client=s.httpx.Client
        plain=b'complete decoded producer response'*100
        compressed=gzip.compress(plain)
        def response(request):
            self.assertEqual(request.headers['Accept-Encoding'],'identity')
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed))
        def client(**kw):return original_client(transport=httpx.MockTransport(response),**kw)
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=client):
            captures=s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(captures),4)
            for body,receipt in captures.values():
                self.assertEqual(body,plain);self.assertEqual(receipt['received_wire_bytes'],len(compressed))
                self.assertNotEqual(receipt['received_wire_bytes'],receipt['byte_length'])
        def truncated(request):return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed[:-4]))
        def badclient(**kw):return original_client(transport=httpx.MockTransport(truncated),**kw)
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=badclient),self.assertRaisesRegex(ValueError,'incomplete_content_length'):
            try:s.acquire(StagingStorageResource(local_dir=d),None)
            finally:self.assertEqual(len(list(Path(d).rglob('report.json'))),3)

    def test_invalid_lookup_quarantine_and_concurrent_valid_winner_share_lock(self):
        # These are explicitly synthetic implementation fixtures, not dataset approval.
        captures={'official_zip':(b'synthetic-body',{})};identity={'fixture':True}
        fp=s.sha(s.encode(identity));cp=s.sha(s.encode({'official_zip':s.sha(b'synthetic-body')}))
        prefix=s.SLUG+'/'+s.SLUG+'/v0.0.0-draft-cbrs20241219-'+cp[:16]+'-'+fp[:16]
        held=threading.Event();release=threading.Event();started=threading.Event();winner=threading.Event();errors=[]
        original_q=s.quarantine_rejected_cache_held;original_diag=s.quarantine
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);target=Path(d)/prefix
            (target/'geopackage').mkdir(parents=True);(target/'geopackage/incomplete.gpkg').write_bytes(b'old incomplete')
            def pause_q(*args):
                held.set();self.assertTrue(release.wait(10));return original_q(*args)
            def diagnostic(*args):
                self.assertTrue(winner.wait(10));return original_diag(*args)
            def source():
                try:s.stage_cbrs_buffer_candidate(storage)
                except Exception as e:errors.append(str(e))
            def install():
                started.set();s.commit_complete_candidate(storage,prefix,{'geopackage/winner.gpkg':b'valid winner fixture'},{'synthetic':True});winner.set()
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'provenance',return_value={}),patch.object(s,'inspect_archive',return_value=([],{},{})),patch.object(s,'processing_identity',return_value=identity),patch.object(s,'quarantine_rejected_cache_held',side_effect=pause_q),patch.object(s,'quarantine',side_effect=diagnostic):
                a=threading.Thread(target=source);a.start();self.assertTrue(held.wait(10))
                b=threading.Thread(target=install);b.start();self.assertTrue(started.wait(10))
                self.assertFalse(winner.wait(.15));release.set();a.join(10);b.join(10)
                self.assertFalse(a.is_alive());self.assertFalse(b.is_alive())
            self.assertTrue(winner.is_set());self.assertTrue(any('missing_ready' in x for x in errors))
            self.assertEqual((target/'geopackage/winner.gpkg').read_bytes(),b'valid winner fixture')
            moved=list(Path(d,'_quarantine',s.SLUG,'rejected-cache').rglob('incomplete.gpkg'))
            self.assertEqual(len(moved),1);self.assertEqual(moved[0].read_bytes(),b'old incomplete')

if __name__=='__main__':unittest.main()
