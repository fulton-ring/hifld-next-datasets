"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import gzip
import threading
import json
import tempfile
import sqlite3
import struct
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.fws_units import fws_cbrs_units_draft_refresh_schedule, fws_cbrs_units
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import cbrs_units as s

CAPTURE=Path(__file__).parents[1]/'docs/dataset-sources/cbrs-units-2/evidence/captures'

class CBRSTests(unittest.TestCase):
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),942);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['Unit_Type'],{'System Unit':592,'Otherwise Protected Area':350})
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(942)})

    def test_all_original_binary_dbf_values_and_declarations(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        dbf=members[s.STEM+'.dbf'];header,width=struct.unpack_from('<HH',dbf,8)
        fields=[]
        for position in range(32,header-1,32):
            field=dbf[position:position+32]
            fields.append((field[:11].split(b'\x00')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
        self.assertEqual(fields,[(k,'C' if v.startswith('str') else 'F',int(v.split(':')[1].split('.')[0]),0 if v.startswith('str') else 11) for k,v in s.NATIVE_SCHEMA.items()])
        self.assertNotIn('D',[f[1] for f in fields]);self.assertNotIn('OBJECTID',[f[0] for f in fields])
        for index,row in enumerate(rows):
            self.assertEqual(row['properties']['__source_fid'],str(index));offset=header+index*width+1
            for name,kind,length,decimals in fields:
                original=dbf[offset:offset+length];offset+=length
                expected=original.decode('utf-8').rstrip(' ') if kind=='C' else float(original.decode('ascii'))
                self.assertEqual(row['properties'][name],expected,(index,name))
        self.assertEqual(rows[679]['properties']['Name'],' Brigantine')
        self.assertEqual(rows[680]['properties']['Name'],' Island Beach')
        # Test the general decoder, not just the two names found by review.
        changed=bytearray(dbf);changed[header+1:header+9]=b'  AB    '
        self.assertEqual(s.decode_physical_dbf(bytes(changed),b'UTF-8')[0][0]['Unit'],'  AB')
        self.assertEqual(audit['physical_dbf_fields'][1]['width'],254)
        for bad in [dbf[:-100],bytes(changed[:32]),b'']:
            with self.assertRaises(ValueError):s.decode_physical_dbf(bad,b'UTF-8')

    def test_first_construction_bundle_preserves_original_full_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=s.acquire(storage,CAPTURE)
            manifest=s.provenance(captures,prebuilt_body=b'clearly synthetic policy fixture')
            manifest['processing_fingerprint']='fixture'
            version='v0.0.0-draft-original-fixture';files=s.processing_identity()['files']
            with s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                original,bound=s.original_construction_bundle(storage,version,captures,manifest,files,create=True)
                self.assertEqual(bound,manifest);self.assertEqual(original,captures)
                later={name:(body,dict(receipt,observed_at='2026-10-20T12:00:00+00:00')) for name,(body,receipt) in captures.items()}
                with patch.object(s.subprocess,'check_output',return_value='f'*40+'\n'):
                    latest=s.provenance(later,prebuilt_body=b'clearly synthetic policy fixture');latest['processing_fingerprint']='fixture'
                    restored,bound=s.original_construction_bundle(storage,version,later,latest,files)
                self.assertEqual(restored,captures);self.assertEqual(bound,manifest)
                key=f'_source-provenance/{s.SLUG}/{version}/bundle.json';bundle=storage.read_key(key)
                self.assertEqual(json.loads(bundle)['manifest']['code_commit'],manifest['code_commit'])
                Path(d,key).unlink()
                with self.assertRaisesRegex(ValueError,'missing_original_construction'):
                    s.original_construction_bundle(storage,version,later,latest,files)
                self.assertFalse(storage.object_exists(key))

    def test_original_bundle_git_authenticity_rejects_forgery_and_wrong_real_commit(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=s.acquire(storage,CAPTURE)
            manifest=s.provenance(captures,prebuilt_body=b'synthetic-authenticity-fixture')
            manifest['processing_fingerprint']='fixture';files=s.processing_identity()['files']
            version='v0.0.0-draft-code-authenticity-fixture';key=f'_source-provenance/{s.SLUG}/{version}/bundle.json'
            for commit,reason in [('0'*40,'construction_invalid_commit'),('f7669766e2cd9168f06f833b70c64fbbf7747edf','construction_processing_blob_hash_mismatch')]:
                changed=copy.deepcopy(manifest);changed['code_commit']=commit
                with self.subTest(commit=commit),s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                    with self.assertRaisesRegex(ValueError,reason):
                        s.original_construction_bundle(storage,version,captures,changed,files,create=True)
                    self.assertFalse(storage.object_exists(key))
            with s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                s.original_construction_bundle(storage,version,captures,manifest,files,create=True)
                original=storage.read_key(key)
                for value in ['0'*40,'f7669766e2cd9168f06f833b70c64fbbf7747edf']:
                    bundle=json.loads(original);bundle['manifest']['code_commit']=value;Path(d,key).write_bytes(s.encode(bundle))
                    with self.assertRaisesRegex(ValueError,'construction_'):
                        s.original_construction_bundle(storage,version,captures,manifest,files)
                Path(d,key).write_bytes(b'{}')
                with self.assertRaisesRegex(ValueError,'bundle_roster_drift'):
                    s.original_construction_bundle(storage,version,captures,manifest,files)

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_units for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_units_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_units_draft_refresh_schedule.cron_schedule,'0 15 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_units_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_all_receipts_preflight_strict_timestamp_url_pins(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        self.assertEqual(len(s.provenance(captures)['inputs']),4)
        for name in s.URLS:
            for field,value in [('requested_url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19T00:00:00+99:00'),('observed_at',None),('byte_length',True),('sha256','0'*64),('status',True)]:
                changed={n:(body,dict(receipt)) for n,(body,receipt) in captures.items()};changed[name][1][field]=value
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=changed),self.assertRaises((ValueError,TypeError)):
                    try:s.stage_cbrs_units_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
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
                target='cbrs-units-2/cbrs-units-2/v0.0.0-draft-fixture'
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
            if mode=='missing':del mutated[0]['properties']['Shape_Leng']
            if mode=='extra':mutated[0]['properties']['surprise']=1
            if mode=='type':mutated[0]['properties']['Shape_Leng']=20
            if mode=='null':mutated[0]['properties']['Shape_Leng']=None
            if mode=='id':mutated[0].id=mutated[1].id
            if mode=='geometry':mutated[0]['geometry']=None
            if mode=='coverage':mutated[0]['properties']['Unit_Type']='Unreviewed'
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive(body,Path(d))

    def test_atomic_target_does_not_replace_existing_release(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'prior'}, {'status':'fixture'})
            with self.assertRaises(ValueError):s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'new'}, {})
            self.assertEqual(Path(d,prefix,'geopackage/a.gpkg').read_bytes(),b'prior')

    def test_cache_metadata_guards_complete_gpkg_and_typed_parquet(self):
        import fiona
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rows,audit,_=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),root)
            gp=root/'source.gpkg'
            s.write_geopackage(gp,rows)
            s.validate_geopackage_metadata(gp,rows)
            original=gp.read_bytes()
            changes=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_geometry_columns SET srs_id=4326", "UPDATE gpkg_contents SET min_x=0", "UPDATE gpkg_spatial_ref_sys SET definition='undefined' WHERE srs_id=4269", "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269", "UPDATE gpkg_geometry_columns SET z=1", 'UPDATE "rtree_'+s.SLUG+'_geom" SET maxx=0 WHERE id=1', 'ALTER TABLE "'+s.SLUG+'" ADD COLUMN injected TEXT']
            for sql in changes:
                gp.write_bytes(original)
                with sqlite3.connect(gp) as db:db.execute(sql)
                with self.subTest(sql=sql),self.assertRaisesRegex(ValueError,'container_bytes_differ_from_original_reconstruction'):
                    s.validate_geopackage_metadata(gp,rows)
            gp.write_bytes(original)
            with sqlite3.connect(gp) as db:
                db.create_function('ST_IsEmpty',1,lambda x:False);db.create_function('ST_MinX',1,lambda x:0);db.create_function('ST_MaxX',1,lambda x:0);db.create_function('ST_MinY',1,lambda x:0);db.create_function('ST_MaxY',1,lambda x:0)
                fid,blob=db.execute('SELECT fid,geom FROM "'+s.SLUG+'" LIMIT 1').fetchone()
                changed=blob[:4]+struct.pack('<i',4326)+blob[8:]
                db.execute('UPDATE "'+s.SLUG+'" SET geom=? WHERE fid=?',(changed,fid))
            with self.assertRaisesRegex(ValueError,'container_bytes_differ_from_original_reconstruction'):
                s.validate_geopackage_metadata(gp,rows)
            table=s.typed_table(rows,audit)
            self.assertEqual(str(table.schema.field('Shore_Mile').type),'double')
            self.assertEqual(table.column('Unit').null_count,0)


    def test_full_source_reports_sidecars_and_unapproved_claims_are_rederived(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rows,_,_=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),root)
            records=b''.join(s.encode(row) for row in rows)
            expected=s.source_verification_payloads(records)
            for name,body in expected.items():
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
            s.validate_source_verification_payloads(root,records)
            mutations=[('metadata/geopackage_source_fidelity.json',('shared_verifier','populations','baseline','rows'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','populations','candidate','rows'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','retained_rows_compared'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','baseline','approved'),True),
                       ('metadata/geopackage_source_fidelity.json',('canonical_full_records_byte_identical',),False),
                       ('metadata/parquet_source_fidelity.json',('shared_verifier','rows'),1),
                       ('metadata/baseline_comparison.json',('approved',),True),
                       ('metadata/baseline_comparison.json',('status',),'pass'),
                       ('metadata/baseline_comparison.json',('acceptance',),'approved')]
            for name,keys,value in mutations:
                changed=json.loads(expected[name]);current=changed
                for key in keys[:-1]:current=current[key]
                current[keys[-1]]=value;(root/name).write_bytes(s.encode(changed))
                with self.subTest(name=name,keys=keys),self.assertRaisesRegex(ValueError,'evidence_or_unapproved_baseline_claim_drift'):
                    s.validate_source_verification_payloads(root,records)
                (root/name).write_bytes(expected[name])
            for name in ['metadata/baseline_field_changes.jsonl','metadata/baseline_differences.jsonl']:
                (root/name).write_bytes(expected[name]+b'altered')
                with self.subTest(sidecar=name),self.assertRaisesRegex(ValueError,'evidence_or_unapproved_baseline_claim_drift'):
                    s.validate_source_verification_payloads(root,records)
                (root/name).write_bytes(expected[name])

    def test_missing_ready_existing_version_is_detached_with_all_bytes_preserved(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        identity={'synthetic_test_identity':'missing-ready-regression'}
        fingerprint=s.sha(s.encode(identity));capture=s.sha(s.encode({n:s.sha(v[0]) for n,v in captures.items()}))
        version=f'v0.0.0-draft-cbrs20241219-{capture[:16]}-{fingerprint[:16]}'
        prefix=f'{s.SLUG}/{s.SLUG}/{version}'
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);target=Path(d)/prefix
            (target/'geopackage').mkdir(parents=True);(target/'geopackage/incomplete.gpkg').write_bytes(b'incomplete synthetic object')
            (target/'raw').mkdir();(target/'raw/kept.bin').write_bytes(b'preserve all original remaining bytes')
            before=s.candidate_inventory(target)
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'processing_identity',return_value=identity),self.assertRaisesRegex(ValueError,'incomplete_existing_candidate_missing_ready'):
                s.stage_cbrs_units_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(target.exists())
            reports=list(Path(d,'_quarantine',s.SLUG,'versions').glob('*/report.json'));self.assertEqual(len(reports),1)
            self.assertEqual(s.candidate_inventory(reports[0].parent/'candidate'),before)
            self.assertEqual(json.loads(reports[0].read_bytes())['inventory'],before)


    def test_live_transport_counts_wire_bytes_when_server_ignores_identity(self):
        payload=b'complete original decoded bytes '*100
        compressed=gzip.compress(payload)
        seen=[]
        def handler(request):
            self.assertEqual(request.headers['Accept-Encoding'],'identity');seen.append(str(request.url))
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed))
        original_client=httpx.Client
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:original_client(transport=httpx.MockTransport(handler),**kw)):
            captures=s.acquire(StagingStorageResource(local_dir=d),None)
        self.assertEqual(set(seen),set(s.URLS.values()))
        for body,receipt in captures.values():
            self.assertEqual(body,payload);self.assertEqual(receipt['wire_bytes_downloaded'],len(compressed))
            self.assertEqual(receipt['byte_length'],len(payload));self.assertEqual(receipt['content_encoding'],'gzip')

    def test_real_httpx_partial_chunks_retained_without_decode_buffer_loss(self):
        payload=b'every decoded byte received before reset'
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):
                yield payload
                raise httpx.ReadError('synthetic interrupted response')
        def handler(request):return httpx.Response(200,headers={'Content-Length':str(len(payload)+1)},stream=Interrupted())
        original_client=httpx.Client
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:original_client(transport=httpx.MockTransport(handler),**kw)):
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):
                s.acquire(StagingStorageResource(local_dir=d),None)
            responses=list(Path(d).rglob('response.body'));self.assertEqual(len(responses),3)
            self.assertTrue(all(p.read_bytes()==payload for p in responses))

    def test_same_version_lock_preserves_winner_after_invalid_cache_detach(self):
        # Real stage control flow and real concurrent install, with clearly
        # synthetic rows/identity; no source or dataset acceptance is implied.
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        identity={'synthetic_test_identity':'deterministic-interleaving'}
        fingerprint=s.sha(s.encode(identity));capture=s.sha(s.encode({n:s.sha(v[0]) for n,v in captures.items()}))
        version=f'v0.0.0-draft-cbrs20241219-{capture[:16]}-{fingerprint[:16]}';prefix=f'{s.SLUG}/{s.SLUG}/{version}'
        at_detach=threading.Event();allow_detach=threading.Event();winner_started=threading.Event();winner_done=threading.Event();failures=[]
        original_detach=s._quarantine_existing_candidate_locked
        def paused_detach(*args):
            at_detach.set()
            if not allow_detach.wait(10):raise RuntimeError('test synchronization timeout')
            return original_detach(*args)
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);target=Path(d)/prefix
            (target/'geopackage').mkdir(parents=True);(target/'geopackage/old.gpkg').write_bytes(b'incomplete synthetic prior')
            old=s.candidate_inventory(target)
            def stale_refresh():
                try:s.stage_cbrs_units_candidate(storage,capture_dir=CAPTURE)
                except Exception as exc:failures.append(exc)
            def valid_winner():
                winner_started.set()
                try:s.commit_complete_candidate(storage,prefix,{'geopackage/winner.gpkg':b'valid synthetic winning object'}, {'status':'synthetic_not_approved'})
                except Exception as exc:failures.append(exc)
                finally:winner_done.set()
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'processing_identity',return_value=identity),patch.object(s,'inspect_archive',return_value=([],{},{})),patch.object(s,'derive_native_parquet_bytes',return_value=b'synthetic-control-flow-only'),patch.object(s,'_quarantine_existing_candidate_locked',side_effect=paused_detach):
                a=threading.Thread(target=stale_refresh);a.start();self.assertTrue(at_detach.wait(10))
                b=threading.Thread(target=valid_winner);b.start();self.assertTrue(winner_started.wait(10))
                self.assertFalse(winner_done.wait(0.1));self.assertTrue((target/'geopackage/old.gpkg').exists())
                allow_detach.set();a.join(10);b.join(10)
                self.assertFalse(a.is_alive() or b.is_alive())
            self.assertEqual(len(failures),1);self.assertIn('incomplete_existing_candidate_missing_ready',str(failures[0]))
            self.assertEqual((target/'geopackage/winner.gpkg').read_bytes(),b'valid synthetic winning object')
            self.assertEqual(json.loads((target/'metadata/cbrs_units_candidate_ready.json').read_bytes())['status'],'synthetic_not_approved')
            retained=list(Path(d,'_quarantine',s.SLUG,'versions').glob('*/candidate'))
            self.assertEqual(len(retained),1);self.assertEqual(s.candidate_inventory(retained[0]),old)


    def test_native_prebuilt_capability_is_closed_and_pins_corrected_source_text(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        with tempfile.TemporaryDirectory() as d:
            rows,audit,_=s.inspect_archive(captures['official_zip'][0],Path(d))
            body=s.derive_native_parquet_bytes(rows,audit)
        self.assertEqual(len(body),6555610)
        self.assertEqual(s.sha(body),'64f1bb9b98ff27b76bcfbeb8f6dc049815c0a828f022c87f261b1f797707520a')
        manifest=s.provenance(captures,prebuilt_body=body)
        self.assertNotIn('geoparquet',manifest['unsupported_output_formats'])
        self.assertEqual(set(manifest['unsupported_output_formats']),{'shapefile_zip','pmtiles'})
        self.assertEqual(manifest['prebuilt_output_policy']['schema_version'],1)
        self.assertEqual(manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'],
            [{'path':'geoparquet/cbrs-units-2.parquet','sha256':s.sha(body),'bytes':len(body)}])
        self.assertFalse(manifest['approved'])
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='test/test/v0.0.0-native'
            storage.write_key(prefix+'/geoparquet/cbrs-units-2.parquet',body)
            self.assertEqual(s.prebuilt_output_keys(manifest,storage,prefix,'geoparquet'),[prefix+'/geoparquet/cbrs-units-2.parquet'])
            storage.write_key(prefix+'/geoparquet/extra.parquet',b'unapproved shard')
            with self.assertRaisesRegex(ValueError,'roster'):s.prebuilt_output_keys(manifest,storage,prefix,'geoparquet')

    def test_invalid_native_capability_fails_before_canonical_namespace(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        bad={'schema_version':True,'formats':{'geoparquet':{'reason':'invalid bool version','objects':[]}}}
        with tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=captures),patch.object(s,'native_prebuilt_policy',return_value=bad):
            with self.assertRaisesRegex(ValueError,'prebuilt_output_policy'):
                s.stage_cbrs_units_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
            self.assertFalse(Path(d,s.SLUG).exists())

if __name__=='__main__':unittest.main()
