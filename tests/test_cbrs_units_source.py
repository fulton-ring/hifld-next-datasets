"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
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

if __name__=='__main__':unittest.main()
