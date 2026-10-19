"""Real complete captures establish draft fidelity; synthetic negatives grant no approval."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.fws_map_panels import fws_cbrs_map_panels, fws_cbrs_map_panels_draft_refresh_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_map_panels as s

CAPTURE=Path(__file__).parents[1]/'docs/dataset-sources/cbrs-map-panels-1/evidence/captures'
def captures(): return {n:((CAPTURE/(n+'.body')).read_bytes(),json.loads((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}

class PanelsTests(unittest.TestCase):
    def test_complete_original(self):
        rows,audit,_=s.inspect_source(captures())
        self.assertEqual(len(rows),574);self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(len({r['properties']['OBJECTID'] for r in rows}),574)
        self.assertEqual(len(s.provenance(captures())['inputs']),8)
        self.assertEqual(s.typed_table(rows,audit).schema.field('Map_Date').type.bit_width,64)

    def test_registry_schedule_and_remote_rejected(self):
        self.assertIn(fws_cbrs_map_panels,hifld_ingest_assets)
        self.assertEqual(fws_cbrs_map_panels_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        for storage in [PublishedStorageResource(local_dir='/tmp/no-write'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

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
            queries=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_geometry_columns SET srs_id=4326", "UPDATE gpkg_contents SET min_x=0", "UPDATE gpkg_spatial_ref_sys SET definition='WRONG' WHERE srs_id=4269", "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=3857 WHERE srs_id=4269", 'UPDATE "'+s.SLUG+'" SET Map_Date=1 WHERE OBJECTID=21']
            for query in queries:
                gp.write_bytes(original)
                with sqlite3.connect(gp) as db:db.execute(query)
                inventory=json.loads(ready);inventory['objects'][key]=s.sha(gp.read_bytes());readyfile.write_bytes(s.encode(inventory))
                with self.subTest(query=query),self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            gp.write_bytes(original);readyfile.write_bytes(ready)
            pqfile=root/'candidate/typed-source.parquet';originalpq=pqfile.read_bytes();table=pq.read_table(pqfile);meta=dict(table.schema.metadata);geo=json.loads(meta[b'geo']);geo['columns']['geometry']['crs']={'type':'GeographicCRS','name':'WRONG'};meta[b'geo']=s.encode(geo);pq.write_table(table.replace_schema_metadata(meta),pqfile)
            inventory=json.loads(ready);inventory['objects']['candidate/typed-source.parquet']=s.sha(pqfile.read_bytes());readyfile.write_bytes(s.encode(inventory))
            with self.assertRaises(ValueError):s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE)
            pqfile.write_bytes(originalpq);readyfile.write_bytes(ready)
            self.assertEqual(result,s.stage_cbrs_map_panels_candidate(store,capture_dir=CAPTURE))

    def test_bad_receipt_original_preserved_no_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw=Path(tmp,'capture');raw.mkdir();body=(CAPTURE/'fws_data.body').read_bytes();bad=b'{bad receipt'
            (raw/'fws_data.body').write_bytes(body);(raw/'fws_data.receipt.json').write_bytes(bad);stage=Path(tmp,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),raw)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())

    def test_live_partial_attempts_retained(self):
        import httpx
        class Response:
            status_code=200;url=s.URLS['fws_data'];headers={}
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

    def test_json_error_duplicate_nonfinite(self):
        for body in [b'[]',b'{"error":{}}',b'{"x":1,"x":2}',b'{"x":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

if __name__=='__main__':unittest.main()
