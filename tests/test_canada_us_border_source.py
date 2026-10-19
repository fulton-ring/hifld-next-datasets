"""Complete authentic producer release fixtures test draft mechanics, not acceptance."""
import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import fiona
import httpx
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely.geometry import shape

from dagster_hifld.assets.ibc import ibc_boundary_draft_refresh_schedule, ibc_canada_us_border
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
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
