"""Complete retained original inputs test draft mechanics; no real acceptance."""
import copy
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld.sources import consolidated_cities as c
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.assets.census_consolidated_cities import concity_source_integrity_schedule
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS

INPUTS=Path(__file__).parents[1]/'docs/dataset-sources/consolidated-cities/evidence/inputs'

def captures():
    return {n:((INPUTS/(n+'.body')).read_bytes(),c.strict_json((INPUTS/(n+'.receipt.json')).read_bytes())) for n in ['census2026_concity',*c.PINS]}

class ConsolidatedCityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.capture=captures()
        cls.table,cls.audit=c.inspect_archives({n:INPUTS/(n+'.body') for n in c.PINS},cls.capture)

    def test_registration_and_stopped_schedule(self):
        self.assertIn((c.SLUG,c.SLUG),SUPPORTED_DATASET_FILE_PAIRS)
        self.assertEqual(concity_source_integrity_schedule.default_status.value,'STOPPED')

    def test_complete_original_rows_types_identifiers_and_dbf_declarations(self):
        self.assertEqual(self.table.num_rows,8)
        self.assertEqual(sorted(self.table['GEOID'].to_pylist()),list(c.GEOIDS))
        self.assertEqual(self.table['OGC_FID'].to_pylist(),[0,0,1,0,0,0,0,0])
        self.assertEqual(tuple(f.type for f in self.table.schema),c.TYPES)
        self.assertEqual(len(self.audit),7)
        for audit in self.audit.values():self.assertEqual(audit['dbf']['fields'],c.DBF_FIELDS)
        for changed in [self.table.slice(0,7),self.table.drop(['GEOID']),self.table.append_column('extra',pa.array([1]*8)),self.table.set_column(13,'ALAND',pa.array([1.0]*8)),self.table.set_column(5,'GEOID',pa.array(['0000000']*8)),self.table.set_column(17,'wkb_geometry',pa.array([None]*8,type=pa.binary()))]:
            with self.assertRaises(ValueError):c.validate_table(changed)

    def test_refuses_published_storage_or_cloud_before_acquisition(self):
        for storage in [PublishedStorageResource(local_dir='/tmp/never-concity-output'),StagingStorageResource(bucket='never')]:
            with patch.object(c,'acquire') as get,self.assertRaises(ValueError):c.stage_consolidated_cities(storage)
            get.assert_not_called()

    def test_strict_json_and_provenance_preflight_precede_canonical_writes(self):
        for body in [b'[]',b'null',b'{"error":{}}',b'{"x":1,"x":2}']:
            with self.assertRaises(ValueError):c.strict_json(body)
        for mutation in [{'observed_at':'not-a-date'},{'observed_at':'2026-10-19T00:00:00+00:99'},{'observed_at':'2026-10-19T00:00:00'},{'observed_at':True},{'url':'https://example.invalid/x'},{'final_url':'https://example.invalid/x'},{'status':True},{'status':403},{'bytes':True},{'sha256':'0'*64},{'complete_body':False}]:
            bad=copy.deepcopy(self.capture);bad['tl_2026_09_concity'][1].update(mutation)
            with tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d);prior=Path(d)/'prior';prior.write_bytes(b'keep')
                with patch.object(c,'acquire',return_value=bad),patch.object(c,'processing_identity',return_value={'test':True}),self.assertRaises(ValueError):c.stage_consolidated_cities(storage)
                self.assertFalse((Path(d)/c.SLUG).exists());self.assertEqual(prior.read_bytes(),b'keep')
                self.assertTrue(list((Path(d)/'_quarantine').rglob('reason.json')))

    def test_changed_archive_or_listing_quarantines_all_without_candidate(self):
        for target in ['tl_2026_09_concity','census2026_concity']:
            bad=copy.deepcopy(self.capture);body,receipt=bad[target]
            body=body[:-1] if target!='census2026_concity' else body.replace(b'tl_2026_09_concity.zip',b'tl_2026_99_concity.zip')
            receipt.update(bytes=len(body),sha256=c.digest(body));bad[target]=(body,receipt)
            with tempfile.TemporaryDirectory() as d:
                with patch.object(c,'acquire',return_value=bad),patch.object(c,'processing_identity',return_value={'test':True}),self.assertRaises(ValueError):c.stage_consolidated_cities(StagingStorageResource(local_dir=d))
                self.assertFalse((Path(d)/c.SLUG).exists())
                self.assertTrue(any(p.read_bytes()==body for p in (Path(d)/'_quarantine').rglob(target+'.body')))

    def test_atomic_failure_and_keyboard_interrupt_never_expose_partial_candidate(self):
        for failure in [OSError('injected'),KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value={'test':True}):
                with patch.object(c.pq,'write_table',side_effect=failure),self.assertRaises(KeyboardInterrupt if isinstance(failure,KeyboardInterrupt) else ValueError):
                    c.stage_consolidated_cities(StagingStorageResource(local_dir=d),capture_dir=INPUTS)
                self.assertFalse((Path(d)/c.SLUG).exists())
                self.assertFalse(list((Path(d)/'_building').rglob('*.gpkg')))
                self.assertEqual(len(list((Path(d)/'_raw-captures').rglob('*.body'))),8)

    def test_idempotency_and_missing_ready_cache_is_moved_intact(self):
        with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value={'test':True}):
            storage=StagingStorageResource(local_dir=d);a=c.stage_consolidated_cities(storage,capture_dir=INPUTS);b=c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            self.assertFalse(a['reused']);self.assertTrue(b['reused'])
            root=Path(d)/c.SLUG/c.SLUG/a['version'];(root/c.READY).unlink();before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with self.assertRaisesRegex(ValueError,'invalid_cache_moved_intact'):c.stage_consolidated_cities(storage,capture_dir=INPUTS)
            self.assertFalse(root.exists());q=next((Path(d)/'_quarantine'/c.SLUG).glob('invalid-candidate-*'))
            self.assertEqual(before,{str(p.relative_to(q)):p.read_bytes() for p in q.rglob('*') if p.is_file()})

    def test_repinned_cache_reports_rows_schema_ids_and_geometry_never_authorize_truth(self):
        with tempfile.TemporaryDirectory() as d,patch.object(c,'processing_identity',return_value={'test':True}):
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

if __name__=='__main__':unittest.main()
