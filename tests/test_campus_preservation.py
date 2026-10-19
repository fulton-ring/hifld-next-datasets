"""Complete real historical inputs exercise draft mechanics, never acceptance."""
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

from dagster import DefaultScheduleStatus
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import campus_preservation as s
from dagster_hifld.assets.campus_preservation import ornl_campus_historical_integrity_schedule


class CampusPreservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='campus-real-healthy-')
        cls.base = Path(cls.temporary.name)/'staging'
        cls.storage = StagingStorageResource(local_dir=str(cls.base))
        cls.output = s.stage_campus_preservation(cls.storage,s.CAPTURES)
        cls.prefix = s.SLUG+'/'+s.SLUG+'/'+cls.output['version']

    @classmethod
    def tearDownClass(cls): cls.temporary.cleanup()

    def clone(self, root):
        target = Path(root)/'staging'; shutil.copytree(self.base,target)
        return StagingStorageResource(local_dir=str(target)),target/ self.prefix

    def test_all_real_native_rows_types_geometry_and_historical_scope(self):
        self.assertEqual(self.output['feature_count'],5898)
        self.assertEqual(self.output['source_invalid_geometry_count'],15)
        self.assertFalse(self.output['source_selection_approved'])
        path = self.base/self.prefix
        native = pq.read_table(path/'geoparquet'/f'{s.SLUG}.parquet')
        other = pq.read_table(path/'candidate/native_wgs84.parquet')
        self.assertEqual((len(native),len(other)),(5898,5898))
        self.assertEqual(native.schema.field('SOURCEDATE').type,pa.date32())
        self.assertEqual(native.schema.field('POPULATION').type,pa.int64())
        self.assertEqual(other.schema.field('POPULATION').type,pa.int32())
        self.assertIn('OBJECTID',other.column_names)
        self.assertEqual(len(set(native['UNIQUEID'].to_pylist())),5898)
        self.assertEqual(len(set(native['__source_fid'].to_pylist())),5898)
        report=s.strict_json((path/'metadata/baseline_comparison.json').read_bytes())
        self.assertFalse(report['approved']);self.assertEqual(report['status'],'blocked')
        self.assertEqual(len(report['all_rows']),5898)
        self.assertEqual(report['published_duplicate_ids'],s.BASELINE_DUPLICATE_IDS)
        self.assertEqual(ornl_campus_historical_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)

    def test_publication_and_alias_roots_rejected_before_source_reads(self):
        with tempfile.TemporaryDirectory() as root,patch.object(Path,'read_bytes',side_effect=AssertionError('source read forbidden')):
            with self.assertRaisesRegex(ValueError,'explicit_unprefixed_local_staging_required'):
                s.stage_campus_preservation(PublishedStorageResource(local_dir=root),s.CAPTURES)
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);(root/'actual').mkdir();(root/'alias').symlink_to(root/'actual',target_is_directory=True)
            for path in [root/'alias',root/'alias'/'child']:
                with self.subTest(path=path),patch.object(Path,'read_bytes',side_effect=AssertionError('source read forbidden')),self.assertRaises(OSError):
                    s.stage_campus_preservation(StagingStorageResource(local_dir=str(path)),s.CAPTURES)
            self.assertEqual(list((root/'actual').iterdir()),[])

    def test_bad_receipt_keeps_exact_untrusted_bytes_and_no_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);captures=root/'captures';shutil.copytree(s.CAPTURES,captures)
            bad=b'{"status":200,"status":403,"literal":"actual bad bytes"}'
            (captures/'pasda-original-zip.receipt.json').write_bytes(bad)
            storage=StagingStorageResource(local_dir=str(root/'staging'))
            with self.assertRaisesRegex(ValueError,'duplicate_json_member'):s.acquire(storage,captures)
            files=list((root/'staging/_quarantine').rglob('pasda-original-zip.receipt.json'))
            self.assertEqual(len(files),1);self.assertEqual(files[0].read_bytes(),bad)
            self.assertFalse((root/'staging'/s.SLUG).exists())

    def test_changed_complete_zip_quarantined_before_decode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);captures=root/'captures';shutil.copytree(s.CAPTURES,captures)
            path=captures/'pasda-original-zip.body';bad=path.read_bytes()+b'changed archive';path.write_bytes(bad)
            receipt=s.strict_json((captures/'pasda-original-zip.receipt.json').read_bytes());receipt.update(bytes=len(bad),sha256=s.digest(bad))
            (captures/'pasda-original-zip.receipt.json').write_bytes(s.encode(receipt))
            storage=StagingStorageResource(local_dir=str(root/'staging'))
            with patch.object(s,'decode_archive',side_effect=AssertionError('decode forbidden')),self.assertRaisesRegex(ValueError,'changed_or_incomplete_pinned_input'):
                s.stage_campus_preservation(storage,captures)
            self.assertTrue(any(p.read_bytes()==bad for p in (root/'staging/_quarantine').rglob('*.body')))

    def test_full_container_auxiliary_crs_self_repin_does_not_authorize_cache(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage,path=self.clone(temporary);gp=path/'geopackage'/f'{s.SLUG}.gpkg'
            with sqlite3.connect(gp) as db:db.execute('UPDATE gpkg_contents SET srs_id=4326')
            ready=s.strict_json((path/s.READY).read_bytes());ready['objects']['geopackage/'+s.SLUG+'.gpkg']={'sha256':s.sha256_file(gp),'bytes':gp.stat().st_size};(path/s.READY).write_bytes(s.encode(ready))
            bad=gp.read_bytes()
            with self.assertRaisesRegex(ValueError,'fresh_native_complete_GPKG_container_disagrees'):s.stage_campus_preservation(storage,s.CAPTURES)
            self.assertFalse(path.exists())
            rejected=list(Path(storage.local_dir).glob('_quarantine/'+s.SLUG+'/invalid-candidate-*'))
            self.assertEqual(len(rejected),1);self.assertEqual((rejected[0]/'geopackage'/gp.name).read_bytes(),bad)

    def test_full_owned_report_self_repin_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage,path=self.clone(temporary);name='metadata/baseline_comparison.json';report=s.strict_json((path/name).read_bytes())
            report.update(approved=True,status='pass',acceptance='approved');(path/name).write_bytes(s.encode(report))
            ready=s.strict_json((path/s.READY).read_bytes());ready['objects'][name]={'sha256':s.sha256_file(path/name),'bytes':(path/name).stat().st_size};(path/s.READY).write_bytes(s.encode(ready))
            with self.assertRaisesRegex(ValueError,'source_owned_full_bytes_drift'):s.stage_campus_preservation(storage,s.CAPTURES)
            self.assertFalse(path.exists())

    def test_missing_ready_preserves_whole_old_candidate_outside_discovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage,path=self.clone(temporary);(path/s.READY).unlink();gp=(path/'geopackage'/f'{s.SLUG}.gpkg').read_bytes()
            with self.assertRaisesRegex(ValueError,'invalid_candidate_preserved_outside_discovery'):s.stage_campus_preservation(storage,s.CAPTURES)
            self.assertFalse(path.exists())
            rejected=next(Path(storage.local_dir).glob('_quarantine/'+s.SLUG+'/invalid-candidate-*'))
            self.assertEqual((rejected/'geopackage'/f'{s.SLUG}.gpkg').read_bytes(),gp)

    def test_first_private_native_failure_has_no_construction_anchor_or_discovery(self):
        for failure in [OSError('actual native write failure'),KeyboardInterrupt('actual interruption')]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as temporary:
                storage=StagingStorageResource(local_dir=temporary)
                prior=Path(temporary)/s.SLUG/s.SLUG/'v9.9.9/geopackage/accepted-fixture.gpkg';prior.parent.mkdir(parents=True);prior.write_bytes(b'isolated accepted fixture, not real acceptance')
                with patch.object(s,'write_geopackage',side_effect=failure),self.assertRaises(type(failure)):
                    s.stage_campus_preservation(storage,s.CAPTURES)
                self.assertFalse((Path(temporary)/'_construction-state').exists())
                self.assertFalse(list((Path(temporary)/s.SLUG/s.SLUG).glob('v0*')))
                self.assertEqual(prior.read_bytes(),b'isolated accepted fixture, not real acceptance')
                self.assertTrue(list((Path(temporary)/'_raw-captures').rglob('*.body')))
                self.assertTrue(list((Path(temporary)/'_quarantine').glob(s.SLUG+'/failed-private-*')))

    def test_missing_original_alias_blocks_two_retries_then_exact_restore(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage,path=self.clone(temporary);state=Path(storage.local_dir)/'_construction-state'/s.SLUG/(self.output['version']+'.json')
            original=state.read_bytes();state.unlink()
            for retry in [1,2]:
                with self.subTest(retry=retry),self.assertRaisesRegex(ValueError,'construction_missing_original_state'):
                    s.stage_campus_preservation(storage,s.CAPTURES)
                self.assertFalse(state.exists());self.assertFalse(path.exists())
            state.write_bytes(original)
            restored=s.stage_campus_preservation(storage,s.CAPTURES)
            self.assertFalse(restored['reused']);self.assertEqual(state.read_bytes(),original)
            self.assertEqual((path/'geoparquet'/f'{s.SLUG}.parquet').read_bytes(),(self.base/self.prefix/'geoparquet'/f'{s.SLUG}.parquet').read_bytes())

    def test_coforged_nonexistent_construction_commit_rejected_by_real_git(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage,path=self.clone(temporary);state=Path(storage.local_dir)/'_construction-state'/s.SLUG/(self.output['version']+'.json')
            value=json.loads(state.read_bytes());value['payload']['manifest']['code_commit']='0'*40
            body=json.dumps(value,sort_keys=True,separators=(',',':')).encode();state.write_bytes(body)
            evidence=str(state.relative_to(Path(storage.local_dir)))+'.evidence/'+s.digest(body)+'.json';(Path(storage.local_dir)/evidence).write_bytes(body)
            marker={'schema_version':1,'version':self.output['version'],'processing_fingerprint':value['processing_fingerprint'],'original_bundle_sha256':s.digest(body),'original_bundle_evidence_key':evidence}
            state.with_name(state.name+'.first-construction.json').write_bytes(json.dumps(marker,sort_keys=True,separators=(',',':')).encode())
            with self.assertRaisesRegex(ValueError,'construction_'):s.stage_campus_preservation(storage,s.CAPTURES)
            self.assertFalse(path.exists())


if __name__=='__main__':unittest.main()
