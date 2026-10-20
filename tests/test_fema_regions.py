"""Small structural/negative fixtures exercise mechanics, never real acceptance.

Complete real polygon coverage is opt-in via HIFLD_FEMA_REGIONS_TEST_CAPTURE_DIR;
actual Dagster verification uses the preserved complete original captures.
"""
import copy
import json
import os
import sqlite3
import io
from types import SimpleNamespace
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import shapely
from dagster import Definitions,DefaultScheduleStatus,build_schedule_context,validate_run_config
from dagster_hifld.resources import PublishedStorageResource,StagingStorageResource
from dagster_hifld.sources import fema_regions as s
from dagster_hifld.assets.fema_regions import (fema_regions_official,fema_regions_full_quality,
    fema_regions_draft_job,fema_regions_draft_schedule)

FIXTURES=Path(__file__).parent/'fixtures/fema_regions'


def fixture_objects():
    layer=s.strict_json((FIXTURES/'official_layer.json').read_bytes())
    attrs=s.strict_json((FIXTURES/'source_attributes.json').read_bytes())
    count=s.strict_json((FIXTURES/'source_count.json').read_bytes())
    ids=s.strict_json((FIXTURES/'source_ids.json').read_bytes())
    data={'official-layer':layer,'official-layer-after':copy.deepcopy(layer),
        'source-attributes':attrs,'source-attributes-after':copy.deepcopy(attrs),
        'source-count':count,'source-count-after':copy.deepcopy(count),
        'source-ids':ids,'source-ids-after':copy.deepcopy(ids)}
    # Explicitly SYNTHETIC geometry, NOT the real FEMA boundary or approval.
    data['source-oid-11']={'fields':s.QUERY_FIELDS,'spatialReference':{'wkid':4326},
        'features':[{'attributes':s.SOURCE_ATTRIBUTES[11],
        'geometry':{'rings':[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]}}]}
    return data


def structural_table():
    geometry=shapely.to_wkb(shapely.box(0,0,1,1))
    rows=[{**attrs,'geometry':geometry}for attrs in s.SOURCE_ATTRIBUTES.values()]
    return s.native_table(rows,['Polygon'])


class FemaRegionsStructuralTests(unittest.TestCase):
    def test_complete_schema_and_census_before_after(self):
        values=fixture_objects()
        with patch.object(s,'body_for',side_effect=lambda n,c:s.encode(values[n])):
            result=s.validate_snapshot({})
        self.assertEqual(result['objectIdField'],'objectid');self.assertEqual(len(result['fields']),6)

    def test_missing_duplicate_or_changed_ids_and_schema_fail(self):
        cases=[]
        for name,key,value in [('source-ids','objectIds',[11]*10),('source-count-after','count',9),
            ('official-layer-after','fields',s.FIELDS[:-1]),('official-layer','hasZ',True)]:
            data=fixture_objects();data[name][key]=value;cases.append(data)
        for data in cases:
            with self.subTest(changed=data),patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaises(ValueError):
                s.validate_snapshot({})

    def test_schema_false_is_not_integer_zero_or_float_zero(self):
        for scalar in [0,0.0]:
            data=fixture_objects()
            data['official-layer']['fields'][0]['nullable']=scalar
            data['official-layer-after']['fields'][0]['nullable']=scalar
            with self.subTest(scalar=scalar),patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaisesRegex(ValueError,'official_layer_schema_or_observed_snapshot_drift'):
                s.validate_snapshot({})
        data=fixture_objects();data['source-oid-11']['features'][0]['attributes']=dict(s.SOURCE_ATTRIBUTES[11],cnt_region=True)
        with patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaisesRegex(ValueError,'source_attribute_or_geometry_encoding_drift'):
            s.decode_native({})

    def test_incomplete_query_and_unapproved_coordinate_change_fail(self):
        for field,value in [('features',[]),('exceededTransferLimit',True),('fields',s.QUERY_FIELDS[:-1])]:
            data=fixture_objects();data['source-oid-11'][field]=value
            with self.subTest(field=field),patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaisesRegex(ValueError,'incomplete_native_per_identifier_query'):
                s.decode_native({})
        data=fixture_objects()
        with patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaisesRegex(ValueError,'native_geometry_or_coverage_drift'):
            s.decode_native({}) # Synthetic rectangle cannot be accepted as real boundary.

    def test_curve_or_extra_dimension_not_silently_densified_or_dropped(self):
        for geometry in [{'curveRings':[]},{'rings':[[[0,0,4],[1,0,4],[0,0,4]]]}]:
            data=fixture_objects();data['source-oid-11']['features'][0]['geometry']=geometry
            with self.subTest(geometry=geometry),patch.object(s,'body_for',side_effect=lambda n,c:s.encode(data[n])),self.assertRaises(ValueError):
                s.decode_native({})

    def test_publication_or_aliased_root_rejected_before_capture_or_network(self):
        with tempfile.TemporaryDirectory()as temporary,patch.object(s,'acquire',side_effect=AssertionError('capture forbidden')),patch.object(s.requests,'get',side_effect=AssertionError('network forbidden')):
            with self.assertRaisesRegex(ValueError,'explicit_unprefixed_local_staging_required'):
                s.stage_fema_regions(PublishedStorageResource(local_dir=temporary),'not-read',True)
            root=Path(temporary);(root/'actual').mkdir();(root/'alias').symlink_to(root/'actual',target_is_directory=True)
            with self.assertRaises(OSError):s.stage_fema_regions(StagingStorageResource(local_dir=str(root/'alias'/'child')),'not-read',True)
            self.assertEqual(list((root/'actual').iterdir()),[])

    def test_oversize_regular_file_rejected_before_descriptor_read(self):
        with tempfile.TemporaryDirectory()as temporary:
            path=Path(temporary)/'oversized.json'
            with path.open('wb')as stream:stream.truncate(128_001)
            with patch.object(s.os,'open',side_effect=AssertionError('must reject size before open')),self.assertRaisesRegex(ValueError,'source_capture_size_limit'):
                s.bounded_regular_read(path,128_000)
            self.assertEqual(path.stat().st_size,128_001)

    def test_every_consumed_oversize_chunk_preserved_before_rejection(self):
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);path=Path(temporary)/'failed.body'
            response=SimpleNamespace(raw=io.BytesIO(b'x'*37))
            with self.assertRaisesRegex(ValueError,'live_response_size_drift_incomplete_prefix_preserved'):
                s.persist_response_stream(storage,response,path,36)
            self.assertEqual(response.raw.tell(),37);self.assertEqual(path.read_bytes(),b'x'*37)

    def test_floor_checked_before_next_read_without_losing_consumed_chunk(self):
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);path=Path(temporary)/'failed.body'
            class Chunks:
                reads=0
                def read(self,size):
                    self.reads+=1;return b'already consumed chunk'
            response=SimpleNamespace(raw=Chunks());real_root=s.local_root;checks=0
            def floor(resource):
                nonlocal checks
                checks+=1
                if checks==3:raise ValueError('synthetic_storage_floor_before_next_read')
                return real_root(resource)
            with patch.object(s,'local_root',side_effect=floor),self.assertRaisesRegex(ValueError,'synthetic_storage_floor_before_next_read'):
                s.persist_response_stream(storage,response,path,100)
            self.assertEqual(response.raw.reads,1);self.assertEqual(path.read_bytes(),b'already consumed chunk')
            response.raw.reads=0
            with patch.object(s,'local_root',side_effect=ValueError('synthetic_storage_floor')),self.assertRaises(ValueError):
                s.persist_response_stream(storage,response,Path(temporary)/'never-read.body',100)
            self.assertEqual(response.raw.reads,0)

    def test_receipts_strict_times_status_types_and_body_pins(self):
        body=(FIXTURES/'official_gis_root.json').read_bytes();receipt=s.strict_json((FIXTURES/'official_gis_root.receipt.json').read_bytes())
        s.validate_capture('official-gis-root',body,receipt)
        for key,value in [('status',200.0),('bytes',True),('started_at','2026-10-19T12:10:00+00:90'),('observed_at','2026-10-19T12:99:00+00:00')]:
            bad=copy.deepcopy(receipt);bad[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):s.validate_capture('official-gis-root',body,bad)
        with self.assertRaises(ValueError):s.validate_capture('official-gis-root',body+b'x',receipt)

    def test_actual_bad_json_bytes_diagnostic_has_untrusted_inventory(self):
        raw=b'{"status":200,"status":403,"actual":"bad receipt bytes"}'
        with self.assertRaisesRegex(ValueError,'duplicate_json_member'):s.strict_json(raw)
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);where=Path(s.retain_diagnostic(storage,{'bad.receipt.json':raw},'duplicate_json_member'))
            self.assertEqual((where/'bad.receipt.json').read_bytes(),raw)
            claim=s.strict_json((where/'incident.json').read_bytes());self.assertFalse(claim['approved'])
            self.assertEqual(claim['objects']['bad.receipt.json']['trust'],'untrusted_diagnostic')

    def test_relative_root_and_partial_observed_loss_journal_fail_closed(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd())as temporary:
            storage=StagingStorageResource(local_dir=os.path.relpath(Path(temporary)/'stage',Path.cwd()))
            candidate=Path(temporary)/'synthetic-candidate';(candidate/'metadata').mkdir(parents=True)
            (candidate/'metadata/source_manifest.json').write_bytes(b'{"synthetic":true}')
            with patch.object(s,'immutable',side_effect=OSError('actual journal body write failure')),self.assertRaises(OSError):
                s.observed_trust_loss(storage,'v0.0.0-synthetic','0'*64,candidate=candidate)
            for retry in [1,2]:
                with self.subTest(retry=retry),self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):
                    s.observed_trust_loss(storage,'v0.0.0-synthetic','0'*64)
            self.assertTrue(candidate.exists())

    def test_native_structural_widths_and_all_auxiliary_GPKG_bytes_checked(self):
        table=structural_table();self.assertEqual(table.schema.field('objectid').type,pa.int32())
        self.assertEqual(table.schema.field('cnt_region').type,pa.int32());self.assertIn('Shape__Length',table.column_names)
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary);(root/'geoparquet').mkdir();(root/'geopackage').mkdir()
            (root/'geoparquet'/f'{s.SLUG}.parquet').write_bytes(s.parquet_bytes(table))
            gp=root/'geopackage'/f'{s.SLUG}.gpkg';s.write_geopackage(table,gp);s.reconcile_native(root,table)
            with sqlite3.connect(gp)as db:db.execute('CREATE TABLE synthetic_unknown_auxiliary (evidence TEXT)')
            with self.assertRaisesRegex(ValueError,'fresh_complete_GPKG_bytes_disagree'):s.reconcile_native(root,table)

    def test_stopped_refresh_schedule_config_uses_actual_op(self):
        self.assertEqual(fema_regions_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
        defs=Definitions(assets=[fema_regions_official],asset_checks=[fema_regions_full_quality],jobs=[fema_regions_draft_job],resources={'staging_storage':StagingStorageResource(local_dir='/tmp/not-materialized-fema-structural')})
        with patch.dict(os.environ,{'HIFLD_FEMA_REGIONS_CAPTURE_DIR':str(FIXTURES)}):
            tick=fema_regions_draft_schedule.evaluate_tick(build_schedule_context())
            validate_run_config(defs.resolve_job_def('fema_regions_draft_refresh'),tick.run_requests[0].run_config)

    @unittest.skipUnless(os.environ.get('HIFLD_FEMA_REGIONS_TEST_CAPTURE_DIR'),'Complete real inputs require explicit preserved capture directory; no live refetch assumed.')
    def test_complete_real_native_population_and_baseline(self):
        captures=s.acquire(os.environ['HIFLD_FEMA_REGIONS_TEST_CAPTURE_DIR']);table,audit,metadata=s.decode_native(captures)
        self.assertEqual(len(table),10);self.assertEqual(sum(r['vertices']for r in audit),2640166)
        self.assertTrue(all(r['ordered_native_rings_and_vertices_exact']and r['valid']for r in audit))
        report=s.compare_published(captures,table,audit);self.assertFalse(report['approved'])
        self.assertEqual([r['original_published_id']for r in report['all_rows']],list(range(1,11)))

if __name__=='__main__':unittest.main()
