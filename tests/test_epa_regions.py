"""EPA Regions now reads EPA_GEO's maintained 2023 service; release stays unapproved."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_regions as s
from dagster_hifld.sources import epa_regions_common as shared
from epa_regions_common_support import captures, documents, page, write_capture_dir


def stage(storage, capture_dir):
    with patch.object(shared, 'validate_construction_code_identity'):
        return s.stage_epa_regions(storage, capture_dir=capture_dir)


def mutate(capture_map, name, change):
    body, receipt = capture_map[name]
    value = json.loads(body)
    change(value)
    body = shared.encode(value)
    capture_map[name] = (body, receipt | {'bytes': len(body), 'sha256': shared.digest(body)})


class SourceIdentityTests(unittest.TestCase):
    def test_only_the_maintained_epa_geo_2023_layer_is_read(self):
        self.assertEqual(shared.LANDING, 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/US_EPA_Regions__DOC___EPA_2023_/FeatureServer')
        self.assertTrue(shared.BASE.endswith('/FeatureServer/13'))
        self.assertEqual(shared.ITEM, '21dfeddb55984d9c957ffad98e7da94e')
        joined = ' '.join(shared.URLS.values())
        self.assertNotIn('geodata.epa.gov', joined)
        self.assertNotIn('EPA_Locations', joined)
        self.assertNotIn('fd4571953b1648f68be2478088a657c6', joined)
        self.assertIn('EPA_GEO', shared.URLS['owner'])

    def test_large_and_small_scale_share_the_one_resolution(self):
        self.assertEqual(shared.REGIONS_IDENTITY.slug, 'epa-regions')
        self.assertEqual(shared.LARGE_SCALE_IDENTITY.slug, 'epa-regions-large-scale-15000000')
        self.assertEqual(shared.SMALL_SCALE_IDENTITY.slug, 'epa-regions-small-scale-15000001')
        self.assertEqual(len({shared.REGIONS_IDENTITY.slug, shared.LARGE_SCALE_IDENTITY.slug, shared.SMALL_SCALE_IDENTITY.slug}), 3)


class DecodeTests(unittest.TestCase):
    def test_all_twelve_original_fields_types_and_native_rings(self):
        table, audit = shared.decode_page(page())
        self.assertEqual(table.column_names, list(shared.FIELDS))
        self.assertEqual(table.num_rows, 10)
        self.assertEqual(table['OBJECTID'].type, pa.int64())
        self.assertEqual(table['EPA_REGION'].type, pa.string())
        self.assertEqual(table['ALAND_MI'].type, pa.float64())
        self.assertEqual(table['RegionTitle'][0].as_py(), 'EPA Region 01')
        self.assertEqual(audit, {'rings': 10, 'vertices': 50})
        for row in table.to_pylist():
            self.assertEqual(row['RegionString'], row['EPA_REGION'])

    def test_attribute_and_type_drift_is_rejected(self):
        for change in [
            lambda p: p['features'][0]['attributes'].pop('RegionTitle'),
            lambda p: p['features'][0]['attributes'].update(extra='x'),
            lambda p: p['features'][0]['attributes'].update(OBJECTID=None),
            lambda p: p['features'][1]['attributes'].update(OBJECTID=1),
            lambda p: p['features'][0]['attributes'].update(EPA_REGION=1),
            lambda p: p['features'][0]['attributes'].update(RegionTitle='x' * 256),
            lambda p: p['features'][0]['attributes'].update(ALAND_MI=float('nan')),
        ]:
            changed = page()
            change(changed)
            with self.subTest(change=change), self.assertRaises(ValueError):
                shared.decode_page(changed)

    def test_transfer_crs_and_geometry_extension_drift_is_rejected(self):
        for change in [
            lambda p: p.update(exceededTransferLimit=True),
            lambda p: p.update(spatialReference={'wkid': 3857, 'latestWkid': 3857}),
            lambda p: p['fields'][0].update(type='esriFieldTypeInteger'),
            lambda p: p['features'][0]['geometry'].update(curveRings=[]),
            lambda p: p['features'][0]['geometry']['rings'][0][1].append(5),
        ]:
            changed = page()
            change(changed)
            with self.subTest(change=change), self.assertRaises((ValueError, RuntimeError)):
                shared.decode_page(changed)


class InspectTests(unittest.TestCase):
    def test_synthetic_captures_validate_authority_and_coverage(self):
        table, audit = shared.inspect_captures(captures())
        self.assertEqual(table.num_rows, 10)
        self.assertEqual(audit['source_CRS'], 4326)
        self.assertEqual(audit['field_count'], 12)
        self.assertEqual(audit['invalid_geometry_count'], 0)
        self.assertFalse(audit['approved'])

    def test_publisher_layer_or_region_drift_is_rejected(self):
        cases = [
            ('item', lambda x: x.update(owner='someone_else')),
            ('item', lambda x: x.update(orgId='other-org')),
            ('item', lambda x: x.update(url='https://example.com/FeatureServer')),
            ('owner', lambda x: x.update(username='someone_else')),
            ('source_service', lambda x: x['layers'][0].update(id=3)),
            ('layer_before', lambda x: x.update(name='Other layer')),
            ('layer_before', lambda x: x.update(hasZ=True)),
            ('layer_before', lambda x: x['fields'][1].update(length=50)),
            ('layer_before', lambda x: x.update(description='no maintenance statement')),
            ('page_00000', lambda x: x['features'][0]['attributes'].update(RegionTitle='Wrong')),
            ('count_after', lambda x: x.update(count=9)),
        ]
        for name, change in cases:
            changed = captures()
            mutate(changed, name, change)
            with self.subTest(name=name, change=change), self.assertRaises(ValueError):
                shared.inspect_captures(changed)


class StageTests(unittest.TestCase):
    def test_each_slug_stages_and_large_small_outputs_match(self):
        from dagster_hifld.sources import epa_large_scale as large
        from dagster_hifld.sources import epa_small_scale as small
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            inputs = write_capture_dir(base / 'inputs')
            outputs = {}
            for module in (s, large, small):
                storage = StagingStorageResource(local_dir=str(base / module.SLUG))
                with patch.object(shared, 'validate_construction_code_identity'):
                    result = module.stage_epa_regions(storage, capture_dir=inputs) if module is s else (
                        module.stage_epa_large_scale(storage, capture_dir=inputs) if module is large else
                        module.stage_epa_small_scale(storage, capture_dir=inputs))
                self.assertEqual(result['dataset_slug'], module.SLUG)
                self.assertEqual(result['feature_count'], 10)
                outputs[module.SLUG] = next((base / module.SLUG).rglob('geoparquet/*.parquet'))
            self.assertEqual(outputs[large.SLUG].read_bytes(), outputs[small.SLUG].read_bytes())

    def test_stage_reuses_the_sealed_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            inputs = write_capture_dir(base / 'inputs')
            storage = StagingStorageResource(local_dir=str(base / 'stage'))
            first = stage(storage, inputs)
            second = stage(storage, inputs)
            self.assertFalse(first['reused'])
            self.assertTrue(second['reused'])
            self.assertEqual(first['version'], second['version'])

    def test_malformed_receipt_retains_consumed_body_before_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            inputs = write_capture_dir(base / 'inputs')
            (inputs / 'source_service.receipt.json').write_bytes(b'{bad')
            with self.assertRaises(ValueError):
                shared.acquire(StagingStorageResource(local_dir=str(base / 'stage')), inputs)
            retained = next((base / 'stage').glob('_raw-captures/*/*/source_service.body'))
            self.assertEqual(retained.read_bytes(), captures()['source_service'][0])
            self.assertFalse((base / 'stage/_construction-state').exists())


class RegistrationTests(unittest.TestCase):
    def test_all_three_assets_and_stopped_refresh_schedules(self):
        from dagster_hifld.assets.epa_large_scale import epa_large_scale_source_integrity_schedule
        from dagster_hifld.assets.epa_regions import epa_regions_draft_schedule
        from dagster_hifld.assets.epa_small_scale import epa_small_scale_source_integrity_schedule

        keys = {tuple(k.path) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        for slug in ('epa-regions', 'epa-regions-large-scale-15000000', 'epa-regions-small-scale-15000001'):
            self.assertIn(('ingest', slug, slug), keys)
        for schedule, job in [
            (epa_regions_draft_schedule, 'epa_regions_source_integrity'),
            (epa_large_scale_source_integrity_schedule, 'epa_large_scale_source_integrity'),
            (epa_small_scale_source_integrity_schedule, 'epa_small_scale_source_integrity'),
        ]:
            self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
            self.assertEqual(defs.resolve_job_def(job).name, job)


if __name__ == '__main__':
    unittest.main()
