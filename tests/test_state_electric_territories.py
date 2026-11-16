import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import Polygon
import unittest

from dagster_hifld.sources import state_electric_territories as source


class StateElectricTerritoryTests(unittest.TestCase):
    def test_iowa_native_null_and_whitespace_survive(self):
        fields = [('OBJECTID', 'esriFieldTypeOID', None), ('Owner', 'esriFieldTypeString', 50)]
        attrs = {'OBJECTID': 3, 'Owner': '  Native  '}
        table = source.iowa_table([{'attributes': attrs, 'geometry': None}], fields)
        self.assertEqual(table['Owner'].to_pylist(), ['  Native  '])
        self.assertEqual(table['geometry'].to_pylist(), [None])
        with self.assertRaisesRegex(ValueError, 'attributes'):
            source.iowa_table([{'attributes': {**attrs, 'EIA_ID': 123}, 'geometry': None}], fields)

    def test_source_contract_drift_refused(self):
        with self.assertRaisesRegex(ValueError, 'contract'):
            source.validate_contract('municipal', {'crs': 'EPSG:4326'}, None)

    def test_replay_never_fetches_missing_capture(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                source.capture('https://psc.wi.gov/example', Path(d), 'missing', replay=True)

    def test_four_registered_ingest_files_and_stopped_schedule(self):
        from dagster import Definitions, DefaultScheduleStatus
        from dagster_hifld.assets.state_electric_territories import INGEST_ASSETS, ASSET_CHECKS, SCHEDULES
        from dagster_hifld.resources import StagingStorageResource
        Definitions.validate_loadable(Definitions(assets=INGEST_ASSETS, asset_checks=ASSET_CHECKS,
            resources={'staging_storage': StagingStorageResource(local_dir='/tmp/hifld-960-test-registration')}))
        self.assertEqual(len(INGEST_ASSETS), 4)
        self.assertEqual(SCHEDULES[0].default_status, DefaultScheduleStatus.STOPPED)

    def test_invalid_and_null_native_geometry_remain_present(self):
        invalid = shapely.to_wkb(Polygon([(0, 0), (3, 3), (0, 3), (3, 0), (0, 0)]))
        table = pa.table({'geometry': pa.array([invalid, None], type=pa.binary())})
        body, quality = source.geoparquet(table, 'ESRI:102004')
        self.assertEqual(pq.read_table(pa.BufferReader(body))['geometry'].to_pylist(), [invalid, None])
        self.assertEqual(quality, {'null_geometry_count': 1, 'invalid_geometry_count': 1})

    def test_native_z_wkb_and_scalar_roundtrip(self):
        geometry = shapely.to_wkb(Polygon([(0, 0, 7), (0, 5, 8), (5, 0, 9), (0, 0, 7)]))
        table = pa.table({'CODE': [' 001 '], 'PSC_ID': pa.array([9007199254740993], type=pa.int64()),
            'wkb_geometry': pa.array([geometry], type=pa.binary())})
        body, quality = source.geoparquet(table, 'EPSG:3071')
        restored = pq.read_table(pa.BufferReader(body))
        self.assertTrue(restored.equals(table, check_metadata=False))
        self.assertEqual(json.loads(restored.schema.metadata[b'geo'])['columns']['wkb_geometry']['geometry_types'], ['Polygon Z'])
        self.assertEqual(quality, {'null_geometry_count': 0, 'invalid_geometry_count': 0})

    def test_replay_refuses_modified_wire(self):
        body = b'{"native":1}'
        url = 'https://psc.wi.gov/example'
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            (directory / 'one.body').write_bytes(body)
            (directory / 'one.wire').write_bytes(b'modified')
            (directory / 'one.receipt.json').write_text(json.dumps({'method': 'GET', 'url': url,
                'final_url': url, 'status': 200, 'complete_body': True, 'bytes': len(body),
                'sha256': source.digest(body), 'wire_bytes': len(body), 'wire_sha256': source.digest(body)}))
            with patch.object(httpx.Client, 'send', side_effect=AssertionError('unexpected network')):
                with self.assertRaisesRegex(ValueError, 'retained_capture_mismatch'):
                    source.capture(url, directory, 'one', replay=True)

    def test_excluded_nbam_entrypoint_refuses_before_side_effects(self):
        from dagster_hifld.sources.electric_retail_service_territories import capture_electric_retail_service_territories
        with patch.object(httpx.Client, 'send', side_effect=AssertionError('excluded HTTP')):
            with self.assertRaisesRegex(ValueError, 'excluded_frozen_hifld_copy'):
                capture_electric_retail_service_territories(None)


if __name__ == '__main__':
    unittest.main()
