"""Focused identity and release-gate checks for the frozen BTS IPCD draft."""

from dagster_hifld.assets.bts_ipcd import (
    bts_intermodal_passenger_connectivity_database_ipcd,
    bts_intermodal_passenger_connectivity_database_ipcd_complete,
    bts_ipcd_frozen_snapshot_schedule,
)
from dagster_hifld.sources.bts_ipcd import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_GEOMETRY_SHA256,
    EXPECTED_ID_SHA256,
    EXPECTED_RECORD_SHA256,
    EXPECTED_SCHEMA_SHA256,
    ITEM_ID,
    LAYER_ID,
    LAYER_NAME,
    SERVICE_URL,
    _arrow_type,
)
import pyarrow as pa
from dagster import DefaultScheduleStatus
import unittest


class TestBtsIpcd(unittest.TestCase):
    def test_bts_ipcd_asset_is_explicit_and_snapshot_schedule_is_stopped(self):
        self.assertEqual(DATASET_SLUG, "intermodal-passenger-connectivity-database-ipcd")
        self.assertEqual(ITEM_ID, "25d9314da4ec44088f1db9e9e11a19d6")
        self.assertEqual(LAYER_ID, 0)
        self.assertEqual(LAYER_NAME, "Intermodal Passenger Connectivity Database (IPCD)")
        self.assertTrue(SERVICE_URL.endswith("NTAD_Intermodal_Passenger_Connectivity_Database/FeatureServer"))
        self.assertEqual(bts_intermodal_passenger_connectivity_database_ipcd.key.path, ["ingest", DATASET_SLUG, DATASET_SLUG])
        self.assertEqual(bts_ipcd_frozen_snapshot_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertTrue(bts_intermodal_passenger_connectivity_database_ipcd_complete.check_specs)

    def test_frozen_ipcd_population_baselines_are_complete_and_pinned(self):
        self.assertEqual(EXPECTED_COUNT, 15488)
        self.assertTrue(all(len(value) == 64 for value in (
            EXPECTED_ID_SHA256, EXPECTED_RECORD_SHA256, EXPECTED_GEOMETRY_SHA256,
            EXPECTED_SCHEMA_SHA256,
        )))

    def test_esri_physical_types_preserve_date_and_numeric_widths(self):
        self.assertEqual(_arrow_type({"type": "esriFieldTypeOID"}), pa.int64())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeInteger"}), pa.int32())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeDouble"}), pa.float64())
        self.assertEqual(_arrow_type({"type": "esriFieldTypeDate"}), pa.int64())
