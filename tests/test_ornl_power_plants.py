import unittest

from dagster_hifld.assets.ornl_power_plants import eia_power_plants_draft_schedule
from dagster_hifld.sources.ornl_power_plants import (
    EXPECTED_COUNT, EXPECTED_FIELDS, EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256,
    SOURCE_RELEASE, SOURCE_URL, _point_wkb,
)


class EiaPowerPlantsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(EXPECTED_COUNT, 10847)
        self.assertEqual(SOURCE_RELEASE, "2021-08")
        self.assertEqual(SOURCE_URL, "https://www.eia.gov/maps/map_data/PowerPlants_US_EIA.zip")
        self.assertEqual(EXPECTED_ZIP_BYTES, 1539615)
        self.assertEqual(len(EXPECTED_ZIP_SHA256), 64)
        self.assertEqual(len(EXPECTED_FIELDS), 31)
        self.assertEqual(EXPECTED_FIELDS[0][0], "Plant_Code")
        self.assertEqual(EXPECTED_FIELDS[-1][0], "Bat_MW")

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(_point_wkb(1.5, -2.25).hex(), "0101000000000000000000f83f00000000000002c0")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(eia_power_plants_draft_schedule.default_status.value, "STOPPED")


if __name__ == "__main__":
    unittest.main()
