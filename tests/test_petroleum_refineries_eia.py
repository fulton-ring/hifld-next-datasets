import unittest

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.petroleum_refineries_eia import (
    petroleum_refineries_eia_draft_schedule,
    petroleum_refineries_eia_historical_candidate_complete,
    petroleum_refineries_eia_ingest,
)
from dagster_hifld.sources import petroleum_refineries_eia as source


class PetroleumRefineriesEiaTests(unittest.TestCase):
    def test_historical_adapter_is_registered_and_refresh_stays_stopped(self):
        self.assertIn(petroleum_refineries_eia_ingest, hifld_ingest_assets)
        self.assertEqual(petroleum_refineries_eia_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(source.SOURCE_VINTAGE, "2021-01-01")
        self.assertEqual(len(source.SOURCE_FIELDS), 22)
        self.assertEqual([field[0] for field in source.SOURCE_FIELDS][:2], ["OBJECTID", "site_id"])

    def test_complete_historical_candidate_never_accepts_currentness_or_release(self):
        check = petroleum_refineries_eia_historical_candidate_complete({
            "feature_count": 130, "source_population_complete": True, "historical_only": True,
            "source_vintage": "2021-01-01",
        })
        self.assertTrue(check.passed)
        for field in ("source_release_accepted", "freshness_accepted", "schema_contract_accepted",
                      "id_contract_accepted", "coverage_accepted", "rights_accepted", "mapping_approved", "release_accepted"):
            self.assertFalse(check.metadata[field].value, field)

    def test_changed_archive_requires_new_release_review(self):
        with self.assertRaisesRegex(ValueError, "new_release_review_required"):
            source._read_archive(b"different upstream bytes")


if __name__ == "__main__":
    unittest.main()
