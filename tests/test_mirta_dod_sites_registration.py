"""Registered blocking source quality, stopped schedule and local draft wiring."""
import unittest
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.mirta_dod_sites import (
    mirta_dod_sites_boundaries,
    mirta_dod_sites_boundaries_complete,
)
from dagster_hifld.definitions import defs


class RegistrationTests(unittest.TestCase):
    def test_source_job_quality_and_stopped_schedule(self):
        repo = defs.get_repository_def()
        job = repo.get_job('mirta_dod_sites_boundaries_draft_refresh')
        self.assertEqual(job.asset_layer.executable_asset_keys, mirta_dod_sites_boundaries.keys)
        self.assertTrue(set(mirta_dod_sites_boundaries_complete.check_keys)
                        <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(mirta_dod_sites_boundaries_complete.check_specs)).blocking)
        schedule = repo.get_schedule_def('mirta_dod_sites_boundaries_draft_refresh_schedule')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 8 1 * *')

    def test_ingest_asset_registered_by_module_lists(self):
        self.assertIn(mirta_dod_sites_boundaries, hifld_ingest_assets)


if __name__ == '__main__':
    unittest.main()
