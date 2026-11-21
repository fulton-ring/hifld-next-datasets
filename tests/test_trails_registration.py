"""Registered Trails ingest, blocking population check and stopped draft schedule.

Regression for the review finding that the Trails module defined its asset,
check, job and schedule without exporting the discovery lists, leaving the
deployed repository without any Trails definitions.
"""
import unittest
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.discovery import discovered
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.trails_usgs import trails_population_complete, trails_usgs
from dagster_hifld.definitions import defs


class TrailsRegistrationTests(unittest.TestCase):
    def test_module_registrations_are_discovered_into_the_repository(self):
        self.assertIn(id(trails_usgs), [id(asset) for asset in discovered("INGEST_ASSETS")])
        self.assertIn(id(trails_usgs), [id(asset) for asset in hifld_ingest_assets])
        repo = defs.get_repository_def()
        job = repo.get_job("trails_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, trails_usgs.keys)

    def test_blocking_population_check_is_registered_on_the_ingest_asset(self):
        self.assertTrue(
            set(trails_population_complete.check_keys) <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(trails_population_complete.check_specs)).blocking)
        check_key = next(iter(trails_population_complete.check_keys))
        self.assertEqual(check_key.name, "trails_population_complete")

    def test_draft_schedule_is_registered_and_stopped(self):
        repo = defs.get_repository_def()
        schedule = repo.get_schedule_def("trails_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, "America/New_York")
        self.assertEqual(schedule.cron_schedule, "0 9 1 * *")


if __name__ == "__main__":
    unittest.main()
