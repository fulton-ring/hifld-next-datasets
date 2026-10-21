"""Registered blocking source quality, stopped schedule and generic release path."""
import unittest
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.arc_regions import arc_regions, arc_regions_full_quality
from dagster_hifld.definitions import defs


class RegistrationTests(unittest.TestCase):
    def test_source_job_quality_and_stopped_schedule(self):
        repo = defs.get_repository_def(); job = repo.get_job('arc_regions_draft_refresh')
        self.assertEqual(job.asset_layer.executable_asset_keys, arc_regions.keys)
        self.assertTrue(set(arc_regions_full_quality.check_keys) <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(arc_regions_full_quality.check_specs)).blocking)
        schedule = repo.get_schedule_def('arc_regions_draft_refresh_schedule')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 9 1 * *')


if __name__ == '__main__': unittest.main()
