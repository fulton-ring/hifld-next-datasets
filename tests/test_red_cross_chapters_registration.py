"""Registered blocking source quality, stopped schedule and generic release path."""
import unittest
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.red_cross_chapters import red_cross_chapters, red_cross_chapters_full_quality
from dagster_hifld.definitions import defs


class RegistrationTests(unittest.TestCase):
    def test_source_job_quality_and_stopped_schedule(self):
        repo = defs.get_repository_def(); job = repo.get_job('red_cross_chapters_draft_refresh')
        self.assertEqual(job.asset_layer.executable_asset_keys, red_cross_chapters.keys)
        self.assertTrue(set(red_cross_chapters_full_quality.check_keys) <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(red_cross_chapters_full_quality.check_specs)).blocking)
        schedule = repo.get_schedule_def('red_cross_chapters_draft_refresh_schedule')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 9 1 * *')


if __name__ == '__main__': unittest.main()
