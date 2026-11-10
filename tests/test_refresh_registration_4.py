"""Check refresh wiring in the normal repository, including its full registry."""
import unittest
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.definitions import defs


class RefreshRegistrationTests(unittest.TestCase):
    def test_registered_stopped_source_only_refresh(self):
        repository = defs.get_repository_def()
        job = repository.get_job('noaa_contiguous_draft_refresh')
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(['ingest', '24nm-contiguous-zone', '24nm-contiguous-zone'])
        })
        schedule = repository.get_schedule_def('noaa_contiguous_draft_weekly')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 9 * * 1')
