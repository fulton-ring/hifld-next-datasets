"""Normal repository wiring for the stopped local draft source refresh."""
import unittest
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.definitions import defs


class TerritorialRefreshRegistrationTests(unittest.TestCase):
    def test_normal_repository_registers_stopped_source_only_refresh(self):
        repository = defs.get_repository_def()
        job = repository.get_job('noaa_territorial_draft_refresh')
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(['ingest', '12nm-territorial-sea', '12nm-territorial-sea'])
        })
        schedule = repository.get_schedule_def('noaa_territorial_draft_weekly')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 6 * * 1')
