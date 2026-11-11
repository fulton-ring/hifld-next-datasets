"""Check refresh wiring in the normal repository, including its full registry."""
import unittest
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.definitions import defs


class RefreshRegistrationTests(unittest.TestCase):
    def test_registered_stopped_source_only_refresh(self):
        repository = defs.get_repository_def()
        job = repository.get_job('fws_cbrs_units_draft_refresh')
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(['ingest', 'cbrs-units-2', 'cbrs-units-2'])
        })
        schedule = repository.get_schedule_def('fws_cbrs_units_draft_refresh_schedule')
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 15 1 * *')
