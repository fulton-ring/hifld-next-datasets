"""Registered blocking source quality, stopped schedule and local draft wiring."""
import unittest
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.wfigs_interagency_fire_perimeters import (
    wfigs_interagency_fire_perimeters,
    wfigs_interagency_fire_perimeters_complete,
)
from dagster_hifld.definitions import defs


class RegistrationTests(unittest.TestCase):
    def test_source_job_quality_and_stopped_schedule(self):
        repo = defs.get_repository_def()
        job = repo.get_job("wfigs_interagency_fire_perimeters_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, wfigs_interagency_fire_perimeters.keys)
        self.assertTrue(set(wfigs_interagency_fire_perimeters_complete.check_keys)
                        <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(wfigs_interagency_fire_perimeters_complete.check_specs)).blocking)
        schedule = repo.get_schedule_def("wfigs_interagency_fire_perimeters_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, "America/New_York")
        self.assertEqual(schedule.cron_schedule, "0 9 1 * *")


if __name__ == "__main__":
    unittest.main()
