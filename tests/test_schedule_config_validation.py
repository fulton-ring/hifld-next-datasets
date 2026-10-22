"""Dry validation of stopped schedule run configs against their resolved jobs."""

import os
import copy
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus, RunRequest, build_schedule_context, validate_run_config

from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource


class ScheduleConfigValidationTests(unittest.TestCase):
    def test_all_stopped_schedule_configs_validate_against_resolved_jobs(self):
        """Evaluate schedule requests with inert paths; never execute a job or touch a source."""
        repo = defs.get_repository_def()
        fixture_root = Path("/tmp/schedule-config-validation-fixtures")
        fixture_env = {
            "HIFLD_CAMPUS_CAPTURE_DIR": str(fixture_root / "campus"),
            "HIFLD_FDIC_LOCATIONS_CAPTURE_DIR": str(fixture_root / "fdic"),
            "HIFLD_FEMA_REGIONS_CAPTURE_DIR": str(fixture_root / "fema"),
            "HIFLD_FERC_CAPTURE_DIR": str(fixture_root / "ferc"),
        }
        storage = StagingStorageResource(
            bucket=None,
            local_dir=str(fixture_root / "staging"),
            use_local=True,
            prefix="",
        )
        context = build_schedule_context(
            scheduled_execution_time=datetime(2026, 10, 22, 12, tzinfo=timezone.utc),
            repository_def=repo,
            resources={"staging_storage": storage},
        )

        with patch.dict(os.environ, fixture_env), context:
            schedules = repo.schedule_defs
            self.assertTrue(schedules)
            self.assertEqual(len({schedule.name for schedule in schedules}), len(schedules))
            for schedule in schedules:
                with self.subTest(schedule=schedule.name):
                    self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
                    data = schedule.evaluate_tick(context)
                    requests = list(data.run_requests or ())
                    self.assertTrue(requests, data.skip_message or "schedule emitted no RunRequest")
                    job = repo.get_job(schedule.job_name)
                    for request in requests:
                        self.assertIsInstance(request, RunRequest)
                        run_config = copy.deepcopy(request.run_config or {})
                        # This source-only schedule carries Dagster EnvVar sentinels in Python;
                        # substitute inert explicit paths for schema validation only.
                        if schedule.name == "census_block_groups_draft_refresh_schedule":
                            self.assertEqual(len(run_config.get("ops", {})), 1)
                            source_config = next(iter(run_config["ops"].values()))["config"]
                            source_config["archive_path"] = str(fixture_root / "census.gdb.zip")
                            source_config["receipt_path"] = str(fixture_root / "census.receipt.json")
                        validate_run_config(job, run_config)


if __name__ == "__main__":
    unittest.main()
