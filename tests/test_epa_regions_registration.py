"""EPA source and stopped draft refresh remain reachable through real Definitions."""
import os
import unittest
from unittest.mock import patch

from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config


class RegistrationTests(unittest.TestCase):
    def test_source_and_stopped_refresh_with_normal_publication_gates(self):
        with patch.dict(os.environ, {'HIFLD_SHADOW_MANUAL_ONLY': '1'}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.epa_regions import epa_regions_draft_schedule
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets

            key = ('ingest', 'epa-regions', 'epa-regions')
            keys = {tuple(k.path) for k in defs.resolve_asset_graph().get_all_asset_keys()}
            for expected in (key, ('publish', 'catalog'), ('publish', 'verify'), ('publish', 'promote')):
                self.assertIn(expected, keys)
            self.assertEqual(sum(key in {tuple(k.path) for k in asset.keys} for asset in hifld_ingest_assets), 1)
            job = defs.resolve_job_def('epa_regions_source_integrity')
            request = epa_regions_draft_schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job, request.run_config)
            self.assertEqual(epa_regions_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
