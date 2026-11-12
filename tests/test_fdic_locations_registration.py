"""Complete-source quality is registered; the stopped integrity job has explicit scope."""
import os
import unittest
from unittest.mock import patch

from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config


class RegistrationTests(unittest.TestCase):
    def test_blocking_quality_and_preservation_only_schedule(self):
        with patch.dict(os.environ, {'HIFLD_SHADOW_MANUAL_ONLY': '1', 'HIFLD_FDIC_LOCATIONS_CAPTURE_DIR': '/tmp/fdic-registration-not-executed'}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.fdic_locations import (
                fdic_locations_official,
                fdic_locations_full_quality,
                fdic_locations_draft_schedule,
            )
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets

            key = ('ingest', 'fdic-insured-banks', 'fdic-insured-banks')
            graph = defs.resolve_asset_graph()
            keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
            for expected in (key, ('publish', 'catalog'), ('publish', 'verify'), ('publish', 'promote')):
                self.assertIn(expected, keys)
            self.assertEqual(sum(key in {tuple(k.path) for k in a.keys} for a in hifld_ingest_assets), 1)
            self.assertTrue(set(fdic_locations_full_quality.check_keys) <= graph.asset_check_keys)
            self.assertTrue(next(iter(fdic_locations_full_quality.check_specs)).blocking)
            job = defs.resolve_job_def('fdic_locations_draft_reconstruction')
            self.assertEqual([node.name for node in job.graph.node_defs], [fdic_locations_official.op.name])
            request = fdic_locations_draft_schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job, request.run_config)
            self.assertEqual(fdic_locations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
