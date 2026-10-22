"""Registered source, blocking native-quality check and opt-in live refresh configuration."""
import os
import unittest
from unittest.mock import patch
from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config

class RegistrationTests(unittest.TestCase):
    def test_explicit_asset_blocking_check_and_stopped_refresh(self):
        with patch.dict(os.environ, {'HIFLD_SHADOW_MANUAL_ONLY': '1', 'HIFLD_FEMA_REGIONS_CAPTURE_DIR': '/tmp/fema-regions-proof'}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.fema_regions import fema_regions_draft_schedule, fema_regions_full_quality
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug = 'federal-emergency-management-agency-fema-regions'
            graph = defs.resolve_asset_graph()
            key = ('ingest', slug, slug)
            keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
            for expected in [key, ('publish', 'catalog'), ('publish', 'verify'), ('publish', 'promote')]:
                self.assertIn(expected, keys)
            self.assertEqual(sum(any(tuple(k.path) == key for k in asset.keys) for asset in hifld_ingest_assets), 1)
            self.assertTrue(set(fema_regions_full_quality.check_keys) <= graph.asset_check_keys)
            self.assertTrue(next(iter(fema_regions_full_quality.check_specs)).blocking)
            job = defs.resolve_job_def('fema_regions_draft_refresh')
            request = fema_regions_draft_schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job, request.run_config)
            self.assertEqual(fema_regions_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
