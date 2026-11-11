"""Preservation-only job, complete blocking quality check, and release gates are explicit."""
import os
import unittest
from unittest.mock import patch
from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config

class RegistrationTests(unittest.TestCase):
    def test_source_quality_and_stopped_explicit_local_input_job(self):
        with patch.dict(os.environ, {'HIFLD_SHADOW_MANUAL_ONLY': '1', 'HIFLD_FERC_CAPTURE_DIR': '/tmp/ferc-proof'}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.ferc_regions import ferc_historical_full_quality, ferc_historical_integrity_schedule
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug = 'ferc-regions'
            key = ('ingest', slug, slug)
            graph = defs.resolve_asset_graph()
            keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
            for expected in [key, ('publish', 'catalog'), ('publish', 'verify'), ('publish', 'promote')]:
                self.assertIn(expected, keys)
            self.assertEqual(sum(any(tuple(k.path) == key for k in asset.keys) for asset in hifld_ingest_assets), 1)
            self.assertTrue(set(ferc_historical_full_quality.check_keys) <= graph.asset_check_keys)
            self.assertTrue(next(iter(ferc_historical_full_quality.check_specs)).blocking)
            job = defs.resolve_job_def('ferc_historical_reconstruction')
            request = ferc_historical_integrity_schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job, request.run_config)
            self.assertEqual(ferc_historical_integrity_schedule.default_status, DefaultScheduleStatus.STOPPED)
