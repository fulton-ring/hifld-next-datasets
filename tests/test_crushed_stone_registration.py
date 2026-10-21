"""Historical reconstruction is explicit, registered and never automatically scheduled."""
import os
import unittest
from unittest.mock import patch
from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config

class RegistrationTests(unittest.TestCase):
    def test_explicit_native_source_catalog_gates_and_stopped_integrity_job(self):
        with patch.dict(os.environ, {'HIFLD_SHADOW_MANUAL_ONLY': '1'}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.crushed_stone import crushed_stone_preservation_schedule
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug = 'crushed-stone-operations'
            key = ('ingest', slug, slug)
            graph = defs.resolve_asset_graph()
            keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
            for expected in [key, ('publish', 'catalog'), ('publish', 'verify'), ('publish', 'promote')]:
                self.assertIn(expected, keys)
            self.assertEqual(sum(any(tuple(k.path) == key for k in asset.keys) for asset in hifld_ingest_assets), 1)
            job = defs.resolve_job_def('crushed_stone_preservation_reconstruct')
            request = crushed_stone_preservation_schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job, request.run_config)
            self.assertEqual(crushed_stone_preservation_schedule.default_status, DefaultScheduleStatus.STOPPED)
