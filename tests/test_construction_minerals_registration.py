"""Explicit draft historical reconstruction is wired and never scheduled live."""
import os
import unittest
from unittest.mock import patch
from dagster import DefaultScheduleStatus

class RegistrationTests(unittest.TestCase):
 def test_explicit_source_catalog_publish_and_stopped_reconstruction(self):
  with patch.dict(os.environ,{'HIFLD_SHADOW_MANUAL_ONLY':'1'}):
   from dagster_hifld.definitions import defs
   from dagster_hifld.assets.construction_minerals import construction_minerals_draft_schedule
   slug='construction-minerals-operations'
   graph=defs.resolve_asset_graph()
   keys={tuple(k.path) for k in graph.get_all_asset_keys()}
   self.assertIn(('ingest',slug,slug),keys)
   self.assertIn(('publish','catalog'),keys)
   self.assertIn(('publish','verify'),keys)
   self.assertIn(('publish','promote'),keys)
   from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
   self.assertEqual(sum(any(tuple(k.path)==('ingest',slug,slug) for k in asset.keys) for asset in hifld_ingest_assets),1)
   self.assertIsNotNone(defs.resolve_job_def('construction_minerals_preservation_reconstruction'))
   self.assertEqual(construction_minerals_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
