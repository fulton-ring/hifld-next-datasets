import types
import unittest
from unittest.mock import patch

from dagster_hifld.assets import discovery
from dagster_hifld.assets.discovery import discovered, unique
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets


class AssetDiscoveryTests(unittest.TestCase):
    def test_unique_keeps_first_occurrence_by_identity(self):
        first, second = object(), object()
        self.assertEqual(unique([first, second], [first]), [first, second])

    def test_module_declared_definitions_are_discovered(self):
        marker = object()
        module = types.SimpleNamespace(INGEST_ASSETS=[marker])
        info = types.SimpleNamespace(name="example_dataset", ispkg=False)
        with (
            patch.object(discovery.pkgutil, "iter_modules", return_value=[info]),
            patch.object(discovery.importlib, "import_module", return_value=module),
        ):
            self.assertEqual(discovered("INGEST_ASSETS"), [marker])
            self.assertEqual(discovered("JOBS"), [])

    def test_registry_contains_every_discovered_ingest_asset_once(self):
        ids = [id(asset) for asset in hifld_ingest_assets]
        self.assertEqual(len(ids), len(set(ids)))
        for asset in discovered("INGEST_ASSETS"):
            self.assertIn(id(asset), ids)


if __name__ == "__main__":
    unittest.main()
