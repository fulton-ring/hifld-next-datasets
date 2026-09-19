import inspect
from typing import get_type_hints
import unittest
from pathlib import Path

from dagster import AssetKey
from dagster import JsonMetadataValue
from dagster import Output
from dagster_hifld.assets import catalog as catalog_assets_module
from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets import noaa as noaa_assets_module
from dagster_hifld.assets import publish as publish_assets_module


class ExplicitAssetTests(unittest.TestCase):
    def test_hifld_dataset_assets_include_ingest_catalog_and_publish_layers(self):
        ingest_count = len(hifld_ingest_assets)
        catalog_count = len(catalog_assets_module.catalog_assets)
        publish_count = len(publish_assets_module.publish_assets)
        self.assertEqual(
            len(hifld_dataset_assets),
            ingest_count + catalog_count + publish_count,
        )
        self.assertTrue(all(asset.key.path[0] == "ingest" for asset in hifld_ingest_assets))
        self.assertTrue(all(asset.key.path[0] == "publish" for asset in catalog_assets_module.catalog_assets))
        self.assertTrue(all(asset.key.path[0] == "publish" for asset in publish_assets_module.publish_assets))

    def test_catalog_and_publish_assets_return_output_annotations_for_bts_key(self):
        catalog_asset = next(
            asset
            for asset in catalog_assets_module.catalog_assets
            if asset.key == AssetKey(["publish", "catalog"])
        )
        publish_asset = next(
            asset
            for asset in publish_assets_module.publish_assets
            if asset.key == AssetKey(["publish", "formats", "geoparquet"])
        )
        catalog_hints = get_type_hints(catalog_asset.op.compute_fn.decorated_fn)
        publish_hints = get_type_hints(publish_asset.op.compute_fn.decorated_fn)

        self.assertEqual(catalog_hints["return"], Output[dict])
        self.assertEqual(publish_hints["return"], Output[dict])

    def test_source_tree_no_longer_references_dataset_registry(self):
        src_root = Path(
            "/Users/jeremyherzog/Documents/projects/pedp/hifld-next-datasets/src"
        )
        offenders = []
        for path in src_root.rglob("*.py"):
            if path.name == "dataset_registry.py":
                offenders.append(str(path))
                continue
            if "dataset_registry" in path.read_text(encoding="utf-8"):
                offenders.append(str(path))
        self.assertEqual(offenders, [])

    def test_noaa_assets_use_distinct_rest_layer_queries(self):
        self.assertIn("/MapServer/1/query", noaa_assets_module._TERRITORIAL_SEA_URL)
        self.assertIn("/MapServer/2/query", noaa_assets_module._CONTIGUOUS_ZONE_URL)
        self.assertIn("/MapServer/3/query", noaa_assets_module._EEZ_URL)
        self.assertTrue(noaa_assets_module._TERRITORIAL_SEA_URL.endswith("&f=geojson"))
        self.assertTrue(noaa_assets_module._CONTIGUOUS_ZONE_URL.endswith("&f=geojson"))
        self.assertTrue(noaa_assets_module._EEZ_URL.endswith("&f=geojson"))

    def test_catalog_output_metadata_includes_quality_and_schema_summary(self):
        metadata = catalog_assets_module._build_catalog_output_metadata(
            dataset_slug="12nm-territorial-sea",
            file_slug="12nm-territorial-sea",
            version="v-test",
            description="Territorial sea",
            quality_manifest={
                "feature_count": 87,
                "bounds": [-180.0, -14.760836, 180.0, 71.588953],
                "geometry_type": "Mixed",
                "invalid_geometry_count": 0,
                "quality_check_passed": True,
                "columns_hash": "abc123",
            },
            data_dictionary={
                "name": "12nm-territorial-sea",
                "columns": [
                    {"name": "OBJECTID", "type": "integer", "nullable": False},
                    {"name": "SUPP_INFO", "type": "string", "nullable": False},
                ],
            },
        )

        self.assertEqual(metadata["feature_count"], 87)
        self.assertEqual(metadata["column_count"], 2)
        self.assertEqual(metadata["columns_hash"], "abc123")
        self.assertIsInstance(metadata["bounds"], JsonMetadataValue)
        self.assertEqual(metadata["bounds"].data, [-180.0, -14.760836, 180.0, 71.588953])
        self.assertIsInstance(metadata["schema_columns"], JsonMetadataValue)
        self.assertEqual(
            metadata["schema_columns"].data,
            [
                {"name": "OBJECTID", "type": "integer", "nullable": False},
                {"name": "SUPP_INFO", "type": "string", "nullable": False},
            ],
        )


if __name__ == "__main__":
    unittest.main()
