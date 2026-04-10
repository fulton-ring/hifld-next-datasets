import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from dagster import AssetKey, DagsterInstance, DynamicPartitionsDefinition, build_sensor_context

from dagster_hifld.assets import catalog as catalog_assets_module
from dagster_hifld.assets import publish as publish_assets_module
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILES
from dagster_hifld.assets.census_blocks_2020 import (
    CENSUS_2020_BLOCKS_DATASET_SLUG,
    CENSUS_2020_TABBLOCK20_FILE_SLUGS,
)
from dagster_hifld.definitions import _iter_staged_version_paths, version_discovery_sensor
import dagster_hifld.definitions as definitions_module
from dagster_hifld.partitions import (
    ALL_PARTITIONS_DEFS,
    CATALOG_PARTITIONS,
    PARTITIONS_BY_PAIR,
    get_partition_name,
)
from dagster_hifld.resources import StagingStorageResource


class DynamicPartitionTests(unittest.TestCase):
    def test_catalog_and_publish_assets_use_dynamic_version_partitions(self):
        partition_name = get_partition_name("amtrak-stations", "amtrak-stations")
        expected = CATALOG_PARTITIONS[partition_name]
        catalog_asset = next(
            asset
            for asset in catalog_assets_module.catalog_assets
            if asset.key == AssetKey(["catalog", "amtrak-stations", "amtrak-stations"])
        )
        publish_asset = next(
            asset
            for asset in publish_assets_module.publish_assets
            if asset.key == AssetKey(["publish", "amtrak-stations", "amtrak-stations"])
        )

        self.assertIsInstance(expected, DynamicPartitionsDefinition)
        self.assertIs(catalog_asset.partitions_def, expected)
        self.assertIs(publish_asset.partitions_def, expected)
        self.assertIn(expected, ALL_PARTITIONS_DEFS)

    def test_legacy_backfill_dataset_has_partition_definition(self):
        partition_name = get_partition_name(
            "119th-congressional-districts", "119th-congressional-districts"
        )
        expected = CATALOG_PARTITIONS[partition_name]
        catalog_asset = next(
            asset
            for asset in catalog_assets_module.catalog_assets
            if asset.key
            == AssetKey(
                ["catalog", "119th-congressional-districts", "119th-congressional-districts"]
            )
        )
        publish_asset = next(
            asset
            for asset in publish_assets_module.publish_assets
            if asset.key
            == AssetKey(
                ["publish", "119th-congressional-districts", "119th-congressional-districts"]
            )
        )

        self.assertIsInstance(expected, DynamicPartitionsDefinition)
        self.assertIs(catalog_asset.partitions_def, expected)
        self.assertIs(publish_asset.partitions_def, expected)

    def test_2020_census_blocks_tabblock20_partition_registry(self):
        self.assertEqual(len(CENSUS_2020_TABBLOCK20_FILE_SLUGS), 56)
        self.assertIn("tl_2024_01_tabblock20", CENSUS_2020_TABBLOCK20_FILE_SLUGS)
        pair = (CENSUS_2020_BLOCKS_DATASET_SLUG, "tl_2024_01_tabblock20")
        self.assertIn(pair, PARTITIONS_BY_PAIR)
        self.assertEqual(
            get_partition_name(*pair),
            "2020-census-blocks-1--tl_2024_01_tabblock20",
        )
        partition_name = get_partition_name(*pair)
        self.assertIn(partition_name, CATALOG_PARTITIONS)

    def test_2020_census_blocks_catalog_and_publish_assets_registered(self):
        catalog_keys = {a.key for a in catalog_assets_module.catalog_assets}
        publish_keys = {a.key for a in publish_assets_module.publish_assets}
        alabama = AssetKey(
            ["catalog", CENSUS_2020_BLOCKS_DATASET_SLUG, "tl_2024_01_tabblock20"]
        )
        alabama_pub = AssetKey(
            ["publish", CENSUS_2020_BLOCKS_DATASET_SLUG, "tl_2024_01_tabblock20"]
        )
        self.assertIn(alabama, catalog_keys)
        self.assertIn(alabama_pub, publish_keys)

    def test_supported_assets_drive_catalog_publish_and_partition_counts(self):
        supported_pairs = {
            (spec.dataset_slug, spec.file_slug) for spec in SUPPORTED_DATASET_FILES
        }
        catalog_pairs = {
            tuple(asset.key.path[1:3]) for asset in catalog_assets_module.catalog_assets
        }
        publish_pairs = {
            tuple(asset.key.path[1:3]) for asset in publish_assets_module.publish_assets
        }
        partition_pairs = set(PARTITIONS_BY_PAIR)

        self.assertEqual(catalog_pairs, supported_pairs)
        self.assertEqual(publish_pairs, supported_pairs)
        self.assertEqual(partition_pairs, supported_pairs)

    def test_version_discovery_sensor_requests_missing_catalog_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_root = (
                Path(tmpdir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "v20260214"
                / "shapefile"
            )
            version_root.mkdir(parents=True)
            (version_root / "stations.shp").write_bytes(b"shp")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ):
                result = version_discovery_sensor(context)

        self.assertEqual(len(result.run_requests), 1)
        self.assertTrue(
            instance.has_dynamic_partition(
                get_partition_name("amtrak-stations", "amtrak-stations"),
                "v20260214",
            )
        )

        run_request = result.run_requests[0]
        self.assertEqual(run_request.partition_key, "v20260214")
        self.assertEqual(
            run_request.asset_selection,
            [AssetKey(["catalog", "amtrak-stations", "amtrak-stations"])],
        )

    def test_version_discovery_sensor_caps_run_requests_per_tick(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "amtrak-stations" / "amtrak-stations"
            for version in ("v1", "v2", "v3"):
                version_dir = root / version / "shapefile"
                version_dir.mkdir(parents=True)
                (version_dir / "stations.shp").write_bytes(b"shp")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ), patch.object(
                definitions_module,
                "MAX_SENSOR_RUN_REQUESTS_PER_TICK",
                2,
            ):
                result = version_discovery_sensor(context)

        self.assertEqual(len(result.run_requests), 2)

    def test_iter_staged_version_paths_detects_gcs_objects(self):
        class FakeGCSFileSystem:
            def find(self, root, maxdepth=None):
                self.root = root
                self.maxdepth = maxdepth
                return [
                    "hifld-next-staging-prod/119th-congressional-districts/119th-congressional-districts/v20260214/shapefile/119th-congressional-districts.shp",
                    "hifld-next-staging-prod/119th-congressional-districts/119th-congressional-districts/v20260214/shapefile/119th-congressional-districts.dbf",
                ]

        storage = StagingStorageResource(
            bucket="hifld-next-staging-prod",
            use_local=False,
            local_dir="data/staging",
        )

        with patch.dict(
            "sys.modules",
            {"gcsfs": SimpleNamespace(GCSFileSystem=FakeGCSFileSystem)},
        ):
            versions = list(_iter_staged_version_paths(storage) or [])

        self.assertEqual(
            versions,
            [
                (
                    "119th-congressional-districts",
                    "119th-congressional-districts",
                    "v20260214",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
