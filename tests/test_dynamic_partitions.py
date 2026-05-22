import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from dagster import AssetKey, DagsterInstance, DynamicPartitionsDefinition, build_sensor_context

from dagster_hifld.assets import catalog as catalog_assets_module
from dagster_hifld.assets import publish as publish_assets_module
from dagster_hifld.assets.census_blocks_2020 import (
    CENSUS_2020_BLOCKS_DATASET_SLUG,
    CENSUS_2020_TABBLOCK20_FILE_SLUGS,
)
import dagster_hifld.definitions as definitions_module
from dagster_hifld.definitions import _iter_staged_version_paths, defs, version_discovery_sensor
from dagster_hifld.partitions import (
    ALL_PARTITIONS_DEFS,
    PUBLISH_PARTITION_NAME,
    PUBLISH_PARTITIONS,
    build_publish_partition_key,
    parse_publish_partition_key,
)
from dagster_hifld.resources import StagingStorageResource


class DynamicPartitionTests(unittest.TestCase):
    def test_k8s_step_executor_requests_one_hundred_gib_scratch_pvc(self):
        definitions_source = (
            Path(__file__).resolve().parents[1] / "src" / "dagster_hifld" / "definitions.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"storage": "100Gi"', definitions_source)
        self.assertIn('"max_concurrent": 1', definitions_source)
        self.assertIn('"pod_template_spec_metadata"', definitions_source)
        self.assertIn('"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"', definitions_source)

    def test_version_discovery_sensor_scans_one_hundred_dataset_slugs_per_tick_by_default(self):
        self.assertEqual(definitions_module.MAX_SENSOR_DATASETS_PER_TICK, 100)

    def test_catalog_and_publish_assets_use_dynamic_version_partitions(self):
        catalog_asset = catalog_assets_module.catalog_assets[0]
        publish_asset = next(
            asset
            for asset in publish_assets_module.publish_assets
            if asset.key == AssetKey(["publish", "formats", "geoparquet"])
        )

        self.assertIsInstance(PUBLISH_PARTITIONS, DynamicPartitionsDefinition)
        self.assertEqual(catalog_asset.key, AssetKey(["publish", "catalog"]))
        self.assertIs(catalog_asset.partitions_def, PUBLISH_PARTITIONS)
        self.assertIs(publish_asset.partitions_def, PUBLISH_PARTITIONS)
        self.assertEqual(ALL_PARTITIONS_DEFS, [PUBLISH_PARTITIONS])

    def test_publish_partition_key_round_trips_dataset_file_version(self):
        partition_key = build_publish_partition_key(
            "119th-congressional-districts",
            "119th-congressional-districts",
            "v1.0.0",
        )

        self.assertEqual(
            partition_key,
            "119th-congressional-districts/119th-congressional-districts/v1.0.0",
        )
        self.assertEqual(
            parse_publish_partition_key(partition_key),
            (
                "119th-congressional-districts",
                "119th-congressional-districts",
                "v1.0.0",
            ),
        )

    def test_2020_census_blocks_tabblock20_ingest_registry_still_exists(self):
        self.assertEqual(len(CENSUS_2020_TABBLOCK20_FILE_SLUGS), 56)
        self.assertIn("tl_2024_01_tabblock20", CENSUS_2020_TABBLOCK20_FILE_SLUGS)
        self.assertEqual(CENSUS_2020_BLOCKS_DATASET_SLUG, "2020-census-blocks-1")

    def test_generic_catalog_and_publish_assets_registered_once(self):
        catalog_keys = {a.key for a in catalog_assets_module.catalog_assets}
        publish_keys = {a.key for a in publish_assets_module.publish_assets}
        self.assertEqual(catalog_keys, {AssetKey(["publish", "catalog"])})
        self.assertIn(AssetKey(["publish", "formats", "geoparquet"]), publish_keys)
        self.assertIn(AssetKey(["publish", "promote"]), publish_keys)
        self.assertFalse(any(key.path[:2] == ["publish", "register"] for key in publish_keys))

    def test_supported_assets_do_not_drive_publish_asset_count(self):
        self.assertEqual(len(catalog_assets_module.catalog_assets), 1)
        self.assertEqual(len(publish_assets_module.publish_assets), 5)

    def test_version_discovery_sensor_discovers_missing_catalog_version_without_requesting_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_root = (
                Path(tmpdir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "v1.0.0"
                / "geopackage"
            )
            version_root.mkdir(parents=True)
            (version_root / "stations.gpkg").write_bytes(b"gpkg")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ):
                result = version_discovery_sensor(context)

        self.assertEqual(len(result.run_requests), 0)
        partition_key = "amtrak-stations/amtrak-stations/v1.0.0"
        self.assertEqual(
            result.dynamic_partitions_requests[0].partitions_def_name,
            PUBLISH_PARTITION_NAME,
        )
        self.assertIn(
            partition_key,
            result.dynamic_partitions_requests[0].partition_keys,
        )

    def test_version_discovery_sensor_discovers_staged_pair_without_ingest_asset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_root = (
                Path(tmpdir)
                / "staged-only-dataset"
                / "staged-only-file"
                / "v1.0.0"
                / "geopackage"
            )
            version_root.mkdir(parents=True)
            (version_root / "source.gpkg").write_bytes(b"gpkg")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ):
                result = version_discovery_sensor(context)

        partition_key = "staged-only-dataset/staged-only-file/v1.0.0"
        self.assertEqual(len(result.run_requests), 0)
        self.assertIn(
            partition_key,
            result.dynamic_partitions_requests[0].partition_keys,
        )

    def test_version_discovery_sensor_registers_partition_when_catalog_metadata_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_root = (
                Path(tmpdir)
                / "already-cataloged"
                / "already-cataloged"
                / "v1.0.0"
            )
            source_root = version_root / "geopackage"
            metadata_root = version_root / "metadata"
            source_root.mkdir(parents=True)
            metadata_root.mkdir()
            (source_root / "source.gpkg").write_bytes(b"gpkg")
            (metadata_root / "quality_manifest.json").write_text("{}", encoding="utf-8")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ):
                result = version_discovery_sensor(context)

        partition_key = "already-cataloged/already-cataloged/v1.0.0"
        self.assertIn(
            partition_key,
            result.dynamic_partitions_requests[0].partition_keys,
        )
        self.assertEqual(
            result.skip_reason.skip_message,
            "Discovered 1 new publish partitions.",
        )

    def test_version_discovery_sensor_evaluates_without_target_job_or_run_requests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_root = (
                Path(tmpdir)
                / "targeted-sensor"
                / "targeted-sensor"
                / "v1.0.0"
                / "geopackage"
            )
            version_root.mkdir(parents=True)
            (version_root / "source.gpkg").write_bytes(b"gpkg")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            with DagsterInstance.local_temp() as instance:
                context = build_sensor_context(
                    instance=instance,
                    instance_ref=instance.get_ref(),
                    definitions=defs,
                )
                with patch(
                    "dagster_hifld.definitions.StagingStorageResource.from_env",
                    return_value=staging_storage,
                ):
                    result = version_discovery_sensor.evaluate_tick(context)

        self.assertEqual(len(result.run_requests), 0)
        self.assertIn(
            "targeted-sensor/targeted-sensor/v1.0.0",
            result.dynamic_partitions_requests[0].partition_keys,
        )

    def test_version_discovery_sensor_discovers_all_partitions_without_run_request_cap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "amtrak-stations" / "amtrak-stations"
            for version in ("v1.0.0", "v1.0.1", "v1.0.2"):
                version_dir = root / version / "geopackage"
                version_dir.mkdir(parents=True)
                (version_dir / "stations.gpkg").write_bytes(b"gpkg")

            staging_storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            instance = DagsterInstance.ephemeral()
            context = build_sensor_context(instance=instance)

            with patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env",
                return_value=staging_storage,
            ):
                result = version_discovery_sensor(context)

        self.assertEqual(len(result.run_requests), 0)
        self.assertCountEqual(
            result.dynamic_partitions_requests[0].partition_keys,
            [
                "amtrak-stations/amtrak-stations/v1.0.0",
                "amtrak-stations/amtrak-stations/v1.0.1",
                "amtrak-stations/amtrak-stations/v1.0.2",
            ],
        )

    def test_iter_staged_version_paths_detects_gcs_objects(self):
        class FakeBlob:
            def __init__(self, name):
                self.name = name

        class FakeStorageClient:
            def list_blobs(self, bucket, prefix=None, match_glob=None):
                self.bucket = bucket
                self.prefix = prefix
                self.match_glob = match_glob
                if "geopackage" not in match_glob:
                    return []
                return [
                    FakeBlob("119th-congressional-districts/119th-congressional-districts/v20260214/geopackage/legacy.gpkg"),
                    FakeBlob("119th-congressional-districts/119th-congressional-districts/v1.0.0/geopackage/119th-congressional-districts.gpkg"),
                ]

        storage = StagingStorageResource(
            bucket="hifld-next-staging-prod",
            use_local=False,
            local_dir="data/staging",
        )

        with patch.dict(
            "sys.modules",
            {
                "google.cloud.storage": SimpleNamespace(Client=FakeStorageClient),
            },
        ):
            versions = list(_iter_staged_version_paths(storage) or [])

        self.assertEqual(
            versions,
            [
                (
                    "119th-congressional-districts",
                    "119th-congressional-districts",
                    "v1.0.0",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
