import os
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, build_sensor_context

from dagster_hifld.definitions import version_discovery_sensor
from dagster_hifld.assets.portolan import prepare_release_metadata, publish_portolan_catalog
from dagster_hifld.partitions import (
    build_publish_partition_key,
    parse_publish_partition_identity,
    parse_publish_partition_key,
    parse_staging_collections,
    resolve_publish_partition,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class CollectionPartitionTests(unittest.TestCase):
    def test_collection_qualified_keys_disambiguate_without_changing_legacy_parse(self):
        legacy = build_publish_partition_key("dataset", "file", "v1.0.0")
        hifld = build_publish_partition_key("dataset", "file", "v1.0.0", collection="hifld")
        openfema = build_publish_partition_key("dataset", "file", "v1.0.0", collection="openfema")

        self.assertEqual(legacy, "dataset/file/v1.0.0")
        self.assertEqual(hifld, legacy)
        self.assertEqual(openfema, "openfema/dataset/file/v1.0.0")
        self.assertEqual(parse_publish_partition_key(openfema), ("dataset", "file", "v1.0.0"))
        self.assertEqual(parse_publish_partition_identity(openfema).collection, "openfema")

    def test_partition_keys_reject_empty_or_unsafe_collection_segments(self):
        for key in ("/openfema/dataset/file/v1.0.0", "openfema//file/v1.0.0", "../dataset/file/v1.0.0", "openfema/dataset/file/"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                parse_publish_partition_identity(key)

    def test_collection_scan_config_rejects_whitespace_and_path_traversal(self):
        for value in ("hifld, openfema", "hifld,,openfema", "hifld,../openfema"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_staging_collections(value)

    def test_resolve_partition_scopes_staging_and_published_storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = StagingStorageResource(local_dir=f"{tmpdir}/staging", prefix="hifld")
            published = PublishedStorageResource(local_dir=f"{tmpdir}/published", prefix="hifld")
            key = build_publish_partition_key("dataset", "file", "v1.0.0", collection="openfema")

            identity, scoped_staging, scoped_published = resolve_publish_partition(key, staging, published)
            scoped_staging.write_key("dataset/file/v1.0.0/source.txt", b"staged")
            scoped_published.write_key("dataset/file/v1.0.0/source.txt", b"published")

            self.assertEqual(identity.collection, "openfema")
            self.assertTrue(Path(tmpdir, "staging/openfema/dataset/file/v1.0.0/source.txt").exists())
            self.assertTrue(Path(tmpdir, "published/openfema/dataset/file/v1.0.0/source.txt").exists())

            with patch.dict(os.environ, {"HIFLD_STAGING_COLLECTIONS": "hifld,openfema"}):
                _, legacy_staging, legacy_published = resolve_publish_partition(
                    "dataset/file/v1.0.0", staging, published
                )
            self.assertEqual(legacy_staging.prefix, "hifld")
            self.assertEqual(legacy_published.prefix, "hifld")

    def test_opt_in_discovery_finds_versions_in_selected_collections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for collection in ("hifld", "openfema"):
                path = Path(tmpdir, collection, "dataset", "file", "v1.0.0", "geopackage")
                path.mkdir(parents=True)
                (path / "data.gpkg").write_bytes(b"gpkg")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            context = build_sensor_context(instance=DagsterInstance.ephemeral())
            with patch.dict(os.environ, {"HIFLD_STAGING_COLLECTIONS": "hifld,openfema"}), patch(
                "dagster_hifld.definitions.StagingStorageResource.from_env", return_value=storage
            ):
                result = version_discovery_sensor(context)

        keys = set(result.dynamic_partitions_requests[0].partition_keys)
        self.assertEqual(keys, {"dataset/file/v1.0.0", "openfema/dataset/file/v1.0.0"})

    def test_gcs_collection_cursor_finishes_each_collection_before_advancing(self):
        storage = StagingStorageResource(bucket="staging", use_local=False, prefix="legacy")
        same_version = [("dataset", "file", "v1.0.0")]
        tick_results = iter(((same_version, "10"), ([], "0"), (same_version, "0")))
        instance = DagsterInstance.ephemeral()
        with patch.dict(os.environ, {"HIFLD_STAGING_COLLECTIONS": "hifld,openfema"}), patch(
            "dagster_hifld.definitions.StagingStorageResource.from_env", return_value=storage
        ), patch(
            "dagster_hifld.definitions._iter_staged_version_paths_for_gcs_tick",
            side_effect=lambda scoped, cursor: next(tick_results),
        ) as list_tick:
            first = version_discovery_sensor(build_sensor_context(instance=instance))
            second = version_discovery_sensor(
                build_sensor_context(instance=instance, cursor="collections:0:10")
            )
            third = version_discovery_sensor(
                build_sensor_context(instance=instance, cursor="collections:1:0")
            )

        self.assertEqual(
            [(call.args[0].prefix, call.args[1]) for call in list_tick.call_args_list],
            [("hifld", "0"), ("hifld", "10"), ("openfema", "0")],
        )
        self.assertEqual(first.dynamic_partitions_requests[0].partition_keys, ["dataset/file/v1.0.0"])
        self.assertEqual(second.dynamic_partitions_requests, [])
        self.assertEqual(third.dynamic_partitions_requests[0].partition_keys, ["openfema/dataset/file/v1.0.0"])
        self.assertEqual(first.cursor, "collections:0:10")
        self.assertEqual(second.cursor, "collections:1:0")

    def test_gcs_collection_cursor_resets_malformed_or_negative_offsets(self):
        storage = StagingStorageResource(bucket="staging", use_local=False)
        with patch.dict(os.environ, {"HIFLD_STAGING_COLLECTIONS": "hifld,openfema"}), patch(
            "dagster_hifld.definitions.StagingStorageResource.from_env", return_value=storage
        ), patch(
            "dagster_hifld.definitions._iter_staged_version_paths_for_gcs_tick",
            return_value=([], "0"),
        ) as list_tick:
            version_discovery_sensor(
                build_sensor_context(
                    instance=DagsterInstance.ephemeral(), cursor="collections:no:-2"
                )
            )

        self.assertEqual(list_tick.call_args.args[0].prefix, "hifld")
        self.assertEqual(list_tick.call_args.args[1], "0")

    def test_multi_collection_cursor_resets_after_collection_config_is_removed_or_reduced(self):
        for configured, expected_prefix in ((None, "legacy"), ("hifld", "hifld")):
            with self.subTest(configured=configured):
                storage = StagingStorageResource(bucket="staging", use_local=False, prefix="legacy")
                env = {} if configured is None else {"HIFLD_STAGING_COLLECTIONS": configured}
                with patch.dict(os.environ, env, clear=True), patch(
                    "dagster_hifld.definitions.StagingStorageResource.from_env",
                    return_value=storage,
                ), patch(
                    "dagster_hifld.definitions._iter_staged_version_paths_for_gcs_tick",
                    return_value=([("dataset", "file", "v1.0.0")], "0"),
                ) as list_tick:
                    result = version_discovery_sensor(
                        build_sensor_context(
                            instance=DagsterInstance.ephemeral(),
                            cursor="collections:1:10",
                        )
                    )

                self.assertEqual(list_tick.call_args.args[0].prefix, expected_prefix)
                self.assertEqual(list_tick.call_args.args[1], "0")
                self.assertEqual(
                    result.dynamic_partitions_requests[0].partition_keys,
                    ["dataset/file/v1.0.0"],
                )

    def test_release_request_uses_partition_collection_and_scoped_storage(self):
        from dagster_hifld.portolan import workflow

        staging = StagingStorageResource(local_dir="/tmp/staging", prefix="hifld")
        published = PublishedStorageResource(local_dir="/tmp/published", prefix="hifld")
        context = SimpleNamespace(
            partition_key="openfema/dataset/file/v1.0.0",
            op_config={"version": "v1.0.1"},
        )
        with patch.dict(os.environ, {"HIFLD_STAGING_COLLECTIONS": "hifld,openfema"}), patch.object(
            workflow, "prepare_staged_release", return_value={"version": "v1.0.1"}
        ) as prepare:
            prepare_release_metadata.op.compute_fn.decorated_fn(context, staging, published)

        request, scoped_staging, scoped_published = prepare.call_args.args
        self.assertEqual(request.collection_slug, "openfema")
        self.assertEqual(scoped_staging.prefix, "openfema")
        self.assertEqual(scoped_published.prefix, "openfema")

    def test_openfema_catalog_request_does_not_inherit_hifld_collection_title(self):
        from dagster_hifld.portolan import workflow

        staging = StagingStorageResource(local_dir="/tmp/staging")
        published = PublishedStorageResource(local_dir="/tmp/published")
        context = SimpleNamespace(
            partition_key="openfema/dataset/file/v1.0.0",
            op_config={"mode": "prepare", "plan_dir": "/tmp/openfema-plan"},
        )
        with patch.dict(
            os.environ,
            {
                "HIFLD_STAGING_COLLECTIONS": "hifld,openfema",
                "HIFLD_PORTOLAN_ENABLED": "1",
                "HIFLD_PORTOLAN_COLLECTION_TITLE": "HIFLD",
            },
        ), patch(
            "dagster_hifld.assets.portolan.read_staged_release",
            return_value=({"id": "openfema/dataset/file/v1.0.0"}, "dataset/file/v1.0.1"),
        ), patch(
            "dagster_hifld.assets.portolan.require_generated_object_approval"
        ), patch(
            "dagster_hifld.resources.StagingStorageResource.read_key",
            return_value=b"collection",
        ), patch.object(
            workflow, "prepare_promoted_portolan_catalog", return_value={"generation": "g1"}
        ) as prepare:
            publish_portolan_catalog.node_def.compute_fn.decorated_fn(
                context, staging, published
            )

        self.assertEqual(prepare.call_args.args[0].collection_slug, "openfema")
        self.assertIsNone(prepare.call_args.args[0].collection_title)


if __name__ == "__main__":
    unittest.main()
