import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.assets.publish import (
    _copy_metadata_files,
    _copy_source_format_files,
    _copy_source_files,
    _copy_version_files,
    _geoparquet_glob_and_hive_status,
    _is_spatial_source_layer,
    _prepare_format_publish,
    _preferred_remote_source_keys,
    _published_outputs_from_keys,
    _write_and_publish_geoparquet,
    _write_geoparquet_layout_manifest,
    _write_geoparquet_layout_manifest_set,
    _write_and_publish_pmtiles,
    _write_and_publish_shapefile_zip,
    publish_assets,
)
from dagster_hifld.conversion import (
    GeoParquetWritePolicy,
    ShapefileZipPolicy,
    process_layer_partitioned_geoparquet,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class PublishTests(unittest.TestCase):
    def test_copy_metadata_files_promotes_catalog_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            metadata_root = (
                Path(staging_dir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_a"
                / "metadata"
            )
            metadata_root.mkdir(parents=True)
            (metadata_root / "quality_manifest.json").write_text(
                '{"quality_check_passed": true}', encoding="utf-8"
            )
            (metadata_root / "data_dictionary.json").write_text(
                '{"name": "amtrak-stations", "columns": []}', encoding="utf-8"
            )

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            copied = _copy_metadata_files(
                staging,
                published,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_a",
            )

            self.assertEqual(
                copied,
                [
                    "amtrak-stations/amtrak-stations/run_a/metadata/quality_manifest.json",
                    "amtrak-stations/amtrak-stations/run_a/metadata/data_dictionary.json",
                ],
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "metadata"
                    / "quality_manifest.json"
                ).exists()
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "metadata"
                    / "data_dictionary.json"
                ).exists()
            )

    def test_build_format_assets_includes_promote_assets(self):
        self.assertTrue(
            any(asset.key.path == ["publish", "promote"] for asset in publish_assets)
        )
        self.assertTrue(
            any(asset.key.path == ["publish", "formats", "geoparquet"] for asset in publish_assets)
        )
        self.assertFalse(any(asset.key.path == ["publish", "promote", "source_files"] for asset in publish_assets))
        self.assertFalse(any(asset.key.path == ["publish", "promote", "metadata"] for asset in publish_assets))
        self.assertFalse(any(asset.key.path[:2] == ["publish", "register"] for asset in publish_assets))

    def test_copy_version_files_promotes_canonical_formats_but_not_legacy_unknown(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            (version_root / "geopackage").mkdir(parents=True)
            (version_root / "geopackage" / "source.gpkg").write_bytes(b"gpkg")
            (version_root / "geojson").mkdir()
            (version_root / "geojson" / "source.geojson").write_bytes(b"geojson")
            (version_root / "unknown").mkdir()
            (version_root / "unknown" / "legacy.shp").write_bytes(b"legacy")
            (version_root / "geoparquet").mkdir()
            (version_root / "geoparquet" / "old.parquet").write_bytes(b"parquet")
            (version_root / "metadata").mkdir()
            (version_root / "metadata" / "quality_manifest.json").write_bytes(b"metadata")

            copied = _copy_version_files(
                StagingStorageResource(local_dir=staging_dir, use_local=True),
                PublishedStorageResource(local_dir=published_dir, use_local=True),
                "dataset-a",
                "file-a",
                "v1.0.0",
            )

            self.assertEqual(
                copied,
                [
                    "dataset-a/file-a/v1.0.0/geojson/source.geojson",
                    "dataset-a/file-a/v1.0.0/geopackage/source.gpkg",
                    "dataset-a/file-a/v1.0.0/geoparquet/old.parquet",
                    "dataset-a/file-a/v1.0.0/metadata/quality_manifest.json",
                ],
            )
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/geopackage/source.gpkg").exists())
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/geoparquet/old.parquet").exists())
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/metadata/quality_manifest.json").exists())
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/geojson").exists())
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/unknown").exists())

    def test_copy_version_files_uses_bulk_copy(self):
        class FakeStaging:
            def __init__(self):
                self.bulk_keys = None

            def list_keys(self, dataset_slug, file_slug, version):
                return [
                    f"{dataset_slug}/{file_slug}/{version}/metadata/quality_manifest.json",
                    f"{dataset_slug}/{file_slug}/{version}/geoparquet/source.parquet",
                ]

            def copy_keys_to(self, destination, keys, *, destination_keys=None):
                self.bulk_keys = list(keys)
                self.destination_keys = list(destination_keys or keys)
                return [f"copied/{Path(key).name}" for key in keys]

        class FakePublished:
            def __init__(self):
                self.deleted = None

            def build_target_location(
                self, dataset_slug, file_slug, version, filename
            ):
                return f"{dataset_slug}/{file_slug}/{version}/{filename}".rstrip("/")

            def delete_prefix(self, prefix):
                self.deleted = prefix

        staging = FakeStaging()
        published = FakePublished()

        copied = _copy_version_files(staging, published, "dataset-a", "file-a", "v1.0.0")

        self.assertEqual(published.deleted, "dataset-a/file-a/v1.0.0")
        self.assertEqual(
            staging.bulk_keys,
            [
                "dataset-a/file-a/v1.0.0/metadata/quality_manifest.json",
                "dataset-a/file-a/v1.0.0/geoparquet/source.parquet",
            ],
        )
        self.assertEqual(copied, ["copied/quality_manifest.json", "copied/source.parquet"])

    def test_copy_source_format_files_promotes_only_canonical_staged_source_formats(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = (
                Path(staging_dir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_a"
            )
            (version_root / "shapefile").mkdir(parents=True)
            (version_root / "file_geodatabase" / "stations.gdb").mkdir(parents=True)
            (version_root / "metadata").mkdir(parents=True)
            (version_root / "shapefile" / "stations.shp").write_bytes(b"shape")
            (version_root / "file_geodatabase" / "stations.gdb" / "a00000001.gdbtable").write_bytes(
                b"gdb"
            )
            (version_root / "metadata" / "quality_manifest.json").write_text(
                "{}", encoding="utf-8"
            )

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("amtrak-stations", "amtrak-stations", "run_a")

            copied = _copy_source_format_files(
                staging,
                published,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_a",
                keys=keys,
            )

            self.assertEqual(
                sorted(copied),
                [
                    "amtrak-stations/amtrak-stations/run_a/file_geodatabase/stations.gdb/a00000001.gdbtable",
                    "amtrak-stations/amtrak-stations/run_a/shapefile/stations.shp",
                ],
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "shapefile"
                    / "stations.shp"
                ).exists()
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "file_geodatabase"
                    / "stations.gdb"
                    / "a00000001.gdbtable"
                ).exists()
            )

    def test_copy_source_format_files_promotes_zipped_file_geodatabase(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            (version_root / "file_geodatabase").mkdir(parents=True)
            (version_root / "file_geodatabase" / "source.gdb.zip").write_bytes(b"zip")
            (version_root / "shapefile").mkdir()
            (version_root / "shapefile" / "source.shp").write_bytes(b"shape")

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("dataset-a", "file-a", "v1.0.0")

            copied = _copy_source_format_files(
                staging,
                published,
                dataset_slug="dataset-a",
                file_slug="file-a",
                version="v1.0.0",
                keys=keys,
            )

            self.assertEqual(
                copied,
                [
                    "dataset-a/file-a/v1.0.0/file_geodatabase/source.gdb.zip",
                    "dataset-a/file-a/v1.0.0/shapefile/source.shp",
                ],
            )
            self.assertTrue(
                (Path(published_dir) / "dataset-a/file-a/v1.0.0/file_geodatabase/source.gdb.zip").exists()
            )
            self.assertTrue(
                (Path(published_dir) / "dataset-a/file-a/v1.0.0/shapefile/source.shp").exists()
            )

    def test_copy_source_format_files_promotes_retained_shapefile_with_sidecars(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            (version_root / "shapefile").mkdir(parents=True)
            (version_root / "unknown").mkdir()
            (version_root / "geoparquet").mkdir()
            for suffix in (".shp", ".shx", ".dbf", ".prj"):
                (version_root / "shapefile" / f"source{suffix}").write_bytes(suffix.encode())
            (version_root / "unknown" / "legacy.shp").write_bytes(b"legacy")
            (version_root / "geoparquet" / "source.parquet").write_bytes(b"derived")

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("dataset-a", "file-a", "v1.0.0")

            copied = _copy_source_format_files(
                staging,
                published,
                "dataset-a",
                "file-a",
                "v1.0.0",
                keys,
            )

            self.assertEqual(
                copied,
                [
                    f"dataset-a/file-a/v1.0.0/shapefile/source{suffix}"
                    for suffix in (".dbf", ".prj", ".shp", ".shx")
                ],
            )
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/unknown").exists())
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/geoparquet").exists())

    def test_copy_source_format_files_canonicalizes_valid_legacy_unknown_shapefile(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            unknown = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "unknown"
            unknown.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(unknown / "source.shp")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("dataset-a", "file-a", "v1.0.0")

            copied = _copy_source_format_files(
                staging,
                published,
                "dataset-a",
                "file-a",
                "v1.0.0",
                keys,
            )

            self.assertTrue(copied)
            self.assertTrue(all("/shapefile/source." in key for key in copied))
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/unknown").exists())
            self.assertTrue(
                (Path(published_dir) / "dataset-a/file-a/v1.0.0/shapefile/source.shp").exists()
            )

    def test_copy_version_files_canonicalizes_legacy_unknown_without_copying_unknown(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            unknown = version_root / "unknown"
            unknown.mkdir(parents=True)
            (version_root / "metadata").mkdir()
            (version_root / "metadata" / "quality_manifest.json").write_bytes(b"{}")
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(unknown / "source.shp")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            copied = _copy_version_files(staging, published, "dataset-a", "file-a", "v1.0.0")

            self.assertIn("dataset-a/file-a/v1.0.0/shapefile/source.shp", copied)
            self.assertIn("dataset-a/file-a/v1.0.0/metadata/quality_manifest.json", copied)
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/unknown").exists())

    def test_copy_version_files_normalizes_different_storage_prefixes(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(
                local_dir=staging_dir,
                use_local=True,
                prefix="staged-prefix",
            )
            published = PublishedStorageResource(
                local_dir=published_dir,
                use_local=True,
                prefix="published-prefix",
            )
            staging.write("dataset-a", "file-a", "v1.0.0", "geopackage/source.gpkg", b"gpkg")
            staging.write("dataset-a", "file-a", "v1.0.0", "unknown/leak.txt", b"legacy")

            copied = _copy_version_files(staging, published, "dataset-a", "file-a", "v1.0.0")

            self.assertEqual(
                copied,
                ["published-prefix/dataset-a/file-a/v1.0.0/geopackage/source.gpkg"],
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "published-prefix/dataset-a/file-a/v1.0.0/geopackage/source.gpkg"
                ).exists()
            )
            self.assertFalse((Path(published_dir) / "published-prefix/staged-prefix").exists())
            self.assertFalse(
                (Path(published_dir) / "published-prefix/dataset-a/file-a/v1.0.0/unknown").exists()
            )

    def test_copy_source_files_normalizes_prefixed_canonical_shapefile_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(
                local_dir=staging_dir,
                use_local=True,
                prefix="staged-prefix",
            )
            published = PublishedStorageResource(
                local_dir=published_dir,
                use_local=True,
                prefix="published-prefix",
            )
            for suffix in (".shp", ".shx", ".dbf"):
                staging.write(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    f"shapefile/source{suffix}",
                    suffix.encode(),
                )

            outputs = _copy_source_files(
                staging,
                published,
                "dataset-a",
                "file-a",
                "v1.0.0",
            )

            self.assertEqual({output.format_type for output in outputs}, {"shapefile"})
            self.assertTrue(
                all(
                    output.path.startswith(
                        "published-prefix/dataset-a/file-a/v1.0.0/shapefile/"
                    )
                    for output in outputs
                )
            )
            self.assertFalse((Path(published_dir) / "published-prefix/staged-prefix").exists())

    def test_legacy_canonicalization_copies_only_listed_sidecars_without_materializing_version(self):
        class FakeStaging:
            prefix = "staged-prefix"

            def copy_keys_to(self, destination, keys, *, destination_keys=None):
                self.copied_keys = list(keys)
                self.destination_keys = list(destination_keys or [])
                if any("geoparquet" in key or "pmtiles" in key for key in keys):
                    raise AssertionError("derived artifacts must not be copied as legacy sidecars")
                return [f"published-prefix/{key}" for key in self.destination_keys]

            def get_local_version_dir(self, *args, **kwargs):
                raise AssertionError("legacy canonicalization must not materialize the version")

            def read_bytes(self, *args, **kwargs):
                raise AssertionError("legacy canonicalization must use storage-side copies")

        staging = FakeStaging()
        published = Mock(prefix="published-prefix")
        keys = [
            "staged-prefix/dataset-a/file-a/v1.0.0/unknown/nested/source.shp",
            "staged-prefix/dataset-a/file-a/v1.0.0/unknown/nested/source.shx",
            "staged-prefix/dataset-a/file-a/v1.0.0/unknown/nested/source.dbf",
            "staged-prefix/dataset-a/file-a/v1.0.0/geoparquet/part.parquet",
            "staged-prefix/dataset-a/file-a/v1.0.0/pmtiles/source.pmtiles",
        ]

        copied = _copy_source_format_files(
            staging,
            published,
            "dataset-a",
            "file-a",
            "v1.0.0",
            keys,
        )

        self.assertEqual(
            getattr(staging, "copied_keys", []),
            [keys[2], keys[0], keys[1]],
        )
        self.assertEqual(
            staging.destination_keys,
            [
                "dataset-a/file-a/v1.0.0/shapefile/nested/source.dbf",
                "dataset-a/file-a/v1.0.0/shapefile/nested/source.shp",
                "dataset-a/file-a/v1.0.0/shapefile/nested/source.shx",
            ],
        )
        self.assertEqual(len(copied), 3)

    def test_copy_source_format_files_reports_and_skips_ambiguous_legacy_unknown(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            unknown = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "unknown"
            unknown.mkdir(parents=True)
            for name in ("source-a", "source-b"):
                gpd.GeoDataFrame(
                    {"name": [name]},
                    geometry=[Point(0, 0)],
                    crs="EPSG:4326",
                ).to_file(unknown / f"{name}.shp")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("dataset-a", "file-a", "v1.0.0")

            with self.assertLogs("dagster_hifld.assets.publish", level="WARNING") as logs:
                copied = _copy_source_format_files(
                    staging,
                    published,
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    keys,
                )

            self.assertEqual(copied, [])
            self.assertIn("multiple Shapefile datasets", " ".join(logs.output))
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0").exists())

    def test_shapefile_publish_preserves_canonical_source_and_sidecars(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            shapefile_dir = (
                Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "shapefile"
            )
            shapefile_dir.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(shapefile_dir / "source.shp")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            original_keys = staging.list_keys("dataset-a", "file-a", "v1.0.0")

            outputs = _write_and_publish_shapefile_zip(
                staging,
                "dataset-a",
                "file-a",
                "v1.0.0",
            )

            self.assertEqual(
                staging.list_keys("dataset-a", "file-a", "v1.0.0"),
                original_keys,
            )
            self.assertCountEqual(
                [output.path for output in outputs],
                original_keys,
            )

    def test_preferred_remote_source_keys_uses_canonical_precedence(self):
        staging = Mock()
        staging.list_keys.return_value = [
            "dataset/file/v1.0.0/pmtiles/source.pmtiles",
            "dataset/file/v1.0.0/file_geodatabase/source.gdb.zip",
            "dataset/file/v1.0.0/geopackage/source.gpkg",
            "dataset/file/v1.0.0/geoparquet/source.parquet",
        ]

        selected_format, selected_keys = _preferred_remote_source_keys(
            staging, "dataset", "file", "v1.0.0"
        )

        self.assertEqual(selected_format, "geopackage")
        self.assertEqual(selected_keys, ["dataset/file/v1.0.0/geopackage/source.gpkg"])

    def test_existing_format_outputs_are_overwritten_by_default(self):
        with tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            version_root.mkdir(parents=True)
            (version_root / "file-a.parquet").write_bytes(b"parquet")

            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            existing = _prepare_format_publish(
                published,
                "dataset-a",
                "file-a",
                "v1.0.0",
                "geoparquet",
            )

            self.assertEqual(existing, [])
            self.assertFalse(version_root.exists())

    def test_existing_format_outputs_can_be_reused_when_overwrite_disabled(self):
        with tempfile.TemporaryDirectory() as published_dir:
            version_root = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            version_root.mkdir(parents=True)
            (version_root / "file-a.parquet").write_bytes(b"parquet")

            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            with unittest.mock.patch.dict("os.environ", {"HIFLD_PUBLISH_OVERWRITE": "false"}):
                existing = _prepare_format_publish(
                    published,
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "geoparquet",
                )

            self.assertEqual(existing, ["dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"])
            self.assertTrue(version_root.exists())

    def test_existing_multi_file_geoparquet_is_registered_as_recursive_glob(self):
        keys = [
            "dataset-a/file-a/v1.0.0/geoparquet/file-a-0.zstd.parquet",
            "dataset-a/file-a/v1.0.0/geoparquet/file-a-1.zstd.parquet",
        ]

        outputs = _published_outputs_from_keys("dataset-a", "file-a", "v1.0.0", keys)

        self.assertEqual(len(outputs), 1)
        self.assertEqual(
            outputs[0].path,
            "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
        )
        self.assertFalse(outputs[0].source_metadata["hive_partitioned"])

    def test_existing_named_nested_geoparquet_is_not_marked_hive_partitioned(self):
        keys = [
            "dataset-a/file-a/v1.0.0/geoparquet/layer-roads/file-a.parquet"
        ]

        outputs = _published_outputs_from_keys(
            "dataset-a", "file-a", "v1.0.0", keys
        )

        self.assertEqual(
            outputs[0].path,
            "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
        )
        self.assertFalse(outputs[0].source_metadata["hive_partitioned"])

    def test_prefixed_partitioned_geoparquet_is_registered_with_prefixed_glob(self):
        keys = [
            "published-prefix/dataset-a/file-a/v1.0.0/geoparquet/state=MD/part.parquet",
            "published-prefix/dataset-a/file-a/v1.0.0/geoparquet/state=VA/part.parquet",
        ]

        outputs = _published_outputs_from_keys("dataset-a", "file-a", "v1.0.0", keys)

        self.assertEqual(len(outputs), 1)
        self.assertEqual(
            outputs[0].path,
            "published-prefix/dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
        )

    def test_overwrite_deletes_only_selected_format_prefix(self):
        with tempfile.TemporaryDirectory() as published_dir:
            gpq_root = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            pmtiles_root = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0" / "pmtiles"
            gpq_root.mkdir(parents=True)
            pmtiles_root.mkdir(parents=True)
            (gpq_root / "file-a.parquet").write_bytes(b"parquet")
            (pmtiles_root / "file-a.pmtiles").write_bytes(b"pmtiles")

            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            existing = _prepare_format_publish(
                published,
                "dataset-a",
                "file-a",
                "v1.0.0",
                "geoparquet",
            )

            self.assertEqual(existing, [])
            self.assertFalse(gpq_root.exists())
            self.assertTrue((pmtiles_root / "file-a.pmtiles").exists())

    def test_spatial_source_layer_rejects_all_null_geometry_sample(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame({"name": ["A", "B"]}, geometry=[None, None], crs="EPSG:4326")
            gdf.to_file(path, driver="GeoJSON")

            self.assertFalse(_is_spatial_source_layer(path, "geojson", None))

    def test_geospatial_format_assets_skip_all_null_geometry_source(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[None], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            self.assertEqual(
                _write_and_publish_geoparquet(staging, "dataset-a", "file-a", "v1.0.0")[0].source_metadata,
                {"skip_reason": "non_spatial_source"},
            )
            self.assertEqual(
                _write_and_publish_pmtiles(staging, "dataset-a", "file-a", "v1.0.0")[0].source_metadata,
                {"skip_reason": "non_spatial_source"},
            )
            self.assertEqual(
                _write_and_publish_shapefile_zip(staging, "dataset-a", "file-a", "v1.0.0")[0].source_metadata,
                {"skip_reason": "non_spatial_source"},
            )

    def test_shapefile_zip_skips_disabled_dataset_family_before_full_read(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_dir = Path(staging_dir) / "nfhl" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            (version_dir / "source.geojson").write_text(
                '{"type":"FeatureCollection","features":[]}',
                encoding="utf-8",
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            with patch("dagster_hifld.assets.publish._read_layer") as read_layer:
                output = _write_and_publish_shapefile_zip(
                    staging,
                    "nfhl",
                    "file-a",
                    "v1.0.0",
                    ShapefileZipPolicy(disabled_dataset_families=("nfhl",)),
                )[0]

            read_layer.assert_not_called()
            self.assertEqual(output.source_metadata, {"skip_reason": "disabled_dataset_family"})

    def test_shapefile_zip_skips_oversized_source_before_materializing_version(self):
        class FakeStorage:
            def list_keys(self, dataset_slug, file_slug, version):
                return [f"{dataset_slug}/{file_slug}/{version}/geojson/source.geojson"]

            def delete_prefix(self, prefix):
                pass

            def key_size(self, key):
                return 10 * 1024 * 1024

            def get_local_version_dir(self, dataset_slug, file_slug, version):
                raise AssertionError("shapefile skip should not download the version")

        output = _write_and_publish_shapefile_zip(
            FakeStorage(),
            "small-family",
            "file-a",
            "v1.0.0",
            ShapefileZipPolicy(max_estimated_zip_bytes=1024),
        )[0]

        self.assertEqual(output.source_metadata, {"skip_reason": "estimated_size_exceeds_limit"})

    def test_pmtiles_does_not_skip_large_source_before_conversion(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            with patch(
                "dagster_hifld.assets.publish._remote_source_size_bytes",
                return_value=10 * 1024 * 1024 * 1024,
            ), patch(
                "dagster_hifld.assets.publish.process_layer_chunked",
                new_callable=AsyncMock,
            ) as chunked:
                chunked.return_value = {
                    "pmtiles_path": "dataset-a/file-a/v1.0.0/pmtiles/file-a.pmtiles",
                    "feature_count": 1,
                    "dropped_for_pmtiles_count": 0,
                }

                output = _write_and_publish_pmtiles(staging, "dataset-a", "file-a", "v1.0.0")[0]

        chunked.assert_called_once()
        self.assertEqual(output.path, "dataset-a/file-a/v1.0.0/pmtiles/file-a.pmtiles")
        self.assertNotEqual(output.source_metadata, {"skip_reason": "estimated_size_exceeds_limit"})

    def test_pmtiles_does_not_skip_wbd_12_digit_watershed_before_conversion(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "wbd" / "12-digit-hu-subwatershed" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            with patch(
                "dagster_hifld.assets.publish.process_layer_chunked",
                new_callable=AsyncMock,
            ) as chunked:
                chunked.return_value = {
                    "pmtiles_path": "wbd/12-digit-hu-subwatershed/v1.0.0/pmtiles/12-digit-hu-subwatershed.pmtiles",
                    "feature_count": 1,
                    "dropped_for_pmtiles_count": 0,
                }

                output = _write_and_publish_pmtiles(
                    staging,
                    "wbd",
                    "12-digit-hu-subwatershed",
                    "v1.0.0",
                )[0]

        chunked.assert_called_once()
        self.assertEqual(
            output.path,
            "wbd/12-digit-hu-subwatershed/v1.0.0/pmtiles/12-digit-hu-subwatershed.pmtiles",
        )
        self.assertNotEqual(output.source_metadata, {"skip_reason": "disabled_dataset_file"})

    def test_geoparquet_publish_always_uses_streaming_hive_writer(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            with patch(
                "dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
                new_callable=AsyncMock,
            ) as hive_writer:
                hive_writer.return_value = {
                    "geoparquet_paths": [
                        "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"
                    ],
                    "feature_count": 1,
                    "partitioning": "single_file",
                    "partition_columns": [],
                    "hive_partitioned": False,
                    "layout": {
                        "schema_version": 1,
                        "layer": "file-a",
                        "source_format": "geojson",
                        "feature_count": 1,
                        "partition_strategy": "single_file",
                        "partition_columns": [],
                        "chosen_s2_level": None,
                        "thresholds": {"write_buffer_bytes": 117440512},
                        "outputs": [
                            {
                                "relative_path": "geoparquet/file-a.parquet",
                                "file_size_bytes": 100,
                                "footer_size_bytes": 10,
                                "sha256": "abc",
                                "row_counts": [1],
                                "row_group_uncompressed_sizes": [50],
                            }
                        ],
                        "validation_status": "valid",
                    },
                }

                outputs = _write_and_publish_geoparquet(
                    staging,
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                )

            self.assertEqual(outputs[0].format_type, "geoparquet")
            hive_writer.assert_called_once()
            import dagster_hifld.assets.publish as publish_module

            self.assertFalse(hasattr(publish_module, "write_geoparquet_dataset"))
            self.assertEqual(outputs[0].path, "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet")
            self.assertEqual(outputs[0].source_metadata["partitioning"], "single_file")
            self.assertNotIn("-0.zstd.parquet", outputs[0].path)
            manifest = json.loads(
                (
                    Path(staging_dir)
                    / "dataset-a/file-a/v1.0.0/metadata/geoparquet_layout.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["schema_version"], 1)
            self.assertEqual(manifest["validation_status"], "valid")
            self.assertEqual(manifest["layers"][0]["feature_count"], 1)
            self.assertEqual(manifest["layers"][0]["outputs"][0]["sha256"], "abc")

    def test_geoparquet_layout_manifest_merges_layers_and_replaces_matching_layer(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            first = {
                "schema_version": 1,
                "layer": "roads",
                "source_format": "geopackage",
                "feature_count": 2,
                "outputs": [],
                "validation_status": "valid",
            }
            second = {
                "schema_version": 1,
                "layer": "bridges",
                "source_format": "geopackage",
                "feature_count": 3,
                "outputs": [],
                "validation_status": "valid",
            }
            replacement = {**first, "feature_count": 4}

            _write_geoparquet_layout_manifest(
                storage, "dataset", "file", "v1", first
            )
            _write_geoparquet_layout_manifest(
                storage, "dataset", "file", "v1", second
            )
            key = _write_geoparquet_layout_manifest(
                storage, "dataset", "file", "v1", replacement
            )

            self.assertEqual(key, "dataset/file/v1/metadata/geoparquet_layout.json")
            manifest = json.loads(
                storage.read_bytes(
                    "dataset", "file", "v1", "metadata/geoparquet_layout.json"
                )
            )
            self.assertEqual(
                [(layer["layer"], layer["feature_count"]) for layer in manifest["layers"]],
                [("bridges", 3), ("roads", 4)],
            )

    def test_geoparquet_layout_manifest_accepts_footer_budget_under_cap(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            layout = {
                "layer": "roads",
                "source_format": "geojson",
                "validation_status": "valid",
                "outputs": [{"footer_size_bytes": 128 * 1024**2}],
            }
            key = _write_geoparquet_layout_manifest_set(
                storage, "dataset", "file", "v1", [layout]
            )
            self.assertEqual(key, "dataset/file/v1/metadata/geoparquet_layout.json")

    def test_geoparquet_layout_manifest_rejects_aggregate_footer_budget_over_cap(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            layout = {
                "layer": "roads",
                "source_format": "geojson",
                "validation_status": "valid",
                "outputs": [{"footer_size_bytes": 128 * 1024**2 + 1}],
            }
            with self.assertRaisesRegex(ValueError, "footer"):
                _write_geoparquet_layout_manifest_set(
                    storage, "dataset", "file", "v1", [layout]
                )

    def test_geoparquet_footer_budget_failure_cleans_version_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset"
                / "file"
                / "v1"
                / "geojson"
            )
            version_dir.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(version_dir / "source.geojson", driver="GeoJSON")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            output_path = "dataset/file/v1/geoparquet/file.parquet"
            layout = {
                "schema_version": 1,
                "layer": "file",
                "source_format": "geojson",
                "feature_count": 1,
                "partition_strategy": "single_file",
                "partition_columns": [],
                "hive_partition_columns": {},
                "chosen_s2_level": None,
                "thresholds": {},
                "outputs": [
                    {
                        "path": output_path,
                        "relative_path": "geoparquet/file.parquet",
                        "file_size_bytes": 1,
                        "footer_size_bytes": 128 * 1024**2 + 1,
                        "sha256": "a" * 64,
                        "row_counts": [1],
                        "row_group_uncompressed_sizes": [1],
                    }
                ],
                "validation_status": "valid",
            }
            with patch(
                "dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
                new_callable=AsyncMock,
            ) as writer:
                writer.return_value = {
                    "geoparquet_paths": [output_path],
                    "feature_count": 1,
                    "partitioning": "single_file",
                    "partition_columns": [],
                    "hive_partitioned": False,
                    "layout": layout,
                }
                with self.assertRaisesRegex(ValueError, "footer"):
                    _write_and_publish_geoparquet(storage, "dataset", "file", "v1")

            self.assertFalse(
                any(
                    key.startswith("dataset/file/v1/geoparquet/")
                    or key.endswith("geoparquet_layout.json")
                    for key in storage.list_keys("dataset", "file", "v1")
                )
            )

    def test_partitioned_multilayer_publish_uses_collision_proof_layer_paths(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads", driver="GPKG")
            gpd.GeoDataFrame(
                {"name": ["bridge"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="bridges", driver="GPKG", mode="a")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            outputs = _write_and_publish_geoparquet(
                storage,
                "dataset-a",
                "file-a",
                "v1.0.0",
                GeoParquetWritePolicy(force_s2=True),
            )

            parquet_keys = sorted(
                key
                for key in storage.list_keys("dataset-a", "file-a", "v1.0.0")
                if key.endswith(".parquet")
            )
            manifest = json.loads(
                storage.read_bytes(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )
            layer_outputs = [layer["outputs"][0] for layer in manifest["layers"]]

            self.assertEqual(len(outputs), 2)
            self.assertEqual(
                {output.path for output in outputs},
                {"dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet"},
            )
            self.assertEqual(len(parquet_keys), 2)
            self.assertEqual(len({output["relative_path"] for output in layer_outputs}), 2)
            self.assertEqual([layer["feature_count"] for layer in manifest["layers"]], [1, 1])
            self.assertTrue(all(output["row_counts"] == [1] for output in layer_outputs))
            self.assertEqual(len({output["sha256"] for output in layer_outputs}), 2)

    def test_partitioned_multilayer_publish_uses_original_layer_names_for_namespaces(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["slash"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads/east", driver="GPKG")
            gpd.GeoDataFrame(
                {"name": ["dash"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads-east", driver="GPKG", mode="a")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            _write_and_publish_geoparquet(
                storage,
                "dataset-a",
                "file-a",
                "v1.0.0",
                GeoParquetWritePolicy(force_s2=True),
            )

            parquet_keys = sorted(
                key
                for key in storage.list_keys("dataset-a", "file-a", "v1.0.0")
                if key.endswith(".parquet")
            )
            manifest = json.loads(
                storage.read_bytes(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )

            self.assertEqual(len(parquet_keys), 2)
            self.assertEqual(
                {layer["layer"] for layer in manifest["layers"]},
                {"roads/east", "roads-east"},
            )
            self.assertEqual(
                len(
                    {
                        layer["outputs"][0]["relative_path"]
                        for layer in manifest["layers"]
                    }
                ),
                2,
            )

    def test_single_file_multilayer_publish_uses_original_layer_names_for_namespaces(
        self,
    ):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["slash"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads/east", driver="GPKG")
            gpd.GeoDataFrame(
                {"name": ["dash"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads-east", driver="GPKG", mode="a")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            outputs = _write_and_publish_geoparquet(
                storage,
                "dataset-a",
                "file-a",
                "v1.0.0",
                GeoParquetWritePolicy(),
            )

            parquet_keys = sorted(
                key
                for key in storage.list_keys("dataset-a", "file-a", "v1.0.0")
                if key.endswith(".parquet")
            )
            manifest = json.loads(
                storage.read_bytes(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )

            self.assertEqual(len(outputs), 2)
            self.assertEqual(
                {output.path for output in outputs},
                {"dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet"},
            )
            self.assertTrue(
                all(
                    output.source_metadata["hive_partitioned"] is False
                    for output in outputs
                )
            )
            self.assertEqual(len(parquet_keys), 2)
            self.assertEqual(
                {Path(key).parent.name for key in parquet_keys},
                {"layer-roads%2Feast", "layer-roads-east"},
            )
            self.assertEqual(
                {layer["layer"] for layer in manifest["layers"]},
                {"roads/east", "roads-east"},
            )
            self.assertEqual(
                len(
                    {
                        layer["outputs"][0]["relative_path"]
                        for layer in manifest["layers"]
                    }
                ),
                2,
            )

    def test_named_single_file_multichunk_publish_uses_recursive_non_hive_glob(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["one", "two"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, layer="roads", driver="GPKG")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            outputs = _write_and_publish_geoparquet(
                storage,
                "dataset-a",
                "file-a",
                "v1.0.0",
                GeoParquetWritePolicy(
                    target_file_size_bytes=1,
                    write_buffer_bytes=10**9,
                    aggregate_buffer_bytes=10**9,
                ),
            )

            self.assertEqual(len(outputs), 1)
            self.assertEqual(
                outputs[0].path,
                "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
            )
            self.assertFalse(outputs[0].source_metadata["hive_partitioned"])
            matched = list(
                Path(staging_dir).glob(
                    "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet"
                )
            )
            self.assertEqual(len(matched), 2)

    def test_prefixed_geoparquet_and_manifest_paths_are_colocated(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "tenant-a"
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads", driver="GPKG")
            storage = StagingStorageResource(
                local_dir=staging_dir,
                prefix="tenant-a",
                use_local=True,
            )

            _write_and_publish_geoparquet(
                storage,
                "dataset-a",
                "file-a",
                "v1.0.0",
            )

            keys = storage.list_keys("dataset-a", "file-a", "v1.0.0")
            parquet_keys = [key for key in keys if key.endswith(".parquet")]
            manifest_key = (
                "tenant-a/dataset-a/file-a/v1.0.0/metadata/"
                "geoparquet_layout.json"
            )
            self.assertIn(manifest_key, keys)
            self.assertEqual(len(parquet_keys), 1)
            manifest = json.loads(
                storage.read_bytes(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )
            self.assertEqual(
                manifest["layers"][0]["outputs"][0]["path"], parquet_keys[0]
            )

    def test_prefix_is_applied_once_when_dataset_slug_matches_prefix(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "tenant"
                / "tenant"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(version_dir / "source.gpkg", layer="roads", driver="GPKG")
            storage = StagingStorageResource(
                local_dir=staging_dir,
                prefix="tenant",
                use_local=True,
            )

            outputs = _write_and_publish_geoparquet(
                storage, "tenant", "file-a", "v1.0.0"
            )

            keys = storage.list_keys("tenant", "file-a", "v1.0.0")
            parquet_keys = [key for key in keys if key.endswith(".parquet")]
            self.assertEqual(len(parquet_keys), 1)
            self.assertTrue(parquet_keys[0].startswith("tenant/tenant/file-a/"))
            self.assertEqual(
                outputs[0].path,
                "tenant/tenant/file-a/v1.0.0/geoparquet/**/*.parquet",
            )
            manifest = json.loads(
                storage.read_bytes(
                    "tenant",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )
            self.assertEqual(manifest["layers"][0]["outputs"][0]["path"], parquet_keys[0])

    def test_geoparquet_glob_uses_rightmost_version_format_root(self):
        paths = [
            "archive/geoparquet/tenant/d/f/v/geoparquet/layer-roads/file.parquet"
        ]

        glob_path, hive_partitioned = _geoparquet_glob_and_hive_status(paths)

        self.assertEqual(
            glob_path,
            "archive/geoparquet/tenant/d/f/v/geoparquet/**/*.parquet",
        )
        self.assertFalse(hive_partitioned)

    def test_overwrite_disabled_rejects_existing_geoparquet_without_manifest(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            key = "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"
            storage.write_key(key, b"legacy")

            with patch.dict("os.environ", {"HIFLD_PUBLISH_OVERWRITE": "false"}):
                with self.assertRaisesRegex(ValueError, "authoritative layout manifest"):
                    _write_and_publish_geoparquet(
                        storage, "dataset-a", "file-a", "v1.0.0"
                    )

            self.assertTrue(storage.object_exists(key))

    def test_overwrite_disabled_rejects_stale_manifest_output_set(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            actual_key = "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"
            storage.write_key(actual_key, b"partial")
            storage.write(
                "dataset-a",
                "file-a",
                "v1.0.0",
                "metadata/geoparquet_layout.json",
                json.dumps(
                    {
                        "schema_version": 1,
                        "validation_status": "valid",
                        "layers": [
                            {
                                "layer": "default",
                                "validation_status": "valid",
                                "outputs": [
                                    {
                                        "path": "dataset-a/file-a/v1.0.0/geoparquet/other.parquet",
                                        "footer_size_bytes": 0,
                                    }
                                ],
                            }
                        ],
                    }
                ).encode(),
            )

            with patch.dict("os.environ", {"HIFLD_PUBLISH_OVERWRITE": "false"}):
                with self.assertRaisesRegex(ValueError, "does not match"):
                    _write_and_publish_geoparquet(
                        storage, "dataset-a", "file-a", "v1.0.0"
                    )

            self.assertTrue(storage.object_exists(actual_key))

    def test_overwrite_disabled_reuses_exact_valid_manifest_output_set(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            actual_key = "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"
            storage.write_key(actual_key, b"valid")
            storage.write(
                "dataset-a",
                "file-a",
                "v1.0.0",
                "metadata/geoparquet_layout.json",
                json.dumps(
                    {
                        "schema_version": 1,
                        "validation_status": "valid",
                        "layers": [
                            {
                                "layer": "default",
                                "validation_status": "valid",
                                "outputs": [
                                    {"path": actual_key, "footer_size_bytes": 0}
                                ],
                            }
                        ],
                    }
                ).encode(),
            )

            with patch.dict("os.environ", {"HIFLD_PUBLISH_OVERWRITE": "false"}):
                outputs = _write_and_publish_geoparquet(
                    storage, "dataset-a", "file-a", "v1.0.0"
                )

            self.assertEqual(len(outputs), 1)
            self.assertEqual(outputs[0].path, actual_key)
            self.assertTrue(storage.object_exists(actual_key))

    def test_geoparquet_overwrite_replaces_manifest_layer_set_and_source_format(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_root = (
                Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            )
            gpkg_dir = version_root / "geopackage"
            gpkg_dir.mkdir(parents=True)
            source = gpkg_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads", driver="GPKG")
            gpd.GeoDataFrame(
                {"name": ["bridge"]}, geometry=[Point(1, 1)], crs="EPSG:4326"
            ).to_file(source, layer="bridges", driver="GPKG", mode="a")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)

            _write_and_publish_geoparquet(
                storage, "dataset-a", "file-a", "v1.0.0"
            )
            source.unlink()
            geojson_dir = version_root / "geojson"
            geojson_dir.mkdir()
            gpd.GeoDataFrame(
                {"name": ["new"]}, geometry=[Point(2, 2)], crs="EPSG:4326"
            ).to_file(geojson_dir / "source.geojson", driver="GeoJSON")

            _write_and_publish_geoparquet(
                storage, "dataset-a", "file-a", "v1.0.0"
            )

            manifest = json.loads(
                storage.read_bytes(
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "metadata/geoparquet_layout.json",
                )
            )
            self.assertEqual(len(manifest["layers"]), 1)
            self.assertEqual(manifest["layers"][0]["source_format"], "geojson")
            parquet_keys = [
                key
                for key in storage.list_keys("dataset-a", "file-a", "v1.0.0")
                if key.endswith(".parquet")
            ]
            self.assertEqual(len(parquet_keys), 1)
            self.assertFalse(any("layer-roads" in key for key in parquet_keys))
            self.assertFalse(any("layer-bridges" in key for key in parquet_keys))

    def test_later_layer_failure_removes_partial_geoparquet_and_manifest(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = (
                Path(staging_dir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            version_dir.mkdir(parents=True)
            source = version_dir / "source.gpkg"
            gpd.GeoDataFrame(
                {"name": ["road"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, layer="roads", driver="GPKG")
            gpd.GeoDataFrame(
                {"name": ["bridge"]}, geometry=[Point(1, 1)], crs="EPSG:4326"
            ).to_file(source, layer="bridges", driver="GPKG", mode="a")
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            calls = 0

            async def fail_second_layer(**kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    return {"error": "injected later-layer failure"}
                return await process_layer_partitioned_geoparquet(**kwargs)

            with patch(
                "dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
                new=fail_second_layer,
            ):
                with self.assertRaisesRegex(ValueError, "injected later-layer failure"):
                    _write_and_publish_geoparquet(
                        storage, "dataset-a", "file-a", "v1.0.0"
                    )

            keys = storage.list_keys("dataset-a", "file-a", "v1.0.0")
            self.assertFalse(any(key.endswith(".parquet") for key in keys))
            self.assertFalse(any(key.endswith("geoparquet_layout.json") for key in keys))


if __name__ == "__main__":
    unittest.main()
