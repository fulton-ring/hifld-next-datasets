import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.assets.publish import (
    _copy_metadata_files,
    _copy_or_generate_geopackage,
    _copy_source_format_files,
    _copy_version_files,
    _is_spatial_source_layer,
    _prepare_format_publish,
    _published_outputs_from_keys,
    _write_and_publish_geoparquet,
    _write_and_publish_pmtiles,
    _write_and_publish_shapefile_zip,
    publish_assets,
)
from dagster_hifld.conversion import ShapefileZipPolicy
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class PublishTests(unittest.TestCase):
    def test_copy_metadata_files_promotes_catalog_outputs(self):
        with (
            tempfile.TemporaryDirectory() as staging_dir,
            tempfile.TemporaryDirectory() as published_dir,
        ):
            metadata_root = Path(staging_dir) / "amtrak-stations" / "amtrak-stations" / "run_a" / "metadata"
            metadata_root.mkdir(parents=True)
            (metadata_root / "quality_manifest.json").write_text('{"quality_check_passed": true}', encoding="utf-8")
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
        self.assertTrue(any(asset.key.path == ["publish", "promote"] for asset in publish_assets))
        self.assertTrue(any(asset.key.path == ["publish", "formats", "geoparquet"] for asset in publish_assets))
        self.assertFalse(any(asset.key.path == ["publish", "promote", "source_files"] for asset in publish_assets))
        self.assertFalse(any(asset.key.path == ["publish", "promote", "metadata"] for asset in publish_assets))
        self.assertFalse(any(asset.key.path[:2] == ["publish", "register"] for asset in publish_assets))

    def test_copy_version_files_promotes_everything_under_staged_version(self):
        with (
            tempfile.TemporaryDirectory() as staging_dir,
            tempfile.TemporaryDirectory() as published_dir,
        ):
            version_root = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0"
            (version_root / "geopackage").mkdir(parents=True)
            (version_root / "geopackage" / "source.gpkg").write_bytes(b"gpkg")
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
                    "dataset-a/file-a/v1.0.0/geopackage/source.gpkg",
                    "dataset-a/file-a/v1.0.0/geoparquet/old.parquet",
                    "dataset-a/file-a/v1.0.0/metadata/quality_manifest.json",
                ],
            )
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/geopackage/source.gpkg").exists())
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/geoparquet/old.parquet").exists())
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/metadata/quality_manifest.json").exists())

    def test_copy_version_files_uses_bulk_copy(self):
        class FakeStaging:
            def __init__(self):
                self.bulk_keys = None

            def list_keys(self, dataset_slug, file_slug, version):
                return [
                    f"{dataset_slug}/{file_slug}/{version}/metadata/quality_manifest.json",
                    f"{dataset_slug}/{file_slug}/{version}/geoparquet/source.parquet",
                ]

            def copy_keys_to(self, destination, keys):
                self.bulk_keys = list(keys)
                return [f"copied/{Path(key).name}" for key in keys]

        class FakePublished:
            def __init__(self):
                self.deleted = None

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

    def test_copy_source_format_files_promotes_only_canonical_staged_source_formats(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as staging_dir,
            tempfile.TemporaryDirectory() as published_dir,
        ):
            version_root = Path(staging_dir) / "amtrak-stations" / "amtrak-stations" / "run_a"
            (version_root / "shapefile").mkdir(parents=True)
            (version_root / "geoparquet").mkdir(parents=True)
            (version_root / "file_geodatabase" / "stations.gdb").mkdir(parents=True)
            (version_root / "metadata").mkdir(parents=True)
            (version_root / "shapefile" / "stations.shp").write_bytes(b"shape")
            (version_root / "geoparquet" / "stations.parquet").write_bytes(b"parquet")
            (version_root / "file_geodatabase" / "stations.gdb" / "a00000001.gdbtable").write_bytes(b"gdb")
            (version_root / "metadata" / "quality_manifest.json").write_text("{}", encoding="utf-8")

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
                    "amtrak-stations/amtrak-stations/run_a/geoparquet/stations.parquet",
                ],
            )
            self.assertFalse(
                (
                    Path(published_dir) / "amtrak-stations" / "amtrak-stations" / "run_a" / "shapefile" / "stations.shp"
                ).exists()
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "geoparquet"
                    / "stations.parquet"
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
        with (
            tempfile.TemporaryDirectory() as staging_dir,
            tempfile.TemporaryDirectory() as published_dir,
        ):
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
                ["dataset-a/file-a/v1.0.0/file_geodatabase/source.gdb.zip"],
            )
            self.assertTrue((Path(published_dir) / "dataset-a/file-a/v1.0.0/file_geodatabase/source.gdb.zip").exists())
            self.assertFalse((Path(published_dir) / "dataset-a/file-a/v1.0.0/shapefile/source.shp").exists())

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

    def test_existing_multi_file_geoparquet_is_registered_as_glob(self):
        keys = [
            "dataset-a/file-a/v1.0.0/geoparquet/file-a-0.zstd.parquet",
            "dataset-a/file-a/v1.0.0/geoparquet/file-a-1.zstd.parquet",
        ]

        outputs = _published_outputs_from_keys("dataset-a", "file-a", "v1.0.0", keys)

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].path, "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet")

    def test_existing_partitioned_geoparquet_is_registered_as_recursive_glob(self):
        keys = [
            "dataset-a/file-a/v1.0.0/geoparquet/statefp=06/part-000.parquet",
            "dataset-a/file-a/v1.0.0/geoparquet/statefp=12/part-000.parquet",
        ]

        outputs = _published_outputs_from_keys("dataset-a", "file-a", "v1.0.0", keys)

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].path, "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet")

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
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[None], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

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
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "nfhl" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            (version_dir / "source.geojson").write_text(
                '{"type":"FeatureCollection","features":[]}',
                encoding="utf-8",
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

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

            with (
                patch(
                    "dagster_hifld.assets.publish._remote_source_size_bytes",
                    return_value=10 * 1024 * 1024 * 1024,
                ),
                patch(
                    "dagster_hifld.assets.publish.process_layer_chunked",
                    new_callable=AsyncMock,
                ) as chunked,
            ):
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
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_file(version_dir / "source.geojson", driver="GeoJSON")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            with patch(
                "dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
                new_callable=AsyncMock,
            ) as hive_writer:
                hive_writer.return_value = {
                    "geoparquet_paths": ["dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet"],
                    "feature_count": 1,
                    "partitioning": "single_file",
                    "partition_columns": [],
                    "hive_partitioned": False,
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

    def test_geoparquet_source_publish_registers_staged_file_without_rewriting(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_parquet(version_dir / "file-a.parquet")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            with patch(
                "dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
                new_callable=AsyncMock,
            ) as hive_writer:
                outputs = _write_and_publish_geoparquet(
                    staging,
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                )

            hive_writer.assert_not_called()
            self.assertEqual(len(outputs), 1)
            self.assertEqual(outputs[0].format_type, "geoparquet")
            self.assertEqual(outputs[0].path, "dataset-a/file-a/v1.0.0/geoparquet/file-a.parquet")

    def test_geoparquet_source_generates_geopackage(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
            gdf.to_parquet(version_dir / "file-a.parquet")
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            outputs = _copy_or_generate_geopackage(staging, "dataset-a", "file-a", "v1.0.0")

            self.assertEqual(len(outputs), 1)
            self.assertEqual(outputs[0].format_type, "geopackage")
            self.assertEqual(outputs[0].path, "dataset-a/file-a/v1.0.0/geopackage/file-a.gpkg")
            self.assertTrue((Path(staging_dir) / "dataset-a/file-a/v1.0.0/geopackage/file-a.gpkg").exists())

    def test_non_spatial_geoparquet_source_skips_spatial_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir:
            version_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            version_dir.mkdir(parents=True)
            gpd.GeoDataFrame({"name": ["A"]}, geometry=[None], crs="EPSG:4326").to_parquet(
                version_dir / "file-a.parquet"
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            self.assertEqual(
                _copy_or_generate_geopackage(staging, "dataset-a", "file-a", "v1.0.0")[0].source_metadata,
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


if __name__ == "__main__":
    unittest.main()
