import tempfile
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.assets import catalog as catalog_assets_module
from dagster_hifld.assets import publish as publish_assets_module
from dagster_hifld.conversion import (
    GeoParquetWritePolicy,
    ShapefileZipPolicy,
    _discover_staged_formats,
    geoparquet_policy_for,
    select_processing_input,
    write_geoparquet_dataset,
    write_shapefile_zip,
)
from dagster_hifld.definitions import _iter_staged_version_paths
from dagster_hifld.partitions import (
    PUBLISH_PARTITIONS,
    SOURCE_FORMAT_DIRS,
    build_publish_partition_key,
    parse_publish_partition_key,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class PipelineUpdateTests(unittest.TestCase):
    def test_staged_version_discovery_recognizes_shapefile_and_ignores_derived_folders(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "dataset-a" / "file-a" / "v1.0.0" / "geopackage"
            source.mkdir(parents=True)
            (source / "file-a.gpkg").write_bytes(b"gpkg")
            shapefile = root / "dataset-b" / "file-b" / "v1.0.0" / "shapefile"
            shapefile.mkdir(parents=True)
            (shapefile / "file-b.shp").write_bytes(b"shp")
            for derived in ("geoparquet", "parquet", "pmtiles", "metadata"):
                folder = root / "dataset-b" / "file-b" / "v1.0.0" / derived
                folder.mkdir(parents=True)
                (folder / "artifact.bin").write_bytes(b"x")

            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            self.assertCountEqual(
                list(_iter_staged_version_paths(storage)),
                [
                    ("dataset-a", "file-a", "v1.0.0"),
                    ("dataset-b", "file-b", "v1.0.0"),
                ],
            )

    def test_source_format_registry_contains_only_canonical_processing_inputs(self):
        self.assertEqual(
            SOURCE_FORMAT_DIRS,
            frozenset({"geopackage", "file_geodatabase", "shapefile", "geojson"}),
        )

    def test_dynamic_partitions_and_assets_include_staged_pairs(self):
        partition_key = build_publish_partition_key("dynamic-dataset", "dynamic-file", "v1.0.0")

        self.assertEqual(
            parse_publish_partition_key(partition_key),
            ("dynamic-dataset", "dynamic-file", "v1.0.0"),
        )
        self.assertIs(catalog_assets_module.catalog_assets[0].partitions_def, PUBLISH_PARTITIONS)
        self.assertTrue(
            any(a.key.path == ["publish", "formats", "geoparquet"] for a in publish_assets_module.publish_assets)
        )
        self.assertFalse(any(a.key.path[:2] == ["publish", "register"] for a in publish_assets_module.publish_assets))

    def test_processing_input_uses_canonical_source_precedence(self):
        processed = {
            "geojson": {"format_type": "geojson", "data_file": Path("file.geojson"), "layers": []},
            "shapefile": {"format_type": "shapefile", "data_file": Path("file.shp"), "layers": []},
            "file_geodatabase": {
                "format_type": "file_geodatabase",
                "data_file": Path("file.gdb"),
                "layers": [],
            },
            "geopackage": {"format_type": "geopackage", "data_file": Path("file.gpkg"), "layers": []},
            "geoparquet": {
                "format_type": "geoparquet",
                "data_file": Path("file.parquet"),
                "layers": [],
            },
            "pmtiles": {
                "format_type": "pmtiles",
                "data_file": Path("file.pmtiles"),
                "layers": [],
            },
        }

        selected, path, fmt = select_processing_input(processed)

        self.assertIs(selected, processed["geopackage"])
        self.assertEqual(path, Path("file.gpkg"))
        self.assertEqual(fmt, "geopackage")

    def test_processing_input_accepts_canonical_shapefile(self):
        processed = {
            "shapefile": {
                "format_type": "shapefile",
                "data_file": Path("file.shp"),
                "layers": [],
            }
        }

        selected, path, fmt = select_processing_input(processed)

        self.assertIs(selected, processed["shapefile"])
        self.assertEqual(path, Path("file.shp"))
        self.assertEqual(fmt, "shapefile")

    def test_processing_input_uses_each_step_of_the_canonical_fallback_chain(self):
        formats = {
            format_name: {
                "format_type": format_name,
                "data_file": Path(filename),
                "layers": [],
            }
            for format_name, filename in (
                ("file_geodatabase", "file.gdb"),
                ("shapefile", "file.shp"),
                ("geojson", "file.geojson"),
            )
        }
        cases = (
            (("file_geodatabase", "shapefile", "geojson"), "file_geodatabase"),
            (("shapefile", "geojson"), "shapefile"),
            (("geojson",), "geojson"),
        )

        for available_formats, expected_format in cases:
            with self.subTest(available_formats=available_formats):
                processed = {
                    format_name: formats[format_name]
                    for format_name in available_formats
                }
                selected, path, format_type = select_processing_input(processed)

                self.assertIs(selected, formats[expected_format])
                self.assertEqual(path, formats[expected_format]["data_file"])
                self.assertEqual(format_type, expected_format)

    def test_processing_input_rejects_derived_outputs(self):
        processed = {
            "geoparquet": {
                "format_type": "geoparquet",
                "data_file": Path("file.parquet"),
                "layers": [],
            },
            "pmtiles": {
                "format_type": "pmtiles",
                "data_file": Path("file.pmtiles"),
                "layers": [],
            },
        }

        self.assertEqual(select_processing_input(processed), (None, None, None))

    def test_shapefile_zip_writes_single_zip_without_loose_sidecars(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gdf = gpd.GeoDataFrame(
                {"name": ["A", "B"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            out_dir = Path(tmpdir) / "out"

            result = write_shapefile_zip(
                gdf,
                out_dir,
                "small-layer",
                ShapefileZipPolicy(max_estimated_zip_bytes=50_000_000),
            )

            self.assertTrue(result.created)
            self.assertEqual(result.path, out_dir / "small-layer.zip")
            self.assertEqual([p.name for p in out_dir.iterdir()], ["small-layer.zip"])
            with zipfile.ZipFile(result.path) as zf:
                names = set(zf.namelist())
            self.assertIn("small-layer.shp", names)
            self.assertIn("small-layer.shx", names)
            self.assertIn("small-layer.dbf", names)
            self.assertIn("small-layer.prj", names)

    def test_geoparquet_default_and_admin_policy_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gdf = gpd.GeoDataFrame(
                {"statefp": ["06", "06", "12"], "name": ["A", "B", "C"]},
                geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
                crs="EPSG:4326",
            )
            default_result = write_geoparquet_dataset(
                gdf,
                Path(tmpdir) / "default",
                "layer",
                GeoParquetWritePolicy(),
            )
            admin_result = write_geoparquet_dataset(
                gdf,
                Path(tmpdir) / "admin",
                "layer",
                GeoParquetWritePolicy(
                    force_admin_columns=["statefp"], large_dataset_threshold_bytes=1
                ),
            )

            self.assertEqual(default_result.glob_path, "layer.parquet")
            self.assertTrue((Path(tmpdir) / "default" / "layer.parquet").exists())
            self.assertEqual(admin_result.glob_path, "**/*.parquet")
            hive_key = admin_result.source_metadata["hive_partition_columns"]["statefp"]
            self.assertTrue(
                any(f"{hive_key}=v-06/" in str(path) for path in admin_result.paths)
            )
            self.assertTrue(
                any(f"{hive_key}=v-12/" in str(path) for path in admin_result.paths)
            )

    def test_large_dataset_policy_registry_uses_admin_candidates(self):
        census_policy = geoparquet_policy_for("2020-census-blocks-1", "tl_2024_48_tabblock20")
        generic_policy = geoparquet_policy_for("unlisted", "layer")
        small_census_policy = geoparquet_policy_for("2020-census-blocks-1", "tl_2024_01_tabblock20")

        self.assertEqual(census_policy.force_admin_columns, ())
        self.assertEqual(generic_policy.force_admin_columns, ())
        self.assertEqual(generic_policy.candidate_admin_columns, ())
        self.assertFalse(generic_policy.force_s2)
        self.assertEqual(small_census_policy.force_admin_columns, ())

    def test_census_block_groups_policy_accepts_live_state_column(self):
        policy = geoparquet_policy_for("census-block-groups-3", "census-block-groups-3")
        self.assertEqual(policy.force_admin_columns, ("STATEFP", "STATE"))

        with tempfile.TemporaryDirectory() as tmpdir:
            result = write_geoparquet_dataset(
                gpd.GeoDataFrame(
                    {"STATE": ["06", "12"]},
                    geometry=[Point(-120, 35), Point(-80, 28)],
                    crs="EPSG:4326",
                ),
                Path(tmpdir),
                "census-block-groups-3",
                replace(policy, large_dataset_threshold_bytes=1),
            )

        self.assertEqual(result.partition_columns, ["STATE", "s2_parent_cell"])
        self.assertEqual(
            result.source_metadata["partition_columns"], ["STATE", "s2_parent_cell"]
        )

    def test_api_register_payload_uses_glob_for_partitioned_geoparquet(self):
        api = Mock()
        api.enabled = True
        published = PublishedStorageResource(local_dir="unused", use_local=True)

        payload = publish_assets_module.register_published_outputs(
            api_resource=api,
            published_storage=published,
            dataset_slug="dataset-a",
            version="v1.0.0",
            storage_location_name="GCS hifld-next-datasets-prod",
            outputs=[
                publish_assets_module.PublishedFormatOutput(
                    file_slug="file-a",
                    format_type="geoparquet",
                    path="dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
                    source_metadata={"hive_partitioned": True},
                )
            ],
        )

        self.assertEqual(payload["version"], "v1.0.0")
        self.assertEqual(payload["files"][0]["path"], "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet")
        api.upsert_dataset_version.assert_called_once()

    def test_local_e2e_converts_and_registers_version(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            source_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geopackage"
            source_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame(
                {"statefp": ["06", "12"], "name": ["A", "B"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(source_dir / "source.gpkg", driver="GPKG")
            api = Mock()
            api.enabled = True

            result = publish_assets_module.run_local_version_pipeline(
                staging_storage=staging,
                published_storage=published,
                api_resource=api,
                dataset_slug="dataset-a",
                file_slug="file-a",
                version="v1.0.0",
                storage_location_name="Local published",
            )

            version_root = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0"
            self.assertTrue((Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "metadata" / "quality_manifest.json").exists())
            self.assertTrue((version_root / "geopackage" / "source.gpkg").exists())
            self.assertTrue(
                (
                    version_root
                    / "geoparquet"
                    / "layer-source"
                    / "file-a.parquet"
                ).exists()
            )
            self.assertTrue((version_root / "metadata" / "data_dictionary.json").exists())
            self.assertTrue(any(path.suffix == ".zip" for path in (version_root / "shapefile").iterdir()))
            self.assertTrue(result["api_payload"]["files"])
            self.assertIn("metadata", {file["format"] for file in result["api_payload"]["files"]})
            self.assertIn("shapefile", {file["format"] for file in result["api_payload"]["files"]})
            api.upsert_dataset_version.assert_called_once()

    def test_local_e2e_overwrites_existing_published_format_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            source_dir = Path(staging_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geopackage"
            source_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            )
            gdf.to_file(source_dir / "source.gpkg", driver="GPKG")
            existing = Path(published_dir) / "dataset-a" / "file-a" / "v1.0.0" / "geoparquet"
            existing.mkdir(parents=True)
            existing_file = existing / "file-a.parquet"
            existing_file.write_bytes(b"exists")

            result = publish_assets_module.run_local_version_pipeline(
                staging_storage=staging,
                published_storage=published,
                api_resource=Mock(enabled=False),
                dataset_slug="dataset-a",
                file_slug="file-a",
                version="v1.0.0",
                storage_location_name="Local published",
            )

            replacement_file = existing / "layer-source" / "file-a.parquet"
            self.assertFalse(existing_file.exists())
            self.assertTrue(replacement_file.exists())
            self.assertNotEqual(replacement_file.read_bytes(), b"exists")
            self.assertIn(
                "dataset-a/file-a/v1.0.0/geoparquet/**/*.parquet",
                [output.path for output in result["outputs"]],
            )


if __name__ == "__main__":
    unittest.main()
