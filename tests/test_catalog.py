import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import zipfile

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.geometry import Point

from dagster_hifld.catalog import (
    generate_data_dictionary,
    generate_quality_manifest,
    load_staged_geodata,
    summarize_staged_catalog,
    _summarize_geospatial_source,
    _iter_geospatial_sources,
    _with_large_geojson_support,
    write_catalog_metadata,
)
from dagster_hifld.resources import StagingStorageResource


class CatalogTests(unittest.TestCase):
    def test_generate_quality_manifest_matches_expected_fields(self):
        gdf = gpd.GeoDataFrame(
            {"station_id": [1, 2], "name": ["A", "B"]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )

        manifest = generate_quality_manifest(gdf)

        self.assertEqual(manifest["feature_count"], 2)
        self.assertEqual(manifest["geometry_type"], "Point")
        self.assertEqual(manifest["invalid_geometry_count"], 0)
        self.assertTrue(manifest["quality_check_passed"])
        self.assertEqual(manifest["bounds"], [0.0, 0.0, 1.0, 1.0])
        self.assertIsInstance(manifest["columns_hash"], str)

    def test_generate_data_dictionary_uses_existing_schema_shape(self):
        gdf = gpd.GeoDataFrame(
            {"station_id": [1, 2], "name": ["Alpha", None]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )

        dictionary = generate_data_dictionary(gdf, "amtrak-stations")

        self.assertEqual(dictionary["name"], "amtrak-stations")
        columns = {column["name"]: column for column in dictionary["columns"]}
        self.assertEqual(columns["station_id"]["type"], "integer")
        self.assertEqual(columns["name"]["type"], "string")
        self.assertEqual(columns["geometry"]["type"], "geometry")
        self.assertEqual(columns["name"]["numNullValues"], 1)

    def test_generate_data_dictionary_includes_resolved_source_metadata(self):
        gdf = gpd.GeoDataFrame(
            {"station_id": [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )

        dictionary = generate_data_dictionary(
            gdf,
            "amtrak-stations",
            source_metadata={
                "title": "Amtrak Stations",
                "description": "Passenger rail stations.",
                "publisher": "Amtrak",
                "keywords": ["rail", "stations"],
                "metadata_sources": ["file"],
                "metadata_resolved_from": {"title": "file"},
            },
        )

        self.assertEqual(dictionary["title"], "Amtrak Stations")
        self.assertEqual(dictionary["description"], "Passenger rail stations.")
        self.assertEqual(dictionary["publisher"], "Amtrak")
        self.assertEqual(dictionary["keywords"], ["rail", "stations"])
        self.assertEqual(dictionary["metadata_sources"], ["file"])

    def test_generate_data_dictionary_sampling_is_deterministic(self):
        gdf = gpd.GeoDataFrame(
            {
                "station_id": list(range(10_005)),
                "name": [f"name-{idx % 25}" for idx in range(10_005)],
            },
            geometry=[Point(float(idx), float(idx)) for idx in range(10_005)],
            crs="EPSG:4326",
        )

        dictionary_a = generate_data_dictionary(gdf, "amtrak-stations")
        dictionary_b = generate_data_dictionary(gdf, "amtrak-stations")

        self.assertEqual(dictionary_a, dictionary_b)

    def test_write_catalog_metadata_writes_both_json_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)

            write_catalog_metadata(
                resource,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_test",
                quality_dict={"feature_count": 2},
                dictionary_dict={"name": "amtrak-stations", "columns": []},
            )

            metadata_dir = (
                Path(tmpdir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_test"
                / "metadata"
            )
            self.assertEqual(
                json.loads((metadata_dir / "quality_manifest.json").read_text()),
                {"feature_count": 2},
            )
            self.assertEqual(
                json.loads((metadata_dir / "data_dictionary.json").read_text()),
                {"name": "amtrak-stations", "columns": []},
            )

    def test_iter_geospatial_sources_extracts_zipped_file_geodatabase(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            fgdb_dir = version_dir / "file_geodatabase"
            fgdb_dir.mkdir()
            zip_path = fgdb_dir / "source.gdb.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("source.gdb/gdb", b"")
                zf.writestr("source.gdb/a00000001.gdbtable", b"")

            with patch("dagster_hifld.catalog.fiona.listlayers", return_value=["layer_a"]):
                sources = list(_iter_geospatial_sources(version_dir))

            self.assertEqual(len(sources), 1)
            source_path, layer = sources[0]
            self.assertEqual(source_path.name, "source.gdb")
            self.assertTrue(source_path.is_dir())
            self.assertEqual(layer, "layer_a")

    def test_iter_geospatial_sources_prefers_canonical_shapefile_over_legacy_unknown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            canonical_dir = version_dir / "shapefile"
            legacy_dir = version_dir / "unknown"
            canonical_dir.mkdir()
            legacy_dir.mkdir()
            canonical = canonical_dir / "canonical.shp"
            legacy = legacy_dir / "legacy.shp"
            for path in (canonical, legacy):
                gpd.GeoDataFrame(
                    {"name": [path.stem]},
                    geometry=[Point(0, 0)],
                    crs="EPSG:4326",
                ).to_file(path)

            sources = list(_iter_geospatial_sources(version_dir))

            self.assertIn((canonical, None), sources)
            self.assertNotIn((legacy, None), sources)

    def test_iter_geospatial_sources_reads_nested_uppercase_legacy_shapefile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            legacy = version_dir / "unknown" / "nested" / "LEGACY.SHP"
            legacy.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["legacy"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(legacy)
            for generated in list(legacy.parent.iterdir()):
                temporary = generated.with_name(f"{generated.name}.rename")
                uppercase = generated.with_suffix(generated.suffix.upper())
                generated.rename(temporary)
                temporary.rename(uppercase)

            self.assertEqual(list(_iter_geospatial_sources(version_dir)), [(legacy, None)])

    def test_summarize_staged_catalog_reads_nested_canonical_json_geojson(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            source = (
                Path(tmpdir)
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geojson"
                / "nested"
                / "SOURCE.JSON"
            )
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")

            try:
                summary = summarize_staged_catalog(
                    resource,
                    "dataset-a",
                    "file-a",
                    "v1.0.0",
                    "dataset-a",
                )
            except ValueError as exc:
                self.fail(f"nested canonical JSON GeoJSON was not discovered: {exc}")

            self.assertEqual(summary.quality_manifest["feature_count"], 1)
            self.assertEqual(summary.quality_manifest["geometry_type"], "Point")

    def test_generate_quality_manifest_supports_tabular_inputs(self):
        df = pd.DataFrame({"station_id": [1, 2], "name": ["A", None]})

        manifest = generate_quality_manifest(df)

        self.assertEqual(manifest["feature_count"], 2)
        self.assertIsNone(manifest["geometry_type"])
        self.assertIsNone(manifest["bounds"])
        self.assertEqual(manifest["invalid_geometry_count"], 0)
        self.assertEqual(manifest["spatial_status"], "non_spatial_source")
        self.assertTrue(manifest["quality_check_passed"])

    def test_generate_quality_manifest_passes_all_null_geometry_as_expected_non_spatial(self):
        gdf = gpd.GeoDataFrame(
            {"name": ["A", "B"]},
            geometry=[None, None],
            crs="EPSG:4326",
        )

        manifest = generate_quality_manifest(gdf)

        self.assertTrue(manifest["quality_check_passed"])
        self.assertEqual(manifest["spatial_status"], "all_null_geometry")
        self.assertEqual(manifest["null_geometry_count"], 2)

    def test_generate_quality_manifest_fails_invalid_spatial_geometry(self):
        invalid = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        gdf = gpd.GeoDataFrame(
            {"name": ["A"]},
            geometry=[invalid],
            crs="EPSG:4326",
        )

        manifest = generate_quality_manifest(gdf)

        self.assertFalse(manifest["quality_check_passed"])
        self.assertEqual(manifest["spatial_status"], "spatial")
        self.assertEqual(manifest["invalid_geometry_count"], 1)

    def test_load_staged_geodata_preserves_source_identifier_columns_without_synthetic_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            version_dir = (
                Path(tmpdir)
                / "12nm-territorial-sea"
                / "12nm-territorial-sea"
                / "v-test"
                / "geojson"
            )
            version_dir.mkdir(parents=True)
            gdf = gpd.GeoDataFrame(
                {"OBJECTID": [101, 102], "name": ["A", "B"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(version_dir / "12nm-territorial-sea.geojson", driver="GeoJSON")

            loaded = load_staged_geodata(
                resource,
                dataset_slug="12nm-territorial-sea",
                file_slug="12nm-territorial-sea",
                version="v-test",
            )

            self.assertNotIn("id", loaded.columns)
            self.assertIn("OBJECTID", loaded.columns)
            self.assertEqual(loaded["OBJECTID"].tolist(), [101, 102])

    def test_summarize_staged_catalog_handles_non_spatial_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            version_dir = Path(tmpdir) / "dataset-a" / "file-a" / "v1.0.0" / "unknown"
            version_dir.mkdir(parents=True)
            (version_dir / "source.csv").write_text("name,value\nA,1\nB,2\n", encoding="utf-8")

            summary = summarize_staged_catalog(
                resource,
                dataset_slug="dataset-a",
                file_slug="file-a",
                version="v1.0.0",
                dictionary_name="dataset-a",
            )

            self.assertEqual(summary.quality_manifest["feature_count"], 2)
            self.assertIsNone(summary.quality_manifest["geometry_type"])
            self.assertEqual(summary.quality_manifest["catalog_mode"], "tabular")
            column_names = [column["name"] for column in summary.data_dictionary["columns"]]
            self.assertEqual(column_names, ["name", "value"])

    def test_summarize_geospatial_source_does_not_require_driver_bounds(self):
        class BoundsFailingCollection:
            crs = "EPSG:4326"
            schema = {"geometry": "Point", "properties": {"name": "str"}}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __len__(self):
                return 1

            @property
            def bounds(self):
                raise RuntimeError("Driver was not able to calculate bounds")

            def __iter__(self):
                return iter(
                    [
                        {
                            "type": "Feature",
                            "properties": {"name": "A"},
                            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                        }
                    ]
                )

        import dagster_hifld.catalog as catalog_module

        original_open = catalog_module.fiona.open
        catalog_module.fiona.open = lambda *args, **kwargs: BoundsFailingCollection()
        try:
            sample, quality = _summarize_geospatial_source(Path("source.gpkg"), None)
        finally:
            catalog_module.fiona.open = original_open

        self.assertEqual(len(sample), 1)
        self.assertEqual(quality["feature_count"], 1)
        self.assertEqual(quality["bounds"], [1.0, 2.0, 1.0, 2.0])
        self.assertEqual(quality["geometry_type"], "Point")

    def test_summarize_geospatial_source_passes_all_null_geometry(self):
        class AllNullGeometryCollection:
            crs = "EPSG:4326"
            schema = {"geometry": "Unknown", "properties": {"name": "str"}}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __len__(self):
                return 2

            @property
            def bounds(self):
                return None

            def __iter__(self):
                return iter(
                    [
                        {"type": "Feature", "properties": {"name": "A"}, "geometry": None},
                        {"type": "Feature", "properties": {"name": "B"}, "geometry": None},
                    ]
                )

        import dagster_hifld.catalog as catalog_module

        original_open = catalog_module.fiona.open
        catalog_module.fiona.open = lambda *args, **kwargs: AllNullGeometryCollection()
        try:
            sample, quality = _summarize_geospatial_source(Path("source.geojson"), None)
        finally:
            catalog_module.fiona.open = original_open

        self.assertEqual(len(sample), 2)
        self.assertTrue(quality["quality_check_passed"])
        self.assertEqual(quality["spatial_status"], "all_null_geometry")
        self.assertEqual(quality["sampled_null_geometry_count"], 2)

    def test_large_geojson_support_sets_gdal_object_size_option(self):
        import dagster_hifld.catalog as catalog_module

        calls = []

        class FakeEnv:
            def __init__(self, **kwargs):
                calls.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        original_env = catalog_module.fiona.Env
        catalog_module.fiona.Env = FakeEnv
        try:
            with _with_large_geojson_support():
                pass
        finally:
            catalog_module.fiona.Env = original_env

        self.assertEqual(calls, [{"OGR_GEOJSON_MAX_OBJ_SIZE": "0"}])

    def test_catalog_error_lists_attempted_sources_when_all_readers_fail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            version_dir = Path(tmpdir) / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            version_dir.mkdir(parents=True)
            (version_dir / "source.geojson").write_text("not geojson", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "source.geojson.*DriverError"):
                summarize_staged_catalog(
                    resource,
                    dataset_slug="dataset-a",
                    file_slug="file-a",
                    version="v1.0.0",
                    dictionary_name="dataset-a",
                )


if __name__ == "__main__":
    unittest.main()
