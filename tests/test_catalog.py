import json
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.catalog import (
    generate_data_dictionary,
    generate_quality_manifest,
    load_staged_geodata,
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

    def test_load_staged_geodata_adds_id_column_from_objectid(self):
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

            self.assertIn("id", loaded.columns)
            self.assertEqual(loaded["id"].tolist(), [101, 102])


if __name__ == "__main__":
    unittest.main()
