import io
import json
import unittest
import zipfile

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.assets.us_county_boundaries import us_county_boundaries_source_complete
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.us_county_boundaries import (
    EXPECTED_FIELDS,
    STRING_FIELDS,
    _geoparquet,
    candidate_version,
    _inspect_archive,
)


def _release(vintage=2026):
    source_file = f"tl_{vintage}_us_county.zip"
    return census_latest.ShapefileRelease(
        vintage=vintage, layer="COUNTY", source_file=source_file,
        source_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/COUNTY/{source_file}",
        directory_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/COUNTY/",
        listing_sha256="0" * 64, listing_bytes=1, latest_checked_at="2026-11-13T00:00:00+00:00",
        annual_listing_sha256="1" * 64,
    )


class UsCountyBoundariesTests(unittest.TestCase):
    def _archive(self, names):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            for name in names:
                archive.writestr(name, b"fixture")
        return sink.getvalue()

    def test_candidate_version_is_bound_to_resolved_vintage_and_code_identity(self):
        identity = {
            "code_commit": "packaged-sha256:" + "a" * 64,
            "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
            "files": {"src/dagster_hifld/example.py": "b" * 64},
            "working_tree_status": "unknown",
            "installed_distributions": [{"name": "dagster", "version": "1.11.0"}],
            "python": "3.12.14", "pyarrow": "23.0.1", "proj": "9.5.1", "geos": "3.13.1",
        }
        first = candidate_version(_release(), "c" * 64, identity)
        self.assertEqual(first, candidate_version(_release(), "c" * 64, identity))
        self.assertNotEqual(first, candidate_version(_release(2027), "c" * 64, identity))
        self.assertNotEqual(first, candidate_version(_release(), "d" * 64, identity))

    def test_rejects_incomplete_tiger_archive_member_roster(self):
        body = self._archive(["tl_2026_us_county.shp"])
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(body, _release())

    def test_rejects_archive_path_traversal_before_extraction(self):
        names = [
            "../tl_2026_us_county.cpg", "tl_2026_us_county.dbf", "tl_2026_us_county.prj",
            "tl_2026_us_county.shp", "tl_2026_us_county.shp.ea.iso.xml",
            "tl_2026_us_county.shp.iso.xml", "tl_2026_us_county.shx",
        ]
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            _inspect_archive(self._archive(names), _release())

    def test_complete_source_gate_keeps_release_acceptance_false(self):
        result = us_county_boundaries_source_complete({
            "feature_count": 3235,
            "source_population_complete": True,
            "source_archive_bytes": 1000,
            "source_archive_sha256": "a" * 64,
            "source_id_sha256": "b" * 64,
            "source_vintage": "2026",
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)

    def test_native_geoparquet_output_is_registered_and_preserves_mixed_polygons(self):
        self.assertIn(("us-county-boundaries-1", "us-county-boundaries-1"), SUPPORTED_DATASET_FILE_PAIRS)
        attributes = {}
        for name in EXPECTED_FIELDS:
            if name in STRING_FIELDS:
                attributes[name] = [None if name in {"CSAFP", "CBSAFP", "METDIVFP"} else f"{name}1",
                                    f"{name}2"]
            else:
                attributes[name] = [1, 2]
        attributes["GEOID"] = ["01001", "01003"]
        polygons = [
            Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]),
            MultiPolygon([
                Polygon([(3, 0), (4, 0), (4, 1), (3, 1), (3, 0)]),
                Polygon([(5, 0), (6, 0), (6, 1), (5, 1), (5, 0)]),
            ]),
        ]
        frame = gpd.GeoDataFrame(attributes, geometry=polygons, crs="EPSG:4269")

        output = _geoparquet(frame)
        table = pq.read_table(pa.BufferReader(output))
        self.assertEqual(table.column_names, EXPECTED_FIELDS + ["geometry"])
        self.assertEqual(table["GEOID"].type, pa.string())
        self.assertEqual(table["ALAND"].type, pa.int64())
        self.assertEqual(table["CSAFP"].to_pylist()[0], None)
        geometries = shapely.from_wkb(table["geometry"].to_pylist())
        self.assertEqual([geometry.geom_type for geometry in geometries], ["Polygon", "MultiPolygon"])
        self.assertTrue(all(shapely.equals_exact(a, b, tolerance=0.0) for a, b in zip(polygons, geometries)))
        self.assertEqual(set(table.schema.metadata), {b"geo"})
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_metadata["version"], "1.1.0")
        self.assertEqual(geo_metadata["primary_column"], "geometry")
        column = geo_metadata["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["geometry_types"], ["MultiPolygon", "Polygon"])
        self.assertEqual(column["crs"], CRS.from_epsg(4269).to_json_dict())


if __name__ == "__main__":
    unittest.main()
