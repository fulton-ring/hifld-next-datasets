import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.conversion import _to_wgs84


class ConversionCRSTests(unittest.TestCase):
    def test_rejects_projected_coordinates_mislabeled_as_geographic(self):
        frame = gpd.GeoDataFrame(
            geometry=[Point(-12573889.746, 4185248.449)], crs="EPSG:4326"
        )
        with self.assertRaisesRegex(ValueError, "longitude/latitude.*source CRS"):
            _to_wgs84(frame)

    def test_does_not_assume_wgs84_for_unreferenced_projected_coordinates(self):
        frame = gpd.GeoDataFrame(geometry=[Point(-12573889.746, 4185248.449)])
        with self.assertRaisesRegex(ValueError, "longitude/latitude.*source CRS"):
            _to_wgs84(frame)

    def test_failed_transformation_does_not_return_unconverted_geometry(self):
        frame = gpd.GeoDataFrame(geometry=[Point(1, 1)], crs="EPSG:3857")
        with (
            patch.object(
                gpd.GeoDataFrame, "to_crs", side_effect=ValueError("bad grid")
            ),
            self.assertRaisesRegex(ValueError, "convert.*EPSG:4326"),
        ):
            _to_wgs84(frame)

    def test_rejects_nonfinite_transformed_coordinates(self):
        frame = gpd.GeoDataFrame(geometry=[Point(float("inf"), 1)], crs="EPSG:4326")
        with self.assertRaisesRegex(ValueError, "longitude/latitude"):
            _to_wgs84(frame)

    def test_correctly_declared_web_mercator_is_transformed(self):
        frame = gpd.GeoDataFrame(
            geometry=[Point(111319.49079327357, 111325.1428663851)], crs="EPSG:3857"
        )
        result = _to_wgs84(frame)
        self.assertEqual(result.crs.to_epsg(), 4326)
        self.assertAlmostEqual(result.geometry.iloc[0].x, 1, places=7)
        self.assertAlmostEqual(result.geometry.iloc[0].y, 1, places=7)
        self.assertEqual(frame.crs.to_epsg(), 3857)

    def test_valid_unreferenced_geographic_coordinates_keep_legacy_assumption(self):
        frame = gpd.GeoDataFrame(geometry=[Point(-91, 40)])
        result = _to_wgs84(frame)
        self.assertEqual(result.crs.to_epsg(), 4326)
        self.assertTrue(result.geometry.iloc[0].equals(frame.geometry.iloc[0]))

    def test_empty_frame_remains_supported(self):
        frame = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        self.assertTrue(_to_wgs84(frame).empty)
