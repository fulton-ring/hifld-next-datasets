"""Focused native fidelity tests for USACE FUDS MRS."""
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.fuds_mrs import (
    EXPECTED_SCHEMA_SHA256,
    _encode_polygon_z,
    _native_parquet,
)


class FudsMrsNativeFidelityTests(unittest.TestCase):
    def test_xyz_m_null_and_zero_are_preserved_in_native_source_column(self):
        ring = [
            [0.0, 0.0, 5.0, None], [0.0, 2.0, 6.0, 0.0],
            [2.0, 2.0, 7.0, None], [2.0, 0.0, 8.0, 0.0],
            [0.0, 0.0, 5.0, None],
        ]
        encoded = _encode_polygon_z([ring])
        geometry = shapely.from_wkb(encoded)
        self.assertTrue(geometry.has_z)
        self.assertEqual(shapely.get_coordinates(geometry, include_z=True).tolist(),
                         [[p[0], p[1], p[2]] for p in ring])

        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "FEATURENAME", "type": "esriFieldTypeString", "nullable": True},
        ]
        features = [
            {"attributes": {"OBJECTID": 1, "FEATURENAME": None},
             "geometry": {"hasZ": True, "hasM": True, "rings": [ring]}},
            {"attributes": {"OBJECTID": 2, "FEATURENAME": "No geometry"}, "geometry": None},
        ]
        table = pq.read_table(pa.BufferReader(_native_parquet(fields, features,
            {1: {"wkid": 4326, "latestWkid": 4326}, 2: {"wkid": 4326, "latestWkid": 4326}})), use_threads=False)
        self.assertEqual(table.schema.names, ["OBJECTID", "FEATURENAME", "geometry", "source_geometry"])
        self.assertEqual(table["OBJECTID"].type, pa.int64())
        self.assertIsNone(table["FEATURENAME"][0].as_py())
        self.assertEqual(table["geometry"].null_count, 1)
        source = table["source_geometry"].to_pylist()
        self.assertEqual(source[0]["rings"][0], [
            {"x": p[0], "y": p[1], "z": p[2], "m": p[3]} for p in ring])
        self.assertEqual(source[0]["spatialReference"], '{"latestWkid":4326,"wkid":4326}')
        self.assertIsNone(source[1])
        self.assertEqual(geometry_metadata(table.schema)[1]["geometry_types"], ["MultiPolygon Z"])

    def test_pinned_schema_digest_is_sha256(self):
        self.assertEqual(len(EXPECTED_SCHEMA_SHA256), 64)
        self.assertTrue(all(char in "0123456789abcdef" for char in EXPECTED_SCHEMA_SHA256))

    def test_nested_island_does_not_steal_containing_hole(self):
        def ring(points):
            return [[x, y, 0, None] for x, y in points]
        # Esri exterior rings are clockwise and holes counterclockwise.
        outer = ring([[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]])
        hole = ring([[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]])
        island = ring([[4, 4], [4, 6], [6, 6], [6, 4], [4, 4]])
        geometry = shapely.from_wkb(_encode_polygon_z([outer, hole, island]))
        self.assertTrue(geometry.is_valid)
        self.assertEqual(len(geometry.geoms), 2)
        self.assertEqual(sorted(len(poly.interiors) for poly in geometry.geoms), [0, 1])


if __name__ == "__main__":
    unittest.main()
