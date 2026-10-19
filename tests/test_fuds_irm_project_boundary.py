"""Focused source-faithfulness tests for the USACE IRM project boundary."""
from __future__ import annotations

import json
import unittest

import pyarrow.parquet as pq
from shapely import from_wkb, is_valid
from shapely.geometry import Polygon

from dagster_hifld.sources.fuds_irm_project_boundary import (
    _parquet_bytes,
    _source_polygon_geometry,
)


FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
    {"name": "SDSFEATURENAME", "type": "esriFieldTypeString", "nullable": True},
]


class IrmProjectBoundaryGeometryTests(unittest.TestCase):
    def test_native_source_geometry_keeps_xyz_ring_order_and_dimensions(self) -> None:
        ring = [[0.0, 0.0, 11.0], [0.0, 2.0, 14.0], [2.0, 2.0, 13.0],
                [2.0, 0.0, 12.0], [0.0, 0.0, 11.0]]
        feature = {"attributes": {"OBJECTID": 1, "SDSFEATURENAME": "sample"},
                   "geometry": {"rings": [ring]}}
        body = _parquet_bytes(FIELDS, [feature], {"wkid": 4326, "latestWkid": 4326})
        table = pq.read_table(__import__("pyarrow").BufferReader(body))
        native = table.column("source_geometry")[0].as_py()
        self.assertTrue(native["hasZ"])
        self.assertFalse(native["hasM"])
        self.assertEqual(native["rings"][0], [
            {"x": point[0], "y": point[1], "z": point[2]} for point in ring
        ])
        projected = from_wkb(table.column("geometry")[0].as_py())
        self.assertTrue(projected.has_z)
        self.assertEqual(list(projected.exterior.coords), [tuple(point) for point in ring])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["version"], "1.1.0")
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon Z", "MultiPolygon Z"])

    def test_source_adapter_rejects_xy_and_xyzm_tuples(self) -> None:
        for ring in (
            [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]],
            [[0, 0, 1, 9], [1, 0, 1, 9], [1, 1, 1, 9], [0, 1, 1, 9], [0, 0, 1, 9]],
        ):
            with self.subTest(dimensions=len(ring[0])):
                with self.assertRaises(ValueError):
                    _source_polygon_geometry([ring])

    def test_invalid_source_polygon_is_not_repaired(self) -> None:
        bowtie = [[0.0, 0.0, 1.0], [2.0, 2.0, 2.0], [0.0, 2.0, 3.0],
                  [2.0, 0.0, 4.0], [0.0, 0.0, 1.0]]
        converted = _source_polygon_geometry([bowtie])
        self.assertIsInstance(converted, Polygon)
        self.assertFalse(is_valid(converted))

    def test_null_geometry_remains_as_a_null_native_feature(self) -> None:
        feature = {"attributes": {"OBJECTID": 1, "SDSFEATURENAME": "source-null"},
                   "geometry": None}
        body = _parquet_bytes(FIELDS, [feature], {"wkid": 4326, "latestWkid": 4326})
        table = pq.read_table(__import__("pyarrow").BufferReader(body))
        self.assertEqual(table.num_rows, 1)
        self.assertIsNone(table.column("geometry")[0].as_py())
        self.assertIsNone(table.column("source_geometry")[0].as_py())


if __name__ == "__main__":
    unittest.main()
