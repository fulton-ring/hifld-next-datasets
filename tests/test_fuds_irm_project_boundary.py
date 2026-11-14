"""Focused source-faithfulness tests for the USACE IRM project boundary."""
from __future__ import annotations

import json
import unittest

import pyarrow.parquet as pq
from shapely import from_wkb, is_valid
from shapely.geometry import MultiPolygon, Polygon

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

    def test_nested_island_holes_attach_to_immediate_shell(self) -> None:
        outer_shell = [[0.0, 0.0, 0.0], [0.0, 10.0, 0.0], [10.0, 10.0, 0.0],
                       [10.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
        island_shell = [[3.0, 3.0, 0.0], [3.0, 7.0, 0.0], [7.0, 7.0, 0.0],
                        [7.0, 3.0, 0.0], [3.0, 3.0, 0.0]]
        outer_hole = [[2.0, 2.0, 0.0], [8.0, 2.0, 0.0], [8.0, 8.0, 0.0],
                      [2.0, 8.0, 0.0], [2.0, 2.0, 0.0]]
        island_hole = [[4.0, 4.0, 0.0], [6.0, 4.0, 0.0], [6.0, 6.0, 0.0],
                       [4.0, 6.0, 0.0], [4.0, 4.0, 0.0]]
        for rings in ([outer_shell, island_shell, outer_hole, island_hole],
                      [island_shell, island_hole, outer_hole, outer_shell]):
            with self.subTest(ring_order=len(rings)):
                geometry = _source_polygon_geometry(rings)
                self.assertIsInstance(geometry, MultiPolygon)
                self.assertTrue(is_valid(geometry))
                by_bounds = {tuple(round(value) for value in polygon.exterior.bounds): polygon
                             for polygon in geometry.geoms}
                self.assertEqual(sorted(by_bounds), [(0, 0, 10, 10), (3, 3, 7, 7)])
                outer_polygon = by_bounds[(0, 0, 10, 10)]
                island_polygon = by_bounds[(3, 3, 7, 7)]
                self.assertEqual(len(outer_polygon.interiors), 1)
                self.assertEqual(len(island_polygon.interiors), 1)
                self.assertEqual(tuple(round(value) for value in outer_polygon.interiors[0].bounds),
                                 (2, 2, 8, 8))
                self.assertEqual(tuple(round(value) for value in island_polygon.interiors[0].bounds),
                                 (4, 4, 6, 6))

    def test_hole_without_fully_containing_shell_stays_deterministically_invalid(self) -> None:
        shell = [[0.0, 0.0, 0.0], [0.0, 4.0, 0.0], [4.0, 4.0, 0.0],
                 [4.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
        overlapping_hole = [[2.0, 2.0, 0.0], [6.0, 2.0, 0.0], [6.0, 6.0, 0.0],
                            [2.0, 6.0, 0.0], [2.0, 2.0, 0.0]]
        geometry = _source_polygon_geometry([shell, overlapping_hole])
        self.assertIsInstance(geometry, Polygon)
        self.assertFalse(is_valid(geometry))
        with self.assertRaises(ValueError):
            _source_polygon_geometry([shell, [[8.0, 8.0, 0.0], [9.0, 8.0, 0.0],
                                              [9.0, 9.0, 0.0], [8.0, 9.0, 0.0],
                                              [8.0, 8.0, 0.0]]])

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
