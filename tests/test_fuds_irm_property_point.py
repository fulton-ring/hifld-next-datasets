"""Focused native geometry preservation tests for USACE IRM point data."""
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.sources.fuds_irm_property_point import _parquet


class IrmPropertyPointNativeTests(unittest.TestCase):
    fields = [
        {"name":"OBJECTID", "type":"esriFieldTypeOID", "nullable":False},
        {"name":"VALUE", "type":"esriFieldTypeString", "nullable":True},
    ]

    def test_xyzm_native_struct_and_standard_xyz_projection(self):
        rows = [
            {"attributes":{"OBJECTID":4,"VALUE":None}, "geometry":{"x":-72.0,"y":41.0,"z":12.5,"m":99.0,"spatialReference":{"wkid":4326}}},
            {"attributes":{"OBJECTID":5,"VALUE":"null geometry"}, "geometry":None},
        ]
        body = _parquet(self.fields, rows, {"wkid":4326})
        table = pq.read_table(pa.BufferReader(body), use_threads=False)
        native = table.column("source_geometry")[0].as_py()
        self.assertTrue(native["hasZ"])
        self.assertTrue(native["hasM"])
        self.assertEqual(native["point"], {"x":-72.0,"y":41.0,"z":12.5,"m":99.0})
        projected = from_wkb(table.column("geometry")[0].as_py())
        self.assertTrue(projected.has_z)
        self.assertEqual(list(projected.coords), [(-72.0, 41.0, 12.5)])
        self.assertIsNone(table.column("source_geometry")[1].as_py())
        self.assertIsNone(table.column("geometry")[1].as_py())
        self.assertIsNone(table.column("VALUE")[0].as_py())
        self.assertEqual(table.schema.field("OBJECTID").type, pa.int64())

    def test_xy_native_point_remains_xy(self):
        rows = [{"attributes":{"OBJECTID":1,"VALUE":"xy"}, "geometry":{"x":1.0,"y":2.0}}]
        table = pq.read_table(pa.BufferReader(_parquet(self.fields, rows, {"wkid":4326})), use_threads=False)
        geometry = from_wkb(table.column("geometry")[0].as_py())
        self.assertFalse(geometry.has_z)
        self.assertEqual(list(geometry.coords), [(1.0,2.0)])

    def test_invalid_source_geometry_is_retained_for_quality_failure(self):
        row = {"attributes":{"OBJECTID":9,"VALUE":"invalid"}, "geometry":{"x":500.0,"y":4.0}}
        table = pq.read_table(pa.BufferReader(_parquet(self.fields, [row], {"wkid":4326})), use_threads=False)
        self.assertIsNone(table.column("geometry")[0].as_py())
        native = table.column("source_geometry")[0].as_py()
        self.assertEqual(native["point"], None)
        self.assertEqual(native["raw_json"], '{"x":500.0,"y":4.0}')


if __name__ == "__main__":
    unittest.main()
