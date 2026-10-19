import io
import unittest

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources.fuds_public_properties import _parquet_bytes


class FudsPublicPropertiesTests(unittest.TestCase):
    def test_native_candidate_preserves_all_properties_and_null_point_rows(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "domain": None},
            {"name": "DODFUDSPROPERTYIDPK", "type": "esriFieldTypeString", "length": 40},
            {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
        ]
        features = [
            {"attributes": {"OBJECTID": 17, "DODFUDSPROPERTYIDPK": "A04MS0001",
                             "SE_ANNO_CAD_DATA": None},
             "geometry": {"x": -89.25194443999999, "y": 31.264444440000034}},
            {"attributes": {"OBJECTID": 18, "DODFUDSPROPERTYIDPK": "0010000007",
                             "SE_ANNO_CAD_DATA": None}, "geometry": None},
        ]
        data = _parquet_bytes(fields, features)
        table = pq.ParquetFile(io.BytesIO(data)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17, 18])
        self.assertEqual(table.column("DODFUDSPROPERTYIDPK").to_pylist(), ["A04MS0001", "0010000007"])
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [None, None])
        geometries = table.column("SHAPE").to_pylist()
        self.assertAlmostEqual(shapely.from_wkb(geometries[0]).x, features[0]["geometry"]["x"])
        self.assertAlmostEqual(shapely.from_wkb(geometries[0]).y, features[0]["geometry"]["y"])
        self.assertIsNone(geometries[1])


if __name__ == "__main__":
    unittest.main()
