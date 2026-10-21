import io
import unittest

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.epa_power_generation import _parquet_bytes, _profile_schema


class EpaPowerGenerationSourceTests(unittest.TestCase):
    def test_native_geoparquet_preserves_source_ids_zeroes_strings_and_point(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "FIPS_CODE", "type": "esriFieldTypeString", "length": 5, "nullable": True},
            {"name": "SECTOR", "type": "esriFieldTypeInteger", "nullable": True},
        ]
        source_rows = [{
            "attributes": {"OBJECTID": 17, "FIPS_CODE": "00107", "SECTOR": 0},
            "geometry": {"x": -10701721.5, "y": 4207903.5},
        }]
        body = _parquet_bytes(fields, source_rows)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)

        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("FIPS_CODE").to_pylist(), ["00107"])
        self.assertEqual(table.column("SECTOR").to_pylist(), [0])
        primary, _column, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 3857)
        point = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertAlmostEqual(point.x, -10701721.5)
        self.assertAlmostEqual(point.y, 4207903.5)

    def test_expected_native_profile_keeps_every_source_field(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OID", "nullable": False},
            {"name": "REGISTRY_ID", "type": "esriFieldTypeString", "length": 12, "alias": "REGISTRY_ID", "nullable": True},
        ]
        plain, digest = _profile_schema({"fields": fields})
        self.assertEqual([field["name"] for field in plain], ["OBJECTID", "REGISTRY_ID"])
        self.assertEqual(len(digest), 64)


if __name__ == "__main__":
    unittest.main()
