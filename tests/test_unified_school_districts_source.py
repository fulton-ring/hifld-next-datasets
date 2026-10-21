from __future__ import annotations

import unittest
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import secondary_school_districts, unified_school_districts
from dagster_hifld.sources import census_school_districts as shared


class UnifiedSchoolDistrictsSourceTests(unittest.TestCase):
    def test_layer_specs_are_explicit_and_independent(self):
        self.assertEqual(unified_school_districts.CONFIG.layer_name, "School_District_Unified")
        self.assertEqual(unified_school_districts.CONFIG.expected_count, 10897)
        self.assertEqual(secondary_school_districts.CONFIG.layer_name, "School_District_Secondary")
        self.assertEqual(secondary_school_districts.CONFIG.expected_count, 481)
        self.assertNotEqual(unified_school_districts.CONFIG, secondary_school_districts.CONFIG)

    def test_native_layer_build_keeps_every_attribute_and_wkb_without_globals(self):
        shape = shapely.to_wkb(shapely.MultiPolygon([shapely.box(-100, 40, -99, 41)]))
        table = pa.Table.from_arrays([
            pa.array(["0100001"], type=pa.string()),
            pa.array(["9600000US0100001"], type=pa.string()),
            pa.array(["District"], type=pa.string()),
            pa.array(["09"], type=pa.string()),
            pa.array(["12"], type=pa.string()),
            pa.array([None], type=pa.string()),
            pa.array([101.0], type=pa.float64()),
            pa.array([3.0], type=pa.float64()),
            pa.array(["+40.0000000"], type=pa.string()),
            pa.array(["-100.0000000"], type=pa.string()),
            pa.array([shape], type=pa.binary()),
        ], names=[name for name, _ in shared.EXPECTED_SOURCE_FIELDS])
        body, _ = shared._build_geoparquet(
            table, [1], replace(unified_school_districts.CONFIG, expected_count=1)
        )
        output = pq.read_table(pa.BufferReader(body))
        self.assertEqual(output.column_names, shared.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(output.column("OBJECTID").to_pylist(), [1])
        self.assertEqual(output.column("GEOID").to_pylist(), ["0100001"])
        self.assertEqual(output.column("SDTYP").to_pylist(), [None])
        self.assertEqual(output.column("geometry").to_pylist(), [shape])


if __name__ == "__main__":
    unittest.main()
