from __future__ import annotations

import json
import re
import unittest
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import secondary_school_districts
from dagster_hifld.sources import unified_school_districts


def _fixture_table(rows=2):
    records = [
        ("01", "00001", "0100001", "9700000US0100001", "Fort Rucker School District", "00",
         "KG", "12", "G5420", "B", "E",
         232946475.0, 2801707.0, "+31.4098725", "-85.7462544",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-85.8, 31.3, -85.7, 31.4)]))),
        ("01", "00003", "0100003", "9700000US0100003", "Maxwell AFB School District", "00",
         "KG", "12", "G5420", None, "E",
         8697486.0, 372428.0, "+32.3808833", "-86.3636970",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-86.4, 32.3, -86.3, 32.4)]))),
    ]
    for i in range(2, rows):
        lea = f"{1000 + i:05d}"
        geoid = f"01{100 + i:04d}"
        records.append((
            "01", lea, geoid, f"9700000US{geoid}", f"Synthetic Unified District {i}", "00",
            "KG", "12", "G5420", None, "E",
            8697486.0 + i, 372428.0 + i, "+32.3808833", "-86.3636970",
            shapely.to_wkb(shapely.MultiPolygon([shapely.box(-86.4 - i, 32.3, -86.3 - i, 32.4)])),
        ))
    records = records[:rows]
    columns = [
        pa.array([r[0] for r in records], type=pa.string()),
        pa.array([r[1] for r in records], type=pa.string()),
        pa.array([r[2] for r in records], type=pa.string()),
        pa.array([r[3] for r in records], type=pa.string()),
        pa.array([r[4] for r in records], type=pa.string()),
        pa.array([r[5] for r in records], type=pa.string()),
        pa.array([r[6] for r in records], type=pa.string()),
        pa.array([r[7] for r in records], type=pa.string()),
        pa.array([r[8] for r in records], type=pa.string()),
        pa.array([r[9] for r in records], type=pa.string()),
        pa.array([r[10] for r in records], type=pa.string()),
        pa.array([r[11] for r in records], type=pa.float64()),
        pa.array([r[12] for r in records], type=pa.float64()),
        pa.array([r[13] for r in records], type=pa.string()),
        pa.array([r[14] for r in records], type=pa.string()),
        pa.array([r[15] for r in records], type=pa.binary()),
    ]
    return pa.Table.from_arrays(columns, names=[name for name, _ in unified_school_districts.EXPECTED_SOURCE_FIELDS])


class UnifiedSchoolDistrictsSourceTests(unittest.TestCase):
    def test_config_discovers_the_current_school_product(self):
        self.assertEqual(unified_school_districts.DATASET_SLUG, "unified-school-districts")
        self.assertEqual(unified_school_districts.LAYER_NAME, "School_District_Unified")
        self.assertIsNone(unified_school_districts.CONFIG.expected_count)
        self.assertEqual(unified_school_districts.CONFIG.feature_count_range, (9_000, 13_000))
        self.assertTrue(unified_school_districts.CONFIG.discover_current)
        self.assertEqual(unified_school_districts.CONFIG.issue_number, 335)
        self.assertEqual(unified_school_districts.CONFIG.source_url, "")
        self.assertEqual(unified_school_districts.CONFIG.source_vintage, "")
        self.assertEqual(unified_school_districts.SOURCE_HOST, "www2.census.gov")
        self.assertEqual([name for name, _ in unified_school_districts.EXPECTED_SOURCE_FIELDS],
                         ["STATEFP", "UNSDLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD", "LOGRADE",
                          "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "SHAPE"])
        self.assertEqual(unified_school_districts.EXPECTED_OUTPUT_FIELDS,
                         ["OBJECTID", "STATEFP", "UNSDLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD",
                          "LOGRADE", "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "geometry"])
        module_code = Path(unified_school_districts.__file__).read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"TGRGDB\d{2}|tlgdb_\d{4}", module_code))

    def test_layer_specs_are_explicit_and_independent(self):
        unified = unified_school_districts.CONFIG
        secondary = secondary_school_districts.CONFIG
        self.assertEqual(unified.layer_name, "School_District_Unified")
        self.assertIsNone(unified.expected_count)
        self.assertEqual(secondary.layer_name, "School_District_Secondary")
        self.assertIsNone(secondary.expected_count)
        self.assertEqual(unified.feature_count_range, (9_000, 13_000))
        self.assertEqual(secondary.feature_count_range, (300, 700))
        self.assertNotEqual(unified, secondary)
        self.assertNotEqual([name for name, _ in unified.expected_source_fields],
                            [name for name, _ in secondary.expected_source_fields])

    def test_native_layer_build_keeps_every_attribute_and_wkb_without_globals(self):
        table = _fixture_table()
        body, _schema = unified_school_districts._shared._build_geoparquet(
            table, [1, 2], replace(unified_school_districts.CONFIG, expected_count=2)
        )
        output = pq.read_table(pa.BufferReader(body))
        self.assertEqual(output.column_names, unified_school_districts.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(output.column("OBJECTID").to_pylist(), [1, 2])
        self.assertEqual(output.column("STATEFP").to_pylist(), ["01", "01"])
        self.assertEqual(output.column("UNSDLEA").to_pylist(), ["00001", "00003"])
        self.assertEqual(output.column("GEOID").to_pylist(), ["0100001", "0100003"])
        self.assertEqual(output.column("MTFCC").to_pylist(), ["G5420", "G5420"])
        self.assertEqual(output.column("SDTYP").to_pylist(), ["B", None])
        self.assertEqual(output.column("geometry").to_pylist(), table.column("SHAPE").to_pylist())
        geo = json.loads(output.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4269})


if __name__ == "__main__":
    unittest.main()
