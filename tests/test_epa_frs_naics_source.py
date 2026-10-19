from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from dagster_hifld.assets.epa_frs_naics_powerplants import _typed_parquet
from dagster_hifld.native_verification import normalize_native_input


class EPAFRSNAICSSourceTests(unittest.TestCase):
    def test_nonspatial_table_round_trips_native_fields_and_string_codes(self):
        records = [
            {
                "OBJECTID": 7,
                "REGISTRY_ID": "001234567890",
                "NAICS_CODE": "002211",
                "PRIMARY_INDICATOR": "PRIMARY",
            },
            {
                "OBJECTID": 8,
                "REGISTRY_ID": None,
                "NAICS_CODE": "22",
                "PRIMARY_INDICATOR": "UNKNOWN",
            },
        ]
        fields = (
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "REGISTRY_ID", "type": "esriFieldTypeString", "nullable": True, "length": 12},
            {"name": "NAICS_CODE", "type": "esriFieldTypeString", "nullable": False, "length": 6},
            {"name": "PRIMARY_INDICATOR", "type": "esriFieldTypeString", "nullable": False, "length": 10},
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_root = root / "input"
            input_root.mkdir()
            path = input_root / "native.parquet"
            path.write_bytes(_typed_parquet(records, fields))
            table = pq.read_table(path)
            self.assertEqual(table.schema.names, ["OBJECTID", "REGISTRY_ID", "NAICS_CODE", "PRIMARY_INDICATOR"])
            self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
            self.assertTrue(table.schema.field("OBJECTID").nullable is False)
            self.assertEqual(str(table.schema.field("NAICS_CODE").type), "string")
            self.assertTrue(table.schema.field("REGISTRY_ID").nullable)
            self.assertEqual(table.to_pylist(), records)

            spec = {
                "schema_version": 1,
                "format": "parquet",
                "root": str(input_root),
                "objects": [
                    {
                        "key": "native.parquet",
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "bytes": path.stat().st_size,
                        "rows": 2,
                    }
                ],
            }
            receipt = normalize_native_input(
                spec,
                {
                    "schema_version": 1,
                    "status": "proposed",
                    "geometry": False,
                    "crs": None,
                    "id_fields": ["OBJECTID"],
                    "coverage_fields": ["REGISTRY_ID"],
                },
                root / "evidence",
            )
            self.assertEqual(receipt["status"], "complete")
            normalized = [
                json.loads(line)
                for line in (root / "evidence" / "normalized_records.jsonl").read_text().splitlines()
            ]
            self.assertEqual(normalized[0]["properties"]["NAICS_CODE"], ["string", "002211"])
            self.assertEqual(normalized[0]["properties"]["REGISTRY_ID"], ["string", "001234567890"])
            self.assertIsNone(normalized[1]["properties"]["REGISTRY_ID"])
            self.assertIsNone(normalized[0]["geometry"])


if __name__ == "__main__":
    unittest.main()
