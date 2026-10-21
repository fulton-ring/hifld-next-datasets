import json
import struct
import unittest

from dagster_hifld.sources.gcca_warehouse_directory import (
    EXPECTED_FIELDS,
    _decode_page,
    _geo_parquet,
    _validate_and_project,
)
import pyarrow as pa
import pyarrow.parquet as pq


def _row(identifier, country, geo):
    return {"series_id": identifier, "fields": {
        "city": "Sample City", "companyname": "Sample Warehouse", "country": country,
        "identifier": identifier, "location_geo": geo, "parentid": None, "state": "CA",
    }}


def _sjts(metadata, records):
    meta = json.dumps(metadata, separators=(",", ":")).encode()
    out = bytearray(b"SJTS" + struct.pack("<I", len(meta)) + meta)
    for record in records:
        body = json.dumps(record, separators=(",", ":")).encode()
        out.extend(struct.pack("<I", len(body)) + body)
    return bytes(out)


class GCCAWarehouseDirectoryTests(unittest.TestCase):
    def test_decode_page_rejects_frame_count_and_duplicate_json_members(self):
        body = _sjts({"success": True, "series": 2, "total": 2,
                      "validate_query": {"type": "structured"}},
                     [_row("a", "United States", None)])
        with self.assertRaisesRegex(ValueError, "fuse_series_count_mismatch"):
            _decode_page(body)
        dup = b'{"success":true,"success":true,"series":0,"total":0,"validate_query":{"type":"structured"}}'
        with self.assertRaisesRegex(ValueError, "source_duplicate_json_member"):
            _decode_page(b"SJTS" + struct.pack("<I", len(dup)) + dup)

    def test_complete_category_and_us_projection_keep_missing_geometry_row(self):
        rows = [
            _row("us-has-geometry", "United States", {"type": "Point", "coordinates": [-120.0, 38.0]}),
            _row("us-missing-geometry", "United States", None),
            _row("foreign", "Canada", {"type": "Point", "coordinates": [-100.0, 50.0]}),
        ]
        facet = {"facets": {"country": {"terms": [
            {"term": "United States", "count": 2}, {"term": "Canada", "count": 1}]}}}
        candidate, audit = _validate_and_project(rows, 3, facet)
        self.assertEqual(len(candidate), 2)
        self.assertEqual(audit["source_population_count"], 3)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["us_geometry_null_count"], 1)
        self.assertEqual({row["fields"]["identifier"] for row in candidate},
                         {"us-has-geometry", "us-missing-geometry"})
        body = _geo_parquet(candidate)
        table = pq.read_table(pa.BufferReader(body))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table["geometry"].null_count, 1)
        self.assertEqual(table["location_geo"].null_count, 1)
        self.assertEqual(table.schema.names, list(EXPECTED_FIELDS) + ["geometry"])

    def test_projection_requires_exact_country_facet_match(self):
        rows = [_row("us", "United States", None)]
        with self.assertRaisesRegex(ValueError, "warehouse_us_projection_does_not_match_source_facet"):
            _validate_and_project(rows, 1, {"facets": {"country": {"terms": []}}})


if __name__ == "__main__":
    unittest.main()
