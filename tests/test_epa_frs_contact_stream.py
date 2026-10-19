import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from dagster_hifld.sources.epa_frs_contact_stream import (
    EPAFRSContactCaptureError,
    FIELDS,
    SERVICE_URL,
    TABLE_ID,
    TABLE_NAME,
    TABLE_URL,
    PageResponse,
    capture_to_parquet,
)


def _table():
    return {"name": TABLE_NAME, "geometryType": None,
            "fields": [{"name": name, "type": esri, "nullable": nullable, "length": length}
                       for name, esri, nullable, length in FIELDS],
            "relationships": [{"name": "PROGRAM_X", "relatedTableId": 8,
                               "keyField": "KEY_FIELD", "cardinality": "esriRelCardinalityOneToMany"}]}


def _row(oid, registry_id):
    return {"OBJECTID": oid, "PGM_SYS_ACRNM": "TEST", "PGM_SYS_ID": registry_id,
            "FULL_NAME": "fixture only", "PHONE_NUMBER": None, "MAILING_ADDRESS": None,
            "SUPPLEMENTAL_ADDRESS": None, "CITY_NAME": "CITY", "STATE_CODE": "ZZ",
            "POSTAL_CODE": "00123", "KEY_FIELD": "TEST" + registry_id}


class EPAFRSContactStreamTests(unittest.TestCase):
    def test_streams_pages_and_preserves_gaps_nulls_and_leading_zero_strings(self):
        rows = [_row(2, "000000000041"), _row(5, "000000000052"), _row(20, "000000000063")]
        table = _table()
        calls = []

        def request(_client, url, params):
            calls.append((url, dict(params)))
            if url == SERVICE_URL:
                payload = {"tables": [{"id": TABLE_ID, "name": TABLE_NAME}]}
            elif url == TABLE_URL:
                payload = table
            elif params.get("returnCountOnly") == "true":
                payload = {"count": len(rows)}
            elif params.get("orderByFields") == "OBJECTID DESC":
                payload = {"features": [{"attributes": {"OBJECTID": 20}}]}
            elif params.get("returnIdsOnly") == "true":
                low, high = (int(part.split()[-1]) for part in params["where"].split("AND"))
                payload = {"objectIds": [row["OBJECTID"] for row in rows
                                         if low < row["OBJECTID"] <= high]}
            else:
                offset = params["resultOffset"]
                payload = {"features": [{"attributes": row} for row in rows[offset:offset + params["resultRecordCount"]]]}
            body = json.dumps(payload, separators=(",", ":")).encode()
            return PageResponse(body, body, {"content-encoding": "identity"}, 200,
                                url + "?fixture=1")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = capture_to_parquet(None, root / "raw", root / "out.parquet",
                                        expected_rows=3, page_size=2, request=request)
            table_out = pq.read_table(root / "out.parquet")
            self.assertEqual(result["row_count"], 3)
            self.assertTrue(result["id_roster_equal_before_after"])
            self.assertEqual(table_out.column("PGM_SYS_ID").to_pylist(),
                             ["000000000041", "000000000052", "000000000063"])
            self.assertEqual(table_out.column("PHONE_NUMBER").to_pylist(), [None, None, None])
            self.assertEqual(len(result["page_receipts"]), 2)
            self.assertTrue(any(item["query"].get("resultRecordCount") == 1
                                for item in result["metadata_receipts"]))
            self.assertTrue((root / "raw" / "page_00000.wire.gz").exists())

    def test_rejects_physical_type_loss_for_native_string_fields(self):
        from dagster_hifld.sources.epa_frs_contact_stream import _validate_attributes
        row = _row(1, "000000000041")
        row["POSTAL_CODE"] = 123
        with self.assertRaisesRegex(EPAFRSContactCaptureError, "source_native_string_type_changed"):
            _validate_attributes(row)

    def test_rejects_extra_native_fields_instead_of_silently_dropping_them(self):
        from dagster_hifld.sources.epa_frs_contact_stream import _validate_attributes
        row = _row(1, "000000000041")
        row["NEW_SOURCE_FIELD"] = "unmapped"
        with self.assertRaisesRegex(EPAFRSContactCaptureError, "source_record_fields_changed"):
            _validate_attributes(row)


if __name__ == "__main__":
    unittest.main()
