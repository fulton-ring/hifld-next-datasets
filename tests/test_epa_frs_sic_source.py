import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.epa_frs_sic_powerplants import PROFILE, _typed_parquet
from dagster_hifld.native_verification import normalize_native_input
from dagster_hifld.sources.epa_frs_arcgis_table import (
    ArcGISTableCaptureError, _metadata_contract, capture_table,
)


class EPAFRSSICTests(unittest.TestCase):
    def test_complete_source_capture_brackets_exact_ids_and_retains_receipts(self):
        profile = replace(PROFILE, expected_rows=2)
        fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
                  for name, esri_type, nullable, length in profile.fields]
        table = {"name": profile.table_name, "geometryType": None, "fields": fields,
                 "relationships": [{"name": profile.relationship_name,
                                    "relatedTableId": profile.related_table_id,
                                    "keyField": profile.relationship_key,
                                    "cardinality": profile.relationship_cardinality}]}
        service = {"description": "EIA data as of June 2014.",
                   "tables": [{"id": profile.table_id, "name": profile.table_name}]}
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "SECONDARY"},
        ]

        class JSONStream(httpx.SyncByteStream):
            def __init__(self, payload):
                self.body = json.dumps(payload, separators=(",", ":")).encode()

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        def respond(request):
            path = urlparse(str(request.url)).path
            query = parse_qs(urlparse(str(request.url)).query)
            if path.endswith("/MapServer"):
                payload = service
            elif path.endswith("/15"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": 2}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [1, 2]}
            else:
                payload = {"features": [{"attributes": row} for row in rows]}
            return httpx.Response(200, stream=JSONStream(payload), headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            client = httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False)
            with client:
                captured, manifest, receipts = capture_table(client, profile, Path(tmp))
            self.assertEqual(captured, rows)
            self.assertTrue(manifest["source_ids_equal_before_after"])
            self.assertEqual(manifest["rows_captured"], 2)
            self.assertEqual(set(receipts), {"service", "table_before", "count_before", "ids_before",
                "page_000", "table_after", "service_after", "count_after", "ids_after"})
            self.assertTrue(all(receipt["complete_body"] for receipt in manifest["response_receipts"].values()))

    def test_exact_nonspatial_schema_and_native_values_survive_parquet(self):
        fields = [
            {"name": name, "type": esri_type, "nullable": nullable, "length": length}
            for name, esri_type, nullable, length in PROFILE.fields
        ]
        metadata = {
            "name": PROFILE.table_name,
            "geometryType": None,
            "fields": fields,
            "relationships": [{"name": PROFILE.relationship_name,
                               "relatedTableId": PROFILE.related_table_id,
                               "keyField": PROFILE.relationship_key,
                               "cardinality": PROFILE.relationship_cardinality}],
        }
        self.assertEqual(len(_metadata_contract(metadata, PROFILE)), 4)
        changed = {**metadata, "fields": fields + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                                    "nullable": True, "length": 4}]}
        with self.assertRaisesRegex(ArcGISTableCaptureError, "source_table_schema_changed"):
            _metadata_contract(changed, PROFILE)

        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "SIC_CODE": "OWNE",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "table.parquet"
            body = _typed_parquet(rows, tuple(fields))
            path.write_bytes(body)
            table = pq.read_table(path)
            self.assertEqual(table.to_pylist(), rows)
            self.assertEqual(table.schema.names, list(PROFILE.field_names))
            self.assertEqual(str(table.schema.field("SIC_CODE").type), "string")
            self.assertIs(table.schema.field("OBJECTID").nullable, False)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),
                             hashlib.sha256(body).hexdigest())
            with tempfile.TemporaryDirectory() as evidence_root:
                receipt = normalize_native_input(
                    {"schema_version": 1, "format": "parquet", "root": tmp,
                     "objects": [{"key": "table.parquet", "sha256": hashlib.sha256(body).hexdigest(),
                                  "bytes": len(body), "rows": len(rows)}]},
                    {"schema_version": 1, "status": "proposed", "geometry": False, "crs": None,
                     "id_fields": ["OBJECTID"], "coverage_fields": ["REGISTRY_ID"]},
                    Path(evidence_root) / "native-evidence",
                )
                self.assertEqual(receipt["status"], "complete")
                self.assertIs(receipt["policy"]["geometry"], False)
                normalized = [json.loads(line) for line in
                              (Path(evidence_root) / "native-evidence" / "normalized_records.jsonl").read_text().splitlines()]
                self.assertTrue(all(row["geometry"] is None for row in normalized))


if __name__ == "__main__":
    unittest.main()
