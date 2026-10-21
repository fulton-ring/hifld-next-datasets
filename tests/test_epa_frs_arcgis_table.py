from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx

from dagster_hifld.assets.epa_frs_naics_powerplants import TABLE_PROFILE
from dagster_hifld.sources.epa_frs_arcgis_table import ArcGISTableCaptureError, capture_table


class EPAFRSArcGISTableTests(unittest.TestCase):
    def test_profile_captures_complete_native_pages_and_relationship(self):
        profile = replace(TABLE_PROFILE, expected_rows=2, page_size=1)
        fields = [{"name": name, "type": kind, "nullable": nullable, "length": length}
                  for name, kind, nullable, length in profile.fields]
        table = {
            "name": profile.table_name,
            "geometryType": None,
            "fields": fields,
            "relationships": [{
                "name": profile.relationship_name,
                "relatedTableId": profile.related_table_id,
                "keyField": profile.relationship_key,
                "cardinality": profile.relationship_cardinality,
            }],
        }
        service = {"description": "EIA-860 data most-current as of June 2014.",
                   "tables": [{"id": profile.table_id, "name": profile.table_name}]}
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "NAICS_CODE": "002211",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "NAICS_CODE": "22",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]

        def respond(request):
            parsed = urlparse(str(request.url))
            query = parse_qs(parsed.query)
            if parsed.path.endswith("/MapServer"):
                payload = service
            elif parsed.path.endswith("/14"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": len(rows)}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [row["OBJECTID"] for row in rows]}
            else:
                offset = int(query.get("resultOffset", [0])[0])
                selected = rows[offset:offset + profile.page_size]
                payload = {"features": [{"attributes": row} for row in selected]}
                if offset == 0:
                    payload["exceededTransferLimit"] = True
            return _json_response(payload)

        with tempfile.TemporaryDirectory() as tmp:
            with httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False) as client:
                captured, manifest, receipts = capture_table(client, profile, Path(tmp))
        self.assertEqual(captured, rows)
        self.assertTrue(manifest["source_ids_equal_before_after"])
        self.assertEqual(manifest["rows_captured"], 2)
        self.assertEqual(manifest["relationship"][0]["name"], "POWERPLANT_LAYER")
        self.assertIn("page_001", receipts)
        self.assertTrue(all(receipt[1]["complete_body"] for receipt in receipts.values()))

    def test_incomplete_final_page_is_rejected(self):
        profile = replace(TABLE_PROFILE, expected_rows=3, page_size=2)
        fields = [{"name": name, "type": kind, "nullable": nullable, "length": length}
                  for name, kind, nullable, length in profile.fields]
        table = {"name": profile.table_name, "geometryType": None, "fields": fields,
                 "relationships": [{"name": profile.relationship_name,
                                    "relatedTableId": profile.related_table_id,
                                    "keyField": profile.relationship_key,
                                    "cardinality": profile.relationship_cardinality}]}
        service = {"description": "EIA-860 data most-current as of June 2014.",
                   "tables": [{"id": profile.table_id, "name": profile.table_name}]}

        def respond(request):
            parsed = urlparse(str(request.url))
            query = parse_qs(parsed.query)
            if parsed.path.endswith("/MapServer"):
                payload = service
            elif parsed.path.endswith("/14"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": 3}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [1, 2, 3]}
            else:
                offset = int(query.get("resultOffset", [0])[0])
                features = [{"attributes": {
                    "OBJECTID": oid, "REGISTRY_ID": None, "NAICS_CODE": "22",
                    "PRIMARY_INDICATOR": "UNKNOWN",
                }} for oid in range(offset + 1, min(offset + profile.page_size, 3) + 1)]
                payload = {"features": features, "exceededTransferLimit": True}
            return _json_response(payload)

        with tempfile.TemporaryDirectory() as tmp:
            with httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False) as client:
                with self.assertRaisesRegex(ArcGISTableCaptureError, "source_final_page_exceeded_transfer_limit"):
                    capture_table(client, profile, Path(tmp))


if __name__ == "__main__":
    unittest.main()


class _JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        self.body = json.dumps(payload, separators=(",", ":")).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def _json_response(payload):
    return httpx.Response(200, stream=_JSONStream(payload), headers={"content-type": "application/json"})
