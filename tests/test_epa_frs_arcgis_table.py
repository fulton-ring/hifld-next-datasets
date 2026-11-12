from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx

from dagster_hifld.assets.epa_frs_naics_powerplants import TABLE_PROFILE, _typed_parquet
from dagster_hifld.sources.epa_frs_arcgis_table import (
    ArcGISTableCaptureError,
    capture_table,
    validate_catalog_derivatives,
)


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
                replayed, replay_manifest, replay_receipts = capture_table(
                    client, profile, Path(tmp), replay_captures=True
                )
        self.assertEqual(captured, rows)
        self.assertEqual(replayed, rows)
        self.assertEqual(replay_receipts, receipts)
        self.assertEqual(replay_manifest["rows_captured"], 2)
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

    def test_catalog_quality_and_native_schema_are_bound_to_captured_rows(self):
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "NAICS_CODE": "002211",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "NAICS_CODE": "22",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]
        fields = tuple({"name": name, "type": esri_type, "nullable": nullable, "length": length}
                       for name, esri_type, nullable, length in TABLE_PROFILE.fields)
        native = _typed_parquet(rows, fields)
        quality = {
            "feature_count": 2, "quality_check_passed": True, "geometry_type": None,
            "spatial_status": "non_spatial_source", "invalid_geometry_count": 0,
            "null_geometry_count": 0,
        }
        dictionary = {"columns": [
            {"name": "OBJECTID", "type": "integer", "numNullValues": 0},
            {"name": "REGISTRY_ID", "type": "string", "numNullValues": 1, "length": 12},
            {"name": "NAICS_CODE", "type": "string", "numNullValues": 0, "length": 6},
            {"name": "PRIMARY_INDICATOR", "type": "string", "numNullValues": 0, "length": 7},
        ]}
        result = validate_catalog_derivatives(
            replace(TABLE_PROFILE, expected_rows=2), rows, native,
            json.dumps(quality).encode(), json.dumps(dictionary).encode(),
        )
        self.assertEqual(result["feature_count"], 2)
        self.assertTrue(result["nonspatial"])
        quality["quality_check_passed"] = False
        with self.assertRaisesRegex(ArcGISTableCaptureError, "cached_catalog_quality_semantics_changed"):
            validate_catalog_derivatives(
                replace(TABLE_PROFILE, expected_rows=2), rows, native,
                json.dumps(quality).encode(), json.dumps(dictionary).encode(),
            )


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
