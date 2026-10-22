from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely.geometry import Point

from dagster_hifld.assets.fema_nss_shelter_locations import (
    fema_nss_shelter_locations_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fema_nss_shelter_locations as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


class FemaNssShelterLocationsTests(unittest.TestCase):
    def test_tiny_synthetic_capture_preserves_native_fields_oid_status_dates_and_xy(self):
        fields = [dict(f) for f in source.FIELD_DECLARATIONS]
        attrs = [f for f in fields if f["type"] != "esriFieldTypeGeometry"]
        service = {"description": source.SERVICE_DESCRIPTION}
        layer = {
            "name": source.LAYER_NAME,
            "description": source.LAYER_DESCRIPTION,
            "geometryType": "esriGeometryPoint",
            "definitionExpression": None,
            "hasZ": False,
            "hasM": False,
            "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
            "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
            "fields": fields,
        }
        portal = {"id": source.ITEM_ID, "owner": "FEMA_MAC",
                  "title": "National Shelter System Facilities", "type": "Feature Service",
                  "url": source.PORTAL_LAYER_URL,
                  "access": "public", "modified": 1756399172000}
        ids = {"objectIdFieldName": "objectid", "objectIds": [101, 102]}
        features = []
        for oid, x, y in ((101, -77.04, 38.90), (102, -122.33, 47.61)):
            row = {f["name"]: None for f in attrs}
            row.update({"objectid": oid, "shelter_id": 9001, "shelter_status_code": "TEST",
                        "status": "SYN", "shelter_open_date": 1767225600000,
                        "org_email": None, "org_poc_phone": None})
            features.append({"attributes": row, "geometry": {"x": x, "y": y}})
        bodies = {
            "portal_item": portal,
            "service_before": service,
            "service_after": service,
            "layer_before": layer,
            "layer_after": layer,
            "count_before": {"count": 2},
            "count_after": {"count": 2},
            "ids_before": ids,
            "ids_after": ids,
            "page_000": {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
                         "exceededTransferLimit": False, "features": features},
        }
        observed = {}

        def fake_capture(_client, url, root, name, **kwargs):
            body = json.dumps(bodies[name], separators=(",", ":"), ensure_ascii=False).encode()
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-10-22T00:00:00+00:00", "status": 200,
                       "complete_body": True, "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest()}
            root.mkdir(parents=True, exist_ok=True)
            kwargs["before_retain"]("wire", body)
            kwargs["before_retain"]("body", body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
            observed[name] = url
            return body, receipt

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            storage = StagingStorageResource(local_dir=str(tmp / "staging"), use_local=True,
                                             backend="local")
            with patch.object(source, "_require_disk_space"), \
                 patch.object(source, "capture_get", side_effect=fake_capture), \
                 patch.object(source.httpx, "Client"), \
                 patch.object(source, "processing_identity", return_value={
                     "code_identity_scheme": "test", "working_tree_status": "unknown",
                     "code_commit": "test", "files": {},
                 }), \
                 patch.object(source, "validate_construction_code_identity"):
                result = source.capture_fema_nss_shelter_locations(
                    storage, version="vsynthetic", capture_directory=str(tmp / "capture"))

            self.assertEqual(result["feature_count"], 2)
            self.assertFalse(result["release_accepted"])
            self.assertFalse(result["rights_accepted"])
            self.assertEqual(fema_nss_shelter_locations_draft_schedule.default_status,
                             DefaultScheduleStatus.STOPPED)
            ready = json.loads(storage.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["historical_equivalence_accepted"])
            native_path = (tmp / "staging" / result["candidate_ready"].replace(
                "metadata/candidate_ready.json", f"geoparquet/{source.DATASET_SLUG}.parquet"))
            table = pq.read_table(native_path)
            self.assertEqual(table.column_names,
                             [f["name"] for f in attrs] + ["geometry"])
            self.assertEqual(table["objectid"].to_pylist(), [101, 102])
            self.assertEqual(table["shelter_id"].to_pylist(), [9001, 9001])
            self.assertEqual(table["shelter_status_code"].to_pylist(), ["TEST", "TEST"])
            self.assertEqual(table["shelter_open_date"].to_pylist(), [1767225600000] * 2)
            self.assertEqual(table["org_email"].to_pylist(), [None, None])
            self.assertEqual(table["geometry"].to_pylist(), [
                Point(-77.04, 38.90).wkb, Point(-122.33, 47.61).wkb])
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"],
                             {"authority": "EPSG", "code": 4326})
            self.assertIn("ids_before", observed)
            self.assertEqual(source.DATASET_SLUG, "national-shelter-system-facilities")

    def test_schema_contract_uses_the_declared_oid_and_all_72_attributes(self):
        metadata = {"fields": source.FIELD_DECLARATIONS}
        parsed = source._field_contract(metadata)
        self.assertEqual(len(parsed), 72)
        self.assertEqual([f["name"] for f in parsed if f["type"] == "esriFieldTypeOID"],
                         ["objectid"])
        changed = {"fields": [dict(f) for f in source.FIELD_DECLARATIONS]}
        changed["fields"][0]["alias"] = "changed"
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            source._field_contract(changed)
        self.assertEqual(source.MAX_RESPONSE_BYTES, 4_000_000)
        self.assertLessEqual(source.MAX_CAPTURE_BYTES, 256_000_000)


if __name__ == "__main__":
    unittest.main()
