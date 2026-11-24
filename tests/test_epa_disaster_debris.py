import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.partitions import build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_disaster_debris
from dagster_hifld.sources.epa_disaster_debris import (
    DATASET_SLUG, DOC_URL, EPA_PAGE, ITEM_ID, LAYER_URL, SERVICE_URL,
    _native_parquet, _portal_publication_snapshot, _sha, _source_layer_stable,
    _validated_page_rows, capture_epa_disaster_debris,
)
from dagster_hifld.sources.capture_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)


class EpaDisasterDebrisTests(unittest.TestCase):
    def test_arcgis_continuation_flag_requires_an_exact_complete_ordered_page(self):
        expected = list(range(1, 10001))
        rows = [{"attributes": {"OBJECTID": value}} for value in expected]
        page = {"features": rows, "exceededTransferLimit": True}
        self.assertEqual(_validated_page_rows(page, expected, has_following=True), rows)
        with self.assertRaisesRegex(ValueError, "source_page_truncated_with_transfer_limit_flag"):
            _validated_page_rows({"features": rows[:321], "exceededTransferLimit": True}, expected[:321], has_following=True)
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_duplicated"):
            _validated_page_rows({"features": rows[:-1] + [rows[-2]], "exceededTransferLimit": True}, expected, has_following=True)
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_duplicated"):
            _validated_page_rows({"features": rows[1:], "exceededTransferLimit": True}, expected, has_following=True)
        with self.assertRaisesRegex(ValueError, "source_page_truncated_with_transfer_limit_flag"):
            _validated_page_rows(page, expected, has_following=False)

    def test_portal_view_counter_is_the_only_ignored_publication_change(self):
        before = {"id": "item", "owner": "EPA_GEO", "url": "https://example.gov/service",
                  "title": "EPA 2026", "type": "Feature Service", "access": "public", "numViews": 12}
        after = {**before, "numViews": 13}
        self.assertEqual(_portal_publication_snapshot(before), _portal_publication_snapshot(after))
        self.assertNotEqual(_portal_publication_snapshot(before), _portal_publication_snapshot({**after, "owner": "other"}))
        layer_before = {"name": "layer", "geometryType": "esriGeometryPoint",
                        "fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"}]}
        self.assertTrue(_source_layer_stable(layer_before, dict(layer_before)))
        self.assertFalse(_source_layer_stable(layer_before, {**layer_before, "fields": [{"name": "OBJECTID", "type": "esriFieldTypeString"}]}))

    def test_native_schema_preserves_zeroes_nulls_global_ids_and_native_points(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "Company", "type": "esriFieldTypeString", "length": 100, "nullable": True},
            {"name": "Recovery", "type": "esriFieldTypeSmallInteger", "nullable": True},
            {"name": "Latitude", "type": "esriFieldTypeDouble", "nullable": True},
            {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "nullable": False},
        ]
        features = [
            {"attributes": {"OBJECTID": 41, "Company": "00107", "Recovery": 0, "Latitude": 0.0, "GlobalID": "{aa}"},
             "geometry": {"x": -10701721.5, "y": 4207903.5}},
            {"attributes": {"OBJECTID": 42, "Company": None, "Recovery": None, "Latitude": None, "GlobalID": "{bb}"},
             "geometry": None},
        ]
        table = pq.ParquetFile(io.BytesIO(_native_parquet(fields, features))).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [41, 42])
        self.assertEqual(table.column("Company").to_pylist(), ["00107", None])
        self.assertEqual(table.column("Recovery").to_pylist(), [0, None])
        self.assertEqual(table.column("Latitude").to_pylist(), [0.0, None])
        self.assertEqual(table.column("GlobalID").to_pylist(), ["{aa}", "{bb}"])
        point = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual((point.x, point.y), (-10701721.5, 4207903.5))
        self.assertIsNone(table.column("geometry")[1].as_py())

    def test_first_candidate_seal_cache_and_partition_use_content_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True)
            identity = processing_identity(Path(__file__).parents[1])
            record_sha, geometry_sha, schema_sha = (_sha(b"records"), _sha(b"geometry"), _sha(b"schema"))
            version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
            prefix = candidate_prefix("epa-disaster-debris-recovery-data", "epa-disaster-debris-recovery-data", version)
            self.assertEqual(build_publish_partition_key("epa-disaster-debris-recovery-data", "epa-disaster-debris-recovery-data", version),
                             f"epa-disaster-debris-recovery-data/epa-disaster-debris-recovery-data/{version}")
            receipt = {"complete_body": True, "status": 200, "bytes": 4, "sha256": _sha(b"body"),
                       "wire_bytes": 4, "wire_sha256": _sha(b"wire"), "observed_at": "2026-10-19T00:00:00Z"}
            outputs = {"raw/page.body": b"body", "raw/page.wire": b"wire",
                       "raw/page.receipt.json": json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n",
                       "geoparquet/native.parquet": b"small synthetic native candidate"}
            descriptors = {}
            for name, body in outputs.items():
                write_immutable(storage, f"{prefix}/{name}", body)
                descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
            fingerprint = _sha(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
            state_key = f"_construction-state/epa-disaster-debris-recovery-data/{version}.json"
            payload = {"processing_identity": identity, "source_record_sha256": record_sha,
                       "source_schema_sha256": schema_sha, "capture_receipts": {"page": receipt},
                       "objects": descriptors}
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)
            cached = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version, fingerprint,
                                             identity, record_sha, schema_sha, descriptors, {"page": receipt})
            self.assertTrue(cached["reused"])
            self.assertEqual(set(storage.list_prefix(prefix)), {f"{prefix}/{name}" for name in outputs})
            with self.assertRaisesRegex(ValueError, "immutable_staged_object_changed"):
                write_immutable(storage, f"{prefix}/raw/page.wire", b"wire-mutated")

    def test_failed_page_retains_readable_captures_and_records_unreadable(self):
        item_id = ITEM_ID
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "Object ID", "length": 8, "nullable": False},
            {"name": "FACILITY", "type": "esriFieldTypeString", "alias": "Facility", "length": 100, "nullable": True},
            {"name": "LATITUDE", "type": "esriFieldTypeDouble", "alias": "Latitude", "nullable": True},
        ]
        portal = {"id": item_id, "owner": "EPA_GEO", "access": "public", "type": "Feature Service",
                  "url": SERVICE_URL, "title": "National Disaster Debris Recovery Facilities (EPA 2026)",
                  "modified": 1785000000000, "numViews": 1}
        service = {"serviceItemId": item_id,
                   "layers": [{"id": 0, "name": "Disaster_Debris_Recovery_Data"}]}
        layer = {"name": "Disaster_Debris_Recovery_Data", "geometryType": "esriGeometryPoint",
                 "hasZ": False, "hasM": False, "objectIdField": "OBJECTID",
                 "extent": {"spatialReference": {"latestWkid": 3857}}, "fields": fields}
        landing = (b"<html>EPA Disaster Debris Recovery Tool, national data layer updated in "
                   b"August 2026. Linked public item " + item_id.encode() + b".</html>")
        document = b"%PDF-1.4 synthetic EPA DDRT information document"
        ids_body = json.dumps({"objectIds": [1, 2, 3]}, separators=(",", ":")).encode()
        count_body = json.dumps({"count": 3}, separators=(",", ":")).encode()

        def page_row(n):
            return {"attributes": {"OBJECTID": n, "FACILITY": f"facility-{n}", "LATITUDE": 38.0 + n},
                    "geometry": {"x": -10700000.0 + n, "y": 4200000.0 + n}}

        page_body = json.dumps({"features": [page_row(1), page_row(2)],
                                "spatialReference": {"latestWkid": 3857},
                                "exceededTransferLimit": False}, separators=(",", ":")).encode()
        ids_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
        count_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
        page_url = LAYER_URL + "/query?" + urlencode(
            {"where": "1=1", "resultOffset": 0, "resultRecordCount": 10000,
             "orderByFields": "OBJECTID ASC", "outFields": "*", "returnGeometry": "true",
             "returnZ": "true", "returnM": "true", "f": "json"})
        bodies = {
            EPA_PAGE: landing, DOC_URL: document,
            f"https://www.arcgis.com/sharing/rest/content/items/{item_id}?f=json": json.dumps(portal, separators=(",", ":")).encode(),
            SERVICE_URL + "?f=pjson": json.dumps(service, separators=(",", ":")).encode(),
            LAYER_URL + "?f=pjson": json.dumps(layer, separators=(",", ":")).encode(),
            count_url: count_body, ids_url: ids_body, page_url: page_body,
        }

        def serve(request):
            if str(request.url) == page_url:
                (next((Path(tmp) / "captures").iterdir()) / "ids_before.receipt.json").write_bytes(b'{"')
            return httpx.Response(200, stream=httpx.ByteStream(bodies[str(request.url)]))

        real_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp, patch.object(
                epa_disaster_debris.httpx, "Client",
                side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(serve), **kwargs)):
            storage = StagingStorageResource(local_dir=tmp, use_local=True)
            with self.assertRaises(ValueError) as raised:
                capture_epa_disaster_debris(storage, version="torn-page",
                                            capture_directory=tmp + "/captures")
            self.assertEqual(str(raised.exception), "source_page_id_slice_incomplete_or_duplicated")
            self.assertNotIsInstance(raised.exception, (json.JSONDecodeError, OSError))
            self.assertEqual(storage.list_prefix(DATASET_SLUG), [])
            quarantine_keys = storage.list_prefix("_quarantine/" + DATASET_SLUG)
            failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
            failure = json.loads(storage.read_key(failure_key))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], sorted([
                "ids_seed", "epa_landing", "epa_document", "portal_item",
                "service_before", "layer_before", "count_before", "page_000",
                "layer_after", "service_after", "portal_item_after",
                "count_after", "ids_after"]))
            self.assertEqual(failure["not_retained"],
                             [{"name": "ids_before",
                               "reason": "retained_capture_receipt_unreadable:ids_before"}])
            self.assertTrue(any(key.endswith("/raw/epa_landing.body") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/page_000.wire") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/ids_seed.receipt.json") for key in quarantine_keys))
            root = next((Path(tmp) / "captures").iterdir())
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertNotIn("receipts", manifest)


if __name__ == "__main__":
    unittest.main()
