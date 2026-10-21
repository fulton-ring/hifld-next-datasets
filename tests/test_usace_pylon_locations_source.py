from __future__ import annotations

import json
import tempfile
import unittest

import httpx
import pyarrow.parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_pylon_locations as source


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, changed_after=False, schema_drift=False):
        self.ids = list(range(1, source.EXPECTED_COUNT + 1))
        self.changed_after = changed_after
        self.schema_drift = schema_drift
        self.id_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.endswith("/MapServer/21"):
            fields = [{"name": name, "type": typ, "alias": name, **({"length": 254} if typ == "esriFieldTypeString" else {})} for name, typ in source.SOURCE_SCHEMA]
            if self.schema_drift:
                fields[1]["type"] = "esriFieldTypeInteger"
            data = {"id": 21, "name": "PYLONS_POINT", "geometryType": "esriGeometryPoint",
                    "maxRecordCount": 2000, "spatialReference": {"wkid": 4326}, "fields": fields}
        elif query.get("returnIdsOnly") == "true":
            self.id_calls += 1
            ids = self.ids[:-1] + [source.EXPECTED_COUNT + 1] if self.changed_after and self.id_calls == 2 else self.ids
            data = {"objectIdFieldName": "OBJECTID", "objectIds": ids}
        elif query.get("returnCountOnly") == "true":
            data = {"count": source.EXPECTED_COUNT}
        else:
            requested = [int(value) for value in query["objectIds"].split(",")]
            features = []
            for identity in requested:
                attrs = {name: (identity if name == "OBJECTID" else 123 if name == "Scale_Minimum" else "" if name != "Feature_ID" else f"id-{identity}") for name, _ in source.FIELDS}
                features.append({"attributes": attrs, "geometry": {"x": -79.5 + identity / 100000, "y": 40.0, "spatialReference": {"wkid": 4326}}})
            data = {"features": features, "exceededTransferLimit": False}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request, headers={"content-type": "application/json"})


class PylonLocationCaptureTests(unittest.TestCase):
    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="pylon-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            result = source.capture(storage, "v20261021T000000Z", client=client)
        return storage, result

    def test_complete_capture_preserves_native_fields_xy_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], 4079)
        self.assertFalse(result["release_accepted"])
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), 21)
        self.assertEqual(manifest["pages"][-1]["count"], 79)
        raw_name = prefix + "raw/page_0000000.json"
        wire_name = prefix + "raw/page_0000000.wire"
        receipt_name = prefix + "raw/page_0000000.receipt.json"
        self.assertTrue(storage.object_exists(raw_name))
        self.assertTrue(storage.object_exists(wire_name))
        receipt = json.loads(storage.read_key(receipt_name))
        self.assertEqual(receipt["sha256"], __import__("hashlib").sha256(storage.read_key(raw_name)).hexdigest())
        parquet_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        parquet_path.close()
        self.addCleanup(lambda: __import__("os").unlink(parquet_path.name))
        with open(parquet_path.name, "wb") as out:
            out.write(storage.read_key(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))
        table = pq.read_table(parquet_path.name)
        self.assertEqual(table.num_rows, 4079)
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(1, 4080)))
        self.assertEqual(table.column("native_x")[0].as_py(), -79.5 + 0.00001)
        self.assertEqual(table.column("native_y")[0].as_py(), 40.0)
        self.assertEqual(json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["geometry_types"], ["Point"])

    def test_before_after_id_drift_refuses_a_candidate(self):
        transport = ArcGISFixture(changed_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-drift-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_population_changed_during_capture"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_schema_drift_refuses_before_feature_fetch(self):
        transport = ArcGISFixture(schema_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-schema-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertTrue(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "metadata/quarantine.json")))


if __name__ == "__main__":
    unittest.main()
