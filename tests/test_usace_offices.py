import json
import tempfile
import unittest
from unittest.mock import patch

import httpx
import pyarrow as pa

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_offices as usace
from dagster_hifld.sources.usace_offices import (
    SLUG,
    USACEOfficesSourceError,
    _editing_witness,
    _query_url,
    _source_contract,
    _table,
    _validate_page_schema,
    _validate_rows,
)


def fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
        {"name": "NAME", "type": "esriFieldTypeString", "nullable": True, "length": 3000},
        {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob", "nullable": True},
    ]
    metadata = {"fields": fields, "objectIdField": "OBJECTID", "geometryType": "esriGeometryPoint",
                "spatialReference": {"wkid": 4326}}
    features = [{"attributes": {"OBJECTID": 7, "NAME": "Example", "SE_ANNO_CAD_DATA": None},
                 "geometry": {"x": -77.0, "y": 39.0, "spatialReference": {"wkid": 4326}}}]
    return metadata, features


def live_layer(edit_ms=None):
    """Mirror the real service's layer-0 declarations, including editingInfo."""
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "editable": False},
        {"name": "CODE", "type": "esriFieldTypeString", "alias": "CODE", "nullable": True, "length": 3000, "editable": True},
        {"name": "NAME", "type": "esriFieldTypeString", "alias": "NAME", "nullable": True, "length": 3000, "editable": True},
        {"name": "HQ_DIVISION_OTHER", "type": "esriFieldTypeString", "alias": "HQ_DIVISION_OTHER", "nullable": True, "length": 3000, "editable": True},
        {"name": "DISTRICT_LAB", "type": "esriFieldTypeString", "alias": "DISTRICT_LAB", "nullable": True, "length": 3000, "editable": True},
        {"name": "PHYSICAL_ADDRESS", "type": "esriFieldTypeString", "alias": "PHYSICAL_ADDRESS", "nullable": True, "length": 3000, "editable": True},
        {"name": "LONGITUDE", "type": "esriFieldTypeDouble", "alias": "LONGITUDE", "nullable": True, "editable": True},
        {"name": "LATITUDE", "type": "esriFieldTypeDouble", "alias": "LATITUDE", "nullable": True, "editable": True},
        {"name": "TIME_ZONE", "type": "esriFieldTypeString", "alias": "TIME_ZONE", "nullable": True, "length": 3000, "editable": True},
        {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob", "alias": "SE_ANNO_CAD_DATA", "nullable": True, "editable": True},
    ]
    metadata = {"currentVersion": "10.81", "id": 0, "name": "usace_offices", "type": "Feature Layer",
        "objectIdField": "OBJECTID", "geometryType": "esriGeometryPoint",
        "fields": fields, "maxRecordCount": 2000,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "sourceSpatialReference": {"wkid": 4326, "latestWkid": 4326},
        "advancedQueryCapabilities": {"supportsPagination": True},
        "extent": {"xmin": -180.0, "ymin": -90.0, "xmax": 180.0, "ymax": 90.0,
                   "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "description": "USACE District, Division and Research Lab Locations. This data set does not include field offices."}
    if edit_ms is not None:
        metadata["editingInfo"] = {"dataLastEditDate": edit_ms, "lastEditDate": edit_ms, "schemaLastEditDate": edit_ms}
    return metadata


def feature(feature_id):
    attrs = {"OBJECTID": feature_id, "CODE": "LRC", "NAME": "Example Office", "HQ_DIVISION_OTHER": "HQ",
             "DISTRICT_LAB": "Example District", "PHYSICAL_ADDRESS": "1 Example Way", "LONGITUDE": -77.0,
             "LATITUDE": 39.0, "TIME_ZONE": "Eastern", "SE_ANNO_CAD_DATA": None}
    return {"attributes": attrs, "geometry": {"x": -77.0, "y": 39.0, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}}


def page_body(layer, ids):
    return json.dumps({"objectIdFieldName": layer["objectIdField"], "geometryType": layer["geometryType"],
                       "spatialReference": layer["spatialReference"], "fields": layer["fields"],
                       "features": [feature(i) for i in ids], "exceededTransferLimit": False}).encode()


def serve(responses):
    def handler(request):
        queue = responses.get(str(request.url))
        if not queue:
            raise httpx.ConnectError("unexpected_mock_url")
        body = queue.pop(0)
        return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                              headers={"content-type": "application/json", "content-length": str(len(body))})
    real_client = httpx.Client
    return lambda **kwargs: real_client(transport=httpx.MockTransport(handler), timeout=kwargs.get("timeout", 45),
        follow_redirects=kwargs.get("follow_redirects", True), headers=kwargs.get("headers"))


def corpus(layer_before, layer_after):
    layer_url = usace.BASE + "?f=pjson"
    count_url = _query_url(where="1=1", returnCountOnly="true")
    ids_url = _query_url(where="1=1", returnIdsOnly="true")
    page_url = _query_url(objectIds="1", outFields="*", returnGeometry="true", returnZ="true",
                          returnM="true", orderByFields="OBJECTID")
    return {layer_url: [json.dumps(layer_before).encode(), json.dumps(layer_after).encode()],
            count_url: [json.dumps({"count": 1}).encode(), json.dumps({"count": 1}).encode()],
            ids_url: [json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [1]}).encode()] * 2,
            page_url: [page_body(layer_before, [1])]}


class USACEOfficesSourceTests(unittest.TestCase):
    def test_native_table_retains_declared_nullable_blob_and_all_values(self):
        metadata, features = fixture()
        table, errors = _table(metadata, features)
        self.assertEqual(errors, [])
        self.assertEqual(table.schema.names, ["OBJECTID", "NAME", "SE_ANNO_CAD_DATA", "geometry"])
        self.assertEqual(table.schema.field("SE_ANNO_CAD_DATA").type, pa.binary())
        self.assertTrue(table.schema.field("SE_ANNO_CAD_DATA").nullable)
        self.assertEqual(table.column("NAME").to_pylist(), ["Example"])

    def test_full_source_ids_and_native_point_validate(self):
        metadata, features = fixture()
        self.assertEqual(_validate_rows(features, ("OBJECTID", "NAME", "SE_ANNO_CAD_DATA"), "OBJECTID", [7]), [])
        audit = _source_contract(metadata, 1, 1, [7], [7], features)
        self.assertTrue(audit["id_roster_preserved"])
        self.assertEqual(audit["feature_count"], 1)
        self.assertEqual(audit["quality_errors"], [])

    def test_missing_native_id_or_geometry_fails_closed(self):
        metadata, features = fixture()
        features[0]["attributes"]["OBJECTID"] = None
        features[0]["geometry"] = None
        self.assertEqual(_validate_rows(features, ("OBJECTID", "NAME", "SE_ANNO_CAD_DATA"), "OBJECTID", [7]),
                         ["full_source_id_roster_mismatch", "invalid_object_id", "missing_or_unexpected_native_point_geometry"])

    def test_blob_base64_is_decoded_to_declared_binary_and_invalid_encoding_fails(self):
        metadata, features = fixture()
        features[0]["attributes"]["SE_ANNO_CAD_DATA"] = "AQI="
        table, _ = _table(metadata, features)
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [b"\x01\x02"])
        features[0]["attributes"]["SE_ANNO_CAD_DATA"] = "not base64!"
        with self.assertRaisesRegex(USACEOfficesSourceError, "source_blob_base64_decode_failure"):
            _table(metadata, features)

    def test_physical_field_width_is_retained_in_source_metadata(self):
        metadata, _ = fixture()
        self.assertEqual(metadata["fields"][1]["length"], 3000)

    def test_query_page_type_drift_is_rejected_even_when_names_match(self):
        metadata, _ = fixture()
        page_fields = [dict(field) for field in metadata["fields"]]
        page_fields[1]["type"] = "esriFieldTypeInteger"
        with self.assertRaisesRegex(USACEOfficesSourceError, "query_field_declaration_drift"):
            _validate_page_schema(page_fields, metadata["fields"])


class USACEOfficesEditingWitnessTests(unittest.TestCase):
    LIVE_EDIT_MS = 1789492219096

    def test_witness_extracts_observed_timestamps_or_reports_absence(self):
        metadata = {"editingInfo": {"dataLastEditDate": self.LIVE_EDIT_MS, "lastEditDate": self.LIVE_EDIT_MS,
                                    "schemaLastEditDate": self.LIVE_EDIT_MS}}
        self.assertEqual(_editing_witness(metadata),
                         {"dataLastEditDate": self.LIVE_EDIT_MS, "lastEditDate": self.LIVE_EDIT_MS,
                          "schemaLastEditDate": self.LIVE_EDIT_MS})
        self.assertIsNone(_editing_witness({}))

    def test_witness_rejects_noninteger_values_or_unexpected_members(self):
        for bad in ({"editingInfo": []},
                    {"editingInfo": {"dataLastEditDate": "2026-09-15"}},
                    {"editingInfo": {"dataLastEditDate": 1.5}},
                    {"editingInfo": {"numViews": 3, "dataLastEditDate": 5}},
                    {"editingInfo": {"dataLastEditDate": 5, "lastEditDate": None}}):
            with self.subTest(bad=bad), self.assertRaisesRegex(USACEOfficesSourceError, "source_editing_info_invalid"):
                _editing_witness(bad)

    def test_source_contract_records_data_update_witness_without_release_equation(self):
        metadata, features = fixture()
        metadata["editingInfo"] = {"dataLastEditDate": self.LIVE_EDIT_MS, "lastEditDate": self.LIVE_EDIT_MS,
                                   "schemaLastEditDate": self.LIVE_EDIT_MS}
        audit = _source_contract(metadata, 1, 1, [7], [7], features)
        self.assertEqual(audit["editing_witness"]["dataLastEditDate"], self.LIVE_EDIT_MS)
        self.assertFalse(audit["editing_witness_is_source_release_date"])
        self.assertEqual(audit["source_data_last_edit_date"], self.LIVE_EDIT_MS)
        self.assertIsNone(audit["source_release_date"])
        self.assertIn("editing_witness", audit["release_date_reason"])
        plain_metadata, plain_features = fixture()
        plain_audit = _source_contract(plain_metadata, 1, 1, [7], [7], plain_features)
        self.assertIsNone(plain_audit["editing_witness"])
        self.assertIsNone(plain_audit["source_release_date"])
        self.assertIn("no ArcGIS editingInfo timestamps", plain_audit["release_date_reason"])

    def stage(self, tmp, version, layer_before, layer_after):
        storage = StagingStorageResource(local_dir=str(tmp))
        with patch.object(usace.httpx, "Client", side_effect=serve(corpus(layer_before, layer_after))):
            return usace.stage_usace_offices(storage, version=version, capture_dir=None), storage

    def ready_key(self, version):
        return f"{SLUG}/{SLUG}/{version}/metadata/candidate_ready.json"

    def test_stage_records_observed_witness_and_stages_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, storage = self.stage(tmp, "v20261114a", live_layer(1000), live_layer(1000))
            self.assertEqual(result["rows"], 1)
            self.assertTrue(result["quality_passed"])
            audit = json.loads(storage.read_key(f"{SLUG}/{SLUG}/v20261114a/metadata/source_audit.json"))
            self.assertEqual(audit["editing_witness"], {"dataLastEditDate": 1000, "lastEditDate": 1000, "schemaLastEditDate": 1000})
            self.assertFalse(audit["editing_witness_is_source_release_date"])
            self.assertEqual(audit["source_data_last_edit_date"], 1000)
            self.assertIsNone(audit["source_release_date"])
            self.assertIn("editing_witness", audit["release_date_reason"])
            manifest = json.loads(storage.read_key(f"{SLUG}/{SLUG}/v20261114a/metadata/source_manifest.json"))
            for entry in manifest["inputs"]:
                self.assertIsNone(entry["release_date"])
                self.assertIn("editing_witness", entry["release_date_unknown_reason"])

    def test_stage_rejects_witness_change_between_before_and_after_probes(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(USACEOfficesSourceError, "source_editing_witness_changed_during_capture"):
                self.stage(tmp, "v20261114b", live_layer(1000), live_layer(2000))
            storage = StagingStorageResource(local_dir=str(tmp))
            self.assertIsNone(storage.object_snapshot(self.ready_key("v20261114b")))
            self.assertEqual(storage.list_keys(SLUG, SLUG, "v20261114b"), [])

    def test_stage_rejects_witness_appearing_or_disappearing_between_probes(self):
        for version, before, after in (("v20261114c", live_layer(None), live_layer(1000)),
                                       ("v20261114d", live_layer(1000), live_layer(None))):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(USACEOfficesSourceError, "source_editing_witness_changed_during_capture"):
                    self.stage(tmp, version, before, after)
                storage = StagingStorageResource(local_dir=str(tmp))
                self.assertIsNone(storage.object_snapshot(self.ready_key(version)))


if __name__ == "__main__":
    unittest.main()
