"""Native preservation, drift refusal and gated publication checks for the IENC LATERAL_BUOY_POINT layer."""
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow
import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import usace_lateral_buoys as source
from dagster_hifld.sources.epa_frs_common import _canonical
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_lateral_buoys import (
    usace_lateral_buoys_complete, usace_lateral_buoys_draft_schedule,
    usace_lateral_buoys_usace_ienc,
)
from dagster_hifld.definitions import defs

# Exact 10-feature population observed from the live LATERAL_BUOY_POINT layer on
# 2026-11-09; the pinned ID/record/geometry/schema snapshot hashes below cover it.
LIVE_PAGE_FEATURES = [
        {"attributes": {"OBJECTID": 1, "Category_of_Lateral_Mark": "Starboard Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Olympus Energy Lighted Danger Buoy", "Color_Pattern": "", "Status": "", "Information": "", "Scale_Minimum": 60000, "Source_Date": "20210303", "Source_Indication": "US,U3,LIGHT,USCG_Light_List_No_365", "Source_Dataset": "U37AG001.000", "Feature_ID": "U3 0000003676 00001"}, "geometry": {"x": -79.77055599999994, "y": 40.535750000000064}},
        {"attributes": {"OBJECTID": 2, "Category_of_Lateral_Mark": "Starboard-Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Russellville Junction Lighted Buoy (208.0)", "Color_Pattern": "", "Status": "", "Information": "Removed during ice season and replaced with an unlighted buoy", "Scale_Minimum": 60000, "Source_Date": "20210323", "Source_Indication": "US,U3,SURVY,SWL_Field_Survey,IENC_21_029", "Source_Dataset": "U37AR186.000", "Feature_ID": "U3 1617842579 00001"}, "geometry": {"x": -93.20314219999995, "y": 35.27340060000006}},
        {"attributes": {"OBJECTID": 3, "Category_of_Lateral_Mark": "Starboard-Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Lake Dardanelle Junction Lighted Buoy (213.9)", "Color_Pattern": "", "Status": "", "Information": "Removed during ice season and replaced with an unlighted buoy", "Scale_Minimum": 60000, "Source_Date": "20210323", "Source_Indication": "US,U3,SURVY,SWL_Field_Survey,IENC_21_029", "Source_Dataset": "U37AR186.000", "Feature_ID": "U3 1617842583 00001"}, "geometry": {"x": -93.27942469999994, "y": 35.315223600000024}},
        {"attributes": {"OBJECTID": 4, "Category_of_Lateral_Mark": "Starboard Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Peoria Heights Lighted Buoy. (167.9)", "Color_Pattern": "", "Status": "", "Information": "Red Nun, Removed during ice season and replaced with unlighted buoy.", "Scale_Minimum": 60000, "Source_Date": "20111103", "Source_Indication": "US,U3,SURVY,MVR_Field_Survey,IENC_12_003", "Source_Dataset": "U37IL137.000", "Feature_ID": "U3 1617847835 00001"}, "geometry": {"x": -89.55136989999994, "y": 40.747360600000036}},
        {"attributes": {"OBJECTID": 5, "Category_of_Lateral_Mark": "Starboard Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Drolls Point Upper Lighted Buoy (169.9)", "Color_Pattern": "", "Status": "", "Information": "Red Nun. Removed during ice season and replaced with unlighted buoy.", "Scale_Minimum": 60000, "Source_Date": "20111103", "Source_Indication": "US,U3,SURVY,MVR_Field_Survey,IENC_12_003", "Source_Dataset": "U37IL137.000", "Feature_ID": "U3 1617848498 00001"}, "geometry": {"x": -89.55796089999996, "y": 40.77820170000007}},
        {"attributes": {"OBJECTID": 6, "Category_of_Lateral_Mark": "Port Hand Lateral Mark", "Buoy_Shape": "Can", "Color": "Green", "Object_Name": "Blue Creek Point Lower Lighted Buoy (172.5)", "Color_Pattern": "", "Status": "", "Information": "Green Can. Removed during ice season and replaced with an unlighted buoy.", "Scale_Minimum": 60000, "Source_Date": "20260415", "Source_Indication": "US,U3,LIGHT,USCG_Light_List_No_7015", "Source_Dataset": "U37IL137.000", "Feature_ID": "U3 1617847841 00001"}, "geometry": {"x": -89.54978389999997, "y": 40.81211890000003}},
        {"attributes": {"OBJECTID": 7, "Category_of_Lateral_Mark": "Port Hand Lateral Mark", "Buoy_Shape": "Can", "Color": "Green", "Object_Name": "WR", "Color_Pattern": "", "Status": "", "Information": "", "Scale_Minimum": 60000, "Source_Date": "20250827", "Source_Indication": "US,U3,LIGHT,USCG_Light_List_No_26047", "Source_Dataset": "U37OH439.000", "Feature_ID": "U3 1574282718 00001"}, "geometry": {"x": -84.56149289999996, "y": 39.09004210000006}},
        {"attributes": {"OBJECTID": 8, "Category_of_Lateral_Mark": "Port-Hand Lateral Mark", "Buoy_Shape": "Can", "Color": "Green", "Object_Name": "Dutchman Island Lighted Buoy (384.8)", "Color_Pattern": "", "Status": "", "Information": "Removed during ice season and replaced with an unlighted buoy.", "Scale_Minimum": 60000, "Source_Date": "20121004", "Source_Indication": "US,U3,SURVY,MVR_Field_Survey,IENC_12_053", "Source_Dataset": "U37UM359.000", "Feature_ID": "U3 1617851139 00001"}, "geometry": {"x": -91.27994739999997, "y": 40.631987200000026}},
        {"attributes": {"OBJECTID": 9, "Category_of_Lateral_Mark": "Starboard-Hand Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": "Waggoner Point Lighted Buoy (367.2)", "Color_Pattern": "", "Status": "", "Information": "Removed during ice season and replaced with an unlighted buoy.", "Scale_Minimum": 60000, "Source_Date": "20121004", "Source_Indication": "US,U3,SURVY,MVR_Field_Survey,IENC_12_053", "Source_Dataset": "U37UM359.000", "Feature_ID": "U3 1617851162 00001"}, "geometry": {"x": -91.37681919999994, "y": 40.43889230000008}},
        {"attributes": {"OBJECTID": 10, "Category_of_Lateral_Mark": "Preferred Channel to Port Lateral Mark", "Buoy_Shape": "Nun", "Color": "Red/Green/Red", "Object_Name": "", "Color_Pattern": "Horizontal Stripe", "Status": "", "Information": "Unlit junction buoy with topmost band red.", "Scale_Minimum": 60000, "Source_Date": "20121004", "Source_Indication": "US,U3,SURVY,MVR_Field_Survey,IENC_12_053", "Source_Dataset": "U37UM481.000", "Feature_ID": "U3 1393230545 00001"}, "geometry": {"x": -90.50110859999995, "y": 41.51746030000004}},
]


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "LATERAL_BUOY_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "LATERAL_BUOY_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 15, "name": "LATERAL_BUOY_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts", "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/LATERAL_BUOY_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = [json.loads(json.dumps(feature)) for feature in LIVE_PAGE_FEATURES if feature["attributes"]["OBJECTID"] in page_ids]
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class LateralBuoysTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_lateral_buoys_usace_ienc, hifld_ingest_assets)
        registered = defs.get_repository_def()
        self.assertIn(usace_lateral_buoys_usace_ienc.key, registered.asset_graph.get_all_asset_keys())
        self.assertIn("usace_lateral_buoys_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(usace_lateral_buoys_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})
        self.assertEqual(usace_lateral_buoys_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_lateral_buoys_complete({"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)
        for name in ("baseline_comparison_accepted", "source_release_accepted", "source_freshness_accepted",
                     "schema_contract_accepted", "id_contract_accepted", "coverage_accepted", "rights_accepted",
                     "mapping_approved", "release_accepted"):
            self.assertFalse(check.metadata[name].value)

    def test_invalid_capture_budget_fails_before_any_request(self):
        with self.assertRaisesRegex(ValueError, "invalid_capture_budget"):
            source.capture_usace_lateral_buoys(None, version="v-test", capture_max_bytes=0)
        with self.assertRaisesRegex(ValueError, "capture_budget_smaller_than_response_limit"):
            source.capture_usace_lateral_buoys(None, version="v-test", capture_max_bytes=100,
                response_max_bytes=101, layer_response_max_bytes=50)
        with self.assertRaisesRegex(ValueError, "configured_capture_budget_exceeds_hard_ceiling"):
            source.capture_usace_lateral_buoys(None, version="v-test",
                capture_max_bytes=source.MAX_CAPTURE_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "configured_capture_budget_exceeds_hard_ceiling"):
            source.capture_usace_lateral_buoys(None, version="v-test",
                response_max_bytes=source.MAX_RESPONSE_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "retained_capture_exceeds_total_byte_budget"):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                (root / "page_000.wire").write_bytes(b"12345")
                source._validate_replay_budget(root, capture_max_bytes=4,
                    response_max_bytes=4, layer_response_max_bytes=4)

    def test_oversized_stream_block_is_refused_before_write_with_canonical_incomplete_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "staging"
            root = Path(temp) / "captures"
            staging.mkdir()
            root.mkdir()
            body = b"0123456789"
            class DeliveredStream(httpx.SyncByteStream):
                def __iter__(self):
                    yield body

            transport = httpx.MockTransport(lambda request: httpx.Response(
                200, headers={"Content-Length": str(len(body))}, stream=DeliveredStream(),
            ))
            with httpx.Client(transport=transport) as client, patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000),
            ), patch.object(source, "_require_capture_capacity"):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                    source._capture(client, "https://fixture.invalid/oversized", root, "page_000",
                        staging=staging, capture_max_bytes=4, response_max_bytes=4,
                        layer_response_max_bytes=4)
            wire = root / "page_000.wire"
            decoded = root / "page_000.body"
            receipt_path = root / "page_000.receipt.json"
            receipt_raw = receipt_path.read_bytes()
            receipt = json.loads(receipt_raw)
            self.assertEqual(wire.read_bytes(), b"")
            self.assertEqual(decoded.read_bytes(), b"")
            self.assertFalse(receipt["complete_body"])
            self.assertEqual(receipt["wire_bytes"], 0)
            self.assertEqual(receipt["bytes"], 0)
            self.assertEqual(receipt["error_type"], "_CaptureBudgetExceeded")
            self.assertEqual(receipt_raw, source._canonical(receipt) + b"\n")

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name, url = source._capture_roster(source.EXPECTED_IDS)[0]
        receipt = {"method": "GET", "url": url, "final_url": url}
        source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = "https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)

    def test_complete_population_native_geometry_pins_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_record_sha256"], source.EXPECTED_RECORD_SHA256)
        self.assertEqual(audit["source_geometry_sha256"], source.EXPECTED_GEOMETRY_SHA256)
        self.assertEqual(audit["source_schema_sha256"], source.EXPECTED_SCHEMA_SHA256)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_feature_id_unique_count"], source.EXPECTED_COUNT)
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(parquet))
        expected_columns = [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"]
        self.assertEqual(table.column_names, expected_columns)
        self.assertTrue(table.schema.field("Category_of_Lateral_Mark").type.equals(pyarrow.string()))
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(pyarrow.int32()))
        self.assertTrue(table.schema.field("OBJECTID").type.equals(pyarrow.int64()))
        # No generated or surrogate identifier column exists: the published candidate carries
        # exactly the declared source attributes plus native geometry.
        self.assertNotIn("id", {name.lower() for name in table.column_names})
        self.assertEqual(table["OBJECTID"].to_pylist(), list(range(1, source.EXPECTED_COUNT + 1)))
        self.assertEqual(table["Feature_ID"].to_pylist(),
                         [feature["attributes"]["Feature_ID"] for feature in LIVE_PAGE_FEATURES])
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])

    def test_rejects_schema_and_count_or_roster_drift(self):
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = layer["fields"][:-1]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = [{**field, "length": 300} if field["name"] == "Color" else field
                           for field in layer["fields"]]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        ids = json.loads(captures["ids_before"][0])
        ids["objectIds"] = ids["objectIds"][:-1]
        captures["ids_before"] = (json.dumps(ids).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
            source._validate(captures)
        captures = _capture_docs()
        count = json.loads(captures["count_after"][0])
        count["count"] = source.EXPECTED_COUNT + 1
        captures["count_after"] = (json.dumps(count).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)

    def test_rejects_id_value_type_or_roster_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["OBJECTID"] = "1"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_oid_value_type_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Feature_ID"] = None
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_record_snapshot_drift"):
            source._validate(captures)

    def test_rejects_geometry_or_declared_type_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = "-90"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Scale_Minimum"] = "60000"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift:Scale_Minimum"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 0.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_zm_dimensions_not_supported"):
            source._validate(captures)

    def test_rejects_out_of_range_and_coverage_drift_coordinates(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = 1000.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_xy_geographic_range_drift:page_000"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["y"] = -90.5
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_xy_geographic_range_drift:page_000"):
            source._validate(captures)
        for index, key, value in ((0, "x", -93.5), (3, "y", 42.0), (0, "x", -79.5), (3, "y", 35.0)):
            captures = _capture_docs()
            page = json.loads(captures["page_000"][0])
            page["features"][index]["geometry"][key] = value
            captures["page_000"] = (json.dumps(page).encode(), {})
            with self.assertRaisesRegex(ValueError, "source_feature_coverage_drift:page_000"):
                source._validate(captures)

    def test_rejects_record_and_geometry_snapshot_drift(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Status"] = "removed"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_record_snapshot_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = -85.0
        page["features"][0]["geometry"]["y"] = 39.5
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_geometry_snapshot_drift"):
            source._validate(captures)

    def test_point_queries_request_z_and_m_and_reject_returned_dimensions(self):
        pages = [(name, url) for name, url in source._capture_roster(source.EXPECTED_IDS)
                 if name.startswith("page_")]
        self.assertEqual(len(pages), 1)
        for _name, url in pages:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["returnZ"], ["true"])
            self.assertEqual(query["returnM"], ["true"])

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_lateral_buoys(storage, version="../outside")

    def _capture_directory(self, root: Path):
        docs = {name: json.loads(body) for name, (body, _receipt) in _capture_docs().items()}
        receipts = {}
        expected_urls = dict(source._capture_roster(list(range(1, source.EXPECTED_COUNT + 1))))
        for name, doc in docs.items():
            body = _canonical(doc)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                       "observed_at": "2026-11-09T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body), "bytes": len(body),
                       "sha256": source._sha(body), "content_encoding": "identity",
                       "content_length": len(body), "error_code": None}
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            receipts[name] = receipt
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True, "receipts": receipts,
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")

    def test_incomplete_capture_is_quarantined_with_retained_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir(parents=True)
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def fake_capture(_client, _url, capture_root, name, *, staging, **_kwargs):
                body = _canonical({"objectIds": [1, 2, 3]} if name == "ids_before" else {})
                (capture_root / f"{name}.body").write_bytes(body)
                (capture_root / f"{name}.wire").write_bytes(body)
                receipt = {"method": "GET", "url": _url, "final_url": _url, "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": source._sha(body),
                           "wire_bytes": len(body), "wire_sha256": source._sha(body)}
                (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                return body, receipt

            with patch.object(source.httpx, "Client"), patch.object(source, "_capture", fake_capture), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                with self.assertRaisesRegex(ValueError, "source_seed_id_population_drift"):
                    source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            quarantine_roots = list((Path(tmp) / "staging" / "_quarantine" / source.DATASET_SLUG).iterdir())
            self.assertEqual(len(quarantine_roots), 1)
            failure = json.loads(storage.read_key(f"{quarantine_roots[0].relative_to(Path(tmp) / 'staging')}/metadata/failure.json"))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], ["ids_before"])
            self.assertIsNotNone(storage.read_key(f"{quarantine_roots[0].relative_to(Path(tmp) / 'staging')}/raw/ids_before.body"))

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, \
                    patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                first = source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            code_identity_check.assert_called_once()
            code_identity_args, code_identity_kwargs = code_identity_check.call_args
            self.assertEqual(code_identity_args[1], code_identity_kwargs["identity"]["code_commit"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            parquet_before = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
            receipts_before = {name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                               for name in json.loads((root / "capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-11-10T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), \
                    patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                second = source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertFalse(first["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"), parquet_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before},
                             receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])
            self.assertEqual(first["feature_count"], source.EXPECTED_COUNT)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(use_local=True, local_dir=staging_dir)
            published = PublishedStorageResource(use_local=True, local_dir=published_dir)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.FILE_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.FILE_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError, "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
