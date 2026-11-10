from __future__ import annotations

import json
import tempfile
import unittest

import httpx
import pyarrow.parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_distance_marks as source


_BTS_ITEM = {
    "id": "7ec34864c8804468a9cf52d437fe4b6c",
    "owner": "USDOT_BTS",
    "title": "Inland Electronic Navigational Charts",
    "type": "PDF",
    "access": "public",
    "description": (
        "Inland Electronic Navigational Charts (IENC) web services: "
        "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and master "
        "geodatabase USACE_IENC_Master_Service_gdb.zip."
    ),
}

_SERVICE_DIRECTORY = {"services": [{"name": "IENC_Feature_Classes/DISTANCE_MARK_POINT", "type": "MapServer"}]}
_SERVICE = {"mapName": "DISTANCE_MARK_POINT", "copyrightText": "USACE IENC",
            "layers": [{"id": 10, "name": "DISTANCE_MARK_POINT", "geometryType": "esriGeometryPoint"}]}


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, changed_after=False, schema_drift=False, changed_page_crs=False, changed_feature_crs=False,
                 geometry_z=False, fail_page_once=None, bts_denial=False, bts_foreign_owner=False,
                 directory_drift=False, service_change_after=False, shifted_roster=False):
        self.ids = (list(range(2, source.EXPECTED_COUNT + 2)) if shifted_roster
                    else list(range(1, source.EXPECTED_COUNT + 1)))
        self.changed_after = changed_after
        self.schema_drift = schema_drift
        self.changed_page_crs = changed_page_crs
        self.changed_feature_crs = changed_feature_crs
        self.geometry_z = geometry_z
        self.fail_page_once = fail_page_once
        self.bts_denial = bts_denial
        self.bts_foreign_owner = bts_foreign_owner
        self.directory_drift = directory_drift
        self.service_change_after = service_change_after
        self.id_calls = 0
        self.service_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.startswith("/sharing/rest/content/items/"):
            if self.bts_denial:
                data = {"error": {"code": 403, "messageCode": "GWM_0003",
                                  "message": "Access denied", "details": []}}
            elif self.bts_foreign_owner:
                data = {**_BTS_ITEM, "owner": "SOMEONE_ELSE"}
            else:
                data = _BTS_ITEM
        elif path == "/arcgis/rest/services/IENC_Feature_Classes":
            data = ({"services": []} if self.directory_drift else _SERVICE_DIRECTORY)
        elif path.endswith("/DISTANCE_MARK_POINT/MapServer"):
            self.service_calls += 1
            data = dict(_SERVICE)
            if self.service_change_after and self.service_calls == 2:
                data = {**_SERVICE, "mapName": "SOME_OTHER_MAP"}
        elif path.endswith("/MapServer/10"):
            fields = [{"name": name, "type": typ, "alias": name, **({"length": 254} if typ == "esriFieldTypeString" else {})} for name, typ in source.SOURCE_SCHEMA]
            if self.schema_drift:
                fields[1]["type"] = "esriFieldTypeInteger"
            data = {"id": 10, "name": "DISTANCE_MARK_POINT", "geometryType": "esriGeometryPoint",
                    "maxRecordCount": 2000, "spatialReference": {"wkid": 4326}, "fields": fields,
                    "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}}}
        elif query.get("returnIdsOnly") == "true":
            self.id_calls += 1
            ids = self.ids[:-1] + [source.EXPECTED_COUNT + 1] if self.changed_after and self.id_calls == 2 else self.ids
            data = {"objectIdFieldName": "OBJECTID", "objectIds": ids}
        elif query.get("returnCountOnly") == "true":
            data = {"count": source.EXPECTED_COUNT}
        else:
            requested = [int(value) for value in query["objectIds"].split(",")]
            if requested and requested[0] == self.fail_page_once:
                self.fail_page_once = None
                return httpx.Response(503, stream=httpx.ByteStream(b"temporarily unavailable"), request=request,
                                      headers={"content-type": "text/plain"})
            features = []
            for identity in requested:
                attrs = {name: (identity if name == "OBJECTID" else 123 if name == "Scale_Minimum" else 12.5 if name == "Waterway_Distance" else "" if name != "Feature_ID" else f"id-{identity}") for name, _ in source.FIELDS}
                geometry = {"x": -79.5 + identity / 100000, "y": 40.0,
                            "spatialReference": {"wkid": 3857 if self.changed_feature_crs else 4326}}
                if self.geometry_z:
                    geometry["z"] = 100.0
                features.append({"attributes": attrs, "geometry": geometry})
            data = {"features": features, "exceededTransferLimit": False,
                    "geometryType": "esriGeometryPoint", "spatialReference": {"wkid": 3857 if self.changed_page_crs else 4326},
                    "fields": [{"name": name, "type": typ} for name, typ in source.FIELDS]}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request, headers={"content-type": "application/json"})


class DistanceMarkCaptureTests(unittest.TestCase):
    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            result = source.capture(storage, "v20261021T000000Z", client=client)
        return storage, result

    def test_complete_capture_preserves_native_fields_xy_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], 7453)
        self.assertFalse(result["release_accepted"])
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), 38)
        self.assertEqual(manifest["pages"][-1]["count"], 53)
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
        self.assertEqual(table.num_rows, 7453)
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(1, 7454)))
        self.assertEqual(table.column("native_x")[0].as_py(), -79.5 + 0.00001)
        self.assertEqual(table.column("native_y")[0].as_py(), 40.0)
        self.assertEqual(json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["geometry_types"], ["Point"])

    def test_transient_page_failure_is_retained_before_bounded_retry(self):
        storage, result = self._capture(ArcGISFixture(fail_page_once=3201))
        self.assertEqual(result["feature_count"], 7453)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        body_key = prefix + "raw/page_0003200.attempt_1.json"
        wire_key = prefix + "raw/page_0003200.attempt_1.wire"
        receipt_key = prefix + "raw/page_0003200.attempt_1.receipt.json"
        self.assertEqual(storage.read_key(body_key), b"temporarily unavailable")
        self.assertEqual(storage.read_key(wire_key), b"temporarily unavailable")
        receipt = json.loads(storage.read_key(receipt_key))
        self.assertEqual(receipt["status"], 503)
        self.assertEqual(receipt["attempt"], 1)
        self.assertTrue(receipt["complete"])
        self.assertTrue(storage.object_exists(prefix + "raw/page_0003200.json"))

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            live = source.capture(storage, "v20261021T000000Z", client=client)
        original_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        original_receipt = json.loads(storage.read_key(original_prefix + "raw/page_0000000.receipt.json"))
        replay = source.capture(storage, "v20261021T000001Z", retained_source_version="v20261021T000000Z")
        replay_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000001Z", "")
        replay_receipt = json.loads(storage.read_key(replay_prefix + "raw/page_0000000.receipt.json"))
        self.assertEqual(live["parquet_sha256"], replay["parquet_sha256"])
        self.assertEqual(live["records_sha256"], replay["records_sha256"])
        self.assertEqual(original_receipt, replay_receipt)
        manifest = json.loads(storage.read_key(replay_prefix + "metadata/source_manifest.json"))
        self.assertEqual(manifest["code_identity_scheme"], "git-commit-blobs-v1")
        self.assertEqual(manifest["lineage"]["retained_source_capture_version"], "v20261021T000000Z")
        self.assertEqual(manifest["lineage"]["release_accepted"], False)
        self.assertEqual(len(manifest["inputs"]), 47)
        bts_input = next(item for item in manifest["inputs"] if item["input_id"].startswith("bts_item:"))
        self.assertEqual(bts_input["role"], "official_distributor")
        self.assertEqual(bts_input["producer"], "USDOT BTS IENC NTAD item")
        self.assertEqual(bts_input["canonical_url"], source.BTS_ITEM_URL)
        self.assertEqual(bts_input["delegation_evidence"]["bts_item_owner"], "USDOT_BTS")
        self.assertFalse(manifest["lineage"]["bts_item_succession_claimed"])

    def test_mid_capture_id_drift_refuses_a_candidate(self):
        transport = ArcGISFixture(changed_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-drift-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_schema_drift_refuses_before_feature_fetch(self):
        transport = ArcGISFixture(schema_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-schema-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertTrue(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "metadata/quarantine.json")))

    def test_conflicting_page_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_page_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-page-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_conflicting_feature_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_feature_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-feature-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_unexpected_z_ordinate_refuses_instead_of_dropping_it(self):
        transport = ArcGISFixture(geometry_z=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-z-ordinate-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_dimension_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_bts_denial_body_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(bts_denial=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-bts-denial-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "invalid_arcgis_response:bts_item"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertEqual(transport.service_calls, 0)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        self.assertTrue(storage.object_exists(prefix + "raw/bts_item.json"))
        self.assertTrue(storage.object_exists(prefix + "metadata/quarantine.json"))
        self.assertFalse(storage.object_exists(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))

    def test_bts_identity_drift_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(bts_foreign_owner=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-bts-owner-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertEqual(transport.service_calls, 0)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "bts_ienc_item_identity_drift")
        self.assertFalse(storage.object_exists(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))

    def test_service_directory_drift_is_refused_before_layer_request(self):
        transport = ArcGISFixture(directory_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-directory-drift-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "official_ienc_service_directory_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)

    def test_service_change_during_capture_is_refused(self):
        transport = ArcGISFixture(service_change_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-service-change-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "official_distance_mark_service_changed_during_capture"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_shifted_id_roster_of_same_size_is_refused(self):
        transport = ArcGISFixture(shifted_roster=True)
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-roster-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_pre_repair_retained_capture_without_bts_item_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="distance-marks-pre-repair-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            source.capture(storage, "v20261021T000000Z", client=client)
        original_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        for suffix in ("json", "wire", "receipt.json"):
            __import__("os").unlink(__import__("pathlib").Path(temporary.name) / (original_prefix + f"raw/bts_item.{suffix}"))
        with self.assertRaisesRegex(ValueError, "retained_source_response_missing:bts_item"):
            source.capture(storage, "v20261021T000001Z", retained_source_version="v20261021T000000Z")


class BtsSourceChainContractTests(unittest.TestCase):
    """Fail-closed delegation guard for the official BTS distribution chain (#56)."""

    def test_pins_current_item_and_official_service_chain(self):
        self.assertEqual(source.BTS_ITEM_ID, "7ec34864c8804468a9cf52d437fe4b6c")
        self.assertEqual(
            source.BTS_ITEM_URL,
            "https://www.arcgis.com/sharing/rest/content/items/"
            "7ec34864c8804468a9cf52d437fe4b6c?f=json",
        )
        self.assertEqual(source.SERVICE_DIRECTORY_URL,
                         "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson")
        self.assertEqual(source.SERVICE_URL,
                         "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/"
                         "DISTANCE_MARK_POINT/MapServer")
        self.assertEqual(source._request_url("bts_item", {}), source.BTS_ITEM_URL)
        self.assertEqual(source._request_url("service_directory", {}), source.SERVICE_DIRECTORY_URL)
        self.assertEqual(source._request_url("service_before", {"f": "json"}), source.SERVICE_URL)
        self.assertEqual(source._request_url("service_after", {"f": "json"}), source.SERVICE_URL)
        self.assertEqual(source._request_url("layer", {"f": "json"}), source.LAYER_URL)
        self.assertEqual(source._request_url("count_before", {"f": "json"}), source.LAYER_URL + "/query")
        self.assertEqual(source.EXPECTED_ID_SHA256,
                         "6e54039c7e61cbf1abd27cd8bcb45e3a447f1cb8ef25cd7f81815263596b26b9")
        self.assertEqual(source.EXPECTED_IDS[0], 1)
        self.assertEqual(source.EXPECTED_IDS[-1], source.EXPECTED_COUNT)

    def test_denial_body_and_identity_drift_fail_closed(self):
        denial = {"error": {"code": 403, "messageCode": "GWM_0003",
                            "message": "Access denied", "details": []}}
        cases = [
            ("anonymous arcgis denial body", denial),
            ("foreign item id", {**_BTS_ITEM, "id": "3607d485a9854d6fa458bf507058ceae"}),
            ("foreign owner", {**_BTS_ITEM, "owner": "SOMEONE_ELSE"}),
            ("non-public access", {**_BTS_ITEM, "access": "private"}),
            ("non-pdf type", {**_BTS_ITEM, "type": "Feature Service"}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_bts_item(doc)
                self.assertEqual(str(raised.exception), "bts_ienc_item_identity_drift")

    def test_delegation_marker_drift_fails_closed(self):
        cases = [
            ("missing ienccloud service link", {**_BTS_ITEM, "description": (
                "Inland Electronic Navigational Charts master geodatabase "
                "USACE_IENC_Master_Service_gdb.zip.")}),
            ("missing master gdb link", {**_BTS_ITEM, "description": (
                "Inland Electronic Navigational Charts web services "
                "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes.")}),
            ("missing ienc program title", {**_BTS_ITEM, "title": "Unrelated item",
                                             "description": (
                                                 "Web services "
                                                 "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes "
                                                 "and USACE_IENC_Master_Service_gdb.zip.")}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_bts_item(doc)
                self.assertEqual(str(raised.exception), "bts_ienc_official_distribution_link_drift")

    def test_service_directory_and_service_identity_drift_fail_closed(self):
        with self.assertRaises(ValueError) as raised:
            source._validate_service_directory({"services": [
                {"name": "IENC_Feature_Classes/DAYMARK_POINT", "type": "MapServer"}]})
        self.assertEqual(str(raised.exception), "official_ienc_service_directory_drift")
        service_cases = [
            ("wrong map name", {**_SERVICE, "mapName": "SOME_OTHER_MAP"}),
            ("wrong copyright", {**_SERVICE, "copyrightText": "Someone Else"}),
            ("extra advertised layer", {**_SERVICE, "layers": _SERVICE["layers"] + [{"id": 11, "name": "OTHER", "geometryType": "esriGeometryPoint"}]}),
        ]
        for label, doc in service_cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_service(doc)
                self.assertEqual(str(raised.exception), "official_distance_mark_service_drift")


    def test_normal_repository_registers_distance_marks_refresh_and_preserves_existing_refreshes(self):
        from dagster import AssetKey, DefaultScheduleStatus
        from dagster_hifld.definitions import defs

        repository = defs.get_repository_def()
        expected = {
            "faa_airspace_draft_refresh",
            "noaa_eez_draft_refresh",
            "noaa_contiguous_draft_refresh",
            "distance_marks_usace_ienc_draft_refresh",
        }
        self.assertTrue(expected.issubset({job.name for job in repository.get_all_jobs()}))
        job = repository.get_job("distance_marks_usace_ienc_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(["ingest", source.DATASET_SLUG, source.FILE_SLUG])
        })
        schedule = repository.get_schedule_def("distance_marks_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
