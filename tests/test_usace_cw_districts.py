from __future__ import annotations

import hashlib
import json
import tempfile
import unittest

import httpx
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_cw_districts import (
    usace_civil_works_districts, usace_civil_works_districts_complete,
    usace_cw_districts_draft_refresh_schedule,
)
from dagster_hifld.catalog import load_staged_geodata, summarize_staged_catalog
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import usace_cw_districts as source


_ITEM = {
    "id": source.ITEM_ID,
    "owner": source._ITEM_OWNER,
    "title": "USACE Civil Works Districts",
    "type": "Feature Service",
    "access": "public",
    "modified": 1737994785000,
    "description": (
        "Polygons showing USACE Civil Works District boundaries. This dataset was digitized from "
        "the NRCS Watershed Boundary Dataset (WBD). Where districts follow administrative boundaries, "
        "such as County and State lines, National Atlas and Census datasets were used. USACE District "
        "GIS POCs also submitted data to incorporate into this dataset. This dataset has been simplified "
        "+/- 30 feet to reduce file size and speed up drawing time. 05/05/20 - Update to show new LRC "
        "boundary. Minor change between LRL and LRH. 07/31/24 - Update to show new SAA Caribbean District."
    ),
}
_SERVICE_DIRECTORY = {"services": [{"name": "usace_cw_districts", "type": "FeatureServer"}]}
_SERVICE = {"serviceItemId": source.ITEM_ID, "maxRecordCount": 1000,
            "serviceDescription": "USACE Civil Works District Boundaries"}
_LAYER = {
    "id": source.LAYER_ID, "name": "USACE Districts", "geometryType": "esriGeometryPolygon",
    "maxRecordCount": 1000, "hasZ": False, "hasM": False,
    "spatialReference": {"wkid": 4140, "latestWkid": 4140},
    "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4140, "latestWkid": 4140}},
    "editingInfo": {"lastEditDate": 1777397738205, "schemaLastEditDate": 1737994782386,
                    "dataLastEditDate": 1737994782386},
    "copyrightText": "US Army Corps of Engineers",
    "description": _ITEM["description"],
    "fields": [{"name": name, "type": typ, "alias": name,
                **({"length": 254} if typ == "esriFieldTypeString" else {})}
               for name, typ in source.FIELDS],
}

_SHELL = [[-92.0, 31.0], [-92.0, 31.5], [-91.5, 31.5], [-91.5, 31.0], [-92.0, 31.0]]
_HOLE = [[-91.9, 31.1], [-91.7, 31.1], [-91.7, 31.3], [-91.9, 31.3], [-91.9, 31.1]]  # counterclockwise
_SECOND_SHELL = [[-90.0, 31.0], [-90.0, 31.2], [-89.8, 31.2], [-89.8, 31.0], [-90.0, 31.0]]


def _fixture_rings(identity: int) -> list[list[list[float]]]:
    if identity == 2:
        return [list(_SHELL), list(_HOLE)]
    if identity == 3:
        return [list(_SHELL), list(_SECOND_SHELL), list(_HOLE)]
    return [list(_SHELL)]


def _fixture_attributes(identity: int) -> dict:
    values = {}
    for name, kind in source.FIELDS:
        if name == "OBJECTID":
            values[name] = identity
        elif kind == "esriFieldTypeDouble":
            values[name] = 11.5 * identity
        elif name == "USACE_DIVISION_CODE":
            values[name] = None
        else:
            values[name] = f"{name}-{identity}"
    return values


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, changed_after=False, schema_drift=False, changed_page_crs=False,
                 geometry_extra_key=False, unclosed_ring=False, out_of_bounds=False,
                 fail_page_once=None, item_denial=False, item_foreign_owner=False,
                 item_description_drift=False, directory_drift=False, service_change_after=False,
                 shifted_roster=False, omit_layer_extent=False, count_drift=False,
                 editing_info_drift=False, page_id_mismatch=False, extra_attribute=False):
        self.ids = (list(range(2, source.EXPECTED_COUNT + 2)) if shifted_roster
                    else list(range(1, source.EXPECTED_COUNT + 1)))
        self.changed_after = changed_after
        self.schema_drift = schema_drift
        self.changed_page_crs = changed_page_crs
        self.geometry_extra_key = geometry_extra_key
        self.unclosed_ring = unclosed_ring
        self.out_of_bounds = out_of_bounds
        self.fail_page_once = fail_page_once
        self.item_denial = item_denial
        self.item_foreign_owner = item_foreign_owner
        self.item_description_drift = item_description_drift
        self.directory_drift = directory_drift
        self.service_change_after = service_change_after
        self.omit_layer_extent = omit_layer_extent
        self.count_drift = count_drift
        self.editing_info_drift = editing_info_drift
        self.page_id_mismatch = page_id_mismatch
        self.extra_attribute = extra_attribute
        self.id_calls = 0
        self.count_calls = 0
        self.service_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.startswith("/sharing/rest/content/items/"):
            if self.item_denial:
                data = {"error": {"code": 403, "messageCode": "GWM_0003",
                                  "message": "Access denied", "details": []}}
            elif self.item_foreign_owner:
                data = {**_ITEM, "owner": "SOMEONE_ELSE"}
            elif self.item_description_drift:
                data = {**_ITEM, "description": "Unrelated item description."}
            else:
                data = _ITEM
        elif path.endswith("/arcgis/rest/services") and query.get("f") == "pjson":
            data = ({"services": []} if self.directory_drift else _SERVICE_DIRECTORY)
        elif path.endswith("/usace_cw_districts/FeatureServer"):
            self.service_calls += 1
            data = dict(_SERVICE)
            if self.service_change_after and self.service_calls == 2:
                data = {**_SERVICE, "serviceDescription": "Some other service description."}
        elif path.endswith("/FeatureServer/0"):
            data = dict(_LAYER)
            if self.schema_drift:
                data["fields"] = [dict(field) for field in _LAYER["fields"]]
                data["fields"][0]["type"] = "esriFieldTypeDouble"
            if self.omit_layer_extent:
                data.pop("extent")
            if self.editing_info_drift:
                data["editingInfo"] = {"dataLastEditDate": "yesterday"}
        elif query.get("returnIdsOnly") == "true":
            self.id_calls += 1
            ids = self.ids[:-1] + [source.EXPECTED_COUNT + 1] if self.changed_after and self.id_calls == 2 else self.ids
            data = {"objectIdFieldName": "OBJECTID", "objectIds": ids}
        elif query.get("returnCountOnly") == "true":
            self.count_calls += 1
            count = source.EXPECTED_COUNT - 1 if self.count_drift and self.count_calls == 2 else source.EXPECTED_COUNT
            data = {"count": count}
        elif path.endswith("/FeatureServer/0/query"):
            requested = [int(value) for value in query["objectIds"].split(",")]
            if requested and self.fail_page_once is not None and requested[0] == self.fail_page_once:
                self.fail_page_once = None
                return httpx.Response(503, stream=httpx.ByteStream(b"temporarily unavailable"), request=request,
                                      headers={"content-type": "text/plain"})
            features = []
            for position, identity in enumerate(requested):
                attributes = _fixture_attributes(identity)
                if self.extra_attribute and position == 0:
                    attributes["EXTRA"] = "unexpected"
                rings = _fixture_rings(identity)
                if self.unclosed_ring:
                    rings = [ring[:-1] for ring in rings]
                if self.out_of_bounds:
                    rings = [[[200.0, 31.0]] + ring[1:-1] + [[200.0, 31.0]] for ring in rings]
                geometry = {"rings": rings}
                if self.geometry_extra_key:
                    geometry["z"] = 100.0
                features.append({"attributes": attributes, "geometry": geometry})
            page_ids = [f["attributes"]["OBJECTID"] for f in features]
            if self.page_id_mismatch and features:
                features[0]["attributes"]["OBJECTID"] = features[-1]["attributes"]["OBJECTID"]
                page_ids = [f["attributes"]["OBJECTID"] for f in features]
            data = {"objectIdFieldName": "OBJECTID", "features": features,
                    "geometryType": "esriGeometryPolygon",
                    "spatialReference": {"wkid": 3857} if self.changed_page_crs else {"wkid": 4140, "latestWkid": 4140},
                    "fields": [{"name": name, "type": typ} for name, typ in source.FIELDS]}
        else:
            return httpx.Response(404, stream=httpx.ByteStream(b"not found"), request=request,
                                  headers={"content-type": "text/plain"})
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request,
                              headers={"content-type": "application/json"})


class UsaceCwDistrictsCaptureTests(unittest.TestCase):
    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="usace-cw-districts-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            result = source.capture_validated(storage, "v20261114T000000Z", client=client)
        return storage, result

    def _prefix(self, storage, version="v20261114T000000Z"):
        return storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, version, "")

    def _refuse(self, transport, expected):
        temporary = tempfile.TemporaryDirectory(prefix="usace-cw-districts-refusal-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, expected):
                source.capture_validated(storage, "v20261114T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))
        return storage

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_civil_works_districts, hifld_ingest_assets)
        self.assertEqual(usace_cw_districts_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_civil_works_districts_complete({"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = usace_civil_works_districts_complete({"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_capture_preserves_native_fields_ids_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], source.EXPECTED_COUNT)
        self.assertFalse(result["release_accepted"])
        self.assertTrue(result["source_population_complete"])
        self.assertEqual(result["ids_sha256"], source.EXPECTED_ID_SHA256)
        prefix = self._prefix(storage)
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), 4)
        self.assertEqual(manifest["pages"][-1]["count"], source.EXPECTED_COUNT - 3 * source.PAGE_SIZE)
        self.assertEqual(manifest["before_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(manifest["native_crs"], "EPSG:4140")
        self.assertEqual(manifest["source_last_data_edit_ms"], 1737994782386)
        self.assertEqual(manifest["item_last_modified_ms"], 1737994785000)
        self.assertFalse(manifest["release_accepted"])
        for name in ("page_0000000", "page_0000010", "page_0000020", "page_0000030"):
            self.assertTrue(storage.object_exists(prefix + f"raw/{name}.json"))
            self.assertTrue(storage.object_exists(prefix + f"raw/{name}.wire"))
            receipt = json.loads(storage.read_key(prefix + f"raw/{name}.receipt.json"))
            self.assertEqual(receipt["sha256"], hashlib.sha256(storage.read_key(prefix + f"raw/{name}.json")).hexdigest())
        parquet_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        parquet_path.close()
        self.addCleanup(lambda: __import__("os").unlink(parquet_path.name))
        with open(parquet_path.name, "wb") as out:
            out.write(storage.read_key(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))
        table = pq.read_table(parquet_path.name)
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)
        self.assertEqual(table.column("OBJECTID").to_pylist(), source.EXPECTED_IDS)
        self.assertEqual(table.column("DISTRICT").to_pylist()[0], "DISTRICT-1")
        self.assertEqual(table.column("USACE_DIVISION_CODE").to_pylist()[0], None)
        self.assertEqual(table.column("Shape__Area").to_pylist()[0], 11.5)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4140)
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(json.loads(storage.read_key(prefix + f"geojson/{source.FILE_SLUG}.geojson"))["type"], "FeatureCollection")

    def test_staged_geojson_preserves_native_crs_through_catalog_reads(self):
        storage, _result = self._capture(ArcGISFixture())
        version = "v20261114T000000Z"
        self.assertEqual(source.NATIVE_GEOJSON_CRS["properties"]["name"], "urn:ogc:def:crs:EPSG::4140")
        staged = load_staged_geodata(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        self.assertEqual(len(staged), source.EXPECTED_COUNT)
        # The source-declared Esri wkid 4140 (NAD83 family) is a deprecated EPSG
        # entry; GDAL normalizes it to the same-datum non-deprecated 4617 when
        # reading the staged GeoJSON crs member. Coordinates are unchanged.
        self.assertEqual(staged.crs.to_epsg(), 4617)
        snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        resolved = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        summary = summarize_staged_catalog(
            storage, source.DATASET_SLUG, source.FILE_SLUG, version,
            source.DATASET_SLUG, source_metadata=resolved.metadata,
        )
        quality = summary.quality_manifest
        self.assertEqual(quality["feature_count"], source.EXPECTED_COUNT)
        self.assertTrue(quality["quality_check_passed"])
        self.assertEqual(quality["bounds"], [-92.0, 31.0, -89.8, 31.5])
        manifest = resolved.metadata
        self.assertFalse(manifest["lineage"]["release_accepted"])
        self.assertFalse(manifest["lineage"]["hifld_continuity"]["hifld_continuity_accepted"])
        self.assertFalse(manifest["lineage"]["producer_succession_claimed"])
        item_input = next(item for item in manifest["inputs"] if item["input_id"].startswith("item:"))
        self.assertEqual(item_input["role"], "producer_publication")
        self.assertEqual(item_input["canonical_url"], source.ITEM_URL)
        self.assertEqual(item_input["distribution_evidence"]["item_owner"], "usace_crrel_als")

    def test_source_manifest_declares_pending_contracts_and_banned_outputs(self):
        storage, _result = self._capture(ArcGISFixture())
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        capture = json.loads(storage.read_key(self._prefix(storage) + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["inputs"]), 13)
        self.assertFalse(manifest["approved"])
        self.assertTrue(all(manifest["lineage"][key] is False for key in (
            "source_contract_accepted", "schema_contract_accepted", "id_contract_accepted",
            "coverage_accepted", "rights_accepted", "baseline_accepted", "release_accepted")))
        self.assertEqual(manifest["unsupported_output_formats"]["geopackage"],
                         "No approved container-schema or release mapping exists.")
        self.assertTrue(storage.object_exists(
            self._prefix(storage) + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))
        self.assertEqual(capture["parquet_sha256"], capture["parquet_sha256"])

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        temporary = tempfile.TemporaryDirectory(prefix="usace-cw-districts-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            live = source.capture_validated(storage, "v20261114T000000Z", client=client)
        replay = source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")
        self.assertEqual(live["parquet_sha256"], replay["parquet_sha256"])
        self.assertEqual(live["records_sha256"], replay["records_sha256"])
        manifest = json.loads(storage.read_key(self._prefix(storage, "v20261114T000001Z") + "metadata/source_manifest.json"))
        self.assertEqual(manifest["code_identity_scheme"], "git-commit-blobs-v1")
        self.assertEqual(manifest["lineage"]["retained_source_capture_version"], "v20261114T000000Z")
        self.assertEqual(manifest["lineage"]["release_accepted"], False)
        self.assertEqual(len(manifest["inputs"]), 13)
        self.assertIn("NRCS Watershed Boundary Dataset", manifest["lineage"]["nrcs_digitization_lineage"])

    def test_transient_page_failure_is_retained_before_bounded_retry(self):
        storage, result = self._capture(ArcGISFixture(fail_page_once=11))
        self.assertEqual(result["feature_count"], source.EXPECTED_COUNT)
        prefix = self._prefix(storage)
        self.assertEqual(storage.read_key(prefix + "raw/page_0000010.attempt_1.json"), b"temporarily unavailable")
        receipt = json.loads(storage.read_key(prefix + "raw/page_0000010.attempt_1.receipt.json"))
        self.assertEqual(receipt["status"], 503)
        self.assertEqual(receipt["attempt"], 1)
        self.assertTrue(receipt["complete"])
        self.assertTrue(storage.object_exists(prefix + "raw/page_0000010.json"))
        manifest = json.loads(storage.read_key(prefix + "metadata/source_manifest.json"))
        self.assertIn({"artifact": "raw/page_0000010.attempt_1.receipt.json", "status": 503,
                       "sha256": receipt["sha256"], "wire_sha256": receipt["wire_sha256"]},
                      manifest["lineage"]["failed_retry_responses"])

    def test_mid_capture_id_drift_refuses_a_candidate(self):
        self._refuse(ArcGISFixture(changed_after=True), "source_id_population_drift")

    def test_schema_drift_refuses_before_feature_fetch(self):
        storage = self._refuse(ArcGISFixture(schema_drift=True), "source_schema_drift")
        self.assertTrue(storage.object_exists(self._prefix(storage) + "metadata/quarantine.json"))

    def test_count_drift_refuses_a_candidate(self):
        self._refuse(ArcGISFixture(count_drift=True), "source_count_drift")

    def test_editing_info_drift_refuses_a_candidate(self):
        self._refuse(ArcGISFixture(editing_info_drift=True), "source_editing_info_drift")

    def test_conflicting_page_crs_refuses_before_candidate(self):
        self._refuse(ArcGISFixture(changed_page_crs=True), "source_crs_drift")

    def test_unexpected_geometry_key_refuses_instead_of_dropping_it(self):
        self._refuse(ArcGISFixture(geometry_extra_key=True), "source_polygon_geometry_structure_drift")

    def test_unclosed_ring_refuses(self):
        self._refuse(ArcGISFixture(unclosed_ring=True), "source_polygon_ring_invalid")

    def test_out_of_bounds_coordinate_refuses(self):
        self._refuse(ArcGISFixture(out_of_bounds=True), "source_polygon_coordinate_out_of_epsg4140_bounds")

    def test_page_id_mismatch_refuses(self):
        self._refuse(ArcGISFixture(page_id_mismatch=True), "source_page_id_or_order_drift")

    def test_unexpected_column_in_page_refuses(self):
        self._refuse(ArcGISFixture(extra_attribute=True), "source_page_attribute_roster_drift")

    def test_item_denial_body_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(item_denial=True)
        storage = self._refuse(transport, "invalid_arcgis_response:item")
        self.assertEqual(transport.id_calls, 0)
        self.assertEqual(transport.service_calls, 0)
        self.assertTrue(storage.object_exists(self._prefix(storage) + "raw/item.json"))
        self.assertTrue(storage.object_exists(self._prefix(storage) + "metadata/quarantine.json"))

    def test_item_identity_drift_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(item_foreign_owner=True)
        storage = self._refuse(transport, "usace_cw_districts_item_identity_drift")
        self.assertEqual(transport.id_calls, 0)
        quarantine = json.loads(storage.read_key(self._prefix(storage) + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "usace_cw_districts_item_identity_drift")

    def test_item_description_drift_is_refused(self):
        self._refuse(ArcGISFixture(item_description_drift=True), "usace_cw_districts_item_description_drift")

    def test_org_service_directory_drift_is_refused_before_layer_request(self):
        transport = ArcGISFixture(directory_drift=True)
        storage = self._refuse(transport, "usace_org_service_directory_drift")
        self.assertEqual(transport.id_calls, 0)

    def test_service_change_during_capture_is_refused(self):
        self._refuse(ArcGISFixture(service_change_after=True),
                     "official_usace_cw_districts_service_changed_during_capture")

    def test_shifted_id_roster_of_same_size_is_refused(self):
        self._refuse(ArcGISFixture(shifted_roster=True), "source_id_population_drift")

    def test_pre_chain_retained_capture_without_item_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="usace-cw-districts-pre-chain-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            source.capture_validated(storage, "v20261114T000000Z", client=client)
        original_prefix = self._prefix(storage)
        for suffix in ("json", "wire", "receipt.json"):
            __import__("os").unlink(__import__("pathlib").Path(temporary.name) / (original_prefix + f"raw/item.{suffix}"))
        with self.assertRaisesRegex(ValueError, "retained_source_response_missing:item"):
            source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")


if __name__ == "__main__":
    unittest.main()
