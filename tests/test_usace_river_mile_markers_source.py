from __future__ import annotations

import json
import tempfile
import unittest

import httpx
import pyarrow.parquet as pq

from dagster_hifld.catalog import load_staged_geodata, summarize_staged_catalog
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import usace_river_mile_markers as source


_ITEM = {
    "id": source.ITEM_ID,
    "owner": "usace_iwr_cac",
    "title": "River Mile Markers",
    "type": "Feature Service",
    "access": "public",
    "description": (
        "<div><p>The Navigation Mile Marker dataset is a collection of over 11,000 waterway "
        "milepoints that describe the physical locations of individual mile-markers along inland "
        "navigation waterways in the United States. The file includes data primarily sourced from "
        "the Corps of Engineers Inland Electronic Navigation Charts (IENC) and additionally contains "
        "milepoints from the National Oceanic Atmospheric Administration (NOAA), USACE Corps of "
        "Engineers Districts (Corps) and other various data sources.</p></div>"
    ),
}

_SERVICE_DIRECTORY = {"services": [{"name": "usace_river_mile_markers", "type": "FeatureServer"}]}
_SERVICE = {"serviceItemId": source.ITEM_ID, "maxRecordCount": 1000,
            "serviceDescription": (
                "The Navigation Mile Marker dataset is a collection of over 11,000 waterway "
                "milepoints. The file includes data primarily sourced from the Corps of Engineers "
                "Inland Electronic Navigation Charts (IENC) and additionally contains milepoints "
                "from the National Oceanic Atmospheric Administration (NOAA), USACE Corps of "
                "Engineers Districts (Corps) and other various data sources.")}


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, changed_after=False, schema_drift=False, changed_page_crs=False, changed_feature_crs=False,
                 geometry_z=False, fail_page_once=None, item_denial=False, item_foreign_owner=False,
                 item_description_drift=False, directory_drift=False, service_change_after=False,
                 shifted_roster=False, omit_layer_extent=False):
        self.ids = (list(range(2, source.EXPECTED_COUNT + 2)) if shifted_roster
                    else list(range(1, source.EXPECTED_COUNT + 1)))
        self.changed_after = changed_after
        self.schema_drift = schema_drift
        self.changed_page_crs = changed_page_crs
        self.changed_feature_crs = changed_feature_crs
        self.geometry_z = geometry_z
        self.fail_page_once = fail_page_once
        self.item_denial = item_denial
        self.item_foreign_owner = item_foreign_owner
        self.item_description_drift = item_description_drift
        self.directory_drift = directory_drift
        self.service_change_after = service_change_after
        self.omit_layer_extent = omit_layer_extent
        self.id_calls = 0
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
        elif path.endswith("/usace_river_mile_markers/FeatureServer"):
            self.service_calls += 1
            data = dict(_SERVICE)
            if self.service_change_after and self.service_calls == 2:
                data = {**_SERVICE, "serviceDescription": "Some other service description."}
        elif path.endswith("/FeatureServer/0"):
            fields = [{"name": name, "type": typ, "alias": name,
                       **({"length": 254} if typ == "esriFieldTypeString" else {})}
                      for name, typ in source.FIELDS]
            if self.schema_drift:
                fields[1]["type"] = "esriFieldTypeDouble"
            data = {"id": source.LAYER_ID, "name": "River Mile Markers", "geometryType": "esriGeometryPoint",
                    "maxRecordCount": 1000, "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                    "hasZ": False, "hasM": False,
                    "fields": fields,
                    "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4269, "latestWkid": 4269}}}
            if self.omit_layer_extent:
                data.pop("extent")
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
                attrs = {name: (identity if name == "OBJECTID"
                                else None if name == "RIVER_NUMB" and identity == 11325
                                else 25 if name == "RIVER_NUMB"
                                else identity if name == "MILE"
                                else 113 if name == "name"
                                else round(-92.7 + identity / 100000, 6) if name == "LONGITUDE1"
                                else 31.4525 if name == "LATITUDE1"
                                else "RD" if name == "RIVER_CODE"
                                else "RED" if name == "RIVER_NAME"
                                else "IENC")
                         for name, _ in source.FIELDS}
                geometry = {"x": -92.7 + identity / 100000, "y": 31.4525,
                            **({"spatialReference": {"wkid": 3857}} if self.changed_feature_crs else {})}
                if self.geometry_z:
                    geometry["z"] = 100.0
                features.append({"attributes": attrs, "geometry": geometry})
            data = {"objectIdFieldName": "OBJECTID", "features": features, "exceededTransferLimit": False,
                    "geometryType": "esriGeometryPoint",
                    "spatialReference": {"wkid": 3857} if self.changed_page_crs else {"wkid": 4269, "latestWkid": 4269},
                    "fields": [{"name": name, "type": typ} for name, typ in source.FIELDS]}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request,
                              headers={"content-type": "application/json"})


class BaselineFixture(httpx.BaseTransport):
    """Serves only the layer document (without a declared extent), ID rosters,
    counts and objectIds pages, and raises on any other request."""

    def __init__(self):
        self.ids = list(range(1, source.EXPECTED_COUNT + 1))
        self.id_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.endswith("/FeatureServer/0"):
            fields = [{"name": name, "type": typ, "alias": name} for name, typ in source.FIELDS]
            data = {"id": source.LAYER_ID, "name": "River Mile Markers", "geometryType": "esriGeometryPoint",
                    "maxRecordCount": 1000, "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                    "fields": fields}
        elif query.get("returnIdsOnly") == "true":
            self.id_calls += 1
            data = {"objectIdFieldName": "OBJECTID", "objectIds": self.ids}
        elif query.get("returnCountOnly") == "true":
            data = {"count": source.EXPECTED_COUNT}
        else:
            requested = [int(value) for value in query["objectIds"].split(",")]
            features = []
            for identity in requested:
                attrs = {name: (identity if name == "OBJECTID" else 113 if name == "name"
                                else None if name == "RIVER_NUMB" and identity == 11325
                                else 25 if name == "RIVER_NUMB"
                                else identity if name == "MILE"
                                else -92.7 + identity / 100000 if name == "LONGITUDE1"
                                else 31.4525 if name == "LATITUDE1"
                                else "RD" if name == "RIVER_CODE"
                                else "RED" if name == "RIVER_NAME"
                                else "IENC")
                         for name, _ in source.FIELDS}
                features.append({"attributes": attrs, "geometry": {"x": -92.7 + identity / 100000, "y": 31.4525}})
            data = {"features": features, "exceededTransferLimit": False,
                    "geometryType": "esriGeometryPoint", "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                    "fields": [{"name": name, "type": typ} for name, typ in source.FIELDS]}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request,
                              headers={"content-type": "application/json"})


class RiverMileMarkerCaptureTests(unittest.TestCase):
    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            result = source.capture_validated(storage, "v20261112T000000Z", client=client)
        return storage, result

    def test_complete_capture_preserves_native_fields_xy_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], 11389)
        self.assertFalse(result["release_accepted"])
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), 46)
        self.assertEqual(manifest["pages"][-1]["count"], 139)
        self.assertEqual(manifest["before_id_sha256"], source.EXPECTED_ID_SHA256)
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
        self.assertEqual(table.num_rows, 11389)
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(1, 11390)))
        self.assertEqual(table.column("native_x")[0].as_py(), -92.7 + 0.00001)
        self.assertEqual(table.column("native_y")[0].as_py(), 31.4525)
        self.assertEqual(table.column("RIVER_NUMB")[11324].as_py(), None)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["primary_column"], "geometry")

    def test_staged_geojson_preserves_native_crs_through_catalog_reads(self):
        """The catalog consumes the canonical GeoJSON, not the native_candidate
        Parquet, so the GeoJSON must carry the native CRS: CRS-free readers
        silently assign EPSG:4326 to the EPSG:4269 coordinates (regression)."""
        storage, _result = self._capture(ArcGISFixture())
        version = "v20261112T000000Z"
        self.assertEqual(source.NATIVE_GEOJSON_CRS["properties"]["name"], "urn:ogc:def:crs:EPSG::4269")
        staged = load_staged_geodata(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        self.assertEqual(len(staged), source.EXPECTED_COUNT)
        self.assertEqual(staged.crs.to_epsg(), 4269)
        snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        resolved = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        summary = summarize_staged_catalog(
            storage, source.DATASET_SLUG, source.FILE_SLUG, version,
            source.DATASET_SLUG, source_metadata=resolved.metadata,
        )
        quality = summary.quality_manifest
        self.assertEqual(quality["feature_count"], source.EXPECTED_COUNT)
        self.assertEqual(quality["catalog_mode"], "streaming_geospatial")
        self.assertTrue(quality["quality_check_passed"])
        self.assertEqual(quality["bounds"], [-92.69999, 31.4525, -92.58611, 31.4525])

    def test_transient_page_failure_is_retained_before_bounded_retry(self):
        storage, result = self._capture(ArcGISFixture(fail_page_once=4001))
        self.assertEqual(result["feature_count"], 11389)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        body_key = prefix + "raw/page_0004000.attempt_1.json"
        wire_key = prefix + "raw/page_0004000.attempt_1.wire"
        receipt_key = prefix + "raw/page_0004000.attempt_1.receipt.json"
        self.assertEqual(storage.read_key(body_key), b"temporarily unavailable")
        self.assertEqual(storage.read_key(wire_key), b"temporarily unavailable")
        receipt = json.loads(storage.read_key(receipt_key))
        self.assertEqual(receipt["status"], 503)
        self.assertEqual(receipt["attempt"], 1)
        self.assertTrue(receipt["complete"])
        self.assertTrue(storage.object_exists(prefix + "raw/page_0004000.json"))

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            live = source.capture_validated(storage, "v20261112T000000Z", client=client)
        original_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        original_receipt = json.loads(storage.read_key(original_prefix + "raw/page_0000000.receipt.json"))
        replay = source.capture_validated(storage, "v20261112T000001Z", retained_source_version="v20261112T000000Z")
        replay_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000001Z", "")
        replay_receipt = json.loads(storage.read_key(replay_prefix + "raw/page_0000000.receipt.json"))
        self.assertEqual(live["parquet_sha256"], replay["parquet_sha256"])
        self.assertEqual(live["records_sha256"], replay["records_sha256"])
        self.assertEqual(original_receipt, replay_receipt)
        manifest = json.loads(storage.read_key(replay_prefix + "metadata/source_manifest.json"))
        self.assertEqual(manifest["code_identity_scheme"], "git-commit-blobs-v1")
        self.assertEqual(manifest["lineage"]["retained_source_capture_version"], "v20261112T000000Z")
        self.assertEqual(manifest["lineage"]["release_accepted"], False)
        self.assertEqual(len(manifest["inputs"]), 55)
        item_input = next(item for item in manifest["inputs"] if item["input_id"].startswith("item:"))
        self.assertEqual(item_input["role"], "producer_publication")
        self.assertEqual(item_input["canonical_url"], source.ITEM_URL)
        self.assertEqual(item_input["distribution_evidence"]["item_owner"], "usace_iwr_cac")
        self.assertFalse(manifest["lineage"]["producer_succession_claimed"])
        self.assertIn("no river-mile-marker feature class", manifest["lineage"]["ienc_relationship"])
        self.assertFalse(manifest["lineage"]["hifld_continuity"]["hifld_continuity_accepted"])

    def test_mid_capture_id_drift_refuses_a_candidate(self):
        transport = ArcGISFixture(changed_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-drift-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_schema_drift_refuses_before_feature_fetch(self):
        transport = ArcGISFixture(schema_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-schema-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertTrue(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "metadata/quarantine.json")))

    def test_conflicting_page_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_page_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-page-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_conflicting_feature_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_feature_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-feature-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_unexpected_z_ordinate_refuses_instead_of_dropping_it(self):
        transport = ArcGISFixture(geometry_z=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-z-ordinate-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_dimension_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_item_denial_body_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(item_denial=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-denial-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "invalid_arcgis_response:item"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertEqual(transport.service_calls, 0)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        self.assertTrue(storage.object_exists(prefix + "raw/item.json"))
        self.assertTrue(storage.object_exists(prefix + "metadata/quarantine.json"))
        self.assertFalse(storage.object_exists(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))

    def test_item_identity_drift_is_refused_and_quarantined_before_any_layer_request(self):
        transport = ArcGISFixture(item_foreign_owner=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-owner-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "usace_river_mile_markers_item_identity_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertEqual(transport.service_calls, 0)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "usace_river_mile_markers_item_identity_drift")
        self.assertFalse(storage.object_exists(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))

    def test_org_service_directory_drift_is_refused_before_layer_request(self):
        transport = ArcGISFixture(directory_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-directory-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "usace_org_service_directory_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)

    def test_service_change_during_capture_is_refused(self):
        transport = ArcGISFixture(service_change_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-service-change-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "official_usace_river_mile_markers_service_changed_during_capture"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_shifted_id_roster_of_same_size_is_refused(self):
        transport = ArcGISFixture(shifted_roster=True)
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-roster-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z",
            f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_pre_chain_retained_capture_without_item_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-pre-chain-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=ArcGISFixture()) as client:
            source.capture_validated(storage, "v20261112T000000Z", client=client)
        original_prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        for suffix in ("json", "wire", "receipt.json"):
            __import__("os").unlink(__import__("pathlib").Path(temporary.name) / (original_prefix + f"raw/item.{suffix}"))
        with self.assertRaisesRegex(ValueError, "retained_source_response_missing:item"):
            source.capture_validated(storage, "v20261112T000001Z", retained_source_version="v20261112T000000Z")


class BaselineContractTests(unittest.TestCase):
    """The capture() primitive must keep serving the baseline fixture, while the
    production capture_validated() entry enforces the distribution chain fail-closed."""

    def _storage(self):
        temporary = tempfile.TemporaryDirectory(prefix="river-mile-markers-baseline-")
        self.addCleanup(temporary.cleanup)
        return StagingStorageResource(use_local=True, local_dir=temporary.name)

    def test_capture_primitive_serves_the_baseline_fixture(self):
        storage = self._storage()
        with httpx.Client(transport=BaselineFixture()) as client:
            result = source.capture(storage, "v20261112T000000Z", client=client)
        self.assertEqual(result["feature_count"], 11389)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        manifest = json.loads(storage.read_key(prefix + "metadata/source_manifest.json"))
        self.assertEqual(len(manifest["inputs"]), 51)
        self.assertFalse(any(item["input_id"].startswith("item:") for item in manifest["inputs"]))
        self.assertNotIn("distribution_chain", manifest["lineage"])
        staged = list(storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z"))
        self.assertFalse(any(key.endswith("raw/item.json") for key in staged))

    def test_validated_entry_refuses_the_baseline_fixture_before_layer_traffic(self):
        storage = self._storage()
        transport = BaselineFixture()
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_transport_error"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertTrue(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "metadata/quarantine.json")))

    def test_capture_primitive_tolerates_layer_without_declared_extent(self):
        storage = self._storage()
        with httpx.Client(transport=ArcGISFixture(omit_layer_extent=True)) as client:
            result = source.capture(storage, "v20261112T000000Z", client=client)
        self.assertEqual(result["feature_count"], 11389)

    def test_validated_entry_pins_declared_extent_fail_closed(self):
        storage = self._storage()
        transport = ArcGISFixture(omit_layer_extent=True)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
                source.capture_validated(storage, "v20261112T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261112T000000Z", "")
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "source_layer_extent_drift")


class USACESourceChainContractTests(unittest.TestCase):
    """Fail-closed producer chain guard for the official USACE IWR distribution."""

    def test_pins_current_item_and_official_service_chain(self):
        self.assertEqual(source.ITEM_ID, "604cdc08fe7d43cb90a0584a0b198875")
        self.assertEqual(
            source.ITEM_URL,
            "https://www.arcgis.com/sharing/rest/content/items/"
            "604cdc08fe7d43cb90a0584a0b198875?f=json",
        )
        self.assertEqual(source.ORG_SERVICES_DIRECTORY_URL,
                         "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services?f=pjson")
        self.assertEqual(
            source.SERVICE_URL,
            "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/"
            "usace_river_mile_markers/FeatureServer",
        )
        self.assertEqual(source._request_url("item", {"f": "json"}),
                         "https://www.arcgis.com/sharing/rest/content/items/"
                         "604cdc08fe7d43cb90a0584a0b198875")
        self.assertEqual(source._request_url("service_directory", {"f": "pjson"}),
                         "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services")
        self.assertEqual(source._request_url("service_before", {"f": "json"}), source.SERVICE_URL)
        self.assertEqual(source._request_url("service_after", {"f": "json"}), source.SERVICE_URL)
        self.assertEqual(source._request_url("layer", {"f": "json"}), source.LAYER_URL)
        self.assertEqual(source._request_url("count_before", {"f": "json"}), source.LAYER_URL + "/query")
        self.assertEqual(source.EXPECTED_ID_SHA256,
                         "251c79ad1bed2abb5cdb3a59a2bf95e4b5f468bc160d40764bbc3cf3922ae456")
        self.assertEqual(source.EXPECTED_IDS[0], 1)
        self.assertEqual(source.EXPECTED_IDS[-1], source.EXPECTED_COUNT)

    def test_denial_body_and_identity_drift_fail_closed(self):
        denial = {"error": {"code": 403, "messageCode": "GWM_0003",
                            "message": "Access denied", "details": []}}
        cases = [
            ("anonymous arcgis denial body", denial),
            ("foreign item id", {**_ITEM, "id": "496c6859c35143c69b7458af6049658b"}),
            ("foreign owner", {**_ITEM, "owner": "SOMEONE_ELSE"}),
            ("non-public access", {**_ITEM, "access": "private"}),
            ("non-feature-service type", {**_ITEM, "type": "PDF"}),
            ("wrong title", {**_ITEM, "title": "Unrelated item"}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_item(doc)
                self.assertEqual(str(raised.exception), "usace_river_mile_markers_item_identity_drift")

    def test_item_description_marker_drift_fails_closed(self):
        cases = [
            ("missing mile marker description", {**_ITEM, "description": (
                "Inland Electronic Navigation Charts (IENC) and National Oceanic Atmospheric "
                "Administration (NOAA) milepoints.")}),
            ("missing ienc program name", {**_ITEM, "description": (
                "Navigation Mile Marker dataset with National Oceanic Atmospheric "
                "Administration (NOAA) milepoints.")}),
            ("missing noaa name", {**_ITEM, "description": (
                "Navigation Mile Marker dataset includes data primarily sourced from "
                "Inland Electronic Navigation Charts (IENC).")}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_item(doc)
                self.assertEqual(str(raised.exception), "usace_river_mile_markers_item_description_drift")

    def test_service_directory_and_service_identity_drift_fail_closed(self):
        with self.assertRaises(ValueError) as raised:
            source._validate_service_directory({"services": [
                {"name": "usace_river_mile_markers", "type": "MapServer"}]})
        self.assertEqual(str(raised.exception), "usace_org_service_directory_drift")
        service_cases = [
            ("wrong service item id", {**_SERVICE, "serviceItemId": "496c6859c35143c69b7458af6049658b"}),
            ("invalid max record count", {**_SERVICE, "maxRecordCount": 0}),
            ("wrong description", {**_SERVICE, "serviceDescription": "Some other service."}),
        ]
        for label, doc in service_cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError) as raised:
                    source._validate_service(doc)
                self.assertEqual(str(raised.exception), "official_usace_river_mile_markers_service_drift")


class RepositoryRegistrationTests(unittest.TestCase):
    def test_blocking_completeness_check_passes_only_on_the_pinned_population(self):
        from dagster_hifld.assets.usace_river_mile_markers import river_mile_markers_usace_ienc_complete

        passed = river_mile_markers_usace_ienc_complete({
            "feature_count": source.EXPECTED_COUNT, "source_population_complete": True,
            "ids_sha256": "a" * 64, "records_sha256": "b" * 64, "source_coverage_scope": "scope"})
        self.assertTrue(passed.passed)
        failed = river_mile_markers_usace_ienc_complete({
            "feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True,
            "ids_sha256": "a" * 64, "records_sha256": "b" * 64, "source_coverage_scope": "scope"})
        self.assertFalse(failed.passed)
        incomplete = river_mile_markers_usace_ienc_complete({
            "feature_count": source.EXPECTED_COUNT, "source_population_complete": False,
            "ids_sha256": "a" * 64, "records_sha256": "b" * 64, "source_coverage_scope": "scope"})
        self.assertFalse(incomplete.passed)

    def test_normal_repository_registers_river_mile_markers_refresh(self):
        import inspect

        from dagster import AssetKey, DefaultScheduleStatus
        from dagster_hifld.assets import usace_river_mile_markers as asset_module
        from dagster_hifld.definitions import defs

        repository = defs.get_repository_def()
        self.assertIn("river_mile_markers_usace_ienc_draft_refresh",
                      {job.name for job in repository.get_all_jobs()})
        job = repository.get_job("river_mile_markers_usace_ienc_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys),
                         {AssetKey(["ingest", source.DATASET_SLUG, source.FILE_SLUG])})
        schedule = repository.get_schedule_def("river_mile_markers_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIs(asset_module.capture_validated, source.capture_validated)
        self.assertIn("capture_validated(", inspect.getsource(asset_module))


if __name__ == "__main__":
    unittest.main()
