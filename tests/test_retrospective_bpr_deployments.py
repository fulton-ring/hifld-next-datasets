"""Native source fidelity, delegation and drift tests for the NCEI retrospective
BPR deployments layer; no acceptance is implied."""
import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import retrospective_bpr_deployments as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def html(body: bytes) -> bytes:
    return b"<!DOCTYPE html><html><head><title>" + body + b"</title></head></html>"


class _ScriptedSource(httpx.BaseTransport):
    """Serves the next scripted (url, body) pair and records the fetch order."""

    def __init__(self, script: list[tuple[str, bytes]]):
        self.script = script
        self.fetched: list[str] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        expected_url, body = self.script[len(self.fetched)]
        if url != expected_url:
            raise AssertionError("unexpected_acquisition_fetch_order:" + url)
        self.fetched.append(url)
        return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                              headers={"content-type": "application/json"})


class _TransportedHTTPX:
    """httpx module proxy that wires the scripted transport into source clients."""

    def __init__(self, real, transport: _ScriptedSource):
        self._real, self._transport = real, transport

    def __getattr__(self, name):
        return getattr(self._real, name)

    def Client(self, **kwargs) -> httpx.Client:
        kwargs["transport"] = self._transport
        return self._real.Client(**kwargs)


class RetrospectiveBprSourceTests(unittest.TestCase):
    FIXTURE_EXTENT = {"xmin": -1746567.0800000002, "ymin": 586099.8000000005,
                      "xmax": 19846439.568791676, "ymax": 7894836.627928906}

    def fixture(self):
        ids = [1572, 1573]
        attrs = []
        features = []
        for index, (oid, x, y) in enumerate(((1572, -1746567.0800000002, 586099.8000000005),
                                             (1573, 19846439.568791676, 7894836.627928906))):
            row = {"STATION_ID": f"46403_{index}", "LATITUDE": 52.65, "LONGITUDE": -156.94,
                   "LOCATION": f"Site {index}", "DEPTH": 4507, "DEPLOYMENT_DATE": 1084060800000,
                   "RECOVERY_DATE": None if index else 1096934400000,
                   "START_DATE": 1084060800000, "END_DATE": 1089244800000,
                   "SAMPLE_RATE": 0.25, "DATA_QUAL": None, "COMMENTS": "No problems",
                   "DEPLOY_PLATFORM": "USCGC Healy", "RECOVERY_PLATFORM": None,
                   "OBJECTID": oid, "DATASET_ID": f"46403_{index}",
                   "DATA_URL": "https://www.ngdc.noaa.gov/hazard/dart/deployments/46403/",
                   "METADATA_URL": None if index else "https://data.nodc.noaa.gov/iso",
                   "DATA_FLAG": None if index else 1, "ID": oid}
            attrs.append(row)
            features.append({"attributes": row, "geometry": {"x": x, "y": y}})
        service = {"currentVersion": 12,
                   "layers": [{"id": 0, "name": "Current DART Deployments"},
                              {"id": 1, "name": "Retrospective BPR Deployments"}]}
        layer = {"id": 1, "name": "Retrospective BPR Deployments", "geometryType": "esriGeometryPoint",
                 "objectIdField": "OBJECTID", "maxRecordCount": 2000, "fields": source.EXPECTED_FIELDS,
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "extent": {**self.FIXTURE_EXTENT,
                            "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
                 "editingInfo": {"lastEditDate": 1794564219406, "schemaLastEditDate": 1794564219406,
                                 "dataLastEditDate": 1794564219406}}
        item = {"id": source.SERVICE_ITEM_ID, "owner": "ncei_noaa", "orgId": "C8EMgrsFcRFL6LrL",
                "type": "Feature Service", "url": source.SERVICE_URL}
        webmap = {"operationalLayers": [{"id": source.DARTS_GROUP_LAYER_ID,
                                         "title": source.DARTS_GROUP_LAYER_TITLE,
                                         "layers": [{"id": source.RETROSPECTIVE_LAYER_ID,
                                                     "title": source.RETROSPECTIVE_LAYER_TITLE,
                                                     "url": source.LAYER_URL,
                                                     "itemId": source.SERVICE_ITEM_ID}]}]}
        docs = {"ncei_landing": html(b"DART Ocean Bottom Pressure Data | National Centers for "
                                     b"Environmental Information (NCEI)"),
                "ncei_map": html(b"Natural Hazards Viewer<script src=\"./assets/index-x.js\">"),
                "webmap": webmap, "service_item": item,
                "service_before": service, "service_after": service,
                "layer_before": layer, "layer_after": layer,
                "count_before": {"count": 2}, "count_after": {"count": 2},
                "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
                "page_000": {"spatialReference": {"wkid": 102100, "latestWkid": 3857},
                             "features": features}}
        return docs

    def validate(self, docs):
        captures = {}
        for name, value in docs.items():
            body = value if isinstance(value, bytes) else canonical(value)
            captures[name] = (body, {})
        with patch.object(source, "EXPECTED_COUNT", 2), \
                patch.object(source, "EXPECTED_IDS", [1572, 1573]), \
                patch.object(source, "PAGE_SIZE", 250), \
                patch.object(source, "EXPECTED_EXTENT", dict(self.FIXTURE_EXTENT)), \
                patch.object(source, "EXPECTED_ID_SHA256",
                             hashlib.sha256(canonical([1572, 1573])).hexdigest()):
            return source._validate(captures)

    def test_native_points_attributes_types_and_nulls_survive_geoparquet(self):
        fields, features, audit = self.validate(self.fixture())
        data = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(data))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1572, 1573])
        self.assertEqual(table.column("ID").to_pylist(), [1572, 1573])
        self.assertEqual(table.column("DEPLOYMENT_DATE").to_pylist(),
                         [1084060800000, 1084060800000])
        self.assertEqual(str(table.schema.field("DEPLOYMENT_DATE").type), "int64")
        self.assertEqual(str(table.schema.field("DEPTH").type), "int32")
        self.assertEqual(str(table.schema.field("LATITUDE").type), "double")
        self.assertEqual(str(table.schema.field("STATION_ID").type), "string")
        self.assertEqual(table.schema.field("OBJECTID").nullable, False)
        self.assertEqual(table.schema.field("RECOVERY_DATE").nullable, True)
        self.assertEqual(table.column("RECOVERY_DATE").to_pylist()[1], None)
        self.assertEqual(table.column("METADATA_URL").to_pylist()[1], None)
        expected_xy = [(-1746567.0800000002, 586099.8000000005),
                       (19846439.568791676, 7894836.627928906)]
        self.assertEqual([struct.unpack("<BIdd", value)[2:]
                          for value in table.column("geometry").to_pylist()], expected_xy)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"],
                         {"authority": "EPSG", "code": 3857})
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["source_null_counts"]["RECOVERY_DATE"], 1)
        self.assertEqual(audit["source_identifier_counts"]["STATION_ID"], 2)
        self.assertEqual(audit["source_last_edit_utc"], "2026-11-13T10:03:39.406000Z")
        self.assertTrue(audit["source_population_complete"])

    def test_incomplete_page_and_schema_drift_fail_closed(self):
        docs = self.fixture()
        docs["page_000"]["features"].pop()
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            self.validate(docs)
        docs = self.fixture()
        docs["layer_before"]["fields"] = docs["layer_before"]["fields"][:-1]
        docs["layer_after"] = docs["layer_before"]
        with self.assertRaisesRegex(ValueError, "source_layer_identity_or_schema_drift"):
            self.validate(docs)

    def test_missing_or_duplicate_identifiers_fail_closed(self):
        for field in ("OBJECTID", "ID", "STATION_ID", "DATASET_ID"):
            docs = self.fixture()
            docs["page_000"]["features"][1]["attributes"][field] = \
                docs["page_000"]["features"][0]["attributes"][field]
            label = "source_objectid_drift" if field == "OBJECTID" else "source_identifier_not_unique"
            with self.subTest(field=field), \
                    self.assertRaisesRegex(ValueError, label):
                self.validate(docs)
        docs = self.fixture()
        docs["page_000"]["features"][0]["attributes"]["OBJECTID"] = None
        with self.assertRaisesRegex(ValueError, "source_objectid_drift"):
            self.validate(docs)

    def test_value_type_width_and_geometry_drift_fail_closed(self):
        def with_change(change):
            docs = self.fixture()
            change(docs["page_000"]["features"][0])
            return docs
        changes = {
            "source_string_type_drift": lambda f: f["attributes"].__setitem__("STATION_ID", 5),
            "source_string_length_drift": lambda f: f["attributes"].__setitem__("COMMENTS", "x" * 3001),
            "source_integer_type_drift": lambda f: f["attributes"].__setitem__("DEPTH", 4507.5),
            "source_double_type_drift": lambda f: f["attributes"].__setitem__("LATITUDE", "52.65"),
            "source_date_epoch_ms_drift": lambda f: f["attributes"].__setitem__("DEPLOYMENT_DATE", "2004-01-05"),
            "source_oid_type_drift": lambda f: f["attributes"].__setitem__("OBJECTID", None),
            "source_attribute_roster_or_order_drift": lambda f: f["attributes"].__setitem__("NEW", 1),
            "source_point_geometry_drift": lambda f: f["geometry"].__setitem__("z", 0),
            "source_coordinate_outside_declared_extent": lambda f: f["geometry"].__setitem__("x", -2.0e7),
        }
        for label, change in changes.items():
            expected = "source_objectid_drift" if label == "source_oid_type_drift" else label
            with self.subTest(change=label), \
                    self.assertRaisesRegex(ValueError, expected):
                self.validate(with_change(change))
        docs = self.fixture()
        docs["page_000"]["features"][0]["attributes"]["DEPTH"] = 2 ** 31
        with self.assertRaisesRegex(ValueError, "source_integer_type_drift"):
            self.validate(docs)

    def test_transfer_limit_and_count_roster_drift_fail_closed(self):
        docs = self.fixture()
        docs["page_000"]["exceededTransferLimit"] = True
        with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
            self.validate(docs)
        docs = self.fixture()
        docs["count_after"]["count"] = 1
        with self.assertRaisesRegex(ValueError, "source_count_drift"):
            self.validate(docs)
        docs = self.fixture()
        docs["ids_after"]["objectIds"] = [1572]
        with self.assertRaisesRegex(ValueError, "source_id_population_changed_during_capture"):
            self.validate(docs)

    def test_producer_item_webmap_delegation_and_map_entry_fail_closed(self):
        docs = self.fixture()
        docs["service_item"]["owner"] = "someone_else"
        with self.assertRaisesRegex(ValueError, "producer_service_item_identity_drift"):
            self.validate(docs)
        docs = self.fixture()
        docs["webmap"]["operationalLayers"][0]["layers"][0]["url"] = "https://example.com/mirror"
        with self.assertRaisesRegex(ValueError, "delegation_changed"):
            self.validate(docs)
        docs = self.fixture()
        docs["ncei_map"] = html(b"Some other viewer")
        with self.assertRaisesRegex(ValueError, "official_noaa_map_entry_changed"):
            self.validate(docs)
        docs = self.fixture()
        docs["ncei_landing"] = html(b"Unrelated page")
        with self.assertRaisesRegex(ValueError, "official_noaa_landing_changed"):
            self.validate(docs)

    def test_conflicting_crs_and_layer_change_fail_closed(self):
        for target, label in (("layer_before", "source_layer_crs_drift"),
                              ("page_000", "page_000_response_crs_drift")):
            docs = self.fixture()
            if target == "layer_before":
                docs[target]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
                docs["layer_after"] = docs[target]
            else:
                docs[target]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
            with self.subTest(target=target), \
                    self.assertRaisesRegex(ValueError, label):
                self.validate(docs)
        docs = self.fixture()
        docs["layer_after"] = {**docs["layer_after"], "maxRecordCount": 1000}
        with self.assertRaisesRegex(ValueError, "source_service_or_layer_changed_during_capture"):
            self.validate(docs)

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit_isolated_local_staging_required"):
            source.capture_retrospective_bpr_deployments(
                StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"),
                version="v1")
        with self.assertRaisesRegex(ValueError, "explicit_isolated_local_staging_required"):
            source.capture_retrospective_bpr_deployments(
                PublishedStorageResource(use_local=True, local_dir="/tmp/x"), version="v1")

    def _patched_constants(self):
        return patch.multiple(
            source, EXPECTED_COUNT=2, EXPECTED_IDS=[1572, 1573], PAGE_SIZE=250,
            EXPECTED_EXTENT=dict(self.FIXTURE_EXTENT),
            EXPECTED_ID_SHA256=hashlib.sha256(canonical([1572, 1573])).hexdigest())

    def _scripted(self, tmp, docs, *, edit_page=None, edit_layer_after=None):
        with self._patched_constants():
            urls = source._expected_capture_urls()

        def body_of(name):
            value = docs[name]
            return value if isinstance(value, bytes) else canonical(value)

        # The before and after witnesses share endpoints, so the script is an
        # ordered (url, body) sequence: pre-page evidence, the page (possibly
        # edited mid-capture), then possibly edited after witnesses.
        script = [(urls[name], body_of(name)) for name in source._PRE_PAGE_CAPTURE_NAMES]
        page = json.loads(body_of("page_000"))
        if edit_page is not None:
            edit_page(page)
        script.append((urls["page_000"], canonical(page)))
        for name in source._POST_PAGE_CAPTURE_NAMES:
            body = body_of(name)
            if name == "layer_after" and edit_layer_after is not None:
                layer = json.loads(body)
                edit_layer_after(layer)
                body = canonical(layer)
            script.append((urls[name], body))
        transport = _ScriptedSource(script)
        storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
        return transport, storage

    def _run_acquisition(self, tmp, transport, storage):
        with self._patched_constants(), \
                patch.object(source, "httpx", _TransportedHTTPX(httpx, transport)), \
                patch.object(source, "validate_construction_code_identity"), \
                patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
            return source.capture_retrospective_bpr_deployments(
                storage, version="draft", capture_directory=str(Path(tmp) / "captures"))

    def _assert_three_phase_order(self, transport):
        self.assertEqual(transport.fetched, [url for url, _body in transport.script])
        self.assertEqual(len(transport.fetched),
                         len(source._PRE_PAGE_CAPTURE_NAMES) + 1
                         + len(source._POST_PAGE_CAPTURE_NAMES))

    def test_acquisition_brackets_pages_between_before_and_after_witnesses(self):
        with tempfile.TemporaryDirectory() as tmp:
            transport, storage = self._scripted(tmp, self.fixture())
            result = self._run_acquisition(tmp, transport, storage)
            self._assert_three_phase_order(transport)
            self.assertFalse(result["reused"])
            self.assertTrue(result["source_population_complete"])
            self.assertEqual(result["feature_count"], 2)
            for flag in ("source_release_accepted", "source_freshness_accepted",
                         "schema_contract_accepted", "id_contract_accepted", "baseline_accepted",
                         "coverage_accepted", "rights_accepted", "mapping_approved",
                         "release_accepted"):
                self.assertFalse(result[flag])
            audit = json.loads(storage.read_key(
                f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
                "/metadata/source_audit.json"))
            _fields, _features, expected = self.validate(self.fixture())
            self.assertEqual(audit["source_record_sha256"], expected["source_record_sha256"])
            self.assertEqual(audit["source_geometry_sha256"], expected["source_geometry_sha256"])
            self.assertEqual(audit["source_schema_sha256"], expected["source_schema_sha256"])
            self.assertEqual(audit["source_null_counts"], expected["source_null_counts"])
            self.assertEqual(audit["source_last_edit_epoch_ms"],
                             expected["source_last_edit_epoch_ms"])

    def test_mid_page_edit_is_rejected_by_after_witness_comparison(self):
        # An attribute is edited when the page is requested and the agency
        # metadata witnesses record it (editingInfo moves); the after
        # witnesses, fetched only after all pages, must expose the edit.
        def edit_page(page):
            page["features"][0]["attributes"]["COMMENTS"] = "Edited during pagination"

        def edit_layer_after(layer):
            layer["editingInfo"]["lastEditDate"] += 1

        with tempfile.TemporaryDirectory() as tmp:
            transport, storage = self._scripted(tmp, self.fixture(), edit_page=edit_page,
                                                edit_layer_after=edit_layer_after)
            with self.assertRaisesRegex(ValueError,
                                        "source_service_or_layer_changed_during_capture"):
                self._run_acquisition(tmp, transport, storage)
            self._assert_three_phase_order(transport)
            root = next((Path(tmp) / "captures" / "draft").iterdir())
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            quarantine = list((Path(tmp) / "staging").glob(
                "_quarantine/retrospective-bpr-deployments/*/metadata/failure.json"))
            self.assertEqual(len(quarantine), 1)
            self.assertEqual(json.loads(quarantine[0].read_bytes())["reason_code"], "ValueError")


class RetrospectiveBprRegistrationTests(unittest.TestCase):
    def test_registered_asset_check_job_and_stopped_schedule(self):
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.assets.retrospective_bpr_deployments import (
            retrospective_bpr_deployments, retrospective_bpr_draft_schedule,
        )
        from dagster_hifld.definitions import defs

        self.assertEqual(sum(asset is retrospective_bpr_deployments
                             for asset in hifld_ingest_assets), 1)
        job = defs.resolve_job_def("retrospective_bpr_draft_refresh")
        self.assertIn("ingest__retrospective_bpr_deployments__retrospective_bpr_deployments",
                      job.graph.node_names())
        self.assertIn(retrospective_bpr_draft_schedule, defs.schedules)
        self.assertEqual(retrospective_bpr_draft_schedule.default_status.value, "STOPPED")

    def test_population_check_requires_complete_native_count(self):
        from dagster_hifld.assets.retrospective_bpr_deployments import (
            retrospective_bpr_population_complete,
        )

        self.assertTrue(retrospective_bpr_population_complete({
            "source_population_complete": True, "feature_count": 524}).passed)
        self.assertFalse(retrospective_bpr_population_complete({
            "source_population_complete": False, "feature_count": 524}).passed)
        self.assertFalse(retrospective_bpr_population_complete({
            "source_population_complete": True, "feature_count": 523}).passed)


if __name__ == "__main__":
    unittest.main()
