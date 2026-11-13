"""Native source fidelity, delegation and drift tests for the NCEI retrospective
BPR deployments layer; no acceptance is implied."""
import hashlib
import io
import json
import struct
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import retrospective_bpr_deployments as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def html(body: bytes) -> bytes:
    return b"<!DOCTYPE html><html><head><title>" + body + b"</title></head></html>"


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
