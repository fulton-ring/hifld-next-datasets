"""Native preservation and completeness checks for the official IENC LATERAL_BEACON_POINT layer."""
import json
import unittest
from urllib.parse import parse_qs, urlsplit

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_lateral_beacons as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.assets.usace_lateral_beacons import (
    usace_lateral_beacons_complete, usace_lateral_beacons_draft_job,
    usace_lateral_beacons_draft_schedule, usace_lateral_beacons_usace_ienc,
)


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "LATERAL_BEACON_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "sourceSpatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "LATERAL_BEACON_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 14, "name": "LATERAL_BEACON_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts", "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/LATERAL_BEACON_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            attrs = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
            attrs.update({"OBJECTID": oid, "Category_of_Lateral_Mark": "port", "Beacon_Shape": "spar",
                "Color": "green", "Color_Pattern": "solid", "Object_Name": "beacon",
                "Direction_of_Impact": None, "Condition": "good", "Information": None,
                "Scale_Minimum": 60000, "Source_Date": "20261001",
                "Source_Indication": "USACE IENC", "Source_Dataset": "IENC",
                "Feature_ID": f"lateral-beacon-{oid}"})
            rows.append({"attributes": attrs,
                "geometry": {"x": -90.0 + oid / 10000, "y": 38.0 + oid / 10000,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class LateralBeaconsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_lateral_beacons_usace_ienc, hifld_ingest_assets)
        registered = defs.get_repository_def()
        self.assertIn(usace_lateral_beacons_usace_ienc.key, registered.asset_graph.get_all_asset_keys())
        self.assertIn("usace_lateral_beacons_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(usace_lateral_beacons_draft_job.name,
                      {job.name for job in registered.get_all_jobs()})
        self.assertIn(usace_lateral_beacons_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})
        self.assertEqual(usace_lateral_beacons_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check=usace_lateral_beacons_complete({"feature_count":source.EXPECTED_COUNT,"source_population_complete":True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)
        for name in ("baseline_comparison_accepted", "source_release_accepted", "source_freshness_accepted",
                     "schema_contract_accepted", "id_contract_accepted", "coverage_accepted", "rights_accepted",
                     "mapping_approved", "release_accepted"):
            self.assertFalse(check.metadata[name].value)

    def test_invalid_capture_budget_fails_before_any_request(self):
        with self.assertRaisesRegex(ValueError, "invalid_capture_budget"):
            source.capture_usace_lateral_beacons(None, version="v-test", capture_max_bytes=0)
        with self.assertRaisesRegex(ValueError, "capture_budget_smaller_than_response_limit"):
            source.capture_usace_lateral_beacons(None, version="v-test", capture_max_bytes=100,
                response_max_bytes=101, layer_response_max_bytes=50)

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name,url=source._capture_roster(source.EXPECTED_IDS)[0]
        receipt={"method":"GET","url":url,"final_url":url}
        source._validate_capture_request_identity(name,receipt,url)
        receipt["final_url"]="https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError,"retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name,receipt,url)

    def test_complete_population_native_geometry_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_population_complete"] if "source_population_complete" in audit else len(features), source.EXPECTED_COUNT)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "lateral-beacon-1")
        self.assertEqual(list(features[0]["attributes"]), [field["name"] for field in source.ATTRIBUTE_FIELDS])
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.column_names, [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertEqual(table["OBJECTID"][0].as_py(), 1)
        self.assertEqual(table["Category_of_Lateral_Mark"][0].as_py(), "port")
        self.assertEqual(table["Feature_ID"][0].as_py(), "lateral-beacon-1")
        self.assertTrue(table.schema.field("Feature_ID").type.equals(__import__("pyarrow").string()))
        self.assertEqual(table["Feature_ID"].null_count, 0)
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(__import__("pyarrow").int32()))

    def test_rejects_schema_and_population_drift(self):
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = layer["fields"][:-1]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)

    def test_rejects_geometry_or_declared_type_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = "-90"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)

    def test_point_queries_request_z_and_m_and_reject_returned_dimensions(self):
        pages = [(name, url) for name, url in source._capture_roster(source.EXPECTED_IDS)
                 if name.startswith("page_")]
        self.assertEqual(len(pages), 4)
        for _name, url in pages:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["returnZ"], ["true"])
            self.assertEqual(query["returnM"], ["true"])
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 17.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_zm_dimensions_not_supported"):
            source._validate(captures)


if __name__ == "__main__":
    unittest.main()
