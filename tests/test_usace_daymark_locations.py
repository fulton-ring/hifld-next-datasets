"""Native preservation and completeness checks for the official IENC DAYMARK_POINT layer."""
import json
import unittest
from urllib.parse import parse_qs, urlsplit

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_daymark_locations as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.assets.usace_daymark_locations import (
    usace_daymark_locations_complete, usace_daymark_locations_draft_job,
    usace_daymark_locations_draft_schedule, usace_daymark_locations_usace_ienc,
)


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "DAYMARK_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "DAYMARK_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 9, "name": "DAYMARK_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts", "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/DAYMARK_POINT", "type": "MapServer"}]},
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
            attrs.update({"OBJECTID": oid, "Color": "red", "Top_Shape": "triangle",
                "Color_Pattern": "solid", "Condition": "good", "Information": None,
                "Scale_Minimum": 60000, "Source_Date": "20260515",
                "Source_Indication": "US,U3,SURVY", "Source_Dataset": "dataset",
                "Feature_ID": f"daymark-{oid}"})
            rows.append({"attributes": attrs,
                "geometry": {"x": -90.0 + oid / 10000, "y": 38.0 + oid / 10000,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class DaymarkLocationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_daymark_locations_usace_ienc, hifld_ingest_assets)
        registered = defs.get_repository_def()
        self.assertIn(usace_daymark_locations_usace_ienc.key, registered.asset_graph.get_all_asset_keys())
        self.assertIn("usace_daymark_locations_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(usace_daymark_locations_draft_job.name,
                      {job.name for job in registered.get_all_jobs()})
        self.assertIn(usace_daymark_locations_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})
        self.assertEqual(usace_daymark_locations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check=usace_daymark_locations_complete({"feature_count":source.EXPECTED_COUNT,"source_population_complete":True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)

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
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "daymark-1")
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
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
        self.assertEqual(len(pages), 5)
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
