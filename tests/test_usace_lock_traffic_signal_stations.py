"""Native preservation and completeness checks for the IENC lock traffic signal subset."""
import json
import unittest

import pyarrow
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_lock_traffic_signal_stations import (
    usace_lock_traffic_signal_stations_complete,
    usace_lock_traffic_signal_stations_draft_schedule,
    usace_lock_traffic_signal_stations_usace_ienc,
)
from dagster_hifld.sources import usace_lock_traffic_signal_stations as source


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": source.LAYER_NAME, "copyrightText": "USACE IENC",
               "layers": [{"id": source.LAYER_ID, "name": source.LAYER_NAME,
                           "geometryType": "esriGeometryPoint"}]}
    ids = list(source.EXPECTED_IDS)
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts",
                     "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/" + source.LAYER_NAME,
                                            "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "layer_count_before": {"count": source.EXPECTED_LAYER_TOTAL_COUNT},
        "layer_count_after": {"count": source.EXPECTED_LAYER_TOTAL_COUNT},
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            rows.append({"attributes": {"OBJECTID": oid,
                "Category_of_Traffic_Signal_Station": "Lock", "Direction_of_Impact": "Upstream",
                "Object_Name": "Lock and Dam", "Information": "", "Scale_Minimum": 60000,
                "Source_Date": "20140805", "Source_Indication": "US,U3,SURVY",
                "Source_Dataset": "U37AG001.000", "Feature_ID": "U3 0000009031 00011"},
                "geometry": {"x": -90.0 + oid / 10000, "y": 38.0 + oid / 10000,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{page_idx:03d}"] = {"features": rows,
                                        "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class LockTrafficSignalStationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_lock_traffic_signal_stations_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_lock_traffic_signal_stations_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = usace_lock_traffic_signal_stations_complete(
            {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)

    def test_subset_roster_pins_lock_category_excluding_bridge_passage(self):
        self.assertEqual(source.EXPECTED_COUNT, 289)
        self.assertEqual(len(source.BRIDGE_PASSAGE_OBJECT_IDS), 9)
        self.assertNotIn(170, source.EXPECTED_IDS)
        self.assertNotIn(205, source.EXPECTED_IDS)
        self.assertIn(169, source.EXPECTED_IDS)
        self.assertIn(207, source.EXPECTED_IDS)
        self.assertEqual("Category_of_Traffic_Signal_Station='Lock'", source._lock_filter())

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name, url = source._capture_roster(source.EXPECTED_IDS)[0]
        receipt = {"method": "GET", "url": url, "final_url": url}
        source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = "https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)

    def test_complete_population_native_geometry_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_layer_total_count"], source.EXPECTED_LAYER_TOTAL_COUNT)
        self.assertEqual(sorted(audit["source_excluded_object_ids"]),
                         sorted(source.BRIDGE_PASSAGE_OBJECT_IDS))
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertTrue(table.schema.field("Feature_ID").type.equals(pyarrow.string()))
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(pyarrow.int32()))
        self.assertTrue(table.schema.field("OBJECTID").type.equals(pyarrow.int64()))
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)

    def test_rejects_bridge_passage_rows_and_non_lock_category_drift(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Category_of_Traffic_Signal_Station"] = "Bridge Passage"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_lock_category_filter_drift"):
            source._validate(captures)

    def test_rejects_schema_population_and_count_drift(self):
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
        captures = _capture_docs()
        count = json.loads(captures["count_after"][0])
        count["count"] = source.EXPECTED_COUNT + 1
        captures["count_after"] = (json.dumps(count).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
            source._validate(captures)
        captures = _capture_docs()
        layer_count = json.loads(captures["layer_count_after"][0])
        layer_count["count"] = source.EXPECTED_LAYER_TOTAL_COUNT + 5
        captures["layer_count_after"] = (json.dumps(layer_count).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
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
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift"):
            source._validate(captures)


if __name__ == "__main__":
    unittest.main()
