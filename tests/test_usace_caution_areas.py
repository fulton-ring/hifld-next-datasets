"""Native capture and release-gate checks for USACE IENC caution-area points."""
import json
import io
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_caution_areas as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_caution_areas import (
    usace_caution_areas_complete, usace_caution_areas_draft_schedule,
    usace_caution_areas_usace_ienc,
)


def _captures():
    ids = source.EXPECTED_IDS
    layer = {"id": 7, "name": "CAUTION_AREA_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}}
    service = {"mapName": "CAUTION_AREA_POINT", "copyrightText": "USACE IENC",
        "layers": [{"id": 7, "name": "CAUTION_AREA_POINT", "geometryType": "esriGeometryPoint"}]}
    docs = {"bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
            "title": "Inland Electronic Navigational Charts",
            "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/CAUTION_AREA_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
        "count_before": {"count": len(ids)}, "count_after": {"count": len(ids)},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids}}
    for idx in range((len(ids) + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        rows = []
        for oid in ids[idx * source.PAGE_SIZE:(idx + 1) * source.PAGE_SIZE]:
            rows.append({"attributes": {"OBJECTID": oid, "Object_Name": f"Caution {oid}",
                "Periodic_Date_Start": None, "Periodic_Date_End": None, "Information": "Observed",
                "Scale_Minimum": 60000, "Source_Date": "20261020", "Source_Indication": "USACE",
                "Source_Dataset": "IENC", "Feature_ID": f"U3 {oid:08d}"},
                "geometry": {"x": -90.0 + oid / 100, "y": 38.0 + oid / 100,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {k: (json.dumps(v).encode(), {}) for k, v in docs.items()}


class CautionAreaTests(unittest.TestCase):
    def test_registered_draft_refresh_is_stopped_and_release_remains_unapproved(self):
        self.assertIn(usace_caution_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_caution_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_caution_areas_complete({"feature_count": 36, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)

    def test_exact_get_request_guard(self):
        name, url = source._capture_roster(source.EXPECTED_IDS)[0]
        receipt = {"method": "GET", "url": url, "final_url": url}
        source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = "https://example.invalid/altered"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)

    def test_complete_native_population_and_typed_geo_parquet_round_trip(self):
        fields, features, audit = source._validate(_captures())
        self.assertEqual(len(features), 36)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_wkid"], 4326)
        data = pq.read_table(io.BytesIO(source._parquet_bytes(fields, features)))
        self.assertTrue(data.schema.field("Scale_Minimum").type.equals(pa.int32()))
        self.assertTrue(data.schema.field("Feature_ID").type.equals(pa.string()))
        self.assertEqual(data["Feature_ID"].null_count, 0)
        self.assertEqual(data["Feature_ID"][0].as_py(), "U3 00000001")
        point = from_wkb(data["geometry"][0].as_py())
        self.assertEqual(point.geom_type, "Point")
        self.assertEqual((point.x, point.y), (features[0]["geometry"]["x"], features[0]["geometry"]["y"]))

    def test_schema_population_and_geometry_drift_fail_closed(self):
        captures = _captures()
        layer = json.loads(captures["layer_before"][0]); layer["fields"] = layer["fields"][:-1]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _captures(); page = json.loads(captures["page_000"][0]); page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)
        captures = _captures(); page = json.loads(captures["page_000"][0]); page["features"][0]["geometry"]["x"] = "-90"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)


if __name__ == "__main__":
    unittest.main()
