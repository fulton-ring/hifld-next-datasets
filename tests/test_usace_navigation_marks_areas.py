import json
import unittest

from dagster_hifld.sources.usace_navigation_marks_areas import (
    BTS_ITEM_ID, EXPECTED_FIELD_DECLARATIONS, EXPECTED_FIELDS, LAYER_ID, _validate,
)


class UsaceNavigationMarksAreasTests(unittest.TestCase):
    def _capture(self):
        fields = EXPECTED_FIELD_DECLARATIONS
        ids = list(range(1001, 1110))
        layer = {"name": "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA", "id": LAYER_ID,
                 "geometryType": "esriGeometryPolygon", "spatialReference": {"latestWkid": 4326},
                 "maxRecordCount": 2000, "fields": fields}
        service = {"mapName": "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA", "copyrightText": "USACE IENC",
                   "layers": [{"id": LAYER_ID, "name": "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA",
                               "geometryType": "esriGeometryPolygon"}]}
        captures = {
            "bts_item": (json.dumps({"id": BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                                      "access": "public", "title": "Inland Electronic Navigational Charts",
                                      "description": "USACE_IENC_Master_Service_gdb.zip https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"}).encode(), {}),
            "service_directory": (json.dumps({"services": [{"name": "IENC_Feature_Classes/NAVIGATIONAL_SYSTEM_OF_MARKS_AREA",
                                                               "type": "MapServer"}]}).encode(), {}),
            "service_before": (json.dumps(service).encode(), {}),
            "service_after": (json.dumps(service).encode(), {}),
            "layer_before": (json.dumps(layer).encode(), {}),
            "layer_after": (json.dumps(layer).encode(), {}),
            "ids_seed": (json.dumps({"objectIds": ids}).encode(), {}),
            "count_before": (b'{"count":109}', {}), "count_after": (b'{"count":109}', {}),
            "ids_before": (json.dumps({"objectIds": ids}).encode(), {}),
            "ids_after": (json.dumps({"objectIds": ids}).encode(), {}),
        }
        features = []
        for oid in ids:
            attributes = {name: None for name, _kind in EXPECTED_FIELDS if name != "SHAPE"}
            attributes.update(OBJECTID=oid, System_of_Navigational_Marks="IALA-A",
                              Feature_ID=f"000{oid}")
            features.append({"attributes": attributes, "geometry": {
                "rings": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                "spatialReference": {"wkid": 4326}}})
        captures["page_000"] = (json.dumps({"spatialReference": {"latestWkid": 4326},
                                             "features": features}).encode(), {})
        return captures

    def test_validates_complete_population_and_keeps_leading_zero_ids(self):
        fields, features, audit = _validate(self._capture())
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 109)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "0001001")
        self.assertEqual(audit["source_feature_count"], 109)
        self.assertEqual(audit["source_id_min"], 1001)

    def test_rejects_missing_ids_and_schema_drift(self):
        captures = self._capture()
        ids = json.loads(captures["ids_after"][0])
        ids["objectIds"].pop()
        captures["ids_after"] = (json.dumps(ids).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            _validate(captures)
        captures = self._capture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "unexpected", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            _validate(captures)

    def test_rejects_unverified_bts_reference_and_attribute_type_loss(self):
        captures = self._capture()
        item = json.loads(captures["bts_item"][0])
        item["owner"] = "third_party"
        captures["bts_item"] = (json.dumps(item).encode(), {})
        with self.assertRaisesRegex(ValueError, "official_bts_distribution_reference_drift"):
            _validate(captures)
        captures = self._capture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Feature_ID"] = 123
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_string_value_type_drift:Feature_ID"):
            _validate(captures)


if __name__ == "__main__":
    unittest.main()
