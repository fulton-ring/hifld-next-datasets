import json
import hashlib
import unittest

from dagster_hifld.sources.usace_obstruction_areas import (
    EXPECTED_FIELD_DECLARATIONS, EXPECTED_FIELDS, _capture_identity_hash, _capture_roster, _validate,
)


class UsaceObstructionAreasTests(unittest.TestCase):
    def test_candidate_capture_identity_binds_body_and_wire_but_not_time(self):
        body = b'{"features":[]}'
        wire = b'{ "features" : [] }'
        receipt = {"complete_body": True, "status": 200, "bytes": len(body),
                   "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(wire),
                   "wire_sha256": hashlib.sha256(wire).hexdigest(), "observed_at": "first"}
        captures = {"page_000": (body, receipt)}
        original = _capture_identity_hash(captures)

        later_receipt = {**receipt, "observed_at": "later"}
        self.assertEqual(original, _capture_identity_hash({"page_000": (body, later_receipt)}))
        changed_body = b'{"features":[1]}'
        changed_body_receipt = {**receipt, "bytes": len(changed_body),
                                "sha256": hashlib.sha256(changed_body).hexdigest()}
        self.assertNotEqual(original, _capture_identity_hash({"page_000": (changed_body, changed_body_receipt)}))
        changed_wire_receipt = {**receipt, "wire_sha256": hashlib.sha256(b"different wire").hexdigest()}
        self.assertNotEqual(original, _capture_identity_hash({"page_000": (body, changed_wire_receipt)}))

    def _captures(self):
        ids = [12]
        fields = EXPECTED_FIELD_DECLARATIONS
        extent = {"xmin": -95, "ymin": 29, "xmax": -79, "ymax": 46,
                  "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        spatial_reference = {"wkid": 4326, "latestWkid": 4326}
        service = {"layers": [{"id": 81, "name": "OBSTRUCTION_AREA"}]}
        layer = {"name": "OBSTRUCTION_AREA", "id": 81, "geometryType": "esriGeometryPolygon",
                 "spatialReference": spatial_reference, "maxRecordCount": 2000,
                 "extent": extent, "fields": fields}
        description = ("The Inland Electronic Navigational Charts (IENC) dataset is updated bi-monthly "
                       "by the US Army Corps (USACE)/Army Geospatial Center (AGC). "
                       "Web mapping services: https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes. "
                       "Data geodatabase: USACE_IENC_Master_Service_gdb.zip")
        item = {"id": "3607d485a9854d6fa458bf507058ceae", "owner": "USDOT_BTS",
                "title": "Inland Electronic Navigational Charts", "description": description}
        attributes = {name: None for name, _kind in EXPECTED_FIELDS if name != "SHAPE"}
        attributes.update({"OBJECTID": 12, "Category_of_Obstruction": "Obstruction",
                          "Feature_ID": "000012", "Scale_Minimum": 50000})
        feature = {"attributes": attributes, "geometry": {
            "rings": [[[-90, 35, 2], [-90, 36, 2], [-89, 36, 2], [-89, 35, 2], [-90, 35, 2]]],
            "hasZ": True, "hasM": False, "spatialReference": spatial_reference}}
        docs = {
            "official_item": item,
            "service_before": service,
            "service_after": service,
            "layer_before": layer,
            "layer_after": layer,
            "count_before": {"count": 1},
            "count_after": {"count": 1},
            "ids_before": {"objectIds": ids},
            "ids_after": {"objectIds": ids},
            "page_000": {"spatialReference": spatial_reference, "features": [feature]},
        }
        return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}

    def test_validates_complete_native_population_and_values(self):
        fields, features, audit = _validate(self._captures())
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "000012")
        self.assertEqual(features[0]["attributes"]["Scale_Minimum"], 50000)
        self.assertEqual(len(features[0]["geometry"]["rings"][0][0]), 3)
        self.assertEqual(audit["source_feature_count"], 1)
        self.assertEqual(audit["source_id_min"], 12)
        self.assertEqual(audit["geometry_valid_count"], 1)

    def test_rejects_missing_id_schema_drift_and_truncation(self):
        captures = self._captures()
        ids_after = json.loads(captures["ids_after"][0])
        ids_after["objectIds"] = []
        captures["ids_after"] = (json.dumps(ids_after).encode(), {})
        with self.assertRaisesRegex(ValueError, "count_or_id_population_drift"):
            _validate(captures)

        captures = self._captures()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "NewField", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "field_schema_drift"):
            _validate(captures)

        captures = self._captures()
        page = json.loads(captures["page_000"][0])
        page["exceededTransferLimit"] = True
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "page_error_or_truncation"):
            _validate(captures)

    def test_capture_roster_uses_native_layer_and_bounded_pages(self):
        roster = _capture_roster(list(range(1, 252)))
        self.assertIn(("layer_before", "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/OBSTRUCTION_AREA/MapServer/81?f=pjson"), roster)
        self.assertEqual([name for name, _url in roster if name.startswith("page_")], ["page_000", "page_001"])


if __name__ == "__main__":
    unittest.main()
