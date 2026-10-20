import json
import unittest

from dagster_hifld.sources.usace_nautical_publication_information_areas import (
    DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELD_DECLARATIONS, EXPECTED_FIELDS,
    LAYER_URL, _validate,
)


class UsaceNauticalPublicationInformationAreasTests(unittest.TestCase):
    def test_official_layer_profile_and_complete_fixture(self):
        self.assertEqual(DATASET_SLUG, "nautical-publication-information-areas-usace-ienc")
        self.assertTrue(LAYER_URL.endswith("/NAUTICAL_PUBLICATION_INFORMATION_AREA/MapServer/67"))
        self.assertEqual(EXPECTED_COUNT, 109)
        ids = list(range(1, EXPECTED_COUNT + 1))
        fields = EXPECTED_FIELD_DECLARATIONS
        layer = {
            "name": "NAUTICAL_PUBLICATION_INFORMATION_AREA", "id": 67,
            "geometryType": "esriGeometryPolygon",
            "spatialReference": {"latestWkid": 4326}, "maxRecordCount": 2000,
            "fields": fields,
        }
        docs = {
            "layer_before": layer,
            "layer_after": {"fields": fields, "geometryType": "esriGeometryPolygon"},
            "count_before": {"count": EXPECTED_COUNT},
            "count_after": {"count": EXPECTED_COUNT},
            "ids_before": {"objectIds": ids},
            "ids_after": {"objectIds": ids},
        }
        attributes = {name: None for name, _ in EXPECTED_FIELDS if name != "SHAPE"}
        features = []
        for oid in ids:
            row = dict(attributes)
            row.update({"OBJECTID": oid, "Information": "Sailing directions", "Feature_ID": f"NP-{oid:03d}"})
            features.append({"attributes": row, "geometry": {
                "rings": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            }})
        docs["page_000"] = {"spatialReference": {"latestWkid": 4326}, "features": features}
        captures = {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}

        parsed_fields, parsed_features, audit = _validate(captures)

        self.assertEqual(parsed_fields, fields)
        self.assertEqual(len(parsed_features), EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], EXPECTED_COUNT)
        self.assertIn("no national coverage claim", audit["source_coverage_scope"])


if __name__ == "__main__":
    unittest.main()
