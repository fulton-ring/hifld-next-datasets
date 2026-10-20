"""Focused fidelity and quarantine checks for the USGS draft capture."""
import hashlib
import json
import unittest

from dagster_hifld.assets.plate_boundaries import plate_boundaries_approval_gate
from dagster_hifld.sources import plate_boundaries as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


class PlateBoundariesSourceTests(unittest.TestCase):
    def setUp(self):
        self.original_count = source.EXPECTED_COUNT
        self.original_id_hash = source.EXPECTED_ID_SHA256
        self.original_null_counts = source.EXPECTED_NULL_COUNTS
        self.ids = [1, 3]
        source.EXPECTED_COUNT = 2
        source.EXPECTED_ID_SHA256 = hashlib.sha256(canonical(self.ids)).hexdigest()
        source.EXPECTED_NULL_COUNTS = {"NAME": 0, "LABEL": 0, "Shape_Length": 0}
        self.layer = {
            "geometryType": "esriGeometryPolyline",
            "spatialReference": {"latestWkid": 3857},
            "extent": dict(source.EXPECTED_EXTENT),
            "fields": [
                {"name": "OBJECTID", "type": "esriFieldTypeOID"},
                {"name": "Shape", "type": "esriFieldTypeGeometry"},
                {"name": "NAME", "type": "esriFieldTypeString", "length": 200},
                {"name": "LABEL", "type": "esriFieldTypeString", "length": 50},
                {"name": "Shape_Length", "type": "esriFieldTypeDouble"},
            ],
        }
        self.features = [{"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]]]},
                          "properties": {"OBJECTID": oid, "NAME": "plate", "LABEL": "Other", "Shape_Length": 1.0}}
                         for oid in self.ids]

    def tearDown(self):
        source.EXPECTED_COUNT = self.original_count
        source.EXPECTED_ID_SHA256 = self.original_id_hash
        source.EXPECTED_NULL_COUNTS = self.original_null_counts

    def run_capture(self, features=None, layer=None, ids_after=None, layer_after=None):
        return source.validate_capture(
            layer or self.layer,
            {"count": 2}, {"objectIds": self.ids},
            [{"features": self.features if features is None else features}],
            {"count": 2}, {"objectIds": self.ids if ids_after is None else ids_after},
            self.layer if layer_after is None else layer_after,
        )

    def test_complete_candidate_preserves_native_ids_and_schema(self):
        features, audit = self.run_capture()
        self.assertEqual([f["properties"]["OBJECTID"] for f in features], self.ids)
        self.assertEqual(audit["feature_count"], 2)
        self.assertTrue(audit["source_population_complete"])

    def test_missing_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            self.run_capture(features=self.features[:1])

    def test_field_drift_fails_closed(self):
        changed = {**self.layer, "fields": self.layer["fields"][:-1]}
        with self.assertRaisesRegex(ValueError, "source_schema_or_geometry_drift"):
            self.run_capture(layer=changed)

    def test_mid_capture_id_roster_drift_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "source_changed_during_capture"):
            self.run_capture(ids_after=[1, 2])

    def test_coverage_extent_drift_fails_closed(self):
        changed = {**self.layer, "extent": {**self.layer["extent"], "xmax": 1}}
        with self.assertRaisesRegex(ValueError, "source_coverage_extent_drift"):
            self.run_capture(layer=changed)

    def test_layer_change_during_capture_fails_closed(self):
        changed = {**self.layer, "name": "Changed"}
        with self.assertRaisesRegex(ValueError, "source_layer_changed_during_capture"):
            self.run_capture(layer_after=changed)

    def test_unapproved_candidate_never_passes_release_gate(self):
        result = plate_boundaries_approval_gate({
            "source_vintage_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "baseline_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
        })
        self.assertFalse(result.passed)


if __name__ == "__main__":
    unittest.main()
