"""Focused contract checks for the official IENC Airport Area adapter."""
import json
import unittest

import shapely

from dagster_hifld.sources.usace_airport_area import (
    EXPECTED_FIELDS,
    _esri_polygon,
    _validate,
)
from dagster_hifld.assets.usace_airport_area import usace_airport_area_release


class UsaceAirportAreaContractTests(unittest.TestCase):
    def test_retains_polygon_zm_in_native_wkb(self):
        geometry = {"rings": [[[0, 0, 10, 2], [0, 1, 11, 3], [1, 1, 12, 4],
                               [0, 0, 10, 2]]], "hasZ": True, "hasM": True}
        wkb = _esri_polygon(geometry)
        parsed = shapely.from_wkb(wkb)
        self.assertEqual(parsed.geom_type, "Polygon")
        self.assertTrue(shapely.has_z(parsed))
        self.assertTrue(shapely.has_m(parsed))

    def test_rejects_seed_or_before_after_id_population_drift(self):
        fields = [
            {"name": name, "type": kind, "alias": name, "domain": None,
             **({"length": 254} if kind == "esriFieldTypeString" else {})}
            for name, kind in EXPECTED_FIELDS
        ]
        ids = list(range(1, 73))
        captures = {}
        layer = {"name": "AIRPORT_AREA", "id": 57, "fields": fields,
                 "geometryType": "esriGeometryPolygon",
                 "spatialReference": {"latestWkid": 4326}, "maxRecordCount": 2000,
                 "hasZ": False, "hasM": False}
        def capture(name, value):
            captures[name] = (json.dumps(value).encode(), {})
        capture("layer_before", layer)
        capture("layer_after", layer)
        capture("count_before", {"count": 72})
        capture("count_after", {"count": 72})
        capture("ids_seed", {"objectIds": ids})
        capture("ids_before", {"objectIds": ids})
        capture("ids_after", {"objectIds": ids[:-1] + [73]})
        feature_rows = []
        attr_names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
        for oid in ids:
            attrs = {name: None for name in attr_names}
            attrs.update(OBJECTID=oid, Scale_Minimum=0)
            x = oid * 3
            feature_rows.append({"attributes": attrs,
                "geometry": {"rings": [[[x, 0], [x, 1], [x + 1, 1], [x, 0]]],
                             "spatialReference": {"latestWkid": 4326}}})
        capture("page_000", {"features": feature_rows,
                              "spatialReference": {"latestWkid": 4326}})
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            _validate(captures)

    def test_metadata_contract_pins_complete_official_class(self):
        self.assertEqual(EXPECTED_FIELDS[0], ("OBJECTID", "esriFieldTypeOID"))
        self.assertIn(("Feature_ID", "esriFieldTypeString"), EXPECTED_FIELDS)

    def test_invalid_native_polygon_is_reported_without_geometry_repair(self):
        fields = [
            {"name": name, "type": kind, "alias": name, "domain": None,
             **({"length": 254} if kind == "esriFieldTypeString" else {})}
            for name, kind in EXPECTED_FIELDS
        ]
        ids = list(range(1, 73))
        layer = {"name": "AIRPORT_AREA", "id": 57, "fields": fields,
                 "geometryType": "esriGeometryPolygon",
                 "spatialReference": {"latestWkid": 4326}, "maxRecordCount": 2000,
                 "hasZ": False, "hasM": False}
        features = []
        attr_names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
        for oid in ids:
            attrs = {name: None for name in attr_names}
            attrs.update(OBJECTID=oid, Scale_Minimum=0)
            x = oid * 3
            ring = ([[0, 0], [3, 0], [0, 4], [4, 4], [0, 0]] if oid == 1 else
                    [[x, 0], [x, 1], [x + 1, 1], [x, 0]])
            features.append({"attributes": attrs, "geometry": {"rings": [ring]}})

        def capture(value):
            return (json.dumps(value).encode(), {})

        captures = {
            "layer_before": capture(layer), "layer_after": capture(layer),
            "count_before": capture({"count": 72}), "count_after": capture({"count": 72}),
            "ids_seed": capture({"objectIds": ids}), "ids_before": capture({"objectIds": ids}),
            "ids_after": capture({"objectIds": ids}),
            "page_000": capture({"spatialReference": {"latestWkid": 4326}, "features": features}),
        }
        _parsed_fields, parsed_features, audit = _validate(captures)
        self.assertEqual(audit["source_invalid_geometry_count"], 1)
        self.assertEqual(audit["source_empty_geometry_count"], 0)
        self.assertEqual(audit["source_null_geometry_count"], 0)
        self.assertEqual(parsed_features[0]["geometry"], features[0]["geometry"])

    def test_release_status_is_pending_without_blocking_draft_research(self):
        spec = next(iter(usace_airport_area_release.check_specs))
        self.assertFalse(spec.blocking)

        pending = usace_airport_area_release.node_def.compute_fn.decorated_fn(
            {"release_accepted": False, "source_release_accepted": False}
        )
        self.assertFalse(pending.passed)
        self.assertEqual(pending.metadata["approval_status"].value, "pending")
        self.assertFalse(pending.metadata["candidate_claimed_release_accepted"].value)
        self.assertIn("publish/verify", pending.metadata["promotion_enforced_by"].value)

        accepted = usace_airport_area_release.node_def.compute_fn.decorated_fn(
            {"release_accepted": True}
        )
        self.assertTrue(accepted.passed)
        self.assertEqual(accepted.metadata["approval_status"].value, "accepted")
        self.assertTrue(accepted.metadata["candidate_claimed_release_accepted"].value)
        self.assertIn("independent exact-object", spec.description)


if __name__ == "__main__":
    unittest.main()
