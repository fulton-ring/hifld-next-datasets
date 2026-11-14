"""Focused contract checks for the official IENC Airport Area adapter."""
import json
import unittest

import shapely

from dagster_hifld.sources.usace_airport_area import (
    BTS_ITEM_ID,
    EXPECTED_FIELDS,
    UNSUPPORTED_OUTPUT_FORMATS,
    _capture_started_at,
    _esri_polygon,
    _validate,
)
from dagster_hifld.assets.usace_airport_area import usace_airport_area_release


BTS_ITEM = {
    "id": BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
    "title": "Inland Electronic Navigational Charts",
    "description": "The Inland Electronic Navigational Charts (IENC) dataset is updated bi-monthly "
    "by the US Army Corps (USACE)/Army Geospatial Center (AGC). The vector data can be downloaded "
    "as a geodatabase here: https://ienccloud.us/ienc/products/files/U37/ienc_master_dataset_gdb/"
    "USACE_IENC_Master_Service_gdb.zip. In addition, web mapping services of the feature "
    "classes/datasets can be found here: "
    "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes.",
}
SERVICE_DIRECTORY = {"services": [{"name": "IENC_Feature_Classes/AIRPORT_AREA_POINT",
                                    "type": "MapServer"},
                                   {"name": "IENC_Feature_Classes/AIRPORT_AREA",
                                    "type": "MapServer"}]}
SERVICE = {"mapName": "AIRPORT_AREA", "copyrightText": "USACE IENC",
           "layers": [{"id": 57, "name": "AIRPORT_AREA",
                       "geometryType": "esriGeometryPolygon"}]}


def _layer_fields():
    return [
        {"name": name, "type": kind, "alias": name, "domain": None,
         **({"length": 254} if kind == "esriFieldTypeString" else {})}
        for name, kind in EXPECTED_FIELDS
    ]


def _feature_rows(ids, *, invalid_first_ring=False):
    attr_names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
    rows = []
    for oid in ids:
        attrs = {name: None for name in attr_names}
        attrs.update(OBJECTID=oid, Scale_Minimum=0)
        x = oid * 3
        ring = ([[0, 0], [3, 0], [0, 4], [4, 4], [0, 0]] if invalid_first_ring and oid == 1
                else [[x, 0], [x, 1], [x + 1, 1], [x, 0]])
        rows.append({"attributes": attrs,
                     "geometry": {"rings": [ring],
                                  "spatialReference": {"latestWkid": 4326}}})
    return rows


def _base_captures(ids, *, invalid_first_ring=False):
    layer = {"name": "AIRPORT_AREA", "id": 57, "fields": _layer_fields(),
             "geometryType": "esriGeometryPolygon",
             "spatialReference": {"latestWkid": 4326}, "maxRecordCount": 2000,
             "hasZ": False, "hasM": False}
    captures = {}

    def capture(name, value):
        captures[name] = (json.dumps(value).encode(), {})

    capture("bts_item", BTS_ITEM)
    capture("service_directory", SERVICE_DIRECTORY)
    capture("service_before", SERVICE)
    capture("service_after", SERVICE)
    capture("layer_before", layer)
    capture("layer_after", layer)
    capture("count_before", {"count": 72})
    capture("count_after", {"count": 72})
    capture("ids_seed", {"objectIds": ids})
    capture("ids_before", {"objectIds": ids})
    capture("ids_after", {"objectIds": ids})
    capture("page_000", {"spatialReference": {"latestWkid": 4326},
                         "features": _feature_rows(ids, invalid_first_ring=invalid_first_ring)})
    return captures


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
        ids = list(range(1, 73))
        captures = _base_captures(ids)
        captures["ids_after"] = (json.dumps({"objectIds": ids[:-1] + [73]}).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            _validate(captures)

    def test_metadata_contract_pins_complete_official_class(self):
        self.assertEqual(EXPECTED_FIELDS[0], ("OBJECTID", "esriFieldTypeOID"))
        self.assertIn(("Feature_ID", "esriFieldTypeString"), EXPECTED_FIELDS)

    def test_replay_capture_time_comes_from_original_seed_receipt(self):
        captures = {"ids_seed": (b"{}", {"observed_at": "2026-10-21T16:33:11.260836+00:00"})}
        self.assertEqual(_capture_started_at(captures), "2026-10-21T16:33:11.260836+00:00")

    def test_lossy_or_unapproved_exports_are_explicitly_excluded(self):
        self.assertIn("shapefile_zip", UNSUPPORTED_OUTPUT_FORMATS)
        self.assertIn("field-name", UNSUPPORTED_OUTPUT_FORMATS["shapefile_zip"])

    def test_invalid_native_polygon_is_reported_without_geometry_repair(self):
        ids = list(range(1, 73))
        captures = _base_captures(ids, invalid_first_ring=True)
        _parsed_fields, parsed_features, audit = _validate(captures)
        self.assertEqual(audit["source_invalid_geometry_count"], 1)
        self.assertEqual(audit["source_empty_geometry_count"], 0)
        self.assertEqual(audit["source_null_geometry_count"], 0)
        self.assertEqual(parsed_features[0]["geometry"],
                         _feature_rows(ids, invalid_first_ring=True)[0]["geometry"])

    def test_current_bts_item_identity_and_distribution_link_drift_rejected(self):
        ids = list(range(1, 73))
        wrong_owner = _base_captures(ids)
        wrong_owner["bts_item"] = (json.dumps({**BTS_ITEM, "owner": "someone_else"}).encode(), {})
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            _validate(wrong_owner)

        lost_link = _base_captures(ids)
        lost_link["bts_item"] = (json.dumps({**BTS_ITEM,
            "description": BTS_ITEM["description"].replace(
                "USACE_IENC_Master_Service_gdb.zip", "master_geodatabase.zip")}).encode(), {})
        with self.assertRaisesRegex(ValueError, "bts_ienc_official_distribution_link_drift"):
            _validate(lost_link)

        captured = _base_captures(ids)
        _fields, _features, audit = _validate(captured)
        self.assertEqual(audit["bts_catalog_item_id"], BTS_ITEM_ID)
        self.assertEqual(audit["prior_bts_catalog_item_id"],
                         "3607d485a9854d6fa458bf507058ceae")
        self.assertTrue(audit["bts_catalog_item_successor_review_pending"])

    def test_official_service_membership_and_identity_drift_rejected(self):
        ids = list(range(1, 73))
        missing_service = _base_captures(ids)
        missing_service["service_directory"] = (
            json.dumps({"services": [{"name": "IENC_Feature_Classes/AIRPORT_AREA_POINT",
                                      "type": "MapServer"}]}).encode(), {})
        with self.assertRaisesRegex(ValueError, "official_ienc_service_directory_drift"):
            _validate(missing_service)

        renamed = _base_captures(ids)
        renamed["service_before"] = (json.dumps({**SERVICE, "mapName": "OTHER_AREA"}).encode(), {})
        with self.assertRaisesRegex(ValueError, "official_airport_area_service_drift"):
            _validate(renamed)

        changed = _base_captures(ids)
        changed["service_after"] = (json.dumps({**SERVICE, "copyrightText": "Changed"}).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_service_changed_during_capture"):
            _validate(changed)

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
