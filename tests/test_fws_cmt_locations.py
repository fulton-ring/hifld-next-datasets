"""Native preservation, dual-coordinate fidelity and baseline drift checks for the FWS CMT layer.

All fixtures are synthetic: no real USFWS organization names, addresses or telephone
values are embedded here. The pinned baseline counts mirror the verified
2026-11-08 snapshot contract (1,816 rows; 4 null geometry/coordinate rows; 140
geometry-vs-attribute disagreements).
"""
import copy
import io
import json
import math
import unittest

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import fws_cmt_locations as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.fws_cmt_locations import (
    fws_cmt_locations_complete, fws_cmt_locations_draft_schedule, fws_cmt_locations,
)

R = source.SPHERE_RADIUS_M
IDS = [1667 + index for index in range(source.EXPECTED_COUNT)]
NULL_ROWS = {source.EXPECTED_COUNT - 4, source.EXPECTED_COUNT - 3,
             source.EXPECTED_COUNT - 2, source.EXPECTED_COUNT - 1}
DISAGREE_ROWS = set(range(140))
LEADING_ZERO_ROWS = set(range(76))


def _forward(latitude: float, longitude: float) -> tuple[float, float]:
    x = longitude * math.pi * R / 180.0
    y = R * math.log(math.tan(math.pi / 4.0 + latitude * math.pi / 360.0))
    return x, y


def _feature(oid: int) -> dict:
    index = oid - 1667
    latitude = -60.0 + (index % 120)
    longitude = -170.0 + (index % 330)
    org_code = f"0{index:04d}" if index in LEADING_ZERO_ROWS else f"1{index:04d}"
    if index in DISAGREE_ROWS:
        geometry_x, geometry_y = _forward(latitude + 5e-4, longitude - 5e-4)
    else:
        geometry_x, geometry_y = _forward(latitude, longitude)
    geometry = None
    if index not in NULL_ROWS:
        geometry = {"x": geometry_x, "y": geometry_y}
    else:
        latitude = longitude = None
    attributes = {"physStateAbbr": "ST", "orgName": f"Synthetic Unit {index}",
                  "physAdd1": f"{index} Synthetic Way", "orgTypeAcro": "SYN",
                  "physCity": "Syntheticville", "mailCity": "Syntheticville",
                  "program": f"Program {index % 7}", "mailStateAbbr": "ST",
                  "telephone": "000-000-0000", "region": 1 + index % 8,
                  "mailAdd1": f"{index} Synthetic Mail Way", "orgType": "Synthetic",
                  "physAdd2": None, "mailAdd2": None, "ObjectId": oid,
                  "physZip": "00000", "mailZip": "00000", "orgCode": org_code,
                  "longitude": longitude, "latitude": latitude,
                  "CCCode": f"SYN{index:08d}", "RptToCC": None, "RptOrgCode": None,
                  "OrgNameAbbr": f"SU{index}", "ProgAcro": "SYN",
                  "RptToOrgName": None, "LIT": None}
    return {"attributes": attributes, "geometry": geometry}


def _capture_docs_plain() -> dict:
    fields = [{**declaration, "actualType": "nvarchar", "sqlType": "sqlTypeNVarchar",
               "nullable": True, "editable": True, "defaultValue": None}
              for declaration in source.EXPECTED_FIELD_DECLARATIONS]
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME,
             "geometryType": "esriGeometryPoint", "maxRecordCount": 2000, "fields": fields,
             "spatialReference": {"wkid": 102100, "latestWkid": 3857},
             "objectIdFieldName": "ObjectId",
             "extent": {"xmin": -2.0e7, "ymin": -3.0e6, "xmax": 2.0e7, "ymax": 1.0e7,
                        "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
             "editingInfo": {"dataLastEditDate": 1793673009098}}
    service = {"serviceItemId": source.ITEM_ID,
               "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                           "defaultVisibility": True, "subLayerIds": None, "minScale": 1,
                           "maxScale": 0, "type": "Feature Layer",
                           "geometryType": "esriGeometryPoint"}],
               "tables": [], "spatialReference": {"wkid": 102100, "latestWkid": 3857}}
    features = [_feature(oid) for oid in IDS]
    docs = {
        "org": {"id": source.ORG_ID, "name": "U.S. Fish & Wildlife Service",
                "portalName": "ArcGIS Online", "urlKey": "fws"},
        "item": {"id": source.ITEM_ID, "owner": source.OWNER_ACCOUNT,
                 "type": "Feature Service", "access": "public", "title": source.ITEM_TITLE,
                 "url": source.SERVICE_URL,
                 "accessInformation": ("FWS GIS (US Fish & Wildlife Service - Information Resources "
                                        "and Technology Management - Branch of Geospatial Data Services)  "
                                        "E-Mail: FWSGIS@fws.gov")},
        "owner": {"username": source.OWNER_ACCOUNT, "fullName": "US Fish and Wildlife Service",
                  "description": "US Fish and Wildlife Service - Information Resources and Technology "
                                  "Management - Branch of Geospatial Data Services."},
        "service_before": copy.deepcopy(service), "service_after": copy.deepcopy(service),
        "layer_before": copy.deepcopy(layer), "layer_after": copy.deepcopy(layer),
        "count_before": {"count": source.EXPECTED_COUNT},
        "count_after": {"count": source.EXPECTED_COUNT},
        "ids_before": {"objectIds": list(IDS)}, "ids_after": {"objectIds": list(IDS)},
    }
    for index in range(source.PAGE_COUNT):
        start = index * source.PAGE_SIZE
        rows = features[start:start + source.PAGE_SIZE]
        page = {"features": rows, "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                "geometryType": "esriGeometryPoint", "objectIdFieldName": "ObjectId",
                "globalIdFieldName": "", "uniqueIdField": {"name": "ObjectId", "isGlobalId": False}}
        if index < source.PAGE_COUNT - 1:
            page["exceededTransferLimit"] = True
        docs[f"page_{index:03d}"] = page
    return docs


def _capture_docs() -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode(), {})
            for name, doc in _capture_docs_plain().items()}


def _mutated_docs(mutator) -> dict[str, tuple[bytes, dict]]:
    docs = _capture_docs_plain()
    mutator(docs)
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


def _edit(docs: dict, name: str) -> dict:
    return docs[name]


class FwsCmtLocationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(fws_cmt_locations, hifld_ingest_assets)
        self.assertEqual(fws_cmt_locations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        candidate = {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True,
                     "null_coordinate_count": source.EXPECTED_NULL_COORDINATE_COUNT,
                     "geometry_attribute_disagreement_count":
                         source.EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT}
        check = fws_cmt_locations_complete(candidate)
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["source_registry_accepted"].value)
        self.assertFalse(check.metadata["mapping_approved"].value)
        rejected = fws_cmt_locations_complete({**candidate, "feature_count": 1815})
        self.assertFalse(rejected.passed)

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        for name, url in source._capture_roster():
            receipt = {"method": "GET", "url": url, "final_url": url}
            source._validate_capture_request_identity(name, receipt, url)
            receipt["final_url"] = "https://example.invalid/wrong"
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
                source._validate_capture_request_identity(name, receipt, url)

    def test_complete_population_preserves_both_representations(self):
        features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_null_coordinate_count"], source.EXPECTED_NULL_COORDINATE_COUNT)
        self.assertEqual(audit["source_geometry_attribute_disagreement_count"],
                         source.EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT)
        self.assertEqual(audit["source_org_code_leading_zero_count"], len(LEADING_ZERO_ROWS))
        self.assertEqual(audit["source_org_code_duplicate_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT - 4)
        self.assertEqual(audit["geometry_empty_count"], 4)
        parquet = source._parquet_bytes(features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)
        org_codes = table["orgCode"].to_pylist()
        self.assertTrue(all(value == f"0{index:04d}" for index, value in
                            enumerate(org_codes) if index in LEADING_ZERO_ROWS))
        self.assertTrue(all(isinstance(value, str) for value in org_codes))
        self.assertEqual(sum(value is None for value in table["latitude"].to_pylist()), 4)
        self.assertEqual(sum(value is None for value in table["longitude"].to_pylist()), 4)
        self.assertEqual(sum(value is None for value in table["geometry"].to_pylist()), 4)
        self.assertTrue(table.schema.field("region").type.equals(__import__("pyarrow").int32()))
        self.assertTrue(table.schema.field("latitude").type.equals(__import__("pyarrow").float64()))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        from pyproj import CRS
        self.assertTrue(CRS.from_json(json.dumps(geo["columns"]["geometry"]["crs"]))
                        .equals(CRS.from_epsg(3857)))
        # Both source representations coexist in the candidate: recompute the
        # disagreement baseline from the published columns themselves.
        recomputed = 0
        for row_index, wkb in enumerate(table["geometry"].to_pylist()):
            attributes_latitude = table["latitude"][row_index].as_py()
            attributes_longitude = table["longitude"][row_index].as_py()
            if wkb is None:
                self.assertIsNone(attributes_latitude)
                continue
            point = from_wkb(wkb)
            self.assertEqual(point.geom_type, "Point")
            delta = max(abs(source._inverse_x(point.x) - attributes_longitude),
                        abs(source._inverse_y(point.y) - attributes_latitude))
            if delta > source.DISAGREEMENT_THRESHOLD_DEG:
                recomputed += 1
        self.assertEqual(recomputed, source.EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT)

    def test_rejects_identity_drift(self):
        def owner_changed(captures):
            _edit(captures, "item")["owner"] = "someone_else"
        def org_changed(captures):
            _edit(captures, "org")["name"] = "Not FWS"
        def account_changed(captures):
            _edit(captures, "owner")["fullName"] = "Not FWS"
        def service_changed(captures):
            _edit(captures, "service_before")["serviceItemId"] = "0" * 32
        def service_moved(captures):
            _edit(captures, "service_after")["layers"] = []
        for mutator, error in ((owner_changed, "source_item_identity_drift"),
                               (org_changed, "source_org_identity_drift"),
                               (account_changed, "source_publisher_account_identity_drift"),
                               (service_changed, "source_service_identity_drift"),
                               (service_moved, "source_service_or_layer_changed_during_capture")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_rejects_schema_and_population_drift(self):
        def drop_field(captures):
            _edit(captures, "layer_before")["fields"].pop()
        def rename_field(captures):
            _edit(captures, "layer_before")["fields"][17]["alias"] = "Renamed"
        def count_drift(captures):
            _edit(captures, "count_before")["count"] = source.EXPECTED_COUNT + 1
        def roster_drift(captures):
            _edit(captures, "ids_after")["objectIds"][0] = 999999
        def page_short(captures):
            _edit(captures, "page_001")["features"].pop()
        def page_transfer_limit(captures):
            _edit(captures, "page_001")["exceededTransferLimit"] = True
        for mutator, error in ((drop_field, "source_field_schema_drift"),
                               (rename_field, "source_field_schema_drift"),
                               (count_drift, "source_count_or_id_roster_drift"),
                               (roster_drift, "source_count_or_id_roster_drift"),
                               (page_short, "source_page_population_incomplete"),
                               (page_transfer_limit, "source_page_error_or_transfer_limit")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_rejects_baseline_count_drift(self):
        def extra_null_row(captures):
            page = _edit(captures, "page_000")
            row = page["features"][10]
            row["geometry"] = None
            row["attributes"]["latitude"] = row["attributes"]["longitude"] = None
        def repaired_disagreement(captures):
            page = _edit(captures, "page_000")
            row = page["features"][0]
            latitude, longitude = row["attributes"]["latitude"], row["attributes"]["longitude"]
            row["geometry"]["x"], row["geometry"]["y"] = _forward(latitude, longitude)
        def duplicate_org_code(captures):
            page = _edit(captures, "page_000")
            page["features"][5]["attributes"]["orgCode"] = page["features"][4]["attributes"]["orgCode"]
        def null_org_code(captures):
            _edit(captures, "page_000")["features"][5]["attributes"]["orgCode"] = None
        for mutator, error in ((extra_null_row, "source_null_coordinate_count_drift"),
                               (repaired_disagreement, "source_geometry_attribute_disagreement_count_drift"),
                               (duplicate_org_code, "source_org_code_duplicate_drift"),
                               (null_org_code, "source_org_code_null_or_empty_drift")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_rejects_coordinate_and_geometry_structure_drift(self):
        def unpaired_null_latitude(captures):
            _edit(captures, "page_000")["features"][7]["attributes"]["latitude"] = None
        def geometry_attr_null_mismatch(captures):
            _edit(captures, "page_000")["features"][7]["geometry"] = None
        def latitude_out_of_range(captures):
            _edit(captures, "page_000")["features"][7]["attributes"]["latitude"] = 91.0
        def z_geometry(captures):
            row = _edit(captures, "page_000")["features"][7]
            row["geometry"]["z"] = 0.0
        def world_bound_breach(captures):
            _edit(captures, "page_000")["features"][7]["geometry"]["x"] = -3.0e7
        def string_ordinate(captures):
            _edit(captures, "page_000")["features"][7]["geometry"]["x"] = "-13000000"
        for mutator, error in ((unpaired_null_latitude, "source_latitude_longitude_null_pairing_drift"),
                               (geometry_attr_null_mismatch, "source_geometry_attribute_null_pairing_drift"),
                               (latitude_out_of_range, "source_attribute_coordinate_range_drift"),
                               (z_geometry, "source_point_geometry_structure_drift"),
                               (world_bound_breach, "source_point_geometry_structure_drift"),
                               (string_ordinate, "source_point_geometry_structure_drift")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))


if __name__ == "__main__":
    unittest.main()
