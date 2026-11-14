"""Focused contract checks for the FAA AIS Runways source adapter."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import runways as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_URL = "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Runways/FeatureServer"

PORTAL_ITEM_BODY = json.dumps({
    "id": "4d8fa46181aa470d809776c57a8ab1f6", "owner": "AeronauticalInformationServices_FAA",
    "orgId": "ssFJjBXIUyZDrSYZ", "title": "Runways",
    "type": "Feature Service", "access": "public", "url": SERVICE_URL,
    "accessInformation": ("Federal Aviation Administration, Air Traffic Organization, "
                          "Mission Support Services, Aeronautical Information Services."),
    "licenseInfo": "The data provided is for public use. A limited number of Non-US Landing "
                   "Facilities are included in this file.",
    "description": ("<P><SPAN>Current Effective Date: 0901Z 29 Oct 2026 to 0901Z 24 Dec 2026"
                    "</SPAN></P><P><SPAN>A runway is a defined rectangular area on a land "
                    "airport prepared for the landing and takeoff of aircraft along its "
                    "length. This geospatial vector file provides information on the airport "
                    "runway and helicopter landing areas. This runway dataset is depicted on "
                    "Enroute charts.</SPAN></P><P><SPAN>Runway information is published every "
                    "eight weeks by the U.S. Department of Transportation, Federal Aviation "
                    "Administration-Aeronautical Information Services.</SPAN></P>"),
}, separators=(",", ":")).encode()

SERVICE_BODY = json.dumps({
    "serviceItemId": "4d8fa46181aa470d809776c57a8ab1f6",
    "layers": [{"id": 0, "name": "Runways", "type": "Feature Layer"}],
    "editingInfo": None,
}, separators=(",", ":")).encode()

EXTENT = {"xmin": -177.39266815799996, "ymin": -14.340687855999931,
          "xmax": 174.13517812600003, "ymax": 76.53588492300008,
          "spatialReference": {"wkid": 4269, "latestWkid": 4269}}


def _fields():
    lengths = {"GLOBAL_ID": 50, "AIRPORT_ID": 50, "DESIGNATOR": 254, "DIM_UOM": 254,
               "COMP_CODE": 254, "LIGHTINTNS": 254}
    small_ints = {"LIGHTACTV", "AK_LOW", "AK_HIGH", "US_LOW", "US_HIGH", "US_AREA", "PACIFIC"}
    doubles = {"LENGTH", "WIDTH", "Shape__Area", "Shape__Length"}
    names = ["OBJECTID", "GLOBAL_ID", "AIRPORT_ID", "DESIGNATOR", "LENGTH", "WIDTH",
             "DIM_UOM", "COMP_CODE", "LIGHTACTV", "LIGHTINTNS", "AK_LOW", "AK_HIGH",
             "US_LOW", "US_HIGH", "US_AREA", "PACIFIC", "Shape__Area", "Shape__Length"]
    fields = []
    for name in names:
        if name == "OBJECTID":
            fields.append({"name": name, "type": "esriFieldTypeOID", "alias": name,
                           "nullable": False, "domain": None})
        elif name in small_ints:
            fields.append({"name": name, "type": "esriFieldTypeSmallInteger", "alias": name,
                           "nullable": True, "domain": None})
        elif name in doubles:
            fields.append({"name": name, "type": "esriFieldTypeDouble", "alias": name,
                           "nullable": True, "domain": None})
        else:
            fields.append({"name": name, "type": "esriFieldTypeString", "alias": name,
                           "length": lengths[name], "nullable": True, "domain": None})
    return fields


def _attributes(oid, designator="05/23", airport_id=None, global_id=None):
    values = {
        "OBJECTID": oid,
        "GLOBAL_ID": global_id or f"10A0B0C0-0000-4000-8000-0000000000{oid:02X}",
        "AIRPORT_ID": airport_id or "D6E7B0C0-0000-4000-8000-0000000000AB",
        "DESIGNATOR": designator,
        "LENGTH": 7790.0, "WIDTH": 200.0, "DIM_UOM": "FT", "COMP_CODE": "ASPH",
        "LIGHTACTV": 2, "LIGHTINTNS": "LIH",
        "AK_LOW": 1, "AK_HIGH": 1, "US_LOW": 0, "US_HIGH": 0, "US_AREA": 0, "PACIFIC": 0,
        "Shape__Area": 1558000.0, "Shape__Length": 15980.0,
    }
    return {"OBJECTID": oid, **{k: v for k, v in values.items() if k != "OBJECTID"}}


def _geometries():
    def ring(x0, y0, x1, y1, reverse=False):
        pts = [(x0, y0), (x0, y1), (x1, y1), (x1, y0), (x0, y0)]
        if reverse:
            pts.reverse()
        return [[float(x), float(y)] for x, y in pts]

    return {
        # oid 10: one shell (clockwise)
        10: {"rings": [ring(-160.0, 14.0, -159.5, 14.4)]},
        # oid 11: shell with a counter-clockwise hole
        11: {"rings": [ring(-119.5, 34.2, -119.0, 34.6),
                       ring(-119.3, 34.3, -119.2, 34.5, reverse=True)]},
        # oid 12: two disjoint shells (MultiPolygon)
        12: {"rings": [ring(-123.1, 49.2, -122.9, 49.4),
                       ring(-122.85, 49.25, -122.7, 49.35)]},
        # oid 13: null geometry
        13: None,
        # oid 14: empty rings
        14: {"rings": []},
        # oid 15: one shell
        15: {"rings": [ring(-59.0, 64.9, -58.9, 65.0)]},
    }


def _fixture():
    fields = _fields()
    layer = {"id": 0, "name": "Runways", "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "objectIdField": "OBJECTID",
             "hasZ": True, "hasM": False, "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "extent": EXTENT, "fields": fields,
             "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
    service = {"serviceItemId": "4d8fa46181aa470d809776c57a8ab1f6",
               "layers": [{"id": 0, "name": "Runways", "type": "Feature Layer"}]}
    geometries = _geometries()
    features = [{"attributes": _attributes(oid), "geometry": geometries[oid]}
                for oid in (10, 11, 12, 13, 14, 15)]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in fields]
    attr_names = [f["name"] for f in fields]
    (geometry_records, null_count, empty_count, invalid_count, _vertices,
     bounds) = source._classify_ring_geometries(features, attr_names)
    crs = {"wkid": 4326, "latestWkid": 4326}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:4], "spatialReference": crs, "exceededTransferLimit": True}
    page2 = {"features": features[4:], "spatialReference": crs}
    docs = {"portal_item": json.loads(PORTAL_ITEM_BODY), "service_before": service,
            "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1, "page_000004_00002": page2}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    counts = (null_count, empty_count, invalid_count)
    return fields, features, captures, schema, geometry_records, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, empty_count, invalid_count = counts
    return dict(EXPECTED_COUNT=6, PAGE_START_RECORDS=2,
                EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_EMPTY_GEOMETRY_COUNT=empty_count,
                EXPECTED_INVALID_GEOMETRY_COUNT=invalid_count,
                EXPECTED_GLOBAL_ID_NULL_COUNT=0, EXPECTED_GLOBAL_ID_DUPLICATE_COUNT=0,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=source._list_sha(geom),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(0, "Runways", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -177.39266815799996, "ymin": -14.340687855999931,
                                       "xmax": 174.13517812600003, "ymax": 76.53588492300008},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


class RunwaysTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), hashlib.sha256(_canonical(items)).hexdigest())

    def test_native_fields_ids_and_leading_zeros_preserved_in_parquet(self):
        fields, features, captures, schema, geom, counts, _ = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom, counts, None)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(table.column("GLOBAL_ID").to_pylist()[0],
                         f"10A0B0C0-0000-4000-8000-0000000000{10:02X}")
        self.assertEqual(table.column("AIRPORT_ID").to_pylist(),
                         ["D6E7B0C0-0000-4000-8000-0000000000AB"] * 6)
        # The producer's leading-zero designator survives verbatim as a string.
        self.assertEqual(table.column("DESIGNATOR").to_pylist(), ["05/23"] * 6)
        self.assertEqual(table.column("LENGTH").type.__str__(), "double")
        self.assertEqual(table.column("LIGHTACTV").type.__str__(), "int16")
        self.assertEqual(table.column("LIGHTACTV").to_pylist(), [2] * 6)
        self.assertEqual(table.column("Shape__Area").to_pylist(), [1558000.0] * 6)
        shapes = [shapely.from_wkb(wkb) if wkb is not None else None
                  for wkb in table.column("geometry").to_pylist()]
        self.assertIsInstance(shapes[0], shapely.MultiPolygon)
        self.assertEqual(shapely.get_num_geometries(shapes[0]), 1)
        # Shell with its hole.
        self.assertEqual(shapely.get_num_geometries(shapes[1]), 1)
        self.assertEqual(shapely.get_num_interior_rings(shapely.get_geometry(shapes[1], 0)), 1)
        self.assertEqual(shapely.get_num_geometries(shapes[2]), 2)
        self.assertIsNone(shapes[3])
        self.assertIsNone(shapes[4])
        self.assertIsInstance(shapes[5], shapely.MultiPolygon)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13, 15]), ("ids_before", [11, 12, 10, 13, 14, 15]),
                          ("ids_after", [10, 11, 12, 13, 14, 15])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14, 15])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["empty_geometry_count"], 1)

    def test_portal_item_owner_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["owner"] = "NOT_FAA"
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_url_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["url"] = "https://mirror.invalid/arcgis/rest/services/Other/FeatureServer"
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_cycle_text_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["description"] = "A repackaged copy of runways, no FAA AIS cycle statement."
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "official_FAA_cycle_item_relationship_drift"):
                source._validate_captures(captures)

    def test_service_item_identity_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["serviceItemId"] = "00000000000000000000000000000000"
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "official_FAA_runways_service_identity_drift"):
                source._validate_captures(captures)

    def test_service_layer_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["layers"].append({"id": 1, "name": "Extra Layer", "type": "Feature Layer"})
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "official_FAA_runways_service_identity_drift"):
                source._validate_captures(captures)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPolyline"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "exact_FAA_runways_layer_identity_drift"):
                source._validate_captures(captures)

    def test_layer_hasz_flag_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["hasZ"] = False
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "exact_FAA_runways_layer_identity_drift"):
                source._validate_captures(captures)

    def test_pagination_capability_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["advancedQueryCapabilities"] = {"supportsPagination": False, "supportsOrderBy": True}
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_pagination_capability_changed"):
                source._validate_captures(captures)

    def test_record_limit_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["maxRecordCount"] = 1000
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_limit_drift"):
                source._validate_captures(captures)

    def test_layer_extent_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["extent"]["xmax"] = 170.0
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
                source._validate_captures(captures)

    def test_native_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_crs_drift"):
                source._validate_captures(captures)

    def test_page_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["spatialReference"] = {"wkid": 4269, "latestWkid": 4269}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"] = page["features"][:1]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(captures)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        final = json.loads(captures["page_000004_00002"][0])
        final["exceededTransferLimit"] = True
        captures["page_000004_00002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["DIM_UOM"] = "METERS"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_geometry_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["rings"][0][1][0] = -159.75
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_coordinate_range_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["rings"][0][1] = [200.0, 14.4]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
                source._validate_captures(captures)

    def test_non_ring_geometry_fails_closed(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["geometry"] = {"x": -123.1, "y": 49.2}
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_null_geometry_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000004_00002"][0])
        page["features"][0]["geometry"] = None
        captures["page_000004_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_empty_geometry_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["geometry"] = {"rings": []}
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_global_id_null_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["GLOBAL_ID"] = None
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
                source._validate_captures(captures)

    def test_global_id_duplicate_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["attributes"]["GLOBAL_ID"] = "10A0B0C0-0000-4000-8000-00000000000B"
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_duplicate_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -160.0, "ymin": 14.0, "xmax": -119.0, "ymax": 34.6}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        count = json.loads(captures["count_after"][0])
        count["count"] = 5
        captures["count_after"] = (json.dumps(count, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                source._validate_captures(captures)

    def test_id_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        ids = json.loads(captures["ids_after"][0])
        ids["objectIds"] = [10, 11, 12, 13, 14, 99]
        captures["ids_after"] = (json.dumps(ids, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(captures)

    def test_seed_roster_mismatch_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        seed = json.loads(captures["ids_seed"][0])
        seed["objectIds"] = [10, 11, 12, 13, 14, 99]
        captures["ids_seed"] = (json.dumps(seed, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(captures)

    def test_page_tiling_gap_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        del captures["page_000004_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_page_name_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000004_00002")
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_layer_metadata_change_during_capture_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_after"][0])
        layer["maxRecordCount"] = 1000
        captures["layer_after"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
                source._validate_captures(captures)

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        ok_body = b'{"objectIds": []}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if len(calls) == 1 else ok_body
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep") as sleep_mock:
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), ok_body)
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), error_body)
            self.assertTrue(sleep_mock.called)

    def test_persistent_producer_error_doc_exhausts_retries(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, error_body)
            self.assertEqual(len(calls), source.DOC_CAPTURE_ATTEMPTS)
            for attempt in range(1, source.DOC_CAPTURE_ATTEMPTS):
                self.assertEqual((root / f"probe.attempt-{attempt:02d}.body").read_bytes(), error_body)

    def test_incomplete_transport_capture_is_retried_bounded(self):
        ok_body = b'{"objectIds": []}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "probe2":
                raise ValueError("unrelated_source_capture_failure")
            if len(calls) == 1:
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            digest = hashlib.sha256(ok_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(ok_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(ok_body)
            (capture_root / f"{name}.wire").write_bytes(ok_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return ok_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
                with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                    source._capture_with_retry(None, "https://producer.invalid/other",
                                               root, "probe2", max_bytes=1024,
                                               attempts=source.DOC_CAPTURE_ATTEMPTS,
                                               base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls.count("probe"), 2)
            self.assertEqual(len(calls), 3)
            self.assertEqual((root / "probe.attempt-01.receipt.json").read_bytes(), b'{"status": null}\n')

    def test_adaptive_bisection_splits_producer_error_doc_failures(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if name == "page_000000_00004" else bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-14T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6)
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual((root / "page_000000_00004.body").read_bytes(), error_body)
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_adaptive_bisection_splits_persistent_transport_failures(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "page_000000_00004":
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-14T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6)
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_unservable_single_feature_fails_explicitly(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=2), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_pages(None, root, 6)

    def test_capture_requires_explicit_local_staging(self):
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_runways(PublishedStorageResource(use_local=True,
                                                                local_dir=str(root)),
                                       version="v20261114")
            bucketed = StagingStorageResource(use_local=False, bucket="some-bucket")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_runways(bucketed, version="v20261114")
            prefixed = StagingStorageResource(use_local=True, local_dir=str(root), prefix="sub")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_runways(prefixed, version="v20261114")

    def test_invalid_version_is_rejected(self):
        from dagster_hifld.resources import StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "invalid_version"):
                source.capture_runways(storage, version="v/invalid")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/runways-1.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/runways-1.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def _seal_catalog_cache_flow(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom, counts, bounds = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-14T00:00:00+00:00",
                       "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest, "content_encoding": None,
                       "content_length": str(len(body)), "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="faa-runways-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_runways(storage, version="vfixture",
                                                   capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_empty_geometry_count"], counts[1])
                self.assertEqual(candidate["source_invalid_geometry_count"], counts[2])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                         candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                # The candidate's geometry column is null for null and empty
                # source geometry; the catalog reports both as null geometry.
                self.assertEqual(summary.quality_manifest["null_geometry_count"],
                                 counts[0] + counts[1])
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG,
                                       file_slug=source.FILE_SLUG, version=candidate["version"],
                                       quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_runways(storage, version="vfixture",
                                                capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="faa-runways-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/runways.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()


if __name__ == "__main__":
    unittest.main()
