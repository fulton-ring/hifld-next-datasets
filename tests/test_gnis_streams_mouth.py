"""Focused contract checks for the USGS GNIS Streams (Mouth) source adapter."""
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

from dagster_hifld.sources import gnis_streams_mouth as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/geonames/MapServer"

DOC_PAGE_BODY = (b"<html>USGS Geographic Names Information System (GNIS). Download the "
                 b"Geographic Names data; feature and XML services at "
                 b"https://carto.nationalmap.gov/arcgis/rest/services/geonames/MapServer"
                 b"</html>")

EXTENT = {"xmin": -19780547.5984, "ymin": -1616028.9976000004,
          "xmax": 20000987.6666, "ymax": 11509303.568499997,
          "spatialReference": {"wkid": 102100, "latestWkid": 3857, "xyTolerance": 0.001}}


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "gaz_id", "type": "esriFieldTypeInteger", "alias": "gaz_id", "domain": None},
        {"name": "gaz_name", "type": "esriFieldTypeString", "alias": "gaz_name", "length": 120, "domain": None},
        {"name": "gaz_featureclass", "type": "esriFieldTypeString", "alias": "gaz_featureclass", "length": 50, "domain": None},
        {"name": "fcode", "type": "esriFieldTypeInteger", "alias": "fcode", "domain": None},
        {"name": "state_alpha", "type": "esriFieldTypeString", "alias": "state_alpha", "length": 1073741822, "domain": None},
        {"name": "county_name", "type": "esriFieldTypeString", "alias": "county_name", "length": 100, "domain": None},
        {"name": "shape", "type": "esriFieldTypeGeometry", "alias": "shape", "domain": None},
        {"name": "objectid_1", "type": "esriFieldTypeInteger", "alias": "objectid_1", "domain": None},
        {"name": "isunknowncoords", "type": "esriFieldTypeInteger", "alias": "isunknowncoords", "domain": None},
    ]


def _attributes(oid):
    rows = {
        10: {"gaz_id": 568708, "gaz_name": "Delta Channel", "gaz_featureclass": "Canal",
             "fcode": 33600, "state_alpha": "AK", "county_name": "Valdez-Cordova",
             "objectid_1": 1001, "isunknowncoords": 2},
        11: {"gaz_id": 3074, "gaz_name": "Cochise Wash", "gaz_featureclass": "Stream",
             "fcode": 46000, "state_alpha": "AZ", "county_name": "Maricopa",
             "objectid_1": 1270, "isunknowncoords": 2},
        12: {"gaz_id": 3074, "gaz_name": "Cochise Wash", "gaz_featureclass": "Stream",
             "fcode": 46000, "state_alpha": "AZ", "county_name": "Pima",
             "objectid_1": 1270, "isunknowncoords": 2},
        13: {"gaz_id": 425, "gaz_name": "Acton Brook", "gaz_featureclass": "Stream",
             "fcode": 46000, "state_alpha": None, "county_name": "Los Angeles",
             "objectid_1": 9, "isunknowncoords": 2},
        14: {"gaz_id": 5550, "gaz_name": "Adams Slough", "gaz_featureclass": "Stream",
             "fcode": 46003, "state_alpha": "WA", "county_name": "Island",
             "objectid_1": 2299425, "isunknowncoords": 2},
        15: {"gaz_id": 3075, "gaz_name": "Rincon Run", "gaz_featureclass": "Stream",
             "fcode": 46006, "state_alpha": "TX", "county_name": "Val Verde",
             "objectid_1": 8812, "isunknowncoords": 2},
    }
    return {"OBJECTID": oid, **rows[oid]}


def _geometries():
    return {
        10: {"hasZ": True, "points": [[-19780547.5984, -1616028.9976000004, 0]]},
        11: {"hasZ": True, "points": [[-12235164.8512, 3780799.8581999987, 0],
                                      [-12235170.0, 3780805.0, 0]]},
        12: {"hasZ": True, "points": [[-12549345.6127, 4203438.773699999, 0],
                                      [-12549350.0, 4203440.0, 0],
                                      [-12549360.0, 4203450.0, 0]]},
        13: {"hasZ": True, "points": [[-13000000.0, 4000000.0, 0]]},
        14: {"hasZ": True, "points": [[20000987.6666, 11509303.568499997, 0]]},
        15: {"hasZ": True, "points": [[-11000000.0, 3500000.0, 0]]},
    }


def _fixture():
    fields = _fields()
    layer = {"id": 6, "name": "Streams (Mouth)", "type": "Feature Layer",
             "geometryType": "esriGeometryMultipoint", "hasZ": True, "maxRecordCount": 2000,
             "displayField": "gaz_name",
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "extent": EXTENT, "fields": fields}
    service = {"copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
               "serviceDescription": source.EXPECTED_SERVICE_DESCRIPTION_PREFIX
                                     + " The National Map Gazetteer contains information about "
                                       "physical and cultural geographic features.",
               "spatialReference": {"wkid": 102100, "latestWkid": 3857},
               "layers": [{"id": entry[0], "name": entry[1], "type": entry[2],
                           "parentLayerId": entry[3]} for entry in source.EXPECTED_SERVICE_LAYERS]}
    features = [{"attributes": _attributes(oid), "geometry": _geometries()[oid]}
                for oid in (10, 11, 12, 13, 14, 15)]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "domain") if k in f}
              for f in fields]
    attr_names = [f["name"] for f in fields if f["type"] != "esriFieldTypeGeometry"]
    geometry_records, null_count, invalid_count, bounds = source._classify_geometries(features, attr_names)
    crs = {"wkid": 102100, "latestWkid": 3857}
    page0 = {"features": features[:4], "spatialReference": crs, "hasZ": True,
             "exceededTransferLimit": True}
    page1 = {"features": features[4:], "spatialReference": crs, "hasZ": True}
    docs = {"usgs_gnis_doc_page": DOC_PAGE_BODY, "service_before": service,
            "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00004": page0, "page_000004_00002": page1}
    captures = {}
    for name, doc in docs.items():
        body = (doc if name == "usgs_gnis_doc_page"
                else json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode())
        captures[name] = (body, {"status": 200})
    geom = geometry_records
    counts = (null_count, invalid_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, invalid_count = counts
    return dict(EXPECTED_COUNT=6, PAGE_START_RECORDS=4,
                EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_INVALID_GEOMETRY_COUNT=invalid_count,
                EXPECTED_GAZ_ID_NULL_COUNT=0,
                EXPECTED_OBJECTID_1_NULL_COUNT=0,
                EXPECTED_STATE_ALPHA_NULL_COUNT=1,
                EXPECTED_COUNTY_NAME_NULL_COUNT=0,
                EXPECTED_ROW_IDENTITY_DUPLICATE_COUNT=0,
                EXPECTED_FEATURE_CLASS_VALUES=["Canal", "Stream"],
                EXPECTED_FCODE_VALUES=[33600, 46000, 46003, 46006],
                EXPECTED_UNKNOWN_COORDS_VALUES=[2],
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=source._list_sha(sorted(geom, key=lambda record: record[0])),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYERS=source.EXPECTED_SERVICE_LAYERS,
                EXPECTED_LAYER_EXTENT={"xmin": -19780547.5984, "ymin": -1616028.9976000004,
                                       "xmax": 20000987.6666, "ymax": 11509303.568499997},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


def _page_body(name: str, all_features: list[dict]) -> bytes:
    """Serve any (possibly bisected) page name from the fixture feature roster."""
    offset, count = source._parse_page_name(name)
    rows = all_features[offset:offset + count]
    doc = {"features": rows, "spatialReference": {"wkid": 102100, "latestWkid": 3857},
           "hasZ": True}
    if offset + count < len(all_features):
        doc["exceededTransferLimit"] = True
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()


class GnisStreamsMouthTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), hashlib.sha256(_canonical(items)).hexdigest())

    def test_native_fields_ids_nulls_and_multipoint_z_preserved_in_parquet(self):
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
        self.assertEqual(table.column("gaz_id").type.__str__(), "int32")
        self.assertEqual(table.column("gaz_id").to_pylist(), [568708, 3074, 3074, 425, 5550, 3075])
        self.assertEqual(table.column("gaz_name").to_pylist()[0], "Delta Channel")
        self.assertEqual(table.column("gaz_featureclass").to_pylist(),
                         ["Canal", "Stream", "Stream", "Stream", "Stream", "Stream"])
        self.assertEqual(table.column("fcode").to_pylist(),
                         [33600, 46000, 46000, 46000, 46003, 46006])
        # The producer's single null state_alpha is preserved as null.
        self.assertEqual(table.column("state_alpha").to_pylist(),
                         ["AK", "AZ", "AZ", None, "WA", "TX"])
        self.assertEqual(table.column("county_name").to_pylist(),
                         ["Valdez-Cordova", "Maricopa", "Pima", "Los Angeles", "Island",
                          "Val Verde"])
        # gaz_id and objectid_1 repeat across the per-county rows of one feature.
        self.assertEqual(table.column("objectid_1").to_pylist()[0], 1001)
        self.assertEqual(table.column("objectid_1").to_pylist()[1:3], [1270, 1270])
        self.assertEqual(table.column("isunknowncoords").to_pylist(), [2] * 6)
        geometries = [shapely.from_wkb(wkb) if wkb is not None else None
                      for wkb in table.column("geometry").to_pylist()]
        self.assertEqual([type(g).__name__ for g in geometries], ["MultiPoint"] * 6)
        self.assertTrue(all(shapely.has_z(g) for g in geometries))
        self.assertEqual((geometries[0].geoms[0].x, geometries[0].geoms[0].y,
                          geometries[0].geoms[0].z),
                         (-19780547.5984, -1616028.9976000004, 0.0))
        self.assertEqual(len(geometries[1].geoms), 2)
        self.assertEqual(len(geometries[2].geoms), 3)
        self.assertEqual((geometries[4].geoms[0].x, geometries[4].geoms[0].y),
                         (20000987.6666, 11509303.568499997))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 3857)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPoint Z"])

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
        self.assertEqual(extracted["audit"]["state_alpha_null_count"], 1)
        self.assertEqual(extracted["audit"]["county_name_null_count"], 0)

    def test_usgs_documentation_link_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        captures["usgs_gnis_doc_page"] = (b"<html>an unrelated page without the service</html>",
                                          {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_gnis_documentation_link_drift"):
                source._validate_captures(captures)

    def test_service_copyright_refresh_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["copyrightText"] = "USGS The National Map: GNIS. Data Refreshed March, 2027."
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_gnis_service_identity_drift:service_before"):
                source._validate_captures(captures)

    def test_service_layer_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["layers"].append({"id": 99, "name": "Extra Layer", "type": "Feature Layer",
                                  "parentLayerId": -1})
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_gnis_service_layer_roster_drift:service_before"):
                source._validate_captures(captures)

    def test_service_description_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["serviceDescription"] = "A repackaged gazetteer copy."
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_gnis_service_description_drift:service_before"):
                source._validate_captures(captures)

    def test_service_spatial_reference_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_service_spatial_reference.*crs_drift"):
                source._validate_captures(captures)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPoint"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "exact_gnis_streams_mouth_layer_metadata_drift"):
                source._validate_captures(captures)

    def test_layer_z_or_m_contract_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["hasZ"] = False
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_z_or_m_contract_drift"):
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
        layer["extent"]["xmax"] = -50.0
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
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
        page = json.loads(captures["page_000000_00004"][0])
        page["features"] = page["features"][:3]
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
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

    def test_page_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["gaz_name"] = "Renamed"
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_geometry_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        # Probe a non-bounds-sentinel feature so the observed coverage is unchanged.
        page["features"][3]["geometry"]["points"][0][0] = -13000010.0
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_multipoint_member_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][1]["geometry"]["points"] = page["features"][1]["geometry"]["points"][:1]
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_coordinate_outside_declared_extent_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["geometry"]["points"][0] = [-20500000.0, -1616028.9, 0]
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
                source._validate_captures(captures)

    def test_non_numeric_point_ordinate_fails_closed(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["geometry"]["points"][0] = ["NaN", -1616028.9, 0]
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_point_ordinate_not_numeric:x"):
                source._validate_captures(captures)

    def test_missing_z_dimension_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["geometry"]["points"][0] = [-19780547.5984, -1616028.9976000004]
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_point_z_dimension_missing"):
                source._validate_captures(captures)

    def test_nonzero_z_ordinate_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["geometry"]["points"][0][2] = 12.5
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_point_z_not_observed_zero"):
                source._validate_captures(captures)

    def test_empty_or_absent_points_is_invalid(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        # Probe a non-bounds-sentinel feature so the observed coverage is unchanged.
        page["features"][3]["geometry"] = {"hasZ": True, "points": []}
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_null_geometry_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        # Probe a non-bounds-sentinel feature so the observed coverage is unchanged.
        page["features"][3]["geometry"] = None
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_feature_class_value_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["gaz_featureclass"] = "Canal?"
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_feature_class_value_drift"):
                source._validate_captures(captures)

    def test_fcode_value_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["fcode"] = 46006
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_fcode_value_drift"):
                source._validate_captures(captures)

    def test_gaz_id_null_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["gaz_id"] = None
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_gaz_id_null_drift"):
                source._validate_captures(captures)

    def test_objectid_1_null_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["objectid_1"] = None
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_objectid_1_null_drift"):
                source._validate_captures(captures)

    def test_county_name_null_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][0]["attributes"]["county_name"] = None
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_county_name_null_count_drift"):
                source._validate_captures(captures)

    def test_row_identity_duplicate_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][2]["attributes"]["county_name"] = "Maricopa"
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_row_identity_duplicate_drift"):
                source._validate_captures(captures)

    def test_state_alpha_null_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00004"][0])
        page["features"][1]["attributes"]["state_alpha"] = None
        captures["page_000000_00004"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_state_alpha_null_count_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -160.0, "ymin": 14.0, "xmax": -119.5, "ymax": 34.42}
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
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if name == "page_000000_00004" else _page_body(name, features)
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
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
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "page_000000_00004":
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            body = _page_body(name, features)
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
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
                source.capture_gnis_streams_mouth(PublishedStorageResource(use_local=True,
                                                                           local_dir=str(root)),
                                                  version="v20261113")
            bucketed = StagingStorageResource(use_local=False, bucket="some-bucket")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_gnis_streams_mouth(bucketed, version="v20261113")
            prefixed = StagingStorageResource(use_local=True, local_dir=str(root), prefix="sub")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_gnis_streams_mouth(prefixed, version="v20261113")

    def test_invalid_version_is_rejected(self):
        from dagster_hifld.resources import StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "invalid_version"):
                source.capture_gnis_streams_mouth(storage, version="v/invalid")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/streams-mouth.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/streams-mouth.parquet", b"accepted")
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
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-13T00:00:00+00:00",
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

        with tempfile.TemporaryDirectory(prefix="gnis-streams-mouth-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_gnis_streams_mouth(storage, version="vfixture",
                                                              capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_invalid_geometry_count"], counts[1])
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
                self.assertEqual(summary.quality_manifest["feature_count"], 6)
                self.assertEqual(summary.quality_manifest["geometry_type"], "MultiPoint")
                self.assertTrue(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG,
                                       file_slug=source.FILE_SLUG, version=candidate["version"],
                                       quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_gnis_streams_mouth(storage, version="vfixture",
                                                           capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="gnis-streams-mouth-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/gnis_streams_mouth.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()


if __name__ == "__main__":
    unittest.main()
