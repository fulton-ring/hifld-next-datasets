import copy
import io
import struct
from datetime import datetime, timezone
import unittest
from urllib.parse import urlencode

import pyarrow.parquet as pq

from dagster_hifld.sources.courthouses_usgs import (
    EXPECTED_COUNT,
    EXPECTED_FIELD_NAMES,
    LAYER_URL,
    _fields,
    _parquet,
    _validate_page_metadata,
    _validate_point_geometry,
    _validate_dimensions_probe,
    _validate_request_scope,
)


def _layer_metadata():
    types = {"OBJECTID": "esriFieldTypeOID", "LOADDATE": "esriFieldTypeDate",
             "FTYPE": "esriFieldTypeInteger", "FCODE": "esriFieldTypeInteger",
             "POINTLOCATIONTYPE": "esriFieldTypeInteger", "ADMINTYPE": "esriFieldTypeInteger",
             "GLOBALID": "esriFieldTypeGlobalID"}
    return {
        "id": 40, "name": "Courthouses", "geometryType": "esriGeometryPoint",
        "geometryField": {"name": "SHAPE"},
        "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}},
        "fields": ([{"name": "OBJECTID", "type": "esriFieldTypeOID"},
                    {"name": "SHAPE", "type": "esriFieldTypeGeometry"}]
                   + [{"name": name, "type": types.get(name, "esriFieldTypeString")}
                      for name in EXPECTED_FIELD_NAMES[1:]]),
    }


class CourthousesUSGSTests(unittest.TestCase):
    def test_courthouse_layer_keeps_all_native_attributes_and_excludes_geometry_member(self):
        fields = _fields(_layer_metadata())
        self.assertEqual([field["name"] for field in fields], EXPECTED_FIELD_NAMES)
        self.assertEqual(fields[0]["type"], "esriFieldTypeOID")
        self.assertEqual(fields[-1]["name"], "GLOBALID")


    def test_courthouse_layer_schema_drift_fails_closed(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["fields"][2]["name"] = "SHAPE_1"
        with self.assertRaisesRegex(ValueError, "courthouses_layer_schema_drift"):
            _fields(layer)

    def test_query_page_metadata_rejects_geometry_or_crs_drift(self):
        page = {"geometryType": "esriGeometryPoint", "spatialReference": {"wkid": 102100, "latestWkid": 3857, "vcsWkid": 5703}}
        _validate_page_metadata(page, "page_00000")
        wrong_crs = copy.deepcopy(page)
        wrong_crs["spatialReference"] = {"wkid": 4326}
        with self.assertRaisesRegex(ValueError, "courthouses_spatial_reference_drift"):
            _validate_page_metadata(wrong_crs, "page_00000")
        wrong_geometry = copy.deepcopy(page)
        wrong_geometry["geometryType"] = "esriGeometryPolygon"
        with self.assertRaisesRegex(ValueError, "courthouses_page_geometry_type_drift"):
            _validate_page_metadata(wrong_geometry, "page_00000")

    def test_query_geometry_does_not_silently_discard_z_or_m(self):
        _validate_point_geometry({"x": 1.5, "y": -2.5})
        for ordinate in ("z", "m"):
            with self.subTest(ordinate=ordinate), self.assertRaisesRegex(ValueError, "courthouses_unhandled_z_or_m_ordinate"):
                _validate_point_geometry({"x": 1.5, "y": -2.5, ordinate: 10})

    def test_dimensions_probe_records_requested_xy_only_response_and_rejects_vertical_values(self):
        probe = {"geometryType": "esriGeometryPoint", "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "features": [{"attributes": {"objectid": 6410266}, "geometry": {"x": 1, "y": 2}}]}
        _validate_dimensions_probe(probe, 6410266)
        probe["features"][0]["geometry"]["z"] = 4
        with self.assertRaisesRegex(ValueError, "courthouses_unhandled_z_or_m_ordinate"):
            _validate_dimensions_probe(probe, 6410266)

    def test_capture_receipt_scope_pins_native_id_roster_request(self):
        ids = [101, 102]
        params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("ids_before", {"url": url, "final_url": url}, ids)
        wrong = LAYER_URL + "/query?" + urlencode({**params, "outFields": "*"})
        with self.assertRaisesRegex(ValueError, "courthouses_capture_request_scope_drift"):
            _validate_request_scope("ids_before", {"url": wrong, "final_url": wrong}, ids)

    def test_page_request_scope_explicitly_requests_z_and_m(self):
        ids = [101, 102]
        params = {"objectIds": "101,102", "outFields": "*", "returnGeometry": "true",
                  "returnZ": "true", "returnM": "true", "orderByFields": "OBJECTID ASC", "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("page_00000", {"url": url, "final_url": url}, ids)
        params.pop("returnM")
        url_without_m = LAYER_URL + "/query?" + urlencode(params)
        with self.assertRaisesRegex(ValueError, "courthouses_capture_request_scope_drift"):
            _validate_request_scope("page_00000", {"url": url_without_m, "final_url": url_without_m}, ids)


    def test_full_population_parquet_round_trips_all_attributes_and_native_point_wkb(self):
        fields = _fields(_layer_metadata())
        features = []
        for row_number in range(EXPECTED_COUNT):
            attrs = {}
            for field in fields:
                name, kind = field["name"], field["type"]
                if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
                    value = row_number + 1
                elif kind == "esriFieldTypeDate":
                    value = row_number * 1000
                elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                    value = float(row_number) + 0.25
                else:
                    value = f"{name}-{row_number}"
                attrs[name] = value
            features.append({"attributes": attrs, "geometry": {"x": row_number + 0.25, "y": -row_number - 0.5}})

        body = _parquet(features, fields)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.num_rows, EXPECTED_COUNT)
        for row_number in (0, 499, EXPECTED_COUNT - 1):
            for field in fields:
                name = field["name"]
                expected = features[row_number]["attributes"][name]
                if field["type"] == "esriFieldTypeDate":
                    expected = datetime.fromtimestamp(expected / 1000, tz=timezone.utc)
                elif field["type"] in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                    expected = float(expected)
                self.assertEqual(table.column(name)[row_number].as_py(), expected)
            self.assertEqual(table.column("geometry")[row_number].as_py(), struct.pack(
                "<BIdd", 1, 1, row_number + 0.25, -row_number - 0.5,
            ))
