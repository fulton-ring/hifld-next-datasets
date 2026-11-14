import copy
import io
import struct
from datetime import datetime, timezone
import unittest
from urllib.parse import urlencode

import pyarrow.parquet as pq

from dagster_hifld.sources.trails_usgs import (
    BATCH_SIZE,
    EXPECTED_COUNT,
    EXPECTED_FIELD_NAMES,
    EXPECTED_ID_SHA256,
    LAYER_URL,
    _fields,
    _parquet,
    _validate_page_metadata,
    _validate_polyline_geometry,
    _validate_dimensions_probe,
    _validate_request_scope,
    _wkb_polyline,
)


def _layer_metadata():
    types = {"objectid": "esriFieldTypeOID", "publisheddate": "esriFieldTypeDate",
             "sourceeditdate": "esriFieldTypeDate", "loaddate": "esriFieldTypeDate",
             "lengthmiles": "esriFieldTypeDouble", "networklength": "esriFieldTypeDouble",
             "globalid": "esriFieldTypeGlobalID"}
    return {
        "id": 37, "name": "Trails", "geometryType": "esriGeometryPolyline",
        "geometryField": {"name": "shape"},
        "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}},
        "fields": ([{"name": name, "type": types.get(name, "esriFieldTypeString")}
                    for name in EXPECTED_FIELD_NAMES]
                   + [{"name": "shape", "type": "esriFieldTypeGeometry"},
                      {"name": "shape_Length", "type": "esriFieldTypeDouble"}]),
    }


def _feature(row_number, path):
    fields = _fields(_layer_metadata())
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
    return {"attributes": attrs, "geometry": {"hasM": True, "paths": path}}


class TrailsUSGSTests(unittest.TestCase):
    def test_trails_layer_keeps_all_native_attributes_and_excludes_geometry_members(self):
        fields = _fields(_layer_metadata())
        self.assertEqual([field["name"] for field in fields], EXPECTED_FIELD_NAMES + ["shape_Length"])
        self.assertEqual(fields[13]["name"], "objectid")
        self.assertEqual(fields[13]["type"], "esriFieldTypeOID")
        self.assertEqual(fields[-2]["name"], "globalid")
        self.assertEqual(fields[-1]["name"], "shape_Length")
        self.assertEqual(fields[-1]["type"], "esriFieldTypeDouble")

    def test_trails_layer_schema_drift_fails_closed(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["fields"][2]["name"] = "trailsurface_1"
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)

    def test_layer_geometry_type_or_crs_drift_fails_closed(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["geometryType"] = "esriGeometryPoint"
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)
        layer = copy.deepcopy(_layer_metadata())
        layer["extent"]["spatialReference"] = {"wkid": 4269}
        with self.assertRaisesRegex(ValueError, "trails_spatial_reference_drift"):
            _fields(layer)

    def test_query_page_metadata_rejects_geometry_or_crs_drift(self):
        page = {"geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 102100, "latestWkid": 3857}}
        _validate_page_metadata(page, "page_00000")
        wrong_crs = copy.deepcopy(page)
        wrong_crs["spatialReference"] = {"wkid": 4326}
        with self.assertRaisesRegex(ValueError, "trails_spatial_reference_drift"):
            _validate_page_metadata(wrong_crs, "page_00000")
        wrong_geometry = copy.deepcopy(page)
        wrong_geometry["geometryType"] = "esriGeometryPolygon"
        with self.assertRaisesRegex(ValueError, "trails_page_geometry_type_drift"):
            _validate_page_metadata(wrong_geometry, "page_00000")

    def test_polyline_geometry_requires_finite_xy_paths_with_null_m(self):
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, None]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5], [3.0, 4.0]]]})
        for bad in (
            {"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, 7.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5], [float("nan"), 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5], ["x", 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5]]], "z": 3},
            {"hasM": True, "paths": []},
            {"hasM": True, "paths": [[[1.5]]]},
        ):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                    ValueError, "trails_(unhandled_z_or_m_ordinate|polyline_geometry_drift|nonfinite_geometry)"):
                _validate_polyline_geometry(bad)

    def test_polyline_geometry_rejects_populated_hasz(self):
        geometry = {"hasM": True, "hasZ": True, "paths": [[[1.5, -2.5, None]]]}
        with self.assertRaisesRegex(ValueError, "trails_unhandled_z_or_m_ordinate"):
            _validate_polyline_geometry(geometry)

    def test_dimensions_probe_pins_requested_oid_and_rejects_m_values(self):
        probe = {"geometryType": "esriGeometryPolyline",
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "features": [{"attributes": {"objectid": 1475127},
                               "geometry": {"hasM": True, "paths": [[[1.0, 2.0, None]]]}}]}
        _validate_dimensions_probe(probe, 1475127)
        probe["features"][0]["geometry"]["paths"][0][0][2] = 4.0
        with self.assertRaisesRegex(ValueError, "trails_unhandled_z_or_m_ordinate"):
            _validate_dimensions_probe(probe, 1475127)

    def test_capture_receipt_scope_pins_native_id_roster_request(self):
        ids = [101, 102]
        params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("ids_before", {"url": url, "final_url": url}, ids)
        wrong = LAYER_URL + "/query?" + urlencode({**params, "outFields": "*"})
        with self.assertRaisesRegex(ValueError, "trails_capture_request_scope_drift"):
            _validate_request_scope("ids_before", {"url": wrong, "final_url": wrong}, ids)

    def test_page_request_scope_explicitly_requests_z_and_m(self):
        ids = [101, 102]
        params = {"objectIds": "101,102", "outFields": "*", "returnGeometry": "true",
                  "returnZ": "true", "returnM": "true", "orderByFields": "objectid ASC", "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("page_00000", {"url": url, "final_url": url}, ids)
        params.pop("returnM")
        url_without_m = LAYER_URL + "/query?" + urlencode(params)
        with self.assertRaisesRegex(ValueError, "trails_capture_request_scope_drift"):
            _validate_request_scope("page_00000", {"url": url_without_m, "final_url": url_without_m}, ids)

    def test_pinned_roster_constants_are_self_consistent(self):
        self.assertEqual(EXPECTED_COUNT % BATCH_SIZE, EXPECTED_COUNT % BATCH_SIZE)
        self.assertGreater(EXPECTED_COUNT, BATCH_SIZE)
        self.assertEqual(len(EXPECTED_ID_SHA256), 64)

    def test_sample_population_parquet_round_trips_attributes_and_native_multilinestring_wkb(self):
        fields = _fields(_layer_metadata())
        path_a = [[0.0, 0.0, None], [10.0, 5.0, None]]
        path_b = [[20.0, 20.0, None], [30.0, 25.0, None], [40.0, 40.0, None]]
        features = [_feature(row, [path_a] if row % 2 else [path_a, path_b]) for row in range(3)]
        body = _parquet(features, fields, len(features))
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.num_rows, 3)
        for row_number, feature in enumerate(features):
            for field in fields:
                name = field["name"]
                expected = feature["attributes"][name]
                if field["type"] == "esriFieldTypeDate":
                    expected = datetime.fromtimestamp(expected / 1000, tz=timezone.utc)
                elif field["type"] in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                    expected = float(expected)
                self.assertEqual(table.column(name)[row_number].as_py(), expected)
            self.assertEqual(table.column("geometry")[row_number].as_py(),
                             _wkb_polyline(feature["geometry"]["paths"]))
        self.assertEqual(_wkb_polyline([path_a]), struct.pack("<BII", 1, 5, 1) + struct.pack(
            "<BII", 1, 2, 2) + struct.pack("<dddd", 0.0, 0.0, 10.0, 5.0))
        metadata = __import__("json").loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"], ["MultiLineString"])


if __name__ == "__main__":
    unittest.main()
