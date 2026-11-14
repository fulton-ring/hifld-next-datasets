import copy
import io
import json
import math
import pathlib
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
    _layer_contract_unchanged,
    _parquet,
    _parquet_pages,
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
        "hasM": True,
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

    def test_polyline_geometry_retains_served_m_and_fails_closed_on_drift(self):
        # The layer declares hasM: true (observed 2026-11-14), so served
        # [x, y, m] vertices are within the producer's declared schema: null m
        # and finite populated m are both retained; z and non-finite fail closed.
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, None]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5], [3.0, 4.0]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, 7.25]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, 1274473973.0000002]]]})
        for bad in (
            {"hasM": True, "paths": [[[1.5, -2.5], [float("nan"), 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, float("nan")]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, float("inf")]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, "x"]]]},
            {"hasM": True, "paths": [[[1.5, -2.5], ["x", 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5]]], "z": 3},
            {"hasM": True, "paths": [[[1.5, -2.5]]], "m": 3},
            {"hasM": True, "paths": []},
            {"hasM": True, "paths": [[[1.5]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, None, 3.0]]]},
        ):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                    ValueError, "trails_(unhandled_z_or_m_ordinate|polyline_geometry_drift|nonfinite_geometry)"):
                _validate_polyline_geometry(bad)

    def test_polyline_geometry_rejects_populated_hasz(self):
        geometry = {"hasM": True, "hasZ": True, "paths": [[[1.5, -2.5, None]]]}
        with self.assertRaisesRegex(ValueError, "trails_unhandled_z_or_m_ordinate"):
            _validate_polyline_geometry(geometry)

    def test_layer_pins_declared_m_dimension(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["hasM"] = False
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)
        layer = copy.deepcopy(_layer_metadata())
        layer.pop("hasM")
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)

    def test_layer_before_after_gate_excludes_presentation_metadata(self):
        # Observed 2026-11-14: the producer rotated drawingInfo.colorRamp
        # colors across a 34-minute live capture while every data-contract
        # member stayed identical; the gate must not fail on presentation.
        after = copy.deepcopy(_layer_metadata())
        after["drawingInfo"] = {"renderer": {"authoringInfo": {"colorRamp": {"fromColor": [1, 2, 3]}}}}
        _layer_contract_unchanged(_layer_metadata(), after)
        after["maxRecordCount"] = 1000
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)
        after = copy.deepcopy(_layer_metadata())
        after["fields"][3]["name"] = "icewatercraft_2"
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)
        after = copy.deepcopy(_layer_metadata())
        after["extent"]["spatialReference"] = {"wkid": 4269}
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)

    def test_dimensions_probe_pins_requested_oid_and_retains_m_values(self):
        probe = {"geometryType": "esriGeometryPolyline",
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "features": [{"attributes": {"objectid": 1475127},
                               "geometry": {"hasM": True, "paths": [[[1.0, 2.0, None]]]}}]}
        _validate_dimensions_probe(probe, 1475127)
        probe["features"][0]["geometry"]["paths"][0][0][2] = 4.0
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
        params = {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                  "returnZ": "true", "returnM": "true", "orderByFields": "objectid ASC",
                  "resultOffset": "0", "resultRecordCount": str(BATCH_SIZE), "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("page_00000", {"url": url, "final_url": url}, ids)
        params.pop("returnM")
        url_without_m = LAYER_URL + "/query?" + urlencode(params)
        with self.assertRaisesRegex(ValueError, "trails_capture_request_scope_drift"):
            _validate_request_scope("page_00000", {"url": url_without_m, "final_url": url_without_m}, ids)
        # A 2,000-id objectIds page URI was answered HTTP 414 by the producer
        # (2026-11-14); the page contract must not reintroduce it.
        self.assertNotIn("objectIds", url)

    def test_pinned_roster_constants_are_self_consistent(self):
        self.assertEqual(EXPECTED_COUNT % BATCH_SIZE, EXPECTED_COUNT % BATCH_SIZE)
        self.assertGreater(EXPECTED_COUNT, BATCH_SIZE)
        self.assertEqual(len(EXPECTED_ID_SHA256), 64)

    def test_wkb_retains_populated_m_ordinates_verbatim(self):
        served = [[[1.5, -2.5, 7.25], [3.0, 4.0, 9.5]]]
        body = _wkb_polyline(served)
        self.assertEqual(body[:9], struct.pack("<BII", 1, 2005, 1))
        self.assertEqual(body[9:18], struct.pack("<BII", 1, 2002, 2))
        self.assertEqual(body[18:], struct.pack("<ddd", 1.5, -2.5, 7.25) + struct.pack("<ddd", 3.0, 4.0, 9.5))
        # All-null served m keeps the established 2D emission.
        plain = _wkb_polyline([[[1.5, -2.5, None], [3.0, 4.0, None]]])
        self.assertEqual(plain[:9], struct.pack("<BII", 1, 5, 1))
        self.assertEqual(plain[9:18], struct.pack("<BII", 1, 2, 2))
        self.assertEqual(plain[18:], struct.pack("<dddd", 1.5, -2.5, 3.0, 4.0))
        # A served null inside a populated-m feature encodes as NaN, the WKB
        # null-m convention, while populated values stay verbatim.
        mixed = _wkb_polyline([[[1.5, -2.5, None], [3.0, 4.0, 9.5]]])
        self.assertEqual(mixed[:9], struct.pack("<BII", 1, 2005, 1))
        x0, y0, m0 = struct.unpack("<ddd", mixed[18:42])
        x1, y1, m1 = struct.unpack("<ddd", mixed[42:66])
        self.assertEqual((x0, y0), (1.5, -2.5))
        self.assertTrue(math.isnan(m0))
        self.assertEqual((x1, y1, m1), (3.0, 4.0, 9.5))

    def test_sample_population_parquet_declares_served_m_dimension(self):
        fields = _fields(_layer_metadata())
        features = [
            _feature(0, [[[20.0, 20.0, 7.25], [30.0, 25.0, 9.5]]]),
            _feature(1, [[[0.0, 0.0, None], [10.0, 5.0, None]]]),
            _feature(2, [[[40.0, 40.0, None], [50.0, 45.0, 11.0]]]),
        ]
        body = _parquet(features, fields, len(features))
        table = pq.read_table(io.BytesIO(body))
        for row_number, feature in enumerate(features):
            self.assertEqual(table.column("geometry")[row_number].as_py(),
                             _wkb_polyline(feature["geometry"]["paths"]))
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"],
                         ["MultiLineString", "MultiLineString M"])

    def test_parquet_pages_rekeys_served_case_insensitive_attribute_keys(self):
        # The producer serves the declared 'shape_Length' as 'shape_length';
        # the page builder must read values through the lowercased served key
        # while emitting the declared name in the schema (regression for the
        # KeyError first reached after the geometry contract amendment).
        import tempfile
        fields = _fields(_layer_metadata())
        feature = _feature(0, [[[1.5, -2.5, 7.25], [3.0, 4.0, None]]])
        served = {"attributes": {key.lower(): value for key, value in feature["attributes"].items()},
                  "geometry": feature["geometry"]}
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "page_00000.body").write_bytes(json.dumps({"features": [served]}).encode())
            body = _parquet_pages(root, fields, 1)
        table = pq.read_table(io.BytesIO(body))
        self.assertIn("shape_Length", table.schema.names)
        self.assertEqual(table.column("shape_Length")[0].as_py(),
                         float(feature["attributes"]["shape_Length"]))
        self.assertEqual(table.column("geometry")[0].as_py(), _wkb_polyline(feature["geometry"]["paths"]))
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"], ["MultiLineString M"])

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
