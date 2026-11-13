"""Focused contract checks for the USGS 3DHP Waterbody areal-hydrography source adapter."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nhd_area_large_scale as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>Access 3DHP Data Products: 3DHP web services at "
                    b"https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer</html>")

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "id3dhp", "type": "esriFieldTypeString", "alias": "id3dhp", "length": 7, "domain": None},
    {"name": "featuredate", "type": "esriFieldTypeDate", "alias": "featuredate", "domain": None},
    {"name": "mainstemid", "type": "esriFieldTypeString", "alias": "mainstemid", "length": 200, "domain": None},
    {"name": "gnisid", "type": "esriFieldTypeInteger", "alias": "gnisid", "domain": None},
    {"name": "gnisidlabel", "type": "esriFieldTypeString", "alias": "gnisidlabel", "length": 120, "domain": None},
    {"name": "featuretype", "type": "esriFieldTypeSmallInteger", "alias": "featuretype", "domain": None},
    {"name": "featuretypelabel", "type": "esriFieldTypeString", "alias": "featuretypelabel", "length": 200, "domain": None},
    {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "areasqkm", "domain": None},
    {"name": "workunitid", "type": "esriFieldTypeString", "alias": "workunitid", "length": 100, "domain": None},
    {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length", "domain": None},
    {"name": "Shape__Area", "type": "esriFieldTypeDouble", "alias": "Shape__Area", "domain": None},
]
ATTR_NAMES = [field["name"] for field in FIELDS]

# Clockwise closed exterior with Z (Esri JSON exterior convention), verbatim source ring.
RING_A = [[-158.6, 68.0, 246.0], [-158.5, 68.1, 246.0], [-158.4, 68.0, 246.0],
          [-158.5, 67.9, 246.0], [-158.6, 68.0, 246.0]]
# Counter-clockwise closed hole inside RING_A's exterior.
HOLE_A = [[-158.55, 68.02, 246.0], [-158.55, 68.01, 246.0], [-158.5, 68.01, 246.0],
          [-158.55, 68.02, 246.0]]
# Second clockwise closed exterior sharing one feature with HOLE_B.
RING_B = [[-158.2, 67.8, 247.0], [-158.2, 67.9, 247.0], [-158.1, 67.9, 247.0],
          [-158.2, 67.8, 247.0]]
HOLE_B = [[-158.18, 67.85, 247.0], [-158.18, 67.84, 247.0], [-158.15, 67.84, 247.0],
          [-158.18, 67.85, 247.0]]
# Counter-clockwise hole lying inside no exterior of its feature.
HOLE_OUTSIDE = [[-150.0, 60.0, 10.0], [-150.0, 59.0, 10.0], [-149.0, 59.0, 10.0],
                [-150.0, 60.0, 10.0]]


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "id3dhp": f"AB{oid:03d}X", "featuredate": 1694649600000,
              "mainstemid": None, "gnisid": None, "gnisidlabel": None, "featuretype": 1,
              "featuretypelabel": "River", "areasqkm": 1.5, "workunitid": "NHD",
              "Shape__Length": 321334.35, "Shape__Area": 9564422.8}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _window_name(ids):
    return f"page_{ids[0]:07d}"


def _fixture():
    fields = FIELDS
    layer = {"id": 60, "name": "Waterbody", "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "hasZ": True, "hasM": False,
             "maxRecordCount": 2500, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 102100, "latestWkid": 3857},
             "extent": {"xmin": -19951755.68, "ymin": -1622985.37, "xmax": 20021551.75,
                        "ymax": 11575301.96,
                        "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
             "fields": fields}
    layers = [{"id": 60, "name": "Waterbody"}, {"id": 50, "name": "Flowline"}]
    copyright_text = source.EXPECTED_COPYRIGHT_TEXT
    service = {"copyrightText": copyright_text, "layers": layers,
               "serviceDescription": "The USGS 3D Hydrography Program (3DHP) ArcGIS REST "
                                     "service (3DHP_all) from The National Map."}
    features = [
        {"attributes": _attrs(10), "geometry": {"hasZ": True, "rings": [RING_A]}},
        {"attributes": _attrs(11), "geometry": {"hasZ": True, "rings": [RING_A, HOLE_A]}},
        {"attributes": _attrs(12, featuretype=2, featuretypelabel="Canal", id3dhp="CD00012"),
         "geometry": {"hasZ": True, "rings": [RING_A, RING_B, HOLE_B]}},
        {"attributes": _attrs(13, id3dhp="NUL0013"), "geometry": None},
        {"attributes": _attrs(14, featuretype=4, featuretypelabel="Ocean or Great Lake",
                              id3dhp="OC00014"), "geometry": {"hasZ": True, "rings": [RING_B]}},
    ]
    ids = [10, 11, 12, 13, 14]
    schema = [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
               if key in field} for field in fields]
    windows = [ids[index:index + 2] for index in range(0, len(ids), 2)]
    pages = [features[index:index + 2] for index in range(0, len(features), 2)]
    crs = {"wkid": 4326, "latestWkid": 4326}
    docs = {"service_before": service, "service_after": service, "alias_service": service,
            "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 5}, "count_after": {"count": 5},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids}}
    page_docs = {}
    for window, page in zip(windows, pages):
        page_docs[_window_name(window)] = {
            "features": page, "spatialReference": crs, "hasZ": True, "geometryType": "esriGeometryPolygon",
            **({"exceededTransferLimit": False})}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    page_index = 0
    for window, doc in page_docs.items():
        url = source._window_url(windows[page_index])
        captures[window] = (
            json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(),
            {"status": 200, "url": url, "final_url": url})
        page_index += 1
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return fields, features, captures, schema, windows


def _bounds(features):
    values = []
    for feature in features:
        geometry = feature.get("geometry")
        if not geometry or "rings" not in geometry:
            continue
        for ring in geometry["rings"]:
            for vertex in ring:
                if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                    values.extend((vertex[0], vertex[1]))
    return {"xmin": min(values[0::2]), "ymin": min(values[1::2]),
            "xmax": max(values[0::2]), "ymax": max(values[1::2])}


def _geometry_records(features, windows):
    records = []
    for window in windows:
        window_features = [feature for feature in features
                           if feature["attributes"]["OBJECTID"] in window]
        records.extend([feature["attributes"]["OBJECTID"],
                        feature["geometry"]["rings"] if feature.get("geometry") else None][0:1]
                       + [feature["geometry"]["rings"] if feature.get("geometry") else None][1:]
                       for feature in window_features)
    # Explicit per-feature records: [oid, rings-or-None]
    out = []
    for window in windows:
        for feature in [item for item in features if item["attributes"]["OBJECTID"] in window]:
            geometry = feature.get("geometry")
            out.append([feature["attributes"]["OBJECTID"],
                        geometry["rings"] if geometry else None])
    return out


def _pins(features, schema, windows):
    attrs = sorted((feature["attributes"] for feature in features),
                   key=lambda row: row["OBJECTID"])
    distribution = {}
    for feature in features:
        if feature.get("geometry") is not None:
            code = feature["attributes"]["featuretype"]
            distribution[code] = distribution.get(code, 0) + 1
    geometry_sha = hashlib.sha256()
    for window in windows:
        page_records = [[feature["attributes"]["OBJECTID"],
                         feature["geometry"]["rings"] if feature.get("geometry") else None]
                        for feature in features if feature["attributes"]["OBJECTID"] in window]
        geometry_sha.update(_canonical(page_records))
    return dict(EXPECTED_COUNT=5, EXPECTED_NULL_GEOMETRY_COUNT=1,
                EXPECTED_REFUSED_IDS=frozenset(),
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(attrs)).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=geometry_sha.hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_FEATURETYPE_DISTRIBUTION=distribution,
                EXPECTED_SERVICE_LAYERS=[(60, "Waterbody"), (50, "Flowline")],
                EXPECTED_LAYER_EXTENT={"xmin": -19951755.68, "ymin": -1622985.37,
                                       "xmax": 20021551.75, "ymax": 11575301.96},
                EXPECTED_OBSERVED_BOUNDS=_bounds(features))


class NhdAreaLargeScaleTests(unittest.TestCase):
    def test_native_rings_z_and_null_geometry_preserved_in_parquet(self):
        fields, features, captures, schema, windows = _fixture()
        with patch.multiple(source, **_pins(features, schema, windows)):
            original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["attributes"], extracted["wkbs"])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14])
        self.assertEqual(table.column("id3dhp").to_pylist(),
                         ["AB010X", "AB011X", "CD00012", "NUL0013", "OC00014"])
        self.assertEqual(table.column("areasqkm").type.__str__(), "double")
        self.assertEqual(table.column("featuretype").to_pylist(), [1, 1, 2, 1, 4])
        self.assertEqual(table.column("geometry").null_count, 1)
        polygon_10 = shapely.from_wkb(table.column("geometry").to_pylist()[0])
        self.assertEqual(len(polygon_10.exterior.coords), len(RING_A))
        self.assertEqual([coord[2] for coord in polygon_10.exterior.coords],
                         [vertex[2] for vertex in RING_A])
        self.assertEqual(len(polygon_10.interiors), 0)
        polygon_11 = shapely.from_wkb(table.column("geometry").to_pylist()[1])
        self.assertEqual(len(polygon_11.interiors), 1)
        self.assertIsNone(table.column("geometry").to_pylist()[3])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon Z", "MultiPolygon Z"])

    def test_hole_joins_smallest_containing_exterior_as_multipolygon(self):
        multipolygon = shapely.from_wkb(source._polygon_wkb(
            {"hasZ": True, "rings": [RING_A, RING_B, HOLE_B]}))
        self.assertIsInstance(multipolygon, shapely.MultiPolygon)
        self.assertTrue(all(geom.has_z for geom in multipolygon.geoms))
        second = list(multipolygon.geoms)[1]
        self.assertEqual(len(second.interiors), 1)
        with self.assertRaisesRegex(ValueError, "source_polygon_hole_unassigned"):
            source._polygon_wkb({"hasZ": True, "rings": [RING_B, HOLE_OUTSIDE]})
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_degenerate"):
            source._polygon_wkb({"hasZ": True, "rings": [
                [[-158.0, 67.0, 1.0], [-158.0, 67.0, 1.0], [-158.0, 67.0, 1.0], [-158.0, 67.0, 1.0]]]})
        with self.assertRaisesRegex(ValueError, "source_polygon_exterior_missing"):
            source._polygon_wkb({"hasZ": True, "rings": [HOLE_A, HOLE_B]})
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_not_closed"):
            source._polygon_wkb({"hasZ": True, "rings": [RING_A[:-1]]})
        with self.assertRaisesRegex(ValueError, "source_polygon_ordinate_contract_drift"):
            source._polygon_wkb({"hasZ": True, "rings": [[point[:2] for point in RING_A]]})
        with self.assertRaisesRegex(ValueError, "source_geometry_z_flag_missing"):
            source._polygon_wkb({"rings": [RING_A]})

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, captures, schema, windows = _fixture()
        for name, order in (("ids_seed", [12, 10, 11, 14, 13]), ("ids_before", [11, 12, 10, 13, 14])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = order
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        _, extracted = self._validated(features, captures, schema, windows)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)

    def _validated(self, features, captures, schema, windows):
        with patch.multiple(source, **_pins(features, schema, windows)):
            return source._validate_captures(captures)

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, captures, schema, windows = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        with self.assertRaisesRegex(ValueError, "usgs_service_documentation_link_drift"):
            self._validated(features, captures, schema, windows)

    def test_page_crs_drift_is_rejected(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["spatialReference"] = {"wkid": 4269, "latestWkid": 4269}
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_page_.*_crs_drift"):
            self._validated(features, captures, schema, windows)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, windows = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPoint"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "exact_3dhp_waterbody_layer_metadata_drift"):
            self._validated(features, captures, schema, windows)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([14])
        page = json.loads(captures[name][0])
        page["exceededTransferLimit"] = True
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_page_transfer_truncated"):
            self._validated(features, captures, schema, windows)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema, windows = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            self._validated(features, captures, schema, windows)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["features"] = page["features"][:1]
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            self._validated(features, captures, schema, windows)

    def test_window_request_identity_mismatch_is_rejected(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        body, receipt = captures[name]
        other_url = source._window_url([10, 11, 12])
        captures[name] = (body, {**receipt, "url": other_url})
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            self._validated(features, captures, schema, windows)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["features"][0]["attributes"]["areasqkm"] = 9.9
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, windows)

    def test_vertex_change_is_detected(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["features"][0]["geometry"]["rings"][0][1] = [-158.501, 68.1, 246.0]
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_geometry_or_coverage_drift"):
            self._validated(features, captures, schema, windows)

    def test_featuretype_label_mismatch_is_rejected(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["features"][0]["attributes"]["featuretypelabel"] = "Lake"
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_featuretype_label_drift"):
            self._validated(features, captures, schema, windows)

    def test_lake_featuretype_is_outside_area_population(self):
        _, features, captures, schema, windows = _fixture()
        name = _window_name([10, 11])
        page = json.loads(captures[name][0])
        page["features"][0]["attributes"]["featuretype"] = 3
        page["features"][0]["attributes"]["featuretypelabel"] = "Lake"
        captures[name] = (json.dumps(page, separators=(",", ":")).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "source_featuretype_outside_area_population"):
            self._validated(features, captures, schema, windows)

    def _refusal_fixture(self):
        """Fixture with roster id 13 persistently refused by the producer.

        Id 13 has no accepted page slice; its attributes arrive through the
        working attributes-only channel (refused_attrs_0000013), exactly as the
        producer serves refused records.
        """
        fields, features, captures, schema, windows = _fixture()
        name12 = _window_name([12, 13])
        page = json.loads(captures[name12][0])
        page["features"] = [page["features"][0]]
        url = source._window_url([12])
        captures[name12] = (json.dumps(page, separators=(",", ":"), ensure_ascii=False).encode(),
                            {"status": 200, "url": url, "final_url": url})
        attrs_url = source._query({"objectIds": "13", "outFields": "*", "returnGeometry": "false",
                                   "f": "json"})
        captures["refused_attrs_0000013"] = (
            json.dumps({"features": [{"attributes": _attrs(13, id3dhp="NUL0013")}]},
                       separators=(",", ":"), ensure_ascii=False).encode(),
            {"status": 200, "url": attrs_url, "final_url": attrs_url})
        return fields, features, captures, schema, windows

    def _refusal_pins(self, features, schema):
        attrs_10, attrs_11, attrs_12, attrs_14, attrs_13 = (
            _attrs(10), _attrs(11), _attrs(12, featuretype=2, featuretypelabel="Canal",
                                           id3dhp="CD00012"),
            _attrs(14, featuretype=4, featuretypelabel="Ocean or Great Lake", id3dhp="OC00014"),
            _attrs(13, id3dhp="NUL0013"))
        ordered = [attrs_10, attrs_11, attrs_12, attrs_14, attrs_13]
        geometry_sha = hashlib.sha256()
        geometry_sha.update(_canonical([[10, [RING_A]], [11, [RING_A, HOLE_A]]]))
        geometry_sha.update(_canonical([[12, [RING_A, RING_B, HOLE_B]]]))
        geometry_sha.update(_canonical([[14, [RING_B]]]))
        geometry_sha.update(_canonical([[13, None]]))
        return dict(EXPECTED_COUNT=5, EXPECTED_NULL_GEOMETRY_COUNT=1,
                    EXPECTED_REFUSED_IDS=frozenset({13}),
                    EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14])).hexdigest(),
                    EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(ordered)).hexdigest(),
                    EXPECTED_GEOMETRY_SHA256=geometry_sha.hexdigest(),
                    EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                    EXPECTED_FEATURETYPE_DISTRIBUTION={1: 2, 2: 1, 4: 1},
                    EXPECTED_SERVICE_LAYERS=[(60, "Waterbody"), (50, "Flowline")],
                    EXPECTED_LAYER_EXTENT={"xmin": -19951755.68, "ymin": -1622985.37,
                                           "xmax": 20021551.75, "ymax": 11575301.96},
                    EXPECTED_OBSERVED_BOUNDS=_bounds(features))

    def test_refused_record_row_is_staged_with_gated_null_geometry(self):
        fields, features, captures, schema, windows = self._refusal_fixture()
        with patch.multiple(source, **self._refusal_pins(features, schema)):
            _, extracted = source._validate_captures(captures)
        audit = extracted["audit"]
        self.assertEqual(audit["source_feature_count"], 5)
        self.assertEqual(audit["source_refused_ids"], [13])
        self.assertEqual(audit["source_refused_count"], 1)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(extracted["ids"], [10, 11, 12, 14, 13])
        self.assertIsNone(extracted["wkbs"][4])
        self.assertEqual(extracted["attributes"][4]["OBJECTID"], 13)

    def test_unpinned_refused_record_fails_closed(self):
        fields, features, captures, schema, windows = self._refusal_fixture()
        pins = self._refusal_pins(features, schema)
        pins["EXPECTED_REFUSED_IDS"] = frozenset()
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_refused_ids_drift"):
                source._validate_captures(captures)

    def test_missing_refused_attrs_capture_fails_closed(self):
        fields, features, captures, schema, windows = self._refusal_fixture()
        del captures["refused_attrs_0000013"]
        with patch.multiple(source, **self._refusal_pins(features, schema)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_single_id_window_refusal_is_gated_with_evidence(self):
        error_body = (b'{"error":{"code":500,"message":"Error performing query operation",'
                      b'"details":[]}}')
        calls = []

        def fake_capture_get(client, url, capture_root, name, **kwargs):
            requested = source._window_url_ids(url)
            calls.append(requested)
            if 7 in requested:
                (capture_root / f"{name}.body").write_bytes(error_body)
                (capture_root / f"{name}.receipt.json").write_bytes(
                    _canonical({"url": url, "status": 200, "error_code": None}))
                return error_body, {"url": url, "status": 200}
            body = json.dumps({"features": [{"attributes": _attrs(oid)} for oid in requested],
                               "spatialReference": {"wkid": 4326}, "hasZ": True,
                               "exceededTransferLimit": False},
                              separators=(",", ":")).encode()
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(
                _canonical({"url": url, "status": 200, "error_code": None}))
            return body, {"url": url, "status": 200}

        with tempfile.TemporaryDirectory(prefix="nhd-area-refusal-") as temporary:
            with patch.object(source, "capture_get", fake_capture_get), \
                    patch.object(source, "PAGE_CAPTURE_BASE_DELAY_SECONDS", 0):
                pages, refused = source._capture_windows(None, [7, 8], Path(temporary), [0])
            self.assertEqual(refused, {"page_0000007": [7]})
            self.assertEqual(sorted(pages), ["page_0000008"])
            self.assertEqual(calls, [[7, 8], [7], [7], [7], [8]])
            preserved = sorted(p.name for p in Path(temporary).glob("page_0000007.attempt-*"))
            self.assertEqual(len(preserved), 6)
            self.assertTrue(all(name.endswith((".body", ".receipt.json")) for name in preserved))

    def test_refused_attrs_capture_rejects_wrong_identity(self):
        def fake_capture_doc(client, url, capture_root, name, budget):
            return (json.dumps({"features": [{"attributes": _attrs(99)}]},
                               separators=(",", ":")).encode(), {"url": url})

        with patch.object(source, "_capture_doc", fake_capture_doc):
            with self.assertRaisesRegex(ValueError, "source_refused_attrs_record_drift"):
                source._capture_refused_attrs(None, Path("."), [0], {}, {"page_0000007": [7]})

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, windows = _fixture()
        pins = _pins(features, schema, windows)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -158.7, "ymin": 67.8, "xmax": -158.1, "ymax": 68.2}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_capture_budget_is_enforced_before_requests(self):
        _, features, captures, schema, windows = _fixture()
        pins = _pins(features, schema, windows)
        with patch.multiple(source, **pins), patch.object(source, "MAX_CAPTURE_BYTES", 0), \
                tempfile.TemporaryDirectory(prefix="nhd-area-budget-") as temporary:
            from dagster_hifld.resources import StagingStorageResource
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                source.capture_nhd_area_large_scale(storage, version="vfixture",
                                                    capture_directory=temporary + "/captures")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.DATASET_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.DATASET_SLUG}.parquet", b"accepted")
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

    def test_tiny_capture_seal_catalog_then_postcatalog_reuse(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, windows = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = dict(fixture_captures[name][1])
            receipt.update({"method": "GET", "url": url, "final_url": url,
                            "observed_at": "2026-11-12T00:00:00+00:00",
                            "status": 200, "complete_body": True,
                            "wire_bytes": len(body), "wire_sha256": digest,
                            "bytes": len(body), "sha256": digest,
                            "content_encoding": None, "content_length": str(len(body)),
                            "error_code": None})
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="nhd-area-large-scale-e2e-") as temporary:
            root = __import__("pathlib").Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, windows)), \
                    patch.object(source, "MIN_FREE_BYTES", 0), \
                    patch.object(source, "PAGE_IDS", 2), \
                    patch.object(source, "capture_get", side_effect=fake_capture_get), \
                    patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nhd_area_large_scale(storage, version="vfixture",
                                                                capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 5)
                self.assertEqual(candidate["source_null_geometry_count"], 1)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                         candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 1)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nhd_area_large_scale(storage, version="vfixture",
                                                             capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
