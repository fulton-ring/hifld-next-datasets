"""Focused contract checks for the FEMA NFHL Transect Baselines source adapter."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_transect_baselines as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

# Verbatim producer paths (Esri JSON polyline semantics; not closed, no orientation rule).
PATH_A = [[-87.93731593322622, 30.482759910130085], [-87.92383713101225, 30.487041054361214],
          [-87.91, 30.49]]
PATH_B = [[-88.02, 30.31], [-88.01, 30.32], [-88.00, 30.33]]
PATH_C = [[-89.5, 30.2], [-89.4, 30.21]]


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
        {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
        {"name": "TBASELN_ID", "type": "esriFieldTypeString", "alias": "TBASELN_ID", "length": 32, "domain": None},
        {"name": "CST_MDL_ID", "type": "esriFieldTypeString", "alias": "CST_MDL_ID", "length": 32, "domain": None},
        {"name": "TBASE_TYP", "type": "esriFieldTypeString", "alias": "TBASE_TYP", "length": 43, "domain": None},
        {"name": "R_ST_DESC", "type": "esriFieldTypeString", "alias": "R_ST_DESC", "length": 254, "domain": None},
        {"name": "R_END_DESC", "type": "esriFieldTypeString", "alias": "R_END_DESC", "length": 254, "domain": None},
        {"name": "V_DATUM", "type": "esriFieldTypeString", "alias": "V_DATUM", "length": 17, "domain": None},
        {"name": "WTR_NM", "type": "esriFieldTypeString", "alias": "WTR_NM", "length": 100, "domain": None},
        {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]


def _attributes(oid, tbase_typ="Ridge"):
    return {"OBJECTID": oid, "DFIRM_ID": "01003C", "VERSION_ID": "1.1.1.0", "TBASELN_ID": f"01003C_{oid}",
            "CST_MDL_ID": None, "TBASE_TYP": tbase_typ, "R_ST_DESC": None, "R_END_DESC": None,
            "V_DATUM": "NAVD88", "WTR_NM": "NP", "SOURCE_CIT": "01003C_STUDY1",
            "SHAPE.STLength()": 0.014142358539189987,
            "GFID": "2daa8f96-8523-4ed3-bdef-653fac6517df",
            "GlobalID": "{6204CBF8-F540-441E-B745-BA1BC5DBA138}"}


def _fixture():
    fields = _fields()
    layer = {"id": 18, "name": "Transect Baselines", "type": "Feature Layer",
             "geometryType": "esriGeometryPolyline",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 17.0, "xmax": -65.0, "ymax": 62.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 18, "name": "Transect Baselines", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attributes(10), "geometry": {"paths": [PATH_A]}},
        {"attributes": _attributes(11, "Ridge/Valley"), "geometry": None},
        {"attributes": _attributes(12), "geometry": {"paths": [[["NaN", "NaN"], ["NaN", "NaN"]]]}},
        {"attributes": _attributes(13), "geometry": {"paths": [PATH_B, PATH_C]}},
        {"attributes": _attributes(14), "geometry": {"paths": [PATH_B, PATH_C, PATH_A]}},
        {"attributes": _attributes(15, "Coastal"), "geometry": {"paths": [PATH_C]}},
    ]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    attr_names = [f["name"] for f in fields if f["name"] != "SHAPE"]
    geometry_records, null_count, nan_count, bounds = source._classify_geometries(features, attr_names)
    crs = {"wkid": 4269, "latestWkid": 4269}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:4], "spatialReference": crs}
    page2 = {"features": features[4:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1, "page_000004_00002": page2}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    geom = geometry_records
    counts = (null_count, nan_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, nan_count = counts
    return dict(EXPECTED_COUNT=6, SEGMENT_START_RECORDS=2, EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_NAN_GEOMETRY_COUNT=nan_count,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=source._list_sha(geom),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(18, "Transect Baselines", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 17.0, "xmax": -65.0, "ymax": 62.0},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


class NfhlTransectBaselinesTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), hashlib.sha256(_canonical(items)).hexdigest())

    def test_native_paths_and_null_geometry_preserved_in_parquet(self):
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
        self.assertEqual(table.column("TBASELN_ID").to_pylist(),
                         ["01003C_10", "01003C_11", "01003C_12", "01003C_13", "01003C_14", "01003C_15"])
        self.assertEqual(table.column("TBASE_TYP").to_pylist(),
                         ["Ridge", "Ridge/Valley", "Ridge", "Ridge", "Ridge", "Coastal"])
        self.assertEqual(table.column("CST_MDL_ID").to_pylist(), [None] * 6)
        self.assertEqual(table.column("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE.STLength()").to_pylist()[0], 0.014142358539189987)
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertIsNone(table.column("R_ST_DESC").to_pylist()[0])
        line_10 = shapely.from_wkb(table.column("SHAPE").to_pylist()[0])
        self.assertIsInstance(line_10, shapely.LineString)
        self.assertEqual([list(coordinate) for coordinate in line_10.coords], PATH_A)
        self.assertIsNone(table.column("SHAPE").to_pylist()[1])
        self.assertIsNone(table.column("SHAPE").to_pylist()[2])
        multi_13 = shapely.from_wkb(table.column("SHAPE").to_pylist()[3])
        self.assertIsInstance(multi_13, shapely.MultiLineString)
        self.assertEqual([[list(coordinate) for coordinate in line.coords] for line in multi_13.geoms],
                         [PATH_B, PATH_C])
        multi_14 = shapely.from_wkb(table.column("SHAPE").to_pylist()[4])
        self.assertIsInstance(multi_14, shapely.MultiLineString)
        self.assertEqual([[list(coordinate) for coordinate in line.coords] for line in multi_14.geoms],
                         [PATH_B, PATH_C, PATH_A])
        line_15 = shapely.from_wkb(table.column("SHAPE").to_pylist()[5])
        self.assertIsInstance(line_15, shapely.LineString)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["LineString", "MultiLineString"])

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
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._validate_captures(captures)

    def test_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPoint"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "exact_nfhl_transect_baselines_layer_metadata_drift"):
                source._validate_captures(captures)

    def test_final_segment_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        final = json.loads(captures["page_000004_00002"][0])
        final["exceededTransferLimit"] = True
        captures["page_000004_00002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
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

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["WTR_NM"] = "Changed"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_vertex_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["paths"][0][1] = [-87.924, 30.487]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_coordinate_range_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["paths"][0][0] = [-200.0, 30.48]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -87.94, "ymin": 30.48, "xmax": -87.92, "ymax": 30.49}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_page_tiling_gap_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        del captures["page_000004_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_segment_name_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000004_00002")
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
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
                       "2026-11-12T00:00:00+00:00", "status": 200, "complete_body": True,
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
            with patch.multiple(source, SEGMENT_START_RECORDS=4,
                                SEGMENT_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                segments = source._capture_segments(None, root, 6)
            self.assertEqual(sorted(segments), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
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
                       "2026-11-12T00:00:00+00:00", "status": 200, "complete_body": True,
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
            with patch.multiple(source, SEGMENT_START_RECORDS=4,
                                SEGMENT_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                segments = source._capture_segments(None, root, 6)
            self.assertEqual(sorted(segments), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
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
            with patch.multiple(source, SEGMENT_START_RECORDS=4,
                                SEGMENT_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=2), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_segments(None, root, 6)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/nfhl-transect-baselines.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-transect-baselines.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            import os
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
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-12T00:00:00+00:00",
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

        with tempfile.TemporaryDirectory(prefix="nfhl-transect-baselines-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_transect_baselines(storage, version="vfixture",
                                                                   capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_nan_string_geometry_count"], counts[1])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 2)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_transect_baselines(storage, version="vfixture",
                                                                capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="nfhl-transect-baselines-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/nfhl_transect_baselines.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()


if __name__ == "__main__":
    unittest.main()
