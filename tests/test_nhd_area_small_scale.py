"""Focused contract checks for the USGS NHD Area - Small Scale source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.sources import nhd_area_small_scale as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>USGS NHD and WBD Map Services Moving to the Cloud: new Cloud endpoint "
                    b"https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer</html>")
PRODUCT_DOC_BODY = (b"<html>Access National Hydrography Products: As of October 1, 2023, the NHD was "
                    b"retired. NHD data will continue to be available, but no longer maintained.</html>")

# Fixture rings use Web Mercator metre coordinates (the producer's served CRS).
SHELL_A = [[-11000000.0, 4500000.0], [-10900000.0, 4500000.0], [-10900000.0, 4400000.0],
           [-11000000.0, 4400000.0], [-11000000.0, 4500000.0]]
HOLE_A = [[-10990000.0, 4490000.0], [-10990000.0, 4450000.0], [-10950000.0, 4450000.0],
          [-10950000.0, 4490000.0], [-10990000.0, 4490000.0]]
SHELL_B = [[-11200000.0, 4400000.0], [-11100000.0, 4400000.0], [-11100000.0, 4300000.0],
           [-11200000.0, 4300000.0], [-11200000.0, 4400000.0]]
ISLAND_IN_HOLE = [[-10985000.0, 4480000.0], [-10985000.0, 4475000.0], [-10980000.0, 4475000.0],
                  [-10980000.0, 4480000.0], [-10985000.0, 4480000.0]]


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "Shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None},
        {"name": "FDATE", "type": "esriFieldTypeDate", "alias": "FDATE", "length": 8, "domain": None},
        {"name": "GNIS_ID", "type": "esriFieldTypeString", "alias": "GNIS_ID", "length": 10, "domain": None},
        {"name": "GNIS_NAME", "type": "esriFieldTypeString", "alias": "GNIS_NAME", "length": 65, "domain": None},
        {"name": "AREASQKM", "type": "esriFieldTypeDouble", "alias": "AREASQKM", "domain": None},
        {"name": "ELEVATION", "type": "esriFieldTypeDouble", "alias": "Elevation", "domain": None},
        {"name": "FCODE", "type": "esriFieldTypeInteger", "alias": "FCode", "domain": None},
        {"name": "Shape_Length", "type": "esriFieldTypeDouble", "alias": "Shape_Length", "domain": None},
        {"name": "Shape_Area", "type": "esriFieldTypeDouble", "alias": "Shape_Area", "domain": None},
        {"name": "COMID", "type": "esriFieldTypeInteger", "alias": "COMID", "domain": None},
        {"name": "RESOLUTION", "type": "esriFieldTypeString", "alias": "RESOLUTION", "length": 7, "domain": None},
        {"name": "FTYPE", "type": "esriFieldTypeString", "alias": "FTYPE", "length": 24, "domain": None},
        {"name": "ONOFFNET", "type": "esriFieldTypeSmallInteger", "alias": "ONOFFNET", "domain": None},
        {"name": "PurpCode", "type": "esriFieldTypeString", "alias": "PurpCode", "length": 2, "domain": None},
        {"name": "PurpDesc", "type": "esriFieldTypeString", "alias": "PurpDesc", "length": 254, "domain": None},
    ]


def _attributes(oid, comid):
    return {"OBJECTID": oid, "FDATE": 930009600000, "GNIS_ID": " ", "GNIS_NAME": " ",
            "AREASQKM": 0.133, "ELEVATION": 0, "FCODE": 43100,
            "Shape_Length": 3240.0625731618834, "Shape_Area": 284596.3866417001,
            "COMID": comid, "RESOLUTION": "Medium", "FTYPE": "Rapids",
            "ONOFFNET": None, "PurpCode": None, "PurpDesc": None}


def _fixture():
    fields = _fields()
    layer = {"id": 7, "name": "Area - Small Scale", "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 102100, "latestWkid": 3857},
             "extent": {"xmin": -11050000.0, "ymin": 4250000.0, "xmax": -10850000.0, "ymax": 4550000.0,
                        "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
             "fields": fields}
    service = {"mapName": "nhd",
               "layers": [{"id": 7, "name": "Area - Small Scale", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attributes(10, 4289001), "geometry": {"rings": [SHELL_A]}},
        {"attributes": _attributes(11, 4289005), "geometry": None},
        {"attributes": _attributes(12, 4289011),
         "geometry": {"rings": [[[-11000000.0, 4500000.0], ["NaN", "NaN"], [-10900000.0, 4500000.0],
                                 [-10900000.0, 4400000.0], [-11000000.0, 4500000.0]]]}},
        {"attributes": _attributes(13, 4289019), "geometry": {"rings": [SHELL_A, HOLE_A]}},
        {"attributes": _attributes(14, 4289027), "geometry": {"rings": [SHELL_B, SHELL_A]}},
        {"attributes": _attributes(15, 4289035), "geometry": {"rings": [SHELL_B]}},
    ]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    attr_names = [f["name"] for f in fields if f["name"] != "Shape"]
    geometry_records, null_count, nan_count, bounds = source._classify_geometries(features, attr_names)
    crs = {"wkid": 102100, "latestWkid": 3857}
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
    captures["product_doc"] = (PRODUCT_DOC_BODY, {"status": 200})
    geom = geometry_records
    counts = (null_count, nan_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, nan_count = counts
    attrs = {key: [row["attributes"][key] for row in features] for key in
             ("RESOLUTION", "FTYPE", "FCODE", "ONOFFNET", "PurpCode")}
    comids = [row["attributes"]["COMID"] for row in features]
    gnis_lengths = sorted({len(row["attributes"]["GNIS_ID"]) if row["attributes"]["GNIS_ID"] is not None else -1
                           for row in features})
    return dict(EXPECTED_COUNT=6, SEGMENT_START_RECORDS=2, EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_NAN_GEOMETRY_COUNT=nan_count,
                EXPECTED_UNASSEMBLED_COUNT=len(source._unassembled_object_ids(
                    sorted(features, key=lambda row: row["attributes"]["OBJECTID"]))),
                EXPECTED_UNASSEMBLED_ID_SAMPLE=source._unassembled_object_ids(
                    sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(7, "Area - Small Scale", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -11050000.0, "ymin": 4250000.0, "xmax": -10850000.0, "ymax": 4550000.0},
                EXPECTED_OBSERVED_BOUNDS=bounds,
                EXPECTED_RESOLUTION_VALUES=sorted(set(attrs["RESOLUTION"])),
                EXPECTED_FTYPE_VALUES=sorted(set(attrs["FTYPE"])),
                EXPECTED_FCODE_VALUES=sorted(set(attrs["FCODE"])),
                EXPECTED_ONOFFNET_VALUES=sorted(set(attrs["ONOFFNET"]), key=lambda value: (value is None, value)),
                EXPECTED_PURPCODE_VALUES=sorted(set(attrs["PurpCode"]), key=lambda value: (value is None, value)),
                EXPECTED_GNIS_ID_LENGTHS=gnis_lengths,
                EXPECTED_COMID_DUPLICATE_COUNT=len(comids) - len(set(comids)))


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


class NhdAreaSmallScaleTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        _, features, _, _, _, _, _ = _fixture()
        self.assertEqual(source._list_sha(features), source._sha(_canonical(features)))
        self.assertEqual(source._list_sha([]), source._sha(_canonical([])))

    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        for name, ids in (("ids_seed", [15, 10, 12, 11, 13, 14]), ("ids_before", [11, 12, 10, 13, 14, 15])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14, 15])
        self.assertEqual(extracted["audit"]["null_geometry_count"], counts[0])
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], counts[1])

    def test_missing_service_documentation_link_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_service_documentation_link_drift"):
                source._validate_captures(captures)

    def test_retirement_documentation_drift_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        captures["product_doc"] = (b"<html>the NHD continues</html>", {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "usgs_product_retirement_documentation_drift"):
                source._validate_captures(captures)

    def test_crs_drift_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_native_mercator_fields_dates_and_null_geometry_preserved_in_parquet(self):
        fields, features, captures, schema, geom, counts, bounds = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(table.column("GNIS_ID").to_pylist(), [" "] * 6)
        self.assertEqual(table.column("GNIS_NAME").to_pylist(), [" "] * 6)
        self.assertEqual(table.column("RESOLUTION").to_pylist(), ["Medium"] * 6)
        self.assertEqual(table.column("FTYPE").to_pylist(), ["Rapids"] * 6)
        self.assertEqual(table.column("COMID").type.__str__(), "int64")
        self.assertEqual(table.column("COMID").to_pylist(), [4289001, 4289005, 4289011, 4289019, 4289027, 4289035])
        self.assertEqual(table.column("FCODE").type.__str__(), "int64")
        self.assertEqual(table.column("ONOFFNET").type.__str__(), "int32")
        self.assertEqual(table.column("ONOFFNET").to_pylist(), [None] * 6)
        self.assertEqual(table.column("PurpCode").to_pylist(), [None] * 6)
        self.assertEqual(table.column("FDATE").type.__str__(), "int64")
        self.assertEqual(table.column("FDATE").to_pylist(), [930009600000] * 6)
        self.assertEqual(table.column("AREASQKM").type.__str__(), "double")
        self.assertEqual(table.column("Shape_Length").type.__str__(), "double")
        self.assertEqual(table.column("Shape").null_count, 2)
        shapes = table.column("Shape").to_pylist()
        self.assertIsNone(shapes[1])
        self.assertIsNone(shapes[2])
        simple = wkb.loads(shapes[0])
        self.assertIsInstance(simple, Polygon)
        self.assertEqual(len(simple.exterior.coords), len(SHELL_A))
        with_hole = wkb.loads(shapes[3])
        self.assertIsInstance(with_hole, Polygon)
        self.assertEqual(len(with_hole.interiors), 1)
        self.assertEqual(len(with_hole.interiors[0].coords), len(HOLE_A))
        multi = wkb.loads(shapes[4])
        self.assertIsInstance(multi, MultiPolygon)
        self.assertEqual(len(multi.geoms), 2)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["Shape"]["crs"]["id"]["code"], 3857)
        self.assertEqual(geo["columns"]["Shape"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_nan_string_vertex_geometry_is_gated_not_coerced(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)
        nan_feature = next(row for row in extracted["features"] if row["attributes"]["OBJECTID"] == 12)
        self.assertEqual(nan_feature["geometry"]["rings"][0][1], ["NaN", "NaN"])
        self.assertIsNone(source._polygon_wkb(nan_feature["geometry"]))

    def test_curve_rings_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"] = {"curveRings": [[[-11000000.0, 4500000.0], [-11010000.0, 4501000.0]]]}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                source._validate_captures(captures)

    def test_degenerate_ring_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["geometry"] = {"rings": [[[-11000000.0, 4500000.0], [-10900000.0, 4500000.0]]]}
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                source._validate_captures(captures)

    def test_island_in_hole_nesting_is_rejected(self):
        rings = [SHELL_A, HOLE_A, ISLAND_IN_HOLE]
        with self.assertRaisesRegex(ValueError, "source_polygon_island_in_hole_unsupported"):
            source._assemble_polygons(rings)

    def test_self_intersecting_ring_is_gated_not_repaired(self):
        self_touching = [[-11000000.0, 4500000.0], [-10900000.0, 4400000.0], [-10900000.0, 4500000.0],
                         [-11000000.0, 4400000.0], [-11000000.0, 4500000.0]]
        feature = {"attributes": {"OBJECTID": 99}, "geometry": {"rings": [self_touching]}}
        self.assertIsNone(source._polygon_wkb(feature["geometry"]))
        self.assertEqual(source._unassembled_object_ids([feature]), [99])

    def test_ring_hole_and_multipart_containers_are_assembled(self):
        single = source._assemble_polygons([SHELL_A])
        self.assertEqual(len(single), 1)
        with_hole = source._assemble_polygons([SHELL_A, HOLE_A])[0]
        self.assertEqual(len(with_hole.interiors), 1)
        multi = source._assemble_polygons([SHELL_B, SHELL_A])
        self.assertEqual(len(multi), 2)
        self.assertEqual(len(multi[0].interiors), 0)
        self.assertEqual(len(multi[1].interiors), 0)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        final = json.loads(captures["page_000004_00002"][0])
        final["exceededTransferLimit"] = True
        captures["page_000004_00002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_value_contract_drift_is_rejected(self):
        _, pristine, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["RESOLUTION"] = "High"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        pins = _pins(pristine, schema, geom, counts, bounds)
        pins["EXPECTED_RECORD_SHA256"] = source._list_sha(
            sorted(features, key=lambda row: row["attributes"]["OBJECTID"]))
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_resolution_values_drift"):
                source._validate_captures(captures)

    def test_comid_uniqueness_drift_is_rejected(self):
        _, pristine, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["COMID"] = 4289005
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        pins = _pins(pristine, schema, geom, counts, bounds)
        pins["EXPECTED_RECORD_SHA256"] = source._list_sha(
            sorted(features, key=lambda row: row["attributes"]["OBJECTID"]))
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_comid_uniqueness_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
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
        page["features"][0]["attributes"]["GNIS_NAME"] = "Changed"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -11300000.0, "ymin": 4310000.0,
                                            "xmax": -10890000.0, "ymax": 4510000.0}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_null_geometry_count_drift_is_detected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_NULL_GEOMETRY_COUNT"] = counts[0] + 1
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_null_or_nan_geometry_count_drift"):
                source._validate_captures(captures)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os
        import tempfile

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/nhd-area-small-scale.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nhd-area-small-scale.parquet", b"accepted")
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

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        import tempfile
        from pathlib import Path

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
        import tempfile
        from pathlib import Path

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

    def test_incomplete_transport_capture_is_retried_once_bounded(self):
        import tempfile
        from pathlib import Path

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

    def test_page_tiling_gap_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        del captures["page_000004_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_segment_name_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000004_00002")
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_adaptive_bisection_splits_producer_cap_failures(self):
        import tempfile
        from pathlib import Path

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

    def test_unservable_single_feature_fails_explicitly(self):
        import tempfile
        from pathlib import Path

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

    def _seal_catalog_cache_flow(self):
        import tempfile
        from pathlib import Path

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

        with tempfile.TemporaryDirectory(prefix="nhd-area-small-scale-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nhd_area_small_scale(storage, version="vfixture",
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
                reused = source.capture_nhd_area_small_scale(storage, version="vfixture",
                                                             capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        import shutil
        import tempfile
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="nhd-area-small-scale-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/nhd_area_small_scale.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()
