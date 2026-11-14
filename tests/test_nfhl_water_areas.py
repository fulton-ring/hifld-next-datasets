"""Focused contract checks for the FEMA NFHL Water Areas source adapter."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.sources import nfhl_water_areas as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

SHELL_A = [[-81.0, 38.0], [-80.0, 38.0], [-80.0, 37.0], [-81.0, 37.0], [-81.0, 38.0]]
HOLE_A = [[-80.9, 37.9], [-80.9, 37.5], [-80.5, 37.5], [-80.5, 37.9], [-80.9, 37.9]]
SHELL_B = [[-83.0, 37.0], [-82.0, 37.0], [-82.0, 36.0], [-83.0, 36.0], [-83.0, 37.0]]
ISLAND_IN_HOLE = [[-80.8, 37.7], [-80.8, 37.6], [-80.7, 37.6], [-80.7, 37.7], [-80.8, 37.7]]


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
        {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
        {"name": "WTR_AR_ID", "type": "esriFieldTypeString", "alias": "WTR_AR_ID", "length": 32, "domain": None},
        {"name": "WTR_NM", "type": "esriFieldTypeString", "alias": "WTR_NM", "length": 100, "domain": None},
        {"name": "SHOWN_FIRM", "type": "esriFieldTypeString", "alias": "SHOWN_FIRM", "length": 1, "domain": None},
        {"name": "SHOWN_INDX", "type": "esriFieldTypeString", "alias": "SHOWN_INDX", "length": 1, "domain": None},
        {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "SHAPE.STArea()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STArea()", "domain": None},
        {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]


def _attributes(oid, wtr_ar_id, wtr_nm="SHADY GROVE"):
    return {"OBJECTID": oid, "DFIRM_ID": "01003C", "VERSION_ID": "1.1.1.0", "WTR_AR_ID": wtr_ar_id,
            "WTR_NM": wtr_nm, "SHOWN_FIRM": "U", "SHOWN_INDX": "U", "SOURCE_CIT": "01031C_BASE5",
            "GFID": "a427acb0-7358-4933-86c1-c9f1ddf77a37",
            "SHAPE.STArea()": 3.427640014972714e-07, "SHAPE.STLength()": 0.0025747610949458195,
            "GlobalID": "{74D8D7FC-85E4-4187-A6AB-3F5463F94000}"}


def _fixture():
    fields = _fields()
    layer = {"id": 32, "name": "Water Areas", "type": "Feature Layer", "geometryType": "esriGeometryPolygon",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -170.0, "ymin": -14.0, "xmax": 146.0, "ymax": 67.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 32, "name": "Water Areas", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attributes(10, "01003C_121"), "geometry": {"rings": [SHELL_A]}},
        {"attributes": _attributes(11, "01003C_122"), "geometry": None},
        {"attributes": _attributes(12, "01003C_123"),
         "geometry": {"rings": [[[ -81.0, 38.0], ["NaN", "NaN"], [-80.0, 38.0], [-80.0, 37.0], [-81.0, 38.0]]]}},
        {"attributes": _attributes(13, "01003C_124"), "geometry": {"rings": [SHELL_A, HOLE_A]}},
        {"attributes": _attributes(14, "01003C_125"), "geometry": {"rings": [SHELL_B, SHELL_A]}},
        {"attributes": _attributes(15, "01003C_126"), "geometry": {"rings": [SHELL_B]}},
    ]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    attr_names = [f["name"] for f in fields if f["name"] != "SHAPE"]
    geometry_records, null_count, nan_count, unplacable_count, empty_count, bounds = source._classify_geometries(features, attr_names)
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
    counts = (null_count, nan_count, unplacable_count, empty_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, nan_count, unplacable_count, empty_count = counts
    return dict(EXPECTED_COUNT=6, SEGMENT_START_RECORDS=2, EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_NAN_GEOMETRY_COUNT=nan_count,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(32, "Water Areas", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -170.0, "ymin": -14.0, "xmax": 146.0, "ymax": 67.0},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


class NfhlWaterAreasTests(unittest.TestCase):
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
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
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

    def test_native_nad83_fields_dates_and_null_geometry_preserved_in_parquet(self):
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
        self.assertEqual(table.column("WTR_AR_ID").to_pylist(),
                         ["01003C_121", "01003C_122", "01003C_123", "01003C_124", "01003C_125",
                          "01003C_126"])
        self.assertEqual(table.column("WTR_NM").to_pylist(), ["SHADY GROVE"] * 6)
        self.assertEqual(table.column("SHOWN_FIRM").to_pylist(), ["U"] * 6)
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("SHAPE.STArea()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        shapes = table.column("SHAPE").to_pylist()
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
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_nan_string_vertex_geometry_is_gated_not_coerced(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)
        nan_feature = next(row for row in extracted["features"] if row["attributes"]["OBJECTID"] == 12)
        self.assertEqual(nan_feature["geometry"]["rings"][0][1], ["NaN", "NaN"])
        self.assertIsNone(source._polygon_wkb(nan_feature["geometry"]))

    def test_out_of_range_geometry_is_gated_not_coerced(self):
        out_of_domain = [[220.0, 40.0], [221.0, 40.0], [221.0, 39.0], [220.0, 39.0], [220.0, 40.0]]
        feature = {"attributes": {"OBJECTID": 98}, "geometry": {"rings": [out_of_domain]}}
        attr_names = ["OBJECTID"]
        records, null_count, nan_count, unplacable_count, empty_count, bounds = source._classify_geometries(
            [feature], attr_names)
        self.assertEqual((null_count, nan_count, unplacable_count, empty_count), (0, 0, 1, 0))
        self.assertEqual(records, [[98, [out_of_domain]]])
        self.assertIsNone(bounds)
        self.assertIsNone(source._polygon_wkb(feature["geometry"]))
        self.assertEqual(source._unplacable_object_ids([feature]), [98])
        self.assertEqual(source._unassembled_object_ids([feature]), [])
        mixed = [[-80.0, 37.0], [220.0, 38.0], [-80.0, 38.0], [-80.0, 37.0]]
        mixed_feature = {"attributes": {"OBJECTID": 97}, "geometry": {"rings": [mixed]}}
        self.assertIsNone(source._polygon_wkb(mixed_feature["geometry"]))
        records2, _, _, unplacable2, _, _ = source._classify_geometries(
            [{"attributes": {"OBJECTID": 97}, "geometry": {"rings": [mixed]}}], attr_names)
        self.assertEqual(unplacable2, 1)
        self.assertEqual(records2[0], [97, [mixed]])

    def test_empty_rings_geometry_is_gated_not_coerced(self):
        feature = {"attributes": {"OBJECTID": 96}, "geometry": {"rings": []}}
        attr_names = ["OBJECTID"]
        records, null_count, nan_count, unplacable_count, empty_count, bounds = \
            source._classify_geometries([feature], attr_names)
        self.assertEqual((null_count, nan_count, unplacable_count, empty_count), (0, 0, 0, 1))
        self.assertEqual(records, [[96, []]])
        self.assertIsNone(source._polygon_wkb(feature["geometry"]))
        self.assertEqual(source._empty_object_ids([feature]), [96])
        self.assertEqual(source._unplacable_object_ids([feature]), [])
        self.assertEqual(source._unassembled_object_ids([feature]), [])

    def test_curve_rings_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"] = {"curveRings": [[[-81.0, 38.0], [-81.1, 38.1]]]}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                source._validate_captures(captures)

    def test_degenerate_ring_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["geometry"] = {"rings": [[[-81.0, 38.0], [-80.0, 38.0]]]}
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
        self_touching = [[-81.0, 38.0], [-80.0, 37.0], [-80.0, 38.0], [-81.0, 37.0], [-81.0, 38.0]]
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
        page["features"][0]["attributes"]["WTR_NM"] = "Changed"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -84.2, "ymin": 36.0, "xmax": -81.0, "ymax": 39.2}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
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
                          "geoparquet/nfhl-water-areas.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-water-areas.parquet", b"accepted")
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

        with tempfile.TemporaryDirectory(prefix="nfhl-water-areas-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_water_areas(storage, version="vfixture",
                                                            capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_nan_string_geometry_count"], counts[1])
                self.assertEqual(candidate["source_unplacable_geometry_count"], counts[2])
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
                reused = source.capture_nfhl_water_areas(storage, version="vfixture",
                                                         capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="nfhl-water-areas-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/nfhl_water_areas.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()
