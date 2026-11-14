"""Focused contract checks for the FEMA NFHL Cross-Sections source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import LineString, MultiLineString

from dagster_hifld.sources import nfhl_cross_sections as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
        {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
        {"name": "XS_LN_ID", "type": "esriFieldTypeString", "alias": "XS_LN_ID", "length": 32, "domain": None},
        {"name": "WTR_NM", "type": "esriFieldTypeString", "alias": "WTR_NM", "length": 100, "domain": None},
        {"name": "STREAM_STN", "type": "esriFieldTypeDouble", "alias": "STREAM_STN", "domain": None},
        {"name": "START_ID", "type": "esriFieldTypeString", "alias": "START_ID", "length": 32, "domain": None},
        {"name": "XS_LTR", "type": "esriFieldTypeString", "alias": "XS_LTR", "length": 12, "domain": None},
        {"name": "XS_LN_TYP", "type": "esriFieldTypeString", "alias": "XS_LN_TYP", "length": 24, "domain": None},
        {"name": "WSEL_REG", "type": "esriFieldTypeDouble", "alias": "WSEL_REG", "domain": None},
        {"name": "STRMBED_EL", "type": "esriFieldTypeDouble", "alias": "STRMBED_EL", "domain": None},
        {"name": "LEN_UNIT", "type": "esriFieldTypeString", "alias": "LEN_UNIT", "length": 16, "domain": None},
        {"name": "V_DATUM", "type": "esriFieldTypeString", "alias": "V_DATUM", "length": 17, "domain": None},
        {"name": "PROFXS_TXT", "type": "esriFieldTypeString", "alias": "PROFXS_TXT", "length": 80, "domain": None},
        {"name": "MODEL_ID", "type": "esriFieldTypeString", "alias": "MODEL_ID", "length": 107, "domain": None},
        {"name": "SEQ", "type": "esriFieldTypeSmallInteger", "alias": "SEQ", "domain": None},
        {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]
    layer = {"id": 14, "name": "Cross-Sections", "type": "Feature Layer", "geometryType": "esriGeometryPolyline",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 14, "name": "Cross-Sections", "type": "Feature Layer"}]}
    attrs = lambda oid, xs_id, ltr: {"OBJECTID": oid, "DFIRM_ID": "39105C", "VERSION_ID": "2.2.2.1",
                                     "XS_LN_ID": xs_id, "WTR_NM": "LITTLE HOCKING RIVER",
                                     "STREAM_STN": 121.4, "START_ID": "39105C_0",
                                     "XS_LTR": ltr, "XS_LN_TYP": "FPXS", "WSEL_REG": 188.9,
                                     "STRMBED_EL": 190.2, "LEN_UNIT": "FEET", "V_DATUM": "NAVD88",
                                     "PROFXS_TXT": "PROFILE BASE XS A", "MODEL_ID": "39105C_STUDY1_HYD",
                                     "SEQ": 1, "SOURCE_CIT": "39105C_STUDY1",
                                     "SHAPE.STLength()": 0.9172877846325435,
                                     "GFID": None,
                                     "GlobalID": "{9585E72B-E15E-4949-8B7A-6D7D29569B64}"}
    features = [
        {"attributes": attrs(10, "39105C_1", "A"),
         "geometry": {"paths": [[[-81.75447115537122, 39.18075764998008],
                                 [-81.9, 39.1], [-82.0, 39.0]]]}},
        {"attributes": attrs(11, "39105C_2", None), "geometry": None},
        {"attributes": attrs(12, "39105C_3", "B"),
         "geometry": {"paths": [[[-81.0, 38.0], ["NaN", "NaN"], [-81.2, 38.2]]]}},
        {"attributes": attrs(13, "39105C_4", "C"),
         "geometry": {"paths": [[[-83.0, 37.0], [-83.1, 37.1]],
                                [[-84.0, 36.0], [-84.2, 36.2]]]}},
    ]
    ids = [10, 11, 12, 13]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    attr_names = [f["name"] for f in fields if f["name"] != "SHAPE"]
    geometry_records, null_count, nan_count, bounds = source._classify_geometries(features, attr_names)
    crs = {"wkid": 4269, "latestWkid": 4269}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 4}, "count_after": {"count": 4},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    geom = geometry_records
    counts = (null_count, nan_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, nan_count = counts
    return dict(EXPECTED_COUNT=4, SEGMENT_START_RECORDS=2, EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_NAN_GEOMETRY_COUNT=nan_count,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(14, "Cross-Sections", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


class NfhlCrossSectionsTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        _, features, _, _, _, _, _ = _fixture()
        self.assertEqual(source._list_sha(features), source._sha(_canonical(features)))
        self.assertEqual(source._list_sha([]), source._sha(_canonical([])))

    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        for name, ids in (("ids_seed", [13, 10, 12, 11]), ("ids_before", [11, 12, 10, 13])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13])
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

    def test_native_nad83_fields_and_null_geometry_preserved_in_parquet(self):
        fields, features, captures, schema, geom, counts, bounds = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13])
        self.assertEqual(table.column("XS_LN_ID").to_pylist(),
                         ["39105C_1", "39105C_2", "39105C_3", "39105C_4"])
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("SEQ").type.__str__(), "int16")
        self.assertEqual(table.column("SEQ").to_pylist(), [1, 1, 1, 1])
        self.assertEqual(table.column("STREAM_STN").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE.STLength()").to_pylist(), [0.9172877846325435] * 4)
        self.assertEqual(table.column("XS_LTR").to_pylist(), ["A", None, "B", "C"])
        self.assertEqual(table.column("GFID").to_pylist(), [None] * 4)
        self.assertEqual(table.column("SHAPE").null_count, 2)
        shapes = table.column("SHAPE").to_pylist()
        self.assertIsInstance(wkb.loads(shapes[0]), LineString)
        self.assertIsNone(shapes[1])
        self.assertIsNone(shapes[2])
        multi = wkb.loads(shapes[3])
        self.assertIsInstance(multi, MultiLineString)
        self.assertEqual(len(multi.geoms), 2)
        self.assertEqual(wkb.loads(shapes[0]).coords[0], (-81.75447115537122, 39.18075764998008))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["LineString", "MultiLineString"])

    def test_curve_paths_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"] = {"curvePaths": [[[-81.0, 38.0], [-81.1, 38.1]]]}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
                source._validate_captures(captures)

    def test_degenerate_line_geometry_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][1]["geometry"] = {"paths": [[[-83.0, 37.0]]]}
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
                source._validate_captures(captures)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        final = json.loads(captures["page_000002_00002"][0])
        final["exceededTransferLimit"] = True
        captures["page_000002_00002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
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
        page["features"][0]["attributes"]["MODEL_ID"] = "Changed"
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
                          "geoparquet/nfhl-cross-sections.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-cross-sections.parquet", b"accepted")
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
        del captures["page_000002_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_segment_name_is_rejected(self):
        _, _, captures, schema, geom, counts, bounds = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000002_00002")
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
                       "2026-11-08T00:00:00+00:00", "status": 200, "complete_body": True,
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
                segments = source._capture_segments(None, root, 4)
            self.assertEqual(sorted(segments), ["page_000000_00002", "page_000002_00002"])
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
                    source._capture_segments(None, root, 4)

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
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
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-08T00:00:00+00:00",
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

        with tempfile.TemporaryDirectory(prefix="nfhl-cross-sections-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_cross_sections(storage, version="vfixture",
                                                                    capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 4)
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
                reused = source.capture_nfhl_cross_sections(storage, version="vfixture",
                                                                capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
