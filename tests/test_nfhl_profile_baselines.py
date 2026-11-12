"""Focused contract checks for the FEMA NFHL Profile Baselines source adapter."""
from __future__ import annotations

import hashlib
import json
import struct
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely
from shapely.geometry import LineString, MultiLineString

from dagster_hifld.sources import nfhl_profile_baselines as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _wkb_vertex(x, y, z, m):
    return struct.pack("<dddd", x, y, z, m)


def _wkb_geometry(paths, type_code, point_count):
    body = bytearray(b"\x01")
    body += struct.pack("<II", type_code, len(paths))
    for path in paths:
        body += struct.pack("<I", len(path))
        for vertex in path:
            body += _wkb_vertex(*vertex)
    return bytes(body), point_count


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
        {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
        {"name": "BASELN_ID", "type": "esriFieldTypeString", "alias": "BASELN_ID", "length": 32, "domain": None},
        {"name": "WTR_NM", "type": "esriFieldTypeString", "alias": "WTR_NM", "length": 100, "domain": None},
        {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]
    layer = {"id": 17, "name": "Profile Baselines", "type": "Feature Layer",
             "geometryType": "esriGeometryPolyline", "hasZ": True, "hasM": True,
             "sourceHeightModelInfo": {"heightModel": "gravity_related_height",
                                       "vertCRS": "NAVD88_height_(ftUS)", "heightUnit": "us-foot"},
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -170.0, "ymin": -14.0, "xmax": 145.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 17, "name": "Profile Baselines", "type": "Feature Layer"}]}
    attrs = lambda oid: {"OBJECTID": oid, "DFIRM_ID": "01049C", "VERSION_ID": "1.1.1.0",
                         "BASELN_ID": f"01049C_{oid}", "WTR_NM": "PHILLIPS BRANCH",
                         "SOURCE_CIT": "01049C_BASE5", "SHAPE.STLength()": 0.036451028277083714,
                         "GFID": "20110929", "GlobalID": "{D47ED623-7299-44AD-9535-513214E5DC7C}"}
    features = [
        {"attributes": attrs(10),
         "geometry": {"hasZ": True, "hasM": True,
                      "paths": [[[-85.89851338304294, 34.51201594594227, 0, None],
                                 [-85.89844733361423, 34.511926091897124, 0, None]]]}},
        {"attributes": attrs(11), "geometry": None},
        {"attributes": attrs(12),
         "geometry": {"hasZ": True, "hasM": True,
                      "paths": [[[-87.0, 33.0, "NaN", 0.0], [-87.2, 33.2, 1033.19, "NaN"]],
                                [[-88.0, 32.0, 0, None], [-89.0, 31.0, 0, None]]]}},
        {"attributes": attrs(13),
         "geometry": {"paths": [[[-90.0, 30.0, 0, None], [-90.1, 30.1, 0, None]]]}},
    ]
    ids = [10, 11, 12, 13]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    attr_names = [f["name"] for f in fields if f["name"] != "SHAPE"]
    stats = source._StreamStats()
    for feature in sorted(features, key=lambda row: row["attributes"]["OBJECTID"]):
        stats.add(feature, attr_names)
    crs = {"wkid": 4269, "latestWkid": 4269}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 4}, "count_after": {"count": 4},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1}
    return fields, features, docs, schema, stats, attr_names, ids


def _docs_to_bodies(docs):
    bodies = {name: json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()
              for name, doc in docs.items()}
    bodies["service_doc"] = SERVICE_DOC_BODY
    return bodies


def _pins(docs, schema, stats, ids):
    null_count = stats.null_geometry_count
    return dict(EXPECTED_COUNT=4, SEGMENT_START_RECORDS=2, MIN_FREE_BYTES=0,
                EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_NULL_GEOMETRY_ID_SHA256=hashlib.sha256(
                    _canonical(sorted(stats.null_geometry_ids))).hexdigest(),
                EXPECTED_NAN_GEOMETRY_COUNT=stats.nan_geometry_count,
                EXPECTED_NULL_Z_VERTEX_COUNT=stats.null_z_vertex_count,
                EXPECTED_NAN_Z_VERTEX_COUNT=stats.nan_z_vertex_count,
                EXPECTED_NULL_M_VERTEX_COUNT=stats.null_m_vertex_count,
                EXPECTED_NAN_M_VERTEX_COUNT=stats.nan_m_vertex_count,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical(sorted(ids))).hexdigest(),
                EXPECTED_RECORD_SHA256=stats.record_sha256,
                EXPECTED_GEOMETRY_SHA256=stats.geometry_sha256,
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(17, "Profile Baselines", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -170.0, "ymin": -14.0, "xmax": 145.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS=stats.observed_bounds,
                EXPECTED_GEOMETRY_TYPES=sorted(stats.geometry_types))


class NfhlProfileBaselinesTests(unittest.TestCase):
    def test_streamed_digests_match_whole_list_digests(self):
        fields, features, _, _, stats, attr_names, _ = _fixture()
        sorted_features = sorted(features, key=lambda row: row["attributes"]["OBJECTID"])
        self.assertEqual(stats.record_sha256, hashlib.sha256(_canonical(sorted_features)).hexdigest())
        geometry_records = []
        for feature in sorted_features:
            geom = feature["geometry"]
            geometry_records.append([feature["attributes"]["OBJECTID"],
                                     None if geom is None else geom["paths"]])
        self.assertEqual(stats.geometry_sha256, hashlib.sha256(_canonical(geometry_records)).hexdigest())
        self.assertEqual(stats.feature_count, 4)

    def test_streamed_digests_are_incremental_over_batches(self):
        fields, features, _, _, stats, attr_names, _ = _fixture()
        batched = source._StreamStats()
        for feature in sorted(features, key=lambda row: row["attributes"]["OBJECTID"]):
            batched.add(feature, attr_names)
        single = source._StreamStats()
        for feature in sorted(features, key=lambda row: row["attributes"]["OBJECTID"]):
            single.add(feature, attr_names)
        self.assertEqual(batched.record_sha256, single.record_sha256)
        self.assertEqual(batched.geometry_sha256, single.geometry_sha256)

    def test_z_and_m_preserved_in_wkb(self):
        line = {"hasZ": True, "hasM": True,
                "paths": [[[-86.4, 34.1, 0, None], [-86.5, 34.2, 1033.19, 2795.88]]]}
        wkb = source._line_wkb(line)
        geom = shapely.from_wkb(wkb)
        self.assertIsInstance(geom, LineString)
        self.assertTrue(geom.has_z and geom.has_m)
        self.assertEqual(list(geom.coords)[1], (-86.5, 34.2, 1033.19, 2795.88))
        self.assertEqual(struct.unpack("<BII", wkb[:9])[1], source.EWKB_LINESTRING_ZM)

    def test_null_m_maps_to_wkb_nan_and_is_counted(self):
        line = {"hasZ": True, "hasM": True,
                "paths": [[[-86.4, 34.1, 0, None], [-86.5, 34.2, 0, None]]]}
        stats = source._StreamStats()
        stats.add({"attributes": {"OBJECTID": 1}, "geometry": line}, ["OBJECTID"])
        self.assertEqual(stats.null_m_vertex_count, 2)
        geom = shapely.from_wkb(source._line_wkb(line))
        self.assertTrue(all(coordinate[3] != coordinate[3] for coordinate in geom.coords))

    def test_multiline_wkb_type_3005(self):
        line = {"hasZ": True, "hasM": True,
                "paths": [[[-87.0, 33.0, 0, None], [-87.2, 33.2, 0, None]],
                          [[-88.0, 32.0, 0, None], [-89.0, 31.0, 0, None]]]}
        wkb = source._line_wkb(line)
        self.assertEqual(struct.unpack("<BII", wkb[:9])[1], source.EWKB_MULTILINESTRING_ZM)
        geom = shapely.from_wkb(wkb)
        self.assertIsInstance(geom, MultiLineString)
        self.assertEqual(len(geom.geoms), 2)

    def test_xy_nan_gates_candidate_geometry_but_keeps_raw(self):
        line = {"hasZ": True, "hasM": True,
                "paths": [[[-87.0, 33.0, 0, None], ["NaN", "NaN", 0, None]]]}
        stats = source._StreamStats()
        stats.add({"attributes": {"OBJECTID": 1}, "geometry": line}, ["OBJECTID"])
        self.assertEqual(stats.nan_geometry_count, 1)
        self.assertIsNone(source._line_wkb(line))
        self.assertEqual(stats.null_geometry_count, 0)

    def test_two_ordinate_vertex_is_rejected(self):
        stats = source._StreamStats()
        with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
            stats.add({"attributes": {"OBJECTID": 1},
                       "geometry": {"paths": [[[-87.0, 33.0], [-87.2, 33.2]]]}}, ["OBJECTID"])

    def test_geometry_without_zm_flags_is_accepted(self):
        stats = source._StreamStats()
        stats.add({"attributes": {"OBJECTID": 1},
                   "geometry": {"paths": [[[-87.0, 33.0, 0, None], [-87.2, 33.2, 0, None]]]}}, ["OBJECTID"])
        self.assertEqual(stats.feature_count, 1)

    def test_wrong_zm_flags_are_rejected(self):
        stats = source._StreamStats()
        with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
            stats.add({"attributes": {"OBJECTID": 1},
                       "geometry": {"hasZ": False, "hasM": True,
                                    "paths": [[[-87.0, 33.0, 0, None], [-87.2, 33.2, 0, None]]]}}, ["OBJECTID"])

    def test_seven_digit_segment_offsets_parse_and_tile(self):
        self.assertEqual(source._parse_segment_name("page_1504000_02494"), (1504000, 2494))
        self.assertEqual(source._parse_segment_name("page_000000_02000"), (0, 2000))
        with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
            source._parse_segment_name("page_abc_00002")

    def test_mixed_width_segment_names_sort_numerically(self):
        names = [source._segment_name(offset, count) for offset, count in
                 ((998000, 2000), (1000000, 2000), (0, 2000), (1002000, 2000))]
        segments = sorted(source._parse_segment_name(name) for name in names)
        self.assertEqual(segments, [(0, 2000), (998000, 2000), (1000000, 2000), (1002000, 2000)])

    def test_contract_rejects_missing_service_documentation_link(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        bodies = _docs_to_bodies(docs)
        bodies["service_doc"] = b"<html>unrelated</html>"
        with patch.multiple(source, **_pins(docs, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._contract_from_docs({**{name: json.loads(bodies[name]) for name in
                                               ("ids_seed", "service_before", "layer_before", "count_before",
                                                "ids_before")},
                                            "service_doc": bodies["service_doc"]})

    def test_contract_rejects_height_model_drift(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        docs = json.loads(json.dumps(docs))
        docs["layer_before"]["sourceHeightModelInfo"]["heightUnit"] = "meter"
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_height_model_drift"):
                source._contract_from_docs({**{name: docs[name] for name in
                                               ("ids_seed", "service_before", "layer_before", "count_before",
                                                "ids_before")},
                                            "service_doc": SERVICE_DOC_BODY})

    def test_contract_rejects_count_and_roster_drift(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        drifted = json.loads(json.dumps(docs))
        drifted["count_before"]["count"] = 3
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                source._contract_from_docs({**{name: drifted[name] for name in
                                               ("ids_seed", "service_before", "layer_before", "count_before",
                                                "ids_before")},
                                            "service_doc": SERVICE_DOC_BODY})
        drift = json.loads(json.dumps(docs))
        drift["ids_seed"]["objectIds"] = [13, 10, 12]
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._contract_from_docs({**{name: drift[name] for name in
                                               ("ids_seed", "service_before", "layer_before", "count_before",
                                                "ids_before")},
                                            "service_doc": SERVICE_DOC_BODY})

    def test_contract_accepts_reversed_roster_response_order(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        reversed_docs = json.loads(json.dumps(docs))
        reversed_docs["ids_seed"]["objectIds"] = [13, 11, 12, 10]
        reversed_docs["ids_before"]["objectIds"] = [11, 12, 10, 13]
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            contract = source._contract_from_docs(
                {**{name: reversed_docs[name] for name in
                    ("ids_seed", "service_before", "layer_before", "count_before", "ids_before")},
                 "service_doc": SERVICE_DOC_BODY})
        self.assertEqual(contract["ids"], [10, 11, 12, 13])

    def test_page_crs_drift_is_rejected(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        page = json.loads(json.dumps(docs["page_000000_00002"]))
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_page(0, 2, page, sorted(ids), attr_names, source._StreamStats())

    def test_page_id_slice_mismatch_is_rejected(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        page = json.loads(json.dumps(docs["page_000000_00002"]))
        page["features"] = page["features"][1:]
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_page(0, 2, page, sorted(ids), attr_names, source._StreamStats())

    def test_final_page_cannot_claim_transfer_truncation(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        page = json.loads(json.dumps(docs["page_000002_00002"]))
        page["exceededTransferLimit"] = True
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_page(2, 2, page, sorted(ids), attr_names, source._StreamStats())

    def test_unexpected_field_is_rejected_by_schema_contract(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        drifted = json.loads(json.dumps(docs))
        drifted["layer_before"]["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._contract_from_docs({**{name: drifted[name] for name in
                                               ("ids_seed", "service_before", "layer_before", "count_before",
                                                "ids_before")},
                                            "service_doc": SERVICE_DOC_BODY})

    def test_record_value_change_is_detected_by_streamed_digest(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        page = json.loads(json.dumps(docs["page_000000_00002"]))
        page["features"][0]["attributes"]["BASELN_ID"] = "TAMPERED"
        drifted_stats = source._StreamStats()
        for feature in page["features"]:
            drifted_stats.add(feature, attr_names)
        for feature in docs["page_000002_00002"]["features"]:
            drifted_stats.add(feature, attr_names)
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._check_population_pins(drifted_stats)

    def test_incomplete_stream_fails_tiling_proof(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        partial = source._StreamStats()
        for feature in docs["page_000000_00002"]["features"]:
            partial.add(feature, attr_names)
        with patch.multiple(source, **_pins({}, schema, stats, ids)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._check_population_pins(partial)

    def test_observed_coverage_drift_is_detected(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        pins = _pins({}, schema, stats, ids)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -90.1, "ymin": 30.0, "xmax": -85.8, "ymax": 34.6}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._check_population_pins(stats)

    def test_geometry_types_drift_is_detected(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        pins = _pins({}, schema, stats, ids)
        pins["EXPECTED_GEOMETRY_TYPES"] = ["LineString ZM"]
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_geometry_types_drift"):
                source._check_population_pins(stats)

    def test_null_geometry_roster_drift_is_detected(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        pins = _pins({}, schema, stats, ids)
        pins["EXPECTED_NULL_GEOMETRY_ID_SHA256"] = "0" * 64
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_null_geometry_id_roster_drift"):
                source._check_population_pins(stats)

    def test_unpinned_module_refuses_capture(self):
        from dagster_hifld.resources import StagingStorageResource
        with TemporaryDirectory() as staging_dir:
            storage = StagingStorageResource(local_dir=staging_dir, use_local=True)
            with self.assertRaisesRegex(ValueError, "source_population_pins_not_established"):
                source.capture_nfhl_profile_baselines(storage, version="vtest")

    def test_parquet_preserves_native_fields_and_zm_geometry(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        pins = _pins({}, schema, stats, ids)
        with patch.multiple(source, **pins):
            with TemporaryDirectory() as capture_dir:
                root = Path(capture_dir)
                for name in ("page_000000_00002", "page_000002_00002"):
                    (root / f"{name}.body").write_bytes(
                        json.dumps(docs[name], separators=(",", ":")).encode())
                parquet = source._parquet_from_pages(root, [(0, 2), (2, 2)], schema)
                parquet_body = Path(parquet["path"]).read_bytes()
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(parquet_body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13])
        self.assertEqual(table.column("BASELN_ID").to_pylist(),
                         ["01049C_10", "01049C_11", "01049C_12", "01049C_13"])
        self.assertEqual(table.column("WTR_NM").to_pylist(), ["PHILLIPS BRANCH"] * 4)
        self.assertEqual(table.column("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE").null_count, 1)
        shapes = table.column("SHAPE").to_pylist()
        first = shapely.from_wkb(shapes[0])
        self.assertIsInstance(first, LineString)
        self.assertTrue(first.has_z and first.has_m)
        first_coord = list(first.coords)[0]
        self.assertEqual(first_coord[:3], (-85.89851338304294, 34.51201594594227, 0.0))
        self.assertTrue(first_coord[3] != first_coord[3])
        self.assertIsNone(shapes[1])
        third = shapely.from_wkb(shapes[2])
        self.assertIsInstance(third, MultiLineString)
        self.assertEqual(len(third.geoms), 2)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(sorted(geo["columns"]["SHAPE"]["geometry_types"]),
                         ["LineString Z", "MultiLineString Z"])

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with TemporaryDirectory() as staging_dir, TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/nfhl-profile-baselines.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-profile-baselines.parquet", b"accepted")
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


def _receipt(body: bytes, url: str = "https://producer.invalid/query") -> dict:
    return {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-12T00:00:00+00:00",
            "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": None,
            "content_length": str(len(body)), "error_code": None}


def _write_evidence(capture_root: Path, name: str, body: bytes, receipt: dict | None = None) -> None:
    capture_root.mkdir(parents=True, exist_ok=True)
    (capture_root / f"{name}.body").write_bytes(body)
    (capture_root / f"{name}.wire").write_bytes(body)
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt or _receipt(body)) + b"\n")


ERROR_BODY = b'{"error": {"code": 500, "message": "Error performing query operation"}}'


class RetryAndResumeTests(unittest.TestCase):
    def _fixture_bodies(self):
        fields, features, docs, schema, stats, attr_names, ids = _fixture()
        return _docs_to_bodies(docs), schema, stats, ids, attr_names

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = ERROR_BODY if len(calls) == 1 else b'{"objectIds": []}'
            _write_evidence(capture_root, name, body)
            return body, _receipt(body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep") as sleep_mock:
                receipt = source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                                  max_bytes=1024, attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                  base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(receipt["sha256"], hashlib.sha256(b'{"objectIds": []}').hexdigest())
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), b'{"objectIds": []}')
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), ERROR_BODY)
            self.assertTrue(sleep_mock.called)

    def test_persistent_producer_error_doc_exhausts_retries(self):
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            _write_evidence(capture_root, name, ERROR_BODY)
            return ERROR_BODY, _receipt(ERROR_BODY)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                receipt = source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                                  max_bytes=1024, attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                  base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(len(calls), source.DOC_CAPTURE_ATTEMPTS)
            for attempt in range(1, source.DOC_CAPTURE_ATTEMPTS):
                self.assertEqual((root / f"probe.attempt-{attempt:02d}.body").read_bytes(), ERROR_BODY)
            self.assertEqual((root / "probe.body").read_bytes(), ERROR_BODY)

    def test_incomplete_transport_capture_is_retried_once_bounded(self):
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
            _write_evidence(capture_root, name, ok_body)
            return ok_body, _receipt(ok_body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                receipt = source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                                  max_bytes=1024, attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                  base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
                with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                    source._ensure_captured(None, "https://producer.invalid/other", root, "probe2",
                                            max_bytes=1024, attempts=source.DOC_CAPTURE_ATTEMPTS,
                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(receipt["sha256"], hashlib.sha256(ok_body).hexdigest())
            self.assertEqual(calls.count("probe"), 2)
            self.assertEqual((root / "probe.attempt-01.receipt.json").read_bytes(), b'{"status": null}\n')

    def test_verified_on_disk_evidence_is_reused_without_network(self):
        ok_body = b'{"objectIds": []}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            _write_evidence(capture_root, name, ok_body)
            return ok_body, _receipt(ok_body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_evidence(root, "probe", ok_body)
            with patch.object(source, "capture_get", side_effect=fake_capture_get):
                receipt = source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                                  max_bytes=1024, attempts=2, base_delay=1.0)
            self.assertEqual(calls, [])
            self.assertEqual(receipt["sha256"], hashlib.sha256(ok_body).hexdigest())

    def test_stale_or_tampered_evidence_is_not_reused(self):
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = b'{"objectIds": [1, 2]}'
            _write_evidence(capture_root, name, body)
            return body, _receipt(body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "a"
            _write_evidence(root, "probe", b'{"objectIds": [1, 2]}')
            (root / "probe.body").write_bytes(b'{"objectIds": [1]}')
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                        max_bytes=1024, attempts=2, base_delay=1.0)
            self.assertEqual(calls, ["probe"])
            self.assertEqual((root / "probe.body").read_bytes(), b'{"objectIds": [1, 2]}')

        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "b"
            _write_evidence(root, "probe", ERROR_BODY)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                source._ensure_captured(None, "https://producer.invalid/page", root, "probe",
                                        max_bytes=1024, attempts=2, base_delay=1.0)
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), b'{"objectIds": [1, 2]}')

    def test_adaptive_bisection_splits_producer_cap_failures(self):
        bodies, schema, stats, ids, attr_names = self._fixture_bodies()
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = ERROR_BODY if name == "page_000000_00004" else bodies[name]
            _write_evidence(capture_root, name, body)
            return body, _receipt(body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            with patch.multiple(source, SEGMENT_START_RECORDS=4, SEGMENT_PROBE_ATTEMPTS=1,
                                DOC_CAPTURE_ATTEMPTS=1, EXPECTED_COUNT=4), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                settled = source._capture_segments(None, root, sorted(ids), attr_names, source._StreamStats(), {})
            self.assertEqual(sorted(settled), [(0, 2), (2, 2)])
            self.assertEqual((root / "page_000000_00004.body").read_bytes(), ERROR_BODY)
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_unservable_single_feature_fails_explicitly(self):
        _, _, _, ids, attr_names = self._fixture_bodies()
        error_body = ERROR_BODY

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            _write_evidence(capture_root, name, error_body)
            return error_body, _receipt(error_body)

        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            with patch.multiple(source, SEGMENT_START_RECORDS=4, SEGMENT_PROBE_ATTEMPTS=1,
                                DOC_CAPTURE_ATTEMPTS=2, EXPECTED_COUNT=4), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_segments(None, root, [10, 11, 12, 13], attr_names, source._StreamStats(), {})

    def test_resumed_capture_reuses_completed_pages(self):
        bodies, schema, stats, ids, attr_names = self._fixture_bodies()
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            _write_evidence(capture_root, name, bodies[name])
            return bodies[name], _receipt(bodies[name])

        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            # First (interrupted) pass captures only the first page.
            _write_evidence(root, "page_000000_00002", bodies["page_000000_00002"])
            with patch.multiple(source, SEGMENT_START_RECORDS=2, SEGMENT_PROBE_ATTEMPTS=2,
                                DOC_CAPTURE_ATTEMPTS=2, MIN_FREE_BYTES=0, EXPECTED_COUNT=4), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                segments = source._capture_segments(None, root, sorted(ids), attr_names, source._StreamStats(), {})
            self.assertEqual(sorted(segments), [(0, 2), (2, 2)])
            self.assertEqual(calls, ["page_000002_00002"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        bodies, schema, stats, ids, attr_names = self._fixture_bodies()

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            _write_evidence(capture_root, name, body)
            return body, _receipt(body, url)

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with TemporaryDirectory(prefix="nfhl-baseline-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins({}, schema, stats, ids)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_profile_baselines(storage, version="vfixture",
                                                                  capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 4)
                self.assertEqual(candidate["source_null_geometry_count"], 1)
                self.assertEqual(candidate["source_null_m_vertex_count"], 6)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name in ("ids_seed", "service_doc", "service_before", "layer_before", "count_before",
                             "ids_before", "page_000000_00002", "page_000002_00002",
                             "layer_after", "service_after", "count_after", "ids_after"):
                    body = bodies[name]
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
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
                reused = source.capture_nfhl_profile_baselines(storage, version="vfixture",
                                                               capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
