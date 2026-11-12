"""Focused contract checks for the FEMA NFHL LOMRs source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_lomrs as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

# Field order and types exactly as served by the producer's layer metadata.
FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "LOMR_ID", "type": "esriFieldTypeString", "alias": "LOMR_ID", "length": 32, "domain": None},
    {"name": "EFF_DATE", "type": "esriFieldTypeDate", "alias": "EFF_DATE", "length": 8, "domain": None},
    {"name": "CASE_NO", "type": "esriFieldTypeString", "alias": "CASE_NO", "length": 13, "domain": None},
    {"name": "SCALE", "type": "esriFieldTypeString", "alias": "SCALE", "length": 5, "domain": None},
    {"name": "STATUS", "type": "esriFieldTypeString", "alias": "STATUS", "length": 12, "domain": None},
    {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "SHAPE.STArea()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STArea()", "domain": None},
    {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]
ATTR_NAMES = [f["name"] for f in FIELDS if f["name"] != "SHAPE"]

# Clockwise closed exterior (Esri JSON exterior convention), verbatim source ring.
RING_A = [[-116.07184768761766, 33.420746150189245], [-116.06, 33.44], [-116.05, 33.43],
          [-116.06, 33.42], [-116.07184768761766, 33.420746150189245]]
# Counter-clockwise closed hole inside RING_A's exterior.
HOLE_A = [[-116.065, 33.427], [-116.065, 33.426], [-116.063, 33.426], [-116.065, 33.427]]
# Second clockwise closed exterior sharing one feature with HOLE_B.
RING_B = [[-116.02, 33.40], [-116.02, 33.41], [-116.01, 33.41], [-116.02, 33.40]]
HOLE_B = [[-116.017, 33.407], [-116.017, 33.406], [-116.016, 33.406], [-116.017, 33.407]]
# Counter-clockwise hole lying inside no exterior of its feature.
HOLE_OUTSIDE = [[-110.0, 40.0], [-110.0, 39.0], [-109.0, 39.0], [-110.0, 40.0]]


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "DFIRM_ID": "01003C", "VERSION_ID": "1.1.1.0", "LOMR_ID": f"01003C_{oid}",
              "EFF_DATE": 1601251200000, "CASE_NO": f"19-04-{oid:04d}P", "SCALE": "6000",
              "STATUS": "Effective", "SOURCE_CIT": "01003C_LOMC1",
              "SHAPE.STArea()": 2.7621940525488277e-05, "SHAPE.STLength()": 0.027409815382218487,
              "GFID": "2daa8f96-8523-4ed3-bdef-653fac6517df",
              "GlobalID": "{49D46D37-05EE-4110-B32C-9F7DC7BB5256}"}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _fixture():
    fields = FIELDS
    layer = {"id": 1, "name": "LOMRs", "type": "Feature Layer", "geometryType": "esriGeometryPolygon",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 1, "name": "LOMRs", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attrs(10), "geometry": {"rings": [RING_A]}},
        {"attributes": _attrs(11), "geometry": {"rings": [RING_A, HOLE_A]}},
        {"attributes": _attrs(12), "geometry": None},
        {"attributes": _attrs(13), "geometry": {"rings": [[["NaN", "NaN"], ["NaN", "NaN"], ["NaN", "NaN"]]]}},
        {"attributes": _attrs(14), "geometry": {"rings": [RING_B, HOLE_B]}},
        # Producer-observed shape (NFHL OBJECTID 3987 on the 2026-11-12 capture):
        # a geometry value that holds no rings at all.
        {"attributes": _attrs(15), "geometry": {"rings": []}},
    ]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    geom_records = [[10, [RING_A]], [11, [RING_A, HOLE_A]], [12, None],
                    [13, [[["NaN", "NaN"], ["NaN", "NaN"], ["NaN", "NaN"]]]],
                    [14, [RING_B, HOLE_B]], [15, []]]
    crs = {"wkid": 4269, "latestWkid": 4269}
    pages = [features[0:2], features[2:4], features[4:6]]
    page_docs = [{"features": page, "spatialReference": crs,
                  **({"exceededTransferLimit": True} if index < len(pages) - 1 else {})}
                 for index, page in enumerate(pages)]
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000": page_docs[0], "page_001": page_docs[1], "page_002": page_docs[2]}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return fields, features, captures, schema, geom_records


def _bounds(features):
    values = []
    for feature in features:
        geom = feature.get("geometry")
        if not geom or "rings" not in geom:
            continue
        for ring in geom["rings"]:
            for vertex in ring:
                if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                    values.extend(vertex)
    return {"xmin": min(values[0::2]), "ymin": min(values[1::2]),
            "xmax": max(values[0::2]), "ymax": max(values[1::2])}


def _pins(features, schema, geom_records):
    return dict(EXPECTED_COUNT=6, PAGE_SIZE=2, EXPECTED_NULL_GEOMETRY_COUNT=1,
                EXPECTED_NAN_GEOMETRY_COUNT=1, EXPECTED_EMPTY_RINGS_GEOMETRY_COUNT=1,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(sorted(features,
                                                                         key=lambda row: row["attributes"]["OBJECTID"]))).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(sorted(geom_records))).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(1, "LOMRs", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS=_bounds(features))


class NfhlLomrsTests(unittest.TestCase):
    def test_native_rings_null_geometry_and_date_values_preserved_in_parquet(self):
        fields, features, captures, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(table.column("LOMR_ID").to_pylist(),
                         ["01003C_10", "01003C_11", "01003C_12", "01003C_13", "01003C_14", "01003C_15"])
        self.assertEqual(table.column("EFF_DATE").type.__str__(), "int64")
        self.assertEqual(table.column("EFF_DATE").to_pylist(), [1601251200000] * 6)
        self.assertEqual(table.column("SHAPE.STArea()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE").null_count, 3)
        polygon_10 = shapely.from_wkb(table.column("SHAPE").to_pylist()[0])
        self.assertEqual(list(polygon_10.exterior.coords), [tuple(vertex) for vertex in RING_A])
        self.assertEqual(len(polygon_10.interiors), 0)
        polygon_11 = shapely.from_wkb(table.column("SHAPE").to_pylist()[1])
        self.assertEqual(list(polygon_11.exterior.coords), [tuple(vertex) for vertex in RING_A])
        self.assertEqual([list(interior.coords) for interior in polygon_11.interiors],
                         [[tuple(vertex) for vertex in HOLE_A]])
        self.assertIsNone(table.column("SHAPE").to_pylist()[2])
        self.assertIsNone(table.column("SHAPE").to_pylist()[3])
        self.assertIsNone(table.column("SHAPE").to_pylist()[5])
        self.assertEqual(table.column("LOMR_ID").to_pylist()[3], "01003C_13")
        self.assertEqual(table.column("CASE_NO").to_pylist()[3], "19-04-0013P")
        self.assertEqual(table.column("CASE_NO").to_pylist()[5], "19-04-0015P")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_hole_joins_smallest_containing_exterior_as_multipolygon(self):
        _, features, _, _, _ = _fixture()
        multipolygon = shapely.from_wkb(source._polygon_wkb({"rings": [RING_A, RING_B, HOLE_B]}))
        self.assertIsInstance(multipolygon, shapely.MultiPolygon)
        self.assertEqual(sum(len(list(geom.exterior.coords)) + sum(len(list(interior.coords))
                                                                  for interior in geom.interiors)
                             for geom in multipolygon.geoms),
                         len(RING_A) + len(RING_B) + len(HOLE_B))
        second = list(multipolygon.geoms)[1]
        self.assertEqual([list(interior.coords) for interior in second.interiors],
                         [[tuple(vertex) for vertex in HOLE_B]])
        with self.assertRaisesRegex(ValueError, "source_polygon_hole_unassigned"):
            source._polygon_wkb({"rings": [RING_B, HOLE_OUTSIDE]})
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_degenerate"):
            source._polygon_wkb({"rings": [[[-116.0, 33.0], [-116.0, 33.0], [-116.0, 33.0]]]})
        with self.assertRaisesRegex(ValueError, "source_polygon_exterior_missing"):
            source._polygon_wkb({"rings": [HOLE_A, HOLE_B]})

    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13, 15]), ("ids_before", [11, 12, 10, 13, 14, 15]),
                          ("ids_after", [10, 11, 12, 13, 14, 15])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        _, _, extracted = self._validated(features, captures, schema, geom)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14, 15])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["empty_rings_geometry_count"], 1)

    def _validated(self, features, captures, schema, geom):
        with patch.multiple(source, **_pins(features, schema, geom)):
            return source._validate_captures(captures)

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
            self._validated(features, captures, schema, geom)

    def test_crs_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_page_000_crs_drift"):
            self._validated(features, captures, schema, geom)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPolyline"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "exact_nfhl_lomrs_layer_metadata_drift"):
            self._validated(features, captures, schema, geom)

    def test_date_field_type_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][4]["type"] = "esriFieldTypeString"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            self._validated(features, captures, schema, geom)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, geom = _fixture()
        final = json.loads(captures["page_002"][0])
        final["exceededTransferLimit"] = True
        captures["page_002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
            self._validated(features, captures, schema, geom)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            self._validated(features, captures, schema, geom)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"] = page["features"][:1]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            self._validated(features, captures, schema, geom)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["STATUS"] = "Retired"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_vertex_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["rings"][0][1] = [-116.061, 33.42]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -116.1, "ymin": 33.4, "xmax": -116.0, "ymax": 33.45}
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
                          "geoparquet/nfhl-lomrs.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-lomrs.parquet", b"accepted")
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

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        import tempfile
        from pathlib import Path

        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom = _fixture()
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

        with tempfile.TemporaryDirectory(prefix="nfhl-lomrs-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_lomrs(storage, version="vfixture",
                                                      capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], 1)
                self.assertEqual(candidate["source_nan_string_geometry_count"], 1)
                self.assertEqual(candidate["source_empty_rings_geometry_count"], 1)
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
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 3)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_lomrs(storage, version="vfixture",
                                                   capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
