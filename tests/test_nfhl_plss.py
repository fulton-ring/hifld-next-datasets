"""Focused contract checks for the FEMA NFHL PLSS source adapter."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_plss as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

# Field order and types exactly as served by the producer's layer metadata.
FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "PLSS_AR_ID", "type": "esriFieldTypeString", "alias": "PLSS_AR_ID", "length": 32, "domain": None},
    {"name": "RANGE", "type": "esriFieldTypeString", "alias": "RANGE", "length": 8, "domain": None},
    {"name": "TWP", "type": "esriFieldTypeString", "alias": "TWP", "length": 8, "domain": None},
    {"name": "SECT_NO", "type": "esriFieldTypeString", "alias": "SECT_NO", "length": 4, "domain": None},
    {"name": "NAME", "type": "esriFieldTypeString", "alias": "NAME", "length": 254, "domain": None},
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
NAN_RING = [[["NaN", "NaN"], ["NaN", "NaN"], ["NaN", "NaN"]]]


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "DFIRM_ID": "01003C", "VERSION_ID": "1.1.1.0", "PLSS_AR_ID": f"01003C_{oid}",
              "RANGE": "03W", "TWP": "26N", "SECT_NO": "PB58", "NAME": "", "SOURCE_CIT": "01003C_BASE4",
              "SHAPE.STArea()": 2.7621940525488277e-05, "SHAPE.STLength()": 0.027409815382218487,
              "GFID": "2daa8f96-8523-4ed3-bdef-653fac6517df",
              "GlobalID": "{49D46D37-05EE-4110-B32C-9F7DC7BB5256}"}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _fixture():
    fields = FIELDS
    layer = {"id": 5, "name": "PLSS", "type": "Feature Layer", "geometryType": "esriGeometryPolygon",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 5, "name": "PLSS", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attrs(10), "geometry": {"rings": [RING_A]}},
        {"attributes": _attrs(11), "geometry": {"rings": [RING_A, HOLE_A]}},
        {"attributes": _attrs(12), "geometry": None},
        {"attributes": _attrs(13), "geometry": {"rings": NAN_RING}},
        {"attributes": _attrs(14), "geometry": {"rings": [RING_B, HOLE_B]}},
    ]
    ids = [10, 11, 12, 13, 14]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    geom_records = [[10, [RING_A]], [11, [RING_A, HOLE_A]], [12, None], [13, NAN_RING], [14, [RING_B, HOLE_B]]]
    crs = {"wkid": 4269, "latestWkid": 4269}
    pages = [features[0:2], features[2:4], features[4:5]]
    page_docs = [{"features": page, "spatialReference": crs,
                  **({"exceededTransferLimit": True} if index < len(pages) - 1 else {})}
                 for index, page in enumerate(pages)]
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 5}, "count_after": {"count": 5},
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
    return dict(EXPECTED_COUNT=5, PAGE_SIZE=2, EXPECTED_NULL_GEOMETRY_COUNT=1,
                EXPECTED_NAN_GEOMETRY_COUNT=1, EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS={},
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(sorted(features,
                                                                         key=lambda row: row["attributes"]["OBJECTID"]))).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(sorted(geom_records))).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(5, "PLSS", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS=_bounds(features))


def _page_doc(features_slice, last=False):
    return {"features": features_slice, "spatialReference": {"wkid": 4269, "latestWkid": 4269},
            **({"exceededTransferLimit": True} if not last else {})}


class NfhlPlssTests(unittest.TestCase):
    def test_stream_digest_matches_array_digest(self):
        chunks = [[1, 2], [3], [4, 5, 6]]
        digest = source._StreamDigest()
        for chunk in chunks:
            digest.update(chunk)
        self.assertEqual(digest.hexdigest(),
                         hashlib.sha256(_canonical([1, 2, 3, 4, 5, 6])).hexdigest())
        self.assertEqual(digest.count, 6)

    def test_native_rings_null_geometry_and_values_preserved_in_parquet(self):
        _, features, _, schema, _ = _fixture()
        with tempfile.TemporaryDirectory() as temporary:
            with patch.multiple(source, **_pins(features, schema, [])):
                stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
                for index in range(3):
                    stream.process(_page_doc(features[index * 2:index * 2 + 2], last=index == 2),
                                   [10, 11, 12, 13, 14], index, index * source.PAGE_SIZE)
                audit = stream.finish()
                table = pq.read_table(str(Path(temporary) / "candidate.geoparquet"))
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(audit["nan_string_geometry_count"], 1)
        self.assertEqual(audit["observed_bounds"], _bounds(features))
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14])
        self.assertEqual(table.column("PLSS_AR_ID").to_pylist(),
                         ["01003C_10", "01003C_11", "01003C_12", "01003C_13", "01003C_14"])
        self.assertEqual(table.column("RANGE").to_pylist(), ["03W"] * 5)
        self.assertEqual(table.column("TWP").to_pylist(), ["26N"] * 5)
        self.assertEqual(table.column("SECT_NO").to_pylist(), ["PB58"] * 5)
        self.assertEqual(table.column("GlobalID").to_pylist(),
                         ["{49D46D37-05EE-4110-B32C-9F7DC7BB5256}"] * 5)
        self.assertEqual(table.column("SHAPE.STArea()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        polygon_10 = shapely.from_wkb(table.column("SHAPE").to_pylist()[0])
        self.assertEqual(list(polygon_10.exterior.coords), [tuple(vertex) for vertex in RING_A])
        self.assertEqual(len(polygon_10.interiors), 0)
        polygon_11 = shapely.from_wkb(table.column("SHAPE").to_pylist()[1])
        self.assertEqual(list(polygon_11.exterior.coords), [tuple(vertex) for vertex in RING_A])
        self.assertEqual([list(interior.coords) for interior in polygon_11.interiors],
                         [[tuple(vertex) for vertex in HOLE_A]])
        self.assertIsNone(table.column("SHAPE").to_pylist()[2])
        self.assertIsNone(table.column("SHAPE").to_pylist()[3])
        self.assertEqual(table.column("PLSS_AR_ID").to_pylist()[3], "01003C_13")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_unusable_geometry_is_gated_null_and_counted(self):
        degenerate = [[[-116.0, 33.0], [-116.0, 33.0], [-116.0, 33.0]]]
        page = {"features": [{"attributes": _attrs(20), "geometry": {"rings": degenerate}},
                             {"attributes": _attrs(21), "geometry": {"rings": [RING_A]}}],
                "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
        with tempfile.TemporaryDirectory() as temporary:
            stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
            stream.process(page, [20, 21], 0, 0)
            audit = stream.finish()
            table = pq.read_table(str(Path(temporary) / "candidate.geoparquet"))
        self.assertEqual(audit["unusable_geometry_feature_counts"], {"source_polygon_ring_degenerate": 1})
        self.assertEqual(table.column("OBJECTID").to_pylist(), [20, 21])
        self.assertIsNone(table.column("SHAPE").to_pylist()[0])
        self.assertEqual(table.column("PLSS_AR_ID").to_pylist()[0], "01003C_20")
        self.assertIsNotNone(table.column("SHAPE").to_pylist()[1])

    def test_hole_joins_smallest_containing_exterior_as_multipolygon(self):
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

    def _docs(self, captures):
        return {name: json.loads(captures[name][0]) for name in
                ("service_before", "layer_before", "count_before", "ids_before", "ids_seed")}

    def test_document_roster_order_is_not_population_drift(self):
        _, features, captures, schema, geom = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13]), ("ids_before", [11, 12, 10, 13, 14])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, _, ids = source._validate_docs(self._docs(captures), SERVICE_DOC_BODY)
        self.assertEqual(ids, [10, 11, 12, 13, 14])

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._validate_docs(self._docs(captures), b"<html>unrelated</html>")

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPolyline"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "exact_nfhl_plss_layer_metadata_drift"):
                source._validate_docs(self._docs(captures), SERVICE_DOC_BODY)

    def test_field_type_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][1]["type"] = "esriFieldTypeDouble"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_docs(self._docs(captures), SERVICE_DOC_BODY)

    def test_unexpected_field_is_rejected_by_contract(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_docs(self._docs(captures), SERVICE_DOC_BODY)

    def test_id_roster_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        ids_doc = json.loads(captures["ids_before"][0])
        ids_doc["objectIds"] = [10, 11, 12, 13]
        captures["ids_before"] = (json.dumps(ids_doc, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_docs(self._docs(captures), SERVICE_DOC_BODY)

    def test_page_crs_drift_is_rejected(self):
        _, features, _, schema, geom = _fixture()
        page = json.loads(json.dumps(_page_doc(features[0:2])))
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_000_crs_drift"):
                source._validate_page(page, [10, 11, 12, 13, 14], 0, 0)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, _, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            final = _page_doc(features[4:5], last=True)
            final["exceededTransferLimit"] = True
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_page(final, [10, 11, 12, 13, 14], 2, 4)

    def test_missing_page_id_is_not_accepted(self):
        _, features, _, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            page = _page_doc(features[0:1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_page(page, [10, 11, 12, 13, 14], 0, 0)

    def test_record_attribute_schema_drift_is_rejected(self):
        _, features, _, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            with tempfile.TemporaryDirectory() as temporary:
                stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
                attrs = _attrs(10)
                attrs["STATUS"] = "Retired"
                page = {"features": [{"attributes": attrs, "geometry": {"rings": [RING_A]}}],
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
                with self.assertRaisesRegex(ValueError, "source_record_attribute_schema_drift"):
                    stream.process(page, [10], 0, 0)

    def test_record_value_change_is_detected(self):
        _, features, _, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        mutated = json.loads(json.dumps(_page_doc(features[0:2])))
        mutated["features"][0]["attributes"]["PLSS_AR_ID"] = "MUTATED"
        with patch.multiple(source, **pins):
            with tempfile.TemporaryDirectory() as temporary:
                stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
                stream.process(mutated, [10, 11, 12, 13, 14], 0, 0)
                stream.process(_page_doc(features[2:4]), [10, 11, 12, 13, 14], 1, 2)
                stream.process(_page_doc(features[4:5], last=True), [10, 11, 12, 13, 14], 2, 4)
                audit = stream.finish()
        self.assertNotEqual(audit["source_record_sha256"], pins["EXPECTED_RECORD_SHA256"])

    def test_vertex_change_is_detected(self):
        _, features, _, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        mutated = json.loads(json.dumps(_page_doc(features[0:2])))
        mutated["features"][0]["geometry"]["rings"][0][1] = [-116.061, 33.4401]
        with patch.multiple(source, **pins):
            with tempfile.TemporaryDirectory() as temporary:
                stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
                stream.process(mutated, [10, 11, 12, 13, 14], 0, 0)
                stream.process(_page_doc(features[2:4]), [10, 11, 12, 13, 14], 1, 2)
                stream.process(_page_doc(features[4:5], last=True), [10, 11, 12, 13, 14], 2, 4)
                audit = stream.finish()
        self.assertNotEqual(audit["source_record_sha256"], pins["EXPECTED_RECORD_SHA256"])
        self.assertNotEqual(audit["source_geometry_sha256"], pins["EXPECTED_GEOMETRY_SHA256"])

    def test_observed_coverage_drift_is_detected(self):
        _, features, _, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -116.1, "ymin": 33.4, "xmax": -116.0, "ymax": 33.45}
        with patch.multiple(source, **pins):
            with tempfile.TemporaryDirectory() as temporary:
                stream = source._PageStream(FIELDS, Path(temporary) / "candidate.geoparquet")
                stream.process(_page_doc(features[0:2]), [10, 11, 12, 13, 14], 0, 0)
                audit = stream.finish()
        self.assertNotEqual(audit["observed_bounds"], pins["EXPECTED_OBSERVED_BOUNDS"])

    def test_after_document_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        after_layer = json.loads(captures["layer_after"][0])
        after_layer["fields"] = [dict(f) for f in after_layer["fields"]]
        after_layer["fields"][1]["length"] = 7
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_field_schema_changed_during_capture"):
                source._validate_after_docs(
                    {"layer_after": after_layer, "service_after": json.loads(captures["service_before"][0]),
                     "count_after": json.loads(captures["count_before"][0]),
                     "ids_after": json.loads(captures["ids_after"][0])},
                    layer, json.loads(captures["service_before"][0]),
                    json.loads(captures["count_before"][0]), layer["fields"], [10, 11, 12, 13, 14])

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/nfhl-plss.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-plss.parquet", b"accepted")
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

    def _fake_capture_get(self, bodies):
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

        return fake_capture_get

    class _EmptyClient:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        with tempfile.TemporaryDirectory(prefix="nfhl-plss-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: self._EmptyClient()):
                candidate = source.capture_nfhl_plss(storage, version="vfixture",
                                                     capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 5)
                self.assertEqual(candidate["source_null_geometry_count"], 1)
                self.assertEqual(candidate["source_nan_string_geometry_count"], 1)
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
                reused = source.capture_nfhl_plss(storage, version="vfixture",
                                                  capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_capture_drift_quarantines_partial_evidence(self):
        from dagster_hifld.resources import StagingStorageResource

        fields, features, fixture_captures, schema, geom = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        mutated = json.loads(bodies["page_001"])
        mutated["features"][0]["attributes"]["PLSS_AR_ID"] = "MUTATED"
        bodies["page_001"] = json.dumps(mutated, separators=(",", ":"), ensure_ascii=False).encode()
        with tempfile.TemporaryDirectory(prefix="nfhl-plss-quarantine-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: self._EmptyClient()):
                with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                    source.capture_nfhl_plss(storage, version="vfixture",
                                             capture_directory=str(root / "captures"))
            quarantine_tokens = list((root / "staging" / "_quarantine" / source.DATASET_SLUG).iterdir())
            self.assertEqual(len(quarantine_tokens), 1)
            keys = set(storage.list_prefix())
            self.assertTrue(any(key.startswith("_quarantine/" + source.DATASET_SLUG + "/") for key in keys))
            self.assertTrue(any(key.endswith("metadata/failure.json") for key in keys))
            self.assertTrue(any(key.endswith("raw/page_001.body") for key in keys))
            self.assertFalse(any(key.startswith(source.DATASET_SLUG + "/") for key in keys))

    def test_late_page_drift_quarantine_is_memory_bounded_and_closes_writer(self):
        """Late page drift with megabyte-scale bodies: quarantine must stream.

        The complete PLSS corpus is gigabytes, so a failure after many captured
        pages must not retain every body in memory before quarantine; evidence
        is preserved one capture at a time from the capture paths, the coverage
        extrema are running values, and the abandoned parquet writer is closed
        so the partial candidate is finalized and readable.
        """
        import tracemalloc

        from dagster_hifld.resources import StagingStorageResource

        count = 12
        padding = 768 * 1024
        ids = [100 + index for index in range(count)]
        features = [{"attributes": _attrs(object_id), "geometry": {"rings": [RING_A]}} for object_id in ids]
        schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in FIELDS]
        layer = {"id": 5, "name": "PLSS", "type": "Feature Layer", "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
                 "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
                 "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                            "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
                 "fields": FIELDS}
        service = {"mapName": "NFHL", "layers": [{"id": 5, "name": "PLSS", "type": "Feature Layer"}]}
        pins = _pins(features, schema, [[object_id, [RING_A]] for object_id in ids])
        pins.update(EXPECTED_COUNT=count,
                    EXPECTED_ID_SHA256=hashlib.sha256(_canonical(ids)).hexdigest(),
                    EXPECTED_NULL_GEOMETRY_COUNT=0, EXPECTED_NAN_GEOMETRY_COUNT=0)
        ids_doc = {"objectIdFieldName": "OBJECTID", "objectIds": ids}
        # Drift on the final page (one feature short) raises with the parquet
        # writer still open, after five megabyte-scale pages are captured.
        page_size = 2
        pages = [_page_doc(features[index * page_size:(index + 1) * page_size
                                        - (1 if index == count // page_size - 1 else 0)],
                          last=index == count // page_size - 1)
                 for index in range(count // page_size)]
        docs = {"service_before": service, "service_after": service, "layer_before": layer,
                "layer_after": layer, "count_before": {"count": count}, "count_after": {"count": count},
                "ids_before": ids_doc, "ids_after": ids_doc, "ids_seed": ids_doc}
        for index, page in enumerate(pages):
            docs[source._page_name(index)] = page
        bodies = {name: json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode() + b" " * padding
                  for name, doc in docs.items()}
        bodies["service_doc"] = SERVICE_DOC_BODY + b" " * padding
        with tempfile.TemporaryDirectory(prefix="nfhl-plss-late-drift-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **pins), \
                 patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: self._EmptyClient()):
                tracemalloc.start()
                try:
                    with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                        source.capture_nfhl_plss(storage, version="vfixture",
                                                 capture_directory=str(root / "captures"))
                finally:
                    peak = tracemalloc.get_traced_memory()[1]
                    tracemalloc.stop()
            # Eager retention would hold every captured padded body
            # simultaneously at quarantine (peak >= corpus); streaming stays far
            # below it.
            captured = [name for name in bodies if name not in
                        ("service_after", "layer_after", "count_after", "ids_after")]
            corpus = sum(len(bodies[name]) for name in captured)
            self.assertGreater(corpus, 8 * padding)
            self.assertLess(peak, corpus)
            keys = set(storage.list_prefix())
            self.assertEqual(len([key for key in keys if key.endswith("metadata/failure.json")]), 1)
            for name in captured:
                self.assertTrue(any(key.endswith(f"raw/{name}.body") for key in keys), name)
                self.assertTrue(any(key.endswith(f"raw/{name}.wire") for key in keys), name)
            self.assertFalse(any(key.startswith(source.DATASET_SLUG + "/") for key in keys))
            attempt_root = next((root / "captures").iterdir())
            table = pq.read_table(str(attempt_root / "candidate.geoparquet"))
            self.assertEqual(table.num_rows, count - page_size)


if __name__ == "__main__":
    unittest.main()
