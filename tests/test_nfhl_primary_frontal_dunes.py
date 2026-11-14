"""Focused contract checks for the FEMA NFHL Primary Frontal Dunes source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_primary_frontal_dunes as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "PFD_ID", "type": "esriFieldTypeString", "alias": "PFD_ID", "length": 32, "domain": None},
    {"name": "VZONE_LIMT", "type": "esriFieldTypeString", "alias": "VZONE_LIMT", "length": 1, "domain": None},
    {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]
ATTR_NAMES = [f["name"] for f in FIELDS if f["name"] != "SHAPE"]

# Verbatim producer paths (Esri JSON polyline semantics; not closed, no orientation rule).
PATH_A = [[-124.09437491279175, 41.27902871983497], [-124.09436942264256, 41.27867573283896],
          [-124.09440864341985, 41.27859493780193]]
PATH_B = [[-70.02, 41.31], [-70.01, 41.32], [-70.00, 41.33]]
PATH_C = [[-71.5, 41.2], [-71.4, 41.21]]


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "DFIRM_ID": "06023C", "VERSION_ID": "2.3.2.0", "PFD_ID": f"06023C_{oid}",
              "VZONE_LIMT": "T", "SOURCE_CIT": "06023C_STUDY9",
              "SHAPE.STLength()": 0.014328450871417635,
              "GFID": None,
              "GlobalID": "{C21D80FE-FFD6-41E4-891B-B1B906AD851A}"}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _fixture():
    fields = FIELDS
    layer = {"id": 25, "name": "Primary Frontal Dunes", "type": "Feature Layer",
             "geometryType": "esriGeometryPolyline",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -124.6744246263649, "ymin": 25.910830204720014,
                        "xmax": -67.30290505374359, "ymax": 48.3433236807993,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 25, "name": "Primary Frontal Dunes", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attrs(10), "geometry": {"paths": [PATH_A]}},
        {"attributes": _attrs(11), "geometry": {"paths": [PATH_B, PATH_C]}},
        {"attributes": _attrs(12, **{"SHAPE.STLength()": None}), "geometry": None},
        {"attributes": _attrs(13), "geometry": {"paths": [[["NaN", "NaN"], ["NaN", "NaN"]]]}},
        {"attributes": _attrs(14), "geometry": {"paths": [PATH_B, PATH_C, PATH_A]}},
    ]
    ids = [10, 11, 12, 13, 14]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    geom_records = [[10, [PATH_A]], [11, [PATH_B, PATH_C]], [12, None, None],
                    [13, [[["NaN", "NaN"], ["NaN", "NaN"]]]],
                    [14, [PATH_B, PATH_C, PATH_A]]]
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
        if not geom or "paths" not in geom:
            continue
        for path in geom["paths"]:
            for vertex in path:
                if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                    values.extend(vertex)
    if not values:
        return None
    return {"xmin": min(values[0::2]), "ymin": min(values[1::2]),
            "xmax": max(values[0::2]), "ymax": max(values[1::2])}


def _pins(features, schema, geom_records):
    return dict(EXPECTED_COUNT=5, PAGE_SIZE=2, EXPECTED_NULL_GEOMETRY_COUNT=1,
                EXPECTED_NAN_GEOMETRY_COUNT=1,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(sorted(features,
                                                                         key=lambda row: row["attributes"]["OBJECTID"]))).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom_records)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(25, "Primary Frontal Dunes", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -124.6744246263649, "ymin": 25.910830204720014,
                                       "xmax": -67.30290505374359, "ymax": 48.3433236807993},
                EXPECTED_OBSERVED_BOUNDS=_bounds(features))


class NfhlPrimaryFrontalDunesTests(unittest.TestCase):
    def test_native_paths_and_null_geometry_preserved_in_parquet(self):
        fields, features, captures, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14])
        self.assertEqual(table.column("PFD_ID").to_pylist(),
                         ["06023C_10", "06023C_11", "06023C_12", "06023C_13", "06023C_14"])
        self.assertEqual(table.column("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE.STLength()").to_pylist()[0], 0.014328450871417635)
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("VZONE_LIMT").to_pylist()[0], "T")
        line_10 = shapely.from_wkb(table.column("SHAPE").to_pylist()[0])
        self.assertIsInstance(line_10, shapely.LineString)
        self.assertEqual([list(coordinate) for coordinate in line_10.coords], PATH_A)
        multi_11 = shapely.from_wkb(table.column("SHAPE").to_pylist()[1])
        self.assertIsInstance(multi_11, shapely.MultiLineString)
        self.assertEqual([[list(coordinate) for coordinate in line.coords] for line in multi_11.geoms],
                         [PATH_B, PATH_C])
        self.assertIsNone(table.column("SHAPE").to_pylist()[2])
        self.assertIsNone(table.column("SHAPE").to_pylist()[3])
        self.assertIsNone(table.column("SHAPE.STLength()").to_pylist()[2])
        self.assertEqual(table.column("SHAPE.STLength()").to_pylist()[3], 0.014328450871417635)
        multi_14 = shapely.from_wkb(table.column("SHAPE").to_pylist()[4])
        self.assertIsInstance(multi_14, shapely.MultiLineString)
        self.assertEqual([[list(coordinate) for coordinate in line.coords] for line in multi_14.geoms],
                         [PATH_B, PATH_C, PATH_A])
        self.assertEqual(table.column("PFD_ID").to_pylist()[3], "06023C_13")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["LineString", "MultiLineString"])

    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13]), ("ids_before", [11, 12, 10, 13, 14]),
                          ("ids_after", [10, 11, 12, 13, 14])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        _, _, extracted = self._validated(features, captures, schema, geom)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)

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

    def test_rotating_colorramp_palette_is_not_layer_metadata_drift(self):
        _, features, captures, schema, geom = _fixture()
        for name in ("layer_before", "layer_after"):
            layer = json.loads(captures[name][0])
            layer["drawingInfo"] = {"renderer": {"authoringInfo": {
                "colorRamp": {"fromColor": [207, 220, 252, 255] if name == "layer_before" else [231, 202, 252, 255]}}}}
            captures[name] = (json.dumps(layer, separators=(",", ":")).encode(), captures[name][1])
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        _, _, extracted = self._validated(features, captures, schema, geom)
        self.assertEqual(extracted["audit"]["source_feature_count"], 5)

    def test_other_drawinginfo_change_is_layer_metadata_drift(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_after"][0])
        layer["drawingInfo"] = {"renderer": {"type": "changed"}}
        captures["layer_after"] = (json.dumps(layer, separators=(",", ":")).encode(), captures["layer_after"][1])
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
            self._validated(features, captures, schema, geom)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPoint"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "exact_nfhl_primary_frontal_dunes_layer_metadata_drift"):
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
        page["features"][0]["attributes"]["VZONE_LIMT"] = "Changed"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_vertex_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["paths"][0][1] = [-87.924, 30.487]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_coordinate_range_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["paths"][0][0] = [-200.0, 30.48]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
            self._validated(features, captures, schema, geom)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -87.94, "ymin": 30.48, "xmax": -87.92, "ymax": 30.49}
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
                          "geoparquet/nfhl-primary-frontal-dunes.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-primary-frontal-dunes.parquet", b"accepted")
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

        with tempfile.TemporaryDirectory(prefix="nfhl-primary-frontal-dunes-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_primary_frontal_dunes(storage, version="vfixture",
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
                reused = source.capture_nfhl_primary_frontal_dunes(storage, version="vfixture",
                                                               capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
