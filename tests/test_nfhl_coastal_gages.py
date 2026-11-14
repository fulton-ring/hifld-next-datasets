"""Focused contract checks for the FEMA NFHL Coastal Gages source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_coastal_gages as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "CSTGAGE_ID", "type": "esriFieldTypeString", "alias": "CSTGAGE_ID", "length": 32, "domain": None},
    {"name": "CST_MDL_ID", "type": "esriFieldTypeString", "alias": "CST_MDL_ID", "length": 32, "domain": None},
    {"name": "GAGE_NM", "type": "esriFieldTypeString", "alias": "GAGE_NM", "length": 150, "domain": None},
    {"name": "AGENCY", "type": "esriFieldTypeString", "alias": "AGENCY", "length": 150, "domain": None},
    {"name": "REC_INTVL", "type": "esriFieldTypeString", "alias": "REC_INTVL", "length": 11, "domain": None},
    {"name": "TIME_UNIT", "type": "esriFieldTypeString", "alias": "TIME_UNIT", "length": 7, "domain": None},
    {"name": "START_PD", "type": "esriFieldTypeDate", "alias": "START_PD", "length": 8, "domain": None},
    {"name": "START_TIME", "type": "esriFieldTypeString", "alias": "START_TIME", "length": 10, "domain": None},
    {"name": "END_PD", "type": "esriFieldTypeDate", "alias": "END_PD", "length": 8, "domain": None},
    {"name": "END_TIME", "type": "esriFieldTypeString", "alias": "END_TIME", "length": 10, "domain": None},
    {"name": "GAGE_TYPE", "type": "esriFieldTypeString", "alias": "GAGE_TYPE", "length": 25, "domain": None},
    {"name": "V_DATUM", "type": "esriFieldTypeString", "alias": "V_DATUM", "length": 17, "domain": None},
    {"name": "DATUM_CONV", "type": "esriFieldTypeDouble", "alias": "DATUM_CONV", "domain": None},
    {"name": "TIDE_TF", "type": "esriFieldTypeString", "alias": "TIDE_TF", "length": 1, "domain": None},
    {"name": "TIDE_EPOCH", "type": "esriFieldTypeString", "alias": "TIDE_EPOCH", "length": 11, "domain": None},
    {"name": "TIDE_VBM", "type": "esriFieldTypeString", "alias": "TIDE_VBM", "length": 11, "domain": None},
    {"name": "WVDIR_TF", "type": "esriFieldTypeString", "alias": "WVDIR_TF", "length": 1, "domain": None},
    {"name": "WVSPEC_TF", "type": "esriFieldTypeString", "alias": "WVSPEC_TF", "length": 1, "domain": None},
    {"name": "WDSPD_TF", "type": "esriFieldTypeString", "alias": "WDSPD_TF", "length": 1, "domain": None},
    {"name": "WDDIR_TF", "type": "esriFieldTypeString", "alias": "WDDIR_TF", "length": 1, "domain": None},
    {"name": "WDSTN_HT", "type": "esriFieldTypeInteger", "alias": "WDSTN_HT", "domain": None},
    {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]
ATTR_NAMES = [f["name"] for f in FIELDS if f["name"] != "SHAPE"]

POINT_A = {"x": -146.36167678737675, "y": 61.13199762857323}
POINT_B = {"x": -67.0, "y": 24.6}
POINT_NAN = {"x": "NaN", "y": "NaN"}


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "DFIRM_ID": "060298", "VERSION_ID": "2.3.2.0", "CSTGAGE_ID": f"060298_{oid}",
              "CST_MDL_ID": f"060298_{oid}", "GAGE_NM": "San Francisco (9414290)", "AGENCY": "NOAA",
              "REC_INTVL": "", "TIME_UNIT": "", "START_PD": -3645043200000, "START_TIME": "",
              "END_PD": 218330035200000, "END_TIME": "", "GAGE_TYPE": "Tide", "V_DATUM": "MLLW",
              "DATUM_CONV": -0.06, "TIDE_TF": "T", "TIDE_EPOCH": "1983-2001", "TIDE_VBM": "",
              "WVDIR_TF": "U", "WVSPEC_TF": "U", "WDSPD_TF": "T", "WDDIR_TF": "T", "WDSTN_HT": 24,
              "SOURCE_CIT": "060298_STUDY3", "GFID": "b85f29b4-ba63-47aa-a416-b802465d8ae3",
              "GlobalID": "{B9CA4E3D-5020-442C-BEE8-982126923003}"}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _fixture():
    fields = FIELDS
    layer = {"id": 9, "name": "Coastal Gages", "type": "Feature Layer", "geometryType": "esriGeometryPoint",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 9, "name": "Coastal Gages", "type": "Feature Layer"}]}
    features = [
        {"attributes": _attrs(10), "geometry": POINT_A},
        {"attributes": _attrs(11), "geometry": POINT_B},
        {"attributes": _attrs(12), "geometry": None},
        {"attributes": _attrs(13), "geometry": POINT_NAN},
        {"attributes": _attrs(14), "geometry": POINT_B},
    ]
    ids = [10, 11, 12, 13, 14]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    geom_records = [[10, POINT_A["x"], POINT_A["y"]], [11, POINT_B["x"], POINT_B["y"]],
                    [12, None, None], [13, "NaN", "NaN"], [14, POINT_B["x"], POINT_B["y"]]]
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
        if isinstance(geom, dict) and type(geom.get("x")) in (int, float) and type(geom.get("y")) in (int, float):
            values.extend((geom["x"], geom["y"]))
    return {"xmin": min(values[0::2]), "ymin": min(values[1::2]),
            "xmax": max(values[0::2]), "ymax": max(values[1::2])}


def _pins(features, schema, geom_records):
    return dict(EXPECTED_COUNT=5, PAGE_SIZE=2, EXPECTED_NULL_GEOMETRY_COUNT=1,
                EXPECTED_NAN_GEOMETRY_COUNT=1,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(sorted(features,
                                                                         key=lambda row: row["attributes"]["OBJECTID"]))).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(sorted(geom_records))).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(9, "Coastal Gages", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS=_bounds(features))


class NfhlCoastalGagesTests(unittest.TestCase):
    def test_native_fields_ids_and_points_preserved_in_parquet(self):
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
        self.assertEqual(table.column_names, [f["name"] for f in FIELDS])
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14])
        self.assertEqual(table.column("CSTGAGE_ID").to_pylist(),
                         ["060298_10", "060298_11", "060298_12", "060298_13", "060298_14"])
        self.assertEqual(table.column("GlobalID").to_pylist(),
                         ["{B9CA4E3D-5020-442C-BEE8-982126923003}"] * 5)
        self.assertEqual(table.column("START_PD").type.__str__(), "int64")
        self.assertEqual(table.column("START_PD").to_pylist()[0], -3645043200000)
        self.assertEqual(table.column("WDSTN_HT").to_pylist()[0], 24)
        self.assertEqual(table.column("DATUM_CONV").type.__str__(), "double")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        self.assertIsNone(table.column("SHAPE").to_pylist()[2])
        self.assertIsNone(table.column("SHAPE").to_pylist()[3])
        self.assertEqual(table.column("CSTGAGE_ID").to_pylist()[3], "060298_13")
        point_10 = shapely.from_wkb(table.column("SHAPE").to_pylist()[0])
        self.assertEqual((point_10.x, point_10.y), (POINT_A["x"], POINT_A["y"]))
        point_14 = shapely.from_wkb(table.column("SHAPE").to_pylist()[4])
        self.assertEqual((point_14.x, point_14.y), (POINT_B["x"], POINT_B["y"]))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Point"])

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

    def test_id_roster_population_drift_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        ids = json.loads(captures["ids_before"][0])
        ids["objectIds"] = ids["objectIds"][:-1]
        captures["ids_before"] = (json.dumps(ids, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
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
        layer["geometryType"] = "esriGeometryPolygon"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "exact_nfhl_coastal_gages_layer_metadata_drift"):
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

    def test_renamed_source_id_field_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][3]["name"] = "CSTGAGE_ID_RENAMED"
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
        page["features"][0]["attributes"]["TIDE_TF"] = "F"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_coordinate_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = -146.361
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validated(features, captures, schema, geom)

    def test_out_of_range_coordinate_is_rejected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = 200.0
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(3) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
            self._validated(features, captures, schema, geom)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -146.4, "ymin": 24.6, "xmax": -66.9, "ymax": 61.2}
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
                          "geoparquet/nfhl-coastal-gages.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-coastal-gages.parquet", b"accepted")
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

        with tempfile.TemporaryDirectory(prefix="nfhl-coastal-gages-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_coastal_gages(storage, version="vfixture",
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
                reused = source.capture_nfhl_coastal_gages(storage, version="vfixture",
                                                           capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
