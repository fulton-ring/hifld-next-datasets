"""Focused contract checks for the FEMA NFHL Seclusion Boundaries source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.sources import nfhl_seclusion_boundaries as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "SUBINFO_ID", "type": "esriFieldTypeString", "alias": "SUBINFO_ID", "length": 32, "domain": None},
    {"name": "CASE_NO", "type": "esriFieldTypeString", "alias": "CASE_NO", "length": 13, "domain": None},
    {"name": "CASE_DESC", "type": "esriFieldTypeString", "alias": "CASE_DESC", "length": 254, "domain": None},
    {"name": "SUBMIT_BY", "type": "esriFieldTypeString", "alias": "SUBMIT_BY", "length": 100, "domain": None},
    {"name": "HUC8", "type": "esriFieldTypeString", "alias": "HUC8", "length": 8, "domain": None},
    {"name": "METHOD_TYP", "type": "esriFieldTypeString", "alias": "METHOD_TYP", "length": 28, "domain": None},
    {"name": "COMP_DATE", "type": "esriFieldTypeDate", "alias": "COMP_DATE", "length": 8, "domain": None},
    {"name": "TASK_TYP", "type": "esriFieldTypeString", "alias": "TASK_TYP", "length": 21, "domain": None},
    {"name": "HYDRO_MDL", "type": "esriFieldTypeString", "alias": "HYDRO_MDL", "length": 40, "domain": None},
    {"name": "HYDRA_MDL", "type": "esriFieldTypeString", "alias": "HYDRA_MDL", "length": 83, "domain": None},
    {"name": "CST_MDL_ID", "type": "esriFieldTypeString", "alias": "CST_MDL_ID", "length": 32, "domain": None},
    {"name": "TOPO_SRC", "type": "esriFieldTypeString", "alias": "TOPO_SRC", "length": 254, "domain": None},
    {"name": "TOPO_V_ACC", "type": "esriFieldTypeString", "alias": "TOPO_V_ACC", "length": 254, "domain": None},
    {"name": "TOPO_H_ACC", "type": "esriFieldTypeString", "alias": "TOPO_H_ACC", "length": 254, "domain": None},
    {"name": "EFF_DATE", "type": "esriFieldTypeDate", "alias": "EFF_DATE", "length": 8, "domain": None},
    {"name": "CONTRCT_NO", "type": "esriFieldTypeString", "alias": "CONTRCT_NO", "length": 50, "domain": None},
    {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "SHAPE.STArea()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STArea()", "domain": None},
    {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]

EXTENT = {"xmin": -159.6874999656741, "ymin": 18.470550941268982,
          "xmax": -66.29530305752041, "ymax": 64.87501005881364}


def _fixture(*, ids_value=None, count=0):
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in FIELDS]
    layer = {"id": 29, "name": "Seclusion Boundaries", "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {**EXTENT, "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": FIELDS}
    service = {"mapName": "NFHL",
               "layers": [{"id": 29, "name": "Seclusion Boundaries", "type": "Feature Layer"}]}
    empty_ids = {"objectIdFieldName": "OBJECTID", "objectIds": ids_value}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": count}, "count_after": {"count": count},
            "ids_seed": empty_ids, "ids_before": empty_ids, "ids_after": empty_ids}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return layer, captures, schema


def _pins(schema):
    return dict(EXPECTED_COUNT=0, EXPECTED_ID_SHA256=source.EMPTY_SHA256,
                EXPECTED_RECORD_SHA256=source.EMPTY_SHA256,
                EXPECTED_GEOMETRY_SHA256=source.EMPTY_SHA256,
                EXPECTED_NULL_GEOMETRY_COUNT=0, EXPECTED_NAN_GEOMETRY_COUNT=0,
                EXPECTED_OBSERVED_BOUNDS=None,
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(29, "Seclusion Boundaries", "Feature Layer")],
                EXPECTED_LAYER_EXTENT=EXTENT)


class NfhlSeclusionBoundariesTests(unittest.TestCase):
    def test_null_and_empty_id_rosters_are_the_same_pinned_empty_population(self):
        for ids_value in (None, []):
            _, captures, schema = _fixture(ids_value=ids_value)
            with patch.multiple(source, **_pins(schema)):
                _, _, extracted = source._validate_captures(captures)
            self.assertEqual(extracted["features"], [])
            self.assertEqual(extracted["ids"], [])
            self.assertEqual(extracted["audit"]["unique_source_id_count"], 0)
            self.assertEqual(extracted["audit"]["source_record_sha256"], source.EMPTY_SHA256)

    def test_populated_id_roster_fails_closed(self):
        _, captures, schema = _fixture(ids_value=[10, 11])
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_populated_ids_while_pinned_empty"):
                source._validate_captures(captures)

    def test_populated_count_fails_closed(self):
        _, captures, schema = _fixture(count=2)
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                source._validate_captures(captures)

    def test_count_changed_during_capture_is_rejected(self):
        _, captures, schema = _fixture()
        captures["count_after"] = (json.dumps({"count": 0, "truncated": True}).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_count_changed_during_capture"):
                source._validate_captures(captures)

    def test_unexpected_page_capture_is_rejected(self):
        _, captures, schema = _fixture()
        captures["page_000000_00001"] = (json.dumps({"features": []}).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_missing_service_documentation_link_is_rejected(self):
        _, captures, schema = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._validate_captures(captures)

    def test_extent_crs_drift_is_rejected(self):
        _, captures, schema = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["extent"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_layer_extent_crs_drift"):
                source._validate_captures(captures)

    def test_layer_metadata_change_during_capture_is_rejected(self):
        _, captures, schema = _fixture()
        layer = json.loads(captures["layer_after"][0])
        layer["description"] = "changed"
        captures["layer_after"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
                source._validate_captures(captures)

    def test_exact_layer_vanishing_from_service_doc_is_rejected(self):
        _, captures, schema = _fixture()
        service = json.loads(captures["service_before"][0])
        service["layers"] = [{"id": 29, "name": "Renamed Layer", "type": "Feature Layer"}]
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **{**_pins(schema),
                                       "EXPECTED_SERVICE_LAYER_ROSTER": [(29, "Renamed Layer", "Feature Layer")]}):
            with self.assertRaisesRegex(ValueError, "exact_nfhl_seclusion_boundaries_layer_missing"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, captures, schema = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_renamed_id_field_is_rejected(self):
        _, captures, schema = _fixture()
        doc = json.loads(captures["ids_before"][0])
        doc["objectIdFieldName"] = "OBJECTID_2"
        captures["ids_before"] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(schema)):
            with self.assertRaisesRegex(ValueError, "source_id_field_changed"):
                source._validate_captures(captures)

    def test_schema_only_geoparquet_preserves_producer_fields_and_types(self):
        _, _, schema = _fixture()
        body = source._parquet_bytes(FIELDS, [])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 0)
        self.assertEqual([field.name for field in table.schema],
                         [field["name"] for field in FIELDS])
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.schema.field("COMP_DATE").type.__str__(), "int64")
        self.assertEqual(table.schema.field("EFF_DATE").type.__str__(), "int64")
        self.assertEqual(table.schema.field("SHAPE.STArea()").type.__str__(), "double")
        self.assertEqual(table.schema.field("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.schema.field("GlobalID").type.__str__(), "string")
        self.assertEqual(table.schema.field("SHAPE").type.__str__(), "binary")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_polygon_wkb_semantics_follow_producer_ring_contract(self):
        exterior = [[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [10.0, 0.0], [0.0, 0.0]]  # clockwise
        hole = [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]          # counterclockwise
        wkb = source._polygon_wkb({"rings": [exterior, hole]})
        geometry = from_wkb(wkb)
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertEqual(len(geometry.interiors), 1)
        self.assertAlmostEqual(geometry.area, 96.0, places=6)
        multipart = source._polygon_wkb({"rings": [exterior, hole,
                                                   [[20.0, 0.0], [20.0, 5.0], [30.0, 5.0], [30.0, 0.0],
                                                    [20.0, 0.0]]]})
        self.assertEqual(from_wkb(multipart).geom_type, "MultiPolygon")
        self.assertIsNone(source._polygon_wkb(None))
        self.assertIsNone(source._polygon_wkb({"rings": []}))
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_degenerate"):
            source._polygon_wkb({"rings": [[[0.0, 0.0], [1.0, 1.0]]]})
        with self.assertRaisesRegex(ValueError, "source_polygon_exterior_missing"):
            source._polygon_wkb({"rings": [[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
                                           [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]]})
        self.assertIsNone(source._polygon_wkb(
            {"rings": [[[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], ["NaN", "NaN"], [0.0, 0.0]]]}))
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_degenerate"):
            source._polygon_wkb({"rings": [[[0.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]]})
        with self.assertRaisesRegex(ValueError, "source_polygon_hole_unassigned"):
            source._polygon_wkb({"rings": [exterior, [[50.0, 50.0], [52.0, 50.0], [52.0, 52.0],
                                                      [50.0, 52.0], [50.0, 50.0]]]})

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
                          "geoparquet/nfhl-seclusion-boundaries.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-seclusion-boundaries.parquet", b"accepted")
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

        _, fixture_captures, schema = _fixture()
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

        with tempfile.TemporaryDirectory(prefix="nfhl-seclusion-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(schema)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_seclusion_boundaries(storage, version="vfixture",
                                                                     capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 0)
                self.assertEqual(candidate["source_null_geometry_count"], 0)
                self.assertEqual(candidate["source_nan_string_geometry_count"], 0)
                self.assertFalse(candidate["release_accepted"])
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
                self.assertEqual(summary.quality_manifest["feature_count"], 0)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_seclusion_boundaries(storage, version="vfixture",
                                                                  capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
