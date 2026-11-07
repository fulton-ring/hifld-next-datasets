"""Focused contract checks for the FEMA NFHL Nodes source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import nfhl_nodes as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                   b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
        {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
        {"name": "WTR_NM", "type": "esriFieldTypeString", "alias": "WTR_NM", "length": 100, "domain": None},
        {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "NODE_ID", "type": "esriFieldTypeString", "alias": "NODE_ID", "length": 32, "domain": None},
        {"name": "NODE_TYP", "type": "esriFieldTypeString", "alias": "NODE_TYP", "length": 16, "domain": None},
        {"name": "NODE_DESC", "type": "esriFieldTypeString", "alias": "NODE_DESC", "length": 100, "domain": None},
        {"name": "MODEL_ID", "type": "esriFieldTypeString", "alias": "MODEL_ID", "length": 107, "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]
    layer = {"id": 11, "name": "Nodes", "type": "Feature Layer", "geometryType": "esriGeometryPoint",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 11, "name": "Nodes", "type": "Feature Layer"}]}
    attrs = lambda oid, node_id: {"OBJECTID": oid, "DFIRM_ID": "01009C", "VERSION_ID": "2.3.3.3",
                                  "WTR_NM": "Aaron Branch", "SOURCE_CIT": "01009C_STUDY2",
                                  "NODE_ID": node_id, "NODE_TYP": "Junction", "NODE_DESC": "NP",
                                  "MODEL_ID": "Approximate Model", "GFID": "ab1bd93a-570b-4cea-8ac3-782f01eaf93f",
                                  "GlobalID": "{9585E72B-E15E-4949-8B7A-6D7D29569B64}"}
    features = [
        {"attributes": attrs(10, "01009C_1"), "geometry": {"x": -86.41197568433415, "y": 34.148463868949534}},
        {"attributes": attrs(11, "01009C_2"), "geometry": None},
        {"attributes": attrs(12, "01009C_3"), "geometry": {"x": "NaN", "y": "NaN"}},
    ]
    ids = [10, 11, 12]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    geom = [[10, -86.41197568433415, 34.148463868949534], [11, None, None], [12, "NaN", "NaN"]]
    crs = {"wkid": 4269, "latestWkid": 4269}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 3}, "count_after": {"count": 3},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000": page0, "page_001": page1}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return fields, features, captures, schema, geom


def _pins(features, schema, geom):
    return dict(EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_NULL_GEOMETRY_COUNT=1, EXPECTED_NAN_GEOMETRY_COUNT=1,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(features)).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(11, "Nodes", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -160.0, "ymin": 20.0, "xmax": -60.0, "ymax": 65.0},
                EXPECTED_OBSERVED_BOUNDS={"xmin": -86.41197568433415, "ymin": 34.148463868949534,
                                          "xmax": -86.41197568433415, "ymax": 34.148463868949534})


class NfhlNodesTests(unittest.TestCase):
    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11]), ("ids_before", [11, 12, 10]), ("ids_after", [10, 11, 12])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = [json.loads(captures[f"page_{i:03d}"][0])["features"] for i in range(2)]
        features = [row for page in features for row in page]
        record_sha = hashlib.sha256(_canonical(features)).hexdigest()
        with patch.multiple(source, **{**_pins(features, schema, geom), "EXPECTED_RECORD_SHA256": record_sha}):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)

    def test_missing_service_documentation_link_is_rejected(self):
        _, _, captures, schema, geom = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        features = [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._validate_captures(captures)

    def test_crs_drift_is_rejected(self):
        _, _, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_000_crs_drift"):
                source._validate_captures(captures)

    def test_native_nad83_fields_and_null_geometry_preserved_in_parquet(self):
        fields, features, captures, schema, geom = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12])
        self.assertEqual(table.column("NODE_ID").to_pylist(), ["01009C_1", "01009C_2", "01009C_3"])
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        self.assertEqual(table.column("NODE_ID").to_pylist()[2], "01009C_3")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, _, captures, schema, geom = _fixture()
        final = json.loads(captures["page_001"][0])
        final["exceededTransferLimit"] = True
        captures["page_001"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, _, captures, schema, geom = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, _, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"] = page["features"][:1]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(captures)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["NODE_TYP"] = "Changed"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -86.41, "ymin": 34.14, "xmax": -86.41, "ymax": 34.15}
        pins["EXPECTED_NULL_GEOMETRY_COUNT"] = 1
        pins["EXPECTED_NAN_GEOMETRY_COUNT"] = 1
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

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
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-07T00:00:00+00:00",
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

        with tempfile.TemporaryDirectory(prefix="nfhl-nodes-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_nodes(storage, version="vfixture",
                                                      capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 3)
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
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 1)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                        version=candidate["version"], quality_dict=summary.quality_manifest,
                                        dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_nodes(storage, version="vfixture",
                                                   capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
