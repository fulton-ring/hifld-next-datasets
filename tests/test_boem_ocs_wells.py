"""Focused contract and repeat-acquisition tests for the BOEM OCS wells adapter."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import boem_ocs_wells as boem
from dagster_hifld.sources import epa_frs_common


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _fixture():
    fields = [
        {"name": "COMPLETION", "type": "esriFieldTypeDate", "nullable": True, "length": 8},
        {"name": "Y", "type": "esriFieldTypeDouble", "nullable": True},
        {"name": "X", "type": "esriFieldTypeDouble", "nullable": True},
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
        {"name": "API_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "OPERATOR", "type": "esriFieldTypeString", "nullable": True, "length": 50},
        {"name": "WELL_NAME", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "SPUD_DATE", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "TYPE_CODE", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "TYPE_CODE_DESC", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "STATUS", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "DEPTH", "type": "esriFieldTypeString", "nullable": True, "length": 20},
        {"name": "STATUS_DESCRIPTION", "type": "esriFieldTypeString", "nullable": True, "length": 40},
    ]
    ids = [1, 2, 3, 4]
    api = ["427054003400", "427054003800", "427054004100", "177024107800"]
    wells = [(" A005", "9/8/1976"), (" A007", "12/30/1976"), (" A009", "6/22/1977"), (" B101", None)]
    completions = [1704112496789, None, None, None]
    features = []
    for oid, number, (name, spud), completion in zip(ids, api, wells, completions):
        features.append({
            "attributes": {"COMPLETION": completion, "Y": 28.0146142 + oid / 1e6, "X": -95.87735874 - oid / 1e6,
                           "OBJECTID": oid, "API_NUMBER": number, "OPERATOR": "02451", "WELL_NAME": name,
                           "SPUD_DATE": spud, "TYPE_CODE": "D", "TYPE_CODE_DESC": "Development",
                           "STATUS": "PA", "DEPTH": "155", "STATUS_DESCRIPTION": "Permanently Abandoned"},
            "geometry": {"x": -95.87759523099999 - oid / 1e6, "y": 28.014903194999988 + oid / 1e6},
        })
    layer = {"id": boem.LAYER_ID, "name": boem.LAYER_NAME, "geometryType": "esriGeometryPoint",
             "objectIdField": "OBJECTID", "hasZ": False, "hasM": False, "maxRecordCount": 10000,
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"spatialReference": {"wkid": 4269, "latestWkid": 4269}}, "fields": fields}
    service = {"serviceDescription": "Map Service contains primary data types created by both the Bureau of "
                                     "Ocean Energy Management (BOEM) and the Bureau of Safety and Environmental "
                                     "Enforcement (BSEE). Contact the Geospatial Services Division at "
                                     "mappingboundarybranch@boem.gov.",
               "spatialReference": {"wkid": 4269, "latestWkid": 4269},
               "layers": [{"id": boem.LAYER_ID, "name": boem.LAYER_NAME}]}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    page = {"spatialReference": {"wkid": 4269, "latestWkid": 4269}, "features": features}
    docs = {
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
        "page_000": page,
    }
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    ordered = sorted(ids)
    page_urls = [boem._query(where="1=1", outFields="*", returnGeometry="true", outSR="4269",
                             orderByFields="OBJECTID ASC", resultOffset=page * boem.PAGE_SIZE,
                             resultRecordCount=boem.PAGE_SIZE)
                 for page in range(0, (len(ordered) + boem.PAGE_SIZE - 1) // boem.PAGE_SIZE)]
    for key, value in docs.items():
        body = _body(value)
        url = (boem.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               boem.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               boem.LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json" if key.startswith("count_") else
               boem.LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json" if key.startswith("ids_") else
               page_urls[int(key.split("_")[1])])
        captures[key] = (body, {**receipt, "url": url, "final_url": url, "bytes": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(body),
                                "wire_sha256": hashlib.sha256(body).hexdigest()})
    return captures


class BoemOcsWellsTests(unittest.TestCase):
    def test_native_fields_points_and_audit_counts(self):
        _, fields, features, audit = boem._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], boem.FIELD_NAMES)
        self.assertEqual(len(features), 4)
        self.assertEqual(audit["unique_objectid_count"], 4)
        self.assertEqual(audit["unique_api_number_count"], 4)
        self.assertEqual(audit["duplicate_api_number_count"], 0)
        self.assertEqual(audit["null_counts_by_field"]["SPUD_DATE"], 1)
        self.assertEqual(audit["source_crs_epsg"], 4269)

    def test_spatial_reference_alias_is_valid_but_contradictions_are_rejected(self):
        captures = _fixture()
        _, _, _, audit = boem._validate(captures)
        self.assertEqual(audit["source_crs_epsg"], 4269)

        candidate = copy.deepcopy(captures)
        doc = json.loads(candidate["layer_before"][0])
        doc["extent"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
        candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
        with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
            boem._validate(candidate)

        candidate = copy.deepcopy(captures)
        doc = json.loads(candidate["page_000"][0])
        doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
        candidate["page_000"] = (_body(doc), candidate["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_page_spatial_reference_drift"):
            boem._validate(candidate)

        candidate = copy.deepcopy(captures)
        doc = json.loads(candidate["page_000"][0])
        doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
        candidate["page_000"] = (_body(doc), candidate["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
            boem._validate(candidate)

    def test_missing_id_truncation_and_schema_drift_are_rejected(self):
        captures = _fixture()
        doc = json.loads(captures["page_000"][0]); doc["features"].pop()
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["attributes"].pop("OBJECTID")
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["attributes"]["OBJECTID"] = 999
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["attributes"]["API_NUMBER"] = None
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_api_number_missing"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["attributes"]["UNEXPECTED_COLUMN"] = "x"
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_field_roster_drift"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["attributes"]["STATUS"] = 7
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_string_field_type_drift"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["geometry"] = {"x": -95.0, "y": 28.0, "z": 3.0}
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_point_geometry_invalid"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][0]["geometry"]["y"] = 99.0
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_point_coordinates_invalid"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["exceededTransferLimit"] = True
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["page_000"][0])
        doc["features"][1]["attributes"]["OBJECTID"], doc["features"][0]["attributes"]["OBJECTID"] = (
            doc["features"][0]["attributes"]["OBJECTID"], doc["features"][1]["attributes"]["OBJECTID"])
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_page_order_drift"):
            boem._validate(captures)

        captures = _fixture()
        doc = json.loads(captures["layer_before"][0])
        doc["fields"][4]["type"] = "esriFieldTypeInteger"
        captures["layer_before"] = (_body(doc), captures["layer_before"][1])
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            boem._validate(captures)

    def test_same_content_reuses_original_receipts_and_changed_content_versions(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        def parquet(features, _fields, _root):
            return b"candidate:" + hashlib.sha256(_body([f["attributes"] for f in features])).digest()
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            def run(observed_at, mutate=False):
                captures = copy.deepcopy(fixture)
                for _, receipt in captures.values(): receipt["observed_at"] = observed_at
                if mutate:
                    doc = json.loads(captures["page_000"][0]); doc["features"][0]["attributes"]["WELL_NAME"] = "Changed"
                    body = _body(doc); receipt = captures["page_000"][1]
                    receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), wire_bytes=len(body), wire_sha256=hashlib.sha256(body).hexdigest())
                    captures["page_000"] = (body, receipt)
                with patch.object(boem, "_load_offline", return_value=captures), \
                     patch.object(boem, "_geoparquet", side_effect=parquet), \
                     patch.object(boem, "processing_identity", return_value=stable_identity), \
                     patch.object(boem, "validate_construction_code_identity", return_value=None), \
                     patch.object(epa_frs_common, "validate_construction_code_identity", return_value=None):
                    return boem.capture_boem_ocs_wells(storage, offline_capture_directory=tmp)

            first = run("2026-01-01T00:00:00Z")
            first_receipts = json.loads(storage.read_key(first["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/source_manifest.json")))
            second = run("2026-01-02T00:00:00Z")
            second_manifest = json.loads(storage.read_key(second["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/source_manifest.json")))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertEqual(first_receipts["inputs"], second_manifest["inputs"])
            changed = run("2026-01-03T00:00:00Z", mutate=True)
            self.assertNotEqual(first["version"], changed["version"])

    def test_manifest_records_first_party_host_and_pending_acceptance(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with patch.object(boem, "_load_offline", return_value=copy.deepcopy(fixture)), \
                 patch.object(boem, "_geoparquet", return_value=b"candidate"), \
                 patch.object(boem, "processing_identity", return_value=stable_identity), \
                 patch.object(boem, "validate_construction_code_identity", return_value=None), \
                 patch.object(epa_frs_common, "validate_construction_code_identity", return_value=None):
                result = boem.capture_boem_ocs_wells(storage, offline_capture_directory=tmp)
            base = result["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/")
            manifest = json.loads(storage.read_key(base + "source_manifest.json"))
            self.assertFalse(manifest["source_authority_accepted"])
            self.assertTrue(manifest["source_host_first_party"])
            self.assertTrue(all(host == "gis.boem.gov" for host in
                                [inp["host"] for inp in manifest["inputs"]]))
            self.assertIn("no ArcGIS Online hosting account involved", manifest["source_authority_note"])
            self.assertIn("maintained producer REST publication", manifest["lineage"])
            self.assertIsNone(manifest["source_release_date"])
            self.assertIn("no dated source release", manifest["inputs"][0]["release_date_unknown_reason"])
            audit = json.loads(storage.read_key(base + "source_audit.json"))
            self.assertFalse(audit["source_authority_accepted"])
            ready = json.loads(storage.read_key(base + "candidate_ready.json"))
            self.assertFalse(ready["source_authority_accepted"])
            self.assertFalse(result["source_authority_accepted"])

    def test_entrypoint_quarantines_acquisition_validation_failures(self):
        fixture = _fixture()
        doc = json.loads(fixture["page_000"][0])
        doc["features"].pop()
        fixture["page_000"] = (_body(doc), fixture["page_000"][1])
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with patch.object(boem, "_load_offline", return_value=fixture):
                with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                    boem.capture_boem_ocs_wells(storage, offline_capture_directory=tmp)
            quarantined = sorted(str(path.relative_to(tmp)) for path in Path(tmp).rglob("*") if path.is_file())
            self.assertTrue(quarantined)
            self.assertTrue(all(name.startswith(f"_quarantine/{boem.DATASET_SLUG}/") for name in quarantined),
                            quarantined)
            token_root = Path(tmp) / "_quarantine" / boem.DATASET_SLUG
            tokens = [path for path in token_root.iterdir() if path.is_dir()]
            self.assertEqual(len(tokens), 1)
            failure = json.loads((tokens[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "source_feature_population_incomplete")
            self.assertEqual(sorted(failure["captured_names"]), sorted(fixture))
            for name in fixture:
                self.assertTrue((tokens[0] / "raw" / f"{name}.body").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.wire").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.receipt.json").is_file(), name)

    def test_geoparquet_roundtrip_preserves_native_values(self):
        fixture = _fixture()
        _, fields, features, _ = boem._validate(copy.deepcopy(fixture))
        with tempfile.TemporaryDirectory() as tmp:
            body = boem._geoparquet(features, fields, Path(tmp))
        import pyarrow.parquet as pq
        import io
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*boem.FIELD_NAMES, "geometry"])
        self.assertEqual(table.num_rows, 4)
        apis = table.column("API_NUMBER").to_pylist()
        self.assertEqual(apis, ["427054003400", "427054003800", "427054004100", "177024107800"])
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2, 3, 4])
        self.assertEqual(table.column("SPUD_DATE").to_pylist(), ["9/8/1976", "12/30/1976", "6/22/1977", None])
        self.assertEqual(table.schema.field("COMPLETION").type, __import__("pyarrow").timestamp("ms", tz="UTC"))
        self.assertEqual(table.column("COMPLETION")[0].value, 1704112496789)
        self.assertEqual(table.column("COMPLETION").to_pylist()[1:], [None, None, None])
        self.assertEqual(table.column("geometry").to_pylist()[0][:5], b"\x01\x01\x00\x00\x00")


if __name__ == "__main__":
    unittest.main()
