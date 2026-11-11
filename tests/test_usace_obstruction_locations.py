"""Native preservation and completeness checks for the official IENC OBSTRUCTION_POINT layer."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_obstruction_locations as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_obstruction_locations import (
    usace_obstruction_locations_complete, usace_obstruction_locations_draft_schedule,
    usace_obstruction_locations_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


def _capture_doc_values(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "OBSTRUCTION_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "OBSTRUCTION_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": source.LAYER_ID, "name": "OBSTRUCTION_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts",
                     "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/OBSTRUCTION_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            rows.append({"attributes": {
                "OBJECTID": oid, "Category_of_Obstruction": "Submerged pile",
                "Value_of_Sounding": "", "Water_Level": "", "Natural_Surface": "",
                "Object_Name": "Obstruction", "Information": "", "Scale_Minimum": 30000,
                "Source_Date": "20020911", "Source_Indication": "US,U3,SURVY",
                "Source_Dataset": "dataset", "Nature_of_Construction": "",
                "Exposition_of_Sounding": "", "Feature_ID": f"U3 1393267508 {oid:05d}"},
                "geometry": {"x": -91.0 + oid / 1000, "y": 30.5 + oid / 1000}})
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return docs


def _capture_docs(count=source.EXPECTED_COUNT):
    return {name: (json.dumps(doc).encode(), {}) for name, doc in _capture_doc_values(count).items()}


class ObstructionLocationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_obstruction_locations_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_obstruction_locations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_obstruction_locations_complete({"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)
        self.assertFalse(check.metadata["mapping_approved"].value)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_obstruction_locations(storage, version="../outside")

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name, url = source._capture_roster(source.EXPECTED_IDS)[0]
        receipt = {"method": "GET", "url": url, "final_url": url}
        source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = "https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = url
        receipt["method"] = "POST"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)

    def test_complete_population_native_geometry_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_feature_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "U3 1393267508 00001")
        self.assertEqual(features[-1]["attributes"]["OBJECTID"], source.EXPECTED_COUNT)
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(parquet))
        expected_columns = [f["name"] for f in fields if f["name"] != "SHAPE"] + ["geometry"]
        self.assertEqual(table.schema.names, expected_columns)
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(pa.int32()))
        self.assertTrue(table.schema.field("OBJECTID").type.equals(pa.int64()))
        self.assertTrue(table.schema.field("Exposition_of_Sounding").type.equals(pa.string()))
        self.assertEqual(table["Exposition_of_Sounding"].null_count, 0)
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})

    def test_rejects_schema_and_population_drift(self):
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][2]["name"] = "Changed"
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)
        captures = _capture_docs()
        ids = json.loads(captures["ids_after"][0])
        ids["objectIds"] = ids["objectIds"][:-1]
        captures["ids_after"] = (json.dumps(ids).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
            source._validate(captures)

    def test_rejects_geometry_or_declared_type_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = "-91"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 4.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Scale_Minimum"] = "30000"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        del page["features"][0]["attributes"]["Feature_ID"]
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            source._validate(captures)

    def test_rejects_layer_page_and_feature_crs_drift(self):
        mutations = [
            ("layer wkid contradiction", lambda d: d["layer_before"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("layer extent CRS drift", lambda d: d["layer_before"]["extent"].update(
                spatialReference={"wkid": 3857})),
            ("page CRS drift", lambda d: d["page_000"].update(
                spatialReference={"wkid": 3857, "latestWkid": 3857})),
            ("feature CRS drift", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857})),
            ("feature wkid/latestWkid contradiction", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("feature WKT contradiction", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"latestWkid": 4326, "wkt": __import__("pyproj").CRS.from_epsg(3857).to_wkt()})),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                docs = _capture_doc_values()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, "crs_drift|spatial_reference_drift"):
                    source._validate({name: (json.dumps(value).encode(), {}) for name, value in docs.items()})

    def test_rejects_delegation_and_service_identity_drift(self):
        captures = _capture_docs()
        item = json.loads(captures["bts_item"][0])
        item["owner"] = "someone_else"
        captures["bts_item"] = (json.dumps(item).encode(), {})
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            source._validate(captures)
        captures = _capture_docs()
        directory = json.loads(captures["service_directory"][0])
        directory["services"] = [{"name": "IENC_Feature_Classes/PILE_POINT", "type": "MapServer"}]
        captures["service_directory"] = (json.dumps(directory).encode(), {})
        with self.assertRaisesRegex(ValueError, "official_ienc_service_directory_drift"):
            source._validate(captures)
        captures = _capture_docs()
        service = json.loads(captures["service_before"][0])
        service["layers"] = [{"id": 20, "name": "PILE_POINT", "geometryType": "esriGeometryPoint"}]
        captures["service_before"] = (json.dumps(service).encode(), {})
        with self.assertRaisesRegex(ValueError, "official_obstruction_point_service_drift"):
            source._validate(captures)

    def test_rejects_points_outside_geographic_bounds_or_declared_coverage(self):
        mutations = [
            ("latitude beyond CRS bounds", lambda d: d["page_000"]["features"][0]["geometry"].update(y=100.0),
             "source_point_outside_geographic_bounds"),
            ("longitude beyond CRS bounds", lambda d: d["page_000"]["features"][0]["geometry"].update(x=-180.5),
             "source_point_outside_geographic_bounds"),
            ("null-island point outside coverage", lambda d: d["page_000"]["features"][0]["geometry"].update(x=0.0, y=0.0),
             "source_point_outside_declared_coverage"),
            ("in-CRS point outside coverage", lambda d: d["page_000"]["features"][0]["geometry"].update(x=-91.0, y=89.9),
             "source_point_outside_declared_coverage"),
            ("west of declared extent", lambda d: d["page_000"]["features"][0]["geometry"].update(x=-95.5),
             "source_point_outside_declared_coverage"),
            ("beyond tolerance over extent", lambda d: d["page_000"]["features"][0]["geometry"].update(
                x=-91.0, y=source.EXPECTED_EXTENT["ymax"] + 1e-8),
             "source_point_outside_declared_coverage"),
        ]
        for label, mutate, expected in mutations:
            with self.subTest(label=label):
                docs = _capture_doc_values()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate({name: (json.dumps(value).encode(), {}) for name, value in docs.items()})

    def test_accepts_boundary_points_within_represented_extent_and_tolerance(self):
        docs = _capture_doc_values()
        docs["page_000"]["features"][0]["geometry"].update(x=source.EXPECTED_EXTENT["xmin"],
                                                           y=source.EXPECTED_EXTENT["ymin"])
        docs["page_000"]["features"][1]["geometry"].update(
            x=source.EXPECTED_EXTENT["xmax"] - 4.8e-11,
            y=source.EXPECTED_EXTENT["ymax"] + 4.8e-11)
        _fields, features, audit = source._validate({name: (json.dumps(value).encode(), {}) for name, value in docs.items()})
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(features[0]["geometry"]["x"], source.EXPECTED_EXTENT["xmin"])

    def _capture_directory(self, root: Path):
        docs = {name: json.loads(body) for name, (body, _receipt) in _capture_docs().items()}
        receipts = {}
        expected_urls = dict(source._capture_roster(source.EXPECTED_IDS))
        for name, doc in docs.items():
            body = _canonical(doc)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                       "observed_at": "2026-11-11T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body),
                       "bytes": len(body), "sha256": source._sha(body),
                       "content_encoding": "identity", "content_length": len(body), "error_code": None}
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            receipts[name] = receipt
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True, "receipts": receipts,
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity"), patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_usace_obstruction_locations(storage, version="draft", capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], source.EXPECTED_COUNT)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            audit_before = storage.read_key(f"{prefix}/metadata/source_audit.json")
            receipts_before = {name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                               for name in json.loads((root / "capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-11-12T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_usace_obstruction_locations(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before}, receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])
            self.assertFalse(second["release_accepted"])


if __name__ == "__main__":
    unittest.main()
