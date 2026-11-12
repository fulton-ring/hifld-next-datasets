import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.sources import land_region_locations_usace_ienc as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.land_region_locations_usace_ienc import (
    land_region_locations_usace_ienc_complete, land_region_locations_usace_ienc_draft_schedule,
    land_region_locations_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


def _documents():
    service = {"mapName": "LAND_REGION_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 12, "name": "LAND_REGION_POINT", "geometryType": "esriGeometryPoint"}]}
    layer = {"id": 12, "name": "LAND_REGION_POINT", "geometryType": "esriGeometryPoint", "maxRecordCount": 2000,
             "spatialReference": {"wkid": 4326, "latestWkid": 4326},
             "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
             "fields": source.EXPECTED_FIELD_DECLARATIONS}
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts",
                     "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes "
                                    "and USACE_IENC_Master_Service_gdb.zip."},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/LAND_REGION_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": source.EXPECTED_COUNT}, "count_after": {"count": source.EXPECTED_COUNT},
        "ids_before": {"objectIds": source.EXPECTED_IDS}, "ids_after": {"objectIds": source.EXPECTED_IDS},
    }
    for page_index in range(source.PAGE_COUNT):
        page_ids = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            values = {field["name"]: (oid if field["name"] == "OBJECTID" else
                                      60000 if field["name"] == "Scale_Minimum" else
                                      "" if field["name"] == "Category_of_Land_Region" else f"{field['name']}-{oid}")
                      for field in source.ATTRIBUTE_FIELDS}
            x = source.EXPECTED_EXTENT["xmin"] + (oid % 100) / 100
            y = source.EXPECTED_EXTENT["ymin"] + oid / 250
            if oid == 1:
                x, y = source.EXPECTED_EXTENT["xmin"], source.EXPECTED_EXTENT["ymin"]
            if oid == 2:
                x, y = source.EXPECTED_EXTENT["xmax"], source.EXPECTED_EXTENT["ymax"]
            rows.append({"attributes": values, "geometry": {"x": x, "y": y}})
        docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326, "latestWkid": 4326}, "features": rows}
    return docs


class LandRegionLocationsUsaceIencTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(land_region_locations_usace_ienc, hifld_ingest_assets)
        self.assertEqual(land_region_locations_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        passed = land_region_locations_usace_ienc_complete(
            {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        self.assertFalse(passed.metadata["id_contract_accepted"].value)
        failed = land_region_locations_usace_ienc_complete(
            {"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_land_region_locations_usace_ienc(storage, version="../outside")

    def test_complete_capture_contract_pins_population_ids_and_attributes(self):
        fields, features, audit = source._validate({name: (_canonical(doc), {}) for name, doc in _documents().items()})
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_feature_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"] + audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["feature_id_distinct_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["feature_id_null_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["attributes"]["Category_of_Land_Region"], "")
        self.assertEqual(features[0]["geometry"]["x"], source.EXPECTED_EXTENT["xmin"])
        self.assertEqual(audit["source_vertex_bounds"], source.EXPECTED_VERTEX_BOUNDS)

    def test_rejects_schema_id_and_page_population_drift(self):
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["layer_before"]["fields"][2]["name"] = "Renamed"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["count_after"]["count"] = source.EXPECTED_COUNT - 1
        with self.assertRaisesRegex(ValueError, "source_count_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["ids_after"]["objectIds"] = source.EXPECTED_IDS[:-1]
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_007"]["features"].pop()
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete:page_007"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_007"]["features"][0]["attributes"]["OBJECTID"] = source.EXPECTED_IDS[0]
        with self.assertRaisesRegex(ValueError, "source_oid_roster_drift:page_007"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_007"]["features"][0]["attributes"]["OBJECTID"] = docs["page_007"]["features"][1]["attributes"]["OBJECTID"]
        with self.assertRaisesRegex(ValueError, "source_page_id_population_drift:page_007"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_rejects_type_loss_and_unexpected_columns(self):
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_000"]["features"][0]["attributes"]["Scale_Minimum"] = "60000"
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift:Scale_Minimum"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_000"]["features"][0]["attributes"]["Unexpected"] = "extra"
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        del docs["page_000"]["features"][0]["attributes"]["Source_Dataset"]
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = _documents(); docs = json.loads(json.dumps(docs))
        docs["page_000"]["features"][0]["attributes"]["OBJECTID"] = "1"
        with self.assertRaisesRegex(ValueError, "source_oid_value_type_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_rejects_geometry_ordinate_and_coverage_drift(self):
        for mutation, error in (({"x": "-90", "y": 40.0}, "source_xy_ordinate_contract_drift"),
                                ({"x": 10000.0, "y": 10000.0}, "source_coordinate_range_drift"),
                                ({"x": 0.0, "y": 0.0}, "source_feature_outside_coverage_extent"),
                                ({"x": 20.0, "y": 40.0, "z": 5.0}, "source_point_geometry_structure_drift"),
                                ({"x": -90.0}, "source_point_coordinates_missing")):
            docs = json.loads(json.dumps(_documents()))
            docs["page_015"]["features"][0]["geometry"] = mutation
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, error):
                source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_rejects_feature_id_null_duplicate_and_observed_coverage_contraction(self):
        docs = json.loads(json.dumps(_documents()))
        docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = None
        with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = json.loads(json.dumps(_documents()))
        docs["page_000"]["features"][1]["attributes"]["Feature_ID"] = \
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"]
        with self.assertRaisesRegex(ValueError, "source_feature_id_duplicate_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = json.loads(json.dumps(_documents()))
        for page_index in range(source.PAGE_COUNT):
            for row in docs[f"page_{page_index:03d}"]["features"]:
                row["geometry"] = {"x": -90.0, "y": 35.0}
        with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = json.loads(json.dumps(_documents()))
        for page_index in range(source.PAGE_COUNT):
            for row in docs[f"page_{page_index:03d}"]["features"]:
                if row["attributes"]["OBJECTID"] == 2:
                    row["geometry"] = {"x": -90.0, "y": 35.0}
        with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_layer_page_and_point_crs_reject_contradictory_representations(self):
        mutations = [
            ("layer wkid contradiction", lambda d: d["layer_before"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("layer extent CRS drift", lambda d: d["layer_before"]["extent"].update(
                spatialReference={"wkid": 3857})),
            ("page CRS drift", lambda d: d["page_000"].update(
                spatialReference={"wkid": 3857, "latestWkid": 3857})),
            ("point CRS drift", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857})),
            ("point WKT contradiction", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"latestWkid": 4326, "wkt": source.CRS.from_epsg(3857).to_wkt()})),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(_documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, "crs_drift"):
                    source._validate({name: (_canonical(value), {}) for name, value in docs.items()})

    def test_parquet_candidate_retains_declared_types_and_point_geometry(self):
        import io

        import pyarrow
        import pyarrow.parquet as pq
        from shapely import from_wkb

        fields, features, _audit = source._validate({name: (_canonical(doc), {}) for name, doc in _documents().items()})
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertTrue(table.schema.field("OBJECTID").type.equals(pyarrow.int64()))
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(pyarrow.int32()))
        self.assertTrue(table.schema.field("Feature_ID").type.equals(pyarrow.string()))
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)
        self.assertEqual(table["Feature_ID"].null_count, 0)
        self.assertEqual(table["Category_of_Land_Region"][0].as_py(), "")
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])

    def _capture_directory(self, root: Path):
        docs = _documents()
        receipts = {}
        expected_urls = dict(source._capture_urls(source.EXPECTED_IDS))
        for name, doc in docs.items():
            body = _canonical(doc)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                       "observed_at": "2026-11-07T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body), "bytes": len(body),
                       "sha256": source._sha(body), "content_encoding": "identity", "content_length": len(body),
                       "error_code": None}
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            receipts[name] = receipt
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True, "receipts": receipts,
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")

    def _repin_receipt_manifest(self, root: Path, name: str, mutation):
        path = root / f"{name}.receipt.json"
        receipt = json.loads(path.read_bytes())
        mutation(receipt)
        path.write_bytes(_canonical(receipt) + b"\n")
        manifest = json.loads((root / "capture_manifest.json").read_bytes())
        manifest["receipts"][name] = receipt
        manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")

    def test_retained_capture_rejects_re_pinned_wrong_origin_query_or_method(self):
        cases = [
            ("origin", lambda r: r.update(url=r["url"].replace("ienccloud.us", "attacker.example"),
                                          final_url=r["final_url"].replace("ienccloud.us", "attacker.example"))),
            ("query", lambda r: r.update(url=r["url"] + "&unexpected=1", final_url=r["final_url"] + "&unexpected=1")),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
                self._repin_receipt_manifest(root, "page_003", mutate)
                with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:page_003"):
                    source._read_replay(root)

    def test_retained_capture_rejects_roster_and_receipt_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            captures = source._read_replay(root)
            self.assertEqual(set(captures), source.CAPTURE_NAMES)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            (root / "page_002.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "retained_capture_artifact_digest_mismatch"):
                source._read_replay(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            del manifest["receipts"]["ids_after"]
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            with self.assertRaisesRegex(ValueError, "retained_capture_name_roster_changed"):
                source._read_replay(root)

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_land_region_locations_usace_ienc(storage, version="draft", capture_directory=str(root))
            code_identity_check.assert_called_once()
            code_identity_args, code_identity_kwargs = code_identity_check.call_args
            self.assertEqual(code_identity_args[1], code_identity_kwargs["identity"]["code_commit"])
            self.assertEqual(code_identity_args[2], code_identity_kwargs["identity"]["files"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            audit_before = storage.read_key(f"{prefix}/metadata/source_audit.json")
            receipts_before = {name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                               for name in json.loads((root / "capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-11-08T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_land_region_locations_usace_ienc(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before}, receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])
            self.assertFalse(second["release_accepted"])


if __name__ == "__main__":
    unittest.main()
