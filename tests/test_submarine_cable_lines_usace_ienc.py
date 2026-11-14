import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.sources import submarine_cable_lines_usace_ienc as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.submarine_cable_lines_usace_ienc import (
    submarine_cable_lines_usace_ienc_complete, submarine_cable_lines_usace_ienc_draft_schedule,
    submarine_cable_lines_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


class SubmarineCableLinesUsaceIencTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(submarine_cable_lines_usace_ienc, hifld_ingest_assets)
        self.assertEqual(submarine_cable_lines_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        passed = submarine_cable_lines_usace_ienc_complete({"feature_count": 227, "source_population_complete": True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        rejected = submarine_cable_lines_usace_ienc_complete({"feature_count": 226, "source_population_complete": True})
        self.assertFalse(rejected.passed)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_submarine_cable_lines_usace_ienc(storage, version="../outside")

    def _line(self, oid):
        x0 = -95.3 + (oid % 100) * 0.003
        y0 = 29.75 + (oid // 100) * 0.003
        return [[x0, y0], [x0 + 0.002, y0 + 0.001], [x0 + 0.004, y0 + 0.002]]

    def _documents(self):
        service = {"mapName": "SUBMARINE_CABLE_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 51, "name": "SUBMARINE_CABLE_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 51, "name": "SUBMARINE_CABLE_LINE", "geometryType": "esriGeometryPolyline", "maxRecordCount": 2000,
                 "spatialReference": {"latestWkid": 4326}, "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"latestWkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        features = []
        for oid in source.EXPECTED_IDS:
            values = {field["name"]: (oid if field["name"] == "OBJECTID" else
                                      60000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                      for field in source.ATTRIBUTE_FIELDS}
            paths = [self._line(oid)]
            if oid % 10 == 0:
                paths.append([[self._line(oid)[0][0] + 0.0005, 29.9], [self._line(oid)[0][0] + 0.0015, 29.9001]])
            features.append({"attributes": values, "geometry": {"paths": paths}})
        return {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "title": "Inland Electronic Navigational Charts",
                         "description": "USACE updates. Feature classes are https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and USACE_IENC_Master_Service_gdb.zip."},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/SUBMARINE_CABLE_LINE", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer, "count_before": {"count": 227}, "ids_before": {"objectIds": source.EXPECTED_IDS},
            "features": {"spatialReference": {"latestWkid": 4326}, "features": features},
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": 227},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }

    def _capture_directory(self, root: Path):
        docs = self._documents()
        receipts = {}
        expected_urls = dict(source._capture_urls(source.EXPECTED_IDS))
        for name, doc in docs.items():
            body = _canonical(doc)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                       "observed_at": "2026-11-13T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body), "bytes": len(body), "sha256": source._sha(body),
                       "content_encoding": "identity", "content_length": len(body), "error_code": None}
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

    def test_retained_capture_rejects_re_pinned_wrong_origin_query_and_method(self):
        cases = [
            ("origin", lambda r: r.update(url=r["url"].replace("ienccloud.us", "attacker.example"),
                                          final_url=r["final_url"].replace("ienccloud.us", "attacker.example"))),
            ("query", lambda r: r.update(url=r["url"] + "&unexpected=1", final_url=r["final_url"] + "&unexpected=1")),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
                self._repin_receipt_manifest(root, "features", mutate)
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                self.assertEqual(manifest["receipts"]["features"], json.loads((root / "features.receipt.json").read_bytes()))
                self.assertEqual(manifest["artifact_roster"], capture_roster_digests(root, exclude={"capture_manifest.json"}))
                with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:features"):
                    source._read_replay(root)

    def test_live_contract_fields_ids_paths_and_attributes_are_complete(self):
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in self._documents().items()}
        fields, features, audit = source._validate(captures)
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 227)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 725)
        self.assertEqual(audit["feature_id_distinct_count"], 227)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["source_geometry_type"], "esriGeometryPolyline")
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["geometry"]["paths"][0][0], self._line(1)[0])
        self.assertIn("not a national submarine-cable inventory", audit["source_coverage_scope"])

    def test_renamed_field_missing_id_or_extra_geometry_key_quarantines_contract(self):
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["layer_before"]["fields"][2]["name"] = "Changed"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["ids_after"]["objectIds"] = [*source.EXPECTED_IDS[:-1]]
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs))
        docs["features"]["features"][0]["geometry"]["unexpected_ordinate"] = 20
        with self.assertRaisesRegex(ValueError, "source_polyline_geometry_structure_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})

    def test_null_integer_and_extra_attribute_values_reject_type_contract(self):
        docs = json.loads(json.dumps(self._documents()))
        docs["features"]["features"][0]["attributes"]["Scale_Minimum"] = "60000"
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift:Scale_Minimum"):
            source._validate({name: (_canonical(value), {}) for name, value in docs.items()})
        docs = json.loads(json.dumps(self._documents()))
        docs["features"]["features"][0]["attributes"]["Unexpected"] = "extra"
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            source._validate({name: (_canonical(value), {}) for name, value in docs.items()})

    def test_polyline_wkb_is_xy_only_and_preserves_path_structure(self):
        line = self._line(1)
        single = source._esri_polyline_wkb({"paths": [line]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 5))
        self.assertEqual(struct.unpack("<I", single[5:9])[0], 1)
        self.assertEqual(struct.unpack("<BI", single[9:14]), (1, 2))
        self.assertEqual(struct.unpack("<I", single[14:18])[0], 3)
        multi = source._esri_polyline_wkb({"paths": [line, self._line(2)]})
        self.assertEqual(struct.unpack("<I", multi[5:9])[0], 2)
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polyline_dimensional_flags_rejected"):
                source._esri_polyline_wkb({"paths": [line], **flags})

    def test_paths_must_match_layer_dimensional_and_coverage_contract(self):
        cases = [
            ("single-point path", lambda d: d["features"]["features"][0]["geometry"].update(
                paths=[[d["features"]["features"][0]["geometry"]["paths"][0][0]]]),
             "source_polyline_path_invalid"),
            ("paths missing", lambda d: d["features"]["features"][0]["geometry"].pop("paths"),
             "source_polyline_paths_missing"),
            ("empty paths", lambda d: d["features"]["features"][0]["geometry"].update(paths=[]),
             "source_polyline_paths_missing"),
            ("three-ordinate vertex", lambda d: d["features"]["features"][0]["geometry"]["paths"][0][0].append(1.0),
             "source_xy_ordinate_contract_drift"),
            ("non-finite vertex", lambda d: d["features"]["features"][0]["geometry"]["paths"][0][1].__setitem__(
                1, None), "source_xy_ordinate_contract_drift"),
            ("outside coverage", lambda d: d["features"]["features"][0]["geometry"]["paths"][0].__setitem__(
                2, [-120.0, 29.9]), "source_feature_outside_coverage_extent"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True), "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["features"].update(hasZ=True), "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["features"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("feature query truncated", lambda d: d["features"].update(exceededTransferLimit=True),
             "source_feature_query_error_or_truncation"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate({name: (_canonical(value), {}) for name, value in docs.items()})

    def test_layer_page_and_explicit_geometry_crs_reject_ambiguous_or_contradictory_values(self):
        mutations = [
            ("layer wkid/latestWkid contradiction", lambda d: d["layer_before"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("layer extent CRS contradiction", lambda d: d["layer_before"]["extent"].update(
                spatialReference={"wkid": 3857})),
            ("page CRS drift", lambda d: d["features"].update(
                spatialReference={"wkid": 3857, "latestWkid": 3857})),
            ("geometry CRS drift", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857})),
            ("geometry wkid/latestWkid contradiction", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("geometry WKT contradiction", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"latestWkid": 4326, "wkt": source.CRS.from_epsg(3857).to_wkt()})),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, "crs_drift"):
                    source._validate({name: (_canonical(value), {}) for name, value in docs.items()})

    def test_parquet_candidate_retains_all_declared_attributes_and_polyline_wkb(self):
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in self._documents().items()}
        fields, features, _audit = source._validate(captures)
        parquet = source._parquet_bytes(fields, features)
        import io
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 227)
        names = table.schema.names
        self.assertEqual(names, [f["name"] for f in source.ATTRIBUTE_FIELDS] + ["geometry"])
        wkb = table.column("geometry").to_pylist()[0]
        self.assertEqual(wkb[:5], b"\x01\x05\x00\x00\x00")
        self.assertEqual(table.column("Feature_ID").to_pylist()[0], "Feature_ID-1")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiLineString"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_submarine_cable_lines_usace_ienc(storage, version="draft", capture_directory=str(root))
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
                receipt["observed_at"] = "2026-11-14T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_submarine_cable_lines_usace_ienc(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before}, receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()
