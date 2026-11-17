import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from shapely.geometry import LinearRing
from dagster_hifld.sources import cable_areas_usace_ienc as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.cable_areas_usace_ienc import (
    cable_areas_usace_ienc_complete, cable_areas_usace_ienc_draft_schedule,
    cable_areas_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.esri_common import canonical as _canonical


class CableAreasUsaceIencTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(cable_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(cable_areas_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        passed = cable_areas_usace_ienc_complete({"feature_count": 30, "source_population_complete": True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        rejected = cable_areas_usace_ienc_complete({"feature_count": 29, "source_population_complete": True})
        self.assertFalse(rejected.passed)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_cable_areas_usace_ienc(storage, version="../outside")

    def _shell(self, x0, y0, w=0.002, h=0.001):
        return [[x0, y0], [x0, y0 + h], [x0 + w, y0 + h], [x0 + w, y0], [x0, y0]]

    def _hole(self, x0, y0, w=0.0008, h=0.0004):
        return [[x0, y0], [x0 + w, y0], [x0 + w, y0 + h], [x0, y0 + h], [x0, y0]]

    def _geometry(self, oid):
        x0 = -92.0 + (oid % 10) * 0.01
        y0 = 31.0 + (oid // 10) * 0.02
        if oid == 1:
            return {"rings": [self._shell(x0, y0), self._hole(x0 + 0.0002, y0 + 0.0002)]}
        if oid == 2:
            return {"rings": [self._shell(x0, y0), self._shell(x0 + 0.01, y0)]}
        return {"rings": [self._shell(x0, y0)]}

    def _expected_geometry_stats(self):
        ring_count = vertex_count = hole_count = 0
        for oid in source.EXPECTED_IDS:
            geometry = self._geometry(oid)
            for ring in geometry["rings"]:
                ring_count += 1
                vertex_count += len(ring)
                if LinearRing([point[:2] for point in ring]).is_ccw:
                    hole_count += 1
        return ring_count, vertex_count, hole_count

    def _documents(self):
        service = {"mapName": "CABLE_AREA", "copyrightText": "USACE IENC",
                   "layers": [{"id": 59, "name": "CABLE_AREA", "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 59, "name": "CABLE_AREA", "geometryType": "esriGeometryPolygon", "maxRecordCount": 2000,
                 "spatialReference": {"latestWkid": 4326}, "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"latestWkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        features = []
        for oid in source.EXPECTED_IDS:
            values = {field["name"]: (oid if field["name"] == "OBJECTID" else
                                      60000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                      for field in source.ATTRIBUTE_FIELDS}
            features.append({"attributes": values, "geometry": self._geometry(oid)})
        return {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "title": "Inland Electronic Navigational Charts",
                         "description": "USACE updates. Feature classes are https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and USACE_IENC_Master_Service_gdb.zip."},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/CABLE_AREA", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer, "count_before": {"count": 30}, "ids_before": {"objectIds": source.EXPECTED_IDS},
            "features": {"spatialReference": {"latestWkid": 4326}, "geometryType": "esriGeometryPolygon", "features": features},
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": 30},
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
                       "observed_at": "2026-11-14T00:00:00Z", "status": 200, "complete_body": True,
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

    def test_live_contract_fields_ids_rings_and_attributes_are_complete(self):
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in self._documents().items()}
        fields, features, audit = source._validate(captures)
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 30)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        ring_count, vertex_count, hole_count = self._expected_geometry_stats()
        self.assertEqual(audit["source_ring_count"], ring_count)
        self.assertEqual(audit["source_vertex_count"], vertex_count)
        self.assertEqual(audit["source_hole_ring_count"], hole_count)
        self.assertEqual(audit["geometry_valid_count"], 30)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["feature_id_distinct_count"], 30)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["source_geometry_type"], "esriGeometryPolygon")
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["geometry"]["rings"][0][0], self._geometry(1)["rings"][0][0])
        bounds = audit["source_vertex_bounds"]
        self.assertGreaterEqual(bounds["xmin"], source.EXPECTED_EXTENT["xmin"])
        self.assertGreaterEqual(bounds["ymin"], source.EXPECTED_EXTENT["ymin"])
        self.assertLessEqual(bounds["xmax"], source.EXPECTED_EXTENT["xmax"])
        self.assertLessEqual(bounds["ymax"], source.EXPECTED_EXTENT["ymax"])
        self.assertIn("not a national cable-area inventory", audit["source_coverage_scope"])

    def test_identity_and_service_metadata_drift_is_rejected(self):
        mutations = [
            ("bts item owner", lambda d: d["bts_item"].update(owner="OTHER")),
            ("bts distribution link", lambda d: d["bts_item"].update(description="no links")),
            ("service directory entry", lambda d: d["service_directory"].update(services=[])),
            ("service identity", lambda d: d["service_before"].update(mapName="OTHER")),
            ("layer geometry type", lambda d: d["layer_before"].update(geometryType="esriGeometryPolyline")),
            ("layer page limit", lambda d: d["layer_before"].update(maxRecordCount=100)),
            ("layer changed during capture", lambda d: d["layer_after"].update(name="OTHER")),
            ("count drift", lambda d: d["count_after"].update(count=29)),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                expected = {
                    "bts item owner": "bts_ienc_item_identity_drift",
                    "bts distribution link": "bts_ienc_official_distribution_link_drift",
                    "service directory entry": "official_ienc_service_directory_drift",
                    "service identity": "official_cable_area_service_drift",
                    "layer geometry type": "source_layer_geometry_crs_or_page_limit_drift",
                    "layer page limit": "source_layer_geometry_crs_or_page_limit_drift",
                    "layer changed during capture": "source_layer_changed_during_capture",
                    "count drift": "source_count_drift",
                }[label]
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate({name: (_canonical(value), {}) for name, value in docs.items()})

    def test_renamed_field_missing_id_or_extra_geometry_key_quarantines_contract(self):
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["layer_before"]["fields"][2]["name"] = "Changed"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["ids_after"]["objectIds"] = [*source.EXPECTED_IDS[:-1]]
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs))
        docs["features"]["features"][0]["geometry"]["unexpected_ordinate"] = 20
        with self.assertRaisesRegex(ValueError, "source_polygon_geometry_structure_drift"):
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

    def test_polygon_wkb_is_xy_only_and_preserves_shell_hole_structure(self):
        shell = self._shell(-92.0, 31.0)
        hole = self._hole(-92.0 + 0.0002, 31.0 + 0.0002)
        single = source._esri_polygon_wkb({"rings": [shell, hole]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        self.assertEqual(struct.unpack("<I", single[5:9])[0], 2)
        self.assertEqual(struct.unpack("<I", single[9:13])[0], len(shell))
        self.assertEqual(struct.unpack("<I", single[13 + 16 * len(shell):13 + 16 * len(shell) + 4])[0], len(hole))
        multi = source._esri_polygon_wkb({"rings": [shell, self._shell(-91.0, 32.0)]})
        self.assertEqual(struct.unpack("<BI", multi[:5]), (1, 6))
        self.assertEqual(struct.unpack("<I", multi[5:9])[0], 2)
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        for label, geometry, expected in [
            ("rings missing", {}, "source_polygon_rings_missing"),
            ("unclosed ring", {"rings": [shell[:-1]]}, "source_polygon_ring_not_closed"),
            ("short ring", {"rings": [shell[:3]]}, "source_polygon_ring_invalid"),
            ("three-ordinate ring", {"rings": [[point + [1.0] for point in shell]]}, "source_polygon_ordinate_count_drift"),
            ("ccw shell only", {"rings": [list(reversed(shell))]}, "source_polygon_shell_missing"),
            ("ambiguous hole ownership", {"rings": [self._shell(-92.0, 31.0), self._shell(-92.0, 31.0, 0.004, 0.002),
                                                    self._hole(-92.0 + 0.0005, 31.0 + 0.0005)]},
             "source_hole_ownership_ambiguous"),
        ]:
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, expected):
                    source._esri_polygon_wkb(geometry)

    def test_rings_must_match_layer_dimensional_and_coverage_contract(self):
        cases = [
            ("three-ordinate vertex", lambda d: d["features"]["features"][0]["geometry"]["rings"][0][1].append(1.0),
             "source_xy_ordinate_contract_drift"),
            ("non-finite vertex", lambda d: d["features"]["features"][0]["geometry"]["rings"][0][1].__setitem__(
                1, None), "source_xy_ordinate_contract_drift"),
            ("outside coverage", lambda d: d["features"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-120.0, 31.0]), "source_feature_outside_coverage_extent"),
            ("unclosed ring", lambda d: d["features"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_invalid"),
            ("short ring", lambda d: d["features"]["features"][0]["geometry"].update(rings=[
                d["features"]["features"][0]["geometry"]["rings"][0][:3]]), "source_polygon_ring_invalid"),
            ("ccw shell only", lambda d: d["features"]["features"][0]["geometry"].update(
                rings=[list(reversed(d["features"]["features"][0]["geometry"]["rings"][0]))]),
             "source_polygon_shell_missing"),
            ("rings missing", lambda d: d["features"]["features"][0]["geometry"].pop("rings"),
             "source_polygon_geometry_structure_drift"),
            ("empty rings", lambda d: d["features"]["features"][0]["geometry"].update(rings=[]),
             "source_polygon_rings_missing"),
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

    def test_parquet_candidate_retains_all_declared_attributes_and_polygon_wkb(self):
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in self._documents().items()}
        fields, features, _audit = source._validate(captures)
        parquet = source._parquet_bytes(fields, features)
        import io
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 30)
        names = table.schema.names
        self.assertEqual(names, [f["name"] for f in source.ATTRIBUTE_FIELDS] + ["geometry"])
        wkb = table.column("geometry").to_pylist()[0]
        self.assertEqual(wkb[:5], b"\x01\x03\x00\x00\x00")
        self.assertEqual(table.column("Feature_ID").to_pylist()[0], "Feature_ID-1")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon", "Polygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"):
                first = source.capture_cable_areas_usace_ienc(storage, version="draft", capture_directory=str(root))
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
                receipt["observed_at"] = "2026-11-15T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"):
                second = source.capture_cable_areas_usace_ienc(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before}, receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()
