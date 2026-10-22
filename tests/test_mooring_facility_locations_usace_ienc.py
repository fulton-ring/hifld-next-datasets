import json
import httpx
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.sources import mooring_facility_locations_usace_ienc as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.mooring_facility_locations_usace_ienc import (
    mooring_facility_locations_usace_ienc_complete, mooring_facility_locations_usace_ienc_draft_schedule,
    mooring_facility_locations_usace_ienc,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


class MooringFacilityLocationsUsaceIencTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(mooring_facility_locations_usace_ienc, hifld_ingest_assets)
        resolved = defs.resolve_job_def("mooring_facility_locations_usace_ienc_draft_refresh")
        self.assertEqual(resolved.asset_layer.executable_asset_keys,
                         {mooring_facility_locations_usace_ienc.key})
        self.assertEqual(mooring_facility_locations_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        passed = mooring_facility_locations_usace_ienc_complete({"feature_count": 9772, "source_population_complete": True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_mooring_facility_locations_usace_ienc(storage, version="../outside")

    def test_code_identity_is_checked_before_staging_or_source_io(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "stage"
            storage = StagingStorageResource(local_dir=str(staging))
            with patch.object(source, "validate_construction_code_identity", side_effect=ValueError("identity")):
                with self.assertRaisesRegex(ValueError, "identity"):
                    source.capture_mooring_facility_locations_usace_ienc(storage, version="draft")
            self.assertFalse(staging.exists())

    def test_replay_preflights_oversized_artifact_before_opening_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            (root / "page_000.wire").write_bytes(b"x" * (source.PAGE_MAX_BYTES + 1))
            opened = []
            original = source._open_capture_file
            def track(path, label):
                opened.append(label)
                return original(path, label)
            with patch.object(source, "_open_capture_file", side_effect=track):
                with self.assertRaisesRegex(ValueError, "retained_capture_file_size_exceeded:page_000.wire"):
                    source._read_replay(root)
            self.assertEqual(opened, ["capture_manifest.json"])

    def test_oversized_delivered_http_block_is_not_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir()
            def handler(request):
                return httpx.Response(200, content=b"x" * (1_048_576 + 1), request=request)
            with patch.object(source, "PAGE_MAX_BYTES", 1_048_576), patch.object(source, "CAPTURE_MAX_BYTES", 4_000_000):
                with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
                    with self.assertRaisesRegex(ValueError, "incomplete_source_capture:oversize"):
                        source._capture(client, "https://example.invalid/test", root, "oversize")
            self.assertEqual((root / "oversize.wire").stat().st_size, 0)
            self.assertEqual((root / "oversize.body").stat().st_size, 0)
            self.assertFalse(json.loads((root / "oversize.receipt.json").read_bytes())["complete_body"])

    def test_z_m_dimensions_are_validated_and_preserved_in_wkb(self):
        docs = self._documents()
        docs["layer_before"]["hasZ"] = True
        docs["layer_before"]["hasM"] = True
        docs["layer_after"]["hasZ"] = True
        docs["layer_after"]["hasM"] = True
        for page_name in source._page_names():
            for feature in docs[page_name]["features"]:
                feature["geometry"].update(z=12.5, m=7.25)
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in docs.items()}
        _fields, features, audit = source._validate(captures)
        self.assertTrue(audit["source_has_z"])
        self.assertTrue(audit["source_has_m"])
        self.assertEqual(source._esri_point_wkb(features[0]["geometry"]),
                         __import__("struct").pack("<BI4d", 1, 3001, features[0]["geometry"]["x"],
                                                   features[0]["geometry"]["y"], 12.5, 7.25))
        docs["page_000"]["features"][0]["geometry"].pop("z")
        with self.assertRaisesRegex(ValueError, "source_geometry_dimension_metadata_drift:feature"):
            source._validate({name: (_canonical(doc), {"status": 200}) for name, doc in docs.items()})

    def _documents(self):
        service = {"mapName": "MOORING_FACILITY_POINT", "copyrightText": "USACE IENC",
                   "layers": [{"id": 17, "name": "MOORING_FACILITY_POINT", "geometryType": "esriGeometryPoint"}]}
        layer = {"id": 17, "name": "MOORING_FACILITY_POINT", "geometryType": "esriGeometryPoint", "maxRecordCount": 2000,
                 "spatialReference": {"latestWkid": 4326}, "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"latestWkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        attributes = []
        for oid in source.EXPECTED_IDS:
            values = {field["name"]: (oid if field["name"] == "OBJECTID" else
                                      60000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                      for field in source.ATTRIBUTE_FIELDS}
            attributes.append({"attributes": values,
                               "geometry": {"x": source.EXPECTED_EXTENT["xmin"] + oid / 10000,
                                            "y": source.EXPECTED_EXTENT["ymin"] + oid / 10000}})
        page_names = source._page_names()
        pages = {}
        for index, name in enumerate(page_names):
            start = index * source.PAGE_SIZE
            rows = attributes[start:start + source.PAGE_SIZE]
            page = {"spatialReference": {"latestWkid": 4326}, "features": rows}
            if start + len(rows) < source.EXPECTED_COUNT:
                page["exceededTransferLimit"] = True
            pages[name] = page
        return {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "title": "Inland Electronic Navigational Charts",
                         "description": "USACE updates. Feature classes are https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and USACE_IENC_Master_Service_gdb.zip."},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/MOORING_FACILITY_POINT", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer, "count_before": {"count": 9772},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            **pages,
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": 9772},
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
                       "observed_at": "2026-10-20T00:00:00Z", "status": 200, "complete_body": True,
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
            ("query", lambda r: r.update(url=r["url"] + "&unexpected=1",
                                         final_url=r["final_url"] + "&unexpected=1")),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
                self._repin_receipt_manifest(root, "page_000", mutate)
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                self.assertEqual(manifest["receipts"]["page_000"], json.loads((root / "page_000.receipt.json").read_bytes()))
                self.assertEqual(manifest["artifact_roster"], capture_roster_digests(root, exclude={"capture_manifest.json"}))
                with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:page_000"):
                    source._read_replay(root)

    def test_live_contract_fields_ids_coordinates_and_attributes_are_complete(self):
        captures = {name: (_canonical(doc), {"status": 200}) for name, doc in self._documents().items()}
        fields, features, audit = source._validate(captures)
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 9772)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 9772)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["geometry"]["x"], source.EXPECTED_EXTENT["xmin"] + 0.0001)

    def test_renamed_field_missing_id_or_extra_geometry_key_quarantines_contract(self):
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["layer_before"]["fields"][2]["name"] = "Changed"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs)); docs["ids_after"]["objectIds"] = [*source.EXPECTED_IDS[:-1]]
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})
        docs = self._documents(); docs = json.loads(json.dumps(docs))
        docs["page_000"]["features"][0]["geometry"]["unexpected_ordinate"] = 20
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate({n: (_canonical(v), {}) for n, v in docs.items()})

    def test_point_coordinates_must_match_geographic_and_layer_coverage_contract(self):
        for mutation, error in (({"x": 10000.0, "y": 10000.0}, "source_coordinate_range_drift"),
                                ({"x": 0.0, "y": 0.0}, "source_feature_outside_coverage_extent")):
            docs = json.loads(json.dumps(self._documents()))
            docs["page_000"]["features"][0]["geometry"] = mutation
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, error):
                source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_layer_page_and_explicit_point_crs_reject_ambiguous_or_contradictory_values(self):
        mutations = [
            ("layer wkid/latestWkid contradiction", lambda d: d["layer_before"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("layer extent CRS contradiction", lambda d: d["layer_before"]["extent"].update(
                spatialReference={"wkid": 3857})),
            ("page CRS drift", lambda d: d["page_000"].update(
                spatialReference={"wkid": 3857, "latestWkid": 3857})),
            ("point CRS drift", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857})),
            ("point wkid/latestWkid contradiction", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326})),
            ("point WKT contradiction", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkt": source.CRS.from_epsg(3857).to_wkt()})),
        ]
        for label, mutate in mutations:
            docs = json.loads(json.dumps(self._documents()))
            mutate(docs)
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "crs_drift"):
                source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_mooring_facility_locations_usace_ienc(storage, version="draft", capture_directory=str(root))
            code_identity_check.assert_called_once()
            code_identity_args, code_identity_kwargs = code_identity_check.call_args
            self.assertEqual(code_identity_args[1], code_identity_kwargs["identity"]["code_commit"])
            self.assertEqual(code_identity_args[1], code_identity_kwargs["identity"]["code_commit"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            audit_before = storage.read_key(f"{prefix}/metadata/source_audit.json")
            receipts_before = {name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                               for name in json.loads((root / "capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-10-20T01:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_mooring_facility_locations_usace_ienc(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            for name, receipt_before in receipts_before.items():
                self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.receipt.json"), receipt_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()
