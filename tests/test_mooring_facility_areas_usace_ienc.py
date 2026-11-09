import json
import re
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.mooring_facility_areas_usace_ienc import (
    mooring_facility_areas_usace_ienc,
    mooring_facility_areas_usace_ienc_complete,
    mooring_facility_areas_usace_ienc_draft_schedule,
)
from dagster_hifld.definitions import defs
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import mooring_facility_areas_usace_ienc as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _clockwise_square(x: float, y: float, d: float) -> list[list[float]]:
    return [[x, y], [x, y + d], [x + d, y + d], [x + d, y], [x, y]]


class MooringFacilityAreasUsaceIencTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(mooring_facility_areas_usace_ienc, hifld_ingest_assets)
        resolved = defs.resolve_job_def("mooring_facility_areas_usace_ienc_draft_refresh")
        self.assertEqual(resolved.asset_layer.executable_asset_keys, {mooring_facility_areas_usace_ienc.key})
        self.assertEqual(mooring_facility_areas_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        good = {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True,
                 "geometry_valid_count": source.EXPECTED_COUNT, "geometry_invalid_count": 0, "geometry_empty_count": 0}
        passed = mooring_facility_areas_usace_ienc_complete(good)
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        for bad in ({"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True,
                     "geometry_valid_count": source.EXPECTED_COUNT, "geometry_invalid_count": 0,
                     "geometry_empty_count": 0},
                    {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True,
                     "geometry_valid_count": source.EXPECTED_COUNT - 1, "geometry_invalid_count": 1,
                     "geometry_empty_count": 0},
                    {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True}):
            self.assertFalse(mooring_facility_areas_usace_ienc_complete(bad).passed)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_mooring_facility_areas_usace_ienc(storage, version="../outside")

    def test_code_identity_is_checked_before_staging_or_source_io(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "stage"
            storage = StagingStorageResource(local_dir=str(staging))
            with patch.object(source, "validate_construction_code_identity", side_effect=ValueError("identity")):
                with self.assertRaisesRegex(ValueError, "identity"):
                    source.capture_mooring_facility_areas_usace_ienc(storage, version="draft")
            self.assertFalse(staging.exists())

    def test_live_contract_fields_ids_geometry_and_attributes_are_complete(self):
        captures = {name: (_canonical(doc), {}) for name, doc in self._documents().items()}
        fields, features, audit = source._validate(captures)
        self.assertEqual([field["name"] for field in fields],
                         [name for name, _kind in source.EXPECTED_FIELDS])
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_ring_count_histogram"], {"1": source.EXPECTED_COUNT - 1, "2": 1})
        self.assertEqual(audit["source_shell_ring_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_hole_ring_count"], 1)
        self.assertEqual(audit["feature_id_distinct_count"], 10)
        self.assertEqual(audit["feature_id_duplicate_count"], source.EXPECTED_COUNT - 10)
        self.assertEqual(audit["feature_id_null_count"], 0)
        first = features[0]
        self.assertEqual(first["attributes"]["OBJECTID"], 1)
        self.assertEqual(first["attributes"]["Shape__Area"], 25.0)
        self.assertEqual(set(first["attributes"]), {name for name, _kind in source.EXPECTED_FIELDS})
        wkb = source._esri_polygon_wkb(first["geometry"], 1)
        expected = struct.pack("<BIII", 1, 3, 1, 5)
        expected += struct.pack("<" + "d" * 10, *[
            value for point in first["geometry"]["rings"][0] for value in point])
        self.assertEqual(wkb, expected)
        parquet = source._parquet_bytes(fields, features)
        import io
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)
        self.assertEqual(table.column_names,
                         [name for name, _kind in source.EXPECTED_FIELDS] + ["geometry"])
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_meta["columns"]["geometry"]["geometry_types"], ["MultiPolygon", "Polygon"])
        self.assertEqual(geo_meta["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})

    def test_item_service_layer_identity_drift_is_rejected(self):
        mutations = [
            (lambda d: d["agc_item"].update(owner="someone_else"), "agc_master_service_item_identity_drift"),
            (lambda d: d["agc_item"].update(accessInformation="other"), "agc_master_service_item_identity_drift"),
            (lambda d: d["service_before"].update(serviceItemId="0000"), "official_master_service_identity_drift"),
            (lambda d: d["service_before"].update(copyrightText="other"), "official_master_service_identity_drift"),
            (lambda d: d["layer_before"].update(serviceItemId="0000"), "source_layer_geometry_crs_or_page_limit_drift"),
            (lambda d: d["layer_before"].update(hasZ=True), "source_layer_geometry_crs_or_page_limit_drift"),
            (lambda d: d["layer_before"].update(maxRecordCount=1000), "source_layer_geometry_crs_or_page_limit_drift"),
            (lambda d: d["layer_before"]["extent"].update(xmin=-100.0), "source_layer_geometry_crs_or_page_limit_drift"),
            (lambda d: d["layer_after"].update(name="CHANGED"), "source_layer_changed_during_capture"),
        ]
        for mutate, error in mutations:
            with self.subTest(error=error):
                docs = self._documents()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, error):
                    source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_field_schema_and_attribute_value_drift_is_rejected(self):
        docs = self._documents()
        docs["layer_before"]["fields"][1]["name"] = "Renamed"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = self._documents()
        page = docs["page_000"]
        page["features"][0]["attributes"]["Shape__Area"] = "not-a-double"
        with self.assertRaisesRegex(ValueError, "source_double_value_type_drift:Shape__Area"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = self._documents()
        docs["page_000"]["features"][0]["attributes"]["Extra_Column"] = "unexpected"
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})
        docs = self._documents()
        docs["page_000"]["features"][0]["geometry"]["unexpected"] = 1
        with self.assertRaisesRegex(ValueError, "source_polygon_geometry_structure_drift"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_count_id_and_page_drift_is_rejected(self):
        mutations = [
            (lambda d: d["count_before"].update(count=5749), "source_count_drift"),
            (lambda d: d["ids_after"].update(objectIds=d["ids_after"]["objectIds"][:-1]), "source_id_population_drift"),
            (lambda d: d["page_002"]["features"].pop(), "source_page_features_missing_or_incomplete"),
            (lambda d: d["page_002"].update(exceededTransferLimit=True), "source_page_transfer_limit_truncated"),
            (lambda d: d["page_001"].update(geometryType="esriGeometryPolyline"), "source_page_geometry_type_drift"),
            (lambda d: d["page_000"].update(error={"code": 500}), "source_page_error_or_truncation"),
        ]
        for mutate, error in mutations:
            with self.subTest(error=error):
                docs = self._documents()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, error):
                    source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_polygon_topology_defects_are_rejected_before_staging(self):
        base = self._documents()
        ring = base["page_000"]["features"][0]["geometry"]["rings"][0]
        open_ring = [list(point) for point in ring]
        open_ring[-1] = [open_ring[-1][0] + 0.0001, open_ring[-1][1]]
        short_ring = open_ring[:3]
        z_ring = [[point[0], point[1], 0.0] for point in ring]
        ccw_exterior = list(reversed(ring[:-1]))
        ccw_exterior = [list(point) for point in ccw_exterior] + [list(ccw_exterior[0])]
        x0, y0 = ring[0][0], ring[0][1]
        spike = [[x0, y0], [x0, y0 + 0.01], [x0 + 0.005, y0 + 0.01], [x0 + 0.005, y0 + 0.005],
                 [x0 + 0.005, y0 + 0.01], [x0 + 0.01, y0 + 0.01], [x0 + 0.01, y0], [x0, y0]]
        cases = [
            (lambda g: g["rings"].__setitem__(0, open_ring), "source_polygon_ring_not_closed:1"),
            (lambda g: g["rings"].__setitem__(0, short_ring), "source_polygon_ring_invalid:1"),
            (lambda g: g["rings"].__setitem__(0, z_ring), "source_polygon_coordinate_not_finite_xy:1"),
            (lambda g: g["rings"].__setitem__(0, ccw_exterior), "source_polygon_shell_missing:1"),
            (lambda g: g["rings"].__setitem__(0, spike), "source_polygon_topology_invalid:1"),
            (lambda g: g.update(hasZ=True), "source_polygon_ordinate_flag_drift:1"),
            (lambda g: g.__setitem__("rings", []), "source_polygon_rings_missing:1"),
        ]
        for mutate, error in cases:
            with self.subTest(error=error):
                docs = self._documents()
                mutate(docs["page_000"]["features"][0]["geometry"])
                with self.assertRaisesRegex(ValueError, re.escape(error)):
                    source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_geometry_outside_declared_coverage_is_rejected(self):
        docs = self._documents()
        outside = [[-120.0, 40.0], [-120.0, 40.01], [-119.99, 40.01], [-119.99, 40.0], [-120.0, 40.0]]
        docs["page_000"]["features"][0]["geometry"]["rings"][0] = outside
        with self.assertRaisesRegex(ValueError, "source_geometry_outside_declared_coverage:1"):
            source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_crs_representations_are_validated(self):
        mutations = [
            (lambda d: d["layer_before"].update(spatialReference={"wkid": 3857, "latestWkid": 4326}), "crs_drift"),
            (lambda d: d["layer_before"]["extent"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            (lambda d: d["service_before"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            (lambda d: d["page_000"].update(spatialReference={"wkid": 3857, "latestWkid": 3857}), "crs_drift"),
            (lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkt": source.CRS.from_epsg(3857).to_wkt()}), "crs_drift"),
        ]
        for mutate, error in mutations:
            with self.subTest(error=error):
                docs = self._documents()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, error):
                    source._validate({name: (_canonical(doc), {}) for name, doc in docs.items()})

    def test_replay_preflights_oversized_artifact_before_opening_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
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
            root = Path(tmp) / "capture"
            root.mkdir()

            def handler(request):
                return httpx.Response(200, content=b"x" * (1_048_576 + 1), request=request)

            with patch.object(source, "PAGE_MAX_BYTES", 1_048_576), patch.object(source, "CAPTURE_MAX_BYTES", 4_000_000):
                with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
                    with self.assertRaisesRegex(ValueError, "incomplete_source_capture:oversize"):
                        source._capture(client, "https://example.invalid/test", root, "oversize")
            self.assertEqual((root / "oversize.wire").stat().st_size, 0)
            self.assertEqual((root / "oversize.body").stat().st_size, 0)
            self.assertFalse(json.loads((root / "oversize.receipt.json").read_bytes())["complete_body"])

    def test_replay_rejects_re_pinned_origin_query_and_method(self):
        cases = [
            ("origin", lambda r: r.update(url=r["url"].replace("services7.arcgis.com", "attacker.example"),
                                          final_url=r["final_url"].replace("services7.arcgis.com", "attacker.example"))),
            ("query", lambda r: r.update(url=r["url"] + "&unexpected=1",
                                         final_url=r["final_url"] + "&unexpected=1")),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "capture"
                root.mkdir()
                self._capture_directory(root)
                self._repin_receipt_manifest(root, "page_000", mutate)
                with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:page_000"):
                    source._read_replay(root)

    def test_replay_rejects_roster_and_receipt_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            (root / "unexpected.file").write_bytes(b"x")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                source._read_replay(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            body = json.loads((root / "page_000.body").read_bytes())
            body["features"][0]["attributes"]["Object_Name"] = "tampered"
            (root / "page_000.body").write_bytes(_canonical(body))
            with self.assertRaisesRegex(ValueError, "retained_capture_artifact_digest_mismatch|retained_capture_receipt_invalid"):
                source._read_replay(root)

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity"), patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_mooring_facility_areas_usace_ienc(storage, version="draft",
                                                                        capture_directory=str(root))
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            audit_before = storage.read_key(f"{prefix}/metadata/source_audit.json")
            ready_before = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready_before["approved"])
            self.assertFalse(ready_before["release_accepted"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertFalse(manifest["approved"])
            for flag in ("source_release_accepted", "source_freshness_accepted", "schema_contract_accepted",
                         "id_contract_accepted", "coverage_accepted", "rights_accepted", "mapping_approved",
                         "release_accepted"):
                self.assertFalse(first[flag])
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-11-09T01:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {name: json.loads((root / f"{name}.receipt.json").read_bytes())
                                            for name in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), patch(
                    "dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_mooring_facility_areas_usace_ienc(storage, version="draft",
                                                                          capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"), audit_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])

    def _documents(self):
        service = {"serviceItemId": source.AGC_ITEM_ID, "copyrightText": source.SERVICE_COPYRIGHT,
                   "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                   "layers": [{"id": 78, "name": "LOCK_GATE_AREA", "geometryType": "esriGeometryPolygon"},
                              {"id": source.LAYER_ID, "name": source.LAYER_NAME,
                               "geometryType": "esriGeometryPolygon"},
                              {"id": 80, "name": "TERMINAL_AREA", "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME, "geometryType": "esriGeometryPolygon",
                 "serviceItemId": source.AGC_ITEM_ID, "objectIdField": "OBJECTID",
                 "maxRecordCount": source.PAGE_SIZE, "hasZ": False, "hasM": False,
                 "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
                 "fields": [{"name": field["name"], "type": field["type"], "alias": field["alias"],
                             "sqlType": "sqlTypeOther", "nullable": True, "editable": True,
                             "domain": None, "defaultValue": None,
                             **({"length": field["length"]} if "length" in field else {})}
                            for field in source.ATTRIBUTE_FIELDS]}
        rows = []
        for oid in source.EXPECTED_IDS:
            values = {}
            for name, kind in source.EXPECTED_FIELDS:
                if name == "OBJECTID":
                    values[name] = oid
                elif name == "Scale_Minimum":
                    values[name] = 60000
                elif name == "Shape__Area":
                    values[name] = 25.0
                elif name == "Shape__Length":
                    values[name] = 20.0
                elif name == "Feature_ID":
                    values[name] = f"F-{oid % 10}"
                else:
                    values[name] = f"{name}-{oid}"
            x = source.EXPECTED_EXTENT["xmin"] + (oid % 100) * 0.01
            y = source.EXPECTED_EXTENT["ymin"] + (oid % 90) * 0.01
            geometry = {"rings": [_clockwise_square(x, y, 0.005)]}
            if oid == source.EXPECTED_COUNT:
                hole = list(reversed(_clockwise_square(x + 0.001, y + 0.001, 0.002)))
                geometry = {"rings": [_clockwise_square(x, y, 0.005), hole]}
            rows.append({"attributes": values, "geometry": geometry})
        pages = {}
        for index, name in enumerate(source._page_names()):
            start = index * source.PAGE_SIZE
            page_rows = rows[start:start + source.PAGE_SIZE]
            page = {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
                    "geometryType": "esriGeometryPolygon", "features": page_rows}
            if start + len(page_rows) < source.EXPECTED_COUNT:
                page["exceededTransferLimit"] = True
            pages[name] = page
        return {
            "agc_item": {"id": source.AGC_ITEM_ID, "owner": source.AGC_ITEM_OWNER, "type": "Feature Service",
                         "title": "USACE_IENC_Master_Service", "url": source.SERVICE_URL,
                         "accessInformation": source.SERVICE_COPYRIGHT, "modified": 1794259368000},
            "service_before": service, "layer_before": layer, "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            **pages,
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": source.EXPECTED_COUNT},
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
                       "observed_at": "2026-11-09T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body), "bytes": len(body),
                       "sha256": source._sha(body), "content_encoding": "identity",
                       "content_length": len(body), "error_code": None}
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


if __name__ == "__main__":
    unittest.main()
