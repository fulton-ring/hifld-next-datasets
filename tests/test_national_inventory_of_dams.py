from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.national_inventory_of_dams import (
    national_inventory_of_dams_draft_schedule,
    national_inventory_of_dams_geometry_quality,
    national_inventory_of_dams_population_complete,
)
from dagster_hifld.sources import national_inventory_of_dams as source


class NationalInventoryOfDamsTests(unittest.TestCase):
    def setUp(self):
        self.fields = [
            {"name": name, "type": kind, "length": length, "nullable": name != "OBJECTID"}
            for name, kind, length in source.EXPECTED_FIELD_SIGNATURES
        ]
        self.service = {"layers": [{"id": 0, "name": source.LAYER_NAME, "type": "Feature Layer"}]}
        self.layer = {
            "id": 0, "name": source.LAYER_NAME, "type": "Feature Layer",
            "geometryType": "esriGeometryPoint", "maxRecordCount": source.PAGE_SIZE,
            "hasZ": False, "hasM": False,
            "objectIdField": "OBJECTID",
            "extent": {"xmin": -176.6873, "ymin": 13.3605, "xmax": 144.7061, "ymax": 71.266667,
                       "spatialReference": {"wkid": 4269}}, "fields": self.fields,
        }
        attrs = {name: None for name in source.EXPECTED_FIELD_NAMES}
        attrs["OBJECTID"] = 7
        attrs["NIDID"] = "00000017"
        self.feature = {"attributes": attrs, "geometry": {"x": -100.125, "y": 40.5, "spatialReference": {"wkid": 4269}}}
        ids = {"objectIds": [7]}
        page = {"spatialReference": {"wkid": 4269}, "features": [self.feature]}
        documents = {
            "service_before": self.service, "service_after": self.service,
            "layer_before": self.layer, "layer_after": self.layer,
            "count_before": {"count": 1}, "count_after": {"count": 1},
            "ids_before": ids, "ids_seed": ids, "ids_after": ids,
            "page_00000": page,
        }
        self.captures = {name: (json.dumps(value).encode(), {"url": "mock"}) for name, value in documents.items()}

    def test_exact_population_schema_and_native_point_are_accepted(self):
        fields, rows, audit = source._validate(self.captures)
        self.assertEqual(len(fields), 81)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attributes"]["NIDID"], "00000017")
        self.assertEqual(audit["source_crs"], "EPSG:4269")
        self.assertEqual(audit["source_feature_count"], 1)
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["invalid_geometry_count"], 0)
        self.assertIn("not yet all represented", audit["nid_scope_note"])

    def test_duplicate_nidid_values_are_preserved_without_claiming_uniqueness(self):
        docs = {name: json.loads(body) for name, (body, _) in self.captures.items()}
        for name in ("ids_before", "ids_seed", "ids_after"):
            docs[name] = {"objectIds": [7, 8]}
        for name in ("count_before", "count_after"):
            docs[name] = {"count": 2}
        second = {"attributes": dict(self.feature["attributes"], OBJECTID=8),
                  "geometry": dict(self.feature["geometry"])}
        docs["page_00000"] = {"spatialReference": {"wkid": 4269},
                              "features": [self.feature, second]}
        captures = {name: (json.dumps(value).encode(), {}) for name, value in docs.items()}
        _, rows, audit = source._validate(captures)
        self.assertEqual([row["attributes"]["NIDID"] for row in rows], ["00000017", "00000017"])
        self.assertEqual(audit["source_feature_count"], 2)

    def test_source_field_type_drift_fails_closed(self):
        layer = dict(self.layer)
        fields = [dict(field) for field in self.fields]
        fields[1]["type"] = "esriFieldTypeInteger"
        layer["fields"] = fields
        bad = dict(self.captures)
        bad["layer_before"] = (json.dumps(layer).encode(), {})
        bad["layer_after"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "field_schema_drift"):
            source._validate(bad)

    def test_population_change_and_page_id_mismatch_fail_closed(self):
        bad = dict(self.captures)
        bad["ids_after"] = (json.dumps({"objectIds": [8]}).encode(), {})
        with self.assertRaisesRegex(ValueError, "population_changed"):
            source._validate(bad)
        bad = dict(self.captures)
        page = {"spatialReference": {"wkid": 4269}, "features": [dict(self.feature)]}
        page["features"][0] = {"attributes": dict(self.feature["attributes"], OBJECTID=8), "geometry": self.feature["geometry"]}
        bad["page_00000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "page_oid_order"):
            source._validate(bad)

    def test_crs_and_geometry_drift_are_not_repaired(self):
        bad = dict(self.captures)
        layer = dict(self.layer, extent=dict(self.layer["extent"], spatialReference={"wkid": 4326}))
        bad["layer_before"] = (json.dumps(layer).encode(), {})
        bad["layer_after"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "native_crs_drift"):
            source._validate(bad)
        row = dict(self.feature)
        row["geometry"] = {"x": 200.0, "y": 40.5, "spatialReference": {"wkid": 4326}}
        bad = dict(self.captures)
        bad["page_00000"] = (json.dumps({"spatialReference": {"wkid": 4269}, "features": [row]}).encode(), {})
        _, _, audit = source._validate(bad)
        self.assertEqual(audit["invalid_geometry_count"], 1)

    def test_declared_z_or_m_dimensions_are_rejected(self):
        layer = dict(self.layer, hasZ=True)
        bad = dict(self.captures)
        bad["layer_before"] = (json.dumps(layer).encode(), {})
        bad["layer_after"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "geometry_dimensions_drift"):
            source._validate(bad)

    def test_parquet_preserves_source_values_native_coordinates_and_crs(self):
        fields, rows, _ = source._validate(self.captures)
        body = source._parquet(fields, rows)
        table = pq.read_table(pa.BufferReader(body))
        self.assertEqual(table.schema.field("NIDID").type, pa.string())
        self.assertEqual(table.column("NIDID")[0].as_py(), "00000017")
        self.assertEqual(table.column("OBJECTID")[0].as_py(), 7)
        self.assertEqual(json.loads(table.column("source_geometry")[0].as_py()["raw_json"])["x"], -100.125)
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["crs"]["id"]["code"], 4269)

    def test_page_request_uses_explicit_source_ids_all_fields_and_native_crs(self):
        urls = source._capture_urls([7, 11])
        self.assertIn("FeatureServer/0/query?", urls["page_00000"])
        self.assertIn("objectIds=7%2C11", urls["page_00000"])
        self.assertIn("outFields=%2A", urls["page_00000"])
        self.assertIn("outSR=4269", urls["page_00000"])
        self.assertEqual(source.SERVICE_URL, "https://geospatial.sec.usace.army.mil/dls/rest/services/NID/National_Inventory_of_Dams_Public_Service/FeatureServer")

    def test_replay_requires_sealed_body_wire_receipt_and_exact_request_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body, wire = b'{"count":1}', b'{"count":1}'
            url = source._capture_urls()["count_before"]
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest()}
            (root / "count_before.body").write_bytes(body)
            (root / "count_before.wire").write_bytes(wire)
            (root / "count_before.receipt.json").write_text(json.dumps(receipt))
            files = {p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                     for p in root.iterdir()}
            (root / "capture_manifest.json").write_text(json.dumps({
                "complete": True, "receipts": {"count_before": receipt}, "files": files,
            }))
            captures = source._read_replay(root)
            source._validate_capture_identity(captures, {"count_before": url})
            changed = dict(receipt, final_url=url + "&redirected=1")
            captures["count_before"] = (body, changed)
            with self.assertRaisesRegex(ValueError, "request_identity_mismatch"):
                source._validate_capture_identity(captures, {"count_before": url})

    def test_oversized_sealed_replay_artifact_is_refused_before_body_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body = b'x' * (source.MAX_RESPONSE_BYTES + 1)
            wire = b'x'
            url = source._capture_urls()["count_before"]
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest()}
            (root / "count_before.body").write_bytes(body)
            (root / "count_before.wire").write_bytes(wire)
            receipt_bytes = json.dumps(receipt).encode()
            (root / "count_before.receipt.json").write_bytes(receipt_bytes)
            files = {
                "count_before.body": {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()},
                "count_before.wire": {"bytes": len(wire), "sha256": hashlib.sha256(wire).hexdigest()},
                "count_before.receipt.json": {"bytes": len(receipt_bytes), "sha256": hashlib.sha256(receipt_bytes).hexdigest()},
            }
            manifest = {"complete": True, "receipts": {"count_before": receipt}, "files": files}
            (root / "capture_manifest.json").write_text(json.dumps(manifest))
            read_names = []
            bounded_read = source._bounded_capture_file
            def track_read(path, limit, expected_identity=None):
                read_names.append(path.name)
                return bounded_read(path, limit, expected_identity)
            with patch.object(source, "_bounded_capture_file", side_effect=track_read), \
                    patch.object(Path, "read_bytes", side_effect=AssertionError("unbounded Path.read_bytes")):
                with self.assertRaisesRegex(ValueError, "unsafe_or_oversized"):
                    source._read_replay(root)
            self.assertEqual(read_names, ["capture_manifest.json"])

    def test_aggregate_replay_budget_is_checked_before_response_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body, wire = b"123456", b"abcdef"
            url = source._capture_urls()["count_before"]
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest()}
            (root / "count_before.body").write_bytes(body)
            (root / "count_before.wire").write_bytes(wire)
            receipt_bytes = json.dumps(receipt).encode()
            (root / "count_before.receipt.json").write_bytes(receipt_bytes)
            files = {p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                     for p in root.iterdir()}
            (root / "capture_manifest.json").write_text(json.dumps({
                "complete": True, "receipts": {"count_before": receipt}, "files": files,
            }))
            read_names = []
            bounded_read = source._bounded_capture_file
            def track_read(path, limit, expected_identity=None):
                read_names.append(path.name)
                return bounded_read(path, limit, expected_identity)
            with patch.object(source, "MAX_CAPTURE_PHYSICAL_BYTES", 20):
                with patch.object(source, "_bounded_capture_file", side_effect=track_read):
                    with self.assertRaisesRegex(ValueError, "aggregate_size_exceeded"):
                        source._read_replay(root)
            self.assertEqual(read_names, ["capture_manifest.json"])

    def test_conflicting_native_crs_aliases_fail(self):
        self.assertTrue(source._is_native_4269({"wkid": 4269, "latestWkid": 4269}))
        self.assertFalse(source._is_native_4269({"wkid": 4326, "latestWkid": 4269}))

    def test_quality_checks_and_schedule_keep_release_gates_closed(self):
        candidate = {"feature_count": 1, "candidate_ready": "metadata/candidate_ready.json",
                     "source_invalid_geometry_count": 0, "source_null_geometry_count": 0,
                     "release_accepted": False, "rights_accepted": False}
        self.assertTrue(national_inventory_of_dams_population_complete(candidate).passed)
        self.assertTrue(national_inventory_of_dams_geometry_quality(candidate).passed)
        self.assertFalse(national_inventory_of_dams_geometry_quality(dict(candidate, source_null_geometry_count=1)).passed)
        self.assertEqual(national_inventory_of_dams_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = source._candidate_result("vtest", {"source_feature_count": 1, "null_geometry_count": 0,
            "invalid_geometry_count": 0}, "candidate", False)
        for gate in ("source_release_accepted", "freshness_accepted", "schema_contract_accepted",
                     "id_contract_accepted", "rights_accepted", "mapping_approved", "release_accepted"):
            self.assertFalse(result[gate])


if __name__ == "__main__":
    unittest.main()
