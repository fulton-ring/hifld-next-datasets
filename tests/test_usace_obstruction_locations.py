"""Focused contract and offline replay tests for the USACE OBSTRUCTION_POINT adapter."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_obstruction_locations as obstructions


class UsaceObstructionLocationsTests(unittest.TestCase):
    def _captures(self):
        fields = [{"name": name, "type": kind, "alias": name,
                   **({"length":254} if kind == "esriFieldTypeString" else {})}
                  for name, kind in obstructions.EXPECTED_FIELDS]
        layer = {"id":19,"name":"OBSTRUCTION_POINT","geometryType":"esriGeometryPoint",
                 "spatialReference":{"latestWkid":4326},"maxRecordCount":2000,"fields":fields}
        ids = [101, 205]
        attrs = []
        for oid in ids:
            attrs.append({name:(oid if name == "OBJECTID" else f"F{oid}" if name == "Feature_ID" else 3 if name == "Scale_Minimum" else "0007" if name == "Source_Date" else None)
                          for name,kind in obstructions.EXPECTED_FIELDS if name != "SHAPE"})
        docs = {
            "layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},
            "ids_seed":{"objectIds":ids},"ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
            "page_000":{"spatialReference":{"latestWkid":4326},"features":[{"attributes":attrs[0],"geometry":{"x":-90.0,"y":30.0}}]},
            "page_001":{"spatialReference":{"latestWkid":4326},"features":[{"attributes":attrs[1],"geometry":{"x":-91.0,"y":31.0}}]},
        }
        return fields, {name:(json.dumps(doc,separators=(",",":")).encode(), {}) for name,doc in docs.items()}

    def test_geoparquet_preserves_every_attribute_and_string_values(self):
        fields, captures = self._captures()
        with patch.object(obstructions, "EXPECTED_COUNT", 2), patch.object(obstructions, "PAGE_SIZE", 1):
            parsed, features, _audit = obstructions.validate_obstruction_snapshot(captures)
            payload = obstructions.obstruction_geoparquet(parsed, features)
        table = pq.read_table(pa.BufferReader(payload))
        expected_columns = [name for name, _kind in obstructions.EXPECTED_FIELDS if name != "SHAPE"] + ["geometry"]
        self.assertEqual(table.column_names, expected_columns)
        self.assertEqual(table["OBJECTID"].to_pylist(), [101, 205])
        self.assertEqual(table["Feature_ID"].to_pylist(), ["F101", "F205"])
        self.assertEqual(table["Source_Date"].to_pylist(), ["0007", "0007"])
        self.assertEqual(table["Scale_Minimum"].to_pylist(), [3, 3])

    def test_small_complete_population_and_drift_failures(self):
        fields, captures = self._captures()
        with patch.object(obstructions, "EXPECTED_COUNT", 2), patch.object(obstructions, "PAGE_SIZE", 1):
            parsed, features, audit = obstructions.validate_obstruction_snapshot(captures)
            self.assertEqual(parsed, fields)
            self.assertEqual(len(features), 2)
            self.assertEqual(audit["source_id_min"], 101)
            self.assertEqual(audit["source_feature_count"], 2)

            bad = dict(captures)
            bad["count_after"] = (b'{"count":3}', {})
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                obstructions.validate_obstruction_snapshot(bad)

            bad = dict(captures)
            changed_layer = json.loads(captures["layer_before"][0])
            changed_layer["fields"][2]["type"] = "esriFieldTypeInteger"
            bad["layer_before"] = (json.dumps(changed_layer).encode(), {})
            with self.assertRaisesRegex(ValueError, "source_layer_schema_or_spatial_contract_drift"):
                obstructions.validate_obstruction_snapshot(bad)

            bad = dict(captures)
            bad["ids_after"] = (b'{"objectIds":[101,206]}', {})
            with self.assertRaisesRegex(ValueError, "source_id_population_changed_or_invalid"):
                obstructions.validate_obstruction_snapshot(bad)

            bad = dict(captures)
            bad["page_001"] = (b'{"error":{"code":500}}', {})
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncated"):
                obstructions.validate_obstruction_snapshot(bad)

            bad = dict(captures)
            del bad["page_001"]
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                obstructions.validate_obstruction_snapshot(bad)

            bad = dict(captures)
            bad["page_000"] = (json.dumps({"spatialReference":{"latestWkid":4326},"features":[
                {"attributes":json.loads(captures["page_000"][0])["features"][0]["attributes"],"geometry":{"x":-90.0,"y":30.0,"z":4.0}}
            ]}).encode(), {})
            with self.assertRaisesRegex(ValueError, "source_point_ordinate_roster_drift"):
                _fields, features, _audit = obstructions.validate_obstruction_snapshot(bad)
                obstructions.obstruction_geoparquet(_fields, features)

    def test_offline_replay_and_first_candidate_cache(self):
        _fields, captures = self._captures()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            capture_root.mkdir()
            receipts = {}
            for name, (body, _unused) in captures.items():
                wire = body
                (capture_root / f"{name}.body").write_bytes(body)
                (capture_root / f"{name}.wire").write_bytes(wire)
                url = obstructions.LAYER_URL + "?f=pjson"
                receipt = {"method":"GET","url":url,"final_url":url,
                           "observed_at":"2026-10-20T00:00:00Z","status":200,"complete_body":True,
                           "bytes":len(body),"sha256":obstructions._sha(body),"wire_bytes":len(wire),
                           "wire_sha256":obstructions._sha(wire),"content_encoding":"identity",
                           "content_length":len(wire),"error_code":None,"error_type":None}
                (capture_root / f"{name}.receipt.json").write_bytes(obstructions._canonical(receipt)+b"\n")
                receipts[name] = receipt
            roster = capture_roster_digests(capture_root)
            (capture_root / "capture_manifest.json").write_bytes(obstructions._canonical({
                "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
            with patch.object(obstructions, "EXPECTED_COUNT", 2), patch.object(obstructions, "PAGE_SIZE", 1), \
                 patch.object(obstructions, "_fetch_retry", side_effect=AssertionError("replay must be offline")):
                first = obstructions.capture_usace_obstruction_locations(storage, version="vfixture", capture_directory=str(capture_root))
                # A later retrieval receipt can have a new observation time while
                # the source payload bytes are unchanged; reuse the sealed first
                # candidate and its original raw receipts in that case.
                for name, receipt in receipts.items():
                    receipt["observed_at"] = "2026-10-21T00:00:00Z"
                    (capture_root / f"{name}.receipt.json").write_bytes(obstructions._canonical(receipt)+b"\n")
                roster = capture_roster_digests(capture_root, exclude={"capture_manifest.json"})
                (capture_root / "capture_manifest.json").write_bytes(obstructions._canonical({
                    "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
                second = obstructions.capture_usace_obstruction_locations(storage, version="vfixture-again", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertEqual(first["feature_count"], 2)
            stored_prefix = (
                f"{obstructions.DATASET_SLUG}/{obstructions.FILE_SLUG}/{first['version']}"
            )
            stored_manifest = json.loads(
                storage.read_key(f"{stored_prefix}/metadata/source_manifest.json")
            )
            self.assertTrue(all(
                item["retrieved_at"] == "2026-10-20T00:00:00Z"
                for item in stored_manifest["inputs"]
            ))

            # Same decoded Esri records after whitespace normalization still
            # produce a distinct version when retained source bytes change.
            page = capture_root / "page_001.body"
            wire = capture_root / "page_001.wire"
            changed_body = b" " + page.read_bytes() + b" "
            page.write_bytes(changed_body)
            wire.write_bytes(changed_body)
            receipt = receipts["page_001"]
            receipt.update({
                "observed_at":"2026-10-22T00:00:00Z",
                "bytes":len(changed_body),"sha256":obstructions._sha(changed_body),
                "wire_bytes":len(changed_body),"wire_sha256":obstructions._sha(changed_body),
                "content_length":len(changed_body),
            })
            (capture_root / "page_001.receipt.json").write_bytes(obstructions._canonical(receipt)+b"\n")
            roster = capture_roster_digests(capture_root, exclude={"capture_manifest.json"})
            (capture_root / "capture_manifest.json").write_bytes(obstructions._canonical({
                "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
            with patch.object(obstructions, "EXPECTED_COUNT", 2), patch.object(obstructions, "PAGE_SIZE", 1):
                changed = obstructions.capture_usace_obstruction_locations(
                    storage, version="vfixture-bytes-changed", capture_directory=str(capture_root))
            self.assertNotEqual(first["version"], changed["version"])
            self.assertFalse(changed.get("reused", False))
            self.assertEqual(changed["feature_count"], 2)

            roster["unexpected.body"] = {"bytes":0,"sha256":obstructions._sha(b"")}
            (capture_root / "capture_manifest.json").write_bytes(obstructions._canonical({
                "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
            with patch.object(obstructions, "EXPECTED_COUNT", 2), patch.object(obstructions, "PAGE_SIZE", 1):
                with self.assertRaisesRegex(ValueError, "retained_capture_roster_digest_mismatch|capture_roster_mismatch"):
                    obstructions.capture_usace_obstruction_locations(storage, version="vfixture", capture_directory=str(capture_root))

    def test_incomplete_acquisition_is_manifested_and_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            body = b'{"features":[]}'
            (root / "layer_before.body").write_bytes(body)
            (root / "layer_before.wire").write_bytes(body)
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            obstructions._record_incomplete(root, storage, "vfailed", {"layer_before":(body,{})}, ValueError("source_count_drift"))
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertTrue(list((Path(tmp) / "staging" / "_quarantine" / obstructions.DATASET_SLUG).iterdir()))


if __name__ == "__main__":
    unittest.main()
