"""Focused contract and offline replay tests for the USACE NOTICE_MARK_POINT adapter."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_notice_marks as marks


class UsaceNoticeMarksTests(unittest.TestCase):
    def _captures(self):
        fields = [{"name": name, "type": kind, "alias": name,
                   **({"length":254} if kind == "esriFieldTypeString" else {})}
                  for name, kind in marks.EXPECTED_FIELDS]
        layer = {"id":18,"name":"NOTICE_MARK_POINT","geometryType":"esriGeometryPoint",
                 "spatialReference":{"latestWkid":4326},"maxRecordCount":2000,"fields":fields}
        ids = [101, 205]
        attrs = []
        for oid in ids:
            attrs.append({name:(oid if name == "OBJECTID" else f"F{oid}" if name == "Feature_ID" else 3 if name == "Scale_Minimum" else None)
                          for name,kind in marks.EXPECTED_FIELDS if name != "SHAPE"})
        docs = {
            "layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},
            "ids_seed":{"objectIds":ids},"ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
            "page_000":{"spatialReference":{"latestWkid":4326},"features":[{"attributes":attrs[0],"geometry":{"x":-90.0,"y":30.0}}]},
            "page_001":{"spatialReference":{"latestWkid":4326},"features":[{"attributes":attrs[1],"geometry":{"x":-91.0,"y":31.0}}]},
        }
        return fields, {name:(json.dumps(doc,separators=(",",":")).encode(), {}) for name,doc in docs.items()}

    def test_small_complete_population_and_drift_failures(self):
        fields, captures = self._captures()
        with patch.object(marks, "EXPECTED_COUNT", 2), patch.object(marks, "PAGE_SIZE", 1):
            parsed, features, audit = marks.validate_notice_snapshot(captures)
            self.assertEqual(parsed, fields)
            self.assertEqual(len(features), 2)
            self.assertEqual(audit["source_id_min"], 101)
            self.assertEqual(audit["source_feature_count"], 2)

            bad = dict(captures)
            bad["count_after"] = (b'{"count":3}', {})
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                marks.validate_notice_snapshot(bad)

            bad = dict(captures)
            changed_layer = json.loads(captures["layer_before"][0])
            changed_layer["fields"][2]["type"] = "esriFieldTypeInteger"
            bad["layer_before"] = (json.dumps(changed_layer).encode(), {})
            with self.assertRaisesRegex(ValueError, "source_layer_schema_or_spatial_contract_drift"):
                marks.validate_notice_snapshot(bad)

            bad = dict(captures)
            bad["ids_after"] = (b'{"objectIds":[101,206]}', {})
            with self.assertRaisesRegex(ValueError, "source_id_population_changed_or_invalid"):
                marks.validate_notice_snapshot(bad)

            bad = dict(captures)
            bad["page_001"] = (b'{"error":{"code":500}}', {})
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncated"):
                marks.validate_notice_snapshot(bad)

            bad = dict(captures)
            del bad["page_001"]
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                marks.validate_notice_snapshot(bad)

            bad = dict(captures)
            bad["page_000"] = (json.dumps({"spatialReference":{"latestWkid":4326},"features":[
                {"attributes":json.loads(captures["page_000"][0])["features"][0]["attributes"],"geometry":{"x":-90.0,"y":30.0,"z":4.0}}
            ]}).encode(), {})
            with self.assertRaisesRegex(ValueError, "source_point_ordinate_roster_drift"):
                _fields, features, _audit = marks.validate_notice_snapshot(bad)
                marks.notice_geoparquet(_fields, features)

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
                url = marks.LAYER_URL + "?f=pjson"
                receipt = {"method":"GET","url":url,"final_url":url,
                           "observed_at":"2026-10-20T00:00:00Z","status":200,"complete_body":True,
                           "bytes":len(body),"sha256":marks._sha(body),"wire_bytes":len(wire),
                           "wire_sha256":marks._sha(wire),"content_encoding":"identity",
                           "content_length":len(wire),"error_code":None,"error_type":None}
                (capture_root / f"{name}.receipt.json").write_bytes(marks._canonical(receipt)+b"\n")
                receipts[name] = receipt
            roster = capture_roster_digests(capture_root)
            (capture_root / "capture_manifest.json").write_bytes(marks._canonical({
                "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
            with patch.object(marks, "EXPECTED_COUNT", 2), patch.object(marks, "PAGE_SIZE", 1), \
                 patch.object(marks, "_fetch_retry", side_effect=AssertionError("replay must be offline")):
                first = marks.capture_usace_notice_marks(storage, version="vfixture", capture_directory=str(capture_root))
                # A later retrieval receipt can have a new observation time while
                # the source payload bytes are unchanged; reuse the sealed first
                # candidate and its original raw receipts in that case.
                for name, receipt in receipts.items():
                    receipt["observed_at"] = "2026-10-21T00:00:00Z"
                    (capture_root / f"{name}.receipt.json").write_bytes(marks._canonical(receipt)+b"\n")
                roster = capture_roster_digests(capture_root, exclude={"capture_manifest.json"})
                (capture_root / "capture_manifest.json").write_bytes(marks._canonical({
                    "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
                second = marks.capture_usace_notice_marks(storage, version="vfixture-again", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertEqual(first["feature_count"], 2)

            roster["unexpected.body"] = {"bytes":0,"sha256":marks._sha(b"")}
            (capture_root / "capture_manifest.json").write_bytes(marks._canonical({
                "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
            with patch.object(marks, "EXPECTED_COUNT", 2), patch.object(marks, "PAGE_SIZE", 1):
                with self.assertRaisesRegex(ValueError, "retained_capture_roster_digest_mismatch|capture_roster_mismatch"):
                    marks.capture_usace_notice_marks(storage, version="vfixture", capture_directory=str(capture_root))

    def test_raw_evidence_change_with_same_rows_gets_new_candidate_identity(self):
        _fields, captures = self._captures()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))

            def write_capture(directory: Path, *, reformat_page: bool):
                directory.mkdir()
                receipts = {}
                for name, (original, _unused) in captures.items():
                    body = original
                    if reformat_page and name == "page_000":
                        body = json.dumps(json.loads(body), indent=2).encode()
                    wire = body
                    (directory / f"{name}.body").write_bytes(body)
                    (directory / f"{name}.wire").write_bytes(wire)
                    url = marks.LAYER_URL + "?f=pjson"
                    receipt = {"method":"GET","url":url,"final_url":url,
                               "observed_at":"2026-10-20T00:00:00Z","status":200,"complete_body":True,
                               "bytes":len(body),"sha256":marks._sha(body),"wire_bytes":len(wire),
                               "wire_sha256":marks._sha(wire),"content_encoding":"identity",
                               "content_length":len(wire),"error_code":None,"error_type":None}
                    (directory / f"{name}.receipt.json").write_bytes(marks._canonical(receipt)+b"\n")
                    receipts[name] = receipt
                roster = capture_roster_digests(directory)
                (directory / "capture_manifest.json").write_bytes(marks._canonical({
                    "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")

            first_capture, second_capture = root / "first", root / "second"
            write_capture(first_capture, reformat_page=False)
            write_capture(second_capture, reformat_page=True)
            with patch.object(marks, "EXPECTED_COUNT", 2), patch.object(marks, "PAGE_SIZE", 1), \
                 patch.object(marks, "_fetch_retry", side_effect=AssertionError("replay must be offline")):
                first = marks.capture_usace_notice_marks(storage, version="vraw-a", capture_directory=str(first_capture))
                second = marks.capture_usace_notice_marks(storage, version="vraw-b", capture_directory=str(second_capture))
                # Receipt timestamps do not affect evidence identity; the sealed
                # first receipts remain authoritative when the source bytes match.
                receipts = {}
                for path in second_capture.glob("*.receipt.json"):
                    receipt = json.loads(path.read_bytes())
                    receipt["observed_at"] = "2026-10-21T00:00:00Z"
                    path.write_bytes(marks._canonical(receipt)+b"\n")
                    receipts[path.name.removesuffix(".receipt.json")] = receipt
                roster = capture_roster_digests(second_capture, exclude={"capture_manifest.json"})
                (second_capture / "capture_manifest.json").write_bytes(marks._canonical({
                    "complete":True,"receipts":receipts,"artifact_roster":roster})+b"\n")
                third = marks.capture_usace_notice_marks(storage, version="vraw-c", capture_directory=str(second_capture))
            self.assertNotEqual(first["version"], second["version"])
            self.assertEqual(second["version"], third["version"])
            self.assertTrue(third["reused"])
            first_audit = json.loads(storage.read_key(f"{marks.DATASET_SLUG}/{marks.FILE_SLUG}/{first['version']}/metadata/source_audit.json"))
            second_audit = json.loads(storage.read_key(f"{marks.DATASET_SLUG}/{marks.FILE_SLUG}/{second['version']}/metadata/source_audit.json"))
            self.assertEqual(first_audit["source_record_sha256"], second_audit["source_record_sha256"])
            self.assertEqual(first_audit["source_geometry_sha256"], second_audit["source_geometry_sha256"])
            self.assertNotEqual(first_audit["source_capture_sha256"], second_audit["source_capture_sha256"])

    def test_incomplete_acquisition_is_manifested_and_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            body = b'{"features":[]}'
            (root / "layer_before.body").write_bytes(body)
            (root / "layer_before.wire").write_bytes(body)
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            marks._record_incomplete(root, storage, "vfailed", {"layer_before":(body,{})}, ValueError("source_count_drift"))
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertTrue(list((Path(tmp) / "staging" / "_quarantine" / marks.DATASET_SLUG).iterdir()))


if __name__ == "__main__":
    unittest.main()
