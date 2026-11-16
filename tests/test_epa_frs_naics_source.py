from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets import epa_frs_naics_powerplants as worker
from dagster_hifld.assets.epa_frs_naics_powerplants import (
    TABLE_PROFILE,
    _canonical,
    _typed_parquet,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.native_verification import normalize_native_input

TINY_ROWS = [
    {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "NAICS_CODE": "002211",
     "PRIMARY_INDICATOR": "PRIMARY"},
    {"OBJECTID": 2, "REGISTRY_ID": "009876543210", "NAICS_CODE": "22",
     "PRIMARY_INDICATOR": "UNKNOWN"},
]


def _tiny_source_client() -> httpx.Client:
    profile = replace(TABLE_PROFILE, expected_rows=len(TINY_ROWS))
    fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
              for name, esri_type, nullable, length in profile.fields]
    table = {"name": profile.table_name, "geometryType": None, "fields": fields,
             "relationships": []}
    service = {"description": "EIA-860 data most-current as of June 2014.",
               "tables": [{"id": profile.table_id, "name": profile.table_name}]}
    portal_item = {"id": profile.portal_item_id, "owner": profile.portal_item_owner,
                   "title": profile.portal_item_title, "type": "Feature Service",
                   "url": profile.service_url, "modified": 1769190978000}

    class JSONStream(httpx.SyncByteStream):
        def __init__(self, payload):
            self.body = json.dumps(payload, separators=(",", ":")).encode()

        def __iter__(self):
            yield self.body

        def close(self):
            return None

    def respond(request):
        parsed = urlparse(str(request.url))
        query = parse_qs(parsed.query)
        if "/sharing/rest/content/items/" in parsed.path:
            payload = portal_item
        elif parsed.path.rstrip("/").endswith("FeatureServer"):
            payload = service
        elif parsed.path.endswith(f"/{profile.table_id}"):
            payload = table
        elif query.get("returnCountOnly") == ["true"]:
            payload = {"count": len(TINY_ROWS)}
        elif query.get("returnIdsOnly") == ["true"]:
            payload = {"objectIds": [row["OBJECTID"] for row in TINY_ROWS]}
        else:
            payload = {"features": [{"attributes": row} for row in TINY_ROWS]}
        return httpx.Response(200, stream=JSONStream(payload),
                              headers={"content-type": "application/json"})

    return httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False)


def _fixture_pins():
    fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
              for name, esri_type, nullable, length
              in replace(TABLE_PROFILE, expected_rows=len(TINY_ROWS)).fields]
    return {
        "rows": len(TINY_ROWS),
        "records": hashlib.sha256(_canonical(TINY_ROWS)).hexdigest(),
        "ids": hashlib.sha256(_canonical(sorted(row["OBJECTID"] for row in TINY_ROWS))).hexdigest(),
        "schema": hashlib.sha256(_canonical(fields)).hexdigest(),
    }


class EPAFRSNAICSSourceTests(unittest.TestCase):
    def test_nonspatial_table_round_trips_native_fields_and_string_codes(self):
        records = [
            {
                "OBJECTID": 7,
                "REGISTRY_ID": "001234567890",
                "NAICS_CODE": "002211",
                "PRIMARY_INDICATOR": "PRIMARY",
            },
            {
                "OBJECTID": 8,
                "REGISTRY_ID": None,
                "NAICS_CODE": "22",
                "PRIMARY_INDICATOR": "UNKNOWN",
            },
        ]
        fields = (
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "REGISTRY_ID", "type": "esriFieldTypeString", "nullable": True, "length": 12},
            {"name": "NAICS_CODE", "type": "esriFieldTypeString", "nullable": False, "length": 6},
            {"name": "PRIMARY_INDICATOR", "type": "esriFieldTypeString", "nullable": False, "length": 10},
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_root = root / "input"
            input_root.mkdir()
            path = input_root / "native.parquet"
            path.write_bytes(_typed_parquet(records, fields))
            table = pq.read_table(path)
            self.assertEqual(table.schema.names, ["OBJECTID", "REGISTRY_ID", "NAICS_CODE", "PRIMARY_INDICATOR"])
            self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
            self.assertTrue(table.schema.field("OBJECTID").nullable is False)
            self.assertEqual(str(table.schema.field("NAICS_CODE").type), "string")
            self.assertTrue(table.schema.field("REGISTRY_ID").nullable)
            self.assertEqual(table.to_pylist(), records)

            spec = {
                "schema_version": 1,
                "format": "parquet",
                "root": str(input_root),
                "objects": [
                    {
                        "key": "native.parquet",
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "bytes": path.stat().st_size,
                        "rows": 2,
                    }
                ],
            }
            receipt = normalize_native_input(
                spec,
                {
                    "schema_version": 1,
                    "status": "proposed",
                    "geometry": False,
                    "crs": None,
                    "id_fields": ["OBJECTID"],
                    "coverage_fields": ["REGISTRY_ID"],
                },
                root / "evidence",
            )
            self.assertEqual(receipt["status"], "complete")
            normalized = [
                json.loads(line)
                for line in (root / "evidence" / "normalized_records.jsonl").read_text().splitlines()
            ]
            self.assertEqual(normalized[0]["properties"]["NAICS_CODE"], ["string", "002211"])
            self.assertEqual(normalized[0]["properties"]["REGISTRY_ID"], ["string", "001234567890"])
            self.assertIsNone(normalized[1]["properties"]["REGISTRY_ID"])
            self.assertIsNone(normalized[0]["geometry"])


class EPAFRSNAICSCaptureLifecycleTests(unittest.TestCase):
    def _fixture_mode(self):
        pins = _fixture_pins()
        return [
            patch.object(worker, "TABLE_PROFILE",
                         replace(TABLE_PROFILE, expected_rows=pins["rows"])),
            patch.object(worker, "EXPECTED_SOURCE_ROWS", pins["rows"]),
            patch.object(worker, "EXPECTED_SOURCE_RECORDS_SHA256", pins["records"]),
            patch.object(worker, "EXPECTED_SOURCE_IDS_SHA256", pins["ids"]),
            patch.object(worker, "EXPECTED_SOURCE_SCHEMA_SHA256", pins["schema"]),
            patch.object(epa_frs_common, "validate_construction_code_identity"),
        ]

    def test_repeat_live_and_both_offline_modes_reuse_the_portal_item_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            contexts = self._fixture_mode()
            for context in contexts:
                context.start()
            try:
                with _tiny_source_client() as client:
                    captured = worker.capture_epa_frs_naics_table(
                        storage, client=client, capture_directory=str(Path(tmp) / "captures"))
                    repeat = worker.capture_epa_frs_naics_table(
                        storage, client=client, capture_directory=str(Path(tmp) / "captures2"))
                    replayed = worker.capture_epa_frs_naics_table(
                        storage, offline_replay_version=captured["version"])
                    retained = worker.capture_epa_frs_naics_table(
                        storage, offline_capture_version=captured["version"])
            finally:
                for context in reversed(contexts):
                    context.stop()
            self.assertEqual(captured["row_count"], 2)
            self.assertFalse(captured["release_accepted"])
            self.assertEqual(repeat["version"], captured["version"])
            self.assertTrue(repeat["reused"])
            self.assertTrue(replayed["reused"])
            self.assertTrue(replayed["offline_replay"])
            self.assertEqual(retained["version"], captured["version"])
            self.assertTrue(retained["reused"])
            state_key = f"_construction-state/{worker.DATASET_SLUG}/{captured['version']}.json"
            sealed = json.loads(storage.read_key(state_key))
            self.assertIn("portal_item", sealed["payload"]["capture_receipts"])
            self.assertIn("raw/portal_item.body", sealed["payload"]["objects"])

    def test_repeat_live_capture_rejects_a_roster_missing_the_portal_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            contexts = self._fixture_mode()
            for context in contexts:
                context.start()
            try:
                with _tiny_source_client() as client:
                    captured = worker.capture_epa_frs_naics_table(
                        storage, client=client, capture_directory=str(Path(tmp) / "captures"))
                version = captured["version"]
                state_key = f"_construction-state/{worker.DATASET_SLUG}/{version}.json"
                payload = json.loads(storage.read_key(state_key))["payload"]
                self.assertIn("portal_item", payload["capture_receipts"])
                stripped = {**payload,
                            "capture_receipts": {name: receipt
                                                 for name, receipt in payload["capture_receipts"].items()
                                                 if name != "portal_item"},
                            "objects": {name: descriptor for name, descriptor in payload["objects"].items()
                                        if not name.startswith("raw/portal_item.")}}
                identity = epa_frs_common.processing_identity(Path(worker.__file__).parents[3])
                fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
                replay_root = Path(tmp) / "replay"
                replay_root.mkdir()
                replay_storage = StagingStorageResource(local_dir=str(replay_root),
                                                        use_local=True, backend="local")
                epa_frs_common.seal_ready_candidate(replay_storage, state_key, version, fingerprint,
                                                    stripped)
                with self.assertRaisesRegex(worker.EPAFRSNAICSError,
                                            "cached_candidate_roster_invalid"):
                    worker.capture_epa_frs_naics_table(replay_storage,
                                                       offline_replay_version=version)
            finally:
                for context in reversed(contexts):
                    context.stop()


if __name__ == "__main__":
    unittest.main()
