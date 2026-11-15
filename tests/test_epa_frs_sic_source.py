import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets import epa_frs_sic_powerplants as worker
from dagster_hifld.assets.epa_frs_sic_powerplants import (
    DATASET_SLUG, FILE_SLUG, PROFILE, _canonical, _reuse, _schema_sha, _sha, _typed_parquet,
)
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.construction_state import seal_first_construction
from dagster_hifld.native_verification import normalize_native_input
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, canonical_construction_bundle, processing_identity,
)
from dagster_hifld.sources import epa_frs_common as frs_common
from dagster_hifld.sources.epa_frs_arcgis_table import (
    ArcGISTableCaptureError, _metadata_contract, capture_table,
)

TINY_ROWS = [
    {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
     "PRIMARY_INDICATOR": "PRIMARY"},
    {"OBJECTID": 2, "REGISTRY_ID": "009876543210", "SIC_CODE": "0041",
     "PRIMARY_INDICATOR": "SECONDARY"},
]


def _tiny_sic_client() -> httpx.Client:
    profile = replace(PROFILE, expected_rows=len(TINY_ROWS))
    fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
              for name, esri_type, nullable, length in profile.fields]
    table = {"name": profile.table_name, "geometryType": None, "fields": fields,
             "relationships": []}
    service = {"description": "EIA data as of June 2014.",
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


class EPAFRSSICTests(unittest.TestCase):
    def test_registered_cache_reuse_rejects_tampered_catalog_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root), prefix="", backend="local")
            version = "v-cache-quality-regression"
            prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
            fields = [
                {"name": name, "type": esri_type, "nullable": nullable, "length": length}
                for name, esri_type, nullable, length in PROFILE.fields
            ]
            rows = [
                {"OBJECTID": object_id, "REGISTRY_ID": f"{object_id:012d}",
                 "SIC_CODE": f"{object_id % 10000:04d}", "PRIMARY_INDICATOR": "PRIMARY"}
                for object_id in range(1, PROFILE.expected_rows + 1)
            ]
            objects: dict[str, dict[str, object]] = {}

            def stage(name: str, body: bytes) -> None:
                storage.write_key(f"{prefix}/{name}", body)
                objects[name] = {"bytes": len(body), "sha256": _sha(body)}

            stage("unknown/" + FILE_SLUG + ".parquet", _typed_parquet(rows, tuple(fields)))
            source_manifest = {
                "title": "EPA FRS SIC cache regression fixture",
                "description": "Synthetic nonspatial test fixture.",
                "source_url": PROFILE.table_url,
                "source_vintage": PROFILE.source_vintage,
                "native_geometry": "absent",
                "refresh_status": "STOPPED",
            }
            stage("metadata/source_manifest.json", _canonical(source_manifest))
            identity = processing_identity(Path(__file__).parents[1])
            fingerprint = _sha(_canonical(identity))
            stage("metadata/processing_identity.json", _canonical(identity))
            stage("metadata/source_ids.json", _canonical(list(range(1, PROFILE.expected_rows + 1))))
            stage("metadata/published_candidate_ids.json", _canonical(list(range(1, PROFILE.expected_rows + 1))))
            stage("metadata/source_capture.json", _canonical({"refresh_status": "STOPPED"}))
            stage("metadata/source_audit.json", _canonical({"drift_reasons": []}))
            stage("metadata/candidate_ready.json", _canonical({"status": "READY"}))

            receipts = {}
            for name in ("service", "portal_item", "service_after", "table_before", "count_before",
                         "ids_before", "table_after", "count_after", "ids_after", "page_000"):
                body = b"{}"
                receipt = {"complete_body": True, "status": 200, "bytes": len(body), "sha256": _sha(body),
                           "wire_bytes": len(body), "wire_sha256": _sha(body)}
                receipts[name] = receipt
                stage(f"raw/{name}.body", body)
                stage(f"raw/{name}.wire", body)
                stage(f"raw/{name}.receipt.json", _canonical(receipt) + b"\n")

            resolved = load_resolved_source_manifest(storage, DATASET_SLUG, FILE_SLUG, version).metadata
            summary = summarize_staged_catalog(
                storage, DATASET_SLUG, FILE_SLUG, version, DATASET_SLUG, source_metadata=resolved
            )
            quality_key = f"{prefix}/metadata/quality_manifest.json"
            storage.write_key(quality_key, _canonical(summary.quality_manifest))
            storage.write_key(f"{prefix}/metadata/data_dictionary.json", _canonical(summary.data_dictionary))

            payload = {
                "processing_identity": identity,
                "source_record_sha256": _sha(_canonical(rows)),
                "source_schema_sha256": _schema_sha(fields),
                "capture_receipts": receipts,
                "objects": objects,
            }
            state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
            seal_first_construction(
                storage, state_key, version, fingerprint,
                canonical_construction_bundle(version, fingerprint, payload),
            )

            self.assertTrue(_reuse(storage, version=version, identity=identity,
                                   fingerprint=fingerprint, offline=True)["offline_replay"])
            tampered = dict(summary.quality_manifest)
            tampered["quality_check_passed"] = False
            storage.write_key(quality_key, _canonical(tampered))
            with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
                _reuse(storage, version=version, identity=identity, fingerprint=fingerprint, offline=True)

    def test_hosted_epa_item_allows_undeclared_join_only_with_exact_item_identity(self):
        profile = replace(
            PROFILE, service_url="https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_Wastewater/FeatureServer",
            table_id=2, table_name="Integrated Compliance Information System (ICIS) Wastewater Treatment Plants SIC Codes",
            expected_rows=1, relationship_name=None, related_table_id=None,
            relationship_key=None, relationship_cardinality=None, source_vintage=None,
            portal_item_id="item-1", portal_item_owner="EPA_GEO", portal_item_title="ICIS WWTP",
        )
        fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
                  for name, esri_type, nullable, length in profile.fields]
        service = {"description": "EPA maintained ICIS wastewater source.", "tables": [
            {"id": profile.table_id, "name": profile.table_name}]}
        table = {"name": profile.table_name, "geometryType": None, "fields": fields,
                 "relationships": []}
        row = {"OBJECTID": 9, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
               "PRIMARY_INDICATOR": "PRIMARY"}
        portal_item = {"id": "item-1", "owner": "EPA_GEO", "title": "ICIS WWTP",
                       "type": "Feature Service", "url": profile.service_url, "modified": 123}

        class JSONStream(httpx.SyncByteStream):
            def __init__(self, payload):
                self.body = json.dumps(payload, separators=(",", ":")).encode()

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        def respond(request):
            path = urlparse(str(request.url)).path
            query = parse_qs(urlparse(str(request.url)).query)
            if "/sharing/rest/content/items/" in path:
                payload = portal_item
            elif path.rstrip("/").endswith("FeatureServer"):
                payload = service
            elif path.endswith(f"/{profile.table_id}"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": 1}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [9]}
            else:
                payload = {"features": [{"attributes": row}]}
            return httpx.Response(200, stream=JSONStream(payload),
                                  headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with client:
                captured, manifest, _ = capture_table(client, profile, Path(tmp))
        self.assertEqual(captured, [row])
        self.assertIsNone(manifest["declared_relationship_contract"])
        self.assertEqual(manifest["relationship"], [])
        self.assertEqual(manifest["portal_item"]["owner"], "EPA_GEO")

        for bad_item in ({**portal_item, "owner": "mirror"},
                         {**portal_item, "url": "https://example.invalid/service"}):
            def bad_respond(request, item=bad_item):
                path = urlparse(str(request.url)).path
                if "/sharing/rest/content/items/" in path:
                    return httpx.Response(200, stream=JSONStream(item),
                                          headers={"content-type": "application/json"})
                return respond(request)
            with tempfile.TemporaryDirectory() as tmp:
                client = httpx.Client(transport=httpx.MockTransport(bad_respond))
                with client, self.assertRaisesRegex(ArcGISTableCaptureError,
                                                    "source_portal_item_identity_changed"):
                    capture_table(client, profile, Path(tmp))

    def test_hosted_item_identity_and_relationship_contracts_are_all_or_none(self):
        with self.assertRaisesRegex(ValueError, "portal_item_contract_must_be_complete"):
            replace(PROFILE, portal_item_id=None)
        with self.assertRaisesRegex(ValueError, "relationship_contract_must_be_complete_or_undeclared"):
            replace(PROFILE, relationship_name="POWERPLANT_LAYER")
        with self.assertRaisesRegex(ValueError, "source_identity_contract_required"):
            replace(PROFILE, source_vintage=None, portal_item_id=None,
                    portal_item_owner=None, portal_item_title=None)

    def test_complete_source_capture_brackets_exact_ids_and_retains_receipts(self):
        profile = replace(PROFILE, expected_rows=2)
        fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
                  for name, esri_type, nullable, length in profile.fields]
        table = {"name": profile.table_name, "geometryType": None, "fields": fields,
                 "relationships": []}
        service = {"description": "EIA data as of June 2014.",
                   "tables": [{"id": profile.table_id, "name": profile.table_name}]}
        portal_item = {
            "id": profile.portal_item_id, "owner": profile.portal_item_owner,
            "title": profile.portal_item_title, "type": "Feature Service",
            "url": profile.service_url, "modified": 1769190978000,
        }
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "SECONDARY"},
        ]

        class JSONStream(httpx.SyncByteStream):
            def __init__(self, payload):
                self.body = json.dumps(payload, separators=(",", ":")).encode()

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        def respond(request):
            path = urlparse(str(request.url)).path
            query = parse_qs(urlparse(str(request.url)).query)
            if "/sharing/rest/content/items/" in path:
                payload = portal_item
            elif path.rstrip("/").endswith("FeatureServer"):
                payload = service
            elif path.endswith(f"/{profile.table_id}"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": 2}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [1, 2]}
            else:
                payload = {"features": [{"attributes": row} for row in rows]}
            return httpx.Response(200, stream=JSONStream(payload), headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            client = httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False)
            with client:
                captured, manifest, receipts = capture_table(client, profile, Path(tmp))
            self.assertEqual(captured, rows)
            self.assertTrue(manifest["source_ids_equal_before_after"])
            self.assertEqual(manifest["rows_captured"], 2)
            self.assertEqual(set(receipts), {"service", "portal_item", "table_before", "count_before",
                "ids_before", "page_000", "table_after", "service_after", "count_after", "ids_after"})
            self.assertTrue(all(receipt["complete_body"] for receipt in manifest["response_receipts"].values()))

    def test_exact_nonspatial_schema_and_native_values_survive_parquet(self):
        fields = [
            {"name": name, "type": esri_type, "nullable": nullable, "length": length}
            for name, esri_type, nullable, length in PROFILE.fields
        ]
        metadata = {
            "name": PROFILE.table_name,
            "geometryType": None,
            "fields": fields,
            "relationships": [],
        }
        self.assertEqual(len(_metadata_contract(metadata, PROFILE)), 4)
        changed = {**metadata, "fields": fields + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                                    "nullable": True, "length": 4}]}
        with self.assertRaisesRegex(ArcGISTableCaptureError, "source_table_schema_changed"):
            _metadata_contract(changed, PROFILE)

        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "SIC_CODE": "0041",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "SIC_CODE": "OWNE",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "table.parquet"
            body = _typed_parquet(rows, tuple(fields))
            path.write_bytes(body)
            table = pq.read_table(path)
            self.assertEqual(table.to_pylist(), rows)
            self.assertEqual(table.schema.names, list(PROFILE.field_names))
            self.assertEqual(str(table.schema.field("SIC_CODE").type), "string")
            self.assertIs(table.schema.field("OBJECTID").nullable, False)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),
                             hashlib.sha256(body).hexdigest())
            with tempfile.TemporaryDirectory() as evidence_root:
                receipt = normalize_native_input(
                    {"schema_version": 1, "format": "parquet", "root": tmp,
                     "objects": [{"key": "table.parquet", "sha256": hashlib.sha256(body).hexdigest(),
                                  "bytes": len(body), "rows": len(rows)}]},
                    {"schema_version": 1, "status": "proposed", "geometry": False, "crs": None,
                     "id_fields": ["OBJECTID"], "coverage_fields": ["REGISTRY_ID"]},
                    Path(evidence_root) / "native-evidence",
                )
                self.assertEqual(receipt["status"], "complete")
                self.assertIs(receipt["policy"]["geometry"], False)
                normalized = [json.loads(line) for line in
                              (Path(evidence_root) / "native-evidence" / "normalized_records.jsonl").read_text().splitlines()]
                self.assertTrue(all(row["geometry"] is None for row in normalized))


    def _fixture_mode(self):
        return [
            patch.object(worker, "PROFILE", replace(PROFILE, expected_rows=len(TINY_ROWS))),
            patch.object(worker, "EXPECTED_ROWS", len(TINY_ROWS)),
            patch.object(worker, "EXPECTED_RECORD_SHA256", _sha(_canonical(TINY_ROWS))),
            patch.object(worker, "EXPECTED_IDS_SHA256",
                         _sha(_canonical(sorted(row["OBJECTID"] for row in TINY_ROWS)))),
            patch.object(worker, "EXPECTED_SCHEMA_SHA256",
                         _schema_sha([{"name": name, "type": esri_type, "nullable": nullable,
                                       "length": length}
                                      for name, esri_type, nullable, length in PROFILE.fields])),
            patch.object(frs_common, "validate_construction_code_identity"),
        ]

    def test_repeat_live_and_both_offline_modes_reuse_the_portal_item_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            contexts = self._fixture_mode()
            for context in contexts:
                context.start()
            try:
                with _tiny_sic_client() as client:
                    captured = worker.capture_epa_frs_sic_table(storage, client=client)
                    repeat = worker.capture_epa_frs_sic_table(storage, client=client)
                    replayed = worker.capture_epa_frs_sic_table(
                        storage, offline_replay_version=captured["version"])
                    retained = worker.capture_epa_frs_sic_table(
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
            state_key = f"_construction-state/{DATASET_SLUG}/{captured['version']}.json"
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
                with _tiny_sic_client() as client:
                    captured = worker.capture_epa_frs_sic_table(storage, client=client)
                version = captured["version"]
                state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
                payload = json.loads(storage.read_key(state_key))["payload"]
                self.assertIn("portal_item", payload["capture_receipts"])
                stripped = {**payload,
                            "capture_receipts": {name: receipt
                                                 for name, receipt in payload["capture_receipts"].items()
                                                 if name != "portal_item"},
                            "objects": {name: descriptor for name, descriptor in payload["objects"].items()
                                        if not name.startswith("raw/portal_item.")}}
                identity = processing_identity(Path(worker.__file__).parents[3])
                fingerprint = _sha(_canonical(identity))
                replay_root = Path(tmp) / "replay"
                replay_root.mkdir()
                replay_storage = StagingStorageResource(local_dir=str(replay_root),
                                                        use_local=True, backend="local")
                frs_common.seal_ready_candidate(replay_storage, state_key, version, fingerprint,
                                                stripped)
                with self.assertRaisesRegex(worker.EPAFRSSICError,
                                            "cached_candidate_roster_invalid"):
                    worker.capture_epa_frs_sic_table(replay_storage,
                                                     offline_replay_version=version)
            finally:
                for context in reversed(contexts):
                    context.stop()


if __name__ == "__main__":
    unittest.main()
