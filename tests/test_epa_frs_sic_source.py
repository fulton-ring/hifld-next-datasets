import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import httpx
import pyarrow.parquet as pq

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
from dagster_hifld.sources.epa_frs_arcgis_table import (
    ArcGISTableCaptureError, _metadata_contract, capture_table,
)


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
            for name in ("service", "service_after", "table_before", "count_before", "ids_before",
                         "table_after", "count_after", "ids_after", "page_000"):
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
            replace(PROFILE, portal_item_id="item-1")
        with self.assertRaisesRegex(ValueError, "relationship_contract_must_be_complete_or_undeclared"):
            replace(PROFILE, relationship_name=None)
        with self.assertRaisesRegex(ValueError, "source_identity_contract_required"):
            replace(PROFILE, source_vintage=None)

    def test_complete_source_capture_brackets_exact_ids_and_retains_receipts(self):
        profile = replace(PROFILE, expected_rows=2)
        fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
                  for name, esri_type, nullable, length in profile.fields]
        table = {"name": profile.table_name, "geometryType": None, "fields": fields,
                 "relationships": [{"name": profile.relationship_name,
                                    "relatedTableId": profile.related_table_id,
                                    "keyField": profile.relationship_key,
                                    "cardinality": profile.relationship_cardinality}]}
        service = {"description": "EIA data as of June 2014.",
                   "tables": [{"id": profile.table_id, "name": profile.table_name}]}
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
            if path.endswith("/MapServer"):
                payload = service
            elif path.endswith("/15"):
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
            self.assertEqual(set(receipts), {"service", "table_before", "count_before", "ids_before",
                "page_000", "table_after", "service_after", "count_after", "ids_after"})
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
            "relationships": [{"name": PROFILE.relationship_name,
                               "relatedTableId": PROFILE.related_table_id,
                               "keyField": PROFILE.relationship_key,
                               "cardinality": PROFILE.relationship_cardinality}],
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


if __name__ == "__main__":
    unittest.main()
