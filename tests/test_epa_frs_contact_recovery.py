import gzip
import hashlib
import json
import tempfile
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DagsterInstance, build_asset_context

from dagster_hifld.assets.epa_frs_interest_contact import (
    DATASET_SLUG, EXPECTED_FIELDS, FILE_SLUG, epa_frs_interest_contact,
    _reuse_existing_contact_candidate,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity, write_immutable
from dagster_hifld.sources.epa_frs_contact_stream import (
    FIELDS, TABLE_URL, canonical, compact_capture_receipt_index,
)


class EpaFrsContactRecoveryTests(unittest.TestCase):
    def test_explicit_recovery_preserves_observation_and_rejects_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            storage = StagingStorageResource(local_dir=directory, use_local=True, backend="local")
            repo = Path(__file__).resolve().parents[1]
            identity = processing_identity(repo)
            old_prefix = f"{DATASET_SLUG}/{FILE_SLUG}/vretained-fixture"
            captured_at = "2026-10-19T12:34:56.000000+00:00"
            source_record_sha = hashlib.sha256(b"fixture source records").hexdigest()
            source_ids_sha = hashlib.sha256(b"fixture ids").hexdigest()
            source_schema_sha = hashlib.sha256(b"fixture schema").hexdigest()

            wire = gzip.compress(b"fixture source response")
            receipt = {"name": "page_00000", "method": "GET", "url": TABLE_URL + "/query",
                       "final_url": TABLE_URL + "/query", "query": {"resultOffset": 0},
                       "status": 200, "content_encoding": "gzip", "wire_bytes": len(wire),
                       "wire_sha256": hashlib.sha256(wire).hexdigest(), "decoded_bytes": 23,
                       "decoded_sha256": hashlib.sha256(b"fixture source response").hexdigest(),
                       "complete_body": True}
            archive_path = root / "source-wire-capture.tar"
            with tarfile.open(archive_path, "w") as archive:
                for suffix, body in ((".wire.gz", wire), (".receipt.json", canonical(receipt) + b"\n")):
                    path = root / (receipt["name"] + suffix)
                    path.write_bytes(body)
                    archive.add(path, arcname=path.name, recursive=False)

            fields = tuple({"name": name, "type": esri, "nullable": nullable, "length": length}
                           for name, esri, nullable, length in FIELDS)
            schema = pa.schema([pa.field(name, pa.int64() if name == "OBJECTID" else pa.string(),
                                         nullable=nullable)
                                for name, _esri, nullable, _length in FIELDS])
            parquet_path = root / f"{FILE_SLUG}.parquet"
            def row(object_id):
                value = {"OBJECTID": object_id}
                for name, _esri, nullable, _length in FIELDS[1:]:
                    value[name] = None if nullable else "fixture"
                return value
            table = pa.Table.from_pylist([
                row(1), row(3),
            ], schema=schema)
            pq.write_table(table, parquet_path)

            capture = {"captured_at": captured_at, "row_count": 2,
                       "source_record_sha256": source_record_sha,
                       "source_ids_sha256": source_ids_sha,
                       "source_schema_sha256": source_schema_sha,
                       "fields": list(fields), "source_count_before": 2,
                       "source_count_after": 2, "id_count_before": 2, "id_count_after": 2,
                       "id_roster_equal_before_after": True, "captured_ids_match_roster": True,
                       "source_release_date": None, "response_count": 1, "page_count": 1}
            parquet_descriptor = {"bytes": parquet_path.stat().st_size,
                                  "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest()}
            metadata = {
                "metadata/source_capture.json": canonical(capture) + b"\n",
                "metadata/source_manifest.json": b'{"capture_observed_at":"' + captured_at.encode() + b'"}\n',
                "metadata/source_audit.json": b'{"source_complete":true}\n',
                "metadata/processing_identity.json": canonical(identity) + b"\n",
                "metadata/source_id_roster_summary.json": canonical(
                    {"count": 2, "sha256": source_ids_sha, "complete": True}) + b"\n",
                "metadata/candidate_ready.json": canonical(
                    {"approved": False, "version": "vretained-fixture",
                     "source_record_sha256": source_record_sha,
                     "source_schema_sha256": source_schema_sha,
                     "objects": {f"unknown/{FILE_SLUG}.parquet": parquet_descriptor}}) + b"\n",
            }
            bodies = {"raw/source-wire-capture.tar": archive_path.read_bytes(),
                      f"unknown/{FILE_SLUG}.parquet": parquet_path.read_bytes(), **metadata}
            for name, body in bodies.items():
                write_immutable(storage, f"{old_prefix}/{name}", body)

            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(
                    asset_config={"resume_candidate": old_prefix}, instance=instance)
                result = epa_frs_interest_contact(context, storage)
            self.assertTrue(result["resumed_offline"])
            self.assertEqual(result["captured_at"], captured_at)
            self.assertEqual(storage.read_key(result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                                                 "metadata/source_capture.json")),
                             canonical(capture) + b"\n")

            with tarfile.open(archive_path, "r:") as archive:
                receipts = [json.loads(archive.extractfile(member).read())
                            for member in archive.getmembers() if member.name.endswith(".receipt.json")]
            capture_index = compact_capture_receipt_index(receipts, archive_path)
            current_observation = {"row_count": 2, "source_record_sha256": source_record_sha,
                                   "source_schema_sha256": source_schema_sha,
                                   "source_ids_sha256": source_ids_sha}
            with patch("dagster_hifld.construction_provenance.validate_construction_code_identity"):
                reused = _reuse_existing_contact_candidate(
                    storage, repo, result["candidate_ready"].removesuffix("/metadata/candidate_ready.json"),
                    result["version"], identity, current_observation, capture_index)
            self.assertTrue(reused["reused"])
            self.assertEqual(reused["captured_at"], captured_at)

            # Resuming from the now-sealed candidate after catalog uses its
            # first construction directly and does not decode any retained page.
            current_prefix = result["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    patch("dagster_hifld.assets.epa_frs_interest_contact.compact_capture_receipt_index",
                          side_effect=AssertionError("sealed cache should not re-decode raw pages")), \
                    patch("dagster_hifld.construction_provenance.validate_construction_code_identity"), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": current_prefix},
                                              instance=instance)
                cache_run = epa_frs_interest_contact(context, storage)
            self.assertTrue(cache_run["reused"])
            self.assertTrue(cache_run["resumed_offline"])
            self.assertEqual(cache_run["captured_at"], captured_at)

            source_capture_key = result["candidate_ready"].replace("candidate_ready.json", "source_capture.json")
            Path(directory, source_capture_key).write_bytes(b'{"tampered":true}')
            with patch("dagster_hifld.construction_provenance.validate_construction_code_identity"):
                with self.assertRaisesRegex(ValueError, "cached_contact_candidate_object_changed"):
                    _reuse_existing_contact_candidate(
                        storage, repo, result["candidate_ready"].removesuffix("/metadata/candidate_ready.json"),
                        result["version"], identity, current_observation, capture_index)


if __name__ == "__main__":
    unittest.main()
