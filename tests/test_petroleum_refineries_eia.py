import unittest
import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.petroleum_refineries_eia import (
    petroleum_refineries_eia_draft_schedule,
    petroleum_refineries_eia_historical_candidate_complete,
    petroleum_refineries_eia_ingest,
)
from dagster_hifld.sources import petroleum_refineries_eia as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity, seal_ready_candidate, verify_cached_candidate, write_immutable


class PetroleumRefineriesEiaTests(unittest.TestCase):
    def test_historical_adapter_is_registered_and_refresh_stays_stopped(self):
        self.assertIn(petroleum_refineries_eia_ingest, hifld_ingest_assets)
        self.assertEqual(petroleum_refineries_eia_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        from dagster_hifld.definitions import defs
        self.assertIn(petroleum_refineries_eia_historical_candidate_complete, defs.asset_checks)
        self.assertIn(petroleum_refineries_eia_draft_schedule, defs.schedules)
        self.assertIn(petroleum_refineries_eia_ingest, hifld_dataset_assets)
        self.assertEqual(source.SOURCE_VINTAGE, "2021-01-01")
        self.assertEqual(len(source.SOURCE_FIELDS), 22)
        self.assertEqual([field[0] for field in source.SOURCE_FIELDS][:2], ["OBJECTID", "site_id"])

    def test_complete_historical_candidate_never_accepts_currentness_or_release(self):
        check = petroleum_refineries_eia_historical_candidate_complete({
            "feature_count": 130, "source_population_complete": True, "historical_only": True,
            "source_vintage": "2021-01-01",
        })
        self.assertTrue(check.passed)
        for field in ("source_release_accepted", "freshness_accepted", "schema_contract_accepted",
                      "id_contract_accepted", "coverage_accepted", "rights_accepted", "mapping_approved", "release_accepted"):
            self.assertFalse(check.metadata[field].value, field)

    def test_changed_archive_requires_new_release_review(self):
        with self.assertRaisesRegex(ValueError, "new_release_review_required"):
            source._read_archive(b"different upstream bytes")

    def test_retained_capture_rejects_bad_receipt_before_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body = b"x" * source.SOURCE_ZIP_BYTES
            receipt = {"method": "GET", "url": source.SOURCE_URL, "final_url": source.SOURCE_URL,
                       "status": 200, "complete_body": False, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "last_modified": source.SOURCE_LAST_MODIFIED}
            (root / "source_zip.body").write_bytes(body)
            (root / "source_zip.wire").write_bytes(body)
            (root / "source_zip.receipt.json").write_bytes(source._json(receipt))
            (root / "capture_manifest.json").write_bytes(source._json({"complete": True, "receipts": {"source_zip": receipt}}))
            with self.assertRaisesRegex(ValueError, "retained_source_capture_invalid"):
                source._capture(root)

    def test_cached_candidate_corruption_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            repo = Path(source.__file__).parents[3]
            identity = processing_identity(repo)
            version, prefix = "vcache-test", "test/test/vcache-test"
            fingerprint = hashlib.sha256(source._json(identity)).hexdigest()
            body = b"original archive"
            sha = hashlib.sha256(body).hexdigest()
            receipt = {"complete_body": True, "status": 200, "bytes": len(body), "sha256": sha,
                       "wire_bytes": len(body), "wire_sha256": sha}
            outputs = {"geoparquet/test.parquet": b"candidate bytes", "raw/source_zip.body": body,
                       "raw/source_zip.wire": body, "raw/source_zip.receipt.json": source._json(receipt)}
            for name, value in outputs.items():
                write_immutable(storage, f"{prefix}/{name}", value)
            objects = {name: {"bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()} for name, value in outputs.items()}
            state_key = f"_construction-state/test/{version}.json"
            seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
                "source_record_sha256": "a" * 64, "source_schema_sha256": "b" * 64,
                "capture_receipts": {"source_zip": receipt}, "objects": objects})
            verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
                "a" * 64, "b" * 64, objects, {"source_zip": receipt})
            storage.write_key(f"{prefix}/geoparquet/test.parquet", b"tampered candidate")
            with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
                    "a" * 64, "b" * 64, objects, {"source_zip": receipt})

    def test_timestamp_only_replay_reuses_first_receipt_and_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            body = b"pinned synthetic ZIP bytes"
            digest = hashlib.sha256(body).hexdigest()
            (root / "source_zip.wire").write_bytes(body)
            audit = {"feature_count": 130, "source_record_sha256": "a" * 64,
                     "source_schema_sha256": "b" * 64, "source_ids_sha256": "c" * 64}
            receipts = iter(("2026-10-20T01:00:00+00:00", "2026-10-20T02:00:00+00:00"))

            def capture(_directory):
                return body, {"method": "GET", "url": source.SOURCE_URL, "final_url": source.SOURCE_URL,
                    "observed_at": next(receipts), "status": 200, "complete_body": True,
                    "bytes": len(body), "sha256": digest,
                    "wire_bytes": len(body), "wire_sha256": digest,
                    "last_modified": source.SOURCE_LAST_MODIFIED}, root

            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "_capture", side_effect=capture), \
                 patch.object(source, "_read_archive", return_value=([], audit)), \
                 patch.object(source, "_parquet", return_value=b"stable candidate parquet"), \
                 patch.object(source, "SOURCE_ZIP_BYTES", len(body)), \
                 patch.object(source, "SOURCE_ZIP_SHA256", digest):
                first = source.capture_petroleum_refineries_eia(storage)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
                receipt_before = storage.read_key(f"{prefix}/raw/source_zip.receipt.json")
                second = source.capture_petroleum_refineries_eia(storage)
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/raw/source_zip.receipt.json"), receipt_before)


if __name__ == "__main__":
    unittest.main()
