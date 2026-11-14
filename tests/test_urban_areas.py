import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.urban_areas import (
    census_urban_areas,
    census_urban_areas_draft_schedule,
    census_urban_areas_source_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.urban_areas import (
    CODE_IDENTITY_SCHEME,
    EXPECTED_BOUNDS,
    EXPECTED_BYTES,
    EXPECTED_CRS,
    EXPECTED_FEATURE_COUNT,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    EXPECTED_SCHEMA_SHA256,
    EXPECTED_SHA256,
    SOURCE_URL,
    _capture,
    _inspect_archive,
    _sha,
    candidate_version,
    capture_census_urban_areas,
    processing_code_identity,
)
from dagster_hifld.sources import urban_areas as source_module
from dagster_hifld.resources import StagingStorageResource

APPROVAL_FLAGS = (
    "source_release_accepted", "source_freshness_accepted", "schema_contract_accepted",
    "id_contract_accepted", "coverage_accepted", "rights_accepted",
    "mapping_approved", "release_accepted",
)


class CensusUrbanAreasTests(unittest.TestCase):
    def test_packaged_code_identity_and_candidate_version_need_no_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
            self.assertRegex(identity, r"^[0-9a-f]{64}$")
            self.assertRegex(candidate_version(), r"^v[0-9a-f]{32}$")

    def test_packaged_identity_resolves_from_source_package_without_dot_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            package = root / "dagster_hifld"
            shutil.copytree(source_module.PROCESSING_SOURCE_PACKAGE, package)
            self.assertFalse((root / ".git").exists())
            with patch.object(source_module, "PROCESSING_SOURCE_ROOT", root), \
                 patch.object(source_module, "PROCESSING_SOURCE_PACKAGE", package):
                identity = source_module.processing_code_identity()
                self.assertEqual(identity, processing_code_identity())
                self.assertEqual(source_module.candidate_version(), candidate_version())

    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(next(iter(census_urban_areas.keys)), keys)
        self.assertEqual(census_urban_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "census_urban_areas_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("census_urban_areas_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            ("ingest/urban-areas-1/urban-areas-1", "census_urban_areas_source_complete"),
            {
                (key.asset_key.to_user_string(), key.name)
                for key in defs.resolve_asset_graph().asset_check_keys
            },
        )

    def test_source_check_requires_pinned_archive_and_complete_population(self):
        candidate = {
            "feature_count": EXPECTED_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": EXPECTED_BYTES,
            "source_archive_sha256": EXPECTED_SHA256,
            "source_id_sha256": EXPECTED_ID_SHA256,
            "source_schema_sha256": EXPECTED_SCHEMA_SHA256,
        }
        self.assertTrue(census_urban_areas_source_complete(candidate).passed)
        candidate["feature_count"] -= 1
        self.assertFalse(census_urban_areas_source_complete(candidate).passed)

    def test_archive_capture_rejects_changed_url_or_bytes(self):
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = {
                "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
                "status": 200, "complete_body": True, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
                "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
                "content_length": len(body), "observed_at": "2026-11-14T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch("dagster_hifld.sources.urban_areas.capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/urban-areas-1")
            self.assertEqual(len(keys), 4)
            body_key = next(key for key in keys if key.endswith("/raw/source.body"))
            wire_key = next(key for key in keys if key.endswith("/raw/source.wire"))
            receipt_key = next(key for key in keys if key.endswith("/raw/source.receipt.json"))
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(storage.read_key(body_key), body)
            self.assertEqual(storage.read_key(wire_key), wire)
            saved_receipt = json.loads(storage.read_key(receipt_key))
            self.assertEqual(saved_receipt, receipt)
            self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], "official_tiger_archive_identity_or_release_drift")

    def test_archive_member_roster_drift_is_rejected_before_extract(self):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("unexpected.txt", "not a TIGER archive")
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(sink.getvalue())

    def test_real_pinned_archive_inspection_succeeds_with_runtime_digest_pins(self):
        path = os.environ.get("URBAN_AREAS_TEST_ARCHIVE")
        if not path:
            self.skipTest("set URBAN_AREAS_TEST_ARCHIVE to a retained official tl_2026_us_uac20.zip")
        body = Path(path).read_bytes()
        self.assertEqual(len(body), EXPECTED_BYTES)
        self.assertEqual(_sha(body), EXPECTED_SHA256)
        audit = _inspect_archive(body)
        self.assertEqual(audit["feature_count"], EXPECTED_FEATURE_COUNT)
        self.assertEqual(audit["source_id_sha256"], EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_schema_sha256"], EXPECTED_SCHEMA_SHA256)
        self.assertEqual(audit["source_crs"], EXPECTED_CRS)
        self.assertEqual(audit["bounds"], EXPECTED_BOUNDS)
        self.assertEqual(audit["attribute_fields"], EXPECTED_FIELDS)
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["empty_geometry_count"], 0)
        self.assertEqual(audit["invalid_geometry_count"], 0)

    def test_real_archive_full_ingest_stages_candidate_and_passes_check(self):
        path = os.environ.get("URBAN_AREAS_TEST_ARCHIVE")
        if not path:
            self.skipTest("set URBAN_AREAS_TEST_ARCHIVE to a retained official tl_2026_us_uac20.zip")
        body = Path(path).read_bytes()
        digest = _sha(body)
        receipt = {
            "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
            "status": 200, "complete_body": True, "bytes": len(body),
            "sha256": digest, "content_encoding": "identity",
            "wire_bytes": len(body), "wire_sha256": digest,
            "content_length": len(body), "observed_at": "2026-11-14T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = Path(tmp) / "capture"
            capture_dir.mkdir()
            (capture_dir / "source.body").write_bytes(body)
            (capture_dir / "source.wire").write_bytes(body)
            (capture_dir / "source.receipt.json").write_text(json.dumps(receipt))
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            candidate = capture_census_urban_areas(
                storage, version="vreal", capture_directory=str(capture_dir),
            )
            self.assertEqual(candidate["feature_count"], EXPECTED_FEATURE_COUNT)
            self.assertEqual(candidate["source_population_complete"], True)
            self.assertEqual(candidate["source_id_sha256"], EXPECTED_ID_SHA256)
            self.assertEqual(candidate["source_schema_sha256"], EXPECTED_SCHEMA_SHA256)
            self.assertEqual(candidate["source_archive_sha256"], EXPECTED_SHA256)
            self.assertEqual(candidate["source_archive_bytes"], EXPECTED_BYTES)
            self.assertEqual(candidate["source_capture_evidence_sha256"], digest)
            self.assertEqual(candidate["source_cache_reused"], False)
            for flag in APPROVAL_FLAGS:
                self.assertIs(candidate[flag], False)
            self.assertTrue(census_urban_areas_source_complete(candidate).passed)
            archive_key, manifest_key, audit_key = candidate["written_keys"]
            self.assertTrue(storage.object_exists(archive_key))
            manifest = json.loads(storage.read_key(manifest_key))
            validate_provenance(manifest)
            self.assertEqual(manifest["title"], "Urban Areas")
            self.assertEqual(manifest["source_url"], SOURCE_URL)
            self.assertEqual(manifest["source_modified"], "2026-10-14")
            self.assertEqual(manifest["inputs"][0]["role"], "producer_publication")
            self.assertEqual(manifest["inputs"][0]["release_date"], "2026-10-14")
            self.assertEqual(manifest["inputs"][0]["sha256"], EXPECTED_SHA256)
            audit_doc = json.loads(storage.read_key(audit_key))
            self.assertEqual(audit_doc["source"]["vintage"], "2026")
            self.assertEqual(audit_doc["source"]["release_date"], "2026-10-14")
            self.assertEqual(audit_doc["feature_count"], EXPECTED_FEATURE_COUNT)
            self.assertEqual(audit_doc["source_id_sha256"], EXPECTED_ID_SHA256)
            self.assertEqual(audit_doc["source_schema_sha256"], EXPECTED_SCHEMA_SHA256)


if __name__ == "__main__":
    unittest.main()
