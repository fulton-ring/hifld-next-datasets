import io
import hashlib
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.census_primary_roads import (
    census_bureau_primary_roads,
    census_primary_roads_draft_schedule,
    census_primary_roads_source_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.sources.census_primary_roads import (
    CODE_IDENTITY_SCHEME,
    EXPECTED_BYTES,
    EXPECTED_FEATURE_COUNT,
    EXPECTED_SHA256,
    SOURCE_URL,
    _capture,
    _inspect_archive,
    candidate_version,
    processing_code_identity,
)
from dagster_hifld.sources import census_primary_roads as source_module
from dagster_hifld.resources import StagingStorageResource


class CensusPrimaryRoadsTests(unittest.TestCase):
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
        self.assertIn(next(iter(census_bureau_primary_roads.keys)), keys)
        self.assertEqual(census_primary_roads_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "census_primary_roads_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("census_primary_roads_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            ("ingest/primary-roads-2/primary-roads-2", "census_primary_roads_source_complete"),
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
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
        }
        self.assertTrue(census_primary_roads_source_complete(candidate).passed)
        candidate["feature_count"] -= 1
        self.assertFalse(census_primary_roads_source_complete(candidate).passed)

    def test_archive_capture_rejects_changed_url_or_bytes(self):
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = {
                "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
                "status": 200, "complete_body": True, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
                "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
                "content_length": len(body), "observed_at": "2026-10-20T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch("dagster_hifld.sources.census_primary_roads.capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/primary-roads-2")
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


if __name__ == "__main__":
    unittest.main()
