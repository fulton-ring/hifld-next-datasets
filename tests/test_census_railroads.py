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

from dagster_hifld.assets.census_railroads import (
    census_bureau_railroads,
    census_railroads_draft_schedule,
    census_railroads_source_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.census_railroads import (
    CODE_IDENTITY_SCHEME,
    _capture,
    _inspect_archive,
    processing_code_identity,
)
from dagster_hifld.sources import census_railroads as source_module
from dagster_hifld.resources import StagingStorageResource


def _release(vintage=2026):
    source_file = f"tl_{vintage}_us_rails.zip"
    return census_latest.ShapefileRelease(
        vintage=vintage, layer="RAILS", source_file=source_file,
        source_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/RAILS/{source_file}",
        directory_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/RAILS/",
        listing_sha256="0" * 64, listing_bytes=1, latest_checked_at="2026-11-13T00:00:00+00:00",
        annual_listing_sha256="1" * 64,
    )


class CensusRailroadsTests(unittest.TestCase):
    def test_packaged_code_identity_need_no_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
            self.assertRegex(identity, r"^[0-9a-f]{64}$")

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

    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(next(iter(census_bureau_railroads.keys)), keys)
        self.assertEqual(census_railroads_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "census_railroads_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("census_railroads_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            ("ingest/railroads/railroads", "census_railroads_source_complete"),
            {
                (key.asset_key.to_user_string(), key.name)
                for key in defs.resolve_asset_graph().asset_check_keys
            },
        )

    def test_source_check_requires_complete_population_and_resolved_vintage(self):
        candidate = {
            "feature_count": 100,
            "source_population_complete": True,
            "source_vintage": "2026",
            "source_archive_sha256": "0" * 64,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
        }
        self.assertTrue(census_railroads_source_complete(candidate).passed)
        candidate["feature_count"] = 0
        self.assertFalse(census_railroads_source_complete(candidate).passed)
        candidate["feature_count"] = 100
        candidate["source_vintage"] = None
        self.assertFalse(census_railroads_source_complete(candidate).passed)

    def test_archive_capture_rejects_receipt_identity_drift(self):
        release = _release()
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = {
                "method": "GET", "url": release.source_url, "final_url": release.source_url,
                "status": 200, "complete_body": True, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
                "wire_bytes": len(wire), "wire_sha256": "0" * 64,
                "content_length": len(body), "observed_at": "2026-11-13T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch.object(census_latest, "discover_shapefile", return_value=release), \
                 patch("dagster_hifld.sources.census_railroads.capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/railroads")
            self.assertEqual(len(keys), 4)
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], "official_tiger_archive_identity_or_release_drift")

    def test_archive_member_roster_drift_is_rejected_before_extract(self):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("unexpected.txt", "not a TIGER archive")
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(sink.getvalue(), _release())

    def test_archive_path_traversal_is_rejected_before_extract(self):
        stem = "tl_2026_us_rails"
        names = [f"../{stem}.cpg", f"{stem}.dbf", f"{stem}.prj", f"{stem}.shp",
                 f"{stem}.shp.ea.iso.xml", f"{stem}.shp.iso.xml", f"{stem}.shx"]
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            for name in names:
                archive.writestr(name, b"fixture")
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            _inspect_archive(sink.getvalue(), _release())


if __name__ == "__main__":
    unittest.main()
