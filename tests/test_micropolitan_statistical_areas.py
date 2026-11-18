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

from dagster_hifld.assets.micropolitan_statistical_areas import (
    census_micropolitan_statistical_areas,
    census_micropolitan_statistical_areas_draft_schedule,
    census_micropolitan_statistical_areas_source_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.micropolitan_statistical_areas import (
    EXPECTED_CRS,
    EXPECTED_FIELDS,
    _capture,
    _inspect_archive,
    _sha,
    candidate_version,
    capture_census_micropolitan_statistical_areas,
    processing_code_identity,
)


def _release(vintage=2026):
    source_file = f"tl_{vintage}_us_cbsa.zip"
    return census_latest.ShapefileRelease(
        vintage=vintage, layer="CBSA", source_file=source_file,
        source_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/CBSA/{source_file}",
        directory_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/CBSA/",
        listing_sha256="0" * 64, listing_bytes=1, latest_checked_at="2026-11-14T00:00:00+00:00",
        annual_listing_sha256="1" * 64,
    )
from dagster_hifld.sources import micropolitan_statistical_areas as source_module
from dagster_hifld.resources import StagingStorageResource

APPROVAL_FLAGS = (
    "source_release_accepted", "source_freshness_accepted", "schema_contract_accepted",
    "id_contract_accepted", "coverage_accepted", "rights_accepted",
    "mapping_approved", "release_accepted",
)


class CensusMicropolitanStatisticalAreasTests(unittest.TestCase):
    def test_packaged_code_identity_and_candidate_version_need_no_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
            self.assertRegex(identity, r"^[0-9a-f]{64}$")
            self.assertRegex(candidate_version(_release(), "c" * 64), r"^v[0-9a-f]{32}$")

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
        self.assertIn(next(iter(census_micropolitan_statistical_areas.keys)), keys)
        self.assertEqual(census_micropolitan_statistical_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "census_micropolitan_statistical_areas_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("census_micropolitan_statistical_areas_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            ("ingest/micropolitan-statistical-areas-3/micropolitan-statistical-areas-3", "census_micropolitan_statistical_areas_source_complete"),
            {
                (key.asset_key.to_user_string(), key.name)
                for key in defs.resolve_asset_graph().asset_check_keys
            },
        )

    def test_source_check_requires_complete_population_and_resolved_vintage(self):
        candidate = {
            "feature_count": 542,
            "source_population_complete": True,
            "source_archive_bytes": 1000,
            "source_archive_sha256": "a" * 64,
            "source_id_sha256": "b" * 64,
            "source_schema_sha256": "c" * 64,
            "source_vintage": "2026",
        }
        self.assertTrue(census_micropolitan_statistical_areas_source_complete(candidate).passed)
        candidate["feature_count"] = 0
        self.assertFalse(census_micropolitan_statistical_areas_source_complete(candidate).passed)

    def test_archive_capture_rejects_changed_url_or_bytes(self):
        release = _release()
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = {
                "method": "GET", "url": release.source_url, "final_url": release.source_url,
                "status": 200, "complete_body": True, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
                "wire_bytes": len(wire), "wire_sha256": "0" * 64,
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

            with patch.object(census_latest, "discover_shapefile", return_value=release), \
                 patch("dagster_hifld.sources.micropolitan_statistical_areas.capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/micropolitan-statistical-areas-3")
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
            _inspect_archive(sink.getvalue(), _release())

    def test_real_pinned_archive_inspection_succeeds_with_runtime_digest_pins(self):
        path = os.environ.get("MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE")
        if not path:
            self.skipTest("set MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE to a retained official tl_2026_us_cbsa.zip")
        body = Path(path).read_bytes()
        audit = _inspect_archive(body, _release())
        self.assertGreater(audit["feature_count"], 0)
        self.assertEqual(set(audit["full_file_memi_counts"]), {"1", "2"})
        self.assertEqual(audit["source_crs"], EXPECTED_CRS)
        self.assertEqual(audit["attribute_fields"], EXPECTED_FIELDS)
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["empty_geometry_count"], 0)
        self.assertEqual(audit["invalid_geometry_count"], 0)

    def test_real_archive_full_ingest_stages_subset_and_passes_check(self):
        path = os.environ.get("MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE")
        if not path:
            self.skipTest("set MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE to a retained official tl_2026_us_cbsa.zip")
        body = Path(path).read_bytes()
        digest = _sha(body)
        release = _release()
        receipt = {
            "method": "GET", "url": release.source_url, "final_url": release.source_url,
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
            candidate = capture_census_micropolitan_statistical_areas(
                storage, version="vreal", capture_directory=str(capture_dir),
            )
            self.assertGreater(candidate["feature_count"], 0)
            self.assertEqual(candidate["source_population_complete"], True)
            self.assertEqual(candidate["source_archive_sha256"], digest)
            self.assertEqual(candidate["source_archive_bytes"], len(body))
            self.assertEqual(candidate["source_capture_evidence_sha256"], digest)
            self.assertEqual(candidate["source_cache_reused"], False)
            for flag in APPROVAL_FLAGS:
                self.assertIs(candidate[flag], False)
            self.assertTrue(census_micropolitan_statistical_areas_source_complete(candidate).passed)
            archive_key, *rest = candidate["written_keys"]
            self.assertTrue(archive_key.endswith("provenance/tl_2026_us_cbsa.zip"))
            manifest_key = next(key for key in rest if key.endswith("metadata/source_manifest.json"))
            audit_key = next(key for key in rest if key.endswith("metadata/source_audit.json"))
            shapefile_keys = [key for key in rest if "/shapefile/" in key]
            self.assertEqual(storage.read_key(archive_key), body)
            self.assertEqual(storage.sha256_key(archive_key), digest)
            manifest = json.loads(storage.read_key(manifest_key))
            validate_provenance(manifest)
            self.assertEqual(manifest["title"], "Micropolitan Statistical Areas")
            self.assertEqual(manifest["source_url"], release.source_url)
            self.assertEqual(manifest["inputs"][0]["role"], "producer_publication")
            self.assertEqual(manifest["inputs"][0]["sha256"], digest)
            self.assertEqual(manifest["source_release"]["tiger_vintage"], "2026")
            self.assertIn("MEMI=2", manifest["transformations"][0])
            audit_doc = json.loads(storage.read_key(audit_key))
            self.assertEqual(audit_doc["source"]["vintage"], "2026")
            self.assertEqual(audit_doc["feature_count"], candidate["feature_count"])
            self.assertEqual(len(shapefile_keys), 5)
            self.assertIn("micropolitan-statistical-areas-3.shp", {key.split("/")[-1] for key in shapefile_keys})
            staged_suffixes = {key.split(".")[-1] for key in shapefile_keys}
            self.assertEqual(staged_suffixes, {"shp", "shx", "dbf", "cpg", "prj"})

    def test_real_archive_replay_pins_staged_dbf_date_across_dates(self):
        path = os.environ.get("MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE")
        if not path:
            self.skipTest("set MICROPOLITAN_STATISTICAL_AREAS_TEST_ARCHIVE to a retained official tl_2026_us_cbsa.zip")
        body = Path(path).read_bytes()
        digest = _sha(body)
        release = _release()
        receipt = {
            "method": "GET", "url": release.source_url, "final_url": release.source_url,
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
            candidate = capture_census_micropolitan_statistical_areas(
                storage, version="vreplay", capture_directory=str(capture_dir),
            )
            dbf_key = next(
                key for key in candidate["written_keys"]
                if key.endswith("/shapefile/micropolitan-statistical-areas-3.dbf")
            )
            day_one = storage.read_key(dbf_key)
            # DBF header bytes 1-3 store the last-update date as years-since-1900,
            # month and day; the pinned source release date must win over the
            # wall clock that GDAL would otherwise stamp in.
            self.assertEqual(bytes(day_one[1:4]), bytes((2026 - 1900, 1, 1)))

            replayed = capture_census_micropolitan_statistical_areas(
                storage, version="vreplay", capture_directory=str(capture_dir),
            )
            self.assertEqual(replayed["source_cache_reused"], True)
            self.assertEqual(storage.read_key(dbf_key), day_one)

            self.assertEqual(storage.read_key(dbf_key), day_one)


if __name__ == "__main__":
    unittest.main()
