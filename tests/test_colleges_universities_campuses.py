"""NCES reporting-institution points as the Colleges and Universities Campuses successor."""
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context, validate_run_config

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import candidate_alias as alias
from dagster_hifld.sources import colleges_universities as colleges

CAPTURE = os.environ.get("COLLEGES_TEST_CAPTURE_DIR")
SOURCE_SLUG = "colleges-and-universities"
TARGET_SLUG = "colleges-and-universities-campuses"
VERSION = "v0.0.0-draft-test"


def _encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _manifest(parquet_body):
    return {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": "a" * 40,
        "code_identity_scheme": "git-commit-blobs-v1",
        "processing_code": {"scheme": "git-commit-blobs-v1", "identity": "a" * 40},
        "title": "Synthetic source candidate",
        "publisher": "Synthetic producer",
        "agency": "Synthetic agency",
        "source_url": "https://example.gov/landing",
        "documentation_url": "https://example.gov/documentation.pdf",
        "license": "Rights review pending.",
        "processing_fingerprint": "b" * 64,
        "inputs": [
            {
                "input_id": "example:abc",
                "producer": "Synthetic producer",
                "host": "example.gov",
                "role": "producer_publication",
                "canonical_url": "https://example.gov/landing",
                "retrieval_url": "https://example.gov/landing",
                "observed_final_url": "https://example.gov/landing",
                "retrieved_at": "2026-11-18T00:00:00Z",
                "http_status": 200,
                "sha256": hashlib.sha256(b"raw").hexdigest(),
                "bytes": 3,
                "license": "Rights review pending.",
                "attribution": "Synthetic producer.",
                "release_date_unknown_reason": "Synthetic fixture has no release date.",
            }
        ],
        "transformations": [{"operation": "synthetic_decode", "parameters": {}}],
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {
                "geoparquet": {
                    "reason": "Synthetic prebuilt output.",
                    "objects": [
                        {
                            "path": "geoparquet/source-file.parquet",
                            "sha256": hashlib.sha256(parquet_body).hexdigest(),
                            "bytes": len(parquet_body),
                        }
                    ],
                }
            },
        },
        "unsupported_output_formats": {
            "pmtiles": "Synthetic fixture has no approved PMTiles mapping."
        },
    }


def _write_source(root, *, parquet_body=b"parquet", audit=None, ready_approved=False, extra=None):
    source = root / SOURCE_SLUG / "source-file" / VERSION
    source.mkdir(parents=True)
    audit = audit or {
        "approved": False,
        "feature_count": 2,
        "points": 2,
        "crs": "EPSG:4326",
        "invalid_geometries": 0,
    }
    files = {
        "geopackage/source-file.gpkg": b"gpkg",
        "geoparquet/source-file.parquet": parquet_body,
        "metadata/source_manifest.json": _encode(_manifest(parquet_body)),
        "metadata/colleges_source_audit.json": _encode(audit),
        "raw/captures/example.body": b"raw",
    }
    if extra:
        files.update(extra)
    for name, body in files.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    ready = {
        "approved": ready_approved,
        "version": VERSION,
        "objects": {name: hashlib.sha256(body).hexdigest() for name, body in files.items()},
    }
    (source / "metadata/colleges_candidate_ready.json").write_bytes(_encode(ready))
    return source


def _alias(storage, **overrides):
    arguments = dict(
        source_dataset_slug=SOURCE_SLUG,
        source_file_slug="source-file",
        source_version=VERSION,
        source_ready_marker="metadata/colleges_candidate_ready.json",
        target_dataset_slug=TARGET_SLUG,
        target_file_slug="target-file",
        target_version=VERSION,
        title="Alias title",
        transformation={"operation": "derive_dataset_alias", "parameters": {"note": "synthetic"}},
        source_audit_marker="metadata/colleges_source_audit.json",
    )
    arguments.update(overrides)
    return alias.stage_candidate_alias(storage, **arguments)


class SyntheticAliasTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.storage = StagingStorageResource(use_local=True, local_dir=str(self.root))

    def test_alias_copies_native_bytes_renames_files_and_rewrites_title(self):
        source = _write_source(self.root)
        before = {path: path.read_bytes() for path in source.rglob("*") if path.is_file()}
        result = _alias(self.storage)
        target = self.root / TARGET_SLUG / "target-file" / VERSION
        self.assertFalse(result["reused"])
        self.assertEqual((target / "geopackage/target-file.gpkg").read_bytes(), b"gpkg")
        self.assertEqual((target / "geoparquet/target-file.parquet").read_bytes(), b"parquet")
        self.assertEqual((target / "raw/captures/example.body").read_bytes(), b"raw")
        manifest = json.loads((target / "metadata/source_manifest.json").read_text())
        self.assertEqual(manifest["title"], "Alias title")
        self.assertEqual(
            manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["path"],
            "geoparquet/target-file.parquet",
        )
        self.assertEqual(manifest["transformations"][-1]["operation"], "derive_dataset_alias")
        self.assertEqual(result["source_audit"]["feature_count"], 2)
        self.assertEqual(result["prebuilt_parquet_sha256"], hashlib.sha256(b"parquet").hexdigest())
        self.assertEqual(before, {path: path.read_bytes() for path in source.rglob("*") if path.is_file()})
        self.assertTrue(_alias(self.storage)["reused"])

    def test_changed_existing_alias_is_quarantined_and_rebuilt(self):
        _write_source(self.root)
        _alias(self.storage)
        target = self.root / TARGET_SLUG / "target-file" / VERSION
        (target / "geopackage/target-file.gpkg").write_bytes(b"tampered")
        result = _alias(self.storage)
        self.assertFalse(result["reused"])
        self.assertEqual((target / "geopackage/target-file.gpkg").read_bytes(), b"gpkg")
        self.assertTrue(list((self.root / "_quarantine" / TARGET_SLUG).glob("invalid-alias-*")))

    def test_missing_ready_marker_fails_closed(self):
        source = _write_source(self.root)
        (source / "metadata/colleges_candidate_ready.json").unlink()
        with self.assertRaisesRegex(alias.CandidateAliasError, "source_ready_marker_missing"):
            _alias(self.storage)

    def test_changed_source_object_fails_closed(self):
        source = _write_source(self.root)
        (source / "geoparquet/source-file.parquet").write_bytes(b"changed")
        with self.assertRaisesRegex(alias.CandidateAliasError, "source_object_changed"):
            _alias(self.storage)

    def test_approved_source_ready_marker_fails_closed(self):
        _write_source(self.root, ready_approved=True)
        with self.assertRaisesRegex(alias.CandidateAliasError, "source_ready_marker_not_unapproved"):
            _alias(self.storage)

    def test_extra_source_file_outside_ready_roster_fails_closed(self):
        source = _write_source(self.root)
        (source / "raw/captures/unlisted.body").write_bytes(b"unlisted")
        with self.assertRaisesRegex(alias.CandidateAliasError, "source_candidate_roster_drift"):
            _alias(self.storage)

    def test_hardlinked_source_file_is_refused(self):
        source = _write_source(self.root)
        os.link(source / "raw/captures/example.body", source / "raw/captures/linked.body")
        with self.assertRaisesRegex(alias.CandidateAliasError, "candidate_alias_or_special_file_refused"):
            _alias(self.storage)

    def test_remote_or_prefixed_staging_is_refused(self):
        for storage in (
            StagingStorageResource(use_local=False, bucket="bucket", local_dir=str(self.root)),
            StagingStorageResource(use_local=True, local_dir=str(self.root), prefix="nested"),
        ):
            with self.assertRaisesRegex(alias.CandidateAliasError, "explicit_isolated_local_staging_required"):
                _alias(storage)

    def test_missing_source_candidate_is_explicit(self):
        with self.assertRaisesRegex(alias.CandidateAliasError, "source_candidate_missing"):
            _alias(self.storage)


@unittest.skipUnless(CAPTURE, "Set COLLEGES_TEST_CAPTURE_DIR to complete unmodified NCES capture")
class RealCaptureAliasTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls._storage_box = {
            "storage": StagingStorageResource(use_local=True, local_dir=str(cls.root))
        }
        cls.source_result = colleges.stage_colleges(
            cls._storage_box["storage"], capture_dir=CAPTURE
        )
        cls.version = cls.source_result["version"]

    @property
    def storage(self):
        return self._storage_box["storage"]

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def alias(self):
        return _alias(
            self.storage,
            source_file_slug=colleges.SLUG,
            source_version=self.version,
            target_file_slug=TARGET_SLUG,
            target_version=self.version,
        )

    def test_alias_materializes_the_same_complete_points(self):
        import pyarrow.parquet as pq
        from dagster_hifld.catalog import summarize_staged_catalog

        result = self.alias()
        self.assertFalse(result["reused"])
        target = self.root / TARGET_SLUG / TARGET_SLUG / self.version
        source = self.root / colleges.SLUG / colleges.SLUG / self.version
        self.assertEqual(
            (target / f"geoparquet/{TARGET_SLUG}.parquet").read_bytes(),
            (source / f"geoparquet/{colleges.SLUG}.parquet").read_bytes(),
        )
        table = pq.read_table(target / f"geoparquet/{TARGET_SLUG}.parquet")
        self.assertEqual(table.num_rows, 6438)
        self.assertEqual(len(set(table["STATE"].to_pylist())), 59)
        summary = summarize_staged_catalog(
            self.storage, TARGET_SLUG, TARGET_SLUG, self.version, TARGET_SLUG
        )
        self.assertEqual(summary.quality_manifest["feature_count"], 6438)
        self.assertEqual(summary.quality_manifest["geometry_type"], "Point")
        self.assertEqual(summary.quality_manifest["invalid_geometry_count"], 0)
        self.assertTrue(self.alias()["reused"])


class RegistrationTests(unittest.TestCase):
    def test_asset_check_job_and_stopped_schedule_are_registered(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster_hifld.assets.colleges_universities_campuses import (
                colleges_universities_campuses_schedule,
            )
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
            from dagster_hifld.definitions import defs

            key = AssetKey(["ingest", TARGET_SLUG, TARGET_SLUG])
            source_key = AssetKey(["ingest", SOURCE_SLUG, SOURCE_SLUG])
            graph = defs.resolve_asset_graph()
            self.assertIn(key, graph.get_all_asset_keys())
            self.assertIn(source_key, graph.get(key).parent_keys)
            self.assertIn((TARGET_SLUG, TARGET_SLUG), SUPPORTED_DATASET_FILE_PAIRS)
            self.assertEqual(
                sum(key in {asset_key for asset_key in asset.keys} for asset in hifld_ingest_assets),
                1,
            )
            self.assertIn("colleges_universities_campuses_source_integrity", defs.get_repository_def().job_names)
            self.assertIn(
                "colleges_universities_campuses_source_integrity_monthly",
                {schedule.name for schedule in defs.get_repository_def().schedule_defs},
            )
            self.assertEqual(
                colleges_universities_campuses_schedule.default_status,
                DefaultScheduleStatus.STOPPED,
            )
            job = defs.resolve_job_def("colleges_universities_campuses_source_integrity")
            request = colleges_universities_campuses_schedule.evaluate_tick(
                build_schedule_context()
            ).run_requests[0]
            validate_run_config(job, request.run_config)


if __name__ == "__main__":
    unittest.main()
