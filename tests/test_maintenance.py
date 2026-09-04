import io
import json
import os
import tempfile
import tomllib
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.maintenance import inventory_published, main, restore_staging
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


def _write(storage: StagingStorageResource, key: str, data: bytes = b"source") -> None:
    storage.write_key(key, data)


class MaintenanceInventoryTests(unittest.TestCase):
    def test_project_exposes_maintenance_cli(self):
        pyproject = tomllib.loads(
            (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
        )

        self.assertEqual(
            pyproject["project"]["scripts"]["hifld-geoparquet-maintenance"],
            "dagster_hifld.maintenance:main",
        )

    def test_inventory_uses_prefix_and_selects_sources_in_shared_precedence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(
                local_dir=tmpdir,
                use_local=True,
                prefix="published-prefix",
            )
            for version, paths in {
                "v1": [
                    "geojson/source.geojson",
                    "shapefile/source.shp",
                    "shapefile/source.shx",
                    "shapefile/source.dbf",
                    "file_geodatabase/source.gdb/a.gdbtable",
                    "geopackage/source.gpkg",
                    "geoparquet/source.parquet",
                ],
                "v2": [
                    "file_geodatabase/source.gdb/a.gdbtable",
                    "shapefile/source.shp",
                    "shapefile/source.shx",
                    "shapefile/source.dbf",
                ],
                "v3": [
                    "shapefile/source.shp",
                    "shapefile/source.shx",
                    "shapefile/source.dbf",
                    "geojson/source.geojson",
                ],
                "v4": ["geojson/source.geojson", "pmtiles/source.pmtiles"],
            }.items():
                for path in paths:
                    _write(published, f"dataset-a/file-a/{version}/{path}")
            _write(published, "_temporary/dataset-a/file-a/v5/geopackage/source.gpkg")
            _write(published, "_rollback/dataset-a/file-a/v6/geopackage/source.gpkg")

            report = inventory_published(published).to_dict()

            self.assertEqual(report["action"], "inventory")
            self.assertFalse(report["apply"])
            self.assertEqual(
                [
                    (item["version"], item["selected_format"])
                    for item in report["versions"]
                ],
                [
                    ("v1", "geopackage"),
                    ("v2", "file_geodatabase"),
                    ("v3", "shapefile"),
                    ("v4", "geojson"),
                ],
            )
            self.assertEqual(
                report["versions"][0]["source_keys"],
                ["published-prefix/dataset-a/file-a/v1/geopackage/source.gpkg"],
            )
            self.assertEqual(
                report["versions"][0]["destination_keys"],
                ["dataset-a/file-a/v1/geopackage/source.gpkg"],
            )

    def test_inventory_groups_file_geodatabase_and_shapefile_sidecars(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            _write(published, "dataset-a/gdb/v1/file_geodatabase/source.gdb/a.gdbtable")
            _write(published, "dataset-a/gdb/v1/file_geodatabase/source.gdb/a.gdbtablx")
            for suffix in (".shp", ".shx", ".dbf", ".prj", ".qix"):
                _write(published, f"dataset-a/shape/v1/shapefile/roads{suffix}")
            _write(published, "dataset-a/shape/v1/shapefile/unrelated.txt")

            report = inventory_published(published).to_dict()

            gdb, shape = report["versions"]
            self.assertEqual(
                gdb["source_keys"],
                [
                    "dataset-a/gdb/v1/file_geodatabase/source.gdb/a.gdbtable",
                    "dataset-a/gdb/v1/file_geodatabase/source.gdb/a.gdbtablx",
                ],
            )
            self.assertEqual(
                shape["source_keys"],
                [
                    "dataset-a/shape/v1/shapefile/roads.dbf",
                    "dataset-a/shape/v1/shapefile/roads.prj",
                    "dataset-a/shape/v1/shapefile/roads.qix",
                    "dataset-a/shape/v1/shapefile/roads.shp",
                    "dataset-a/shape/v1/shapefile/roads.shx",
                ],
            )

    def test_inventory_accepts_one_zipped_file_geodatabase(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            _write(
                published,
                "dataset-a/file-a/v1/file_geodatabase/source.gdb.zip",
                b"zip",
            )

            result = inventory_published(published).to_dict()["versions"][0]

            self.assertEqual(result["selected_format"], "file_geodatabase")
            self.assertEqual(
                result["source_keys"],
                ["dataset-a/file-a/v1/file_geodatabase/source.gdb.zip"],
            )

    def test_inventory_canonicalizes_complete_legacy_shapefile_but_canonical_wins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            for suffix in (".shp", ".shx", ".dbf", ".prj"):
                _write(published, f"dataset-a/legacy/v1/unknown/nested/roads{suffix}")
                _write(published, f"dataset-a/canonical/v1/unknown/legacy{suffix}")
                _write(published, f"dataset-a/canonical/v1/shapefile/roads{suffix}")

            report = inventory_published(published).to_dict()

            canonical, legacy = report["versions"]
            self.assertEqual(canonical["file"], "canonical")
            self.assertTrue(
                all("/shapefile/roads." in key for key in canonical["source_keys"])
            )
            self.assertEqual(legacy["selected_format"], "shapefile")
            self.assertTrue(
                all("/unknown/nested/roads." in key for key in legacy["source_keys"])
            )
            self.assertTrue(
                all(
                    "/shapefile/nested/roads." in key
                    for key in legacy["destination_keys"]
                )
            )

    def test_inventory_reports_ambiguity_incomplete_legacy_and_no_allowed_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            _write(published, "dataset-a/ambiguous/v1/geopackage/a.gpkg")
            _write(published, "dataset-a/ambiguous/v1/geopackage/b.gpkg")
            _write(published, "dataset-a/ambiguous/v1/geojson/fallback.geojson")
            _write(published, "dataset-a/incomplete/v1/unknown/roads.shp")
            for name in ("roads", "rails"):
                for suffix in (".shp", ".shx", ".dbf"):
                    _write(
                        published,
                        f"dataset-a/legacy-ambiguous/v1/unknown/{name}{suffix}",
                    )
            _write(published, "dataset-a/derived/v1/geoparquet/source.parquet")
            _write(published, "dataset-a/derived/v1/pmtiles/source.pmtiles")

            report = inventory_published(published).to_dict()

            by_file = {item["file"]: item for item in report["versions"]}
            self.assertEqual(
                set(by_file),
                {"ambiguous", "derived", "incomplete", "legacy-ambiguous"},
            )
            self.assertTrue(
                all(item["status"] == "blocked" for item in by_file.values())
            )
            self.assertIn("multiple", " ".join(by_file["ambiguous"]["errors"]).lower())
            self.assertIn(
                "requires .shp, .shx, and .dbf",
                " ".join(by_file["incomplete"]["errors"]),
            )
            self.assertIn(
                "multiple Shapefile datasets",
                " ".join(by_file["legacy-ambiguous"]["errors"]),
            )
            self.assertIn(
                "No allowed processing source", " ".join(by_file["derived"]["errors"])
            )

    def test_inventory_filters_dataset_file_and_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            for key in (
                "dataset-a/file-a/v1/geojson/source.geojson",
                "dataset-a/file-a/v2/geojson/source.geojson",
                "dataset-a/file-b/v2/geojson/source.geojson",
                "dataset-b/file-a/v2/geojson/source.geojson",
            ):
                _write(published, key)

            report = inventory_published(
                published,
                dataset="dataset-a",
                file="file-a",
                version="v2",
            ).to_dict()

            self.assertEqual(
                [
                    (item["dataset"], item["file"], item["version"])
                    for item in report["versions"]
                ],
                [("dataset-a", "file-a", "v2")],
            )


class RestoreStagingTests(unittest.TestCase):
    def test_restore_defaults_to_dry_run_without_writes(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            source = Path(published_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")

            report = restore_staging(published, staging).to_dict()

            self.assertFalse(report["apply"])
            self.assertEqual(report["versions"][0]["status"], "planned")
            self.assertEqual(staging.list_prefix(), [])

    def test_apply_copies_only_selected_source_and_manifests_then_regenerates_catalog(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published_root = Path(published_dir)
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            source = published_root / "dataset-a/file-a/v1/geopackage/source.gpkg"
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GPKG")
            _write(published, "dataset-a/file-a/v1/geojson/fallback.geojson")
            _write(published, "dataset-a/file-a/v1/geoparquet/derived.parquet")
            _write(published, "dataset-a/file-a/v1/pmtiles/derived.pmtiles")
            _write(
                published,
                "dataset-a/metadata/source_manifest.json",
                json.dumps({"publisher": "Publisher A"}).encode(),
            )
            _write(
                published,
                "dataset-a/file-a/metadata/source_manifest.json",
                json.dumps(
                    {"title": "File A", "description": "Description A"}
                ).encode(),
            )
            _write(
                published,
                "dataset-a/file-a/v1/metadata/quality_manifest.json",
                b'{"stale": true}',
            )
            _write(
                published,
                "dataset-a/file-a/v1/metadata/data_dictionary.json",
                b'{"stale": true}',
            )
            staging = StagingStorageResource(
                local_dir=staging_dir,
                use_local=True,
                prefix="staged-prefix",
            )

            report = restore_staging(published, staging, apply=True).to_dict()

            result = report["versions"][0]
            self.assertEqual(result["status"], "restored")
            restored_root = Path(staging_dir) / "staged-prefix/dataset-a/file-a/v1"
            self.assertTrue((restored_root / "geopackage/source.gpkg").is_file())
            self.assertFalse((restored_root / "geojson").exists())
            self.assertFalse((restored_root / "geoparquet").exists())
            self.assertFalse((restored_root / "pmtiles").exists())
            version_manifest = json.loads(
                (restored_root / "metadata/source_manifest.json").read_text()
            )
            self.assertEqual(version_manifest["publisher"], "Publisher A")
            self.assertEqual(version_manifest["title"], "File A")
            self.assertEqual(version_manifest["metadata_sources"], ["dataset", "file"])
            quality = json.loads(
                (restored_root / "metadata/quality_manifest.json").read_text()
            )
            dictionary = json.loads(
                (restored_root / "metadata/data_dictionary.json").read_text()
            )
            self.assertNotIn("stale", quality)
            self.assertNotIn("stale", dictionary)
            self.assertEqual(quality["feature_count"], 1)
            self.assertEqual(dictionary["title"], "File A")
            self.assertTrue(
                (
                    Path(staging_dir)
                    / "staged-prefix/dataset-a/metadata/source_manifest.json"
                ).is_file()
            )
            self.assertTrue(
                (
                    Path(staging_dir)
                    / "staged-prefix/dataset-a/file-a/metadata/source_manifest.json"
                ).is_file()
            )

    def test_apply_preserves_raw_version_override_separately_and_writes_resolved_manifest(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published_root = Path(published_dir)
            source = published_root / "dataset-a/file-a/v1/geojson/source.geojson"
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            _write(
                PublishedStorageResource(local_dir=published_dir, use_local=True),
                "dataset-a/metadata/source_manifest.json",
                json.dumps({"publisher": "Publisher A"}).encode(),
            )
            _write(
                PublishedStorageResource(local_dir=published_dir, use_local=True),
                "dataset-a/file-a/metadata/source_manifest.json",
                json.dumps({"title": "File title"}).encode(),
            )
            manifest_bytes = json.dumps({"description": "Version description"}).encode()
            _write(
                PublishedStorageResource(local_dir=published_dir, use_local=True),
                "dataset-a/file-a/v1/metadata/source_manifest.json",
                manifest_bytes,
            )
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            first = restore_staging(published, staging, apply=True).to_dict()
            staged_source = (
                Path(staging_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            )
            staged_manifest = (
                Path(staging_dir) / "dataset-a/file-a/v1/metadata/source_manifest.json"
            )
            preserved_timestamp = 1_700_000_000
            os.utime(staged_source, (preserved_timestamp, preserved_timestamp))
            os.utime(staged_manifest, (preserved_timestamp, preserved_timestamp))
            second = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(first["versions"][0]["status"], "restored")
            self.assertEqual(second["versions"][0]["status"], "restored")
            self.assertEqual(
                (staged_manifest.parent / "upstream_source_manifest.json").read_bytes(),
                manifest_bytes,
            )
            resolved = json.loads(staged_manifest.read_text())
            self.assertEqual(resolved["publisher"], "Publisher A")
            self.assertEqual(resolved["title"], "File title")
            self.assertEqual(resolved["description"], "Version description")
            self.assertEqual(resolved["manifest_role"], "resolved_version")
            self.assertEqual(resolved["schema_version"], "v1")
            self.assertEqual(staged_source.stat().st_mtime, preserved_timestamp)
            self.assertEqual(staged_manifest.stat().st_mtime, preserved_timestamp)

    def test_changed_hierarchical_metadata_blocks_then_overwrite_refreshes_resolved_outputs(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            source = Path(published_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            _write(
                published,
                "dataset-a/metadata/source_manifest.json",
                json.dumps({"publisher": "Publisher One"}).encode(),
            )
            _write(
                published,
                "dataset-a/file-a/metadata/source_manifest.json",
                json.dumps({"title": "Title One"}).encode(),
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            first = restore_staging(published, staging, apply=True).to_dict()
            self.assertEqual(first["versions"][0]["status"], "restored")
            before = _storage_snapshot(Path(staging_dir))

            _write(
                published,
                "dataset-a/metadata/source_manifest.json",
                json.dumps({"publisher": "Publisher Two"}).encode(),
            )
            _write(
                published,
                "dataset-a/file-a/metadata/source_manifest.json",
                json.dumps({"title": "Title Two"}).encode(),
            )

            blocked = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(blocked["versions"][0]["status"], "failed")
            self.assertIn(
                "conflicting managed destination",
                " ".join(blocked["versions"][0]["errors"]),
            )
            self.assertEqual(_storage_snapshot(Path(staging_dir)), before)

            refreshed = restore_staging(
                published,
                staging,
                apply=True,
                overwrite=True,
            ).to_dict()

            self.assertEqual(refreshed["versions"][0]["status"], "restored")
            metadata_root = Path(staging_dir) / "dataset-a/file-a/v1/metadata"
            resolved = json.loads((metadata_root / "source_manifest.json").read_text())
            dictionary = json.loads(
                (metadata_root / "data_dictionary.json").read_text()
            )
            self.assertEqual(resolved["publisher"], "Publisher Two")
            self.assertEqual(resolved["title"], "Title Two")
            self.assertEqual(dictionary["publisher"], "Publisher Two")
            self.assertEqual(dictionary["title"], "Title Two")

    def test_changed_upstream_version_override_conflicts_before_write(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            source = Path(published_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            raw_key = "dataset-a/file-a/v1/metadata/source_manifest.json"
            _write(
                published,
                raw_key,
                json.dumps({"description": "Version one"}).encode(),
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            first = restore_staging(published, staging, apply=True).to_dict()
            self.assertEqual(first["versions"][0]["status"], "restored")
            before = _storage_snapshot(Path(staging_dir))

            updated_raw = json.dumps({"description": "Version two"}).encode()
            _write(published, raw_key, updated_raw)

            blocked = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(blocked["versions"][0]["status"], "failed")
            self.assertEqual(_storage_snapshot(Path(staging_dir)), before)

            restored = restore_staging(
                published,
                staging,
                apply=True,
                overwrite=True,
            ).to_dict()

            self.assertEqual(restored["versions"][0]["status"], "restored")
            metadata_root = Path(staging_dir) / "dataset-a/file-a/v1/metadata"
            self.assertEqual(
                (metadata_root / "upstream_source_manifest.json").read_bytes(),
                updated_raw,
            )
            self.assertEqual(
                json.loads((metadata_root / "source_manifest.json").read_text())[
                    "description"
                ],
                "Version two",
            )

    def test_apply_persists_inventory_fallback_as_version_manifest(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published_root = Path(published_dir)
            source = (
                published_root
                / "2020-census-blocks-1/tl_2024_01_tabblock20/v1.0.0/geojson/source.geojson"
            )
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            report = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(report["versions"][0]["status"], "restored")
            manifest = json.loads(
                (
                    Path(staging_dir)
                    / "2020-census-blocks-1/tl_2024_01_tabblock20/v1.0.0/metadata/source_manifest.json"
                ).read_text()
            )
            self.assertEqual(manifest["metadata_sources"], ["inventory"])
            self.assertEqual(
                manifest["title"],
                "2020 Census Blocks - tl_2024_01_tabblock20",
            )

    def test_candidate_catalog_scratch_files_are_not_promoted(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            _write(
                published,
                "dataset-a/file-a/v1/file_geodatabase/source.gdb.zip",
                b"zip",
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)

            def summarize_with_scratch(candidate, *args, **kwargs):
                candidate.write_key(
                    "dataset-a/file-a/v1/file_geodatabase/.extracted/leak.gdb/a.gdbtable",
                    b"scratch",
                )
                return SimpleNamespace(
                    quality_manifest={"feature_count": 1},
                    data_dictionary={"name": "dataset-a", "columns": []},
                )

            with patch(
                "dagster_hifld.maintenance.summarize_staged_catalog",
                side_effect=summarize_with_scratch,
            ):
                report = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(report["versions"][0]["status"], "restored")
            version_root = Path(staging_dir) / "dataset-a/file-a/v1"
            self.assertTrue(
                (version_root / "file_geodatabase/source.gdb.zip").is_file()
            )
            self.assertFalse((version_root / "file_geodatabase/.extracted").exists())

    def test_conflicting_canonical_source_requires_overwrite_and_overwrite_is_narrow(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published_source = (
                Path(published_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            )
            published_source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["new"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(published_source, driver="GeoJSON")
            _write(staging, "dataset-a/file-a/v1/geojson/source.geojson", b"old")
            _write(staging, "dataset-a/file-a/v1/shapefile/old.shp", b"old")
            _write(staging, "dataset-a/file-a/v1/geoparquet/keep.parquet", b"derived")

            dry_run = restore_staging(published, staging).to_dict()
            blocked = restore_staging(published, staging, apply=True).to_dict()

            self.assertEqual(dry_run["versions"][0]["status"], "blocked")
            self.assertEqual(blocked["versions"][0]["status"], "failed")
            self.assertIn(
                "conflicting managed destination",
                " ".join(blocked["versions"][0]["errors"]),
            )
            self.assertEqual(
                (
                    Path(staging_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
                ).read_bytes(),
                b"old",
            )

            restored = restore_staging(
                published,
                staging,
                apply=True,
                overwrite=True,
            ).to_dict()

            self.assertEqual(restored["versions"][0]["status"], "restored")
            self.assertFalse(
                (Path(staging_dir) / "dataset-a/file-a/v1/shapefile/old.shp").exists()
            )
            self.assertTrue(
                (
                    Path(staging_dir) / "dataset-a/file-a/v1/geoparquet/keep.parquet"
                ).is_file()
            )
            self.assertNotEqual(
                (
                    Path(staging_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
                ).read_bytes(),
                b"old",
            )

    def test_apply_reports_partial_catalog_failure_and_nonzero_cli_exit(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published_root = Path(published_dir)
            valid = published_root / "dataset-a/good/v1/geojson/source.geojson"
            valid.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["A"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(valid, driver="GeoJSON")
            _write(
                PublishedStorageResource(local_dir=published_dir, use_local=True),
                "dataset-a/bad/v1/geojson/source.geojson",
                b"not geojson",
            )
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            _write(staging, "dataset-a/bad/v1/geojson/good-old.geojson", b"old source")
            _write(
                staging,
                "dataset-a/bad/v1/metadata/quality_manifest.json",
                b'{"old": true}',
            )
            before_bad = _storage_snapshot(Path(staging_dir) / "dataset-a/bad/v1")

            report = restore_staging(
                published,
                staging,
                apply=True,
                overwrite=True,
            ).to_dict()

            by_file = {item["file"]: item for item in report["versions"]}
            self.assertEqual(by_file["good"]["status"], "restored")
            self.assertEqual(by_file["bad"]["status"], "failed")
            self.assertTrue(by_file["bad"]["errors"])
            self.assertEqual(
                _storage_snapshot(Path(staging_dir) / "dataset-a/bad/v1"),
                before_bad,
            )
            self.assertFalse(any("_temporary" in key for key in staging.list_prefix()))
            stdout = io.StringIO()
            with (
                patch(
                    "dagster_hifld.maintenance.PublishedStorageResource.from_env",
                    return_value=published,
                ),
                patch(
                    "dagster_hifld.maintenance.StagingStorageResource.from_env",
                    return_value=staging,
                ),
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "restore-staging",
                        "--apply",
                        "--overwrite-existing-sources",
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertTrue(json.loads(stdout.getvalue())["versions"])

    def test_injected_final_promotion_failure_restores_preexisting_state(self):
        with (
            tempfile.TemporaryDirectory() as published_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            published_source = (
                Path(published_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            )
            published_source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["new"]},
                geometry=[Point(1, 2)],
                crs="EPSG:4326",
            ).to_file(published_source, driver="GeoJSON")
            published = PublishedStorageResource(
                local_dir=published_dir, use_local=True
            )
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            old_source = (
                Path(staging_dir) / "dataset-a/file-a/v1/geojson/source.geojson"
            )
            old_source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["old"]},
                geometry=[Point(9, 9)],
                crs="EPSG:4326",
            ).to_file(old_source, driver="GeoJSON")
            _write(
                staging,
                "dataset-a/file-a/v1/metadata/quality_manifest.json",
                b'{"old": true}',
            )
            before = _storage_snapshot(Path(staging_dir))

            original_copy = StagingStorageResource.copy_key_to
            promoted = False

            def fail_after_first_promoted_object(
                source_storage,
                destination_storage,
                key,
                destination_key=None,
            ):
                nonlocal promoted
                result = original_copy(
                    source_storage,
                    destination_storage,
                    key,
                    destination_key,
                )
                if (
                    source_storage.prefix.endswith("/candidate")
                    and destination_storage is staging
                    and not promoted
                ):
                    promoted = True
                    raise RuntimeError("injected final promotion failure")
                return result

            with patch.object(
                StagingStorageResource,
                "copy_key_to",
                new=fail_after_first_promoted_object,
            ):
                report = restore_staging(
                    published,
                    staging,
                    apply=True,
                    overwrite=True,
                ).to_dict()

            self.assertEqual(report["versions"][0]["status"], "failed")
            self.assertIn(
                "injected final promotion failure",
                " ".join(report["versions"][0]["errors"]),
            )
            self.assertEqual(_storage_snapshot(Path(staging_dir)), before)
            self.assertFalse(any("_temporary" in key for key in staging.list_prefix()))


def _storage_snapshot(root: Path) -> dict[str, bytes]:
    if not root.is_dir():
        return {}
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and "_temporary" not in path.parts
    }


if __name__ == "__main__":
    unittest.main()
