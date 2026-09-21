import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import (
    ResolvedSourceManifest,
    load_resolved_source_manifest,
    snapshot_source_metadata,
)


class SourceManifestTests(unittest.TestCase):
    def test_snapshots_authored_metadata_before_generated_catalog_overwrites_it(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = StagingStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            sources = {
                "dataset/metadata/source_manifest.json": b'{"title":"Dataset"}',
                "dataset/file/metadata/source_manifest.json": b'{"title":"File"}',
                "dataset/file/v1.0.0/metadata/source_manifest.json": b'{"title":"Version"}',
                "dataset/file/v1.0.0/metadata/data_dictionary.json": b'{"columns":[]}',
                "dataset/file/v1.0.0/metadata/quality_manifest.json": b'{"feature_count":1}',
            }
            for key, body in sources.items():
                staging.write_key(key, body)
            snapshot_source_metadata(staging, "dataset", "file", "v1.0.0")
            staging.write_key(
                "dataset/file/v1.0.0/metadata/data_dictionary.json",
                b'{"generated":true}',
            )
            snapshot_source_metadata(staging, "dataset", "file", "v1.0.0")

            for key, body in sources.items():
                prefix, name = key.rsplit("/", 1)
                pinned = f"{prefix}/source/{name}"
                self.assertEqual(staging.read_key(pinned), body)

    def test_manifest_inherits_dataset_file_and_version_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "dataset-a"
            (root / "metadata").mkdir(parents=True)
            (root / "file-a" / "metadata").mkdir(parents=True)
            (root / "file-a" / "v1.2.3" / "metadata").mkdir(parents=True)
            (root / "metadata" / "source_manifest.json").write_text(
                json.dumps(
                    {
                        "publisher": "Dataset Publisher",
                        "agency": "Dataset Agency",
                        "keywords": ["dataset", "shared"],
                        "license": "Public Domain",
                    }
                ),
                encoding="utf-8",
            )
            (root / "file-a" / "metadata" / "source_manifest.json").write_text(
                json.dumps(
                    {
                        "title": "File A",
                        "description": "File-level description",
                        "keywords": ["file"],
                    }
                ),
                encoding="utf-8",
            )
            (
                root / "file-a" / "v1.2.3" / "metadata" / "source_manifest.json"
            ).write_text(
                json.dumps(
                    {
                        "description": "Version-specific description",
                        "date_issued": "2026-04-06",
                        "source_modified": "2026-05-01",
                    }
                ),
                encoding="utf-8",
            )

            resolved = load_resolved_source_manifest(
                StagingStorageResource(local_dir=tmpdir, use_local=True),
                "dataset-a",
                "file-a",
                "v1.2.3",
            )

            self.assertIsInstance(resolved, ResolvedSourceManifest)
            self.assertEqual(resolved.metadata["publisher"], "Dataset Publisher")
            self.assertEqual(resolved.metadata["title"], "File A")
            self.assertEqual(
                resolved.metadata["description"], "Version-specific description"
            )
            self.assertEqual(resolved.metadata["keywords"], ["file"])
            self.assertEqual(
                resolved.metadata["metadata_sources"],
                ["dataset", "file", "version"],
            )
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["publisher"], "dataset"
            )
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["title"], "file"
            )
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["description"], "version"
            )
            self.assertEqual(resolved.metadata["date_issued"], "2026-04-06")
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["date_issued"], "version"
            )

    def test_manifest_falls_back_to_inventory_parent_family_for_v1(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = load_resolved_source_manifest(
                StagingStorageResource(local_dir=tmpdir, use_local=True),
                "2020-census-blocks-1",
                "tl_2024_01_tabblock20",
                "v1.0.0",
            )

            self.assertEqual(resolved.metadata["metadata_sources"], ["inventory"])
            self.assertEqual(
                resolved.metadata["title"], "2020 Census Blocks - tl_2024_01_tabblock20"
            )
            self.assertIn("description", resolved.metadata)
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["title"], "inventory"
            )

    def test_partial_version_manifest_inherits_missing_authored_inventory_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = StagingStorageResource(local_dir=tmpdir, use_local=True)
            staging.write_key(
                "amtrak-stations/amtrak-stations/v1.0.0/metadata/source_manifest.json",
                b'{"title":"Staged Amtrak title","description":"Staged description","publisher":null}',
            )

            resolved = load_resolved_source_manifest(
                staging, "amtrak-stations", "amtrak-stations", "v1.0.0"
            )

            self.assertEqual(resolved.metadata["title"], "Staged Amtrak title")
            self.assertEqual(resolved.metadata["description"], "Staged description")
            self.assertEqual(resolved.metadata["publisher"], "Amtrak")
            self.assertEqual(
                resolved.metadata["agency"], "Department of Transportation"
            )
            self.assertEqual(
                resolved.metadata["metadata_sources"], ["inventory", "version"]
            )
            self.assertEqual(
                resolved.metadata["metadata_resolved_from"]["publisher"], "inventory"
            )

    def test_inventory_does_not_override_a_later_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = StagingStorageResource(local_dir=tmpdir, use_local=True)
            staging.write_key(
                "hospitals-3/hospitals-3/v1.1.0/metadata/source_manifest.json",
                b'{"title":"Niyam update"}',
            )

            resolved = load_resolved_source_manifest(
                staging, "hospitals-3", "hospitals-3", "v1.1.0"
            )

            self.assertEqual(resolved.metadata["title"], "Niyam update")
            self.assertNotIn("publisher", resolved.metadata)

    def test_inventory_recovers_unique_filename_with_unscoped_done_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = load_resolved_source_manifest(
                StagingStorageResource(local_dir=tmpdir, use_local=True),
                "uniform-hazard-ground-motion",
                "us-pga-5pct50yrs-bc-arc",
                "v1.0.0",
            )

            self.assertEqual(
                resolved.metadata["publisher"],
                "U.S. Geological Survey - Geologic Hazards Science Center",
            )
            self.assertEqual(
                resolved.metadata["inventory_match_type"], "unscoped_filename"
            )

    def test_manifest_uses_generated_fallback_when_no_metadata_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = load_resolved_source_manifest(
                StagingStorageResource(local_dir=tmpdir, use_local=True),
                "new-dataset",
                "new-file",
                "v2.0.0",
            )

            self.assertEqual(resolved.metadata["metadata_sources"], ["generated"])
            self.assertEqual(resolved.metadata["title"], "new-file")
            self.assertEqual(
                resolved.metadata["description"],
                "Staged dataset file new-dataset/new-file.",
            )


if __name__ == "__main__":
    unittest.main()
