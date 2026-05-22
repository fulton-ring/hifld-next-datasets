import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import (
    ResolvedSourceManifest,
    load_resolved_source_manifest,
)


class SourceManifestTests(unittest.TestCase):
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
            (root / "file-a" / "v1.2.3" / "metadata" / "source_manifest.json").write_text(
                json.dumps(
                    {
                        "description": "Version-specific description",
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
            self.assertEqual(resolved.metadata["description"], "Version-specific description")
            self.assertEqual(resolved.metadata["keywords"], ["file"])
            self.assertEqual(
                resolved.metadata["metadata_sources"],
                ["dataset", "file", "version"],
            )
            self.assertEqual(resolved.metadata["metadata_resolved_from"]["publisher"], "dataset")
            self.assertEqual(resolved.metadata["metadata_resolved_from"]["title"], "file")
            self.assertEqual(resolved.metadata["metadata_resolved_from"]["description"], "version")

    def test_manifest_falls_back_to_inventory_parent_family_for_v1(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = load_resolved_source_manifest(
                StagingStorageResource(local_dir=tmpdir, use_local=True),
                "2020-census-blocks-1",
                "tl_2024_01_tabblock20",
                "v1.0.0",
            )

            self.assertEqual(resolved.metadata["metadata_sources"], ["inventory"])
            self.assertEqual(resolved.metadata["title"], "2020 Census Blocks - tl_2024_01_tabblock20")
            self.assertIn("description", resolved.metadata)
            self.assertEqual(resolved.metadata["metadata_resolved_from"]["title"], "inventory")

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
