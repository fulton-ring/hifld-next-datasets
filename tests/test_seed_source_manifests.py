import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.seed_source_manifests import (
    build_manifest_for_staged_dataset,
    build_manifest_for_staged_file,
    parse_gcloud_ls_output,
    seed_dataset_source_manifests,
    seed_source_manifests,
)


class SeedSourceManifestTests(unittest.TestCase):
    def test_build_manifest_uses_dataset_fields_and_tags(self):
        row = {
            "slug": "alternative-fueling-stations",
            "name": "Alternative Fueling Stations",
            "description": "Fuel station locations.",
            "tags": {
                "inventory_name": "alternative-fueling-stations",
                "categories": ["Energy", "Transportation Ground"],
            },
            "files": [],
        }

        manifest = build_manifest_for_staged_file(
            "alternative-fueling-stations",
            "alternative-fueling-stations",
            {"alternative-fueling-stations": row},
        )

        self.assertEqual(manifest["title"], "Alternative Fueling Stations")
        self.assertEqual(manifest["description"], "Fuel station locations.")
        self.assertEqual(
            manifest["tags"],
            {
                "inventory_name": "alternative-fueling-stations",
                "categories": ["Energy", "Transportation Ground"],
            },
        )

    def test_build_manifest_prefers_file_description_when_file_slug_matches(self):
        row = {
            "slug": "dataset-a",
            "name": "Dataset A",
            "description": "Dataset description.",
            "tags": {"categories": ["Boundaries"]},
            "files": [
                {
                    "slug": "file-a",
                    "name": "File A",
                    "description": "File description.",
                }
            ],
        }

        manifest = build_manifest_for_staged_file(
            "dataset-a",
            "file-a",
            {"dataset-a": row},
        )

        self.assertEqual(manifest["title"], "File A")
        self.assertEqual(manifest["description"], "File description.")
        self.assertEqual(manifest["tags"], {"categories": ["Boundaries"]})

    def test_build_manifest_falls_back_to_file_slug_for_grouped_staging_paths(self):
        row = {
            "slug": "amtrak-stations",
            "name": "Amtrak Stations",
            "description": "Rail passenger station terminals.",
            "tags": {"categories": ["Transportation Ground"]},
            "files": [],
        }

        manifest = build_manifest_for_staged_file(
            "bts",
            "amtrak-stations",
            {"amtrak-stations": row},
        )

        self.assertEqual(manifest["title"], "Amtrak Stations")
        self.assertEqual(manifest["description"], "Rail passenger station terminals.")
        self.assertEqual(manifest["tags"], {"categories": ["Transportation Ground"]})

    def test_build_dataset_manifest_uses_exact_dataset_row(self):
        row = {
            "slug": "alternative-fueling-stations",
            "name": "Alternative Fueling Stations",
            "description": "Fuel station locations.",
            "tags": {
                "inventory_name": "alternative-fueling-stations",
                "categories": ["Energy"],
            },
            "files": [],
        }

        manifest = build_manifest_for_staged_dataset(
            "alternative-fueling-stations",
            ["alternative-fueling-stations"],
            {"alternative-fueling-stations": row},
        )

        self.assertEqual(manifest["title"], "Alternative Fueling Stations")
        self.assertEqual(manifest["description"], "Fuel station locations.")
        self.assertEqual(
            manifest["tags"],
            {
                "inventory_name": "alternative-fueling-stations",
                "categories": ["Energy"],
            },
        )

    def test_build_dataset_manifest_summarizes_grouped_children(self):
        row_a = {
            "slug": "study-info",
            "name": "Study Info",
            "description": "NFHL shared description.",
            "tags": {
                "inventory_name": "study-info",
                "categories": ["National Flood Hazard"],
            },
            "files": [],
        }
        row_b = {
            "slug": "flood-hazard-zones-1",
            "name": "Flood Hazard Zones",
            "description": "NFHL shared description.",
            "tags": {
                "inventory_name": "flood-hazard-zones-1",
                "categories": ["Natural Hazards", "Water Supply"],
            },
            "files": [],
        }

        manifest = build_manifest_for_staged_dataset(
            "nfhl",
            ["study-info", "flood-hazard-zones-1"],
            {"study-info": row_a, "flood-hazard-zones-1": row_b},
        )

        self.assertEqual(manifest["title"], "NFHL")
        self.assertEqual(manifest["description"], "NFHL shared description.")
        self.assertEqual(manifest["tags"]["inventory_name"], "nfhl")
        self.assertEqual(
            manifest["tags"]["categories"],
            ["National Flood Hazard", "Natural Hazards", "Water Supply"],
        )

    def test_seed_source_manifests_writes_file_level_manifests_for_staged_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = Path(tmpdir) / "staging"
            datasets_jsonl = Path(tmpdir) / "datasets.jsonl"
            datasets_jsonl.write_text(
                json.dumps(
                    {
                        "slug": "dataset-a",
                        "name": "Dataset A",
                        "description": "Dataset description.",
                        "tags": {"categories": ["Boundaries"]},
                        "files": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            source_dir = (
                staging_root
                / "dataset-a"
                / "file-a"
                / "v1.0.0"
                / "geopackage"
            )
            source_dir.mkdir(parents=True)
            (source_dir / "source.gpkg").write_bytes(b"placeholder")

            result = seed_source_manifests(
                str(staging_root),
                datasets_jsonl,
                dry_run=False,
            )

            manifest_path = (
                staging_root
                / "dataset-a"
                / "file-a"
                / "metadata"
                / "source_manifest.json"
            )
            self.assertTrue(manifest_path.exists())
            self.assertEqual(result.written, 1)
            self.assertEqual(result.skipped_existing, 0)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["title"], "file-a")
            self.assertEqual(manifest["description"], "Dataset description.")
            self.assertEqual(manifest["tags"], {"categories": ["Boundaries"]})

    def test_seed_source_manifests_dry_run_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = Path(tmpdir) / "staging"
            datasets_jsonl = Path(tmpdir) / "datasets.jsonl"
            datasets_jsonl.write_text(
                json.dumps(
                    {
                        "slug": "dataset-a",
                        "name": "Dataset A",
                        "description": "Dataset description.",
                        "tags": {},
                        "files": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            source_dir = staging_root / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            source_dir.mkdir(parents=True)
            (source_dir / "source.geojson").write_text("{}", encoding="utf-8")

            result = seed_source_manifests(
                str(staging_root),
                datasets_jsonl,
                dry_run=True,
            )

            self.assertEqual(result.to_write, 1)
            self.assertEqual(result.written, 0)
            self.assertFalse(
                (
                    staging_root
                    / "dataset-a"
                    / "file-a"
                    / "metadata"
                    / "source_manifest.json"
                ).exists()
            )

    def test_seed_dataset_source_manifests_writes_dataset_level_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = Path(tmpdir) / "staging"
            datasets_jsonl = Path(tmpdir) / "datasets.jsonl"
            datasets_jsonl.write_text(
                json.dumps(
                    {
                        "slug": "dataset-a",
                        "name": "Dataset A",
                        "description": "Dataset description.",
                        "tags": {"categories": ["Boundaries"]},
                        "files": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            source_dir = staging_root / "dataset-a" / "file-a" / "v1.0.0" / "geojson"
            source_dir.mkdir(parents=True)
            (source_dir / "source.geojson").write_text("{}", encoding="utf-8")

            result = seed_dataset_source_manifests(
                str(staging_root),
                datasets_jsonl,
                dry_run=False,
            )

            manifest_path = staging_root / "dataset-a" / "metadata" / "source_manifest.json"
            self.assertTrue(manifest_path.exists())
            self.assertEqual(result.written, 1)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["title"], "Dataset A")
            self.assertEqual(manifest["description"], "Dataset description.")
            self.assertEqual(manifest["tags"], {"categories": ["Boundaries"]})

    def test_parse_gcloud_ls_output_returns_relative_keys(self):
        keys = parse_gcloud_ls_output(
            "gs://hifld-next-staging-prod/dataset-a/file-a/v1.0.0/geopackage/source.gpkg\n"
            "gs://hifld-next-staging-prod/dataset-a/file-a/metadata/source_manifest.json\n",
            "gs://hifld-next-staging-prod",
        )

        self.assertEqual(
            keys,
            [
                "dataset-a/file-a/v1.0.0/geopackage/source.gpkg",
                "dataset-a/file-a/metadata/source_manifest.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()
