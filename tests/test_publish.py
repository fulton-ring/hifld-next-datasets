import tempfile
import unittest
from pathlib import Path

from dagster_hifld.assets.publish import _copy_metadata_files, _copy_source_format_files
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class PublishTests(unittest.TestCase):
    def test_copy_metadata_files_promotes_catalog_outputs(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            metadata_root = (
                Path(staging_dir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_a"
                / "metadata"
            )
            metadata_root.mkdir(parents=True)
            (metadata_root / "quality_manifest.json").write_text(
                '{"quality_check_passed": true}', encoding="utf-8"
            )
            (metadata_root / "data_dictionary.json").write_text(
                '{"name": "amtrak-stations", "columns": []}', encoding="utf-8"
            )

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)

            copied = _copy_metadata_files(
                staging,
                published,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_a",
            )

            self.assertEqual(
                copied,
                [
                    "amtrak-stations/amtrak-stations/run_a/metadata/quality_manifest.json",
                    "amtrak-stations/amtrak-stations/run_a/metadata/data_dictionary.json",
                ],
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "metadata"
                    / "quality_manifest.json"
                ).exists()
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "metadata"
                    / "data_dictionary.json"
                ).exists()
            )

    def test_copy_source_format_files_promotes_staged_source_formats(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            version_root = (
                Path(staging_dir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_a"
            )
            (version_root / "shapefile").mkdir(parents=True)
            (version_root / "file_geodatabase" / "stations.gdb").mkdir(parents=True)
            (version_root / "metadata").mkdir(parents=True)
            (version_root / "shapefile" / "stations.shp").write_bytes(b"shape")
            (version_root / "file_geodatabase" / "stations.gdb" / "a00000001.gdbtable").write_bytes(
                b"gdb"
            )
            (version_root / "metadata" / "quality_manifest.json").write_text(
                "{}", encoding="utf-8"
            )

            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            keys = staging.list_keys("amtrak-stations", "amtrak-stations", "run_a")

            copied = _copy_source_format_files(
                staging,
                published,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_a",
                keys=keys,
            )

            self.assertEqual(
                sorted(copied),
                [
                    "amtrak-stations/amtrak-stations/run_a/file_geodatabase/stations.gdb/a00000001.gdbtable",
                    "amtrak-stations/amtrak-stations/run_a/shapefile/stations.shp",
                ],
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "shapefile"
                    / "stations.shp"
                ).exists()
            )
            self.assertTrue(
                (
                    Path(published_dir)
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "file_geodatabase"
                    / "stations.gdb"
                    / "a00000001.gdbtable"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()
