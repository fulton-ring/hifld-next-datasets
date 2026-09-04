import tempfile
import unittest
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
import zipfile

from dagster_hifld.download import _extract_zip, build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource


class StagingStorageResourceTests(unittest.TestCase):
    def test_build_version_id_uses_run_create_timestamp(self):
        run_record = SimpleNamespace(create_timestamp=datetime(2026, 4, 7, 20, 40, 46, tzinfo=timezone.utc))
        context = SimpleNamespace(
            run_id="fa8b8d5b-a72c-4bbe-9c16-5efd0b9a5095",
            instance=SimpleNamespace(get_run_record_by_id=lambda _: run_record),
        )

        self.assertEqual(build_version_id(context), "v20260407T204046Z")

    def test_build_target_location_uses_dataset_and_file_slug(self):
        resource = StagingStorageResource(local_dir="unused", use_local=True)

        key = resource.build_target_location(
            "amtrak-stations",
            "amtrak-stations",
            "run_1234567890ab",
            "metadata/quality_manifest.json",
        )

        self.assertEqual(
            key,
            "amtrak-stations/amtrak-stations/run_1234567890ab/metadata/quality_manifest.json",
        )

    def test_write_and_object_exists_do_not_double_apply_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(
                local_dir=tmpdir,
                use_local=True,
                prefix="nested",
            )

            key = resource.write(
                "amtrak-stations",
                "amtrak-stations",
                "run_a",
                "metadata/quality_manifest.json",
                b"{}",
            )

            self.assertEqual(
                key,
                "nested/amtrak-stations/amtrak-stations/run_a/metadata/quality_manifest.json",
            )
            self.assertTrue((Path(tmpdir) / key).exists())
            self.assertFalse(
                (
                    Path(tmpdir)
                    / "nested"
                    / "nested"
                    / "amtrak-stations"
                    / "amtrak-stations"
                    / "run_a"
                    / "metadata"
                    / "quality_manifest.json"
                ).exists()
            )
            self.assertTrue(resource.object_exists(key))

    def test_list_versions_returns_immediate_version_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "amtrak-stations" / "amtrak-stations" / "run_a" / "metadata").mkdir(parents=True)
            (root / "amtrak-stations" / "amtrak-stations" / "run_b" / "shapefile").mkdir(parents=True)
            (root / "amtrak-stations" / "other-file" / "run_c").mkdir(parents=True)

            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)

            self.assertEqual(
                resource.list_versions("amtrak-stations", "amtrak-stations"),
                ["run_a", "run_b"],
            )

    def test_gcs_list_keys_uses_recursive_find_without_prefix_exists_guard(self):
        resource = StagingStorageResource(
            bucket="staging-bucket",
            use_local=False,
        )
        fake_fs = Mock()
        fake_fs.find.return_value = [
            "staging-bucket/address-ranges/address-ranges/v1.0.0/geopackage/source.gpkg",
        ]

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            keys = resource.list_keys("address-ranges", "address-ranges", "v1.0.0")

        fake_fs.exists.assert_not_called()
        fake_fs.find.assert_called_once_with("staging-bucket/address-ranges/address-ranges/v1.0.0")
        self.assertEqual(
            keys,
            ["address-ranges/address-ranges/v1.0.0/geopackage/source.gpkg"],
        )

    def test_gcs_list_keys_skips_prefix_marker_objects(self):
        resource = StagingStorageResource(bucket="staging-bucket", use_local=False)
        key = "dataset/file/v1.0.0/geoparquet/source.parquet"
        fake_fs = Mock()
        fake_fs.find.return_value = [
            "staging-bucket/dataset/file/v1.0.0/",
            "staging-bucket/dataset/file/v1.0.0/geoparquet/",
            f"staging-bucket/{key}",
        ]

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            keys = resource.list_keys("dataset", "file", "v1.0.0")

        self.assertEqual(keys, [key])

    def test_gcs_get_local_version_dir_streams_objects_to_disk(self):
        resource = StagingStorageResource(bucket="staging-bucket", use_local=False)
        key = "dataset/file/v1.0.0/geopackage/source.gpkg"
        fake_fs = Mock()
        fake_fs.find.return_value = [f"staging-bucket/{key}"]
        fake_file = MagicMock()
        fake_file.__enter__.return_value = BytesIO(b"source bytes")
        fake_fs.open.return_value = fake_file

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            with resource.get_local_version_dir("dataset", "file", "v1.0.0") as version_dir:
                copied = Path(version_dir) / "geopackage" / "source.gpkg"
                self.assertEqual(copied.read_bytes(), b"source bytes")

        fake_fs.read_bytes.assert_not_called()
        fake_fs.open.assert_called_once_with(f"staging-bucket/{key}", "rb")

    def test_gcs_get_local_version_dir_skips_prefix_marker_objects(self):
        resource = StagingStorageResource(bucket="staging-bucket", use_local=False)
        key = "dataset/file/v1.0.0/geoparquet/source.parquet"
        fake_fs = Mock()
        fake_fs.find.return_value = [
            "staging-bucket/dataset/file/v1.0.0/geoparquet",
            f"staging-bucket/{key}",
        ]
        fake_file = MagicMock()
        fake_file.__enter__.return_value = BytesIO(b"source bytes")
        fake_fs.open.return_value = fake_file

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            with resource.get_local_version_dir("dataset", "file", "v1.0.0") as version_dir:
                copied = Path(version_dir) / "geoparquet" / "source.parquet"
                self.assertEqual(copied.read_bytes(), b"source bytes")

        fake_fs.open.assert_called_once_with(f"staging-bucket/{key}", "rb")

    def test_object_exists_checks_exact_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            key = "amtrak-stations/amtrak-stations/run_a/metadata/quality_manifest.json"
            path = root / key
            path.parent.mkdir(parents=True)
            path.write_text("{}", encoding="utf-8")

            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)

            self.assertTrue(resource.object_exists(key))
            self.assertFalse(resource.object_exists("amtrak-stations/amtrak-stations/run_a/metadata/missing.json"))

    def test_build_version_id_raises_if_run_record_cannot_be_resolved(self):
        context = SimpleNamespace(
            run_id="fa8b8d5b-a72c-4bbe-9c16-5efd0b9a5095",
            instance=SimpleNamespace(get_run_record_by_id=lambda _: None),
        )

        with self.assertRaises(ValueError):
            build_version_id(context)

    def test_download_convert_and_stage_only_stages_source_formats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            zip_bytes = self._build_shapefile_zip()

            with patch("dagster_hifld.download.httpx.Client") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get.return_value.content = zip_bytes
                client.get.return_value.headers = {}
                client.get.return_value.raise_for_status.return_value = None

                result = download_convert_and_stage(
                    [("https://example.com/amtrak.zip", "amtrak.zip")],
                    "amtrak-stations",
                    "amtrak-stations",
                    "run_test",
                    resource,
                )

            version_root = Path(tmpdir) / "amtrak-stations" / "amtrak-stations" / "run_test"
            self.assertTrue((version_root / "shapefile" / "stations.shp").exists())
            self.assertTrue((version_root / "shapefile" / "stations.dbf").exists())
            self.assertFalse((version_root / "parquet").exists())
            self.assertFalse((version_root / "pmtiles").exists())
            self.assertEqual(result["dataset_slug"], "amtrak-stations")
            self.assertEqual(result["file_slug"], "amtrak-stations")
            self.assertEqual(result["version"], "run_test")

    def test_download_convert_and_stage_reuses_one_http_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            zip_bytes = self._build_shapefile_zip()

            with patch("dagster_hifld.download.httpx.Client") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                response_a = Mock()
                response_a.content = zip_bytes
                response_a.headers = {}
                response_a.raise_for_status.return_value = None
                response_b = Mock()
                response_b.content = zip_bytes
                response_b.headers = {}
                response_b.raise_for_status.return_value = None
                client.get.side_effect = [response_a, response_b]

                download_convert_and_stage(
                    [
                        ("https://example.com/amtrak-a.zip", "amtrak-a.zip"),
                        ("https://example.com/amtrak-b.zip", "amtrak-b.zip"),
                    ],
                    "amtrak-stations",
                    "amtrak-stations",
                    "run_test",
                    resource,
                )

            self.assertEqual(client_cls.call_count, 1)
            self.assertEqual(client.get.call_count, 2)

    def test_extract_zip_rejects_parent_traversal(self):
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("../escape.shp", b"shp")

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                _extract_zip(buffer.getvalue(), Path(tmpdir))

    @staticmethod
    def _build_shapefile_zip() -> bytes:
        import io
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("stations.shp", b"shp")
            zf.writestr("stations.dbf", b"dbf")
            zf.writestr("stations.shx", b"shx")
        return buffer.getvalue()


if __name__ == "__main__":
    unittest.main()
