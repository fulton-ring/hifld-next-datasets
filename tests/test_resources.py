import base64
import hashlib
import os
import tempfile
import unittest
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
import zipfile

import google_crc32c

from dagster_hifld import resources
from dagster_hifld.download import (
    _collect_staging_files,
    _extract_zip,
    _pick_primary_file,
    build_version_id,
    download_convert_and_stage,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class StagingStorageResourceTests(unittest.TestCase):
    def test_bucket_resources_apply_portolan_prefix_from_environment(self):
        with patch.dict(
            os.environ,
            {
                "HIFLD_STAGING_BUCKET": "staging-bucket",
                "HIFLD_DATASETS_BUCKET": "published-bucket",
                "HIFLD_STAGING_PREFIX": "hifld",
                "HIFLD_DATASETS_PREFIX": "hifld",
            },
        ):
            staging = StagingStorageResource.from_env()
            published = PublishedStorageResource.from_env()
        self.assertEqual(staging.prefix, "hifld")
        self.assertEqual(published.prefix, "hifld")

    def test_list_prefix_returns_sorted_prefixed_keys_without_reading_contents(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = StagingStorageResource(
                local_dir=tmpdir,
                use_local=True,
                prefix="tenant-a",
            )
            storage.write_key("dataset-b/file/v1/geojson/b.geojson", b"b")
            storage.write_key("dataset-a/file/v1/geojson/a.geojson", b"a")

            self.assertEqual(
                storage.list_prefix(),
                [
                    "tenant-a/dataset-a/file/v1/geojson/a.geojson",
                    "tenant-a/dataset-b/file/v1/geojson/b.geojson",
                ],
            )
            self.assertEqual(
                storage.list_prefix("dataset-b"),
                ["tenant-a/dataset-b/file/v1/geojson/b.geojson"],
            )
            self.assertEqual(
                storage.list_prefix("dataset-a/file/v1/geojson/a.geojson"),
                ["tenant-a/dataset-a/file/v1/geojson/a.geojson"],
            )

    def test_downloader_uses_canonical_source_precedence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shapefile = root / "source.shp"
            shapefile.write_bytes(b"shp")
            geodatabase_file = root / "source.gdb" / "a00000001.gdbtable"
            geodatabase_file.parent.mkdir()
            geodatabase_file.write_bytes(b"gdb")
            geopackage = root / "source.gpkg"
            geopackage.write_bytes(b"gpkg")

            self.assertEqual(
                _pick_primary_file([shapefile, geodatabase_file, geopackage]),
                geopackage,
            )

    def test_downloader_does_not_select_derived_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            geoparquet = root / "source.parquet"
            pmtiles = root / "source.pmtiles"
            geoparquet.write_bytes(b"parquet")
            pmtiles.write_bytes(b"pmtiles")

            self.assertIsNone(_pick_primary_file([geoparquet, pmtiles]))

    def test_downloader_collects_case_insensitive_shapefile_sidecars(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shapefile = root / "SOURCE.SHP"
            shapefile.write_bytes(b"shp")
            (root / "SOURCE.SHX").write_bytes(b"shx")
            (root / "SOURCE.DBF").write_bytes(b"dbf")
            (root / "SOURCE.QIX").write_bytes(b"qix")
            (root / "SOURCE.NAME.ATX").write_bytes(b"atx")
            (root / "SOURCE.SHP.XML").write_bytes(b"xml")

            collected = _collect_staging_files(shapefile)

            self.assertCountEqual(
                [relative_key for _path, relative_key in collected],
                [
                    "SOURCE.SHP",
                    "SOURCE.SHX",
                    "SOURCE.DBF",
                    "SOURCE.QIX",
                    "SOURCE.NAME.ATX",
                    "SOURCE.SHP.XML",
                ],
            )

    def test_build_version_id_uses_run_create_timestamp(self):
        run_record = SimpleNamespace(
            create_timestamp=datetime(2026, 4, 7, 20, 40, 46, tzinfo=timezone.utc)
        )
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
            (root / "amtrak-stations" / "amtrak-stations" / "run_a" / "metadata").mkdir(
                parents=True
            )
            (
                root / "amtrak-stations" / "amtrak-stations" / "run_b" / "shapefile"
            ).mkdir(parents=True)
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
        fake_fs.find.assert_called_once_with(
            "staging-bucket/address-ranges/address-ranges/v1.0.0"
        )
        self.assertEqual(
            keys,
            ["address-ranges/address-ranges/v1.0.0/geopackage/source.gpkg"],
        )

    def test_gcs_list_prefix_lists_configured_resource_prefix(self):
        resource = StagingStorageResource(
            bucket="staging-bucket",
            use_local=False,
            prefix="tenant-a",
        )
        fake_fs = Mock()
        fake_fs.find.return_value = [
            "staging-bucket/tenant-a/dataset/file/v1/geojson/source.geojson",
        ]

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            keys = resource.list_prefix()

        fake_fs.find.assert_called_once_with("staging-bucket/tenant-a")
        self.assertEqual(
            keys,
            ["tenant-a/dataset/file/v1/geojson/source.geojson"],
        )

    def test_gcs_get_local_version_dir_streams_objects_to_disk(self):
        resource = StagingStorageResource(bucket="staging-bucket", use_local=False)
        key = "dataset/file/v1.0.0/geopackage/source.gpkg"
        fake_fs = Mock()
        fake_fs.find.return_value = [f"staging-bucket/{key}"]
        fake_file = MagicMock()
        fake_file.__enter__.return_value = BytesIO(b"source bytes")
        fake_fs.open.return_value = fake_file

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            with resource.get_local_version_dir(
                "dataset", "file", "v1.0.0"
            ) as version_dir:
                copied = Path(version_dir) / "geopackage" / "source.gpkg"
                self.assertEqual(copied.read_bytes(), b"source bytes")

        fake_fs.read_bytes.assert_not_called()
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
            self.assertFalse(
                resource.object_exists(
                    "amtrak-stations/amtrak-stations/run_a/metadata/missing.json"
                )
            )

    def test_gcs_content_match_uses_object_checksums_without_downloading(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        destination = StagingStorageResource(bucket="staging-bucket", use_local=False)
        fake_fs = Mock()
        fake_fs.exists.return_value = True
        fake_fs.info.side_effect = [
            {"size": 12, "md5Hash": "same-checksum"},
            {"size": 12, "md5Hash": "same-checksum"},
        ]

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            matches = source.object_content_matches(
                destination,
                "dataset/file/v1/geojson/source.geojson",
                "dataset/file/v1/geojson/source.geojson",
            )

        self.assertTrue(matches)
        fake_fs.open.assert_not_called()
        fake_fs.read_bytes.assert_not_called()

    def test_local_candidate_matches_gcs_destination_by_md5_without_downloading(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            key = "dataset/file/v1/geojson/source.geojson"
            source = StagingStorageResource(local_dir=tmpdir, use_local=True)
            source.write_key(key, b"candidate bytes")
            destination = StagingStorageResource(
                bucket="staging-bucket",
                use_local=False,
            )
            checksum = base64.b64encode(
                hashlib.md5(b"candidate bytes").digest()
            ).decode()
            fake_fs = Mock()
            fake_fs.exists.return_value = True
            fake_fs.info.return_value = {
                "size": len(b"candidate bytes"),
                "md5Hash": checksum,
            }

            with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
                matches = source.object_content_matches(destination, key, key)

            self.assertTrue(matches)
            fake_fs.open.assert_not_called()
            fake_fs.read_bytes.assert_not_called()

    def test_local_candidate_matches_crc32c_only_gcs_object(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            key = "dataset/file/v1/geojson/source.geojson"
            data = b"candidate bytes"
            source = StagingStorageResource(local_dir=tmpdir, use_local=True)
            source.write_key(key, data)
            destination = StagingStorageResource(
                bucket="staging-bucket",
                use_local=False,
            )
            checksum = base64.b64encode(
                google_crc32c.value(data).to_bytes(4, "big")
            ).decode()
            fake_fs = Mock()
            fake_fs.exists.return_value = True
            fake_fs.info.return_value = {
                "size": len(data),
                "crc32c": checksum,
            }

            with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
                matches = source.object_content_matches(destination, key, key)

            self.assertTrue(matches)
            fake_fs.open.assert_not_called()
            fake_fs.read_bytes.assert_not_called()

    def test_gcs_listing_captures_generation_checksum_and_size_in_one_call(self):
        storage = StagingStorageResource(
            bucket="published-bucket",
            prefix="production",
            use_local=False,
        )
        key = "production/dataset/file/v1/geojson/source.geojson"
        fake_fs = Mock()
        fake_fs.find.return_value = {
            f"published-bucket/{key}": {
                "name": f"published-bucket/{key}",
                "size": 12,
                "generation": "123",
                "md5Hash": "md5-value",
                "crc32c": "crc-value",
            }
        }

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            snapshots = storage.list_object_snapshots("dataset")

        fake_fs.find.assert_called_once_with(
            "published-bucket/production/dataset",
            detail=True,
        )
        self.assertEqual(
            snapshots,
            (
                resources.StorageObjectSnapshot(
                    key=key,
                    size=12,
                    generation="123",
                    md5="md5-value",
                    crc32c="crc-value",
                ),
            ),
        )

    def test_generic_gcs_write_uses_streaming_file_api(self):
        storage = StagingStorageResource(bucket="staging-bucket", use_local=False)
        fake_fs = Mock()
        output = BytesIO()
        fake_fs.open.return_value.__enter__ = Mock(return_value=output)
        fake_fs.open.return_value.__exit__ = Mock(return_value=False)

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            result = storage.write_key(
                "dataset/file/v1/geojson/source.geojson", b"data"
            )

        self.assertEqual(result, "dataset/file/v1/geojson/source.geojson")
        fake_fs.open.assert_called_once_with(
            "staging-bucket/dataset/file/v1/geojson/source.geojson",
            "wb",
            fixed_key_metadata={"cache_control": "no-cache"},
            metadata={"sha256": hashlib.sha256(b"data").hexdigest()},
        )
        self.assertEqual(output.getvalue(), b"data")
        fake_fs.call.assert_not_called()

    def test_gcs_to_local_conditional_copy_uses_captured_generation(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        source_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="101",
            md5="source-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs.open.return_value.__enter__ = Mock(
            return_value=BytesIO(b"source data!")
        )
        fake_fs.open.return_value.__exit__ = Mock(return_value=False)

        with (
            tempfile.TemporaryDirectory() as destination_dir,
            patch("gcsfs.GCSFileSystem", return_value=fake_fs) as filesystem,
        ):
            destination = StagingStorageResource(
                local_dir=destination_dir,
                use_local=True,
            )
            result = source.copy_key_to_if_unchanged(
                destination,
                source_snapshot.key,
                source_snapshot.key,
                source_snapshot=source_snapshot,
                destination_snapshot=None,
            )

            self.assertEqual(result, destination.object_snapshot(source_snapshot.key))

        filesystem.assert_called_once_with(version_aware=True)
        fake_fs.open.assert_called_once_with(
            "published-bucket/dataset/file/v1/geojson/source.geojson#101",
            "rb",
        )

    def test_gcs_conditional_copy_returns_atomic_destination_snapshot(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        destination = StagingStorageResource(bucket="staging-bucket", use_local=False)
        source_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="101",
            md5="source-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs.call.return_value = {
            "done": True,
            "resource": {
                "size": "12",
                "generation": "505",
                "md5Hash": "copied-md5",
            },
        }

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            result = source.copy_key_to_if_unchanged(
                destination,
                source_snapshot.key,
                source_snapshot.key,
                source_snapshot=source_snapshot,
                destination_snapshot=None,
            )

        self.assertEqual(
            result,
            resources.StorageObjectSnapshot(
                key=source_snapshot.key,
                size=12,
                generation="505",
                md5="copied-md5",
                crc32c=None,
            ),
        )
        fake_fs.info.assert_not_called()

    def test_gcs_conditional_copy_uses_source_and_destination_generations(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        destination = StagingStorageResource(bucket="staging-bucket", use_local=False)
        source_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="101",
            md5="source-md5",
            crc32c=None,
        )
        destination_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=8,
            generation="202",
            md5="old-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs.call.return_value = {
            "done": True,
            "resource": {
                "size": "12",
                "generation": "203",
                "md5Hash": "source-md5",
            },
        }

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            source.copy_key_to_if_unchanged(
                destination,
                source_snapshot.key,
                destination_snapshot.key,
                source_snapshot=source_snapshot,
                destination_snapshot=destination_snapshot,
            )

        fake_fs.call.assert_called_once()
        _args, kwargs = fake_fs.call.call_args
        self.assertEqual(kwargs["ifSourceGenerationMatch"], "101")
        self.assertEqual(kwargs["ifGenerationMatch"], "202")

    def test_gcs_conditional_create_copy_uses_zero_destination_generation(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        destination = StagingStorageResource(bucket="staging-bucket", use_local=False)
        source_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="101",
            md5="source-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs.call.return_value = {
            "done": True,
            "resource": {
                "size": "12",
                "generation": "102",
                "md5Hash": "source-md5",
            },
        }

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            source.copy_key_to_if_unchanged(
                destination,
                source_snapshot.key,
                source_snapshot.key,
                source_snapshot=source_snapshot,
                destination_snapshot=None,
            )

        _args, kwargs = fake_fs.call.call_args
        self.assertEqual(kwargs["ifSourceGenerationMatch"], "101")
        self.assertEqual(kwargs["ifGenerationMatch"], "0")

    def test_gcs_conditional_delete_uses_expected_generation(self):
        storage = StagingStorageResource(bucket="staging-bucket", use_local=False)
        snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="303",
            md5="source-md5",
            crc32c=None,
        )
        fake_fs = Mock()

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            storage.delete_key_if_unchanged(snapshot.key, snapshot)

        fake_fs.call.assert_called_once()
        _args, kwargs = fake_fs.call.call_args
        self.assertEqual(kwargs["ifGenerationMatch"], "303")

    def test_gcs_conditional_write_uses_expected_generation(self):
        storage = StagingStorageResource(bucket="staging-bucket", use_local=False)
        snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/metadata/source_manifest.json",
            size=2,
            generation="404",
            md5="old-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs._location = "https://storage.googleapis.test"
        fake_fs.call.return_value = {
            "size": "2",
            "generation": "405",
            "md5Hash": "new-md5",
        }

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            result = storage.write_key_if_unchanged(snapshot.key, b"{}", snapshot)

        fake_fs.call.assert_called_once()
        _args, kwargs = fake_fs.call.call_args
        self.assertEqual(kwargs["ifGenerationMatch"], "404")
        self.assertIn(
            b'"metadata":{"sha256":"'
            + hashlib.sha256(b"{}").hexdigest().encode()
            + b'"}',
            kwargs["data"],
        )
        self.assertIn(b'"cacheControl":"no-cache"', kwargs["data"])
        self.assertIn(
            b"Content-Type: application/octet-stream\r\n",
            kwargs["data"],
        )
        self.assertEqual(result.generation, "405")
        self.assertEqual(result.key, snapshot.key)
        fake_fs.info.assert_not_called()

    def test_gcs_conditional_write_rejects_snapshot_for_another_key(self):
        storage = StagingStorageResource(bucket="staging-bucket", use_local=False)
        snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/metadata/other.json",
            size=2,
            generation="404",
            md5="old-md5",
            crc32c=None,
        )
        fake_fs = Mock()

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            with self.assertRaisesRegex(ValueError, "snapshot key"):
                storage.write_key_if_unchanged(
                    "dataset/file/v1/metadata/source_manifest.json",
                    b"{}",
                    snapshot,
                )

        fake_fs.call.assert_not_called()

    def test_gcs_conditional_mutations_reject_missing_generations(self):
        source = StagingStorageResource(bucket="published-bucket", use_local=False)
        destination = StagingStorageResource(bucket="staging-bucket", use_local=False)
        source_snapshot = resources.StorageObjectSnapshot(
            key="dataset/file/v1/geojson/source.geojson",
            size=12,
            generation="101",
            md5="source-md5",
            crc32c=None,
        )
        destination_snapshot = resources.StorageObjectSnapshot(
            key=source_snapshot.key,
            size=12,
            generation=None,
            md5="old-md5",
            crc32c=None,
        )
        fake_fs = Mock()
        fake_fs._location = "https://storage.googleapis.test"
        fake_fs.call.return_value = {"done": True}

        with patch("gcsfs.GCSFileSystem", return_value=fake_fs):
            with self.assertRaisesRegex(RuntimeError, "no generation"):
                source.copy_key_to_if_unchanged(
                    destination,
                    source_snapshot.key,
                    destination_snapshot.key,
                    source_snapshot=source_snapshot,
                    destination_snapshot=destination_snapshot,
                )
            with self.assertRaisesRegex(RuntimeError, "no generation"):
                destination.write_key_if_unchanged(
                    destination_snapshot.key,
                    b"new",
                    destination_snapshot,
                )

        fake_fs.call.assert_not_called()

    def test_local_conditional_copy_rejects_changed_source_or_destination(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as destination_dir,
        ):
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            destination = StagingStorageResource(
                local_dir=destination_dir,
                use_local=True,
            )
            key = "dataset/file/v1/geojson/source.geojson"
            source.write_key(key, b"source one")
            destination.write_key(key, b"destination one")
            source_snapshot = source.object_snapshot(key)
            destination_snapshot = destination.object_snapshot(key)
            self.assertIsNotNone(source_snapshot)
            self.assertIsNotNone(destination_snapshot)

            source.write_key(key, b"source changed")
            with self.assertRaisesRegex(RuntimeError, "source changed"):
                source.copy_key_to_if_unchanged(
                    destination,
                    key,
                    key,
                    source_snapshot=source_snapshot,
                    destination_snapshot=destination_snapshot,
                )
            self.assertEqual(
                destination.read_bytes("dataset", "file", "v1", key), b"destination one"
            )

            current_source = source.object_snapshot(key)
            destination.write_key(key, b"destination changed")
            with self.assertRaisesRegex(RuntimeError, "destination changed"):
                source.copy_key_to_if_unchanged(
                    destination,
                    key,
                    key,
                    source_snapshot=current_source,
                    destination_snapshot=destination_snapshot,
                )
            self.assertEqual(
                destination.read_bytes("dataset", "file", "v1", key),
                b"destination changed",
            )

    def test_local_conditional_copy_returns_committed_snapshot(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as destination_dir,
        ):
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            destination = StagingStorageResource(
                local_dir=destination_dir,
                use_local=True,
            )
            key = "dataset/file/v1/geojson/source.geojson"
            source.write_key(key, b"source")
            source_snapshot = source.object_snapshot(key)
            self.assertIsNotNone(source_snapshot)

            committed = source.copy_key_to_if_unchanged(
                destination,
                key,
                key,
                source_snapshot=source_snapshot,
                destination_snapshot=None,
            )

            self.assertIsInstance(committed, resources.StorageObjectSnapshot)
            self.assertEqual(committed, destination.object_snapshot(key))

    def test_local_to_gcs_copy_cleans_up_created_generation_when_source_races(self):
        with tempfile.TemporaryDirectory() as source_dir:
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            destination = StagingStorageResource(
                bucket="staging-bucket", use_local=False
            )
            key = "dataset/file/v1/geojson/source.geojson"
            source.write_key(key, b"source")
            source_snapshot = source.object_snapshot(key)
            self.assertIsNotNone(source_snapshot)
            output = Mock()
            output.generation = "900"
            output.__enter__ = Mock(return_value=output)
            output.__exit__ = Mock(return_value=False)
            fake_fs = Mock()
            fake_fs.open.return_value = output

            def copy_and_race(source_file, destination_file, length):
                destination_file.write(source_file.read())
                source.write_key(key, b"changed")

            with (
                patch("gcsfs.GCSFileSystem", return_value=fake_fs),
                patch.object(
                    resources.shutil, "copyfileobj", side_effect=copy_and_race
                ),
                self.assertRaisesRegex(RuntimeError, "source changed"),
            ):
                source.copy_key_to_if_unchanged(
                    destination,
                    key,
                    key,
                    source_snapshot=source_snapshot,
                    destination_snapshot=None,
                )

            calls = [
                call
                for call in fake_fs.call.call_args_list
                if call.args[:2] == ("DELETE", "b/{}/o/{}")
            ]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0].kwargs["ifGenerationMatch"], "900")

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

            version_root = (
                Path(tmpdir) / "amtrak-stations" / "amtrak-stations" / "run_test"
            )
            self.assertTrue((version_root / "shapefile" / "amtrak.zip").exists())
            self.assertFalse((version_root / "shapefile" / "stations.shp").exists())
            with zipfile.ZipFile(version_root / "shapefile" / "amtrak.zip") as archive:
                self.assertEqual(
                    sorted(archive.namelist()),
                    ["stations.dbf", "stations.shp", "stations.shx"],
                )
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
