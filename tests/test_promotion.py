import io
import tempfile
import unittest
import zipfile

from dagster_hifld.assets.publish import _copy_version_files

from dagster_hifld.portolan.workflow import PortolanPublishRequest, _promote_staged_data
from dagster_hifld.promotion import promote_immutable_objects, write_immutable_object
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class ImmutablePromotionTests(unittest.TestCase):
    def test_legacy_zip_normalization_is_compressed_and_idempotent(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as target_dir,
        ):
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            target = PublishedStorageResource(local_dir=target_dir, use_local=True)
            for extension in ("shp", "shx", "dbf"):
                source.write(
                    "dataset", "file", "v1", f"unknown/source.{extension}", b"contents"
                )
            copied = _copy_version_files(source, target, "dataset", "file", "v1")
            self.assertEqual(copied, ["dataset/file/v1/shapefile/source.zip"])
            before = target.object_snapshot(copied[0])
            _copy_version_files(source, target, "dataset", "file", "v1")
            self.assertEqual(target.object_snapshot(copied[0]), before)
            with zipfile.ZipFile(io.BytesIO(target.read_key(copied[0]))) as archive:
                self.assertTrue(
                    all(
                        info.compress_type == zipfile.ZIP_DEFLATED
                        for info in archive.infolist()
                    )
                )
                self.assertTrue(
                    all(
                        info.date_time == (1980, 1, 1, 0, 0, 0)
                        for info in archive.infolist()
                    )
                )

    def test_partial_copy_resumes_without_replacing_identical_objects(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as target_dir,
        ):
            source = StagingStorageResource(
                local_dir=source_dir, use_local=True, prefix="staged"
            )
            target = PublishedStorageResource(
                local_dir=target_dir, use_local=True, prefix="published"
            )
            keys = [
                source.write(
                    "dataset", "file", "v1", f"geoparquet/{name}.parquet", name.encode()
                )
                for name in ("a", "b")
            ]
            first = target.write("dataset", "file", "v1", "geoparquet/a.parquet", b"a")
            before = target.object_snapshot(first)
            copied = promote_immutable_objects(
                source, target, [(key, key.removeprefix("staged/")) for key in keys]
            )
            self.assertEqual(target.object_snapshot(first), before)
            self.assertEqual(len(copied), 2)
            self.assertEqual(
                target.read_key("dataset/file/v1/geoparquet/b.parquet"), b"b"
            )

    def test_metadata_only_data_promotion_cannot_replace_existing_bytes(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as target_dir,
        ):
            source = StagingStorageResource(
                local_dir=source_dir, use_local=True, prefix="hifld"
            )
            target = PublishedStorageResource(
                local_dir=target_dir, use_local=True, prefix="hifld"
            )
            key = target.write(
                "dataset", "file", "v1", "geoparquet/data.parquet", b"old"
            )
            source.write("dataset", "file", "v1", "geoparquet/data.parquet", b"new")
            before = target.object_snapshot(key)
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1", "Title", "Description", "Agency"
            )
            with self.assertRaisesRegex(ValueError, "immutable"):
                _promote_staged_data(source, target, request)
            self.assertEqual(target.object_snapshot(key), before)
            self.assertEqual(target.read_key(key), b"old")

    def test_generated_assets_are_idempotent_and_never_replaced(self):
        with tempfile.TemporaryDirectory() as target_dir:
            target = PublishedStorageResource(local_dir=target_dir, use_local=True)
            key = "dataset/file/v1/styles/maplibre.json"
            write_immutable_object(target, key, b"style")
            before = target.object_snapshot(key)
            write_immutable_object(target, key, b"style")
            self.assertEqual(target.object_snapshot(key), before)
            with self.assertRaisesRegex(ValueError, "immutable"):
                write_immutable_object(target, key, b"changed")
            self.assertEqual(target.object_snapshot(key), before)
