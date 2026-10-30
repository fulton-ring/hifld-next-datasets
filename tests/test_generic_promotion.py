from publication_mechanics import PublicationMechanicsMixin

import tempfile
import unittest

from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.portolan.workflow import PortolanPublishRequest, _promote_staged_data
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class GenericPromotionTests(PublicationMechanicsMixin, unittest.TestCase):
    def test_omitted_generic_public_object_blocks_same_version_promotion(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as target_dir,
        ):
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            target = PublishedStorageResource(local_dir=target_dir, use_local=True)
            stale = "dataset/file/v1/raw/json/part.json"
            current = "dataset/file/v1/raw/json/current.json"
            target.write("dataset", "file", "v1", "raw/json/part.json", b"old shard")
            source.write("dataset", "file", "v1", "raw/json/current.json", b"new shard")

            with self.assertRaisesRegex(ValueError, "layout is immutable"):
                promote_immutable_objects(
                    source,
                    target,
                    [(current, current)],
                    version_prefix="dataset/file/v1",
                )

            self.assertEqual(target.read_key(stale), b"old shard")
            self.assertIsNone(target.object_snapshot(current))

    def test_generated_style_and_thumbnail_do_not_block_idempotent_data_retry(self):
        with (
            tempfile.TemporaryDirectory() as source_dir,
            tempfile.TemporaryDirectory() as target_dir,
        ):
            source = StagingStorageResource(local_dir=source_dir, use_local=True)
            target = PublishedStorageResource(local_dir=target_dir, use_local=True)
            source.write("dataset", "file", "v1", "raw/source.json", b"same data")
            target.write("dataset", "file", "v1", "raw/source.json", b"same data")
            style = target.write(
                "dataset", "file", "v1", "styles/maplibre.json", b"generated style"
            )
            thumbnail = target.write(
                "dataset", "file", "v1", "thumbnail/thumbnail.png", b"generated thumbnail"
            )
            style_before = target.object_snapshot(style)
            thumbnail_before = target.object_snapshot(thumbnail)
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1", "Title", "Description", "Agency"
            )

            _promote_staged_data(source, target, request)

            self.assertEqual(target.object_snapshot(style), style_before)
            self.assertEqual(target.object_snapshot(thumbnail), thumbnail_before)
            self.assertEqual(target.read_key("dataset/file/v1/raw/source.json"), b"same data")
