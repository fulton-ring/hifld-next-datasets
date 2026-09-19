import asyncio
import unittest
from unittest.mock import patch

from dagster_hifld.conversion import _StorageAdapter
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import _read_optional_key


class S3ConversionAdapterTests(unittest.TestCase):
    def test_s3_adapter_never_initializes_gcs(self):
        storage = StagingStorageResource(bucket="hifld-acceptance", use_local=False,
                                         backend="s3", s3_endpoint_url="http://localhost:8333")
        with patch("gcsfs.GCSFileSystem", side_effect=AssertionError("S3 fell through to GCS")):
            adapter = _StorageAdapter(storage)
            self.assertEqual(adapter.path_to_storage_uri("hifld/test/data.parquet"),
                             "s3://hifld-acceptance/hifld/test/data.parquet")
            with patch.object(StagingStorageResource, "read_key", return_value=b"data"):
                self.assertEqual(asyncio.run(adapter.read_bytes("hifld/test/data.parquet")), b"data")

    def test_s3_source_manifest_uses_selected_backend(self):
        storage = StagingStorageResource(bucket="hifld-acceptance", use_local=False, backend="s3")
        with patch("gcsfs.GCSFileSystem", side_effect=AssertionError("S3 fell through to GCS")):
            with patch.object(StagingStorageResource, "object_exists", return_value=True):
                with patch.object(StagingStorageResource, "read_key", return_value=b"{}"):
                    self.assertEqual(_read_optional_key(storage, "dataset/metadata/source_manifest.json"), b"{}")


if __name__ == "__main__":
    unittest.main()
