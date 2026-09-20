import gzip
import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.portolan.pmtiles import extract_vector_layer_ids
from dagster_hifld.resources import PublishedStorageResource


def _pmtiles_archive(metadata: dict[str, object], *, compression: int = 2) -> bytes:
    encoded = json.dumps(metadata).encode("utf-8")
    payload = gzip.compress(encoded) if compression == 2 else encoded
    header = bytearray(127)
    header[:7] = b"PMTiles"
    header[7] = 3
    header[24:32] = (127).to_bytes(8, "little")
    header[32:40] = len(payload).to_bytes(8, "little")
    header[97] = compression
    return bytes(header) + payload


class PortolanPmtilesTests(unittest.TestCase):
    def test_extracts_deduplicated_vector_layer_ids_from_gzip_metadata(self):
        archive = _pmtiles_archive(
            {
                "vector_layers": [
                    {"id": "roads"},
                    {"id": "water"},
                    {"id": "roads"},
                ]
            }
        )

        self.assertEqual(
            extract_vector_layer_ids(
                lambda offset, length: archive[offset : offset + length], len(archive)
            ),
            ("roads", "water"),
        )

    def test_rejects_missing_vector_layer_metadata(self):
        archive = _pmtiles_archive({"name": "raster"})

        with self.assertRaisesRegex(ValueError, "vector_layers"):
            extract_vector_layer_ids(
                lambda offset, length: archive[offset : offset + length], len(archive)
            )

    def test_rejects_unsupported_internal_compression(self):
        archive = _pmtiles_archive({"vector_layers": [{"id": "roads"}]}, compression=3)

        with self.assertRaisesRegex(ValueError, "compression"):
            extract_vector_layer_ids(
                lambda offset, length: archive[offset : offset + length], len(archive)
            )

    def test_published_storage_reads_a_bounded_local_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            key = "hifld/roads/roads/v1.0.0/pmtiles/roads.pmtiles"
            path = root / key
            path.parent.mkdir(parents=True)
            path.write_bytes(b"0123456789")
            storage = PublishedStorageResource(use_local=True, local_dir=str(root))
            snapshot = storage.object_snapshot(key)
            if snapshot is None:
                self.fail("Expected local PMTiles snapshot")

            self.assertEqual(storage.read_key_range(key, 2, 4, snapshot), b"2345")


if __name__ == "__main__":
    unittest.main()
