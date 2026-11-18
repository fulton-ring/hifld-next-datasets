"""The canonical file digest must reproduce every retired private copy.

The copies removed in issue #750 read a path in fixed-size blocks and updated
one ``hashlib.sha256`` object; chunk size did not affect the returned hex
string. These cases pin the shared helper to that reference behaviour at and
around block boundaries.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.validation import sha256_file


class Sha256FileTest(unittest.TestCase):
    def test_known_vectors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "payload"
            path.write_bytes(b"")
            self.assertEqual(
                sha256_file(path),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
            path.write_bytes(b"abc")
            self.assertEqual(
                sha256_file(path),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )

    def test_matches_hashlib_across_block_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "payload"
            for size in (1, 1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 1, 8 * 1024 * 1024 + 7):
                with self.subTest(size=size):
                    body = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
                    path.write_bytes(body)
                    self.assertEqual(sha256_file(path), hashlib.sha256(body).hexdigest())


if __name__ == "__main__":
    unittest.main()
