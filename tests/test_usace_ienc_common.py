import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources import usace_ienc_common as common
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha


class UsaceIencCommonTests(unittest.TestCase):
    def _retained(self, root: Path, body: bytes = b"{}"):
        (root / "page.body").write_bytes(body)
        (root / "page.wire").write_bytes(body)
        (root / "page.receipt.json").write_bytes(_canonical({
            "method": "GET", "url": "https://ienccloud.us/example",
            "final_url": "https://ienccloud.us/example", "observed_at": "2026-11-01T00:00:00Z",
            "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body),
            "wire_bytes": len(body), "wire_sha256": _sha(body),
        }) + b"\n")

    def test_load_capture_rejects_symlinked_retained_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._retained(root)
            (root / "page.receipt.json").unlink()
            (root / "page.receipt.json").symlink_to(root / "page.wire")
            with self.assertRaisesRegex(ValueError, "retained_capture_symlink_rejected"):
                common.load_capture(root, "page")

    def test_load_capture_rejects_receipt_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._retained(root)
            (root / "page.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "retained_capture_receipt_invalid:page"):
                common.load_capture(root, "page")

    def test_capture_page_refuses_capture_over_budget_before_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir()
            (root / "earlier.body").write_bytes(b"x" * 400)
            (root / "earlier.wire").write_bytes(b"x" * 400)
            with patch.object(common, "capture_get") as capture_get:
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded_before_request"):
                    common.capture_page(object(), "https://ienccloud.us/example", root, "page",
                                        page_max_bytes=100, capture_max_bytes=900)
            capture_get.assert_not_called()

    def test_capture_page_at_exact_budget_proceeds_and_forwards_page_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir()
            (root / "earlier.body").write_bytes(b"x" * 400)
            (root / "earlier.wire").write_bytes(b"x" * 400)
            receipt = {"status": 200}
            client = object()
            with patch.object(common, "capture_get", return_value=(b"{}", receipt)) as capture_get:
                body, returned = common.capture_page(client, "https://ienccloud.us/example", root, "page",
                                                     page_max_bytes=100, capture_max_bytes=1000)
            self.assertEqual((body, returned), (b"{}", receipt))
            capture_get.assert_called_once_with(client, "https://ienccloud.us/example", root, "page", max_bytes=100)

    def test_esri_polyline_wkb_encodes_two_paths(self):
        geometry = {"paths": [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0]]]}
        expected = bytearray(struct.pack("<BI", 1, 5))  # little-endian MultiLineString
        expected.extend(struct.pack("<I", len(geometry["paths"])))
        for path in geometry["paths"]:
            expected.extend(struct.pack("<BI", 1, 2))  # nested LineString
            expected.extend(struct.pack("<I", len(path)))
            for x, y in path:
                expected.extend(struct.pack("<dd", x, y))
        self.assertEqual(common.esri_polyline_wkb(geometry), bytes(expected))

    def test_private_aliases_bind_the_public_implementations(self):
        self.assertIs(common._capture, common.capture_page)
        self.assertIs(common._load_capture, common.load_capture)
        self.assertIs(common._capture_identity, common.capture_identity)
        self.assertIs(common._esri_polyline_wkb, common.esri_polyline_wkb)
        self.assertIs(common._parquet_bytes, common.polyline_parquet_bytes)


if __name__ == "__main__":
    unittest.main()
