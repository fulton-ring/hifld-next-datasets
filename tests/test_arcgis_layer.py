import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources import arcgis_layer as common
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha


class ArcgisLayerTests(unittest.TestCase):
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

    def test_ienc_layer_capture_roster_pins_pjson_urls(self):
        self.assertEqual(common.ienc_layer_capture_roster(
            [1, 2, 3],
            bts_item_url="https://bts.example/item?f=json",
            service_directory_url="https://svc.example/dir?f=pjson",
            service_url="https://svc.example/LAYER/MapServer",
            layer_url="https://svc.example/LAYER/MapServer/1",
            page_size=2,
        ), [
            ("bts_item", "https://bts.example/item?f=json"),
            ("service_directory", "https://svc.example/dir?f=pjson"),
            ("service_before", "https://svc.example/LAYER/MapServer?f=pjson"),
            ("layer_before", "https://svc.example/LAYER/MapServer/1?f=pjson"),
            ("count_before", "https://svc.example/LAYER/MapServer/1/query?where=1%3D1&returnCountOnly=true&f=json"),
            ("ids_before", "https://svc.example/LAYER/MapServer/1/query?where=1%3D1&returnIdsOnly=true&f=json"),
            ("page_000", "https://svc.example/LAYER/MapServer/1/query?objectIds=1%2C2&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&outSR=4326&orderByFields=OBJECTID+ASC&f=json"),
            ("page_001", "https://svc.example/LAYER/MapServer/1/query?objectIds=3&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&outSR=4326&orderByFields=OBJECTID+ASC&f=json"),
            ("layer_after", "https://svc.example/LAYER/MapServer/1?f=pjson"),
            ("count_after", "https://svc.example/LAYER/MapServer/1/query?where=1%3D1&returnCountOnly=true&f=json"),
            ("ids_after", "https://svc.example/LAYER/MapServer/1/query?where=1%3D1&returnIdsOnly=true&f=json"),
            ("service_after", "https://svc.example/LAYER/MapServer?f=pjson"),
        ])

    def test_ienc_layer_capture_roster_pins_json_urls(self):
        roster = common.ienc_layer_capture_roster(
            [1, 2, 3],
            bts_item_url="https://bts.example/item?f=json",
            service_directory_url="https://svc.example/dir?f=pjson",
            service_url="https://svc.example/LAYER/MapServer",
            layer_url="https://svc.example/LAYER/MapServer/1",
            page_size=2,
            layer_format="json",
        )
        urls = dict(roster)
        self.assertEqual(urls["layer_before"], "https://svc.example/LAYER/MapServer/1?f=json")
        self.assertEqual(urls["layer_after"], "https://svc.example/LAYER/MapServer/1?f=json")
        self.assertEqual([name for name, _url in roster],
                         ["bts_item", "service_directory", "service_before", "layer_before",
                          "count_before", "ids_before", "page_000", "page_001", "layer_after",
                          "count_after", "ids_after", "service_after"])

    def test_private_aliases_bind_the_public_implementations(self):
        self.assertIs(common._capture, common.capture_page)
        self.assertIs(common._load_capture, common.load_capture)
        self.assertIs(common._capture_identity, common.capture_identity)
        self.assertIs(common._esri_polyline_wkb, common.esri_polyline_wkb)
        self.assertIs(common._parquet_bytes, common.polyline_parquet_bytes)


if __name__ == "__main__":
    unittest.main()
