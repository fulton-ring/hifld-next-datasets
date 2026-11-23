import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pyproj

from dagster_hifld.sources import usace_ienc_common as ienc_common
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
                ienc_common.load_capture(root, "page")

    def test_load_capture_rejects_receipt_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir(); self._retained(root)
            (root / "page.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "retained_capture_receipt_invalid:page"):
                ienc_common.load_capture(root, "page")

    def test_capture_page_refuses_capture_over_budget_before_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"; root.mkdir()
            (root / "earlier.body").write_bytes(b"x" * 400)
            (root / "earlier.wire").write_bytes(b"x" * 400)
            with patch.object(ienc_common, "capture_get") as capture_get:
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded_before_request"):
                    ienc_common.capture_page(object(), "https://ienccloud.us/example", root, "page",
                                             page_max_bytes=100, capture_max_bytes=900)
            capture_get.assert_not_called()

    def test_esri_polyline_wkb_encodes_two_paths(self):
        geometry = {"paths": [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0]]]}
        expected = bytearray(struct.pack("<BI", 1, 5))  # little-endian MultiLineString
        expected.extend(struct.pack("<I", len(geometry["paths"])))
        for path in geometry["paths"]:
            expected.extend(struct.pack("<BI", 1, 2))  # nested LineString
            expected.extend(struct.pack("<I", len(path)))
            for x, y in path:
                expected.extend(struct.pack("<dd", x, y))
        self.assertEqual(ienc_common.esri_polyline_wkb(geometry), bytes(expected))


class PolylineCandidateTests(unittest.TestCase):
    FIELDS = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "Object_Name", "type": "esriFieldTypeString", "alias": "Object_Name",
         "length": 254, "domain": None},
        {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum",
         "domain": None},
    ]

    def _features(self, count: int):
        return [{"attributes": {"OBJECTID": oid, "Object_Name": f"line-{oid}",
                                 "Scale_Minimum": 30000},
                 "geometry": {"paths": [[[-88.0 + oid / 1000, 37.0], [-87.9 + oid / 1000, 37.1]]]}}
                for oid in range(1, count + 1)]

    def test_candidate_streams_all_rows_with_geo_metadata(self):
        features = self._features(300)
        with tempfile.TemporaryDirectory() as tmp:
            path, digest, rows = ienc_common.polyline_candidate_parquet(
                self.FIELDS, features, 4326, Path(tmp))
            table = pq.read_table(path)
            candidate_bytes = path.read_bytes()
        self.assertEqual(rows, 300)
        self.assertEqual(table.num_rows, 300)
        self.assertEqual([f.name for f in table.schema],
                         ["OBJECTID", "Object_Name", "Scale_Minimum", "geometry"])
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(1, 301)))
        self.assertEqual(hashlib.sha256(candidate_bytes).hexdigest(), digest)
        metadata = json.loads(table.schema.metadata[b"geo"].decode())
        self.assertEqual(metadata["version"], "1.1.0")
        self.assertEqual(metadata["primary_column"], "geometry")
        column = metadata["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["geometry_types"], ["MultiLineString"])
        self.assertEqual(column["crs"]["id"], {"authority": "EPSG", "code": 4326})
        self.assertEqual(pyproj.CRS.from_json_dict(column["crs"]).to_epsg(), 4326)

    def test_non_4326_source_crs_is_not_mislabeled(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _digest, _rows = ienc_common.polyline_candidate_parquet(
                self.FIELDS, self._features(10), 4269, Path(tmp))
            table = pq.read_table(path)
        column = json.loads(table.schema.metadata[b"geo"].decode())["columns"]["geometry"]
        self.assertEqual(column["crs"]["id"], {"authority": "EPSG", "code": 4269})
        self.assertEqual(pyproj.CRS.from_json_dict(column["crs"]).to_epsg(), 4269)
        self.assertNotIn("WGS 84", json.dumps(column["crs"]))

    def test_legacy_bytes_wrapper_delegates_to_streamed_candidate(self):
        features = self._features(600)
        legacy = ienc_common.polyline_parquet_bytes(self.FIELDS, features)
        with tempfile.TemporaryDirectory() as tmp:
            path, digest, rows = ienc_common.polyline_candidate_parquet(
                self.FIELDS, features, 4326, Path(tmp))
            streamed = pq.read_table(path)
        self.assertEqual(rows, 600)
        self.assertEqual(hashlib.sha256(legacy).hexdigest(), digest)
        self.assertTrue(pq.read_table(io.BytesIO(legacy)).equals(streamed))


if __name__ == "__main__":
    unittest.main()
