import struct
import unittest

from dagster_hifld.sources.coastlines_usace_ienc import _esri_polyline


class CoastlinesPolylineTests(unittest.TestCase):
    def test_retains_xy_line_coordinates_as_wkb(self):
        raw = {"paths": [[[1.25, 2.5], [3.75, 4.5]]], "hasZ": False, "hasM": False}
        wkb = _esri_polyline(raw)
        byte_order, geometry_type, count = struct.unpack("<BII", wkb[:9])
        self.assertEqual((byte_order, geometry_type, count), (1, 2, 2))
        self.assertEqual(struct.unpack("<dddd", wkb[9:]), (1.25, 2.5, 3.75, 4.5))

    def test_retains_multiple_paths_as_multilinestring(self):
        raw = {"paths": [[[0, 1], [2, 3]], [[4, 5], [6, 7]]], "hasZ": False, "hasM": False}
        wkb = _esri_polyline(raw)
        self.assertEqual(struct.unpack("<BII", wkb[:9]), (1, 5, 2))

    def test_rejects_unclosed_source_ordinate_dimensions(self):
        raw = {"paths": [[[0, 1], [2, 3, 4]]], "hasZ": False, "hasM": False}
        with self.assertRaisesRegex(ValueError, "ordinate_count_drift"):
            _esri_polyline(raw)

    def test_rejects_nonfinite_source_coordinates(self):
        raw = {"paths": [[[0, 1], [float("nan"), 3]]], "hasZ": False, "hasM": False}
        with self.assertRaisesRegex(ValueError, "coordinate_invalid"):
            _esri_polyline(raw)
