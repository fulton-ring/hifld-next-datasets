"""The shared Esri polyline reader must preserve path order and null measures."""
import math
import struct
import unittest
from unittest.mock import patch

import pyarrow as pa
from shapely import from_wkb

from dagster_hifld.sources import esri_geometry as src


def paths(*parts):
    return [list(part) for part in parts]


def hand_encoded(paths):
    """XY encoder retained from wbd_wbdline before the shared reader replaced it."""
    if len(paths) == 1:
        wkb = bytearray(struct.pack("<BI", 1, 2))
    else:
        wkb = bytearray(struct.pack("<BI", 1, 5))
        wkb.extend(struct.pack("<I", len(paths)))
    for path in paths:
        if len(paths) > 1:
            wkb.extend(struct.pack("<BI", 1, 2))
        wkb.extend(struct.pack("<I", len(path)))
        for point in path:
            wkb.extend(struct.pack("<dd", point[0], point[1]))
    return bytes(wkb)


def fake_conversion(test, wkb):
    table = pa.table({"wkb_geometry": pa.array([wkb], type=pa.binary())})
    return patch.object(src.pyogrio, "read_arrow", return_value=({}, table))


class EsriPolylineWkbTests(unittest.TestCase):
    def test_xy_paths_match_the_retired_hand_encoder_and_keep_order(self):
        for roster in (paths([[1, 2], [3, 4]]),
                       paths([[1, 2], [3, 4]], [[5, 6], [7, 8], [9, 10]])):
            with self.subTest(paths=roster):
                wkb = src.esri_polyline_wkb({"paths": roster})
                self.assertEqual(wkb, hand_encoded(roster))
                decoded = from_wkb(wkb)
                self.assertEqual(decoded.geom_type,
                                 "LineString" if len(roster) == 1 else "MultiLineString")
                parts = decoded.geoms if len(roster) > 1 else [decoded]
                self.assertEqual([[tuple(point) for point in part.coords] for part in parts],
                                 [[tuple(point) for point in path] for path in roster])

    def test_z_m_and_zm_with_null_measures_are_preserved(self):
        for has_z, has_m in ((True, False), (False, True), (True, True)):
            width = 2 + int(has_z) + int(has_m)
            for null_indexes in ((), (0, -1), (1,)):
                for null_value in (None, float("nan")):
                    roster = []
                    for path in ([[1, 2], [3, 4]], [[5, 6], [7, 8], [9, 10]]):
                        roster.append([
                            point + ([index + .125] if has_z else []) +
                            ([null_value if index in null_indexes else index + .375] if has_m else [])
                            for index, point in enumerate(path)])
                    with self.subTest(has_z=has_z, has_m=has_m,
                                      null_indexes=null_indexes, null_value=null_value):
                        wkb = src.esri_polyline_wkb({"paths": roster}, has_z=has_z, has_m=has_m)
                        decoded = from_wkb(wkb)
                        self.assertEqual((decoded.has_z, decoded.has_m), (has_z, has_m))
                        parts = decoded.geoms if decoded.geom_type == "MultiLineString" else [decoded]
                        output = [list(part.coords) for part in parts]
                        self.assertEqual(len(output), len(roster))
                        for source_path, output_path in zip(roster, output):
                            self.assertEqual(len(source_path), len(output_path))
                            for source_point, output_point in zip(source_path, output_path):
                                self.assertEqual(len(source_point), width)
                                for index, (expected, actual) in enumerate(
                                        zip(source_point, output_point)):
                                    null_measure = (has_m and index == width - 1
                                                    and expected is not None
                                                    and isinstance(expected, float)
                                                    and math.isnan(expected))
                                    if expected is None or null_measure:
                                        self.assertTrue(math.isnan(actual))
                                    else:
                                        self.assertEqual(actual, expected)

    def test_null_measure_wkb_carries_a_nan_and_not_an_invented_number(self):
        wkb = src.esri_polyline_wkb({"paths": [[[1, 2, None], [3, 4, 5.5]]], "hasM": True})
        self.assertIn(struct.pack("<d", float("nan")), wkb)
        self.assertTrue(math.isnan(from_wkb(wkb).coords[0][2]))
        self.assertEqual(from_wkb(wkb).coords[1][2], 5.5)

    def test_reordered_paths_and_changed_ordinates_are_refused(self):
        roster = paths([[1, 2], [3, 4]], [[5, 6], [7, 8]])
        reordered = src.esri_polyline_wkb({"paths": list(reversed(roster))})
        changed = src.esri_polyline_wkb({"paths": [[[1, 2], [3, 4]], [[5, 6], [7, 9]]]})
        for wkb in (reordered, changed):
            with self.subTest(wkb=wkb):
                with fake_conversion(self, wkb):
                    with self.assertRaisesRegex(ValueError, "esri_conversion_coordinates_changed"):
                        src.esri_polyline_wkb({"paths": roster})

    def test_reader_refusals(self):
        cases = [
            ({"curvePaths": [[[1, 2], [3, 4]]]}, {}, "unsupported_esri_curves"),
            ({"curveRings": [[[1, 2], [3, 4]]]}, {}, "unsupported_esri_curves"),
            ({"paths": [[[1, 2], [3, 4]]], "rings": []}, {}, "unexpected_esri_geometry_members"),
            ({"paths": [[[1, 2], [3, 4]]], "hasZ": 1}, {}, "esri_dimension_contract_drift:hasZ"),
            ({"paths": [[[1, 2], [3, 4]]], "hasM": True}, {"has_m": False},
             "esri_dimension_contract_drift:hasM"),
            ({"paths": [[[1, 2], [3, 4]]]}, {"has_z": True}, "esri_ordinate_contract_drift"),
            ({"paths": [[[1, 2], [3, float("inf")]]]}, {}, "esri_ordinate_not_finite_numeric"),
            ({"paths": [[[1, 2], [3, 2**60 + 1]]]}, {}, "esri_ordinate_precision_loss"),
            ({"paths": []}, {}, "invalid_esri_polyline_paths"),
            ({"paths": [[[1, 2]]]}, {}, "invalid_esri_polyline_path"),
            ({"paths": [("a", "b")]}, {}, "invalid_esri_polyline_path"),
            ([], {}, "invalid_esri_geometry"),
        ]
        for geometry, options, reason in cases:
            with self.subTest(geometry=geometry, reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    src.esri_polyline_wkb(geometry, **options)

    def test_null_geometry_preserves_source_ordinal(self):
        self.assertIsNone(src.esri_polyline_wkb(None))


if __name__ == "__main__":
    unittest.main()
