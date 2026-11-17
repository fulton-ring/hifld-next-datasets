"""Native Esri geometry conversion must preserve source coordinates, including M."""
import math
import unittest
from collections import Counter
from unittest.mock import patch

import pyarrow as pa

from shapely import from_wkb, get_coordinates

from dagster_hifld.sources import esri_geometry as src


def square(x=0, y=0, size=10):
    return [[x, y], [x, y + size], [x + size, y + size], [x + size, y], [x, y]]


def ring_key(ring):
    # Independent, orientation/start-independent source-coordinate comparison.
    values = tuple(tuple('null' if v is None or math.isnan(v) else float(v).hex()
                         for v in point) for point in ring[:-1])
    return min(sequence[i:] + sequence[:i]
               for sequence in (values, values[::-1]) for i in range(len(values)))


class EsriGeometryConversionTests(unittest.TestCase):
    def assert_rings(self, rings, result):
        parts = result.geoms if result.geom_type == 'MultiPolygon' else [result]
        actual = [list(r.coords) for p in parts for r in [p.exterior, *p.interiors]]
        self.assertEqual(Counter(ring_key(r) for r in actual),
                         Counter(ring_key(r) for r in rings))

    def test_multiple_exteriors_holes_and_island_from_failure_families(self):
        rings = [square(), list(reversed(square(1, 1, 7))), square(2, 2, 1), square(20)]
        result = from_wkb(src.to_wkb({'rings': rings}, 'esriGeometryPolygon'))
        self.assertEqual(result.geom_type, 'MultiPolygon')
        self.assertEqual(len(result.geoms), 3)
        self.assertEqual(sum(len(p.interiors) for p in result.geoms), 1)
        self.assert_rings(rings, result)

    def test_touching_parts_and_tiny_projected_coordinates_are_unchanged(self):
        for rings in ([square(), square(10, 10)],
                      [square(12345678.123456789, 7654321.987654321, 1e-6)]):
            with self.subTest(rings=rings):
                result = from_wkb(src.to_wkb({'rings': rings, 'spatialReference': {'wkid': 3857}},
                                            'esriGeometryPolygon'))
                self.assert_rings(rings, result)

    def test_z_m_zm_and_null_measures_are_distinct_and_preserved(self):
        for z, m in ((True, False), (False, True), (True, True)):
            for null_m in (False, True, "all") if m else (False,):
                rings = [[v + ([i + .125] if z else []) +
                          ([None if null_m and (null_m == "all" or i in (0, 4)) else i + .375] if m else [])
                          for i, v in enumerate(square())]]
                # Closing ordinates must also match, including the null measure.
                rings[0][-1] = rings[0][0][:]
                with self.subTest(z=z, m=m, null_m=null_m):
                    result = from_wkb(src.to_wkb({'rings': rings}, 'esriGeometryPolygon',
                                                has_z=z, has_m=m))
                    self.assertEqual((result.has_z, result.has_m), (z, m))
                    self.assert_rings(rings, result)
                    self.assertEqual(get_coordinates(result, include_z=z, include_m=m).shape[1],
                                     2 + z + m)

    def test_ogc_invalid_source_rings_are_retained_without_repair(self):
        cases = [[[[0, 0], [3, 4], [1, 4], [4, 0], [0, 0]]],
                 [[[0, 0], [0, 0], [1, 1], [1, 1], [0, 0]]],
                 [square(), list(reversed(square(5, 5, 10)))]]
        for rings in cases:
            with self.subTest(rings=rings):
                result = from_wkb(src.to_wkb({'rings': rings}, 'esriGeometryPolygon'))
                self.assertFalse(result.is_valid)
                self.assert_rings(rings, result)

    def test_ambiguous_dimensions_actual_curves_and_unclosed_rings_refuse(self):
        cases = [({'rings': [[v + [4] for v in square()]]}, {}),
                 ({'curveRings': [square()]}, {}),
                 ({'rings': [square()[:-1]]}, {}),
                 ({'rings': [square()], 'hasZ': True}, {'has_z': False}),
                 ({'rings': [[v + [3, 4] for v in square()]]}, {'has_z': True}),
                 ({'rings': [[[float('inf'), 0]] * 4]}, {})]
        for geometry, options in cases:
            with self.subTest(geometry=geometry):
                with self.assertRaises(ValueError):
                    src.to_wkb(geometry, 'esriGeometryPolygon', **options)

    def test_null_geometry_preserves_source_ordinal(self):
        geometries = [{'rings': [square()]}, None, {'rings': [square(20)]}]
        result = [src.to_wkb(g, 'esriGeometryPolygon') for g in geometries]
        self.assertIsNone(result[1])
        self.assertEqual(from_wkb(result[0]).bounds, (0, 0, 10, 10))
        self.assertEqual(from_wkb(result[2]).bounds, (20, 0, 30, 10))

    def test_gdal_coordinate_dimension_and_row_loss_are_refused(self):
        geometry = {'rings': [square()]}
        other = src.to_wkb({'rings': [square(100)]}, 'esriGeometryPolygon')
        z_only = src.to_wkb({'rings': [[v + [3] for v in square()]]},
                            'esriGeometryPolygon', has_z=True)
        for body, reason in (([other], 'coordinates_changed'),
                             ([z_only], 'dimension_or_type_drift'),
                             ([None], 'geometry_lost'),
                             ([], 'row_count_drift'),
                             ([other, other], 'row_count_drift')):
            with self.subTest(reason=reason):
                table = pa.table({'wkb_geometry': pa.array(body, type=pa.binary())})
                with patch.object(src.pyogrio, 'read_arrow', return_value=({}, table)):
                    with self.assertRaisesRegex(ValueError, reason):
                        src.to_wkb(geometry, 'esriGeometryPolygon')

    def test_unsupported_geometry_and_inexact_integer_refuse(self):
        with self.assertRaisesRegex(ValueError, 'unsupported_esri_geometry_type'):
            src.to_wkb({'x': 1, 'y': 2}, 'esriGeometryPoint')
        with self.assertRaisesRegex(ValueError, 'precision_loss'):
            src.to_wkb({'rings': [square(2**60 + 1)]}, 'esriGeometryPolygon')
