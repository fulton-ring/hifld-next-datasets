"""The shared ArcGIS polygon encoder must match the copies it replaces byte for byte.

Families A, B and C below are the three hand-written encoders taken from
``main`` before they were consolidated into
``esri_geometry.esri_polygon_wkb``. They stay inline so the shared encoder is
checked against the real prior behavior, not a rewritten expectation.
"""
import struct
import unittest

from shapely import from_wkb
from shapely.geometry import LinearRing, Polygon

from dagster_hifld.sources.esri_geometry import esri_polygon_wkb


def family_a(geometry: dict) -> bytes:
    """XY/Z/M-aware encoder that checks for a missing shell after hole assignment."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    shells, holes = [], []
    dimension = None
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        point_dims = next(iter(dims))
        if dimension is None:
            dimension = point_dims
        if dimension != point_dims:
            raise ValueError("source_polygon_ordinate_count_drift")
        expected_dims = 2 + int(has_z) + int(has_m)
        if point_dims != expected_dims:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        xy = [point[:2] for point in coords]
        ring = LinearRing(xy)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    dim_code = {2: 0, 3: (1000 if has_z else 2000), 4: 3000}[dimension]

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, dim_code + 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<" + "d" * dimension, *point))
        return bytes(out)

    polygons = [polygon_wkb(shell, assignments[i]) for i, shell in enumerate(shells)]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, dim_code + 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def family_b(geometry: dict) -> bytes:
    """XY/Z/M-aware encoder that checks for a missing shell before hole assignment."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    shells, holes = [], []
    dimension = None
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        point_dims = next(iter(dims))
        if dimension is None:
            dimension = point_dims
        if dimension != point_dims:
            raise ValueError("source_polygon_ordinate_count_drift")
        expected_dims = 2 + int(has_z) + int(has_m)
        if point_dims != expected_dims:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        xy = [point[:2] for point in coords]
        ring = LinearRing(xy)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)
    dim_code = {2: 0, 3: (1000 if has_z else 2000), 4: 3000}[dimension]

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, dim_code + 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<" + "d" * dimension, *point))
        return bytes(out)

    polygons = [polygon_wkb(shell, assignments[i]) for i, shell in enumerate(shells)]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, dim_code + 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def family_c(geometry: dict) -> bytes:
    """XY-only encoder that rejects any flagged Z/M geometry up front."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    if has_z or has_m:
        raise ValueError("source_polygon_dimensional_flags_rejected")
    shells, holes = [], []
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        if next(iter(dims)) != 2:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        ring = LinearRing(coords)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<dd", *point))
        return bytes(out)

    polygons = [polygon_wkb(shells[i], assignments[i]) for i in range(len(shells))]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def square(x=0.0, y=0.0, size=10.0):
    return [[x, y], [x, y + size], [x + size, y + size], [x + size, y], [x, y]]


def inset_hole(shell, inset=1.0):
    xs = [point[0] for point in shell]
    ys = [point[1] for point in shell]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    return [[x0 + inset, y0 + inset], [x0 + inset, y1 - inset],
            [x1 - inset, y1 - inset], [x1 - inset, y0 + inset], [x0 + inset, y0 + inset]]


class EsriPolygonWkbTests(unittest.TestCase):
    def assert_matches_families(self, geometry):
        expected = family_b(geometry)
        self.assertEqual(family_a(geometry), expected)
        self.assertEqual(esri_polygon_wkb(geometry), expected)
        if not geometry.get("hasZ", False) and not geometry.get("hasM", False):
            self.assertEqual(family_c(geometry), expected)
            self.assertEqual(esri_polygon_wkb(geometry, allow_zm=False), expected)

    def test_single_xy_ring(self):
        geometry = {"rings": [square()]}
        self.assert_matches_families(geometry)
        self.assertEqual(
            esri_polygon_wkb(geometry),
            bytes.fromhex(
                "0103000000010000000500000000000000000000000000000000000000000000"
                "0000000000000000000000244000000000000024400000000000002440000000"
                "0000002440000000000000000000000000000000000000000000000000"))

    def test_ring_with_hole(self):
        geometry = {"rings": [square(), list(reversed(inset_hole(square())))]}
        self.assert_matches_families(geometry)
        decoded = from_wkb(esri_polygon_wkb(geometry))
        self.assertEqual(decoded.geom_type, "Polygon")
        self.assertEqual(len(decoded.interiors), 1)

    def test_two_shells_form_multipolygon(self):
        geometry = {"rings": [square(), square(20.0)]}
        self.assert_matches_families(geometry)
        self.assertEqual(from_wkb(esri_polygon_wkb(geometry)).geom_type, "MultiPolygon")

    def test_z_m_and_zm_are_preserved(self):
        for has_z, has_m in ((True, False), (False, True), (True, True)):
            width = 2 + int(has_z) + int(has_m)
            ring = [point + [float(index + 1) for index in range(width - 2)]
                    for point in square()]
            geometry = {"rings": [ring], "hasZ": has_z, "hasM": has_m}
            with self.subTest(has_z=has_z, has_m=has_m):
                self.assert_matches_families(geometry)
                decoded = from_wkb(esri_polygon_wkb(geometry))
                self.assertEqual((decoded.has_z, decoded.has_m), (has_z, has_m))
                with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                    esri_polygon_wkb(geometry, allow_zm=False)

    def test_open_ring_is_rejected(self):
        geometry = {"rings": [square()[:-1]]}
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_not_closed"):
            esri_polygon_wkb(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_not_closed"):
            family_a(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_not_closed"):
            family_b(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_ring_not_closed"):
            family_c(geometry)

    def test_mixed_ordinate_counts_are_rejected(self):
        geometry = {"rings": [[[0.0, 0.0], [0.0, 1.0, 2.0], [1.0, 1.0], [0.0, 0.0]]]}
        with self.assertRaisesRegex(ValueError, "source_polygon_ordinate_count_drift"):
            esri_polygon_wkb(geometry)

    def test_flags_that_do_not_match_coordinates_are_rejected(self):
        geometry = {"rings": [square()], "hasZ": True}
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            esri_polygon_wkb(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
            esri_polygon_wkb(geometry, allow_zm=False)

    def test_holes_only_fails_closed(self):
        geometry = {"rings": [list(reversed(inset_hole(square())))]}
        with self.assertRaisesRegex(ValueError, "source_polygon_shell_missing"):
            esri_polygon_wkb(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_shell_missing"):
            family_b(geometry)
        with self.assertRaisesRegex(ValueError, "source_polygon_shell_missing"):
            family_c(geometry)
        # Family A checked for a shell after hole assignment and raised a
        # different code; the shared encoder intentionally adopts family B.
        with self.assertRaisesRegex(ValueError, "source_hole_ownership_ambiguous"):
            family_a(geometry)

    def test_hole_outside_any_shell_is_rejected(self):
        geometry = {"rings": [square(), list(reversed(inset_hole(square(100.0))))]}
        with self.assertRaisesRegex(ValueError, "source_hole_ownership_ambiguous"):
            esri_polygon_wkb(geometry)
        with self.assertRaisesRegex(ValueError, "source_hole_ownership_ambiguous"):
            family_a(geometry)
        with self.assertRaisesRegex(ValueError, "source_hole_ownership_ambiguous"):
            family_b(geometry)
        with self.assertRaisesRegex(ValueError, "source_hole_ownership_ambiguous"):
            family_c(geometry)


if __name__ == "__main__":
    unittest.main()
