"""Shared native Esri polygon conversion and spatial-reference primitives.

Canonical CRS-drift guard and ArcGIS ring contract for service payloads. The
error codes (``<label>_crs_drift`` and ``source_*``) are part of the contract
with downstream checks and must not change; the function bodies are
byte-identical to the copies they replace.
"""
from __future__ import annotations

import json
import math
from collections import Counter

import pyogrio
from pyproj import CRS
from shapely import from_wkb
from shapely.geometry import LinearRing, Polygon


SOURCE_CRS = CRS.from_epsg(4326)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    """Require every advertised ArcGIS CRS representation to agree with EPSG:4326."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _ring_coordinates(ring: list) -> tuple:
    """Fingerprint a closed coordinate sequence independent of start and winding.

    This compares coordinates only; GDAL owns ring orientation/hole assignment.
    Null M is represented by the WKB NaN measure, never an invented number.
    """
    points = tuple(tuple("null" if v is None or math.isnan(v) else float(v).hex()
                         for v in point) for point in ring[:-1])
    # Usually only one minimum vertex; repeated minima are compared explicitly.
    minimum = min(points)
    return min(sequence[i:] + sequence[:i]
               for sequence in (points, points[::-1])
               for i, point in enumerate(sequence) if point == minimum)


def to_wkb(esri_geom: object, geometry_type: str, *,
           has_z: bool | None = None, has_m: bool | None = None) -> bytes | None:
    """Read native straight Esri polygon rings through GDAL's ESRIJSON driver.

    No CRS is assumed or changed. Layer hasZ/hasM must be supplied for extra
    ordinates unless the geometry carries these flags. Missing flags mean XY,
    never an inferred Z. Actual curves, unclosed rings, unknown dimensions and
    any GDAL coordinate loss are refused. OGC invalidity does not reject or
    repair a representable producer polygon; callers report it diagnostically.

    The returned Arrow WKB is used directly, preserving M (including NaN null
    measures) without a GeoDataFrame/Shapely serialization round trip. This
    first shared consumer supports polygons only; other types refuse explicitly.
    """
    if geometry_type != "esriGeometryPolygon":
        raise ValueError("unsupported_esri_geometry_type:" + str(geometry_type))
    if esri_geom is None:
        return None
    if not isinstance(esri_geom, dict):
        raise ValueError("invalid_esri_geometry")
    if "curveRings" in esri_geom or "curvePaths" in esri_geom:
        raise ValueError("unsupported_esri_curves")
    if set(esri_geom) - {"rings", "hasZ", "hasM", "spatialReference"}:
        raise ValueError("unexpected_esri_geometry_members")
    flags = []
    for key, supplied in (("hasZ", has_z), ("hasM", has_m)):
        advertised = esri_geom.get(key)
        if ((key in esri_geom and type(advertised) is not bool)
                or (supplied is not None and type(supplied) is not bool)
                or (supplied is not None and key in esri_geom and supplied != advertised)):
            raise ValueError("esri_dimension_contract_drift:" + key)
        flags.append(supplied if supplied is not None else bool(advertised))
    z, m = flags
    width = 2 + z + m
    rings = esri_geom.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("invalid_esri_polygon_rings")
    native_rings = []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError("invalid_esri_polygon_ring")
        native_ring = []
        for point in ring:
            if not isinstance(point, (list, tuple)) or len(point) != width:
                raise ValueError("esri_ordinate_contract_drift")
            values = []
            for index, value in enumerate(point):
                if m and index == width - 1 and (value is None or
                        (type(value) is float and math.isnan(value))):
                    values.append(float("nan"))
                elif type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError("esri_ordinate_not_finite_numeric")
                elif type(value) is int and int(float(value)) != value:
                    raise ValueError("esri_ordinate_precision_loss")
                else:
                    values.append(value)
            native_ring.append(values)
        # Compare normalized values so null M also closes correctly.
        if ["null" if isinstance(v, float) and math.isnan(v) else v for v in native_ring[0]] != [
                "null" if isinstance(v, float) and math.isnan(v) else v for v in native_ring[-1]]:
            raise ValueError("esri_polygon_ring_not_closed")
        native_rings.append(native_ring)
    geometry = {**esri_geom, "hasZ": z, "hasM": m, "rings": native_rings}
    document = {"geometryType": geometry_type, "hasZ": z, "hasM": m,
                "features": [{"attributes": {}, "geometry": geometry}]}
    # GDAL accepts numeric NaN for absent measures; JSON null otherwise causes
    # ESRIJSON to drop the entire geometry. The retained source is never edited.
    _metadata, table = pyogrio.read_arrow(json.dumps(document).encode())
    if table.num_rows != 1 or table.num_columns != 1:
        raise ValueError("esri_conversion_row_count_drift")
    wkb = table.column(0)[0].as_py()
    if wkb is None:
        raise ValueError("esri_conversion_geometry_lost")
    decoded = from_wkb(wkb)
    if decoded.geom_type not in ("Polygon", "MultiPolygon") or (decoded.has_z, decoded.has_m) != (z, m):
        raise ValueError("esri_conversion_dimension_or_type_drift")
    parts = decoded.geoms if decoded.geom_type == "MultiPolygon" else [decoded]
    output_rings = [list(r.coords) for p in parts for r in [p.exterior, *p.interiors]]
    if Counter(map(_ring_coordinates, native_rings)) != Counter(map(_ring_coordinates, output_rings)):
        raise ValueError("esri_conversion_coordinates_changed")
    return wkb


def esri_polygon_wkb(geometry: dict, *, allow_zm: bool = True) -> bytes:
    """Validate ArcGIS polygon rings and return their WKB.

    ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    Reject non-standard winding rather than repairing or guessing. Holes must
    fall inside exactly one shell. When ``allow_zm`` is false, correctly flagged
    Z/M geometry is source drift and fails closed. Serialization delegates to
    ``to_wkb`` so GDAL owns the byte encoding.
    """
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    if not allow_zm and (has_z or has_m):
        raise ValueError("source_polygon_dimensional_flags_rejected")
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
        ring = LinearRing([point[:2] for point in coords])
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
    ordered = [ring for i, shell in enumerate(shells) for ring in (shell, *assignments[i])]
    return to_wkb({"rings": ordered, "hasZ": has_z, "hasM": has_m}, "esriGeometryPolygon")
