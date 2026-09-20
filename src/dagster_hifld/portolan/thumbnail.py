"""Render bounded, data-derived GeoParquet thumbnails for STAC assets."""

from __future__ import annotations

from collections.abc import Callable
from io import BytesIO
from math import isfinite
from pathlib import Path

import pyarrow.parquet as pa_parquet
from PIL import Image, ImageDraw
from shapely import from_wkb, total_bounds
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry

_CANVAS_SIZE = 512
_MARGIN = 28
_MAX_FEATURES = 2_000
_BATCH_SIZE = 256

Project = Callable[[float, float], tuple[float, float]]


def render_geoparquet_thumbnail(
    version_dir: Path,
    geometry_column: str,
) -> bytes | None:
    """Return a PNG sampled from GeoParquet geometry, or ``None`` when empty.

    The publisher intentionally samples a bounded number of non-empty geometries.
    A thumbnail is a visual discovery aid, not an analytical representation, and
    this keeps publication practical for multi-million-feature datasets.
    """
    geometries: list[BaseGeometry] = []
    paths = sorted((version_dir / "geoparquet").rglob("*.parquet"))
    for path in paths:
        parquet = pa_parquet.ParquetFile(path)
        for batch in parquet.iter_batches(
            batch_size=_BATCH_SIZE,
            columns=[geometry_column],
        ):
            values = [
                value
                for value in batch.column(0).to_pylist()
                if isinstance(value, bytes)
            ]
            for geometry in from_wkb(values):
                if not geometry.is_empty:
                    geometries.append(geometry)
                if len(geometries) >= _MAX_FEATURES:
                    return _render(geometries)
    return _render(geometries) if geometries else None


def _render(geometries: list[BaseGeometry]) -> bytes | None:
    bounds = total_bounds(geometries)
    if len(bounds) != 4 or not all(isfinite(float(value)) for value in bounds):
        return None
    minimum_x, minimum_y, maximum_x, maximum_y = (float(value) for value in bounds)
    span_x = max(maximum_x - minimum_x, 1.0)
    span_y = max(maximum_y - minimum_y, 1.0)
    drawable = _CANVAS_SIZE - 2 * _MARGIN
    scale = min(drawable / span_x, drawable / span_y)
    offset_x = (_CANVAS_SIZE - scale * (maximum_x - minimum_x)) / 2
    offset_y = (_CANVAS_SIZE - scale * (maximum_y - minimum_y)) / 2

    def project(x: float, y: float) -> tuple[float, float]:
        return (
            offset_x + (x - minimum_x) * scale,
            _CANVAS_SIZE - (offset_y + (y - minimum_y) * scale),
        )

    image = Image.new("RGB", (_CANVAS_SIZE, _CANVAS_SIZE), "#f8fafc")
    draw = ImageDraw.Draw(image, "RGBA")
    draw.rectangle(
        (_MARGIN, _MARGIN, _CANVAS_SIZE - _MARGIN, _CANVAS_SIZE - _MARGIN),
        outline="#cbd5e1",
        width=1,
    )
    for geometry in geometries:
        _draw_geometry(draw, geometry, project)
    destination = BytesIO()
    image.save(destination, format="PNG", optimize=True)
    return destination.getvalue()


def _coordinates(
    geometry: LineString,
    project: Project,
) -> list[tuple[float, float]]:
    return [project(float(x), float(y)) for x, y in geometry.coords]


def _draw_geometry(
    draw: ImageDraw.ImageDraw,
    geometry: BaseGeometry,
    project: Project,
) -> None:
    if isinstance(geometry, Point):
        x, y = project(float(geometry.x), float(geometry.y))
        draw.ellipse((x - 2, y - 2, x + 2, y + 2), fill="#2563eb")
    elif isinstance(geometry, MultiPoint):
        for point in geometry.geoms:
            _draw_geometry(draw, point, project)
    elif isinstance(geometry, LineString):
        coordinates = _coordinates(geometry, project)
        if len(coordinates) > 1:
            draw.line(coordinates, fill="#0f766e", width=2)
    elif isinstance(geometry, MultiLineString):
        for line in geometry.geoms:
            _draw_geometry(draw, line, project)
    elif isinstance(geometry, Polygon):
        exterior = [project(float(x), float(y)) for x, y in geometry.exterior.coords]
        if len(exterior) > 2:
            draw.polygon(exterior, fill="#2563eb55", outline="#1d4ed8")
        for interior in geometry.interiors:
            ring = [project(float(x), float(y)) for x, y in interior.coords]
            if len(ring) > 2:
                draw.polygon(ring, fill="#f8fafc")
    elif isinstance(geometry, MultiPolygon):
        for polygon in geometry.geoms:
            _draw_geometry(draw, polygon, project)
    elif isinstance(geometry, GeometryCollection):
        for child in geometry.geoms:
            _draw_geometry(draw, child, project)
