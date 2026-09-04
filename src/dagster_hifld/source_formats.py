"""Canonical source-format definitions and legacy source discovery."""

from __future__ import annotations

from pathlib import Path

import fiona

CANONICAL_SOURCE_FORMAT_PRECEDENCE = (
    "geopackage",
    "file_geodatabase",
    "shapefile",
    "geojson",
)
CANONICAL_SOURCE_FORMAT_DIRS = frozenset(CANONICAL_SOURCE_FORMAT_PRECEDENCE)
DERIVED_OUTPUT_FORMAT_DIRS = frozenset({"geoparquet", "pmtiles"})

SOURCE_FORMAT_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "geopackage": (".gpkg",),
    "file_geodatabase": (".gdb",),
    "shapefile": (".shp",),
    "geojson": (".geojson", ".json"),
}

_REQUIRED_SHAPEFILE_SUFFIXES = frozenset({".shp", ".shx", ".dbf"})


def discover_legacy_unknown_shapefile(unknown_dir: Path) -> Path | None:
    """Return the sole complete, readable legacy Shapefile under ``unknown_dir``.

    Missing or empty legacy directories have no source. Any non-empty legacy
    directory that cannot be identified unambiguously is invalid.
    """
    if not unknown_dir.is_dir():
        return None

    files = sorted(path for path in unknown_dir.rglob("*") if path.is_file())
    if not files:
        return None

    shapefiles = [path for path in files if path.suffix.lower() == ".shp"]
    if len(shapefiles) > 1:
        raise ValueError(
            f"Legacy {unknown_dir} contains multiple Shapefile datasets; "
            "move one complete dataset to shapefile/."
        )
    if not shapefiles:
        raise ValueError(
            f"Legacy {unknown_dir} does not contain one complete readable Shapefile."
        )

    shapefile = shapefiles[0]
    sibling_suffixes = {
        path.suffix.lower()
        for path in shapefile.parent.iterdir()
        if path.is_file() and path.stem.casefold() == shapefile.stem.casefold()
    }
    if not _REQUIRED_SHAPEFILE_SUFFIXES <= sibling_suffixes:
        raise ValueError(
            f"Legacy {unknown_dir} does not contain one complete readable Shapefile: "
            f"{shapefile.name} requires .shp, .shx, and .dbf sidecars."
        )

    try:
        with fiona.open(shapefile):
            pass
    except Exception as exc:
        raise ValueError(
            f"Legacy {unknown_dir} does not contain one complete readable Shapefile: "
            f"{shapefile.name} could not be opened."
        ) from exc
    return shapefile
