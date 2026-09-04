"""Canonical source-format definitions and legacy source discovery."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Iterable

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
SHAPEFILE_DATASET_SUFFIXES = frozenset(
    {
        ".aih",
        ".ain",
        ".atx",
        ".cpg",
        ".dbf",
        ".fbn",
        ".fbx",
        ".fix",
        ".ixs",
        ".mxs",
        ".prj",
        ".qix",
        ".qpj",
        ".sbn",
        ".sbx",
        ".shp",
        ".shp.xml",
        ".shx",
    }
)


def _is_shapefile_dataset_filename(filename: str, stem: str) -> bool:
    normalized_name = filename.casefold()
    normalized_stem = stem.casefold()
    if normalized_name.startswith(f"{normalized_stem}.") and normalized_name.endswith(".atx"):
        return True
    return any(
        normalized_name == f"{normalized_stem}{suffix}"
        for suffix in SHAPEFILE_DATASET_SUFFIXES
    )


def shapefile_dataset_files(shapefile: Path) -> list[Path]:
    """Return a Shapefile and every same-basename sidecar beside it."""
    return sorted(
        path
        for path in shapefile.parent.iterdir()
        if path.is_file()
        and _is_shapefile_dataset_filename(path.name, shapefile.stem)
    )


def discover_canonical_source_file(
    format_dir: Path,
    format_name: str,
) -> Path | None:
    """Return one recursively discovered canonical single-file source."""
    extensions = SOURCE_FORMAT_EXTENSIONS[format_name]
    if not format_dir.is_dir():
        return None
    candidates = sorted(
        path
        for path in format_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in extensions
    )
    if len(candidates) > 1:
        raise ValueError(
            f"Found multiple canonical {format_name} sources under {format_dir}."
        )
    return candidates[0] if candidates else None


def discover_legacy_unknown_shapefile(unknown_dir: Path) -> Path | None:
    """Return the sole complete, readable legacy Shapefile under ``unknown_dir``.

    Missing directories and directories without a Shapefile candidate have no
    geospatial source. Shapefile candidates must be unambiguous and valid.
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
        return None

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


def discover_legacy_unknown_shapefile_keys(keys: Iterable[str]) -> tuple[str, ...]:
    """Validate one legacy Shapefile from object names and return all its sidecars."""
    object_keys = sorted(keys)
    shapefile_keys = [
        key for key in object_keys if PurePosixPath(key).suffix.lower() == ".shp"
    ]
    if len(shapefile_keys) > 1:
        raise ValueError("Legacy unknown/ contains multiple Shapefile datasets.")
    if not shapefile_keys:
        return ()

    shapefile_key = shapefile_keys[0]
    shapefile_path = PurePosixPath(shapefile_key)
    dataset_keys = tuple(
        key
        for key in object_keys
        if PurePosixPath(key).parent == shapefile_path.parent
        and _is_shapefile_dataset_filename(
            PurePosixPath(key).name,
            shapefile_path.stem,
        )
    )
    suffixes = {PurePosixPath(key).suffix.lower() for key in dataset_keys}
    if not _REQUIRED_SHAPEFILE_SUFFIXES <= suffixes:
        raise ValueError(
            "Legacy unknown/ does not contain one complete Shapefile: "
            f"{shapefile_path.name} requires .shp, .shx, and .dbf sidecars."
        )
    return dataset_keys
