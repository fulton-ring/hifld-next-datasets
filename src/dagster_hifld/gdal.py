"""GDAL runtime configuration helpers."""

from __future__ import annotations

from contextlib import contextmanager
from threading import RLock

import fiona
import pyogrio


@contextmanager
def with_large_geojson_support():
    """Disable GDAL's GeoJSON object-size limit for Fiona and pyogrio reads."""
    pyogrio = None
    previous_value = None
    try:
        import pyogrio as pyogrio_module

        pyogrio = pyogrio_module
        previous_value = pyogrio.get_gdal_config_option("OGR_GEOJSON_MAX_OBJ_SIZE")
        pyogrio.set_gdal_config_options({"OGR_GEOJSON_MAX_OBJ_SIZE": "0"})
    except Exception:
        pyogrio = None

    try:
        with fiona.Env(OGR_GEOJSON_MAX_OBJ_SIZE="0"):
            yield
    finally:
        if pyogrio is not None:
            pyogrio.set_gdal_config_options({"OGR_GEOJSON_MAX_OBJ_SIZE": previous_value})


_GPKG_CREATION_LOCK = RLock()


@contextmanager
def with_geopackage_creation_time(timestamp: str):
    """Pin GDAL creation time so SQLite transaction headers are reproducible.

    Updating last_change after creation cannot undo variable GDAL commits.
    See https://gdal.org/en/stable/drivers/vector/gpkg.html#creation-issues.
    """
    with _GPKG_CREATION_LOCK:
        previous = pyogrio.get_gdal_config_option("OGR_CURRENT_DATE")
        try:
            pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": timestamp})
            with fiona.Env(OGR_CURRENT_DATE=timestamp):
                yield
        finally:
            pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": previous})
