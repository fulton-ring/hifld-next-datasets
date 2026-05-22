"""GDAL runtime configuration helpers."""

from __future__ import annotations

from contextlib import contextmanager

import fiona


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
