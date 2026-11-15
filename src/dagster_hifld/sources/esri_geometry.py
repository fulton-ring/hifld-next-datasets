"""Shared Esri source spatial-reference primitives.

Canonical CRS-drift guard for ArcGIS service payloads. The error codes
(``<label>_crs_drift``) are part of the contract with downstream checks and
must not change; the function body is byte-identical to the copies it
replaces.
"""
from __future__ import annotations

from pyproj import CRS

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
