"""Shared acquisition/replay machinery for the USGS WBD hydrologic-unit layers.

The USGS hydro.nationalmap.gov ``wbd`` MapServer publishes one polygon layer per
hydrologic-unit level (layer 1 "2-digit HU (Region)", layer 4 "8-digit HU
(Subbasin)", layer 6 "12-digit HU (Subwatershed)", ...). The level-specific
source modules captured through the native web-mercator keyset window policy
(``wbd_2_digit_hu`` and ``wbd_12_digit_hu``) carried byte-identical copies of
the service validation, refused-window capture, streaming replay and Esri
polygon assembly primitives (audited 2026-11-15 at issue #924), so this module
owns one parameterized implementation of those primitives and each level module
stays a thin adapter that supplies its own layer identity, HUC code width,
schema and baseline pins, page/capture budgets and evidence text.

Two acquisition policies coexist deliberately:

- The keyset window policy (``OBJECTID``-keyed ``where`` windows, refused
  windows bisected from the pinned roster) is shared here.
  ``max_bisection_depth``, ``single_refusal_retries`` and ``attrs_parts``
  parameterize the one-level 2-digit bisection versus the 12-digit recursive
  bisection with bounded single-feature retries and the attributes-only
  fallback part for permanently unservable rows.
- The offset-segment checkpoint policy of ``wbd_hu8_subbasin`` (offset-paged
  segments tiling an enumerated roster, resumable ``capture_state.json``,
  cross-checked attributes-only extraction, refreshed closing brackets,
  producer-error retry/backoff, server-reprojected EPSG:4326 rings validated
  and repaired through ``levee_areas_usace_ienc._esri_polygon_wkb``) is a
  distinct policy that this module does not absorb; the HU8 module keeps its
  own capture flow and reuses only the small primitives that are byte
  equivalent (query URL building, producer error detection, per-field value
  validation).

Every candidate stays unapproved: the level modules keep the runtime approval
flags false and the draft schedules stopped.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely import from_wkb, is_valid_reason
from shapely.geometry import LinearRing, MultiPolygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.esri_geometry import to_wkb, zero_area_rings
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, validate_capture_request_identity,
    verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.arcgis_layer import (
    _canonical, _capture_identity, _load_capture, _sha,
)

METADATA_NAMES = ("service_doc", "service_before", "layer_before", "count_before",
                  "ids_before", "count_after", "ids_after", "layer_after", "service_after")
DIGEST_SEPARATOR = b"\x00"

# Polygon-ring quarantine modes retired by the shared GDAL Esri polygon
# converter (issue #997); only the producer-refused attributes-only window
# mode remains a real gate.
REFUSED_GEOMETRY_GATE_MODE = "source_page_window_refused_geometry"

PARTS_POLICY_SINGLE_OR_AB = "single_or_ab"
PARTS_POLICY_BISECT_ATTRS = "bisect_attrs"


@dataclass(frozen=True)
class WbdHuSpec:
    """The level-specific pins and policies one WBD layer adapter supplies."""

    dataset_slug: str
    file_slug: str
    publisher: str
    agency: str
    service_doc_url: str
    wbd_program_doc_url: str
    dhs_crosswalk_evidence_url: str
    service_url: str
    layer_id: int
    layer_name: str
    map_name: str
    expected_count: int
    expected_id_min: int
    expected_id_max: int
    expected_id_sha256: str
    page_size: int
    page_count: int
    declared_max_record_count: int
    expected_extent: Mapping[str, float]
    expected_extent_tolerance: float
    expected_copyright_text: str
    expected_service_layers: tuple
    native_wkid: int
    native_latest_wkid: int
    source_crs_wkid: int
    display_field_name: str
    user_agent: str
    doc_user_agent: str
    page_max_bytes: int
    doc_max_bytes: int
    capture_max_bytes: int
    disk_floor_bytes: int
    shape_parity_tolerance: float
    expected_schema_sha256: str
    expected_record_sha256: str
    expected_geometry_sha256: str
    expected_null_geometry_count: int | None
    expected_unusable_geometry_counts: Mapping[str, int] | None
    expected_refused_geometry_oids: tuple | None
    attribute_fields: tuple
    expected_layer_fields: list
    gate_modes: frozenset
    parts_policy: str
    max_bisection_depth: int
    single_refusal_retries: int
    single_refusal_retry_seconds: float
    attrs_parts: bool
    digest_geometry_before_gate: bool
    capture_root_family: str
    baseline_pending_note: str | None
    audit_page_refusal_count: bool
    coverage_scope: str
    baseline_digest_scheme: str
    manifest_title: str
    manifest_description: str
    transformation_source_fields: str
    transformation_source_ids: str
    transformation_raw_evidence: str
    lineage: str
    policy_reason: str

    @property
    def layer_url(self) -> str:
        return f"{self.service_url}/{self.layer_id}"


def query_url(layer_url: str, params: dict[str, object]) -> str:
    return layer_url + "/query?" + urlencode(params)


def count_url(spec: WbdHuSpec) -> str:
    return query_url(spec.layer_url, {"where": "1=1", "returnCountOnly": "true", "f": "json"})


def ids_url(spec: WbdHuSpec) -> str:
    return query_url(spec.layer_url, {"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def page_window_url(spec: WbdHuSpec, last_objectid: int, record_count: int) -> str:
    return query_url(spec.layer_url, {
        "where": f"objectid>{last_objectid}", "outFields": "*",
        "returnGeometry": "true", "returnZ": "true", "returnM": "true",
        "orderByFields": "objectid ASC",
        "resultRecordCount": str(record_count), "f": "json"})


def page_url(spec: WbdHuSpec, last_objectid: int) -> str:
    return page_window_url(spec, last_objectid, spec.page_size)


def capture_roster(spec: WbdHuSpec, roster: list[int]) -> list[tuple[str, str]]:
    """Exact GET URL bound to every capture slot; page slots are derived from
    the sorted roster so a replay can re-derive each request without extra
    state: page N refetches ``objectid>{last id of page N-1}``."""
    urls = [("service_doc", spec.service_doc_url),
            ("service_before", spec.service_url + "?f=pjson"),
            ("layer_before", spec.layer_url + "?f=pjson"),
            ("count_before", count_url(spec)), ("ids_before", ids_url(spec))]
    last = 0
    for index in range(spec.page_count):
        record_count = min(spec.page_size, len(roster) - index * spec.page_size)
        urls.append((f"page_{index:04d}", page_window_url(spec, last, record_count)))
        end = (index + 1) * spec.page_size
        if end < len(roster):
            last = roster[end - 1]
    urls.extend([("count_after", count_url(spec)), ("ids_after", ids_url(spec)),
                 ("layer_after", spec.layer_url + "?f=pjson"),
                 ("service_after", spec.service_url + "?f=pjson")])
    return urls


def capture_budgeted(root: Path, max_bytes: int, capture_max_bytes: int,
                     client: httpx.Client, url: str, name: str) -> tuple[bytes, dict]:
    """One retained GET behind the shared capture budget guard."""
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * max_bytes > capture_max_bytes:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=max_bytes)


def producer_error_document(body: bytes, *, max_bytes: int,
                            require_int_code: bool) -> bool:
    """The producer's deterministic error envelope document.

    Retained as evidence, never validated as a page. The keyset-window layers
    answer refused windows with HTTP 200 Esri error code 500 bodies of at most
    a few KiB; the offset-segment HU8 policy accepts any small ``error``
    envelope and lets the caller decide between bisection, retry and failure.
    """
    if not body.lstrip().startswith(b"{") or len(body) > max_bytes:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    error = document.get("error") if isinstance(document, dict) else None
    if not isinstance(error, dict):
        return False
    return type(error.get("code")) is int if require_int_code else True


def is_refusal(body: bytes) -> bool:
    """The producer's deterministic refused-window document (Esri error code
    500 "Error performing query operation" on windows that would decode beyond
    the serving envelope). Retained as evidence, never validated as a page."""
    return producer_error_document(body, max_bytes=4096, require_int_code=True)


def retain_refusal(root: Path, name: str, body: bytes, receipt: dict) -> tuple[bytes, dict]:
    """Rename a refused window's retained response to its refusal evidence name."""
    for suffix in (".body", ".wire", ".receipt.json"):
        source = root / f"{name}{suffix}"
        target = root / f"{name}_refusal{suffix}"
        if target.exists():
            raise ValueError("source_refusal_evidence_name_collision:" + target.name)
        source.replace(target)
    return body, receipt


def page_part_url(spec: WbdHuSpec, roster: list[int], index: int, part_name: str) -> str:
    """Exact GET URL bound to a bisected page part artifact, re-derived from the
    pinned roster so replay needs no extra trusted state: each ``a``/``b``
    suffix character halves the remaining id slice."""
    sub = roster[index * spec.page_size:(index + 1) * spec.page_size]
    last = roster[index * spec.page_size - 1] if index > 0 else 0
    for tag in part_name[9:]:
        half = (len(sub) + 1) // 2
        if tag == "a":
            sub, last = sub[:half], (sub[0] - 1 if sub[0] > 1 else 0)
        else:
            sub, last = sub[half:], sub[half] - 1
    return page_window_url(spec, last, len(sub))


def record_attrs_url(spec: WbdHuSpec, oid: int) -> str:
    """Attributes-only URL for one refused row; the producer serves attributes
    for records whose geometry response it cannot construct."""
    return query_url(spec.layer_url, {"where": f"objectid={oid}", "outFields": "*",
                                      "returnGeometry": "false",
                                      "resultRecordCount": "1", "f": "json"})


def capture_page_window(spec: WbdHuSpec, client: httpx.Client, roster: list[int],
                        index: int, root: Path, *,
                        capture: Callable) -> tuple[dict[str, tuple[bytes, dict]], list[str]]:
    """Capture one nominal page window, bisecting refused windows per policy.

    The producer deterministically refuses some full-size windows (content-
    dependent server-side failure); the refusal document is retained as
    evidence and the window is re-fetched as half-size parts whose URLs are
    derived from the pinned roster. ``max_bisection_depth`` bounds the
    recursion (the 2-digit layer bisects exactly once and fails on a refusing
    half; the 12-digit layer recurses until every part is served). A single-
    feature window is retried bounded times first when
    ``single_refusal_retries`` is positive; when its geometry stays refused and
    ``attrs_parts`` is set its attributes are captured through the producer's
    working attributes-only channel and staged as a named ``_attrs`` part (the
    row's geometry gates to null at validation time) so a permanently
    unservable row cannot block the complete capture. The refusal evidence and
    the attributes document are both retained.
    """
    name = f"page_{index:04d}"
    parts: dict[str, tuple[bytes, dict]] = {}
    refusals: list[str] = []

    def walk(part_name: str, sub: list[int], last: int, depth: int) -> None:
        body, receipt = capture(client, page_window_url(spec, last, len(sub)),
                                root, part_name)
        if is_refusal(body) and len(sub) == 1 and spec.single_refusal_retries:
            for _attempt in range(spec.single_refusal_retries):
                time.sleep(spec.single_refusal_retry_seconds)
                body, receipt = capture(client, page_window_url(spec, last, len(sub)),
                                        root, part_name)
                if not is_refusal(body):
                    break
        if not is_refusal(body):
            parts[part_name] = (body, receipt)
            return
        refusals.append(f"{part_name}_refusal")
        parts[f"{part_name}_refusal"] = retain_refusal(root, part_name, body, receipt)
        if len(sub) == 1:
            if spec.attrs_parts:
                attrs_name = f"{part_name}_attrs"
                attrs_body, attrs_receipt = capture(client, record_attrs_url(spec, sub[0]),
                                                    root, attrs_name)
                parts[attrs_name] = (attrs_body, attrs_receipt)
                return
            raise ValueError("source_page_window_refused_after_bisection:" + part_name)
        if depth + 1 > spec.max_bisection_depth:
            raise ValueError("source_page_window_refused_after_bisection:" + part_name)
        half = (len(sub) + 1) // 2
        walk(part_name + "a", sub[:half], sub[0] - 1 if sub[0] > 1 else 0, depth + 1)
        walk(part_name + "b", sub[half:], sub[half] - 1, depth + 1)

    walk(name, roster[index * spec.page_size:(index + 1) * spec.page_size],
         roster[index * spec.page_size - 1] if index > 0 else 0, 0)
    return parts, refusals


def is_page_part_name(spec: WbdHuSpec, name: str) -> bool:
    """True for a nominal page name or a bisected a/b part name of one page."""
    if (not name.startswith("page_") or len(name) < 9 or not name[5:9].isdigit()
            or int(name[5:9]) >= spec.page_count):
        return False
    suffix = name[9:]
    return not suffix or set(suffix) <= {"a", "b"}


def is_attrs_part_name(spec: WbdHuSpec, name: str) -> bool:
    """True for a refused single's attributes-only part (``..._attrs``)."""
    return name.endswith("_attrs") and is_page_part_name(spec, name[:-len("_attrs")])


def validate_spatial_reference(value: object, label: str, *, required: bool = True,
                               native_wkid: int = 102100, native_latest_wkid: int = 3857,
                               source_crs_wkid: int = 3857) -> bool:
    """Validate every advertised ArcGIS CRS representation against the layer's
    native web-mercator CRS.

    ArcGIS may return either/both WKID properties (102100 aliases 3857) and
    optional WKT strings; all present representations must identify the
    layer's native CRS; a missing feature-level spatial reference inherits only
    after its page CRS has been validated.
    """
    if value is None:
        if required:
            raise ValueError(f"{label}_crs_drift")
        return False
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid not in (native_wkid, native_latest_wkid):
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = pyproj.CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(pyproj.CRS.from_epsg(source_crs_wkid), ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def validate_service(spec: WbdHuSpec, service: dict, label: str) -> None:
    if (service.get("currentVersion") != 11.3
            or service.get("mapName") != spec.map_name
            or service.get("copyrightText") != spec.expected_copyright_text
            or service.get("capabilities") != "Map,Query,Data"
            or service.get("maxRecordCount") != spec.declared_max_record_count
            or service.get("units") != "esriMeters" or service.get("tables") != []):
        raise ValueError(f"{label}_service_identity_drift")
    validate_spatial_reference(service.get("spatialReference"), label + "_service",
                               native_wkid=spec.native_wkid,
                               native_latest_wkid=spec.native_latest_wkid,
                               source_crs_wkid=spec.source_crs_wkid)
    full = service.get("fullExtent")
    if (not isinstance(full, dict)
            or {key: full.get(key) for key in spec.expected_extent} != dict(spec.expected_extent)):
        raise ValueError(f"{label}_service_extent_drift")
    validate_spatial_reference(full.get("spatialReference"), label + "_service_full_extent",
                               native_wkid=spec.native_wkid,
                               native_latest_wkid=spec.native_latest_wkid,
                               source_crs_wkid=spec.source_crs_wkid)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if roster != list(spec.expected_service_layers):
        raise ValueError(f"{label}_service_layer_roster_drift")


def validate_layer(spec: WbdHuSpec, layer: dict) -> None:
    if layer.get("fields") != spec.expected_layer_fields:
        raise ValueError("source_field_schema_drift")
    if spec.expected_schema_sha256 and _sha(_canonical(layer.get("fields"))) \
            != spec.expected_schema_sha256:
        raise ValueError("source_schema_baseline_drift")
    if (layer.get("id") != spec.layer_id or layer.get("name") != spec.layer_name
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("maxRecordCount") != spec.declared_max_record_count
            or layer.get("copyrightText") != "U.S. Geological Survey"
            or layer.get("displayField") != spec.display_field_name
            or layer.get("geometryField", {}).get("name") != "shape"
            or layer.get("geometryProperties") != {"shapeAreaFieldName": "shape_Area",
                                                   "shapeLengthFieldName": "shape_Length",
                                                   "units": "esriMeters",
                                                   "mapUnits": {"uwkid": 9001}}
            or layer.get("typeIdField") is not None
            or layer.get("isDataVersioned") is not False
            or layer.get("isDataArchived") is not False
            or layer.get("hasZ") is True or layer.get("hasM") is True
            or {key: layer.get("extent", {}).get(key) for key in spec.expected_extent}
            != dict(spec.expected_extent)
            or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True):
        raise ValueError("source_layer_identity_or_extent_drift")
    validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer_native",
                               native_wkid=spec.native_wkid,
                               native_latest_wkid=spec.native_latest_wkid,
                               source_crs_wkid=spec.source_crs_wkid)
    # Individual layer resources omit the top-level spatialReference the service
    # root carries; when present it must still identify the native CRS, and the
    # layer's source/extent references and every page carry validated CRS data
    # either way.
    validate_spatial_reference(layer.get("spatialReference"), "source_layer", required=False,
                               native_wkid=spec.native_wkid,
                               native_latest_wkid=spec.native_latest_wkid,
                               source_crs_wkid=spec.source_crs_wkid)
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        validate_spatial_reference(extent_reference, "source_layer_extent",
                                   native_wkid=spec.native_wkid,
                                   native_latest_wkid=spec.native_latest_wkid,
                                   source_crs_wkid=spec.source_crs_wkid)


def validate_roster_document(spec: WbdHuSpec, document: object, label: str) -> list[int]:
    if not isinstance(document, dict) or document.get("objectIdFieldName") != "objectid":
        raise ValueError("source_object_id_field_drift:" + label)
    ids = document.get("objectIds")
    if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
            or len(ids) != spec.expected_count or len(set(ids)) != spec.expected_count
            or ids != sorted(ids) or min(ids) != spec.expected_id_min
            or max(ids) != spec.expected_id_max):
        raise ValueError("source_id_population_drift:" + label)
    if spec.expected_id_sha256 and _sha(_canonical(ids)) != spec.expected_id_sha256:
        raise ValueError("source_id_roster_baseline_drift:" + label)
    return ids


def validate_count(spec: WbdHuSpec, document: object, label: str) -> int:
    if not isinstance(document, dict):
        raise ValueError("source_count_population_drift:" + label)
    count = document.get("count")
    if type(count) is not int or count != spec.expected_count:
        raise ValueError("source_count_population_drift:" + label)
    return count


def validate_service_doc(body: bytes) -> None:
    for marker in (b"hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer",
                   b"StagedProducts/Hydrography/WBD"):
        if marker not in body:
            raise ValueError("usgs_service_documentation_drift")


EPSG3857_PROJJSON = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "ProjectedCRS",
    "name": "WGS 84 / Pseudo-Mercator",
    "base_crs": {
        "type": "GeographicCRS", "name": "WGS 84",
        "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                  "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137,
                                "inverse_flattening": 298.257223563},
                  "id": {"authority": "EPSG", "code": 6326}},
        "coordinate_system": {"subtype": "ellipsoidal",
                              "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat",
                                        "direction": "north", "unit": "degree"},
                                       {"name": "Geodetic longitude", "abbreviation": "Lon",
                                        "direction": "east", "unit": "degree"}]},
        "id": {"authority": "EPSG", "code": 4326}},
    "conversion": {"type": "Conversion", "name": "Popular Visualisation Pseudo-Mercator",
                   "method": {"name": "Popular Visualisation Pseudo Mercator",
                              "id": {"authority": "EPSG", "code": 1024}},
                   "parameters": [{"name": "Latitude of natural origin", "value": 0,
                                   "unit": "degree", "id": {"authority": "EPSG", "code": 8801}},
                                  {"name": "Longitude of natural origin", "value": 0,
                                   "unit": "degree", "id": {"authority": "EPSG", "code": 8802}},
                                  {"name": "False easting", "value": 0, "unit": "metre",
                                   "id": {"authority": "EPSG", "code": 8806}},
                                  {"name": "False northing", "value": 0, "unit": "metre",
                                   "id": {"authority": "EPSG", "code": 8807}}]},
    "coordinate_system": {"subtype": "Cartesian",
                          "axis": [{"name": "Easting", "abbreviation": "X",
                                    "direction": "east", "unit": "metre"},
                                   {"name": "Northing", "abbreviation": "Y",
                                    "direction": "north", "unit": "metre"}]},
    "id": {"authority": "EPSG", "code": 3857},
}

_ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
    "esriFieldTypeDate": pa.int64(), "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeGlobalID": pa.string(),
}


def arrow_type(field: dict) -> pa.DataType:
    return _ARROW_TYPES[field["type"]]


def native_schema(spec: WbdHuSpec) -> pa.Schema:
    fields = [pa.field(field["name"], arrow_type(field), nullable=True)
              for field in spec.attribute_fields]
    fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB",
                                    "geometry_types": ["Polygon", "MultiPolygon"],
                                    "crs": EPSG3857_PROJJSON, "edges": "planar"}}}
    return pa.schema(fields, metadata={b"geo": _canonical(geo)})


def validate_esri_value(field: dict, value: object, *, check_width: bool = False,
                        oid_suffix: str = "") -> None:
    """Validate one served attribute value against its declared physical type.

    ``check_width`` adds the declared string-length bound used by the
    offset-segment policy; ``oid_suffix`` carries the artifact name suffix the
    keyset-window policy appends to OID drift messages.
    """
    if value is None:
        return
    kind = field["type"]
    if kind == "esriFieldTypeOID":
        if type(value) is not int or value <= 0:
            raise ValueError("source_oid_value_type_drift" + oid_suffix)
        return
    if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID"):
        if not isinstance(value, str):
            raise ValueError("source_string_value_type_drift:" + str(field["name"]))
        if check_width and len(value) > field.get("length", 10**9):
            raise ValueError("source_string_value_width_drift:" + str(field["name"]))
        return
    if kind == "esriFieldTypeDate":
        if type(value) is not int:
            raise ValueError("source_date_value_type_drift:" + str(field["name"]))
        return
    if kind == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise ValueError("source_double_value_type_drift:" + str(field["name"]))
        return
    if kind == "esriFieldTypeGeometry":
        return
    raise ValueError("source_unexpected_field_type:" + str(field["name"]))


def _validate_page_document(spec: WbdHuSpec, body: bytes, part_name: str,
                            expected_part_ids: list[int], attrs_part: bool) -> dict:
    page = json.loads(body)
    if "error" in page:
        raise ValueError("source_page_error:" + part_name)
    if attrs_part:
        if set(page) - {"displayFieldName", "features", "fieldAliases", "fields"}:
            raise ValueError("source_page_attrs_unexpected_member:" + part_name)
    else:
        if set(page) - {"displayFieldName", "exceededTransferLimit", "features",
                        "fieldAliases", "fields", "geometryType", "spatialReference"}:
            raise ValueError("source_page_unexpected_member:" + part_name)
    if page.get("displayFieldName") != spec.display_field_name:
        raise ValueError("source_page_identity_drift:" + part_name)
    rows = page.get("features")
    if not isinstance(rows, list) or len(rows) != len(expected_part_ids):
        raise ValueError("source_page_population_incomplete:" + part_name)
    if not attrs_part:
        validate_spatial_reference(page.get("spatialReference"),
                                   "source_" + part_name + "_page",
                                   native_wkid=spec.native_wkid,
                                   native_latest_wkid=spec.native_latest_wkid,
                                   source_crs_wkid=spec.source_crs_wkid)
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_identity_drift:" + part_name)
    aliases = page.get("fieldAliases")
    if (not isinstance(aliases, dict)
            or aliases != {field["name"]: field["alias"] for field in spec.attribute_fields}):
        raise ValueError("source_page_alias_drift:" + part_name)
    if page.get("fields") != list(spec.attribute_fields):
        raise ValueError("source_page_physical_schema_changed:" + part_name)
    return page


def _stream_page_plan(spec: WbdHuSpec, page_parts: Mapping[str, list] | None,
                      name: str, expected_page_ids: list[int]) -> list[tuple]:
    """The ordered (first_id, part_name, part_ids, attrs_part) validation plan
    for one page, per the level's retained page-part naming policy."""
    declared_parts = (page_parts or {}).get(name) or [name]
    if spec.parts_policy == PARTS_POLICY_SINGLE_OR_AB:
        half = (len(expected_page_ids) + 1) // 2
        if len(declared_parts) not in (1, 2):
            raise ValueError("source_page_parts_naming_drift:" + name)
        plan = []
        for part_no, part_name in enumerate(declared_parts):
            expected_name = name if len(declared_parts) == 1 else f"{name}{'ab'[part_no]}"
            if part_name != expected_name:
                raise ValueError("source_page_parts_naming_drift:" + name)
            part_ids = (expected_page_ids if len(declared_parts) == 1
                        else (expected_page_ids[:half] if part_name.endswith("a")
                              else expected_page_ids[half:]))
            plan.append((part_ids[0] if part_ids else 0, part_name, part_ids, False))
        return plan
    plan = []
    for part_name in declared_parts:
        attrs_part = part_name.endswith("_attrs")
        base = part_name[:-len("_attrs")] if attrs_part else part_name
        if not (base == name or (base.startswith(name) and base[len(name):]
                and set(base[len(name):]) <= {"a", "b"})):
            raise ValueError("source_page_parts_naming_drift:" + name)
        part_ids = expected_page_ids
        for tag in base[len(name):]:
            half = (len(part_ids) + 1) // 2
            part_ids = part_ids[:half] if tag == "a" else part_ids[half:]
        if attrs_part and len(part_ids) != 1:
            raise ValueError("source_page_parts_attrs_window:" + name)
        plan.append((part_ids[0] if part_ids else 0, part_name, part_ids, attrs_part))
    if len({entry[1] for entry in plan}) != len(plan):
        raise ValueError("source_page_parts_duplicate:" + name)
    plan.sort(key=lambda entry: entry[0])
    return plan


def validate_stream(spec: WbdHuSpec, root: Path, *, parquet_sink: Path | None,
                    page_parts: Mapping[str, list] | None = None) -> tuple[dict, dict]:
    """Validate every retained response and build the native geoparquet.

    Pages are processed in ascending-OBJECTID keyset order; refused windows are
    validated through their retained bisected page parts (and, under the
    recursive policy, the attributes-only parts of permanently unservable
    rows). Each page's rows are validated, gated and written as one parquet row
    group before the next page is parsed, so the population is never held in
    memory. Returns the source audit and derived candidate facts.
    """
    loaded = {name: _load_capture(root, name) for name in METADATA_NAMES}
    docs = {name: value[0] for name, value in loaded.items()}
    receipts = {name: value[2] for name, value in loaded.items()}
    validate_service_doc(docs["service_doc"])
    validate_service(spec, json.loads(docs["service_before"]), "service_before")
    layer = json.loads(docs["layer_before"])
    validate_layer(spec, layer)
    if json.loads(docs["service_after"]) != json.loads(docs["service_before"]) \
            or json.loads(docs["layer_after"]) != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    validate_count(spec, json.loads(docs["count_before"]), "before")
    validate_count(spec, json.loads(docs["count_after"]), "after")
    roster = validate_roster_document(spec, json.loads(docs["ids_before"]), "before")
    roster_after = validate_roster_document(spec, json.loads(docs["ids_after"]), "after")
    if roster != roster_after:
        raise ValueError("source_id_roster_changed_during_capture")
    expected_urls = dict(capture_roster(spec, roster))
    for name in METADATA_NAMES:
        validate_capture_request_identity(name, receipts[name], expected_urls[name])

    tmp_path = (parquet_sink.with_name(parquet_sink.name + ".construction-tmp")
                if parquet_sink is not None else None)
    if parquet_sink is not None:
        parquet_sink.parent.mkdir(parents=True, exist_ok=True)
    schema = native_schema(spec)
    writer = (pq.ParquetWriter(tmp_path, schema, compression="zstd", use_dictionary=False)
              if tmp_path is not None else None)
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    vertices = 0
    winding = {"cw_shells": 0, "ccw_holes": 0}
    ogc_invalid_geometry: dict[str, int] = {}
    ogc_invalid_sample_objectids: list[int] = []
    gate_counts: dict[str, int] = {}
    null_geometry_count = 0
    geometry_valid = 0
    multipart = 0
    with_hole = 0
    ids_member_features = 0
    refused_geometry_oids: list[int] = []
    bounds: list[float] | None = None
    coverage = {key: spec.expected_extent[key] for key in ("xmin", "ymin", "xmax", "ymax")}
    last_objectid = 0
    names = tuple(field["name"] for field in spec.attribute_fields)
    geometry_types_seen: set[str] = set()
    try:
        for index in range(spec.page_count):
            name = f"page_{index:04d}"
            expected_page_ids = roster[index * spec.page_size:(index + 1) * spec.page_size]
            part_slices = _stream_page_plan(spec, page_parts, name, expected_page_ids)
            columns: dict[str, list] = {field: [] for field in names}
            geometries: list[bytes | None] = []
            page_ids = []
            for part_no, (_first_id, part_name, part_ids, attrs_part) \
                    in enumerate(part_slices):
                body, _wire, receipt = _load_capture(root, part_name)
                expected_url = (expected_urls[name] if part_name == name
                                else record_attrs_url(spec, part_ids[0]) if attrs_part
                                else page_part_url(spec, roster, index, part_name))
                validate_capture_request_identity(part_name, receipt, expected_url)
                page = _validate_page_document(spec, body, part_name, part_ids, attrs_part)
                exceeded = page.get("exceededTransferLimit")
                if exceeded is not None and type(exceeded) is not bool:
                    raise ValueError("source_page_transfer_flag_drift:" + part_name)
                if (not attrs_part and exceeded is True and index == spec.page_count - 1
                        and part_no == len(part_slices) - 1):
                    raise ValueError("source_page_transfer_limit_on_final_page:" + part_name)
                if (not attrs_part and len(part_slices) == 1 and exceeded is True
                        and len(page["features"]) < spec.page_size):
                    raise ValueError("source_page_transfer_truncated:" + part_name)
                for feature in page["features"]:
                    attrs, geometry = feature.get("attributes"), feature.get("geometry")
                    if (not isinstance(attrs, dict) or len(attrs) != len(spec.attribute_fields)
                            or tuple(attrs) != names):
                        raise ValueError("source_attribute_roster_or_order_drift:" + part_name)
                    for field in spec.attribute_fields:
                        validate_esri_value(field, attrs[field["name"]],
                                            oid_suffix=":" + part_name
                                            if field["type"] == "esriFieldTypeOID" else "")
                    oid = attrs.get("objectid")
                    if type(oid) is not int or oid not in part_ids:
                        raise ValueError("source_feature_id_roster_drift:" + part_name)
                    if oid <= last_objectid:
                        raise ValueError("source_feature_ids_not_strictly_increasing:" + part_name)
                    last_objectid = oid
                    page_ids.append(oid)
                    if attrs_part:
                        if feature.get("geometry") is not None:
                            raise ValueError("source_page_attrs_geometry_drift:" + part_name)
                        gate_counts[REFUSED_GEOMETRY_GATE_MODE] = \
                            gate_counts.get(REFUSED_GEOMETRY_GATE_MODE, 0) + 1
                        null_geometry_count += 1
                        refused_geometry_oids.append(oid)
                        geometries.append(None)
                        for field in names:
                            columns[field].append(attrs[field])
                        record_hash.update(_canonical(dict(attrs)) + DIGEST_SEPARATOR)
                        continue
                    if not isinstance(geometry, dict) \
                            or set(geometry) - {"rings", "ids", "hasZ", "hasM",
                                                "spatialReference"}:
                        raise ValueError("source_polygon_geometry_structure_drift:" + part_name)
                    if geometry.get("hasZ") is True or geometry.get("hasM") is True:
                        raise ValueError("source_geometry_dimensional_drift:" + part_name)
                    if geometry.get("spatialReference") is not None:
                        validate_spatial_reference(geometry.get("spatialReference"),
                                                   "source_" + part_name + "_feature",
                                                   native_wkid=spec.native_wkid,
                                                   native_latest_wkid=spec.native_latest_wkid,
                                                   source_crs_wkid=spec.source_crs_wkid)
                    rings = geometry.get("rings")
                    if not isinstance(rings, list) or not rings:
                        raise ValueError("source_polygon_rings_missing:" + part_name)
                    zero_area = frozenset(zero_area_rings(rings))
                    for ring_index, ring in enumerate(rings):
                        if not isinstance(ring, list) or len(ring) < 4:
                            raise ValueError("source_polygon_ring_invalid:" + part_name)
                        if ring[0] != ring[-1]:
                            raise ValueError("source_polygon_ring_not_closed:" + part_name)
                        for point in ring:
                            if (not isinstance(point, list) or len(point) != 2
                                    or any(type(coord) not in (int, float)
                                           or not math.isfinite(coord) for coord in point)):
                                raise ValueError("source_xy_ordinate_contract_drift:" + part_name)
                            if not (coverage["xmin"] - spec.expected_extent_tolerance
                                    <= point[0] <= coverage["xmax"] + spec.expected_extent_tolerance
                                    and coverage["ymin"] - spec.expected_extent_tolerance
                                    <= point[1] <= coverage["ymax"] + spec.expected_extent_tolerance):
                                raise ValueError("source_geometry_outside_declared_coverage:"
                                                 + part_name)
                        vertices += len(ring)
                        if ring_index not in zero_area:
                            # Zero-area rings carry no orientation; the census
                            # counts winding only for winding-bearing rings,
                            # measured with shapely instead of a hand-written
                            # signed area.
                            if LinearRing([(point[0], point[1]) for point in ring]).is_ccw:
                                winding["ccw_holes"] += 1
                            else:
                                winding["cw_shells"] += 1
                    served_ids = geometry.get("ids")
                    if served_ids is not None:
                        if (not isinstance(served_ids, list)
                                or len(served_ids) != len(rings)
                                or any(not isinstance(entry, list)
                                        or len(entry) != len(ring)
                                        or any(type(value) is not int for value in entry)
                                        for entry, ring in zip(served_ids, rings))):
                            raise ValueError("source_geometry_ids_member_drift:" + part_name)
                        ids_member_features += 1
                    if spec.digest_geometry_before_gate:
                        # The served rings are digested for every row, before
                        # assembly and gating: a gated row's raw coordinates
                        # stay covered by the pinned geometry baseline, so a
                        # coordinate change that still raises the same gate
                        # mode is refused.
                        geometry_hash.update(_canonical([oid, rings]) + DIGEST_SEPARATOR)
                    # Ring orientation and hole assignment are GDAL's through
                    # the shared native Esri polygon conversion; a conversion
                    # failure refuses the capture, and OGC invalidity is a
                    # producer defect retained verbatim with a counted reason,
                    # never repaired or coerced to null geometry.
                    wkb = to_wkb({"rings": rings}, "esriGeometryPolygon",
                                 has_z=False, has_m=False)
                    decoded = from_wkb(wkb)
                    reason = is_valid_reason(decoded)
                    if reason != "Valid Geometry":
                        ogc_invalid_geometry[reason] = \
                            ogc_invalid_geometry.get(reason, 0) + 1
                        if len(ogc_invalid_sample_objectids) < 20:
                            ogc_invalid_sample_objectids.append(oid)
                    else:
                        # Shape parity is a contract about an OGC-valid
                        # assembly; rows retained as OGC-invalid are counted
                        # above instead.
                        for parity_field, observed in (("shape_Area", attrs.get("shape_Area")),
                                                       ("shape_Length", attrs.get("shape_Length"))):
                            if observed is None:
                                continue
                            computed = decoded.area if parity_field == "shape_Area" else decoded.length
                            if abs(computed - observed) \
                                    > spec.shape_parity_tolerance * max(abs(observed), 1e-12):
                                raise ValueError(f"source_{parity_field}_parity_drift:" + part_name)
                    geometry_valid += 1
                    if isinstance(decoded, MultiPolygon):
                        multipart += 1
                        geometry_types_seen.add("MultiPolygon")
                        decoded_rings = [ring for polygon in decoded.geoms
                                         for ring in (polygon.exterior, *polygon.interiors)]
                    else:
                        geometry_types_seen.add("Polygon")
                        decoded_rings = [decoded.exterior, *decoded.interiors]
                    if any(len(ring.interiors) for ring in
                           (decoded.geoms if isinstance(decoded, MultiPolygon) else [decoded])):
                        with_hole += 1
                    for ring in decoded_rings:
                        for x, y in ring.coords:
                            if not (coverage["xmin"] - spec.expected_extent_tolerance
                                    <= x <= coverage["xmax"] + spec.expected_extent_tolerance
                                    and coverage["ymin"] - spec.expected_extent_tolerance
                                    <= y <= coverage["ymax"] + spec.expected_extent_tolerance):
                                raise ValueError("source_geometry_outside_declared_coverage:"
                                                 + part_name)
                            if bounds is None:
                                bounds = [x, y, x, y]
                            else:
                                if x < bounds[0]:
                                    bounds[0] = x
                                if y < bounds[1]:
                                    bounds[1] = y
                                if x > bounds[2]:
                                    bounds[2] = x
                                if y > bounds[3]:
                                    bounds[3] = y
                    geometries.append(wkb)
                    for field in names:
                        columns[field].append(attrs[field])
                    record_hash.update(_canonical(dict(attrs)) + DIGEST_SEPARATOR)
                    if not spec.digest_geometry_before_gate:
                        geometry_hash.update(_canonical([oid, rings]) + DIGEST_SEPARATOR)
            if sorted(page_ids) != expected_page_ids or len(set(page_ids)) != len(page_ids):
                raise ValueError("source_page_id_population_drift:" + name)
            if writer is not None:
                writer.write_table(pa.Table.from_pydict(
                    {**columns, "geometry": geometries}, schema=schema))
        if writer is not None:
            writer.close()
            writer = None
            tmp_path.replace(parquet_sink)
        ids_sha = _sha(_canonical(roster))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
        if spec.expected_record_sha256 and record_sha != spec.expected_record_sha256:
            raise ValueError("source_record_baseline_drift")
        if spec.expected_geometry_sha256 and geometry_sha != spec.expected_geometry_sha256:
            raise ValueError("source_geometry_baseline_drift")
        if (spec.expected_null_geometry_count is not None
                and null_geometry_count != spec.expected_null_geometry_count):
            raise ValueError("source_null_geometry_count_drift")
        if (spec.expected_unusable_geometry_counts is not None
                and gate_counts != dict(spec.expected_unusable_geometry_counts)):
            raise ValueError("source_unusable_geometry_counts_drift")
        if (spec.expected_refused_geometry_oids is not None
                and tuple(sorted(refused_geometry_oids))
                != tuple(spec.expected_refused_geometry_oids)):
            raise ValueError("source_refused_geometry_oids_drift")
        audit = {
            "source_feature_count": spec.expected_count,
            "source_id_sha256": ids_sha, "source_id_min": spec.expected_id_min,
            "source_id_max": spec.expected_id_max,
            "source_schema_sha256": _sha(_canonical(layer["fields"])),
            "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
            "baseline_digest_scheme": spec.baseline_digest_scheme,
            "source_geometry_type": "esriGeometryPolygon",
            "source_crs": f"EPSG:{spec.source_crs_wkid}",
            "source_native_wkid": spec.native_wkid,
            "source_vertex_count": vertices,
            "source_ring_winding_counts": dict(sorted(winding.items())),
            "geometry_valid_count": geometry_valid,
            "null_geometry_count": null_geometry_count,
            "unusable_geometry_counts": dict(sorted(gate_counts.items())),
            "ogc_invalid_geometry_counts": dict(sorted(ogc_invalid_geometry.items())),
            "ogc_invalid_geometry_sample_objectids": ogc_invalid_sample_objectids,
            "multipart_feature_count": multipart, "feature_with_hole_count": with_hole,
            "source_ids_member_feature_count": ids_member_features,
            "source_refused_geometry_oids": sorted(refused_geometry_oids),
            "geometry_bounds": bounds, "declared_extent": dict(spec.expected_extent),
            "service_copyright_text": spec.expected_copyright_text,
            "coverage_scope": spec.coverage_scope,
        }
        if spec.parts_policy == PARTS_POLICY_SINGLE_OR_AB:
            audit["page_parts"] = dict(sorted((page_parts or {}).items()))
        return audit, {"parquet_path": parquet_sink, "geometry_types": sorted(geometry_types_seen)}
    except BaseException:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
            writer = None
        if parquet_sink is not None:
            parquet_sink.with_name(parquet_sink.name + ".construction-tmp").unlink(missing_ok=True)
        raise


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def _validate_manifest_page_parts(spec: WbdHuSpec, page_parts: object) -> dict:
    if not isinstance(page_parts, dict):
        raise ValueError("retained_capture_page_parts_invalid")
    for page, parts in page_parts.items():
        if spec.parts_policy == PARTS_POLICY_SINGLE_OR_AB:
            if page not in {f"page_{index:04d}" for index in range(spec.page_count)} \
                    or not isinstance(parts, list) \
                    or (parts != [page] and parts != [page + "a", page + "b"]):
                raise ValueError("retained_capture_page_parts_invalid")
        else:
            if (not is_page_part_name(spec, page) or not isinstance(parts, list) or not parts
                    or len(set(parts)) != len(parts)
                    or not all((is_page_part_name(spec, part) or is_attrs_part_name(spec, part))
                               and part.startswith(page) for part in parts)):
                raise ValueError("retained_capture_page_parts_invalid")
    return page_parts


def read_replay(spec: WbdHuSpec, root: Path) -> tuple[dict[str, tuple[bytes, dict]],
                                                      dict[str, list[str]], list[str]]:
    """Re-read a completed capture directory against its sealed manifest.

    Verifies the artifact digests, the per-slot retained receipts and the exact
    request URL each response is bound to (nominal pages, bisected parts and
    attributes-only parts alike), refusing partial or corrupt captures.
    """
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    page_parts = _validate_manifest_page_parts(spec, manifest.get("page_parts") or {})
    page_refusals = manifest.get("page_refusals") or []
    if not isinstance(page_refusals, list) or not all(
            isinstance(entry, str) and entry.endswith("_refusal")
            and is_page_part_name(spec, entry[:-len("_refusal")])
            # The one-level policy never retains mid-node refusals: a refusing
            # half aborts the capture, so only nominal page refusals are valid.
            and (spec.parts_policy == PARTS_POLICY_BISECT_ATTRS
                 or len(entry[:-len("_refusal")]) == 9)
            for entry in page_refusals):
        raise ValueError("retained_capture_page_refusals_invalid")
    page_slots = set()
    for index in range(spec.page_count):
        page = f"page_{index:04d}"
        if page in page_parts:
            page_slots.update(page_parts[page])
        else:
            page_slots.add(page)
    expected_names = {*METADATA_NAMES, *page_slots, *page_refusals}
    roster_files = manifest.get("artifact_roster")
    if not isinstance(roster_files, dict) or set(manifest["receipts"]) != expected_names:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster_files) | {"capture_manifest.json"})
    if roster_files != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    ids = validate_roster_document(spec, json.loads(_load_capture(root, "ids_before")[0]),
                                   "replay")
    expected_urls = dict(capture_roster(spec, ids))
    captures = {}
    for name in sorted(expected_names):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        if name.endswith("_refusal"):
            bound = name[:-len("_refusal")]
            expected_url = (expected_urls[bound] if len(bound) == 9
                            else page_part_url(spec, ids, int(bound[5:9]), bound))
            validate_capture_request_identity(name, receipt, expected_url)
            if not is_refusal(body):
                raise ValueError("retained_capture_refusal_document_drift:" + name)
        elif is_attrs_part_name(spec, name):
            bound = name[:-len("_attrs")]
            sub = ids[int(bound[5:9]) * spec.page_size:(int(bound[5:9]) + 1) * spec.page_size]
            for tag in bound[9:]:
                half = (len(sub) + 1) // 2
                sub = sub[:half] if tag == "a" else sub[half:]
            validate_capture_request_identity(name, receipt, record_attrs_url(spec, sub[0]))
        elif name in expected_urls:
            validate_capture_request_identity(name, receipt, expected_urls[name])
        else:
            validate_capture_request_identity(name, receipt,
                                               page_part_url(spec, ids, int(name[5:9]), name))
        captures[name] = (body, receipt)
    return captures, {page: list(parts) for page, parts in page_parts.items()}, \
        sorted(page_refusals)


def _unsafe_capture_version(version: object) -> ValueError | None:
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        return ValueError("unsafe_capture_version")
    return None


def capture_candidate(spec: WbdHuSpec, storage: StagingStorageResource, *, version: str,
                      capture_directory: str | None, capture: Callable) -> dict[str, object]:
    """Capture or replay one complete, unapproved native WBD HU candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    error = _unsafe_capture_version(version)
    if error is not None:
        raise error
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < spec.disk_floor_bytes:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures, page_parts, page_refusals = read_replay(spec, root)
    else:
        if requested and (any(requested.glob("*.body")) or any(requested.glob("*.wire"))
                          or any(requested.glob("*.receipt.json"))):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                default_capture_dir("hifld-native-reconstruction") / spec.capture_root_family
                / "captures" / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        page_parts: dict[str, list[str]] = {}
        page_refusals: list[str] = []
        try:
            with httpx.Client(follow_redirects=False,
                              timeout=httpx.Timeout(120, connect=20, read=600),
                              headers={"User-Agent": spec.user_agent}) as client:
                ids_body, ids_receipt = capture_budgeted(root, spec.page_max_bytes,
                                                         spec.capture_max_bytes,
                                                         client, ids_url(spec), "ids_before")
                roster = validate_roster_document(spec, json.loads(ids_body), "seed")
                captures["ids_before"] = (ids_body, ids_receipt)
                doc_client = httpx.Client(follow_redirects=True,
                                          timeout=httpx.Timeout(60, connect=20, read=300),
                                          headers={"Connection": "close",
                                                   "User-Agent": spec.doc_user_agent})
                try:
                    for name, url in capture_roster(spec, roster):
                        if name in captures:
                            continue
                        if name.startswith("page_"):
                            window, refusals = capture_page_window(
                                spec, client, roster, int(name[5:9]), root, capture=capture)
                            captures.update(window)
                            page_parts[name] = [part for part in sorted(window)
                                                if not part.endswith("_refusal")]
                            page_refusals.extend(refusals)
                            continue
                        active = doc_client if name == "service_doc" else client
                        captures[name] = capture(active, url, root, name,
                                                 max_bytes=spec.doc_max_bytes
                                                 if name == "service_doc"
                                                 else spec.page_max_bytes)
                finally:
                    doc_client.close()
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, spec.dataset_slug, version,
                                              captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({
            "complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
            "page_parts": dict(sorted(page_parts.items())),
            "page_refusals": sorted(page_refusals)}) + b"\n")
        captures, page_parts, page_refusals = read_replay(spec, root)

    parquet_sink = staging / "_construction-tmp" / f"{spec.dataset_slug}-{version}.parquet"
    audit, derived = validate_stream(spec, root, parquet_sink=parquet_sink,
                                     page_parts=page_parts)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False})
    if spec.audit_page_refusal_count:
        audit["source_page_refusal_count"] = len(page_refusals)
    if spec.baseline_pending_note is not None and (
            not spec.expected_id_sha256 or not spec.expected_record_sha256
            or not spec.expected_geometry_sha256
            or spec.expected_null_geometry_count is None
            or spec.expected_unusable_geometry_counts is None):
        audit["baseline_pending_note"] = spec.baseline_pending_note
        audit["baseline_pending_complete_capture"] = True
    content_version = "v" + _sha(_canonical({
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = f"{spec.dataset_slug}/{spec.file_slug}/{content_version}"
    output_path = f"geoparquet/{spec.file_slug}.parquet"
    parquet_path = derived["parquet_path"]
    parquet_bytes = parquet_path.stat().st_size
    digest = hashlib.sha256()
    with open(parquet_path, "rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    parquet_sha = digest.hexdigest()
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    gate_note = (f"{audit['null_geometry_count']} rows gate to null geometry and are "
                 "counted by failure mode: "
                 + json.dumps(audit["unusable_geometry_counts"], sort_keys=True)
                 if audit["unusable_geometry_counts"] else
                 "no rows gate: every served polygon assembles to a valid OGC geometry")
    inputs = [{"input_id": name, "producer": spec.publisher,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": spec.service_doc_url if name == "service_doc" else spec.layer_url,
        "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": (
            "The service publishes no per-snapshot release date; its copyrightText "
            "records a service refresh (October 2026) and the producer publishes staged "
            "products on a continuing basis; neither is an immutable release date."),
        "license": "Public domain (U.S. Government work) per the producer service use "
                   "constraints; formal rights acceptance remains pending.",
        "attribution": "U.S. Geological Survey (acknowledgment of the USGS is requested for "
                       "products derived from these data).",
        **({"documentation_evidence": {
            "service_documentation_url": spec.service_doc_url,
            "wbd_program_url": spec.wbd_program_doc_url,
            "dhs_crosswalk_status": "Deprecate",
            "dhs_crosswalk_layer_name": "Watershed Boundary Dataset (WBD)",
            "old_hosting_item_id": "409515c7627c444fb08b66cf89634028",
            "old_hosting_location": ("https://geoplatform.maps.arcgis.com/home/item.html?id="
                                     "409515c7627c444fb08b66cf89634028"),
            "crosswalk_open_rest_service": spec.service_url,
            "crosswalk_evidence_url": spec.dhs_crosswalk_evidence_url,
            "basis": "The DHS HIFLD Open crosswalk (August 2025) marks the retired "
                     "GeoPlatform-hosted HIFLD item 409515c7627c444fb08b66cf89634028 as "
                     "Deprecate and names this USGS hydro.nationalmap.gov wbd MapServer "
                     "as the agency's current open REST service; the USGS page captured "
                     "as service_doc publishes exactly this service URL and the "
                     "producer's staged-product locations; the captured layer response, "
                     "not either page, supplies the data."}}
         if name == "service_doc" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": spec.manifest_title,
        "description": spec.manifest_description,
        "publisher": spec.publisher, "agency": spec.agency, "source_url": spec.layer_url,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri web-mercator polygon capture to typed native "
                                        "geoparquet candidate",
            "parameters": {
                "source_fields": spec.transformation_source_fields,
                "source_ids": spec.transformation_source_ids,
                "source_crs": f"EPSG:{spec.source_crs_wkid}",
                "geometry": ("2D ring order, winding and hole ownership preserved; no "
                             "reprojection, repair or simplification; multi-owned holes "
                             "attach to the smallest containing exterior per the merged "
                             "USACE reservoir precedent and rows that still cannot form a "
                             "valid OGC polygon are counted by failure mode; " + gate_note),
                "raw_evidence": spec.transformation_raw_evidence,
                "candidate_status": "unapproved; baseline/schema/ID/coverage/rights/release "
                                    "decisions pending"}}],
        "lineage": spec.lineage,
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": spec.policy_reason,
            "objects": [{"path": output_path, "bytes": parquet_bytes, "sha256": parquet_sha}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    audit.update({"source_fields": spec.expected_layer_fields,
                  "official_layer_name": spec.layer_name,
                  "official_layer_id": spec.layer_id,
                  "official_service_url": spec.service_url,
                  "source_population_complete": True,
                  "declared_page_geometry_types": derived["geometry_types"]})
    ready = {"approved": False, "version": content_version,
        "objects": {output_path: {"bytes": parquet_bytes, "sha256": parquet_sha}},
        "source_population_complete": True,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False}
    fingerprint = _sha(_canonical({"identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{spec.dataset_slug}/{content_version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, content_version,
                                                      fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix,
            content_version, fingerprint, identity, audit["source_record_sha256"],
            audit["source_schema_sha256"], original.get("objects", {}), original_receipts)
        return candidate_result(spec, audit, content_version, prefix, capture_hash,
                                reused=True, root=root)
    descriptors = {}
    for name, key, body in (
            (output_path, f"{prefix}/{output_path}", None),
            ("metadata/source_manifest.json", f"{prefix}/metadata/source_manifest.json",
             _canonical(manifest) + b"\n"),
            ("metadata/source_audit.json", f"{prefix}/metadata/source_audit.json",
             _canonical(audit) + b"\n"),
            ("metadata/processing_identity.json", f"{prefix}/metadata/processing_identity.json",
             _canonical(identity) + b"\n"),
            ("metadata/candidate_ready.json", f"{prefix}/metadata/candidate_ready.json",
             _canonical(ready) + b"\n")):
        if body is None:
            storage.upload_local_file(parquet_path, key)
        else:
            write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptor = {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
        if name == output_path and descriptor != {"bytes": parquet_bytes, "sha256": parquet_sha}:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    for name in sorted(captures):
        body, wire, receipt = _load_capture(root, name)
        for suffix, payload in (("body", body), ("wire", wire),
                                ("receipt.json", _canonical(receipt) + b"\n")):
            key = f"{prefix}/raw/{name}.{suffix}"
            write_immutable(storage, key, payload)
            snapshot = storage.object_snapshot(key)
            if snapshot is None or snapshot.size != len(payload) \
                    or storage.sha256_key(key) != _sha(payload):
                raise ValueError("candidate_object_readback_failed:raw/" + name + "." + suffix)
            descriptors[f"raw/{name}.{suffix}"] = {"bytes": len(payload), "sha256": _sha(payload)}
    seal_ready_candidate(storage, state_key, content_version, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return candidate_result(spec, audit, content_version, prefix, capture_hash,
                            reused=False, root=root)


def candidate_result(spec: WbdHuSpec, audit: dict, version: str, prefix: str,
                     capture_hash: str, *, reused: bool, root: Path) -> dict[str, object]:
    return {"dataset_slug": spec.dataset_slug, "file_slug": spec.file_slug, "version": version,
            "feature_count": spec.expected_count, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_id_min": spec.expected_id_min, "source_id_max": spec.expected_id_max,
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": reused,
            "source_capture_evidence_sha256": capture_hash,
            "null_geometry_count": audit["null_geometry_count"],
            "unusable_geometry_counts": audit["unusable_geometry_counts"],
            "geometry_valid_count": audit["geometry_valid_count"],
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(root)}
