"""Approval-gated draft capture of the USGS "US Landslide Regions" polygons.

The HIFLD Open inventory entry ``us-landslide-regions`` ("US Landslide
Regions", agency "USGS - Rocky Mountain Region", date_issued 2024-05-15)
carries, verbatim, the abstract of the USGS data release "Landslide
Inventories across the United States (ver. 3.0, February 2025)" (DOI
10.5066/P14AJF8I, ScienceBase item 671eef1fd34ed0f827ea9f12, USGS Geologic
Hazards Science Center; version history v1 2019-09-19, v2 2022-06-03, v3
revised and reposted 2025-02-03 per us_ls_v3_changes.txt). The layer is not a
row of the DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, byte-identical
public copy sha256 7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38
772bc; checked 2026-11-14, no landslide row), so the layer's own producer
identity anchors are used: the producing agency's ArcGIS Online organization
(orgid v01gqwM5QqNysAAi, usgs.maps.arcgis.com) item f332e370f4dd4952a77a9d454
f2362eb "US_Landslide_poly_v3", owner esjones@usgs.gov_USGS, whose service is
https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services/
US_Landslide_poly_v2/FeatureServer layer 12 "US_Landslide_v3poly"
(serviceDescription "USGS US Landslide version 3 polygons"; the service name
keeps its v2 form while item title, snippet and dataLastEditDate
2025-02-19T18:22Z record the version 3 publication). This is the producing
agency's own hosted publication of the data release's polygon feature class
(us_ls_v3_poly), not a mirror. The DataLumos project 240751 archive copy and
the HIFLD Next PEDP repackaging are identification context only and are never
fetched.

The service is maintained by USGS (the producer states the inventory
"will continue to be updated" and the service metadata carries
maintenanceUpdateFrequency Annually), so completeness comes from the
source's own capture-time witnesses rather than a population pinned in
code: the count and the complete OBJECTID roster are observed before and
after the capture and must agree with each other (native ID gaps are
tolerated, duplicates are not), and the layer is captured as
adaptively bisected ``resultOffset`` segments of the sorted roster: each
accepted page must return exactly its expected id slice, the accepted
pages must tile the complete roster without gaps or overlaps, and a
recovered or resumed segment forces a fresh closing witness. A
single-record page the producer still refuses ends the capture explicitly
rather than guessing.

Geometry is the layer's native Z-aware polygon: every feature geometry is
``{"rings", "hasZ"}`` in the page CRS (EPSG:3857) and, when ``returnZ`` is
requested, ring points carry exactly three ordinates ``[x, y, z]`` with the
served z value retained verbatim (observed 2026-11-14: first rings clockwise
shells, subsequent counter-clockwise holes, tiny non-zero z ordinates,
``hasM`` true in the layer document but no M ordinates served without
``returnM``). Rings are validated (closed, uniform ordinate count, finite,
inside the declared layer extent), part structure is built strictly per the
ArcGIS ring convention (clockwise shells, counter-clockwise holes, hole
is GDAL's containment assembly) and stored verbatim as Polygon Z / MultiPolygon
Z WKB: no rounding, reordering, winding repair, reprojection or geometry
repair is applied, and topology validity is counted, never silently enforced.

The per-value and per-geometry digests, geometry gate counts and observed
bounds are computed from a completed capture directory and recorded as
observations in the candidate audit and ``computed_baseline.json``; they
are never exact-equality gates, and count/ID drift against an
independently approved release is reported by the non-blocking
approved-baseline drift check, not refused at capture time. Publication
stays blocked pending the succession, freshness, schema/ID contract,
rights and release decisions; all runtime approval flags are false and
the draft refresh schedule is stopped.
"""
from __future__ import annotations

from functools import partial
import hashlib
import json
import re
import shutil
import stat
import time
from array import array
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.sources import esri_geometry as crs_validation
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_layer import (
    _validate_offset_replay_refusal, load_replay, save_state as _save_state,
)
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "us-landslide-regions"
FILE_SLUG = DATASET_SLUG
REPO_ROOT = Path(__file__).parents[3]
SERVICE_HOST = "https://services.arcgis.com"
SERVICE_URL = (SERVICE_HOST + "/v01gqwM5QqNysAAi/arcgis/rest/services/"
               "US_Landslide_poly_v2/FeatureServer")
AGOL_ITEM_ID = "f332e370f4dd4952a77a9d454f2362eb"
AGOL_ITEM_URL = ("https://www.arcgis.com/sharing/rest/content/items/"
                 + AGOL_ITEM_ID + "?f=json")
SCIENCEBASE_ITEM_ID = "671eef1fd34ed0f827ea9f12"
SCIENCEBASE_DOI = "10.5066/P14AJF8I"
LAYER_ID = 12
LAYER_NAME = "US_Landslide_v3poly"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolygon"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/us-landslide-regions-draft"
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
SOURCE_CRS_WKID = NATIVE_LAYER_SR["latestWkid"]
GEOMETRY_COLUMN = "geometry"
WEB_MERCATOR_X_LIMIT = 20_037_508.342789244
WEB_MERCATOR_Y_LIMIT = 20_048_966.1040146
MAX_RECORD_COUNT = 2000
PAGE_START_RECORDS = 1000
# Observed layer 12 pages in the giant-polygon band (offset >= 224000) exceed
# 96 MiB decoded from a handful of multi-megabyte features alone, so a smaller
# cap only ever produced truncated refusals there; individual features stay far
# below this bound and the bisection still handles any larger segment.
MAX_BODY_BYTES = 512 * 1024 * 1024
MAX_CAPTURE_BYTES = 8 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 8_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_PROBE_ATTEMPTS = 3
PAGE_PROBE_BASE_DELAY_SECONDS = 5.0
EXPECTED_SERVICE_DESCRIPTION = "USGS US Landslide version 3 polygons"
EXPECTED_COPYRIGHT_TEXT = ""
EXPECTED_SERVICE_LAYERS = [(LAYER_ID, LAYER_NAME, GEOMETRY_TYPE)]
# Schema contract pin: the producer's declared field roster, not a population
# snapshot.
EXPECTED_SCHEMA_SHA256 = "f137264736def9130497b117d8618a98ef972b7355e9701c0340dddf6a9873de"
AGOL_ITEM_OWNER = "esjones@usgs.gov_USGS"
AGOL_ITEM_ORGID = "v01gqwM5QqNysAAi"
AGOL_ITEM_TITLE = "US_Landslide_poly_v3"
AGOL_ITEM_TYPE = "Feature Service"
SCIENCEBASE_RELEASE_TITLE = "Landslide Inventories across the United States"
CROSSWALK_COPY_SHA256 = ("7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38"
                         "772bc")

NATIVE_FIELDS: tuple[tuple[str, str, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", None),
    ("fid_1", "esriFieldTypeDouble", None),
    ("USGS_ID", "esriFieldTypeString", 254),
    ("Date_Min", "esriFieldTypeString", 24),
    ("Date_Max", "esriFieldTypeString", 24),
    ("Fatalities", "esriFieldTypeDouble", None),
    ("Confidence", "esriFieldTypeSmallInteger", None),
    ("LS_Type", "esriFieldTypeString", 254),
    ("Inventory", "esriFieldTypeString", 254),
    ("Inv_URL", "esriFieldTypeString", 254),
    ("Info_Sourc", "esriFieldTypeString", 254),
    ("Notes", "esriFieldTypeString", 254),
    ("Lat_N", "esriFieldTypeDouble", None),
    ("Lon_W", "esriFieldTypeDouble", None),
    ("Shape__Area", "esriFieldTypeDouble", None),
    ("Shape__Length", "esriFieldTypeDouble", None),
)
EXPECTED_ATTRIBUTE_NAMES = tuple(name for name, _type, _length in NATIVE_FIELDS)
METADATA_NAMES = ("item_before", "service_before", "layer_before", "count_before",
                  "ids_seed", "ids_before")
AFTER_NAMES = ("layer_after", "service_after", "count_after", "ids_after", "item_after")
# AGOL portal item usage counters change whenever the item page is viewed and
# are not dataset metadata; both drifted inside the 2026-11-14 capture window
# while the layer, service, count and roster brackets stayed byte-identical.
_ITEM_VOLATILE_FIELDS = frozenset({"lastViewed", "numViews"})
INTEGER_TYPES = ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger")

_ARROW_TYPES: dict[str, pa.DataType] = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int32(),
    "esriFieldTypeSmallInteger": pa.int16(),
    "esriFieldTypeSingle": pa.float32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.int64(),
}


class UsLandslideRegionsError(ValueError):
    """The source snapshot cannot be represented as the pinned candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable",
                                        "domain") if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise UsLandslideRegionsError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def docs_fields_plain(layer: Mapping[str, object]) -> list[dict[str, object]]:
    """Plain-field projection used for the before/after field-schema mirror."""
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise UsLandslideRegionsError("source_field_roster_drift")
    return [_plain_field(x) for x in fields]


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "returnZ": "true",
                   "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


_PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    match = _PAGE_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise UsLandslideRegionsError(
            "source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str) -> list[int]:
    """Require a nonempty roster of unique positive native IDs.

    Completeness is not pinned here: the captured population is whatever the
    source's own count and roster witnesses report. ID gaps are tolerated,
    duplicates and non-native types are not.
    """
    if not isinstance(roster, list) or not roster \
            or any(type(value) is not int or value <= 0 for value in roster) \
            or len(set(roster)) != len(roster):
        raise UsLandslideRegionsError(
            "source_id_population_drift:" + label)
    return sorted(roster)


_validate_spatial_reference = partial(
    crs_validation.validate_native_wkid_pair,
    native_wkid=NATIVE_LAYER_SR["wkid"], native_latest_wkid=NATIVE_LAYER_SR["latestWkid"],
    error_type=UsLandslideRegionsError, missing_error="source_spatial_reference_missing:{label}",
    drift_error="source_spatial_reference_drift:{label}")


def _validate_item_document(item: object, label: str) -> None:
    if not isinstance(item, dict):
        raise UsLandslideRegionsError("source_item_document_invalid")
    item_url = str(item.get("url") or "").split("?")[0]
    if (item.get("id") != AGOL_ITEM_ID or item.get("owner") != AGOL_ITEM_OWNER
            or item.get("orgId") != AGOL_ITEM_ORGID
            or item.get("title") != AGOL_ITEM_TITLE
            or item.get("type") != AGOL_ITEM_TYPE
            or item_url != SERVICE_URL):
        raise UsLandslideRegionsError(
            "us_landslide_regions_item_identity_drift:" + label)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise UsLandslideRegionsError(
            "source_service_or_layer_document_invalid")
    if service.get("serviceDescription") != EXPECTED_SERVICE_DESCRIPTION:
        raise UsLandslideRegionsError(
            "us_landslide_regions_service_identity_drift:" + label)
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise UsLandslideRegionsError(
            "us_landslide_regions_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name"), entry.get("geometryType"))
              for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list) or roster != EXPECTED_SERVICE_LAYERS:
        raise UsLandslideRegionsError(
            "us_landslide_regions_service_layer_roster_drift:" + label)
    if service.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise UsLandslideRegionsError("source_record_limit_drift:" + label)
    _validate_spatial_reference(service.get("spatialReference"), label)


def _validate_layer_document(layer: object) \
        -> tuple[list[dict[str, object]], str, dict[str, float]]:
    if not isinstance(layer, dict):
        raise UsLandslideRegionsError(
            "source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != GEOMETRY_TYPE):
        raise UsLandslideRegionsError(
            "us_landslide_regions_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not True:
        raise UsLandslideRegionsError(
            "source_layer_z_or_m_contract_drift")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise UsLandslideRegionsError("source_record_limit_drift")
    advanced = layer.get("advancedQueryCapabilities")
    if not isinstance(advanced, dict) or advanced.get("supportsPagination") is not True:
        raise UsLandslideRegionsError(
            "source_pagination_capability_changed")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise UsLandslideRegionsError("source_layer_extent_missing")
    _validate_spatial_reference(extent.get("spatialReference"), "layer_extent")
    observed_extent = {key: extent.get(key)
                       for key in ("xmin", "ymin", "xmax", "ymax")}
    if (any(type(value) not in (int, float)
            or not (value == value and -1e308 < value < 1e308)
            for value in observed_extent.values())
            or observed_extent["xmin"] > observed_extent["xmax"]
            or observed_extent["ymin"] > observed_extent["ymax"]):
        raise UsLandslideRegionsError("source_layer_extent_drift")
    plain, schema_sha = _profile_schema(layer)
    attributes = [field for field in plain
                  if field.get("type") != "esriFieldTypeGeometry"]
    if ([field.get("name") for field in attributes] != list(EXPECTED_ATTRIBUTE_NAMES)
            or [(field.get("name"), field.get("type"), field.get("length"))
                for field in attributes] != [entry for entry in NATIVE_FIELDS]):
        raise UsLandslideRegionsError("source_schema_drift")
    for field in attributes:
        if field["type"] not in _ARROW_TYPES:
            raise UsLandslideRegionsError(
                "unsupported_source_physical_type:" + str(field["type"]))
    return attributes, schema_sha, observed_extent


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise UsLandslideRegionsError(
            "unsupported_source_physical_type:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, value: object) -> None:
    if value is None:
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise UsLandslideRegionsError(
                "source_native_type_changed:" + name)
        return
    if esri_type in INTEGER_TYPES:
        if type(value) is not int:
            raise UsLandslideRegionsError(
                "source_native_type_changed:" + name)
        return
    if esri_type in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        if type(value) not in (int, float) or not (value == value
                                                   and -1e308 < value < 1e308):
            raise UsLandslideRegionsError(
                "source_native_type_changed:" + name)
        return
    raise UsLandslideRegionsError(
        "unsupported_source_physical_type:" + esri_type)


def _ring_ordinates(point: object) -> list[float]:
    if not isinstance(point, list) or len(point) != 3:
        raise UsLandslideRegionsError("source_polygon_ordinate_count_drift")
    ordinates: list[float] = []
    for value in point:
        if type(value) not in (int, float):
            raise UsLandslideRegionsError("source_polygon_ordinate_not_numeric")
        if not (value == value and -1e308 < value < 1e308):
            raise UsLandslideRegionsError("source_polygon_ordinate_not_finite")
        ordinates.append(float(value))
    return ordinates


def _validate_rings(geometry: Mapping[str, object]) -> tuple[list[list[list[float]]], int]:
    """Validate served rings verbatim and return (rings, vertex_count).

    Every ring must be closed with uniform three-ordinate points in the Web
    Mercator domain; nothing is repaired, reordered or rounded.
    """
    rings_raw = geometry.get("rings")
    if not isinstance(rings_raw, list) or not rings_raw:
        raise UsLandslideRegionsError("source_polygon_rings_missing")
    rings: list[list[list[float]]] = []
    vertices = 0
    for ring_raw in rings_raw:
        if not isinstance(ring_raw, list) or len(ring_raw) < 4:
            raise UsLandslideRegionsError("source_polygon_ring_invalid")
        ring: list[list[float]] = []
        for point in ring_raw:
            ordinates = _ring_ordinates(point)
            x, y = ordinates[0], ordinates[1]
            if not (-WEB_MERCATOR_X_LIMIT <= x <= WEB_MERCATOR_X_LIMIT
                    and -WEB_MERCATOR_Y_LIMIT <= y <= WEB_MERCATOR_Y_LIMIT):
                raise UsLandslideRegionsError("source_coordinate_range_drift")
            ring.append(ordinates)
        if ring[0][:2] != ring[-1][:2]:
            raise UsLandslideRegionsError("source_polygon_ring_not_closed")
        vertices += len(ring)
        rings.append(ring)
    return rings, vertices


def _validate_feature_geometry(geometry: object) -> tuple[list[list[list[float]]], int]:
    if not isinstance(geometry, dict):
        raise UsLandslideRegionsError("unexpected_source_geometry_members")
    if set(geometry) - {"rings", "hasZ", "spatialReference"}:
        raise UsLandslideRegionsError("unexpected_source_geometry_members")
    if geometry.get("hasZ") is not True:
        raise UsLandslideRegionsError("source_polygon_z_flag_drift")
    if "spatialReference" in geometry:
        _validate_spatial_reference(geometry["spatialReference"], "feature")
    return _validate_rings(geometry)


def _geometry_wkb(geometry: dict) -> bytes:
    """Convert served Z rings through the shared GDAL Esri polygon conversion.

    Ring orientation and hole assignment are GDAL's; every served ordinate is
    verified unchanged and the result is Polygon Z / MultiPolygon Z. The
    structural contracts (closure, three finite ordinates, Web Mercator
    range) raise in the caller's validation.
    """
    return to_wkb(geometry, GEOMETRY_TYPE, has_z=True, has_m=False)


def _validate_feature(feature: object, expected_objectid: int) \
        -> tuple[int, dict | None, list[list[list[float]]] | None, int]:
    """Validate one feature and return (OBJECTID, geometry, rings, vertices)."""
    if not isinstance(feature, dict):
        raise UsLandslideRegionsError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(EXPECTED_ATTRIBUTE_NAMES):
        raise UsLandslideRegionsError(
            "source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise UsLandslideRegionsError("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise UsLandslideRegionsError(
            "source_page_id_slice_incomplete_or_reordered")
    rings = None
    vertices = 0
    if geometry is not None:
        rings, vertices = _validate_feature_geometry(geometry)
    for name, esri_type, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, attrs[name])
    return objectid, geometry, rings, vertices


def _preflight_capture(capture_root: Path, *, names: Iterable[str] | None = None) -> None:
    if any(path.is_symlink() for path in (capture_root, *capture_root.parents)):
        raise UsLandslideRegionsError("retained_capture_nonregular_path")
    paths = list(capture_root.iterdir()) if names is None else [
        capture_root / f"{name}.{suffix}" for name in names
        for suffix in ("body", "wire", "receipt.json")]
    retained_bytes = 0
    for path in paths:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise UsLandslideRegionsError("retained_capture_nonregular_path:" + path.name)
        if path.name in {f"{FILE_SLUG}.parquet", f"{FILE_SLUG}.parquet.construction-tmp"}:
            if info.st_size > MAX_CAPTURE_BYTES:
                raise UsLandslideRegionsError("retained_derived_output_budget_exceeded")
            continue
        if info.st_size > MAX_BODY_BYTES:
            raise UsLandslideRegionsError("retained_capture_response_budget_exceeded")
        retained_bytes += info.st_size
    if retained_bytes > MAX_CAPTURE_BYTES:
        raise UsLandslideRegionsError("retained_capture_total_budget_exceeded")


def _validate_retained_transport(capture_root: Path, *, names: Iterable[str] | None = None) -> None:
    stems = set(names) if names is not None else None
    _preflight_capture(capture_root, names=stems)
    if stems is None:
        stems = {path.name.removesuffix(suffix) for path in capture_root.iterdir()
                 for suffix in (".body", ".wire", ".receipt.json") if path.name.endswith(suffix)}
    for name in sorted(stems):
        match = re.fullmatch(r"(.+)\.(attempt-[0-9]{2}|error-[0-9]+)", name)
        base = match[1] if match else name
        if match and match[2].startswith("attempt-"):
            attempts = (PAGE_PROBE_ATTEMPTS if _PAGE_NAME_PATTERN.fullmatch(base)
                        and _parse_page_name(base)[1] > 1 else DOC_CAPTURE_ATTEMPTS)
            if not 1 <= int(match[2].removeprefix("attempt-")) < attempts:
                raise UsLandslideRegionsError("retained_capture_attempt_invalid:" + name)
        paths = [capture_root / f"{name}.{suffix}" for suffix in ("body", "wire", "receipt.json")]
        if any(not path.exists() for path in paths):
            raise UsLandslideRegionsError("retained_capture_artifact_missing:" + name)
        receipt = json.loads(paths[2].read_bytes())
        if not isinstance(receipt, dict):
            raise UsLandslideRegionsError("retained_capture_receipt_invalid:" + name)
        _validate_request_scope(base, receipt)
        try:
            if (receipt.get("complete_body") is True and receipt.get("status") == 200
                    and (not match or match[2].startswith("error-"))):
                _read_capture(capture_root, name)
            else:
                _validate_offset_replay_refusal(capture_root, name, receipt["url"],
                                              max_capture_bytes=MAX_BODY_BYTES)
        except ValueError as exc:
            raise UsLandslideRegionsError(str(exc)) from exc


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsLandslideRegionsError(
            "retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise UsLandslideRegionsError(
            "retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    _load_receipt(capture_root, name)
    _preflight_capture(capture_root, names=(name,))
    try:
        return load_replay(capture_root, name)
    except ValueError as exc:
        raise UsLandslideRegionsError(str(exc)) from exc


def _validate_request_scope(name: str, receipt: Mapping[str, object]) -> None:
    """Pin every retained request to this producer, layer and roster slice."""
    url = receipt.get("url")
    if not isinstance(url, str):
        raise UsLandslideRegionsError(
            "source_capture_endpoint_drift:" + name)
    parsed = urlsplit(url)
    hostname = parsed.hostname
    if parsed.scheme != "https":
        raise UsLandslideRegionsError(
            "source_capture_endpoint_drift:" + name)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if name in ("service_before", "service_after"):
        if hostname != "services.arcgis.com" \
                or parsed.path != urlsplit(SERVICE_URL).path:
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        if params != {"f": ["pjson"]}:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    elif name in ("layer_before", "layer_after"):
        if hostname != "services.arcgis.com" \
                or parsed.path != urlsplit(LAYER_URL).path:
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        if params != {"f": ["pjson"]}:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    elif name in ("item_before", "item_after"):
        if hostname != "www.arcgis.com" or parsed.path != \
                f"/sharing/rest/content/items/{AGOL_ITEM_ID}":
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        if params != {"f": ["json"]}:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    elif name.startswith("count_"):
        if hostname != "services.arcgis.com" \
                or parsed.path != urlsplit(LAYER_URL).path + "/query":
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        if params != {"where": ["1=1"], "returnCountOnly": ["true"], "f": ["json"]}:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    elif name.startswith("ids_"):
        if hostname != "services.arcgis.com" \
                or parsed.path != urlsplit(LAYER_URL).path + "/query":
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        if params != {"where": ["1=1"], "returnIdsOnly": ["true"], "f": ["json"]}:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    elif _PAGE_NAME_PATTERN.fullmatch(name):
        offset, count = _parse_page_name(name)
        if offset < 0 or count < 1:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
        if hostname != "services.arcgis.com" \
                or parsed.path != urlsplit(LAYER_URL).path + "/query":
            raise UsLandslideRegionsError(
                "source_capture_endpoint_drift:" + name)
        expected = {
            "where": ["1=1"], "outFields": ["*"], "returnGeometry": ["true"],
            "returnZ": ["true"],
            "orderByFields": ["OBJECTID ASC"],
            "resultOffset": [str(offset)], "resultRecordCount": [str(count)],
            "f": ["json"]}
        if params != expected:
            raise UsLandslideRegionsError(
                "source_capture_request_scope_drift:" + name)
    else:
        raise UsLandslideRegionsError(
            "source_capture_request_scope_drift:" + name)


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"pages": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsLandslideRegionsError("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("pages"), list):
        raise UsLandslideRegionsError("capture_state_invalid")
    return state


def _witness_aside(capture_root: Path, name: str) -> None:
    """Move a retained refused/torn response aside so the slot can be refetched.

    A complete HTTP 200 error body carries a self-consistent receipt, so
    without this witness step a resumed capture would replay the same refusal
    forever; the witnessed evidence stays on disk next to the capture.
    """
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def _retained_or_fetch(client: httpx.Client, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except UsLandslideRegionsError:
            _witness_aside(capture_root, name)
    return _capture_with_retry(client, url, capture_root, name,
                               attempts=DOC_CAPTURE_ATTEMPTS,
                               base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    return isinstance(document, dict) and "error" in document


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents
    and incomplete transport captures (connection resets, truncation); failed
    attempts are preserved on disk under ``<name>.attempt-NN`` evidence names
    and never enter the capture roster. The final attempt's body is always
    returned as captured, so a persistent producer failure surfaces to the
    caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name,
                                        max_bytes=MAX_BODY_BYTES)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _producer_error_doc(body) and attempt < attempts:
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


def _plan_page_gaps(capture_root: Path, roster: list[int]) -> list[tuple[int, int]]:
    """Plan the roster gaps that still need a capture this session.

    A retained page counts as accepted only when its receipt and body verify,
    its body is not a producer error document, and it already sits in the
    capture state; anything else is witnessed aside and refetched.
    """
    state = _load_state(capture_root)
    accepted_names = {entry.get("name") for entry in state["pages"]
                      if isinstance(entry, dict)}
    retained: list[tuple[int, int]] = []
    for path in sorted(capture_root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        if _PAGE_NAME_PATTERN.fullmatch(name) is None:
            continue
        try:
            body, _receipt = _read_capture(capture_root, name)
        except UsLandslideRegionsError:
            _witness_aside(capture_root, name)
            continue
        if body.lstrip().startswith(b'{"error"') or name not in accepted_names:
            _witness_aside(capture_root, name)
            continue
        offset, count = _parse_page_name(name)
        if offset + count > len(roster):
            _witness_aside(capture_root, name)
            continue
        retained.append((offset, count))
    gaps: list[tuple[int, int]] = []
    covered = bytearray(len(roster))
    for offset, count in sorted(retained):
        if offset < 0 or count < 1 or offset + count > len(roster) \
                or any(covered[offset:offset + count]):
            _witness_aside(capture_root, _page_name(offset, count))
            continue
        covered[offset:offset + count] = b"\x01" * count
    cursor = 0
    while cursor < len(roster):
        if covered[cursor]:
            cursor += 1
            continue
        start = cursor
        while cursor < len(roster) and not covered[cursor]:
            cursor += 1
        chunk_start = start
        while chunk_start < cursor:
            count_chunk = min(PAGE_START_RECORDS, cursor - chunk_start)
            gaps.append((chunk_start, count_chunk))
            chunk_start += count_chunk
    return gaps


def _page_matches_slice(body: bytes, offset: int, count: int,
                        roster: list[int]) -> bool:
    """Check a candidate page against its expected id slice before accepting it."""
    try:
        page = json.loads(body)
    except ValueError:
        return False
    if not isinstance(page, dict) or "error" in page:
        return False
    rows = page.get("features")
    if not isinstance(rows, list) or len(rows) != count:
        return False
    expected = roster[offset:offset + count]
    actual = [row.get("attributes", {}).get("OBJECTID") for row in rows
              if isinstance(row, dict)]
    return actual == expected


def _fetch_pages(client: httpx.Client, capture_root: Path, roster: list[int],
                 state: dict[str, object]) -> bool:
    """Capture the complete layer as adaptively bisected roster pages.

    Pages start at PAGE_START_RECORDS. A page the producer keeps refusing is
    bisected until the producer serves both halves, whether the refusal is a
    persistent error document or a persistent incomplete transport capture;
    the refused page's final body and receipts stay on disk as evidence under
    witnessed names. A single-record page that still fails ends the capture
    explicitly. Returns whether this run accepted at least one new page.
    """
    pages = state.setdefault("pages", [])
    accepted = {entry.get("name") for entry in pages if isinstance(entry, dict)}
    refreshed = False
    todo: deque[tuple[int, int]] = deque(_plan_page_gaps(capture_root, roster))
    while todo:
        offset, count = todo.popleft()
        name = _page_name(offset, count)
        if name in accepted and (capture_root / f"{name}.body").exists() \
                and not (capture_root / f"{name}.body").read_bytes().lstrip().startswith(
                    b'{"error"'):
            continue
        url = _segment_query(offset, count)
        try:
            body, receipt = _capture_with_retry(
                client, url, capture_root, name,
                attempts=PAGE_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=PAGE_PROBE_BASE_DELAY_SECONDS if count > 1
                else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise UsLandslideRegionsError(
                "source_capture_failed:" + type(exc).__name__) from exc
        if _producer_error_doc(body):
            _witness_aside(capture_root, name)
            if count == 1:
                raise UsLandslideRegionsError(
                    "source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        if not _page_matches_slice(body, offset, count, roster):
            _witness_aside(capture_root, name)
            raise UsLandslideRegionsError(
                "source_page_id_slice_incomplete_or_reordered")
        if name not in accepted:
            pages.append({"name": name, "offset": offset, "count": count,
                          "features": count, "captured_at": receipt["observed_at"],
                          "body_sha256": _sha(body)})
            accepted.add(name)
            refreshed = True
            _save_state(capture_root, state)
    return refreshed


def fetch_us_landslide_regions_pages(
        capture_root: Path, *, client: httpx.Client | None = None) \
        -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page
    and document.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    _validate_retained_transport(capture_root)
    return _fetch_verified_capture(capture_root, client=client)


def _fetch_verified_capture(capture_root: Path, *, client: httpx.Client | None = None) -> dict[str, object]:
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    owner = client is None
    active = client or _default_client()
    pages: list[dict[str, object]] = list(state.get("pages", []))
    first_captured_at = (pages[0]["captured_at"] if pages
                         else datetime.now(timezone.utc).isoformat())
    try:
        metadata_urls = {"item_before": AGOL_ITEM_URL,
                         "service_before": SERVICE_URL + "?f=pjson",
                         "layer_before": LAYER_URL + "?f=pjson",
                         "count_before": _count_url(),
                         "ids_seed": _ids_url(),
                         "ids_before": _ids_url()}
        after_urls = {"layer_after": LAYER_URL + "?f=pjson",
                      "service_after": SERVICE_URL + "?f=pjson",
                      "count_after": _count_url(),
                      "ids_after": _ids_url(),
                      "item_after": AGOL_ITEM_URL}
        for name in METADATA_NAMES:
            body, _receipt = _retained_or_fetch(active, capture_root, name,
                                                metadata_urls[name])
            if _producer_error_doc(body):
                raise UsLandslideRegionsError(
                    "source_page_error:" + name)
        seed = json.loads(_read_capture(capture_root, "ids_seed")[0]).get("objectIds")
        ids = _validate_roster(seed, "ids_seed")
        before = json.loads(_read_capture(capture_root, "ids_before")[0])
        if before.get("objectIdFieldName") != "OBJECTID":
            raise UsLandslideRegionsError("source_id_field_changed")
        roster = _validate_roster(before.get("objectIds"), "ids_before")
        if roster != ids:
            raise UsLandslideRegionsError(
                "source_id_population_drift:ids_seed_vs_before")
        count_before = json.loads(_read_capture(capture_root, "count_before")[0])
        if type(count_before.get("count")) is not int \
                or count_before["count"] != len(roster):
            raise UsLandslideRegionsError("source_count_drift")
        refreshed = _fetch_pages(active, capture_root, roster, state)
        pages = list(state["pages"])
        # A recovered or resumed segment means the retained closing witnesses
        # no longer bound this capture, so all of them are witnessed aside and
        # refetched; otherwise the retained closing bracket is reused as-is.
        refresh_closing = refreshed or state.get("complete") is not True
        if not refresh_closing:
            for name in AFTER_NAMES:
                try:
                    body, _receipt = _read_capture(capture_root, name)
                    document = json.loads(body)
                    if not isinstance(document, dict) or "error" in document:
                        raise UsLandslideRegionsError("source_page_error:" + name)
                except (OSError, ValueError):
                    refresh_closing = True
                    break
        if refresh_closing:
            state["complete"] = False
            _save_state(capture_root, state)
            for name in AFTER_NAMES:
                _witness_aside(capture_root, name)
            for name in AFTER_NAMES:
                body, _receipt = capture_get(active, after_urls[name], capture_root,
                                             name, max_bytes=MAX_BODY_BYTES)
                if _producer_error_doc(body):
                    raise UsLandslideRegionsError("source_page_error:" + name)
        count_after = json.loads(_read_capture(capture_root, "count_after")[0])
        if type(count_after.get("count")) is not int \
                or count_after["count"] != len(roster):
            raise UsLandslideRegionsError(
                "source_metadata_or_count_changed_during_capture")
        after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
        if after_ids.get("objectIdFieldName") != "OBJECTID" \
                or _validate_roster(after_ids.get("objectIds"), "ids_after") != roster:
            raise UsLandslideRegionsError(
                "source_id_population_drift:ids_after")
    except UsLandslideRegionsError:
        if owner:
            active.close()
        raise
    except Exception as exc:
        if owner:
            active.close()
        raise UsLandslideRegionsError(
            "source_capture_failed:" + type(exc).__name__) from exc
    if owner:
        active.close()
    _save_state(capture_root, {"pages": pages, "complete": True})
    last_captured_at = max((str(entry.get("captured_at", "")) for entry in pages),
                           default=first_captured_at)
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": extracted["feature_count"],
        "source_reported_count": extracted["source_reported_count"],
        "source_total_vertex_count": extracted["total_vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_empty_geometry_count": extracted["empty_geometry_count"],
        "source_invalid_geometry_count": extracted["invalid_geometry_count"],
        "source_rings_per_feature_distribution":
            extracted["rings_per_feature_distribution"],
        "observed_bounds": extracted["observed_bounds"],
        "resumed_pages": len(pages),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    return {"capture_directory": str(capture_root), "pages": len(pages), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


class _PageStream:
    """Streaming per-page validator, digester and candidate parquet writer."""

    def __init__(self, capture_root: Path, fields: list[dict[str, object]],
                 parquet_sink: Path | None):
        self._capture_root = Path(capture_root).absolute()
        self.expected_names = tuple(entry["name"] for entry in fields)
        declared = {name: (esri_type, length)
                    for name, esri_type, length in NATIVE_FIELDS}
        if declared != {field["name"]: (field["type"], field.get("length"))
                        for field in fields}:
            raise UsLandslideRegionsError("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
                        for field in fields]
        arrow_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
        crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
        geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
               "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                             "geometry_types": ["Polygon Z",
                                                                "MultiPolygon Z"],
                                             "crs": crs, "edges": "planar"}}}
        self.schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
        self.parquet_sink = parquet_sink
        self.tmp_path = (parquet_sink.with_name(f"{parquet_sink.name}.construction-tmp")
                         if parquet_sink is not None else None)
        if self.tmp_path is not None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        # Never trust an existing output file: reconstruction always streams
        # from the validated pages into a temporary file that atomically
        # replaces the sink only after every check passes, so an interrupted
        # construction can never be sealed.
        self._writer = (pq.ParquetWriter(self.tmp_path, self.schema, compression="zstd",
                                         use_dictionary=False)
                        if self.tmp_path is not None else None)
        self.record_hash = hashlib.sha256()
        self.geometry_hash = hashlib.sha256()
        self.ids: array = array("q")
        self.last_objectid = 0
        self.null_geometry = 0
        self.empty_geometry = 0
        self.invalid_geometry = 0
        self.total_vertex_count = 0
        self.rings_per_feature_distribution: dict[int, int] = {}
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page_name: str, offset: int, count: int, roster: list[int]) -> None:
        """Digest one retained roster page into the digests and optional parquet sink."""
        if self._writer_closed:
            raise UsLandslideRegionsError("stream_writer_already_closed")
        page = json.loads(_read_capture(self._capture_root, page_name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise UsLandslideRegionsError(
                "source_page_error:" + page_name)
        reference = page.get("spatialReference")
        if not isinstance(reference, dict) \
                or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"] \
                or reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"]):
            raise UsLandslideRegionsError(
                "source_page_spatial_reference_drift:" + page_name)
        if page.get("geometryType") != GEOMETRY_TYPE:
            raise UsLandslideRegionsError(
                "source_page_geometry_type_drift:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise UsLandslideRegionsError(
                "source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, geometry, rings, vertices = _validate_feature(
                feature, expected_objectid)
            attrs = feature["attributes"]
            self.record_hash.update(_canonical(attrs) + b"\x00")
            self.ids.append(objectid)
            self.last_objectid = objectid
            wkb = None
            if geometry is None or rings is None:
                self.null_geometry += 1
            else:
                self.geometry_hash.update(_canonical([objectid, geometry]) + b"\x00")
                self.total_vertex_count += vertices
                ring_count = len(rings)
                self.rings_per_feature_distribution[ring_count] = \
                    self.rings_per_feature_distribution.get(ring_count, 0) + 1
                wkb = _geometry_wkb(geometry)
                parsed = shapely.from_wkb(wkb)
                if parsed.is_empty:
                    self.empty_geometry += 1
                elif not parsed.is_valid:
                    self.invalid_geometry += 1
                for ring in rings:
                    for x, y, _z in ring:
                        if self._xmin is None or x < self._xmin:
                            self._xmin = x
                        if self._ymin is None or y < self._ymin:
                            self._ymin = y
                        if self._xmax is None or x > self._xmax:
                            self._xmax = x
                        if self._ymax is None or y > self._ymax:
                            self._ymax = y
            geometries.append(wkb)
            for name in self.expected_names:
                columns[name].append(attrs[name])
        if self._writer is not None:
            batch = pa.Table.from_pydict({**columns, GEOMETRY_COLUMN: geometries},
                                         schema=self.schema)
            self._writer.write_table(batch)

    def close_writer(self) -> None:
        if not self._writer_closed:
            self._writer_closed = True
            if self._writer is not None:
                self._writer.close()
                self._writer = None

    def finish(self, parquet_ok: bool) -> dict[str, object]:
        self.close_writer()
        if self.tmp_path is not None:
            if parquet_ok:
                self.tmp_path.replace(self.parquet_sink)
            else:
                self.tmp_path.unlink(missing_ok=True)
        return {"record_sha": self.record_hash.hexdigest(),
                "geometry_sha": self.geometry_hash.hexdigest(),
                "ids_sha": _sha(_canonical(list(self.ids))),
                "null_geometry_count": self.null_geometry,
                "empty_geometry_count": self.empty_geometry,
                "invalid_geometry_count": self.invalid_geometry,
                "total_vertex_count": self.total_vertex_count,
                "rings_per_feature_distribution":
                    dict(sorted(self.rings_per_feature_distribution.items())),
                "max_objectid": self.last_objectid,
                "observed_bounds": None if self._xmin is None else
                {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax,
                 "ymax": self._ymax}}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    _preflight_capture(capture_root)
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES}
    _validate_item_document(docs["item_before"], "item_before")
    _validate_service_document(docs["service_before"], "service_before")
    fields, schema_sha, observed_layer_extent = _validate_layer_document(
        docs["layer_before"])
    declared = {name: (esri_type, length)
                for name, esri_type, length in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field.get("length"))
                    for field in fields}:
        raise UsLandslideRegionsError("source_schema_drift")
    if EXPECTED_SCHEMA_SHA256 and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise UsLandslideRegionsError("source_schema_baseline_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise UsLandslideRegionsError("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before")
    count_before = docs["count_before"].get("count")
    if type(count_before) is not int or count_before != len(roster):
        raise UsLandslideRegionsError("source_count_drift")
    if roster != seed:
        raise UsLandslideRegionsError(
            "source_id_population_drift:ids_seed_vs_before")
    after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
    if after_ids.get("objectIdFieldName") != "OBJECTID":
        raise UsLandslideRegionsError("source_id_field_changed")
    after_roster = _validate_roster(after_ids.get("objectIds"), "ids_after")
    if after_roster != roster:
        raise UsLandslideRegionsError(
            "source_id_population_drift:ids_after")
    for name in ("layer_after", "service_after", "count_after", "item_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before",
                  "item_after": "item_before"}[name]
        observed = json.loads(body)
        if name == "item_after":
            observed = {key: value for key, value in observed.items()
                        if key not in _ITEM_VOLATILE_FIELDS}
            mirror_doc = {key: value for key, value in docs[mirror].items()
                          if key not in _ITEM_VOLATILE_FIELDS}
        else:
            mirror_doc = docs[mirror]
        if observed != mirror_doc:
            raise UsLandslideRegionsError(
                "source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if (not isinstance(after_fields, list)
            or [_plain_field(x) for x in after_fields]
            != docs_fields_plain(docs["layer_before"])):
        raise UsLandslideRegionsError(
            "source_field_schema_changed_during_capture")
    state = _load_state(capture_root)
    pages = state.get("pages")
    if not isinstance(pages, list) or not pages \
            or any(not isinstance(item, dict) for item in pages):
        raise UsLandslideRegionsError(
            "source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(pages, key=lambda item: (item.get("offset", -1),
                                                  item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _page_name(offset, count) or offset != cursor
                or offset + count > len(roster) or entry.get("features") != count):
            raise UsLandslideRegionsError(
                "source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != len(roster):
        raise UsLandslideRegionsError(
            "source_capture_roster_incomplete_or_unexpected")
    for name in [*METADATA_NAMES, *AFTER_NAMES, *(item[0] for item in ordered)]:
        _validate_request_scope(name, _load_receipt(capture_root, name))
    stream = _PageStream(capture_root, fields, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        count_after = json.loads(
            _read_capture(capture_root, "count_after")[0]).get("count")
        try:
            validate_capture_witness(count_before, roster, count_after, after_roster,
                                     list(stream.ids))
        except ValueError as exc:
            raise UsLandslideRegionsError(str(exc)) from exc
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    ids_sha = summary["ids_sha"]
    return {"fields": fields, "schema_sha": schema_sha, "pages": len(ordered),
            "feature_count": len(stream.ids),
            "source_reported_count": count_before,
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": ids_sha, "parquet_path": parquet_sink,
            "null_geometry_count": summary["null_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "invalid_geometry_count": summary["invalid_geometry_count"],
            "total_vertex_count": summary["total_vertex_count"],
            "rings_per_feature_distribution":
                summary["rings_per_feature_distribution"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"],
            "declared_layer_extent_3857": observed_layer_extent}


def _code_identity() -> dict[str, object]:
    return processing_identity(REPO_ROOT)


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": USER_AGENT},
    )


def _load_receipt_map(capture_root: Path) -> dict[str, dict[str, object]]:
    """Load every expected receipt without holding any page body in memory."""
    state = _load_state(capture_root)
    names = [*METADATA_NAMES, *AFTER_NAMES,
             *(str(entry["name"]) for entry in state.get("pages", [])
               if isinstance(entry, dict) and entry.get("name"))]
    return {name: _load_receipt(capture_root, name) for name in names}


class _RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return _read_capture(self._capture_root, name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def _candidate_result(audit: Mapping[str, object], prefix: str, version: str, *,
                      reused: bool) -> dict[str, object]:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_reported_count": audit["source_reported_count"],
            "source_population_complete": audit["source_population_complete"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_empty_geometry_count": audit["empty_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "source_total_vertex_count": audit["total_vertex_count"],
            "source_rings_per_feature_distribution":
                audit["rings_per_feature_distribution"],
            "observed_bounds": audit["observed_bounds"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "reused": reused,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_pages: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    _preflight_capture(capture_root)
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    audit = {
        "source_feature_count": extracted["feature_count"],
        "source_id_sha256": extracted["ids_sha"],
        "source_reported_count": extracted["source_reported_count"],
        "source_population_complete": True,
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Polygon Z / MultiPolygon Z",
        "null_geometry_count": extracted["null_geometry_count"],
        "empty_geometry_count": extracted["empty_geometry_count"],
        "invalid_geometry_count": extracted["invalid_geometry_count"],
        "total_vertex_count": extracted["total_vertex_count"],
        "rings_per_feature_distribution":
            extracted["rings_per_feature_distribution"],
        "unique_source_id_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": extracted["declared_layer_extent_3857"],
        "completeness_basis": "stable captured counts and unique native ID rosters, "
                              "with exact ordered page coverage",
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL,
        "producer_agol_item": {"id": AGOL_ITEM_ID, "owner": AGOL_ITEM_OWNER,
                               "orgId": AGOL_ITEM_ORGID, "title": AGOL_ITEM_TITLE},
        "sciencebase_release": {"item_id": SCIENCEBASE_ITEM_ID, "doi": SCIENCEBASE_DOI,
                                "title": SCIENCEBASE_RELEASE_TITLE},
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": None,
        "crosswalk_check_note": "The layer is not a row of the DHS HIFLD Open "
                                "crosswalk (checked 2026-11-14 against the "
                                "byte-identical public copy sha256 " + CROSSWALK_COPY_SHA256
                                + "); the producer identity anchors above were used.",
        "crosswalk_public_copy_sha256": CROSSWALK_COPY_SHA256,
        "producer_service_refresh_statement": EXPECTED_SERVICE_DESCRIPTION,
        "source_release_date": "2025-02-03",
        "source_release_date_note": "ScienceBase lists lastRevision 2025-02-03 and "
                                    "us_ls_v3_changes.txt records 'Version 3 revised "
                                    "and reposted online February 3, 2025'; the layer's "
                                    "dataLastEditDate 2025-02-19T18:22Z is the v3 "
                                    "publication upload to the service.",
        "pagination": {"mode": "enumerated_roster_bisected_resultoffset_segments",
                       "page_start_records": PAGE_START_RECORDS,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "pages": extracted["pages"], "resumable": True,
                       "resumed_pages": resumed_pages,
                        "id_roster_enumeration_note":
                            "returnIdsOnly enumerated the complete native OBJECTID "
                            "roster at capture time. Pages request consecutive "
                            "roster windows by resultOffset/resultRecordCount "
                            "starting at 1000 rows below the producer's 2000-record "
                            "cap and refused pages are bisected until served; "
                            "accepted pages must tile the sorted roster exactly "
                            "and before/after count/roster witnesses prove nothing "
                            "was skipped, duplicated or reordered."},
        "capture_window_utc": {"first_page": first_captured_at,
                               "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    if storage.object_snapshot(state_key) is not None:
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        result = _verify_sealed_candidate(storage, prefix, state_key, version,
                                          identity, audit, parquet_body)
        return {**result, "capture_directory": str(capture_root)}
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body),
                                     "sha256": _sha(parquet_body)}}
    receipts = _load_receipt_map(capture_root)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "US Landslide Regions (USGS landslide inventory polygons)",
        "description": "USGS ArcGIS Online FeatureServer "
                       "US_Landslide_poly_v2/FeatureServer layer 12 "
                       "'US_Landslide_v3poly' (the polygon feature class of the USGS "
                       "data release 'Landslide Inventories across the United States "
                       "(ver. 3.0, February 2025)', DOI 10.5066/P14AJF8I), the layer "
                       "the HIFLD Open inventory entry us-landslide-regions "
                       "describes, captured completely from the producing agency's "
                       "own hosted publication. Complete source attributes and "
                       "native EPSG:3857 Z-aware polygon geometry are represented "
                       "in the candidate. Succession, freshness, schema/ID "
                       "contract, coverage, rights and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey",
        "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no "
                   "access or use constraints. Rights acceptance pending.",
        "source_modified": "2025-02-19", "date_issued": "2025-02-03",
        "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication", "canonical_url": SERVICE_URL,
                    "retrieval_url": receipt["url"],
                    "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"],
                    "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": "2025-02-03",
                    "release_date_note": "USGS data release ver. 3.0 reposted "
                                         "2025-02-03; layer dataLastEditDate "
                                         "2025-02-19T18:22Z.",
                    "license": "Public domain (U.S. Government work) per the producer "
                               "service; rights acceptance pending.",
                    "attribution": "Belair, G.M., Jones, E.S., Luna, L.V., and Mirus, "
                                   "B.B., 2019 (v3.0, February 2025), Landslide "
                                   "Inventories across the United States: U.S. "
                                   "Geological Survey data release, "
                                   "https://doi.org/10.5066/P14AJF8I."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {
                                 "layer_id": LAYER_ID,
                                 "population": "the complete enumerated OBJECTID roster "
                                               "captured as adaptively bisected "
                                               "resultOffset segments (full US "
                                               "Landslide v3 polygon population)",
                                 "source_fields": "all producer attribute fields "
                                                  "retained in producer order and "
                                                  "declared physical types",
                                 "source_ids": "Preserve producer OBJECTID and USGS_ID "
                                               "(ply<n> permanent polygon identifier) "
                                               "values unchanged; no substitute IDs, "
                                               "casts or joins",
                                 "geometry": "Native EPSG:3857 Z-aware polygon rings "
                                             "retained verbatim (served z ordinates "
                                             "carried through; clockwise shells and "
                                             "counter-clockwise holes classified "
                                             "strictly per the ArcGIS convention with "
                                             "unambiguous hole ownership) as Polygon "
                                             "Z / MultiPolygon Z WKB without "
                                             "rounding, winding repair, reprojection "
                                             "or geometry repair; topology validity "
                                             "is counted, never enforced",
                                 "candidate_status": "unapproved; publication blocked "
                                                     "pending succession, freshness, "
                                                     "schema/ID contract, coverage, "
                                                     "attribution and release "
                                                     "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry us-landslide-regions ('US Landslide "
                   "Regions', agency 'USGS - Rocky Mountain Region', date_issued "
                   "2024-05-15) carries verbatim the abstract of USGS data release "
                   "'Landslide Inventories across the United States (ver. 3.0, "
                   "February 2025)' (DOI 10.5066/P14AJF8I, ScienceBase item "
                   "671eef1fd34ed0f827ea9f12). The layer is absent from the DHS HIFLD "
                   "Open crosswalk (DOI 10.3886/E241367V1; checked 2026-11-14). The "
                   "producing agency publishes the data release's polygon feature "
                   "class itself on its own ArcGIS Online organization "
                   "(orgid " + AGOL_ITEM_ORGID + ") as item " + AGOL_ITEM_ID + " "
                   "'US_Landslide_poly_v3' owned by esjones@usgs.gov_USGS, whose "
                   "service URL is the FeatureServer captured here; the service name "
                   "keeps its v2 form while item title, snippet and layer "
                   "dataLastEditDate 2025-02-19T18:22Z record the version 3 "
                   "publication. This is the producer's own current, maintained "
                   "publication, not a mirror or successor substitute. The DataLumos "
                   "project 240751 archive copy (archive inventory modification "
                   "2025-02-19) and the HIFLD Next PEDP repackaging are "
                   "identification and comparison context only and are never "
                   "fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "Z-aware polygon geometry retained in the native page CRS; "
                      "all succession, freshness, contract, rights and release "
                      "approvals remain pending.",
            "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"],
             "succession_accepted": False, "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    metadata_outputs: dict[str, bytes] = {
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the seal never
    # holds more than one page body/wire pair plus the small receipt maps.
    descriptors: dict[str, dict[str, object]] = {}
    for relative, body in metadata_outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
        descriptors[relative] = {"bytes": len(body), "sha256": _sha(body)}
    for name in sorted(receipts):
        body, receipt = _read_capture(capture_root, name)
        wire = (capture_root / f"{name}.wire").read_bytes()
        for suffix, blob in (("body", body), ("wire", wire),
                             ("receipt.json", _canonical(receipt) + b"\n")):
            relative = f"raw/{name}.{suffix}"
            write_immutable(storage, f"{prefix}/{relative}", blob)
            descriptors[relative] = {"bytes": len(blob), "sha256": _sha(blob)}
    evidence = {path.name.removesuffix(suffix) for path in capture_root.iterdir()
                for suffix in (".body", ".wire", ".receipt.json") if path.name.endswith(suffix)} - set(receipts)
    for name in sorted(evidence):
        paths = [capture_root / f"{name}.{suffix}" for suffix in ("body", "wire", "receipt.json")]
        before = [path.stat() for path in paths]
        _validate_retained_transport(capture_root, names=(name,))
        blobs = [path.read_bytes() for path in paths]
        after = [path.stat() for path in paths]
        if any((a.st_dev, a.st_ino, a.st_size, a.st_mtime_ns, a.st_ctime_ns)
               != (b.st_dev, b.st_ino, b.st_size, b.st_mtime_ns, b.st_ctime_ns)
               for a, b in zip(before, after)):
            raise UsLandslideRegionsError("retained_capture_changed_during_admission:" + name)
        for suffix, blob in zip(("body", "wire", "receipt.json"), blobs):
            relative = f"evidence/refused/{name}.{suffix}"
            write_immutable(storage, f"{prefix}/{relative}", blob)
            descriptors[relative] = {"bytes": len(blob), "sha256": _sha(blob)}
    fingerprint = _sha(_canonical(identity))
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": receipts, "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    # identity= selects the scheme-aware validation branch: a packaged runtime
    # (/app without .git) carries a packaged-sha256: commit that the Git-only
    # branch would reject as construction_invalid_commit after a valid seal.
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"],
                                        identity["files"], identity=identity)
    return {**_candidate_result(audit, prefix, version, reused=False),
            "capture_directory": str(capture_root)}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str,
                             state_key: str, version: str,
                             identity: Mapping[str, object],
                             audit: Mapping[str, object], current_parquet: bytes) \
        -> dict[str, object]:
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity",
            "source_record_sha256", "source_schema_sha256", "capture_receipts",
            "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise UsLandslideRegionsError(
            "cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor
                        for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, REPO_ROOT, state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise UsLandslideRegionsError(
            "cached_native_candidate_changed")
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"],
                                        identity["files"], identity=identity)
    return _candidate_result(audit, prefix, version, reused=True)


def capture_us_landslide_regions(
        storage: StagingStorageResource, *, version: str,
        capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir \
            or storage.prefix:
        raise UsLandslideRegionsError(
            "explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "0123456789.-_" for c in version):
        raise UsLandslideRegionsError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise UsLandslideRegionsError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise UsLandslideRegionsError("disk_floor_before_acquisition")
    attempt_root = default_capture_dir("hifld-native-reconstruction", "368", "captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise UsLandslideRegionsError("unsafe_capture_directory")
    if capture_root.exists():
        _validate_retained_transport(capture_root)
    resumed_pages = len(_load_state(capture_root).get("pages", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        _fetch_verified_capture(capture_root)
        state = _load_state(capture_root)
        pages = [entry for entry in state["pages"] if isinstance(entry, dict)]
        captured = [str(entry.get("captured_at", "")) for entry in pages]
        extracted = _stream_validate(
            capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted,
                               resumed_pages=resumed_pages,
                               first_captured_at=captured[0] if captured else "",
                               last_captured_at=captured[-1] if captured else "")
    except Exception as exc:
        valid_names = []
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) \
                if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                _read_capture(capture_root, name)
            except (OSError, UsLandslideRegionsError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root,
                                                                valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_us_landslide_regions_baseline(
        capture_root: str) -> dict[str, object]:
    """Recompute observed audit facts from a complete retained capture."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": extracted["feature_count"],
            "source_reported_count": extracted["source_reported_count"],
            "source_total_vertex_count": extracted["total_vertex_count"],
            "source_max_objectid": extracted["max_objectid"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_invalid_geometry_count": extracted["invalid_geometry_count"],
            "source_rings_per_feature_distribution":
                extracted["rings_per_feature_distribution"],
            "observed_bounds": extracted["observed_bounds"]}
