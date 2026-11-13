"""Approval-gated draft capture of the USGS GNIS Historical Cultural-Political Points.

The HIFLD Open inventory entry ``nmg/historical-cultural-political-points-1``
("Historical Cultural-Political Points", agency U.S. Geological Survey) carries
the description of The National Map Gazetteer / Geographic Names Information
System (GNIS) verbatim. The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1;
the byte-identical public copy is sha256
7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc) consolidates
the whole GNIS family into one row, "Geographic Names Information System (GNIS)
Feature Layers" (Status Deprecate, DUA/GII not required, Agency U.S.
Geological Survey), whose Open REST Service is the producer's own
``carto.nationalmap.gov`` ``geonames`` MapServer. Its layer 12 "Historical
Cultural-Political Points" carries exactly the inventory title and serves the
maintained producer publication captured here (service copyrightText: "USGS
The National Map: Geographic Names Information System. Data Refreshed October,
2026."). The layer held 16,115 features on 2026-11-13, so this module retains
every raw response in a resumable capture directory before any candidate is
sealed. The DataLumos project 240195 archive copy and the Esri "U.S. Federal
Datasets" repackaging are identification context only and are never fetched.

The producer serves the complete OBJECTID roster (``returnIdsOnly`` returned
exactly 16,115 ids spanning 2..19460 with gaps on 2026-11-13), so the layer is
captured as adaptively bisected ``objectIds`` pages of the sorted roster: each
accepted page must return exactly its expected id slice, the accepted pages
must tile the complete roster without gaps or overlaps, and before/after
count/roster/layer/service brackets prove that nothing was skipped,
duplicated, or reordered. A single-record page the producer still refuses ends
the capture explicitly rather than guessing.

Geometry is the layer's native Z-aware multipoint: every feature geometry is
``{"points": [[x, y, z], ...]}`` in the page CRS (EPSG:3857), one location per
gazetteer point and several points for features with multiple recorded
locations. Ordinates are validated numerically and range-checked in the Web
Mercator domain, then stored verbatim as MultiPoint Z WKB: no rounding,
reordering, reprojection or repair is applied.

The per-value and per-geometry baseline digests, gate counts and observed
bounds are computed and reported by
``build_gnis_historical_cultural_political_points_baseline`` from a completed
capture directory and pinned as constants for later drift refusal; pins left
empty (or ``None``) are checked only once pinned. Publication stays blocked
pending the succession, freshness, schema/ID contract, rights and release
decisions; all runtime approval flags are false and the draft refresh schedule
is stopped.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import struct
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

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "historical-cultural-political-points-1"
FILE_SLUG = DATASET_SLUG
# Repository/package root: in the shipped container layout (/app/src/dagster_hifld)
# this resolves to /app, which carries no .git, so the processing identity uses the
# packaged scheme. Tests redirect this constant to a no-Git packaged layout.
REPO_ROOT = Path(__file__).parents[3]
SERVICE_HOST = "https://carto.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/geonames/MapServer"
SERVICE_DOC_URL = "https://www.usgs.gov/us-board-on-geographic-names/download-gnis-data"
LAYER_ID = 12
LAYER_NAME = "Historical Cultural-Political Points"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryMultipoint"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/historical-cultural-political-points-draft"
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
SOURCE_CRS_WKID = NATIVE_LAYER_SR["latestWkid"]
GEOMETRY_COLUMN = "geometry"
# EPSG:3857 declared domain.
WEB_MERCATOR_X_LIMIT = 20_037_508.342789244
WEB_MERCATOR_Y_LIMIT = 20_048_966.1040146
# The producer's maxRecordCount is 2000; pages start at half the cap so a
# refused page can be bisected while staying below the cap at every step.
MAX_RECORD_COUNT = 2000
PAGE_START_RECORDS = 1000
MAX_BODY_BYTES = 96 * 1024 * 1024
MAX_CAPTURE_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 2_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_PROBE_ATTEMPTS = 3
PAGE_PROBE_BASE_DELAY_SECONDS = 5.0
# Producer-stated service refresh ("Data Refreshed October, 2026" in the captured
# service copyrightText) is freshness evidence, not a release date.
EXPECTED_COPYRIGHT_TEXT = ("USGS The National Map: Geographic Names Information System. "
                           "Data Refreshed October, 2026.")
EXPECTED_SERVICE_LAYERS = [
    (0, "Places (expand for more)"),
    (1, "Incorporated Places (Civil)"),
    (2, "Unincorporated Places (Census)"),
    (3, "Populated Places"),
    (4, "Physical Points (expand for more)"),
    (5, "Landforms"),
    (6, "Streams (Mouth)"),
    (7, "Other Hydrographic Features"),
    (8, "Antarctica"),
    (9, "Cultural Points (expand for more)"),
    (10, "Crossings"),
    (11, "Historical Points (expand for more)"),
    (12, "Historical Cultural-Political Points"),
    (13, "Historical Hydrographic Points"),
    (14, "Historical Physical Points"),
]
EXPECTED_COUNT = 16_115
EXPECTED_LAYER_EXTENT = {"xmin": -19818952.8156, "ymin": -1608688.4560999982,
                         "xmax": 19325062.035899997, "ymax": 11526405.634300001}
# The per-value and per-geometry baselines, geometry gate counts and observed
# bounds are pinned from the dated complete capture (2026-11-13); unpinned
# values ("", None) are checked only once pinned.
EXPECTED_IDS_SHA256 = ""
EXPECTED_SCHEMA_SHA256 = ""
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_EMPTY_GEOMETRY_COUNT = 0
EXPECTED_TOTAL_POINT_COUNT = None
# GNIS gazetteer per-feature value histograms observed via grouped counts on
# 2026-11-13 (sum 16,115); every feature must carry one of these values.
EXPECTED_FEATURECLASS_DISTRIBUTION = {"Census": 1003, "Crossing": 2428,
                                      "Military": 1919, "Populated Place": 10765}
EXPECTED_ISUNKNOWNCOORDS_DISTRIBUTION = {2: 16115}
EXPECTED_MULTIPOINT_POINT_COUNT_DISTRIBUTION: dict[int, int] | None = None
EXPECTED_Z_ORDINATE_DISTRIBUTION: dict[float, int] | None = None
EXPECTED_OBSERVED_BOUNDS: dict[str, float] | None = None
BASELINE_PENDING_NOTE = (
    "The record and geometry baseline digests, geometry gate counts and "
    "observed bounds are pinned from a completed resumable capture directory "
    "by build_gnis_historical_cultural_political_points_baseline; unpinned "
    "values are checked only once pinned.")
CROSSWALK_ROW = "Geographic Names Information System (GNIS) Feature Layers"
CROSSWALK_OPEN_ID = "5091b822ad3e47f3b6bc5bc275fb3c22"
CROSSWALK_COPY_SHA256 = ("7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38"
                         "772bc")

NATIVE_FIELDS: tuple[tuple[str, str, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", None),
    ("gaz_id", "esriFieldTypeInteger", None),
    ("gaz_name", "esriFieldTypeString", 120),
    ("gaz_featureclass", "esriFieldTypeString", 50),
    ("fcode", "esriFieldTypeInteger", None),
    ("state_alpha", "esriFieldTypeString", 1073741822),
    ("county_name", "esriFieldTypeString", 100),
    ("objectid_1", "esriFieldTypeInteger", None),
    ("isunknowncoords", "esriFieldTypeInteger", None),
    ("incounty_id", "esriFieldTypeInteger", None),
)
EXPECTED_ATTRIBUTE_NAMES = tuple(name for name, _type, _length in NATIVE_FIELDS)
GEOMETRY_FIELD_NAME = "shape"
METADATA_NAMES = ("service_before", "layer_before", "count_before", "ids_seed",
                  "ids_before")
AFTER_NAMES = ("layer_after", "service_after", "count_after", "ids_after")
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
    # Esri JSON delivers date fields as raw epoch milliseconds; the producer's
    # exact numeric wire values are retained verbatim as int64 with no
    # timezone or calendar interpretation applied.
    "esriFieldTypeDate": pa.int64(),
}


class GnisHistoricalCulturalPoliticalPointsError(ValueError):
    """The source snapshot cannot be represented as the pinned candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable",
                                        "domain") if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise GnisHistoricalCulturalPoliticalPointsError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def docs_fields_plain(layer: Mapping[str, object]) -> list[dict[str, object]]:
    """Plain-field projection used for the before/after field-schema mirror."""
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise GnisHistoricalCulturalPoliticalPointsError("source_field_roster_drift")
    return [_plain_field(x) for x in fields]


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _page_url(ids_slice: list[int]) -> str:
    return _query({"objectIds": ",".join(str(value) for value in ids_slice),
                   "outFields": "*", "returnGeometry": "true", "returnZ": "true",
                   "returnM": "true", "orderByFields": "OBJECTID ASC", "f": "json"})


_PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    match = _PAGE_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str) -> list[int]:
    if not isinstance(roster, list) or len(roster) != EXPECTED_COUNT \
            or any(type(value) is not int for value in roster) \
            or len(set(roster)) != EXPECTED_COUNT:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_id_population_drift:" + label)
    ids = sorted(roster)
    if EXPECTED_IDS_SHA256 and _sha(_canonical(ids)) != EXPECTED_IDS_SHA256:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_id_roster_drift:" + label)
    return ids


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_spatial_reference_missing:" + label)
    if (value.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]
            or value.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_spatial_reference_drift:" + label)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "gnis_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list) or roster != EXPECTED_SERVICE_LAYERS:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "gnis_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "Geographic Names Information System (GNIS)" not in description:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "gnis_service_description_drift:" + label)
    _validate_spatial_reference(service.get("spatialReference"), label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != GEOMETRY_TYPE
            or (layer.get("geometryField") or {}).get("name") != GEOMETRY_FIELD_NAME):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "gnis_historical_cultural_political_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is True:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_layer_z_or_m_contract_drift")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise GnisHistoricalCulturalPoliticalPointsError("source_record_limit_drift")
    advanced = layer.get("advancedQueryCapabilities")
    if not isinstance(advanced, dict) or advanced.get("supportsPagination") is not True:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_pagination_capability_changed")
    source_reference = layer.get("sourceSpatialReference")
    if not isinstance(source_reference, dict) \
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_layer_spatial_reference_drift")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise GnisHistoricalCulturalPoliticalPointsError("source_layer_extent_missing")
    _validate_spatial_reference(extent.get("spatialReference"), "layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise GnisHistoricalCulturalPoliticalPointsError("source_layer_extent_drift")
    plain, schema_sha = _profile_schema(layer)
    geometry_fields = [field for field in plain
                       if field.get("type") == "esriFieldTypeGeometry"]
    if [field.get("name") for field in geometry_fields] != [GEOMETRY_FIELD_NAME]:
        raise GnisHistoricalCulturalPoliticalPointsError("source_field_roster_drift")
    attributes = [field for field in plain
                  if field.get("type") != "esriFieldTypeGeometry"]
    if ([field.get("name") for field in attributes] != list(EXPECTED_ATTRIBUTE_NAMES)
            or [(field.get("name"), field.get("type"), field.get("length"))
                for field in attributes] != [entry for entry in NATIVE_FIELDS]):
        raise GnisHistoricalCulturalPoliticalPointsError("source_schema_drift")
    for field in attributes:
        if field["type"] not in _ARROW_TYPES:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "unsupported_source_physical_type:" + str(field["type"]))
    return attributes, schema_sha


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "unsupported_source_physical_type:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, value: object) -> None:
    if value is None:
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_native_type_changed:" + name)
        return
    if esri_type in INTEGER_TYPES or esri_type == "esriFieldTypeDate":
        if type(value) is not int:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_native_type_changed:" + name)
        return
    if esri_type in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        if type(value) not in (int, float) or not (value == value
                                                   and -1e308 < value < 1e308):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_native_type_changed:" + name)
        return
    raise GnisHistoricalCulturalPoliticalPointsError(
        "unsupported_source_physical_type:" + esri_type)


def _validate_multipoint(geometry: object) -> list[tuple[float, float, float]]:
    """Validate one native Z-aware multipoint geometry and return its ordinates.

    Every accepted feature geometry is ``{"points": [[x, y, z], ...]}`` with
    optional ``hasZ`` (must be true when present) and an optional spatial
    reference equal to the page CRS. Ordinates are validated numerically and
    range-checked in the EPSG:3857 domain, then passed through verbatim.
    """
    if not isinstance(geometry, dict) or "points" not in geometry \
            or set(geometry) - {"points", "hasZ", "spatialReference"}:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "unexpected_source_geometry_members")
    if "hasZ" in geometry and geometry["hasZ"] is not True:
        raise GnisHistoricalCulturalPoliticalPointsError("source_multipoint_z_flag_drift")
    if "spatialReference" in geometry:
        _validate_spatial_reference(geometry["spatialReference"], "feature")
    rows = geometry["points"]
    if not isinstance(rows, list) or not rows \
            or any(not isinstance(row, list) for row in rows):
        raise GnisHistoricalCulturalPoliticalPointsError("source_multipoint_rows_drift")
    points: list[tuple[float, float, float]] = []
    for row in rows:
        if len(row) != 3:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_multipoint_ordinate_count_drift")
        for value in row:
            if type(value) not in (int, float):
                raise GnisHistoricalCulturalPoliticalPointsError(
                    "source_point_ordinate_not_numeric")
            if not (value == value and -1e308 < value < 1e308):
                raise GnisHistoricalCulturalPoliticalPointsError(
                    "source_point_ordinate_not_finite")
        x, y = row[0], row[1]
        if not (-WEB_MERCATOR_X_LIMIT <= x <= WEB_MERCATOR_X_LIMIT
                and -WEB_MERCATOR_Y_LIMIT <= y <= WEB_MERCATOR_Y_LIMIT):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_coordinate_range_drift")
        points.append((row[0], row[1], row[2]))
    return points


def _multipoint_wkb(points: list[tuple[float, float, float]]) -> bytes:
    """Store validated native ordinates verbatim as MultiPoint Z WKB."""
    body = struct.pack("<BII", 1, 0x80000004, len(points))
    for x, y, z in points:
        body += struct.pack("<BI3d", 1, 0x80000001, x, y, z)
    return body


def _validate_feature(feature: object, expected_objectid: int) \
        -> tuple[int, dict | None, list[tuple[float, float, float]] | None]:
    """Validate one feature's attributes and multipoint geometry structure.

    Returns the OBJECTID, the verbatim geometry dict (or None), and the
    validated ordinate rows (or None when the feature carries no geometry).
    """
    if not isinstance(feature, dict):
        raise GnisHistoricalCulturalPoliticalPointsError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(EXPECTED_ATTRIBUTE_NAMES):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise GnisHistoricalCulturalPoliticalPointsError("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_page_id_slice_incomplete_or_reordered")
    featureclass = attrs["gaz_featureclass"]
    if featureclass not in EXPECTED_FEATURECLASS_DISTRIBUTION:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_featureclass_outside_gazetteer_population")
    if attrs["isunknowncoords"] not in EXPECTED_ISUNKNOWNCOORDS_DISTRIBUTION:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_isunknowncoords_value_drift")
    points = None
    if geometry is not None:
        points = _validate_multipoint(geometry)
    for name, esri_type, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, attrs[name])
    return objectid, geometry, points


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = _load_receipt(capture_root, name)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire)
            or receipt.get("wire_sha256") != _sha(wire)):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "retained_capture_receipt_invalid:" + name)
    return body, receipt


def _validate_request_scope(name: str, receipt: Mapping[str, object],
                            roster: list[int]) -> None:
    """Pin every retained request to this producer, layer and roster slice."""
    url = receipt.get("url")
    if not isinstance(url, str):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_endpoint_drift:" + name)
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "carto.nationalmap.gov":
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_endpoint_drift:" + name)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if name in ("service_before", "service_after"):
        expected_path, expected = urlsplit(SERVICE_URL).path, {"f": ["pjson"]}
    elif name in ("layer_before", "layer_after"):
        expected_path, expected = urlsplit(LAYER_URL).path, {"f": ["pjson"]}
    elif name.startswith("count_"):
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "where": ["1=1"], "returnCountOnly": ["true"], "f": ["json"]}
    elif name.startswith("ids_"):
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "where": ["1=1"], "returnIdsOnly": ["true"], "f": ["json"]}
    elif _PAGE_NAME_PATTERN.fullmatch(name):
        offset, count = _parse_page_name(name)
        if offset + count > len(roster):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_capture_request_scope_drift:" + name)
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "objectIds": [",".join(str(value) for value in roster[offset:offset + count])],
            "outFields": ["*"], "returnGeometry": ["true"], "returnZ": ["true"],
            "returnM": ["true"], "orderByFields": ["OBJECTID ASC"], "f": ["json"]}
    else:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_request_scope_drift:" + name)
    if parsed.path != expected_path or params != expected:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_request_scope_drift:" + name)


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"pages": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GnisHistoricalCulturalPoliticalPointsError("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("pages"), list):
        raise GnisHistoricalCulturalPoliticalPointsError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


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
        except GnisHistoricalCulturalPoliticalPointsError:
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
            # Witnessed refusal evidence (page_*.error-*.body) and other
            # non-canonical names are not roster pages; skip them.
            continue
        try:
            body, _receipt = _read_capture(capture_root, name)
        except GnisHistoricalCulturalPoliticalPointsError:
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
                 state: dict[str, object]) -> None:
    """Capture the complete layer as adaptively bisected roster pages.

    Pages start at PAGE_START_RECORDS. A page the producer keeps refusing is
    bisected until the producer serves both halves, whether the refusal is a
    persistent error document or a persistent incomplete transport capture;
    the refused page's final body and receipts stay on disk as evidence under
    witnessed names. A single-record page that still fails ends the capture
    explicitly.
    """
    pages = state.setdefault("pages", [])
    accepted = {entry.get("name") for entry in pages if isinstance(entry, dict)}
    todo: deque[tuple[int, int]] = deque(_plan_page_gaps(capture_root, roster))
    while todo:
        offset, count = todo.popleft()
        name = _page_name(offset, count)
        if name in accepted and (capture_root / f"{name}.body").exists() \
                and not (capture_root / f"{name}.body").read_bytes().lstrip().startswith(
                    b'{"error"'):
            continue
        url = _page_url(roster[offset:offset + count])
        try:
            body, receipt = _capture_with_retry(
                client, url, capture_root, name,
                attempts=PAGE_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=PAGE_PROBE_BASE_DELAY_SECONDS if count > 1
                else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            # The producer cap can also manifest as truncation/connection reset
            # instead of a clean error document; a multi-record page whose
            # attempts were still incomplete is bisected like an error
            # document, while a single-record page keeps failing explicitly.
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_capture_failed:" + type(exc).__name__) from exc
        if _producer_error_doc(body):
            _witness_aside(capture_root, name)
            if count == 1:
                raise GnisHistoricalCulturalPoliticalPointsError(
                    "source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        if not _page_matches_slice(body, offset, count, roster):
            _witness_aside(capture_root, name)
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_id_slice_incomplete_or_reordered")
        if name not in accepted:
            pages.append({"name": name, "offset": offset, "count": count,
                          "features": count, "captured_at": receipt["observed_at"],
                          "body_sha256": _sha(body)})
            accepted.add(name)
            _save_state(capture_root, state)


def fetch_gnis_historical_cultural_political_points_pages(
        capture_root: Path, *, client: httpx.Client | None = None) \
        -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page
    and document.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    owner = client is None
    active = client or _default_client()
    pages: list[dict[str, object]] = list(state.get("pages", []))
    first_captured_at = (pages[0]["captured_at"] if pages
                         else datetime.now(timezone.utc).isoformat())
    try:
        for name in METADATA_NAMES:
            url = {"service_before": SERVICE_URL + "?f=pjson",
                   "layer_before": LAYER_URL + "?f=pjson",
                   "count_before": _count_url(),
                   "ids_seed": _ids_url(),
                   "ids_before": _ids_url()}[name]
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            if _producer_error_doc(body):
                raise GnisHistoricalCulturalPoliticalPointsError(
                    "source_page_error:" + name)
        seed = json.loads(_read_capture(capture_root, "ids_seed")[0]).get("objectIds")
        ids = _validate_roster(seed, "ids_seed")
        before = json.loads(_read_capture(capture_root, "ids_before")[0])
        if before.get("objectIdFieldName") != "OBJECTID":
            raise GnisHistoricalCulturalPoliticalPointsError("source_id_field_changed")
        roster = _validate_roster(before.get("objectIds"), "ids_before")
        if roster != ids:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_id_population_drift:ids_seed_vs_before")
        count_before = json.loads(_read_capture(capture_root, "count_before")[0])
        if count_before.get("count") != EXPECTED_COUNT:
            raise GnisHistoricalCulturalPoliticalPointsError("source_count_drift")
        _fetch_pages(active, capture_root, roster, state)
        pages = list(state["pages"])
        for name in AFTER_NAMES:
            url = {"layer_after": LAYER_URL + "?f=pjson",
                   "service_after": SERVICE_URL + "?f=pjson",
                   "count_after": _count_url(),
                   "ids_after": _ids_url()}[name]
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            if _producer_error_doc(body):
                raise GnisHistoricalCulturalPoliticalPointsError(
                    "source_page_error:" + name)
        count_after = json.loads(_read_capture(capture_root, "count_after")[0])
        if count_after.get("count") != EXPECTED_COUNT:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_metadata_or_count_changed_during_capture")
        after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
        if after_ids.get("objectIdFieldName") != "OBJECTID" \
                or _validate_roster(after_ids.get("objectIds"), "ids_after") != roster:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_id_population_drift:ids_after")
    except GnisHistoricalCulturalPoliticalPointsError:
        if owner:
            active.close()
        raise
    except Exception as exc:
        if owner:
            active.close()
        raise GnisHistoricalCulturalPoliticalPointsError(
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
        "source_feature_count": EXPECTED_COUNT,
        "source_total_point_count": extracted["total_point_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_empty_geometry_count": extracted["empty_geometry_count"],
        "source_featureclass_distribution": extracted["featureclass_distribution"],
        "source_isunknowncoords_distribution": extracted["isunknowncoords_distribution"],
        "source_multipoint_point_count_distribution":
            extracted["multipoint_point_count_distribution"],
        "source_z_ordinate_distribution": extracted["z_ordinate_distribution"],
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
            raise GnisHistoricalCulturalPoliticalPointsError("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
                        for field in fields]
        arrow_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
        crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
        geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
               "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                             "geometry_types": ["MultiPoint Z"],
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
        self.total_point_count = 0
        self.point_count_distribution: dict[int, int] = {}
        self.z_ordinate_distribution: dict[float, int] = {}
        self.featureclass_distribution: dict[str, int] = {}
        self.isunknowncoords_distribution: dict[int, int] = {}
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page_name: str, offset: int, count: int, roster: list[int]) -> None:
        """Digest one retained roster page into the digests and optional parquet sink."""
        if self._writer_closed:
            raise GnisHistoricalCulturalPoliticalPointsError("stream_writer_already_closed")
        page = json.loads(_read_capture(self._capture_root, page_name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_error:" + page_name)
        reference = page.get("spatialReference")
        if not isinstance(reference, dict) \
                or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"] \
                or reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"]):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_spatial_reference_drift:" + page_name)
        if page.get("geometryType") != GEOMETRY_TYPE:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_geometry_type_drift:" + page_name)
        if page.get("hasZ") is not True:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_z_flag_missing:" + page_name)
        # The layer's multipoint pages omit hasM entirely (hasM is null in the
        # layer document); a present hasM must still be false.
        if "hasM" in page and page.get("hasM") is not False:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_m_flag_drift:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, geometry, points = _validate_feature(feature, expected_objectid)
            attrs = feature["attributes"]
            self.record_hash.update(_canonical(feature) + b"\x00")
            self.ids.append(objectid)
            self.last_objectid = objectid
            featureclass = attrs["gaz_featureclass"]
            self.featureclass_distribution[featureclass] = \
                self.featureclass_distribution.get(featureclass, 0) + 1
            unknown = attrs["isunknowncoords"]
            self.isunknowncoords_distribution[unknown] = \
                self.isunknowncoords_distribution.get(unknown, 0) + 1
            if geometry is None or points is None:
                self.null_geometry += 1
                geometries.append(None)
            else:
                self.geometry_hash.update(_canonical([objectid, points]) + b"\x00")
                self.point_count_distribution[len(points)] = \
                    self.point_count_distribution.get(len(points), 0) + 1
                self.total_point_count += len(points)
                for x, y, z in points:
                    self.z_ordinate_distribution[z] = \
                        self.z_ordinate_distribution.get(z, 0) + 1
                    if self._xmin is None or x < self._xmin:
                        self._xmin = x
                    if self._ymin is None or y < self._ymin:
                        self._ymin = y
                    if self._xmax is None or x > self._xmax:
                        self._xmax = x
                    if self._ymax is None or y > self._ymax:
                        self._ymax = y
                geometries.append(_multipoint_wkb(points))
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
                "total_point_count": self.total_point_count,
                "multipoint_point_count_distribution":
                    dict(sorted(self.point_count_distribution.items())),
                "z_ordinate_distribution":
                    dict(sorted(self.z_ordinate_distribution.items())),
                "featureclass_distribution":
                    dict(sorted(self.featureclass_distribution.items())),
                "isunknowncoords_distribution":
                    dict(sorted(self.isunknowncoords_distribution.items())),
                "max_objectid": self.last_objectid,
                "observed_bounds": None if self._xmin is None else
                {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax,
                 "ymax": self._ymax}}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES}
    _validate_service_document(docs["service_before"], "service_before")
    fields, schema_sha = _validate_layer_document(docs["layer_before"])
    declared = {name: (esri_type, length)
                for name, esri_type, length in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field.get("length"))
                    for field in fields}:
        raise GnisHistoricalCulturalPoliticalPointsError("source_schema_drift")
    if EXPECTED_SCHEMA_SHA256 and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise GnisHistoricalCulturalPoliticalPointsError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise GnisHistoricalCulturalPoliticalPointsError("source_count_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise GnisHistoricalCulturalPoliticalPointsError("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before")
    if roster != seed:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_id_population_drift:ids_seed_vs_before")
    after_roster = _validate_roster(json.loads(
        _read_capture(capture_root, "ids_after")[0]).get("objectIds"), "ids_after")
    if after_roster != roster:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_id_population_drift:ids_after")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if (not isinstance(after_fields, list)
            or [_plain_field(x) for x in after_fields]
            != docs_fields_plain(docs["layer_before"])):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_field_schema_changed_during_capture")
    state = _load_state(capture_root)
    pages = state.get("pages")
    if not isinstance(pages, list) or not pages \
            or any(not isinstance(item, dict) for item in pages):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(pages, key=lambda item: (item.get("offset", -1),
                                                 item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _page_name(offset, count) or offset != cursor
                or offset + count > EXPECTED_COUNT or entry.get("features") != count):
            raise GnisHistoricalCulturalPoliticalPointsError(
                "source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != EXPECTED_COUNT:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_capture_roster_incomplete_or_unexpected")
    for name in [*METADATA_NAMES, *AFTER_NAMES, *(item[0] for item in ordered)]:
        _validate_request_scope(name, _load_receipt(capture_root, name), roster)
    stream = _PageStream(capture_root, fields, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    if len(stream.ids) != EXPECTED_COUNT or summary["max_objectid"] <= 0:
        raise GnisHistoricalCulturalPoliticalPointsError("source_page_population_incomplete")
    ids_sha = summary["ids_sha"]
    if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
        raise GnisHistoricalCulturalPoliticalPointsError("source_id_roster_drift")
    if summary["featureclass_distribution"] != EXPECTED_FEATURECLASS_DISTRIBUTION:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_featureclass_distribution_drift")
    if summary["isunknowncoords_distribution"] != EXPECTED_ISUNKNOWNCOORDS_DISTRIBUTION:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_isunknowncoords_distribution_drift")
    if (EXPECTED_MULTIPOINT_POINT_COUNT_DISTRIBUTION is not None
            and summary["multipoint_point_count_distribution"]
            != EXPECTED_MULTIPOINT_POINT_COUNT_DISTRIBUTION):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_multipoint_point_count_distribution_drift")
    if (EXPECTED_TOTAL_POINT_COUNT is not None
            and summary["total_point_count"] != EXPECTED_TOTAL_POINT_COUNT):
        raise GnisHistoricalCulturalPoliticalPointsError("source_total_point_count_drift")
    if (EXPECTED_Z_ORDINATE_DISTRIBUTION is not None
            and summary["z_ordinate_distribution"] != EXPECTED_Z_ORDINATE_DISTRIBUTION):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_z_ordinate_distribution_drift")
    if EXPECTED_RECORD_SHA256 and summary["record_sha"] != EXPECTED_RECORD_SHA256:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_value_or_geometry_baseline_drift")
    if EXPECTED_GEOMETRY_SHA256 and summary["geometry_sha"] != EXPECTED_GEOMETRY_SHA256:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_geometry_or_coverage_drift")
    if (EXPECTED_NULL_GEOMETRY_COUNT is not None
            and summary["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_null_geometry_count_drift")
    if (EXPECTED_EMPTY_GEOMETRY_COUNT is not None
            and summary["empty_geometry_count"] != EXPECTED_EMPTY_GEOMETRY_COUNT):
        raise GnisHistoricalCulturalPoliticalPointsError(
            "source_empty_geometry_count_drift")
    if (EXPECTED_OBSERVED_BOUNDS is not None
            and summary["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS):
        raise GnisHistoricalCulturalPoliticalPointsError("source_observed_bounds_drift")
    return {"fields": fields, "schema_sha": schema_sha, "pages": len(ordered),
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": ids_sha, "parquet_path": parquet_sink,
            "null_geometry_count": summary["null_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "total_point_count": summary["total_point_count"],
            "multipoint_point_count_distribution":
                summary["multipoint_point_count_distribution"],
            "z_ordinate_distribution": summary["z_ordinate_distribution"],
            "featureclass_distribution": summary["featureclass_distribution"],
            "isunknowncoords_distribution": summary["isunknowncoords_distribution"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"]}


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


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_pages: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"]},
            parquet_body)
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "MultiPoint Z",
        "null_geometry_count": extracted["null_geometry_count"],
        "empty_geometry_count": extracted["empty_geometry_count"],
        "total_point_count": extracted["total_point_count"],
        "multipoint_point_count_distribution":
            extracted["multipoint_point_count_distribution"],
        "z_ordinate_distribution": extracted["z_ordinate_distribution"],
        "featureclass_distribution": extracted["featureclass_distribution"],
        "isunknowncoords_distribution": extracted["isunknowncoords_distribution"],
        "unique_source_id_count": EXPECTED_COUNT,
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": CROSSWALK_ROW,
        "crosswalk_open_id": CROSSWALK_OPEN_ID,
        "crosswalk_public_copy_sha256": CROSSWALK_COPY_SHA256,
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": bool(
            not EXPECTED_RECORD_SHA256 or not EXPECTED_GEOMETRY_SHA256),
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The geonames MapServer publishes no per-snapshot release date; the "
            "service copyrightText records a service refresh (October 2026) and "
            "USGS documents that GNIS names data products are refreshed every "
            "other month. Neither is a dataset release date.",
        "pagination": {"mode": "enumerated_roster_bisected_objectids_pages",
                       "page_start_records": PAGE_START_RECORDS,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "pages": extracted["pages"], "resumable": True,
                       "resumed_pages": resumed_pages,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerated the complete OBJECTID roster "
                           "(16,115 ids spanning 2..19460 with gaps, observed "
                           "2026-11-13). Pages start at 1000 ids below the producer's "
                           "2000-record cap and refused pages are bisected until "
                           "served; accepted pages must tile the sorted roster "
                           "exactly and before/after count/roster brackets prove "
                           "nothing was skipped, duplicated or reordered."},
        "capture_window_utc": {"first_page": first_captured_at,
                               "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body),
                                     "sha256": _sha(parquet_body)}}
    receipts = _load_receipt_map(capture_root)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Historical Cultural-Political Points (USGS GNIS gazetteer)",
        "description": "USGS The National Map Gazetteer (GNIS) geonames MapServer layer "
                       "12 'Historical Cultural-Political Points' (the exact HIFLD Open "
                       "inventory title nmg/historical-cultural-political-points-1), "
                       "captured completely from the producer's own REST publication. "
                       "Complete source attributes and native EPSG:3857 Z-aware "
                       "multipoint geometry are represented in the candidate. "
                       "Succession, freshness, schema/ID contract, coverage, rights and "
                       "release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey",
        "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no "
                   "access or use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication", "canonical_url": SERVICE_URL,
                    "retrieval_url": receipt["url"],
                    "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"],
                    "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason":
                        "The geonames MapServer publishes no per-snapshot release "
                        "date; its copyrightText records a service refresh (October "
                        "2026) and USGS documents that GNIS names data products are "
                        "refreshed every other month.",
                    "license": "Public domain (U.S. Government work) per the producer "
                               "service; rights acceptance pending.",
                    "attribution": "USGS The National Map: Geographic Names "
                                   "Information System (GNIS); formal attribution "
                                   "terms require maintainer review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {
                                 "layer_id": LAYER_ID,
                                 "population": "the complete enumerated OBJECTID roster "
                                               "captured as adaptively bisected "
                                               "objectIds pages (full Historical "
                                               "Cultural-Political Points gazetteer "
                                               "population)",
                                 "source_fields": "all producer attribute fields "
                                                  "retained in producer order and "
                                                  "declared physical types",
                                 "source_ids": "Preserve producer OBJECTID and gaz_id "
                                               "(GNIS permanent Feature ID) values "
                                               "unchanged; no substitute IDs, casts "
                                               "or joins",
                                 "geometry": "Native EPSG:3857 multipoint ordinates "
                                             "(x, y, z) retained verbatim as "
                                             "MultiPoint Z WKB without rounding, "
                                             "projection or repair",
                                 "candidate_status": "unapproved; publication blocked "
                                                     "pending succession, freshness, "
                                                     "schema/ID contract, coverage, "
                                                     "attribution and release "
                                                     "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nmg/"
                   "historical-cultural-political-points-1 ('Historical "
                   "Cultural-Political Points', agency U.S. Geological Survey) carries "
                   "the description of The National Map Gazetteer / GNIS verbatim. The "
                   "DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, byte-identical "
                   "public copy sha256 " + CROSSWALK_COPY_SHA256 + ", consolidated row '"
                   + CROSSWALK_ROW + "', Status Deprecate, Open ID " + CROSSWALK_OPEN_ID
                   + ") directs the GNIS-family layers to the carto.nationalmap.gov "
                   "geonames MapServer captured here; its layer 12 carries exactly the "
                   "inventory title and the service copyrightText states 'Data "
                   "Refreshed October, 2026'. This is the producer's own current, "
                   "maintained publication (a continuation of the layer HIFLD Open "
                   "pointed at), not a successor substitute. The DataLumos project "
                   "240195 archive copy (archive inventory modification 2025-03-20) "
                   "and the Esri 'U.S. Federal Datasets' repackaging are identification "
                   "and comparison context only and are never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "Z-aware multipoint geometry retained in the native page CRS; "
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
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_total_point_count": extracted["total_point_count"],
            "source_featureclass_distribution": extracted["featureclass_distribution"],
            "source_id_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False,
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
        raise GnisHistoricalCulturalPoliticalPointsError(
            "cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor
                        for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, REPO_ROOT, state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "cached_native_candidate_changed")
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": EXPECTED_NULL_GEOMETRY_COUNT,
            "source_id_sha256": audit.get("source_ids_sha256", ""),
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def capture_gnis_historical_cultural_political_points(
        storage: StagingStorageResource, *, version: str,
        capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir \
            or storage.prefix:
        raise GnisHistoricalCulturalPoliticalPointsError(
            "explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "0123456789.-_" for c in version):
        raise GnisHistoricalCulturalPoliticalPointsError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise GnisHistoricalCulturalPoliticalPointsError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise GnisHistoricalCulturalPoliticalPointsError("disk_floor_before_acquisition")
    attempt_root = Path("/data/workspace/scratch/historical-cultural-political-points-1"
                        "/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise GnisHistoricalCulturalPoliticalPointsError("unsafe_capture_directory")
    resumed_pages = len(_load_state(capture_root).get("pages", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_gnis_historical_cultural_political_points_pages(capture_root)
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
            except (OSError, GnisHistoricalCulturalPoliticalPointsError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root,
                                                                valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_gnis_historical_cultural_political_points_baseline(
        capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": EXPECTED_COUNT,
            "source_total_point_count": extracted["total_point_count"],
            "source_max_objectid": extracted["max_objectid"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_featureclass_distribution": extracted["featureclass_distribution"],
            "source_isunknowncoords_distribution":
                extracted["isunknowncoords_distribution"],
            "source_multipoint_point_count_distribution":
                extracted["multipoint_point_count_distribution"],
            "source_z_ordinate_distribution": extracted["z_ordinate_distribution"],
            "observed_bounds": extracted["observed_bounds"]}
