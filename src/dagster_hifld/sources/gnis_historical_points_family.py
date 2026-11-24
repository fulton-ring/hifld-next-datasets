"""Shared adapter for the USGS GNIS historical point layers.

The ``carto.nationalmap.gov`` ``geonames`` MapServer serves three historical
point layers (12 "Historical Cultural-Political Points", 13 "Historical
Hydrographic Points", 14 "Historical Physical Points") with one shared
schema, CRS and acquisition contract. This module holds the capture,
validation, replay, baseline and candidate code once; each layer is one
frozen ``GnisHistoricalPointLayer`` spec plus the call-time values a run may
patch (``GnisHistoricalPointRuntime``) defined in its dataset module.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import partial
import hashlib
import json
import re
import shutil
import struct
from array import array
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.sources import esri_geometry as crs_validation
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_adaptive import AdaptiveCaptureSpec, capture_adaptive_layer, page_matches_id_slice
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    content_derived_version,
    retain_failed_capture,
    verify_cached_candidate,
    reuse_sealed_candidate,
    load_candidate_checkpoint,
    stage_candidate_objects,
    load_receipt,
    read_capture,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import (
    capture_witness, capture_with_retry,
    producer_error_doc, preserve_failed_attempt,
    witness_aside,
    save_state as _save_state,
)

SERVICE_HOST = "https://carto.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/geonames/MapServer"
SERVICE_DOC_URL = "https://www.usgs.gov/us-board-on-geographic-names/download-gnis-data"
GEOMETRY_TYPE = "esriGeometryMultipoint"
PUBLISHER = "U.S. Geological Survey"
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
SOURCE_CRS_WKID = NATIVE_LAYER_SR["latestWkid"]
GEOMETRY_COLUMN = "geometry"
# EPSG:3857 declared domain.
WEB_MERCATOR_X_LIMIT = 20_037_508.342789244
WEB_MERCATOR_Y_LIMIT = 20_048_966.1040146
# The producer's maxRecordCount is 2000; pages start at half the cap so a
# refused page can be bisected while staying below the cap at every step.
MAX_RECORD_COUNT = 2000
MAX_BODY_BYTES = 96 * 1024 * 1024
MAX_CAPTURE_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 2_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
PAGE_PROBE_ATTEMPTS = 3
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
BASELINE_PENDING_NOTE = (
    "Completeness is enforced from the producer's own before/after count and "
    "OBJECTID roster at capture time; observed value histograms and bounds are "
    "diagnostics, not release baselines.")
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


class GnisHistoricalPointsError(ValueError):
    """The source snapshot cannot be represented as the pinned candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class GnisHistoricalPointLayer:
    """One frozen GNIS historical point layer: identity and per-layer pins."""

    dataset_slug: str
    file_slug: str
    layer_id: int
    layer_name: str
    user_agent: str
    metadata_drift_code: str
    expected_layer_extent: Mapping[str, float]
    manifest_title: str
    manifest_description: str
    manifest_lineage: str


@dataclass(frozen=True)
class GnisHistoricalPointRuntime:
    """Call-time values a dataset module exposes and its tests may patch."""

    repo_root: Path
    page_start_records: int
    page_probe_delay: float
    doc_delay: float
    expected_schema_sha256: str
    default_client: Callable[[], httpx.Client]
    code_identity: Callable[[], dict[str, object]]
    validate_code_identity: Callable[..., None]


def layer_url(spec: GnisHistoricalPointLayer) -> str:
    return SERVICE_URL + f"/{spec.layer_id}"


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable",
                                        "domain") if key in field}


def _profile_schema(layer: Mapping[str, object], error: type) \
        -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise error("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def docs_fields_plain(layer: Mapping[str, object], error: type) \
        -> list[dict[str, object]]:
    """Plain-field projection used for the before/after field-schema mirror."""
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise error("source_field_roster_drift")
    return [_plain_field(x) for x in fields]


def _query(spec: GnisHistoricalPointLayer, params: dict[str, object]) -> str:
    return layer_url(spec) + "/query?" + urlencode(params)


def _count_url(spec: GnisHistoricalPointLayer) -> str:
    return _query(spec, {"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url(spec: GnisHistoricalPointLayer) -> str:
    return _query(spec, {"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _page_url(spec: GnisHistoricalPointLayer, ids_slice: list[int]) -> str:
    return _query(spec, {"objectIds": ",".join(str(value) for value in ids_slice),
                   "outFields": "*", "returnGeometry": "true", "returnZ": "true",
                   "returnM": "true", "orderByFields": "OBJECTID ASC", "f": "json"})


_PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str, error: type) -> tuple[int, int]:
    match = _PAGE_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise error(
            "source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str, error: type) -> list[int]:
    """Require a unique positive-integer native ID roster from one witness."""
    if not isinstance(roster, list) or not roster \
            or any(type(value) is not int or value <= 0 for value in roster) \
            or len(set(roster)) != len(roster):
        raise error(
            "source_id_population_drift:" + label)
    return sorted(roster)


def _spatial_reference_validator(error: type) -> Callable:
    return partial(
        crs_validation.validate_native_wkid_pair,
        native_wkid=NATIVE_LAYER_SR["wkid"], native_latest_wkid=NATIVE_LAYER_SR["latestWkid"],
        error_type=error, missing_error="source_spatial_reference_missing:{label}",
        drift_error="source_spatial_reference_drift:{label}")


def _validate_service_document(service: object, label: str, error: type) -> None:
    if not isinstance(service, dict):
        raise error(
            "source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise error(
            "gnis_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list) or roster != EXPECTED_SERVICE_LAYERS:
        raise error(
            "gnis_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "Geographic Names Information System (GNIS)" not in description:
        raise error(
            "gnis_service_description_drift:" + label)
    _spatial_reference_validator(error)(service.get("spatialReference"), label)


def _validate_layer_document(layer: object, spec: GnisHistoricalPointLayer,
                            error: type) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise error(
            "source_service_or_layer_document_invalid")
    if (layer.get("id") != spec.layer_id or layer.get("name") != spec.layer_name
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != GEOMETRY_TYPE
            or (layer.get("geometryField") or {}).get("name") != GEOMETRY_FIELD_NAME):
        raise error(spec.metadata_drift_code)
    if layer.get("hasZ") is not True or layer.get("hasM") is True:
        raise error(
            "source_layer_z_or_m_contract_drift")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise error("source_record_limit_drift")
    advanced = layer.get("advancedQueryCapabilities")
    if not isinstance(advanced, dict) or advanced.get("supportsPagination") is not True:
        raise error(
            "source_pagination_capability_changed")
    source_reference = layer.get("sourceSpatialReference")
    if not isinstance(source_reference, dict) \
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise error(
            "source_layer_spatial_reference_drift")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise error("source_layer_extent_missing")
    _spatial_reference_validator(error)(extent.get("spatialReference"), "layer_extent")
    if {key: extent.get(key) for key in spec.expected_layer_extent} != spec.expected_layer_extent:
        raise error("source_layer_extent_drift")
    plain, schema_sha = _profile_schema(layer, error)
    geometry_fields = [field for field in plain
                       if field.get("type") == "esriFieldTypeGeometry"]
    if [field.get("name") for field in geometry_fields] != [GEOMETRY_FIELD_NAME]:
        raise error("source_field_roster_drift")
    attributes = [field for field in plain
                  if field.get("type") != "esriFieldTypeGeometry"]
    if ([field.get("name") for field in attributes] != list(EXPECTED_ATTRIBUTE_NAMES)
            or [(field.get("name"), field.get("type"), field.get("length"))
                for field in attributes] != [entry for entry in NATIVE_FIELDS]):
        raise error("source_schema_drift")
    for field in attributes:
        if field["type"] not in _ARROW_TYPES:
            raise error(
                "unsupported_source_physical_type:" + str(field["type"]))
    return attributes, schema_sha


def _arrow_type(field: Mapping[str, object], error: type) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise error(
            "unsupported_source_physical_type:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, value: object, error: type) -> None:
    if value is None:
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise error(
                "source_native_type_changed:" + name)
        return
    if esri_type in INTEGER_TYPES or esri_type == "esriFieldTypeDate":
        if type(value) is not int:
            raise error(
                "source_native_type_changed:" + name)
        return
    if esri_type in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        if type(value) not in (int, float) or not (value == value
                                                   and -1e308 < value < 1e308):
            raise error(
                "source_native_type_changed:" + name)
        return
    raise error(
        "unsupported_source_physical_type:" + esri_type)


def _validate_multipoint(geometry: object, error: type) \
        -> list[tuple[float, float, float]]:
    """Validate one native Z-aware multipoint geometry and return its ordinates.

    Every accepted feature geometry is ``{"points": [[x, y, z], ...]}`` with
    optional ``hasZ`` (must be true when present) and an optional spatial
    reference equal to the page CRS. Ordinates are validated numerically and
    range-checked in the EPSG:3857 domain, then passed through verbatim.
    """
    if not isinstance(geometry, dict) or "points" not in geometry \
            or set(geometry) - {"points", "hasZ", "spatialReference"}:
        raise error(
            "unexpected_source_geometry_members")
    if "hasZ" in geometry and geometry["hasZ"] is not True:
        raise error("source_multipoint_z_flag_drift")
    if "spatialReference" in geometry:
        _spatial_reference_validator(error)(geometry["spatialReference"], "feature")
    rows = geometry["points"]
    if not isinstance(rows, list) or not rows \
            or any(not isinstance(row, list) for row in rows):
        raise error("source_multipoint_rows_drift")
    points: list[tuple[float, float, float]] = []
    for row in rows:
        if len(row) != 3:
            raise error(
                "source_multipoint_ordinate_count_drift")
        for value in row:
            if type(value) not in (int, float):
                raise error(
                    "source_point_ordinate_not_numeric")
            if not (value == value and -1e308 < value < 1e308):
                raise error(
                    "source_point_ordinate_not_finite")
        x, y = row[0], row[1]
        if not (-WEB_MERCATOR_X_LIMIT <= x <= WEB_MERCATOR_X_LIMIT
                and -WEB_MERCATOR_Y_LIMIT <= y <= WEB_MERCATOR_Y_LIMIT):
            raise error(
                "source_coordinate_range_drift")
        points.append((row[0], row[1], row[2]))
    return points


def _multipoint_wkb(points: list[tuple[float, float, float]]) -> bytes:
    """Store validated native ordinates verbatim as MultiPoint Z WKB."""
    body = struct.pack("<BII", 1, 0x80000004, len(points))
    for x, y, z in points:
        body += struct.pack("<BI3d", 1, 0x80000001, x, y, z)
    return body


def _validate_feature(feature: object, expected_objectid: int, error: type) \
        -> tuple[int, dict | None, list[tuple[float, float, float]] | None]:
    """Validate one feature's attributes and multipoint geometry structure.

    Returns the OBJECTID, the verbatim geometry dict (or None), and the
    validated ordinate rows (or None when the feature carries no geometry).
    """
    if not isinstance(feature, dict):
        raise error("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(EXPECTED_ATTRIBUTE_NAMES):
        raise error(
            "source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise error("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise error(
            "source_page_id_slice_incomplete_or_reordered")
    points = None
    if geometry is not None:
        points = _validate_multipoint(geometry, error)
    for name, esri_type, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, attrs[name], error)
    return objectid, geometry, points


def _validate_request_scope(name: str, receipt: Mapping[str, object],
                            roster: list[int], spec: GnisHistoricalPointLayer,
                            error: type) -> None:
    """Pin every retained request to this producer, layer and roster slice."""
    url = receipt.get("url")
    if not isinstance(url, str):
        raise error(
            "source_capture_endpoint_drift:" + name)
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "carto.nationalmap.gov":
        raise error(
            "source_capture_endpoint_drift:" + name)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if name in ("service_before", "service_after"):
        expected_path, expected = urlsplit(SERVICE_URL).path, {"f": ["pjson"]}
    elif name in ("layer_before", "layer_after"):
        expected_path, expected = urlsplit(layer_url(spec)).path, {"f": ["pjson"]}
    elif name.startswith("count_"):
        expected_path, expected = urlsplit(layer_url(spec)).path + "/query", {
            "where": ["1=1"], "returnCountOnly": ["true"], "f": ["json"]}
    elif name.startswith("ids_"):
        expected_path, expected = urlsplit(layer_url(spec)).path + "/query", {
            "where": ["1=1"], "returnIdsOnly": ["true"], "f": ["json"]}
    elif _PAGE_NAME_PATTERN.fullmatch(name):
        offset, count = _parse_page_name(name, error)
        if offset + count > len(roster):
            raise error(
                "source_capture_request_scope_drift:" + name)
        expected_path, expected = urlsplit(layer_url(spec)).path + "/query", {
            "objectIds": [",".join(str(value) for value in roster[offset:offset + count])],
            "outFields": ["*"], "returnGeometry": ["true"], "returnZ": ["true"],
            "returnM": ["true"], "orderByFields": ["OBJECTID ASC"], "f": ["json"]}
    else:
        raise error(
            "source_capture_request_scope_drift:" + name)
    if parsed.path != expected_path or params != expected:
        raise error(
            "source_capture_request_scope_drift:" + name)


def _load_state(capture_root: Path, error: type) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"pages": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("pages"), list):
        raise error("capture_state_invalid")
    return state


def _read_capture(capture_root: Path, name: str, error: type) -> tuple[bytes, dict[str, object]]:
    return read_capture(capture_root, name, error)


def _load_receipt(capture_root: Path, name: str, error: type) -> dict[str, object]:
    return load_receipt(capture_root, name, error)


def _adaptive_capture_spec(spec: GnisHistoricalPointLayer, error: type,
                          runtime: GnisHistoricalPointRuntime) -> AdaptiveCaptureSpec:
    return AdaptiveCaptureSpec(
        byte_budget=MAX_CAPTURE_BYTES,
        service_url=SERVICE_URL, layer_url=layer_url(spec),
        count_url=_count_url(spec), ids_url=_ids_url(spec),
        metadata_names=tuple(METADATA_NAMES), after_names=tuple(AFTER_NAMES),
        page_size=runtime.page_start_records, page_pattern=_PAGE_NAME_PATTERN,
        page_name=_page_name,
        parse_page_name=lambda name: _parse_page_name(name, error),
        page_url=lambda roster, offset, count: _page_url(spec, roster[offset:offset + count]),
        page_matches=page_matches_id_slice,
        validate_roster=lambda roster, label: _validate_roster(roster, label, error),
        load_state=lambda root: _load_state(root, error), save_state=_save_state,
        read_capture=lambda root, name: _read_capture(root, name, error),
        witness_aside=witness_aside,
        capture=partial(capture_with_retry, max_bytes=MAX_BODY_BYTES), producer_error=producer_error_doc,
        default_client=runtime.default_client, error=error,
        page_attempts=PAGE_PROBE_ATTEMPTS, page_delay=runtime.page_probe_delay,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, doc_delay=runtime.doc_delay,
        reconcile_invalidated_pages=False,
    )


def fetch_gnis_historical_points_pages(spec: GnisHistoricalPointLayer, error: type,
                                       runtime: GnisHistoricalPointRuntime,
                                       capture_root: Path, *,
                                       client: httpx.Client | None = None) \
        -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page
    and document.
    """
    acquisition = capture_adaptive_layer(_adaptive_capture_spec(spec, error, runtime),
                                         capture_root, client=client)
    capture_root = acquisition.root
    roster, pages = acquisition.roster, acquisition.pages
    first_captured_at = acquisition.first_captured_at
    last_captured_at = acquisition.last_captured_at
    extracted = _stream_validate(spec, error, runtime, capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": len(roster),
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
                 error: type, parquet_sink: Path | None):
        self._capture_root = Path(capture_root).absolute()
        self._error = error
        self.expected_names = tuple(entry["name"] for entry in fields)
        declared = {name: (esri_type, length)
                    for name, esri_type, length in NATIVE_FIELDS}
        if declared != {field["name"]: (field["type"], field.get("length"))
                        for field in fields}:
            raise self._error("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field, self._error), nullable=True)
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
            raise self._error("stream_writer_already_closed")
        page = json.loads(_read_capture(self._capture_root, page_name, self._error)[0])
        if not isinstance(page, dict) or "error" in page:
            raise self._error(
                "source_page_error:" + page_name)
        reference = page.get("spatialReference")
        if not isinstance(reference, dict) \
                or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"] \
                or reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"]):
            raise self._error(
                "source_page_spatial_reference_drift:" + page_name)
        if page.get("geometryType") != GEOMETRY_TYPE:
            raise self._error(
                "source_page_geometry_type_drift:" + page_name)
        if page.get("hasZ") is not True:
            raise self._error(
                "source_page_z_flag_missing:" + page_name)
        # The layer's multipoint pages omit hasM entirely (hasM is null in the
        # layer document); a present hasM must still be false.
        if "hasM" in page and page.get("hasM") is not False:
            raise self._error(
                "source_page_m_flag_drift:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise self._error(
                "source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, geometry, points = _validate_feature(feature, expected_objectid, self._error)
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


def _stream_validate(spec: GnisHistoricalPointLayer, error: type,
                     runtime: GnisHistoricalPointRuntime, capture_root: Path,
                     *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    docs = {name: json.loads(_read_capture(capture_root, name, error)[0])
            for name in METADATA_NAMES}
    _validate_service_document(docs["service_before"], "service_before", error)
    fields, schema_sha = _validate_layer_document(docs["layer_before"], spec, error)
    declared = {name: (esri_type, length)
                for name, esri_type, length in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field.get("length"))
                    for field in fields}:
        raise error("source_schema_drift")
    if runtime.expected_schema_sha256 and schema_sha != runtime.expected_schema_sha256:
        raise error("source_schema_baseline_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed", error)
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise error("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before", error)
    if roster != seed:
        raise error("source_id_population_drift:ids_seed_vs_before")
    after_roster = _validate_roster(json.loads(
        _read_capture(capture_root, "ids_after", error)[0]).get("objectIds"), "ids_after", error)
    if after_roster != roster:
        raise error("source_id_population_drift:ids_after")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name, error)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise error("source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after", error)[0]).get("fields")
    if (not isinstance(after_fields, list)
            or [_plain_field(x) for x in after_fields]
            != docs_fields_plain(docs["layer_before"], error)):
        raise error("source_field_schema_changed_during_capture")
    state = _load_state(capture_root, error)
    pages = state.get("pages")
    if not isinstance(pages, list) or not pages \
            or any(not isinstance(item, dict) for item in pages):
        raise error("source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(pages, key=lambda item: (item.get("offset", -1),
                                                 item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _page_name(offset, count) or offset != cursor
                or offset + count > len(roster) or entry.get("features") != count):
            raise error("source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != len(roster):
        raise error("source_capture_roster_incomplete_or_unexpected")
    for name in [*METADATA_NAMES, *AFTER_NAMES, *(item[0] for item in ordered)]:
        _validate_request_scope(name, _load_receipt(capture_root, name, error),
                                 roster, spec, error)
    stream = _PageStream(capture_root, fields, error, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    if len(stream.ids) != len(roster) or summary["max_objectid"] <= 0:
        raise error("source_page_population_incomplete")
    capture_witness(
        {name: _read_capture(capture_root, name, error)
         for name in ("count_before", "ids_before", "count_after", "ids_after")},
        captured_ids=list(stream.ids))
    return {"fields": fields, "schema_sha": schema_sha, "pages": len(ordered),
            "feature_count": len(stream.ids),
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": summary["ids_sha"], "parquet_path": parquet_sink,
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


def _load_receipt_map(capture_root: Path, error: type) -> dict[str, dict[str, object]]:
    """Load every expected receipt without holding any page body in memory."""
    state = _load_state(capture_root, error)
    names = [*METADATA_NAMES, *AFTER_NAMES,
             *(str(entry["name"]) for entry in state.get("pages", [])
               if isinstance(entry, dict) and entry.get("name"))]
    return {name: _load_receipt(capture_root, name, error) for name in names}


def _seal_candidate(spec: GnisHistoricalPointLayer, error: type,
                    runtime: GnisHistoricalPointRuntime, storage: StagingStorageResource,
                    capture_root: Path, extracted: Mapping[str, object], *,
                    resumed_pages: int, first_captured_at: str,
                    last_captured_at: str) -> dict[str, object]:
    identity = runtime.code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(spec.dataset_slug, spec.file_slug, version)
    state_key = f"_construction-state/{spec.dataset_slug}/{version}.json"

    def verify_existing():
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            spec, error, runtime, storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"],
             "source_feature_count": extracted["feature_count"],
             "null_geometry_count": extracted["null_geometry_count"],
             "source_id_sha256": extracted["ids_sha"]},
            parquet_body)

    reused = reuse_sealed_candidate(storage, state_key, verify_existing)
    if reused is not None:
        return reused
    audit = {
        "source_feature_count": extracted["feature_count"],
        "source_id_sha256": extracted["ids_sha"],
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
        "unique_source_id_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": dict(spec.expected_layer_extent),
        "source_fields": extracted["fields"],
        "official_layer_name": spec.layer_name, "layer_id": spec.layer_id,
        "service_url": SERVICE_URL, "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": CROSSWALK_ROW,
        "crosswalk_open_id": CROSSWALK_OPEN_ID,
        "crosswalk_public_copy_sha256": CROSSWALK_COPY_SHA256,
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": True,
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The geonames MapServer publishes no per-snapshot release date; the "
            "service copyrightText records a service refresh (October 2026) and "
            "USGS documents that GNIS names data products are refreshed every "
            "other month. Neither is a dataset release date.",
        "pagination": {"mode": "enumerated_roster_bisected_objectids_pages",
                       "page_start_records": runtime.page_start_records,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "pages": extracted["pages"], "resumable": True,
                       "resumed_pages": resumed_pages,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerated the complete OBJECTID roster "
                           "at capture time; pages start below the producer's "
                           "record cap and refused pages are bisected until served; "
                           "accepted pages must tile the sorted roster exactly and "
                           "before/after count/roster brackets prove nothing was "
                           "skipped, duplicated or reordered."},
        "capture_window_utc": {"first_page": first_captured_at,
                               "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{spec.dataset_slug}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body),
                                     "sha256": _sha(parquet_body)}}
    receipts = _load_receipt_map(capture_root, error)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": spec.manifest_title,
        "description": spec.manifest_description,
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey",
        "source_url": layer_url(spec),
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
                                 "layer_id": spec.layer_id,
                                 "population": "the complete enumerated OBJECTID roster "
                                               "captured as adaptively bisected "
                                               "objectIds pages (full "
                                               f"{spec.layer_name} gazetteer population)",
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
        "lineage": spec.manifest_lineage,
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
    stage_candidate_objects(
        storage, prefix, state_key, version, identity, metadata_outputs,
        capture_root, receipts, partial(_read_capture, error=error),
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'])
    # identity= selects the scheme-aware validation branch: a packaged runtime
    # (/app without .git) carries a packaged-sha256: commit that the Git-only
    # branch would reject as construction_invalid_commit after a valid seal.
    runtime.validate_code_identity(runtime.repo_root, identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": spec.dataset_slug, "file_slug": spec.file_slug,
            "version": version, "capture_complete": True,
            "feature_count": extracted["feature_count"],
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


def _verify_sealed_candidate(spec: GnisHistoricalPointLayer, error: type,
                             runtime: GnisHistoricalPointRuntime,
                             storage: StagingStorageResource, prefix: str,
                             state_key: str, version: str,
                             identity: Mapping[str, object],
                             audit: Mapping[str, object], current_parquet: bytes) \
        -> dict[str, object]:
    fingerprint, payload = load_candidate_checkpoint(
        storage, state_key, version, identity,
        audit['source_record_sha256'], audit['source_schema_sha256'], error)
    expected_objects = {name: descriptor
                        for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, runtime.repo_root, state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{spec.dataset_slug}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise error(
            "cached_native_candidate_changed")
    runtime.validate_code_identity(runtime.repo_root, identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": spec.dataset_slug, "file_slug": spec.file_slug,
            "version": version, "capture_complete": True,
            "feature_count": audit["source_feature_count"],
            "source_null_geometry_count": audit.get("null_geometry_count"),
            "source_id_sha256": audit.get("source_id_sha256", ""),
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def capture_gnis_historical_points(spec: GnisHistoricalPointLayer, error: type,
                                   runtime: GnisHistoricalPointRuntime,
                                   storage: StagingStorageResource, *,
                                   version: str,
                                   capture_directory: str | None = None) \
        -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir \
            or storage.prefix:
        raise error(
            "explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "0123456789.-_" for c in version):
        raise error("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise error("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise error("disk_floor_before_acquisition")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (default_capture_dir(spec.dataset_slug, "captures") / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise error("unsafe_capture_directory")
    resumed_pages = len(_load_state(capture_root, error).get("pages", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_gnis_historical_points_pages(spec, error, runtime, capture_root)
        state = _load_state(capture_root, error)
        pages = [entry for entry in state["pages"] if isinstance(entry, dict)]
        captured = [str(entry.get("captured_at", "")) for entry in pages]
        extracted = _stream_validate(
            spec, error, runtime, capture_root,
            parquet_sink=capture_root / f"{spec.file_slug}.parquet")
        return _seal_candidate(spec, error, runtime, storage, capture_root, extracted,
                               resumed_pages=resumed_pages,
                               first_captured_at=captured[0] if captured else "",
                               last_captured_at=captured[-1] if captured else "")
    except Exception as exc:
        retain_failed_capture(storage, capture_root, spec.dataset_slug, version, exc,
                              error=error)
        raise


def build_gnis_historical_points_baseline(spec: GnisHistoricalPointLayer, error: type,
                                          runtime: GnisHistoricalPointRuntime,
                                          capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(spec, error, runtime,
                                 Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": extracted["feature_count"],
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
