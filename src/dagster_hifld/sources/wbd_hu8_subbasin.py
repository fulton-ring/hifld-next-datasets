"""Complete native capture of the USGS WBD 8-digit HU (Subbasin) layer.

The HIFLD Open inventory entry ``wbd/8-digit-hu-subbasin`` ("8-digit HU
(Subbasin)", agency US Geological Survey, publisher "U.S. Geological Survey,
National Geospatial Technical Operations Center") was a Watershed Boundary
Dataset (WBD) feature service on the retired DHS HIFLD Open portal. The DHS
HIFLD Open crosswalk (August 2025) row "Watershed Boundary Dataset (WBD)"
marks the former GeoPlatform item as Deprecate with "No longer supported as
part of the HIFLD mission" and names the producing agency's own publication as
the successor Open REST Service:
``https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer``. That
USGS The National Map service is the maintained publication captured here;
layer 4 "8-digit HU  (Subbasin)" (the layer name carries a double space)
matches the inventory entry exactly. Its copyrightText states "USGS WBD -
Watershed Boundary Dataset. Data refreshed October, 2026.", which is the
producer's own freshness statement for the current service, not a release
date. USGS also publishes an annual "authoritative WBD snapshot" while the
program shifts focus to the 3DHP data model
(``https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/
Hydrography/WBD/National/``, staged product files dated 2026-09-02); the
service is the fresher maintained publication and the staged snapshot is
comparison context only. The DataLumos project 240701 archive copy is
identification context only and is never fetched.

The capture pages the complete enumerated native OBJECTID roster
in resultOffset/resultRecordCount segments, because this MapServer
layer refuses geometry queries carrying the ``objectIds`` parameter with a
complete HTTP 200 Esri error envelope (verified 2026-11-14) and serves only
about 25-40 geometry-bearing features per request. Segments tile the roster
exactly, and the request order brackets the whole extraction window: the
service/layer/count/roster documents are captured before the first segment,
the independent attributes-only second extraction (up to 2000 records per page)
follows the segments, and the closing brackets are captured (refreshed on a
resumed capture that filled segment gaps) after everything else, so producer
changes during extraction fail the capture closed and nothing is skipped,
duplicated, reordered or altered between paths. All 16 source attributes (the
``shape`` geometry field carries no attribute value) are retained. The
server-reprojected EPSG:4326 polygon rings are kept byte-exact in the raw
evidence; the two pinned OGC-invalid ring self-intersections (OBJECTIDs 220
and 897, valid under Esri semantics) are minimally repaired with
``make_valid(method=structure)`` in the candidate only, under an explicit
  unaccepted repair gate. Publication stays blocked pending succession,
  freshness, schema/ID contract, coverage, rights and release decisions.

The offset-segment checkpoint acquisition uses shared ``wbd_capture`` and
``arcgis_adaptive`` owners for resumable segment tiling, independent attributes
extraction and refreshed closing brackets. Native source validation remains here.
"""
from __future__ import annotations

from functools import partial

import hashlib
import json
import math
import shutil
from array import array
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely import from_wkb
from shapely.validation import explain_validity

from dagster_hifld.sources import wbd_capture
from dagster_hifld.sources.arcgis_adaptive import SliceCaptureSpec
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_layer import (
    capture_with_retry, producer_error_doc as _producer_error_doc,
)
from dagster_hifld.sources.capture_common import (
    candidate_prefix, content_derived_version,
    processing_identity, quarantine_incomplete_capture,
    verify_cached_candidate, write_immutable,
    validate_capture_request_identity,
    reuse_sealed_candidate,
    load_candidate_checkpoint,
    stage_candidate_objects,
    upload_candidate_file,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import (
    esri_polygon_wkb as _esri_polygon_wkb, load_replay,
)
from dagster_hifld.sources.wbd_hu_family import (
    query_url as _layer_query_url, validate_esri_value as _validate_field_value,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "wbd"
FILE_SLUG = "8-digit-hu-subbasin"
PUBLISHER = ("U.S. Geological Survey (USGS), National Geospatial Technical "
             "Operations Center")
AGENCY = "U.S. Geological Survey"
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
LAYER_ID = 4
# The producer publishes the layer name with two spaces between "HU" and
# "(Subbasin)"; it must match the inventory entry verbatim, so the pin keeps it.
LAYER_NAME = "8-digit HU  (Subbasin)"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LANDING_URL = "https://www.usgs.gov/national-hydrography/watershed-boundary-dataset"
USER_AGENT = "hifld-next-datasets/wbd-8-digit-hu-subbasin-draft"
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
EXPECTED_COPYRIGHT_TEXT = ("USGS WBD - Watershed Boundary Dataset. "
                           "Data refreshed October, 2026.")
LAYER_COPYRIGHT_TEXT = "U.S. Geological Survey"
EXPECTED_SERVICE_CAPABILITIES = "Map,Query,Data"
EXPECTED_SERVICE_QUERY_FORMATS = "JSON, geoJSON, PBF"
EXPECTED_MAX_RECORD_COUNT = 2000
EXPECTED_SERVICE_LAYERS = [
    (0, "WBDLine"), (1, "2-digit HU (Region)"), (2, "4-digit HU (Subregion)"),
    (3, "6-digit HU (Basin)"), (4, "8-digit HU  (Subbasin)"),
    (5, "10-digit HU (Watershed)"), (6, "12-digit HU (Subwatershed)"),
    (7, "14-digit HU"), (8, "16-digit HU"),
]
EXPECTED_DISPLAY_FIELD = "NAME"
EXPECTED_GEOMETRY_PROPERTIES = {"shapeAreaFieldName": "shape_Area",
                                "shapeLengthFieldName": "shape_Length",
                                "units": "esriMeters",
                                "mapUnits": {"uwkid": 9001}}
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "objectid", "type": "esriFieldTypeOID", "alias": "ObjectID", "domain": None},
    {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40, "domain": None},
    {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40, "domain": None},
    {"name": "sourcedatadesc", "type": "esriFieldTypeString", "alias": "SourceDataDesc", "length": 100, "domain": None},
    {"name": "sourceoriginator", "type": "esriFieldTypeString", "alias": "SourceOriginator", "length": 130, "domain": None},
    {"name": "sourcefeatureid", "type": "esriFieldTypeString", "alias": "SourceFeatureID", "length": 40, "domain": None},
    {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8, "domain": None},
    {"name": "referencegnis_ids", "type": "esriFieldTypeString", "alias": "ReferenceGNIS_IDs", "length": 50, "domain": None},
    {"name": "areaacres", "type": "esriFieldTypeDouble", "alias": "AreaAcres", "domain": None},
    {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "AreaSqKm", "domain": None},
    {"name": "states", "type": "esriFieldTypeString", "alias": "States", "length": 50, "domain": None},
    {"name": "huc8", "type": "esriFieldTypeString", "alias": "HUC8", "length": 8, "domain": None},
    {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "length": 120, "domain": None},
    {"name": "shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None},
    {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38, "domain": None},
    {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "shape_Length", "domain": None},
    {"name": "shape_Area", "type": "esriFieldTypeDouble", "alias": "shape_Area", "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS
                    if field["type"] != "esriFieldTypeGeometry"]
ATTRIBUTE_NAMES = [field["name"] for field in ATTRIBUTE_FIELDS]
# Live-verified 2026-11-14: this MapServer layer refuses geometry queries that
# carry the ``objectIds`` parameter with a complete HTTP 200 error envelope,
# and refuses 50-feature geometry pages (25-40 features served reliably), so
# segments are offset-paged 25 features at a time.
PAGE_SIZE = 25
MAX_BODY_BYTES = 64 * 1024 * 1024
MAX_CAPTURE_BYTES = 8 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 8_000_000_000
CAPTURE_ATTEMPTS = 4
CAPTURE_RETRY_BASE_DELAY_SECONDS = 5.0
ATTRS_PAGE_SIZE = 2000


def _doc_names(population_count: int) -> tuple[str, ...]:
    return ("service_before", "layer_before", "count_before", "ids_before",
            "service_after", "layer_after", "count_after", "ids_after",
            *(f"attrs_{offset:05d}" for offset in range(0, population_count, ATTRS_PAGE_SIZE)))
EXPECTED_SCHEMA_SHA256 = "40da5d74cb4436d30c9284dfa4603e9da7cd1e584a1cfb02768583cbfe0c821e"
EXPECTED_INVALID_OBJECTIDS = [220, 897]
EXPECTED_INVALID_VALIDITY_EXPLANATIONS = {
    220: "Ring Self-intersection[-73.0254787636093 41.2132910533631]",
    897: "Ring Self-intersection[-123.014188872192 37.9433382647988]",
}
GEOMETRY_REPAIR_AREA_TOLERANCE = 1e-12
CROSSWALK_OPEN_REST_SERVICE = SERVICE_URL
CROSSWALK_STATUS = "Deprecate"
CROSSWALK_JUSTIFICATION = "No longer supported as part of the HIFLD mission"
SNAPSHOT_URL = ("https://prd-tnm.s3.amazonaws.com/index.html"
                "?prefix=StagedProducts/Hydrography/WBD/National/")


class WbdHu8SubbasinError(ValueError):
    """Incomplete, changed or drifting source input; the capture fails closed."""


def _query(params: dict[str, object]) -> str:
    return _layer_query_url(LAYER_URL, params)


def _segment_name(offset: int, count: int) -> str:
    return f"seg_{offset:05d}_{count:03d}"


def _before_doc_urls() -> list[tuple[str, str]]:
    """Opening brackets, captured before the first geometry segment request."""
    return [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _attrs_doc_urls(population_count: int) -> list[tuple[str, str]]:
    """Independent attributes-only second extraction, captured after every segment."""
    return [(f"attrs_{offset:05d}", _query({
        "where": "1=1", "outFields": "*", "returnGeometry": "false",
        "resultOffset": str(offset), "resultRecordCount": str(ATTRS_PAGE_SIZE),
        "f": "json"})) for offset in range(0, population_count, ATTRS_PAGE_SIZE)]


def _after_doc_urls() -> list[tuple[str, str]]:
    """Closing brackets; they are only truthful after every segment and attrs page."""
    return [
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _doc_urls(population_count: int) -> list[tuple[str, str]]:
    """Every document URL in capture order: opening brackets, the independent
    attributes-only extraction, then the closing brackets."""
    return [*_before_doc_urls(), *_attrs_doc_urls(population_count), *_after_doc_urls()]


def _segment_url(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "outSR": str(SOURCE_CRS_WKID), "resultOffset": str(offset),
                   "resultRecordCount": str(count), "f": "json"})


def _capture_urls(population_count: int) -> dict[str, str]:
    urls = dict(_doc_urls(population_count))
    for offset in range(0, population_count, PAGE_SIZE):
        count = min(PAGE_SIZE, population_count - offset)
        urls[_segment_name(offset, count)] = _segment_url(offset, count)
    return urls


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    if (capture_root / f"{name}.receipt.json").is_symlink():
        raise WbdHu8SubbasinError("retained_capture_symlink_rejected:" + name)
    try:
        if (capture_root / f"{name}.receipt.json").stat().st_size > 65536:
            raise WbdHu8SubbasinError("retained_capture_size_limit:" + name)
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WbdHu8SubbasinError("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise WbdHu8SubbasinError("retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body_path, wire_path = capture_root / f"{name}.body", capture_root / f"{name}.wire"
    if any(path.is_symlink() for path in (body_path, wire_path)):
        raise WbdHu8SubbasinError("retained_capture_symlink_rejected:" + name)
    if any(path.stat().st_size > MAX_BODY_BYTES for path in (body_path, wire_path)):
        raise WbdHu8SubbasinError("retained_capture_size_limit:" + name)
    receipt = _load_receipt(capture_root, name)
    try:
        body, replay_receipt = load_replay(capture_root, name)
    except ValueError as exc:
        raise WbdHu8SubbasinError(str(exc)) from exc
    if replay_receipt != receipt:
        raise WbdHu8SubbasinError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if path.is_symlink():
        raise WbdHu8SubbasinError("retained_capture_symlink_rejected:capture_state")
    if not path.exists():
        return {"segments": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WbdHu8SubbasinError("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("segments"), list):
        raise WbdHu8SubbasinError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: dict[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def _roster(document: object, label: str) -> list[int]:
    if not isinstance(document, dict):
        raise WbdHu8SubbasinError(label + "_document_invalid")
    ids = document.get("objectIds")
    if (document.get("objectIdFieldName") != "objectid"
            or not isinstance(ids, list) or not ids
            or any(type(oid) is not int or oid <= 0 for oid in ids)
            or len(set(ids)) != len(ids)):
        raise WbdHu8SubbasinError("source_id_population_drift:" + label)
    return sorted(ids)


def _plain_field(field: object) -> dict[str, object]:
    if not isinstance(field, dict):
        raise WbdHu8SubbasinError("source_field_schema_drift")
    return {key: field[key] for key in ("name", "type", "alias", "length", "domain")
            if key in field}


def _physical_field(field: object) -> dict[str, object]:
    """A page's field member omits layer-only keys such as ``domain``."""
    if not isinstance(field, dict):
        raise WbdHu8SubbasinError("source_field_schema_drift")
    return {key: field[key] for key in ("name", "type", "alias", "length")
            if key in field}


def _validate_service(document: object, label: str) -> None:
    if not isinstance(document, dict):
        raise WbdHu8SubbasinError(label + "_document_invalid")
    if (document.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT
            or document.get("capabilities") != EXPECTED_SERVICE_CAPABILITIES
            or document.get("supportedQueryFormats") != EXPECTED_SERVICE_QUERY_FORMATS
            or document.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT
            or type(document.get("currentVersion")) is not float):
        raise WbdHu8SubbasinError(label + "_service_identity_drift")
    reference = document.get("spatialReference")
    if (not isinstance(reference, dict)
            or reference.get("wkid") != NATIVE_LAYER_SR["wkid"]
            or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise WbdHu8SubbasinError(label + "_service_crs_drift")
    layers = document.get("layers")
    if (not isinstance(layers, list) or len(layers) != len(EXPECTED_SERVICE_LAYERS)):
        raise WbdHu8SubbasinError(label + "_service_layer_roster_drift")
    for layer, (layer_id, layer_name) in zip(layers, EXPECTED_SERVICE_LAYERS):
        if (not isinstance(layer, dict) or layer.get("id") != layer_id
                or layer.get("name") != layer_name or layer.get("type") != "Feature Layer"
                or layer.get("parentLayerId") != -1):
            raise WbdHu8SubbasinError(label + "_service_layer_roster_drift")


def _validate_layer(document: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(document, dict):
        raise WbdHu8SubbasinError("layer_document_invalid")
    if (document.get("id") != LAYER_ID or document.get("name") != LAYER_NAME
            or document.get("type") != "Feature Layer"
            or document.get("geometryType") != "esriGeometryPolygon"
            or document.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT
            or document.get("displayField") != EXPECTED_DISPLAY_FIELD
            or document.get("capabilities") != EXPECTED_SERVICE_CAPABILITIES
            or document.get("supportedQueryFormats") != EXPECTED_SERVICE_QUERY_FORMATS
            or document.get("copyrightText") != LAYER_COPYRIGHT_TEXT
            or document.get("geometryProperties") != EXPECTED_GEOMETRY_PROPERTIES
            or document.get("advancedQueryCapabilities", {}).get("supportsPagination")
            is not True):
        raise WbdHu8SubbasinError("source_layer_identity_drift")
    extent = document.get("extent")
    if (not isinstance(extent, dict)
            or any(type(extent.get(key)) not in (int, float)
                   for key in ("xmin", "ymin", "xmax", "ymax"))):
        raise WbdHu8SubbasinError("source_layer_extent_drift")
    reference = extent.get("spatialReference")
    if (not isinstance(reference, dict)
            or reference.get("wkid") != NATIVE_LAYER_SR["wkid"]
            or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise WbdHu8SubbasinError("source_layer_native_crs_drift")
    fields = [_plain_field(field) for field in document.get("fields", [])]
    if fields != EXPECTED_FIELD_DECLARATIONS:
        raise WbdHu8SubbasinError("source_field_schema_drift")
    return fields, _sha(_canonical(fields))


def _validate_value(field: dict[str, object], value: object) -> None:
    try:
        _validate_field_value(field, value, check_width=True)
    except ValueError as exc:
        raise WbdHu8SubbasinError(str(exc)) from exc


def _validate_geometry(geometry: object, bounds: tuple[float, float, float, float]) \
        -> tuple[list[list], object]:
    if (not isinstance(geometry, dict)
            or set(geometry) - {"rings", "ids", "hasZ", "hasM", "spatialReference"}):
        raise WbdHu8SubbasinError("unexpected_source_geometry_members")
    if geometry.get("hasZ") or geometry.get("hasM"):
        raise WbdHu8SubbasinError("source_polygon_dimensional_flags_rejected")
    ring_ids = geometry.get("ids")
    if ring_ids is not None:
        # The producer emits the ESRI JSON 'ids' member (per-part vertex
        # identifier metadata) on a few features. It is validated structurally,
        # hashed into the source geometry identity and kept byte-exact in the
        # raw evidence; the candidate geometry (WKB) represents rings only.
        if not isinstance(ring_ids, list):
            raise WbdHu8SubbasinError("source_geometry_ids_member_invalid")
        for part in ring_ids:
            if (not isinstance(part, list)
                    or any(type(value) is not int or value < 0 for value in part)):
                raise WbdHu8SubbasinError("source_geometry_ids_member_invalid")
    reference = geometry.get("spatialReference")
    if reference is not None:
        if (not isinstance(reference, dict) or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                or reference.get("latestWkid") not in (None, SOURCE_CRS_WKID)):
            raise WbdHu8SubbasinError("source_geometry_crs_drift")
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise WbdHu8SubbasinError("source_polygon_rings_missing")
    xmin, ymin, xmax, ymax = bounds
    for ring in rings:
        if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]):
            raise WbdHu8SubbasinError("source_polygon_ring_invalid_or_unclosed")
        for point in ring:
            if (not isinstance(point, list) or len(point) != 2
                    or any(type(coord) not in (int, float) or not math.isfinite(coord)
                           for coord in point)):
                raise WbdHu8SubbasinError("source_polygon_ordinate_invalid")
            if not (-180.5 <= point[0] <= 180.5 and -90.5 <= point[1] <= 90.5):
                raise WbdHu8SubbasinError("source_coordinate_range_drift")
            if (point[0] < xmin - 1e-9 or point[0] > xmax + 1e-9
                    or point[1] < ymin - 1e-9 or point[1] > ymax + 1e-9):
                raise WbdHu8SubbasinError("source_geometry_outside_observed_coverage")
    return rings, ring_ids


_ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.int64(),
    "esriFieldTypeDouble": pa.float64(),
}


def _arrow_schema(fields: list[dict[str, object]], geometry_types: list[str],
                  crs: dict) -> pa.Schema:
    arrow_fields = []
    for field in fields:
        if field["type"] == "esriFieldTypeGeometry":
            continue
        arrow_fields.append(pa.field(
            field["name"], _ARROW_TYPES[field["type"]],
            nullable=field["type"] != "esriFieldTypeOID"))
    arrow_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    schema = pa.schema(arrow_fields)
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB",
                                    "geometry_types": sorted(geometry_types),
                                    "crs": crs, "edges": "planar"}}}
    return schema.with_metadata({b"geo": _canonical(geo)})


class _StreamBuilder:
    """Digest every segment and stream the native candidate parquet to disk."""

    def __init__(self, capture_root: Path, fields: list[dict[str, object]],
                 parquet_sink: Path | None):
        self.capture_root = capture_root
        self.expected_names = [field["name"] for field in fields
                               if field["type"] != "esriFieldTypeGeometry"]
        self.fields = fields
        self.declared_physical = {field["name"]: _physical_field(field)
                                  for field in fields}
        self.schema = None
        self.parquet_sink = parquet_sink
        self.tmp_path = (parquet_sink.with_name(f"{parquet_sink.name}.construction-tmp")
                         if parquet_sink is not None else None)
        if self.tmp_path is not None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            if self.tmp_path.exists():
                self.tmp_path.unlink()
        self._writer = None
        self.record_hash = hashlib.sha256()
        self.attrs_path_hash = hashlib.sha256()
        self.geometry_hash = hashlib.sha256()
        self.ids: array = array("q")
        self.vertex_count = 0
        self.invalid_geometry = 0
        self.invalid_geometry_ids: list[int] = []
        self.repair_records: list[dict[str, object]] = []
        self.candidate_invalid_geometry = 0
        self.empty_geometry = 0
        self.huc8_codes: set[str] = set()
        self.null_attr_counts: dict[str, int] = {name: 0 for name in self.expected_names}
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self.geometry_types: set[str] = set()
        self.ids_member_roster: list[int] = []
        self._writer_closed = False

    def _ensure_writer(self) -> None:
        if self._writer is None and self.tmp_path is not None:
            crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
            declared_types = ["MultiPolygon", "Polygon"]
            self.schema = _arrow_schema(self.fields, declared_types, crs)
            self._writer = pq.ParquetWriter(self.tmp_path, self.schema,
                                            compression="zstd", use_dictionary=False)

    def _repair_candidate(self, objectid: int, raw_wkb: bytes, raw_geometry) \
            -> tuple[bytes, dict[str, object] | None]:
        """Apply the minimal pinned make_valid repair to source-invalid rings.

        The raw capture keeps every source ring byte-exact; the candidate
        repairs only the pinned OGC-invalid ring self-intersections (valid
        under Esri semantics). The structure-method make_valid must keep a
        valid Polygon or MultiPolygon with unchanged area and bounds or the
        capture fails closed; repair acceptance is its own pending gate.
        """
        if raw_geometry.is_valid:
            return raw_wkb, None
        if objectid not in EXPECTED_INVALID_OBJECTIDS:
            raise WbdHu8SubbasinError("source_geometry_unexpected_invalid_roster")
        validity_before = explain_validity(raw_geometry)
        if validity_before != EXPECTED_INVALID_VALIDITY_EXPLANATIONS.get(objectid):
            raise WbdHu8SubbasinError("source_geometry_validity_explanation_drift")
        fixed = shapely.make_valid(raw_geometry, method="structure")
        if (fixed.is_empty or not fixed.is_valid
                or fixed.geom_type not in ("Polygon", "MultiPolygon")):
            raise WbdHu8SubbasinError("source_geometry_repair_not_minimal")
        if abs(fixed.area - raw_geometry.area) > GEOMETRY_REPAIR_AREA_TOLERANCE * max(
                abs(raw_geometry.area), 1e-12):
            raise WbdHu8SubbasinError("source_geometry_repair_area_drift")
        if tuple(raw_geometry.bounds) != tuple(fixed.bounds):
            raise WbdHu8SubbasinError("source_geometry_repair_bounds_drift")
        record = {"objectid": objectid, "operation": "make_valid(method=structure)",
                  "validity_before": validity_before,
                  "repaired_part_count": len(getattr(fixed, "geoms", [fixed])),
                  "repaired_vertex_count": sum(
                      len(part.exterior.coords)
                      + sum(len(interior.coords) for interior in part.interiors)
                      for part in getattr(fixed, "geoms", [fixed])),
                  "source_area": raw_geometry.area, "repaired_area": fixed.area}
        return fixed.wkb, record

    def process_attrs_page(self, name: str, offset: int, roster: list[int]) -> None:
        page = json.loads(_read_capture(self.capture_root, name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise WbdHu8SubbasinError("source_attrs_page_error:" + name)
        features = page.get("features")
        if not isinstance(features, list):
            raise WbdHu8SubbasinError("source_attrs_page_incomplete:" + name)
        expected = roster[offset:offset + len(features)]
        if offset + len(features) < len(roster):
            if page.get("exceededTransferLimit") is not True:
                raise WbdHu8SubbasinError("source_attrs_page_transfer_flag_drift:" + name)
        elif page.get("exceededTransferLimit"):
            raise WbdHu8SubbasinError("source_attrs_page_transfer_flag_drift:" + name)
        if len(features) != min(ATTRS_PAGE_SIZE, len(roster) - offset):
            raise WbdHu8SubbasinError("source_attrs_page_incomplete:" + name)
        for index, feature in enumerate(features):
            if not isinstance(feature, dict) or set(feature) != {"attributes"}:
                raise WbdHu8SubbasinError("source_attrs_feature_structure_drift")
            attrs = feature["attributes"]
            if tuple(attrs) != tuple(ATTRIBUTE_NAMES):
                raise WbdHu8SubbasinError("source_attribute_roster_or_order_drift")
            if attrs.get("objectid") != expected[index]:
                raise WbdHu8SubbasinError("source_page_id_slice_incomplete_or_reordered")
            for field in ATTRIBUTE_FIELDS:
                _validate_value(field, attrs.get(field["name"]))
            self.attrs_path_hash.update(_canonical(attrs) + b"\x00")

    def process(self, name: str, offset: int, count: int, roster: list[int],
                bounds: tuple[float, float, float, float]) -> None:
        if self._writer_closed:
            raise WbdHu8SubbasinError("stream_writer_already_closed")
        page = json.loads(_read_capture(self.capture_root, name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise WbdHu8SubbasinError("source_page_error:" + name)
        reference = page.get("spatialReference")
        if (not isinstance(reference, dict)
                or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                or reference.get("latestWkid") not in (None, SOURCE_CRS_WKID)):
            raise WbdHu8SubbasinError("source_page_crs_drift:" + name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise WbdHu8SubbasinError("source_page_row_count_incomplete:" + name)
        if offset + count < len(roster):
            if page.get("exceededTransferLimit") is not True:
                raise WbdHu8SubbasinError("source_page_transfer_flag_drift:" + name)
        elif page.get("exceededTransferLimit"):
            raise WbdHu8SubbasinError("source_page_transfer_flag_drift:" + name)
        for page_field in page.get("fields", []):
            if self.declared_physical.get(page_field.get("name")) != _physical_field(page_field):
                raise WbdHu8SubbasinError("source_page_physical_schema_changed")
        columns: dict[str, list[object]] = {field: [] for field in self.expected_names}
        geometries: list[bytes] = []
        for index, feature in enumerate(features):
            if not isinstance(feature, dict) or set(feature) - {"attributes", "geometry"}:
                raise WbdHu8SubbasinError("source_feature_structure_drift")
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or tuple(attrs) != tuple(ATTRIBUTE_NAMES):
                raise WbdHu8SubbasinError("source_attribute_roster_or_order_drift")
            objectid = attrs.get("objectid")
            if type(objectid) is not int or objectid != roster[offset + index]:
                raise WbdHu8SubbasinError("source_page_id_slice_incomplete_or_reordered")
            for field in ATTRIBUTE_FIELDS:
                _validate_value(field, attrs.get(field["name"]))
                if attrs.get(field["name"]) is None:
                    self.null_attr_counts[field["name"]] += 1
            self.record_hash.update(_canonical(attrs) + b"\x00")
            rings, ring_ids = _validate_geometry(feature.get("geometry"), bounds)
            self.geometry_hash.update(
                _canonical([objectid, rings, ring_ids]) + b"\x00")
            if ring_ids is not None:
                self.ids_member_roster.append(objectid)
            raw = _esri_polygon_wkb(feature["geometry"])
            decoded = from_wkb(raw)
            self.geometry_types.add(decoded.geom_type)
            if decoded.is_empty:
                self.empty_geometry += 1
                wkb = raw
            else:
                wkb, repair = self._repair_candidate(objectid, raw, decoded)
                if not decoded.is_valid:
                    self.invalid_geometry += 1
                    self.invalid_geometry_ids.append(objectid)
                    if repair is not None:
                        self.repair_records.append(repair)
            written = decoded if wkb is raw else from_wkb(wkb)
            if not written.is_valid:
                self.candidate_invalid_geometry += 1
            self.vertex_count += sum(len(ring) for ring in rings)
            huc8 = attrs.get("huc8")
            if not isinstance(huc8, str) or len(huc8) != 8:
                raise WbdHu8SubbasinError("source_huc8_identifier_drift")
            self.huc8_codes.add(huc8)
            x = [point[0] for ring in rings for point in ring]
            y = [point[1] for ring in rings for point in ring]
            self._xmin = min([self._xmin, *x]) if self._xmin is not None else min(x)
            self._ymin = min([self._ymin, *y]) if self._ymin is not None else min(y)
            self._xmax = max([self._xmax, *x]) if self._xmax is not None else max(x)
            self._ymax = max([self._ymax, *y]) if self._ymax is not None else max(y)
            for field_name in self.expected_names:
                columns[field_name].append(attrs[field_name])
            geometries.append(wkb)
        self._ensure_writer()
        if self._writer is not None:
            batch = pa.Table.from_pydict({**columns, "geometry": geometries},
                                         schema=self.schema)
            self._writer.write_table(batch)
        self.ids.extend(roster[offset:offset + count])

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
        if self.geometry_types - {"Polygon", "MultiPolygon"}:
            raise WbdHu8SubbasinError("source_candidate_geometry_types_drift")
        bounds = (None if self._xmin is None else
                  {"xmin": self._xmin, "ymin": self._ymin,
                   "xmax": self._xmax, "ymax": self._ymax})
        return {"record_sha": self.record_hash.hexdigest(),
                "attrs_path_sha": self.attrs_path_hash.hexdigest(),
                "geometry_sha": self.geometry_hash.hexdigest(),
                "vertex_count": self.vertex_count,
                "invalid_geometry_count": self.invalid_geometry,
                "invalid_geometry_ids": sorted(self.invalid_geometry_ids),
                "repair_records": self.repair_records,
                "candidate_invalid_geometry_count": self.candidate_invalid_geometry,
                "empty_geometry_count": self.empty_geometry,
                "distinct_huc8_count": len(self.huc8_codes),
                "huc8_sha": _sha(_canonical(sorted(self.huc8_codes))),
                "null_attr_counts": dict(sorted(self.null_attr_counts.items())),
                "geometry_types": sorted(self.geometry_types),
                "ids_member_roster": self.ids_member_roster,
                "observed_bounds": bounds,
                "ids_sha": _sha(_canonical(list(self.ids))),
                "max_objectid": self.ids[-1] if len(self.ids) else 0}


def _plan_segments(capture_root: Path, population_count: int) -> list[tuple[str, int, int]]:
    """Order the state's accepted segments; they must tile the roster exactly.

    Segments start at PAGE_SIZE but a producer refusal bisects the refused
    window, so only the roster tiling (and the PAGE_SIZE cap) is required,
    not a uniform page size.
    """
    state = _load_state(capture_root)
    segments = state.get("segments")
    if not isinstance(segments, list) or not segments \
            or any(not isinstance(item, dict) for item in segments):
        raise WbdHu8SubbasinError("source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(segments, key=lambda item: (item.get("offset", -1), item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _segment_name(offset, count) or offset != cursor
                or offset + count > population_count or entry.get("features") != count
                or count > PAGE_SIZE):
            raise WbdHu8SubbasinError("source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != population_count:
        raise WbdHu8SubbasinError("source_capture_roster_incomplete_or_unexpected")
    return ordered


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally rebuild the parquet."""
    capture_root = Path(capture_root).absolute()
    roster = _roster(json.loads(_read_capture(capture_root, "ids_before")[0]), "ids_before")
    population_count = len(roster)
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name, _url in (*_before_doc_urls(), *_after_doc_urls())}
    _validate_service(docs["service_before"], "service_before")
    fields, schema_sha = _validate_layer(docs["layer_before"])
    count_before = docs["count_before"].get("count")
    if type(count_before) is not int or count_before != population_count:
        raise WbdHu8SubbasinError("source_count_drift:before")
    for name, mirror in (("layer_after", "layer_before"), ("service_after", "service_before"),
                         ("count_after", "count_before"), ("ids_after", "ids_before")):
        if json.loads(_read_capture(capture_root, name)[0]) != docs[mirror]:
            raise WbdHu8SubbasinError("source_metadata_or_count_changed_during_capture:" + name)
    if EXPECTED_SCHEMA_SHA256 and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise WbdHu8SubbasinError("source_schema_baseline_drift")
    roster_after = _roster(docs["ids_after"], "ids_after")
    validate_capture_witness(count_before, roster, docs["count_after"].get("count"), roster_after)
    bounds = (-180.5, -90.5, 180.5, 90.5)
    ordered = _plan_segments(capture_root, population_count)
    expected_urls = dict(_doc_urls(population_count))
    expected_urls.update((name, _segment_url(offset, count)) for name, offset, count in ordered)
    for name, url in expected_urls.items():
        validate_capture_request_identity(name, _read_capture(capture_root, name)[1], url)
    stream = _StreamBuilder(capture_root, fields, parquet_sink)
    try:
        for offset in range(0, population_count, ATTRS_PAGE_SIZE):
            stream.process_attrs_page(f"attrs_{offset:05d}", offset, roster)
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster, bounds)
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    if len(stream.ids) != population_count or summary["max_objectid"] <= 0:
        raise WbdHu8SubbasinError("source_page_population_incomplete")
    validate_capture_witness(count_before, roster, docs["count_after"]["count"],
                             roster_after, list(stream.ids))
    if summary["attrs_path_sha"] != summary["record_sha"]:
        raise WbdHu8SubbasinError("source_attribute_cross_path_drift")
    if summary["distinct_huc8_count"] != population_count:
        raise WbdHu8SubbasinError("source_distinct_huc8_count_drift")
    if summary["empty_geometry_count"]:
        raise WbdHu8SubbasinError("source_empty_geometry_count_drift")
    if summary["candidate_invalid_geometry_count"]:
        raise WbdHu8SubbasinError("candidate_invalid_geometry_drift")
    return {"fields": fields, "schema_sha": schema_sha, "segments": len(ordered),
            "source_reported_count": count_before, "feature_count": population_count,
            "min_objectid": min(roster),
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "attrs_path_sha": summary["attrs_path_sha"],
            "ids_sha": summary["ids_sha"], "huc8_sha": summary["huc8_sha"],
            "parquet_path": parquet_sink,
            "vertex_count": summary["vertex_count"],
            "invalid_geometry_count": summary["invalid_geometry_count"],
            "invalid_geometry_ids": summary["invalid_geometry_ids"],
            "repair_records": summary["repair_records"],
            "candidate_invalid_geometry_count": summary["candidate_invalid_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "distinct_huc8_count": summary["distinct_huc8_count"],
            "null_attr_counts": summary["null_attr_counts"],
            "geometry_types": summary["geometry_types"],
            "ids_member_roster": summary["ids_member_roster"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"]}


def _default_client() -> httpx.Client:
    return httpx.Client(follow_redirects=False,
                        timeout=httpx.Timeout(90.0, read=600.0),
                        headers={"Connection": "close", "User-Agent": USER_AGENT})


def _covered_tiling(state: dict[str, object], population_count: int) -> list[tuple[int, int]]:
    """Validate the state's accepted segments and return their covered spans."""
    segments = state.get("segments")
    if not isinstance(segments, list) or any(not isinstance(item, dict) for item in segments):
        raise WbdHu8SubbasinError("capture_state_invalid")
    spans: list[tuple[int, int]] = []
    for entry in segments:
        offset, count = entry.get("offset"), entry.get("count")
        if (type(offset) is not int or type(count) is not int or offset < 0
                or count < 1 or count > PAGE_SIZE or offset + count > population_count
                or entry.get("name") != _segment_name(offset, count)
                or entry.get("features") != count):
            raise WbdHu8SubbasinError("capture_state_invalid")
        spans.append((offset, offset + count))
    spans.sort()
    for (start, end), (next_start, _next_end) in zip(spans, spans[1:]):
        if next_start < end:
            raise WbdHu8SubbasinError("capture_state_overlapping_segments")
    return spans


def _segment_gaps(spans: list[tuple[int, int]], population_count: int) -> list[tuple[int, int]]:
    gaps: list[tuple[int, int]] = []
    cursor = 0
    for start, end in spans:
        if start > cursor:
            gaps.append((cursor, start - cursor))
        cursor = max(cursor, end)
    if cursor < population_count:
        gaps.append((cursor, population_count - cursor))
    return gaps


def _fetch_doc_group(client: httpx.Client, urls: list[tuple[str, str]], capture_root: Path,
                     *, refresh: bool = False) -> None:
    wbd_capture.capture_document_group(client, urls, capture_root,
        capture=partial(capture_with_retry, max_bytes=MAX_BODY_BYTES, attempts=CAPTURE_ATTEMPTS,
            base_delay=CAPTURE_RETRY_BASE_DELAY_SECONDS, max_total_bytes=MAX_CAPTURE_BYTES,
            budget_error=WbdHu8SubbasinError, preserve_final_failure=True),
        read=_read_capture, producer_error=_producer_error_doc,
        error_type=WbdHu8SubbasinError, refresh=refresh)


def fetch_wbd_hu8_subbasin_pages(capture_root: Path) -> None:
    """Fetch (resumable) the complete metadata/roster/segment capture.

    The producer refuses some offset-paged windows with a persistent HTTP 200
    error envelope (observed 2026-11-14 for 25-feature pages around large
    subbasins); a refused window is bisected deterministically until served,
    and accepted segments must tile the enumerated roster exactly.

    Request order brackets the whole extraction window: the opening
    service/layer/count/roster documents precede every geometry segment, the
    independent attributes-only second extraction follows the segments, and
    the closing brackets are captured (or, on a resumed capture that filled
    segment gaps, refreshed) after every segment and attributes page, so a
    producer change during extraction fails the capture closed.
    """
    capture_root = Path(capture_root).absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent
                                         in (capture_root, *capture_root.parents)):
        raise WbdHu8SubbasinError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(capture_root).free < MIN_FREE_BYTES:
        raise WbdHu8SubbasinError("disk_floor_before_acquisition")
    state = _load_state(capture_root)
    with _default_client() as client:
        wbd_capture.capture_segmented_layer(
            SliceCaptureSpec(byte_budget=MAX_CAPTURE_BYTES,
                page_name=_segment_name, page_url=_segment_url,
                capture=partial(capture_with_retry, max_bytes=MAX_BODY_BYTES,
                    max_total_bytes=MAX_CAPTURE_BYTES, budget_error=WbdHu8SubbasinError,
                    preserve_final_failure=True),
                producer_error=_producer_error_doc, page_attempts=CAPTURE_ATTEMPTS,
                page_delay=CAPTURE_RETRY_BASE_DELAY_SECONDS, doc_attempts=CAPTURE_ATTEMPTS,
                doc_delay=CAPTURE_RETRY_BASE_DELAY_SECONDS, error=WbdHu8SubbasinError,
                bisect_incomplete=False),
            client, capture_root, state,
            documents=wbd_capture.SegmentDocuments(before=_before_doc_urls(),
                attributes=_attrs_doc_urls, after=_after_doc_urls(), fetch_group=_fetch_doc_group,
                read=_read_capture, validate_roster=_roster),
            checkpoints=wbd_capture.SegmentCheckpoints(page_size=PAGE_SIZE, oid_field="objectid",
                covered=_covered_tiling, gaps=_segment_gaps, save=_save_state,
                parse_name=lambda name: tuple(map(int, name[4:].split("_")))))


def _receipt_map(capture_root: Path, segment_names: list[str], population_count: int) -> dict[str, dict[str, object]]:
    names = [*_doc_names(population_count), *segment_names]
    return {name: _load_receipt(capture_root, name) for name in names}


def _parquet_descriptor(storage: StagingStorageResource, key: str) -> dict[str, object]:
    """Descriptor for one staged file-sized object without holding its bytes."""
    size = storage.key_size(key)
    digest = storage.sha256_key(key)
    if not isinstance(size, int) or not isinstance(digest, str) or len(digest) != 64:
        raise WbdHu8SubbasinError("candidate_parquet_readback_failed")
    return {"bytes": size, "sha256": digest}


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
                    extracted: dict[str, object], *, resumed_segments: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    parquet_path = Path(extracted["parquet_path"])
    parquet_name = f"geoparquet/{FILE_SLUG}.parquet"
    def verify_existing():
        return _verify_sealed_candidate(storage, prefix, state_key, version, identity,
                                        extracted, parquet_path)

    reused = reuse_sealed_candidate(storage, state_key, verify_existing)
    if reused is not None:
        return reused
    audit = {
        "source_feature_count": extracted["feature_count"],
        "source_reported_count": extracted["source_reported_count"],
        "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_huc8_sha256": extracted["huc8_sha"],
        "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "esriGeometryPolygon",
        "candidate_geometry_types": extracted["geometry_types"],
        "vertex_count": extracted["vertex_count"],
        "invalid_geometry_count": extracted["invalid_geometry_count"],
        "invalid_geometry_objectids": extracted["invalid_geometry_ids"],
        "geometry_repair_records": extracted["repair_records"],
        "candidate_invalid_geometry_count": extracted["candidate_invalid_geometry_count"],
        "geometry_repair_accepted": False,
        "empty_geometry_count": extracted["empty_geometry_count"],
        "distinct_huc8_count": extracted["distinct_huc8_count"],
        "null_attribute_counts": extracted["null_attr_counts"],
        "source_esri_ids_member_objectids": extracted["ids_member_roster"],
        "unique_source_id_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL,
        "landing_page_url": LANDING_URL,
        "dhs_crosswalk_row": {"layer_name": "Watershed Boundary Dataset (WBD)",
                              "status": CROSSWALK_STATUS,
                              "agency": "U.S. Geological Survey",
                              "old_hosting_location":
                                  "https://geoplatform.maps.arcgis.com/home/item.html"
                                  "?id=409515c7627c444fb08b66cf89634028",
                              "deprecation_justification": CROSSWALK_JUSTIFICATION,
                              "crosswalk_open_rest_service": CROSSWALK_OPEN_REST_SERVICE,
                              "evidence_url":
                                  "https://atcoordinates.info/wp-content/uploads/2025/08/"
                                  "hifld_open_crosswalk_geoplatform.xlsx"},
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "authoritative_snapshot_note":
            ("USGS announces an annual 'authoritative WBD snapshot' while the program "
             "shifts focus to the 3DHP data model (" + SNAPSHOT_URL + ", staged products "
             "dated 2026-09-02); the live service refreshed later (October, 2026) and is "
             "the publication captured here. The snapshot is comparison context only."),
        "baseline_note": ("No prior HIFLD Next publication baseline was recovered for the "
                          "wbd dataset family (backlog ledger: missing_or_unrecovered_"
                          "evidence); candidate acceptance and baseline comparison remain "
                          "unaccepted maintainer decisions."),
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The WBD MapServer publishes no release date; the copyrightText refresh "
            "statement (October, 2026) and per-feature LoadDate values (2012-2024 core "
            "database loads) are not dataset release dates. USGS documents periodic "
            "service refreshes and an annual staged snapshot.",
        "pagination": {"mode": "result_offset_roster_segments",
                       "page_size": PAGE_SIZE, "pages": extracted["segments"],
                       "producer_max_record_count": EXPECTED_MAX_RECORD_COUNT,
                       "resumable": True, "resumed_segments": resumed_segments,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerates the complete native OBJECTID "
                           "roster. The layer refuses "
                           "geometry queries carrying objectIds (HTTP 200 error envelope) "
                           "and about 50-feature pages, so segments are offset-paged 25 "
                           "features at a time; accepted segments must tile the roster "
                           "exactly and before/after service/layer/count/roster brackets "
                           "plus an independent attributes-only extraction prove nothing "
                           "was skipped, duplicated, reordered or altered."},
        "capture_window_utc": {"first_segment": first_captured_at,
                               "last_segment": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    ordered = _plan_segments(capture_root, extracted["feature_count"])
    receipts = _receipt_map(capture_root, [name for name, _offset, _count in ordered],
                            extracted["feature_count"])
    repaired_roster = ", ".join(str(oid) for oid in (EXPECTED_INVALID_OBJECTIDS or []))
    repair_declaration = (
        "minimally repairs exactly the pinned OGC-invalid ring self-intersections "
        "(OBJECTIDs " + repaired_roster + ", valid under the producer's Esri topology "
        "semantics) with shapely make_valid(method=structure), guarded to unchanged "
        "area and bounds; repair acceptance is its own unaccepted gate "
        "(geometry_repair_accepted is false)"
        if repaired_roster else
        "no pinned OGC-invalid rings exist and any source-invalid ring fails the "
        "capture closed")
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "8-digit HU (Subbasin) — USGS Watershed Boundary Dataset",
        "description":
            "Complete observed USGS The National Map 'wbd' MapServer layer "
            + str(LAYER_ID) + " '" + LAYER_NAME + "' (" + str(extracted["feature_count"])
            + " subbasins, service data refreshed October, 2026) — the layer named by the "
              "HIFLD Open inventory entry wbd/8-digit-hu-subbasin and the DHS HIFLD Open "
              "crosswalk's Open REST Service for the WBD family. Succession, freshness, "
              "schema/ID contract, coverage, rights and release decisions remain "
              "unaccepted.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": ("Public domain (U.S. Government work) per the producer service; "
                    "rights acceptance pending."),
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": [],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {
            "layer_id": LAYER_ID,
            "population": "the complete enumerated OBJECTID roster captured as "
                          "offset-paged 25-feature segments (the full national 8-digit "
                          "hydrologic unit population of the layer)",
            "source_fields": ("all producer attributes retained in producer order and "
                              "declared physical types; the geometry field 'shape' "
                              "carries no source attribute value and no attribute "
                              "column is invented for it"),
            "source_ids": ("producer OBJECTID, TNMID, MetaSourceID, SourceFeatureID and "
                           "globalid retained unchanged; no substitute IDs, casts or "
                           "joins"),
            "source_crs": "EPSG:4326 (server-reprojected page CRS; the layer's native "
                          "storage CRS is ESRI 102100 / EPSG:3857 and only its metadata "
                          "is pinned as such)",
            "geometry": ("Source EPSG:4326 XY polygon rings retained as WKB without "
                         "rounding, reprojection or densification; every source ring "
                         "is kept byte-exact in the raw evidence, while the candidate "
                         + repair_declaration + "; ring order, winding and hole "
                         "ownership are validated against the producer's ArcGIS "
                         "convention and rejected on drift; the ESRI JSON 'ids' "
                         "member the producer emits on a few features (per-part "
                         "vertex identifier metadata) is kept byte-exact in the raw "
                         "evidence and hashed into the source geometry identity, "
                         "while the candidate geometry represents the rings only"),
            "candidate_status": ("unapproved; publication blocked pending succession, "
                                 "freshness, schema/ID contract, coverage, attribution "
                                 "and release acceptance")}}],
        "lineage":
            "The HIFLD Open inventory entry wbd/8-digit-hu-subbasin ('8-digit HU "
            "(Subbasin)', agency US Geological Survey, publisher 'U.S. Geological Survey, "
            "National Geospatial Technical Operations Center') was a WBD feature service "
            "on the retired DHS HIFLD Open portal. The DHS HIFLD Open crosswalk (August "
            "2025) row 'Watershed Boundary Dataset (WBD)' marks the former GeoPlatform "
            "item 409515c7627c444fb08b66cf89634028 as Deprecate ('No longer supported as "
            "part of the HIFLD mission') and names the USGS The National Map service "
            + SERVICE_URL + " as the successor Open REST Service; layer "
            + str(LAYER_ID) + " matches the inventory entry verbatim. USGS also publishes "
            "an annual 'authoritative WBD snapshot' while the program shifts focus to the "
            "3DHP data model (" + SNAPSHOT_URL + ", staged product files dated 2026-09-02); "
            "the live service is the fresher maintained publication (copyrightText: "
            + EXPECTED_COPYRIGHT_TEXT + ") and the snapshot is comparison context only. "
            "The DataLumos project 240701 archive copy is identification context only and "
            "is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("Complete native source fields, IDs, values and server-"
                       "reprojected EPSG:4326 polygon geometry retained in a typed "
                       "candidate; all succession, freshness, contract, rights and "
                       "release approvals remain pending."),
            "objects": []}}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved."},
    }
    for name, receipt in sorted(receipts.items()):
        manifest["inputs"].append({
            "input_id": name, "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication", "canonical_url": LAYER_URL,
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": None,
            "release_date_unknown_reason":
                "The WBD MapServer publishes no release date; the copyrightText records "
                "a service refresh (October, 2026), which is not a dataset release date.",
            "license": ("Public domain (U.S. Government work) per the producer service; "
                        "rights acceptance pending."),
            "attribution": ("USGS Watershed Boundary Dataset (WBD), The National Map; "
                            "produced with the USDA Natural Resources Conservation "
                            "Service; formal attribution terms require maintainer "
                            "review.")})
    ready = {"approved": False, "version": version,
             "source_record_sha256": audit["source_record_sha256"],
             "source_schema_sha256": audit["source_schema_sha256"],
             "succession_accepted": False, "freshness_accepted": False,
             "geometry_repair_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    parquet_key = f"{prefix}/{parquet_name}"
    parquet_descriptor = upload_candidate_file(
        storage, parquet_key, parquet_path, _parquet_descriptor,
        WbdHu8SubbasinError)
    descriptors: dict[str, dict[str, object]] = {parquet_name: parquet_descriptor}
    manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"] = [
        {"path": parquet_name, **parquet_descriptor}]
    ready["objects"] = {parquet_name: parquet_descriptor}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    metadata_outputs: dict[str, bytes] = {
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the seal never
    # holds more than one segment body/wire pair plus the small receipt map.
    stage_candidate_objects(
        storage, prefix, state_key, version, identity, metadata_outputs,
        capture_root, receipts, _read_capture,
        descriptors=descriptors,
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'])
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": extracted["feature_count"],
            "source_reported_count": extracted["source_reported_count"],
            "source_population_complete": True,
            "source_id_min": extracted["min_objectid"], "source_id_max": extracted["max_objectid"],
            "source_vertex_count": extracted["vertex_count"],
            "source_invalid_geometry_count": extracted["invalid_geometry_count"],
            "source_invalid_geometry_ids": extracted["invalid_geometry_ids"],
            "source_repair_record_count": len(extracted["repair_records"]),
            "candidate_invalid_geometry_count": extracted["candidate_invalid_geometry_count"],
            "geometry_repair_accepted": False,
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_distinct_huc8_count": extracted["distinct_huc8_count"],
            "source_null_attribute_counts": extracted["null_attr_counts"],
            "source_id_sha256": extracted["ids_sha"],
            "source_huc8_sha256": extracted["huc8_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                             version: str, identity: Mapping, extracted: Mapping,
                             parquet_path: Path) -> dict[str, object]:
    fingerprint, payload = load_candidate_checkpoint(
        storage, state_key, version, identity,
        extracted['record_sha'], extracted['schema_sha'], WbdHu8SubbasinError)
    expected_objects = dict(payload["objects"])
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, extracted["record_sha"],
                            extracted["schema_sha"], expected_objects,
                            payload["capture_receipts"])
    parquet_name = f"geoparquet/{FILE_SLUG}.parquet"
    if expected_objects.get(parquet_name) != {"bytes": parquet_path.stat().st_size,
                                              "sha256": sha256_file(parquet_path)}:
        raise WbdHu8SubbasinError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": extracted["feature_count"],
            "source_reported_count": extracted["source_reported_count"],
            "source_population_complete": True,
            "source_id_min": extracted["min_objectid"], "source_id_max": extracted["max_objectid"],
            "source_vertex_count": extracted["vertex_count"],
            "source_invalid_geometry_count": extracted["invalid_geometry_count"],
            "source_invalid_geometry_ids": extracted["invalid_geometry_ids"],
            "source_repair_record_count": len(extracted["repair_records"]),
            "candidate_invalid_geometry_count": extracted["candidate_invalid_geometry_count"],
            "geometry_repair_accepted": False,
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_distinct_huc8_count": extracted["distinct_huc8_count"],
            "source_null_attribute_counts": extracted["null_attr_counts"],
            "source_id_sha256": extracted["ids_sha"],
            "source_huc8_sha256": extracted["huc8_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def capture_wbd_hu8_subbasin(storage: StagingStorageResource, *, version: str,
                             capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise WbdHu8SubbasinError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "0123456789.-_" for c in version):
        raise WbdHu8SubbasinError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise WbdHu8SubbasinError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise WbdHu8SubbasinError("disk_floor_before_acquisition")
    # Retained capture layout: keep the historical wbd-hu8-subbasin parent so
    # an interrupted acquisition resumes its capture_state.json after rollout.
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else default_capture_dir("wbd-hu8-subbasin", "captures").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent
                                         in (capture_root, *capture_root.parents)):
        raise WbdHu8SubbasinError("unsafe_capture_directory")
    resumed_segments = len(_load_state(capture_root).get("segments", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_wbd_hu8_subbasin_pages(capture_root)
        state = _load_state(capture_root)
        segments = [entry for entry in state["segments"] if isinstance(entry, dict)]
        captured = [str(entry.get("captured_at", "")) for entry in segments]
        extracted = _stream_validate(capture_root,
                                     parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted,
                               resumed_segments=resumed_segments,
                               first_captured_at=captured[0] if captured else "",
                               last_captured_at=captured[-1] if captured else "")
    except Exception as exc:
        valid_names = []
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) \
                if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                _read_capture(capture_root, name)
            except (OSError, WbdHu8SubbasinError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_wbd_hu8_subbasin_baseline(capture_root: str) -> dict[str, object]:
    """Report observed content facts from a completed capture without approving it."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_huc8_sha256": extracted["huc8_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": extracted["feature_count"],
            "source_max_objectid": extracted["max_objectid"],
            "source_vertex_count": extracted["vertex_count"],
            "source_invalid_geometry_count": extracted["invalid_geometry_count"],
            "source_invalid_geometry_ids": extracted["invalid_geometry_ids"],
            "source_repair_record_count": len(extracted["repair_records"]),
            "candidate_invalid_geometry_count": extracted["candidate_invalid_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_distinct_huc8_count": extracted["distinct_huc8_count"],
            "source_null_attribute_counts": extracted["null_attr_counts"],
            "source_geometry_types": extracted["geometry_types"],
            "observed_bounds": extracted["observed_bounds"]}
