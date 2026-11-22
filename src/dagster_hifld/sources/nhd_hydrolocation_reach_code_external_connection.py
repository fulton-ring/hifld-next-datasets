"""Resumable, approval-gated capture of USGS 3DHP_all layer 40.

The HIFLD Open inventory entry
``nhd/hydrolocation-reach-code-external-connection`` ("HydroLocation - Reach
Code, External Connection", agency US Geological Survey) names the producer
layer grouping ``3DHP_all`` layer 40 "HydroLocation - External Connection":
its hydrolocation category enumeration is External Connection (9), Reachcode
Start (10) and Reachcode End (11), USGS's own hydrogeofetch client calls this
grouping "hydrolocation - reach code, external connection", and the alias
``hydro.nationalmap.gov`` 3DHP_all publication historically carried the
grouping's inventory name verbatim. USGS retired the NHD as a maintained
product on 2023-10-01; the DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1)
consolidates the NHD-family layers into the USGS ``3dhp.nationalmap.gov``
``usgs_3dhp_all`` FeatureServer captured here. The DataLumos project 240692
archive copy is identification context only and is never fetched.

Capture-time count and native OBJECTID witnesses prove complete roster
coverage. Adaptive offset segments retain all native attributes and
publisher-served Point Z ordinates in EPSG:4326. Geometry gates are counted,
not repaired. Publication remains unapproved and the draft refresh schedule
stays stopped.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from array import array
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources import arcgis_layer
from dagster_hifld.sources.capture_common import (
    load_checkpoint_receipt_map,
    candidate_prefix,
    capture_get,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    verify_cached_candidate,
    validate_capture_request_identity,
    write_immutable,
    reuse_sealed_candidate,
    stage_candidate_objects,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import (
    RetainedCaptureViews as _RetainedCaptureViews,
    capture_with_retry as _capture_with_retry,
    producer_error_doc as _producer_error_doc,
    save_state as _save_state,
    witness_aside as _witness_aside,
)

DATASET_SLUG = "nhd-hydrolocation-reach-code-external-connection"
FILE_SLUG = DATASET_SLUG
# Repository/package root: in the shipped container layout (/app/src/dagster_hifld)
# this resolves to /app, which carries no .git, so the processing identity uses the
# packaged scheme. Tests redirect this constant to a no-Git packaged layout.
REPO_ROOT = Path(__file__).parents[3]
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 40
LAYER_NAME = "HydroLocation - External Connection"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPoint"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-hydrolocation-reach-code-external-connection-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
# The producer's maxRecordCount is 2500; sibling 3DHP_all layers were observed
# refusing cap-sized pages with complete HTTP 200 Esri error documents while
# smaller slices of the same span were served, so segments start below the cap
# and refused segments are bisected.
MAX_RECORD_COUNT = 2500
SEGMENT_START_RECORDS = 2000
MAX_BODY_BYTES = 96 * 1024 * 1024
MAX_CAPTURE_BYTES = 20 * 1024 * 1024 * 1024
MAX_RECEIPT_BYTES = 64 * 1024
MIN_FREE_BYTES = 20_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 3
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Producer-stated service refresh ("Data refreshed September 4, 2026" in the
# captured service copyrightText) is freshness evidence, not a release date.
EXPECTED_COPYRIGHT_TEXT = ("Credits: USGS TNM / NGTOC \u2013 3D National Hydrographic Program "
                           "(3DHP.) Data refreshed September 4, 2026.")
EXPECTED_SERVICE_LAYERS = [
    (20, "HydroLocation - Sink, Spring, Waterbody Outlet"),
    (30, "HydroLocation - Headwater, Terminus, Divergence, Confluence, Catchment Outlet"),
    (40, "HydroLocation - External Connection"),
    (50, "Flowline"),
    (60, "Waterbody"),
    (80, "Catchment"),
    (90, "Spatial Metadata - EDH Workunit Area"),
    (92, "Spatial Metadata - Elevation Workunit Area"),
    (94, "Spatial Metadata - DEM LimitationArea"),
]
# The producer's two same-org endpoints (3dhp.nationalmap.gov and its
# hydro.nationalmap.gov alias) were observed carrying the three "Spatial
# Metadata" layer names on permuted ids (90/92/94) while the core hydrography
# roster matched exactly; the pin therefore fixes the core rows and only
# requires the Spatial Metadata names to stay a permutation of these ids.
SPATIAL_METADATA_IDS = frozenset({90, 92, 94})
SPATIAL_METADATA_NAMES = frozenset({"Spatial Metadata - DEM LimitationArea",
                                    "Spatial Metadata - EDH Workunit Area",
                                    "Spatial Metadata - Elevation Workunit Area"})
EXPECTED_SCHEMA_SHA256 = "4c4ab8e34f34e4705dd04722bc2e4e0c7cacc210c7bf7e0a34741b86cbebf981"
EXPECTED_FEATURETYPE_LABELS = {9: "External Connection"}
EXPECTED_LAYER_EXTENT = {"xmin": -17130976.996, "ymin": 4295096.859999999,
                         "xmax": -8100889.426200001, "ymax": 10136264.621600002}

NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("id3dhp", "esriFieldTypeString", True, 7),
    ("featuredate", "esriFieldTypeDate", True, 8),
    ("mainstemid", "esriFieldTypeString", True, 200),
    ("universalreferenceid", "esriFieldTypeString", True, 200),
    ("gnisid", "esriFieldTypeInteger", True, None),
    ("gnisidlabel", "esriFieldTypeString", True, 120),
    ("featuretype", "esriFieldTypeSmallInteger", True, None),
    ("featuretypelabel", "esriFieldTypeString", True, 200),
    ("workunitid", "esriFieldTypeString", True, 100),
)
METADATA_NAMES = ("service_doc", "alias_service", "service_before", "layer_before",
                  "count_before", "ids_seed", "ids_before")
AFTER_NAMES = ("layer_after", "service_after", "count_after", "ids_after")
GEOMETRY_COLUMN = "geometry"

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


class NhdHydrolocationReachCodeExternalConnectionError(ValueError):
    """The source snapshot cannot be represented as the pinned candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


# ArcGIS serves display-only rendering hints (for example
# drawingInfo.authoringInfo.colorRamp) nondeterministically from the same
# layer within minutes of each other, so the layer witness comparison covers
# only the data-bearing contract fields; each is pinned individually above.
LAYER_CONTRACT_KEYS = ("id", "name", "type", "geometryType", "hasZ", "hasM",
                       "capabilities", "advancedQueryCapabilities",
                       "sourceSpatialReference", "extent", "fields", "maxRecordCount")


def _layer_contract_view(document: Mapping[str, object]) -> dict[str, object]:
    return {key: document.get(key) for key in LAYER_CONTRACT_KEYS}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "returnZ": "true", "returnM": "false", "outSR": str(SOURCE_CRS_WKID),
                   "orderByFields": "OBJECTID ASC", "resultOffset": str(offset),
                   "resultRecordCount": str(count), "f": "json"})


def _document_urls() -> dict[str, str]:
    return {"service_doc": SERVICE_DOC_URL,
            "alias_service": ALIAS_SERVICE_URL + "?f=pjson",
            "service_before": SERVICE_URL + "?f=pjson", "service_after": SERVICE_URL + "?f=pjson",
            "layer_before": LAYER_URL + "?f=pjson", "layer_after": LAYER_URL + "?f=pjson",
            "count_before": _count_url(), "count_after": _count_url(),
            "ids_seed": _ids_url(), "ids_before": _ids_url(), "ids_after": _ids_url()}


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str) -> list[int]:
    if not isinstance(roster, list) or not roster \
            or any(type(value) is not int or value <= 0 for value in roster) \
            or len(set(roster)) != len(roster):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_id_population_drift:" + label)
    return sorted(roster)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_3dhp_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list):
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_3dhp_service_layer_roster_drift:" + label)
    core = len(EXPECTED_SERVICE_LAYERS) - len(SPATIAL_METADATA_IDS)
    if roster[:core] != EXPECTED_SERVICE_LAYERS[:core]:
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_3dhp_service_layer_roster_drift:" + label)
    metadata_rows = roster[core:]
    if (len(metadata_rows) != len(SPATIAL_METADATA_IDS)
            or {entry_id for entry_id, _name in metadata_rows} != set(SPATIAL_METADATA_IDS)
            or {name for _entry_id, name in metadata_rows} != set(SPATIAL_METADATA_NAMES)):
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_3dhp_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "3DHP_all" not in description or "3D Hydrography Program" not in description:
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_3dhp_service_description_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_service_spatial_reference_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise NhdHydrolocationReachCodeExternalConnectionError("exact_3dhp_hydrolocation_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_record_limit_drift")
    source_reference = layer.get("sourceSpatialReference")
    if (not isinstance(source_reference, dict)
            or source_reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_layer_spatial_reference_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_layer_extent_drift")
    return _profile_schema(layer)


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise NhdHydrolocationReachCodeExternalConnectionError(
            "source_field_type_unsupported:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_native_type_changed:" + name)
        if name == "id3dhp" and value and any(
                character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in value):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_id3dhp_value_drift")
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_native_type_changed:" + name)
        return
    raise NhdHydrolocationReachCodeExternalConnectionError("unsupported_source_physical_type:" + esri_type)


def _point_wkb(geometry: object) -> bytes:
    """Convert one Esri JSON point geometry to Point Z WKB, values verbatim.

    The layer is Z-aware (``hasZ`` true in layer and page metadata) and the
    producer serves every geometry as exactly ``{"x", "y", "z"}`` ordinate
    triples in the page CRS (EPSG:4326 pages are requested explicitly). The
    ordinates are validated numerically and range-checked in x/y, then passed
    to shapely verbatim: no rounding, reordering or projection is applied.
    """
    if not isinstance(geometry, dict) or set(geometry) != {"x", "y", "z"}:
        raise NhdHydrolocationReachCodeExternalConnectionError("unexpected_source_geometry_members")
    x, y, z = geometry["x"], geometry["y"], geometry["z"]
    for name, value in (("x", x), ("y", y), ("z", z)):
        if type(value) not in (int, float):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_point_ordinate_not_numeric:" + name)
        if not (value == value and -1e308 < value < 1e308):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_point_ordinate_not_finite:" + name)
    if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_coordinate_range_drift")
    return Point(x, y, z).wkb


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      expected_objectid: int) -> tuple[int, dict | None]:
    """Validate one feature's attributes and point geometry structure.

    Returns the OBJECTID and the verbatim geometry dict (or None when the
    feature carries no geometry). Ordinate validation happens here so every
    retained record digest is computed from a structurally sound feature; the
    WKB conversion runs in the caller.
    """
    if not isinstance(feature, dict):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(expected_names):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_page_id_slice_incomplete_or_reordered")
    featuretype = attrs["featuretype"]
    if featuretype not in EXPECTED_FEATURETYPE_LABELS:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_featuretype_outside_hydrolocation_population")
    if attrs["featuretypelabel"] != EXPECTED_FEATURETYPE_LABELS[featuretype]:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_featuretype_label_drift")
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    if geometry is not None:
        if not isinstance(geometry, dict) or set(geometry) != {"x", "y", "z"}:
            raise NhdHydrolocationReachCodeExternalConnectionError("unexpected_source_geometry_members")
        x, y = geometry["x"], geometry["y"]
        if x == "NaN" and y == "NaN":
            return objectid, geometry
        if not (type(x) in (int, float) and type(y) in (int, float)):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_point_ordinate_not_numeric")
        if not (x == x and y == y and -1e308 < x < 1e308 and -1e308 < y < 1e308):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_point_ordinate_not_finite")
        if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_coordinate_range_drift")
    return objectid, geometry


def _preflight_capture(capture_root: Path) -> None:
    if ".." in capture_root.parts or any(path.is_symlink() for path in (capture_root, *capture_root.parents)):
        raise NhdHydrolocationReachCodeExternalConnectionError("unsafe_capture_directory")
    if not capture_root.exists():
        return
    entries = list(capture_root.iterdir())
    try:
        validate_capture_roster(capture_root, {path.name for path in entries})
    except ValueError as exc:
        raise NhdHydrolocationReachCodeExternalConnectionError(str(exc)) from exc
    total = 0
    for path in entries:
        size = path.stat().st_size
        limit = MAX_RECEIPT_BYTES if path.name.endswith(".receipt.json") else MAX_BODY_BYTES
        if path.suffix != ".parquet" and size > limit:
            raise NhdHydrolocationReachCodeExternalConnectionError("retained_capture_file_too_large:" + path.name)
        total += size
    if total > MAX_CAPTURE_BYTES:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_exceeds_scratch_budget")


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    path = capture_root / f"{name}.receipt.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_RECEIPT_BYTES:
        raise NhdHydrolocationReachCodeExternalConnectionError("retained_capture_receipt_invalid:" + name)
    return arcgis_layer.load_receipt(capture_root, name,
                                    NhdHydrolocationReachCodeExternalConnectionError)


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    _load_receipt(capture_root, name)
    for suffix in ("body", "wire"):
        path = capture_root / f"{name}.{suffix}"
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BODY_BYTES:
            raise NhdHydrolocationReachCodeExternalConnectionError("retained_capture_file_invalid:" + name)
    try:
        body, receipt = arcgis_layer.load_replay(capture_root, name)
        if name in METADATA_NAMES + AFTER_NAMES:
            validate_capture_request_identity(name, receipt, _document_urls()[name])
        elif _SEGMENT_NAME_PATTERN.fullmatch(name):
            validate_capture_request_identity(name, receipt, _segment_query(*_parse_segment_name(name)))
        return body, receipt
    except (OSError, ValueError) as exc:
        raise NhdHydrolocationReachCodeExternalConnectionError(str(exc)) from exc


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if path.is_symlink() or (path.exists() and (not path.is_file() or path.stat().st_size > MAX_BODY_BYTES)):
        raise NhdHydrolocationReachCodeExternalConnectionError("capture_state_invalid")
    return arcgis_layer.load_state(capture_root, NhdHydrolocationReachCodeExternalConnectionError)


def _retained_or_fetch(client: httpx.Client, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except NhdHydrolocationReachCodeExternalConnectionError:
            _witness_aside(capture_root, name)
    return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)


def _plan_segment_gaps(capture_root: Path, roster: list[int]) -> list[tuple[int, int]]:
    """Plan the roster gaps that still need a capture this session.

    A retained segment counts as accepted only when its receipt and body
    verify, its body is not a producer error document, its retained digest
    still matches the checkpoint, its rows still serve exactly the roster
    slice the segment name requests, and it already sits in the capture
    state; anything else is witnessed aside and refetched.
    """
    state = _load_state(capture_root)
    accepted = {entry.get("name"): entry for entry in state["segments"] if isinstance(entry, dict)}
    retained: list[tuple[int, int]] = []
    for path in sorted(capture_root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        match = _SEGMENT_NAME_PATTERN.fullmatch(name)
        if match is None:
            # Witnessed refusal evidence (page_*.error-*.body) and other
            # non-canonical names are not roster segments; skip them.
            continue
        try:
            body, _receipt = _read_capture(capture_root, name)
        except NhdHydrolocationReachCodeExternalConnectionError:
            _witness_aside(capture_root, name)
            continue
        if _producer_error_doc(body) or name not in accepted:
            _witness_aside(capture_root, name)
            continue
        offset, count = _parse_segment_name(name)
        try:
            rows = json.loads(body).get("features")
            actual = [row.get("attributes", {}).get("OBJECTID") for row in rows]
        except (ValueError, TypeError, AttributeError):
            actual = None
        if (count < 1 or offset + count > len(roster)
                or actual != roster[offset:offset + count]
                or accepted[name].get("body_sha256") != _sha(body)):
            _witness_aside(capture_root, name)
            continue
        retained.append((offset, count))
    gaps: list[tuple[int, int]] = []
    covered = bytearray(len(roster))
    for offset, count in sorted(retained):
        if offset < 0 or count < 1 or offset + count > len(roster) \
                or any(covered[offset:offset + count]):
            _witness_aside(capture_root, _segment_name(offset, count))
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
            count_chunk = min(SEGMENT_START_RECORDS, cursor - chunk_start)
            gaps.append((chunk_start, count_chunk))
            chunk_start += count_chunk
    return gaps


def _fetch_segments(client: httpx.Client, capture_root: Path, roster: list[int],
                    state: dict[str, object]) -> bool:
    """Capture the complete layer as adaptively bisected roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing is bisected until the producer serves both halves, whether the
    refusal is a persistent error document or a persistent incomplete transport
    capture; the refused segment's final body and receipts stay on disk as
    evidence under witnessed names. A single-record segment that still fails
    ends the capture explicitly.
    """
    segments = state.setdefault("segments", [])
    todo: deque[tuple[int, int]] = deque(_plan_segment_gaps(capture_root, roster))
    refreshed = bool(todo)
    if refreshed:
        state["complete"] = False
        if segments:
            _save_state(capture_root, state)
    while todo:
        offset, count = todo.popleft()
        name = _segment_name(offset, count)
        try:
            body, receipt = _capture_with_retry(
                client, _segment_query(offset, count), capture_root, name,
                attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1
                else DOC_CAPTURE_BASE_DELAY_SECONDS,
                max_bytes=MAX_BODY_BYTES)
        except ValueError as exc:
            # The producer cap can also manifest as truncation/connection reset
            # instead of a clean error document; a multi-record segment whose
            # attempts were still incomplete is bisected like an error
            # document, while a single-record segment keeps failing explicitly.
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise NhdHydrolocationReachCodeExternalConnectionError(
                "source_capture_failed:" + type(exc).__name__) from exc
        if _producer_error_doc(body):
            _witness_aside(capture_root, name)
            if count == 1:
                raise NhdHydrolocationReachCodeExternalConnectionError("source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        page = json.loads(body)
        if not isinstance(page, dict) or "error" in page:
            _witness_aside(capture_root, name)
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_error:" + name)
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) != count:
            _witness_aside(capture_root, name)
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_row_count_incomplete:" + name)
        expected = roster[offset:offset + count]
        actual = [row.get("attributes", {}).get("OBJECTID") for row in rows
                  if isinstance(row, dict)]
        if actual != expected:
            _witness_aside(capture_root, name)
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_id_slice_incomplete_or_reordered")
        segments[:] = [entry for entry in segments if entry.get("name") != name]
        segments.append({"name": name, "offset": offset, "count": count,
                         "features": len(rows), "captured_at": receipt["observed_at"],
                         "body_sha256": _sha(body)})
        _save_state(capture_root, state)
    return refreshed


def fetch_nhd_hydrolocation_reach_code_external_connection_pages(
        capture_root: Path, *, client: httpx.Client | None = None,
        doc_client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required segment
    and document.
    """
    capture_root = Path(capture_root).absolute()
    _preflight_capture(capture_root)
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    owner_client = client is None
    owner_doc = doc_client is None
    active = client or _default_client()
    docs_active = doc_client or _doc_client()
    segments: list[dict[str, object]] = list(state.get("segments", []))
    first_captured_at = (segments[0]["captured_at"] if segments
                         else datetime.now(timezone.utc).isoformat())
    urls = _document_urls()
    try:
        for name in METADATA_NAMES:
            active_client = docs_active if name == "service_doc" else active
            body, _receipt = _retained_or_fetch(active_client, capture_root, name, urls[name])
            if name != "service_doc" and _producer_error_doc(body):
                raise NhdHydrolocationReachCodeExternalConnectionError("source_page_error:" + name)
        seed = json.loads(_read_capture(capture_root, "ids_seed")[0]).get("objectIds")
        ids = _validate_roster(seed, "ids_seed")
        before = json.loads(_read_capture(capture_root, "ids_before")[0])
        if before.get("objectIdFieldName") != "OBJECTID":
            raise NhdHydrolocationReachCodeExternalConnectionError("source_id_field_changed")
        roster = _validate_roster(before.get("objectIds"), "ids_before")
        if sorted(roster) != ids:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_id_population_drift:ids_seed_vs_before")
        count_before = json.loads(_read_capture(capture_root, "count_before")[0])
        if type(count_before.get("count")) is not int or count_before["count"] != len(roster):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_count_drift")
        refreshed = _fetch_segments(active, capture_root, roster, state)
        segments = list(state["segments"])
        refresh_closing = refreshed or state.get("complete") is not True
        if not refresh_closing:
            for name in AFTER_NAMES:
                try:
                    body, _receipt = _read_capture(capture_root, name)
                    document = json.loads(body)
                    if not isinstance(document, dict) or "error" in document:
                        raise NhdHydrolocationReachCodeExternalConnectionError("source_page_error:" + name)
                except (OSError, ValueError):
                    refresh_closing = True
                    break
        if refresh_closing:
            state["complete"] = False
            _save_state(capture_root, state)
            for name in AFTER_NAMES:
                _witness_aside(capture_root, name)
            for name in AFTER_NAMES:
                body, _receipt = capture_get(active, urls[name], capture_root, name, max_bytes=MAX_BODY_BYTES)
                if _producer_error_doc(body):
                    raise NhdHydrolocationReachCodeExternalConnectionError("source_page_error:" + name)
        count_after = json.loads(_read_capture(capture_root, "count_after")[0])
        if type(count_after.get("count")) is not int or count_after["count"] != len(roster):
            raise NhdHydrolocationReachCodeExternalConnectionError(
                "source_metadata_or_count_changed_during_capture")
        after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
        if after_ids.get("objectIdFieldName") != "OBJECTID" \
                or sorted(_validate_roster(after_ids.get("objectIds"), "ids_after")) != ids:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_id_population_drift:ids_after")
    except NhdHydrolocationReachCodeExternalConnectionError:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise
    except Exception as exc:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()
    last_captured_at = max((str(entry.get("captured_at", "")) for entry in segments),
                           default=first_captured_at)
    extracted = _stream_validate(capture_root, parquet_sink=None)
    _save_state(capture_root, {"segments": segments, "complete": True})
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_nan_geometry_count": extracted["nan_geometry_count"],
        "source_empty_geometry_count": extracted["empty_geometry_count"],
        "source_featuretype_distribution": extracted["featuretype_distribution"],
        "observed_bounds": extracted["observed_bounds"],
        "resumed_segments": len(segments),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    return {"capture_directory": str(capture_root), "segments": len(segments), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


class _SegmentStream:
    """Streaming per-segment validator, digester and candidate parquet writer."""

    def __init__(self, capture_root: Path, fields: list[dict[str, object]],
                 parquet_sink: Path | None):
        self._capture_root = Path(capture_root).absolute()
        self.expected_names = tuple(entry["name"] for entry in fields)
        declared = {name: (esri_type, nullable)
                    for name, esri_type, nullable, _l in NATIVE_FIELDS}
        if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
                        for field in fields]
        arrow_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
        crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
        geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
               "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                             "geometry_types": ["Point Z"],
                                             "crs": crs, "edges": "planar"}}}
        self.schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
        self.parquet_sink = parquet_sink
        self.tmp_path = (parquet_sink.with_name(f"{parquet_sink.name}.construction-tmp")
                         if parquet_sink is not None else None)
        if self.tmp_path is not None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        # Never trust an existing output file: reconstruction always streams
        # from the validated segments into a temporary file that atomically
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
        self.nan_geometry = 0
        self.empty_geometry = 0
        self.distribution: dict[int, int] = {}
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page_name: str, offset: int, count: int, roster: list[int]) -> None:
        """Digest one retained roster segment into the digests and optional parquet sink."""
        if self._writer_closed:
            raise NhdHydrolocationReachCodeExternalConnectionError("stream_writer_already_closed")
        page = json.loads(_read_capture(self._capture_root, page_name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_error:" + page_name)
        reference = page.get("spatialReference")
        if (not isinstance(reference, dict)
                or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                or reference.get("latestWkid") != SOURCE_CRS_WKID):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_spatial_reference_drift:" + page_name)
        if page.get("objectIdFieldName") != "OBJECTID":
            raise NhdHydrolocationReachCodeExternalConnectionError("source_id_field_changed:" + page_name)
        # Layer 40 pages were observed carrying hasZ true with no hasM member;
        # the layer document pins the Z-aware/no-M contract, and a page that
        # explicitly declares M data is refused.
        if page.get("hasZ") is not True or bool(page.get("hasM")):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_z_or_m_flag_missing:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise NhdHydrolocationReachCodeExternalConnectionError("source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, geometry = _validate_feature(feature, self.expected_names,
                                                   expected_objectid)
            attrs = feature["attributes"]
            self.record_hash.update(_canonical(feature) + b"\x00")
            self.ids.append(objectid)
            self.last_objectid = objectid
            featuretype = attrs["featuretype"]
            self.distribution[featuretype] = self.distribution.get(featuretype, 0) + 1
            if geometry is None:
                self.null_geometry += 1
                geometries.append(None)
            elif geometry["x"] == "NaN" and geometry["y"] == "NaN":
                self.nan_geometry += 1
                geometries.append(None)
            else:
                self.geometry_hash.update(
                    _canonical([objectid, [geometry["x"], geometry["y"], geometry["z"]]])
                    + b"\x00")
                x, y = geometry["x"], geometry["y"]
                if self._xmin is None or x < self._xmin:
                    self._xmin = x
                if self._ymin is None or y < self._ymin:
                    self._ymin = y
                if self._xmax is None or x > self._xmax:
                    self._xmax = x
                if self._ymax is None or y > self._ymax:
                    self._ymax = y
                geometries.append(_point_wkb(geometry))
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
                "nan_geometry_count": self.nan_geometry,
                "empty_geometry_count": self.empty_geometry,
                "featuretype_distribution": dict(sorted(self.distribution.items())),
                "max_objectid": self.last_objectid,
                "observed_bounds": None if self._xmin is None else
                {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax, "ymax": self._ymax}}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    _preflight_capture(capture_root)
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    if b"3dhp.nationalmap.gov" not in _read_capture(capture_root, "service_doc")[0]:
        raise NhdHydrolocationReachCodeExternalConnectionError("usgs_service_documentation_link_drift")
    _validate_service_document(docs["service_before"], "service_before")
    _validate_service_document(docs["alias_service"], "alias_service")
    fields, schema_sha = _validate_layer_document(docs["layer_before"])
    declared = {name: (esri_type, nullable)
                for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_schema_baseline_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise NhdHydrolocationReachCodeExternalConnectionError("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before")
    count_before = docs["count_before"].get("count")
    if type(count_before) is not int or count_before != len(roster):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_count_drift")
    if roster != seed:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_id_population_drift:ids_seed_vs_before")
    after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
    if after_ids.get("objectIdFieldName") != "OBJECTID":
        raise NhdHydrolocationReachCodeExternalConnectionError("source_id_field_changed")
    after_roster = _validate_roster(after_ids.get("objectIds"), "ids_after")
    if after_roster != roster:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_id_population_drift:ids_after")
    # ArcGIS serves display-only rendering hints (for example
    # drawingInfo.authoringInfo.colorRamp) nondeterministically from the same
    # layer within minutes of each other, so the layer witness comparison
    # covers only the data-bearing contract fields; each is pinned
    # individually above. The service and count documents are compared whole.
    for name in ("service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"service_after": "service_before", "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise NhdHydrolocationReachCodeExternalConnectionError(
                "source_metadata_or_count_changed_during_capture")
    body, _receipt = _read_capture(capture_root, "layer_after")
    if _layer_contract_view(json.loads(body)) != _layer_contract_view(docs["layer_before"]):
        raise NhdHydrolocationReachCodeExternalConnectionError(
            "source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise NhdHydrolocationReachCodeExternalConnectionError("source_field_schema_changed_during_capture")
    state = _load_state(capture_root)
    segments = state.get("segments")
    if not isinstance(segments, list) or not segments \
            or any(not isinstance(item, dict) for item in segments):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(segments, key=lambda item: (item.get("offset", -1), item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _segment_name(offset, count) or offset != cursor
                or offset + count > len(roster) or entry.get("features") != count):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_roster_incomplete_or_unexpected")
        body, _receipt = _read_capture(capture_root, name)
        if entry.get("body_sha256") != _sha(body):
            raise NhdHydrolocationReachCodeExternalConnectionError("source_checkpoint_body_mismatch:" + name)
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != len(roster):
        raise NhdHydrolocationReachCodeExternalConnectionError("source_capture_roster_incomplete_or_unexpected")
    stream = _SegmentStream(capture_root, fields, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        count_after = json.loads(_read_capture(capture_root, "count_after")[0]).get("count")
        try:
            validate_capture_witness(count_before, roster, count_after, after_roster, list(stream.ids))
        except ValueError as exc:
            raise NhdHydrolocationReachCodeExternalConnectionError(str(exc)) from exc
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    ids_sha = summary["ids_sha"]
    return {"fields": fields, "schema_sha": schema_sha, "segments": len(ordered),
            "feature_count": len(stream.ids), "source_reported_count": count_before,
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": ids_sha, "parquet_path": parquet_sink,
            "null_geometry_count": summary["null_geometry_count"],
            "nan_geometry_count": summary["nan_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "featuretype_distribution": summary["featuretype_distribution"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"]}


def _code_identity() -> dict[str, object]:
    return processing_identity(REPO_ROOT)


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": USER_AGENT},
    )


def _doc_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=True, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": DOC_USER_AGENT},
    )


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_segments: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    audit = {
        "source_feature_count": extracted["feature_count"], "source_id_sha256": extracted["ids_sha"],
        "source_reported_count": extracted["source_reported_count"],
        "source_population_complete": True,
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Point Z",
        "null_geometry_count": extracted["null_geometry_count"],
        "nan_geometry_count": extracted["nan_geometry_count"],
        "empty_geometry_count": extracted["empty_geometry_count"],
        "featuretype_distribution": extracted["featuretype_distribution"],
        "featuretype_labels": dict(sorted(EXPECTED_FEATURETYPE_LABELS.items())),
        "unique_source_id_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "alias_service_url": ALIAS_SERVICE_URL,
        "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": "3D Hydrography Program (3DHP) Layers",
        "crosswalk_open_id": "650c07e542fa43b6a327c7f683681712",
        "completeness_basis": "stable captured counts and unique native ID rosters, "
                              "with exact ordered segment coverage",
        "baseline_comparison": "skipped_no_approved_baseline",
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The 3DHP_all service publishes no per-snapshot release date; the service "
            "copyrightText states a service refresh (September 4, 2026) and per-feature "
            "featuredate values are core-database load dates; neither is a dataset release "
            "date. Quarterly service updates and annual staged products are documented by USGS.",
        "pagination": {"mode": "enumerated_roster_bisected_segments",
                       "segment_start_records": SEGMENT_START_RECORDS,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "segments": extracted["segments"], "resumable": True,
                       "resumed_segments": resumed_segments,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerated the complete native OBJECTID roster. "
                           "Segments start at 2000 records "
                           "below the producer's 2500-record cap (sibling 3DHP_all layers "
                           "refused cap-sized pages with complete HTTP 200 Esri error "
                           "documents) and refused segments are bisected until served; "
                           "accepted segments must tile the roster exactly and before/after "
                           "count/roster brackets prove nothing was skipped, duplicated or "
                           "reordered."},
        "capture_window_utc": {"first_segment": first_captured_at,
                               "last_segment": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    def verify_existing():
        result = _verify_sealed_candidate(storage, prefix, state_key, version, identity,
                                          audit, parquet_body)
        return {**result, "capture_directory": str(capture_root)}

    reused = reuse_sealed_candidate(storage, state_key, verify_existing)
    if reused is not None:
        return reused
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    receipts = load_checkpoint_receipt_map(capture_root, (*METADATA_NAMES, *AFTER_NAMES),
                                           _load_state(capture_root), _load_receipt)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "HydroLocation - Reach Code, External Connection (USGS 3DHP_all)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 40 "
                       "'HydroLocation - External Connection' (the producer layer grouping "
                       "covering the External Connection and Reach Code hydrolocation point "
                       "categories named by the HIFLD Open inventory entry "
                       "nhd/hydrolocation-reach-code-external-connection), captured "
                       "completely from the producer's own REST publication. Complete "
                       "source attributes and EPSG:4326 Z-aware point geometry are "
                       "represented in the candidate. Succession, freshness, schema/ID "
                       "contract, coverage, rights and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no access or "
                   "use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication", "canonical_url": SERVICE_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason":
                        "The 3DHP_all service publishes no per-snapshot release date; its "
                        "copyrightText records a service refresh (September 4, 2026) and USGS "
                        "documents quarterly service updates and annual staged products.",
                    "license": "Public domain (U.S. Government work) per the producer service; "
                               "rights acceptance pending.",
                    "attribution": "USGS 3D Hydrography Program (3DHP_all), The National Map; "
                                   "formal attribution terms require maintainer review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": "the complete enumerated OBJECTID "
                                                          "roster captured as adaptively "
                                                          "bisected roster segments (the "
                                                          "producer layer grouping for the "
                                                          "External Connection and Reach "
                                                          "Code hydrolocation categories "
                                                          "named by the HIFLD inventory "
                                                          "entry)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types",
                                            "source_ids": "Preserve producer OBJECTID and id3dhp "
                                                          "values unchanged (id3dhp is the "
                                                          "producer's base-36 identifier, "
                                                          "documented as not persistent); no "
                                                          "substitute IDs, casts or joins",
                                            "geometry": "Source EPSG:4326 point ordinates (x, y, "
                                                        "z) retained verbatim as Point Z WKB "
                                                        "without rounding, projection or repair; "
                                                        "rows whose x/y ordinates are the "
                                                        "producer's NaN marker are gated to null "
                                                        "candidate geometry and counted instead "
                                                        "of being coerced",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/hydrolocation-reach-code-external-"
                   "connection ('HydroLocation - Reach Code, External Connection', agency US "
                   "Geological Survey) carries the description of the USGS 3D Hydrography "
                   "Program (3DHP) 3DHP_all service verbatim. USGS retired the NHD as a "
                   "maintained product on 2023-10-01 and the DHS HIFLD Open crosswalk "
                   "(DOI 10.3886/E241367V1, consolidated row '3D Hydrography Program (3DHP) "
                   "Layers', Open ID 650c07e542fa43b6a327c7f683681712) directs the NHD-family "
                   "layers to the 3dhp.nationalmap.gov usgs_3dhp_all FeatureServer captured "
                   "here (layer 40, the producer layer grouping for the External Connection "
                   "and Reach Code hydrolocation categories named by the inventory title: "
                   "USGS's own hydrogeofetch client calls this grouping 'hydrolocation - "
                   "reach code, external connection', the producer's hydrolocation category "
                   "enumeration defines External Connection (9), Reachcode Start (10) and "
                   "Reachcode End (11) for this layer, and the alias "
                   "hydro.nationalmap.gov 3DHP_all publication historically carried the "
                   "layer name 'HydroLocation - Reach Code, External Connection' verbatim; "
                   "the producer's current FeatureServer and MapServer layer name on both "
                   "hosts is 'HydroLocation - External Connection'); the same-org "
                   "hydro.nationalmap.gov 3DHP_all service is captured as the producer's "
                   "alias publication. The 3DHP hydrolocation point schema (10 fields, "
                   "non-persistent base-36 id3dhp) replaces the legacy NHD point-event "
                   "schema wholesale, so legacy attributes beyond these fields have no "
                   "successor equivalents and that succession/coverage contract change "
                   "remains an unaccepted maintainer decision. The DataLumos project 240692 "
                   "archive copy (archive modification 2025-04-08) is identification and "
                   "comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "Z-aware point geometry retained in the producer page CRS; all "
                      "succession, freshness, contract, rights and release approvals remain "
                      "pending.",
            "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile "
                                                        "field mapping.",
                                       "geopackage": "No approved source-to-public schema "
                                                     "mapping.",
                                       "pmtiles": "No display transformation or generalized "
                                                  "geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"],
             "succession_accepted": False, "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    metadata_outputs: dict[str, bytes] = {
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the seal never
    # holds more than one segment body/wire pair plus the small receipt maps.
    stage_candidate_objects(
        storage, prefix, state_key, version, identity, metadata_outputs,
        capture_root, receipts, _read_capture,
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'])
    # identity= selects the scheme-aware validation branch: a packaged runtime
    # (/app without .git) carries a packaged-sha256: commit that the Git-only
    # branch would reject as construction_invalid_commit after a valid seal.
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"],
                                        identity["files"], identity=identity)
    return {**_candidate_result(audit, prefix, version, reused=False),
            "capture_directory": str(capture_root)}


def _candidate_result(audit: Mapping[str, object], prefix: str, version: str, *, reused: bool) -> dict:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_reported_count": audit["source_reported_count"],
            "source_population_complete": audit["source_population_complete"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_geometry_count": audit["nan_geometry_count"],
            "source_empty_geometry_count": audit["empty_geometry_count"],
            "source_featuretype_distribution": audit["featuretype_distribution"],
            "observed_bounds": audit["observed_bounds"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": reused,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                             version: str, identity: Mapping[str, object],
                             audit: Mapping[str, object], current_parquet: bytes) \
        -> dict[str, object]:
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity",
            "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise NhdHydrolocationReachCodeExternalConnectionError("cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, REPO_ROOT, state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise NhdHydrolocationReachCodeExternalConnectionError("cached_native_candidate_changed")
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"],
                                        identity["files"], identity=identity)
    return _candidate_result(audit, prefix, version, reused=True)


def capture_nhd_hydrolocation_reach_code_external_connection(storage: StagingStorageResource, *, version: str,
                                       capture_directory: str | None = None) \
        -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NhdHydrolocationReachCodeExternalConnectionError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "0123456789.-_" for c in version):
        raise NhdHydrolocationReachCodeExternalConnectionError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise NhdHydrolocationReachCodeExternalConnectionError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise NhdHydrolocationReachCodeExternalConnectionError("disk_floor_before_acquisition")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (default_capture_dir(DATASET_SLUG, "captures") / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise NhdHydrolocationReachCodeExternalConnectionError("unsafe_capture_directory")
    _preflight_capture(capture_root)
    resumed_segments = len(_load_state(capture_root).get("segments", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_nhd_hydrolocation_reach_code_external_connection_pages(capture_root)
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
            except (OSError, NhdHydrolocationReachCodeExternalConnectionError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(
                                              capture_root, valid_names,
                                              NhdHydrolocationReachCodeExternalConnectionError),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_nhd_hydrolocation_reach_code_external_connection_baseline(capture_root: str) -> dict[str, object]:
    """Recompute observed audit facts from a complete retained capture."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": extracted["feature_count"],
            "source_max_objectid": extracted["max_objectid"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_nan_geometry_count": extracted["nan_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_featuretype_distribution": extracted["featuretype_distribution"],
            "observed_bounds": extracted["observed_bounds"]}
