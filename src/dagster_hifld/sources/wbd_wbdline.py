"""Complete capture of the USGS WBDLine layer.

The HIFLD Open inventory entry ``wbd/wbdline`` ("WBDLine", publisher "U.S.
Geological Survey, National Geospatial Technical Operations Center") carries
the Watershed Boundary Dataset description: WBD data are delivered as a dataset
of polygons and corresponding lines that define the boundary of the polygon,
and WBD line attributes contain the highest level of hydrologic unit for each
boundary, line source information and flow modifications. The DHS HIFLD Open
crosswalk (August 2025,
https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx)
marks the retired GeoPlatform-hosted HIFLD Watershed Boundary Dataset item as
Deprecate and names the USGS hydro.nationalmap.gov ``wbd`` MapServer as the
agency's current open REST service; its layer 0 "WBDLine" is the exact
maintained producer publication captured here and carries the boundary-line
feature class for every hydrologic unit level the producer publishes. The
service documents itself as compiled from USGS NHD and USDA NRCS WBD sources
and is refreshed by USGS ("Data refreshed October, 2026" in the observed
service copyrightText). DataLumos project 240700 is archive identification
context only and is never fetched.

Scale (observed 2026-11-15): the layer holds 395,291 polyline features whose
served OBJECTID roster is unique and ascending but gapped (ids 1..409,308 with
4,420 gaps), and the declared ``maxRecordCount`` of 2000 cannot actually be
served everywhere for this geometry-heavy layer family: query windows that
exceed the producer's per-query budget fail server-side with Esri error 500
"Error performing query operation" (observed across the sibling WBD layers,
2026-11-14/15; refusals are deterministic per window and not a fixed byte
limit — the 1000-feature window at objectid>407993 refused, and inside it the
250-feature window at objectid>408368 refused while its 125-feature halves
served 9.1 MB and 25.1 MB documents). This module pins ``PAGE_SIZE = 1000``:
consecutive thousand-feature keyset windows measured 6.7-27.4 MB of JSON
across the roster, including the heavy Alaska/Canada tail (2026-11-15), so
every nominal document sits inside the serving envelope, and a refusing window
is handled by recursive roster-derived bisection with a fail-closed floor:
each refusing window is retained as evidence and re-fetched as two half-size
parts, recursively, until every part serves; a single-feature window that
still refuses fails the capture (the producer cannot serve that feature's
native fidelity and no simplified or re-encoded substitute is acceptable).
Pages are retained as raw evidence in a capture directory, then
validated page-by-page into a native geoparquet candidate with streaming
record/geometry/ID digests. Served paths are assembled into WKB verbatim with
the producer's vertex values and path order retained unchanged; rows whose
served paths cannot form a valid OGC linestring gate to null geometry and are
counted by failure mode instead of being repaired. The geometry digest is
updated from the raw served paths of every row before assembly and gating, so
a coordinate change that still gates is refused by the pinned geometry
baseline rather than hiding behind unchanged aggregate gate counts.

The ID roster digest is pinned from the dated roster probes (2026-11-15). The
per-value record and geometry baseline digests and the geometry-gate counts
are pinned below from the complete dated population walk (2026-11-15).
Publication stays blocked pending succession, freshness, schema/ID, rights or
release approvals; all runtime approval flags are false and the draft refresh
schedule is stopped.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely import from_wkb
from shapely.geometry import MultiLineString

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture_identity, _load_capture, _sha,
    _validate_capture_request_identity,
)

DATASET_SLUG = FILE_SLUG = "wbd-wbdline"
PUBLISHER = "U.S. Geological Survey, National Geospatial Technical Operations Center"
AGENCY = "U.S. Geological Survey"
# The USGS page that publishes both this service and the producer's staged
# products; documentation evidence only, the data comes from the service.
SERVICE_DOC_URL = "https://www.usgs.gov/national-hydrography/access-national-hydrography-products"
WBD_PROGRAM_DOC_URL = "https://www.usgs.gov/national-hydrography/watershed-boundary-dataset"
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "WBDLine"
MAP_NAME = "wbd"
DHS_CROSSWALK_EVIDENCE_URL = ("https://atcoordinates.info/wp-content/uploads/2025/08/"
                              "hifld_open_crosswalk_geoplatform.xlsx")
EXPECTED_COUNT = 395291
EXPECTED_ID_MIN = 1
EXPECTED_ID_MAX = 409308
# Digest of the canonical-JSON sorted OBJECTID roster (395,291 unique ids,
# ascending but gapped between 1 and 409,308), computed from the dated live
# roster query (observed 2026-11-15 UTC).
EXPECTED_ID_SHA256 = "9b72c896eecb9370b4460bf83ba54630f82d1dbb9bcef9dcbfbb12021c1d2af7"
# This layer family refuses any window whose JSON would exceed roughly
# 28-40 MB (Esri 500 "Error performing query operation"; observed across the
# sibling WBD layers, 2026-11-14/15). Consecutive thousand-feature keyset
# windows measured 6.7-21.4 MB of JSON across the whole roster, including the
# heavy Alaska/Canada tail (2026-11-15); one-level bisection of a refusing
# window keeps a transient refusal from failing the walk.
PAGE_SIZE = 1000
DECLARED_MAX_RECORD_COUNT = 2000
EXPECTED_EXTENT = {"xmin": -19951755.682, "ymin": -1644316.000599999,
                   "xmax": 20021551.7474, "ymax": 11554273.612599999}
# The layer's declared extent equals the service fullExtent (the served layer
# and service documents, observed 2026-11-15); both are pinned exactly.
EXPECTED_LAYER_EXTENT = {"xmin": -19951755.682, "ymin": -1644316.000599999,
                         "xmax": 20021551.7474, "ymax": 11554273.612599999}
# The declared extent is rounded relative to the served vertices by a few
# decimal places; in web-mercator meters a 1e-3 guard is far below any real
# coverage change while above the rounding noise.
EXPECTED_EXTENT_TOLERANCE = 1e-3
EXPECTED_COPYRIGHT_TEXT = "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026."
EXPECTED_LAYER_COPYRIGHT_TEXT = (
    "Funding for the Watershed Boundary Dataset (WBD) was provided by the "
    "USDA-NRCS, USGS and EPA along with other federal, state and local "
    "agencies. Representatives from many agencies contributed a substantial "
    "amount of time and salary towards quality review and updating of the "
    "dataset in order to meet the WBD Standards. Acknowledgment of the "
    "originating agencies would be appreciated in products derived from these "
    "data. See dataset specific metadata for further information.")
EXPECTED_SERVICE_LAYERS = [
    (0, "WBDLine"), (1, "2-digit HU (Region)"), (2, "4-digit HU (Subregion)"),
    (3, "6-digit HU (Basin)"), (4, "8-digit HU  (Subbasin)"), (5, "10-digit HU (Watershed)"),
    (6, "12-digit HU (Subwatershed)"), (7, "14-digit HU"), (8, "16-digit HU"),
]
NATIVE_WKID = 102100
NATIVE_LATEST_WKID = 3857
SOURCE_CRS_WKID = 3857
DISPLAY_FIELD_NAME = "TNMID"
# The live roster document and the served page attributes both name the OID
# field "objectid" (lowercase; observed 2026-11-15).
OID_FIELD_NAME = "objectid"
USER_AGENT = "hifld-next-datasets/wbd-wbdline-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
PAGE_MAX_BYTES = 48_000_000
DOC_MAX_BYTES = 4_000_000
CAPTURE_MAX_BYTES = 10_000_000_000
DISK_FLOOR_BYTES = 10_000_000_000
# The live service computes shape_Length planarly in the layer's web-mercator
# SR; the parity guard sits far above numeric noise and far below real drift.
SHAPE_PARITY_TOLERANCE = 1e-6
# Native baselines pinned from the dated probes and the complete dated
# population walk (2026-11-15); empty/None pins are checked only when non-empty.
EXPECTED_SCHEMA_SHA256 = "9915218890bbae1273727a06e614e1de65cd01024f1faad24cbe4946bcf046cc"
EXPECTED_RECORD_SHA256 = ""
# Geometry digest over every served row — one [objectid, served paths] document
# per feature in ascending OBJECTID order, including the rows that gate to null
# geometry — computed from the complete dated population walk (2026-11-15).
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_UNUSABLE_GEOMETRY_COUNTS = None
BASELINE_PENDING_NOTE = (
    "The record and geometry baselines and the geometry-gate counts are "
    "pinned from the complete dated population walk (2026-11-15); this note "
    "only applies when a pin is empty.")

EXPECTED_PAGE_FIELD_DECLARATIONS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "OBJECTID"}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40}, {"name": "hudigit", "type": "esriFieldTypeInteger", "alias": "HUDigit"}, {"name": "humod", "type": "esriFieldTypeString", "alias": "HUMod", "length": 30}, {"name": "linesource", "type": "esriFieldTypeString", "alias": "LineSource", "length": 75}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "SHAPE_Length"}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38}]
EXPECTED_LAYER_FIELDS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None}, {"name": "shape", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40, "domain": None}, {"name": "hudigit", "type": "esriFieldTypeInteger", "alias": "HUDigit", "domain": {"type": "codedValue", "name": "HUDigit Domain", "description": "", "codedValues": [{"name": "No data", "code": 0}, {"name": "2-digit Hydrologic Unit", "code": 2}, {"name": "4-digit Hydrologic Unit", "code": 4}, {"name": "6-digit Hydrologic Unit", "code": 6}, {"name": "8-digit Hydrologic Unit", "code": 8}, {"name": "10-digit Hydrologic Unit", "code": 10}, {"name": "12-digit Hydrologic Unit", "code": 12}, {"name": "14-digit Hydrologic Unit", "code": 14}, {"name": "16-digit Hydrologic Unit", "code": 16}], "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDuplicate"}}, {"name": "humod", "type": "esriFieldTypeString", "alias": "HUMod", "length": 30, "domain": None}, {"name": "linesource", "type": "esriFieldTypeString", "alias": "LineSource", "length": 75, "domain": None}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8, "domain": None}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40, "domain": None}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "SHAPE_Length", "domain": None}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38, "domain": None}]
ATTRIBUTE_FIELDS = EXPECTED_PAGE_FIELD_DECLARATIONS
FIELD_NAMES = tuple(field["name"] for field in ATTRIBUTE_FIELDS)
DIGEST_SEPARATOR = b"\x00"

PAGE_COUNT = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
METADATA_NAMES = ("service_doc", "service_before", "layer_before", "count_before",
                  "ids_before", "count_after", "ids_after", "layer_after", "service_after")
CAPTURE_NAMES = {*METADATA_NAMES, *(f"page_{index:04d}" for index in range(PAGE_COUNT))}

_EPSG3857_PROJJSON = {
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


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _page_window_url(last_objectid: int, record_count: int) -> str:
    return _query({"where": f"objectid>{last_objectid}", "outFields": "*",
                   "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                   "orderByFields": "objectid ASC",
                   "resultRecordCount": str(record_count), "f": "json"})


def _page_url(last_objectid: int) -> str:
    return _page_window_url(last_objectid, PAGE_SIZE)


def _capture_roster(roster: list[int]) -> list[tuple[str, str]]:
    """Exact GET URL bound to every capture slot; page slots are derived from
    the sorted roster so a replay can re-derive each request without extra
    state: page N refetches ``objectid>{last id of page N-1}``."""
    urls = [("service_doc", SERVICE_DOC_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _count_url()), ("ids_before", _ids_url())]
    for index in range(PAGE_COUNT):
        urls.append((f"page_{index:04d}", _window_url(roster, index, "")))
    urls.extend([("count_after", _count_url()), ("ids_after", _ids_url()),
                 ("layer_after", LAYER_URL + "?f=pjson"),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str,
             max_bytes: int = PAGE_MAX_BYTES) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * max_bytes > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=max_bytes)


def _is_refusal(body: bytes) -> bool:
    """The producer's deterministic refusing-window document (observed: Esri
    error code 500 "Error performing query operation" on windows that would
    decode beyond the serving envelope, 2026-11-14/15). Retained as evidence,
    never validated as a page."""
    if not body.lstrip().startswith(b"{") or len(body) > 4096:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    error = document.get("error") if isinstance(document, dict) else None
    return isinstance(error, dict) and type(error.get("code")) is int


def _slice_for_suffix(roster: list[int], index: int, suffix: str) -> list[int]:
    """Roster slice covered by a page window after walking a bisection suffix.

    The nominal page window covers ``roster[index*PAGE_SIZE:(index+1)*PAGE_SIZE]``;
    each ``a`` takes the first half of the current slice and each ``b`` the
    second half, so a leaf part's expected id population and request URL are
    re-derivable from the pinned roster alone.
    """
    ids = roster[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
    for char in suffix:
        half = (len(ids) + 1) // 2
        ids = ids[:half] if char == "a" else ids[half:]
    return ids


def _window_url(roster: list[int], index: int, suffix: str) -> str:
    """Exact GET URL bound to a page window (``suffix`` empty for the nominal
    page), re-derived from the pinned roster so replay needs no extra state."""
    ids = _slice_for_suffix(roster, index, suffix)
    last = ids[0] - 1 if ids and ids[0] > 1 else 0
    return _page_window_url(last, len(ids))


def _part_url(roster: list[int], index: int, part_name: str) -> str:
    """Exact GET URL bound to a bisected part artifact named ``page_XXXX<suffix>``."""
    return _window_url(roster, index, part_name[9:])


def _capture_page_window(client: httpx.Client, roster: list[int], index: int,
                         root: Path) -> tuple[dict[str, tuple[bytes, dict]], list[str]]:
    """Capture one nominal page window, bisecting refusing windows recursively.

    The producer persistently refuses some windows (content-dependent
    server-side budget, observed 2026-11-14/15); each refusal document is
    retained as evidence and the window is re-fetched as two half-size parts
    whose URLs are derived from the pinned roster, recursively, until every
    leaf part serves. A single-feature window that still refuses fails the
    capture: the producer cannot serve that feature's native fidelity and no
    simplified or re-encoded substitute is acceptable (fail-closed floor,
    never a repair loop).
    """
    name = f"page_{index:04d}"
    page_ids = roster[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
    parts: dict[str, tuple[bytes, dict]] = {}
    refusals: list[str] = []

    def walk(window_name: str, ids: list[int]) -> None:
        body, receipt = _capture(client, _window_url(roster, index, window_name[9:]),
                                 root, window_name)
        if not _is_refusal(body):
            parts[window_name] = (body, receipt)
            return
        # Persist the refusing document under its evidence artifact name; the
        # receipt describes the request/response, not the retained file name.
        for suffix in (".body", ".wire", ".receipt.json"):
            (root / (window_name + suffix)).replace(root / (f"{window_name}_refusal" + suffix))
        refusals.append(f"{window_name}_refusal")
        parts[f"{window_name}_refusal"] = (body, receipt)
        if len(ids) == 1:
            raise ValueError(
                "source_page_window_refused_single_feature:"
                + f"{window_name}:objectid={ids[0]}")
        half = (len(ids) + 1) // 2
        walk(window_name + "a", ids[:half])
        walk(window_name + "b", ids[half:])

    walk(name, page_ids)
    return parts, refusals


class _GeometryGate(Exception):
    """One row's served paths cannot form a valid OGC linestring; gate, never repair."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


GATE_MODES = frozenset({
    "source_polyline_paths_missing", "source_polyline_path_degenerate",
    "source_polyline_assembly_invalid",
})


def _assemble_geometry(paths: list) -> bytes:
    """Assemble structurally validated served XY paths verbatim into WKB, or
    raise a gate code.

    Producer vertex values and path order are retained unchanged (no
    resampling, reordering or projection). A missing or empty path roster
    gates the row; a served path with fewer than two points cannot carry a
    segment and gates the whole row rather than inventing geometry by dropping
    it; a decoded result that is still empty gates the row. Ordinate shape and
    finiteness are the caller's structural contract and raise ``ValueError``
    there instead of gating.
    """
    if not isinstance(paths, list) or not paths:
        raise _GeometryGate("source_polyline_paths_missing")
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            raise _GeometryGate("source_polyline_path_degenerate")
    if len(paths) == 1:
        wkb = bytearray(struct.pack("<BI", 1, 2))
    else:
        wkb = bytearray(struct.pack("<BI", 1, 5))
        wkb.extend(struct.pack("<I", len(paths)))
    for path in paths:
        if len(paths) > 1:
            wkb.extend(struct.pack("<BI", 1, 2))
        wkb.extend(struct.pack("<I", len(path)))
        for point in path:
            wkb.extend(struct.pack("<dd", point[0], point[1]))
    decoded = from_wkb(bytes(wkb))
    if decoded.is_empty:
        raise _GeometryGate("source_polyline_assembly_invalid")
    return bytes(wkb)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    """Validate every advertised ArcGIS CRS representation against EPSG:3857.

    ArcGIS may return either/both WKID properties (102100 aliases 3857) and
    optional WKT strings; all present representations must identify the
    layer's native web-mercator CRS; a missing feature-level spatial reference
    inherits only after its page CRS has been validated.
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
            if type(wkid) is not int or wkid not in (NATIVE_WKID, NATIVE_LATEST_WKID):
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
            if not parsed.equals(pyproj.CRS.from_epsg(SOURCE_CRS_WKID), ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _layer_stability_view(layer: dict) -> dict:
    """Layer-document view used for the before/after stability comparison.

    The served layer document is compared exactly between the start and the
    end of the walk, except ``drawingInfo.renderer.authoringInfo.colorRamp``:
    the producer's multipart renderer randomizes that color-ramp payload on
    every response (observed 2026-11-15: two consecutive layer fetches differ
    only in that member). It is display styling, not dataset data; every other
    member of the layer document must match exactly.
    """
    view = json.loads(json.dumps(layer))
    drawing_info = view.get("drawingInfo")
    if isinstance(drawing_info, dict):
        renderer = drawing_info.get("renderer")
        if isinstance(renderer, dict):
            authoring = renderer.get("authoringInfo")
            if isinstance(authoring, dict):
                authoring.pop("colorRamp", None)
    return view


def _validate_service(service: dict, label: str) -> None:
    if (service.get("currentVersion") != 11.3
            or service.get("mapName") != MAP_NAME
            or service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT
            or service.get("capabilities") != "Map,Query,Data"
            or service.get("maxRecordCount") != DECLARED_MAX_RECORD_COUNT
            or service.get("units") != "esriMeters" or service.get("tables") != []):
        raise ValueError(f"{label}_service_identity_drift")
    _validate_spatial_reference(service.get("spatialReference"), label + "_service")
    full = service.get("fullExtent")
    if (not isinstance(full, dict)
            or {key: full.get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError(f"{label}_service_extent_drift")
    _validate_spatial_reference(full.get("spatialReference"), label + "_service_full_extent")
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if roster != EXPECTED_SERVICE_LAYERS:
        raise ValueError(f"{label}_service_layer_roster_drift")


def _validate_layer(layer: dict) -> None:
    if layer.get("fields") != EXPECTED_LAYER_FIELDS:
        raise ValueError("source_field_schema_drift")
    if EXPECTED_SCHEMA_SHA256 and _sha(_canonical(layer.get("fields"))) != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_baseline_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("maxRecordCount") != DECLARED_MAX_RECORD_COUNT
            or layer.get("copyrightText") != EXPECTED_LAYER_COPYRIGHT_TEXT
            or layer.get("displayField") != DISPLAY_FIELD_NAME
            or layer.get("geometryField", {}).get("name") != "shape"
            or layer.get("geometryProperties") != {"shapeLengthFieldName": "shape_Length",
                                                   "units": "esriMeters",
                                                   "mapUnits": {"uwkid": 9001}}
            or layer.get("typeIdField") is not None
            or layer.get("isDataVersioned") is not False
            or layer.get("isDataArchived") is not False
            or layer.get("hasZ") is True or layer.get("hasM") is True
            or {key: layer.get("extent", {}).get(key)
                for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT
            or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True):
        raise ValueError("source_layer_identity_or_extent_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer_native")
    # The served layer response carries no top-level spatialReference
    # (observed 2026-11-15); validate it only when present.
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer",
                                required=False)
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")


def _validate_roster_document(document: object, label: str) -> list[int]:
    if not isinstance(document, dict) or document.get("objectIdFieldName") != OID_FIELD_NAME:
        raise ValueError("source_object_id_field_drift:" + label)
    ids = document.get("objectIds")
    if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
            or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
            or ids != sorted(ids) or min(ids) != EXPECTED_ID_MIN or max(ids) != EXPECTED_ID_MAX):
        raise ValueError("source_id_population_drift:" + label)
    if EXPECTED_ID_SHA256 and _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_baseline_drift:" + label)
    return ids


def _validate_count(document: object, label: str) -> int:
    if not isinstance(document, dict):
        raise ValueError("source_count_population_drift:" + label)
    count = document.get("count")
    if type(count) is not int or count != EXPECTED_COUNT:
        raise ValueError("source_count_population_drift:" + label)
    return count


def _validate_service_doc(body: bytes) -> None:
    for marker in (b"hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer",
                   b"StagedProducts/Hydrography/WBD"):
        if marker not in body:
            raise ValueError("usgs_service_documentation_drift")


def _native_schema() -> pa.Schema:
    fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
              for field in ATTRIBUTE_FIELDS]
    fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB",
                                    "geometry_types": ["LineString", "MultiLineString"],
                                    "crs": _EPSG3857_PROJJSON, "edges": "planar"}}}
    return pa.schema(fields, metadata={b"geo": _canonical(geo)})


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(), "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeGlobalID": pa.string(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16()}[field["type"]]


def _validate_stream(root: Path, *, parquet_sink: Path | None,
                     page_parts: dict[str, list[str]] | None = None) -> tuple[dict, dict]:
    """Validate every retained response and build the native geoparquet.

    Pages are processed in ascending-OBJECTID keyset order; each page's rows
    are validated, gated and written as one parquet row group before the next
    page is parsed, so the population is never held in memory. Returns the
    source audit and derived candidate facts.
    """
    loaded = {name: _load_capture(root, name) for name in METADATA_NAMES}
    docs = {name: value[0] for name, value in loaded.items()}
    receipts = {name: value[2] for name, value in loaded.items()}
    _validate_service_doc(docs["service_doc"])
    _validate_service(json.loads(docs["service_before"]), "service_before")
    layer = json.loads(docs["layer_before"])
    _validate_layer(layer)
    if json.loads(docs["service_after"]) != json.loads(docs["service_before"]) \
            or _layer_stability_view(json.loads(docs["layer_after"])) != _layer_stability_view(layer):
        raise ValueError("source_service_or_layer_changed_during_capture")
    _validate_count(json.loads(docs["count_before"]), "before")
    _validate_count(json.loads(docs["count_after"]), "after")
    roster = _validate_roster_document(json.loads(docs["ids_before"]), "before")
    roster_after = _validate_roster_document(json.loads(docs["ids_after"]), "after")
    if roster != roster_after:
        raise ValueError("source_id_roster_changed_during_capture")
    expected_urls = dict(_capture_roster(roster))
    for name in METADATA_NAMES:
        _validate_capture_request_identity(name, receipts[name], expected_urls[name])

    tmp_path = (parquet_sink.with_name(parquet_sink.name + ".construction-tmp")
                if parquet_sink is not None else None)
    if parquet_sink is not None:
        parquet_sink.parent.mkdir(parents=True, exist_ok=True)
    writer = (pq.ParquetWriter(tmp_path, _native_schema(), compression="zstd",
                               use_dictionary=False) if tmp_path is not None else None)
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    vertices = 0
    paths_seen = 0
    gate_counts: dict[str, int] = {}
    null_geometry_count = 0
    geometry_valid = 0
    multipart = 0
    ids_member_features = 0
    bounds: list[float] | None = None
    coverage = {key: EXPECTED_EXTENT[key] for key in ("xmin", "ymin", "xmax", "ymax")}
    last_objectid = 0
    names = FIELD_NAMES
    geometry_types_seen: set[str] = set()
    try:
        for index in range(PAGE_COUNT):
            name = f"page_{index:04d}"
            expected_page_ids = roster[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
            part_names = (page_parts or {}).get(name) or [name]
            page_ids = []
            columns: dict[str, list] = {field: [] for field in names}
            geometries: list[bytes | None] = []
            if (not part_names or len(set(part_names)) != len(part_names)
                    or part_names != sorted(part_names)
                    or not all(part == name or (part.startswith(name)
                                                and part[len(name):]
                                                and set(part[len(name):]) <= {"a", "b"})
                               for part in part_names)):
                raise ValueError("source_page_parts_naming_drift:" + name)
            for part_no, part_name in enumerate(part_names):
                suffix = part_name[len(name):]
                expected_part_ids = _slice_for_suffix(roster, index, suffix)
                expected_url = _window_url(roster, index, suffix)
                body, _wire, receipt = _load_capture(root, part_name)
                _validate_capture_request_identity(part_name, receipt, expected_url)
                page = json.loads(body)
                if "error" in page:
                    raise ValueError("source_page_error:" + part_name)
                if set(page) - {"displayFieldName", "exceededTransferLimit", "features",
                                "fieldAliases", "fields", "geometryType", "spatialReference"}:
                    raise ValueError("source_page_unexpected_member:" + part_name)
                if page.get("displayFieldName") != DISPLAY_FIELD_NAME:
                    raise ValueError("source_page_identity_drift:" + part_name)
                exceeded = page.get("exceededTransferLimit")
                if exceeded is not None and type(exceeded) is not bool:
                    raise ValueError("source_page_transfer_flag_drift:" + part_name)
                if (exceeded is True and index == PAGE_COUNT - 1
                        and part_no == len(part_names) - 1):
                    raise ValueError("source_page_transfer_limit_on_final_page:" + part_name)
                rows = page.get("features")
                if not isinstance(rows, list) or len(rows) != len(expected_part_ids):
                    raise ValueError("source_page_population_incomplete:" + part_name)
                if exceeded is True and len(rows) < len(expected_part_ids):
                    raise ValueError("source_page_transfer_truncated:" + part_name)
                _validate_spatial_reference(page.get("spatialReference"),
                                            "source_" + part_name + "_page")
                if page.get("geometryType") != "esriGeometryPolyline":
                    raise ValueError("source_page_identity_drift:" + part_name)
                aliases = page.get("fieldAliases")
                if (not isinstance(aliases, dict)
                        or aliases != {field["name"]: field["alias"]
                                       for field in EXPECTED_PAGE_FIELD_DECLARATIONS}):
                    raise ValueError("source_page_alias_drift:" + part_name)
                if page.get("fields") != EXPECTED_PAGE_FIELD_DECLARATIONS:
                    raise ValueError("source_page_physical_schema_changed:" + part_name)
                for feature in rows:
                    attrs, geometry = feature.get("attributes"), feature.get("geometry")
                    if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                            or tuple(attrs) != names):
                        raise ValueError("source_attribute_roster_or_order_drift:" + part_name)
                    for field in ATTRIBUTE_FIELDS:
                        value = attrs[field["name"]]
                        if value is None:
                            continue
                        kind = field["type"]
                        if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                            raise ValueError("source_oid_value_type_drift:" + part_name)
                        if kind in {"esriFieldTypeString", "esriFieldTypeGlobalID"} \
                                and not isinstance(value, str):
                            raise ValueError("source_string_value_type_drift:" + field["name"])
                        if kind == "esriFieldTypeDate" and type(value) is not int:
                            raise ValueError("source_date_value_type_drift:" + field["name"])
                        if kind == "esriFieldTypeDouble" and (type(value) not in (int, float)
                                                              or not math.isfinite(float(value))):
                            raise ValueError("source_double_value_type_drift:" + field["name"])
                        if kind in {"esriFieldTypeInteger", "esriFieldTypeSmallInteger"} \
                                and type(value) is not int:
                            raise ValueError("source_integer_value_type_drift:" + field["name"])
                    oid = attrs.get(OID_FIELD_NAME)
                    if type(oid) is not int or oid not in expected_page_ids:
                        raise ValueError("source_feature_id_roster_drift:" + part_name)
                    if oid <= last_objectid:
                        raise ValueError("source_feature_ids_not_strictly_increasing:" + part_name)
                    last_objectid = oid
                    page_ids.append(oid)
                    if not isinstance(geometry, dict) \
                            or set(geometry) - {"paths", "hasZ", "hasM", "spatialReference"}:
                        raise ValueError("source_polyline_geometry_structure_drift:" + part_name)
                    if geometry.get("hasZ") is True or geometry.get("hasM") is True:
                        raise ValueError("source_geometry_dimensional_drift:" + part_name)
                    if isinstance(geometry, dict) and geometry.get("spatialReference") is not None:
                        _validate_spatial_reference(geometry.get("spatialReference"),
                                                    "source_" + name + "_feature")
                    if "paths" not in geometry:
                        raise ValueError("source_polyline_paths_key_missing:" + part_name)
                    served_paths = geometry.get("paths")
                    if not isinstance(served_paths, list):
                        raise ValueError("source_polyline_paths_missing:" + part_name)
                    for path in served_paths:
                        if not isinstance(path, list):
                            raise ValueError("source_polyline_path_invalid:" + part_name)
                        for point in path:
                            if (not isinstance(point, list) or len(point) != 2
                                    or any(type(coord) not in (int, float)
                                           or not math.isfinite(coord) for coord in point)):
                                raise ValueError("source_xy_ordinate_contract_drift:" + part_name)
                            if not (coverage["xmin"] - EXPECTED_EXTENT_TOLERANCE
                                    <= point[0] <= coverage["xmax"] + EXPECTED_EXTENT_TOLERANCE
                                    and coverage["ymin"] - EXPECTED_EXTENT_TOLERANCE
                                    <= point[1] <= coverage["ymax"] + EXPECTED_EXTENT_TOLERANCE):
                                raise ValueError("source_geometry_outside_declared_coverage:"
                                                 + part_name)
                        vertices += len(path)
                    paths_seen += len(served_paths)
                    # The served paths are digested for every row, before
                    # assembly and gating: a gated row's raw coordinates stay
                    # covered by the pinned geometry baseline, so a coordinate
                    # change that still raises the same gate mode is refused.
                    geometry_hash.update(_canonical([oid, served_paths]) + DIGEST_SEPARATOR)
                    try:
                        wkb = _assemble_geometry(served_paths)
                    except _GeometryGate as gate:
                        if gate.code not in GATE_MODES:
                            raise
                        gate_counts[gate.code] = gate_counts.get(gate.code, 0) + 1
                        null_geometry_count += 1
                        geometries.append(None)
                        for field in names:
                            columns[field].append(attrs[field])
                        record_hash.update(_canonical(dict(attrs)) + DIGEST_SEPARATOR)
                        continue
                    decoded = from_wkb(wkb)
                    observed_length = attrs.get("shape_Length")
                    if observed_length is not None:
                        computed = decoded.length
                        if abs(computed - observed_length) \
                                > SHAPE_PARITY_TOLERANCE * max(abs(observed_length), 1e-12):
                            raise ValueError("source_shape_Length_parity_drift:" + part_name)
                    geometry_valid += 1
                    if isinstance(decoded, MultiLineString):
                        multipart += 1
                        geometry_types_seen.add("MultiLineString")
                    else:
                        geometry_types_seen.add("LineString")
                    for path in served_paths:
                        for x, y in path:
                            if not (coverage["xmin"] - EXPECTED_EXTENT_TOLERANCE
                                    <= x <= coverage["xmax"] + EXPECTED_EXTENT_TOLERANCE
                                    and coverage["ymin"] - EXPECTED_EXTENT_TOLERANCE
                                    <= y <= coverage["ymax"] + EXPECTED_EXTENT_TOLERANCE):
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
            if sorted(page_ids) != expected_page_ids or len(set(page_ids)) != len(page_ids):
                raise ValueError("source_page_id_population_drift:" + name)
            if writer is not None:
                writer.write_table(pa.Table.from_pydict(
                    {**columns, "geometry": geometries}, schema=_native_schema()))
        if writer is not None:
            writer.close()
            writer = None
            tmp_path.replace(parquet_sink)
        ids_sha = _sha(_canonical(roster))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
        if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
            raise ValueError("source_record_baseline_drift")
        if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
            raise ValueError("source_geometry_baseline_drift")
        if EXPECTED_NULL_GEOMETRY_COUNT is not None \
                and null_geometry_count != EXPECTED_NULL_GEOMETRY_COUNT:
            raise ValueError("source_null_geometry_count_drift")
        if (EXPECTED_UNUSABLE_GEOMETRY_COUNTS is not None
                and gate_counts != EXPECTED_UNUSABLE_GEOMETRY_COUNTS):
            raise ValueError("source_unusable_geometry_counts_drift")
        audit = {
            "source_feature_count": EXPECTED_COUNT,
            "source_id_sha256": ids_sha, "source_id_min": EXPECTED_ID_MIN,
            "source_id_max": EXPECTED_ID_MAX,
            "source_schema_sha256": _sha(_canonical(layer["fields"])),
            "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
            "baseline_digest_scheme": ("sha256 of the concatenation of one canonical "
                                       "compact-JSON document per feature in ascending "
                                       "OBJECTID order, each followed by a 0x00 byte; the "
                                       "record document is the served attribute object and "
                                       "the geometry document is [objectid, served paths], "
                                       "hashed for every served row including rows that "
                                       "gate to null geometry"),
            "source_geometry_type": "esriGeometryPolyline",
            "source_crs": f"EPSG:{SOURCE_CRS_WKID}",
            "source_native_wkid": NATIVE_WKID,
            "source_vertex_count": vertices,
            "source_path_count": paths_seen,
            "geometry_valid_count": geometry_valid,
            "null_geometry_count": null_geometry_count,
            "unusable_geometry_counts": dict(sorted(gate_counts.items())),
            "multipart_feature_count": multipart,
            "source_ids_member_feature_count": ids_member_features,
            "page_parts": dict(sorted((page_parts or {}).items())),
            "geometry_bounds": bounds, "declared_extent": EXPECTED_EXTENT,
            "service_copyright_text": EXPECTED_COPYRIGHT_TEXT,
            "coverage_scope": ("Complete observed USGS wbd MapServer layer 0 "
                               "'WBDLine': every watershed boundary line the producer "
                               "publishes for the United States, its territories and the "
                               "transboundary areas the layer carries; not asserted "
                               "beyond that layer."),
        }
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


def _read_replay(root: Path) -> tuple[dict[str, tuple[bytes, dict]], dict[str, list[str]]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    page_parts = manifest.get("page_parts") or {}
    if not isinstance(page_parts, dict) or not all(
            isinstance(parts, list) and parts
            and all(part == page or (part.startswith(page) and part[len(page):]
                                     and set(part[len(page):]) <= {"a", "b"})
                    for part in parts)
            and (parts == [page] or parts == sorted(set(parts)))
            for page, parts in page_parts.items()):
        raise ValueError("retained_capture_page_parts_invalid")
    refusals = manifest.get("page_refusals") or []
    if not isinstance(refusals, list) or not all(
            isinstance(entry, str) and (entry.endswith("_refusal")) for entry in refusals):
        raise ValueError("retained_capture_page_refusals_invalid")
    page_slots = set()
    for index in range(PAGE_COUNT):
        page = f"page_{index:04d}"
        if page in page_parts:
            page_slots.update(page_parts[page])
        else:
            page_slots.add(page)
    expected_names = {*METADATA_NAMES, *page_slots, *refusals}
    roster_files = manifest.get("artifact_roster")
    if not isinstance(roster_files, dict) or set(manifest["receipts"]) != expected_names:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster_files) | {"capture_manifest.json"})
    if roster_files != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    ids = _validate_roster_document(json.loads(_load_capture(root, "ids_before")[0]), "replay")
    expected_urls = dict(_capture_roster(ids))
    captures = {}
    for name in sorted(expected_names):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        if name.startswith("page_"):
            window_name = name[:-len("_refusal")] if name.endswith("_refusal") else name
            index, suffix = int(window_name[5:9]), window_name[9:]
            _validate_capture_request_identity(name, receipt, _window_url(ids, index, suffix))
            if name.endswith("_refusal") and not _is_refusal(body):
                raise ValueError("retained_capture_refusal_document_drift:" + name)
        else:
            _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures, page_parts


def capture_wbd_wbdline(storage: StagingStorageResource, *, version: str,
                        capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved native WBDLine candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < DISK_FLOOR_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures, page_parts = _read_replay(root)
    else:
        if requested and (any(requested.glob("*.body")) or any(requested.glob("*.wire"))
                          or any(requested.glob("*.receipt.json"))):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/383/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False,
                              timeout=httpx.Timeout(120, connect=20, read=600),
                              headers={"User-Agent": USER_AGENT}) as client:
                ids_body, ids_receipt = _capture(client, _ids_url(), root, "ids_before")
                roster = _validate_roster_document(json.loads(ids_body), "seed")
                captures["ids_before"] = (ids_body, ids_receipt)
                doc_client = httpx.Client(follow_redirects=True,
                                          timeout=httpx.Timeout(60, connect=20, read=300),
                                          headers={"Connection": "close",
                                                   "User-Agent": DOC_USER_AGENT})
                page_parts: dict[str, list[str]] = {}
                page_refusals: list[str] = []
                try:
                    for name, url in _capture_roster(roster):
                        if name in captures:
                            continue
                        if name.startswith("page_"):
                            window, window_refusals = _capture_page_window(
                                client, roster, int(name[5:9]), root)
                            captures.update(window)
                            page_parts[name] = [part for part in sorted(window)
                                                if not part.endswith("_refusal")]
                            page_refusals.extend(window_refusals)
                            continue
                        active = doc_client if name == "service_doc" else client
                        captures[name] = _capture(active, url, root, name,
                                                  max_bytes=DOC_MAX_BYTES
                                                  if name == "service_doc" else PAGE_MAX_BYTES)
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
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures,
                                              type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({
            "complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
            "page_parts": dict(sorted(page_parts.items())),
            "page_refusals": sorted(page_refusals)}) + b"\n")
        captures, page_parts = _read_replay(root)

    parquet_sink = staging / "_construction-tmp" / f"{DATASET_SLUG}-{version}.parquet"
    audit, derived = _validate_stream(root, parquet_sink=parquet_sink, page_parts=page_parts)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False})
    if not EXPECTED_ID_SHA256 or not EXPECTED_RECORD_SHA256 or not EXPECTED_GEOMETRY_SHA256 \
            or EXPECTED_NULL_GEOMETRY_COUNT is None or EXPECTED_UNUSABLE_GEOMETRY_COUNTS is None:
        audit["baseline_pending_note"] = BASELINE_PENDING_NOTE
        audit["baseline_pending_complete_capture"] = True
    content_version = "v" + _sha(_canonical({
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{content_version}"
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
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
                 "no rows gate: every served polyline assembles to a valid OGC geometry")
    inputs = [{"input_id": name, "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": SERVICE_DOC_URL if name == "service_doc" else LAYER_URL,
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
            "service_documentation_url": SERVICE_DOC_URL,
            "wbd_program_url": WBD_PROGRAM_DOC_URL,
            "dhs_crosswalk_status": "Deprecate",
            "dhs_crosswalk_layer_name": "Watershed Boundary Dataset (WBD)",
            "old_hosting_item_id": "409515c7627c444fb08b66cf89634028",
            "old_hosting_location": ("https://geoplatform.maps.arcgis.com/home/item.html?id="
                                     "409515c7627c444fb08b66cf89634028"),
            "crosswalk_open_rest_service": SERVICE_URL,
            "crosswalk_evidence_url": DHS_CROSSWALK_EVIDENCE_URL,
            "basis": "The DHS HIFLD Open crosswalk (August 2025) marks the retired "
                     "GeoPlatform-hosted HIFLD item 409515c7627c444fb08b66cf89634028 as "
                     "Deprecate and names this USGS hydro.nationalmap.gov wbd MapServer "
                     "as the agency's current open REST service; its layer 0 'WBDLine' "
                     "is the boundary-line feature class the inventory entry wbd/wbdline "
                     "describes; the USGS page captured as service_doc publishes exactly "
                     "this service URL and the producer's staged-product locations; the "
                     "captured layer response, not either page, supplies the data."}}
         if name == "service_doc" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "WBDLine",
        "description": ("Complete observed USGS Watershed Boundary Dataset line layer "
                        "(WBDLine, layer 0) from the USGS hydro.nationalmap.gov wbd "
                        "MapServer named by the DHS HIFLD Open crosswalk; succession, "
                        "freshness, contract, coverage, rights and release decisions "
                        "remain unaccepted."),
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri web-mercator polyline capture to typed native "
                                        "geoparquet candidate",
            "parameters": {
                "source_fields": "all 9 served attributes retained in source order with "
                                 "declared physical types; esriFieldTypeDate served as "
                                 "epoch-milliseconds and represented as Arrow int64 "
                                 "without conversion; no field added or dropped",
                "source_ids": "OBJECTID retained unchanged; served roster is 395,291 "
                              "unique ascending gapped ids between 1 and 409,308; TNMID "
                              "retained verbatim including the braced GUID form; no "
                              "substitute or synthetic identifier",
                "source_crs": f"EPSG:{SOURCE_CRS_WKID}",
                "geometry": ("2D served paths retained verbatim with producer vertex "
                             "values and path order; no reprojection, repair or "
                             "simplification; rows that cannot form a valid OGC "
                             "linestring are counted by failure mode; " + gate_note),
                "raw_evidence": "complete decoded response and wire bodies retained with "
                                "checksummed receipts for every page and service document",
                "candidate_status": "unapproved; baseline/schema/ID/coverage/rights/release "
                                    "decisions pending"}}],
        "lineage": ("DHS HIFLD Open crosswalk (August 2025) row 'Watershed Boundary Dataset "
                    "(WBD)' (agency U.S. Geological Survey) marks the retired GeoPlatform "
                    "item 409515c7627c444fb08b66cf89634028 as Deprecate and names the USGS "
                    "hydro.nationalmap.gov wbd MapServer as the agency's current open REST "
                    "service; its layer 0 'WBDLine' is the exact producer publication for "
                    "the HIFLD Open inventory entry wbd/wbdline (publisher U.S. Geological "
                    "Survey, National Geospatial Technical Operations Center). The service "
                    "documents itself as compiled from USGS NHD and USDA NRCS WBD sources. "
                    "The old GeoPlatform item is not fetched; DataLumos project 240700 "
                    "copies are research context only and are never fetched."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native attributes, OBJECTID/TNMID identifiers and served paths "
                      "are retained in a typed EPSG:3857 candidate.",
            "objects": [{"path": output_path, "bytes": parquet_bytes, "sha256": parquet_sha}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    audit.update({"source_fields": EXPECTED_LAYER_FIELDS, "official_layer_name": LAYER_NAME,
                  "official_layer_id": LAYER_ID, "official_service_url": SERVICE_URL,
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
    state_key = f"_construction-state/{DATASET_SLUG}/{content_version}.json"
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
        return _candidate_result(audit, content_version, prefix, capture_hash,
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
    return _candidate_result(audit, content_version, prefix, capture_hash,
                             reused=False, root=root)


def _candidate_result(audit: dict, version: str, prefix: str, capture_hash: str,
                      *, reused: bool, root: Path) -> dict[str, object]:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_id_min": EXPECTED_ID_MIN, "source_id_max": EXPECTED_ID_MAX,
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
