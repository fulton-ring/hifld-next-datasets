"""Lossless, approval-gated capture of the USGS 3DHP_all Waterbody areal hydrography.

The HIFLD Open inventory entry ``nhd/area-large-scale-2`` ("Area - Large Scale",
agency US Geological Survey) was the National Hydrography Dataset (NHD)
high-resolution areal hydrography layer (legacy feature class ``NHDArea``). USGS
retired the NHD as a maintained product on 2023-10-01 and the inventory
description itself says "Please use 3DHP for updated data"; the DHS HIFLD Open
crosswalk (DOI 10.3886/E241367V1) consolidates every NHD-family layer into one
"3D Hydrography Program (3DHP) Layers" row whose Open REST Service is the USGS
``3dhp.nationalmap.gov`` ``usgs_3dhp_all`` FeatureServer. That producer service
is the maintained successor captured here; the DataLumos project 239953 archive
copy is identification and comparison context only and is never fetched.

Successor scope per the USGS NHD->3DHP crosswalk: legacy NHDArea water-extent
content maps into the 3DHP Waterbody feature class as featuretype River (legacy
StreamRiver 460), Canal (legacy CanalDitch 336) and Ocean or Great Lake (legacy
SeaOcean 445); the legacy Lake features (NHDWaterbody 390/436/493) belong to the
waterbody datasets, so this capture takes Waterbody rows with ``featuretype<>3``
(not Lake). Other legacy NHDArea landmark subtypes (bridge, dam/weir, lock
chamber, rapids, wash, levee, ...) have no polygon counterpart in the 3DHP
crosswalk and are therefore not reconstructible from this successor; that
coverage change is recorded in the manifest lineage and remains an unaccepted
maintainer decision. The 3DHP schema (12 fields, non-persistent base-36
``id3dhp``) replaces the legacy NHDArea schema wholesale; publication stays
blocked pending the schema/ID contract, coverage, rights and release decisions.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import MultiPolygon, Polygon

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

DATASET_SLUG = "nhd-area-large-scale-2"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 60
LAYER_NAME = "Waterbody"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-area-large-scale-2-draft"
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
POPULATION_WHERE = "featuretype<>3"
# Initial window size over the pinned OBJECTID roster; oversized or failing
# windows are bisected down to single OBJECTIDs deterministically (each captured
# page is named by the roster's first OBJECTID it requests).
PAGE_IDS = 500
MAX_BODY_BYTES = 96 * 1024 * 1024
# The 3DHP non-Lake waterbody population is unusually heavy per feature (the
# roster's main-stem polygons carry tens of thousands of Z vertices each);
# the budget counts both wire and decoded bytes under the producer's identity
# encoding, so it is sized for the observed full-population envelope with
# headroom rather than a generic page-domain budget.
MAX_CAPTURE_BYTES = 30 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 25_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Producer-stated service refresh ("Data refreshed September 4, 2026" in the
# captured service copyrightText) is freshness evidence, not a release date.
EXPECTED_COPYRIGHT_TEXT = ("Credits: USGS TNM / NGTOC – 3D National Hydrographic Program "
                           "(3DHP.) Data refreshed September 4, 2026.")
EXPECTED_SERVICE_LAYERS = [
    (20, "HydroLocation - Sink, Spring, Waterbody Outlet"),
    (30, "HydroLocation - Headwater, Terminus, Divergence, Confluence, Catchment Outlet"),
    (40, "HydroLocation - External Connection"),
    (50, "Flowline"),
    (60, "Waterbody"),
    (80, "Catchment"),
    (90, "Spatial Metadata - DEM LimitationArea"),
    (92, "Spatial Metadata - EDH Workunit Area"),
    (94, "Spatial Metadata - Elevation Workunit Area"),
]
EXPECTED_COUNT = 95423
# Producer-refused records (dated 2026-11-13 evidence): OBJECTIDs the producer's
# own REST publication persistently refuses to serve geometry for (HTTP 200 Esri
# ``{"error":{"code":500,"message":"Error performing query operation"}}`` on both
# 3dhp.nationalmap.gov and the same-org alias hydro.nationalmap.gov/3DHP_all,
# single-id and windowed, while the same record's attributes and layer extent
# are served normally and the full roster pins it into the population). Their
# attributes are captured through the producer's working attributes-only channel
# and staged verbatim with explicitly gated null candidate geometry; the refusal
# set is pinned and any change fails closed.
EXPECTED_REFUSED_IDS: frozenset[int] = frozenset({
    1293173, 2520496, 4245000, 4694427, 7059540, 7059541, 7098625, 7135496, 7157221})
EXPECTED_NULL_GEOMETRY_COUNT = 9
# Observed 2026-11-13 complete non-Lake population contract (featuretype<>3),
# re-derived from the dated full capture (330 pages, 9.0 GB wire+body under the
# producer's identity encoding, no validation errors): the roster drifted from
# the 2026-11-12 draft pins and the service refuses geometry for the nine
# pinned records above, so null_geometry_count equals the refused count and the
# featuretype distribution counts served geometries only.
EXPECTED_ID_SHA256 = "da7641ddc6a3bc7849ce36069620de9c1c8b395b9fcb4d0dff4e49b7696942af"
EXPECTED_SCHEMA_SHA256 = "51f11bd107670196d396c0908b3c68a4dce0f37466d3dd179c3df149fd4a04d3"
EXPECTED_RECORD_SHA256 = "b424080aa54579c8011c24ae2f4e90550394756e9a81230fcfe37ac386b4868d"
EXPECTED_GEOMETRY_SHA256 = "baaf0ae8c825420e64efa19ca7074b52e9abd5a7d61c8a546f6eae4983f31ab7"
EXPECTED_FEATURETYPE_DISTRIBUTION = {1: 85215, 2: 9235, 4: 964}
EXPECTED_FEATURETYPE_LABELS = {1: "River", 2: "Canal", 4: "Ocean or Great Lake"}
EXPECTED_LAYER_EXTENT = {"xmin": -19951755.6817, "ymin": -1622985.3665999994,
                         "xmax": 20021551.747199997, "ymax": 11575301.958199998}
EXPECTED_OBSERVED_BOUNDS = {"xmin": -179.22967073889612, "ymin": -14.4246875666806,
                            "xmax": 179.85665946299667, "ymax": 71.49960643639866}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _window_url(window_ids: list[int]) -> str:
    return _query({"objectIds": ",".join(map(str, window_ids)), "outFields": "*",
                   "returnGeometry": "true", "returnZ": "true", "outSR": "4326",
                   "orderByFields": "OBJECTID ASC", "f": "json"})


def _window_url_ids(url: str) -> list[int]:
    parsed = parse_qs(urlsplit(url).query)
    raw = parsed.get("objectIds")
    if not raw:
        raise ValueError("source_window_request_missing_ids")
    return [int(value) for value in raw[0].split(",")]


def _validate_layer_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != NATIVE_LAYER_SR[key]:
            raise ValueError(label + "_crs_drift")


def _validate_page_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")
    if not {"wkid", "latestWkid"} & set(value):
        raise ValueError(label + "_crs_missing")


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _failed_receipt_error_code(capture_root: Path, name: str) -> str | None:
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, ValueError):
        return None
    code = receipt.get("error_code")
    return code if isinstance(code, str) else None


def _capture_doc(client: httpx.Client, url: str, capture_root: Path, name: str,
                 budget: list[int]) -> tuple[bytes, dict[str, object]]:
    """Capture one producer document, retrying bounded times on transient failures."""
    for attempt in range(1, DOC_CAPTURE_ATTEMPTS + 1):
        _budget_check(budget)
        try:
            return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES,
                               before_retain=_budget_guard(budget))
        except ValueError as exc:
            if attempt >= DOC_CAPTURE_ATTEMPTS or not str(exc).startswith("incomplete_source_capture:"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(DOC_CAPTURE_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
    raise AssertionError("unreachable_retry_state")


def _budget_guard(budget: list[int]) -> Callable[[str, bytes], None]:
    def before_retain(kind: str, data: bytes) -> None:
        budget[0] += len(data)
        if budget[0] > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
    return before_retain


def _budget_check(budget: list[int]) -> None:
    if budget[0] > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded")


def _validate_attrs_types(attrs: Mapping[str, object], fields: list[dict[str, object]]) -> None:
    for field in fields:
        name_attr, kind = field["name"], field["type"]
        value = attrs.get(name_attr)
        if name_attr == "OBJECTID":
            continue
        if value is None:
            continue
        if kind == "esriFieldTypeString":
            if not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + name_attr)
            if name_attr == "id3dhp" and (not value or any(
                    character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    for character in value)):
                raise ValueError("source_id3dhp_value_drift")
        elif kind in ("esriFieldTypeOID", "esriFieldTypeInteger",
                      "esriFieldTypeSmallInteger", "esriFieldTypeDate"):
            if type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + name_attr)
        elif kind == "esriFieldTypeDouble":
            if type(value) not in (int, float) or not (value == value
                                                       and -1e308 < value < 1e308):
                raise ValueError("source_double_value_type_drift:" + name_attr)


def _capture_windows(client: httpx.Client, roster: list[int], capture_root: Path,
                     budget: list[int]) -> tuple[dict[str, tuple[bytes, dict[str, object]]],
                                                 dict[str, list[int]]]:
    """Capture every roster window, bisecting oversized or persistently failing windows.

    Each accepted window is one exact ``objectIds`` request over a contiguous
    roster slice. A window that answers with an Esri error document, exceeds the
    response byte cap or reports a truncated result set is bisected into two
    smaller windows over the same roster slice: those producer answers are
    deterministic for the requested window (observed: oversized windows persistently
    refuse with the same HTTP 200 error document), so retrying the same window
    only burns bounded backoff time — bisection is completeness-neutral and only
    changes page boundaries. Transport faults (connection/reset/truncation) are
    still retried a bounded number of times with backoff for transient producer
    faults, and failed attempts are preserved on disk as ``.attempt-NN`` evidence
    that never enters the capture roster. Bisection continues down to
    single-OBJECTID windows: a single id that still answers with the error
    document after the bounded retries is recorded as refused (its attempt
    evidence stays on disk and its id must match the pinned refused set) instead
    of aborting the whole capture, because the producer is known to persistently
    refuse serving the geometry of specific records (dated evidence on the
    capture manifest). The pinned before/after rosters plus per-page slice
    equality make bisection completeness-neutral: it can only change page
    boundaries, never population.
    """
    pages: dict[str, tuple[bytes, dict[str, object]]] = {}
    refused: dict[str, list[int]] = {}
    plan: deque[tuple[int, int]] = deque((start, PAGE_IDS) for start in range(0, len(roster), PAGE_IDS))
    while plan:
        start, size = plan.popleft()
        window_ids = roster[start:start + size]
        size = len(window_ids)
        name = f"page_{window_ids[0]:07d}"
        url = _window_url(window_ids)
        accepted = False
        last_error: Exception | None = None
        for attempt in range(1, PAGE_CAPTURE_ATTEMPTS + 1):
            _budget_check(budget)
            try:
                body, receipt = capture_get(client, url, capture_root, name,
                                            max_bytes=MAX_BODY_BYTES,
                                            before_retain=_budget_guard(budget))
            except ValueError as exc:
                if "source_capture_budget_exceeded" in str(exc):
                    raise
                last_error = exc
                error_code = _failed_receipt_error_code(capture_root, name)
                if error_code in ("wire_limit_exceeded", "decoded_limit_exceeded"):
                    break
                if attempt < PAGE_CAPTURE_ATTEMPTS:
                    _preserve_failed_attempt(capture_root, name, attempt)
                    time.sleep(PAGE_CAPTURE_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
                    continue
                break
            if _producer_error_doc(body):
                last_error = ValueError("source_window_error_document")
                _preserve_failed_attempt(capture_root, name, attempt)
                if size > 1:
                    break
                if attempt < PAGE_CAPTURE_ATTEMPTS:
                    time.sleep(PAGE_CAPTURE_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
                    continue
                break
            doc = json.loads(body)
            if doc.get("exceededTransferLimit"):
                last_error = ValueError("source_window_transfer_truncated")
                break
            pages[name] = (body, receipt)
            accepted = True
            break
        if accepted:
            continue
        if size > 1:
            mid = size // 2
            plan.appendleft((start + mid, size - mid))
            plan.appendleft((start, mid))
            continue
        refused[name] = window_ids
    return pages, refused


def _polygon_wkb(geometry: object) -> bytes | None:
    """WKB for one feature's Esri rings under the producer's verified ring semantics.

    Esri JSON (verified on the pinned capture): every ring is explicitly closed,
    clockwise rings are exteriors, counter-clockwise rings are holes, and every
    ordinate triple carries the layer's Z value (``returnZ=true`` response).
    Vertices are passed to shapely verbatim; exteriors with no claimable hole
    keep their ring order and holes join their smallest containing exterior.
    """
    if geometry is None:
        return None
    if not isinstance(geometry, dict) or "rings" not in geometry \
            or not isinstance(geometry["rings"], list) or not geometry["rings"]:
        raise ValueError("source_polygon_geometry_invalid")
    if set(geometry) - {"hasZ", "rings"}:
        raise ValueError("unexpected_source_geometry_members")
    if geometry.get("hasZ") is not True:
        raise ValueError("source_geometry_z_flag_missing")
    exteriors: list[list] = []
    holes: list[list] = []
    for ring in geometry["rings"]:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError("source_polygon_ring_invalid")
        area = 0.0
        for index, vertex in enumerate(ring):
            if not isinstance(vertex, list) or len(vertex) != 3:
                raise ValueError("source_polygon_ordinate_contract_drift")
            x, y, z = vertex
            if not (type(x) in (int, float) and type(y) in (int, float) and type(z) in (int, float)):
                raise ValueError("source_polygon_ordinate_not_numeric")
            if not (all(value == value and -1e308 < value < 1e308 for value in (x, y, z))):
                raise ValueError("source_polygon_ordinate_not_finite")
            if x == "NaN" or y == "NaN" or z == "NaN":
                raise ValueError("source_polygon_ordinate_string_nan")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise ValueError("source_coordinate_range_drift")
            nxt = ring[(index + 1) % len(ring)]
            area += x * nxt[1] - nxt[0] * y
        area /= 2
        if ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if area == 0:
            raise ValueError("source_polygon_ring_degenerate")
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise ValueError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    # Prepared shell geometries: hole containment is queried against every
    # exterior, and the producer's island/coastal features carry thousands of
    # holes over six-figure-vertex exteriors where unprepared contains() per
    # hole is pathologically slow. Preparation builds an index only and the
    # containment results are identical.
    for shell, _area, _index in shells:
        shapely.prepare(shell)
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        probe = Polygon(hole).representative_point()
        candidates = [(abs(area), index) for shell, area, index in shells if shell.contains(probe)]
        if not candidates:
            raise ValueError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for _, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        nxt = ring[(index + 1) % len(ring)]
        area += vertex[0] * nxt[1] - nxt[0] * vertex[1]
    return area / 2


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGlobalID": pa.string()}.get(str(field["type"]), pa.string())


GEOMETRY_COLUMN = "geometry"


def _parquet_bytes(fields: list[dict[str, object]], attributes: list[dict[str, object]],
                   wkbs: list[bytes | None]) -> bytes:
    schema_fields = [pa.field(field["name"], _arrow_type(field), nullable=True) for field in fields]
    schema_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
    arrays = [pa.array([row[field["name"]] for row in attributes], type=schema_fields[index].type)
              for index, field in enumerate(fields)]
    arrays.append(pa.array(wkbs, type=pa.binary()))
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
           "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                         "geometry_types": ["Polygon Z", "MultiPolygon Z"],
                                         "crs": crs, "edges": "planar"}}}
    table = pa.Table.from_arrays(arrays, names=[field.name for field in schema_fields])
    table = table.replace_schema_metadata({b"geo": _canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    """Validate the complete capture against the pinned producer population contract."""
    doc_names = set(captures)
    refused_attr_names = {name for name in doc_names if name.startswith("refused_attrs_")}
    required = {"service_doc", "alias_service", "service_before", "service_after",
                "layer_before", "layer_after", "count_before", "count_after",
                "ids_seed", "ids_before", "ids_after",
                *{f"refused_attrs_{oid:07d}" for oid in EXPECTED_REFUSED_IDS}}
    page_names = [name for name in doc_names if name.startswith("page_")]
    if not required <= doc_names or not page_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if refused_attr_names != {f"refused_attrs_{oid:07d}" for oid in EXPECTED_REFUSED_IDS}:
        raise ValueError("source_refused_ids_drift")
    unexpected = doc_names - required - set(page_names) - refused_attr_names
    if unexpected:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()
            if name != "service_doc"}
    if SERVICE_HOST.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("usgs_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or not isinstance(layer, dict):
        raise ValueError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise ValueError("usgs_3dhp_service_identity_drift")
    service_layers = service.get("layers")
    roster_value = [(entry.get("id"), entry.get("name")) for entry in service_layers
                    if isinstance(entry, dict)] if isinstance(service_layers, list) else None
    if roster_value != EXPECTED_SERVICE_LAYERS:
        raise ValueError("usgs_3dhp_service_layer_roster_drift")
    alias = docs["alias_service"]
    if (not isinstance(alias, dict) or alias.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT
            or [(entry.get("id"), entry.get("name")) for entry in alias.get("layers", [])
                if isinstance(entry, dict)] != EXPECTED_SERVICE_LAYERS):
        raise ValueError("usgs_3dhp_alias_service_identity_drift")
    for name in ("service_before", "service_after", "alias_service"):
        description = str(docs[name].get("serviceDescription", ""))
        if "3DHP_all" not in description or "3D Hydrography Program" not in description:
            raise ValueError("usgs_3dhp_service_description_drift")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_3dhp_waterbody_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise ValueError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2500:
        raise ValueError("source_record_limit_drift")
    _validate_layer_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_layer_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(entry, dict) for entry in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(entry) for entry in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    field_names = [entry["name"] for entry in fields_value]
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer:
            raise ValueError("source_layer_metadata_changed_during_capture")
    counts = [docs[name].get("count") for name in ("count_before", "count_after")]
    if any(value != EXPECTED_COUNT for value in counts):
        raise ValueError("source_count_drift")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    for roster in rosters:
        if (not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
                or any(type(value) is not int for value in roster)
                or len(set(roster)) != EXPECTED_COUNT):
            raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")

    page_entries = []
    for name in page_names:
        start = int(name.removeprefix("page_"))
        if not 0 <= start <= 9_999_999:
            raise ValueError("source_page_name_invalid:" + name)
        page_entries.append((start, name))
    page_entries.sort()
    attributes: list[dict[str, object]] = []
    wkbs: list[bytes | None] = []
    geometry_records: list[list] = []
    null_geometry = 0
    observed_bounds: list[float] = []
    distribution: dict[int, int] = {}
    id3dhp_seen: set[str] = set()
    ring_count = 0
    vertex_count = 0
    captured_ids: list[int] = []
    for _start, name in page_entries:
        body, receipt = captures[name]
        page = docs[name]
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error:" + name)
        _validate_page_spatial_reference(page.get("spatialReference"), f"source_page_{name}")
        if page.get("exceededTransferLimit"):
            raise ValueError("source_page_transfer_truncated:" + name)
        if page.get("hasZ") is not True:
            raise ValueError("source_page_z_flag_missing:" + name)
        requested = _window_url_ids(receipt["url"])
        rows = page.get("features")
        if not isinstance(rows, list) or not rows:
            raise ValueError("source_page_missing_features:" + name)
        page_ids = []
        page_geometry_records = []
        for row in rows:
            attrs = row.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != field_names:
                raise ValueError("source_record_attribute_schema_drift:" + name)
            oid = attrs["OBJECTID"]
            if type(oid) is not int or oid <= 0:
                raise ValueError("source_native_identifier_missing_or_type_drift:" + name)
            featuretype = attrs["featuretype"]
            if featuretype not in EXPECTED_FEATURETYPE_LABELS:
                raise ValueError("source_featuretype_outside_area_population:" + name)
            if attrs["featuretypelabel"] != EXPECTED_FEATURETYPE_LABELS[featuretype]:
                raise ValueError("source_featuretype_label_drift:" + name)
            _validate_attrs_types(attrs, fields)
            geometry = row.get("geometry")
            if geometry is None:
                null_geometry += 1
                page_geometry_records.append([oid, None])
                wkbs.append(None)
                captured_ids.append(oid)
                attributes.append(attrs)
                id3dhp_seen.add(attrs["id3dhp"])
                continue
            wkb = _polygon_wkb(geometry)
            rings = geometry["rings"]
            ring_count += len(rings)
            vertex_count += sum(len(ring) for ring in rings)
            page_geometry_records.append([oid, rings])
            for ring in rings:
                for vertex in ring:
                    observed_bounds.extend((vertex[0], vertex[1]))
            wkbs.append(wkb)
            captured_ids.append(oid)
            attributes.append(attrs)
            distribution[attrs["featuretype"]] = distribution.get(attrs["featuretype"], 0) + 1
            id3dhp_seen.add(attrs["id3dhp"])
        page_ids = [row["attributes"]["OBJECTID"] for row in rows]
        if page_ids != sorted(page_ids):
            raise ValueError("source_page_ids_not_ascending:" + name)
        if sorted(page_ids) != sorted(requested):
            raise ValueError("source_page_id_slice_incomplete_or_reordered:" + name)
        geometry_records.extend(page_geometry_records)
    # Producer-refused records: their attributes were captured through the
    # working attributes-only channel and are appended in ascending OBJECTID
    # order with explicitly gated null candidate geometry (the producer's own
    # geometry response is the persistent refusal evidence pinned in
    # EXPECTED_REFUSED_IDS); nothing is substituted or fabricated for them.
    refused_oids = sorted(EXPECTED_REFUSED_IDS)
    for oid in refused_oids:
        attrs_name = f"refused_attrs_{oid:07d}"
        refused_rows = docs[attrs_name].get("features")
        if (not isinstance(refused_rows, list) or len(refused_rows) != 1
                or not isinstance(refused_rows[0], dict)
                or not isinstance(refused_rows[0].get("attributes"), dict)
                or refused_rows[0].get("geometry") is not None):
            raise ValueError("source_refused_attrs_record_drift:" + attrs_name)
        attrs = refused_rows[0]["attributes"]
        if list(attrs) != field_names or attrs.get("OBJECTID") != oid:
            raise ValueError("source_refused_attrs_record_drift:" + attrs_name)
        _validate_attrs_types(attrs, fields)
        wkbs.append(None)
        captured_ids.append(oid)
        attributes.append(attrs)
        null_geometry += 1
        id3dhp_seen.add(attrs["id3dhp"])
    if refused_oids:
        geometry_records.extend([oid, None] for oid in refused_oids)
    if sorted(captured_ids) != ids:
        raise ValueError("source_feature_population_incomplete")
    if distribution != EXPECTED_FEATURETYPE_DISTRIBUTION:
        raise ValueError("source_featuretype_distribution_drift")
    if null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    bounds = None
    if observed_bounds:
        bounds = {"xmin": min(observed_bounds[0::2]), "ymin": min(observed_bounds[1::2]),
                  "xmax": max(observed_bounds[0::2]), "ymax": max(observed_bounds[1::2])}
    if bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    record_sha = _sha(_canonical(attributes))
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    # Boundary-independent geometry digest: one canonical stream over the whole
    # record sequence (served pages in ascending roster order, refused records
    # appended in ascending OBJECTID order). Hashing per-page chunks would make
    # the pin depend on the capture's bisection page boundaries, which are
    # completeness-neutral and may legitimately differ between captures.
    geometry_hash = hashlib.sha256()
    geometry_hash.update(b"[")
    for index, record in enumerate(geometry_records):
        if index:
            geometry_hash.update(b",")
        geometry_hash.update(_canonical(record))
    geometry_hash.update(b"]")
    geometry_sha = geometry_hash.hexdigest()
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    audit = {"source_feature_count": len(attributes), "source_id_sha256": _sha(_canonical(captured_ids)),
             "source_record_sha256": record_sha,
             "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha,
             "source_featuretype_distribution": distribution,
             "source_crs_epsg": SOURCE_CRS_WKID, "source_layer_native_sr": NATIVE_LAYER_SR,
             "geometry_type": "Polygon Z / MultiPolygon Z", "null_geometry_count": null_geometry,
             "observed_bounds": bounds, "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(set(captured_ids)),
             "id3dhp_distinct_count": len(id3dhp_seen),
             "id3dhp_duplicate_count": len(attributes) - len(id3dhp_seen),
             "source_ring_count": ring_count, "source_vertex_count": vertex_count,
             "source_refused_ids": refused_oids,
             "source_refused_count": len(refused_oids),
             "producer_refusal_note":
                 "Records listed in source_refused_ids are part of the pinned population "
                 "(they appear in the id roster and count) but the producer's own REST "
                 "publication persistently refuses to serve their geometry (HTTP 200 Esri "
                 "error document code 500 'Error performing query operation' on both "
                 "3dhp.nationalmap.gov and the same-org alias hydro.nationalmap.gov/3DHP_all, "
                 "single-id and windowed, retried with backoff, attempt evidence retained in "
                 "the capture). Their attributes are served normally through the "
                 "attributes-only channel and are staged verbatim with explicitly gated null "
                 "candidate geometry; publication stays blocked pending the maintainer "
                 "completeness/coverage decision for these records.",
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The 3DHP_all service publishes no per-snapshot release date; the service "
                 "copyrightText states a service refresh (September 4, 2026), the per-feature "
                 "featuredate is the 3DHP core database load date, and neither is a dataset "
                 "release date. Quarterly service updates and annual staged products are "
                 "documented by USGS.",
             "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT}
    return fields, {"attributes": attributes, "wkbs": wkbs, "ids": captured_ids,
                    "null_geometry_count": null_geometry, "audit": audit}


def _capture_refused_attrs(client: httpx.Client, capture_root: Path, budget: list[int],
                           captures: dict[str, tuple[bytes, dict[str, object]]],
                           refused: Mapping[str, list[int]]) -> None:
    """Capture producer-refused records' attributes through the attributes-only channel.

    The producer refuses to serve some records' geometry (persistent Esri 500
    error document on both of its own hosts, dated on the capture manifest) but
    serves the same records' attributes with ``returnGeometry=false``. Each
    refused id gets one attributes-only capture named ``refused_attrs_<OID>``
    validated to exactly that record; the refusal itself is pinned in
    ``EXPECTED_REFUSED_IDS`` and re-verified by every later capture.
    """
    for name in sorted(refused):
        for oid in refused[name]:
            attrs_name = f"refused_attrs_{oid:07d}"
            body, receipt = _capture_doc(
                client,
                _query({"objectIds": str(oid), "outFields": "*", "returnGeometry": "false",
                        "f": "json"}),
                capture_root, attrs_name, budget)
            doc = json.loads(body)
            rows = doc.get("features") if isinstance(doc, dict) else None
            if (not isinstance(rows, list) or len(rows) != 1
                    or not isinstance(rows[0], dict) or not isinstance(rows[0].get("attributes"), dict)
                    or rows[0].get("geometry") is not None
                    or rows[0]["attributes"].get("OBJECTID") != oid):
                raise ValueError("source_refused_attrs_record_drift:" + attrs_name)
            captures[attrs_name] = (body, receipt)


def capture_nhd_area_large_scale(storage: StagingStorageResource, *, version: str,
                                 capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved 3DHP non-Lake waterbody candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/data/workspace/scratch/nhd-area-large-scale-2/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    budget = [0]
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(300, connect=30),
                          headers={"User-Agent": USER_AGENT}) as client:
            doc_urls = [
                ("service_doc", SERVICE_DOC_URL),
                ("alias_service", ALIAS_SERVICE_URL + "?f=pjson"),
                ("service_before", SERVICE_URL + "?f=pjson"),
                ("layer_before", LAYER_URL + "?f=pjson"),
                ("count_before", _query({"where": POPULATION_WHERE, "returnCountOnly": "true", "f": "json"})),
                ("ids_before", _query({"where": POPULATION_WHERE, "returnIdsOnly": "true", "f": "json"})),
            ]
            for name, url in doc_urls:
                captures[name] = _capture_doc(client, url, capture_root, name, budget)
            seed_body, seed_receipt = _capture_doc(
                client, _query({"where": POPULATION_WHERE, "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", budget)
            captures["ids_seed"] = (seed_body, seed_receipt)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            pages, refused = _capture_windows(client, sorted(seed), capture_root, budget)
            captures.update(pages)
            _capture_refused_attrs(client, capture_root, budget, captures, refused)
            for name, url in doc_urls[2:]:
                after_name = name.replace("_before", "_after")
                captures[after_name] = _capture_doc(client, url, capture_root, after_name, budget)
        fields, extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        retained = dict(captures)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(),
                                  json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          type(exc).__name__)
        raise
    audit = extracted["audit"]
    identity = processing_identity(Path(__file__).parents[3])
    parquet_body = _parquet_bytes(fields, extracted["attributes"], extracted["wkbs"])
    del extracted
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Area - Large Scale (USGS 3DHP Waterbody, non-Lake areal hydrography)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 60 "
                       "'Waterbody', rows with featuretype<>3 (River, Canal, Ocean or Great "
                       "Lake), captured completely from the producer's own REST publication as "
                       "the maintained successor of the retired NHD high-resolution NHDArea "
                       "layer (HIFLD inventory nhd/area-large-scale-2). Complete source "
                       "attributes and Z-aware EPSG:4326 polygon geometry are represented in "
                       "the candidate, except the pinned set of producer-refused records "
                       "(source_refused_ids in the audit): their attributes are captured "
                       "verbatim through the producer's working attributes-only channel and "
                       "their candidate geometry is explicitly gated to null because the "
                       "producer itself refuses to serve it. The 3DHP schema replaces the "
                       "legacy NHDArea schema; succession, freshness, rights, public "
                       "schema/ID contract, coverage and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no access or "
                   "use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": SERVICE_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The 3DHP_all service publishes no per-snapshot "
                                                   "release date; its copyrightText records a "
                                                   "service refresh (September 4, 2026) and USGS "
                                                   "documents quarterly service updates and "
                                                   "annual staged products.",
                    "license": "Public domain (U.S. Government work) per the producer service; "
                               "rights acceptance pending.",
                    "attribution": "USGS 3D Hydrography Program (3DHP_all), The National Map; "
                                   "formal attribution terms require maintainer review."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": POPULATION_WHERE + " over the pinned "
                                                          "complete OBJECTID roster (legacy "
                                                          "NHDArea water-extent successor scope; "
                                                          "Lake rows belong to the waterbody "
                                                          "datasets)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types, including the service-generated "
                                                             "Shape__Length and Shape__Area doubles",
                                            "source_ids": "Preserve producer OBJECTID and id3dhp "
                                                          "values unchanged (id3dhp is the producer's "
                                                          "base-36 identifier, documented as not "
                                                          "persistent); no substitute IDs, casts or "
                                                          "joins",
                                            "geometry": "Source EPSG:4326 polygon rings with Z "
                                                        "retained as WKB without projection under "
                                                        "the producer's verified Esri ring semantics "
                                                        "(closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes); no vertex "
                                                        "dropped, added or reordered; null source "
                                                        "geometry retained as null; pinned "
                                                        "producer-refused records keep their "
                                                        "captured attributes with explicitly "
                                                        "gated null candidate geometry",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/area-large-scale-2 ('Area - Large Scale', "
                   "agency US Geological Survey, last HIFLD refresh January 2024) carried the "
                   "legacy NHD high-resolution NHDArea layer and its description already said "
                   "'Please use 3DHP for updated data'. USGS retired the NHD as a maintained "
                   "product on 2023-10-01 (usgs.gov National Hydrography Dataset page) and the "
                   "DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, consolidated row '3D "
                   "Hydrography Program (3DHP) Layers', Open ID 650c07e542fa43b6a327c7f683681712) "
                   "directs all NHD-family layers to the 3dhp.nationalmap.gov usgs_3dhp_all "
                   "FeatureServer captured here; the same-org hydro.nationalmap.gov 3DHP_all "
                   "service is captured as the producer's alias publication. Per USGS's own "
                   "NHD->3DHP crosswalk, legacy NHDArea water-extent content (StreamRiver 460, "
                   "CanalDitch 336, SeaOcean 445) maps to 3DHP Waterbody featuretypes River, "
                   "Canal and Ocean or Great Lake, while legacy Lake/Pond/Reservoir content "
                   "(NHDWaterbody) maps to featuretype Lake and is excluded here as belonging "
                   "to the waterbody datasets. Legacy NHDArea landmark subtypes (bridge, "
                   "dam/weir, lock chamber, rapids, wash, levee and others) have no polygon "
                   "counterpart in the 3DHP crosswalk and are not reconstructible from this "
                   "maintained successor; legacy attributes (Permanent_Identifier, ReachCode, "
                   "FType/FCode, Resolution) have no 3DHP equivalents (3DHP id3dhp is "
                   "documented as not persistent). That succession coverage and contract change "
                   "is recorded here and remains an unaccepted maintainer decision. The "
                   "DataLumos project 239953 archive copy (archive modification 2025-07-03) is "
                   "identification and comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and Z-aware "
                      "geometry retained in the producer page CRS; all succession, freshness, "
                      "contract, rights and release approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile "
                                                        "field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized "
                                                  "geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    full_audit = {**audit, "source_fields": fields, "official_layer_name": LAYER_NAME,
                  "layer_id": LAYER_ID, "service_url": SERVICE_URL,
                  "alias_service_url": ALIAS_SERVICE_URL, "service_doc_url": SERVICE_DOC_URL,
                  "crosswalk_doi": "10.3886/E241367V1",
                  "crosswalk_layer_row": "3D Hydrography Program (3DHP) Layers",
                  "crosswalk_open_id": "650c07e542fa43b6a327c7f683681712",
                  "release_date_unknown": True, "freshness_accepted": False,
                  "succession_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "coverage_accepted": False,
                  "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(full_audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "succession_accepted": False,
         "freshness_accepted": False, "schema_contract_accepted": False,
         "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT,
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_id_sha256": audit["source_id_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                "release_accepted": False}
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}
