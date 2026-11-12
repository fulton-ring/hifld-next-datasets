"""Lossless, approval-gated capture of the FEMA NFHL "Political Jurisdictions" polygon layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 22 "Political
Jurisdictions" as the producer's own publication of this HIFLD inventory
dataset (``nfhl/political-jurisdictions-1``, DataLumos 239983 historical
archive copy used for identification only). The layer is continuously updated
and publishes no per-snapshot release date; source freshness, rights, and
public-schema compatibility are not accepted by this draft.

Producer polygon semantics follow the Esri JSON convention verified on the
2026-11-12 complete capture: every source ring is explicitly closed and
clockwise rings are exteriors while counter-clockwise rings are holes, so
candidate WKB geometry is built by that rule (one exterior per feature in the
observed population) without dropping, adding, or reordering any source vertex.
The producer publishes 2,518 rings across 1,647 features that GEOS/OGC
validity rules call self-intersecting (Esri topology tolerates self-touching
rings); candidate WKB retains them verbatim, the counts are pinned, and the
technical geometry quality acceptance stays unapproved for the maintainer
decision. Null source geometries (the producer's 292 rows with no SHAPE,
observed with the same capture) are retained as null candidate geometry, never
coerced to a location.

Pages are bounded OBJECTID windows aligned to the sorted source id roster
instead of deep ``resultOffset`` offsets: the producer's MapServer
intermittently answers wide polygon page requests — both resultOffset pages
and full-width windows — with complete HTTP 200 Esri error documents or
connection resets (observed 2026-11-12 while probing this layer; e.g. an
``OBJECTID`` window for the same roster slice answered correctly at half
width while the full window failed repeatedly). One-roster-slice windows of
1000 features are the verified stable granularity, and when even a slice's
full window keeps failing, that slice is captured as bounded dyadic
sub-windows (down to 16 features each) whose responses are kept verbatim;
validation still requires every tile to return its exact roster sub-slice,
so the merged page equals the pinned id slice and record digests. Identical
requests still occasionally fail, so every request also retries a bounded
number of times for that transient mode. A retry re-issues the exact same
request, and completeness is still enforced by the pinned before/after id
rosters, counts, exact page id slices, and full-record digests, so retries
can never mask drift.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
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

DATASET_SLUG = "nfhl-political-jurisdictions"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 22
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Political Jurisdictions"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
PAGE_SIZE = 1000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 1_500_000_000
# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or transport failures (observed 2026-11-12
# while probing this layer; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact page id slices, and
# full-record digests, so retries can never mask drift.
DOC_CAPTURE_ATTEMPTS = 6
DOC_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Page windows are retried a bounded number of times before a roster slice is
# captured as dyadic sub-tiles; each sub-tile gets its own bounded retries.
PAGE_FULL_ATTEMPTS = 3
TILE_ATTEMPTS = 4
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
MIN_TILE_IDS = 16
# Pins below are the observed 2026-11-12 complete-layer population contract.
EXPECTED_COUNT = 88375
EXPECTED_ID_SHA256 = "dd540eb1dc06734b3c490ce6f26abec51a2a956a4b3e56a521d1c6c21bfd10a1"
EXPECTED_RECORD_SHA256 = "8c1957da5f9a1a6c0569257d37178f7b2274eac5968d1e9e85f56b9d50e8e27a"
EXPECTED_GEOMETRY_SHA256 = "8d69d05c5456564a0c0e056cb206492b8083f6b191988487331469ff7fd75b60"
EXPECTED_SCHEMA_SHA256 = "01b1e90652867538ffceef925822b01f4902dc76757a6bb7288a6610c4c4a955"
EXPECTED_NULL_GEOMETRY_COUNT = 292
EXPECTED_NAN_GEOMETRY_COUNT = 0
# GEOS/OGC self-intersecting source rings are retained verbatim in candidate
# WKB (no vertex dropped, added, or reordered); these pins are the observed
# 2026-11-12 complete-layer population contract, and the technical geometry
# quality acceptance stays unapproved for the maintainer decision.
EXPECTED_SELF_INTERSECTING_RING_COUNT = 2518
EXPECTED_SELF_INTERSECTING_RING_FEATURE_COUNT = 1647
EXPECTED_ZERO_AREA_RING_COUNT = 5
EXPECTED_OBSERVED_BOUNDS = {"xmin": -170.84698891836925, "ymin": -14.373782300647008,
                            "xmax": 145.83077660155834, "ymax": 66.61058213599716}
EXPECTED_LAYER_EXTENT = {"xmin": -170.84698891836925, "ymin": -14.373782300647008,
                         "xmax": 145.83077660155834, "ymax": 66.61058213599716}
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "NFHL Availability", "Feature Layer"), (3, "FIRM Panels", "Feature Layer"),
    (1, "LOMRs", "Feature Layer"), (34, "LOMAs", "Feature Layer"),
    (22, "Political Jurisdictions", "Feature Layer"), (17, "Profile Baselines", "Feature Layer"),
    (20, "Water Lines", "Feature Layer"), (14, "Cross-Sections", "Feature Layer"),
    (16, "Base Flood Elevations", "Feature Layer"), (23, "Levees", "Feature Layer"),
    (29, "Seclusion Boundaries", "Feature Layer"), (15, "Coastal Transects", "Feature Layer"),
    (18, "Transect Baselines", "Feature Layer"), (24, "General Structures", "Feature Layer"),
    (7, "River Mile Markers", "Feature Layer"), (32, "Water Areas", "Feature Layer"),
    (5, "PLSS", "Feature Layer"), (19, "Limit of Moderate Wave Action", "Feature Layer"),
    (27, "Flood Hazard Boundaries", "Feature Layer"), (28, "Flood Hazard Zones", "Feature Layer"),
    (25, "Primary Frontal Dunes", "Feature Layer"), (4, "Base Index", "Feature Layer"),
    (6, "Topographic Low Confidence Areas", "Feature Layer"), (8, "Datum Conversion Points", "Feature Layer"),
    (9, "Coastal Gages", "Feature Layer"), (10, "Gages", "Feature Layer"),
    (11, "Nodes", "Feature Layer"), (12, "High Water Marks", "Feature Layer"),
    (13, "Station Start Points", "Feature Layer"), (26, "Hydrologic Reaches", "Feature Layer"),
    (30, "Alluvial Fans", "Feature Layer"), (31, "Subbasins", "Feature Layer"),
]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_slice_length(page: int) -> int:
    return min(PAGE_SIZE, EXPECTED_COUNT - page * PAGE_SIZE)


def _page_where(slice_ids: list[int]) -> str:
    """One bounded OBJECTID window exactly covering one consecutive roster slice.

    The window endpoints are the first and last OBJECTID of the slice, so the
    returned population can only equal the slice: every producer OBJECTID
    between them is already in the sorted roster inside the same slice.
    """
    low, high = slice_ids[0], slice_ids[-1]
    if low == high:
        return f"OBJECTID={low}"
    return f"OBJECTID>={low} AND OBJECTID<={high}"


def _page_url(slice_ids: list[int]) -> str:
    return _query({"where": _page_where(slice_ids), "outFields": "*", "returnGeometry": "true",
                   "orderByFields": "OBJECTID ASC", "resultRecordCount": len(slice_ids), "f": "json"})


_BRACKET_URLS = (
    ("service_doc", SERVICE_DOC_URL),
    ("service_before", SERVICE_URL + "?f=pjson"),
    ("layer_before", LAYER_URL + "?f=pjson"),
    ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
    ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
)
_AFTER_BRACKET_URLS = (
    ("layer_after", LAYER_URL + "?f=pjson"),
    ("service_after", SERVICE_URL + "?f=pjson"),
    ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
    ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
)


def _capture_page(client: httpx.Client, capture_root: Path, page: int,
                  slice_ids: list[int]) -> list[tuple[str, bytes, dict[str, object]]]:
    """Capture one roster slice, then dyadic sub-tiles when the window keeps failing.

    The full slice window is captured as ``page_XXX``. If it persistently
    answers with the producer's transient error documents or transport
    failures, the slice is split into bounded sub-windows named
    ``page_XXX_oOOOOO_cCCCC`` (offset and length inside the roster slice);
    each sub-window is captured verbatim and validated against its exact
    roster sub-slice. A sub-tile no smaller than ``MIN_TILE_IDS`` features
    that still keeps failing fails the capture.
    """
    for attempt in range(1, PAGE_FULL_ATTEMPTS + 1):
        try:
            body, receipt = _capture_with_retry(
                client, _page_url(slice_ids), capture_root, f"page_{page:03d}",
                max_bytes=MAX_CAPTURE_BYTES, attempts=1, base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS)
            if not _producer_error_doc(body):
                return [(f"page_{page:03d}", body, receipt)]
            _preserve_failed_attempt(capture_root, f"page_{page:03d}", attempt)
        except ValueError as exc:
            _preserve_failed_attempt(capture_root, f"page_{page:03d}", attempt)
            if attempt >= PAGE_FULL_ATTEMPTS:
                # The producer's transient transport failures and complete-200
                # error documents both fall through to bounded sub-tiling of
                # the same roster slice; only non-transient failures surface.
                if not str(exc).startswith("incomplete_source_capture:"):
                    raise
                break
        time.sleep(PAGE_CAPTURE_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
    captured: list[tuple[str, bytes, dict[str, object]]] = []
    segments = [(0, len(slice_ids))]
    while segments:
        offset, count = segments.pop(0)
        name = f"page_{page:03d}_o{offset:05d}_c{count:04d}"
        sub = slice_ids[offset:offset + count]
        tile = None
        last_error = None
        for attempt in range(1, TILE_ATTEMPTS + 1):
            try:
                body, receipt = _capture_with_retry(
                    client, _page_url(sub), capture_root, name,
                    max_bytes=MAX_CAPTURE_BYTES, attempts=1, base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS)
                if not _producer_error_doc(body):
                    tile = (name, body, receipt)
                    break
                last_error = "producer_error_document"
            except ValueError as exc:
                _preserve_failed_attempt(capture_root, name, attempt)
                if not str(exc).startswith("incomplete_source_capture:"):
                    raise
                last_error = str(exc)
            time.sleep(PAGE_CAPTURE_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
        if tile is None:
            if count <= MIN_TILE_IDS:
                raise ValueError(f"source_page_window_persistent_error:{name}:{last_error}")
            half = count // 2
            segments.insert(0, (offset + half, count - half))
            segments.insert(0, (offset, half))
            continue
        captured.append(tile)
    return captured


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


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


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster. The
    final attempt's body is always returned as captured, so a persistent
    producer failure surfaces to the caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
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


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items() if name != "service_doc"}
    page_count = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
    non_page_names = {"service_doc", "service_before", "layer_before", "count_before", "ids_before", "ids_seed",
                      "layer_after", "service_after", "count_after", "ids_after"}
    page_groups: dict[int, list[str]] = {}
    for name in set(captures) - non_page_names:
        base = name.split("_o")[0]
        if (not base.startswith("page_") or not base[5:].isdigit() or len(base) != 8
                or (len(name) != 8 and "_o" not in name)):
            raise ValueError("source_capture_roster_incomplete_or_unexpected")
        page = int(base[5:])
        if page >= page_count or (name != base and not name.startswith(f"page_{page:03d}_o")):
            raise ValueError("source_capture_roster_incomplete_or_unexpected")
        page_groups.setdefault(page, []).append(name)
    if set(page_groups) != set(range(page_count)):
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if SERVICE_URL.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("fema_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("fema_nfhl_service_layer_roster_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer"
               for x in service_layers):
        raise ValueError("exact_nfhl_political_jurisdictions_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_nfhl_political_jurisdictions_layer_metadata_drift")
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer:
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")

    features = []
    for page in range(page_count):
        names = page_groups[page]
        responses = []
        if f"page_{page:03d}" in docs:
            if len(names) != 1:
                raise ValueError("source_capture_roster_incomplete_or_unexpected")
            responses = [(0, _page_slice_length(page), docs[f"page_{page:03d}"])]
        else:
            tiled = []
            for name in names:
                marker = f"page_{page:03d}_o"
                if not name.startswith(marker) or "_c" not in name:
                    raise ValueError("source_capture_roster_incomplete_or_unexpected")
                offset_text, count_text = name[len(marker):].split("_c")
                if len(offset_text) != 5 or len(count_text) != 4 or not (offset_text + count_text).isdigit():
                    raise ValueError("source_capture_roster_incomplete_or_unexpected")
                tiled.append((int(offset_text), int(count_text), docs[name]))
            tiled.sort(key=lambda item: item[0])
            expected_start = 0
            for offset, count, _doc in tiled:
                if offset != expected_start:
                    raise ValueError("source_page_tile_gap_or_overlap")
                expected_start += count
            if expected_start != _page_slice_length(page):
                raise ValueError("source_page_tile_coverage_incomplete")
            responses = tiled
        merged_rows = []
        for offset, _count, page_doc in responses:
            if not isinstance(page_doc, dict) or "error" in page_doc:
                raise ValueError("source_page_error")
            _validate_spatial_reference(page_doc.get("spatialReference"), f"source_page_{page:03d}")
            # Window queries bound every response to its roster slice, and the
            # producer's window responses carry no exceededTransferLimit flag
            # (verified across the pinned capture); any appearance fails the
            # capture closed rather than risking silent truncation.
            if page_doc.get("exceededTransferLimit") is True:
                raise ValueError("source_page_unexpected_exceeded_transfer_limit")
            rows = page_doc.get("features")
            if not isinstance(rows, list):
                raise ValueError("source_page_missing_features")
            expected_tile = ids[page * PAGE_SIZE + offset:page * PAGE_SIZE + offset + _count]
            actual_tile = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
            if actual_tile != expected_tile or len(rows) != len(expected_tile):
                raise ValueError("source_page_id_slice_incomplete_or_reordered")
            merged_rows.extend(rows)
        features.extend(merged_rows)
    attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
    null_geometry = 0
    nan_geometry = 0
    self_intersecting_ring_feature_count = 0
    self_intersecting_ring_count = 0
    zero_area_ring_feature_count = 0
    zero_area_ring_count = 0
    geometry_records = []
    observed_bounds: list[float] = []
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        if geom is None:
            null_geometry += 1
            geometry_records.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) \
                or not geom["rings"]:
            raise ValueError("source_polygon_geometry_invalid")
        nan_rings = False
        validated_rings = []
        feature_bounds: list[float] = []
        for ring in geom["rings"]:
            if not isinstance(ring, list) or not ring:
                raise ValueError("source_polygon_geometry_invalid")
            for vertex in ring:
                if not isinstance(vertex, list) or len(vertex) != 2:
                    raise ValueError("source_polygon_geometry_invalid")
                x, y = vertex
                if type(x) in (int, float) and type(y) in (int, float):
                    if not (-180 <= x <= 180 and -90 <= y <= 90):
                        raise ValueError("source_coordinate_range_drift")
                    feature_bounds.extend((x, y))
                elif x == "NaN" and y == "NaN":
                    # String-"NaN" ordinates (seen on FEMA's sibling NFHL layers)
                    # are retained verbatim in the record/raw evidence and gated
                    # as unusable geometry, never silently coerced to a location.
                    nan_rings = True
                else:
                    raise ValueError("source_polygon_geometry_invalid")
            validated_rings.append(ring)
        geometry_records.append([attrs["OBJECTID"], validated_rings])
        if nan_rings:
            nan_geometry += 1
        else:
            # GEOS/OGC validity is counted, never enforced as a rewrite: the
            # producer publishes self-intersecting rings (pinned counts below)
            # and candidate WKB keeps every source vertex verbatim.
            invalid_rings = 0
            zero_area_rings = 0
            for ring in validated_rings:
                polygon = shapely.Polygon(ring)
                if not shapely.is_valid(polygon):
                    invalid_rings += 1
                if _signed_area(ring) == 0:
                    zero_area_rings += 1
            if invalid_rings:
                self_intersecting_ring_feature_count += 1
                self_intersecting_ring_count += invalid_rings
            if zero_area_rings:
                zero_area_ring_feature_count += 1
                zero_area_ring_count += zero_area_rings
            if feature_bounds:
                observed_bounds.extend(feature_bounds)
    if self_intersecting_ring_count != EXPECTED_SELF_INTERSECTING_RING_COUNT \
            or self_intersecting_ring_feature_count != EXPECTED_SELF_INTERSECTING_RING_FEATURE_COUNT \
            or zero_area_ring_count != EXPECTED_ZERO_AREA_RING_COUNT:
        raise ValueError("source_ring_validity_count_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(features))
    geometry_records.sort(key=lambda row: row[0])
    geometry_sha = _sha(_canonical(geometry_records))
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    bounds = None
    if observed_bounds:
        bounds = {"xmin": min(observed_bounds[0::2]), "ymin": min(observed_bounds[1::2]),
                  "xmax": max(observed_bounds[0::2]), "ymax": max(observed_bounds[1::2])}
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polygon", "null_geometry_count": null_geometry,
             "nan_string_geometry_count": nan_geometry,
             "self_intersecting_ring_feature_count": self_intersecting_ring_feature_count,
             "self_intersecting_ring_count": self_intersecting_ring_count,
             "zero_area_ring_feature_count": zero_area_ring_feature_count,
             "zero_area_ring_count": zero_area_ring_count,
             "self_intersecting_ring_note":
                 "GEOS/OGC validity rules call these producer rings self-intersecting; candidate WKB retains "
                 "every source vertex verbatim and the technical geometry quality acceptance remains unapproved.",
             "zero_area_ring_note":
                 "The producer also publishes collapsed zero-area rings (pinned count); candidate WKB retains "
                 "their vertices verbatim as degenerate exterior parts pending the technical quality decision.",
             "observed_bounds": bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[str(field["type"])]


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        x1, y1 = vertex
        x2, y2 = ring[(index + 1) % len(ring)]
        area += x1 * y2 - x2 * y1
    return area / 2


def _usable_rings(geom: object) -> list | None:
    if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) or not geom["rings"]:
        return None
    for ring in geom["rings"]:
        if not isinstance(ring, list) or not ring:
            return None
        for vertex in ring:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                return None
    return geom["rings"]


def _polygon_wkb(geom: object) -> bytes | None:
    """WKB for one feature's rings under the producer's verified ring semantics.

    Esri JSON (verified on the pinned capture): every ring is explicitly closed,
    clockwise rings are exteriors, counter-clockwise rings are holes. Vertices
    are passed to shapely verbatim; exteriors with no claimable hole keep their
    ring order and holes join their smallest containing exterior.
    """
    rings = _usable_rings(geom)
    if rings is None:
        return None
    exteriors, holes = [], []
    for ring in rings:
        area = _signed_area(ring)
        if area == 0:
            # Producer zero-area (collapsed) rings (5 on the pinned capture) are
            # retained verbatim as degenerate exterior parts: their vertices are
            # kept, the pinned count and the unapproved technical quality gate
            # carry the decision, and no hole-assignment semantics are invented.
            exteriors.append(ring)
            continue
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise ValueError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    # Note: the producer publishes rings that GEOS/OGC validity rules call
    # self-intersecting (2,518 rings in 1,647 features on the pinned capture).
    # They are represented verbatim in WKB with no vertex dropped, added, or
    # reordered; the pinned count and the false technical_geometry_quality
    # acceptance gate carry the quality decision, never a silent rewrite.
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        polygon = Polygon(hole)
        candidates = [(abs(area), index) for shell, area, index in shells if shell.contains(polygon.representative_point())]
        if not candidates:
            raise ValueError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for shell, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "SHAPE" else _arrow_type(field)
        if name == "SHAPE":
            values = [_polygon_wkb(row.get("geometry")) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        else:
            values = [row["attributes"].get(name) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        columns[name] = pa.array(values, type=typ)
    geo = {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_nfhl_political_jurisdictions(storage: StagingStorageResource, *, version: str,
                                         capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Political Jurisdictions candidate."""
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/206/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-political-jurisdictions-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            ids = sorted(seed)
            for name, url in _BRACKET_URLS:
                captures[name] = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            for page in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE):
                for name, body, receipt in _capture_page(client, capture_root, page,
                                                         ids[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]):
                    captures[name] = (body, receipt)
            for name, url in _AFTER_BRACKET_URLS:
                captures[name] = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
        layer, source_fields, extracted = _validate_captures(captures)
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
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained, type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Political Jurisdictions (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 22 'Political Jurisdictions', "
                       "captured completely from the producer's own REST publication. Complete source attributes "
                       "and NAD83 polygon geometry are represented in the candidate, including the producer's "
                       "null-geometry rows. The NFHL is continuously updated and publishes no per-snapshot "
                       "release date; freshness, rights, public schema/ID contract, and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": SERVICE_DOC_URL if name == "service_doc" else LAYER_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The NFHL is continuously updated; the service and its "
                                                   "documentation page publish no per-snapshot release date.",
                    "license": "Not stated by the captured endpoints; rights review pending.",
                    "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                                   "review."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's derived "
                                                             "SHAPE.STArea() and SHAPE.STLength() doubles; SHAPE is "
                                                             "the original geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, POL_AR_ID, GFID and "
                                                          "GlobalID values unchanged; no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polygon rings retained as WKB "
                                                        "without projection under the producer's verified Esri "
                                                        "ring semantics (closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes); no vertex dropped, added, or "
                                                        "reordered; GEOS/OGC self-intersecting source rings "
                                                        "(2,518 rings in 1,647 features on the pinned capture) "
                                                        "retained verbatim pending the technical quality "
                                                        "decision; null source geometry retained as null",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 22 'Political Jurisdictions' of that "
                   "producer service. The HIFLD Open inventory lists this dataset as "
                   "nfhl/political-jurisdictions-1 with DataLumos project 239983; that ICPSR archive copy and the "
                   "2024-10-31 archive modification date are identification and comparison context only, never "
                   "pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
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
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
         "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
                "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
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
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}
