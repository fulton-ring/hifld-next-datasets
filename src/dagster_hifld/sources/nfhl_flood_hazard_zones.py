"""Lossless, approval-gated capture of the FEMA NFHL "Flood Hazard Zones" polygon layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 28 "Flood Hazard
Zones" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/flood-hazard-zones-1``, DataLumos 239971 historical archive copy used
for identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

Scale economics measured by dated anonymous probes (2026-11-12): the live
layer holds 5,824,639 polygon features; the producer refuses whole
``maxRecordCount`` (2000-record) pages with a complete HTTP 200 Esri error
document (its serialized responses approach the producer's transfer cap), a
200-record slice of the roster head alone serialized to 26.7 MB (~133 KB per
coastal feature), and mid-roster 100-record slices serialized to ~0.9-1.1 MB
(~9-11 KB per feature). A complete lossless capture therefore spans multiple
tens of gigabytes of producer JSON across many thousands of requests, so this
module is built for resumable, memory-bounded capture instead of the
single-pass pattern of its smaller NFHL siblings:

- The capture attempt directory is stable per version and persists accepted
  segment bodies with hash-pinned receipts plus an accepted-segment plan
  (``segments.json``); a resumed run re-verifies each cached body against its
  receipt digest, re-fetches every service/layer/count/roster bracket fresh,
  and acquires only the missing roster ranges. Because every page carries its
  OBJECTID roster slice, re-checking cached pages against the fresh bracket
  roster proves each page still belongs to the live population.
- Validation streams page bodies from disk one page at a time: the roster
  slice check, strict OBJECTID monotonicity, per-feature canonical record and
  geometry digests, bounds, and null/NaN counts are computed incrementally and
  the GeoParquet candidate is written incrementally, so agent memory stays
  bounded no matter how large the layer grows.
- Until a dated complete capture establishes the population pins below, the
  module runs in live mode: acquisition and streaming validation complete, the
  computed population contract is written to ``population_pins.json`` in the
  attempt directory, and the run fails closed with
  ``source_population_pins_not_established`` without staging anything. With
  pins set, the module reproduces every pin and stages the unapproved
  candidate exactly like its NFHL siblings.

Producer polygon semantics follow the Esri JSON convention verified on FEMA's
sibling NFHL polygon layer (Alluvial Fans, pinned 2026-11-12): every source
ring is explicitly closed and clockwise rings are exteriors while
counter-clockwise rings are holes, so candidate WKB geometry is built by that
rule (holes join their smallest containing exterior) without dropping, adding,
or reordering any source vertex.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
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
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "nfhl-flood-hazard-zones"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
SERVICE_DOC_BODY_MARKER = b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
LAYER_ID = 28
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Flood Hazard Zones"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Any
# segment whose response would approach the producer's transfer cap is
# bisected until the producer serves it; see the module docstring.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
# Roster (returnIdsOnly) documents for this layer span several tens of
# megabytes, larger than the sibling layers' rosters; they use their own cap.
DOC_CAPTURE_MAX_BYTES = 256 * 1024 * 1024
# The producer refuses a whole-layer returnIdsOnly request with its 500 error
# document (observed 2026-11-12), but serves OBJECTID-range queries: a
# one-million-OBJECTID window returned ~2.6 MB / 385,763 ids, so the roster
# is assembled from disjoint windows of this width.
ROSTER_WINDOW = 1_000_000
MIN_FREE_BYTES = 24_000_000_000
# The producer's MapServer also intermittently answers valid requests with
# complete HTTP 200 Esri error documents or transient connection resets
# (observed on FEMA's sibling NFHL layers and on this layer's 2026-11-12
# probes). Every request retries a bounded number of times for that transient
# mode; a retry re-issues the exact same request, and completeness is still
# enforced by the before/after id rosters, counts, exact segment tiling, and
# full-record digests, so retries can never mask drift. Segments wider than
# one record that keep failing are bisected (the producer's transfer cap); a
# single-record segment that keeps failing fails the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Population pins below are None until a dated complete capture of the whole
# roster establishes them; see the module docstring. In live mode the capture
# completes, writes the computed contract to ``population_pins.json``, and
# fails closed without staging. The service layer roster, verified live
# 2026-11-12, is pinned immediately: it is one cheap independently
# re-verifiable service contract, not a capture-derived population contract.
EXPECTED_COUNT = None
EXPECTED_ID_SHA256 = None
EXPECTED_SCHEMA_SHA256 = None
EXPECTED_RECORD_SHA256 = None
EXPECTED_GEOMETRY_SHA256 = None
EXPECTED_OBSERVED_BOUNDS = None
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_NAN_GEOMETRY_COUNT = None
EXPECTED_LAYER_EXTENT = None
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


class _PinsPendingError(Exception):
    """Raised when a live-mode capture completes but population pins are unset."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:07d}_{count:04d}"


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{7,})_([0-9]{4,})$")


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("service_doc", SERVICE_DOC_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
    ]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
    ]


def _stats_query() -> str:
    return _query({"where": "1=1",
                   "outStatistics": json.dumps([
                       {"statisticType": "min", "onStatisticField": "OBJECTID",
                        "outStatisticFieldName": "MIN_OBJECTID"},
                       {"statisticType": "max", "onStatisticField": "OBJECTID",
                        "outStatisticFieldName": "MAX_OBJECTID"}]),
                   "returnGeometry": "false", "f": "json"})


def _roster_range_url(low: int, high: int) -> str:
    return _query({"where": f"OBJECTID>={low} AND OBJECTID<{high}", "returnIdsOnly": "true", "f": "json"})


def _capture_roster(client: httpx.Client, capture_root: Path, name_prefix: str,
                    min_oid: int, max_oid: int) -> list[int]:
    """Capture the complete OBJECTID roster as range-chunked returnIdsOnly requests.

    The producer refuses a whole-layer ``returnIdsOnly`` request for this
    layer's population (observed 2026-11-12: a complete HTTP 200 Esri error
    document of 80 bytes, retried four times), but serves constrained
    OBJECTID ranges; the roster is therefore assembled from disjoint windows
    covering [min_oid, max_oid], each window's response captured as its own
    evidence. The assembled roster must be unique and integer-valued.
    """
    ids: list[int] = []
    for index, low in enumerate(range(min_oid, max_oid + 1, ROSTER_WINDOW)):
        name = f"{name_prefix}_r{index:03d}"
        body, _receipt = _capture_with_retry(
            client, _roster_range_url(low, min(low + ROSTER_WINDOW, max_oid + 1)),
            capture_root, name, max_bytes=DOC_CAPTURE_MAX_BYTES,
            attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
        doc = json.loads(body)
        if not isinstance(doc, dict) or doc.get("objectIdFieldName") != "OBJECTID":
            raise ValueError("source_id_field_changed")
        chunk = doc.get("objectIds")
        if not isinstance(chunk, list) or any(type(value) is not int for value in chunk):
            raise ValueError("source_id_population_drift")
        ids.extend(chunk)
    if not ids or len(set(ids)) != len(ids):
        raise ValueError("source_id_population_drift")
    return sorted(ids)


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents
    and incomplete transport captures; failed attempts are preserved on disk
    under ``<name>.attempt-NN`` evidence names and never enter the capture
    roster. The final attempt's body is always returned as captured, so a
    persistent producer failure surfaces to the caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            for suffix in (".body", ".wire", ".receipt.json"):
                path = capture_root / f"{name}{suffix}"
                if path.exists():
                    path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _producer_error_doc(body) and attempt < attempts:
            for suffix in (".body", ".wire", ".receipt.json"):
                path = capture_root / f"{name}{suffix}"
                if path.exists():
                    path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


class _DiskPageBodies(Mapping):
    """Lazy read-only view of captured segment bodies stored on disk."""

    def __init__(self, capture_root: Path, names: list[str]):
        self._capture_root = capture_root
        self._names = {name: None for name in names}

    def __len__(self) -> int:
        return len(self._names)

    def __iter__(self):
        return iter(self._names)

    def __getitem__(self, name: str) -> bytes:
        return (self._capture_root / f"{name}.body").read_bytes()


def _load_plan(capture_root: Path) -> list[tuple[int, int]]:
    """Load the accepted-segment plan persisted by prior attempts, if any."""
    path = capture_root / "segments.json"
    if not path.exists():
        return []
    try:
        state = json.loads(path.read_bytes())
        segments = [tuple(pair) for pair in state["accepted"]]
    except (ValueError, KeyError, TypeError):
        raise ValueError("resume_segment_plan_unreadable")
    if any(not isinstance(offset, int) or not isinstance(count, int) or offset < 0 or count < 1
           for offset, count in segments):
        raise ValueError("resume_segment_plan_unreadable")
    segments.sort()
    previous_end = 0
    for offset, count in segments:
        if offset < previous_end:
            raise ValueError("resume_segment_plan_overlapped")
        previous_end = offset + count
    return segments


def _persist_plan(capture_root: Path, segments: list[tuple[int, int]]) -> None:
    payload = _canonical({"accepted": [[offset, count] for offset, count in sorted(segments)]})
    tmp = capture_root / "segments.json.tmp"
    tmp.write_bytes(payload)
    tmp.replace(capture_root / "segments.json")


def _verify_cached_segment(capture_root: Path, name: str) -> bool:
    body_path = capture_root / f"{name}.body"
    receipt_path = capture_root / f"{name}.receipt.json"
    if not body_path.exists() or not receipt_path.exists():
        return False
    try:
        receipt = json.loads(receipt_path.read_bytes())
        body = body_path.read_bytes()
    except (OSError, ValueError):
        return False
    return (receipt.get("complete_body") is True and receipt.get("status") == 200
            and receipt.get("bytes") == len(body) and receipt.get("sha256") == _sha(body)
            and not _producer_error_doc(body))


def _resolve_capture_root(attempt_root: Path, version: str) -> Path:
    """Reuse the one resumable attempt for this version, or start a fresh one.

    A prior incomplete attempt (``capture_manifest.json`` present and not
    marked sealed) is reused so its accepted segment bodies carry forward;
    multiple incomplete attempts are ambiguous and fail closed rather than
    guessing which evidence to trust. A prior sealed attempt is never reused
    (its candidate must be consumed through the construction-state cache).
    """
    prefix = f"{version}-"
    sealed = []
    candidates = []
    for child in sorted(child for child in attempt_root.iterdir()
                        if child.is_dir() and child.name.startswith(prefix)):
        manifest_path = child / "capture_manifest.json"
        manifest = {}
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_bytes())
            except ValueError:
                manifest = {}
        if manifest.get("complete") is True and manifest.get("staged") is True:
            # A sealed attempt's candidate is consumed through the
            # construction-state cache, never through its capture directory;
            # sealed attempts are ignored so a fresh attempt can still run.
            sealed.append(child)
            continue
        candidates.append(child)
    if len(candidates) > 1:
        raise ValueError("ambiguous_resume_capture_attempts")
    if candidates:
        return candidates[0]
    return attempt_root / f"{version}-{uuid4().hex}"


def _acquire_segments(client: httpx.Client, capture_root: Path, total: int) -> list[tuple[int, int]]:
    """Capture every roster segment, resuming from the persisted plan.

    The work queue starts from the gaps not already covered by accepted
    segments (a prior attempt's partially bisected tile leaves gaps inside its
    tile, which are re-enqueued as ranges and bisected again if the producer
    still refuses them). Every accepted segment is appended to the persisted
    plan before the next request, so an interrupted session loses at most the
    one in-flight segment.
    """
    accepted = _load_plan(capture_root)
    accepted = [(offset, count) for offset, count in accepted if _verify_cached_segment(
        capture_root, _segment_name(offset, count))]
    _persist_plan(capture_root, accepted)
    accepted_set = set(accepted)
    queue = []
    position = 0
    for offset, count in sorted(accepted) + [(total, 0)]:
        while position < offset:
            take = min(SEGMENT_START_RECORDS, offset - position)
            queue.append((position, take))
            position += take
        position = max(position, offset + count)
    accepted_segments: list[tuple[int, int]] = list(accepted)
    while queue:
        offset, count = queue.pop(0)
        name = _segment_name(offset, count)
        if (offset, count) in accepted_set:
            continue
        try:
            body, receipt = _capture_with_retry(
                client, _segment_query(offset, count), capture_root, name,
                max_bytes=MAX_CAPTURE_BYTES,
                attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1 else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            # A segment the producer keeps refusing or that cannot be served
            # within this module's capture cap (this layer's dense head region
            # makes 2000-record pages far larger than the sibling layers', both
            # observed 2026-11-12) is bisected; its refused attempts stay on
            # disk as evidence under the segment's name. A single-record
            # segment that still fails ends the capture explicitly. Smaller
            # requests are also more likely to survive the producer's transient
            # connection resets, and correctness never depends on why a segment
            # was refused: every served segment is verified against the pinned
            # roster slice.
            if count == 1 or not str(exc).startswith("incomplete_source_capture:"):
                raise
            middle = count // 2
            queue.insert(0, (offset + middle, count - middle))
            queue.insert(0, (offset, middle))
            continue
        if _producer_error_doc(body):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            queue.insert(0, (offset + middle, count - middle))
            queue.insert(0, (offset, middle))
            continue
        if (offset, count) not in accepted_segments:
            accepted_segments.append((offset, count))
            _persist_plan(capture_root, accepted_segments)
    return sorted(accepted_segments)


def _classify_geometries_streaming(features: list[dict[str, object]], attr_names: list[str],
                                   bounds: list[float], counters: dict[str, int]) -> list:
    """Validate one segment's features and return their geometry records.

    Each feature's geometry is validated and returned as ``[OBJECTID, rings]``
    (``[OBJECTID, None]`` for null geometry) aligned with the feature order;
    the caller folds them, in roster order and with the same list separators
    as the record digest, into the geometry digest. Bounds and null/NaN
    counters accumulate here. String-"NaN" ordinates (seen on FEMA's sibling
    NFHL layers) are retained verbatim in the record/raw evidence and gated as
    unusable geometry, never silently coerced.
    """
    geometry_records = []
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        if geom is None:
            counters["null_geometry"] += 1
            geometry_records.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) \
                or not geom["rings"]:
            raise ValueError("source_polygon_geometry_invalid")
        nan_rings = False
        validated_rings = []
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
                    bounds.extend((x, y))
                elif x == "NaN" and y == "NaN":
                    nan_rings = True
                else:
                    raise ValueError("source_polygon_geometry_invalid")
            validated_rings.append(ring)
        geometry_records.append([attrs["OBJECTID"], validated_rings])
        if nan_rings:
            counters["nan_geometry"] += 1
    return geometry_records


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[str(field["type"])]


def _signed_area(ring: list) -> float:
    """Signed area of a planar ring (positive counterclockwise), translated origin.

    The shoelace sum is taken around the first vertex instead of the global
    origin: per-edge terms then scale with the ring's own extent, so a tiny
    exterior ring next to global lon/lat coordinates keeps its sign. The raw
    global sum loses such rings to cancellation — live FEMA NFHL layer 4
    (Base Index) ring OBJECTID 49181, 2026-11-12, summed to exactly 0.0
    despite a true signed area of about -1.5e-15, which misclassified that
    clockwise exterior as a hole; this layer's dense national polygon
    population spans the same coordinate scale. Vertices are only read to
    measure orientation; stored geometry is never translated.
    """
    closed = ring if ring[0] == ring[-1] else ring + [ring[0]]
    x0, y0 = closed[0]
    area = math.fsum((x1 - x0) * (y2 - y0) - (x2 - x0) * (y1 - y0)
                     for (x1, y1), (x2, y2) in zip(closed, closed[1:]))
    return area / 2.0


def _polygon_wkb(geom: object) -> bytes | None:
    """WKB for one feature's rings under the producer's verified ring semantics.

    Esri JSON (verified on FEMA's pinned sibling polygon captures): every ring
    is explicitly closed, clockwise rings are exteriors, counter-clockwise
    rings are holes. Vertices are passed to shapely verbatim; exteriors with
    no claimable hole keep their ring order and holes join their smallest
    containing exterior.
    """
    if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) \
            or not geom["rings"]:
        return None
    rings = geom["rings"]
    for ring in rings:
        if not isinstance(ring, list) or not ring:
            return None
        for vertex in ring:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                return None
    exteriors, holes = [], []
    for ring in rings:
        area = _signed_area(ring)
        if area == 0:
            raise ValueError("source_polygon_ring_degenerate")
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise ValueError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), index) for index, ring in enumerate(exteriors)]
    for shell, _ in shells:
        if not shell.is_valid:
            raise ValueError("source_polygon_ring_self_intersecting")
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        polygon = Polygon(hole)
        candidates = [(abs(_signed_area(ring)), index) for index, ring in enumerate(exteriors)
                      if Polygon(ring).contains(polygon.representative_point())]
        if not candidates:
            raise ValueError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = [Polygon(exteriors[index], assigned[index]) for _, index in sorted(shells, key=lambda item: item[1])]
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _validate_captures(page_bodies: Mapping[str, bytes], docs: Mapping[str, object],
                       ids: list[int], *, parquet_path: Path | None = None):
    """Stream-validate the capture and compute the live population contract.

    Page bodies are consumed one page at a time in roster order; the accepted
    segments must tile the roster exactly, every page must carry its exact
    OBJECTID slice in ascending order, and every feature's canonical record
    and geometry are folded into streaming digests. When ``parquet_path`` is
    given, the GeoParquet candidate is written incrementally alongside the
    digest pass. In live mode (pins unset) the computed contract is returned
    for pin establishment; with pins set every computed value is compared to
    its pin.
    """
    segments = sorted(_parse_segment_name(name) for name in page_bodies)
    page_names = [_segment_name(offset, count) for offset, count in segments]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != len(ids):
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if SERVICE_DOC_BODY_MARKER not in docs["service_doc"]:
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
        raise ValueError("exact_nfhl_flood_hazard_zones_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_nfhl_flood_hazard_zones_layer_metadata_drift")
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if EXPECTED_LAYER_EXTENT is not None \
            and {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if EXPECTED_SCHEMA_SHA256 is not None and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if _stable_layer(docs[name]) != _stable_layer(layer):
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if (docs["count_before"].get("count") != len(ids)
            or docs["count_after"].get("count") != len(ids)):
        raise ValueError("source_count_drift")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    for name in ("ids_before", "ids_after"):
        roster_ids = docs[name].get("objectIds")
        if (not isinstance(roster_ids, list) or len(roster_ids) != len(ids)
                or any(type(value) is not int for value in roster_ids)
                or len(set(roster_ids)) != len(ids) or sorted(roster_ids) != ids):
            raise ValueError("source_id_population_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after")):
        raise ValueError("source_id_field_changed")

    attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
    record_digest = hashlib.sha256()
    geometry_digest = hashlib.sha256()
    record_digest.update(b"[")
    geometry_digest.update(b"[")
    counters = {"null_geometry": 0, "nan_geometry": 0, "features": 0}
    bounds: list[float] = []
    writer = None
    arrow_schema = None
    first_feature_index = 0
    previous_oid = None
    try:
        for (offset, count), name in zip(segments, page_names):
            page = json.loads(page_bodies[name])
            if not isinstance(page, dict) or "error" in page:
                raise ValueError("source_page_error")
            _validate_spatial_reference(page.get("spatialReference"), f"source_page_{offset:07d}")
            rows = page.get("features")
            if not isinstance(rows, list):
                raise ValueError("source_page_missing_features")
            expected = ids[offset:offset + count]
            actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
            if actual != expected or len(rows) != len(expected):
                raise ValueError("source_page_id_slice_incomplete_or_reordered")
            if page.get("exceededTransferLimit") is True and offset + count >= len(ids):
                raise ValueError("source_page_unexpected_exceeded_transfer_limit")
            for index, feature in enumerate(rows):
                attrs = feature.get("attributes")
                oid = attrs["OBJECTID"] if isinstance(attrs, dict) else None
                if previous_oid is not None and oid is not None and oid <= previous_oid:
                    raise ValueError("source_page_objectid_not_ascending")
                previous_oid = oid
            geometry_records = _classify_geometries_streaming(rows, attr_names, bounds, counters)
            for feature, geometry_record in zip(rows, geometry_records):
                if first_feature_index:
                    record_digest.update(b",")
                    geometry_digest.update(b",")
                record_digest.update(_canonical(feature))
                geometry_digest.update(_canonical(geometry_record))
                first_feature_index += 1
            counters["features"] += len(rows)
            if parquet_path is not None:
                if writer is None:
                    arrow_schema = pa.schema(
                        [pa.field(field["name"], pa.binary() if field["name"] == "SHAPE"
                                  else _arrow_type(field), nullable=True) for field in fields])
                    geo = {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
                        "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"],
                        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
                    writer = pq.ParquetWriter(parquet_path, arrow_schema.with_metadata({b"geo": _canonical(geo)}),
                                              compression="zstd")
                columns = {}
                for field in fields:
                    column_name = field["name"]
                    if column_name == "SHAPE":
                        values = [_polygon_wkb(row.get("geometry")) for row in rows]
                    else:
                        values = [row["attributes"].get(column_name) for row in rows]
                    columns[column_name] = pa.array(values, type=arrow_schema.field(column_name).type)
                writer.write_table(pa.Table.from_pydict(columns, schema=arrow_schema))
    finally:
        if writer is not None:
            writer.close()
    record_digest.update(b"]")
    geometry_digest.update(b"]")
    record_sha = record_digest.hexdigest()
    geometry_sha = geometry_digest.hexdigest()
    computed_bounds = None
    if bounds:
        computed_bounds = {"xmin": min(bounds[0::2]), "ymin": min(bounds[1::2]),
                           "xmax": max(bounds[0::2]), "ymax": max(bounds[1::2])}
    audit = {"source_feature_count": counters["features"], "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polygon", "null_geometry_count": counters["null_geometry"],
             "nan_string_geometry_count": counters["nan_geometry"],
             "observed_bounds": computed_bounds,
             "declared_layer_extent": {key: extent.get(key) for key in
                                       ("xmin", "ymin", "xmax", "ymax")} if extent else None,
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    if EXPECTED_COUNT is not None:
        if counters["features"] != EXPECTED_COUNT:
            raise ValueError("source_feature_count_drift")
        if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
            raise ValueError("source_id_roster_drift")
        if geometry_sha != EXPECTED_GEOMETRY_SHA256:
            raise ValueError("source_geometry_or_coverage_drift")
        if record_sha != EXPECTED_RECORD_SHA256:
            raise ValueError("source_record_value_drift")
        if (counters["null_geometry"] != EXPECTED_NULL_GEOMETRY_COUNT
                or counters["nan_geometry"] != EXPECTED_NAN_GEOMETRY_COUNT):
            raise ValueError("source_geometry_null_or_nan_count_drift")
        if EXPECTED_OBSERVED_BOUNDS is not None and computed_bounds != EXPECTED_OBSERVED_BOUNDS:
            raise ValueError("source_observed_coverage_drift")
        contract = {}
    else:
        contract = {"expected_count": counters["features"], "expected_id_sha256": _sha(_canonical(ids)),
                    "expected_schema_sha256": schema_sha, "expected_record_sha256": record_sha,
                    "expected_geometry_sha256": geometry_sha,
                    "expected_observed_bounds": computed_bounds,
                    "expected_null_geometry_count": counters["null_geometry"],
                    "expected_nan_geometry_count": counters["nan_geometry"],
                    "expected_layer_extent": audit["declared_layer_extent"]}
    return layer, fields_value, {"audit": audit, "ids": ids, "contract": contract}


def _stable_layer(doc: dict) -> dict:
    """Layer document without the producer's per-request volatile symbology.

    Verified live 2026-11-12 on this layer (and previously on the sibling NFHL
    polygon layer): every layer fetch rotates
    ``drawingInfo.renderer.authoringInfo.colorRamp.colorRamps`` palette colors;
    everything else in the layer document is stable. Only that palette is
    excluded from before/after equality; the rest of the renderer and the whole
    layer document are still compared.
    """
    copy = json.loads(json.dumps(doc))
    ramp = copy.get("drawingInfo", {}).get("renderer", {}).get("authoringInfo", {}).get("colorRamp", {})
    if isinstance(ramp, dict) and "colorRamps" in ramp:
        del ramp["colorRamps"]
    return copy


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _manifest_body(manifest: Mapping[str, object]) -> bytes:
    return _canonical(manifest) + b"\n"


def capture_nfhl_flood_hazard_zones(storage: StagingStorageResource, *, version: str,
                                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and (with pins set) stage the NFHL Flood Hazard Zones candidate.

    With population pins unset, the resumable capture completes in live mode:
    the computed population contract is written to ``population_pins.json`` in
    the attempt directory and the run fails closed without staging anything.
    """
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/194/captures")
    attempt_root.mkdir(parents=True, exist_ok=True)
    capture_root = _resolve_capture_root(attempt_root, version).absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    docs: dict[str, object] = {}
    doc_bodies: dict[str, bytes] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=300,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-flood-hazard-zones-draft"}) as client:
            stats_body, _ = _capture_with_retry(
                client, _stats_query(), capture_root, "ids_stats", max_bytes=DOC_CAPTURE_MAX_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            stats_features = json.loads(stats_body).get("features")
            stats_attrs = stats_features[0].get("attributes") \
                if isinstance(stats_features, list) and stats_features and isinstance(stats_features[0], dict) else None
            stats_attrs = stats_attrs if isinstance(stats_attrs, dict) else {}
            min_oid, max_oid = stats_attrs.get("MIN_OBJECTID"), stats_attrs.get("MAX_OBJECTID")
            if type(min_oid) is not int or type(max_oid) is not int or max_oid < min_oid:
                raise ValueError("source_oid_stats_drift")
            ids = _capture_roster(client, capture_root, "ids_seed", min_oid, max_oid)
            for name, url in _doc_urls():
                body, _ = _capture_with_retry(client, url, capture_root, name,
                                              max_bytes=DOC_CAPTURE_MAX_BYTES,
                                              attempts=DOC_CAPTURE_ATTEMPTS,
                                              base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                doc_bodies[name] = body
            ids_before = _capture_roster(client, capture_root, "ids_before", min_oid, max_oid)
            accepted = _acquire_segments(client, capture_root, len(ids))
            page_bodies = _DiskPageBodies(capture_root, [_segment_name(offset, count)
                                                         for offset, count in accepted])
            for name, url in _after_doc_urls():
                body, _ = _capture_with_retry(client, url, capture_root, name,
                                              max_bytes=DOC_CAPTURE_MAX_BYTES,
                                              attempts=DOC_CAPTURE_ATTEMPTS,
                                              base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                doc_bodies[name] = body
            ids_after = _capture_roster(client, capture_root, "ids_after", min_oid, max_oid)
        for name in ("service_before", "layer_before", "count_before",
                     "layer_after", "service_after", "count_after"):
            docs[name] = json.loads(doc_bodies[name])
        docs["ids_before"] = {"objectIdFieldName": "OBJECTID", "objectIds": ids_before}
        docs["ids_after"] = {"objectIdFieldName": "OBJECTID", "objectIds": ids_after}
        docs["service_doc"] = doc_bodies["service_doc"]
        parquet_path = capture_root / f"{DATASET_SLUG}.parquet"
        layer, source_fields, extracted = _validate_captures(page_bodies, docs, ids, parquet_path=parquet_path)
    except ValueError as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
             "staged": False, "error_code": type(exc).__name__,
             "error_text": str(exc), "resumable": True, "receipt_names": sorted(receipts)}))
        raise
    audit = dict(extracted["audit"])
    identity = _code_identity()
    if not audit["source_feature_count"] or audit["source_feature_count"] != len(extracted["ids"]):
        raise ValueError("source_feature_count_roster_mismatch")
    if EXPECTED_COUNT is None:
        (capture_root / "population_pins.json").write_bytes(_manifest_body({
            "established_by": "live-mode complete capture (pins unset at capture time)",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "layer_id": LAYER_ID, "layer_name": LAYER_NAME, **extracted["contract"]}))
        (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "staged": False,
             "reason_code": "source_population_pins_not_established",
             "source_feature_count": audit["source_feature_count"]}))
        raise _PinsPendingError("source_population_pins_not_established")
    parquet_rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_rel: {"bytes": parquet_path.stat().st_size, "sha256": _file_sha256(parquet_path)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Flood Hazard Zones (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 28 'Flood Hazard Zones', captured "
                       "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                       "polygon geometry are represented in the candidate. The NFHL is continuously updated and "
                       "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                       "release acceptance remain unresolved and unapproved.",
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
                   for name, receipt in sorted(receipt_index(capture_root).items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's derived "
                                                             "SHAPE.STArea() and SHAPE.STLength() doubles; SHAPE is "
                                                             "the original geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, GFID, GlobalID, FLD_AR_ID "
                                                          "and all source attribute values unchanged; no "
                                                          "substitute IDs, casts, trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polygon rings retained as WKB "
                                                        "without projection under the producer's verified ring "
                                                        "semantics (closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes; holes join their smallest "
                                                        "containing exterior); null source geometry retained as "
                                                        "null; string-'NaN' ordinate rows retained as records "
                                                        "with null candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-"
                   "hazard-layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer "
                   "(NFHL)' documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/"
                   "NFHL/MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 28 'Flood Hazard Zones' of that "
                   "producer service. The HIFLD Open inventory lists this dataset as nfhl/flood-hazard-zones-1 "
                   "with DataLumos project 239971; that ICPSR archive copy and the 2024-10-31 archive "
                   "modification date are identification and comparison context only, never pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": parquet_rel, **source_objects[parquet_rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_fields": [_plain_field(f) for f in source_fields],
                  "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
                  "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False})
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    metadata_objects = {
        "metadata/source_manifest.json": _manifest_body(manifest),
        "metadata/source_audit.json": _manifest_body(audit),
        "metadata/processing_identity.json": _manifest_body(identity),
        "metadata/candidate_ready.json": _manifest_body(
            {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}),
    }
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
                "feature_count": audit["source_feature_count"],
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    for name, body in metadata_objects.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(f"{prefix}/{name}") != _sha(body):
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    _upload_immutable_file(storage, f"{prefix}/{parquet_rel}", parquet_path, source_objects[parquet_rel])
    capture_receipts = {}
    for name in sorted(page_bodies):
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
        capture_receipts[name] = receipt
        _upload_immutable_file(storage, f"{prefix}/raw/{name}.body", capture_root / f"{name}.body",
                               {"bytes": (capture_root / f"{name}.body").stat().st_size,
                                "sha256": receipt["sha256"]})
        _upload_immutable_file(storage, f"{prefix}/raw/{name}.wire", capture_root / f"{name}.wire",
                               {"bytes": (capture_root / f"{name}.wire").stat().st_size,
                                "sha256": _file_sha256(capture_root / f"{name}.wire")})
        write_immutable(storage, f"{prefix}/raw/{name}.receipt.json", _manifest_body(receipt))
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in metadata_objects.items()}
    descriptors[parquet_rel] = source_objects[parquet_rel]
    for name in sorted(page_bodies):
        descriptors[f"raw/{name}.body"] = {"bytes": (capture_root / f"{name}.body").stat().st_size,
                                           "sha256": capture_receipts[name]["sha256"]}
        descriptors[f"raw/{name}.wire"] = {"bytes": (capture_root / f"{name}.wire").stat().st_size,
                                           "sha256": _file_sha256(capture_root / f"{name}.wire")}
        descriptors[f"raw/{name}.receipt.json"] = {"bytes": len(_manifest_body(capture_receipts[name])),
                                                   "sha256": _sha(_manifest_body(capture_receipts[name]))}
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": capture_receipts,
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
        {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "staged": True,
         "candidate_version": version, "candidate_prefix": prefix}))
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def receipt_index(capture_root: Path) -> dict[str, dict[str, object]]:
    receipts = {}
    for path in sorted(capture_root.glob("*.receipt.json")):
        name = path.name.removesuffix(".receipt.json")
        if name.startswith("page_") or name in ("ids_seed", "service_doc", "service_before", "layer_before",
                                                "count_before", "ids_before", "layer_after", "service_after",
                                                "count_after", "ids_after"):
            try:
                receipts[name] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                continue
    return receipts


def _upload_immutable_file(storage: StagingStorageResource, key: str, source: Path,
                           descriptor: Mapping[str, object]) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.upload_local_file(source, key)
        snapshot = storage.object_snapshot(key)
    if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
        raise ValueError("candidate_output_physical_readback_failed:" + key)
