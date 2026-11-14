"""Lossless, approval-gated capture of the FEMA NFHL "Flood Hazard Boundaries" line layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 27 "Flood Hazard
Boundaries" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/flood-hazard-boundaries``, DataLumos 239970 historical archive copy
used for identification only). The layer is continuously updated and publishes
no per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

Scale and resumability: the producer reported 15,270,272 features for this
layer on 2026-11-12 (returnCountOnly, observed twice in agreement), roughly
ten times the sibling Base Flood Elevations layer, so the capture is
deliberately resumable. Every served roster segment is persisted under the
capture directory with its wire copy and hash-pinned receipt; a restarted
capture re-verifies and reuses each complete segment instead of refetching
it, and never treats a stale or mismatched segment as evidence. The capture
still proves completeness the same way as the sibling NFHL adapters: pinned
count and the complete sorted OBJECTID roster, exact segment tiling,
per-page id-slice equality, before/after count/id/metadata brackets, and
full-record digests.

Roster assembly: the producer refuses a whole-layer ``returnIdsOnly``
request for this population with its complete HTTP 200 Esri 500 error
document (observed 2026-11-12, retried in the prior session and again in
this one), but serves OBJECTID-range id queries, so the roster is assembled
from disjoint OBJECTID windows exactly like the sibling Flood Hazard Zones
capture; every window is its own captured, digest-pinned request, and the
validation re-assembles all three rosters (seed, before, after) from the
persisted window evidence.

Population digest pins (record, geometry, null/nan counts, observed bounds)
are established only by a first complete capture; they are ``None`` in this
draft until that capture completes. While they are pending, the capture
computes the digests from its own complete segment set, records them in the
capture manifest, and then fails closed with
``population_digest_pins_not_established`` so nothing is staged and no
candidate exists until the pins are filled from that dated evidence. The
producer also intermittently answers valid requests with transient connection
resets or complete HTTP 200 Esri error documents, so bounded retries re-issue
the identical request and preserve refused attempts as evidence; retries can
never mask drift.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import time
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
import shapely
from shapely.geometry import LineString, MultiLineString

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

DATASET_SLUG = "nfhl-flood-hazard-boundaries"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 27
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Flood Hazard Boundaries"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Dense
# multipart boundary features (observed up to ~150 KB of JSON per feature in
# the low-OBJECTID coastal region on 2026-11-12) make some full-width pages
# exceed this cap; such a segment is bisected until the producer serves both
# halves, exactly like the sibling layers' transfer-cap handling.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 64_000_000_000
# The producer's MapServer also intermittently answers valid requests with
# complete HTTP 200 Esri error documents or transient connection resets
# (observed 2026-11-12 while probing this layer; identical requests succeed
# moments later). Every request retries a bounded number of times for that
# transient mode; a retry re-issues the exact same request, and completeness
# is still enforced by the pinned before/after id rosters, counts, exact
# segment tiling, and full-record digests, so retries can never mask drift.
# Segments wider than one record that keep failing are bisected (the
# producer's transfer cap); a single-record segment that keeps failing fails
# the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Roster windows: the producer refuses a whole-layer returnIdsOnly request
# for this population with its complete HTTP 200 Esri 500 error document
# (observed 2026-11-12: ~40 s to produce the 80-byte error document, after
# ten refusals in the prior session's probes), but it serves OBJECTID-range
# id queries: 1,000,000-wide windows returned 155,786 ids / 1,031,989 bytes
# over OBJECTID 3414..1003414, 53,607 / 482,509 over 30000000..31000000, and
# 822,806 / 7,405,300 over the dense 70000000..71308225 span (all ascending),
# so the roster is assembled from disjoint windows of this width, the same
# established pattern as the sibling Flood Hazard Zones capture.
DOC_CAPTURE_MAX_BYTES = 256 * 1024 * 1024
ROSTER_WINDOW = 1_000_000
# Structural pins below are the observed 2026-11-12 producer contract,
# established by dated anonymous queries of the producer's own endpoints
# (returnCountOnly agreed twice, the complete OBJECTID roster assembled from
# range-window id queries, and the layer/service metadata documents); the
# module's own capture must reproduce every pin. The population digest pins
# (record, geometry, null/nan counts, observed bounds) are None until a first
# complete capture establishes them; see the module docstring. They fail
# closed, so the capture can never stage an unverified candidate.
EXPECTED_COUNT = 15270272
EXPECTED_ID_SHA256: str | None = None
EXPECTED_SCHEMA_SHA256 = "3218e41168e49ca448b3e782a7eee571f24ef33996afd64f6f4e6736959ec1a5"
EXPECTED_RECORD_SHA256: str | None = None
EXPECTED_GEOMETRY_SHA256: str | None = None
EXPECTED_OBSERVED_BOUNDS: dict[str, float] | None = None
EXPECTED_NULL_GEOMETRY_COUNT: int | None = None
EXPECTED_NAN_GEOMETRY_COUNT: int | None = None
EXPECTED_LAYER_EXTENT = {"xmin": -170.84840210923284, "ymin": -14.375160286190692,
                         "xmax": 145.83219414839436, "ymax": 66.61058213599716}
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

_PENDING_PIN_NAMES = ("EXPECTED_ID_SHA256", "EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256",
                      "EXPECTED_OBSERVED_BOUNDS", "EXPECTED_NULL_GEOMETRY_COUNT",
                      "EXPECTED_NAN_GEOMETRY_COUNT")


class PopulationDigestPinsPending(ValueError):
    """The complete segment set validated, but population digest pins are not set.

    Carries the digests computed from this capture so the capture manifest can
    record them as dated evidence for filling the module pins. Nothing may be
    staged while this is pending.
    """

    def __init__(self, audit: dict[str, object]):
        super().__init__("population_digest_pins_not_established")
        self.audit = audit


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``.

    Byte-identical to ``_sha(_canonical(list(items)))`` without materializing
    the complete document, which for this layer is far larger than memory
    should ever hold.
    """
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6,})_([0-9]{5,})$")


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


_ROSTER_WINDOW_NAME_PATTERN = re.compile(r"ids_(seed|before|after)_r([0-9]{3,})$")


def _parse_roster_window_name(name: str) -> tuple[str, int]:
    match = _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return match.group(1), int(match.group(2))


def _assemble_roster(capture_root: Path, window_names: list[str]) -> list[int]:
    """Re-assemble one stage's OBJECTID roster from its persisted window evidence.

    Every window body is parsed from the capture directory, must carry the
    producer's ``OBJECTID`` id field with integer ids, and the assembled
    roster must be non-empty and unique. This is the validation-side proof
    that the persisted window evidence itself reproduces the roster the
    capture assembled in memory.
    """
    ids: list[int] = []
    for name in window_names:
        body = (capture_root / f"{name}.body").read_bytes()
        if _producer_error_doc(body):
            raise ValueError("source_ids_roster_window_refused")
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


def _capture_roster(client: httpx.Client, capture_root: Path, stage: str,
                    captures: dict[str, tuple[bytes | None, dict[str, object]]],
                    min_oid: int, max_oid: int) -> list[int]:
    """Capture the complete OBJECTID roster as range-window returnIdsOnly requests.

    The producer refuses a whole-layer ``returnIdsOnly`` request for this
    layer's population with its complete HTTP 200 Esri 500 error document
    (observed 2026-11-12), but serves constrained OBJECTID ranges, so the
    roster is assembled from disjoint windows covering [min_oid, max_oid],
    each window's response captured as its own digest-pinned request. The
    assembled roster must be unique and integer-valued; window bodies stay
    in the resumable capture directory, and the capture's roster validation
    re-assembles every stage's roster from that persisted evidence.
    """
    ids: list[int] = []
    for index, low in enumerate(range(min_oid, max_oid + 1, ROSTER_WINDOW)):
        name = f"ids_{stage}_r{index:03d}"
        body, receipt = _capture_with_retry(
            client, _roster_range_url(low, min(low + ROSTER_WINDOW, max_oid + 1)),
            capture_root, name, max_bytes=DOC_CAPTURE_MAX_BYTES,
            attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
        if _producer_error_doc(body):
            raise ValueError("source_ids_roster_window_refused")
        doc = json.loads(body)
        if not isinstance(doc, dict) or doc.get("objectIdFieldName") != "OBJECTID":
            raise ValueError("source_id_field_changed")
        chunk = doc.get("objectIds")
        if not isinstance(chunk, list) or any(type(value) is not int for value in chunk):
            raise ValueError("source_id_population_drift")
        ids.extend(chunk)
        captures[name] = (None, receipt)
    if not ids or len(set(ids)) != len(ids):
        raise ValueError("source_id_population_drift")
    return sorted(ids)


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


def _reuse_existing_segment(capture_root: Path, name: str, url: str) -> dict[str, object] | None:
    """Return the receipt of a previously captured, digest-verified segment, else None.

    A resumable capture must never treat stale evidence as a served segment:
    the body's own sha256 must equal the pinned receipt digest, the receipt
    must record this exact request URL with a complete HTTP 200 status, and
    the body must not be a producer error document. Anything else is renamed
    to a ``.stale`` evidence name so it is preserved but never reused.
    """
    body_path = capture_root / f"{name}.body"
    receipt_path = capture_root / f"{name}.receipt.json"
    if not body_path.exists() or not receipt_path.exists():
        return None
    try:
        receipt = json.loads(receipt_path.read_bytes())
        body = body_path.read_bytes()
    except (OSError, ValueError):
        return None
    usable = (isinstance(receipt, dict) and receipt.get("url") == url
              and receipt.get("status") == 200 and receipt.get("complete_body") is True
              and receipt.get("sha256") == _sha(body))
    if not usable or _producer_error_doc(body):
        for suffix in (".body", ".wire", ".receipt.json"):
            path = capture_root / f"{name}{suffix}"
            if path.exists():
                path.rename(capture_root / f"{name}.stale{suffix}")
        return None
    return receipt


def _capture_segments(client: httpx.Client, capture_root: Path,
                      total: int) -> dict[str, tuple[None, dict[str, object]]]:
    """Capture the complete layer as adaptively bisected, resumable roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the
    refused segment's final body and receipts stay on disk as evidence under
    its own name. A single-record segment that still fails ends the capture
    explicitly. A complete, digest-verified segment left on disk by an earlier
    attempt is reused without refetching so a restarted capture resumes
    instead of repeating completed downloads.

    Segment bodies are returned to the caller only as on-disk files and their
    receipts: this population's bodies cannot be held in memory at once.
    """
    captures: dict[str, tuple[None, dict[str, object]]] = {}
    segments: deque[tuple[int, int]] = deque(
        (offset, min(SEGMENT_START_RECORDS, total - offset))
        for offset in range(0, total, SEGMENT_START_RECORDS)
    )
    while segments:
        offset, count = segments.popleft()
        name = _segment_name(offset, count)
        url = _segment_query(offset, count)
        reused = _reuse_existing_segment(capture_root, name, url)
        if reused is not None:
            captures[name] = (None, reused)
            continue
        body, receipt = _capture_with_retry(
            client, url, capture_root, name,
            max_bytes=MAX_CAPTURE_BYTES,
            attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
            base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1 else DOC_CAPTURE_BASE_DELAY_SECONDS)
        if _producer_error_doc(body):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            segments.appendleft((offset + middle, count - middle))
            segments.appendleft((offset, middle))
            continue
        captures[name] = (None, receipt)
    return captures


class _SegmentAccumulator:
    """Streams one page's records into the whole-population digests and gates.

    The producer serves pages in ascending OBJECTID order and every page's id
    slice is enforced against the pinned sorted roster, so concatenating the
    segment streams reproduces the record order of the sibling adapters'
    sorted population without holding the layer in memory (this population is
    roughly ten times the sibling Base Flood Elevations layer).
    """

    def __init__(self, attr_names: list[str]):
        self.attr_names = attr_names
        self.record_digest = hashlib.sha256()
        self.geometry_digest = hashlib.sha256()
        self.record_digest.update(b"[")
        self.geometry_digest.update(b"[")
        self.feature_count = 0
        self.geometry_count = 0
        self.null_geometry = 0
        self.nan_geometry = 0
        self.bounds: list[float] | None = None
        self.last_oid: int | None = None

    def _extend_digest(self, digest: hashlib.sha256, item: object, first: bool) -> None:
        if not first:
            digest.update(b",")
        digest.update(_canonical(item))

    def add_page(self, features: list[dict[str, object]]) -> None:
        for feature in features:
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != self.attr_names:
                raise ValueError("source_record_attribute_schema_drift")
            oid = attrs["OBJECTID"]
            if self.last_oid is not None and type(oid) is int and oid <= self.last_oid:
                raise ValueError("source_page_id_slice_incomplete_or_reordered")
            self.last_oid = oid
            self._extend_digest(self.record_digest, feature, self.feature_count == 0)
            self.feature_count += 1
            geometry_record = self._geometry_record(feature, oid)
            if geometry_record is not None:
                self._extend_digest(self.geometry_digest, geometry_record, self.geometry_count == 0)
                self.geometry_count += 1

    def _geometry_record(self, feature: dict[str, object], oid: object):
        geom = feature.get("geometry")
        if geom is None:
            self.null_geometry += 1
            return [oid, None]
        if not isinstance(geom, dict) or set(geom) != {"paths"} or not isinstance(geom["paths"], list) \
                or not geom["paths"]:
            raise ValueError("source_line_geometry_invalid")
        nan_paths = False
        feature_xmin = feature_ymin = feature_xmax = feature_ymax = None
        for path in geom["paths"]:
            if not isinstance(path, list) or len(path) < 2:
                raise ValueError("source_line_geometry_invalid")
            for vertex in path:
                if not isinstance(vertex, list) or len(vertex) != 2:
                    raise ValueError("source_line_geometry_invalid")
                x, y = vertex
                if type(x) in (int, float) and type(y) in (int, float):
                    if not (-180 <= x <= 180 and -90 <= y <= 90):
                        raise ValueError("source_coordinate_range_drift")
                    if feature_xmin is None or x < feature_xmin:
                        feature_xmin = x
                    if feature_xmax is None or x > feature_xmax:
                        feature_xmax = x
                    if feature_ymin is None or y < feature_ymin:
                        feature_ymin = y
                    if feature_ymax is None or y > feature_ymax:
                        feature_ymax = y
                elif x == "NaN" and y == "NaN":
                    # FEMA's sibling NFHL point layer publishes string-"NaN"
                    # ordinates; such vertices are retained verbatim in the
                    # record/raw evidence and gated as unusable geometry here,
                    # never silently coerced to a location.
                    nan_paths = True
                else:
                    raise ValueError("source_line_geometry_invalid")
        if nan_paths:
            self.nan_geometry += 1
        elif feature_xmin is not None:
            if self.bounds is None:
                self.bounds = [feature_xmin, feature_ymin, feature_xmax, feature_ymax]
            else:
                if feature_xmin < self.bounds[0]:
                    self.bounds[0] = feature_xmin
                if feature_ymin < self.bounds[1]:
                    self.bounds[1] = feature_ymin
                if feature_xmax > self.bounds[2]:
                    self.bounds[2] = feature_xmax
                if feature_ymax > self.bounds[3]:
                    self.bounds[3] = feature_ymax
        return [oid, geom["paths"]]

    def finish(self) -> dict[str, object]:
        self.record_digest.update(b"]")
        self.geometry_digest.update(b"]")
        observed_bounds = None if self.bounds is None else {
            "xmin": self.bounds[0], "ymin": self.bounds[1], "xmax": self.bounds[2], "ymax": self.bounds[3]}
        return {"record_sha256": self.record_digest.hexdigest(),
                "geometry_sha256": self.geometry_digest.hexdigest(),
                "null_geometry_count": self.null_geometry,
                "nan_string_geometry_count": self.nan_geometry,
                "observed_bounds": observed_bounds}


def _validate_docs_and_service(captures: dict[str, tuple[bytes, dict[str, object]]],
                               docs: dict[str, object]):
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
        raise ValueError("exact_nfhl_flood_hazard_boundaries_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolyline"):
        raise ValueError("exact_nfhl_flood_hazard_boundaries_layer_metadata_drift")
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
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    return layer, fields_value


def _validate_rosters(capture_root: Path, docs: dict[str, object],
                     roster_windows: dict[str, list[str]]) -> list[int]:
    """Re-assemble every stage's OBJECTID roster from its persisted windows.

    The window plan must exactly cover the captured min/max OBJECTID span at
    ROSTER_WINDOW width; each stage's roster is re-assembled from disk, must
    have the pinned count, be unique, and match the seed roster, and the
    pinned id-roster digest must reproduce. Returns the sorted seed roster.
    """
    stats_features = docs["ids_stats"].get("features")
    stats_attrs = stats_features[0].get("attributes") \
        if isinstance(stats_features, list) and stats_features and isinstance(stats_features[0], dict) else None
    stats_attrs = stats_attrs if isinstance(stats_attrs, dict) else {}
    min_oid, max_oid = stats_attrs.get("MIN_OBJECTID"), stats_attrs.get("MAX_OBJECTID")
    if type(min_oid) is not int or type(max_oid) is not int or max_oid < min_oid:
        raise ValueError("source_oid_stats_drift")
    expected_windows = list(range((max_oid - min_oid + 1 + ROSTER_WINDOW - 1) // ROSTER_WINDOW))
    for stage in ("ids_seed", "ids_before", "ids_after"):
        indices = sorted(_parse_roster_window_name(name)[1] for name in roster_windows.get(stage, []))
        if indices != expected_windows:
            raise ValueError("source_id_population_drift")
    ids = _assemble_roster(capture_root, roster_windows["ids_seed"])
    if len(ids) != EXPECTED_COUNT:
        raise ValueError("source_id_population_drift")
    for stage in ("ids_before", "ids_after"):
        if _assemble_roster(capture_root, roster_windows[stage]) != ids:
            raise ValueError("source_id_population_drift")
    if EXPECTED_ID_SHA256 is not None and _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    return ids


def _validate_captures(capture_root: Path, captures: dict[str, tuple[bytes | None, dict[str, object]]],
                       roster_windows: dict[str, list[str]]):
    """Validate the complete capture without holding the layer in memory.

    Streams every segment body once from the capture directory, enforcing the
    exact segment tiling, per-page id slices, attribute schema, geometry
    validity and coordinate range, ascending OBJECTID order, and the pinned
    metadata contracts, while accumulating the whole-population digests. The
    three OBJECTID rosters are re-assembled from their persisted window
    evidence. Population digest pins are enforced when set and reported as
    pending otherwise (fails closed; see the module docstring).
    """
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name != "service_doc" and body is not None and not name.startswith("page_")
            and not _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)}
    # Sort segment names by their parsed numeric (offset, count), never
    # lexicographically: this layer's roster spans more than fifteen million
    # OBJECTIDs, so segment names mix six-, seven- and eight-digit offsets and
    # a lexicographic sort would mis-order them, falsely failing the tiling
    # proof.
    segments = sorted((_parse_segment_name(name) for name in captures if name.startswith("page_")))
    page_names = [_segment_name(offset, count) for offset, count in segments]
    window_names = [name for name in captures if _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {"service_doc", *window_names, *page_names}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    layer, fields_value = _validate_docs_and_service(captures, docs)
    ids = _validate_rosters(capture_root, docs, roster_windows)

    attr_names = [field["name"] for field in fields_value if field["name"] != "SHAPE"]
    accumulator = _SegmentAccumulator(attr_names)
    for (offset, count), name in zip(segments, page_names):
        page = json.loads((capture_root / f"{name}.body").read_bytes())
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error")
        _validate_spatial_reference(page.get("spatialReference"), f"source_page_{offset:06d}")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_missing_features")
        expected = ids[offset:offset + count]
        actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
        if actual != expected or len(rows) != len(expected):
            raise ValueError("source_page_id_slice_incomplete_or_reordered")
        if page.get("exceededTransferLimit") is True and offset + count >= EXPECTED_COUNT:
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
        accumulator.add_page(rows)
        del page, rows
    audited = accumulator.finish()
    if accumulator.feature_count != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if accumulator.feature_count != len(ids):
        raise ValueError("source_id_population_drift")
    if EXPECTED_RECORD_SHA256 is not None and audited["record_sha256"] != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if EXPECTED_GEOMETRY_SHA256 is not None and audited["geometry_sha256"] != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and audited["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    if EXPECTED_NULL_GEOMETRY_COUNT is not None and audited["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if EXPECTED_NAN_GEOMETRY_COUNT is not None and audited["nan_string_geometry_count"] != EXPECTED_NAN_GEOMETRY_COUNT:
        raise ValueError("source_nan_geometry_count_drift")
    pending = [name for name in _PENDING_PIN_NAMES if globals()[name] is None]
    audit = {"source_feature_count": accumulator.feature_count, "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": audited["record_sha256"],
             "source_geometry_sha256": audited["geometry_sha256"],
             "source_schema_sha256": EXPECTED_SCHEMA_SHA256, "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polyline", "null_geometry_count": audited["null_geometry_count"],
             "nan_string_geometry_count": audited["nan_string_geometry_count"],
             "observed_bounds": audited["observed_bounds"], "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    if pending:
        raise PopulationDigestPinsPending({**audit, "pending_pins": pending})
    return layer, fields_value, {"ids": ids, "audit": audit}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[str(field["type"])]


def _usable_paths(geom: object) -> list | None:
    if not isinstance(geom, dict) or set(geom) != {"paths"}:
        return None
    paths = geom.get("paths")
    if not isinstance(paths, list) or not paths:
        return None
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            return None
        for vertex in path:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                return None
    return paths


def _line_wkb(geom: object) -> bytes | None:
    paths = _usable_paths(geom)
    if paths is None:
        return None
    return (MultiLineString(paths) if len(paths) > 1 else LineString(paths[0])).wkb


def _parquet_schema(fields: list[dict[str, object]]) -> pa.Schema:
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "SHAPE" else _arrow_type(field)
        schema_fields.append(pa.field(name, typ, nullable=True))
    geo = {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": ["LineString", "MultiLineString"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
    return pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)})


def _write_parquet(fields: list[dict[str, object]], capture_root: Path, page_names: list[str],
                   ids: list[int], output_path: Path) -> dict[str, object]:
    """Write the candidate GeoParquet by streaming one segment at a time.

    The producer's complete attribute set and original NAD83 polyline paths
    are represented verbatim; this population cannot be materialized in
    memory, so each segment contributes one row group read from its captured
    body on disk, and the finished body is hashed from disk.
    """
    schema = _parquet_schema(fields)
    position = 0
    with pq.ParquetWriter(output_path, schema, compression="zstd") as writer:
        for name in page_names:
            page = json.loads((capture_root / f"{name}.body").read_bytes())
            rows = page["features"]
            expected = ids[position:position + len(rows)]
            actual = [r["attributes"]["OBJECTID"] for r in rows]
            if actual != expected:
                raise ValueError("source_page_id_slice_incomplete_or_reordered")
            position += len(rows)
            columns = {}
            for field in fields:
                fname = field["name"]
                if fname == "SHAPE":
                    values = [_line_wkb(row.get("geometry")) for row in rows]
                else:
                    values = [row["attributes"].get(fname) for row in rows]
                columns[fname] = pa.array(values, type=schema.field(fname).type)
            writer.write_table(pa.Table.from_pydict(columns, schema=schema))
            del page, rows, columns
    digest = hashlib.sha256()
    with output_path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return {"bytes": output_path.stat().st_size, "sha256": digest.hexdigest()}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_nfhl_flood_hazard_boundaries(storage: StagingStorageResource, *, version: str,
                                         capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Flood Hazard Boundaries candidate."""
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
    # An explicit capture_directory is reused verbatim so a restarted capture
    # resumes its completed segment evidence; without one, a fresh unique
    # directory keeps unrelated attempts isolated.
    if capture_directory:
        capture_root = Path(capture_directory).absolute()
    else:
        capture_root = (Path("/tmp/hifld-native-reconstruction/193/captures")
                        / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-flood-hazard-boundaries-draft"}) as client:
            stats_body, stats_receipt = _capture_with_retry(
                client, _stats_query(), capture_root, "ids_stats",
                max_bytes=DOC_CAPTURE_MAX_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            if _producer_error_doc(stats_body):
                raise ValueError("source_oid_stats_refused")
            stats_features = json.loads(stats_body).get("features")
            stats_attrs = stats_features[0].get("attributes") \
                if isinstance(stats_features, list) and stats_features and isinstance(stats_features[0], dict) else None
            stats_attrs = stats_attrs if isinstance(stats_attrs, dict) else {}
            min_oid, max_oid = stats_attrs.get("MIN_OBJECTID"), stats_attrs.get("MAX_OBJECTID")
            if type(min_oid) is not int or type(max_oid) is not int or max_oid < min_oid:
                raise ValueError("source_oid_stats_drift")
            captures["ids_stats"] = (stats_body, stats_receipt)
            ids = _capture_roster(client, capture_root, "seed", captures, min_oid, max_oid)
            if len(ids) != EXPECTED_COUNT:
                raise ValueError("source_seed_id_population_drift")
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            ids_before = _capture_roster(client, capture_root, "before", captures, min_oid, max_oid)
            captures.update(_capture_segments(client, capture_root, EXPECTED_COUNT))
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            ids_after = _capture_roster(client, capture_root, "after", captures, min_oid, max_oid)
        roster_windows = {stage: sorted(name for name in captures
                                        if _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)
                                        and _parse_roster_window_name(name)[0] == stage)
                          for stage in ("seed", "before", "after")}
        roster_windows = {f"ids_{stage}": names for stage, names in roster_windows.items()}
        layer, source_fields, extracted = _validate_captures(capture_root, captures, roster_windows)
    except PopulationDigestPinsPending as pending:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                        "staged": False, "error_code": "population_digest_pins_not_established",
                        "source_feature_count": pending.audit["source_feature_count"],
                        "source_id_sha256": pending.audit["source_id_sha256"],
                        "source_record_sha256": pending.audit["source_record_sha256"],
                        "source_geometry_sha256": pending.audit["source_geometry_sha256"],
                        "source_schema_sha256": pending.audit["source_schema_sha256"],
                        "source_null_geometry_count": pending.audit["null_geometry_count"],
                        "source_nan_string_geometry_count": pending.audit["nan_string_geometry_count"],
                        "source_observed_bounds": pending.audit["observed_bounds"],
                        "source_segment_count": pending.audit["source_segment_count"],
                        "pending_pins": pending.audit["pending_pins"],
                        "capture_directory": str(capture_root),
                        "receipts": receipts}) + b"\n")
        retained = {name: value for name, value in captures.items()
                    if not name.startswith("page_") and not _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)}
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained or name.startswith("page_") \
                    or _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name):
                # Segment bodies and roster windows are kept only in the
                # resumable capture directory; this failure is not a source
                # drift, so only the bounded document evidence is copied into
                # staging quarantine.
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            # Segment bodies are kept only in the resumable capture directory;
            # this failure is not a source drift, so only the bounded document
            # evidence is copied into staging quarantine.
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          "population_digest_pins_not_established")
        raise
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
        retained = {name: value for name, value in captures.items()
                    if not name.startswith("page_") and not _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name)}
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained or name.startswith("page_") \
                    or _ROSTER_WINDOW_NAME_PATTERN.fullmatch(name):
                # Segment bodies and roster windows are kept only in the
                # resumable capture directory; this failure is not a source
                # drift, so only the bounded document evidence is copied into
                # staging quarantine.
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained, type(exc).__name__)
        raise
    identity = _code_identity()
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    # Raw page bodies stay on disk in the resumable capture directory and are
    # staged from there one object at a time; this population cannot be
    # materialized in memory.
    parquet_path = capture_root / f"{DATASET_SLUG}.parquet"
    page_names = [_segment_name(offset, count) for offset, count in
                  sorted(_parse_segment_name(name) for name in captures
                         if _SEGMENT_NAME_PATTERN.fullmatch(name))]
    parquet_stat = _write_parquet(source_fields, capture_root, page_names, extracted["ids"], parquet_path)
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: dict(parquet_stat)}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Flood Hazard Boundaries (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 27 'Flood Hazard Boundaries', "
                       "captured completely from the producer's own REST publication. Complete source attributes "
                       "and NAD83 polyline geometry are represented in the candidate. The NFHL is continuously "
                       "updated and publishes no per-snapshot release date; freshness, rights, public schema/ID "
                       "contract, and release acceptance remain unresolved and unapproved.",
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
                                                             "SHAPE.STLength() double; SHAPE is the original "
                                                             "geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, FLD_LN_ID, GFID and GlobalID "
                                                          "values unchanged; no substitute IDs, casts, trimming, or "
                                                          "joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polyline paths retained as WKB "
                                                        "without projection; null source geometry retained as null; "
                                                        "producer string-'NaN' ordinate rows are retained as "
                                                        "records with null candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 27 'Flood Hazard Boundaries' of that "
                   "producer service. The HIFLD Open inventory lists this dataset as nfhl/flood-hazard-boundaries "
                   "with DataLumos project 239970; that ICPSR archive copy and the 2024-10-31 archive modification "
                   "date are identification and comparison context only, never pipeline inputs.",
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
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs: dict[str, bytes | Path] = {
        rel: parquet_path,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name in sorted(captures):
        outputs[f"raw/{name}.body"] = capture_root / f"{name}.body"
        outputs[f"raw/{name}.wire"] = capture_root / f"{name}.wire"
        outputs[f"raw/{name}.receipt.json"] = _canonical(captures[name][1]) + b"\n"
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
    descriptors: dict[str, dict[str, object]] = {}
    for name, value in outputs.items():
        key = f"{prefix}/{name}"
        if isinstance(value, Path):
            storage.upload_local_file(value, key)
        else:
            write_immutable(storage, key, value)
        snapshot = storage.object_snapshot(key)
        if snapshot is None:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
        descriptor = {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
        if isinstance(value, bytes) and (descriptor["bytes"] != len(value) or descriptor["sha256"] != _sha(value)):
            raise ValueError("candidate_output_physical_readback_failed:" + name)
        if descriptor["sha256"] is None:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
        descriptors[name] = descriptor
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
