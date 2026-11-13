"""Lossless, approval-gated capture of the FEMA NFHL "Subbasins" polygon layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 31 "Subbasins" as
the producer's own publication of this HIFLD inventory dataset
(``nfhl/subbasins``, DataLumos 239990 historical archive copy used for
identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

Scale and resumability: the producer reported 167,124 features for this layer
on 2026-11-13 (returnCountOnly, observed twice in agreement; the complete
whole-layer OBJECTID roster answered 167,124 unique integer ids over the span
1..613,918), and the layer's serialized polygon density is high (observed
2026-11-13: a 100-feature slice alone returned ~16 MB of JSON), so fixed
full-width pagination cannot serve the complete layer. The producer refuses
queries whose serialized response approaches its transfer cap with a complete
HTTP 200 Esri error document (observed 2026-11-13: whole-layer and
250-record slices of this layer refused, 100-record slices served), the same
mode dated on the sibling Hydrologic Reaches layer, so the capture runs as
adaptively bisected segments of the sorted OBJECTID roster starting at the
producer's maxRecordCount (2000): a segment the producer keeps refusing is
bisected until the producer serves both halves, and every served segment must
return exactly its expected roster slice, with the accepted segments tiling
the complete roster without gaps or overlaps. The producer also intermittently
answers valid requests with transient error documents or truncated
transports, so bounded retries re-issue the identical request and preserve
refused attempts as evidence; retries can never mask drift.

Every served roster segment is persisted under the capture directory with its
wire copy and hash-pinned receipt; a restarted capture re-verifies and reuses
each complete segment instead of refetching it, and never treats stale or
mismatched segment evidence as served. The capture still proves completeness
the same way as the sibling NFHL adapters: pinned count and id-roster digest,
exact segment tiling, per-segment id-slice equality, before/after
count/id/metadata brackets, and full-record digests.

Population digest pins (record, geometry, null/nan counts, unusable ring
counts, observed bounds) are established only by a first complete capture;
they are ``None`` in this draft until that capture completes. While they are
pending, the capture computes the digests from its own complete segment set,
records them in the capture manifest, and then fails closed with
``population_digest_pins_not_established`` so nothing is staged and no
candidate exists until the pins are filled from that dated evidence.

Producer polygon semantics follow the Esri JSON convention verified on the
sibling NFHL polygon adapters: every source ring is explicitly closed and
clockwise rings are exteriors while counter-clockwise rings are holes, so
candidate WKB geometry is built by that rule without dropping, adding, or
reordering any source vertex. Features whose rings cannot form a valid OGC
polygon (zero-area slivers, unassignable holes) are retained verbatim with
their candidate geometry explicitly gated to null and their failure modes
counted and pinned, never silently coerced.
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

DATASET_SLUG = "nfhl-subbasins"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 31
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Subbasins"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Dense
# polygon features (observed ~16 MB of JSON per 100-feature slice on
# 2026-11-13) make wide segments exceed the producer's transfer cap; such a
# segment is bisected until the producer serves both halves.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 64_000_000_000
# The producer's MapServer also intermittently answers valid requests with
# complete HTTP 200 Esri error documents or truncated transports (observed
# 2026-11-13 while probing this layer; identical requests succeed moments
# later). Every request retries a bounded number of times for that transient
# mode; a retry re-issues the exact same request, and completeness is still
# enforced by the pinned before/after id rosters, counts, exact segment
# tiling, and full-record digests, so retries can never mask drift. Segments
# wider than one record that keep failing are bisected (the producer's
# transfer cap); a single-record segment that keeps failing fails the capture
# explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Structural pins below are the observed 2026-11-13 producer contract,
# established by dated anonymous queries of the producer's own endpoints
# (returnCountOnly agreed twice, the complete whole-layer OBJECTID roster of
# 167,124 unique ids over 1..613,918, and the layer/service metadata
# documents); the module's own capture must reproduce every pin. The
# population digest pins (record, geometry, null/nan/unusable counts, observed
# bounds) are None until a first complete capture establishes them; see the
# module docstring. They fail closed, so the capture can never stage an
# unverified candidate.
EXPECTED_COUNT = 167124
EXPECTED_ID_SHA256 = "38f108c72ac38bd6437b5dacaaca81f59cf492d43307939f767233f25b1cacb0"
EXPECTED_SCHEMA_SHA256 = "f8c42f149476e34212d0c3b7ac3c8419acb5d5597a4a9f3d56687d4e6b401630"
EXPECTED_RECORD_SHA256: str | None = None
EXPECTED_GEOMETRY_SHA256: str | None = None
EXPECTED_OBSERVED_BOUNDS: dict[str, float] | None = None
EXPECTED_NULL_GEOMETRY_COUNT: int | None = None
EXPECTED_NAN_GEOMETRY_COUNT: int | None = None
EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS: dict[str, int] | None = None
EXPECTED_LAYER_EXTENT = {"xmin": -159.78813569214097, "ymin": 21.077081185895054,
                         "xmax": -66.98807377888954, "ymax": 64.84158943029774}
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

_PENDING_PIN_NAMES = ("EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256", "EXPECTED_OBSERVED_BOUNDS",
                      "EXPECTED_NULL_GEOMETRY_COUNT", "EXPECTED_NAN_GEOMETRY_COUNT",
                      "EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS")


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


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6,})_([0-9]{5,})$")


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


def _capture_segments(client: httpx.Client, capture_root: Path, ids: list[int], stream) \
        -> dict[str, tuple[None, dict[str, object]]]:
    """Capture the complete layer as adaptively bisected, resumable roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the
    refused segment's final body and receipts stay on disk as evidence under
    its own name. A single-record segment that still fails ends the capture
    explicitly. A complete, digest-verified segment left on disk by an earlier
    attempt is reused without refetching so a restarted capture resumes
    instead of repeating completed downloads. Every served or reused segment
    is streamed through ``stream`` at its roster position, so the digests,
    quality gates and candidate parquet are built from the identical segment
    evidence that completeness is proven over.

    Segment bodies live on disk in the resumable capture directory and are
    read back one segment at a time; this population's bodies cannot be held
    in memory at once.
    """
    captures: dict[str, tuple[None, dict[str, object]]] = {}
    total = len(ids)
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
            stream.process(json.loads((capture_root / f"{name}.body").read_bytes()), ids, offset, count)
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
        stream.process(json.loads(body), ids, offset, count)
        del body
    return captures


def _validated_rings(geometry: object) -> list | None:
    """Validate one feature's ring geometry, returning the source rings verbatim."""
    if geometry is None:
        return None
    if not isinstance(geometry, dict) or set(geometry) != {"rings"} or not isinstance(geometry["rings"], list) \
            or not geometry["rings"]:
        raise ValueError("source_polygon_geometry_invalid")
    for ring in geometry["rings"]:
        if not isinstance(ring, list) or not ring:
            raise ValueError("source_polygon_geometry_invalid")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise ValueError("source_polygon_geometry_invalid")
            x, y = vertex
            if type(x) in (int, float) and type(y) in (int, float):
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError("source_coordinate_range_drift")
            elif not (x == "NaN" and y == "NaN"):
                # String-"NaN" ordinates (seen on FEMA's sibling NFHL layers)
                # are retained verbatim in the record/raw evidence and gated
                # as unusable geometry, never silently coerced to a location.
                raise ValueError("source_polygon_geometry_invalid")
    return geometry["rings"]


def _has_nan_vertex(geometry: object) -> bool:
    return any(vertex[0] == "NaN" and vertex[1] == "NaN"
               for ring in geometry["rings"] for vertex in ring)


def _geo_metadata() -> dict:
    return {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[str(field["type"])]


UNUSABLE_GEOMETRY_MODES = frozenset({
    "source_polygon_ring_degenerate", "source_polygon_exterior_missing",
    "source_polygon_ring_self_intersecting", "source_polygon_hole_unassigned",
})


class _StreamDigest:
    """Incremental digest of a canonical JSON array without materializing it."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._started = False
        self._closed = False

    def update(self, items: list) -> None:
        if self._closed:
            raise AssertionError("stream_digest_update_after_close")
        for item in items:
            if self._started:
                self._digest.update(b",")
            else:
                self._digest.update(b"[")
                self._started = True
            self._digest.update(_canonical(item))

    def hexdigest(self) -> str:
        if self._closed:
            return self._digest.hexdigest()
        if self._started:
            self._digest.update(b"]")
        else:
            self._digest.update(b"[]")
        self._closed = True
        return self._digest.hexdigest()


class _SegmentStream:
    """Streams served segments through validation, digests and candidate parquet.

    The producer serves segments in ascending OBJECTID order (bisected halves
    keep their roster position) and every segment's id slice is enforced
    against the pinned sorted roster, so concatenating the segment streams
    reproduces the record order the pins are computed over without holding
    the layer in memory. A single pass writes the candidate parquet while the
    digests and quality gates are accumulated.
    """

    def __init__(self, fields: list[dict[str, object]], parquet_path: Path) -> None:
        self.attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
        self.schema = pa.schema([pa.field(field["name"],
                                          pa.binary() if field["name"] == "SHAPE"
                                          else _arrow_type(field),
                                          nullable=True) for field in fields],
                                metadata={b"geo": _canonical(_geo_metadata())})
        self.parquet_path = parquet_path
        self._writer = pq.ParquetWriter(str(parquet_path), self.schema, compression="zstd")
        self.record_digest = _StreamDigest()
        self.geometry_digest = _StreamDigest()
        self.null_geometry = 0
        self.nan_geometry = 0
        self.unusable_geometry: dict[str, int] = {}
        self.feature_count = 0
        # Running extrema only: retaining every ordinate of the complete layer
        # would hold far more memory than any late failure justifies.
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False
        self.last_oid: int | None = None

    def process(self, page: object, ids: list[int], offset: int, count: int) -> None:
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
        if page.get("exceededTransferLimit") is True and offset + count >= len(ids):
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
        geometry_records = []
        columns = {name: [] for name in self.attr_names}
        shapes = []
        for feature in rows:
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != self.attr_names:
                raise ValueError("source_record_attribute_schema_drift")
            oid = attrs["OBJECTID"]
            if self.last_oid is not None and type(oid) is int and oid <= self.last_oid:
                raise ValueError("source_page_id_slice_incomplete_or_reordered")
            self.last_oid = oid
            geometry_records.append([oid, _validated_rings(feature.get("geometry"))])
            for name in self.attr_names:
                columns[name].append(attrs.get(name))
            try:
                shapes.append(_polygon_wkb(feature.get("geometry")))
            except ValueError as exc:
                if str(exc) not in UNUSABLE_GEOMETRY_MODES:
                    raise
                # The producer may publish features whose rings cannot form a
                # valid OGC polygon (zero/signed-zero area slivers,
                # unassignable holes). The row and its raw rings are retained
                # verbatim; the candidate geometry is explicitly gated to null
                # and the failure mode is counted and pinned, never silently
                # coerced into an invented polygon.
                shapes.append(None)
                self.unusable_geometry[str(exc)] = self.unusable_geometry.get(str(exc), 0) + 1
            geometry = feature.get("geometry")
            if geometry is None:
                self.null_geometry += 1
            elif _has_nan_vertex(geometry):
                self.nan_geometry += 1
            else:
                for ring in geometry["rings"]:
                    for vertex in ring:
                        if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                            x, y = vertex
                            if self._xmin is None or x < self._xmin:
                                self._xmin = x
                            if self._ymin is None or y < self._ymin:
                                self._ymin = y
                            if self._xmax is None or x > self._xmax:
                                self._xmax = x
                            if self._ymax is None or y > self._ymax:
                                self._ymax = y
        self.feature_count += len(rows)
        self.record_digest.update(rows)
        self.geometry_digest.update(geometry_records)
        self._writer.write_table(pa.Table.from_pydict(
            {**columns, "SHAPE": pa.array(shapes, type=pa.binary())}, schema=self.schema))

    def observed_bounds(self) -> dict[str, float] | None:
        if self._xmin is None:
            return None
        return {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax, "ymax": self._ymax}

    def close_writer(self) -> None:
        """Finalize the candidate parquet once, whether or not the capture succeeds."""
        if not self._writer_closed:
            self._writer_closed = True
            self._writer.close()

    def finish(self) -> dict[str, object]:
        self.close_writer()
        return {"source_feature_count": self.feature_count,
                "source_record_sha256": self.record_digest.hexdigest(),
                "source_geometry_sha256": self.geometry_digest.hexdigest(),
                "geometry_type": "Polygon",
                "null_geometry_count": self.null_geometry,
                "nan_string_geometry_count": self.nan_geometry,
                "unusable_geometry_feature_counts": dict(sorted(self.unusable_geometry.items())),
                "observed_bounds": self.observed_bounds()}


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

    Esri JSON (verified on the sibling NFHL polygon adapters): every ring is
    explicitly closed, clockwise rings are exteriors, counter-clockwise rings
    are holes. Vertices are passed to shapely verbatim; exteriors with no
    claimable hole keep their ring order and holes join their smallest
    containing exterior.
    """
    rings = _usable_rings(geom)
    if rings is None:
        return None
    exteriors, holes = [], []
    for ring in rings:
        area = _signed_area(ring)
        if area == 0:
            raise ValueError("source_polygon_ring_degenerate")
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise ValueError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    for shell, _, _ in shells:
        if not shell.is_valid:
            raise ValueError("source_polygon_ring_self_intersecting")
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
        raise ValueError("exact_nfhl_subbasins_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_nfhl_subbasins_layer_metadata_drift")
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


def _validate_rosters(captures: dict[str, tuple[bytes | None, dict[str, object]]]) -> list[int]:
    rosters = []
    for name in ("ids_seed", "ids_before", "ids_after"):
        roster = captures[name][0]
        doc = json.loads(roster)
        ids = doc.get("objectIds") if isinstance(doc, dict) else None
        if (not isinstance(ids, list) or len(ids) != EXPECTED_COUNT
                or any(type(value) is not int for value in ids) or len(set(ids)) != EXPECTED_COUNT
                or (isinstance(doc, dict) and doc.get("objectIdFieldName") != "OBJECTID")):
            raise ValueError("source_id_population_drift")
        rosters.append(sorted(ids))
    if rosters[1] != rosters[0] or rosters[2] != rosters[0]:
        raise ValueError("source_id_population_drift")
    if _sha(_canonical(rosters[0])) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    return rosters[0]


def _validate_captures(captures: dict[str, tuple[bytes | None, dict[str, object]]], audit: dict[str, object]):
    """Validate the capture brackets without holding the layer in memory.

    The segment stream has already validated every segment body, id slice,
    attribute schema, ring geometry and coordinate range in ascending order
    while accumulating the whole-population digests and building the candidate
    parquet. This pass enforces the exact segment tiling over the pinned
    roster, the documented metadata contracts, and the population pins.
    """
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name != "service_doc" and body is not None and not name.startswith("page_")}
    segments = sorted((_parse_segment_name(name) for name in captures if name.startswith("page_")))
    page_names = [_segment_name(offset, count) for offset, count in segments]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    expected_names = {"service_doc", "ids_seed", "ids_before", "ids_after", *docs, *page_names}
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    layer, fields_value = _validate_docs_and_service(captures, docs)
    ids = _validate_rosters(captures)
    if audit["source_feature_count"] != EXPECTED_COUNT or len(ids) != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if EXPECTED_RECORD_SHA256 is not None and audit["source_record_sha256"] != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if EXPECTED_GEOMETRY_SHA256 is not None and audit["source_geometry_sha256"] != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if EXPECTED_NULL_GEOMETRY_COUNT is not None and audit["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if EXPECTED_NAN_GEOMETRY_COUNT is not None and audit["nan_string_geometry_count"] != EXPECTED_NAN_GEOMETRY_COUNT:
        raise ValueError("source_nan_geometry_count_drift")
    if EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS is not None \
            and audit["unusable_geometry_feature_counts"] != EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS:
        raise ValueError("source_unusable_geometry_count_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and audit["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    audit = {**audit, "source_id_sha256": _sha(_canonical(ids)), "source_schema_sha256": EXPECTED_SCHEMA_SHA256,
             "source_crs_epsg": SOURCE_CRS_WKID, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    pending = [name for name in _PENDING_PIN_NAMES if globals()[name] is None]
    if pending:
        raise PopulationDigestPinsPending({**audit, "pending_pins": pending})
    return layer, fields_value, {"ids": ids, "audit": audit}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_nfhl_subbasins(storage: StagingStorageResource, *, version: str,
                           capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Subbasins candidate."""
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
        capture_root = (Path("/tmp/hifld-native-reconstruction/213/captures")
                        / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes | None, dict[str, object]]] = {}
    stream = None
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-subbasins-draft"}) as client:
            ids_seed_body, ids_seed_receipt = _capture_with_retry(
                client, _ids_url(), capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            if _producer_error_doc(ids_seed_body):
                raise ValueError("source_ids_roster_refused")
            captures["ids_seed"] = (ids_seed_body, ids_seed_receipt)
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            ids_before_body, ids_before_receipt = _capture_with_retry(
                client, _ids_url(), capture_root, "ids_before", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            if _producer_error_doc(ids_before_body):
                raise ValueError("source_ids_roster_refused")
            captures["ids_before"] = (ids_before_body, ids_before_receipt)
            ids = _validate_rosters({"ids_seed": captures["ids_seed"], "ids_before": captures["ids_before"],
                                     "ids_after": captures["ids_before"]})
            layer = json.loads(captures["layer_before"][0])
            source_fields = layer["fields"]
            parquet_path = capture_root / f"{DATASET_SLUG}.parquet"
            stream = _SegmentStream(source_fields, parquet_path)
            captures.update(_capture_segments(client, capture_root, ids, stream))
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            ids_after_body, ids_after_receipt = _capture_with_retry(
                client, _ids_url(), capture_root, "ids_after", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            if _producer_error_doc(ids_after_body):
                raise ValueError("source_ids_roster_refused")
            captures["ids_after"] = (ids_after_body, ids_after_receipt)
        extracted_audit = stream.finish()
        if extracted_audit["source_feature_count"] != len(ids):
            raise ValueError("source_count_drift")
        layer, source_fields, extracted = _validate_captures(captures, extracted_audit)
    except PopulationDigestPinsPending as pending:
        stream_close(stream)
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
                        "source_unusable_geometry_feature_counts":
                            pending.audit["unusable_geometry_feature_counts"],
                        "source_observed_bounds": pending.audit["observed_bounds"],
                        "source_segment_count": pending.audit["source_segment_count"],
                        "pending_pins": pending.audit["pending_pins"],
                        "capture_directory": str(capture_root),
                        "receipts": receipts}) + b"\n")
        retained = {name: value for name, value in captures.items()
                    if not name.startswith("page_")}
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained or name.startswith("page_"):
                # Segment bodies are kept only in the resumable capture
                # directory; this failure is not a source drift, so only the
                # bounded document evidence is copied into staging quarantine.
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          "population_digest_pins_not_established")
        raise
    except Exception as exc:
        stream_close(stream)
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
                    if not name.startswith("page_")}
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained or name.startswith("page_"):
                # Segment bodies are kept only in the resumable capture
                # directory; bounded document evidence is copied into staging
                # quarantine one capture at a time.
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
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    parquet_digest = hashlib.sha256()
    with parquet_path.open("rb") as stream_handle:
        for block in iter(lambda: stream_handle.read(1 << 20), b""):
            parquet_digest.update(block)
    source_objects = {rel: {"bytes": parquet_path.stat().st_size, "sha256": parquet_digest.hexdigest()}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Subbasins (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 31 'Subbasins', captured "
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
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's derived "
                                                             "SHAPE.STArea() and SHAPE.STLength() doubles; SHAPE is "
                                                             "the original geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, SUBBAS_ID, GFID and GlobalID "
                                                          "values unchanged; no substitute IDs, casts, trimming, or "
                                                          "joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polygon rings retained as WKB "
                                                        "without projection under the producer's verified Esri "
                                                        "ring semantics (closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes); no vertex dropped, added, or "
                                                        "reordered; null source geometry and producer "
                                                        "string-'NaN' ordinate rows retained as records with null "
                                                        "candidate geometry, raw evidence intact; unusable ring "
                                                        "semantics gated to null candidate geometry with pinned "
                                                        "failure modes",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 31 'Subbasins' of that producer "
                   "service. The HIFLD Open inventory lists this dataset as nfhl/subbasins with DataLumos "
                   "project 239990; that ICPSR archive copy and the 2024-10-31 archive modification date are "
                   "identification and comparison context only, never pipeline inputs.",
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
        receipt = captures[name][1]
        if name.startswith("page_"):
            outputs[f"raw/{name}.body"] = capture_root / f"{name}.body"
            outputs[f"raw/{name}.wire"] = capture_root / f"{name}.wire"
            outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
        else:
            outputs[f"raw/{name}.body"] = captures[name][0]
            outputs[f"raw/{name}.wire"] = capture_root / f"{name}.wire"
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
                "source_unusable_geometry_feature_counts": audit["unusable_geometry_feature_counts"],
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
            "source_unusable_geometry_feature_counts": audit["unusable_geometry_feature_counts"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def stream_close(stream) -> None:
    """Best-effort finalization of the abandoned parquet writer.

    Quarantine and the original drift error must survive even if the writer
    cannot be closed; this never masks the failure that caused it.
    """
    if stream is None:
        return
    try:
        stream.close_writer()
    except Exception:
        pass
