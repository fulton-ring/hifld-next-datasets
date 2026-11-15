"""Shared FEMA NFHL capture, replay and candidate machinery.

The NFHL MapServer layer adapters in this package are separate datasets from
FEMA's National Flood Hazard Layer service, but they repeat the same
acquisition path: a pinned OBJECTID roster, before/after count/id/metadata
brackets, exact page or segment tiling of that roster, bounded retries for
the producer's transient complete-HTTP-200 Esri error documents and
incomplete transports, hash-pinned per-request receipts, replay (resumable
reuse) of digest-verified segment evidence, population digest pins, and the
identical unapproved-candidate packaging and approval gates.

This module is that shared path. Each layer adapter stays a thin entry point
that passes its own layer URL/id, geometry family, schema/population pins,
capture budgets and provenance text; nothing here relaxes a per-layer
contract, and every adapter keeps its own asset, checks and approval gates.
Publication stays blocked pending the runtime approval flags, which remain
false everywhere.

The only hosts callers of this module fetch are FEMA's own publication
(https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer and
FEMA's MSC documentation page); this module performs no fetches itself.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

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
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

PUBLISHER = "Federal Emergency Management Agency (FEMA)"
AGENCY = "Federal Emergency Management Agency"
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
SOURCE_CRS_WKID = 4269
MAX_RECORD_COUNT = 2000
# The producer's MapServer also intermittently answers valid requests with
# complete HTTP 200 Esri error documents or truncated transports (observed on
# the NFHL layers while paging them; the identical request succeeds moments
# later). Every request retries a bounded number of times for that transient
# mode; a retry re-issues the exact same request, and completeness is still
# enforced by the pinned before/after id rosters, counts, exact page/segment
# tiling, and full-record digests, so retries can never mask drift. Segments
# wider than one record that keep failing are bisected (the producer's
# transfer cap); a single-record segment that keeps failing fails the capture
# explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0

RELEASE_DATE_UNKNOWN_REASON = (
    "FEMA updates the NFHL continuously and publishes no release date for this layer; "
    "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates.")
INPUT_RELEASE_DATE_UNKNOWN_REASON = ("The NFHL is continuously updated; the service and its "
                                     "documentation page publish no per-snapshot release date.")
INPUT_LICENSE_NOTE = "Not stated by the captured endpoints; rights review pending."
INPUT_ATTRIBUTION_NOTE = ("FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                          "review.")
CANDIDATE_STATUS_NOTE = ("unapproved; publication blocked pending freshness, "
                         "schema/ID contract, attribution and release "
                         "acceptance")
PARQUET_POLICY_REASON = ("Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                         "producer CRS; all release and compatibility approvals remain pending.")
UNSUPPORTED_OUTPUT_FORMATS = {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                              "geopackage": "No approved source-to-public schema mapping.",
                              "pmtiles": "No display transformation or generalized geometry approved."}
LINEAGE_PREFIX = (
    "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-"
    "hazard-layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
    "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
    "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
    "'service_doc' evidence. ")


def producer_error_doc(body: bytes) -> bool:
    """True when a captured body is a complete HTTP-200 Esri error document."""
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       max_bytes: int, attempts: int, base_delay: float,
                       capture_get: Callable = capture_get) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster. The
    final attempt's body is always returned as captured, so a persistent
    producer failure surfaces to the caller unchanged. ``capture_get`` is the
    single-request primitive (the ``epa_frs_common`` receipt helper); callers
    pass their own module-level reference so test patching keeps working. An
    ``attempts`` budget of 1 never retries and behaves exactly like one
    ``capture_get`` call.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if producer_error_doc(body) and attempt < attempts:
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


def streamed_list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``.

    Byte-identical to ``_sha(_canonical(items))`` without materializing the
    complete document, which for the larger NFHL layers is far larger than
    agent memory should ever hold twice.
    """
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


class StreamDigest:
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


def plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def query_url(layer_url: str, params: dict[str, object]) -> str:
    return layer_url + "/query?" + urlencode(params)


def count_query_url(layer_url: str) -> str:
    return query_url(layer_url, {"where": "1=1", "returnCountOnly": "true", "f": "json"})


def ids_query_url(layer_url: str) -> str:
    return query_url(layer_url, {"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6,})_([0-9]{5,})$")


def parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def segment_query_url(layer_url: str, offset: int, count: int) -> str:
    return query_url(layer_url, {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                                 "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                                 "resultRecordCount": count, "f": "json"})


def fixed_page_name(index: int) -> str:
    return f"page_{index:03d}"


def fixed_page_query_url(layer_url: str, index: int, page_size: int) -> str:
    return query_url(layer_url, {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                                 "orderByFields": "OBJECTID ASC", "resultOffset": index * page_size,
                                 "resultRecordCount": page_size, "f": "json"})


def doc_urls(service_url: str, service_doc_url: str, layer_url: str, *,
             include_ids: bool = True) -> list[tuple[str, str]]:
    urls = [
        ("service_doc", service_doc_url),
        ("service_before", service_url + "?f=pjson"),
        ("layer_before", layer_url + "?f=pjson"),
        ("count_before", count_query_url(layer_url)),
    ]
    if include_ids:
        urls.append(("ids_before", ids_query_url(layer_url)))
    return urls


def after_doc_urls(service_url: str, layer_url: str, *, include_ids: bool = True) -> list[tuple[str, str]]:
    urls = [
        ("layer_after", layer_url + "?f=pjson"),
        ("service_after", service_url + "?f=pjson"),
        ("count_after", count_query_url(layer_url)),
    ]
    if include_ids:
        urls.append(("ids_after", ids_query_url(layer_url)))
    return urls


def fixed_page_urls(service_url: str, service_doc_url: str, layer_url: str,
                    page_size: int, count: int) -> list[tuple[str, str]]:
    """The fixed-pagination capture plan: bracketing docs, all pages, after docs."""
    urls = doc_urls(service_url, service_doc_url, layer_url, include_ids=True)
    page_count = (count + page_size - 1) // page_size
    urls.extend((fixed_page_name(i), fixed_page_query_url(layer_url, i, page_size))
                for i in range(page_count))
    urls.extend(after_doc_urls(service_url, layer_url, include_ids=True))
    return urls


def validate_spatial_reference(value: object, label: str, wkid: int = SOURCE_CRS_WKID) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        observed = value.get(key)
        if observed is not None and observed != wkid:
            raise ValueError(label + "_crs_drift")


@dataclass(frozen=True)
class LayerContract:
    """Explicit per-layer capture contract. Nothing defaults to a permissive mode.

    ``mode`` selects the acquisition plan the layer's pinned capture used:
    ``"fixed"`` walks resultOffset pages of the pinned width; ``"bisect"``
    starts at ``segment_start_records`` and adaptively bisects segments the
    producer refuses. ``resumable`` (replay) reuses an explicit capture
    directory verbatim, streams served segments through validation, digests
    and the candidate parquet, and reuses digest-verified segment evidence
    left by an earlier attempt.
    """

    dataset_slug: str
    file_slug: str
    layer_id: int
    layer_name: str
    geometry_family: str          # "point" | "polyline" | "polygon"
    esri_geometry_type: str       # the producer's declared geometryType
    mode: str                     # "fixed" | "bisect"
    max_capture_bytes: int
    min_free_bytes: int
    reconstruction_dir: str
    service_url: str = SERVICE_URL
    service_doc_url: str = SERVICE_DOC_URL
    crs_wkid: int = SOURCE_CRS_WKID
    entity_kind: str = "layer"    # "layer" | "table" (the exact-<kind> identity codes)
    pending_pin_names: tuple[tuple[str, str], ...] = ()  # (module pin constant, PopulationPins field)
    page_size: int = MAX_RECORD_COUNT
    segment_start_records: int = MAX_RECORD_COUNT
    doc_attempts: int = DOC_CAPTURE_ATTEMPTS
    doc_base_delay: float = DOC_CAPTURE_BASE_DELAY_SECONDS
    arrow_type_overrides: Mapping[str, pa.DataType] | None = None  # per-layer physical-type pins
    page_attempts: int | None = None       # fixed mode pages; None -> doc budgets
    page_base_delay: float | None = None
    segment_attempts: int = SEGMENT_PROBE_ATTEMPTS
    segment_base_delay: float = SEGMENT_PROBE_BASE_DELAY_SECONDS
    client_timeout: float = 180.0
    gated_rings: bool = False     # polygon: count unusable ring modes, gate candidate geometry to null
    resumable: bool = False       # replay: capture_directory reuse, streaming, replay segment reuse

    @property
    def layer_url(self) -> str:
        return self.service_url + "/" + str(self.layer_id)

    def user_agent(self) -> str:
        return f"hifld-next-datasets/{self.dataset_slug}-draft"


@dataclass(frozen=True)
class PopulationPins:
    """The observed complete-layer population contract a capture must reproduce.

    ``None`` digest pins mean "established only by a first complete capture";
    such a capture computes the digests from its own complete segment set,
    records them in the capture manifest, and then fails closed with
    ``PopulationDigestPinsPending`` so nothing is staged and no candidate
    exists until the pins are filled from that dated evidence.
    """

    count: int
    id_sha256: str
    schema_sha256: str
    record_sha256: str | None
    geometry_sha256: str | None
    observed_bounds: dict[str, float] | None
    null_geometry_count: int | None = None
    nan_geometry_count: int | None = None
    unusable_geometry_feature_counts: dict[str, int] | None = None
    layer_extent: dict[str, float] | None = None
    service_layer_roster: list | None = None


PENDING_PIN_NAMES = (("EXPECTED_RECORD_SHA256", "record_sha256"),
                     ("EXPECTED_GEOMETRY_SHA256", "geometry_sha256"),
                     ("EXPECTED_OBSERVED_BOUNDS", "observed_bounds"),
                     ("EXPECTED_NULL_GEOMETRY_COUNT", "null_geometry_count"),
                     ("EXPECTED_NAN_GEOMETRY_COUNT", "nan_geometry_count"),
                     ("EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS", "unusable_geometry_feature_counts"))


class PopulationDigestPinsPending(ValueError):
    """The complete segment set validated, but population digest pins are not set.

    Carries the digests computed from this capture so the capture manifest can
    record them as dated evidence for filling the module pins. Nothing may be
    staged while this is pending.
    """

    def __init__(self, audit: dict[str, object]):
        super().__init__("population_digest_pins_not_established")
        self.audit = audit


UNUSABLE_GEOMETRY_MODES = frozenset({
    "source_polygon_ring_degenerate", "source_polygon_exterior_missing",
    "source_polygon_ring_self_intersecting", "source_polygon_hole_unassigned",
})


def validate_segment_page(page: object, ids: list[int], offset: int, count: int, *,
                          wkid: int, total: int, exceeded_rule: str,
                          label: str | None = None) -> list:
    """Validate one served page/segment and return its feature rows.

    ``exceeded_rule`` reproduces the two observed producer behaviours, which
    are part of the layer pins: bisected segments (``"bisect"``) may claim
    ``exceededTransferLimit`` anywhere short of the roster end, while fixed
    pagination pages (``"fixed"``) may claim it only when the page is full and
    not final. ``label`` is the per-page CRS label used in drift error codes;
    bisect segments name themselves by roster offset (six digits), fixed pages
    by page index (three digits), exactly as each layer's pins were captured.
    """
    label = label if label is not None else f"source_page_{offset:06d}"
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    validate_spatial_reference(page.get("spatialReference"), label, wkid)
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    expected = ids[offset:offset + count]
    actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
    if actual != expected or len(rows) != len(expected):
        raise ValueError("source_page_id_slice_incomplete_or_reordered")
    exceeded = page.get("exceededTransferLimit") is True
    if exceeded_rule == "bisect":
        if exceeded and offset + count >= total:
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    elif exceeded_rule == "fixed":
        if exceeded and not (len(expected) == count and offset + count < total):
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    else:
        raise ValueError("unknown_exceeded_transfer_limit_rule")
    return rows


def reuse_existing_segment(capture_root: Path, name: str, url: str) -> dict[str, object] | None:
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
    if not usable or producer_error_doc(body):
        for suffix in (".body", ".wire", ".receipt.json"):
            path = capture_root / f"{name}{suffix}"
            if path.exists():
                path.rename(capture_root / f"{name}.stale{suffix}")
        return None
    return receipt


def capture_bisect_segments(client: httpx.Client, contract: LayerContract, capture_root: Path,
                            total: int, capture_get: Callable, stream=None,
                            ids: list[int] | None = None) \
        -> dict[str, tuple[bytes | None, dict[str, object]]]:
    """Capture the complete layer as adaptively bisected roster segments.

    Segments start at ``segment_start_records``. A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the
    refused segment's final body and receipts stay on disk as evidence under
    its own name. A single-record segment that still fails ends the capture
    explicitly. In resumable (streaming) mode a complete, digest-verified
    segment left on disk by an earlier attempt is reused without refetching so
    a restarted capture resumes instead of repeating completed downloads, and
    every served or reused segment is streamed through ``stream`` at its
    roster position, so the digests, quality gates and candidate parquet are
    built from the identical segment evidence that completeness is proven
    over; segment bodies live on disk in the resumable capture directory and
    are read back one segment at a time, with ``None`` standing in for the
    body in the returned captures. Without ``stream`` the segment bodies are
    returned in memory.
    """
    captures: dict[str, tuple[bytes | None, dict[str, object]]] = {}
    segments: deque[tuple[int, int]] = deque(
        (offset, min(contract.segment_start_records, total - offset))
        for offset in range(0, total, contract.segment_start_records)
    )
    while segments:
        offset, count = segments.popleft()
        name = segment_name(offset, count)
        url = segment_query_url(contract.layer_url, offset, count)
        if stream is not None:
            reused = reuse_existing_segment(capture_root, name, url)
            if reused is not None:
                captures[name] = (None, reused)
                stream.process(json.loads((capture_root / f"{name}.body").read_bytes()), ids, offset, count)
                continue
        body, receipt = capture_with_retry(
            client, url, capture_root, name,
            max_bytes=contract.max_capture_bytes,
            attempts=contract.segment_attempts if count > 1 else contract.doc_attempts,
            base_delay=contract.segment_base_delay if count > 1 else contract.doc_base_delay,
            capture_get=capture_get)
        if producer_error_doc(body):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            segments.appendleft((offset + middle, count - middle))
            segments.appendleft((offset, middle))
            continue
        captures[name] = (None, receipt) if stream is not None else (body, receipt)
        if stream is not None:
            stream.process(json.loads(body), ids, offset, count)
            del body
    return captures


# The family-wide physical-type contract, verified against the producer's own
# JSON: Esri date fields arrive as epoch-millisecond integers and are kept
# verbatim as int64 (never reinterpreted or reformatted), and Esri integers
# arrive as JSON integers kept verbatim as int64. ``esriFieldTypeSingle`` is
# float32 only in the base_index layer's pinned map; other layers pass
# ``arrow_type_overrides`` on their contract if their pinned candidate bytes
# need a different physical type.
ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeSmallInteger": pa.int16(),
    "esriFieldTypeSingle": pa.float32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.int64(),
}


def arrow_type(field: Mapping[str, object], overrides: Mapping[str, pa.DataType] | None = None) -> pa.DataType:
    type_map = ARROW_TYPES if not overrides else {**ARROW_TYPES, **overrides}
    typ = type_map.get(str(field["type"]))
    if typ is None:
        raise ValueError("source_field_type_unsupported:" + str(field["type"]))
    return typ


def _usable_point(geom: object) -> bool:
    return isinstance(geom, dict) and type(geom.get("x")) in (int, float) and type(geom.get("y")) in (int, float)


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


def point_wkb(geom: object) -> bytes | None:
    return Point(geom["x"], geom["y"]).wkb if _usable_point(geom) else None


def line_wkb(geom: object) -> bytes | None:
    paths = _usable_paths(geom)
    if paths is None:
        return None
    return (MultiLineString(paths) if len(paths) > 1 else LineString(paths[0])).wkb


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        x1, y1 = vertex
        x2, y2 = ring[(index + 1) % len(ring)]
        area += x1 * y2 - x2 * y1
    return area / 2


def _usable_rings(geom: object) -> list | None:
    if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) \
            or not geom["rings"]:
        return None
    for ring in geom["rings"]:
        if not isinstance(ring, list) or not ring:
            return None
        for vertex in ring:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                return None
    return geom["rings"]


def polygon_wkb(geom: object) -> bytes | None:
    """WKB for one feature's rings under the producer's verified ring semantics.

    Esri JSON (verified on the NFHL polygon adapters): every ring is
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
        candidates = [(abs(area), index) for shell, area, index in shells
                      if shell.contains(polygon.representative_point())]
        if not candidates:
            raise ValueError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for shell, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def geometry_wkb(family: str, geom: object, *, gated_rings: bool = False) -> bytes | None:
    """Candidate WKB for one feature under its geometry family.

    Gated polygon layers count the producer's unusable ring semantics (zero
    area slivers, unassignable holes) and gate the candidate geometry to null
    instead of failing; plain layers propagate the refusal.
    """
    if family == "point":
        return point_wkb(geom)
    if family == "polyline":
        return line_wkb(geom)
    if family == "polygon":
        if gated_rings:
            try:
                return polygon_wkb(geom)
            except ValueError as exc:
                if str(exc) not in UNUSABLE_GEOMETRY_MODES:
                    raise
                return None
        return polygon_wkb(geom)
    raise ValueError("unknown_geometry_family:" + family)


def geo_metadata(family: str, wkid: int = SOURCE_CRS_WKID) -> dict:
    geometry_types = {"point": ["Point"], "polyline": ["LineString", "MultiLineString"],
                      "polygon": ["Polygon", "MultiPolygon"]}[family]
    return {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": geometry_types,
        "crs": pyproj.CRS.from_epsg(wkid).to_json_dict(), "edges": "planar"}}}


def parquet_bytes(fields: list[dict[str, object]], features: list, family: str, *,
                  wkid: int = SOURCE_CRS_WKID, gated_rings: bool = False,
                  arrow_type_overrides: Mapping[str, pa.DataType] | None = None) -> bytes:
    """Serialize the captured features as the candidate GeoParquet bytes."""
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "SHAPE" else arrow_type(field, arrow_type_overrides)
        if name == "SHAPE":
            values = [geometry_wkb(family, row.get("geometry"), gated_rings=gated_rings) for row in features]
        else:
            values = [row["attributes"].get(name) for row in features]
        schema_fields.append(pa.field(name, typ, nullable=True))
        columns[name] = pa.array(values, type=typ)
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata(
        {b"geo": _canonical(geo_metadata(family, wkid))}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def validated_rings(geometry: object) -> list | None:
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


def has_nan_vertex(geometry: object) -> bool:
    return any(vertex[0] == "NaN" and vertex[1] == "NaN"
               for ring in geometry["rings"] for vertex in ring)


def _bounds_dict(observed_bounds: list[float]) -> dict[str, float] | None:
    if not observed_bounds:
        return None
    return {"xmin": min(observed_bounds[0::2]), "ymin": min(observed_bounds[1::2]),
            "xmax": max(observed_bounds[0::2]), "ymax": max(observed_bounds[1::2])}


def classify_geometries(features: list, attr_names: list[str], family: str, *,
                        wkid: int = SOURCE_CRS_WKID, gated_rings: bool = False) -> tuple:
    """Classify one family's geometries exactly as the layer adapters pin them.

    Returns ``(geometry_records, null_geometry, nan_geometry, bounds,
    unusable_geometry)``. The record shapes are part of the per-layer digest
    pins and are reproduced verbatim: points carry ``[oid, x, y]`` (with
    ``[oid, "NaN", "NaN"]`` for the producer's string-"NaN" ordinate rows and
    ``[oid, None, None]`` for null geometry), polylines carry
    ``[oid, paths]`` with ``[oid, None, None]`` for null geometry, and
    polygons carry ``[oid, rings]`` with ``[oid, None]`` for null geometry.
    Features whose ordinates include the producer's string-"NaN" contribute
    no observed bounds.
    """
    null_geometry = 0
    nan_geometry = 0
    geometry_records = []
    unusable_geometry: dict[str, int] = {}
    observed_bounds: list[float] = []
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        oid = attrs["OBJECTID"]
        geom = feature.get("geometry")
        if family == "point":
            if geom is None:
                null_geometry += 1
                geometry_records.append([oid, None, None])
            elif isinstance(geom, dict) and set(geom) == {"x", "y"} \
                    and all(isinstance(geom[k], str) and geom[k] == "NaN" for k in ("x", "y")):
                # FEMA publishes rows with Esri string-"NaN" ordinates; the
                # values are retained verbatim in the record/raw evidence and
                # gated as unusable geometry here, never silently coerced to
                # a location.
                nan_geometry += 1
                geometry_records.append([oid, geom["x"], geom["y"]])
            elif isinstance(geom, dict) and set(geom) == {"x", "y"} \
                    and all(type(geom[k]) in (int, float) for k in ("x", "y")):
                x, y = geom["x"], geom["y"]
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError("source_coordinate_range_drift")
                geometry_records.append([oid, x, y])
                observed_bounds.extend((x, y))
            else:
                raise ValueError("source_point_geometry_invalid")
            continue
        if family == "polyline":
            if geom is None:
                null_geometry += 1
                geometry_records.append([oid, None, None])
                continue
            if not isinstance(geom, dict) or set(geom) != {"paths"} or not isinstance(geom["paths"], list) \
                    or not geom["paths"]:
                raise ValueError("source_line_geometry_invalid")
            nan_paths = False
            feature_bounds: list[float] = []
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
                        feature_bounds.extend((x, y))
                    elif x == "NaN" and y == "NaN":
                        # FEMA's sibling NFHL point layer publishes string-"NaN"
                        # ordinates; such vertices are retained verbatim in the
                        # record/raw evidence and gated as unusable geometry here,
                        # never silently coerced to a location.
                        nan_paths = True
                    else:
                        raise ValueError("source_line_geometry_invalid")
            geometry_records.append([oid, geom["paths"]])
            if nan_paths:
                nan_geometry += 1
            else:
                observed_bounds.extend(feature_bounds)
            continue
        if family == "polygon":
            if geom is None:
                null_geometry += 1
                geometry_records.append([oid, None])
                continue
            rings = validated_rings(geom)
            geometry_records.append([oid, rings])
            if gated_rings:
                try:
                    polygon_wkb(geom)
                except ValueError as exc:
                    if str(exc) not in UNUSABLE_GEOMETRY_MODES:
                        raise
                    # The producer may publish features whose rings cannot
                    # form a valid OGC polygon. The row and its raw rings are
                    # retained verbatim; the failure mode is counted and
                    # pinned, never silently coerced into an invented polygon.
                    unusable_geometry[str(exc)] = unusable_geometry.get(str(exc), 0) + 1
            if has_nan_vertex(geom):
                nan_geometry += 1
            else:
                for ring in geom["rings"]:
                    for vertex in ring:
                        if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                            observed_bounds.extend((vertex[0], vertex[1]))
            continue
        raise ValueError("unknown_geometry_family:" + family)
    return geometry_records, null_geometry, nan_geometry, _bounds_dict(observed_bounds), unusable_geometry


class SegmentStream:
    """Streams served segments through validation, digests and candidate parquet.

    The producer serves segments in ascending OBJECTID order (bisected halves
    keep their roster position) and every segment's id slice is enforced
    against the pinned sorted roster, so concatenating the segment streams
    reproduces the record order the pins are computed over without holding
    the layer in memory. A single pass writes the candidate parquet while the
    digests and quality gates are accumulated.
    """

    def __init__(self, fields: list[dict[str, object]], parquet_path: Path, family: str, *,
                 wkid: int = SOURCE_CRS_WKID) -> None:
        self.family = family
        self.wkid = wkid
        self.attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
        self.schema = pa.schema([pa.field(field["name"],
                                          pa.binary() if field["name"] == "SHAPE"
                                          else arrow_type(field),
                                          nullable=True) for field in fields],
                                metadata={b"geo": _canonical(geo_metadata(family, wkid))})
        self.parquet_path = parquet_path
        self._writer = pq.ParquetWriter(str(parquet_path), self.schema, compression="zstd")
        self.record_digest = StreamDigest()
        self.geometry_digest = StreamDigest()
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
        rows = validate_segment_page(page, ids, offset, count, wkid=self.wkid,
                                     total=len(ids), exceeded_rule="bisect")
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
            geometry_records.append([oid, validated_rings(feature.get("geometry"))])
            for name in self.attr_names:
                columns[name].append(attrs.get(name))
            try:
                shapes.append(polygon_wkb(feature.get("geometry")))
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
            elif has_nan_vertex(geometry):
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
                "geometry_type": {"point": "Point", "polyline": "Polyline", "polygon": "Polygon"}[self.family],
                "null_geometry_count": self.null_geometry,
                "nan_string_geometry_count": self.nan_geometry,
                "unusable_geometry_feature_counts": dict(sorted(self.unusable_geometry.items())),
                "observed_bounds": self.observed_bounds()}


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


GEOMETRY_TYPE_NAMES = {"point": "Point", "polyline": "Polyline", "polygon": "Polygon"}


def _exact_layer_code(contract: LayerContract, suffix: str) -> str:
    entity = contract.dataset_slug.removeprefix("nfhl-").replace("-", "_")
    return f"exact_nfhl_{entity}_{contract.entity_kind}_{suffix}"


def _validate_service_and_layer(contract: LayerContract, pins: PopulationPins,
                                captures: dict, docs: dict) -> tuple[dict, list, str]:
    """Validate the captured service/layer documents against the layer contract."""
    if contract.service_url.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("fema_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != pins.service_layer_roster:
        raise ValueError("fema_nfhl_service_layer_roster_drift")
    if not any(x.get("id") == contract.layer_id and x.get("name") == contract.layer_name
               and x.get("type") == "Feature Layer" for x in service_layers):
        raise ValueError(_exact_layer_code(contract, "missing"))
    if (layer.get("id") != contract.layer_id or layer.get("name") != contract.layer_name
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != contract.esri_geometry_type):
        raise ValueError(_exact_layer_code(contract, "metadata_drift"))
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise ValueError("source_record_limit_drift")
    validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer", contract.crs_wkid)
    extent = layer.get("extent", {})
    validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent", contract.crs_wkid)
    if {key: extent.get(key) for key in pins.layer_extent} != pins.layer_extent:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != pins.schema_sha256:
        raise ValueError("source_schema_contract_drift")
    if docs["layer_after"].get("fields") is None \
            or [plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer:
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_before"].get("count") != pins.count or docs["count_after"].get("count") != pins.count:
        raise ValueError("source_count_drift")
    return layer, fields_value, schema_sha


def validate_rosters_classic(docs: dict, pins: PopulationPins) -> list[int]:
    """The in-memory roster check: brackets may arrive in any order.

    A wrong objectIdFieldName is attributed to ``source_id_field_changed``
    only after the population, equality and digest checks pass.
    """
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != pins.count
           or any(type(value) is not int for value in roster) or len(set(roster)) != pins.count
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != pins.id_sha256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    return ids


def validate_rosters_resumable(roster_captures: dict[str, tuple], pins: PopulationPins) -> list[int]:
    """The resumable roster check over explicit bracket captures.

    A wrong objectIdFieldName folds into ``source_id_population_drift``: these
    rosters are captured as first-class bracket requests, and a roster that
    does not answer with the producer's OBJECTID field is not the pinned
    population contract.
    """
    rosters = []
    for name in ("ids_seed", "ids_before", "ids_after"):
        doc = json.loads(roster_captures[name][0])
        ids = doc.get("objectIds") if isinstance(doc, dict) else None
        if (not isinstance(ids, list) or len(ids) != pins.count
                or any(type(value) is not int for value in ids) or len(set(ids)) != pins.count
                or (isinstance(doc, dict) and doc.get("objectIdFieldName") != "OBJECTID")):
            raise ValueError("source_id_population_drift")
        rosters.append(sorted(ids))
    if rosters[1] != rosters[0] or rosters[2] != rosters[0]:
        raise ValueError("source_id_population_drift")
    if _sha(_canonical(rosters[0])) != pins.id_sha256:
        raise ValueError("source_id_roster_drift")
    return rosters[0]


def _check_population_pins(pins: PopulationPins, audit: dict[str, object], *, resumable: bool) -> None:
    if pins.record_sha256 is not None and audit["source_record_sha256"] != pins.record_sha256:
        raise ValueError("source_record_value_drift")
    if pins.geometry_sha256 is not None and audit["source_geometry_sha256"] != pins.geometry_sha256:
        raise ValueError("source_geometry_or_coverage_drift")
    if resumable:
        if pins.null_geometry_count is not None \
                and audit["null_geometry_count"] != pins.null_geometry_count:
            raise ValueError("source_null_geometry_count_drift")
        if pins.nan_geometry_count is not None \
                and audit["nan_string_geometry_count"] != pins.nan_geometry_count:
            raise ValueError("source_nan_geometry_count_drift")
        if pins.unusable_geometry_feature_counts is not None \
                and audit["unusable_geometry_feature_counts"] != pins.unusable_geometry_feature_counts:
            raise ValueError("source_unusable_geometry_count_drift")
    if pins.observed_bounds is not None and audit["observed_bounds"] != pins.observed_bounds:
        raise ValueError("source_observed_coverage_drift")


def validate_in_memory_capture(contract: LayerContract, pins: PopulationPins,
                               captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple:
    """Validate a fully in-memory capture without resumable segment evidence.

    Covers the fixed-pagination and bisected modes whose segment bodies are
    held in the capture roster: page/segment tiling, per-page id slices,
    document brackets, roster population and digest pins, then the complete
    geometry classification and digest pins.
    """
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name != "service_doc" and not name.startswith("page_")}
    page_plans: list[tuple[int, int, str, str]] = []
    if contract.mode == "fixed":
        page_count = (pins.count + contract.page_size - 1) // contract.page_size
        expected_names = {"service_doc", "service_before", "layer_before", "count_before", "ids_before",
                          "ids_seed", "layer_after", "service_after", "count_after", "ids_after",
                          *(fixed_page_name(i) for i in range(page_count))}
        if set(captures) != expected_names:
            raise ValueError("source_capture_roster_incomplete_or_unexpected")
        page_plans = [(i * contract.page_size, contract.page_size, fixed_page_name(i),
                       f"source_page_{i:03d}") for i in range(page_count)]
    else:
        page_names = sorted(name for name in captures if name.startswith("page_"))
        segments = [parse_segment_name(name) for name in page_names]
        position = 0
        for offset, count in segments:
            if offset != position:
                raise ValueError("source_page_tiling_incomplete_or_overlapped")
            position += count
        if position != pins.count:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        if set(captures) != set(docs) | {"service_doc", *page_names}:
            raise ValueError("source_capture_roster_incomplete_or_unexpected")
        page_plans = [(offset, count, name, f"source_page_{offset:06d}")
                      for (offset, count), name in zip(segments, page_names)]
    layer, fields_value, schema_sha = _validate_service_and_layer(contract, pins, captures, docs)
    ids = validate_rosters_classic(docs, pins)
    features: list[dict[str, object]] = []
    for offset, count, name, label in page_plans:
        features.extend(validate_segment_page(
            json.loads(captures[name][0]), ids, offset, count, wkid=contract.crs_wkid,
            total=pins.count, exceeded_rule="bisect" if contract.mode == "bisect" else "fixed",
            label=label))
    attr_names = [field["name"] for field in fields_value if field["name"] != "SHAPE"]
    geometry_records, null_geometry, nan_geometry, bounds, unusable_geometry = classify_geometries(
        features, attr_names, contract.geometry_family, wkid=contract.crs_wkid,
        gated_rings=contract.gated_rings)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    geometry_records.sort(key=lambda row: row[0])
    record_sha = streamed_list_sha(features)
    geometry_sha = streamed_list_sha(geometry_records)
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": contract.crs_wkid,
             "geometry_type": GEOMETRY_TYPE_NAMES[contract.geometry_family],
             "null_geometry_count": null_geometry, "nan_string_geometry_count": nan_geometry,
             "observed_bounds": bounds, "declared_layer_extent": pins.layer_extent,
             "unique_source_id_count": len(ids), "source_release_date": None,
             "source_release_date_unknown_reason": RELEASE_DATE_UNKNOWN_REASON}
    if contract.mode == "bisect":
        audit["source_segment_count"] = len(page_plans)
    if contract.gated_rings:
        audit["unusable_geometry_feature_counts"] = dict(sorted(unusable_geometry.items()))
    _check_population_pins(pins, audit, resumable=False)
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


def validate_resumable_capture(contract: LayerContract, pins: PopulationPins,
                               captures: dict[str, tuple[bytes | None, dict[str, object]]],
                               stream_audit: dict[str, object]) -> tuple:
    """Validate a resumable capture without holding the layer in memory.

    The segment stream has already validated every segment body, id slice,
    attribute schema, ring geometry and coordinate range in ascending order
    while accumulating the whole-population digests and building the candidate
    parquet. This pass enforces the exact segment tiling over the pinned
    roster, the documented metadata contracts, and the population pins.
    """
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name != "service_doc" and body is not None and not name.startswith("page_")}
    segments = sorted(parse_segment_name(name) for name in captures if name.startswith("page_"))
    page_names = [segment_name(offset, count) for offset, count in segments]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != pins.count:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    expected_names = {"service_doc", "ids_seed", "ids_before", "ids_after", *docs, *page_names}
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    layer, fields_value, _schema_sha = _validate_service_and_layer(contract, pins, captures, docs)
    ids = validate_rosters_resumable(captures, pins)
    if stream_audit["source_feature_count"] != pins.count or len(ids) != pins.count:
        raise ValueError("source_count_drift")
    audit = {**stream_audit, "source_id_sha256": _sha(_canonical(ids)),
             "source_schema_sha256": pins.schema_sha256, "source_crs_epsg": contract.crs_wkid,
             "declared_layer_extent": pins.layer_extent, "unique_source_id_count": len(ids),
             "source_segment_count": len(segments), "source_release_date": None,
             "source_release_date_unknown_reason": RELEASE_DATE_UNKNOWN_REASON}
    _check_population_pins(pins, audit, resumable=True)
    pending_names = contract.pending_pin_names or PENDING_PIN_NAMES
    pending = [constant for constant, field in pending_names if getattr(pins, field) is None]
    if pending:
        raise PopulationDigestPinsPending({**audit, "pending_pins": pending})
    return layer, fields_value, {"ids": ids, "audit": audit}


@dataclass(frozen=True)
class ManifestFragments:
    """The per-layer provenance text that stays in the layer adapter.

    Everything else in the candidate manifest is shared machinery; these
    fragments are the layer's own identity and honesty notes.
    """

    title: str
    description: str
    source_fields_note: str
    source_ids_note: str
    geometry_note: str
    lineage_sentences: str


def _capture_seed_ids(client: httpx.Client, contract: LayerContract, capture_root: Path,
                      capture_get: Callable, *, refusal_code: str | None = None) -> tuple[bytes, dict]:
    body, receipt = capture_with_retry(
        client, ids_query_url(contract.layer_url), capture_root, "ids_seed",
        max_bytes=contract.max_capture_bytes, attempts=contract.doc_attempts,
        base_delay=contract.doc_base_delay, capture_get=capture_get)
    if refusal_code and producer_error_doc(body):
        raise ValueError(refusal_code)
    return body, receipt


def code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _version_char_allowed(version: str) -> bool:
    return all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version)


def _validate_staging_resource(storage) -> None:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")


def _local_staging_root(storage) -> Path:
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    return base


def _quarantine_receipts(capture_root: Path) -> dict[str, object]:
    receipts = {}
    for path in sorted(capture_root.glob("*.receipt.json")):
        try:
            receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
        except (ValueError, OSError):
            receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
    return receipts


def _write_failure_manifest(capture_root: Path, payload: dict[str, object]) -> None:
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), **payload}) + b"\n")


def _retained_document_evidence(capture_root: Path, captures: dict, *, resumable: bool) -> dict:
    if resumable:
        retained = {name: value for name, value in captures.items() if not name.startswith("page_")}
    else:
        retained = dict(captures)
    for receipt_path in capture_root.glob("*.receipt.json"):
        name = receipt_path.name.removesuffix(".receipt.json")
        if name in retained or (resumable and name.startswith("page_")):
            # Segment bodies are kept only in the resumable capture directory;
            # these failures are not source drift, so only the bounded document
            # evidence is copied into staging quarantine.
            continue
        try:
            retained[name] = ((capture_root / f"{name}.body").read_bytes(),
                              json.loads(receipt_path.read_bytes()))
        except (OSError, ValueError):
            continue
    return retained


def _seed_population_ok(seed: object, count: int) -> bool:
    return (isinstance(seed, list) and len(seed) == count
            and all(type(value) is int for value in seed) and len(set(seed)) == count)


def capture_layer(contract: LayerContract, pins: PopulationPins, storage, *, version: str,
                  capture_directory: str | None, capture_get: Callable,
                  fragments: ManifestFragments) -> dict[str, object]:
    """Capture, verify, and stage one complete unapproved NFHL layer candidate.

    The acquisition/replay/packaging path is shared machinery; the layer
    adapter contributes only its contract, pins, provenance fragments and its
    own module reference for ``capture_get`` so module-level patching keeps
    working. Publication stays blocked: every approval flag in the staged
    candidate remains false.
    """
    _validate_staging_resource(storage)
    if not version or not _version_char_allowed(version):
        raise ValueError("invalid_version")
    base = _local_staging_root(storage)
    if shutil.disk_usage(base).free < contract.min_free_bytes:
        raise ValueError("disk_floor_before_acquisition")
    if contract.resumable and capture_directory:
        # An explicit capture_directory is reused verbatim so a restarted
        # capture resumes its completed segment evidence; without one, a
        # fresh unique directory keeps unrelated attempts isolated.
        capture_root = Path(capture_directory).absolute()
    else:
        attempt_root = Path(capture_directory) if capture_directory else Path(contract.reconstruction_dir)
        capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes | None, dict[str, object]]] = {}
    stream = None
    parquet_path: Path | None = None
    try:
        with httpx.Client(follow_redirects=False, timeout=contract.client_timeout,
                          headers={"User-Agent": contract.user_agent()}) as client:
            if contract.mode == "fixed":
                seed_body, seed_receipt = _capture_seed_ids(client, contract, capture_root, capture_get)
                seed = json.loads(seed_body).get("objectIds")
                if not _seed_population_ok(seed, pins.count):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_seed"] = (seed_body, seed_receipt)
                for name, url in fixed_page_urls(contract.service_url, contract.service_doc_url,
                                                 contract.layer_url, contract.page_size, len(seed)):
                    attempts = (contract.page_attempts or contract.doc_attempts) \
                        if name.startswith("page_") else contract.doc_attempts
                    base_delay = (contract.page_base_delay or contract.doc_base_delay) \
                        if name.startswith("page_") else contract.doc_base_delay
                    body, receipt = capture_with_retry(client, url, capture_root, name,
                                                       max_bytes=contract.max_capture_bytes,
                                                       attempts=attempts, base_delay=base_delay,
                                                       capture_get=capture_get)
                    captures[name] = (body, receipt)
                layer, source_fields, extracted = validate_in_memory_capture(contract, pins, captures)
            elif contract.resumable:
                ids_seed_body, ids_seed_receipt = _capture_seed_ids(
                    client, contract, capture_root, capture_get, refusal_code="source_ids_roster_refused")
                captures["ids_seed"] = (ids_seed_body, ids_seed_receipt)
                for name, url in doc_urls(contract.service_url, contract.service_doc_url,
                                          contract.layer_url, include_ids=False):
                    body, receipt = capture_with_retry(client, url, capture_root, name,
                                                       max_bytes=contract.max_capture_bytes,
                                                       attempts=contract.doc_attempts,
                                                       base_delay=contract.doc_base_delay,
                                                       capture_get=capture_get)
                    captures[name] = (body, receipt)
                ids_before_body, ids_before_receipt = capture_with_retry(
                    client, ids_query_url(contract.layer_url), capture_root, "ids_before",
                    max_bytes=contract.max_capture_bytes, attempts=contract.doc_attempts,
                    base_delay=contract.doc_base_delay, capture_get=capture_get)
                if producer_error_doc(ids_before_body):
                    raise ValueError("source_ids_roster_refused")
                captures["ids_before"] = (ids_before_body, ids_before_receipt)
                ids = validate_rosters_resumable({"ids_seed": captures["ids_seed"],
                                                  "ids_before": captures["ids_before"],
                                                  "ids_after": captures["ids_before"]}, pins)
                layer_doc = json.loads(captures["layer_before"][0])
                source_fields = layer_doc["fields"]
                parquet_path = capture_root / f"{contract.dataset_slug}.parquet"
                stream = SegmentStream(source_fields, parquet_path, contract.geometry_family,
                                       wkid=contract.crs_wkid)
                captures.update(capture_bisect_segments(client, contract, capture_root, len(ids),
                                                        capture_get, stream=stream, ids=ids))
                for name, url in after_doc_urls(contract.service_url, contract.layer_url, include_ids=False):
                    body, receipt = capture_with_retry(client, url, capture_root, name,
                                                       max_bytes=contract.max_capture_bytes,
                                                       attempts=contract.doc_attempts,
                                                       base_delay=contract.doc_base_delay,
                                                       capture_get=capture_get)
                    captures[name] = (body, receipt)
                ids_after_body, ids_after_receipt = capture_with_retry(
                    client, ids_query_url(contract.layer_url), capture_root, "ids_after",
                    max_bytes=contract.max_capture_bytes, attempts=contract.doc_attempts,
                    base_delay=contract.doc_base_delay, capture_get=capture_get)
                if producer_error_doc(ids_after_body):
                    raise ValueError("source_ids_roster_refused")
                captures["ids_after"] = (ids_after_body, ids_after_receipt)
                extracted_audit = stream.finish()
                if extracted_audit["source_feature_count"] != len(ids):
                    raise ValueError("source_count_drift")
                layer, source_fields, extracted = validate_resumable_capture(contract, pins, captures,
                                                                             extracted_audit)
            elif contract.mode == "bisect":
                seed_body, seed_receipt = _capture_seed_ids(client, contract, capture_root, capture_get)
                seed = json.loads(seed_body).get("objectIds")
                if not _seed_population_ok(seed, pins.count):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_seed"] = (seed_body, seed_receipt)
                for name, url in doc_urls(contract.service_url, contract.service_doc_url,
                                          contract.layer_url, include_ids=True):
                    body, receipt = capture_with_retry(client, url, capture_root, name,
                                                       max_bytes=contract.max_capture_bytes,
                                                       attempts=contract.doc_attempts,
                                                       base_delay=contract.doc_base_delay,
                                                       capture_get=capture_get)
                    captures[name] = (body, receipt)
                captures.update(capture_bisect_segments(client, contract, capture_root, pins.count,
                                                        capture_get))
                for name, url in after_doc_urls(contract.service_url, contract.layer_url, include_ids=True):
                    body, receipt = capture_with_retry(client, url, capture_root, name,
                                                       max_bytes=contract.max_capture_bytes,
                                                       attempts=contract.doc_attempts,
                                                       base_delay=contract.doc_base_delay,
                                                       capture_get=capture_get)
                    captures[name] = (body, receipt)
                layer, source_fields, extracted = validate_in_memory_capture(contract, pins, captures)
            else:
                raise ValueError("unknown_capture_mode:" + contract.mode)
    except PopulationDigestPinsPending as pending:
        stream_close(stream)
        _write_failure_manifest(capture_root, {
            "complete": True, "staged": False, "error_code": "population_digest_pins_not_established",
            "source_feature_count": pending.audit["source_feature_count"],
            "source_id_sha256": pending.audit["source_id_sha256"],
            "source_record_sha256": pending.audit["source_record_sha256"],
            "source_geometry_sha256": pending.audit["source_geometry_sha256"],
            "source_schema_sha256": pending.audit["source_schema_sha256"],
            "source_null_geometry_count": pending.audit["null_geometry_count"],
            "source_nan_string_geometry_count": pending.audit["nan_string_geometry_count"],
            "source_unusable_geometry_feature_counts": pending.audit["unusable_geometry_feature_counts"],
            "source_observed_bounds": pending.audit["observed_bounds"],
            "source_segment_count": pending.audit["source_segment_count"],
            "pending_pins": pending.audit["pending_pins"],
            "capture_directory": str(capture_root),
            "receipts": _quarantine_receipts(capture_root)})
        retained = _retained_document_evidence(capture_root, captures, resumable=contract.resumable)
        if retained:
            quarantine_incomplete_capture(storage, capture_root, contract.dataset_slug, version, retained,
                                          "population_digest_pins_not_established")
        raise
    except Exception as exc:
        stream_close(stream)
        _write_failure_manifest(capture_root, {"complete": False, "error_code": type(exc).__name__,
                                               "receipts": _quarantine_receipts(capture_root)})
        retained = _retained_document_evidence(capture_root, captures, resumable=contract.resumable)
        if retained:
            quarantine_incomplete_capture(storage, capture_root, contract.dataset_slug, version, retained,
                                          type(exc).__name__)
        raise
    identity = code_identity()
    audit = {**extracted["audit"], "source_fields": [plain_field(f) for f in source_fields],
             "official_layer_name": contract.layer_name, "layer_id": contract.layer_id,
             "service_url": contract.service_url,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(contract.dataset_slug, contract.file_slug, version)
    rel = f"geoparquet/{contract.dataset_slug}.parquet"
    if parquet_path is not None:
        parquet_digest = hashlib.sha256()
        with parquet_path.open("rb") as stream_handle:
            for block in iter(lambda: stream_handle.read(1 << 20), b""):
                parquet_digest.update(block)
        source_objects = {rel: {"bytes": parquet_path.stat().st_size, "sha256": parquet_digest.hexdigest()}}
    else:
        parquet_body = parquet_bytes(source_fields, extracted["features"], contract.geometry_family,
                                     wkid=contract.crs_wkid, gated_rings=contract.gated_rings,
                                     arrow_type_overrides=contract.arrow_type_overrides)
        source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": fragments.title,
        "description": fragments.description,
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": contract.layer_url,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": contract.service_doc_url if name == "service_doc" else contract.layer_url,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": INPUT_RELEASE_DATE_UNKNOWN_REASON,
                    "license": INPUT_LICENSE_NOTE,
                    "attribution": INPUT_ATTRIBUTION_NOTE}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": contract.layer_id,
                                            "source_fields": fragments.source_fields_note,
                                            "source_ids": fragments.source_ids_note,
                                            "geometry": fragments.geometry_note,
                                            "candidate_status": CANDIDATE_STATUS_NOTE}}],
        "lineage": LINEAGE_PREFIX + fragments.lineage_sentences,
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": PARQUET_POLICY_REASON,
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": UNSUPPORTED_OUTPUT_FORMATS,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    _write_failure_manifest(capture_root, {
        "complete": True, "source_feature_count": pins.count,
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}})
    state_key = f"_construction-state/{contract.dataset_slug}/{version}.json"
    outputs: dict[str, bytes | Path] = {}
    if parquet_path is not None:
        outputs[rel] = parquet_path
    else:
        outputs[rel] = parquet_body
    outputs["metadata/source_manifest.json"] = _canonical(manifest) + b"\n"
    outputs["metadata/source_audit.json"] = _canonical(audit) + b"\n"
    outputs["metadata/processing_identity.json"] = _canonical(identity) + b"\n"
    resumable_outputs = parquet_path is not None
    for name in sorted(captures):
        receipt = captures[name][1]
        body = captures[name][0]
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
        if resumable_outputs:
            # Segment bodies stay on disk in the resumable capture directory;
            # bounded document bodies are already in memory.
            outputs[f"raw/{name}.body"] = (capture_root / f"{name}.body") if body is None else body
            outputs[f"raw/{name}.wire"] = capture_root / f"{name}.wire"
        else:
            outputs[f"raw/{name}.body"] = body
            outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
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
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                objects, payload.get("capture_receipts", {}))
        return _candidate_result(contract, pins, audit, version, prefix, reused=True)
    descriptors: dict[str, dict[str, object]] = {}
    for name, value in outputs.items():
        key = f"{prefix}/{name}"
        if isinstance(value, Path):
            storage.upload_local_file(value, key)
            snapshot = storage.object_snapshot(key)
            if snapshot is None:
                raise ValueError("candidate_output_physical_readback_failed:" + name)
            descriptor = {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
            if descriptor["sha256"] is None:
                raise ValueError("candidate_output_physical_readback_failed:" + name)
        else:
            write_immutable(storage, key, value)
            snapshot = storage.object_snapshot(key)
            if snapshot is None or snapshot.size != len(value) or storage.sha256_key(key) != _sha(value) \
                    or storage.object_snapshot(key) != snapshot:
                raise ValueError("candidate_output_physical_readback_failed:" + name)
            descriptor = {"bytes": len(value), "sha256": _sha(value)}
        descriptors[name] = descriptor
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return _candidate_result(contract, pins, audit, version, prefix, reused=False,
                             capture_directory=str(capture_root))


def _candidate_result(contract: LayerContract, pins: PopulationPins, audit: dict[str, object],
                      version: str, prefix: str, *, reused: bool,
                      capture_directory: str | None = None) -> dict[str, object]:
    result = {"dataset_slug": contract.dataset_slug, "file_slug": contract.file_slug,
              "version": version,
              "feature_count": pins.count,
              "source_null_geometry_count": audit["null_geometry_count"],
              "source_nan_string_geometry_count": audit["nan_string_geometry_count"]}
    if contract.gated_rings:
        result["source_unusable_geometry_feature_counts"] = audit["unusable_geometry_feature_counts"]
    result["candidate_ready"] = prefix + "/metadata/candidate_ready.json"
    if reused:
        result["reused"] = True
        result.update({"source_release_accepted": False, "source_freshness_accepted": False,
                       "schema_contract_accepted": False, "id_contract_accepted": False,
                       "rights_accepted": False, "mapping_approved": False, "release_accepted": False})
        return result
    result.update({"source_release_accepted": False, "source_freshness_accepted": False,
                   "schema_contract_accepted": False, "id_contract_accepted": False,
                   "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
                   "capture_directory": capture_directory})
    return result
