"""Lossless, approval-gated capture of the FEMA NFHL "Subbasins" polygon layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 31 "Subbasins" as
the producer's own publication of this HIFLD inventory dataset
(``nfhl/subbasins``, DataLumos 239990 historical archive copy used for
identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

Scale and resumability: the producer reported 169,815 features for this layer
on 2026-11-14 (the continuously updated NFHL grew while this draft was being
captured: 167,124 on 2026-11-13 over the span 1..613,918, then 169,681, then
169,815; five dated probes on 2026-11-14, after that edit window settled,
agreed between returnCountOnly and the complete whole-layer OBJECTID
roster: 169,815 unique integer ids over the span 1..616,767, sorted-roster
sha ``24cee71c...``), and the layer's serialized polygon density is high (observed
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

This adapter is a thin entry point over the shared NFHL capture and replay
machinery in ``dagster_hifld.sources.nfhl_common``; it contributes only this
layer's URL/id, schema/population pins, capture budgets, geometry validation
and provenance text.
"""
from __future__ import annotations

import httpx
import time
from pathlib import Path

from dagster_hifld.sources import nfhl_common as _nfhl
from dagster_hifld.sources.epa_frs_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

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
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Structural pins below are the observed 2026-11-14 producer contract,
# established by dated anonymous queries of the producer's own endpoints. The
# producer's continuously updated layer grew while drafting (167,124 on
# 2026-11-13 over the span 1..613,918 -> 169,681 -> 169,760 -> 169,815 on
# 2026-11-14) and churns interior ids as well as appending: of the 169,681 ids
# pinned on 2026-11-13 only 169,678 remained at or below the old span max
# 616,630 (3 deleted, 137 appended, 3 re-inserted below), so the pins were
# re-established only after that edit window settled: five agreeing dated
# whole-roster probes between 2026-11-14 02:59:43Z and PROBE3_Z, spanning
# more than 30 minutes with no further change, agreed between returnCountOnly
# and the complete whole-layer OBJECTID roster of 169,815 unique ids over the
# span 1..616,767 (sorted-roster sha pinned below), with schema, layer extent,
# service roster, pagination capability, record limit and transfer-cap mode
# re-verified unchanged in the same probes. Segment evidence captured against
# the earlier roster is healed against the live roster and renamed to .stale
# evidence before reuse, never trusted as served. The module's own capture
# must reproduce every pin; the before/after count/id/metadata brackets fail
# closed if the producer moves again during a capture. The population digest
# pins (record, geometry, null/nan/unusable counts, observed bounds) are None
# until a first complete capture establishes them; see the module docstring.
# They fail closed, so the capture can never stage an unverified candidate.
EXPECTED_COUNT = 169815
EXPECTED_ID_SHA256 = "24cee71c525261b6e0ce7ddd1ce8e8f6b926778271743c697dc9815fc232adaf"
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

_PENDING_PIN_FIELDS = (
    ("EXPECTED_RECORD_SHA256", "record_sha256"),
    ("EXPECTED_GEOMETRY_SHA256", "geometry_sha256"),
    ("EXPECTED_OBSERVED_BOUNDS", "observed_bounds"),
    ("EXPECTED_NULL_GEOMETRY_COUNT", "null_geometry_count"),
    ("EXPECTED_NAN_GEOMETRY_COUNT", "nan_geometry_count"),
    ("EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS", "unusable_geometry_feature_counts"))

_RECONSTRUCTION_DIR = "/tmp/hifld-native-reconstruction/213/captures"

_FRAGMENTS = _nfhl.ManifestFragments(
    title="Subbasins (FEMA NFHL)",
    description="FEMA National Flood Hazard Layer (NFHL) MapServer layer 31 'Subbasins', captured "
                "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                "polygon geometry are represented in the candidate. The NFHL is continuously updated and "
                "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                "release acceptance remain unresolved and unapproved.",
    source_fields_note="all producer fields retained in producer order and "
                       "physical types, including the producer's derived "
                       "SHAPE.STArea() and SHAPE.STLength() doubles; SHAPE is "
                       "the original geometry field represented as WKB",
    source_ids_note="Preserve producer OBJECTID, SUBBAS_ID, GFID and GlobalID "
                    "values unchanged; no substitute IDs, casts, trimming, or "
                    "joins",
    geometry_note="Source NAD83 (EPSG:4269) polygon rings retained as WKB "
                  "without projection under the producer's verified Esri "
                  "ring semantics (closed rings; clockwise exteriors, "
                  "counter-clockwise holes); no vertex dropped, added, or "
                  "reordered; null source geometry and producer "
                  "string-'NaN' ordinate rows retained as records with null "
                  "candidate geometry, raw evidence intact; unusable ring "
                  "semantics gated to null candidate geometry with pinned "
                  "failure modes",
    lineage_sentences="This candidate is exactly layer 31 'Subbasins' of that producer "
                      "service. The HIFLD Open inventory lists this dataset as nfhl/subbasins with DataLumos "
                      "project 239990; that ICPSR archive copy and the 2024-10-31 archive modification date are "
                      "identification and comparison context only, never pipeline inputs.")


def _contract() -> _nfhl.LayerContract:
    return _nfhl.LayerContract(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        geometry_family="polygon", esri_geometry_type="esriGeometryPolygon", mode="bisect",
        max_capture_bytes=MAX_CAPTURE_BYTES, min_free_bytes=MIN_FREE_BYTES,
        reconstruction_dir=_RECONSTRUCTION_DIR, segment_start_records=SEGMENT_START_RECORDS,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, doc_base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
        segment_attempts=SEGMENT_PROBE_ATTEMPTS, segment_base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS,
        client_timeout=180.0, gated_rings=True, resumable=True,
        pending_pin_names=_PENDING_PIN_FIELDS)


def _pins() -> _nfhl.PopulationPins:
    return _nfhl.PopulationPins(
        count=EXPECTED_COUNT, id_sha256=EXPECTED_ID_SHA256, schema_sha256=EXPECTED_SCHEMA_SHA256,
        record_sha256=EXPECTED_RECORD_SHA256, geometry_sha256=EXPECTED_GEOMETRY_SHA256,
        observed_bounds=EXPECTED_OBSERVED_BOUNDS, null_geometry_count=EXPECTED_NULL_GEOMETRY_COUNT,
        nan_geometry_count=EXPECTED_NAN_GEOMETRY_COUNT,
        unusable_geometry_feature_counts=EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS,
        layer_extent=EXPECTED_LAYER_EXTENT, service_layer_roster=EXPECTED_SERVICE_LAYER_ROSTER)


_segment_name = _nfhl.segment_name
_parse_segment_name = _nfhl.parse_segment_name
_StreamDigest = _nfhl.StreamDigest
PopulationDigestPinsPending = _nfhl.PopulationDigestPinsPending
_code_identity = _nfhl.code_identity


class _SegmentStream(_nfhl.SegmentStream):
    """The layer's streaming validator over the shared segment machinery."""

    def __init__(self, fields: list, parquet_path: Path):
        super().__init__(fields, parquet_path, "polygon", wkid=SOURCE_CRS_WKID)


def _ids_url() -> str:
    return _nfhl.ids_query_url(LAYER_URL)


def _segment_query(offset: int, count: int) -> str:
    return _nfhl.segment_query_url(LAYER_URL, offset, count)


_polygon_wkb = _nfhl.polygon_wkb


def _capture_with_retry(client, url: str, capture_root, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float):
    return _nfhl.capture_with_retry(client, url, capture_root, name, max_bytes=max_bytes,
                                    attempts=attempts, base_delay=base_delay, capture_get=capture_get)


def _capture_segments(client, capture_root, ids: list, stream=None):
    return _nfhl.capture_bisect_segments(client, _contract(), capture_root, len(ids), capture_get,
                                         stream=stream, ids=ids)


def _validate_captures(captures: dict, audit: dict):
    return _nfhl.validate_resumable_capture(_contract(), _pins(), captures, audit)


def stream_close(stream) -> None:
    _nfhl.stream_close(stream)


def capture_nfhl_subbasins(storage, *, version: str,
                           capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Subbasins candidate."""
    return _nfhl.capture_layer(_contract(), _pins(), storage, version=version,
                               capture_directory=capture_directory, capture_get=capture_get,
                               fragments=_FRAGMENTS)
