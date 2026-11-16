"""Lossless, approval-gated capture of the FEMA NFHL "Profile Baselines" line layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 17 "Profile
Baselines" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/profile-baselines``, DataLumos 239985 historical archive copy used for
identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

This layer declares Z and M geometry (verified 2026-11-12: ``hasZ``/``hasM``
both true, ``sourceHeightModelInfo`` gravity-related NAVD88 heights in US
survey feet), so the capture requests ``returnZ=true&returnM=true`` and every
vertex is taken as the producer's four-ordinate ``[x, y, z, m]`` form; a
two-ordinate vertex would silently drop the elevation and measure ordinates.
Candidate geometry is ISO WKB XYZM, stored with a native Parquet GEOMETRY
logical annotation and GeoParquet 2.0 metadata (GeoParquet 1.1 cannot carry M): producer JSON ``null`` or string-``"NaN"``
Z/M ordinates map to WKB NaN, the OGC "no value" measure convention, with every
such vertex counted in the audit while the raw paths in the record digest keep
the producer's original values. The GeoParquet ``geo`` metadata declares
"LineString ZM"/"MultiLineString ZM", matching the full four-ordinate WKB
and native GEOMETRY CRS annotation. The audit also records the
source's hasZ/hasM contract and per-ordinate null/NaN counts.
X/Y ``"NaN"`` string vertices gate the record's candidate geometry to null
(never a coerced location), raw evidence intact.

The contract is FEMA's published default REST linear XYZM representation:
``returnTrueCurves`` is omitted (publisher default false). All returned source
attributes and ordinates are retained without client densification or repair.
The layer supports true-curve queries, but capability is not population evidence;
eight spread explicit-true/default sample geometries matched exactly. National
equivalence to backing-geodatabase true curves is not established or claimed.

The national layer cannot be materialized as one in-memory
feature list (observed per-feature response sizes range from under 1 KB to
over 180 KB), so capture and validation are streamed: pages are captured as
adaptively bisected segments of the sorted OBJECTID roster starting at the
producer's maxRecordCount, each page is validated against the pinned roster
slice as it is captured, and the full-record and geometry digests are streamed
over the pages in roster order - byte-identical to digesting the complete
sorted feature list - before the GeoParquet candidate is assembled in a
second streamed pass. Producer transfer-cap refusals (persistent HTTP 200
Esri error documents, as observed on sibling NFHL layers) bisect until served;
the producer also intermittently resets connections mid-service. Wide failed
segments split promptly; document and single-feature requests retain bounded
retries, with refused attempts preserved as evidence. A completed page is the unit of resume: with the same capture
directory, bodies verified against their hash-pinned receipts are reused
instead of re-downloaded, while the before/after count/id/metadata brackets,
exact segment tiling, per-page roster slices, and full-record digests still
prove nothing was skipped, duplicated, reordered, or silently substituted.
Document captures (count/ID rosters, layer metadata) are always fetched fresh
because the NFHL is continuously updated - observed on 2026-11-12/13, when
FEMA edited this layer during a capture window (2,254 features added, 2,025
removed) - and a reused page that no longer matches its roster slice is
archived and re-captured fresh exactly once. These checks establish the
observed ID roster, not attribute freshness for every reused page or an atomic
source snapshot; original receipt timestamps remain part of the audit.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import struct
import time
from collections import abc, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.native_geoparquet_catalog import _safe_local_key
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "nfhl-profile-baselines"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 17
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Profile Baselines"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# The producer declares Z (gravity-related NAVD88 height, US survey feet) and
# M (route measure) on this layer; pages are requested with returnZ/returnM so
# each vertex carries all four producer ordinates.
SOURCE_HAS_Z = True
SOURCE_HAS_M = True
EXPECTED_SOURCE_HEIGHT_MODEL = {"heightModel": "gravity_related_height",
                                "vertCRS": "NAVD88_height_(ftUS)", "heightUnit": "us-foot"}
# Initial segment width, capped by the producer's maxRecordCount (2000). Any
# segment the producer keeps refusing (its serialized response would approach
# the producer's transfer cap) is bisected until it is served; see the module
# docstring.
SEGMENT_START_RECORDS = 2000
# Per-feature JSON sizes on this layer reach beyond 180 KB, so a full
# 2000-record page can approach 400 MB; the per-request cap must cover the
# widest plausible served page, and narrower segments remain available through
# bisection when the producer refuses.
MAX_CAPTURE_BYTES = 512 * 1024 * 1024
# Dense producer pages exceed 29 MB for just 100 records. Use the last served
# page density to split subsequent requests before repeatedly hitting that cap.
# This is a request-size hint; all records and geometry still validate below.
TARGET_SEGMENT_BYTES = 16 * 1024 * 1024
# Keep a bounded reserve, plus the request's maximum wire/decoded bytes.
# A complete replay does not need another national download's disk space.
MIN_FREE_BYTES = 2_000_000_000
# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or resets connections (observed 2026-11-12
# while probing this layer; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact segment tiling, and
# full-record digests, so retries can never mask drift. Segments wider than
# one record that keep failing are bisected (the producer's transfer cap); a
# single-record segment that keeps failing fails the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
# A wide failed request can always be retried as two complete smaller slices;
# repeating its oversized payload first only delays the safe bisection.
# Documents and an unservable single feature retain bounded retries below.
SEGMENT_PROBE_ATTEMPTS = 1
SEGMENT_PROBE_BASE_DELAY_SECONDS = 10.0
# Retain a pause for incomplete transports, throttling and unclassified
# producer errors. The complete-200 query refusal observed on dense pages is
# split promptly instead; all failed bytes/receipts remain evidence.
SEGMENT_BISECT_COOLDOWN_SECONDS = 20.0
# Observed complete published-REST population: 339,709 features / 1,342 segments.
# Successful page receipt window: 2026-11-14T11:03:48.847307Z through
# 2026-11-16T00:30:13.706903Z. Final count/ID/metadata brackets passed at
# 2026-11-16T00:30:19Z; every page matched its sorted OBJECTID roster slice.
# These are capture-value pins, not an atomic/as-of or attribute-freshness
# guarantee. The registered ingest must reproduce every pin without patching.
EXPECTED_COUNT = 339709
EXPECTED_ID_SHA256 = 'f34d8b66f29cd429bea6ca522ae7313e13b78cae2a33472d8784a234f1653c75'
EXPECTED_SCHEMA_SHA256 = 'd569797441ac90099e5117a72cff319029a432c155c88492664908707e70d995'
EXPECTED_RECORD_SHA256 = '57625d9ba12d909628d46cf8b1ea26eb01c59df2ae727831a593855ea5ce2ffa'
EXPECTED_GEOMETRY_SHA256 = 'cac8e4478e015f42baa1c2c249fb532aa6afecb42654fb3bf57ac69da7860e35'
EXPECTED_NULL_GEOMETRY_COUNT = 68
EXPECTED_NULL_GEOMETRY_ID_SHA256 = '435a6585f8bfbe7c99a6c34af4d0017700f648b10a79a44f4aa528a53c3a845b'
EXPECTED_EMPTY_GEOMETRY_COUNT = 1
EXPECTED_EMPTY_GEOMETRY_ID_SHA256 = '6e8adb9c5f79eb8ea1ca111dcb3231f5bb0c1004651d9f2d2b8b28fddc2092cd'
EXPECTED_NAN_GEOMETRY_COUNT = 0
EXPECTED_NULL_Z_VERTEX_COUNT = 0
EXPECTED_NAN_Z_VERTEX_COUNT = 0
EXPECTED_NULL_M_VERTEX_COUNT = 74763707
EXPECTED_NAN_M_VERTEX_COUNT = 0
EXPECTED_OBSERVED_BOUNDS = {'xmax': -64.82526046639344, 'xmin': -159.71434248258717, 'ymax': 64.8925007875672, 'ymin': 17.685498241353855}
EXPECTED_LAYER_EXTENT = {'xmax': -64.82526046639344, 'xmin': -159.71434248258717, 'ymax': 64.8925007875672, 'ymin': 17.685498241353855}
EXPECTED_GEOMETRY_TYPES = ['LineString ZM']
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

_SEED_DOC = "ids_seed"
_DOC_NAMES = ("service_doc", "service_before", "layer_before", "count_before", "ids_before")
_AFTER_DOC_NAMES = ("layer_after", "service_after", "count_after", "ids_after")

# ISO/IEC 13249-3 four-ordinate type codes, including each nested part header.
ISO_LINESTRING_ZM = 3002
ISO_MULTILINESTRING_ZM = 3005

def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "returnZ": "true", "returnM": "true",
                   "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _ids_query() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("service_doc", SERVICE_DOC_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _ids_query()),
    ]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _ids_query()),
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


def _body_is_producer_error_doc(capture_root: Path, name: str) -> bool:
    body_path = capture_root / f"{name}.body"
    try:
        body = body_path.read_bytes()
    except OSError:
        return False
    return _producer_error_doc(body)


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    while any((capture_root / f"{name}.attempt-{attempt:02d}{suffix}").exists()
              for suffix in (".body", ".wire", ".receipt.json")):
        attempt += 1
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _reusable_receipt(capture_root: Path, name: str) -> dict[str, object] | None:
    """Receipt of a previously captured request when its evidence still verifies.

    A completed body/wire pair on disk is reusable only when its hash-pinned
    receipt is complete and every digest still matches the stored bytes;
    anything else (including a missing or malformed receipt, or a retained
    producer error document from an exhausted retry) forces a fresh capture.
    """
    body_path, wire_path = capture_root / f"{name}.body", capture_root / f"{name}.wire"
    receipt_path = capture_root / f"{name}.receipt.json"
    if not (body_path.is_file() and wire_path.is_file() and receipt_path.is_file()):
        return None
    try:
        receipt = json.loads(receipt_path.read_bytes())
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
    except (OSError, ValueError):
        return None
    if not isinstance(receipt, dict):
        return None
    if receipt.get("complete_body") is not True or receipt.get("status") != 200 \
            or receipt.get("final_url") != receipt.get("url"):
        return None
    if _producer_error_doc(body):
        return None
    if receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) \
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire):
        return None
    return receipt


def _ensure_captured(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                     max_bytes: int, attempts: int, base_delay: float,
                     reuse: bool = True) -> tuple[dict[str, object], bool]:
    """Capture one producer request, optionally reusing verified on-disk evidence.

    Retries cover the producer's transient complete-200 Esri error documents
    and incomplete transport captures; failed attempts are preserved on disk
    under ``<name>.attempt-NN`` evidence names and never enter the capture
    roster. A request whose completed body/wire/receipt evidence is already on
    disk and still verifies is reused without a network request when ``reuse``
    is set, so an interrupted capture resumes instead of repeating its
    completed pages. Returns the receipt and whether it came from disk;
    document captures always fetch fresh because the NFHL is continuously
    updated and a stale roster would mis-slice every page.
    """
    if reuse:
        cached = _reusable_receipt(capture_root, name)
        if cached is not None and cached.get("url") == url:
            return cached, True
    capture_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(capture_root).free < MIN_FREE_BYTES + 2 * max_bytes:
        raise ValueError("disk_floor_before_acquisition")
    # Retain prior generations, refused responses and partial transports before
    # capture_get replaces the active request files.
    _preserve_failed_attempt(capture_root, name, 1)
    for attempt in range(1, attempts + 1):
        try:
            capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _body_is_producer_error_doc(capture_root, name) and attempt < attempts:
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return json.loads((capture_root / f"{name}.receipt.json").read_bytes()), False
    raise AssertionError("unreachable_retry_state")


def _read_doc(capture_root: Path, name: str) -> object:
    body = (capture_root / f"{name}.body").read_bytes()
    if _producer_error_doc(body):
        raise ValueError("source_producer_error_document_persisted:" + name)
    try:
        return json.loads(body)
    except ValueError as exc:
        raise ValueError("source_document_not_json:" + name) from exc


def _roster_ids(doc: object) -> list[int]:
    if not isinstance(doc, dict):
        raise ValueError("source_id_roster_invalid")
    roster = doc.get("objectIds")
    if not isinstance(roster, list) or any(type(value) is not int for value in roster) \
            or len(set(roster)) != len(roster):
        raise ValueError("source_id_population_drift")
    return roster


def _contract_from_docs(docs: Mapping[str, object]) -> dict[str, object]:
    """Validate the service/layer identity and derive the population contract.

    Structural failures (wrong service, wrong layer, missing evidence) raise
    immediately; the pinned population values are checked against the
    ``EXPECTED_*`` pins so a drifted source fails the capture before any
    feature page is requested.
    """
    if not isinstance(docs.get("service_doc"), bytes) \
            or SERVICE_URL.encode("utf-8") not in docs["service_doc"]:
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
        raise ValueError("exact_nfhl_profile_baselines_layer_missing")
    if (not isinstance(layer, dict) or layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolyline"):
        raise ValueError("exact_nfhl_profile_baselines_layer_metadata_drift")
    if layer.get("hasZ") is not SOURCE_HAS_Z or layer.get("hasM") is not SOURCE_HAS_M:
        raise ValueError("source_z_or_m_geometry_contract_drift")
    if layer.get("sourceHeightModelInfo") != EXPECTED_SOURCE_HEIGHT_MODEL:
        raise ValueError("source_height_model_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    if not isinstance(extent, dict):
        raise ValueError("source_layer_extent_missing")
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    declared_extent = {key: extent.get(key) for key in ("xmin", "ymin", "xmax", "ymax")}
    if EXPECTED_LAYER_EXTENT is not None and declared_extent != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    geometry_fields = [field["name"] for field in fields if field.get("type") == "esriFieldTypeGeometry"]
    if geometry_fields != ["SHAPE"]:
        raise ValueError("source_geometry_field_roster_drift")
    schema_sha = _sha(_canonical(fields))
    if EXPECTED_SCHEMA_SHA256 and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [_roster_ids(docs["ids_before"]), _roster_ids(docs[_SEED_DOC])]
    if any(len(roster) != EXPECTED_COUNT for roster in rosters):
        raise ValueError("source_id_population_drift")
    if sorted(rosters[0]) != sorted(rosters[1]):
        raise ValueError("source_id_population_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", _SEED_DOC)):
        raise ValueError("source_id_field_changed")
    ids = sorted(rosters[0])
    if EXPECTED_ID_SHA256 and _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    return {"service": service, "layer": layer, "fields_value": fields_value, "fields": fields,
            "schema_sha256": schema_sha, "ids": ids, "declared_extent": declared_extent}


def _validate_after_docs(docs_after: Mapping[str, object], contract: Mapping[str, object]) -> None:
    """Require the capture brackets to show an unchanged source population."""
    layer, service = contract["layer"], contract["service"]
    fields = contract["fields"]
    if docs_after["layer_after"].get("fields") is None \
            or [_plain_field(x) for x in docs_after["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    if docs_after["layer_after"] != layer:
        raise ValueError("source_layer_metadata_changed_during_capture")
    if docs_after["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs_after["count_after"] != docs_after["count_before"]:
        raise ValueError("source_count_changed_during_capture")
    if docs_after["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    roster_after = _roster_ids(docs_after["ids_after"])
    if len(roster_after) != EXPECTED_COUNT or sorted(roster_after) != sorted(contract["ids"]):
        raise ValueError("source_id_population_drift")
    if docs_after["ids_after"].get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_id_field_changed")


class _ItemDigest:
    """Streamed sha256 over the canonical JSON array of added items."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._first = True

    def add(self, item: object) -> None:
        if not self._first:
            self._digest.update(b",")
        self._digest.update(_canonical(item))
        self._first = False

    def hexdigest(self) -> str:
        digest = self._digest.copy()
        digest.update(b"]")
        return digest.hexdigest()

    def snapshot(self) -> tuple:
        return self._digest.copy(), self._first

    def restore(self, snapshot: tuple) -> None:
        self._digest, self._first = snapshot[0], snapshot[1]


class _StreamStats:
    """Streamed full-record/geometry digests and population tallies.

    Features are fed in roster (OBJECTID ascending) order - each page is
    proven to equal its sorted roster slice - so the streamed canonical-item
    digests are byte-identical to digesting the complete sorted feature and
    geometry-record lists without ever materializing them.
    """

    def __init__(self) -> None:
        self._records = _ItemDigest()
        self._geometries = _ItemDigest()
        self.feature_count = 0
        self.null_geometry_count = 0
        self.nan_geometry_count = 0
        self.empty_geometry_count = 0
        self.null_geometry_ids: list[int] = []
        self.empty_geometry_ids: list[int] = []
        self.null_z_vertex_count = 0
        self.nan_z_vertex_count = 0
        self.null_m_vertex_count = 0
        self.nan_m_vertex_count = 0
        self.xmin = self.ymin = self.xmax = self.ymax = None
        self.geometry_types: set[str] = set()

    def snapshot(self) -> dict[str, object]:
        return {"records": self._records.snapshot(), "geometries": self._geometries.snapshot(),
                "feature_count": self.feature_count, "null_geometry_count": self.null_geometry_count,
                "nan_geometry_count": self.nan_geometry_count,
                "null_geometry_ids": list(self.null_geometry_ids),
                "empty_geometry_count": self.empty_geometry_count,
                "empty_geometry_ids": list(self.empty_geometry_ids),
                "null_z_vertex_count": self.null_z_vertex_count,
                "nan_z_vertex_count": self.nan_z_vertex_count,
                "null_m_vertex_count": self.null_m_vertex_count,
                "nan_m_vertex_count": self.nan_m_vertex_count,
                "bounds": (self.xmin, self.ymin, self.xmax, self.ymax),
                "geometry_types": set(self.geometry_types)}

    def restore(self, snapshot: Mapping[str, object]) -> None:
        self._records.restore(snapshot["records"])
        self._geometries.restore(snapshot["geometries"])
        self.feature_count = snapshot["feature_count"]
        self.null_geometry_count = snapshot["null_geometry_count"]
        self.nan_geometry_count = snapshot["nan_geometry_count"]
        self.null_geometry_ids = list(snapshot["null_geometry_ids"])
        self.empty_geometry_count = snapshot["empty_geometry_count"]
        self.empty_geometry_ids = list(snapshot["empty_geometry_ids"])
        self.null_z_vertex_count = snapshot["null_z_vertex_count"]
        self.nan_z_vertex_count = snapshot["nan_z_vertex_count"]
        self.null_m_vertex_count = snapshot["null_m_vertex_count"]
        self.nan_m_vertex_count = snapshot["nan_m_vertex_count"]
        self.xmin, self.ymin, self.xmax, self.ymax = snapshot["bounds"]
        self.geometry_types = set(snapshot["geometry_types"])

    def add(self, feature: object, attr_names: list[str]) -> None:
        if not isinstance(feature, dict):
            raise ValueError("source_record_invalid")
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        oid = attrs["OBJECTID"]
        self._records.add(feature)
        self.feature_count += 1
        geom = feature.get("geometry")
        if geom is None:
            self.null_geometry_count += 1
            self.null_geometry_ids.append(oid)
            self._geometries.add([oid, None])
            return
        if not isinstance(geom, dict) or not (set(geom) == {"paths"}
                                              or (set(geom) == {"hasZ", "hasM", "paths"}
                                                  and geom["hasZ"] is SOURCE_HAS_Z
                                                  and geom["hasM"] is SOURCE_HAS_M)) \
                or not isinstance(geom["paths"], list):
            raise ValueError("source_line_geometry_invalid")
        if not geom["paths"]:
            # The producer publishes geometry objects with an empty paths list
            # (no parts); the record is retained with null candidate geometry
            # and its own audited roster, never dropped.
            self.empty_geometry_count += 1
            self.empty_geometry_ids.append(oid)
            self._geometries.add([oid, []])
            return
        nan_paths = False
        for path in geom["paths"]:
            if not isinstance(path, list) or len(path) < 2:
                raise ValueError("source_line_geometry_invalid")
            for vertex in path:
                if not isinstance(vertex, list) or len(vertex) != 4:
                    raise ValueError("source_line_geometry_invalid")
                x, y, z, m = vertex
                if type(x) in (int, float) and type(y) in (int, float):
                    if not (-180 <= x <= 180 and -90 <= y <= 90):
                        raise ValueError("source_coordinate_range_drift")
                    if self.xmin is None or x < self.xmin:
                        self.xmin = x
                    if self.xmax is None or x > self.xmax:
                        self.xmax = x
                    if self.ymin is None or y < self.ymin:
                        self.ymin = y
                    if self.ymax is None or y > self.ymax:
                        self.ymax = y
                elif x == "NaN" and y == "NaN":
                    # String-"NaN" planar ordinates gate this record's
                    # candidate geometry to null; the raw paths stay in the
                    # record and geometry digests either way and the record is
                    # never silently coerced to a location.
                    nan_paths = True
                else:
                    raise ValueError("source_line_geometry_invalid")
                if isinstance(z, str):
                    if z != "NaN":
                        raise ValueError("source_line_geometry_invalid")
                    self.nan_z_vertex_count += 1
                elif z is None:
                    self.null_z_vertex_count += 1
                elif type(z) not in (int, float):
                    raise ValueError("source_line_geometry_invalid")
                if isinstance(m, str):
                    if m != "NaN":
                        raise ValueError("source_line_geometry_invalid")
                    self.nan_m_vertex_count += 1
                elif m is None:
                    self.null_m_vertex_count += 1
                elif type(m) not in (int, float):
                    raise ValueError("source_line_geometry_invalid")
        self._geometries.add([oid, geom["paths"]])
        if nan_paths:
            self.nan_geometry_count += 1
        else:
            # Both dimensions remain in the ISO WKB, including no-value NaNs.
            self.geometry_types.add("MultiLineString ZM" if len(geom["paths"]) > 1 else "LineString ZM")

    @property
    def record_sha256(self) -> str:
        return self._records.hexdigest()

    @property
    def geometry_sha256(self) -> str:
        return self._geometries.hexdigest()

    @property
    def observed_bounds(self) -> dict[str, object] | None:
        if self.xmin is None:
            return None
        return {"xmin": self.xmin, "ymin": self.ymin, "xmax": self.xmax, "ymax": self.ymax}


def _capture_segments(client: httpx.Client, capture_root: Path, ids: list[int], attr_names: list[str],
                      stats: "_StreamStats", receipts: dict[str, dict[str, object]]) -> list[tuple[int, int]]:
    """Capture and stream-validate the complete layer as bisected roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the
    refused segment's final body and receipts stay on disk as evidence under
    its own name. A single-record segment that still fails ends the capture
    explicitly. Each served page is validated against its sorted roster slice
    and fed to the streamed tallies immediately, so memory stays bounded by
    one page regardless of layer size; each settled page's receipt is recorded
    in ``receipts`` for the candidate evidence roster. A reused page that no
    longer matches its roster slice is producer-stale evidence: it is archived
    and re-captured fresh once (the NFHL is continuously updated, and FEMA
    edited this layer during a capture window on 2026-11-12/13).
    """
    total = len(ids)
    segments: deque[tuple[int, int]] = deque(
        (offset, min(SEGMENT_START_RECORDS, total - offset))
        for offset in range(0, total, SEGMENT_START_RECORDS)
    )
    settled: list[tuple[int, int]] = []
    purged: set[str] = set()
    bytes_per_record = 0.0
    while segments:
        offset, count = segments.popleft()
        name = _segment_name(offset, count)
        # Resume the saved bisection tree before probing refused ancestors.
        # Child evidence only chooses request boundaries; every leaf still
        # verifies its receipt and current roster slice below.
        middle = count // 2
        if count > 1 and (count * bytes_per_record > TARGET_SEGMENT_BYTES or any(
                (capture_root / f"{_segment_name(start, width)}.receipt.json").is_file()
                for start, width in ((offset, middle), (offset + middle, count - middle)))) \
                and _reusable_receipt(capture_root, name) is None:
            segments.appendleft((offset + middle, count - middle))
            segments.appendleft((offset, middle))
            continue
        try:
            receipt, reused = _ensure_captured(
                client, _segment_query(offset, count), capture_root, name,
                max_bytes=MAX_CAPTURE_BYTES,
                attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1 else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            # The producer's transfer cap can also surface as a persistent
            # mid-stream reset/oversized-response (observed 2026-11-12 on this
            # layer: 2000-record pages of the densest baselines never complete
            # while the bisected halves serve immediately), so a segment wider
            # than one record that keeps failing to transport completely is
            # bisected exactly like a refused error document; the retained
            # partial evidence stays on disk under the refused segment's name.
            if str(exc).startswith("incomplete_source_capture:") and count > 1:
                middle = count // 2
                segments.appendleft((offset + middle, count - middle))
                segments.appendleft((offset, middle))
                time.sleep(SEGMENT_BISECT_COOLDOWN_SECONDS)
                continue
            raise
        if _body_is_producer_error_doc(capture_root, name):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            segments.appendleft((offset + middle, count - middle))
            segments.appendleft((offset, middle))
            error = json.loads((capture_root / f"{name}.body").read_bytes()).get("error")
            # The observed complete-200 query refusal resolves when its dense
            # payload is split. It is not an HTTP throttle/transport failure;
            # retain backoff for 429/503 and other unclassified errors.
            if not (receipt.get("status") == 200 and isinstance(error, dict)
                    and error.get("code") == 500
                    and error.get("message") in ("Error performing query operation", "Error performing query operation.")):
                time.sleep(SEGMENT_BISECT_COOLDOWN_SECONDS)
            continue
        snapshot = stats.snapshot()
        try:
            page = _read_doc(capture_root, name)
            _validate_page(offset, count, page, ids, attr_names, stats)
        except ValueError as exc:
            stats.restore(snapshot)
            # A reused page whose served content no longer matches its roster
            # slice is stale evidence from a roster generation the producer has
            # since edited (observed 2026-11-12/13: FEMA edited this layer
            # during a capture window, adding 2,254 and removing 2,025
            # features). Its cached evidence is purged and the page is
            # re-captured fresh exactly once; a freshly captured page that
            # still mismatches, or any other page defect, fails the capture.
            if reused and str(exc).startswith("source_page_id_slice_incomplete_or_reordered") \
                    and name not in purged:
                purged.add(name)
                _preserve_failed_attempt(capture_root, name, 1)
                segments.appendleft((offset, count))
                continue
            raise
        bytes_per_record = receipt["bytes"] / count
        settled.append((offset, count))
        receipts[name] = receipt
    return settled


def _validate_page(offset: int, count: int, page: object, ids: list[int], attr_names: list[str],
                   stats: "_StreamStats") -> None:
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
    for row in rows:
        stats.add(row, attr_names)


def _require_pins() -> None:
    if (not EXPECTED_ID_SHA256 or not EXPECTED_SCHEMA_SHA256 or not EXPECTED_RECORD_SHA256
            or not EXPECTED_GEOMETRY_SHA256 or not EXPECTED_NULL_GEOMETRY_ID_SHA256
            or not EXPECTED_OBSERVED_BOUNDS or not EXPECTED_GEOMETRY_TYPES
            or min(EXPECTED_COUNT, EXPECTED_NULL_GEOMETRY_COUNT, EXPECTED_NAN_GEOMETRY_COUNT,
                   EXPECTED_NULL_Z_VERTEX_COUNT, EXPECTED_NAN_Z_VERTEX_COUNT,
                   EXPECTED_NULL_M_VERTEX_COUNT, EXPECTED_NAN_M_VERTEX_COUNT,
                   EXPECTED_EMPTY_GEOMETRY_COUNT) < 0):
        raise ValueError("source_population_pins_not_established")


def _check_population_pins(stats: "_StreamStats") -> None:
    _require_pins()
    if stats.feature_count != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if stats.record_sha256 != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if stats.geometry_sha256 != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if stats.null_geometry_count != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if _sha(_canonical(sorted(stats.null_geometry_ids))) != EXPECTED_NULL_GEOMETRY_ID_SHA256:
        raise ValueError("source_null_geometry_id_roster_drift")
    if stats.empty_geometry_count != EXPECTED_EMPTY_GEOMETRY_COUNT:
        raise ValueError("source_empty_geometry_count_drift")
    if _sha(_canonical(sorted(stats.empty_geometry_ids))) != EXPECTED_EMPTY_GEOMETRY_ID_SHA256:
        raise ValueError("source_empty_geometry_id_roster_drift")
    if stats.nan_geometry_count != EXPECTED_NAN_GEOMETRY_COUNT:
        raise ValueError("source_nan_string_geometry_count_drift")
    if stats.null_z_vertex_count != EXPECTED_NULL_Z_VERTEX_COUNT:
        raise ValueError("source_null_z_vertex_count_drift")
    if stats.nan_z_vertex_count != EXPECTED_NAN_Z_VERTEX_COUNT:
        raise ValueError("source_nan_z_vertex_count_drift")
    if stats.null_m_vertex_count != EXPECTED_NULL_M_VERTEX_COUNT:
        raise ValueError("source_null_m_vertex_count_drift")
    if stats.nan_m_vertex_count != EXPECTED_NAN_M_VERTEX_COUNT:
        raise ValueError("source_nan_m_vertex_count_drift")
    if stats.observed_bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    if sorted(stats.geometry_types) != sorted(EXPECTED_GEOMETRY_TYPES):
        raise ValueError("source_geometry_types_drift")


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    arrow = {"esriFieldTypeOID": pa.int64(),
             "esriFieldTypeGlobalID": pa.string(),
             "esriFieldTypeString": pa.string(),
             "esriFieldTypeDouble": pa.float64()}.get(str(field["type"]))
    if arrow is None:
        raise ValueError("source_field_type_unsupported:" + str(field.get("name")))
    return arrow


def _usable_paths(geom: object) -> list | None:
    """Producer paths when every ordinate converts to a faithful WKB value.

    ``None`` covers null geometry and X/Y ``"NaN"`` string ordinates (records
    that can never be given a coerced location); Z/M ``null`` or string-``"NaN"``
    ordinates remain usable and map to WKB NaN with the audit counts intact.
    """
    if not isinstance(geom, dict) or not (set(geom) == {"paths"}
                                          or (set(geom) == {"hasZ", "hasM", "paths"}
                                              and geom["hasZ"] is SOURCE_HAS_Z
                                              and geom["hasM"] is SOURCE_HAS_M)):
        return None
    paths = geom.get("paths")
    if not isinstance(paths, list) or not paths:
        return None
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            return None
        for vertex in path:
            if not (isinstance(vertex, list) and len(vertex) == 4):
                return None
            x, y = vertex[0], vertex[1]
            if not (type(x) in (int, float) and type(y) in (int, float)):
                return None
    return paths


def _path_vertices(path: list) -> bytes:
    out = bytearray(struct.pack("<I", len(path)))
    for x, y, z, m in path:
        z_value = float("nan") if (z is None or z == "NaN") else float(z)
        m_value = float("nan") if (m is None or m == "NaN") else float(m)
        out += struct.pack("<dddd", float(x), float(y), z_value, m_value)
    return bytes(out)


def _polyline_wkb_xyzm(type_code: int, paths: list) -> bytes:
    """ISO WKB XYZM for one LineString or MultiLineString.

    A LineString body is the vertex count plus the vertices; a MultiLineString
    body is the part count plus one full nested ISO LineString (byte-order
    marker, type 3002, vertex count, vertices) per part.
    """
    if type_code == ISO_LINESTRING_ZM:
        return b"\x01" + struct.pack("<I", type_code) + _path_vertices(paths[0])
    out = bytearray(b"\x01")
    out += struct.pack("<II", type_code, len(paths))
    for path in paths:
        out += b"\x01" + struct.pack("<I", ISO_LINESTRING_ZM)
        out += _path_vertices(path)
    return bytes(out)


def _line_wkb(geom: object) -> bytes | None:
    """ISO WKB XYZM for a producer polyline geometry, or ``None`` when unusable.

    ``None`` is reserved for null source geometry and X/Y ``"NaN"`` planar
    ordinates; producer ``null``/``"NaN"`` Z/M ordinates map to WKB NaN (the
    OGC no-value measure convention) and are counted in the audit, never
    silently dropped or repaired.
    """
    paths = _usable_paths(geom)
    if paths is None:
        return None
    if len(paths) == 1:
        return _polyline_wkb_xyzm(ISO_LINESTRING_ZM, [paths[0]])
    return _polyline_wkb_xyzm(ISO_MULTILINESTRING_ZM, paths)


class _GeometryWkbType(pa.ExtensionType):
    """Arrow's geoarrow.wkb mapping emits Parquet's native GEOMETRY annotation.

    GeoParquet 2.0 requires this physical annotation; file metadata alone is
    insufficient: https://geoparquet.org/releases/v2.0.0/
    No global extension registration is needed to write this type.
    """

    def __init__(self, crs: dict[str, object]) -> None:
        self.crs = crs
        super().__init__(pa.binary(), "geoarrow.wkb")

    def __arrow_ext_serialize__(self) -> bytes:
        return _canonical({"crs": self.crs, "edges": "planar"})

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, metadata):
        return cls(json.loads(metadata)["crs"])


def _parquet_from_pages(capture_root: Path, segments: list[tuple[int, int]],
                        fields: list[Mapping[str, object]]) -> dict[str, object]:
    """Assemble the candidate GeoParquet in one streamed pass over page bodies.

    One row group per captured page keeps memory bounded by a single page;
    the resulting file is read back once for the staged candidate.
    """
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geometry_type = _GeometryWkbType(crs)
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = geometry_type if name == "SHAPE" else _arrow_type(field)
        schema_fields.append(pa.field(name, typ, nullable=True))
    geo = {"version": "2.0.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": sorted(EXPECTED_GEOMETRY_TYPES or []),
        "crs": crs, "edges": "planar"}}}
    schema = pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)})
    path = capture_root / f"{DATASET_SLUG}.parquet"
    _safe_local_key(StagingStorageResource(use_local=True, local_dir=str(capture_root), prefix=""),
                    path.name, required=False)
    writer = pq.ParquetWriter(path, schema, compression="zstd")
    try:
        for offset, count in segments:
            page = json.loads((capture_root / (_segment_name(offset, count) + ".body")).read_bytes())
            rows = page["features"]
            columns = {}
            for field in fields:
                name = field["name"]
                if name == "SHAPE":
                    values = [_line_wkb(row.get("geometry")) for row in rows]
                    columns[name] = pa.ExtensionArray.from_storage(geometry_type, pa.array(values, type=pa.binary()))
                    continue
                else:
                    values = [row["attributes"].get(name) for row in rows]
                columns[name] = pa.array(values, type=schema.field(name).type)
            table = pa.Table.from_pydict(columns, schema=schema)
            if shutil.disk_usage(capture_root).free < MIN_FREE_BYTES + table.nbytes:
                raise ValueError("disk_floor_before_parquet_write")
            writer.write_table(table)
    finally:
        writer.close()
    return {"bytes": path.stat().st_size, "sha256": sha256_file(path), "path": path}


class _DiskCaptures(abc.Mapping):
    """Lazy view of completed capture evidence for quarantine on failure."""

    def __init__(self, capture_root: Path, receipts: Mapping[str, Mapping[str, object]]) -> None:
        self._capture_root = Path(capture_root)
        self._receipts = dict(receipts)

    def __iter__(self):
        return iter(sorted(self._receipts))

    def __len__(self) -> int:
        return len(self._receipts)

    def __getitem__(self, name: str) -> tuple[bytes, Mapping[str, object]]:
        return (self._capture_root / f"{name}.body").read_bytes(), self._receipts[name]


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _write_immutable_file(storage: StagingStorageResource, key: str, source_path: Path,
                          descriptor: Mapping[str, object]) -> None:
    """Stream a local file through the storage resource's conditional copy.

    The temporary copy is independent of the mutable capture file. Creation
    remains exclusive and every existing/new object is checked at its snapshot.
    """
    _safe_local_key(storage, key, required=False)
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        origin = StagingStorageResource(use_local=True, local_dir=str(source_path.parent), prefix="")
        _safe_local_key(origin, source_path.name)
        source_snapshot = origin.object_snapshot(source_path.name)
        if source_snapshot is None or {"bytes": source_snapshot.size, "sha256": source_snapshot.sha256} != descriptor:
            raise ValueError("candidate_source_file_changed:" + key)
        origin.copy_key_to_if_unchanged(storage, source_path.name, key,
                                        source_snapshot=source_snapshot, destination_snapshot=None)
        snapshot = storage.object_snapshot(key)
    if snapshot is None or {"bytes": snapshot.size, "sha256": snapshot.sha256} != descriptor \
            or storage.object_snapshot(key) != snapshot:
        raise ValueError("immutable_staged_object_changed:" + key)


def _write_candidate_outputs(storage: StagingStorageResource, prefix: str, capture_root: Path,
                             receipts: Mapping[str, Mapping[str, object]], parquet: Mapping[str, object],
                             manifest: Mapping[str, object], audit: Mapping[str, object],
                             identity: Mapping[str, object]) -> dict[str, dict[str, object]]:
    """Stage every candidate object, streaming raw evidence from disk."""
    descriptors: dict[str, dict[str, object]] = {}
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    descriptors[rel] = {"bytes": parquet["bytes"], "sha256": parquet["sha256"]}
    files = {rel: (Path(parquet["path"]), descriptors[rel])}
    for name, receipt in sorted(receipts.items()):
        for suffix, descriptor in (
                (".body", {"bytes": receipt["bytes"], "sha256": receipt["sha256"]}),
                (".wire", {"bytes": receipt["wire_bytes"], "sha256": receipt["wire_sha256"]}),
                (".receipt.json", {"bytes": len(_canonical(dict(receipt))) + 1,
                                   "sha256": _sha(_canonical(dict(receipt)) + b"\n")})):
            files[f"raw/{name}{suffix}"] = (capture_root / f"{name}{suffix}", descriptor)
    small_outputs = {
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(
            {"approved": False, "version": audit["candidate_version"],
             "objects": {rel: descriptors[rel]},
             "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}) + b"\n",
    }
    # Count only bytes not already staged. Conditional writes still validate
    # every existing object; this preflight grants no cache/source acceptance.
    remaining = sum(descriptor["bytes"] for name, (_, descriptor) in files.items()
                    if storage.object_snapshot(f"{prefix}/{name}") is None)
    remaining += sum(len(body) for name, body in small_outputs.items()
                     if storage.object_snapshot(f"{prefix}/{name}") is None)
    if shutil.disk_usage(Path(storage.local_dir)).free < remaining + MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_staging")
    for name, (path, descriptor) in files.items():
        _write_immutable_file(storage, f"{prefix}/{name}", path, descriptor)
        descriptors[name] = dict(descriptor)
    for name, body in small_outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
    return descriptors


def _verify_staged_descriptors(storage: StagingStorageResource, prefix: str,
                               descriptors: Mapping[str, Mapping[str, object]]) -> None:
    for name, descriptor in sorted(descriptors.items()):
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)


def capture_nfhl_profile_baselines(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Profile Baselines candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    _require_pins()
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    if capture_directory:
        capture_root = Path(capture_directory).absolute()
        if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
            raise ValueError("unsafe_capture_directory")
        capture_root.mkdir(parents=True, exist_ok=True)
    else:
        attempt_root = Path("/tmp/hifld-native-reconstruction/208/captures")
        capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
        if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
            raise ValueError("unsafe_capture_directory")
        capture_root.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, dict[str, object]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-profile-baselines-draft"}) as client:
            for name, url in [(_SEED_DOC, _ids_query()), *_doc_urls()]:
                receipts[name] = _ensure_captured(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                  attempts=DOC_CAPTURE_ATTEMPTS,
                                                  base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
                                                  reuse=False)[0]
            docs = {name: _read_doc(capture_root, name)
                    for name in (_SEED_DOC, "service_before", "layer_before", "count_before", "ids_before")}
            docs["service_doc"] = (capture_root / "service_doc.body").read_bytes()
            contract = _contract_from_docs(docs)
            ids = contract["ids"]
            attr_names = [field["name"] for field in contract["fields"] if field["name"] != "SHAPE"]
            stats = _StreamStats()
            segments = _capture_segments(client, capture_root, ids, attr_names, stats, receipts)
            for name, url in _after_doc_urls():
                receipts[name] = _ensure_captured(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                  attempts=DOC_CAPTURE_ATTEMPTS,
                                                  base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
                                                  reuse=False)[0]
            docs_after = {name: _read_doc(capture_root, name) for name in _AFTER_DOC_NAMES}
            docs_after["count_before"] = docs["count_before"]
            _validate_after_docs(docs_after, contract)
        _check_population_pins(stats)
    except Exception as exc:
        failure_receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                failure_receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                failure_receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": failure_receipts}) + b"\n")
        if failure_receipts:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _DiskCaptures(capture_root, failure_receipts), type(exc).__name__)
        raise
    identity = _code_identity()
    version = content_derived_version(stats.record_sha256, stats.geometry_sha256,
                                      contract["schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": stats.feature_count,
                    "source_record_sha256": stats.record_sha256,
                    "source_geometry_sha256": stats.geometry_sha256,
                    "receipts": dict(sorted(receipts.items()))}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, _sha(_canonical(identity))))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                _sha(_canonical(identity)), identity, stats.record_sha256,
                                contract["schema_sha256"], objects, payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": stats.feature_count, "source_null_geometry_count": stats.null_geometry_count,
                "source_empty_geometry_count": stats.empty_geometry_count,
                "source_nan_string_geometry_count": stats.nan_geometry_count,
                "source_null_m_vertex_count": stats.null_m_vertex_count,
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    parquet = _parquet_from_pages(capture_root, segments, contract["fields"])
    release_date_reason = ("FEMA updates the NFHL continuously and publishes no release date for this layer; "
                           "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates.")
    page_times = [datetime.fromisoformat(receipt["observed_at"]) for name, receipt in receipts.items()
                  if _SEGMENT_NAME_PATTERN.fullmatch(name)]
    audit = {"source_page_observed_at_min": min(page_times).isoformat(),
             "source_page_observed_at_max": max(page_times).isoformat(),
             "source_atomic_snapshot_established": False,
             "source_temporal_contract": "Receipt-dated REST pages; no atomic snapshot, as-of or national attribute-freshness guarantee.",
             "source_feature_count": stats.feature_count, "source_id_sha256": EXPECTED_ID_SHA256,
             "source_record_sha256": stats.record_sha256, "source_geometry_sha256": stats.geometry_sha256,
             "source_schema_sha256": contract["schema_sha256"], "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polyline", "source_has_z": SOURCE_HAS_Z, "source_has_m": SOURCE_HAS_M,
             "source_geometry_representation": "published_default_rest_linear_xyzm",
             "backing_geodatabase_true_curve_equivalence_established": False,
             "source_height_model": EXPECTED_SOURCE_HEIGHT_MODEL,
             "null_geometry_count": stats.null_geometry_count,
             "null_geometry_object_ids": sorted(stats.null_geometry_ids),
             "empty_geometry_count": stats.empty_geometry_count,
             "empty_geometry_object_ids": sorted(stats.empty_geometry_ids),
             "nan_string_geometry_count": stats.nan_geometry_count,
             "null_z_vertex_count": stats.null_z_vertex_count,
             "nan_z_vertex_count": stats.nan_z_vertex_count,
             "null_m_vertex_count": stats.null_m_vertex_count,
             "nan_m_vertex_count": stats.nan_m_vertex_count,
             "observed_bounds": stats.observed_bounds, "declared_layer_extent": contract["declared_extent"],
             "observed_geometry_types": sorted(stats.geometry_types),
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             "source_release_date": None, "source_release_date_unknown_reason": release_date_reason}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Profile Baselines (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 17 'Profile Baselines', captured "
                       "completely from the producer's own REST publication with returnZ/returnM so every vertex "
                       "carries the producer's four ordinates. Complete source attributes and NAD83 (EPSG:4269) "
                       "polyline geometry with NAVD88 ftUS heights (Z) and route measures (M) are represented in "
                       "the candidate (ISO WKB XYZM bytes; the advisory GeoParquet geometry_types declare the "
                       "four-ordinate ' ZM' labels). The NFHL is continuously updated and publishes no "
                       "per-snapshot release "
                       "date; freshness, rights, public schema/ID contract, and release acceptance remain "
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
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "published_fema_rest_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "query": "Publisher default REST linear representation (returnTrueCurves "
                                                     "omitted/default false); no client densification or repair; "
                                                     "national backing-geodatabase true-curve equivalence unverified. "
                                                     "where 1=1, all producer fields, geometry requested with "
                                                     "returnZ=true and returnM=true because the layer declares Z "
                                                     "(gravity-related NAVD88 height, US survey feet) and M "
                                                     "(route measure); every vertex is captured as the producer's "
                                                     "[x, y, z, m] ordinates",
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's derived "
                                                             "SHAPE.STLength() double and the baseline attributes "
                                                             "(DFIRM_ID, VERSION_ID, BASELN_ID, WTR_NM, SEGMT_NAME, "
                                                             "WATER_TYP, STUDY_TYP, SHOWN_FIRM, R_ST_DESC, "
                                                             "R_END_DESC, V_DATM_OFF, DATUM_UNIT, FLD_PROB1-3, "
                                                             "SPEC_CONS1-2, START_ID, SOURCE_CIT); SHAPE is the "
                                                             "original geometry field represented as ISO WKB XYZM",
                                            "source_ids": "Preserve producer OBJECTID, BASELN_ID, GFID and "
                                                          "GlobalID values unchanged; no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polyline paths retained as ISO WKB "
                                                        "LineString ZM / MultiLineString ZM without "
                                                        "projection; producer JSON null or string-'NaN' Z/M "
                                                        "ordinates map to WKB NaN (the OGC no-value measure "
                                                        "convention) with each such vertex counted in the audit, "
                                                        "while the raw paths in the record digest keep the "
                                                        "producer's original values; X/Y string-'NaN' vertices "
                                                        "and null geometry are retained as records with null "
                                                        "candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 17 'Profile Baselines' of that producer "
                   "service. The HIFLD Open inventory lists this dataset as nfhl/profile-baselines with DataLumos "
                   "project 239985; that ICPSR archive copy and the 2024-10-31 archive modification date are "
                   "identification and comparison context only, never pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and four-ordinate geometry "
                      "retained in the producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": f"geoparquet/{DATASET_SLUG}.parquet",
                         "bytes": parquet["bytes"], "sha256": parquet["sha256"]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit["source_fields"] = [_plain_field(f) for f in contract["fields"]]
    audit["official_layer_name"] = LAYER_NAME
    audit["layer_id"] = LAYER_ID
    audit["service_url"] = SERVICE_URL
    audit["release_date_unknown"] = True
    audit["freshness_accepted"] = False
    audit["schema_contract_accepted"] = False
    audit["id_contract_accepted"] = False
    audit["rights_accepted"] = False
    audit["mapping_approved"] = False
    audit["release_accepted"] = False
    audit["candidate_version"] = content_derived_version(stats.record_sha256, stats.geometry_sha256,
                                                         contract["schema_sha256"], identity)
    version = audit["candidate_version"]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    descriptors = _write_candidate_outputs(storage, prefix, capture_root, receipts, parquet, manifest, audit,
                                           identity)
    _verify_staged_descriptors(storage, prefix, descriptors)
    payload = {"processing_identity": identity,
               "source_record_sha256": stats.record_sha256,
               "source_schema_sha256": contract["schema_sha256"],
               "capture_receipts": dict(sorted(receipts.items())),
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, _sha(_canonical(identity)), payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": stats.feature_count, "source_null_geometry_count": stats.null_geometry_count,
            "source_empty_geometry_count": stats.empty_geometry_count,
            "source_nan_string_geometry_count": stats.nan_geometry_count,
            "source_null_m_vertex_count": stats.null_m_vertex_count,
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root), "reused": False}
