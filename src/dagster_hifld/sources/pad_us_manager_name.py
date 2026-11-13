"""Streaming, approval-gated capture of the USGS PAD-US Manager Name layer.

The HIFLD Open inventory entry ``pad-us-manager-name`` ("PAD-US Manager Name",
agency US Geological Survey, publisher U.S. Geological Survey (USGS) Gap
Analysis Project (GAP)) names the USGS PAD-US Manager Name publication. The
producer's maintained publication for this entry is USGS's companion ArcGIS
Online item ``d1c464bac63d44bc88a4a059dc69d259`` ("PAD-US Manager Name", owned
by the USGS organizational account ``mcroft@usgs.gov_USUS``) whose
``Manager_Name_PADUS`` FeatureServer layer 0 ``PADUS4_1ManagerName`` carries
the PAD-US 4.1 data release (DOI 10.5066/P96WBCHS, cited verbatim in the
service copyright text) Combined feature class with the manager-name mapping
fields. That producer service is captured here. The DataLumos project 240684
archive copy is identification context only and is never fetched.

Why the release ZIP is corroboration, not the pipeline input: the official
PAD-US 4.1 national geodatabase (ScienceBase item ``652d4fc5d34e44db0e2ee45e``,
1,523,434,496 bytes, SHA-256 ``62ed113335e359509ad68a731146c74aa873c5a92da4f3
fda572e54bc9b823ab``, probed 2026-11-13) holds the Combined feature class with
656,986 rows and a different field roster (Term and Duration present;
EsmtHldr, EHoldTyp, WebServiceMangName, TranspPct and the service Shape__
doubles absent), while this publication's layer holds 655,335 rows with those
service-derived fields. The ZIP therefore cannot reconstruct this dataset's
content; it is recorded as dated producer-lineage corroboration only.

The layer serves 655,335 polygon features (maxRecordCount 2000, sparse
OBJECTID roster enumerated by ``returnIdsOnly``), so this module streams the
source in the native ``f=pbf`` binary pages the service publishes (a full
``f=json`` enumeration would be tens of gigabytes): every roster segment is
retained as raw wire evidence in a resumable capture directory, decoded by the
repository's bounded FeatureCollection-PBF decoder, and re-verified before any
candidate is sealed. Segments are adaptively bisected when the producer
refuses, must tile the complete roster exactly, and before/after count/roster/
service/layer brackets prove nothing was skipped, duplicated, or reordered.
The PBF transport carries the service's own 1e-9-degree coordinate
quantization; raw PBF pages are the authoritative transport evidence and the
decoded coordinates are what the service itself serves in this format.

Per-value and per-geometry baseline digests, geometry gate counts and observed
bounds are computed by ``build_pad_us_manager_name_baseline`` from a completed
capture directory and pinned as constants; unpinned values are checked only
once pinned. Publication stays blocked pending the succession, freshness,
schema/ID contract, rights and release decisions; all runtime approval flags
are false and the draft refresh schedule is stopped.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import time
from array import array
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.esri_pbf import decode_feature_collection_pbf
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "pad-us-manager-name"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Geological Survey (USGS) Gap Analysis Project (GAP)"
AGENCY = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/pad-us-manager-name-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SERVICE_HOST = "https://services.arcgis.com"
SERVICE_URL = (SERVICE_HOST + "/v01gqwM5QqNysAAi/arcgis/rest/services/"
               "Manager_Name_PADUS/FeatureServer")
LAYER_ID = 0
LAYER_NAME = "PADUS4_1ManagerName"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolygon"
AGOL_ITEM_ID = "d1c464bac63d44bc88a4a059dc69d259"
AGOL_ITEM_TITLE = "PAD-US Manager Name"
AGOL_ITEM_OWNER = "mcroft@usgs.gov_USGS"
AGOL_ITEM_URL = (f"https://www.arcgis.com/sharing/rest/content/items/{AGOL_ITEM_ID}?f=json")
SB_ITEM_ID = "652d4fc5d34e44db0e2ee45e"
SB_ITEM_URL = f"https://www.sciencebase.gov/catalog/item/{SB_ITEM_ID}?format=json"
RELEASE_DOI = "https://doi.org/10.5066/P96WBCHS"
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
MAX_RECORD_COUNT = 2000
SEGMENT_START_RECORDS = 2000
MAX_BODY_BYTES = 24 * 1024 * 1024
MAX_CAPTURE_BYTES = 12 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 20_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 3
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
EXPECTED_COPYRIGHT_TEXT = ("U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2025, "
                           "Protected Areas Database of the United States (PAD-US) 4.1: "
                           "U.S. Geological Survey data release, "
                           "https://doi.org/10.5066/P96WBCHS")
EXPECTED_COUNT = 655_335
EXPECTED_IDS_SHA256 = "c796d962b10cfb278175ca0076a528414fcd6241514a7f75dd6980c430b3153a"
EXPECTED_SCHEMA_SHA256 = "700b505d62ec00dda9ca8bb23a44283f928be1167971bd409a6477b83a4e7510"
EXPECTED_CORROBORATION_COUNT = 656_986
EXPECTED_ZIP_BYTES = 1_523_434_496
EXPECTED_ZIP_SHA256 = "62ed113335e359509ad68a731146c74aa873c5a92da4f3fda572e54bc9b823ab"
# The per-value and per-geometry baselines, geometry gate counts and observed
# bounds are pinned from the dated complete capture (see the module
# docstring); unpinned values ("", None) are checked only once pinned.
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_NAN_GEOMETRY_COUNT = None
EXPECTED_EMPTY_GEOMETRY_COUNT = None
EXPECTED_UNUSABLE_GEOMETRY_COUNTS: dict[str, int] | None = None
EXPECTED_OBSERVED_BOUNDS: dict[str, float] | None = None
EXPECTED_LAYER_EXTENT = {"xmin": -20037507.0672, "ymin": -1733741.441300001,
                         "xmax": 20037507.067199998, "ymax": 12930675.6069}
BASELINE_PENDING_NOTE = (
    "The record and geometry baseline digests, geometry gate counts and "
    "observed bounds are pinned from a completed resumable capture directory "
    "by build_pad_us_manager_name_baseline; unpinned values are checked only "
    "once pinned.")
# Service-declared native field roster (layer 0 f=pjson, observed 2026-11-13);
# the PBF page schema must reproduce this exact name order.
NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("FeatClass", "esriFieldTypeString", True, 12),
    ("Category", "esriFieldTypeString", True, 50),
    ("Own_Type", "esriFieldTypeString", True, 50),
    ("Own_Name", "esriFieldTypeString", True, 70),
    ("Loc_Own", "esriFieldTypeString", True, 250),
    ("Mang_Type", "esriFieldTypeString", True, 50),
    ("Mang_Name", "esriFieldTypeString", True, 70),
    ("Loc_Mang", "esriFieldTypeString", True, 250),
    ("Des_Tp", "esriFieldTypeString", True, 75),
    ("Loc_Ds", "esriFieldTypeString", True, 250),
    ("Unit_Nm", "esriFieldTypeString", True, 250),
    ("Loc_Nm", "esriFieldTypeString", True, 250),
    ("State_Nm", "esriFieldTypeString", True, 50),
    ("Agg_Src", "esriFieldTypeString", True, 150),
    ("GIS_Src", "esriFieldTypeString", True, 200),
    ("Src_Date", "esriFieldTypeString", True, 15),
    ("GIS_Acres", "esriFieldTypeInteger", True, None),
    ("Source_PAID", "esriFieldTypeString", True, 100),
    ("WDPA_Cd", "esriFieldTypeInteger", True, None),
    ("Pub_Access", "esriFieldTypeString", True, 20),
    ("Access_Src", "esriFieldTypeString", True, 150),
    ("Access_Dt", "esriFieldTypeString", True, 4),
    ("GAP_Sts", "esriFieldTypeString", True, 95),
    ("GAPCdSrc", "esriFieldTypeString", True, 150),
    ("GAPCdDt", "esriFieldTypeString", True, 4),
    ("IUCN_Cat", "esriFieldTypeString", True, 70),
    ("IUCNCtSrc", "esriFieldTypeString", True, 150),
    ("IUCNCtDt", "esriFieldTypeString", True, 4),
    ("Date_Est", "esriFieldTypeString", True, 4),
    ("Comments", "esriFieldTypeString", True, 255),
    ("EsmtHldr", "esriFieldTypeString", True, 100),
    ("EHoldTyp", "esriFieldTypeString", True, 50),
    ("WebServiceMangName", "esriFieldTypeString", True, 150),
    ("TranspPct", "esriFieldTypeSmallInteger", True, None),
    ("Shape__Area", "esriFieldTypeDouble", True, None),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
)
METADATA_NAMES = ("sb_item", "agol_item", "service_before", "layer_before",
                  "count_before", "ids_seed", "ids_before")
AFTER_NAMES = ("layer_after", "service_after", "count_after", "ids_after")
GEOMETRY_COLUMN = "geometry"
UNUSABLE_GEOMETRY_MODES = frozenset({
    "source_polygon_ring_degenerate", "source_polygon_exterior_missing",
    "source_polygon_ring_self_intersecting", "source_polygon_hole_unassigned",
})

_ARROW_TYPES: dict[str, pa.DataType] = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int32(),
    "esriFieldTypeSmallInteger": pa.int16(),
    "esriFieldTypeSingle": pa.float32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.int64(),
}


class PadUsManagerNameError(ValueError):
    """The source snapshot cannot be represented as the pinned candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise PadUsManagerNameError("source_field_roster_drift")
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
                   "outSR": str(SOURCE_CRS_WKID),
                   "orderByFields": "OBJECTID ASC", "resultOffset": str(offset),
                   "resultRecordCount": str(count), "f": "pbf"})


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise PadUsManagerNameError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str) -> list[int]:
    if not isinstance(roster, list) or len(roster) != EXPECTED_COUNT \
            or any(type(value) is not int for value in roster) \
            or len(set(roster)) != EXPECTED_COUNT:
        raise PadUsManagerNameError("source_id_population_drift:" + label)
    ids = sorted(roster)
    if EXPECTED_IDS_SHA256 and _sha(_canonical(ids)) != EXPECTED_IDS_SHA256:
        raise PadUsManagerNameError("source_id_roster_drift:" + label)
    return ids


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise PadUsManagerNameError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise PadUsManagerNameError("service_copyright_release_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise PadUsManagerNameError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise PadUsManagerNameError("exact_padus_manager_layer_metadata_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise PadUsManagerNameError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise PadUsManagerNameError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise PadUsManagerNameError("source_record_limit_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise PadUsManagerNameError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise PadUsManagerNameError("source_layer_extent_drift")
    return _profile_schema(layer)


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise PadUsManagerNameError(
            "source_field_type_unsupported:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise PadUsManagerNameError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise PadUsManagerNameError("source_native_type_changed:" + name)
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise PadUsManagerNameError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise PadUsManagerNameError("source_native_type_changed:" + name)
        return
    raise PadUsManagerNameError("unsupported_source_physical_type:" + esri_type)


def _signed_area(ring: list) -> float:
    """Signed area of a planar ring (positive counterclockwise), translated origin."""
    closed = ring if ring[0] == ring[-1] else ring + [ring[0]]
    x0, y0 = closed[0][0], closed[0][1]
    area = 0.0
    for (x1, y1), (x2, y2) in zip(closed, closed[1:]):
        area += (x1 - x0) * (y2 - y0) - (x2 - x0) * (y1 - y0)
    return area / 2.0


def _polygon_wkb(geometry: object) -> bytes:
    """Assemble one decoded Esri JSON rings value into 2D polygon WKB.

    Esri JSON polygon semantics: rings are closed, clockwise rings are
    exteriors, counterclockwise rings are holes, and a feature may hold several
    exteriors. Assembly is the standard producer reading, not a geometric
    interpretation: exteriors with no claimable hole keep their ring order and
    holes join their smallest containing exterior. Decoded producer vertices
    are retained verbatim (no reorientation, resampling, or projection). Rows
    whose rings cannot form a valid OGC polygon are reported through the
    UNUSABLE_GEOMETRY_MODES gates instead of silently coercing a polygon.
    """
    if not isinstance(geometry, dict) or set(geometry) != {"rings"}:
        raise PadUsManagerNameError("unexpected_source_geometry_members")
    rings = geometry["rings"]
    if not isinstance(rings, list) or not rings:
        raise PadUsManagerNameError("source_polygon_geometry_invalid")
    for ring in rings:
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise PadUsManagerNameError("source_polygon_ordinate_contract_drift")
            x, y = vertex
            if not (type(x) in (int, float) and type(y) in (int, float)):
                raise PadUsManagerNameError("source_polygon_ordinate_not_numeric")
            if not all(value == value and -1e308 < value < 1e308 for value in (x, y)):
                raise PadUsManagerNameError("source_polygon_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise PadUsManagerNameError("source_coordinate_range_drift")
    exteriors, holes = [], []
    for ring in rings:
        area = _signed_area(ring)
        if area == 0:
            raise PadUsManagerNameError("source_polygon_ring_degenerate")
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise PadUsManagerNameError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    for shell, _area, _index in shells:
        if not shell.is_valid:
            raise PadUsManagerNameError("source_polygon_ring_self_intersecting")
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        polygon = Polygon(hole)
        candidates = [(abs(area), index) for shell, area, index in shells
                      if shell.contains(polygon.representative_point())]
        if not candidates:
            raise PadUsManagerNameError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for _shell, _area, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      expected_objectid: int) -> tuple[int, object]:
    """Validate one decoded feature's attributes and polygon geometry structure.

    Returns the OBJECTID and the verbatim decoded rings (or None when the
    feature has no geometry). Ordinate validation happens here so every
    retained record digest is computed from a structurally sound feature; the
    WKB assembly runs in the caller.
    """
    if not isinstance(feature, dict):
        raise PadUsManagerNameError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(expected_names):
        raise PadUsManagerNameError("source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise PadUsManagerNameError("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise PadUsManagerNameError("source_page_id_slice_incomplete_or_reordered")
    rings: list | None = None
    if geometry is not None:
        if not isinstance(geometry, dict) or set(geometry) != {"rings"}:
            raise PadUsManagerNameError("unexpected_source_geometry_members")
        rings = geometry["rings"]
        if not isinstance(rings, list):
            raise PadUsManagerNameError("source_polygon_geometry_invalid")
        for ring in rings:
            if not isinstance(ring, list) or len(ring) < 4:
                raise PadUsManagerNameError("source_polygon_ring_too_short")
            for vertex in ring:
                if not isinstance(vertex, list) or len(vertex) != 2:
                    raise PadUsManagerNameError("source_polygon_ordinate_contract_drift")
                x, y = vertex
                if x == "NaN" and y == "NaN":
                    continue
                if not (type(x) in (int, float) and type(y) in (int, float)):
                    raise PadUsManagerNameError("source_polygon_ordinate_not_numeric")
                if not all(value == value and -1e308 < value < 1e308 for value in (x, y)):
                    raise PadUsManagerNameError("source_polygon_ordinate_not_finite")
                if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                    raise PadUsManagerNameError("source_coordinate_range_drift")
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    return objectid, rings


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PadUsManagerNameError("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise PadUsManagerNameError("retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = _load_receipt(capture_root, name)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise PadUsManagerNameError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"segments": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PadUsManagerNameError("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("segments"), list):
        raise PadUsManagerNameError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def _witness_aside(capture_root: Path, name: str) -> None:
    """Move a retained refused/torn response aside so the slot can be refetched."""
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def _retained_or_fetch(client: httpx.Client, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except PadUsManagerNameError:
            _witness_aside(capture_root, name)
    return _capture_with_retry(client, url, capture_root, name,
                               attempts=DOC_CAPTURE_ATTEMPTS,
                               base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    return isinstance(document, dict) and "error" in document


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures."""
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
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


def _plan_segment_gaps(capture_root: Path, roster: list[int]) -> list[tuple[int, int]]:
    """Plan the roster gaps that still need a capture this session.

    A retained segment counts as accepted only when its receipt and body
    verify, its body decodes as a complete PBF page, and it already sits in
    the capture state; anything else is witnessed aside and refetched.
    """
    state = _load_state(capture_root)
    accepted_names = {entry.get("name") for entry in state["segments"]
                      if isinstance(entry, dict)}
    retained: list[tuple[int, int]] = []
    for path in sorted(capture_root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        match = _SEGMENT_NAME_PATTERN.fullmatch(name)
        if match is None:
            continue
        try:
            body, _receipt = _read_capture(capture_root, name)
        except PadUsManagerNameError:
            _witness_aside(capture_root, name)
            continue
        if name not in accepted_names:
            _witness_aside(capture_root, name)
            continue
        try:
            page = decode_feature_collection_pbf(body, allow_transfer_limit=True)
        except ValueError:
            _witness_aside(capture_root, name)
            continue
        offset, count = _parse_segment_name(name)
        if offset + count > len(roster) or \
                [row.get("attributes", {}).get("OBJECTID") for row in page["features"]] \
                != roster[offset:offset + count]:
            _witness_aside(capture_root, name)
            continue
        retained.append((offset, count))
    gaps: list[tuple[int, int]] = []
    covered = bytearray(EXPECTED_COUNT)
    for offset, count in sorted(retained):
        if offset < 0 or count < 1 or offset + count > EXPECTED_COUNT \
                or any(covered[offset:offset + count]):
            _witness_aside(capture_root, _segment_name(offset, count))
            continue
        covered[offset:offset + count] = b"\x01" * count
    cursor = 0
    while cursor < EXPECTED_COUNT:
        if covered[cursor]:
            cursor += 1
            continue
        start = cursor
        while cursor < EXPECTED_COUNT and not covered[cursor]:
            cursor += 1
        chunk_start = start
        while chunk_start < cursor:
            count_chunk = min(SEGMENT_START_RECORDS, cursor - chunk_start)
            gaps.append((chunk_start, count_chunk))
            chunk_start += count_chunk
    return gaps


def _fetch_segments(client: httpx.Client, capture_root: Path, roster: list[int],
                    state: dict[str, object]) -> None:
    """Capture the complete layer as adaptively bisected roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing is bisected until the producer serves both halves, whether the
    refusal is a persistent error document or a persistent incomplete transport
    capture. A single-record segment that still fails ends the capture
    explicitly.
    """
    segments = state.setdefault("segments", [])
    accepted = {entry.get("name") for entry in segments if isinstance(entry, dict)}
    todo: deque[tuple[int, int]] = deque(_plan_segment_gaps(capture_root, roster))
    while todo:
        offset, count = todo.popleft()
        name = _segment_name(offset, count)
        if name in accepted and (capture_root / f"{name}.body").exists():
            continue
        try:
            body, receipt = _capture_with_retry(
                client, _segment_query(offset, count), capture_root, name,
                attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1
                else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise PadUsManagerNameError(
                "source_capture_failed:" + type(exc).__name__) from exc
        if _producer_error_doc(body):
            _witness_aside(capture_root, name)
            if count == 1:
                raise PadUsManagerNameError("source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        try:
            page = decode_feature_collection_pbf(body, allow_transfer_limit=True)
        except ValueError as exc:
            _witness_aside(capture_root, name)
            raise PadUsManagerNameError("source_page_decode_failed:" + name) from exc
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) != count:
            _witness_aside(capture_root, name)
            raise PadUsManagerNameError("source_page_row_count_incomplete:" + name)
        expected = roster[offset:offset + count]
        actual = [row.get("attributes", {}).get("OBJECTID") for row in rows
                  if isinstance(row, dict)]
        if actual != expected:
            _witness_aside(capture_root, name)
            raise PadUsManagerNameError("source_page_id_slice_incomplete_or_reordered")
        if name not in accepted:
            segments.append({"name": name, "offset": offset, "count": count,
                             "features": len(rows), "captured_at": receipt["observed_at"],
                             "body_sha256": _sha(body)})
            accepted.add(name)
            _save_state(capture_root, state)


def fetch_pad_us_manager_name_pages(capture_root: Path, *, client: httpx.Client | None = None,
                                    doc_client: httpx.Client | None = None) \
        -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required segment
    and document.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    owner_client = client is None
    owner_doc = doc_client is None
    active = client or _default_client()
    docs_active = doc_client or _doc_client()
    segments: list[dict[str, object]] = list(state.get("segments", []))
    first_captured_at = (segments[0]["captured_at"] if segments
                         else datetime.now(timezone.utc).isoformat())
    try:
        for name in METADATA_NAMES:
            url = {"sb_item": SB_ITEM_URL,
                   "agol_item": AGOL_ITEM_URL,
                   "service_before": SERVICE_URL + "?f=pjson",
                   "layer_before": LAYER_URL + "?f=pjson",
                   "count_before": _count_url(),
                   "ids_seed": _ids_url(),
                   "ids_before": _ids_url()}[name]
            active_client = docs_active if name in ("sb_item", "agol_item") else active
            body, _receipt = _retained_or_fetch(active_client, capture_root, name, url)
            if name not in ("sb_item", "agol_item") and _producer_error_doc(body):
                raise PadUsManagerNameError("source_page_error:" + name)
        _validate_corroboration_documents(capture_root)
        seed = json.loads(_read_capture(capture_root, "ids_seed")[0]).get("objectIds")
        ids = _validate_roster(seed, "ids_seed")
        before = json.loads(_read_capture(capture_root, "ids_before")[0])
        if before.get("objectIdFieldName") != "OBJECTID":
            raise PadUsManagerNameError("source_id_field_changed")
        roster = _validate_roster(before.get("objectIds"), "ids_before")
        if sorted(roster) != ids:
            raise PadUsManagerNameError("source_id_population_drift:ids_seed_vs_before")
        count_before = json.loads(_read_capture(capture_root, "count_before")[0])
        if count_before.get("count") != EXPECTED_COUNT:
            raise PadUsManagerNameError("source_count_drift")
        _fetch_segments(active, capture_root, roster, state)
        segments = list(state["segments"])
        for name in AFTER_NAMES:
            url = {"layer_after": LAYER_URL + "?f=pjson",
                   "service_after": SERVICE_URL + "?f=pjson",
                   "count_after": _count_url(),
                   "ids_after": _ids_url()}[name]
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            if _producer_error_doc(body):
                raise PadUsManagerNameError("source_page_error:" + name)
        count_after = json.loads(_read_capture(capture_root, "count_after")[0])
        if count_after.get("count") != EXPECTED_COUNT:
            raise PadUsManagerNameError("source_metadata_or_count_changed_during_capture")
        after_ids = json.loads(_read_capture(capture_root, "ids_after")[0])
        if after_ids.get("objectIdFieldName") != "OBJECTID" \
                or sorted(_validate_roster(after_ids.get("objectIds"), "ids_after")) != ids:
            raise PadUsManagerNameError("source_id_population_drift:ids_after")
    except PadUsManagerNameError:
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
        raise PadUsManagerNameError("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()
    _save_state(capture_root, {"segments": segments, "complete": True})
    last_captured_at = max((str(entry.get("captured_at", "")) for entry in segments),
                           default=first_captured_at)
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": EXPECTED_COUNT,
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_nan_geometry_count": extracted["nan_geometry_count"],
        "source_empty_geometry_count": extracted["empty_geometry_count"],
        "source_unusable_geometry_counts": extracted["unusable_geometry_counts"],
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "observed_bounds": extracted["observed_bounds"],
        "resumed_segments": len(segments),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    return {"capture_directory": str(capture_root), "segments": len(segments), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


def _validate_corroboration_documents(capture_root: Path) -> None:
    """Validate the captured identity documents (producer + lineage corroboration)."""
    sb_body, _r = _read_capture(capture_root, "sb_item")
    sb = json.loads(sb_body)
    if not isinstance(sb, dict) or sb.get("id") != SB_ITEM_ID \
            or "PAD-US 4.1" not in str(sb.get("title") or "") \
            or RELEASE_DOI not in str(sb.get("citation") or ""):
        raise PadUsManagerNameError("sciencebase_corroboration_identity_drift")
    zip_files = [f for f in sb.get("files", []) if isinstance(f, dict)
                 and str(f.get("name", "")).endswith("Geodatabase.zip")]
    if len(zip_files) != 1 or int(zip_files[0].get("size") or 0) != EXPECTED_ZIP_BYTES:
        raise PadUsManagerNameError("sciencebase_corroboration_zip_reference_drift")
    agol_body, _r = _read_capture(capture_root, "agol_item")
    agol = json.loads(agol_body)
    if not isinstance(agol, dict) or agol.get("id") != AGOL_ITEM_ID \
            or agol.get("owner") != AGOL_ITEM_OWNER \
            or agol.get("title") != AGOL_ITEM_TITLE:
        raise PadUsManagerNameError("agol_item_identity_drift")


class _SegmentStream:
    """Streaming per-segment validator, digester and candidate parquet writer."""

    def __init__(self, capture_root: Path, fields: list[dict[str, object]],
                 parquet_sink: Path | None):
        self._capture_root = Path(capture_root).absolute()
        self.expected_names = tuple(entry["name"] for entry in fields)
        declared = {name: (esri_type, nullable)
                    for name, esri_type, nullable, _l in NATIVE_FIELDS}
        if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
            raise PadUsManagerNameError("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
                        for field in fields]
        arrow_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
        crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
        geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
               "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                             "geometry_types": ["Polygon", "MultiPolygon"],
                                             "crs": crs, "edges": "planar"}}}
        self.schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
        self.parquet_sink = parquet_sink
        self.tmp_path = (parquet_sink.with_name(f"{parquet_sink.name}.construction-tmp")
                         if parquet_sink is not None else None)
        if self.tmp_path is not None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
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
        self.unusable_geometry: dict[str, int] = {}
        self.ring_count = 0
        self.vertex_count = 0
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page_name: str, offset: int, count: int, roster: list[int]) -> None:
        """Digest one retained roster segment into the digests and optional parquet sink."""
        if self._writer_closed:
            raise PadUsManagerNameError("stream_writer_already_closed")
        page = decode_feature_collection_pbf(
            _read_capture(self._capture_root, page_name)[0], allow_transfer_limit=True)
        if not isinstance(page, dict) or page.get("exceededTransferLimit") is not True:
            raise PadUsManagerNameError("source_page_decode_failed:" + page_name)
        reference = page.get("spatialReference")
        if (not isinstance(reference, dict)
                or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                or reference.get("latestWkid") not in (None, SOURCE_CRS_WKID)):
            raise PadUsManagerNameError("source_page_spatial_reference_drift:" + page_name)
        if page.get("objectIdFieldName") != "OBJECTID":
            raise PadUsManagerNameError("source_id_field_changed:" + page_name)
        if page.get("hasZ") is not False or page.get("hasM") is not False:
            raise PadUsManagerNameError("source_page_z_or_m_flag_drift:" + page_name)
        page_fields = [entry["name"] for entry in page.get("fields", [])]
        if page_fields != list(self.expected_names):
            raise PadUsManagerNameError("source_page_field_roster_drift:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise PadUsManagerNameError("source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, rings = _validate_feature(feature, self.expected_names,
                                                expected_objectid)
            attrs = feature["attributes"]
            self.record_hash.update(_canonical(feature) + b"\x00")
            self.ids.append(objectid)
            self.last_objectid = objectid
            if rings is None:
                self.null_geometry += 1
                geometries.append(None)
            elif any(vertex[0] == "NaN" and vertex[1] == "NaN"
                     for ring in rings for vertex in ring):
                self.nan_geometry += 1
                geometries.append(None)
            elif not rings:
                self.empty_geometry += 1
                geometries.append(None)
            else:
                for ring in rings:
                    for vertex in ring:
                        x, y = vertex[0], vertex[1]
                        if self._xmin is None or x < self._xmin:
                            self._xmin = x
                        if self._ymin is None or y < self._ymin:
                            self._ymin = y
                        if self._xmax is None or x > self._xmax:
                            self._xmax = x
                        if self._ymax is None or y > self._ymax:
                            self._ymax = y
                self.ring_count += len(rings)
                self.vertex_count += sum(len(ring) for ring in rings)
                try:
                    wkb = _polygon_wkb({"rings": rings})
                except PadUsManagerNameError as exc:
                    if exc.code not in UNUSABLE_GEOMETRY_MODES:
                        raise
                    self.unusable_geometry[exc.code] = \
                        self.unusable_geometry.get(exc.code, 0) + 1
                    geometries.append(None)
                else:
                    self.geometry_hash.update(_canonical([objectid, rings]) + b"\x00")
                    geometries.append(wkb)
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
                "unusable_geometry_counts": dict(sorted(self.unusable_geometry.items())),
                "ring_count": self.ring_count, "vertex_count": self.vertex_count,
                "max_objectid": self.last_objectid,
                "observed_bounds": None if self._xmin is None else
                {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax, "ymax": self._ymax}}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "sb_item"}
    _validate_corroboration_documents(capture_root)
    _validate_service_document(docs["service_before"], "service_before")
    _validate_service_document(json.loads(_read_capture(capture_root, "service_after")[0]),
                               "service_after")
    fields, schema_sha = _validate_layer_document(docs["layer_before"])
    declared = {name: (esri_type, nullable)
                for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise PadUsManagerNameError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise PadUsManagerNameError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise PadUsManagerNameError("source_count_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise PadUsManagerNameError("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before")
    if roster != seed:
        raise PadUsManagerNameError("source_id_population_drift:ids_seed_vs_before")
    after_roster = _validate_roster(json.loads(
        _read_capture(capture_root, "ids_after")[0]).get("objectIds"), "ids_after")
    if after_roster != roster:
        raise PadUsManagerNameError("source_id_population_drift:ids_after")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise PadUsManagerNameError("source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise PadUsManagerNameError("source_field_schema_changed_during_capture")
    state = _load_state(capture_root)
    segments = state.get("segments")
    if not isinstance(segments, list) or not segments \
            or any(not isinstance(item, dict) for item in segments):
        raise PadUsManagerNameError("source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(segments, key=lambda item: (item.get("offset", -1), item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _segment_name(offset, count) or offset != cursor
                or offset + count > EXPECTED_COUNT or entry.get("features") != count):
            raise PadUsManagerNameError("source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != EXPECTED_COUNT:
        raise PadUsManagerNameError("source_capture_roster_incomplete_or_unexpected")
    stream = _SegmentStream(capture_root, fields, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    if len(stream.ids) != EXPECTED_COUNT or summary["max_objectid"] <= 0:
        raise PadUsManagerNameError("source_page_population_incomplete")
    ids_sha = summary["ids_sha"]
    if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
        raise PadUsManagerNameError("source_id_roster_drift")
    if EXPECTED_RECORD_SHA256 and summary["record_sha"] != EXPECTED_RECORD_SHA256:
        raise PadUsManagerNameError("source_value_or_geometry_baseline_drift")
    if EXPECTED_GEOMETRY_SHA256 and summary["geometry_sha"] != EXPECTED_GEOMETRY_SHA256:
        raise PadUsManagerNameError("source_geometry_or_coverage_drift")
    for pin_name, value, actual in (("null", EXPECTED_NULL_GEOMETRY_COUNT,
                                     summary["null_geometry_count"]),
                                    ("nan", EXPECTED_NAN_GEOMETRY_COUNT,
                                     summary["nan_geometry_count"]),
                                    ("empty", EXPECTED_EMPTY_GEOMETRY_COUNT,
                                     summary["empty_geometry_count"])):
        if value is not None and actual != value:
            raise PadUsManagerNameError("source_" + pin_name + "_geometry_count_drift")
    if (EXPECTED_UNUSABLE_GEOMETRY_COUNTS is not None
            and summary["unusable_geometry_counts"] != EXPECTED_UNUSABLE_GEOMETRY_COUNTS):
        raise PadUsManagerNameError("source_unusable_geometry_counts_drift")
    if (EXPECTED_OBSERVED_BOUNDS is not None
            and summary["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS):
        raise PadUsManagerNameError("source_observed_bounds_drift")
    return {"fields": fields, "schema_sha": schema_sha, "segments": len(ordered),
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": ids_sha, "parquet_path": parquet_sink,
            "null_geometry_count": summary["null_geometry_count"],
            "nan_geometry_count": summary["nan_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "unusable_geometry_counts": summary["unusable_geometry_counts"],
            "ring_count": summary["ring_count"], "vertex_count": summary["vertex_count"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"]}


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


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


def _load_receipt_map(capture_root: Path) -> dict[str, dict[str, object]]:
    state = _load_state(capture_root)
    names = [*METADATA_NAMES, *AFTER_NAMES,
             *(str(entry["name"]) for entry in state.get("segments", [])
               if isinstance(entry, dict) and entry.get("name"))]
    return {name: _load_receipt(capture_root, name) for name in names}


class _RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return _read_capture(self._capture_root, name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_segments: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"]},
            parquet_body)
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Polygon / MultiPolygon",
        "null_geometry_count": extracted["null_geometry_count"],
        "nan_geometry_count": extracted["nan_geometry_count"],
        "empty_geometry_count": extracted["empty_geometry_count"],
        "unusable_geometry_counts": extracted["unusable_geometry_counts"],
        "unique_source_id_count": EXPECTED_COUNT,
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "agol_item_url": AGOL_ITEM_URL,
        "agol_item_owner": AGOL_ITEM_OWNER,
        "lineage_corroboration": {"sciencebase_item_id": SB_ITEM_ID,
                                  "release_doi": RELEASE_DOI,
                                  "national_geodatabase_zip_bytes": EXPECTED_ZIP_BYTES,
                                  "national_geodatabase_zip_sha256": EXPECTED_ZIP_SHA256,
                                  "combined_feature_class_rows": EXPECTED_CORROBORATION_COUNT,
                                  "note": "The official PAD-US 4.1 national geodatabase ZIP "
                                          "(ScienceBase item 652d4fc5d34e44db0e2ee45e) holds "
                                          "the Combined feature class with 656,986 rows and a "
                                          "different field roster (Term/Duration present; "
                                          "service-derived fields absent); it is producer "
                                          "lineage corroboration and is never fetched as "
                                          "pipeline input."},
        "transport": {"format": "pbf", "decoder": "dagster_hifld.esri_pbf",
                      "quantization_note": "The service's PBF transport quantizes "
                                           "coordinates (observed scale 1e-9 degree, "
                                           "translate -400); raw PBF pages are the "
                                           "authoritative transport evidence and decoded "
                                           "coordinates are what the service serves in "
                                           "this format."},
        "crosswalk_doi": "10.3886/E241367V1",
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": bool(
            not EXPECTED_RECORD_SHA256 or not EXPECTED_GEOMETRY_SHA256),
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The PAD-US 4.1 release is a fixed dated USGS data release (DOI "
            "10.5066/P96WBCHS, 2024 in the ScienceBase citation and 2025 in the service "
            "copyright text); the service publishes no additional per-snapshot release "
            "date and the portal item's 2026 modification time is metadata activity.",
        "pagination": {"mode": "enumerated_roster_bisected_segments",
                       "segment_start_records": SEGMENT_START_RECORDS,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "segments": extracted["segments"], "resumable": True,
                       "resumed_segments": resumed_segments,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerated the complete unique OBJECTID roster "
                           "(655,335 ids; sparse non-contiguous span observed 2026-11-13). "
                           "Segments page 2000 records by resultOffset over the sorted "
                           "roster and refused segments are bisected until served; "
                           "accepted segments must tile the roster exactly and "
                           "before/after count/roster brackets prove nothing was skipped, "
                           "duplicated or reordered."},
        "capture_window_utc": {"first_segment": first_captured_at,
                               "last_segment": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    receipts = _load_receipt_map(capture_root)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "PAD-US Manager Name (USGS PAD-US 4.1)",
        "description": "USGS PAD-US Manager Name publication: the PADUS4_1ManagerName "
                       "FeatureServer layer of the USGS-owned ArcGIS item "
                       "d1c464bac63d44bc88a4a059dc69d259, carrying the PAD-US 4.1 release "
                       "(DOI 10.5066/P96WBCHS) Combined feature class with the "
                       "manager-name mapping fields, captured completely from the "
                       "producer's own service. Succession, freshness, schema/ID "
                       "contract, coverage, rights and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": "U.S. Government work in the public domain; the item publishes a USGS "
                   "no-warranty statement. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication", "canonical_url": LAYER_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason":
                        "The PAD-US 4.1 release is a fixed dated USGS data release "
                        "(DOI 10.5066/P96WBCHS); the service publishes no additional "
                        "per-snapshot release date.",
                    "license": "U.S. Government work in the public domain per the producer "
                               "item; rights acceptance pending.",
                    "attribution": "U.S. Geological Survey (USGS) Gap Analysis Project "
                                   "(GAP), PAD-US 4.1; formal attribution terms require "
                                   "maintainer review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_pbf_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": "the complete enumerated OBJECTID "
                                                          "roster captured as adaptively "
                                                          "bisected roster segments",
                                            "source_fields": "all producer fields retained "
                                                             "in producer order and declared "
                                                             "physical types, including the "
                                                             "service-generated Shape__Length "
                                                             "and Shape__Area doubles",
                                            "source_ids": "Preserve producer OBJECTID values "
                                                          "unchanged; no substitute IDs, "
                                                          "casts or joins",
                                            "geometry": "Service PBF pages decoded by the "
                                                        "repository's bounded "
                                                        "FeatureCollection-PBF decoder and "
                                                        "assembled to 2D Polygon/MultiPolygon "
                                                        "WKB in EPSG:4326 with the producer's "
                                                        "Esri ring semantics (closed rings; "
                                                        "clockwise exteriors, "
                                                        "counterclockwise holes joined to "
                                                        "their smallest containing exterior); "
                                                        "rows whose rings cannot form a valid "
                                                        "OGC polygon are gated to null "
                                                        "candidate geometry and counted by "
                                                        "failure mode instead of being "
                                                        "coerced; the PBF transport carries "
                                                        "the service's own 1e-9-degree "
                                                        "quantization",
                                            "candidate_status": "unapproved; publication "
                                                                "blocked pending succession, "
                                                                "freshness, schema/ID "
                                                                "contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry pad-us-manager-name ('PAD-US Manager "
                   "Name', agency US Geological Survey) names the USGS PAD-US Manager Name "
                   "publication. The producer publication captured here is USGS's ArcGIS "
                   "Online item d1c464bac63d44bc88a4a059dc69d259 (owned by the USGS "
                   "organizational account mcroft@usgs.gov_USGS), whose "
                   "Manager_Name_PADUS FeatureServer layer 0 PADUS4_1ManagerName carries "
                   "the PAD-US 4.1 release (DOI 10.5066/P96WBCHS, cited verbatim in the "
                   "service copyright text) Combined feature class with manager-name "
                   "mapping fields. The official PAD-US 4.1 national geodatabase ZIP "
                   "(ScienceBase item 652d4fc5d34e44db0e2ee45e, 1,523,434,496 bytes, SHA-256 "
                   "62ed113335e359509ad68a731146c74aa873c5a92da4f3fda572e54bc9b823ab, "
                   "probed 2026-11-13) holds the Combined feature class with 656,986 rows "
                   "and a different field roster (Term and Duration present; EsmtHldr, "
                   "EHoldTyp, WebServiceMangName, TranspPct and the service Shape__ doubles "
                   "absent), so it cannot reconstruct this publication's content and is "
                   "recorded as producer lineage corroboration only. The DataLumos project "
                   "240684 archive copy (archive modification 2025-06-04) is identification "
                   "and comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "EPSG:4326 polygon geometry retained from the producer's native "
                      "PBF transport; all succession, freshness, contract, rights and "
                      "release approvals remain pending.",
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
    descriptors: dict[str, dict[str, object]] = {}
    for relative, body in metadata_outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
        descriptors[relative] = {"bytes": len(body), "sha256": _sha(body)}
    for name in sorted(receipts):
        body, receipt = _read_capture(capture_root, name)
        wire = (capture_root / f"{name}.wire").read_bytes()
        for suffix, blob in (("body", body), ("wire", wire),
                             ("receipt.json", _canonical(receipt) + b"\n")):
            relative = f"raw/{name}.{suffix}"
            write_immutable(storage, f"{prefix}/{relative}", blob)
            descriptors[relative] = {"bytes": len(blob), "sha256": _sha(blob)}
    fingerprint = _sha(_canonical(identity))
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": receipts, "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_nan_geometry_count": extracted["nan_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_unusable_geometry_counts": extracted["unusable_geometry_counts"],
            "source_id_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


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
        raise PadUsManagerNameError("cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise PadUsManagerNameError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": EXPECTED_NULL_GEOMETRY_COUNT,
            "source_id_sha256": audit.get("source_ids_sha256", ""),
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def capture_pad_us_manager_name(storage: StagingStorageResource, *, version: str,
                                capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) \
            or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise PadUsManagerNameError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "0123456789.-_" for c in version):
        raise PadUsManagerNameError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise PadUsManagerNameError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise PadUsManagerNameError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory \
        else Path("/data/workspace/scratch/pad-us-manager-name/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise PadUsManagerNameError("unsafe_capture_directory")
    resumed_segments = len(_load_state(capture_root).get("segments", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_pad_us_manager_name_pages(capture_root)
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
            except (OSError, PadUsManagerNameError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_pad_us_manager_name_baseline(capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": EXPECTED_COUNT,
            "source_max_objectid": extracted["max_objectid"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_nan_geometry_count": extracted["nan_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_unusable_geometry_counts": extracted["unusable_geometry_counts"],
            "source_ring_count": extracted["ring_count"],
            "source_vertex_count": extracted["vertex_count"],
            "observed_bounds": extracted["observed_bounds"]}
