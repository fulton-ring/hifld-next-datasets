"""Lossless, approval-gated capture of the FEMA NFHL "Cross-Sections" line layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 14
"Cross-Sections" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/cross-sections-1``, DataLumos 239967 historical archive copy used
for identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The layer publishes 954,282 polyline features (dated 2026-11-12 capture) and
the producer also intermittently answers valid requests with complete HTTP 200
Esri error documents or resets the transport outright (both observed while
paging this layer on 2026-11-12), so the complete layer is captured as
adaptively bisected segments of the sorted OBJECTID roster: each accepted
segment must return exactly its expected id slice, the accepted segments must
tile the complete roster without gaps or overlaps, and the before/after
count/id/metadata brackets plus full-record digests prove that nothing was
skipped, duplicated, or reordered.
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

DATASET_SLUG = "nfhl-cross-sections"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 14
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Cross-Sections"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Any
# segment whose response would approach the producer's ~36 MB transfer cap is
# bisected until the producer serves it; see the module docstring.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 12_000_000_000
# The producer's MapServer also intermittently answers valid requests with
# complete HTTP 200 Esri error documents (observed 2026-11-08 while paging this
# layer; the identical request succeeds moments later). Every request retries a
# bounded number of times for that transient mode; a retry re-issues the exact
# same request, and completeness is still enforced by the pinned before/after
# id rosters, counts, exact segment tiling, and full-record digests, so retries
# can never mask drift. Segments wider than one record that keep failing are
# bisected (the producer's transfer cap); a single-record segment that keeps
# failing fails the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-12 complete-layer population contract.
EXPECTED_COUNT = 954282
EXPECTED_ID_SHA256 = "88c4e3387078077abe66c7273b85893fae441d9c554cf3e575a803a0b496b622"
EXPECTED_SCHEMA_SHA256 = "7a15c06a35f35fa4aa4c9bcc3a840db04f8c8442f9a9cf32de52bc6ef4448956"
EXPECTED_RECORD_SHA256 = "PENDING_20261112_CAPTURE"
EXPECTED_GEOMETRY_SHA256 = "PENDING_20261112_CAPTURE"
EXPECTED_OBSERVED_BOUNDS = {"xmin": "PENDING_20261112_CAPTURE", "ymin": "PENDING_20261112_CAPTURE",
                            "xmax": "PENDING_20261112_CAPTURE", "ymax": "PENDING_20261112_CAPTURE"}
EXPECTED_NULL_GEOMETRY_COUNT = -1
EXPECTED_NAN_GEOMETRY_COUNT = -1
EXPECTED_LAYER_EXTENT = {"xmin": -170.8101923523096, "ymin": -14.35089818917669,
                         "xmax": 145.7600641593766, "ymax": 64.89803977710869}
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
# Complete-population pins above are established by one dated complete capture;
# the module docstring records the adaptive segment plan that served it.


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``.

    Byte-identical to ``_sha(_canonical(items))`` without materializing the
    complete document, which for this layer is far larger than agent memory
    should ever hold twice.
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


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


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
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


def _collect_segment(offset: int, count: int, page: object, ids: list[int],
                     features: list[dict[str, object]]) -> None:
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
    features.extend(rows)


def _classify_geometries(features: list[dict[str, object]], attr_names: list[str]):
    null_geometry = 0
    nan_geometry = 0
    geometry_records = []
    xmin = ymin = xmax = ymax = None
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        if geom is None:
            null_geometry += 1
            geometry_records.append([attrs["OBJECTID"], None, None])
            continue
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
        geometry_records.append([attrs["OBJECTID"], geom["paths"]])
        if nan_paths:
            nan_geometry += 1
        elif feature_xmin is not None:
            if xmin is None or feature_xmin < xmin:
                xmin = feature_xmin
            if ymin is None or feature_ymin < ymin:
                ymin = feature_ymin
            if xmax is None or feature_xmax > xmax:
                xmax = feature_xmax
            if ymax is None or feature_ymax > ymax:
                ymax = feature_ymax
    observed_bounds = None if xmin is None else {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
    return geometry_records, null_geometry, nan_geometry, observed_bounds


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name != "service_doc" and not name.startswith("page_")}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    segments = [_parse_segment_name(name) for name in page_names]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {"service_doc", *page_names}:
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
        raise ValueError("exact_nfhl_cross_sections_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolyline"):
        raise ValueError("exact_nfhl_cross_sections_layer_metadata_drift")
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

    features: list[dict[str, object]] = []
    for (offset, count), name in zip(segments, page_names):
        _collect_segment(offset, count, json.loads(captures[name][0]), ids, features)
    attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
    geometry_records, null_geometry, nan_geometry, bounds = _classify_geometries(features, attr_names)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _list_sha(features)
    geometry_sha = _list_sha(geometry_records)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polyline", "null_geometry_count": null_geometry,
             "nan_string_geometry_count": nan_geometry,
             "observed_bounds": bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSmallInteger": pa.int16()}[str(field["type"])]


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


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "SHAPE" else _arrow_type(field)
        if name == "SHAPE":
            values = [_line_wkb(row.get("geometry")) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        else:
            values = [row["attributes"].get(name) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        columns[name] = pa.array(values, type=typ)
    geo = {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": ["LineString", "MultiLineString"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


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


def _capture_segments(client: httpx.Client, capture_root: Path,
                      total: int) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Capture the complete layer as adaptively bisected roster segments.

    Segments start at SEGMENT_START_RECORDS. A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the refused
    segment's final body and receipts stay on disk as evidence under its own
    name. A single-record segment that still fails ends the capture explicitly.
    """
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    segments: deque[tuple[int, int]] = deque(
        (offset, min(SEGMENT_START_RECORDS, total - offset))
        for offset in range(0, total, SEGMENT_START_RECORDS)
    )
    while segments:
        offset, count = segments.popleft()
        name = _segment_name(offset, count)
        body, receipt = _capture_with_retry(
            client, _segment_query(offset, count), capture_root, name,
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
        captures[name] = (body, receipt)
    return captures


def capture_nfhl_cross_sections(storage: StagingStorageResource, *, version: str,
                                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Cross-Sections candidate."""
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/190/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-cross-sections-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            captures.update(_capture_segments(client, capture_root, EXPECTED_COUNT))
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
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
        "schema_version": 1, "approved": False, "title": "Cross-Sections (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 14 'Cross-Sections', captured "
                        "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                        "polyline geometry are represented in the candidate. The NFHL is continuously updated and "
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
                                                             "SHAPE.STLength() double; SHAPE is the original "
                                                             "geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, DFIRM_ID, VERSION_ID, XS_LN_ID, START_ID, "
                                                          "MODEL_ID, GFID and GlobalID values unchanged; no "
                                                          "substitute IDs, casts, trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polyline paths retained as WKB "
                                                        "without projection; null source geometry retained as null; "
                                                        "producer string-'NaN' ordinate rows are retained as "
                                                        "records with null candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-"
                   "hazard-layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 14 'Cross-Sections' of that producer "
                   "service. The HIFLD Open inventory lists this dataset as nfhl/cross-sections-1 with DataLumos "
                   "project 239967; that ICPSR archive copy and the 2024-10-31 archive modification date are "
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
