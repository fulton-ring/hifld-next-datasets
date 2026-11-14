"""Bounded, approval-gated capture of the USGS WBD 10-digit HU (Watershed) layer.

The HIFLD Open inventory entry ``wbd/10-digit-hu-watershed`` ("10-digit HU
(Watershed)", agency US Geological Survey, publisher U.S. Geological Survey,
National Geospatial Technical Operations Center) was the Watershed Boundary
Dataset hydrologic-unit polygon layer. The DHS HIFLD Open crosswalk (byte-verified
public copy of DOI 10.3886/E241367V1, sha256 7f3394fed2712fdcc176b7ce658293fdded
656b9b719f0d90154c07dc38772bc, verified 2026-11-14) marks the WBD row "Deprecate"
("No longer supported as part of the HIFLD mission", DUA No, GII access No) and
directs users to the producing agency's own publication: USGS The National Map
``wbd`` service on hydro.nationalmap.gov (the crosswalk's Open REST Service).
That producer service is captured here: the ``wbd`` FeatureServer layer 5 carries
the exact inventory layer title "10-digit HU (Watershed)" and the same layer
description verbatim. The DataLumos project 240691 archive copy is identification
context only and is never fetched.

Capture is bounded and fail-closed: the host services directory, service,
same-org alias MapServer and layer documents are pinned before any feature
request, the complete OBJECTID roster is pinned and re-verified around the
capture, pages are exact ``objectIds`` windows (bisected deterministically when
the producer refuses an oversized window), every served value and ring is
validated against the declared producer schema, and all digests are pinned
against the dated complete capture. Pages are requested at outSR=4326 because
the layer's native publication SR is Web Mercator (102100/3857, pinned); WGS84
lon/lat is the served coordinate form the producer's own map viewer and the
crosswalk target consume. Publication stays blocked pending freshness,
schema/ID contract, coverage, rights, baseline and release acceptance.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "wbd-10-digit-hu-watershed"
FILE_SLUG = DATASET_SLUG
SERVICES_DIRECTORY_URL = "https://hydro.nationalmap.gov/arcgis/rest/services?f=json"
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/FeatureServer"
# The crosswalk's Open REST Service target; the same producer publication's
# mapping twin on the same host, pinned as alias identity evidence.
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
LAYER_ID = 5
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
CROSSWALK_DOI = "10.3886/E241367V1"
CROSSWALK_ROW = "Watershed Boundary Dataset (WBD)"
CROSSWALK_OPEN_ID = "409515c7627c444fb08b66cf89634028"
PUBLISHER = "U.S. Geological Survey, National Geospatial Technical Operations Center"
USER_AGENT = "hifld-next-datasets/wbd-10-digit-hu-watershed-draft"
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
EXPECTED_COUNT = 18911
# Live-verified 2026-11-14: a 100-roster-id window answers ~15.5 MB of JSON and
# a 250-roster-id window persistently refuses with the producer's Esri 500 error
# document, so pages are pinned at 100 roster ids and any refusing window is
# bisected deterministically instead of retried.
PAGE_IDS = 100
MAX_PAGE_BYTES = 32 * 1024 * 1024
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
TOTAL_CAPTURE_BUDGET = 8 * 1024 * 1024 * 1024
PAGE_ATTEMPTS = 3
PAGE_BASE_DELAY_SECONDS = 5.0
DOC_ATTEMPTS = 4
DOC_BASE_DELAY_SECONDS = 10.0
EXPECTED_COPYRIGHT_TEXT = "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026."
# Live-verified 2026-11-14 (the FeatureServer and the alias MapServer publish
# the same layer roster; note the double space in the 8-digit title).
EXPECTED_SERVICE_LAYERS = [
    (0, "WBDLine"),
    (1, "2-digit HU (Region)"),
    (2, "4-digit HU (Subregion)"),
    (3, "6-digit HU (Basin)"),
    (4, "8-digit HU  (Subbasin)"),
    (5, "10-digit HU (Watershed)"),
    (6, "12-digit HU (Subwatershed)"),
    (7, "14-digit HU"),
    (8, "16-digit HU"),
]
EXPECTED_LAYER_EXTENT = {"xmin": -19951755.682, "ymin": -1644316.000599999,
                         "xmax": 20021551.7474, "ymax": 11554273.612599999}
# Declared producer schema in producer order (layer document, 2026-11-14).
EXPECTED_FIELDS = [
    ("objectid", "esriFieldTypeOID"),
    ("tnmid", "esriFieldTypeString"),
    ("metasourceid", "esriFieldTypeString"),
    ("sourcedatadesc", "esriFieldTypeString"),
    ("sourceoriginator", "esriFieldTypeString"),
    ("sourcefeatureid", "esriFieldTypeString"),
    ("loaddate", "esriFieldTypeDate"),
    ("referencegnis_ids", "esriFieldTypeString"),
    ("areaacres", "esriFieldTypeDouble"),
    ("areasqkm", "esriFieldTypeDouble"),
    ("states", "esriFieldTypeString"),
    ("huc10", "esriFieldTypeString"),
    ("name", "esriFieldTypeString"),
    ("hutype", "esriFieldTypeString"),
    ("humod", "esriFieldTypeString"),
    ("globalid", "esriFieldTypeGlobalID"),
    ("Shape__Length", "esriFieldTypeDouble"),
    ("Shape__Area", "esriFieldTypeDouble"),
]
FIELD_NAMES = [name for name, _kind in EXPECTED_FIELDS]
_SERVICE_DESCRIPTION_MARKERS = (
    "Hydrologic Unit (HU) polygon boundaries for the United States, Puerto Rico, and the U.S. Virgin Islands",
    "seamless National representation of HU boundaries from 2 to 16 digits",
    "All data are open and non-proprietary",
    "Acknowledgment of the U.S. Geological Survey would be appreciated",
)
_LAYER_DESCRIPTION_MARKERS = (
    "seamless National representation of HU boundaries from 2 to 14 digits",
    "compiled from U.S. Geological Survey (USGS) National Hydrography Dataset (NHD) and U.S. "
    "Department of Agriculture (USDA) National Resources Conservation Service (NRCS) Watershed "
    "Boundary Dataset (WBD) sources",
    "1:250,000 to 1:2,000,000",
)
_PAGE_ALLOWED_MEMBERS = frozenset({"objectIdFieldName", "globalIdFieldName", "geometryType",
                                   "spatialReference", "fields", "features",
                                   "exceededTransferLimit", "requested_url"})
# Pinned complete-population contract, re-derived from the dated complete
# capture (2026-11-14; roster sha over the canonical JSON array of the sorted
# OBJECTID roster; record/geometry digests over the canonical whole-population
# streams defined below).
EXPECTED_ID_SHA256 = "f76070a8f58607499cb528be34afa191c2065faead0fab765c6990b86d8c384b"
EXPECTED_SCHEMA_SHA256 = "PENDING_LIVE_PIN"
EXPECTED_RECORD_SHA256 = "PENDING_LIVE_PIN"
EXPECTED_GEOMETRY_SHA256 = "PENDING_LIVE_PIN"
EXPECTED_OBSERVED_BOUNDS = {"xmin": 0.0, "ymin": 0.0, "xmax": 0.0, "ymax": 0.0}
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_UNIQUE_HUC10_COUNT = EXPECTED_COUNT
GEOMETRY_COLUMN = "geometry"


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: object) -> bytes:
    return _canonical(value) + b"\n"


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _request_url(name: str, params: dict[str, object]) -> str:
    if name == "services_directory":
        return "https://hydro.nationalmap.gov/arcgis/rest/services"
    if name in ("service_before", "service_after"):
        return SERVICE_URL
    if name == "alias_service":
        return ALIAS_SERVICE_URL
    if name in ("layer_before", "layer_after"):
        return LAYER_URL
    return _query(params)


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


def _reserve(budget: list[int], upcoming: int) -> None:
    if budget[0] + upcoming > TOTAL_CAPTURE_BUDGET:
        raise ValueError("source_total_capture_budget_exceeded")


def _parse_response(name: str, body: bytes) -> dict:
    parsed = json.loads(body)
    if not isinstance(parsed, dict) or "error" in parsed:
        raise ValueError(f"invalid_arcgis_response:{name}")
    return parsed


def _request(client, name: str, params: dict[str, object], staging, version_key: str,
             budget: list[int], receipts: dict[str, dict] | None = None,
             max_bytes: int = MAX_DOCUMENT_BYTES) -> dict:
    """Fetch or replay one producer document, retaining body/wire/receipt."""
    retained = getattr(client, "retained_capture", None)
    if retained is not None:
        body, wire, receipt = retained.read_response(name, params)
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.json", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.receipt.json",
                      _json_bytes(receipt))
        if receipts is not None:
            receipts[name] = receipt
        return _parse_response(name, body)
    url = _request_url(name, params)
    for attempt in range(1, DOC_ATTEMPTS + 1):
        raw_parts: list[bytes] = []
        wire_parts: list[bytes] = []
        try:
            with client.stream("GET", url, headers={"Accept-Encoding": "identity",
                                                    "User-Agent": USER_AGENT}) as response:
                capture = capture_http_response(
                    response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                    reserve=lambda progress: _reserve(budget, progress.wire_bytes_retained
                                                      + progress.decoded_bytes_retained
                                                      + progress.reserve_bytes),
                    retain_wire=wire_parts.append, retain_decoded=raw_parts.append,
                )
        except Exception as exc:
            raise ValueError("source_transport_error:" + type(exc).__name__) from exc
        body = b"".join(raw_parts)
        wire = b"".join(wire_parts)
        status_ok = capture.complete and 200 <= capture.status_code < 300
        # Failed attempts carry the window size in their evidence name so a
        # bisected child can never overwrite its parent's retained response.
        artifact_name = name if status_ok else f"{name}.attempt_{attempt}_{max_bytes}cap"
        receipt = {
            "method": "GET", "url": str(response.request.url), "status": capture.status_code,
            "attempt": attempt, "observed_at": datetime.now(timezone.utc).isoformat(),
            "bytes": len(body), "sha256": _sha(body),
            "wire_bytes": len(wire), "wire_sha256": _sha(wire),
            "content_type": response.headers.get("content-type"),
            "content_encoding": capture.content_encoding,
            "complete": capture.complete, "error_code": capture.error_code,
        }
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.json", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.receipt.json",
                      _json_bytes(receipt))
        if status_ok:
            if _producer_error_doc(body):
                raise ValueError("source_document_error:" + name)
            if receipts is not None:
                receipts[name] = receipt
            return _parse_response(name, body)
        if capture.status_code in {429, 500, 502, 503, 504} and attempt < DOC_ATTEMPTS:
            time.sleep(DOC_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
            continue
        raise ValueError("incomplete_source_response:" + name)
    raise ValueError("source_retry_limit_exceeded:" + name)


def _failed_attempt_error_code(staging, version_key: str, name: str, attempt: int,
                               max_bytes: int) -> str | None:
    try:
        receipt = json.loads(staging.read_key(staging.build_target_location(
            DATASET_SLUG, FILE_SLUG, version_key,
            f"raw/{name}.attempt_{attempt}_{max_bytes}cap.receipt.json")))
    except (OSError, ValueError):
        return None
    code = receipt.get("error_code")
    return code if isinstance(code, str) else None


def _validate_service_roster(service: dict) -> None:
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise ValueError("usgs_wbd_service_identity_drift")
    roster = [(entry.get("id"), entry.get("name")) for entry in service.get("layers", [])
              if isinstance(entry, dict)]
    if roster != EXPECTED_SERVICE_LAYERS:
        raise ValueError("usgs_wbd_service_layer_roster_drift")


def _validate_service_document(service: dict) -> None:
    if service.get("currentVersion") != 11.3 or service.get("maxRecordCount") != 2000:
        raise ValueError("official_usgs_wbd_service_identity_drift")
    if service.get("supportedQueryFormats") != "JSON":
        raise ValueError("usgs_wbd_service_query_formats_drift")
    description = str(service.get("serviceDescription", ""))
    if any(marker not in description for marker in _SERVICE_DESCRIPTION_MARKERS):
        raise ValueError("usgs_wbd_service_description_drift")
    _validate_service_roster(service)


def _validate_alias_document(alias: dict) -> None:
    _validate_service_roster(alias)
    if alias.get("supportedQueryFormats") != "JSON, geoJSON, PBF":
        raise ValueError("usgs_wbd_alias_service_query_formats_drift")


def _validate_directory(directory: dict) -> None:
    entries = {(service.get("name"), service.get("type"))
               for service in directory.get("services", []) if isinstance(service, dict)}
    if {("wbd", "FeatureServer"), ("wbd", "MapServer")} - entries:
        raise ValueError("usgs_wbd_services_directory_drift")


def _validate_layer_document(layer: dict) -> list[dict]:
    if (layer.get("id") != LAYER_ID or layer.get("name") != "10-digit HU (Watershed)"
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_usgs_wbd_layer_metadata_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("source_layer_z_or_m_contract_drift")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    if layer.get("objectIdField") != "objectid" or layer.get("globalIdField") != "globalid":
        raise ValueError("source_object_id_field_drift")
    if layer.get("supportedQueryFormats") != "JSON, geoJSON, PBF":
        raise ValueError("usgs_wbd_layer_query_formats_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    _validate_layer_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_layer_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    description = str(layer.get("description", ""))
    if any(marker not in description for marker in _LAYER_DESCRIPTION_MARKERS):
        raise ValueError("usgs_wbd_layer_description_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(entry, dict) for entry in fields_value):
        raise ValueError("source_field_roster_drift")
    if [(field.get("name"), field.get("type")) for field in fields_value] != EXPECTED_FIELDS:
        raise ValueError("source_schema_drift")
    return [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
             if key in field} for field in fields_value]


def _schema_sha(fields: list[dict]) -> str:
    return _sha(_canonical(fields))


def _roster_params() -> dict[str, str]:
    return {"f": "json", "where": "1=1", "returnIdsOnly": "true"}


def _count_params() -> dict[str, str]:
    return {"f": "json", "where": "1=1", "returnCountOnly": "true"}


def _validate_roster(doc: dict, error: str) -> list[int]:
    if doc.get("objectIdFieldName") != "objectid":
        raise ValueError("source_object_id_field_drift")
    ids = doc.get("objectIds")
    if (not isinstance(ids, list) or len(ids) != EXPECTED_COUNT
            or any(type(value) is not int for value in ids) or len(set(ids)) != len(ids)):
        raise ValueError(error)
    if _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    return sorted(ids)


def _id_snapshot(client, staging, version_key: str, name: str, budget: list[int],
                 receipts: dict[str, dict]) -> list[int]:
    return _validate_roster(_request(client, name, _roster_params(), staging, version_key,
                                     budget, receipts), "source_id_population_drift")


def _count_snapshot(client, staging, version_key: str, name: str, budget: list[int],
                    receipts: dict[str, dict]) -> int:
    result = _request(client, name, _count_params(), staging, version_key, budget, receipts)
    count = result.get("count")
    if type(count) is not int or count != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    return count


def _window_params(window_ids: list[int]) -> dict[str, str]:
    return {"objectIds": ",".join(map(str, window_ids)), "outFields": "*",
            "returnGeometry": "true", "outSR": "4326",
            "orderByFields": "objectid ASC", "f": "json"}


def _window_url_ids(url: str) -> list[int]:
    parsed = parse_qs(urlsplit(url).query)
    raw = parsed.get("objectIds")
    if not raw:
        raise ValueError("source_window_request_missing_ids")
    return [int(value) for value in raw[0].split(",")]


def _validate_attrs_types(attrs: dict) -> None:
    for name_attr, kind in EXPECTED_FIELDS:
        if name_attr == "objectid":
            continue
        value = attrs.get(name_attr)
        if value is None:
            continue
        if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID"):
            if not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + name_attr)
        elif kind == "esriFieldTypeDate":
            if type(value) is not int:
                raise ValueError("source_date_value_type_drift:" + name_attr)
        elif kind == "esriFieldTypeDouble":
            if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
                raise ValueError("source_double_value_type_drift:" + name_attr)


def _validate_ring(ring: object) -> list[list[float]]:
    if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
        raise ValueError("source_polygon_ring_invalid")
    points: list[list[float]] = []
    for point in ring:
        if not isinstance(point, list) or len(point) != 2 \
                or type(point[0]) not in (int, float) or type(point[1]) not in (int, float):
            raise ValueError("source_polygon_ordinate_contract_drift")
        x, y = float(point[0]), float(point[1])
        if not (x == x and y == y and -1e308 < x < 1e308 and -1e308 < y < 1e308):
            raise ValueError("source_polygon_ordinate_not_finite")
        if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
            raise ValueError("source_coordinate_range_drift")
        points.append([x, y])
    return points


def _ring_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0]
    return sum((ring[index][0] - x0) * (ring[index + 1][1] - y0)
               - (ring[index + 1][0] - x0) * (ring[index][1] - y0)
               for index in range(len(ring) - 1)) / 2


def _group_rings(geometry: object) -> list[list[list[list[float]]]]:
    """Group the producer's served rings into [shell, holes...] lists, unrepaired.

    Verified Esri semantics (pinned capture): rings are closed, clockwise rings
    are exteriors, counter-clockwise rings are holes, and every vertex is an
    (x, y) pair. Vertices pass through verbatim; holes join the smallest
    containing exterior. No vertex is dropped, added, reordered or reprojected.
    """
    if not isinstance(geometry, dict) or set(geometry) - {"rings", "spatialReference"}:
        raise ValueError("source_polygon_geometry_structure_drift")
    if "spatialReference" in geometry:
        _validate_page_spatial_reference(geometry["spatialReference"], "source_geometry")
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    shells: list[tuple[list[list[float]], Polygon]] = []
    holes: list[list[list[float]]] = []
    for ring in rings:
        points = _validate_ring(ring)
        polygon = Polygon(points)
        if _ring_area(points) < 0:
            shells.append((points, polygon))
        else:
            holes.append(points)
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list[list[list[float]]]] = {index: [] for index in range(len(shells))}
    if holes:
        if len(shells) > 1:
            for _points, polygon in shells:
                shapely.prepare(polygon)
        for hole in holes:
            probe = Polygon(hole).representative_point()
            candidates = [(abs(_ring_area(shell_points)), index)
                          for index, (shell_points, polygon) in enumerate(shells)
                          if polygon.contains(probe)]
            if not candidates:
                raise ValueError("source_polygon_hole_unassigned")
            assigned[min(candidates)[1]].append(hole)
    return [[shells[index][0], *assigned[index]] for index in range(len(shells))]


def _geometry_wkb(grouped: list[list[list[list[float]]]]) -> bytes:
    members = [Polygon(shell, holes) for shell, *holes in grouped]
    geometry = members[0] if len(members) == 1 else MultiPolygon(members)
    return shapely.to_wkb(geometry, byte_order=1, output_dimension=2)


def _arrow_type(field: tuple[str, str]) -> pa.DataType:
    kind = field[1]
    if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID"):
        return pa.string()
    if kind in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                "esriFieldTypeDate"):
        return pa.int64()
    if kind in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        return pa.float64()
    raise ValueError("unsupported_source_field_type:" + kind)


def _parquet_bytes(attributes: list[dict], wkbs: list[bytes | None]) -> bytes:
    arrays = []
    for name, kind in EXPECTED_FIELDS:
        values = [row[name] for row in attributes]
        if kind == "esriFieldTypeDate":
            if any(value is not None and type(value) is not int for value in values):
                raise ValueError("source_date_value_type_drift:" + name)
        if kind == "esriFieldTypeOID" and any(type(value) is not int for value in values):
            raise ValueError("source_object_id_value_drift")
        arrays.append(pa.array(values, type=_arrow_type((name, kind))))
    arrays.append(pa.array(wkbs, type=pa.binary()))
    schema_fields = [pa.field(field_name, _arrow_type((field_name, field_kind)), nullable=True)
                     for field_name, field_kind in EXPECTED_FIELDS]
    schema_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
    table = pa.Table.from_arrays(arrays, names=[field.name for field in schema_fields])
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
           "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                         "geometry_types": ["Polygon", "MultiPolygon"],
                                         "crs": crs, "edges": "planar"}}}
    table = table.replace_schema_metadata({b"geo": _json_bytes(geo).strip()})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _capture_windows(client, roster: list[int], staging, version_key: str, budget: list[int],
                     receipts: dict[str, dict]) -> dict[str, dict]:
    """Capture every roster window, bisecting refused or oversized windows.

    Each accepted window is one exact ``objectIds`` request over a contiguous
    roster slice. A window that answers with an Esri error document, exceeds the
    byte cap or reports a truncated result is bisected into two smaller windows
    over the same roster slice: those producer answers are deterministic for the
    requested window (observed 2026-11-14: windows above ~100 roster ids refuse
    with the same HTTP 200 Esri 500 error document), so bisection is
    completeness-neutral and only changes page boundaries. Transport faults are
    retried a bounded number of times with backoff and every failed attempt is
    preserved on disk under a collision-free ``.attempt_<n>_<cap>cap`` evidence
    name that never enters the capture roster. A single roster id that still
    refuses after bisection fails the capture closed.
    """
    pages: dict[str, dict] = {}
    refused: dict[str, list[int]] = {}
    plan: deque[tuple[int, int]] = deque((start, PAGE_IDS)
                                         for start in range(0, len(roster), PAGE_IDS))
    while plan:
        start, size = plan.popleft()
        window_ids = roster[start:start + size]
        size = len(window_ids)
        name = f"page_{window_ids[0]:07d}"
        params = _window_params(window_ids)
        accepted = False
        for attempt in range(1, PAGE_ATTEMPTS + 1):
            try:
                doc = _request(client, name, params, staging, version_key, budget, receipts,
                               max_bytes=MAX_PAGE_BYTES)
            except ValueError as exc:
                if "source_total_capture_budget_exceeded" in str(exc):
                    raise
                error_code = _failed_attempt_error_code(staging, version_key, name, attempt,
                                                        MAX_PAGE_BYTES)
                if error_code in ("wire_limit_exceeded", "decoded_limit_exceeded"):
                    break
                if attempt < PAGE_ATTEMPTS:
                    time.sleep(PAGE_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
                    continue
                break
            if doc.get("exceededTransferLimit"):
                break
            pages[name] = doc
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
    if refused:
        raise ValueError("source_window_persistent_producer_refusal:" + ",".join(sorted(refused)))
    for name in pages:
        pages[name]["requested_url"] = receipts[name]["url"]
    return pages


def _validate_pages(pages: dict[str, dict]) -> dict:
    """Validate the complete page capture against the pinned producer contract.

    Attributes and per-feature ring groups are accumulated in roster order;
    the geometry digest is streamed over the same record sequence so the pin is
    independent of the capture's (completeness-neutral) page boundaries.
    """
    page_entries = []
    for name in pages:
        start = int(name.removeprefix("page_"))
        if not 0 <= start <= 9_999_999:
            raise ValueError("source_page_name_invalid:" + name)
        page_entries.append((start, name))
    page_entries.sort()
    attributes: list[dict] = []
    wkbs: list[bytes | None] = []
    captured_ids: list[int] = []
    null_geometry = 0
    observed_bounds: list[float] = []
    huc10_seen: set[str] = set()
    ring_count = 0
    vertex_count = 0
    geometry_hash = hashlib.sha256()
    geometry_hash.update(b"[")
    first_record = True
    for _start, name in page_entries:
        page = pages[name]
        _validate_page_spatial_reference(page.get("spatialReference"), f"source_page_{name}")
        if page.get("exceededTransferLimit"):
            raise ValueError("source_page_transfer_truncated:" + name)
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_geometry_type_drift:" + name)
        if set(page) - _PAGE_ALLOWED_MEMBERS:
            raise ValueError("source_page_unexpected_members:" + name)
        requested = _window_url_ids(page["requested_url"])
        rows = page.get("features")
        if not isinstance(rows, list) or not rows:
            raise ValueError("source_page_missing_features:" + name)
        page_ids = []
        for row in rows:
            attrs = row.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != FIELD_NAMES:
                raise ValueError("source_record_attribute_schema_drift:" + name)
            oid = attrs["objectid"]
            if type(oid) is not int or oid <= 0:
                raise ValueError("source_native_identifier_missing_or_type_drift:" + name)
            huc10 = attrs["huc10"]
            if not isinstance(huc10, str) or not huc10:
                raise ValueError("source_huc10_missing:" + name)
            _validate_attrs_types(attrs)
            geometry = row.get("geometry")
            if geometry is None:
                null_geometry += 1
                grouped: list[list[list[list[float]]]] = []
                wkbs.append(None)
            else:
                grouped = _group_rings(geometry)
                wkbs.append(_geometry_wkb(grouped))
                rings = [ring for polygon in grouped for ring in polygon]
                ring_count += len(rings)
                vertex_count += sum(len(ring) for ring in rings)
                for ring in rings:
                    for vertex in ring:
                        observed_bounds.extend((vertex[0], vertex[1]))
            page_ids.append(oid)
            captured_ids.append(oid)
            attributes.append(attrs)
            huc10_seen.add(huc10)
            if not first_record:
                geometry_hash.update(b",")
            first_record = False
            geometry_hash.update(_canonical([oid, grouped]))
        if page_ids != sorted(page_ids):
            raise ValueError("source_page_ids_not_ascending:" + name)
        if sorted(page_ids) != sorted(requested):
            raise ValueError("source_page_id_slice_incomplete_or_reordered:" + name)
    geometry_hash.update(b"]")
    return {"attributes": attributes, "wkbs": wkbs, "ids": captured_ids,
            "null_geometry_count": null_geometry,
            "huc10_seen": huc10_seen, "ring_count": ring_count, "vertex_count": vertex_count,
            "observed_bounds_raw": observed_bounds, "geometry_sha256": geometry_hash.hexdigest()}


def _validate_captures(roster: list[int], pages: dict[str, dict], fields: list[dict]) -> dict:
    """Validate the complete capture against the pinned producer contract."""
    result = _validate_pages(pages)
    if sorted(result["ids"]) != roster:
        raise ValueError("source_feature_population_incomplete")
    if result["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if len(result["huc10_seen"]) != EXPECTED_UNIQUE_HUC10_COUNT:
        raise ValueError("source_huc10_identifier_drift")
    raw = result["observed_bounds_raw"]
    if not raw:
        raise ValueError("source_observed_coverage_empty")
    bounds = {"xmin": min(raw[0::2]), "ymin": min(raw[1::2]),
              "xmax": max(raw[0::2]), "ymax": max(raw[1::2])}
    if bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    record_sha = _sha(_canonical(result["attributes"]))
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if result["geometry_sha256"] != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    audit = {"source_feature_count": len(result["attributes"]),
             "source_id_sha256": _sha(_canonical(result["ids"])),
             "source_record_sha256": record_sha,
             "source_geometry_sha256": result["geometry_sha256"],
             "source_schema_sha256": _schema_sha(fields),
             "source_crs_epsg": SOURCE_CRS_WKID, "source_layer_native_sr": NATIVE_LAYER_SR,
             "geometry_type": "Polygon / MultiPolygon",
             "null_geometry_count": result["null_geometry_count"],
             "observed_bounds": bounds, "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(set(result["ids"])),
             "unique_huc10_count": len(result["huc10_seen"]),
             "source_ring_count": result["ring_count"],
             "source_vertex_count": result["vertex_count"],
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The wbd service publishes no per-snapshot release date; the service "
                 "copyrightText records a service refresh (October, 2026) and is freshness "
                 "evidence only.",
             "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT}
    return {**result, "audit": audit}


class _RetainedCaptureClient:
    """Read immutable successful responses from an earlier retained capture."""

    def __init__(self, staging, source_version: str):
        self.staging = staging
        self.source_version = source_version
        self.retained_capture = self

    def read_response(self, name: str, params: dict[str, object]):
        root = self.staging.build_target_location(DATASET_SLUG, FILE_SLUG, self.source_version, "")
        body_key = root + f"raw/{name}.json"
        wire_key = root + f"raw/{name}.wire"
        receipt_key = root + f"raw/{name}.receipt.json"
        try:
            body, wire = self.staging.read_key(body_key), self.staging.read_key(wire_key)
            receipt = json.loads(self.staging.read_key(receipt_key))
        except (OSError, ValueError) as exc:
            raise ValueError("retained_source_response_missing:" + name) from exc
        if (receipt.get("method") != "GET" or receipt.get("complete") is not True
                or type(receipt.get("status")) is not int or not 200 <= receipt["status"] < 300
                or receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("sha256") != _sha(body) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("retained_source_receipt_mismatch:" + name)
        parsed_url = urlsplit(receipt.get("url", ""))
        expected_url = urlsplit(_request_url(name, params))
        if (parsed_url.scheme != expected_url.scheme or parsed_url.netloc != expected_url.netloc
                or parsed_url.path != expected_url.path):
            raise ValueError("retained_source_request_identity_mismatch:" + name)
        query = parse_qs(parsed_url.query, keep_blank_values=True)
        for key, value in params.items():
            expected = str(value).lower() if isinstance(value, bool) else str(value)
            if query.get(key) != [expected]:
                raise ValueError("retained_source_request_parameters_mismatch:" + name)
        return body, wire, receipt


def _capture(client, staging, version_key: str, budget: list[int],
             receipts: dict[str, dict], distribution_chain: bool) -> dict:
    if distribution_chain:
        _validate_directory(_request(client, "services_directory", {}, staging, version_key,
                                     budget, receipts))
        service_before = _request(client, "service_before", {}, staging, version_key, budget, receipts)
        _validate_service_document(service_before)
        _validate_alias_document(_request(client, "alias_service", {}, staging, version_key,
                                          budget, receipts))
    else:
        service_before = _request(client, "service_before", {}, staging, version_key, budget, receipts)
    layer_before = _request(client, "layer_before", {}, staging, version_key, budget, receipts)
    fields = _validate_layer_document(layer_before)
    count_before = _count_snapshot(client, staging, version_key, "count_before", budget, receipts)
    ids_before = _id_snapshot(client, staging, version_key, "ids_before", budget, receipts)
    # The seed roster is what the windows actually walk; it is re-fetched after
    # the pinned roster so population drift between the two reads fails closed.
    seed_doc = _request(client, "ids_seed", _roster_params(), staging, version_key, budget, receipts)
    roster = _validate_roster(seed_doc, "source_seed_id_population_drift")
    pages = _capture_windows(client, roster, staging, version_key, budget, receipts)
    service_after = _request(client, "service_after", {}, staging, version_key, budget, receipts)
    _validate_service_roster(service_after)
    if service_after != service_before:
        raise ValueError("official_usgs_wbd_service_changed_during_capture")
    layer_after = _request(client, "layer_after", {}, staging, version_key, budget, receipts)
    if layer_after != layer_before:
        raise ValueError("source_layer_metadata_changed_during_capture")
    if _count_snapshot(client, staging, version_key, "count_after", budget, receipts) != count_before:
        raise ValueError("source_count_changed_during_capture")
    if _id_snapshot(client, staging, version_key, "ids_after", budget, receipts) != ids_before:
        raise ValueError("source_population_changed_during_capture")
    extracted = _validate_captures(roster, pages, fields)
    client_docs = {"services_directory": None, "service_before": service_before,
                   "alias_service": None, "layer_before": layer_before,
                   "count_before": {"count": count_before}, "ids_before": {"objectIds": ids_before},
                   "ids_seed": seed_doc, "service_after": service_after,
                   "layer_after": layer_after,
                   "count_after": {"count": count_before},
                   "ids_after": {"objectIds": ids_before}}
    return {"fields": fields, "extracted": extracted, "pages": pages,
            "client_docs": client_docs, "receipts": receipts}


def _write_source_manifest(staging, version_key: str, client_docs: dict, fields: list[dict],
                           audit: dict, identity: dict, receipts: dict[str, dict],
                           retained_source_version: str | None, parquet_body: bytes,
                           distribution_chain: bool) -> None:
    prefix = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
    inputs = []
    retry_receipts = []
    page_names = []
    for key in staging.list_keys(DATASET_SLUG, FILE_SLUG, version_key):
        marker = "/raw/"
        if marker not in key or not key.endswith(".receipt.json"):
            continue
        name = key.split(marker, 1)[1][:-len(".receipt.json")]
        receipt = json.loads(staging.read_key(key))
        if ".attempt_" in name:
            retry_receipts.append({"artifact": f"raw/{name}.receipt.json",
                                   "status": receipt.get("status"),
                                   "sha256": receipt.get("sha256"),
                                   "wire_sha256": receipt.get("wire_sha256")})
            continue
        if receipt.get("complete") is not True or not 200 <= receipt.get("status", 0) < 300:
            raise ValueError("manifest_contains_failed_source_input")
        if name.startswith("page_"):
            page_names.append(name)
            continue
        request_url = receipt["url"]
        if name == "services_directory":
            canonical_url = SERVICES_DIRECTORY_URL
        elif name.startswith("service"):
            canonical_url = SERVICE_URL
        elif name.startswith("alias"):
            canonical_url = ALIAS_SERVICE_URL
        else:
            canonical_url = LAYER_URL
        inputs.append({
            "input_id": name, "producer": PUBLISHER,
            "host": urlsplit(request_url).hostname, "role": "producer_publication",
            "canonical_url": canonical_url, "retrieval_url": request_url,
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "raw_path": f"raw/{name}.json",
            "license": ("The producer's service description states 'All data are open and "
                        "non-proprietary'; rights acceptance pending."),
            "attribution": "U.S. Geological Survey (the service requests acknowledgment of "
                           "the U.S. Geological Survey; attribution terms pending review).",
            "release_date": None,
            "release_date_unknown_reason": "The wbd service publishes no per-snapshot release "
                                           "date; its copyrightText records a service refresh "
                                           "(October, 2026), which is freshness evidence only.",
        })
    if not page_names:
        raise ValueError("paged_capture_inputs_missing")
    page_receipts = [receipts[name] for name in sorted(page_names)]
    if {receipt["status"] for receipt in page_receipts} != {200}:
        raise ValueError("paged_capture_http_status_drift")
    parquet_rel = f"geoparquet/{FILE_SLUG}.parquet"
    page_entry = {"input_id": "wbd_paged_feature_queries", "producer": PUBLISHER,
                  "host": urlsplit(page_receipts[0]["url"]).hostname,
                  "role": "producer_publication",
                  "canonical_url": LAYER_URL,
                  "retrieval_url": page_receipts[0]["url"],
                  "retrieved_at": page_receipts[0]["observed_at"],
                  "http_status": 200,
                  "bytes": sum(int(receipt["bytes"]) for receipt in page_receipts),
                  "sha256": _sha(_canonical(page_receipts)),
                  "page_query_count": len(page_receipts),
                  "page_capture_window": [min(receipt["observed_at"] for receipt in page_receipts),
                                          max(receipt["observed_at"] for receipt in page_receipts)],
                  "page_receipts_sha256_basis": "SHA-256 over the canonical JSON array of the "
                                                "per-page receipt objects in ascending page name "
                                                "order, staged verbatim as raw/<name>.receipt.json "
                                                "next to each page's raw body and wire transcript",
                  "license": ("The producer's service description states 'All data are open and "
                              "non-proprietary'; rights acceptance pending."),
                  "attribution": "U.S. Geological Survey; attribution terms pending review.",
                  "release_date": None,
                  "release_date_unknown_reason": "The wbd service publishes no per-snapshot "
                                                 "release date; its copyrightText records a "
                                                 "service refresh (October, 2026)."}
    inputs.append(page_entry)
    manifest = {
        "schema_version": 1, "approved": False, "processor": "HIFLD Next",
        "title": "10-digit HU (Watershed) — unaccepted USGS WBD native snapshot draft",
        "description": "Complete observed USGS The National Map wbd FeatureServer layer 5 "
                       "'10-digit HU (Watershed)' candidate; 18,911 hydrologic-unit watershed "
                       "polygons with zero-padded ten-character HUC10 identifiers. The DHS HIFLD "
                       "Open crosswalk marks the WBD row Deprecate and directs users to the "
                       "producing agency's own wbd service. Edition, HIFLD continuity, baseline, "
                       "schema/ID contract, coverage, rights and release decisions remain pending.",
        "agency": "U.S. Geological Survey",
        "publisher": PUBLISHER,
        "source_url": LAYER_URL,
        "documentation_url": SERVICE_URL,
        "license": "Producer service states 'All data are open and non-proprietary'; rights "
                   "acceptance pending.",
        "date_issued": None, "date_modified": None,
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "processing_code": identity, "inputs": inputs,
        "transformations": [{"operation": "Complete ESRI JSON WBD polygon capture to typed GeoParquet evidence candidate",
                             "parameters": {"attributes": "All declared nongeometry fields and values retained in source order; producer-declared string fields as Arrow string, doubles as float64, the objectid OID as int64 and the loaddate epoch-milliseconds as int64.",
                                            "identifiers": "objectid, tnmid and huc10 retained exactly, including the zero-padded ten-character HUC10 strings; no synthetic identifier or join.",
                                            "geometry": "Served outSR=4326 polygon rings retained verbatim as WKB in EPSG:4326 under the producer's verified Esri ring semantics (closed rings; clockwise exteriors, counter-clockwise holes assigned to their smallest containing exterior); no reprojection, quantization, repair or vertex change.",
                                            "candidate_status": "Unaccepted draft; no output or release approval implied."}}],
        "lineage": {"scope": "Complete observed USGS wbd FeatureServer layer 5 '10-digit HU "
                             "(Watershed)'; the hydrologic-unit watershed polygons only; WBDLine "
                             "and the other HU levels are separate inventory entries.",
                    "layer_id": LAYER_ID,
                    "source_description": client_docs["layer_before"].get("description"),
                    "source_copyright_text": client_docs["layer_before"].get("copyrightText"),
                    "service_url": SERVICE_URL, "alias_service_url": ALIAS_SERVICE_URL,
                    "services_directory": SERVICES_DIRECTORY_URL,
                    **({"distribution_chain": "USGS The National Map server hydro.nationalmap.gov "
                                              "publishes the wbd FeatureServer and wbd MapServer "
                                              "(both listed in the host's own services directory); "
                                              "the DHS HIFLD Open crosswalk row 'Watershed Boundary "
                                              "Dataset (WBD)' (Status Deprecate, DUA Required No, "
                                              "GII Access Required No) directs users to the wbd "
                                              "MapServer as the producer publication. The same-org "
                                              "MapServer is captured as alias identity evidence."}
                       if distribution_chain else {}),
                    "crosswalk_doi": CROSSWALK_DOI,
                    "crosswalk_row": CROSSWALK_ROW,
                    "crosswalk_status": "Deprecate ('No longer supported as part of the HIFLD "
                                        "mission'; DUA Required No, GII Access Required No)",
                    "crosswalk_open_id": CROSSWALK_OPEN_ID,
                    "crosswalk_verified": "2026-11-14 from the byte-verified public crosswalk file "
                                          "(sha256 7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d9"
                                          "0154c07dc38772bc); the DataLumos-hosted DOI copy was not "
                                          "fetched (banned mirror host).",
                    "hifld_continuity": {"basis": "The archived HIFLD inventory row's description is "
                                                  "the producer layer description verbatim and the "
                                                  "producer layer title is the exact inventory "
                                                  "title; the DataLumos archive (project 240691) is "
                                                  "identification context only and is never "
                                                  "fetched.",
                                         "hifld_continuity_accepted": False},
                    "producer_succession_claimed": False,
                    "source_release_date_known": False,
                    "source_last_data_edit_observed": None,
                    "source_last_data_edit_basis": "The wbd layer and service documents expose no "
                                                   "editingInfo/dataLastEditDate; the service "
                                                   "copyrightText 'Data refreshed October, 2026.' is "
                                                   "the producer's own refresh statement.",
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "raw_capture_mode": ("retained_response_replay" if retained_source_version
                                         else "live_http_capture"),
                    "retained_source_capture_version": retained_source_version,
                    "failed_retry_responses": retry_receipts,
                    "source_contract_accepted": False, "schema_contract_accepted": False,
                    "id_contract_accepted": False, "coverage_accepted": False,
                    "rights_accepted": False, "baseline_accepted": False, "release_accepted": False},
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and served "
                      "EPSG:4326 polygon geometry retained; all freshness, contract, coverage, "
                      "rights and release approvals remain pending.",
            "objects": [{"path": parquet_rel,
                         "bytes": len(parquet_body), "sha256": _sha(parquet_body)}]}}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved field-name, type, or geometry conversion mapping exists.",
            "geopackage": "No approved container-schema or release mapping exists.",
            "pmtiles": "No approved reprojection, quantization, or visualization mapping exists.",
        },
    }
    validate_provenance(manifest)
    staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/source_manifest.json",
                  _json_bytes(manifest))


def _quarantined_capture(staging, version_key: str, client: httpx.Client | None,
                         retained_source_version: str | None, distribution_chain: bool) -> dict:
    if retained_source_version is not None:
        if client is not None:
            raise ValueError("retained_replay_does_not_accept_http_client")
        client = _RetainedCaptureClient(staging, retained_source_version)
    owned = client is None
    client = client or httpx.Client(timeout=300, follow_redirects=False)
    try:
        try:
            budget = [0]
            receipts: dict[str, dict] = {}
            capture = _capture(client, staging, version_key, budget, receipts, distribution_chain)
            fields = capture["fields"]
            extracted = capture["extracted"]
            audit = extracted["audit"]
            parquet = _parquet_bytes(extracted["attributes"], extracted["wkbs"])
            del extracted
            staging.write(DATASET_SLUG, FILE_SLUG, version_key,
                          f"geoparquet/{FILE_SLUG}.parquet", parquet)
            identity = processing_identity(Path(__file__).parents[3])
            _write_source_manifest(staging, version_key, capture["client_docs"], fields, audit,
                                   identity, receipts, retained_source_version, parquet,
                                   distribution_chain)
            staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/native_capture.json",
                          _json_bytes({
                              "source_layer": LAYER_URL,
                              "official_service": SERVICE_URL,
                              "alias_service": ALIAS_SERVICE_URL,
                              "services_directory": SERVICES_DIRECTORY_URL,
                              "service_layer_id": LAYER_ID,
                              "layer_name": "10-digit HU (Watershed)",
                              "pinned_id_roster_sha256": EXPECTED_ID_SHA256,
                              "pinned_layer_extent_3857": EXPECTED_LAYER_EXTENT,
                              "declared_layer_extent": capture["client_docs"]["layer_before"].get("extent"),
                              "service_description":
                                  capture["client_docs"]["service_before"].get("serviceDescription"),
                              "source_copyright_text": EXPECTED_COPYRIGHT_TEXT,
                              "source_release_date": None,
                              "source_release_date_known": False,
                              "feature_count": EXPECTED_COUNT,
                              "before_id_sha256": EXPECTED_ID_SHA256,
                              "after_id_sha256": EXPECTED_ID_SHA256,
                              "source_record_sha256": audit["source_record_sha256"],
                              "source_geometry_sha256": audit["source_geometry_sha256"],
                              "source_schema_sha256": audit["source_schema_sha256"],
                              "parquet_sha256": _sha(parquet),
                              "native_crs": f"EPSG:{SOURCE_CRS_WKID}",
                              "field_schema": [list(field) for field in EXPECTED_FIELDS],
                              "attribute_fields": EXPECTED_FIELDS,
                              "page_count": len(capture["pages"]),
                              "pages": [{"page": name,
                                         "first_objectid": int(name.removeprefix("page_")),
                                         "feature_count": len(page["features"])}
                                        for name, page in sorted(capture["pages"].items())],
                              "code_identity": identity,
                              "coverage_statement": "Complete observed USGS wbd FeatureServer layer "
                                                    "5 '10-digit HU (Watershed)'; hydrologic-unit "
                                                    "watershed polygons only.",
                              "capture_mode": ("retained_response_replay" if retained_source_version
                                               else "live_http_capture"),
                              "retained_source_capture_version": retained_source_version,
                              "refresh_semantics": "Default runs capture a fresh source snapshot. An "
                                                   "explicit retained_response_replay is an evidence-only "
                                                   "reprocessing operation, not a cache or currentness claim.",
                              "historical_hifld_continuity_accepted": False,
                              "source_contract_accepted": False,
                              "schema_contract_accepted": False,
                              "id_contract_accepted": False,
                              "coverage_accepted": False,
                              "rights_accepted": False,
                              "release_accepted": False,
                          }))
            return {"feature_count": EXPECTED_COUNT, "ids_sha256": EXPECTED_ID_SHA256,
                    "records_sha256": audit["source_record_sha256"],
                    "geometry_sha256": audit["source_geometry_sha256"],
                    "schema_sha256": audit["source_schema_sha256"],
                    "parquet_sha256": _sha(parquet), "release_accepted": False,
                    "source_population_complete": True,
                    "source_coverage_scope": "Complete observed USGS wbd FeatureServer layer 5 "
                                             "'10-digit HU (Watershed)'; hydrologic-unit watershed "
                                             "polygons only."}
        except Exception as exc:
            error = str(exc).split(":", 1)[0]
            staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/quarantine.json",
                          _json_bytes({
                              "status": "quarantined", "source_layer": LAYER_URL,
                              "observed_at": datetime.now(timezone.utc).isoformat(),
                              "failure_code": error,
                              "capture_module_sha256": _sha(Path(__file__).read_bytes()),
                              "release_accepted": False,
                          }))
            raise
    finally:
        if owned:
            client.close()


def capture(staging, version_key: str, client: httpx.Client | None = None,
            retained_source_version: str | None = None) -> dict:
    """Baseline capture primitive: complete layer snapshot with roster/count
    checks and per-page fail-closed validation, retaining failed attempts as
    quarantine evidence. This function intentionally does not request the host
    services directory or the alias MapServer document. Production callers must
    use capture_validated."""
    return _quarantined_capture(staging, version_key, client, retained_source_version,
                                distribution_chain=False)


def capture_validated(staging, version_key: str, client: httpx.Client | None = None,
                      retained_source_version: str | None = None) -> dict:
    """Production capture entry: capture() plus fail-closed validation of the
    USGS distribution chain. Before any layer traffic it verifies the host
    services directory entry, the wbd FeatureServer service document and the
    same-org wbd MapServer alias document; after extraction it re-verifies the
    service and layer documents, and it enforces the live-verified pins for the
    OBJECTID roster digest, layer extent and observed coverage."""
    return _quarantined_capture(staging, version_key, client, retained_source_version,
                                distribution_chain=True)
