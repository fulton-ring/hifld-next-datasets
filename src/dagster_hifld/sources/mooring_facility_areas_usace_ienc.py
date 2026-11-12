"""Bounded, source-faithful capture for the official USACE IENC mooring facility areas."""
from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import stat
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import LinearRing, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import CaptureProgress, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "mooring-facility-areas-usace-ienc-2"
FILE_SLUG = DATASET_SLUG
AGC_ITEM_ID = "6ee858a220a143a48c8d40a6cf0d3680"
AGC_ITEM_OWNER = "usace_agc_dkd"
AGC_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{AGC_ITEM_ID}?f=json"
SERVICE_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/USACE_IENC_Master_Service/FeatureServer"
SERVICE_COPYRIGHT = "USACE, US Army Geospatial Center, US Coast Guard, IIC Technologies"
LAYER_ID = 79
LAYER_NAME = "MOORING_FACILITY_AREA"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official USACE_IENC_Master_Service"
EXPECTED_COUNT = 5750
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "129316d2d9b584ad74c644ed2cf03b2f5dc0cdc7e9b4eaa76774a1694fc7cb1f"
PAGE_SIZE = 2000
PAGE_MAX_BYTES = 16_000_000
CAPTURE_MAX_BYTES = 120_000_000
CAPTURE_MANIFEST_MAX_BYTES = 1_000_000
CAPTURE_RECEIPT_MAX_BYTES = 64_000
MIN_FREE_BYTES = 300_000_000
EXPECTED_EXTENT = {"xmin": -96.38243899999998, "ymin": 30.725778000000048,
                   "xmax": -79.48742699999997, "ymax": 45.03707040000006}
# Live 2026-11-10 capture evidence: the source's own vertices touch the declared
# extent within float serialization noise (worst observed 4.8e-11 degrees across
# 3 OIDs), so containment is enforced with this sub-millimetre tolerance. Any
# real coverage change stays far outside it and still fails closed.
EXTENT_TOLERANCE_DEGREES = 1e-9
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"),
    ("Category_of_Mooring_Facility", "esriFieldTypeString"),
    ("Nature_of_Construction", "esriFieldTypeString"),
    ("Object_Name", "esriFieldTypeString"),
    ("Water_Level", "esriFieldTypeString"),
    ("Condition", "esriFieldTypeString"),
    ("Information", "esriFieldTypeString"),
    ("Scale_Minimum", "esriFieldTypeInteger"),
    ("Source_Date", "esriFieldTypeString"),
    ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"),
    ("Feature_ID", "esriFieldTypeString"),
    ("Shape__Area", "esriFieldTypeDouble"),
    ("Shape__Length", "esriFieldTypeDouble"),
]
ATTRIBUTE_FIELDS = [{"name": name, "type": kind, "alias": name, "domain": None,
                    **({"length": 254} if kind == "esriFieldTypeString" else {})}
                   for name, kind in EXPECTED_FIELDS]
SOURCE_CRS = CRS.from_epsg(4326)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    """Validate every advertised ArcGIS CRS representation without guessing.

    ArcGIS may return either/both WKID properties and optional WKT strings.
    All present representations must identify EPSG:4326; missing feature-level
    spatial references inherit only after their page CRS has been validated.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_names() -> list[str]:
    return [f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)]


def _capture_urls(ids: list[int]) -> list[tuple[str, str]]:
    page_count = (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE
    urls = [
        ("agc_item", AGC_ITEM_URL),
        ("service_before", SERVICE_URL + "?f=json"),
        ("layer_before", LAYER_URL + "?f=json"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for index in range(page_count):
        urls.append((f"page_{index:03d}", _query({"where": "1=1", "resultOffset": index * PAGE_SIZE,
                                                   "resultRecordCount": PAGE_SIZE, "outFields": "*",
                                                   "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                                                   "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=json"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    def retained_size() -> int:
        total = 0
        for path in root.iterdir():
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                raise ValueError("source_capture_nonregular_path:" + path.name)
            total += info.st_size
        return total

    def reserve(progress: CaptureProgress) -> None:
        retained = retained_size()
        if shutil.disk_usage(root).free < MIN_FREE_BYTES + progress.reserve_bytes:
            raise ValueError("disk_floor_before_capture_read")
        if (progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > PAGE_MAX_BYTES
                or retained + progress.reserve_bytes > CAPTURE_MAX_BYTES):
            raise ValueError("source_capture_budget_exceeded_before_read")

    def before_retain(kind: str, data: bytes) -> None:
        retained = retained_size()
        if shutil.disk_usage(root).free < MIN_FREE_BYTES + len(data):
            raise ValueError("disk_floor_before_capture_persist")
        proposed = retained + len(data)
        if proposed > CAPTURE_MAX_BYTES:
            raise ValueError("source_capture_budget_exceeded_before_persist")
        if kind in {"wire", "body"} and len(data) > PAGE_MAX_BYTES:
            raise ValueError("source_capture_page_limit_exceeded_before_persist")

    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_source_read")
    if retained_size() + 2 * PAGE_MAX_BYTES + CAPTURE_RECEIPT_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES,
                                reserve=reserve, before_retain=before_retain)
    return body, receipt


def _open_capture_file(path: Path, label: str):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ValueError("retained_capture_file_open_failed:" + label) from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("retained_capture_nonregular_file:" + label)
        return os.fdopen(fd, "rb"), before
    except BaseException:
        os.close(fd)
        raise


def _capture_file_limit(name: str) -> int:
    if name == "capture_manifest.json":
        return CAPTURE_MANIFEST_MAX_BYTES
    if name.endswith(".body") or name.endswith(".wire"):
        return PAGE_MAX_BYTES
    if name.endswith(".receipt.json"):
        return CAPTURE_RECEIPT_MAX_BYTES
    raise ValueError("retained_capture_unexpected_file:" + name)


def _bounded_capture_read(path: Path, limit: int, label: str) -> bytes:
    stream, before = _open_capture_file(path, label)
    with stream:
        if before.st_size > limit:
            raise ValueError("retained_capture_file_size_exceeded:" + label)
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        ident = lambda stat_result: (stat_result.st_dev, stat_result.st_ino, stat_result.st_size,
                                     stat_result.st_mtime_ns, stat_result.st_ctime_ns)
        if len(data) > limit or len(data) != before.st_size or ident(before) != ident(after):
            raise ValueError("retained_capture_file_changed_or_oversized:" + label)
    return data


def _bounded_capture_roster(root: Path, expected_files: set[str]) -> dict[str, dict[str, object]]:
    """Preflight the complete replay budget, then hash artifacts with bounded reads."""
    if not root.is_dir() or root.is_symlink():
        raise ValueError("retained_capture_roster_root_invalid")
    entries = {}
    total = 0
    for entry in root.iterdir():
        info = entry.lstat()
        if entry.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise ValueError("retained_capture_nonregular_path:" + entry.name)
        if info.st_size > _capture_file_limit(entry.name):
            raise ValueError("retained_capture_file_size_exceeded:" + entry.name)
        entries[entry.name] = info
        total += info.st_size
        if total > CAPTURE_MAX_BYTES:
            raise ValueError("retained_capture_total_size_exceeded")
    if set(entries) != expected_files:
        raise ValueError("capture_roster_mismatch")
    result = {}
    for name in sorted(expected_files - {"capture_manifest.json"}):
        stream, before = _open_capture_file(root / name, name)
        digest = hashlib.sha256()
        size = 0
        limit = _capture_file_limit(name)
        with stream:
            while True:
                block = stream.read(min(64 * 1024, limit + 1 - size))
                if not block:
                    break
                size += len(block)
                if size > limit:
                    raise ValueError("retained_capture_file_size_exceeded:" + name)
                digest.update(block)
            after = os.fstat(stream.fileno())
            ident = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
            if size != before.st_size or ident(before) != ident(after):
                raise ValueError("retained_capture_file_changed_or_oversized:" + name)
        result[name] = {"bytes": size, "sha256": digest.hexdigest()}
    return result


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                          root / f"{name}.receipt.json")
    body = _bounded_capture_read(body_path, PAGE_MAX_BYTES, name + ".body")
    wire = _bounded_capture_read(wire_path, PAGE_MAX_BYTES, name + ".wire")
    receipt = json.loads(_bounded_capture_read(receipt_path, CAPTURE_RECEIPT_MAX_BYTES, name + ".receipt.json"))
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, wire, receipt


def _validate_capture_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    """Bind retained response bytes to the exact GET endpoint that names them."""
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    manifest = json.loads(_bounded_capture_read(manifest_path, CAPTURE_MANIFEST_MAX_BYTES, "capture_manifest.json"))
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict):
        raise ValueError("retained_capture_roster_missing")
    if set(manifest["receipts"]) != {name for name, _url in _capture_urls(EXPECTED_IDS)}:
        raise ValueError("retained_capture_name_roster_changed")
    expected_files = set(roster) | {"capture_manifest.json"}
    if set(roster) != {f"{name}.{suffix}" for name, _url in _capture_urls(EXPECTED_IDS)
                       for suffix in ("body", "wire", "receipt.json")}:
        raise ValueError("retained_capture_roster_changed")
    validate_capture_roster(root, expected_files)
    if roster != _bounded_capture_roster(root, expected_files):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_urls(EXPECTED_IDS))
    captures = {}
    for name in sorted(manifest["receipts"]):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"][name]:
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _esri_polygon_wkb(geometry: dict, oid: int) -> bytes:
    """Encode native ArcGIS polygon rings in source order without reorientation or repair."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError(f"source_polygon_rings_missing:{oid}")
    for flag in ("hasZ", "hasM"):
        if flag in geometry and geometry[flag] is not False:
            raise ValueError(f"source_polygon_ordinate_flag_drift:{oid}")
    shells: list[list[list[float]]] = []
    holes: list[list[list[float]]] = []
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError(f"source_polygon_ring_invalid:{oid}")
        if coords[0] != coords[-1]:
            raise ValueError(f"source_polygon_ring_not_closed:{oid}")
        if any(not isinstance(point, list) or len(point) != 2
               or any(isinstance(value, bool) or not isinstance(value, (int, float))
                      or not math.isfinite(float(value)) for value in point)
               for point in coords):
            raise ValueError(f"source_polygon_coordinate_not_finite_xy:{oid}")
        ring = LinearRing([point[:2] for point in coords])
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    # ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    # Reject non-standard winding rather than repairing or guessing.
    if not shells:
        raise ValueError(f"source_polygon_shell_missing:{oid}")
    shell_polys = [Polygon(ring) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon(ring)
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError(f"source_hole_ownership_ambiguous:{oid}")
        assignments[owners[0]].append(ring)

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<dd", float(point[0]), float(point[1])))
        return bytes(out)

    polygons = [polygon_wkb(shells[index], assignments[index]) for index in range(len(shells))]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    item = docs["agc_item"]
    if (item.get("id") != AGC_ITEM_ID or item.get("owner") != AGC_ITEM_OWNER
            or item.get("type") != "Feature Service" or item.get("title") != "USACE_IENC_Master_Service"
            or item.get("url") != SERVICE_URL
            or item.get("accessInformation") != SERVICE_COPYRIGHT):
        raise ValueError("agc_master_service_item_identity_drift")
    service = docs["service_before"]
    service_layers = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                      for layer in service.get("layers", [])]
    if (service.get("serviceItemId") != AGC_ITEM_ID or service.get("copyrightText") != SERVICE_COPYRIGHT
            or (LAYER_ID, LAYER_NAME, "esriGeometryPolygon") not in service_layers):
        raise ValueError("official_master_service_identity_drift")
    _validate_spatial_reference(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected = [{key: field[key] for key in ("name", "type", "alias", "length", "domain") if key in field}
                 for field in fields] if isinstance(fields, list) else None
    if projected != ATTRIBUTE_FIELDS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("serviceItemId") != AGC_ITEM_ID or layer.get("objectIdField") != "OBJECTID"
            or layer.get("maxRecordCount") != PAGE_SIZE
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_geometry_crs_or_page_limit_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent", {})
    if "spatialReference" in extent:
        _validate_spatial_reference(extent["spatialReference"], "source_layer_extent")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or sorted(ids_before) != EXPECTED_IDS or ids_after != ids_before
            or len(set(ids_before)) != EXPECTED_COUNT or _sha(_canonical(sorted(ids_before))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_population_drift")
    features: list[dict] = []
    page_names = sorted(name for name in captures if name.startswith("page_"))
    if len(page_names) != (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in page_names:
        page = docs[name]
        if "error" in page:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), "source_page")
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_geometry_type_drift")
        rows = page.get("features")
        expected_page_size = min(PAGE_SIZE, EXPECTED_COUNT - len(features))
        if not isinstance(rows, list) or len(rows) != expected_page_size:
            raise ValueError("source_page_features_missing_or_incomplete")
        final_page = len(features) + len(rows) == EXPECTED_COUNT
        if final_page and page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_transfer_limit_truncated")
        features.extend(rows)
    expected_attrs = [field["name"] for field in ATTRIBUTE_FIELDS]
    ids: list[int] = []
    geometry_valid = geometry_empty = 0
    vertex_count = 0
    ring_count_histogram: dict[str, int] = {}
    shell_count = 0
    hole_count = 0
    xs: list[float] = []
    ys: list[float] = []
    feature_ids: list[str] = []
    feature_id_null_count = 0
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(expected_attrs) or len(attrs) != len(expected_attrs):
            raise ValueError("source_attribute_roster_drift")
        for field_name, field_type in EXPECTED_FIELDS:
            value = attrs[field_name]
            if value is None:
                continue
            if field_type == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if field_type == "esriFieldTypeInteger" and type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + field_name)
            if field_type == "esriFieldTypeDouble" and (type(value) not in (int, float)
                                                        or isinstance(value, bool) or not math.isfinite(value)):
                raise ValueError("source_double_value_type_drift:" + field_name)
            if field_type == "esriFieldTypeString" and not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + field_name)
        oid = attrs["OBJECTID"]
        if oid not in EXPECTED_IDS:
            raise ValueError("source_oid_roster_drift")
        ids.append(oid)
        feature_id = attrs["Feature_ID"]
        if feature_id is None:
            feature_id_null_count += 1
        else:
            feature_ids.append(feature_id)
        if not isinstance(geometry, dict) or set(geometry) - {"rings", "hasZ", "hasM", "spatialReference"}:
            raise ValueError("source_polygon_geometry_structure_drift")
        if "spatialReference" in geometry:
            _validate_spatial_reference(geometry["spatialReference"], "source_feature_geometry")
        rings = geometry.get("rings")
        if not isinstance(rings, list) or not rings:
            raise ValueError(f"source_polygon_rings_missing:{oid}")
        ring_count_histogram[str(len(rings))] = ring_count_histogram.get(str(len(rings)), 0) + 1
        for ring in rings:
            for point in ring:
                if (not isinstance(point, list) or len(point) != 2
                        or any(isinstance(value, bool) or not isinstance(value, (int, float))
                               or not math.isfinite(float(value)) for value in point)):
                    raise ValueError(f"source_polygon_coordinate_not_finite_xy:{oid}")
                if not (-180.0 <= point[0] <= 180.0 and -90.0 <= point[1] <= 90.0):
                    raise ValueError(f"source_point_outside_epsg4326_domain:{oid}")
                if not (EXPECTED_EXTENT["xmin"] - EXTENT_TOLERANCE_DEGREES <= point[0]
                        <= EXPECTED_EXTENT["xmax"] + EXTENT_TOLERANCE_DEGREES
                        and EXPECTED_EXTENT["ymin"] - EXTENT_TOLERANCE_DEGREES <= point[1]
                        <= EXPECTED_EXTENT["ymax"] + EXTENT_TOLERANCE_DEGREES):
                    raise ValueError(f"source_geometry_outside_declared_coverage:{oid}")
                xs.append(point[0])
                ys.append(point[1])
                vertex_count += 1
        wkb = _esri_polygon_wkb(geometry, oid)
        shell_count += sum(1 for ring in rings if not LinearRing([point[:2] for point in ring]).is_ccw)
        hole_count += len(rings) - sum(1 for ring in rings if not LinearRing([point[:2] for point in ring]).is_ccw)
        shape = shapely.from_wkb(wkb)
        # Reject invalid or empty topology before anything is staged or sealed.
        # The captured geometry is preserved verbatim; it is never repaired.
        if shape.is_empty:
            raise ValueError(f"source_polygon_topology_empty:{oid}")
        if not shape.is_valid:
            raise ValueError(f"source_polygon_topology_invalid:{oid}")
        geometry_valid += 1
    if sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_ID_population_drift")
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids), "source_schema_sha256": _sha(_canonical(fields)),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]]
                                                   for row in features])),
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertex_count,
        "source_vertex_bounds": {"xmin": min(xs), "ymin": min(ys), "xmax": max(xs), "ymax": max(ys)},
        "source_ring_count_histogram": ring_count_histogram,
        "source_shell_ring_count": shell_count, "source_hole_ring_count": hole_count,
        "geometry_valid_count": geometry_valid, "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "feature_id_distinct_count": len(set(feature_ids)),
        "feature_id_duplicate_count": len(feature_ids) - len(set(feature_ids)),
        "feature_id_null_count": feature_id_null_count,
        "source_extent": layer["extent"],
        "source_item_modified": item.get("modified"),
        "source_coverage_scope": "Complete current MOORING_FACILITY_AREA layer 79 of the official USACE Army "
                                 "Geospatial Center USACE_IENC_Master_Service. IENC coverage is 7,260 miles "
                                 "across 21 rivers, primarily in the central United States; this is not "
                                 "national mooring facility coverage and not asserted equivalent to the "
                                 "historical HIFLD/DataLumos archive.",
        "drift_guard": "Exact AGC master-service item identity (owner usace_agc_dkd) and its serviceItemId "
                       "binding, advertised service/layer identity, declared field roster/types/order, every "
                       "present layer/page/feature wkid, latestWkid and WKT/WKT2 CRS representation, declared "
                       "2D ordinates, count, before/after IDs, per-page feature completeness, complete returned "
                       "attributes, polygon ring structure/winding and valid non-empty topology, EPSG:4326 "
                       "domain per vertex, and every vertex against the declared layer coverage extent within "
                       "a 1e-9 degree serialization tolerance are checked.",
    }
    return fields, features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in fields]
    arrays.append(pa.array([_esri_polygon_wkb(row["geometry"], row["attributes"]["OBJECTID"])
                             for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in fields] + ["geometry"])
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                     {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"],
                                                             "crs": crs}}})}
    import io
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity(captures: dict[str, tuple[bytes, dict]], root: Path) -> dict:
    # Candidate identity binds every decoded and wire artifact. Receipt JSON is
    # retained as provenance but its observation time does not change content identity.
    digests = {}
    for name in sorted(captures):
        _body, _receipt = captures[name]
        _body2, wire, _receipt2 = _load_capture(root, name)
        digests[name] = {"decoded_sha256": _sha(_body), "wire_sha256": _sha(wire)}
    return digests


def capture_mooring_facility_areas_usace_ienc(storage: StagingStorageResource, *, version: str,
                                              capture_directory: str | None = None) -> dict:
    """Capture or replay one complete, unapproved native IENC source candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    identity = processing_identity(Path(__file__).parents[3])
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and (any(requested.glob("*.body")) or any(requested.glob("*.wire"))
                          or any(requested.glob("*.receipt.json"))):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = (requested / (version + "-" + uuid4().hex)) if requested else Path(
            "/tmp/hifld-native-reconstruction/160/captures") / (version + "-" + uuid4().hex)
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                # IDs are acquired first to establish the exact population
                # contract; count and ID brackets below independently detect
                # source drift across the paginated feature retrieval.
                body, receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                                         root, "ids_before")
                ids = json.loads(body).get("objectIds")
                if (not isinstance(ids, list) or sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT
                        or any(type(oid) is not int for oid in ids)):
                    raise ValueError("source_seed_ID_population_drift")
                urls = _capture_urls(ids)
                urls = [(name, url) for name, url in urls if name != "ids_before"]
                captures["ids_before"] = (body, receipt)
                for name, url in urls:
                    captures[name] = _capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            retained = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    retained[path.name.removesuffix(".receipt.json")] = json.loads(
                        _bounded_capture_read(path, CAPTURE_RECEIPT_MAX_BYTES, path.name)
                    )
                except (OSError, ValueError):
                    retained[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"captured_at": datetime.now(timezone.utc).isoformat(),
                                                                     "complete": False,
                                                                     "error_code": type(exc).__name__,
                                                                     "receipts": retained}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        artifact_names = {f"{name}.{suffix}" for name in captures for suffix in ("body", "wire", "receipt.json")}
        (root / "capture_manifest.json").write_bytes(_canonical({"captured_at": datetime.now(timezone.utc).isoformat(),
                                                                 "complete": True,
                                                                 "source_feature_count": audit["source_feature_count"],
                                                                 "source_record_sha256": audit["source_record_sha256"],
                                                                 "source_schema_sha256": audit["source_schema_sha256"],
                                                                 "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
                                                                 "artifact_roster": _bounded_capture_roster(root, artifact_names)}) + b"\n")
        captures = _read_replay(root)
    fields, features, audit = _validate(captures)
    evidence = _capture_identity(captures, root)
    capture_sha = _sha(_canonical(evidence))
    record_body = _canonical([row["attributes"] for row in features])
    geometry_body = _canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features])
    schema_body = _canonical(fields)
    audit.update({"source_capture_evidence_sha256": capture_sha, "source_capture_evidence": evidence,
                  "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
                  "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    version = "v" + _sha(_canonical({"record_sha256": _sha(record_body), "geometry_sha256": _sha(geometry_body),
                                     "schema_sha256": _sha(schema_body), "decoded_wire_evidence_sha256": capture_sha,
                                     "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet = _parquet_bytes(fields, features)
    inputs = [{"input_id": name,
               "producer": "US Army Corps of Engineers, Army Geospatial Center (AGC) USACE_IENC_Master_Service item"
                           if name == "agc_item" else PUBLISHER,
               "host": urlsplit(receipt["url"]).hostname,
               "role": "producer_publication",
               "canonical_url": AGC_ITEM_URL if name == "agc_item" else LAYER_URL,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
               "release_date": None, "release_date_unknown_reason": "The AGC master service item carries a dated revision tag in its snippet but publishes no per-layer release date for this extracted layer snapshot.",
               "license": "Unresolved; rights review pending.",
               "attribution": "USACE / US Army Geospatial Center; attribution and rights review pending.",
               **({"item_evidence": {"agc_item_url": AGC_ITEM_URL, "agc_item_id": AGC_ITEM_ID,
                   "agc_item_owner": AGC_ITEM_OWNER, "service_url": SERVICE_URL,
                   "basis": "The AGC-owned ArcGIS item identifies this FeatureServer as the complete IENC vector data publication; its service and layer 79 metadata re-bind serviceItemId to the same item."}} if name == "agc_item" else {})}
              for name, (_body, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Mooring Facility Areas (USACE IENC)",
                "description": "Complete current MOORING_FACILITY_AREA layer 79 from the official USACE Army "
                               "Geospatial Center USACE_IENC_Master_Service, scoped to that exact inland-waterway "
                               "layer. IENC coverage is 7,260 miles across 21 rivers, primarily in the central "
                               "United States; not national mooring coverage and not asserted equivalent to the "
                               "historical HIFLD/DataLumos archive. Unapproved faithful candidate.",
                "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
                "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
                "transformations": [{"operation": "Native Esri JSON polygon rings to GeoParquet WKB candidate",
                                     "parameters": {"feature_count": EXPECTED_COUNT,
                                                    "source_fields": "all 14 declared attributes retained in source order and declared types, including Shape__Area and Shape__Length",
                                                    "source_ids": "OBJECTID and Feature_ID preserved unchanged",
                                                    "source_crs": "EPSG:4326",
                                                    "source_geometry": "2D polygon rings exactly as captured; no projection, repair or ring reorientation",
                                                    "raw_evidence": "complete decoded response and wire bytes retained with checksummed receipts",
                                                    "candidate_status": "unapproved"}}],
                "lineage": "The US Army Geospatial Center publishes the USACE_IENC_Master_Service FeatureServer as its complete IENC vector data service (ArcGIS item 6ee858a220a143a48c8d40a6cf0d3680, owner usace_agc_dkd; service copyright USACE, US Army Geospatial Center, US Coast Guard, IIC Technologies). This candidate is exactly that service's MOORING_FACILITY_AREA layer 79. The inventory's DataLumos V1 archive is a historical fallback, not the maintained feed, and no equivalence is asserted.",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "All declared source attributes and IDs are typed and retained; the 2D source polygon rings are stored as WKB with source EPSG:4326. Candidate remains unapproved pending compatibility and release decisions.",
                    "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
                "unsupported_output_formats": {"geopackage": "No approved format-specific delivery contract exists.",
                                               "pmtiles": "No approved display/generalization policy exists for this navigation feature class."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    audit["source_fields"] = fields
    audit["official_layer_name"] = LAYER_NAME
    audit["official_layer_id"] = LAYER_ID
    audit["official_source_item"] = AGC_ITEM_OWNER + " " + AGC_ITEM_ID
    outputs = {f"geoparquet/{FILE_SLUG}.parquet": parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
             "objects": {f"geoparquet/{FILE_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
             "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
                                   "source_schema_sha256": audit["source_schema_sha256"],
                                   "capture_evidence_sha256": capture_sha}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if (not isinstance(original, dict) or original.get("processing_identity") != identity
                or original.get("source_record_sha256") != audit["source_record_sha256"]
                or original.get("source_schema_sha256") != audit["source_schema_sha256"]
                or not isinstance(original.get("capture_receipts"), dict)):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            fresh_body, fresh_wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != fresh_body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != fresh_wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
                "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
                "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
                "geometry_valid_count": audit["geometry_valid_count"],
                "geometry_invalid_count": audit["geometry_invalid_count"],
                "geometry_empty_count": audit["geometry_empty_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": capture_sha,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
                "capture_directory": str(root)}
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
                                                                    "source_record_sha256": audit["source_record_sha256"],
                                                                    "source_schema_sha256": audit["source_schema_sha256"],
                                                                    "capture_receipts": receipts,
                                                                    "objects": descriptors})
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
            "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
            "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"],
            "geometry_empty_count": audit["geometry_empty_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_capture_evidence_sha256": capture_sha,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(root)}
