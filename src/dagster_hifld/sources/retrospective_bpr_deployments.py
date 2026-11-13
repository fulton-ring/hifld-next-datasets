"""Native, receipt-bound draft capture of the official NOAA NCEI retrospective
BPR deployments layer.

Producer: NOAA National Centers for Environmental Information. NOAA's published
Natural Hazards Viewer map (www.ncei.noaa.gov/maps/hazards) embeds webmap item
5cb326668189408e8c4c20ddd92aa2ed, whose retrospective-dart-deployments layer
delegates to the NOAA GeoPlatform hosted Feature Service item
764d3a99a4f8440ea05522815b5f5e19 (owner ncei_noaa), layer 1 "Retrospective BPR
Deployments". Every source column, both native identifier populations, the
original Web Mercator coordinates and all raw responses are retained. Pinned
drift quarantines; historical published IDs remain separately preserved.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = FILE_SLUG = "retrospective-bpr-deployments"
SERVICE_URL = ("https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/"
               "Current_DARTs_and_Retrospective_BPR_Deployments/FeatureServer")
LAYER_ID = 1
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
SERVICE_ITEM_ID = "764d3a99a4f8440ea05522815b5f5e19"
SERVICE_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{SERVICE_ITEM_ID}?f=pjson"
WEBMAP_ITEM_ID = "5cb326668189408e8c4c20ddd92aa2ed"
WEBMAP_URL = f"https://www.arcgis.com/sharing/rest/content/items/{WEBMAP_ITEM_ID}/data?f=pjson"
NCEI_MAP_URL = "https://www.ncei.noaa.gov/maps/hazards/?layers=4"
NCEI_LANDING_URL = ("https://www.ncei.noaa.gov/products/natural-hazards/"
                    "tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure")
NOAA_DISCLAIMER_URL = "https://www.noaa.gov/disclaimer"
PUBLISHER = ("NOAA National Centers for Environmental Information; NOAA GeoPlatform hosted "
             "service item " + SERVICE_ITEM_ID + " owned by ncei_noaa")
DARTS_GROUP_LAYER_ID = "darts-group-layer"
DARTS_GROUP_LAYER_TITLE = "Current DARTs and Retrospective BPR Deployments"
RETROSPECTIVE_LAYER_ID = "retrospective-dart-deployments"
RETROSPECTIVE_LAYER_TITLE = "Retrospective BPR Deployments"
SERVICE_LAYER_ROSTER = [(0, "Current DART Deployments"), (1, "Retrospective BPR Deployments")]
EXPECTED_COUNT = 524
EXPECTED_IDS = list(range(1572, 2096))
EXPECTED_ID_SHA256 = "ae90c8160b053709169c9ba8aa58228569d674565799c8d25b59eaec1c4ed603"
PAGE_SIZE = 250
MAX_CAPTURE_BYTES = 6_000_000
EXPECTED_FIELDS = [
    {"alias": "Deployment ID", "defaultValue": None, "domain": None, "editable": True, "length": 30, "name": "STATION_ID", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Latitude", "defaultValue": None, "domain": None, "editable": True, "name": "LATITUDE", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDouble"},
    {"alias": "Longitude", "defaultValue": None, "domain": None, "editable": True, "name": "LONGITUDE", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDouble"},
    {"alias": "Location", "defaultValue": None, "domain": None, "editable": True, "length": 150, "name": "LOCATION", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Depth", "defaultValue": None, "domain": None, "editable": True, "name": "DEPTH", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeInteger"},
    {"alias": "Deployment Date", "defaultValue": None, "domain": None, "editable": True, "length": 8, "name": "DEPLOYMENT_DATE", "nullable": True, "precision": 1, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDate"},
    {"alias": "Recovery Date", "defaultValue": None, "domain": None, "editable": True, "length": 8, "name": "RECOVERY_DATE", "nullable": True, "precision": 1, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDate"},
    {"alias": "Data Start Date", "defaultValue": None, "domain": None, "editable": True, "length": 8, "name": "START_DATE", "nullable": True, "precision": 1, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDate"},
    {"alias": "Data End Date", "defaultValue": None, "domain": None, "editable": True, "length": 8, "name": "END_DATE", "nullable": True, "precision": 1, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDate"},
    {"alias": "Sample Rate", "defaultValue": None, "domain": None, "editable": True, "name": "SAMPLE_RATE", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeDouble"},
    {"alias": "Data Quality", "defaultValue": None, "domain": None, "editable": True, "length": 300, "name": "DATA_QUAL", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Comments", "defaultValue": None, "domain": None, "editable": True, "length": 3000, "name": "COMMENTS", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Deployment Platform", "defaultValue": None, "domain": None, "editable": True, "length": 125, "name": "DEPLOY_PLATFORM", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Recovery Platform", "defaultValue": None, "domain": None, "editable": True, "length": 125, "name": "RECOVERY_PLATFORM", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "OBJECTID", "defaultValue": None, "domain": None, "editable": False, "name": "OBJECTID", "nullable": False, "sqlType": "sqlTypeOther", "type": "esriFieldTypeOID"},
    {"alias": "Dataset ID", "defaultValue": None, "domain": None, "editable": True, "length": 30, "name": "DATASET_ID", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Data URL", "defaultValue": None, "domain": None, "editable": True, "length": 200, "name": "DATA_URL", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Metadata URL", "defaultValue": None, "domain": None, "editable": True, "length": 200, "name": "METADATA_URL", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeString"},
    {"alias": "Data Flag", "defaultValue": None, "domain": None, "editable": True, "name": "DATA_FLAG", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeSmallInteger"},
    {"alias": "ID", "defaultValue": None, "domain": None, "editable": True, "name": "ID", "nullable": True, "sqlType": "sqlTypeOther", "type": "esriFieldTypeInteger"},
]
ATTRIBUTE_FIELDS = list(EXPECTED_FIELDS)
ID_FIELDS = ("OBJECTID", "ID", "STATION_ID", "DATASET_ID")
EXPECTED_EXTENT = {"xmin": -19627852.616669994, "ymin": -5930818.77964573,
                   "xmax": 19846439.568791676, "ymax": 7894836.627928906}
# Every XY coordinate must stay inside the pinned native EPSG:3857 layer extent
# within binary64 float-noise tolerance; drift outside either fails closed.
EXTENT_TOLERANCE_METERS = 1e-6
EXPECTED_CRS = CRS.from_epsg(3857)
_BASE_CAPTURE_NAMES = ("ncei_landing", "ncei_map", "webmap", "service_item", "service_before",
                       "layer_before", "count_before", "ids_before", "service_after",
                       "layer_after", "count_after", "ids_after")
_HTML_CAPTURE_NAMES = frozenset({"ncei_landing", "ncei_map"})


def _page_count() -> int:
    return (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE


def _expected_capture_names() -> set[str]:
    return set(_BASE_CAPTURE_NAMES) | {f"page_{index:03d}" for index in range(_page_count())}


def _expected_capture_urls(ids: list[int] | None = None) -> dict[str, str]:
    roster = _expected_capture_names()
    if ids is None:
        ids = EXPECTED_IDS
    urls = {"ncei_landing": NCEI_LANDING_URL, "ncei_map": NCEI_MAP_URL, "webmap": WEBMAP_URL,
            "service_item": SERVICE_ITEM_URL, "service_before": SERVICE_URL + "?f=pjson",
            "layer_before": LAYER_URL + "?f=pjson",
            "count_before": _query({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
            "ids_before": _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})}
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls[f"page_{index:03d}"] = _query({
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
            "returnGeometry": "true", "orderByFields": "OBJECTID ASC", "f": "json"})
    urls.update({"service_after": SERVICE_URL + "?f=pjson", "layer_after": LAYER_URL + "?f=pjson",
                 "count_after": _query({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
                 "ids_after": _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})})
    if set(urls) != roster:
        raise ValueError("source_capture_url_roster_drift")
    return urls


def _validate_capture_request_identity(name: str, receipt: dict, url: str) -> None:
    if not isinstance(receipt, dict) or receipt.get("url") != url:
        raise ValueError("capture_request_identity_drift:" + name)
    if receipt.get("final_url") != url:
        raise ValueError("capture_redirect_not_allowed:" + name)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_drift")
    found = []
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid not in (3857, 102100):
                raise ValueError(label + "_crs_drift")
            found.append(3857)
    for key in ("wkt", "wkt2"):
        if key in value:
            raw = value[key]
            if not isinstance(raw, str) or not raw.strip():
                raise ValueError(label + "_crs_drift")
            try:
                parsed = CRS.from_wkt(raw)
            except Exception as exc:
                raise ValueError(label + "_crs_drift") from exc
            if not parsed.equals(EXPECTED_CRS, ignore_axis_order=True):
                raise ValueError(label + "_crs_drift")
            found.append(3857)
    if (required and not found) or len(set(found)) > 1:
        raise ValueError(label + "_crs_drift")
    return bool(found)


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture(client: httpx.Client, url: str, root: Path, name: str):
    retained = sum(p.stat().st_size for suffix in ("*.body", "*.wire") for p in root.glob(suffix)
                   if p.is_file())
    if retained + 2 * 2_000_000 > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded")
    return capture_get(client, url, root, name, max_bytes=2_000_000)


def _load_capture(root: Path, name: str):
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(p.is_symlink() for p in paths):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = (paths[0].read_bytes(), paths[1].read_bytes(),
                           json.loads(paths[2].read_bytes()))
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("capture_receipt_invalid:" + name)
    return body, wire, receipt


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    names = _expected_capture_names()
    roster = manifest.get("artifact_roster")
    if (manifest.get("complete") is not True or not isinstance(roster, dict)
            or set(manifest.get("receipts", {})) != names):
        raise ValueError("capture_manifest_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("capture_artifact_roster_changed")
    expected_urls = _expected_capture_urls()
    result = {}
    for name in sorted(names):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("capture_receipt_manifest_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        result[name] = (body, receipt)
    return result


def _docs(captures: dict[str, tuple[bytes, dict]]) -> dict:
    parsed = {}
    for name, (body, _receipt) in captures.items():
        if name in _HTML_CAPTURE_NAMES:
            if b"<!DOCTYPE html" not in body[:200] and b"<html" not in body[:200]:
                raise ValueError("source_html_capture_changed:" + name)
            parsed[name] = body
        else:
            parsed[name] = json.loads(body)
    return parsed


def _find_webmap_layer(items: list, layer_id: str) -> dict | None:
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("id") == layer_id:
            return item
        nested = _find_webmap_layer(item.get("layers", []), layer_id)
        if nested is not None:
            return nested
    return None


def _validate_value(field: dict, value: object, label: str) -> None:
    if value is None:
        if not field.get("nullable", True):
            raise ValueError("source_nonnullable_value_null:" + label)
        return
    kind = field["type"]
    if kind == "esriFieldTypeString":
        if not isinstance(value, str):
            raise ValueError("source_string_type_drift:" + label)
        length = field.get("length")
        if length is not None and len(value) > int(length):
            raise ValueError("source_string_length_drift:" + label)
    elif kind in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
        if type(value) is not int or not -(2 ** 31) <= value < 2 ** 31:
            raise ValueError("source_integer_type_drift:" + label)
    elif kind == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise ValueError("source_double_type_drift:" + label)
    elif kind == "esriFieldTypeDate":
        # Query responses return declared date fields as UTC epoch milliseconds.
        if type(value) is not int or value <= 0:
            raise ValueError("source_date_epoch_ms_drift:" + label)
    elif kind == "esriFieldTypeOID":
        if type(value) is not int or value <= 0:
            raise ValueError("source_oid_type_drift:" + label)
    else:
        raise ValueError("source_unexpected_field_type:" + label + ":" + kind)


def _validate(captures: dict[str, tuple[bytes, dict]]):
    if set(captures) != _expected_capture_names():
        raise ValueError("source_capture_name_roster_drift")
    docs = _docs(captures)
    item = docs["service_item"]
    if (item.get("id") != SERVICE_ITEM_ID or item.get("owner") != "ncei_noaa"
            or item.get("orgId") != "C8EMgrsFcRFL6LrL" or item.get("type") != "Feature Service"
            or item.get("url") != SERVICE_URL):
        raise ValueError("producer_service_item_identity_drift")
    webmap_layer = _find_webmap_layer(docs["webmap"].get("operationalLayers", []),
                                      RETROSPECTIVE_LAYER_ID)
    if (not isinstance(webmap_layer, dict) or webmap_layer.get("url") != LAYER_URL
            or webmap_layer.get("itemId") != SERVICE_ITEM_ID
            or webmap_layer.get("title") != RETROSPECTIVE_LAYER_TITLE):
        raise ValueError("official_noaa_webmap_layer_delegation_changed")
    group = _find_webmap_layer(docs["webmap"].get("operationalLayers", []), DARTS_GROUP_LAYER_ID)
    if not isinstance(group, dict) or group.get("title") != DARTS_GROUP_LAYER_TITLE:
        raise ValueError("official_noaa_webmap_group_delegation_changed")
    map_html = docs["ncei_map"]
    if (b'src="./assets/index-' not in map_html
            or b"<title>Natural Hazards Viewer" not in map_html):
        raise ValueError("official_noaa_map_entry_changed")
    landing = docs["ncei_landing"]
    if (b"DART Ocean Bottom Pressure Data" not in landing
            or b"National Centers for Environmental Information" not in landing):
        raise ValueError("official_noaa_landing_changed")
    service = docs["service_before"]
    observed_layers = [(layer.get("id"), layer.get("name")) for layer in service.get("layers", [])]
    if observed_layers != SERVICE_LAYER_ROSTER:
        raise ValueError("official_service_layer_roster_drift")
    if docs["service_after"] != service or docs["layer_after"] != docs["layer_before"]:
        raise ValueError("source_service_or_layer_changed_during_capture")
    layer = docs["layer_before"]
    if (layer.get("id") != LAYER_ID or layer.get("name") != RETROSPECTIVE_LAYER_TITLE
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("maxRecordCount") != 2000
            or layer.get("fields") != EXPECTED_FIELDS):
        raise ValueError("source_layer_identity_or_schema_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent", {})
    if {key: extent.get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT:
        raise ValueError("source_extent_drift")
    if "spatialReference" in extent:
        _validate_spatial_reference(extent["spatialReference"], "source_layer_extent")
    if not isinstance(layer.get("editingInfo", {}).get("lastEditDate"), int):
        raise ValueError("source_editing_info_missing")
    ids = docs["ids_before"].get("objectIds")
    if (not isinstance(ids, list) or any(type(i) is not int for i in ids)
            or sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_roster_drift")
    if docs["ids_after"].get("objectIds") != ids:
        raise ValueError("source_id_population_changed_during_capture")
    if (docs["count_before"].get("count") != EXPECTED_COUNT
            or docs["count_after"].get("count") != EXPECTED_COUNT):
        raise ValueError("source_count_drift")
    seen: set[int] = set()
    identifiers = {field: set() for field in ID_FIELDS}
    nulls = {field["name"]: 0 for field in ATTRIBUTE_FIELDS}
    features = []
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if page.get("error") or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), name + "_response")
        rows = page.get("features")
        index = int(name[-3:])
        expected = EXPECTED_IDS[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("source_page_population_incomplete:" + name)
        got = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or set(attrs) != {f["name"] for f in ATTRIBUTE_FIELDS}):
                raise ValueError("source_attribute_roster_or_order_drift")
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected or oid in seen:
                raise ValueError("source_objectid_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    nulls[field["name"]] += 1
                _validate_value(field, value, field["name"])
                if field["name"] in ID_FIELDS and value is not None:
                    identifiers[field["name"]].add(value)
            if not isinstance(geometry, dict) or set(geometry) - {"x", "y", "spatialReference"}:
                raise ValueError("source_point_geometry_drift")
            if "spatialReference" in geometry:
                _validate_spatial_reference(geometry.get("spatialReference"), "feature_geometry")
            x, y = geometry.get("x"), geometry.get("y")
            if (type(x) not in (int, float) or type(y) not in (int, float)
                    or not math.isfinite(x) or not math.isfinite(y)):
                raise ValueError("source_xy_drift")
            if not (EXPECTED_EXTENT["xmin"] - EXTENT_TOLERANCE_METERS <= x
                    <= EXPECTED_EXTENT["xmax"] + EXTENT_TOLERANCE_METERS
                    and EXPECTED_EXTENT["ymin"] - EXTENT_TOLERANCE_METERS <= y
                    <= EXPECTED_EXTENT["ymax"] + EXTENT_TOLERANCE_METERS):
                raise ValueError("source_coordinate_outside_declared_extent")
            bounds = [min(bounds[0], x), min(bounds[1], y), max(bounds[2], x), max(bounds[3], y)]
            seen.add(oid)
            got.append(oid)
            features.append(feature)
        if sorted(got) != expected:
            raise ValueError("source_page_ids_drift:" + name)
    if seen != set(ids) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_population_incomplete")
    for field, values in identifiers.items():
        if len(values) != EXPECTED_COUNT:
            raise ValueError("source_identifier_not_unique:" + field)
    if any(abs(a - b) > EXTENT_TOLERANCE_METERS for a, b in zip(
            bounds, (EXPECTED_EXTENT["xmin"], EXPECTED_EXTENT["ymin"],
                     EXPECTED_EXTENT["xmax"], EXPECTED_EXTENT["ymax"]))):
        raise ValueError("source_geometry_coverage_drift")
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    last_edit_ms = layer["editingInfo"]["lastEditDate"]
    audit = {
        "source_feature_count": len(features),
        "source_id_sha256": _sha(_canonical(sorted(seen))),
        "source_id_min": min(seen), "source_id_max": max(seen),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([f["attributes"] for f in features])),
        "source_geometry_sha256": _sha(_canonical(
            [[f["attributes"]["OBJECTID"], f["geometry"]] for f in features])),
        "source_extent": extent, "source_bounds": bounds,
        "source_geometry_type": "esriGeometryPoint", "source_wkid": 3857,
        "source_null_counts": nulls,
        "source_identifier_counts": {field: len(values) for field, values in identifiers.items()},
        "source_last_edit_epoch_ms": last_edit_ms,
        "source_last_edit_utc": datetime.fromtimestamp(last_edit_ms / 1000, tz=timezone.utc)
            .isoformat().replace("+00:00", "Z"),
        "source_coverage_scope": "Complete observed NOAA NCEI retrospective BPR deployments layer: "
            "524 recovered DART bottom-pressure-recorder deployments worldwide with per-deployment "
            "data/metadata links; not restricted to the currently operational DART array.",
        "producer_service_item_id": SERVICE_ITEM_ID,
        "producer_service_item_owner": "ncei_noaa",
        "producer_webmap_item_id": WEBMAP_ITEM_ID,
        "release_date_note": "The layer's editingInfo lastEditDate and portal timestamps are "
            "operational metadata observed during capture, not a declared data release date.",
        "drift_guard": "Producer landing page, NOAA Natural Hazards Viewer entry, webmap item "
            "retrospective-dart-deployments delegation to the ncei_noaa service item, exact "
            "service layer roster, full 20-field declarations, native EPSG:3857 extent, count "
            "and before/after ID roster, per-page attribute order/types/lengths/nulls, unique "
            "OBJECTID/ID/STATION_ID/DATASET_ID populations, and per-point extent containment "
            "are checked.",
        "source_population_complete": True,
    }
    return layer["fields"], features, audit


_PARQUET_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int32(),
    "esriFieldTypeSmallInteger": pa.int32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeDate": pa.int64(),
    "esriFieldTypeString": pa.string(),
}


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet_bytes(fields, features) -> bytes:
    arrays = []
    names = []
    for field in fields:
        name = field["name"]
        values = [row["attributes"][name] for row in features]
        arrays.append(pa.array(values, type=_PARQUET_TYPES[field["type"]]))
        names.append(name)
    # WKB Point retains the native Web Mercator coordinate values without any
    # reprojection, repair or attribute change.
    arrays.append(pa.array([_point_wkb(row["geometry"]["x"], row["geometry"]["y"])
                            for row in features], type=pa.binary()))
    names.append("geometry")
    schema = pa.schema(
        [pa.field(field["name"], _PARQUET_TYPES[field["type"]], nullable=bool(field["nullable"]))
         for field in fields]
        + [pa.field("geometry", pa.binary(), nullable=False)])
    table = pa.Table.from_arrays(arrays, schema=schema)
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": EXPECTED_CRS.to_json_dict()}}}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata({b"geo": _canonical(geo)}), sink,
                   compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity(captures, root):
    return {name: {"body_sha256": _sha(_load_capture(root, name)[0]),
                   "wire_sha256": _sha(_load_capture(root, name)[1])} for name in sorted(captures)}


def capture_retrospective_bpr_deployments(storage: StagingStorageResource, *, version: str,
                                          capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_isolated_local_staging_required")
    if (not isinstance(version, str) or not version or Path(version).name != version
            or version in {".", ".."} or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(p.is_symlink() for p in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("unsealed_capture_attempt_exists")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/retrospective-bpr-deployments/captures")
                / (version + "-" + uuid4().hex))
        if any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/ncei-retro-bpr-draft"}) as client:
                for name, url in sorted(_expected_capture_urls().items()):
                    if name.startswith("page_"):
                        continue
                    captures[name] = _capture(client, url, root, name)
                ids = json.loads(captures["ids_before"][0]).get("objectIds")
                if (not isinstance(ids, list) or any(type(i) is not int for i in ids)
                        or sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT
                        or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
                    raise ValueError("source_seed_id_roster_drift")
                for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
                    page_ids = ids[start:start + PAGE_SIZE]
                    url = _query({"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                                  "returnGeometry": "true", "orderByFields": "OBJECTID ASC",
                                  "f": "json"})
                    captures[f"page_{index:03d}"] = _capture(client, url, root, f"page_{index:03d}")
            fields, features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            names = set()
            for path in sorted(root.glob("*.receipt.json")):
                name = path.name.removesuffix(".receipt.json")
                names.add(name)
                try:
                    receipts[name] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({
                "complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
                "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
            for name in names:
                try:
                    body = (root / f"{name}.body").read_bytes()
                    receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
                    if (root / f"{name}.wire").is_file():
                        captures[name] = (body, receipt)
                except (OSError, ValueError):
                    pass
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures,
                                              type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({
            "complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    fields, features, audit = _validate(captures)
    repo = Path(__file__).parents[3]
    identity = processing_identity(repo)
    capture_identity = _capture_identity(captures, root)
    capture_hash = _sha(_canonical(capture_identity))
    parquet = _parquet_bytes(fields, features)
    version = content_derived_version(audit["source_record_sha256"],
                                      audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    fingerprint = _sha(_canonical({"identity": identity, "record": audit["source_record_sha256"],
                                   "geometry": audit["source_geometry_sha256"],
                                   "schema": audit["source_schema_sha256"],
                                   "capture": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    existing = None
    if storage.object_snapshot(state_key) is not None:
        existing = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        previous = existing.get("capture_receipts") if isinstance(existing, dict) else None
        if not isinstance(previous, dict) or set(previous) != set(captures):
            raise ValueError("cached_capture_receipt_roster_changed")
        for name, (body, _r) in captures.items():
            current, wire, receipt = _load_capture(root, name)
            old = previous[name]
            if (any(receipt.get(k) != old.get(k) for k in
                    ("complete_body", "status", "bytes", "sha256", "wire_bytes", "wire_sha256"))
                    or current != body or _sha(wire) != old.get("wire_sha256")):
                raise ValueError("cached_source_capture_content_changed:" + name)
    receipts = existing["capture_receipts"] if existing else {
        name: receipt for name, (_body, receipt) in sorted(captures.items())}
    audit.update({"source_capture_evidence_sha256": capture_hash,
                  "source_capture_evidence": capture_identity,
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "baseline_accepted": False, "coverage_accepted": False,
                  "rights_accepted": False, "mapping_approved": False, "release_accepted": False})
    inputs = [{"input_id": name,
               "producer": PUBLISHER if name not in ("webmap", "service_item") else
                   "NOAA NCEI (ncei_noaa) via the ArcGIS Online hosting platform",
               "host": urlsplit(receipts[name]["url"]).hostname,
               "role": "producer_publication" if name in ("ncei_landing", "ncei_map")
                   else "official_distributor",
               "canonical_url": receipts[name]["url"], "retrieval_url": receipts[name]["url"],
               "observed_final_url": receipts[name]["final_url"],
               "retrieved_at": receipts[name]["observed_at"],
               "http_status": receipts[name]["status"], "bytes": receipts[name]["bytes"],
               "sha256": receipts[name]["sha256"], "wire_bytes": receipts[name]["wire_bytes"],
               "wire_sha256": receipts[name]["wire_sha256"], "release_date": None,
               "release_date_unknown_reason": "No dated data release is declared; layer "
                   "editingInfo and portal timestamps are operational metadata, not release dates.",
               "license": "Rights review pending.", "rights_review_reference": NOAA_DISCLAIMER_URL,
               "attribution": "NOAA NCEI / PMEL / NDBC; attribution and rights review pending.",
               **({"distributor": "Esri ArcGIS Online hosting the NOAA GeoPlatform organization "
                   "(service item owner ncei_noaa)", "delegation_evidence": {
                       "producer_landing_url": NCEI_LANDING_URL, "producer_map_url": NCEI_MAP_URL,
                       "webmap_url": WEBMAP_URL, "webmap_item_id": WEBMAP_ITEM_ID,
                       "webmap_layer_id": RETROSPECTIVE_LAYER_ID,
                       "service_item_id": SERVICE_ITEM_ID, "service_item_owner": "ncei_noaa",
                       "service_item_org": "NOAA GeoPlatform (C8EMgrsFcRFL6LrL)",
                       "layer_id": LAYER_ID, "layer_name": RETROSPECTIVE_LAYER_TITLE}}
                   if name in ("webmap", "service_item") else {})}
              for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Retrospective BPR Deployments",
        "description": "Complete observed NOAA NCEI retrospective BPR deployments layer (524 "
            "recovered DART bottom-pressure-recorder deployments); candidate only; historical "
            "archive equivalence, schema/ID contract, rights, baseline and release remain "
            "unapproved.",
        "publisher": PUBLISHER, "agency": "National Oceanic and Atmospheric Administration",
        "source_url": LAYER_URL, "license": None, "source_modified": None, "date_issued": None,
        "date_modified": None, "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Native ESRI Point to typed GeoParquet",
            "parameters": {"fields": "all 20 declared attributes and values preserved including nulls",
                "ids": "OBJECTID, ID, STATION_ID and DATASET_ID preserved unchanged",
                "crs": "EPSG:3857 (native Web Mercator)", "xy": "native X/Y retained with no "
                    "reprojection or geometry repair",
                "dates": "declared Esri date fields retained as their source epoch-millisecond values",
                "raw_evidence": "complete decoded response and wire bodies retained with "
                    "checksummed receipts",
                "candidate_status": "unapproved; archive equivalence, source edition, contract, "
                    "rights, baseline and release decisions pending"}}],
        "lineage": "NOAA's published Natural Hazards Viewer (www.ncei.noaa.gov/maps/hazards) "
            "embeds webmap item " + WEBMAP_ITEM_ID + ", whose retrospective-dart-deployments "
            "layer delegates to the NOAA GeoPlatform hosted Feature Service item "
            + SERVICE_ITEM_ID + " (owner ncei_noaa) layer 1 Retrospective BPR Deployments. The "
            "producer program page is the NCEI DART Ocean Bottom Pressure Data page. The HIFLD "
            "inventory attributes the dataset to USACE, but the inventory description, tags and "
            "the only maintained publication identify NOAA NCEI/PMEL/NDBC DART data; the "
            "DataLumos rescue copy is a third-party mirror and is not a pipeline input. No "
            "historical HIFLD archive equivalence is claimed.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native point coordinates and all attributes retained without reprojection.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, _r) in captures.items():
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipts[name]) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical({
        "approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "release_accepted": False}) + b"\n"
    if existing:
        verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                existing["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "source_capture_evidence_sha256": capture_hash, "reused": True,
                "source_last_edit_utc": audit["source_last_edit_utc"],
                "source_null_counts": audit["source_null_counts"],
                "source_coverage_scope": audit["source_coverage_scope"],
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    objects = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or snapshot.size != len(body)
                or storage.sha256_key(key) != _sha(body)):
            raise ValueError("candidate_readback_failed:" + name)
        objects[name] = {"bytes": len(body), "sha256": _sha(body)}
    seal_ready_candidate(storage, state_key, version, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": receipts, "objects": objects})
    validate_construction_code_identity(repo, identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_capture_evidence_sha256": capture_hash,
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "capture_directory": str(root), "reused": False,
            "source_last_edit_utc": audit["source_last_edit_utc"],
            "source_null_counts": audit["source_null_counts"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
