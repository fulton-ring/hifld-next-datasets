"""Complete native capture of the official Census TIGERweb secondary roads layer.

The DHS HIFLD Open crosswalk (row 303, Agency Census, DOI 10.3886/E241367V1)
points the retired HIFLD Open item 0cc7fa5e342a4455aeeed2b24d98ba69 at the
producer's maintained TIGERweb Transportation (Roads and Railroads) MapServer
layer 3, whose name is exactly "Secondary Roads Interstates and US Highways".
Its observed population is the secondary-class (MTFCC S1200) US Highway
(RTTYP U) and Interstate (RTTYP I) segments. The inventory row's Download pin
(the Census TIGER/Line All Roads national geodatabase) holds the general roads
product, not this layer's 31212-feature population, so it is recorded as
lineage context and never fetched as a pipeline input.
"""
from __future__ import annotations

import json
import math
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
from pyproj import CRS
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    PAGE_MAX_BYTES, _canonical, _capture_identity, _esri_polyline_wkb,
    _load_capture, _sha, _validate_capture_request_identity,
)

DATASET_SLUG = "secondary-roads-interstates-and-us-highways-2"
FILE_SLUG = DATASET_SLUG
SERVICE_FOLDER_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb?f=pjson"
SERVICE_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Transportation/MapServer"
LAYER_ID = 3
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Census Bureau, TIGERweb Transportation (Roads and Railroads) service"
EXPECTED_COUNT = 31212
EXPECTED_ID_MIN = 1
EXPECTED_ID_MAX = 248105
# OBJECTIDs are not contiguous; the roster digest pins the exact population.
EXPECTED_ID_SHA256 = "0d68e9aedf6c55cb27beecf88f9dfabcf0f8ac05e19ec6cb0e024d22716eb73d"
PAGE_SIZE = 250
MAX_CAPTURE_BYTES = 900_000_000
EXPECTED_FIELD_DECLARATIONS = [
    {"alias": "BASENAME", "domain": None, "length": 100, "name": "BASENAME", "type": "esriFieldTypeString"},
    {"alias": "MTFCC", "domain": None, "length": 5, "name": "MTFCC", "type": "esriFieldTypeString"},
    {"alias": "NAME", "domain": None, "length": 100, "name": "NAME", "type": "esriFieldTypeString"},
    {"alias": "OBJECTID", "domain": None, "name": "OBJECTID", "type": "esriFieldTypeOID"},
    {"alias": "OID", "domain": None, "length": 22, "name": "OID", "type": "esriFieldTypeString"},
    {"alias": "PREDIR", "domain": None, "name": "PREDIR", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PREDIRABRV", "domain": None, "length": 15, "name": "PREDIRABRV", "type": "esriFieldTypeString"},
    {"alias": "PREQUAL", "domain": None, "name": "PREQUAL", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PREQUALABRV", "domain": None, "length": 15, "name": "PREQUALABRV", "type": "esriFieldTypeString"},
    {"alias": "PRETYP", "domain": None, "name": "PRETYP", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PRETYPEABRV", "domain": None, "length": 14, "name": "PRETYPEABRV", "type": "esriFieldTypeString"},
    {"alias": "RTTYP", "domain": None, "length": 1, "name": "RTTYP", "type": "esriFieldTypeString"},
    {"alias": "STGEOMETRY", "domain": None, "name": "STGEOMETRY", "type": "esriFieldTypeGeometry"},
    {"alias": "SUFDIR", "domain": None, "name": "SUFDIR", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFDIRABRV", "domain": None, "length": 15, "name": "SUFDIRABRV", "type": "esriFieldTypeString"},
    {"alias": "SUFQUAL", "domain": None, "name": "SUFQUAL", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFQUALABRV", "domain": None, "length": 15, "name": "SUFQUALABRV", "type": "esriFieldTypeString"},
    {"alias": "SUFTYP", "domain": None, "name": "SUFTYP", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFTYPEABRV", "domain": None, "length": 14, "name": "SUFTYPEABRV", "type": "esriFieldTypeString"},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["type"] != "esriFieldTypeGeometry"]
EXPECTED_EXTENT = {"xmin": -19017198.084, "ymin": -1616302.3542999998,
                   "xmax": 16231857.854099996, "ymax": 11136279.419299997}
# The service advertises wkid 102100 / latestWkid 3857 (Web Mercator meters).
WEB_MERCATOR_WKIDS = {102100, 3857}
WEB_MERCATOR_LIMIT = 20037508.342789244
# The pinned layer extent is the observed data bbox; every XY vertex must stay
# inside the Web Mercator domain and the pinned extent within float noise.
EXTENT_TOLERANCE_METERS = 1e-6
EXPECTED_SERVICE_MAP_NAME = "Transportation (Roads and Railroads)"
EXPECTED_SERVICE_MAX_RECORD_COUNT = 100000
EXPECTED_SERVICE_LAYERS = [
    (0, "Primary Roads Interstates 5M scale"), (1, "Primary Roads 2_1M scale"),
    (2, "Primary Roads"), (3, "Secondary Roads Interstates and US Highways"),
    (4, "Secondary Roads 578k scale"), (5, "Secondary Roads 289_144k scale"),
    (6, "Secondary Roads 72_1k scale"), (7, "Local Roads 72k scale"),
    (8, "Local Roads"), (9, "Railroads"),
]
EXPECTED_LAYER_NAME = "Secondary Roads Interstates and US Highways"
# The publisher stamps the layer description with the vintage of the observed
# layer population (verified live 2026-11-14); the trailing date is recorded as
# the publisher-stated source vintage, not a per-feature last-edit timestamp
# and not the HTTP retrieval time. Freshness/release acceptance stays pending.
EXPECTED_LAYER_DESCRIPTION = "Secondary roads - Interstates and US Highways; January 1, 2026"
SOURCE_VINTAGE_NOTE = ("Publisher-stated vintage of the observed TIGERweb layer population, parsed from the "
    "captured layer description 'Secondary roads - Interstates and US Highways; January 1, 2026'; it dates the "
    "published layer population and is not a per-feature last-edit timestamp or the HTTP retrieval time. "
    "Freshness acceptance remains pending.")
EXPECTED_SCALE_RANGE = {"minScale": 2500000, "maxScale": 650001}
# Population contract observed on the live layer (2026-11-14): 31212 features,
# every one MTFCC S1200 with route type U (30607) or I (605), no nulls.
EXPECTED_MTFCC_VALUES = {"S1200"}
EXPECTED_RTTYPE_VALUES = {"U", "I"}
_BASE_CAPTURE_NAMES = ("folder", "service_before", "layer_before", "count_before", "ids_before",
                       "layer_after", "count_after", "ids_after", "service_after")
_TRANSIENT_RETRY_DELAYS = (5.0, 15.0, 45.0, 90.0)


def _capture_names() -> set[str]:
    return set(_BASE_CAPTURE_NAMES) | {
        f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _validate_roster(ids: object) -> list[int]:
    if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
            or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
            or ids != sorted(ids)
            or min(ids) != EXPECTED_ID_MIN or max(ids) != EXPECTED_ID_MAX
            or _sha(_canonical(ids)) != EXPECTED_ID_SHA256):
        raise ValueError("source_count_or_id_population_drift")
    return ids


def _validate_web_mercator_reference(value: object, label: str) -> None:
    """Validate every advertised ArcGIS Web Mercator representation."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid not in WEB_MERCATOR_WKIDS:
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
            if not parsed.equals(CRS.from_epsg(3857), ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
    if not found:
        raise ValueError(f"{label}_crs_drift")


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire") for path in root.glob(suffix)
                   if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    for attempt, extra_delay in enumerate((0.0, *_TRANSIENT_RETRY_DELAYS)):
        if attempt:
            for suffix in (".body", ".wire", ".receipt.json"):
                path = root / f"{name}{suffix}"
                if path.exists():
                    path.unlink()
            if extra_delay:
                time.sleep(extra_delay)
        try:
            return capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
        except ValueError as exc:
            if (attempt == len(_TRANSIENT_RETRY_DELAYS)
                    or not str(exc).startswith(f"incomplete_source_capture:{name}")):
                raise
    raise ValueError(f"incomplete_source_capture:{name}")


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("folder", SERVICE_FOLDER_URL),
            ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds": ",".join(map(str, page_ids)),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != _capture_names():
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    captures = {}
    for name in sorted(_capture_names()):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        captures[name] = (body, receipt)
    ids = _validate_roster(json.loads(captures["ids_before"][0]).get("objectIds"))
    expected_urls = dict(_capture_roster(ids))
    for name in sorted(captures):
        _validate_capture_request_identity(name, captures[name][1], expected_urls[name])
    return captures


_MONTH_NAMES = {name: number for number, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"), start=1)}


def _publisher_stated_vintage(description: object) -> str:
    """Parse the publisher-stated vintage from the captured layer description.

    The TIGERweb Transportation layer description ends with a 'Month D, YYYY'
    stamp for the vintage of the published layer population. The stamp is
    preserved as the publisher-stated source vintage; no last-edit timestamp
    is invented for it.
    """
    match = (re.fullmatch(r"(?s)(.*);\s+([A-Z][a-z]+) (\d{1,2}), (\d{4})", description)
             if isinstance(description, str) else None)
    if not match or match.group(2) not in _MONTH_NAMES:
        raise ValueError("source_layer_description_or_vintage_drift")
    return f"{int(match.group(4)):04d}-{_MONTH_NAMES[match.group(2)]:02d}-{int(match.group(3)):02d}"


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    if set(captures) != _capture_names():
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    folder = docs["folder"]
    if not any(service.get("name") == "TIGERweb/Transportation" and service.get("type") == "MapServer"
               for service in folder.get("services", [])):
        raise ValueError("official_tigerweb_service_directory_drift")
    service = docs["service_before"]
    advertised = [(layer.get("id"), layer.get("name")) for layer in service.get("layers", [])]
    if (service.get("mapName") != EXPECTED_SERVICE_MAP_NAME
            or service.get("maxRecordCount") != EXPECTED_SERVICE_MAX_RECORD_COUNT
            or advertised != EXPECTED_SERVICE_LAYERS):
        raise ValueError("official_tigerweb_transportation_service_drift")
    _validate_web_mercator_reference(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != EXPECTED_LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("description") != EXPECTED_LAYER_DESCRIPTION
            or {key: layer.get(key) for key in EXPECTED_SCALE_RANGE} != EXPECTED_SCALE_RANGE
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_scale_drift")
    vintage = _publisher_stated_vintage(layer.get("description"))
    _validate_web_mercator_reference(layer.get("spatialReference"), "source_layer")
    _validate_web_mercator_reference(layer.get("extent", {}).get("spatialReference"), "source_layer_extent")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids = _validate_roster(docs["ids_before"].get("objectIds"))
    if (count_before != EXPECTED_COUNT or count_after != count_before
            or _validate_roster(docs["ids_after"].get("objectIds")) != ids):
        raise ValueError("source_count_or_id_population_drift")
    features = []
    vertices = 0
    nulls = {field["name"]: 0 for field in ATTRIBUTE_FIELDS}
    geometry_valid = geometry_empty = 0
    mtfcc_counts: dict[str, int] = {}
    rttype_counts: dict[str, int] = {}
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        _validate_web_mercator_reference(page.get("spatialReference"), "source_" + name + "_page")
        rows = page.get("features")
        expected_index = int(name[-3:])
        expected_ids = ids[expected_index * PAGE_SIZE:(expected_index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or set(attrs) != {field["name"] for field in ATTRIBUTE_FIELDS}):
                raise ValueError("source_attribute_roster_or_order_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    nulls[field["name"]] += 1
                    continue
                kind = field["type"]
                if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                    raise ValueError("source_oid_value_type_drift")
                if kind == "esriFieldTypeSmallInteger" and type(value) is not int:
                    raise ValueError("source_integer_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeString":
                    if not isinstance(value, str):
                        raise ValueError("source_string_value_type_drift:" + field["name"])
                    if len(value) > int(field["length"]):
                        raise ValueError("source_string_length_drift:" + field["name"])
            if attrs["MTFCC"] is None or attrs["MTFCC"] not in EXPECTED_MTFCC_VALUES:
                raise ValueError("source_mtfcc_population_contract_drift")
            if attrs["RTTYP"] is None or attrs["RTTYP"] not in EXPECTED_RTTYPE_VALUES:
                raise ValueError("source_rttype_population_contract_drift")
            mtfcc_counts[attrs["MTFCC"]] = mtfcc_counts.get(attrs["MTFCC"], 0) + 1
            rttype_counts[attrs["RTTYP"]] = rttype_counts.get(attrs["RTTYP"], 0) + 1
            if not isinstance(geometry, dict) or set(geometry) - {"paths", "hasZ", "hasM", "spatialReference"}:
                raise ValueError("source_polyline_geometry_structure_drift")
            if geometry.get("spatialReference") is not None:
                _validate_web_mercator_reference(geometry.get("spatialReference"),
                                                 "source_" + name + "_feature")
            if geometry.get("hasZ", False) is not False or geometry.get("hasM", False) is not False:
                raise ValueError("source_polyline_zm_not_supported")
            paths = geometry.get("paths")
            if not isinstance(paths, list) or not paths:
                raise ValueError("source_polyline_paths_missing")
            for path in paths:
                if not isinstance(path, list) or len(path) < 2:
                    raise ValueError("source_polyline_path_invalid")
                for point in path:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                        raise ValueError("source_xy_ordinate_contract_drift")
                    if not (-WEB_MERCATOR_LIMIT <= point[0] <= WEB_MERCATOR_LIMIT
                            and -WEB_MERCATOR_LIMIT <= point[1] <= WEB_MERCATOR_LIMIT):
                        raise ValueError("source_xy_web_mercator_domain_violation")
                    if not (EXPECTED_EXTENT["xmin"] - EXTENT_TOLERANCE_METERS <= point[0]
                            <= EXPECTED_EXTENT["xmax"] + EXTENT_TOLERANCE_METERS
                            and EXPECTED_EXTENT["ymin"] - EXTENT_TOLERANCE_METERS <= point[1]
                            <= EXPECTED_EXTENT["ymax"] + EXTENT_TOLERANCE_METERS):
                        raise ValueError("source_vertex_outside_declared_extent")
                    vertices += 1
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            page_ids.append(oid)
            wkb = _esri_polyline_wkb(geometry)
            geom = from_wkb(wkb)
            geometry_empty += int(geom.is_empty)
            geometry_valid += int(geom.is_valid and not geom.is_empty)
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    all_ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if (sorted(all_ids) != ids or len(features) != EXPECTED_COUNT or len(set(all_ids)) != EXPECTED_COUNT
            or any(count != EXPECTED_COUNT for count in mtfcc_counts.values())
            or set(mtfcc_counts) != EXPECTED_MTFCC_VALUES
            or sum(rttype_counts.values()) != EXPECTED_COUNT
            or set(rttype_counts) != EXPECTED_RTTYPE_VALUES):
        raise ValueError("source_feature_id_or_population_contract_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    bounds = [from_wkb(_esri_polyline_wkb(row["geometry"])).bounds for row in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features])),
        "source_geometry_type": "esriGeometryPolyline", "source_wkid": 102100, "source_latest_wkid": 3857,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "source_null_counts": nulls,
        "source_mtfcc_counts": mtfcc_counts, "source_rttype_counts": rttype_counts,
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"], "source_scale_range": EXPECTED_SCALE_RANGE,
        "source_vintage": vintage, "source_vintage_evidence": layer.get("description"),
        "source_vintage_note": SOURCE_VINTAGE_NOTE,
        "source_coverage_scope": "Complete observed TIGERweb Transportation MapServer layer 3 "
            "Secondary Roads Interstates and US Highways (MTFCC S1200 segments with route type U or I) "
            "from the directly queried U.S. Census Bureau service; a continuously maintained national "
            "web-service subset at the 1:650,001-1:2,500,000 display scale, not the TIGER/Line All Roads "
            "geodatabase product.",
        "drift_guard": "Exact TIGERweb folder/service/layer roster, full field declarations/types, "
            "Web Mercator layer and response-page SRs, scale range, count and before/after ID roster "
            "digest, per-page IDs/attributes/transfer limits, XY path dimensions, Web Mercator domain "
            "and pinned-extent containment of every vertex, and the observed MTFCC/RTTYP population contract.",
    }
    return layer["fields"], features, audit


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


# EPSG:3857 (WGS 84 / Pseudo-Mercator) PROJJSON, pinned literally so the
# candidate parquet bytes are reproducible across pyproj releases. The
# datum_ensemble carries its full member list; pyproj CRS parsing rejects a
# members-less ensemble.
WEB_MERCATOR_PROJJSON = {
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "area": "World between 85.06°S and 85.06°N.",
        "base_crs": {
            "coordinate_system": {
                "axis": [
                    {
                        "abbreviation": "Lat",
                        "direction": "north",
                        "name": "Geodetic latitude",
                        "unit": "degree"
                    },
                    {
                        "abbreviation": "Lon",
                        "direction": "east",
                        "name": "Geodetic longitude",
                        "unit": "degree"
                    }
                ],
                "subtype": "ellipsoidal"
            },
            "datum_ensemble": {
                "accuracy": "2.0",
                "ellipsoid": {
                    "inverse_flattening": 298.257223563,
                    "name": "WGS 84",
                    "semi_major_axis": 6378137
                },
                "id": {
                    "authority": "EPSG",
                    "code": 6326
                },
                "members": [
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1166
                        },
                        "name": "World Geodetic System 1984 (Transit)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1152
                        },
                        "name": "World Geodetic System 1984 (G730)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1153
                        },
                        "name": "World Geodetic System 1984 (G873)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1154
                        },
                        "name": "World Geodetic System 1984 (G1150)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1155
                        },
                        "name": "World Geodetic System 1984 (G1674)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1156
                        },
                        "name": "World Geodetic System 1984 (G1762)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1309
                        },
                        "name": "World Geodetic System 1984 (G2139)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1383
                        },
                        "name": "World Geodetic System 1984 (G2296)"
                    }
                ],
                "name": "World Geodetic System 1984 ensemble"
            },
            "id": {
                "authority": "EPSG",
                "code": 4326
            },
            "name": "WGS 84"
        },
        "bbox": {
            "east_longitude": 180,
            "north_latitude": 85.06,
            "south_latitude": -85.06,
            "west_longitude": -180
        },
        "conversion": {
            "method": {
                "id": {
                    "authority": "EPSG",
                    "code": 1024
                },
                "name": "Popular Visualisation Pseudo Mercator"
            },
            "name": "Popular Visualisation Pseudo-Mercator",
            "parameters": [
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8801
                    },
                    "name": "Latitude of natural origin",
                    "unit": "degree",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8802
                    },
                    "name": "Longitude of natural origin",
                    "unit": "degree",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8806
                    },
                    "name": "False easting",
                    "unit": "metre",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8807
                    },
                    "name": "False northing",
                    "unit": "metre",
                    "value": 0
                }
            ]
        },
        "coordinate_system": {
            "axis": [
                {
                    "abbreviation": "X",
                    "direction": "east",
                    "name": "Easting",
                    "unit": "metre"
                },
                {
                    "abbreviation": "Y",
                    "direction": "north",
                    "name": "Northing",
                    "unit": "metre"
                }
            ],
            "subtype": "Cartesian"
        },
        "id": {
            "authority": "EPSG",
            "code": 3857
        },
        "name": "WGS 84 / Pseudo-Mercator",
        "scope": "Web mapping and visualisation.",
        "type": "ProjectedCRS"
    }


def _arrow_type(field: dict):
    import pyarrow as pa
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeString": pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict], source_crs: str) -> bytes:
    """Typed candidate parquet preserving native attributes, IDs and 2D paths."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq
    properties = [field for field in fields if field["type"] != "esriFieldTypeGeometry"]
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in properties]
    arrays.append(pa.array([_esri_polyline_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in properties] + ["geometry"])
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiLineString"],
                                 "crs": WEB_MERCATOR_PROJJSON}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def capture_secondary_roads_census(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 900_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/306/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/census-tigerweb-draft"}) as client:
                ids_body, ids_receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before")
                ids = _validate_roster(json.loads(ids_body).get("objectIds"))
                captures["ids_before"] = (ids_body, ids_receipt)
                roster = _capture_roster(ids)
                for name, url in roster:
                    if name == "ids_before":
                        continue
                    captures[name] = _capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__,
                "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(), "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features, "EPSG:3857")
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    # capture_get fails closed unless final_url == url (no redirects are
    # followed) and every receipt retains final_url, so the manifest records
    # observed_final_url only when it actually differs (bounded manifest size).
    canonical_by_prefix = (("page_", LAYER_URL), ("layer_", LAYER_URL), ("count_", LAYER_URL), ("ids_", LAYER_URL))
    inputs = []
    vintage = audit["source_vintage"]
    for name, receipt in sorted(receipts.items()):
        canonical = SERVICE_FOLDER_URL if name == "folder" else (
            SERVICE_URL if name.startswith("service_") else
            next((url for fragment, url in canonical_by_prefix if name.startswith(fragment)), LAYER_URL))
        inputs.append({"input_id": name, "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": canonical, "retrieval_url": receipt["url"],
            **({} if receipt["final_url"] == receipt["url"] else {"observed_final_url": receipt["final_url"]}),
            "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": vintage, "release_date_meaning": SOURCE_VINTAGE_NOTE,
            "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
            "attribution": "U.S. Census Bureau, TIGERweb Transportation (Roads and Railroads), layer 3 Secondary Roads Interstates and US Highways."})
    manifest = {"schema_version": 1, "approved": False, "title": "Secondary Roads Interstates and US Highways",
        "description": "Complete observed Census TIGERweb Transportation MapServer layer 3 "
            "Secondary Roads Interstates and US Highways: MTFCC S1200 secondary-class road segments "
            "with US Highway (RTTYP U) or Interstate (RTTYP I) route types, at the layer's 1:650,001-1:2,500,000 "
            "display scale. Historical HIFLD Open equivalence, contract, baseline, rights and release remain unaccepted.",
        "publisher": PUBLISHER, "agency": "U.S. Census Bureau", "source_url": LAYER_URL,
        "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
        "source_modified": None, "date_issued": vintage, "date_modified": None,
        "source_vintage_note": SOURCE_VINTAGE_NOTE,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polyline (Web Mercator) to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID and OID retained unchanged", "source_crs": "EPSG:3857 (service wkid 102100)",
            "geometry": "2D path order and coordinates preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": "The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, row 'Secondary Roads Interstates and US "
            "Highways', Status Deprecate, Agency Census) points the retired HIFLD Open geoplatform item "
            "0cc7fa5e342a4455aeeed2b24d98ba69 at this producer service layer. The HIFLD Open inventory row's "
            "Download pin (www2.census.gov TIGER/Line TGRGDB24 All Roads geodatabase, successor TGRGDB25) holds the "
            "general roads product whose secondary population (247956 S1200 segments in TGRGDB25) is not this layer's "
            "31212-feature population, so the geodatabase is recorded as lineage context and never fetched. The "
            "DataLumos record 240643 is a historical archive copy consulted as research context only.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D paths are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": EXPECTED_LAYER_NAME, "official_layer_id": LAYER_ID,
                  "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            body, wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": EXPECTED_ID_MIN,
            "source_id_max": EXPECTED_ID_MAX,
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": EXPECTED_ID_MIN,
        "source_id_max": EXPECTED_ID_MAX,
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
