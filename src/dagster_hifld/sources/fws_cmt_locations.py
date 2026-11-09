"""Complete native capture for the official USFWS Corporate Master Table locations layer.

Fidelity contract (issue #365): the FWS_HQ_BA_CMT_Locations FeatureServer layer 0
publishes BOTH a native ESRI point geometry (Web Mercator) and CMT
latitude/longitude attribute columns. The two representations disagree on a
measured subset of rows and no first-party documentation establishes precedence,
so this adapter preserves BOTH losslessly and never reconciles, corrects, or
chooses an authoritative published geometry. Snapshot-verified baseline counts
(feature count, null-coordinate count, geometry-vs-attribute disagreement
count) are pinned fail-closed drift gates: any change raises instead of being
silently absorbed.
"""
from __future__ import annotations

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
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    PAGE_MAX_BYTES, _capture, _canonical, _capture_identity, _load_capture, _sha,
)

DATASET_SLUG = "usfws-corporate-master-table"
FILE_SLUG = DATASET_SLUG
ORG_ID = "QVENGdaPbd4LUkLV"
ORG_URL = f"https://www.arcgis.com/sharing/rest/portals/{ORG_ID}?f=json"
ITEM_ID = "0ae3bdbf09894d2d9a59ff9ba4f5859f"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
OWNER_ACCOUNT = "FWSGIS"
OWNER_URL = f"https://www.arcgis.com/sharing/rest/community/users/{OWNER_ACCOUNT}?f=json"
SERVICE_URL = ("https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/"
               "FWS_HQ_BA_CMT_Locations/FeatureServer")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FWS_HQ_BA_CMT_Locations_0"
ITEM_TITLE = "FWS HQ BA CMT Locations"
PUBLISHER = ("US Fish and Wildlife Service, Information Resources and Technology Management, "
             "Branch of Geospatial Data Services (FWSGIS); official FWS ArcGIS Online organization")
AUTHORITY_URL = "https://www.fws.gov/policy-library/260fw14"

# Snapshot-verified counts (2026-11-08 bounded capture, independently recomputed
# 2026-11-09 from the hash-verified preserved pages): 1,816 rows; 4 rows null on
# BOTH geometry and latitude/longitude; 140 of the 1,812 comparable rows disagree
# by more than 1e-6 degrees between native geometry and attribute coordinates.
# These are DRAFT drift-guard pins: any change raises fail-closed for maintainer
# re-inspection. Baseline acceptance itself, and any published-geometry contract,
# remain open maintainer decisions (all approval flags stay false).
EXPECTED_COUNT = 1816
EXPECTED_NULL_COORDINATE_COUNT = 4
EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT = 140
DISAGREEMENT_THRESHOLD_DEG = 1e-6
EXPECTED_ORG_CODE_NULL_COUNT = 0
EXPECTED_ORG_CODE_DUPLICATE_COUNT = 0
PAGE_SIZE = 1000
WEB_MERCATOR_WORLD_BOUND = 20037508.342789244
SPHERE_RADIUS_M = 6378137.0

# (name, esri type, alias) in the layer's declared order; verified stable and
# byte-identical across dated metadata probes (2026-11-08, 2026-11-09).
_FIELD_TABLE = (
    ("physStateAbbr", "esriFieldTypeString", "physStateAbbr"),
    ("orgName", "esriFieldTypeString", "orgName"),
    ("physAdd1", "esriFieldTypeString", "physAdd1"),
    ("orgTypeAcro", "esriFieldTypeString", "orgTypeAcro"),
    ("physCity", "esriFieldTypeString", "physCity"),
    ("mailCity", "esriFieldTypeString", "mailCity"),
    ("program", "esriFieldTypeString", "program"),
    ("mailStateAbbr", "esriFieldTypeString", "mailStateAbbr"),
    ("telephone", "esriFieldTypeString", "telephone"),
    ("region", "esriFieldTypeInteger", "region"),
    ("mailAdd1", "esriFieldTypeString", "mailAdd1"),
    ("orgType", "esriFieldTypeString", "orgType"),
    ("physAdd2", "esriFieldTypeString", "physAdd2"),
    ("mailAdd2", "esriFieldTypeString", "mailAdd2"),
    ("ObjectId", "esriFieldTypeOID", "ObjectId"),
    ("physZip", "esriFieldTypeString", "physZip"),
    ("mailZip", "esriFieldTypeString", "mailZip"),
    ("orgCode", "esriFieldTypeString", "orgCode"),
    ("longitude", "esriFieldTypeDouble", "longitude"),
    ("latitude", "esriFieldTypeDouble", "latitude"),
    ("CCCode", "esriFieldTypeString", "Cost Center Code"),
    ("RptToCC", "esriFieldTypeString", "Report To CC"),
    ("RptOrgCode", "esriFieldTypeString", "Report To Org Code"),
    ("OrgNameAbbr", "esriFieldTypeString", "Organization Name Abbreviation"),
    ("ProgAcro", "esriFieldTypeString", "Program Acronym"),
    ("RptToOrgName", "esriFieldTypeString", "Report To Organization Name"),
    ("LIT", "esriFieldTypeString", "LIT"),
)
EXPECTED_FIELD_DECLARATIONS = [
    {"name": name, "type": kind, "alias": alias,
     "length": 256 if kind == "esriFieldTypeString" else None, "domain": None}
    for name, kind, alias in _FIELD_TABLE
]
ATTRIBUTE_FIELDS = EXPECTED_FIELD_DECLARATIONS
ATTRIBUTE_FIELD_NAMES = [field["name"] for field in ATTRIBUTE_FIELDS]
EXPECTED_SERVICE_LAYERS = [{"id": 0, "name": LAYER_NAME, "geometryType": "esriGeometryPoint"}]
PAGE_COUNT = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
PAGE_NAMES = tuple(f"page_{index:03d}" for index in range(PAGE_COUNT))
CAPTURE_NAMES = {"org", "item", "owner", "service_before", "layer_before", "count_before",
                 "ids_before", *PAGE_NAMES, "layer_after", "count_after", "ids_after",
                 "service_after"}
UNACCEPTED_FLAGS = {"source_registry_accepted": False, "source_release_accepted": False,
                    "source_freshness_accepted": False, "schema_contract_accepted": False,
                    "id_contract_accepted": False, "coverage_accepted": False,
                    "rights_accepted": False, "mapping_approved": False,
                    "release_accepted": False, "baseline_accepted": False}

WEB_MERCATOR_PROJJSON = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "ProjectedCRS", "name": "WGS 84 / Pseudo-Mercator",
    "base_crs": {"type": "GeographicCRS", "name": "WGS 84",
        "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
            "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137,
                          "inverse_flattening": 298.257223563},
            "id": {"authority": "EPSG", "code": 6326}},
        "coordinate_system": {"subtype": "ellipsoidal",
            "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north",
                      "unit": "degree"},
                     {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east",
                      "unit": "degree"}], "id": {"authority": "EPSG", "code": 6422}},
        "id": {"authority": "EPSG", "code": 4326}},
    "conversion": {"name": "Popular Visualisation Pseudo-Mercator",
        "method": {"name": "Popular Visualisation Pseudo-Mercator",
                   "id": {"authority": "EPSG", "code": 1024}},
        "parameters": [
            {"name": "Latitude of natural origin", "value": 0, "unit": "degree",
             "id": {"authority": "EPSG", "code": 8801}},
            {"name": "Longitude of natural origin", "value": 0, "unit": "degree",
             "id": {"authority": "EPSG", "code": 8802}},
            {"name": "False easting", "value": 0, "unit": "metre",
             "id": {"authority": "EPSG", "code": 8806}},
            {"name": "False northing", "value": 0, "unit": "metre",
             "id": {"authority": "EPSG", "code": 8807}}]},
    "coordinate_system": {"subtype": "Cartesian",
        "axis": [{"name": "Easting", "abbreviation": "X", "direction": "east", "unit": "metre"},
                 {"name": "Northing", "abbreviation": "Y", "direction": "north",
                  "unit": "metre"}]},
    "id": {"authority": "EPSG", "code": 3857},
}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_query(offset: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "orderByFields": "ObjectId ASC", "resultOffset": str(offset),
                   "resultRecordCount": str(PAGE_SIZE), "f": "json"})


def _capture_roster() -> list[tuple[str, str]]:
    urls = [("org", ORG_URL), ("item", ITEM_URL), ("owner", OWNER_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index in range(PAGE_COUNT):
        urls.append((f"page_{index:03d}", _page_query(index * PAGE_SIZE)))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _validate_capture_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_roster())
    captures = {}
    for name in sorted(CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _inverse_x(x: float) -> float:
    """Documented EPSG:3857 inverse (spherical Mercator, R=6378137)."""
    return x * 180.0 / (math.pi * SPHERE_RADIUS_M)


def _inverse_y(y: float) -> float:
    return (2.0 * math.atan(math.exp(y / SPHERE_RADIUS_M)) - math.pi / 2.0) * 180.0 / math.pi


def _validate_web_mercator_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_drift")
    found = False
    for key, expected in (("wkid", 102100), ("latestWkid", 3857)):
        if key in value:
            if type(value[key]) is not int or value[key] != expected:
                raise ValueError(label + "_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(label + "_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(label + "_crs_drift") from exc
            if not parsed.equals(CRS.from_epsg(3857), ignore_axis_order=True):
                raise ValueError(label + "_crs_drift")
            found = True
    if not found:
        raise ValueError(label + "_crs_drift")


def _field_projection(fields: object) -> list[dict]:
    if not isinstance(fields, list):
        raise ValueError("source_field_schema_drift")
    return [{"name": field.get("name"), "type": field.get("type"), "alias": field.get("alias"),
             "length": field.get("length"), "domain": field.get("domain")} for field in fields]


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    org = docs["org"]
    if (org.get("id") != ORG_ID or org.get("name") != "U.S. Fish & Wildlife Service"
            or org.get("portalName") != "ArcGIS Online" or org.get("urlKey") != "fws"):
        raise ValueError("source_org_identity_drift")
    item = docs["item"]
    if (item.get("id") != ITEM_ID or item.get("owner") != OWNER_ACCOUNT
            or item.get("type") != "Feature Service" or item.get("access") != "public"
            or item.get("title") != ITEM_TITLE or item.get("url") != SERVICE_URL
            or "Branch of Geospatial Data Services" not in str(item.get("accessInformation", ""))
            or "FWSGIS@fws.gov" not in str(item.get("accessInformation", ""))):
        raise ValueError("source_item_identity_drift")
    owner = docs["owner"]
    if (owner.get("username") != OWNER_ACCOUNT
            or owner.get("fullName") != "US Fish and Wildlife Service"
            or "Information Resources and Technology Management" not in str(owner.get("description", ""))):
        raise ValueError("source_publisher_account_identity_drift")
    service = docs["service_before"]
    advertised = [{key: layer.get(key) for key in ("id", "name", "geometryType")}
                  for layer in service.get("layers", [])]
    if (service.get("serviceItemId") != ITEM_ID or advertised != EXPECTED_SERVICE_LAYERS
            or service.get("tables") != []):
        raise ValueError("source_service_identity_drift")
    _validate_web_mercator_reference(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    if _field_projection(layer.get("fields")) != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("maxRecordCount") != 2000
            or layer.get("objectIdFieldName") != "ObjectId"):
        raise ValueError("source_layer_identity_drift")
    _validate_web_mercator_reference(layer.get("spatialReference"), "source_layer")
    _validate_web_mercator_reference(layer.get("extent", {}).get("spatialReference"), "source_layer_extent")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (count_before != EXPECTED_COUNT or count_after != count_before
            or not isinstance(ids_before, list) or any(type(oid) is not int for oid in ids_before)
            or len(ids_before) != EXPECTED_COUNT or len(set(ids_before)) != EXPECTED_COUNT
            or ids_after != ids_before):
        raise ValueError("source_count_or_id_roster_drift")
    sorted_ids = sorted(ids_before)
    features = []
    org_codes = []
    null_coordinate_count = 0
    disagreement_count = 0
    for name in PAGE_NAMES:
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True and name == PAGE_NAMES[-1]:
            raise ValueError("source_page_error_or_transfer_limit:" + name)
        _validate_web_mercator_reference(page.get("spatialReference"), "source_" + name)
        if page.get("geometryType") != "esriGeometryPoint" or page.get("objectIdFieldName") != "ObjectId":
            raise ValueError("source_page_identity_drift:" + name)
        rows = page.get("features")
        expected_ids = sorted_ids[int(name[-3:]) * PAGE_SIZE:(int(name[-3:]) + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        for feature, expected_oid in zip(rows, expected_ids):
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if not isinstance(attrs, dict) or set(attrs) != set(ATTRIBUTE_FIELD_NAMES):
                raise ValueError("source_attribute_roster_drift")
            oid = attrs.get("ObjectId")
            if type(oid) is not int or oid <= 0 or oid != expected_oid:
                raise ValueError("source_feature_id_roster_drift")
            for field in ATTRIBUTE_FIELDS:
                value, kind = attrs[field["name"]], field["type"]
                if value is None:
                    continue
                if kind == "esriFieldTypeOID" and type(value) is not int:
                    raise ValueError("source_oid_value_type_drift")
                if kind == "esriFieldTypeInteger" and type(value) is not int:
                    raise ValueError("source_integer_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeDouble" and type(value) not in (int, float):
                    raise ValueError("source_double_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
            org_code = attrs["orgCode"]
            if not isinstance(org_code, str) or not org_code:
                raise ValueError("source_org_code_null_or_empty_drift")
            org_codes.append(org_code)
            latitude, longitude = attrs["latitude"], attrs["longitude"]
            if (latitude is None) != (longitude is None):
                raise ValueError("source_latitude_longitude_null_pairing_drift")
            if (geometry is None) != (latitude is None):
                raise ValueError("source_geometry_attribute_null_pairing_drift")
            if geometry is not None:
                if (not isinstance(geometry, dict)
                        or set(geometry) - {"x", "y", "spatialReference"} 
                        or type(geometry.get("x")) not in (int, float)
                        or type(geometry.get("y")) not in (int, float)
                        or not math.isfinite(geometry["x"]) or not math.isfinite(geometry["y"])
                        or abs(geometry["x"]) > WEB_MERCATOR_WORLD_BOUND
                        or abs(geometry["y"]) > WEB_MERCATOR_WORLD_BOUND):
                    raise ValueError("source_point_geometry_structure_drift")
                if geometry.get("spatialReference") is not None:
                    _validate_web_mercator_reference(geometry["spatialReference"],
                                                     "source_" + name + "_feature")
                if latitude is not None:
                    if abs(latitude) > 90.0 or abs(longitude) > 180.0:
                        raise ValueError("source_attribute_coordinate_range_drift")
                    delta = max(abs(_inverse_x(geometry["x"]) - longitude),
                                abs(_inverse_y(geometry["y"]) - latitude))
                    if delta > DISAGREEMENT_THRESHOLD_DEG:
                        disagreement_count += 1
            else:
                null_coordinate_count += 1
            features.append({"attributes": {field_name: attrs[field_name]
                                           for field_name in ATTRIBUTE_FIELD_NAMES},
                            "geometry": geometry})
    if len(features) != EXPECTED_COUNT:
        raise ValueError("source_feature_count_drift")
    if null_coordinate_count != EXPECTED_NULL_COORDINATE_COUNT:
        raise ValueError("source_null_coordinate_count_drift")
    if disagreement_count != EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT:
        raise ValueError("source_geometry_attribute_disagreement_count_drift")
    if len(org_codes) - len(set(org_codes)) != EXPECTED_ORG_CODE_DUPLICATE_COUNT:
        raise ValueError("source_org_code_duplicate_drift")
    features.sort(key=lambda row: row["attributes"]["ObjectId"])
    ids = [row["attributes"]["ObjectId"] for row in features]
    geometry_coordinates = [[row["attributes"]["ObjectId"],
                             row["geometry"]["x"] if row["geometry"] else None,
                             row["geometry"]["y"] if row["geometry"] else None]
                            for row in features]
    populated = [row for row in features if row["geometry"] is not None]
    bounds = [min(row["geometry"]["x"] for row in populated),
              min(row["geometry"]["y"] for row in populated),
              max(row["geometry"]["x"] for row in populated),
              max(row["geometry"]["y"] for row in populated)]
    latitudes = [row["attributes"]["latitude"] for row in populated]
    longitudes = [row["attributes"]["longitude"] for row in populated]
    audit = {"source_feature_count": len(features),
             "source_id_sha256": _sha(_canonical(ids)),
             "source_schema_sha256": _sha(_canonical(EXPECTED_FIELD_DECLARATIONS)),
             "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
             "source_geometry_sha256": _sha(_canonical(geometry_coordinates)),
             "source_geometry_type": "esriGeometryPoint", "source_wkid": 3857,
             "source_vertex_count": len(populated),
             "geometry_valid_count": len(populated), "geometry_empty_count": null_coordinate_count,
             "geometry_invalid_count": 0, "geometry_bounds": bounds,
             "source_attribute_bounds": [min(longitudes), min(latitudes),
                                         max(longitudes), max(latitudes)],
             "source_null_coordinate_count": null_coordinate_count,
             "source_geometry_attribute_disagreement_count": disagreement_count,
             "source_disagreement_threshold_deg": DISAGREEMENT_THRESHOLD_DEG,
             "source_org_code_null_count": EXPECTED_ORG_CODE_NULL_COUNT,
             "source_org_code_duplicate_count": EXPECTED_ORG_CODE_DUPLICATE_COUNT,
             "source_org_code_leading_zero_count": sum(code.startswith("0") for code in org_codes),
             "observed_data_last_edit_date": (docs["layer_before"].get("editingInfo") or {}).get("dataLastEditDate"),
             "source_coverage_scope": ("Complete current FWS_HQ_BA_CMT_Locations layer 0 point "
                                       "roster as returned by the service; the source's own "
                                       "description warns unstaffed offices may be absent and "
                                       "one location can represent many offices."),
             "geometry_basis": ("Both native Web Mercator point geometry and CMT latitude/longitude "
                                "attributes preserved verbatim; no reconciliation and no "
                                "authoritative-geometry choice; published-geometry precedence "
                                "remains an open maintainer decision."),
             "drift_guard": ("FWS AGOL org/item/publisher-account identity, exact 27-field schema, "
                             "layer/service identity, before/after count and ObjectId roster, "
                             "per-page native XY in EPSG:3857, orgCode string identifiers with "
                             "null/duplicate and leading-zero preservation, null-coordinate "
                             "pairing, and draft drift-guard pins (features 1816, nulls 4, "
                             "geometry-vs-attribute disagreements 140) that raise on change; "
                             "baseline acceptance is pending, not a published-geometry contract.")}
    return features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string()}[field["type"]]


def _esri_point_wkb(geometry: dict) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(geometry["x"]), float(geometry["y"]))


def _parquet_bytes(features: list[dict]) -> bytes:
    arrays = [pa.array([row["attributes"][field["name"]] for row in features],
                       type=_arrow_type(field)) for field in ATTRIBUTE_FIELDS]
    arrays.append(pa.array([_esri_point_wkb(row["geometry"]) if row["geometry"] else None
                            for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=ATTRIBUTE_FIELD_NAMES + ["geometry"])
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                 "crs": WEB_MERCATOR_PROJJSON}}})}
    import io
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd",
                   use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_fws_cmt_locations(storage: StagingStorageResource, *, version: str,
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
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/365/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/fws-cmt-draft"}) as client:
                for name, url in _capture_roster():
                    captures[name] = _capture(client, url, root, name)
            features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
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

    features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        **UNACCEPTED_FLAGS})
    parquet = _parquet_bytes(features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name,
        "producer": ("US Fish and Wildlife Service ArcGIS Online organization" if name in {"org", "item", "owner"}
                     else PUBLISHER),
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": {"org": ORG_URL, "item": ITEM_URL, "owner": OWNER_URL}.get(name, LAYER_URL),
        "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The FWS FeatureServer is a live nightly-updating service "
                                         "(observed dataLastEditDate at capture); it publishes no "
                                         "dated releases."),
        "license": "Unresolved; rights review pending.",
        "attribution": "USFWS FWSGIS / IRTM Branch of Geospatial Data Services; rights review pending.",
        **({"producer_account": OWNER_ACCOUNT, "authority_evidence": {
            "authority_url": AUTHORITY_URL,
            "authority_basis": "FWS policy 260 FW 14 defines the CMT as the official source of "
                               "Service organization codes; the FWSGIS-owned item and FWS ArcGIS "
                               "Hub catalog this service under the first-party FWS organization.",
            "item_url": ITEM_URL, "service_item_id": ITEM_ID,
            "registry_status": "The trusted source registry has no USFWS agency entry; source "
                               "acceptance and registration remain pending."}}
           if name == "item" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False,
        "title": "US Fish and Wildlife Service Corporate Master Table (CMT)",
        "description": ("Complete current official FWS_HQ_BA_CMT_Locations layer 0 roster; faithful "
                        "unapproved candidate, not accepted for release. Contains USFWS "
                        "organization names, mailing/physical addresses and telephone contact "
                        "attributes exactly as published by the source."),
        "publisher": PUBLISHER, "agency": "US Fish and Wildlife Service", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "source_registry_accepted": False, "inputs": inputs,
        "transformations": [{"operation": "Esri native Web Mercator point XY and CMT latitude/longitude attributes to GeoParquet WKB candidate",
            "parameters": {
                "source_fields": "all 27 declared CMT attributes retained in declared order and declared types, including organization names, addresses and telephone strings",
                "source_ids": "ObjectId and string orgCode retained unchanged, including leading zeros; orgCode null/duplicate baseline pinned at 0/0",
                "source_crs": "EPSG:3857 (native Web Mercator; no reprojection, repair, or quantization)",
                "geometry": "native XY point geometry preserved as WKB in EPSG:3857; null geometry preserved as null",
                "attributes": "native latitude/longitude degree attributes preserved verbatim as separate columns, including 4 null rows; both coordinate representations kept without reconciliation and without choosing an authoritative published geometry",
                "baselines": "pinned fail-closed drift gates: feature count 1816, null-coordinate rows 4, geometry-vs-attribute disagreements (>1e-6 deg via documented EPSG:3857 inverse) 140",
                "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
                "candidate_status": "unapproved; source registry, source, schema/ID, coverage, rights and release decisions pending"}},
        ],
        "lineage": f"FWSGIS item {ITEM_ID} (FWS ArcGIS Online org {ORG_ID}) is the first-party maintained CMT locations service; FWS policy 260 FW 14 ({AUTHORITY_URL}) defines the CMT as the official source of Service organization codes. The trusted source registry has no USFWS agency entry, so source acceptance and registration remain pending. This candidate contains complete service layer {LAYER_ID} only.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All 27 ArcGIS-declared CMT attributes, source IDs, native EPSG:3857 point geometry and native latitude/longitude attributes are retained in a typed candidate with both coordinate representations preserved.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "shapefile_zip": "Source field names exceed the 10-character format limit; no faithful name mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": EXPECTED_FIELD_DECLARATIONS,
                  "official_layer_name": LAYER_NAME, "official_layer_id": LAYER_ID,
                  "official_source_item": "FWSGIS " + ITEM_ID,
                  "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_null_coordinate_count": audit["source_null_coordinate_count"],
        "source_geometry_attribute_disagreement_count": audit["source_geometry_attribute_disagreement_count"],
        **UNACCEPTED_FLAGS}
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
        return _result(audit, version, prefix, root, capture_hash, reused=True)
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
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return _result(audit, version, prefix, root, capture_hash, reused=False)


def _result(audit: dict, version: str, prefix: str, root: Path, capture_hash: str,
            *, reused: bool) -> dict[str, object]:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": reused,
        "source_capture_evidence_sha256": capture_hash,
        "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"],
        "geometry_empty_count": audit["geometry_empty_count"],
        "null_coordinate_count": audit["source_null_coordinate_count"],
        "geometry_attribute_disagreement_count": audit["source_geometry_attribute_disagreement_count"],
        "org_code_leading_zero_count": audit["source_org_code_leading_zero_count"],
        "org_code_duplicate_count": audit["source_org_code_duplicate_count"],
        "observed_data_last_edit_date": audit.get("observed_data_last_edit_date"),
        **UNACCEPTED_FLAGS, "capture_directory": str(root)}
