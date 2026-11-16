"""Complete native capture for the AO of the U.S. Courts district boundary layer."""
from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_retained_capture_replay
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
    validate_capture_request_identity,
)
from dagster_hifld.sources.levee_areas_usace_ienc import _esri_polygon_wkb
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture_identity, _load_capture, _sha,
    _validate_spatial_reference,
)

DATASET_SLUG = "us-district-court-jurisdictions"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "Administrative Office of the U.S. Courts (AOUSC)"
# Succession: the DHS HIFLD Open crosswalk (August 2025) row "US District Court
# Jurisdictions" marks the retired HIFLD/GeoPlatform item ed086b05d8dc46eab57
# 4e0fecdcf0f1f as Deprecate with no successor ("No longer supported as part of
# the HIFLD mission") and points the agency only to a static 1999 Federal
# Judicial Center map PDF. Probes on 2026-11-14 found both the old REST service
# and the old item dead. The federal judiciary itself maintains the operative
# district boundary product in the ArcGIS Online organization of the
# Administrative Office of the U.S. Courts ("Judicial Branch", urlKey AOUSC):
# the public hosted feature service below, data edited March 2026.
CATALOG_ITEM_ID = "dca3851a95ba4cc2a1fb19ab5dff1f5d"
CATALOG_ITEM_OWNER = "Courts_GIS_AOUSC"
CATALOG_ORG_ID = "LJKafwRzLHEkKeOL"
CATALOG_ORG_NAME = "Judicial Branch (AOUSC)"
CATALOG_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{CATALOG_ITEM_ID}?f=json"
LANDING_URL = f"https://aousc.maps.arcgis.com/home/item.html?id={CATALOG_ITEM_ID}"
SERVICE_URL = ("https://services9.arcgis.com/LJKafwRzLHEkKeOL/arcgis/rest/services/"
               "USDistricts_Project/FeatureServer")
LAYER_URL = f"{SERVICE_URL}/5"
LAYER_ID = 5
LAYER_NAME = "TMB U.S. Courts Districts Final"
ITEM_TITLE = "TMB U.S. Courts Districts Final"
ITEM_ACCESS_INFORMATION = ""
ITEM_DESCRIPTION = ""
ITEM_LICENSE = ""
ITEM_TAGS = ["USDistricts_Project", "2025"]
ITEM_SNIPPET = ("TMB U.S. Courts Districts Final. Do not delete. This layer is used within the "
                "TMB VAR U.S. Courts Asset Information Final group layer. ")
ITEM_CATEGORIES = ["/Categories/FSO/TMB/GIS"]
ITEM_EXTENT = [[-178.22526585499998, 13.235007063000069],
               [145.8180773050001, 71.38310212600004]]
SERVICE_DESCRIPTION = "USDistricts_Project"
SERVICE_CAPABILITIES = "Query,Extract"
LAYER_COPYRIGHT = ""
LAYER_DISPLAY_FIELD = "NAME"
EXPECTED_COUNT = 95
EXPECTED_IDS = list(range(1, 96))
EXPECTED_ID_SHA256 = "04fd325cb6bf5cbb4e33c02b3f5297be13f8acc9a4ab0508290b482ba51bc14f"
# The 94 federal judicial districts carry distinct names plus one unassigned
# placeholder row; the rosters below are pinned exactly as observed on
# 2026-11-14, including the producer's own 'Lousiana' spelling in DistName.
EXPECTED_DISTRICT_NAME_SHA256 = "7885e39b6cdbb79dc3855aa423f09595b294f032a4a0b81532fbf118d47a979c"
EXPECTED_ABBREVIATION_SHA256 = "2479cb3b285c82b4547d0978fa30b70137ebe719406c82cea38c3edd1359d965"
EXPECTED_DISTRICT_SHORT_NAME_SHA256 = "0440d2bac3749e65800c9010fab76d15ae06f56fc91dce6e7adb5c912e4d1b58"
EXPECTED_STATE_SHA256 = "ee748039f2994f2041127fc63433e1eee180ab45ca8adb91f77f24ded1f8e8e1"
EXPECTED_CIRCUIT_SHA256 = "be999d46bf5474b6c379f76c9d29f68460427789613e19f87d56c2579fb603a4"
EXPECTED_CIRCUIT_ROSTER = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "DC", "XX"]
# The roster closes with an intentionally unassigned placeholder row: OBJECTID
# 95 carries NAME/ABV/DistName 'XXX', Circuit 'XX' and no geometry at all in
# the source response, plus null State/sqmi/shape measures. The placeholder is
# preserved as a null-geometry record and never replaced with substitute
# geometry or values.
EXPECTED_NULL_GEOMETRY_IDS = [95]
EXPECTED_NULL_ATTRIBUTE_IDS = {"NAME": [], "ABV": [], "DistName": [], "State": [95],
                               "Circuit": [], "Shape_Leng": [94, 95], "sqmi": [95],
                               "Shape__Area": [95], "Shape__Length": [95]}
# Three producer-published polygons are invalid under OGC (ring
# self-intersection in District of Oregon, Western District of Washington and
# Eastern District of Virginia). The capture is faithful: invalid source
# geometry is retained unchanged, never repaired or dropped, and its roster is
# pinned so any change is drift.
EXPECTED_GEOMETRY_INVALID_IDS = [24, 86, 92]
EXPECTED_RECORD_SHA256 = "7ecef55b16f611c7c6bbda6732b8bba6c24b9e79d38f45f976b4482863c2b76b"
EXPECTED_GEOMETRY_SHA256 = "d71a2a2e91ec5d41fc87128c6b833a3a8aecd278ccc37ad02a1adde861fd2386"
PAGE_SIZE = 2000
PAGE_COUNT = (len(EXPECTED_IDS) + PAGE_SIZE - 1) // PAGE_SIZE
EXPECTED_EXTENT = {"xmin": -178.22526585499998, "ymin": 13.235007063000069,
                   "xmax": 145.8180773050001, "ymax": 71.38310212600004}
EXPECTED_EXTENT_TOLERANCE = 1e-9
EXPECTED_EDITING_INFO = {"lastEditDate": 1776352570444,
                         "schemaLastEditDate": 1776352570444,
                         "dataLastEditDate": 1774371266262}
EXPECTED_LAST_DATA_EDIT_UTC = "2026-03-24T16:54:26.262000Z"
EXPECTED_LAST_EDIT_UTC = "2026-04-16T15:16:10.444000Z"
EXPECTED_GEOMETRY_PROPERTIES = {"shapeAreaFieldName": "Shape__Area",
                                "shapeLengthFieldName": "Shape__Length",
                                "units": "esriDecimalDegrees"}
SERVICE_FULL_EXTENT = {"xmin": -178.22526585499998, "ymin": 13.235007063000069,
                       "xmax": 145.8180773050001, "ymax": 71.38310212600004}
LAYER_MAX_RECORD_COUNT = 2000
SERVICE_MAX_RECORD_COUNT = 2000
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID",
     "sqlType": "sqlTypeOther", "nullable": False, "editable": False,
     "domain": None, "defaultValue": None},
    {"name": "NAME", "type": "esriFieldTypeString", "alias": "NAME",
     "sqlType": "sqlTypeOther", "length": 40, "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "ABV", "type": "esriFieldTypeString", "alias": "ABV",
     "sqlType": "sqlTypeOther", "length": 3, "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "DistName", "type": "esriFieldTypeString", "alias": "DistName",
     "sqlType": "sqlTypeOther", "length": 36, "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "State", "type": "esriFieldTypeString", "alias": "State",
     "sqlType": "sqlTypeOther", "length": 50, "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "Circuit", "type": "esriFieldTypeString", "alias": "Circuit",
     "sqlType": "sqlTypeOther", "length": 4, "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "Shape_Leng", "type": "esriFieldTypeDouble", "alias": "Shape_Leng",
     "sqlType": "sqlTypeOther", "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "sqmi", "type": "esriFieldTypeInteger", "alias": "Square Miles",
     "sqlType": "sqlTypeOther", "nullable": True, "editable": True,
     "domain": None, "defaultValue": None},
    {"name": "Shape__Area", "type": "esriFieldTypeDouble", "alias": "Shape__Area",
     "sqlType": "sqlTypeDouble", "nullable": True, "editable": False,
     "domain": None, "defaultValue": None},
    {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length",
     "sqlType": "sqlTypeDouble", "nullable": True, "editable": False,
     "domain": None, "defaultValue": None},
]
ATTRIBUTE_FIELDS = EXPECTED_FIELD_DECLARATIONS
PAGE_FIELD_KEYS = tuple(field["name"] for field in ATTRIBUTE_FIELDS)
CAPTURE_NAMES = {"catalog_item", "service_before", "layer_before", "count_before",
                 "ids_before", *{f"page_{index:03d}" for index in range(PAGE_COUNT)},
                 "layer_after", "count_after", "ids_after", "service_after"}
SHAPE_PARITY_TOLERANCE = 1e-9


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


PAGE_MAX_BYTES = 24_000_000
CAPTURE_MAX_BYTES = 96_000_000


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
    return body, receipt


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("catalog_item", CATALOG_ITEM_URL),
            ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds": ",".join(map(str, page_ids)),
            "outFields": "*", "returnGeometry": "true", "returnZ": "false", "returnM": "false",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    return validate_retained_capture_replay(
        root,
        capture_names=CAPTURE_NAMES,
        expected_urls=dict(_capture_roster(EXPECTED_IDS)),
        load_capture=_load_capture,
        validate_request_identity=validate_capture_request_identity,
    )


def _validate_page_fields(page_fields: object, layer_fields: list[dict]) -> None:
    """Reject page-level physical schema drift before constructing a native table."""
    if not isinstance(page_fields, list) or len(page_fields) != len(layer_fields):
        raise ValueError("source_page_physical_schema_changed")
    for page_field, layer_field in zip(page_fields, layer_fields):
        if not isinstance(page_field, dict) or not isinstance(layer_field, dict):
            raise ValueError("source_page_physical_schema_changed")
        reduced = {key: value for key, value in layer_field.items()
                   if key not in ("nullable", "editable")}
        if page_field != reduced:
            raise ValueError("source_page_physical_schema_changed")


def _validate_catalog_item(item: dict) -> None:
    if (item.get("id") != CATALOG_ITEM_ID or item.get("owner") != CATALOG_ITEM_OWNER
            or item.get("orgId") != CATALOG_ORG_ID or item.get("type") != "Feature Service"
            or item.get("title") != ITEM_TITLE or item.get("access") != "public"
            or item.get("accessInformation") != ITEM_ACCESS_INFORMATION
            or item.get("spatialReference") != "4326"):
        raise ValueError("aousc_catalog_item_identity_drift")
    if (item.get("description") != ITEM_DESCRIPTION or item.get("licenseInfo") != ITEM_LICENSE
            or item.get("snippet") != ITEM_SNIPPET or item.get("tags") != ITEM_TAGS
            or item.get("categories") != ITEM_CATEGORIES):
        raise ValueError("aousc_catalog_item_text_drift")
    extent = item.get("extent")
    if (not isinstance(extent, list) or len(extent) != 2
            or extent[0] != ITEM_EXTENT[0] or extent[1] != ITEM_EXTENT[1]):
        raise ValueError("aousc_catalog_item_extent_drift")


def _validate_service(service: dict, label: str) -> None:
    if (service.get("serviceItemId") != CATALOG_ITEM_ID
            or service.get("serviceDescription") != SERVICE_DESCRIPTION
            or service.get("capabilities") != SERVICE_CAPABILITIES
            or service.get("maxRecordCount") != SERVICE_MAX_RECORD_COUNT
            or service.get("hasVersionedData") is not False
            or service.get("supportedQueryFormats") != "JSON"):
        raise ValueError(f"{label}_service_identity_drift")
    _validate_spatial_reference(service.get("spatialReference"), label + "_service")
    full = service.get("fullExtent")
    if (not isinstance(full, dict)
            or {key: full.get(key) for key in SERVICE_FULL_EXTENT} != SERVICE_FULL_EXTENT):
        raise ValueError(f"{label}_service_extent_drift")
    _validate_spatial_reference(full.get("spatialReference"), label + "_service_full_extent")
    layers = service.get("layers")
    if (not isinstance(layers, list) or len(layers) != 1
            or layers[0].get("id") != LAYER_ID or layers[0].get("name") != LAYER_NAME
            or layers[0].get("parentLayerId") != -1 or layers[0].get("subLayerIds") is not None
            or layers[0].get("type") != "Feature Layer"
            or layers[0].get("geometryType") != "esriGeometryPolygon"):
        raise ValueError(f"{label}_service_layer_roster_drift")


def _validate_layer(layer: dict) -> None:
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("maxRecordCount") != LAYER_MAX_RECORD_COUNT
            or layer.get("objectIdField") != "OBJECTID" or layer.get("globalIdField") != ""
            or layer.get("typeIdField") != "" or layer.get("displayField") != LAYER_DISPLAY_FIELD
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or layer.get("enableNullGeometry") is not False
            or layer.get("isDataVersioned") is not False
            or layer.get("copyrightText") != LAYER_COPYRIGHT
            or layer.get("serviceItemId") != CATALOG_ITEM_ID
            or layer.get("editingInfo") != EXPECTED_EDITING_INFO
            or layer.get("geometryProperties") != EXPECTED_GEOMETRY_PROPERTIES
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT
            or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True):
        raise ValueError("source_layer_identity_extent_or_freshness_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    _validate_catalog_item(docs["catalog_item"])
    service = docs["service_before"]
    _validate_service(service, "service_before")
    layer = docs["layer_before"]
    _validate_layer(layer)
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    for label, count in (("before", count_before), ("after", count_after)):
        if type(count) is not int or count != EXPECTED_COUNT:
            raise ValueError("source_count_population_drift:" + label)
    for label, ids in (("before", ids_before), ("after", ids_after)):
        if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
                or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
                or sorted(ids) != EXPECTED_IDS
                or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
            raise ValueError("source_id_population_drift:" + label)
    if sorted(ids_after) != sorted(ids_before):
        raise ValueError("source_id_roster_changed_during_capture")
    features = []
    vertices = 0
    geometry_valid = geometry_empty = 0
    null_geometry_ids = []
    invalid_geometry_ids = []
    coverage = {key: EXPECTED_EXTENT[key] for key in ("xmin", "ymin", "xmax", "ymax")}
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        if (page.get("objectIdFieldName") != "OBJECTID"
                or page.get("geometryType") != "esriGeometryPolygon"
                or page.get("globalIdFieldName") != ""
                or page.get("uniqueIdField") != {"name": "OBJECTID", "isSystemMaintained": True}):
            raise ValueError("source_page_identity_drift")
        if page.get("geometryProperties") != EXPECTED_GEOMETRY_PROPERTIES:
            raise ValueError("source_page_geometry_properties_drift")
        _validate_page_fields(page.get("fields"), layer["fields"])
        expected_index = int(name[-3:])
        expected_ids = EXPECTED_IDS[expected_index * PAGE_SIZE:(expected_index + 1) * PAGE_SIZE]
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or tuple(attrs) != PAGE_FIELD_KEYS):
                raise ValueError("source_attribute_roster_or_order_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    continue
                kind = field["type"]
                if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                    raise ValueError("source_oid_value_type_drift")
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeInteger" and (type(value) is not int or value <= 0):
                    raise ValueError("source_integer_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeDouble" and (type(value) not in (int, float)
                                                      or not math.isfinite(float(value))):
                    raise ValueError("source_double_value_type_drift:" + field["name"])
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            if geometry is None:
                null_geometry_ids.append(oid)
                page_ids.append(oid)
                features.append(feature)
                continue
            if not isinstance(geometry, dict) or set(geometry) - {"rings", "hasZ", "hasM", "spatialReference"}:
                raise ValueError("source_polygon_geometry_structure_drift")
            if "rings" not in geometry:
                raise ValueError("source_polygon_rings_missing")
            if geometry.get("hasZ") or geometry.get("hasM"):
                raise ValueError("source_geometry_dimensional_drift")
            if geometry.get("spatialReference") is not None:
                _validate_spatial_reference(geometry.get("spatialReference"),
                                            "source_" + name + "_feature")
            rings = geometry.get("rings")
            if not isinstance(rings, list) or not rings:
                raise ValueError("source_polygon_rings_missing")
            for ring in rings:
                if not isinstance(ring, list) or len(ring) < 4:
                    raise ValueError("source_polygon_ring_invalid")
                if ring[0] != ring[-1]:
                    raise ValueError("source_polygon_ring_not_closed")
                for point in ring:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                        raise ValueError("source_xy_ordinate_contract_drift")
                    if not (coverage["xmin"] - EXPECTED_EXTENT_TOLERANCE <= point[0] <= coverage["xmax"] + EXPECTED_EXTENT_TOLERANCE
                            and coverage["ymin"] - EXPECTED_EXTENT_TOLERANCE <= point[1] <= coverage["ymax"] + EXPECTED_EXTENT_TOLERANCE):
                        raise ValueError("source_geometry_outside_declared_coverage")
                    vertices += 1
            geom = from_wkb(_esri_polygon_wkb(geometry))
            geometry_valid += int(geom.is_valid and not geom.is_empty)
            geometry_empty += int(geom.is_empty)
            if not geom.is_valid and not geom.is_empty:
                invalid_geometry_ids.append(oid)
            for parity_field, observed in (("source_shape_area_parity_drift", attrs.get("Shape__Area")),
                                           ("source_shape_length_parity_drift", attrs.get("Shape__Length"))):
                if observed is None:
                    continue
                computed = geom.area if parity_field.startswith("source_shape_area") else geom.length
                if abs(computed - observed) > SHAPE_PARITY_TOLERANCE * max(abs(observed), 1e-12):
                    raise ValueError(parity_field)
            page_ids.append(oid)
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if sorted(ids) != EXPECTED_IDS or len(features) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_id_population_drift")
    names = [feature["attributes"]["NAME"] for feature in features]
    abbreviations = [feature["attributes"]["ABV"] for feature in features]
    short_names = [feature["attributes"]["DistName"] for feature in features]
    states = [feature["attributes"]["State"] for feature in features if feature["attributes"]["State"] is not None]
    circuits = [feature["attributes"]["Circuit"] for feature in features]
    if (len(set(names)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(names))) != EXPECTED_DISTRICT_NAME_SHA256):
        raise ValueError("source_district_name_roster_drift")
    if (len(set(abbreviations)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(abbreviations))) != EXPECTED_ABBREVIATION_SHA256):
        raise ValueError("source_abbreviation_roster_drift")
    if (len(set(short_names)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(short_names))) != EXPECTED_DISTRICT_SHORT_NAME_SHA256):
        raise ValueError("source_district_short_name_roster_drift")
    if _sha(_canonical(sorted(set(states)))) != EXPECTED_STATE_SHA256:
        raise ValueError("source_state_roster_drift")
    if (len(set(circuits)) != len(EXPECTED_CIRCUIT_ROSTER)
            or _sha(_canonical(sorted(set(circuits)))) != EXPECTED_CIRCUIT_SHA256
            or sorted(set(circuits)) != EXPECTED_CIRCUIT_ROSTER):
        raise ValueError("source_circuit_roster_drift")
    for field, expected_null_ids in EXPECTED_NULL_ATTRIBUTE_IDS.items():
        observed = sorted(feature["attributes"]["OBJECTID"] for feature in features
                          if feature["attributes"][field] is None)
        if observed != expected_null_ids:
            raise ValueError(f"source_null_attribute_roster_drift:{field}")
    if sorted(null_geometry_ids) != EXPECTED_NULL_GEOMETRY_IDS:
        raise ValueError("source_null_geometry_roster_drift")
    if sorted(invalid_geometry_ids) != EXPECTED_GEOMETRY_INVALID_IDS:
        raise ValueError("source_geometry_invalid_roster_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(
        [[row["attributes"]["OBJECTID"], row["attributes"]] for row in features]))
    geometry_sha = _sha(_canonical(
        [[row["attributes"]["OBJECTID"], row.get("geometry")] for row in features]))
    bounds = [from_wkb(_esri_polygon_wkb(row["geometry"])).bounds for row in features
              if row["attributes"]["OBJECTID"] not in EXPECTED_NULL_GEOMETRY_IDS]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_id_note": ("The layer holds 95 features with OBJECTID 1..95; the roster is "
                           "pinned and preserved; no substitute IDs are generated."),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_null_count": len(null_geometry_ids),
        "geometry_null_ids": sorted(null_geometry_ids),
        "geometry_null_note": ("The final roster row (OBJECTID 95, NAME/ABV/DistName 'XXX', "
                               "Circuit 'XX') publishes no geometry at all in the source "
                               "response; it is retained as a null-geometry record and never "
                               "given substitute geometry."),
        "geometry_invalid_count": len(invalid_geometry_ids),
        "geometry_invalid_ids": sorted(invalid_geometry_ids),
        "geometry_invalid_note": ("Three producer-published polygons (District of Oregon "
                                  "OBJECTID 24, Western District of Washington OBJECTID 86, "
                                  "Eastern District of Virginia OBJECTID 92) are invalid under "
                                  "OGC (ring self-intersection); they are retained exactly as "
                                  "published and never repaired."),
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"],
        "source_last_data_edit_utc": EXPECTED_LAST_DATA_EDIT_UTC,
        "source_last_edit_utc": EXPECTED_LAST_EDIT_UTC,
        "source_coverage_scope": ("Complete observed 'TMB U.S. Courts Districts Final' layer 5 "
                                  "from the Administrative Office of the U.S. Courts (Judicial "
                                  "Branch, urlKey AOUSC) ArcGIS Online organization's hosted "
                                  "service. The layer spans the 94 federal judicial districts "
                                  "(89 within the states plus the District of Columbia, Puerto "
                                  "Rico, the U.S. Virgin Islands, Guam and the Northern Mariana "
                                  "Islands) plus one unassigned null-geometry 'XXX' placeholder "
                                  "row; it is not asserted to cover any other judiciary level."),
        "catalog_item_id": CATALOG_ITEM_ID, "catalog_item_owner": CATALOG_ITEM_OWNER,
        "catalog_item_org": CATALOG_ORG_ID + " " + CATALOG_ORG_NAME,
        "catalog_item_url": CATALOG_ITEM_URL, "landing_page_url": LANDING_URL,
        "layer_copyright": LAYER_COPYRIGHT,
        "dhs_crosswalk_row": {"layer_name": "US District Court Jurisdictions", "status": "Deprecate",
                              "agency": "Federal Judicial Center",
                              "old_hifld_item_id": "ed086b05d8dc46eab574e0fecdcf0f1f",
                              "old_hosting_location": "https://geoplatform.maps.arcgis.com/home/item.html?id=ed086b05d8dc46eab574e0fecdcf0f1f",
                              "crosswalk_new_hosting_location": "N/A",
                              "crosswalk_deprecation_justification": "No longer supported as part of the HIFLD mission",
                              "crosswalk_external_landing_page": "https://www.fjc.gov/sites/default/files/2012/IJR00007.pdf",
                              "crosswalk_open_rest_service": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/US_District_Court_Jurisdictions/FeatureServer",
                              "successor_probes_2026_11_14": ("The crosswalk's Open REST service "
                                  "answers HTTP 400 'Invalid URL' and the old hosting item answers "
                                  "CONT_0001 on both portals; the crosswalk's External Landing Page "
                                  "is a static January 1999 FJC map PDF. The live maintained "
                                  "publication is this Administrative Office of the U.S. Courts "
                                  "organization's hosted districts service, verified live on "
                                  "2026-11-14 (owner Courts_GIS_AOUSC in org LJKafwRzLHEkKeOL)."),
                              "evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx"},
        "drift_guard": ("Current AOUSC catalog item identity (id, owner, owning organization, "
                        "title, access, accessInformation, description, license, snippet, tags, "
                        "category, CRS and extent), service identity roster, exact field "
                        "declarations/types/nullability, editingInfo freshness pins, layer "
                        "display/type fields, copyright, geometry properties, explicit EPSG:4326 "
                        "layer/page SRs, XY-only dimensional flags, count and before/after ID "
                        "roster, distinct NAME/ABV/DistName/State/Circuit rosters, per-field "
                        "null rosters (State and shape measures null only on the 'XXX' "
                        "placeholder and Shape_Leng also null on Eastern Washington), the pinned "
                        "null-geometry 'XXX' row, the pinned invalid-geometry roster (producer "
                        "ring self-intersections in Oregon, Western Washington and Eastern "
                        "Virginia retained, never repaired), per-page IDs/attributes/transfer "
                        "limits, ring closure/winding/hole ownership, declared-extent containment "
                        "and Shape__Area/Shape__Length parity are checked."),
    }
    return layer["fields"], features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeInteger": pa.int64(), "esriFieldTypeDouble": pa.float64()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    import io
    geometry_types = set()
    columns = {field["name"]: pa.array([row["attributes"][field["name"]] for row in features],
                                       type=_arrow_type(field)) for field in fields}
    wkb_column = []
    for row in features:
        geometry = row.get("geometry")
        if geometry is None:
            wkb_column.append(None)
        else:
            wkb = _esri_polygon_wkb(geometry)
            geometry_types.add("MultiPolygon" if from_wkb(wkb).geom_type == "MultiPolygon" else "Polygon")
            wkb_column.append(wkb)
    columns["geometry"] = pa.array(wkb_column, type=pa.binary())
    table = pa.Table.from_arrays(list(columns.values()), names=list(columns))
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                   "columns": {"geometry": {"encoding": "WKB", "geometry_types": sorted(geometry_types), "crs": crs}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_us_district_court_jurisdictions(storage: StagingStorageResource, *, version: str,
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
                Path("/tmp/hifld-native-reconstruction/366/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/us-district-court-jurisdictions-draft"}) as client:
                ids_body, ids_receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before")
                ids = json.loads(ids_body).get("objectIds")
                if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
                        or len(ids) != EXPECTED_COUNT or sorted(ids) != EXPECTED_IDS
                        or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_before"] = (ids_body, ids_receipt)
                roster = _capture_roster(sorted(ids))
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
    if (audit["source_record_sha256"] != EXPECTED_RECORD_SHA256
            or audit["source_geometry_sha256"] != EXPECTED_GEOMETRY_SHA256):
        raise ValueError("source_record_or_geometry_digest_drift")
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name, "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": CATALOG_ITEM_URL if name == "catalog_item" else LAYER_URL, "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The service editingInfo.dataLastEditDate ("
                                        + EXPECTED_LAST_DATA_EDIT_UTC + ") is a service edit timestamp, "
                                        "not a producer release date; the item publishes no release-date field."),
        "license": ("The AOUSC item publishes no licenseInfo text; judicial branch work product of "
                    "the Administrative Office of the U.S. Courts. Formal rights acceptance remains pending."),
        "attribution": "Administrative Office of the U.S. Courts",
        **({"delegation_evidence": {
            "dhs_crosswalk_status": "Deprecate",
            "dhs_crosswalk_layer_name": "US District Court Jurisdictions",
            "dhs_crosswalk_agency": "Federal Judicial Center",
            "old_hifld_item_id": "ed086b05d8dc46eab574e0fecdcf0f1f",
            "crosswalk_open_rest_service": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/US_District_Court_Jurisdictions/FeatureServer",
            "crosswalk_evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx",
            "catalog_item_owner": CATALOG_ITEM_OWNER,
            "catalog_item_org": CATALOG_ORG_ID + " " + CATALOG_ORG_NAME,
            "producer_identity_basis": ("The August 2025 DHS HIFLD Open crosswalk marks 'US District Court "
                "Jurisdictions' as Deprecate with no successor and names the Federal Judicial Center, "
                "whose External Landing Page is a static January 1999 map PDF. Probes on 2026-11-14 found "
                "the crosswalk's Open REST service dead (HTTP 400 'Invalid URL') and the old item deleted "
                "(CONT_0001). The federal judiciary maintains the operative product itself: the item is "
                "hosted in the ArcGIS Online organization 'Judicial Branch' (urlKey AOUSC) of the "
                "Administrative Office of the U.S. Courts, published by account Courts_GIS_AOUSC, whose "
                "snippet names the office's own 'TMB VAR U.S. Courts Asset Information Final group layer', "
                "alongside sibling AO products (courts circuits, divisions, states & territories). "
                "District composition is fixed by 28 U.S.C. Sec. 81-131. The captured service response, "
                "not the item page, supplies the data.")}}
           if name == "catalog_item" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "US District Court Jurisdictions",
        "description": ("Complete observed 'TMB U.S. Courts Districts Final' layer 5 from the "
                        "Administrative Office of the U.S. Courts (Judicial Branch, AOUSC) ArcGIS Online "
                        "organization after the DHS HIFLD Open crosswalk retired the layer with no named "
                        "successor. Unapproved faithful candidate; source acceptance, baseline, schema/ID "
                        "contract, coverage, rights and release decisions remain pending."),
        "publisher": PUBLISHER, "agency": "Administrative Office of the U.S. Courts", "source_url": LAYER_URL,
        "license": None, "source_modified": EXPECTED_LAST_DATA_EDIT_UTC, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polygon to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all 10 declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID retained unchanged; the unassigned null-geometry 'XXX' row is retained, not dropped or reidentified",
            "source_crs": "EPSG:4326",
            "geometry": ("2D ring order, winding and hole ownership preserved; no reprojection or "
                         "simplification; the null-geometry 'XXX' row stays null and the three "
                         "producer-invalid polygons (ring self-intersections) are retained unchanged"),
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; source/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("DHS HIFLD Open crosswalk (August 2025) row 'US District Court Jurisdictions' marks the "
                    "retired HIFLD/GeoPlatform item ed086b05d8dc46eab574e0fecdcf0f1f as Deprecate ('No "
                    "longer supported as part of the HIFLD mission') with no successor, pointing only to a "
                    "static 1999 Federal Judicial Center map PDF; the crosswalk's Open REST service and old "
                    "item were verified dead on 2026-11-14. The Administrative Office of the U.S. Courts "
                    "maintains the same product (the 94 federal judicial districts plus an unassigned 'XXX' "
                    "placeholder) in its own 'Judicial Branch' ArcGIS Online organization as "
                    + LAYER_URL + " with data edited in March 2026. No third-party mirror or archive is "
                    "used; DataLumos/PASDA copies and non-producer ArcGIS items (Esri U.S. Federal "
                    "Datasets, Esri professional services, DOJ OIG audit snapshot, frozen 2020 USMS "
                    "boundaries) are research context only and were never fetched."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D rings are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": LAYER_NAME, "official_layer_id": LAYER_ID,
                  "official_source_item": CATALOG_ITEM_OWNER + " " + CATALOG_ITEM_ID,
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
        return _candidate_result(audit, prefix, version, capture_hash, reused=True, root=root)
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
    return _candidate_result(audit, prefix, version, capture_hash, reused=False, root=root)


def _candidate_result(audit: dict, prefix: str, version: str, capture_hash: str, *,
                      reused: bool, root: Path) -> dict[str, object]:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1, "source_id_max": 95,
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": reused,
            "source_capture_evidence_sha256": capture_hash,
            "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_null_count": audit["geometry_null_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"],
            "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(root)}
