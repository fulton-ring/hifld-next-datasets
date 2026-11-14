"""Complete native capture for the official USCG Sectors public view layer."""
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
import shapely
from shapely import from_wkb
from shapely.validation import explain_validity

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
from dagster_hifld.sources.levee_areas_usace_ienc import _esri_polygon_wkb
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture_identity, _load_capture, _sha,
    _validate_capture_request_identity, _validate_spatial_reference,
)

DATASET_SLUG = "uscg-sectors"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "United States Coast Guard (USCG)"
# Succession: the DHS HIFLD Open crosswalk (August 2025) row "USCG Sectors" marks
# the retired HIFLD/GeoPlatform item 0b10e10d139f40bf9014a0627f4fbc81 as Migrated
# to the GII Portal. Probes on 2026-11-14 found both the crosswalk's GII REST
# service and the old hosting item dead. The producing agency itself now
# maintains the same product in its own ArcGIS Online organization: a public
# view service whose layer copyright names "Operation Systems Center (OSC),
# United States Coast Guard (USCG)", whose item category is
# "/Categories/Coast Guard Boundaries/Sectors", whose publishing account is a
# USCG officer, and whose data was edited in January 2026.
CATALOG_ITEM_ID = "ec3a1f03ec4548bb9d4435ea7e580c1a"
CATALOG_ITEM_OWNER = "Ian.S.Hanna_uscoastguard"
CATALOG_ORG_ID = "Fr5UUTVwNRJlXAkf"
CATALOG_ORG_NAME = "USCG FSLTP Portal"
CATALOG_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{CATALOG_ITEM_ID}?f=json"
LANDING_URL = f"https://uscoastguard.maps.arcgis.com/home/item.html?id={CATALOG_ITEM_ID}"
SERVICE_URL = ("https://services9.arcgis.com/Fr5UUTVwNRJlXAkf/arcgis/rest/services/"
               "USCG_Sectors_view/FeatureServer")
LAYER_URL = f"{SERVICE_URL}/0"
LAYER_ID = 0
LAYER_NAME = "USCGSectors"
ITEM_TITLE = "USCG Sectors (Public View)"
ITEM_ACCESS_INFORMATION = "United States Coast Guard"
ITEM_CATEGORY = "/Categories/Coast Guard Boundaries/Sectors"
ITEM_SNIPPET = ("The purpose of this layer is to provide the United States Coast Guard "
                "with a single map layer that includes the Coast Guard Sector Boundaries "
                "and Area of Responsibility.")
ITEM_DESCRIPTION = ("The Coast Guard Sectors are delineated in the description in the 33 Code "
                    "of Federal Regulations (CFR) for each Sector Boundary and Area of Responsibility "
                    "where latitude and longitude coordinates, as well as county/state/national boundaries "
                    "are included to describe the boundaries for each zone. In addition, whenever the Area "
                    "of Responsibility boundary is over water, the EEZ shapefile is referenced for those "
                    "occurrences. This layer displays the Coast Guard Sector Boundaries for the following "
                    "sectors: Western Alaska and US Arctic (Formerly Anchorage), Baltimore, Boston, Buffalo, "
                    "Charleston, Columbia River, Corpus Christi, Delaware Bay, Detroit, Guam, Hampton Roads, "
                    "Honolulu, Houston - Galveston, Humboldt Bay, Jacksonville, Juneau, Key West, Lake Michigan, "
                    "Long Island Sound, Los Angeles - Long Beach, Lower Mississippi, Miami, Mobile, New Orleans, "
                    "New York, North Bend, North Carolina, Northern New England, Ohio Valley, Puget Sound, "
                    "San Diego, San Francisco, San Juan, Sault Ste Marie, Southeastern New England, "
                    "St. Petersburg, and Upper Mississippi.")
ITEM_LICENSE = ("<span style='font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, "
                "&quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>"
                "Not recommended for navigation. These unaltered data may not be redistributed without all of "
                "the elements of the metadata listed in the Supplemental Information section of this metadata "
                "document. It is recommended that third parties wishing to use these data obtain them from the "
                "Distributor listed in this metadata document. If these data are altered or incorporated into "
                "another dataset, they are not to be redistributed without also: altering the name of the "
                "datasetluding a Content Standards for Digital Geospatial Metadata (FGDC-STD-001-1998) compliant "
                "metadata file that describes the dataset and reflects the alteration steps that makes the new "
                "dataset different from this one, and citing this dataset in the metadata as a source for the "
                "altered dataset using the source citation specified below. If these digital data are used in the "
                "production of a report or in the compilation of a standalone printed map, then this dataset is to "
                "be cited in the report or on the map using the source citation specified below. The following "
                "source citation is to be used when citing this dataset: USCG - OSC Enterprise GIS Inventory "
                "Dataset 'Coast Guard Sectors source data, Geographic NAD83, USCG - OSC Enterprise GIS "
                "Users hereby agree to abide by the use and reproduction conditions specified above. By using "
                "the data described in these metadata, the user acknowledges that the terms and conditions of use "
                "have been read and that the user is bound by these criteria.</span>")
ITEM_EXTENT = [[-179.99998854073854, -17.557773333172747],
               [179.99998854093846, 74.70976665460748]]
LAYER_COPYRIGHT = "Operation Systems Center (OSC), United States Coast Guard (USCG),"
EXPECTED_COUNT = 36
# Observed FID roster: the layer has 36 features with FID 1..37 and deleted FID 30.
EXPECTED_IDS = sorted([*range(1, 30), *range(31, 38)])
EXPECTED_ID_SHA256 = "28605f79a965b6306aae8b8e7daa9d2f3e7e01d8dbcca6e325c0fe75627690ca"
EXPECTED_SECTOR_NAME_SHA256 = "0f5943abff8ff6327801eb9c291b29a4bb17237bb53034adb07cdc779f21c4d1"
PAGE_SIZE = 10
PAGE_COUNT = (len(EXPECTED_IDS) + PAGE_SIZE - 1) // PAGE_SIZE
EXPECTED_EXTENT = {"xmin": -179.99999999999997, "ymin": -17.557773333172747,
                   "xmax": 179.9999885410001, "ymax": 74.70976665500007}
# The advertised extent is rounded relative to the observed vertex bounds by a
# few ulps; the containment guard uses a tolerance far below any real change.
EXPECTED_EXTENT_TOLERANCE = 1e-9
EXPECTED_EDITING_INFO = {"lastEditDate": 1774374531856, "schemaLastEditDate": 1774374529592,
                         "dataLastEditDate": 1769797781779}
EXPECTED_LAST_DATA_EDIT_UTC = "2026-01-30T18:29:41.779000Z"
EXPECTED_SCHEMA_EDIT_UTC = "2026-03-24T17:48:49.592000Z"
EXPECTED_GEOMETRY_PROPERTIES = {"shapeAreaFieldName": "Shape__Area",
                                "shapeLengthFieldName": "Shape__Length",
                                "units": "esriDecimalDegrees"}
SERVICE_FULL_EXTENT = {"xmin": -179.99998854073854, "ymin": -17.557773333172747,
                       "xmax": 179.99998854093846, "ymax": 74.70976665460748}
LAYER_MAX_RECORD_COUNT = 2000
SERVICE_MAX_RECORD_COUNT = 1000
# The source itself publishes one self-intersecting ring (Sector New York,
# "Ring Self-intersection[-74.6500029470783 42.0000067959769]"): a single-ring
# polygon whose boundary touches itself at exactly one point, valid under Esri
# semantics but invalid under OGC. Raw capture keeps the ring byte-exact; the
# candidate applies the minimal OGC make_valid repair to that pinned feature
# only (observed: area unchanged, one Polygon, one inserted intersection
# vertex) and fails closed on anything else. Repair acceptance is its own gate.
EXPECTED_GEOMETRY_INVALID_COUNT = 1
EXPECTED_REPAIRED_IDS = [16]
GEOMETRY_REPAIR_AREA_TOLERANCE = 1e-12
EXPECTED_FIELD_DECLARATIONS = [
    {'name': 'FID', 'type': 'esriFieldTypeOID', 'alias': 'FID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None},
    {'name': 'SectorName', 'type': 'esriFieldTypeString', 'alias': 'Sector Name', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None},
    {'name': 'Atu', 'type': 'esriFieldTypeString', 'alias': 'Atu', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'Opfac', 'type': 'esriFieldTypeString', 'alias': 'Opfac', 'sqlType': 'sqlTypeOther', 'length': 5, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'DistrictNa', 'type': 'esriFieldTypeString', 'alias': 'DistrictNa', 'sqlType': 'sqlTypeOther', 'length': 15, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'AreaName', 'type': 'esriFieldTypeString', 'alias': 'AreaName', 'sqlType': 'sqlTypeOther', 'length': 20, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'NAME', 'type': 'esriFieldTypeString', 'alias': 'NAME', 'sqlType': 'sqlTypeOther', 'length': 35, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'Shape_Leng', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Leng', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'Emergency_', 'type': 'esriFieldTypeString', 'alias': 'Emergency Phone Num', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None},
    {'name': 'Shape__Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape__Area', 'sqlType': 'sqlTypeDouble', 'nullable': True, 'editable': False, 'domain': None, 'defaultValue': None},
    {'name': 'Shape__Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape__Length', 'sqlType': 'sqlTypeDouble', 'nullable': True, 'editable': False, 'domain': None, 'defaultValue': None},
]
ATTRIBUTE_FIELDS = EXPECTED_FIELD_DECLARATIONS
CAPTURE_NAMES = {"catalog_item", "service_before", "layer_before", "count_before",
                 "ids_before", "layer_after", "count_after", "ids_after", "service_after",
                 *{f"page_{index:03d}" for index in range(PAGE_COUNT)}}
SHAPE_PARITY_TOLERANCE = 1e-9


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


# The biggest observed page (chunked by the pinned FID roster) decodes to about
# 4.5 MB; this dataset carries its own budgets like the other native captures.
PAGE_MAX_BYTES = 16_000_000
CAPTURE_MAX_BYTES = 128_000_000


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
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "FID ASC", "f": "json"})))
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
    if not isinstance(roster, dict) or set(manifest["receipts"]) != CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_roster(EXPECTED_IDS))
    captures = {}
    for name in sorted(CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _validate_page_fields(page_fields: object, layer_fields: list[dict]) -> None:
    """Reject page-level physical schema drift before constructing a native table."""
    if not isinstance(page_fields, list) or len(page_fields) != len(layer_fields):
        raise ValueError("source_page_physical_schema_changed")
    for page_field, layer_field in zip(page_fields, layer_fields):
        if not isinstance(page_field, dict) or not isinstance(layer_field, dict):
            raise ValueError("source_page_physical_schema_changed")
        for key, value in page_field.items():
            if layer_field.get(key) != value:
                raise ValueError("source_page_physical_schema_changed")


def _validate_catalog_item(item: dict) -> None:
    if (item.get("id") != CATALOG_ITEM_ID or item.get("owner") != CATALOG_ITEM_OWNER
            or item.get("orgId") != CATALOG_ORG_ID or item.get("type") != "Feature Service"
            or item.get("title") != ITEM_TITLE or item.get("access") != "public"
            or item.get("accessInformation") != ITEM_ACCESS_INFORMATION
            or item.get("spatialReference") != "4326"):
        raise ValueError("uscg_catalog_item_identity_drift")
    if item.get("description") != ITEM_DESCRIPTION or item.get("licenseInfo") != ITEM_LICENSE:
        raise ValueError("uscg_catalog_item_text_drift")
    if item.get("snippet") != ITEM_SNIPPET or item.get("categories") != [ITEM_CATEGORY]:
        raise ValueError("uscg_catalog_item_text_drift")
    extent = item.get("extent")
    if (not isinstance(extent, list) or len(extent) != 2
            or extent[0] != ITEM_EXTENT[0] or extent[1] != ITEM_EXTENT[1]):
        raise ValueError("uscg_catalog_item_extent_drift")


def _validate_service(service: dict, label: str) -> None:
    if (service.get("serviceItemId") != CATALOG_ITEM_ID
            or service.get("description") != ""
            or service.get("serviceDescription") != ""
            or service.get("capabilities") != "Query,Extract"
            or service.get("maxRecordCount") != SERVICE_MAX_RECORD_COUNT
            or service.get("hasVersionedData") is not False
            or service.get("isView") is not True):
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
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("maxRecordCount") != LAYER_MAX_RECORD_COUNT
            or layer.get("objectIdField") != "FID" or layer.get("globalIdField") != ""
            or layer.get("typeIdField") != "SectorName" or layer.get("displayField") != "NAME"
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or layer.get("isView") is not True or layer.get("enableNullGeometry") is not False
            or layer.get("copyrightText") != LAYER_COPYRIGHT
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
    coverage = {key: EXPECTED_EXTENT[key] for key in ("xmin", "ymin", "xmax", "ymax")}
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        if page.get("objectIdFieldName") != "FID" or page.get("geometryType") != "esriGeometryPolygon":
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
                    or tuple(attrs) != tuple(field["name"] for field in ATTRIBUTE_FIELDS)):
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
                if kind == "esriFieldTypeDouble" and (type(value) not in (int, float)
                                                      or not math.isfinite(float(value))):
                    raise ValueError("source_double_value_type_drift:" + field["name"])
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
            oid = attrs.get("FID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            page_ids.append(oid)
            geom = from_wkb(_esri_polygon_wkb(geometry))
            geometry_empty += int(geom.is_empty)
            geometry_valid += int(geom.is_valid and not geom.is_empty)
            for parity_field, observed in (("source_shape_area_parity_drift", attrs.get("Shape__Area")),
                                           ("source_shape_length_parity_drift", attrs.get("Shape__Length"))):
                if observed is None:
                    continue
                computed = geom.area if parity_field.startswith("source_shape_area") else geom.length
                if abs(computed - observed) > SHAPE_PARITY_TOLERANCE * max(abs(observed), 1e-12):
                    raise ValueError(parity_field)
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["FID"] for feature in features]
    if sorted(ids) != EXPECTED_IDS or len(features) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_id_population_drift")
    names = [feature["attributes"]["SectorName"] for feature in features]
    if (len(set(names)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(set(names)))) != EXPECTED_SECTOR_NAME_SHA256):
        raise ValueError("source_sector_name_roster_drift")
    geometry_invalid = len(features) - geometry_valid - geometry_empty
    if geometry_invalid != EXPECTED_GEOMETRY_INVALID_COUNT:
        raise ValueError("source_geometry_invalid_count_drift")
    invalid_ids = sorted(row["attributes"]["FID"] for row in features
                         if not from_wkb(_esri_polygon_wkb(row["geometry"])).is_valid)
    if invalid_ids != EXPECTED_REPAIRED_IDS:
        raise ValueError("source_geometry_invalid_roster_drift")
    features.sort(key=lambda row: row["attributes"]["FID"])
    bounds = [from_wkb(_esri_polygon_wkb(row["geometry"])).bounds for row in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_id_note": ("The layer holds 36 features with FID 1..37; FID 30 is absent "
                           "(deleted upstream). The gap is part of the pinned roster and is "
                           "preserved; no substitute IDs are generated."),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["FID"], row["geometry"]] for row in features])),
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": geometry_invalid,
        "geometry_invalid_ids": invalid_ids,
        "geometry_invalid_note": ("Exactly one feature is invalid in the source itself "
                                  "(FID 16, Sector New York): a single-ring polygon whose "
                                  "boundary touches itself at exactly one point, valid under "
                                  "Esri semantics but invalid under OGC. Raw capture keeps "
                                  "the ring byte-exact; the candidate applies the minimal "
                                  "pinned make_valid repair recorded under geometry_repair."),
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"],
        "source_last_data_edit_utc": EXPECTED_LAST_DATA_EDIT_UTC,
        "source_last_schema_edit_utc": EXPECTED_SCHEMA_EDIT_UTC,
        "source_coverage_scope": ("Complete observed USCGSectors layer 0 from the USCG-owned "
                                  "USCG FSLTP Portal ArcGIS Online organization (public view "
                                  "service USCG_Sectors_view). The layer spans the Coast Guard "
                                  "sector boundaries and areas of responsibility including "
                                  "over-water EEZ extents and both dateline halves; it is not "
                                  "asserted to cover any other Coast Guard administrative level."),
        "catalog_item_id": CATALOG_ITEM_ID, "catalog_item_owner": CATALOG_ITEM_OWNER,
        "catalog_item_org": CATALOG_ORG_ID + " " + CATALOG_ORG_NAME,
        "catalog_item_url": CATALOG_ITEM_URL, "landing_page_url": LANDING_URL,
        "layer_copyright": LAYER_COPYRIGHT,
        "dhs_crosswalk_row": {"layer_name": "USCG Sectors", "status": "Migrated",
                              "agency": "U.S. Coast Guard (USCG)",
                              "old_hifld_item_id": "0b10e10d139f40bf9014a0627f4fbc81",
                              "old_hosting_location": "https://geoplatform.maps.arcgis.com/home/item.html?id=0b10e10d139f40bf9014a0627f4fbc81",
                              "crosswalk_new_hosting_location": "GII Portal",
                              "crosswalk_open_rest_service": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/USCG_Sectors_/FeatureServer",
                              "successor_probes_2026_11_14": ("The crosswalk's GII REST service "
                                  "answers only with an ArcGIS 'Invalid URL' error envelope and the "
                                  "old hosting item no longer exists or is inaccessible; the live "
                                  "maintained publication is this USCG-owned organization's public "
                                  "view service."),
                              "evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx"},
        "drift_guard": ("Current USCG catalog item identity (id, owner, owning organization, "
                        "title, access, accessInformation, description, license, snippet, "
                        "category, CRS and extent), service identity roster, exact field "
                        "declarations/types/nullability/view-override, editingInfo freshness "
                        "pins, layer display/type fields, copyright, geometry properties, "
                        "explicit EPSG:4326 layer/page/feature SRs, XY-only dimensional flags, "
                        "count and before/after ID roster (including the documented deleted-FID "
                        "30 gap), distinct SectorName roster, per-page IDs/attributes/transfer "
                        "limits, ring closure/winding/hole ownership, declared-extent "
                        "containment, Shape__Area/Shape__Length parity and the single "
                        "source-published invalid geometry are checked."),
    }
    return layer["fields"], features, audit


def _candidate_geometry_wkb(fid: int, geometry: dict) -> tuple[bytes, dict | None]:
    """Return candidate WKB for one feature plus its repair record, if any.

    Source-valid features pass through the shared XY-ring converter unchanged.
    The pinned source-invalid feature gets the minimal OGC make_valid repair
    (structure method, so no linear debris is invented); any repair that would
    alter area, bounds or polygon-ness fails closed instead of sealing
    altered geometry.
    """
    raw = from_wkb(_esri_polygon_wkb(geometry))
    if raw.is_valid:
        return _esri_polygon_wkb(geometry), None
    if raw.is_empty or raw.geom_type != "Polygon":
        raise ValueError("source_geometry_unsupported_repair_input")
    fixed = shapely.make_valid(raw, method="structure")
    if (fixed.is_empty or not fixed.is_valid
            or fixed.geom_type not in ("Polygon", "MultiPolygon")):
        raise ValueError("source_geometry_repair_not_minimal")
    if abs(fixed.area - raw.area) > GEOMETRY_REPAIR_AREA_TOLERANCE * max(abs(raw.area), 1e-12):
        raise ValueError("source_geometry_repair_area_drift")
    if tuple(raw.bounds) != tuple(fixed.bounds):
        raise ValueError("source_geometry_repair_bounds_drift")
    record = {"feature_id": fid, "operation": "make_valid(method=structure)",
              "validity_before": explain_validity(raw),
              "source_ring_vertex_count": sum(len(ring) for ring in geometry["rings"]),
              "repaired_part_count": len(getattr(fixed, "geoms", [fixed])),
              "repaired_vertex_count": sum(len(part.exterior.coords)
                                           + sum(len(interior.coords)
                                                 for interior in part.interiors)
                                           for part in getattr(fixed, "geoms", [fixed])),
              "source_area": raw.area, "repaired_area": fixed.area}
    return fixed.wkb, record


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict],
                   geometry_wkb: dict[int, bytes]) -> bytes:
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in fields]
    arrays.append(pa.array([geometry_wkb[row["attributes"]["FID"]] for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in fields] + ["geometry"])
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                   "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"], "crs": crs}}})}
    import io
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_uscg_sectors(storage: StagingStorageResource, *, version: str,
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
                Path("/tmp/hifld-native-reconstruction/371/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/uscg-sectors-draft"}) as client:
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
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    geometry_wkb = {}
    repair_records = []
    for row in features:
        fid = row["attributes"]["FID"]
        wkb, record = _candidate_geometry_wkb(fid, row["geometry"])
        geometry_wkb[fid] = wkb
        if record is not None:
            repair_records.append(record)
    if [record["feature_id"] for record in repair_records] != EXPECTED_REPAIRED_IDS:
        raise ValueError("source_geometry_repair_roster_drift")
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "geometry_repair": {"operation": "make_valid", "applied_to_feature_ids": EXPECTED_REPAIRED_IDS,
            "records": repair_records,
            "candidate_geometry_sha256": _sha(_canonical(
                [[fid, geometry_wkb[fid].hex()] for fid in sorted(geometry_wkb)])),
            "note": ("Raw capture keeps every source ring byte-exact. The candidate repairs "
                     "only the pinned OGC-invalid self-touching ring; the structure-method "
                     "make_valid must keep a valid Polygon or MultiPolygon with unchanged "
                     "area and bounds or the capture fails closed. Repair acceptance is its "
                     "own unaccepted gate.")},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False,
        "geometry_repair_accepted": False})
    parquet = _parquet_bytes(fields, features, geometry_wkb)
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
                                        "not a producer release date; no release-date field exists on the layer."),
        "license": ("USCG ArcGIS item licenseInfo states the data are not recommended for navigation and "
                    "binds redistribution to metadata retention; formal rights acceptance remains pending."),
        "attribution": "United States Coast Guard",
        **({"delegation_evidence": {
            "dhs_crosswalk_status": "Migrated",
            "dhs_crosswalk_layer_name": "USCG Sectors",
            "dhs_crosswalk_agency": "U.S. Coast Guard (USCG)",
            "old_hifld_item_id": "0b10e10d139f40bf9014a0627f4fbc81",
            "crosswalk_open_rest_service": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/USCG_Sectors_/FeatureServer",
            "crosswalk_evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx",
            "catalog_item_owner": CATALOG_ITEM_OWNER,
            "catalog_item_org": CATALOG_ORG_ID + " " + CATALOG_ORG_NAME,
            "producer_identity_basis": ("After the August 2025 DHS HIFLD Open crosswalk marked 'USCG Sectors' "
                "as Migrated to the GII Portal, probes on 2026-11-14 found the named GII REST service absent "
                "(ArcGIS 'Invalid URL' error envelope) and the old hosting item dead. The producing agency "
                "itself maintains the successor: item category '/Categories/Coast Guard Boundaries/Sectors', "
                "accessInformation 'United States Coast Guard', layer copyright 'Operation Systems Center "
                "(OSC), United States Coast Guard (USCG),' (the USCG Operation Systems Center at Kearneysville "
                "named by the HIFLD inventory publisher), a publishing account of a USCG officer in the "
                "USCG-owned 'USCG FSLTP Portal' organization, and sector data edited in January 2026. The "
                "captured service response, not the item page, supplies the data.")}}
           if name == "catalog_item" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "USCG Sectors",
        "description": ("Complete observed USCG Sectors (Public View) layer 0 from the USCG-owned "
                        "USCG FSLTP Portal ArcGIS Online organization; successor acceptance, baseline, "
                        "contract, coverage, rights and release decisions remain unaccepted."),
        "publisher": PUBLISHER, "agency": "U.S. Coast Guard", "source_url": LAYER_URL,
        "license": None, "source_modified": EXPECTED_LAST_DATA_EDIT_UTC, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polygon to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all 11 declared attributes retained in source order and declared types",
            "source_ids": "FID retained unchanged; the source roster skips deleted FID 30 and no substitute IDs are generated",
            "source_crs": "EPSG:4326",
            "geometry": ("2D ring order, winding and hole ownership preserved; no reprojection or "
                         "simplification; dateline-split shell rings retained as separate polygons"),
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; baseline/schema/ID/coverage/rights/release decisions pending"}},
            {"operation": "Minimal OGC make_valid repair of the pinned source-invalid ring", "parameters": {
            "applied_to_feature_ids": EXPECTED_REPAIRED_IDS,
            "reason": ("The source publishes exactly one OGC-invalid polygon (FID 16, Sector New York): a "
                       "single ring whose boundary touches itself at one point, valid under Esri semantics "
                       "but rejected by the shared publication catalog's zero-invalid-geometry bar."),
            "guarantees": ("structure-method make_valid must keep a valid Polygon or MultiPolygon with "
                           "unchanged area (1e-12 relative tolerance) and unchanged bounds, or the capture "
                           "fails closed; the repaired part/vertex counts and before-validity text are "
                           "recorded in the audit"),
            "raw_evidence": "the original self-touching ring is retained byte-exact in raw/page_001.body",
            "acceptance": "geometry_repair_accepted remains False; repair acceptance is its own pending gate"}}],
        "lineage": ("DHS HIFLD Open crosswalk (August 2025) row 'USCG Sectors' marks the retired "
                    "HIFLD/GeoPlatform item 0b10e10d139f40bf9014a0627f4fbc81 as Migrated to the GII Portal; "
                    "the named GII REST service and old item were verified dead on 2026-11-14. The USCG-owned "
                    "'USCG FSLTP Portal' ArcGIS Online organization now maintains the same product (sector "
                    "boundaries per 33 CFR with EEZ-based over-water extents) as the public view service "
                    + LAYER_URL + " with data edits in January 2026. No third-party mirror or archive is used; "
                    "DataLumos copies are research context only."),
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
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "geometry_repair_accepted": False}
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
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1, "source_id_max": 37,
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "geometry_repair_accepted": False,
            "capture_directory": str(root)}
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
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1, "source_id_max": 37,
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "geometry_repair_accepted": False,
            "capture_directory": str(root)}
