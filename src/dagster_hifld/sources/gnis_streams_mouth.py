"""Lossless, approval-gated capture of the USGS GNIS "Streams (Mouth)" gazetteer layer.

The HIFLD Open inventory lists ``nmg/streams-mouth-1`` ("Streams (Mouth)",
agency: U.S. Geological Survey) inside the National Map Gazetteer group; the
DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1) consolidates the group under
the row "Geographic Names Information System (GNIS) Feature Layers" and names
the producer's own REST service
``https://carto.nationalmap.gov/arcgis/rest/services/geonames/MapServer/`` as
the open hosting location. That service is published by USGS The National Map
on USGS's own ArcGIS Server (no ArcGIS Online serviceItemId); its
``copyrightText`` states "Data Refreshed October, 2026" (a service refresh
statement, not a per-snapshot release date). The producer's GNIS documentation
page (https://www.usgs.gov/tools/geographic-names-information-system-gnis)
links this exact service URL and is captured as dated documentation evidence.
The retired ``services.nationalmap.gov`` host no longer resolves (observed
2026-11-13), so no alias publication exists. The inventory's DataLumos project
240202 archive copy is identification context only and is never fetched.

Layer 6 "Streams (Mouth)" is the gazetteer's hydrographic mouth layer
(``gaz_featureclass`` values "Stream" and "Canal"; ``fcode`` values 33600,
36200, 46000, 46003, 46006 and 46100): one multipoint feature per gazetteer
stream/canal name whose point(s) sit at the mouth of the feature. The geometry
carries the producer's Z-aware declaration (``hasZ`` true) and every served
ordinate triple ``[x, y, z]`` holds ``z == 0`` when ``returnZ`` is requested.
Coordinates are served in the layer's native web-Mercator spatial reference
(wkid 102100, latestWkid 3857); they are captured verbatim without
reprojection. The layer spans the whole GNIS publication area including the
Pacific and Caribbean territories (the declared layer extent reaches
y = -1616028.9976, i.e. American Samoa, and x = 20000987.6666, i.e. Guam).
``gaz_id`` and ``objectid_1`` repeat across the per-county rows of one
gazetteer feature while each ``(objectid_1, state_alpha, county_name)`` row
identity is unique; every source value, null (347 null ``state_alpha`` and 2
null ``county_name`` rows), and the exact multipoint membership are preserved
unchanged.

The layer is captured as ordered-offset pages (the service reports
``maxRecordCount`` 2000 and advertises pagination plus ordering) of the sorted
OBJECTID roster: every accepted page must return exactly its expected sorted
id slice, the accepted pages must tile the complete roster without gaps or
overlaps, and the before/after count/id/metadata brackets plus full-record and
geometry digests prove that nothing was skipped, duplicated, or reordered.
"""
from __future__ import annotations

import hashlib
import json
import math
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
from shapely.geometry import MultiPoint

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get, candidate_prefix, content_derived_version,
    processing_identity, quarantine_incomplete_capture, seal_ready_candidate,
    verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "streams-mouth"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://carto.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/geonames/MapServer"
GNIS_DOC_URL = "https://www.usgs.gov/tools/geographic-names-information-system-gnis"
LAYER_ID = 6
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Streams (Mouth)"
PUBLISHER = "U.S. Geological Survey (The National Map)"
AGENCY = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/gnis-streams-mouth-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 3857
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
# Initial page width, capped by the producer's maxRecordCount (2000). Any page
# whose response the producer refuses or drops mid-stream is bisected until the
# producer serves both halves; see the module docstring.
PAGE_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MAX_CAPTURE_TOTAL_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 4_000_000_000
# Every request retries a bounded number of times on transient producer error
# documents or incomplete transport captures; a retry re-issues the exact same
# request, and completeness is still enforced by the pinned before/after id
# rosters, counts, exact page tiling, and full-record digests, so retries can
# never mask drift. Pages wider than one record that keep failing are bisected;
# a single-record page that keeps failing fails the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_PROBE_ATTEMPTS = 2
PAGE_PROBE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-13 complete-layer population contract,
# established by two dated complete captures (independent passes) that produced
# identical record and geometry digests.
EXPECTED_COUNT = 293894
EXPECTED_ID_SHA256 = "8f80e48547a86655f1fb9a17aa6b2dd3b9d20114bb3d62aa5afffe69a3f28fe9"
EXPECTED_SCHEMA_SHA256 = "054726f396a55139d7db341622d192fdac21b854cb011e5569c897838c83d6ac"
EXPECTED_RECORD_SHA256 = "PENDING_FULL_CAPTURE_EVIDENCE"
EXPECTED_GEOMETRY_SHA256 = "PENDING_FULL_CAPTURE_EVIDENCE"
EXPECTED_LAYER_EXTENT = {"xmin": -19780547.5984, "ymin": -1616028.9976000004,
                         "xmax": 20000987.6666, "ymax": 11509303.568499997}
EXPECTED_OBSERVED_BOUNDS = None  # PENDING_FULL_CAPTURE_EVIDENCE
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_INVALID_GEOMETRY_COUNT = 0
EXPECTED_GAZ_ID_NULL_COUNT = 0
EXPECTED_OBJECTID_1_NULL_COUNT = 0
EXPECTED_STATE_ALPHA_NULL_COUNT = 347
EXPECTED_COUNTY_NAME_NULL_COUNT = 2
EXPECTED_FEATURE_CLASS_VALUES = ["Canal", "Stream"]
EXPECTED_FCODE_VALUES = [33600, 36200, 46000, 46003, 46006, 46100]
EXPECTED_UNKNOWN_COORDS_VALUES = [2]
EXPECTED_FIELD_NAMES = ["OBJECTID", "gaz_id", "gaz_name", "gaz_featureclass", "fcode",
                        "state_alpha", "county_name", "objectid_1", "isunknowncoords"]
EXPECTED_FIELD_TYPES = ["esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeInteger", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeInteger", "esriFieldTypeInteger"]
EXPECTED_COPYRIGHT_TEXT = ("USGS The National Map: Geographic Names Information System. "
                           "Data Refreshed October, 2026.")
EXPECTED_SERVICE_DESCRIPTION_PREFIX = ("USGS developed The National Map Gazetteer as the "
                                       "Federal and national standard (ANSI INCITS 446-2008) "
                                       "for geographic nomenclature based on the Geographic "
                                       "Names Information System (GNIS).")
EXPECTED_SERVICE_LAYERS = [
    (0, "Places (expand for more)", "Group Layer", -1),
    (1, "Incorporated Places (Civil)", "Feature Layer", 0),
    (2, "Unincorporated Places (Census)", "Feature Layer", 0),
    (3, "Populated Places", "Feature Layer", 0),
    (4, "Physical Points (expand for more)", "Group Layer", -1),
    (5, "Landforms", "Feature Layer", 4),
    (6, "Streams (Mouth)", "Feature Layer", 4),
    (7, "Other Hydrographic Features", "Feature Layer", 4),
    (8, "Antarctica", "Feature Layer", 4),
    (9, "Cultural Points (expand for more)", "Group Layer", -1),
    (10, "Crossings", "Feature Layer", 9),
    (11, "Historical Points (expand for more)", "Group Layer", -1),
    (12, "Historical Cultural-Political Points", "Feature Layer", 11),
    (13, "Historical Hydrographic Points", "Feature Layer", 11),
    (14, "Historical Physical Points", "Feature Layer", 11),
]


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``.

    Byte-identical to ``_sha(_canonical(items))`` without materializing the
    complete document twice.
    """
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "domain")
            if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


_PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    match = _PAGE_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _page_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "returnZ": "true", "returnM": "false",
                   "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
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
    """The producer's native web-Mercator pair (wkid 102100, latestWkid 3857)."""
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    if value.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"]) \
            or value.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise ValueError(label + "_crs_drift")


def _collect_page(offset: int, count: int, page: object, ids: list[int],
                  features: list[dict[str, object]]) -> None:
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    reference = page.get("spatialReference")
    if not isinstance(reference, dict) or reference != dict(NATIVE_LAYER_SR):
        raise ValueError(f"source_page_{offset:06d}_crs_drift")
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    expected = ids[offset:offset + count]
    actual = [row.get("attributes", {}).get("OBJECTID") for row in rows if isinstance(row, dict)]
    if actual != expected or len(rows) != len(expected):
        raise ValueError("source_page_id_slice_incomplete_or_reordered")
    if page.get("exceededTransferLimit") is True and offset + count >= EXPECTED_COUNT:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    features.extend(rows)


def _classify_geometries(features: list[dict[str, object]], attr_names: list[str]):
    """Multipoint-Z geometry audit: [OBJECTID, [[x, y, z], ...]] records.

    The producer declares the layer Z-aware (``hasZ`` true in the layer and page
    documents) and, when ``returnZ`` is requested, serves every point as an
    exact three-element ``[x, y, z]`` triple whose ``z`` is 0 (observed for the
    complete 293,894-feature layer on 2026-11-13); coordinates are native
    web-Mercator values contained in the declared layer extent. A geometry that
    is not exactly a multipoint ``points`` array of such triples is counted as
    invalid and the capture fails closed (never repaired or coerced).
    """
    invalid_geometry = 0
    null_geometry = 0
    geometry_records: list[list] = []
    xmin = ymin = xmax = ymax = None
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        oid = attrs["OBJECTID"]
        if geom is None:
            null_geometry += 1
            geometry_records.append([oid, None])
            continue
        if (not isinstance(geom, dict) or set(geom) != {"hasZ", "points"}
                or geom.get("hasZ") is not True):
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        points = geom.get("points")
        if not isinstance(points, list) or not points:
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        for point in points:
            if not isinstance(point, list) or len(point) != 3:
                raise ValueError("source_point_z_dimension_missing")
            x, y, z = point
            for name, value in (("x", x), ("y", y), ("z", z)):
                if type(value) not in (int, float):
                    raise ValueError(f"source_point_ordinate_not_numeric:{name}")
                if not math.isfinite(value):
                    raise ValueError(f"source_point_ordinate_not_finite:{name}")
            if z != 0:
                raise ValueError("source_point_z_not_observed_zero")
            if not (EXPECTED_LAYER_EXTENT["xmin"] <= x <= EXPECTED_LAYER_EXTENT["xmax"]
                    and EXPECTED_LAYER_EXTENT["ymin"] <= y <= EXPECTED_LAYER_EXTENT["ymax"]):
                raise ValueError("source_coordinate_range_drift")
            if xmin is None or x < xmin:
                xmin = x
            if xmax is None or x > xmax:
                xmax = x
            if ymin is None or y < ymin:
                ymin = y
            if ymax is None or y > ymax:
                ymax = y
        geometry_records.append([oid, points])
    observed_bounds = None if xmin is None else {"xmin": xmin, "ymin": ymin,
                                                 "xmax": xmax, "ymax": ymax}
    return geometry_records, null_geometry, invalid_geometry, observed_bounds


def _validate_value_column(features: list[dict[str, object]], name: str,
                           expected_values: list, error_code: str) -> None:
    values = sorted({row["attributes"].get(name) for row in features})
    if values != expected_values:
        raise ValueError(error_code)


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if not name.startswith("page_") and name != "usgs_gnis_doc_page"}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pages = [_parse_page_name(name) for name in page_names]
    position = 0
    for offset, count in pages:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {*page_names} | {"usgs_gnis_doc_page"}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    _validate_gnis_documentation(captures["usgs_gnis_doc_page"][0])
    service, layer = docs["service_before"], docs["layer_before"]
    _validate_service_document(service, "service_before")
    _validate_service_document(docs["service_after"], "service_after")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    layer_fields = _validate_layer_document(layer)
    if docs["layer_after"].get("fields") is None \
            or [_plain_field(x) for x in docs["layer_after"]["fields"]] != layer_fields:
        raise ValueError("source_field_schema_changed_during_capture")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT \
            or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    rosters = [docs[name].get("objectIds") for name in ("ids_seed", "ids_before", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID"
           for name in ("ids_seed", "ids_before", "ids_after")):
        raise ValueError("source_id_field_changed")

    features: list[dict[str, object]] = []
    for (offset, count), name in zip(pages, page_names):
        _collect_page(offset, count, json.loads(captures[name][0]), ids, features)
    attr_names = [field["name"] for field in layer_fields
                  if field["type"] != "esriFieldTypeGeometry"]
    geometry_records, null_geometry, invalid_geometry, bounds = _classify_geometries(
        features, attr_names)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    if (null_geometry != EXPECTED_NULL_GEOMETRY_COUNT
            or invalid_geometry != EXPECTED_INVALID_GEOMETRY_COUNT):
        raise ValueError("source_geometry_population_drift")
    record_sha = _list_sha(features)
    geometry_sha = _list_sha(geometry_records)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    _validate_value_column(features, "gaz_featureclass", EXPECTED_FEATURE_CLASS_VALUES,
                           "source_feature_class_value_drift")
    _validate_value_column(features, "fcode", EXPECTED_FCODE_VALUES,
                           "source_fcode_value_drift")
    _validate_value_column(features, "isunknowncoords", EXPECTED_UNKNOWN_COORDS_VALUES,
                           "source_unknown_coords_value_drift")
    gaz_ids = [row["attributes"].get("gaz_id") for row in features]
    if sum(1 for value in gaz_ids if value is None) != EXPECTED_GAZ_ID_NULL_COUNT:
        raise ValueError("source_gaz_id_null_drift")
    objectid_1s = [row["attributes"].get("objectid_1") for row in features]
    if sum(1 for value in objectid_1s if value is None) != EXPECTED_OBJECTID_1_NULL_COUNT:
        raise ValueError("source_objectid_1_null_drift")
    state_alphas = [row["attributes"].get("state_alpha") for row in features]
    if sum(1 for value in state_alphas if value is None) != EXPECTED_STATE_ALPHA_NULL_COUNT:
        raise ValueError("source_state_alpha_null_count_drift")
    county_names = [row["attributes"].get("county_name") for row in features]
    if sum(1 for value in county_names if value is None) != EXPECTED_COUNTY_NAME_NULL_COUNT:
        raise ValueError("source_county_name_null_count_drift")
    row_identities = [(row["attributes"].get("objectid_1"),
                       row["attributes"].get("state_alpha"),
                       row["attributes"].get("county_name")) for row in features]
    if len(row_identities) != len(set(row_identities)):
        raise ValueError("source_row_identity_duplicate_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": _sha(_canonical(layer_fields)),
             "source_crs_wkid": SOURCE_CRS_WKID,
             "source_geometry_type": "esriGeometryMultipoint",
             "null_geometry_count": null_geometry,
             "invalid_geometry_count": invalid_geometry,
             "observed_bounds": bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_object_id_count": len(ids),
             "unique_gaz_id_count": len({value for value in gaz_ids if value is not None}),
             "unique_objectid_1_count": len({value for value in objectid_1s
                                             if value is not None}),
             "unique_gaz_name_count": len({row["attributes"].get("gaz_name") for row in features}),
             "state_alpha_null_count": sum(1 for value in state_alphas if value is None),
             "county_name_null_count": sum(1 for value in county_names if value is None),
             "source_page_count": len(pages), "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The National Map Gazetteer service publishes no per-snapshot release date "
                 "for this layer; the service copyrightText refresh statement (October 2026) "
                 "is retained as separate freshness evidence, not as an accepted release "
                 "date."}
    return layer, layer_fields, {"features": features, "audit": audit, "ids": ids}


def _validate_gnis_documentation(page: object) -> None:
    text = page.decode("utf-8", "replace") if isinstance(page, bytes) else str(page or "")
    if ("Geographic Names Information System" not in text
            or SERVICE_HOST + "/arcgis/rest/services/geonames/MapServer" not in text):
        raise ValueError("usgs_gnis_documentation_link_drift")


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise ValueError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise ValueError("usgs_gnis_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name"), entry.get("type"),
               entry.get("parentLayerId")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if roster != EXPECTED_SERVICE_LAYERS:
        raise ValueError("usgs_gnis_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if (not description.startswith(EXPECTED_SERVICE_DESCRIPTION_PREFIX)
            or "Geographic Names Information System (GNIS)" not in description):
        raise ValueError("usgs_gnis_service_description_drift:" + label)
    _validate_spatial_reference(service.get("spatialReference"),
                                "source_service_spatial_reference:" + label)


def _validate_layer_document(layer: object) -> list[dict[str, object]]:
    if not isinstance(layer, dict):
        raise ValueError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryMultipoint"
            or layer.get("displayField") != "gaz_name"):
        raise ValueError("exact_gnis_streams_mouth_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") not in (None, False):
        raise ValueError("source_layer_z_or_m_contract_drift")
    capabilities = layer.get("advancedQueryCapabilities")
    if not (isinstance(capabilities, dict) and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True):
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
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
    attribute_fields = [field for field in fields if field["type"] != "esriFieldTypeGeometry"]
    if ([f.get("name") for f in attribute_fields] != EXPECTED_FIELD_NAMES
            or [f.get("type") for f in attribute_fields] != EXPECTED_FIELD_TYPES):
        raise ValueError("source_schema_roster_drift")
    return fields


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGUID": pa.string(),
            "esriFieldTypeGlobalID": pa.string()}[str(field["type"])]


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    attribute_fields = [field for field in fields if field["type"] != "esriFieldTypeGeometry"]
    columns = {}
    schema_fields = []
    for field in attribute_fields:
        name = field["name"]
        typ = _arrow_type(field)
        values = [row["attributes"].get(name) for row in features]
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(name, typ, nullable=True))
    columns["geometry"] = pa.array([
        None if row.get("geometry") is None
        else MultiPoint([tuple(point) for point in row["geometry"]["points"]]).wkb
        for row in features
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["MultiPoint Z"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata(
        {b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


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


def _capture_pages(client: httpx.Client, capture_root: Path,
                   total: int) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Capture the complete layer as ordered, adaptively bisected roster pages.

    Pages start at PAGE_START_RECORDS. A page the producer keeps refusing with
    an error document or keeps dropping mid-stream is bisected until the
    producer serves both halves; the refused page's final body and receipts stay
    on disk as evidence under its own name. A single-record page that still
    fails ends the capture explicitly.
    """
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    pages: deque[tuple[int, int]] = deque(
        (offset, min(PAGE_START_RECORDS, total - offset))
        for offset in range(0, total, PAGE_START_RECORDS)
    )
    while pages:
        offset, count = pages.popleft()
        name = _page_name(offset, count)
        try:
            body, receipt = _capture_with_retry(
                client, _page_query(offset, count), capture_root, name,
                max_bytes=MAX_CAPTURE_BYTES,
                attempts=PAGE_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
                base_delay=PAGE_PROBE_BASE_DELAY_SECONDS if count > 1 else DOC_CAPTURE_BASE_DELAY_SECONDS)
        except ValueError as exc:
            if count == 1 or not str(exc).startswith("incomplete_source_capture:"):
                raise
            middle = count // 2
            pages.appendleft((offset + middle, count - middle))
            pages.appendleft((offset, middle))
            continue
        if _producer_error_doc(body):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            pages.appendleft((offset + middle, count - middle))
            pages.appendleft((offset, middle))
            continue
        captures[name] = (body, receipt)
    return captures


def capture_gnis_streams_mouth(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved GNIS Streams (Mouth) candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path(
        "/tmp/hifld-native-reconstruction/242/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": USER_AGENT}) as client, \
                httpx.Client(follow_redirects=True, timeout=180,
                             headers={"User-Agent": DOC_USER_AGENT}) as doc_client:
            doc_body, doc_receipt = _capture_with_retry(
                doc_client, GNIS_DOC_URL, capture_root, "usgs_gnis_doc_page",
                max_bytes=MAX_CAPTURE_BYTES, attempts=DOC_CAPTURE_ATTEMPTS,
                base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            captures["usgs_gnis_doc_page"] = (doc_body, doc_receipt)
            captured_total = doc_receipt["bytes"] + doc_receipt["wire_bytes"]
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            captured_total += seed_receipt["bytes"] + seed_receipt["wire_bytes"]
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name,
                                                    max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
                captured_total += receipt["bytes"] + receipt["wire_bytes"]
                if captured_total > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
            layer_doc = json.loads(captures["layer_before"][0])
            if layer_doc.get("maxRecordCount") != 2000 or PAGE_START_RECORDS > layer_doc["maxRecordCount"]:
                raise ValueError("source_record_limit_drift")
            captures.update(_capture_pages(client, capture_root, EXPECTED_COUNT))
            captured_total += sum(receipt["bytes"] + receipt["wire_bytes"]
                                  for _, receipt in _page_receipts(captures))
            if captured_total > MAX_CAPTURE_TOTAL_BYTES:
                raise ValueError("source_capture_exceeds_scratch_budget")
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name,
                                                    max_bytes=MAX_CAPTURE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
                captured_total += receipt["bytes"] + receipt["wire_bytes"]
                if captured_total > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
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
                retained[name] = ((capture_root / f"{name}.body").read_bytes(),
                                  json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    service_doc = json.loads(captures["service_before"][0])
    layer_doc = json.loads(captures["layer_before"][0])
    vintage_evidence = {
        "service_copyright_text": service_doc.get("copyrightText"),
        "service_description": service_doc.get("serviceDescription"),
        "usgs_gnis_page_delegation": (
            "USGS's own Geographic Names Information System documentation page links this "
            "exact carto.nationalmap.gov geonames MapServer URL and is captured as dated "
            "documentation evidence."),
        "retired_alias_host_probe": (
            "The former services.nationalmap.gov geonames host no longer resolves "
            "(observed 2026-11-13); carto.nationalmap.gov is the producer's only "
            "publication host for this service."),
        "release_date_selected": None,
        "freshness_accepted": False,
        "assessment": (
            "The service copyrightText states a refresh (October 2026) and USGS documents "
            "quarterly Gazetteer service updates; neither is treated as an independently "
            "verified source release date."),
    }
    manifest = {
        "schema_version": 1, "approved": False, "title": f"{LAYER_NAME} (USGS GNIS Gazetteer)",
        "description": "U.S. Geological Survey Geographic Names Information System (GNIS) "
                       "gazetteer 'Streams (Mouth)' layer (hydrographic Stream/Canal mouth "
                       "points), captured completely from the producer's own The National Map "
                       "geonames MapServer publication named by the DHS HIFLD Open crosswalk. "
                       "Complete source attributes and native web-Mercator (EPSG:3857) "
                       "multipoint-Z geometry are represented in the candidate. Source release "
                       "date/freshness, rights, public schema/ID contract, and release "
                       "acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": "https://www.usa.gov/publicdomain/label/1.0/",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "source_vintage_evidence": vintage_evidence,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": (GNIS_DOC_URL if name == "usgs_gnis_doc_page" else LAYER_URL),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The National Map Gazetteer service carries no "
                                                   "per-snapshot release date; the copyrightText "
                                                   "refresh statement is retained as separate "
                                                   "vintage evidence.",
                    "license": "https://www.usa.gov/publicdomain/label/1.0/",
                    "attribution": "U.S. Geological Survey, The National Map Gazetteer "
                                   "(Geographic Names Information System); item metadata states "
                                   "U.S. public domain; rights review remains pending."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all nine source attribute fields "
                                                             "retained in producer order and "
                                                             "physical types (OBJECTID, gaz_id, "
                                                             "gaz_name, gaz_featureclass, fcode, "
                                                             "state_alpha, county_name, "
                                                             "objectid_1, isunknowncoords)",
                                            "source_ids": "Preserve producer OBJECTID, gaz_id "
                                                          "and objectid_1 values unchanged; "
                                                          "gaz_id/objectid_1 repeat across the "
                                                          "per-county rows of one gazetteer "
                                                          "feature and each (objectid_1, "
                                                          "state_alpha, county_name) row "
                                                          "identity is unique; no substitute "
                                                          "IDs, casts, trimming, or joins",
                                            "geometry": "Native web-Mercator (EPSG:3857) "
                                                        "multipoint [x, y, z] triples retained "
                                                        "verbatim as MultiPoint Z WKB without "
                                                        "reprojection or repair; the producer's "
                                                        "hasZ declaration is honored and every "
                                                        "observed z equals 0",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending freshness, schema/ID "
                                                                "contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1) consolidates the "
                   "inventory's National Map Gazetteer group (including nmg/streams-mouth-1) "
                   "under 'Geographic Names Information System (GNIS) Feature Layers' and names "
                   + SERVICE_URL + " as the open hosting location; the "
                   "producer's own GNIS documentation page (" + GNIS_DOC_URL + ") links this "
                   "exact service URL and is captured as dated evidence. USGS develops the GNIS "
                   "gazetteer as the Federal standard (ANSI INCITS 446-2008) and publishes it "
                   "through The National Map on its own ArcGIS Server (no ArcGIS Online "
                   "serviceItemId); the retired services.nationalmap.gov host no longer "
                   "resolves. The inventory's DataLumos project 240202 archive copy and "
                   "modification date are identification and comparison context only, never "
                   "pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and "
                      "web-Mercator multipoint-Z geometry retained in the producer CRS; all "
                      "release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "service_copyright_text": service_doc.get("copyrightText"),
             "source_relationship": "USGS GNIS gazetteer Stream/Canal mouth layer, The National "
                                    "Map geonames MapServer publication",
             "source_vintage_evidence": vintage_evidence, "release_date_unknown": True,
             "freshness_accepted": False, "schema_contract_accepted": False,
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
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT,
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_invalid_geometry_count": audit["invalid_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
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
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}


def _page_receipts(captures: dict[str, tuple[bytes, dict[str, object]]]):
    for name, (_, receipt) in sorted(captures.items()):
        if name.startswith("page_"):
            yield name, receipt
