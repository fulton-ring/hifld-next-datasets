"""Lossless, approval-gated capture of the FAA AIS Runways layer.

The Federal Aviation Administration (FAA) Air Traffic Organization Mission
Support Services, Aeronautical Information Services (AIS) publishes the
Runways dataset and updates it on the FAA's 56-day (8-week) aeronautical
data cycle: the producer's own ArcGIS item 4d8fa46181aa470d809776c57a8ab1f6
(owner ``AeronauticalInformationServices_FAA`` in the FAA ArcGIS Online
organization ``ssFJjBXIUyZDrSYZ``, "Federal Aviation Administration - AIS";
access public, serviceItemId matching) carries the exact HIFLD Open inventory
abstract for ``runways-1`` ("A runway is a defined rectangular area ... This
geospatial vector file provides information on the airport runway and
helicopter landing areas") with the current cycle statement ("Current
Effective Date: 0901Z 29 Oct 2026 to 0901Z 24 Dec 2026" observed 2026-11-14,
layer ``editingInfo.lastEditDate`` 2026-10-29T12:00:23.783Z). FAA's public
Open Data Hub publishes this same item as ``faa::runways``. This is the
producing agency's own maintained publication, named as the "Open REST
Service" for this layer in DHS's HIFLD Open crosswalk (public copy of DOI
10.3886/E241367V1, row "Runways": Agency Federal Aviation Administration,
Status Deprecate "No longer supported as part of the HIFLD mission", old
HIFLD item 98e35a36d3ce46aca1f759aa1664aa3b retired). The HIFLD Open
inventory lists ``runways-1`` with agency "US Army Corps of Engineers" and a
2024-06-26 issue date while its own description names FAA-AIS as the
publisher; that inventory metadata inconsistency is recorded, and the
crosswalk's FAA agency attribution is accepted as the producer identity.
The DataLumos project 240623 archive copy and the inventory modification
date are identification and comparison context only, never pipeline inputs.

The layer's declared native spatial reference is EPSG:4269 (NAD83) while the
service serves EPSG:4326 (WGS 84) coordinates on request via ``outSR=4326``;
both contracts are pinned. Although the layer advertises ``hasZ: true``, the
producer returns plain 2-component XY rings with no Z/M flags in query
responses, so the captured geometry contract is exactly ``{"rings"}`` with
closed 2-component rings; any dimensional ordinate change fails closed.

The layer is captured as ordered-offset pages (the service reports
``maxRecordCount`` 2000 and advertises pagination plus ordering) of the
sorted OBJECTID roster: every accepted page must return exactly its expected
sorted id slice, the accepted pages must tile the complete roster without
gaps or overlaps, and the before/after count/id/metadata brackets plus
full-record and geometry digests prove that nothing was skipped, duplicated,
or reordered. Schema, IDs, geometry, coordinates and every attribute value
are preserved unchanged, including leading-zero designators such as "05/23"
and the producer's nulls.
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
from shapely.geometry import MultiPolygon, Polygon

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

DATASET_SLUG = "runways-1"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "4d8fa46181aa470d809776c57a8ab1f6"
ITEM_ORG_ID = "ssFJjBXIUyZDrSYZ"
ITEM_OWNER = "AeronauticalInformationServices_FAA"
SERVICE_URL = ("https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/"
               "Runways/FeatureServer")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Runways"
PUBLISHER = ("U.S. Department of Transportation, Federal Aviation Administration (FAA), "
             "Air Traffic Organization, Mission Support Services, Aeronautical "
             "Information Services (AIS)")
AGENCY = "Department of Transportation"
SOURCE_CRS_WKID = 4326
NATIVE_CRS_WKID = 4269
# Initial page width, capped by the producer's maxRecordCount (2000). Any page
# whose response the producer refuses or drops mid-stream is bisected until the
# producer serves both halves; see the module docstring.
PAGE_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 32 * 1024 * 1024
MAX_CAPTURE_TOTAL_BYTES = 128 * 1024 * 1024
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
# Pins below are the observed 2026-11-14 complete-layer population contract,
# established by a dated complete capture (12 accepted pages tiling the sorted
# OBJECTID roster without gaps or overlaps; every accepted page returned
# exactly its expected sorted-OBJECTID slice). The record and geometry digests
# cover every attribute value and every captured ring coordinate in order.
EXPECTED_COUNT = 23389
EXPECTED_ID_SHA256 = "97c1a3a52b6027afed4dcba4acd3348652f88d0194f128dcdf4a0e4dcc456935"
EXPECTED_SCHEMA_SHA256 = "0a16b70f4083707b231f5a9f53dc528b0d891a1529941f52349b1acf982600de"
EXPECTED_RECORD_SHA256 = "ac2fcf739fb660b497a64eb09060a2b4da079faf674558256d1ac5aa1e2bc92b"
EXPECTED_GEOMETRY_SHA256 = "f509e8db426ecb11f8bb72e10c3f6a714584aea1129032377f82fea76aa75ab6"
EXPECTED_OBSERVED_BOUNDS = {"xmin": -177.392680849287, "ymin": -14.3406803431274,
                            "xmax": 174.135163860959, "ymax": 76.5358969831087}
EXPECTED_LAYER_EXTENT = {"xmin": -177.39266815799996, "ymin": -14.340687855999931,
                         "xmax": 174.13517812600003, "ymax": 76.53588492300008}
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_EMPTY_GEOMETRY_COUNT = 0
EXPECTED_INVALID_GEOMETRY_COUNT = 0
EXPECTED_GLOBAL_ID_NULL_COUNT = 0
EXPECTED_GLOBAL_ID_DUPLICATE_COUNT = 0
EXPECTED_FIELD_NAMES = ["OBJECTID", "GLOBAL_ID", "AIRPORT_ID", "DESIGNATOR", "LENGTH",
                        "WIDTH", "DIM_UOM", "COMP_CODE", "LIGHTACTV", "LIGHTINTNS",
                        "AK_LOW", "AK_HIGH", "US_LOW", "US_HIGH", "US_AREA", "PACIFIC",
                        "Shape__Area", "Shape__Length"]
EXPECTED_FIELD_TYPES = ["esriFieldTypeOID", "esriFieldTypeString", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeDouble", "esriFieldTypeDouble",
                        "esriFieldTypeString", "esriFieldTypeString",
                        "esriFieldTypeSmallInteger", "esriFieldTypeString",
                        "esriFieldTypeSmallInteger", "esriFieldTypeSmallInteger",
                        "esriFieldTypeSmallInteger", "esriFieldTypeSmallInteger",
                        "esriFieldTypeSmallInteger", "esriFieldTypeSmallInteger",
                        "esriFieldTypeDouble", "esriFieldTypeDouble"]
EXPECTED_SERVICE_LAYER_ROSTER = [(LAYER_ID, LAYER_NAME, "Feature Layer")]


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
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
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
                   "outSR": "4326", "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
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


def _validate_page_spatial_reference(value: object, label: str) -> None:
    """Captured pages are served at the pinned EPSG:4326 output reference."""
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


def _validate_native_spatial_reference(value: object, label: str) -> None:
    """Layer and extent metadata advertise the producer's native EPSG:4269."""
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != NATIVE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


def _collect_page(offset: int, count: int, page: object, ids: list[int],
                  features: list[dict[str, object]]) -> None:
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    _validate_page_spatial_reference(page.get("spatialReference"), f"source_page_{offset:06d}")
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


def _ring_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0]
    return math.fsum((ring[i][0] - x0) * (ring[i + 1][1] - y0)
                     - (ring[i][1] - y0) * (ring[i + 1][0] - x0)
                     for i in range(len(ring) - 1)) / 2


def _esri_polygon_wkb(rings: list) -> bytes:
    """Encode captured XY rings as a WKB MultiPolygon without repair.

    ArcGIS encodes exterior rings clockwise and interior rings
    counter-clockwise. Holes are assigned to their smallest containing shell
    so nested islands stay distinct; a hole without a containing shell, a
    non-simple ring, or any other structural surprise fails closed.
    """
    shells, holes = [], []
    for ring in rings:
        polygon = Polygon(ring)
        (shells if _ring_area(ring) < 0 else holes).append((ring, polygon))
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list] = {i: [] for i in range(len(shells))}
    for ring, hole in holes:
        parents = [(shell.area, i) for i, (_, shell) in enumerate(shells) if shell.contains(hole)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        assigned[min(parents)[1]].append(ring)
    return MultiPolygon([Polygon(ring, assigned[index])
                         for index, (ring, _polygon) in enumerate(shells)]).wkb


def _classify_ring_geometries(features: list[dict[str, object]], attr_names: list[str]):
    """Polygon geometry audit: [OBJECTID, geometry] records (or [OBJECTID, None]).

    The producer publishes plain 2-component XY rings (no Z/M flags) despite
    the layer advertising ``hasZ``; a geometry that is not exactly ``{"rings"}``
    with closed 2-component numeric rings is counted invalid, ``rings: []`` is
    counted empty, and the pinned counts enforce the observed contract (fail
    closed, never repaired or coerced). Ring coordinates are checked against
    the geographic range and accumulated into the observed bounds.
    """
    null_geometry = 0
    empty_geometry = 0
    invalid_geometry = 0
    vertices = 0
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
        if not isinstance(geom, dict) or set(geom) != {"rings"} \
                or not isinstance(geom["rings"], list):
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        rings = geom["rings"]
        if not rings:
            empty_geometry += 1
            geometry_records.append([oid, geom])
            continue
        rings_ok = True
        for ring in rings:
            if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                    or any(not isinstance(point, list) or len(point) != 2
                           or any(type(value) not in (int, float)
                                  or not math.isfinite(value) for value in point)
                           for point in ring)):
                rings_ok = False
                break
        if not rings_ok:
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        for ring in rings:
            for point in ring:
                x, y = point
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError("source_coordinate_range_drift")
                if xmin is None or x < xmin:
                    xmin = x
                if xmax is None or x > xmax:
                    xmax = x
                if ymin is None or y < ymin:
                    ymin = y
                if ymax is None or y > ymax:
                    ymax = y
                vertices += 1
        try:
            _esri_polygon_wkb(rings)
        except Exception:
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        geometry_records.append([oid, geom])
    observed_bounds = None if xmin is None else {"xmin": xmin, "ymin": ymin,
                                                 "xmax": xmax, "ymax": ymax}
    return geometry_records, null_geometry, empty_geometry, invalid_geometry, vertices, \
        observed_bounds


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if not name.startswith("page_")}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pages = [_parse_page_name(name) for name in page_names]
    position = 0
    for offset, count in pages:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {*page_names}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    portal = docs["portal_item"]
    if (not isinstance(portal, dict) or portal.get("id") != ITEM_ID
            or portal.get("owner") != ITEM_OWNER or portal.get("orgId") != ITEM_ORG_ID
            or portal.get("title") != LAYER_NAME or portal.get("access") != "public"
            or portal.get("type") != "Feature Service" or portal.get("url") != SERVICE_URL):
        raise ValueError("source_portal_item_identity_drift")
    description = str(portal.get("description", ""))
    if ("Current Effective Date" not in description
            or "airport runway and helicopter landing areas" not in description
            or "Runway information is published every eight weeks" not in description
            or "Aeronautical Information Services" not in description):
        raise ValueError("official_FAA_cycle_item_relationship_drift")
    if "The data provided is for public use" not in str(portal.get("licenseInfo", "")):
        raise ValueError("official_FAA_public_use_license_drift")
    if portal.get("accessInformation") != ("Federal Aviation Administration, Air Traffic "
                                           "Organization, Mission Support Services, "
                                           "Aeronautical Information Services."):
        raise ValueError("official_FAA_access_information_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("serviceItemId") != ITEM_ID:
        raise ValueError("official_FAA_runways_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers
              if isinstance(x, dict)] if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("official_FAA_runways_service_identity_drift")
    if (not isinstance(layer, dict) or layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not True
            or layer.get("hasM") is not False):
        raise ValueError("exact_FAA_runways_layer_identity_drift")
    capabilities = layer.get("advancedQueryCapabilities")
    if not (isinstance(capabilities, dict) and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True):
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_native_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_native_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if ([f.get("name") for f in fields] != EXPECTED_FIELD_NAMES
            or [f.get("type") for f in fields] != EXPECTED_FIELD_TYPES):
        raise ValueError("source_schema_roster_drift")
    if docs["layer_after"].get("fields") is None \
            or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer:
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
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
    attr_names = [field["name"] for field in fields]
    geometry_records, null_geometry, empty_geometry, invalid_geometry, vertices, bounds = \
        _classify_ring_geometries(features, attr_names)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    if (null_geometry != EXPECTED_NULL_GEOMETRY_COUNT
            or empty_geometry != EXPECTED_EMPTY_GEOMETRY_COUNT
            or invalid_geometry != EXPECTED_INVALID_GEOMETRY_COUNT):
        raise ValueError("source_geometry_population_drift")
    record_sha = _list_sha(features)
    geometry_sha = _list_sha(geometry_records)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    global_ids = [row["attributes"].get("GLOBAL_ID") for row in features]
    global_id_nulls = sum(1 for value in global_ids if value is None)
    global_id_duplicates = len(global_ids) - len({value for value in global_ids
                                                  if value is not None})
    if global_id_nulls != EXPECTED_GLOBAL_ID_NULL_COUNT:
        raise ValueError("source_feature_id_null_drift")
    if global_id_duplicates != EXPECTED_GLOBAL_ID_DUPLICATE_COUNT:
        raise ValueError("source_feature_id_duplicate_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_wkid": SOURCE_CRS_WKID,
             "source_native_crs_wkid": NATIVE_CRS_WKID,
             "source_geometry_type": "esriGeometryPolygon",
             "null_geometry_count": null_geometry,
             "empty_geometry_count": empty_geometry,
             "invalid_geometry_count": invalid_geometry,
             "source_vertex_count": vertices,
             "observed_bounds": bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_object_id_count": len(ids), "unique_global_id_count":
                 len({value for value in global_ids if value is not None}),
             "global_id_null_count": global_id_nulls,
             "global_id_duplicate_count": global_id_duplicates,
             "source_page_count": len(pages), "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The FAA AIS publication states only its current 8-week aeronautical "
                 "data-cycle effective window in the item description and provides no "
                 "per-snapshot release date; item/layer editing timestamps are retained "
                 "as separate vintage evidence, not as an accepted release date."}
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


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
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = _arrow_type(field)
        values = [row["attributes"].get(name) for row in features]
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(name, typ, nullable=True))
    columns["geometry"] = pa.array([
        None if row.get("geometry") is None or not row["geometry"]["rings"]
        else _esri_polygon_wkb(row["geometry"]["rings"])
        for row in features
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["MultiPolygon"],
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


def capture_runways(storage: StagingStorageResource, *, version: str,
                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved FAA AIS Runways candidate."""
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
        "/tmp/hifld-native-reconstruction/300/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/faa-runways-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            captured_total = seed_receipt["bytes"] + seed_receipt["wire_bytes"]
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
    portal_doc = json.loads(captures["portal_item"][0])
    service_doc = json.loads(captures["service_before"][0])
    layer_doc = json.loads(captures["layer_before"][0])
    vintage_evidence = {
        "portal_description": portal_doc.get("description"),
        "portal_access_information": portal_doc.get("accessInformation"),
        "portal_item_created_epoch_ms": portal_doc.get("created"),
        "portal_item_modified_epoch_ms": portal_doc.get("modified"),
        "service_editing_info": service_doc.get("editingInfo"),
        "layer_editing_info": layer_doc.get("editingInfo"),
        "assessment": (
            "The item description states the current FAA 8-week aeronautical data-cycle "
            "effective window; item modified and layer editingInfo timestamps coincide with "
            "that cycle publication. None of these is treated as an independently verified "
            "source release date; the HIFLD Open inventory archive copy reflects the "
            "2024-05-16..2024-07-11 cycle only as identification context."),
        "release_date_selected": None,
        "freshness_accepted": False,
    }
    manifest = {
        "schema_version": 1, "approved": False, "title": f"{LAYER_NAME} (FAA AIS)",
        "description": "Federal Aviation Administration (FAA) Aeronautical Information "
                       "Services (AIS) Runways layer, captured completely from the "
                       "producer's own ArcGIS publication (item "
                       "4d8fa46181aa470d809776c57a8ab1f6, owner "
                       "AeronauticalInformationServices_FAA in the FAA organization "
                       "ssFJjBXIUyZDrSYZ, published as faa::runways on FAA's Open Data "
                       "Hub). Complete source attributes and EPSG:4326 polygon geometry "
                       "are represented in the candidate. Source release date/freshness, "
                       "rights, public schema/ID contract, and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": None,
        "source_modified": None, "date_issued": None, "date_modified": None,
        "source_vintage_evidence": vintage_evidence,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": (f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
                                      if name == "portal_item" else LAYER_URL),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The FAA AIS publication carries no per-snapshot "
                                                   "release date; the item's 8-week cycle statement and "
                                                   "edit timestamps are retained as separate vintage "
                                                   "evidence.",
                    "license": "Unresolved; the producer item states the data is provided for "
                               "public use without a formal license; rights review pending.",
                    "attribution": "FAA Aeronautical Information Services; rights review "
                                   "remains pending."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all eighteen source fields "
                                                             "retained in producer order and "
                                                             "physical types (OBJECTID, "
                                                             "GLOBAL_ID, AIRPORT_ID, "
                                                             "DESIGNATOR, LENGTH, WIDTH, "
                                                             "DIM_UOM, COMP_CODE, LIGHTACTV, "
                                                             "LIGHTINTNS, AK_LOW, AK_HIGH, "
                                                             "US_LOW, US_HIGH, US_AREA, "
                                                             "PACIFIC, Shape__Area, "
                                                             "Shape__Length)",
                                            "source_ids": "Preserve producer OBJECTID and "
                                                          "GLOBAL_ID values unchanged "
                                                          "(GLOBAL_ID fully populated and "
                                                          "unique in the captured roster); "
                                                          "no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source EPSG:4326 2-component XY "
                                                        "rings retained as WKB MultiPolygon "
                                                        "without reprojection or repair; the "
                                                        "layer advertises hasZ but the "
                                                        "producer serves plain 2D rings; "
                                                        "null/empty source geometry retained "
                                                        "as null",
                                            "candidate_status": "unapproved; publication "
                                                                "blocked pending freshness, "
                                                                "schema/ID contract, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The FAA Air Traffic Organization Mission Support Services Aeronautical "
                   "Information Services publishes the Runways layer and refreshes it every "
                   "8 weeks with each FAA aeronautical data cycle; the producer's own ArcGIS "
                   "item 4d8fa46181aa470d809776c57a8ab1f6 (owner AeronauticalInformationServices_FAA, "
                   "organization ssFJjBXIUyZDrSYZ 'Federal Aviation Administration - AIS', "
                   "public, serviceItemId matching) carries the exact HIFLD Open abstract for "
                   "runways-1 with the current cycle statement and is published as "
                   "faa::runways on FAA's Open Data Hub. DHS's HIFLD Open crosswalk (public "
                   "copy of DOI 10.3886/E241367V1) lists this exact REST URL as the layer's "
                   "'Open REST Service' with Agency 'Federal Aviation Administration' and "
                   "Status 'Deprecate' for the retired HIFLD Open item "
                   "98e35a36d3ce46aca1f759aa1664aa3b. The HIFLD Open inventory row runways-1 "
                   "names agency 'US Army Corps of Engineers' while its own description names "
                   "FAA-AIS as the publisher; the crosswalk FAA attribution is accepted as "
                   "producer identity and the inventory agency field is recorded as an "
                   "inconsistency. The DataLumos project 240623 archive copy is "
                   "identification and comparison context only, never a pipeline input.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and 2D "
                      "polygon geometry retained in the requested CRS; all release and "
                      "compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "arcgis_item_id": ITEM_ID, "arcgis_item_owner": portal_doc.get("owner"),
             "source_relationship": ("FAA AIS producer publication on the FAA 8-week "
                                     "aeronautical data cycle; HIFLD Open layer retired"),
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
                "source_empty_geometry_count": audit["empty_geometry_count"],
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
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_empty_geometry_count": audit["empty_geometry_count"],
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
