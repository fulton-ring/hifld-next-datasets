"""Lossless, approval-gated capture of the USGS 3DHP_all Waterbody areal hydrography.

The HIFLD Open inventory entry ``nhd/area-large-scale-2`` ("Area - Large Scale",
agency US Geological Survey) was the National Hydrography Dataset (NHD)
high-resolution areal hydrography layer (legacy feature class ``NHDArea``). USGS
retired the NHD as a maintained product on 2023-10-01 and the inventory
description itself says "Please use 3DHP for updated data"; the DHS HIFLD Open
crosswalk (DOI 10.3886/E241367V1) consolidates every NHD-family layer into one
"3D Hydrography Program (3DHP) Layers" row whose Open REST Service is the USGS
``3dhp.nationalmap.gov`` ``usgs_3dhp_all`` FeatureServer. That producer service
is the maintained successor captured here; the DataLumos project 239953 archive
copy is identification and comparison context only and is never fetched.

Successor scope per the USGS NHD->3DHP crosswalk: legacy NHDArea water-extent
content maps into the 3DHP Waterbody feature class as featuretype River (legacy
StreamRiver 460), Canal (legacy CanalDitch 336) and Ocean or Great Lake (legacy
SeaOcean 445); the legacy Lake features (NHDWaterbody 390/436/493) belong to the
waterbody datasets, so this capture takes Waterbody rows with ``featuretype<>3``
(not Lake). Other legacy NHDArea landmark subtypes (bridge, dam/weir, lock
chamber, rapids, wash, levee, ...) have no polygon counterpart in the 3DHP
crosswalk and are therefore not reconstructible from this successor; that
coverage change is recorded in the manifest lineage and remains an unaccepted
maintainer decision. The 3DHP schema (12 fields, non-persistent base-36
``id3dhp``) replaces the legacy NHDArea schema wholesale; publication stays
blocked pending the schema/ID contract, coverage, rights and release decisions.
"""
from __future__ import annotations


import hashlib
import json
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.http_capture import CaptureDocuments
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources import nhd_capture, nhd_transport
from dagster_hifld.sources.nhd_transport import failed_receipt_error_code as _failed_receipt_error_code
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources.arcgis_layer import (
    producer_error_doc as _producer_error_doc, preserve_failed_attempt as _preserve_failed_attempt,
)
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable, write_immutable_file,
    seal_candidate_objects, write_candidate_outputs,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "nhd-area-large-scale-2"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 60
LAYER_NAME = "Waterbody"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-area-large-scale-2-draft"
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
POPULATION_WHERE = "featuretype<>3"
# Initial window size over the captured OBJECTID roster; oversized or failing
# windows are bisected down to single OBJECTIDs deterministically (each captured
# page is named by the roster's first OBJECTID it requests).
PAGE_IDS = 500
MAX_BODY_BYTES = 96 * 1024 * 1024
# The 3DHP non-Lake waterbody population is unusually heavy per feature (the
# roster's main-stem polygons carry tens of thousands of Z vertices each);
# the budget counts both wire and decoded bytes under the producer's identity
# encoding, so it is sized for the observed full-population envelope with
# headroom rather than a generic page-domain budget.
MAX_CAPTURE_BYTES = 30 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 25_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Producer-stated service refresh ("Data refreshed September 4, 2026" in the
# captured service copyrightText) is freshness evidence, not a release date.
EXPECTED_COPYRIGHT_TEXT = ("Credits: USGS TNM / NGTOC – 3D National Hydrographic Program "
                           "(3DHP.) Data refreshed September 4, 2026.")
# Live-verified 2026-11-13 (both USGS hosts agree). The producer reordered the
# Spatial Metadata layers between the 04:06Z and 05:37Z captures of that day
# (90 DEM/92 EDH/94 Elevation -> 90 EDH/92 Elevation/94 DEM) without changing
# the service copyrightText; the roster pin tracks the maintained publication.
EXPECTED_SERVICE_LAYERS = [
    (20, "HydroLocation - Sink, Spring, Waterbody Outlet"),
    (30, "HydroLocation - Headwater, Terminus, Divergence, Confluence, Catchment Outlet"),
    (40, "HydroLocation - External Connection"),
    (50, "Flowline"),
    (60, "Waterbody"),
    (80, "Catchment"),
    (90, "Spatial Metadata - EDH Workunit Area"),
    (92, "Spatial Metadata - Elevation Workunit Area"),
    (94, "Spatial Metadata - DEM LimitationArea"),
]
# The producer's same-org alias publication (hydro.nationalmap.gov 3DHP_all)
# still serves the pre-reorder Spatial Metadata ordering while the main host
# already serves the reordered one (live-verified 2026-11-13, mid-deployment
# metadata propagation); each host's roster is pinned with its own dated
# evidence so identity drift is detected per host, not smeared across them.
EXPECTED_ALIAS_SERVICE_LAYERS = [
    (20, "HydroLocation - Sink, Spring, Waterbody Outlet"),
    (30, "HydroLocation - Headwater, Terminus, Divergence, Confluence, Catchment Outlet"),
    (40, "HydroLocation - External Connection"),
    (50, "Flowline"),
    (60, "Waterbody"),
    (80, "Catchment"),
    (90, "Spatial Metadata - DEM LimitationArea"),
    (92, "Spatial Metadata - EDH Workunit Area"),
    (94, "Spatial Metadata - Elevation Workunit Area"),
]
# Producer-refused records: OBJECTIDs whose geometry the producer's own REST
# publication persistently refuses to serve (HTTP 200 Esri
# ``{"error":{"code":500,"message":"Error performing query operation"}}`` on both
# 3dhp.nationalmap.gov and the same-org alias hydro.nationalmap.gov/3DHP_all,
# single-id and windowed, while the same record's attributes and layer extent
# are served normally and the full roster includes it). Their attributes are
# captured through the producer's working attributes-only channel and staged
# verbatim with explicitly gated null candidate geometry. Which records are
# refused is read from the capture state, not pinned.
# Producer-served features whose rings cannot form a valid OGC polygon are
# retained verbatim and counted as ogc_invalid_geometry_counts by the shared
# GDAL Esri polygon converter (issue #997); structural failures still fail
# closed.
EXPECTED_SCHEMA_SHA256 = "51f11bd107670196d396c0908b3c68a4dce0f37466d3dd179c3df149fd4a04d3"
EXPECTED_FEATURETYPE_LABELS = {1: "River", 2: "Canal", 4: "Ocean or Great Lake"}
EXPECTED_LAYER_EXTENT = {"xmin": -19951755.6817, "ymin": -1622985.3665999994,
                         "xmax": 20021551.747199997, "ymax": 11575301.958199998}


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _window_url(window_ids: list[int]) -> str:
    return _query({"objectIds": ",".join(map(str, window_ids)), "outFields": "*",
                   "returnGeometry": "true", "returnZ": "true", "outSR": "4326",
                   "orderByFields": "OBJECTID ASC", "f": "json"})


def _window_url_ids(url: str) -> list[int]:
    parsed = parse_qs(urlsplit(url).query)
    raw = parsed.get("objectIds")
    if not raw:
        raise ValueError("source_window_request_missing_ids")
    return [int(value) for value in raw[0].split(",")]


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


_ATTEMPT_SUFFIXES = (".body", ".wire", ".receipt.json")


def _capture_doc(client: httpx.Client, url: str, capture_root: Path, name: str,
                 budget: list[int]) -> tuple[bytes, dict[str, object]]:
    return nhd_transport.capture_document(
        client, url, capture_root, name, budget, attempts=DOC_CAPTURE_ATTEMPTS,
        base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS, max_body_bytes=MAX_BODY_BYTES,
        check_budget=_budget_check, retain_budget=_budget_guard, fetch=capture_get,
        sleep=time.sleep, retry_http_errors=False, preserve_attempt=_preserve_failed_attempt)


def _budget_guard(budget: list[int]) -> Callable[[str, bytes], None]:
    return nhd_transport.budget_guard(budget, MAX_CAPTURE_BYTES, ValueError)


def _budget_check(budget: list[int]) -> None:
    nhd_transport.budget_check(budget, MAX_CAPTURE_BYTES, ValueError)


def _validate_attrs_types(attrs: Mapping[str, object], fields: list[dict[str, object]]) -> None:
    for field in fields:
        name_attr, kind = field["name"], field["type"]
        value = attrs.get(name_attr)
        if name_attr == "OBJECTID":
            continue
        if value is None:
            continue
        if kind == "esriFieldTypeString":
            if not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + name_attr)
            if name_attr == "id3dhp" and (not value or any(
                    character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    for character in value)):
                raise ValueError("source_id3dhp_value_drift")
        elif kind in ("esriFieldTypeOID", "esriFieldTypeInteger",
                      "esriFieldTypeSmallInteger", "esriFieldTypeDate"):
            if type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + name_attr)
        elif kind == "esriFieldTypeDouble":
            if type(value) not in (int, float) or not (value == value
                                                       and -1e308 < value < 1e308):
                raise ValueError("source_double_value_type_drift:" + name_attr)


def _capture_windows(client: httpx.Client, roster: list[int], capture_root: Path,
                     budget: list[int]) -> tuple[dict[str, tuple[bytes, dict[str, object]]],
                                                 dict[str, list[int]]]:
    return nhd_capture.capture_id_windows(
        client, roster, capture_root, budget, page_size=PAGE_IDS,
        attempts=PAGE_CAPTURE_ATTEMPTS, base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS,
        max_body_bytes=MAX_BODY_BYTES, window_url=_window_url,
        budget_check=_budget_check, budget_guard=_budget_guard,
        failed_receipt_error_code=_failed_receipt_error_code,
        preserve_failed_attempt=_preserve_failed_attempt,
        producer_error_doc=_producer_error_doc, fetch=capture_get, sleep=time.sleep)


def _validate_polygon_geometry(geometry: object) -> None:
    """Structural contract for one served polygon geometry (no assembly).

    Esri JSON rings are explicitly closed Z triples in the source lon/lat
    domain; ordinate shape, type, finiteness and coordinate range are checked
    here so structurally invalid source geometry refuses the capture with
    stable codes. Ring orientation and hole assignment are GDAL's (shared
    native Esri polygon conversion); OGC-invalid-but-representable producer
    polygons are retained with a counted diagnostic instead of being gated.
    """
    if not isinstance(geometry, dict) or "rings" not in geometry \
            or not isinstance(geometry["rings"], list) or not geometry["rings"]:
        raise ValueError("source_polygon_geometry_invalid")
    if set(geometry) - {"hasZ", "rings"}:
        raise ValueError("unexpected_source_geometry_members")
    if geometry.get("hasZ") is not True:
        raise ValueError("source_geometry_z_flag_missing")
    for ring in geometry["rings"]:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError("source_polygon_ring_invalid")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 3:
                raise ValueError("source_polygon_ordinate_contract_drift")
            x, y, z = vertex
            if not (type(x) in (int, float) and type(y) in (int, float) and type(z) in (int, float)):
                raise ValueError("source_polygon_ordinate_not_numeric")
            if not (all(value == value and -1e308 < value < 1e308 for value in (x, y, z))):
                raise ValueError("source_polygon_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise ValueError("source_coordinate_range_drift")
        if ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_not_closed")


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGlobalID": pa.string()}.get(str(field["type"]), pa.string())


GEOMETRY_COLUMN = "geometry"


def _parquet_bytes(fields: list[dict[str, object]], attributes: list[dict[str, object]],
                   wkbs: list[bytes | None]) -> bytes:
    schema_fields = [pa.field(field["name"], _arrow_type(field), nullable=True) for field in fields]
    schema_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
    arrays = [pa.array([row[field["name"]] for row in attributes], type=schema_fields[index].type)
              for index, field in enumerate(fields)]
    arrays.append(pa.array(wkbs, type=pa.binary()))
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
           "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                         "geometry_types": ["Polygon Z", "MultiPolygon Z"],
                                         "crs": crs, "edges": "planar"}}}
    table = pa.Table.from_arrays(arrays, names=[field.name for field in schema_fields])
    table = table.replace_schema_metadata({b"geo": _canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


# ArcGIS serves display-only rendering hints (for example
# drawingInfo.authoringInfo.colorRamp) nondeterministically from the same
# layer within minutes of each other, so the before/after witness comparison
# covers only the data-bearing contract fields; each is pinned individually
# against the approved baseline above.
LAYER_CONTRACT_KEYS = ("id", "name", "type", "geometryType", "hasZ", "hasM",
                       "capabilities", "advancedQueryCapabilities",
                       "sourceSpatialReference", "extent", "fields", "maxRecordCount")


def _layer_contract_view(document: dict) -> dict:
    return {key: document.get(key) for key in LAYER_CONTRACT_KEYS}


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    """Validate the complete capture against capture-time completeness witnesses."""
    doc_names = set(captures)
    refused_attr_names = {name for name in doc_names if name.startswith("refused_attrs_")}
    required = {"service_doc", "alias_service", "service_before", "service_after",
                "layer_before", "layer_after", "count_before", "count_after",
                "ids_seed", "ids_before", "ids_after"}
    page_names = [name for name in doc_names if name.startswith("page_")]
    if not required <= doc_names or not page_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    refused_oids = []
    for name in refused_attr_names:
        try:
            oid = int(name.removeprefix("refused_attrs_"))
        except ValueError as exc:
            raise ValueError("source_refused_ids_drift") from exc
        if oid <= 0 or name != f"refused_attrs_{oid:07d}":
            raise ValueError("source_refused_ids_drift")
        refused_oids.append(oid)
    if len(set(refused_oids)) != len(refused_oids):
        raise ValueError("source_refused_ids_drift")
    unexpected = doc_names - required - set(page_names) - refused_attr_names
    if unexpected:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    docs = CaptureDocuments(captures, (name for name in captures if name != "service_doc"))
    if SERVICE_HOST.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("usgs_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or not isinstance(layer, dict):
        raise ValueError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise ValueError("usgs_3dhp_service_identity_drift")
    service_layers = service.get("layers")
    roster_value = [(entry.get("id"), entry.get("name")) for entry in service_layers
                    if isinstance(entry, dict)] if isinstance(service_layers, list) else None
    if roster_value != EXPECTED_SERVICE_LAYERS:
        raise ValueError("usgs_3dhp_service_layer_roster_drift")
    alias = docs["alias_service"]
    if (not isinstance(alias, dict) or alias.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT
            or [(entry.get("id"), entry.get("name")) for entry in alias.get("layers", [])
                if isinstance(entry, dict)] != EXPECTED_ALIAS_SERVICE_LAYERS):
        raise ValueError("usgs_3dhp_alias_service_identity_drift")
    for name in ("service_before", "service_after", "alias_service"):
        description = str(docs[name].get("serviceDescription", ""))
        if "3DHP_all" not in description or "3D Hydrography Program" not in description:
            raise ValueError("usgs_3dhp_service_description_drift")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_3dhp_waterbody_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise ValueError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2500:
        raise ValueError("source_record_limit_drift")
    _validate_layer_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_layer_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(entry, dict) for entry in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(entry) for entry in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    field_names = [entry["name"] for entry in fields_value]
    for name in ("layer_before", "layer_after"):
        if _layer_contract_view(docs[name]) != _layer_contract_view(layer):
            raise ValueError("source_layer_metadata_changed_during_capture")
    count_before = docs["count_before"].get("count")
    if type(count_before) is not int or count_before <= 0:
        raise ValueError("source_count_drift")
    count_after = docs["count_after"].get("count")
    if count_after != count_before:
        raise ValueError("source_count_changed_during_capture")
    ids = validate_capture_witness(count_before, docs["ids_before"].get("objectIds"),
                                   count_after, docs["ids_after"].get("objectIds"))
    seed = docs["ids_seed"].get("objectIds")
    if (not isinstance(seed, list) or len(seed) != count_before
            or any(type(value) is not int or value <= 0 for value in seed)
            or len(set(seed)) != count_before or sorted(seed) != ids):
        raise ValueError("source_id_population_drift")
    if not set(refused_oids) <= set(ids):
        raise ValueError("source_refused_ids_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")

    page_entries = []
    for name in page_names:
        start = int(name.removeprefix("page_"))
        if not 0 <= start <= 9_999_999:
            raise ValueError("source_page_name_invalid:" + name)
        page_entries.append((start, name))
    page_entries.sort()
    attributes: list[dict[str, object]] = []
    wkbs: list[bytes | None] = []
    geometry_records: list[list] = []
    null_geometry = 0
    observed_bounds: list[float] = []
    distribution: dict[int, int] = {}
    ogc_invalid_geometry: dict[str, int] = {}
    ogc_invalid_sample_objectids: list[int] = []
    id3dhp_seen: set[str] = set()
    ring_count = 0
    vertex_count = 0
    captured_ids: list[int] = []
    for _start, name in page_entries:
        body, receipt = captures[name]
        page = docs[name]
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error:" + name)
        _validate_page_spatial_reference(page.get("spatialReference"), f"source_page_{name}")
        if page.get("exceededTransferLimit"):
            raise ValueError("source_page_transfer_truncated:" + name)
        if page.get("hasZ") is not True:
            raise ValueError("source_page_z_flag_missing:" + name)
        requested = _window_url_ids(receipt["url"])
        rows = page.get("features")
        if not isinstance(rows, list) or not rows:
            raise ValueError("source_page_missing_features:" + name)
        page_ids = []
        page_geometry_records = []
        for row in rows:
            attrs = row.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != field_names:
                raise ValueError("source_record_attribute_schema_drift:" + name)
            oid = attrs["OBJECTID"]
            if type(oid) is not int or oid <= 0:
                raise ValueError("source_native_identifier_missing_or_type_drift:" + name)
            featuretype = attrs["featuretype"]
            if featuretype not in EXPECTED_FEATURETYPE_LABELS:
                raise ValueError("source_featuretype_outside_area_population:" + name)
            if attrs["featuretypelabel"] != EXPECTED_FEATURETYPE_LABELS[featuretype]:
                raise ValueError("source_featuretype_label_drift:" + name)
            _validate_attrs_types(attrs, fields)
            geometry = row.get("geometry")
            if geometry is None:
                null_geometry += 1
                page_geometry_records.append([oid, None])
                wkbs.append(None)
                captured_ids.append(oid)
                attributes.append(attrs)
                id3dhp_seen.add(attrs["id3dhp"])
                continue
            _validate_polygon_geometry(geometry)
            # Ring orientation and hole assignment are GDAL's through the
            # shared native Esri polygon conversion; a conversion failure
            # refuses the capture, and OGC invalidity is a producer defect
            # retained verbatim with a counted reason, never repaired or
            # coerced to null geometry.
            wkb = to_wkb(geometry, "esriGeometryPolygon", has_z=True, has_m=False)
            reason = shapely.is_valid_reason(shapely.from_wkb(wkb))
            if reason != "Valid Geometry":
                ogc_invalid_geometry[reason] = ogc_invalid_geometry.get(reason, 0) + 1
                if len(ogc_invalid_sample_objectids) < 20:
                    ogc_invalid_sample_objectids.append(oid)
            rings = geometry["rings"]
            ring_count += len(rings)
            vertex_count += sum(len(ring) for ring in rings)
            page_geometry_records.append([oid, rings])
            for ring in rings:
                for vertex in ring:
                    observed_bounds.extend((vertex[0], vertex[1]))
            wkbs.append(wkb)
            captured_ids.append(oid)
            attributes.append(attrs)
            distribution[attrs["featuretype"]] = distribution.get(attrs["featuretype"], 0) + 1
            id3dhp_seen.add(attrs["id3dhp"])
        page_ids = [row["attributes"]["OBJECTID"] for row in rows]
        if page_ids != sorted(page_ids):
            raise ValueError("source_page_ids_not_ascending:" + name)
        if sorted(page_ids) != sorted(requested):
            raise ValueError("source_page_id_slice_incomplete_or_reordered:" + name)
        geometry_records.extend(page_geometry_records)
    # Producer-refused records: their attributes were captured through the
    # working attributes-only channel and are appended in ascending OBJECTID
    # order with explicitly gated null candidate geometry. The refused set is
    # whatever this capture actually recorded, not a pinned roster; nothing is
    # substituted or fabricated for them.
    refused_oids.sort()
    for oid in refused_oids:
        attrs_name = f"refused_attrs_{oid:07d}"
        refused_rows = docs[attrs_name].get("features")
        if (not isinstance(refused_rows, list) or len(refused_rows) != 1
                or not isinstance(refused_rows[0], dict)
                or not isinstance(refused_rows[0].get("attributes"), dict)
                or refused_rows[0].get("geometry") is not None):
            raise ValueError("source_refused_attrs_record_drift:" + attrs_name)
        attrs = refused_rows[0]["attributes"]
        if list(attrs) != field_names or attrs.get("OBJECTID") != oid:
            raise ValueError("source_refused_attrs_record_drift:" + attrs_name)
        _validate_attrs_types(attrs, fields)
        wkbs.append(None)
        captured_ids.append(oid)
        attributes.append(attrs)
        null_geometry += 1
        id3dhp_seen.add(attrs["id3dhp"])
    if refused_oids:
        geometry_records.extend([oid, None] for oid in refused_oids)
    if len(captured_ids) != count_before:
        raise ValueError("source_feature_population_incomplete")
    validate_capture_witness(count_before, docs["ids_before"].get("objectIds"),
                             count_after, docs["ids_after"].get("objectIds"),
                             list(captured_ids))
    bounds = None
    if observed_bounds:
        bounds = {"xmin": min(observed_bounds[0::2]), "ymin": min(observed_bounds[1::2]),
                  "xmax": max(observed_bounds[0::2]), "ymax": max(observed_bounds[1::2])}
    record_sha = _sha(_canonical(attributes))
    # Boundary-independent geometry digest: one canonical stream over the whole
    # record sequence (served pages in ascending roster order, refused records
    # appended in ascending OBJECTID order). Hashing per-page chunks would make
    # the digest depend on the capture's bisection page boundaries, which are
    # completeness-neutral and may legitimately differ between captures.
    geometry_hash = hashlib.sha256()
    geometry_hash.update(b"[")
    for index, record in enumerate(geometry_records):
        if index:
            geometry_hash.update(b",")
        geometry_hash.update(_canonical(record))
    geometry_hash.update(b"]")
    geometry_sha = geometry_hash.hexdigest()
    audit = {"source_feature_count": len(attributes), "source_reported_count": count_before,
             "source_id_sha256": _sha(_canonical(captured_ids)),
             "source_record_sha256": record_sha,
             "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha,
             "source_featuretype_distribution": distribution,
             "source_crs_epsg": SOURCE_CRS_WKID, "source_layer_native_sr": NATIVE_LAYER_SR,
             "geometry_type": "Polygon Z / MultiPolygon Z", "null_geometry_count": null_geometry,
             "unusable_geometry_feature_counts": {},
             "ogc_invalid_geometry_counts": dict(sorted(ogc_invalid_geometry.items())),
             "ogc_invalid_geometry_sample_objectids": ogc_invalid_sample_objectids,
             "observed_bounds": bounds, "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(set(captured_ids)),
             "id3dhp_distinct_count": len(id3dhp_seen),
             "id3dhp_duplicate_count": len(attributes) - len(id3dhp_seen),
             "source_ring_count": ring_count, "source_vertex_count": vertex_count,
             "source_refused_ids": refused_oids,
             "source_refused_count": len(refused_oids),
             "producer_refusal_note":
                 "Records listed in source_refused_ids are part of the captured population "
                 "(they appear in the id roster and count) but the producer's own REST "
                 "publication persistently refuses to serve their geometry (HTTP 200 Esri "
                 "error document code 500 'Error performing query operation' on both "
                 "3dhp.nationalmap.gov and the same-org alias hydro.nationalmap.gov/3DHP_all, "
                 "single-id and windowed, retried with backoff; the producer's actual "
                 "refusal responses are staged verbatim as raw attempt evidence). "
                 "Their attributes are served normally through the "
                 "attributes-only channel and are staged verbatim with explicitly gated null "
                 "candidate geometry; publication stays blocked pending the maintainer "
                 "completeness/coverage decision for these records.",
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The 3DHP_all service publishes no per-snapshot release date; the service "
                 "copyrightText states a service refresh (September 4, 2026), the per-feature "
                 "featuredate is the 3DHP core database load date, and neither is a dataset "
                 "release date. Quarterly service updates and annual staged products are "
                 "documented by USGS.",
             "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT}
    return fields, {"attributes": attributes, "wkbs": wkbs, "ids": captured_ids,
                    "null_geometry_count": null_geometry, "audit": audit}


def _capture_refused_attrs(client: httpx.Client, capture_root: Path, budget: list[int],
                           captures, refused: Mapping[str, list[int]]) -> None:
    nhd_transport.capture_refused_attributes(
        client, capture_root, budget, captures, refused, capture_doc=_capture_doc,
        name_for_id=lambda oid: f"refused_attrs_{oid:07d}",
        attributes_url=lambda oid: _query({"objectIds": str(oid), "outFields": "*",
                                           "returnGeometry": "false", "f": "json"}),
        error_type=ValueError)


def capture_nhd_area_large_scale(storage: StagingStorageResource, *, version: str,
                                 capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved 3DHP non-Lake waterbody candidate."""
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
    capture_root = ((Path(capture_directory) if capture_directory
                     else default_capture_dir(DATASET_SLUG, "captures"))
                    / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures = RetainedCaptureView(capture_root)
    budget = [0]
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(300, connect=30),
                          headers={"User-Agent": USER_AGENT}) as client:
            doc_urls = [
                ("service_doc", SERVICE_DOC_URL),
                ("alias_service", ALIAS_SERVICE_URL + "?f=pjson"),
                ("service_before", SERVICE_URL + "?f=pjson"),
                ("layer_before", LAYER_URL + "?f=pjson"),
                ("count_before", _query({"where": POPULATION_WHERE, "returnCountOnly": "true", "f": "json"})),
                ("ids_before", _query({"where": POPULATION_WHERE, "returnIdsOnly": "true", "f": "json"})),
            ]
            nhd_capture.capture_id_window_layer(
                client, capture_root, budget, captures, document_urls=doc_urls,
                ids_url=_query({"where": POPULATION_WHERE, "returnIdsOnly": "true", "f": "json"}),
                capture_doc=_capture_doc, capture_windows=_capture_windows,
                capture_refused_attrs=_capture_refused_attrs, error_type=ValueError)
        fields, extracted = _validate_captures(captures)
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
        retained = RetainedCaptureView(capture_root)
        for name, receipt in captures.receipts.items():
            path = capture_root / f"{name}.body"
            if path.is_file():
                retained[name] = (path, receipt)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                body_path = capture_root / f"{name}.body"
                with body_path.open("rb") as stream:
                    stream.read(0)
                retained[name] = (body_path, json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          type(exc).__name__)
        raise
    audit = extracted["audit"]
    # Producer-refusal and producer-fault evidence: every .attempt-NN body/wire/
    # receipt retained by the capture flow is staged verbatim into the
    # candidate's raw evidence — never as accepted feature pages — so a sealed
    # candidate that claims a persistent producer refusal also carries the
    # producer's actual refusal responses. Attempt receipts are
    # listed in the capture manifest but stay out of the bounded
    # source-manifest input roster.
    attempt_evidence: dict[str, Path] = {}
    attempt_receipts: dict[str, dict[str, object]] = {}
    attempt_names = sorted({path.name.removesuffix(suffix) for suffix in _ATTEMPT_SUFFIXES
                            for path in capture_root.glob(f"*.attempt-*{suffix}")})
    for name in attempt_names:
        for suffix in _ATTEMPT_SUFFIXES:
            path = capture_root / f"{name}{suffix}"
            if not path.is_file():
                raise ValueError("source_attempt_evidence_incomplete:" + name)
            attempt_evidence[f"raw/{name}{suffix}"] = path
            if suffix == ".receipt.json":
                try:
                    receipt = json.loads(path.read_bytes())
                except ValueError as exc:
                    raise ValueError("source_attempt_evidence_incomplete:" + name) from exc
                if not isinstance(receipt, dict) or not isinstance(receipt.get("url"), str):
                    raise ValueError("source_attempt_evidence_incomplete:" + name)
                attempt_receipts[name] = receipt
    identity = processing_identity(Path(__file__).parents[3])
    parquet_body = _parquet_bytes(fields, extracted["attributes"], extracted["wkbs"])
    del extracted
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    # The catalog reads source_manifest.json under a bounded 2 MiB layer limit,
    # so the per-page feature-query requests are represented by one aggregate
    # input whose digest covers their complete receipt roster; every per-page
    # receipt is still staged verbatim under raw/<name>.receipt.json next to
    # its body and wire transcript, keeping the full request-level provenance
    # machine-readable without bloating the manifest layer.
    _unknown_release_reason = ("The 3DHP_all service publishes no per-snapshot "
                               "release date; its copyrightText records a "
                               "service refresh (September 4, 2026) and USGS "
                               "documents quarterly service updates and "
                               "annual staged products.")
    _input_license = ("Public domain (U.S. Government work) per the producer service; "
                      "rights acceptance pending.")
    _input_attribution = ("USGS 3D Hydrography Program (3DHP_all), The National Map; "
                          "formal attribution terms require maintainer review.")

    def _input_entry(name, receipt):
        return {"input_id": name, "producer": PUBLISHER,
                "host": urlsplit(receipt["url"]).hostname,
                "role": "producer_publication",
                "canonical_url": SERVICE_URL,
                "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                "release_date": None,
                "release_date_unknown_reason": _unknown_release_reason,
                "license": _input_license, "attribution": _input_attribution}

    doc_captures = [(n, r) for n, (_, r) in ((name, captures[name]) for name in sorted(captures)) if not n.startswith("page_")]
    page_captures = [(n, r) for n, (_, r) in ((name, captures[name]) for name in sorted(captures)) if n.startswith("page_")]
    if not page_captures:
        raise ValueError("paged_capture_inputs_missing")
    if {r["status"] for _, r in page_captures} != {200}:
        raise ValueError("paged_capture_http_status_drift")
    page_roster_sha = _sha(_canonical([r for _, r in page_captures]))
    page_entry = {"input_id": "3dhp_paged_feature_queries", "producer": PUBLISHER,
                  "host": urlsplit(page_captures[0][1]["url"]).hostname,
                  "role": "producer_publication",
                  "canonical_url": SERVICE_URL,
                  "retrieval_url": page_captures[0][1]["url"],
                  "observed_final_url": page_captures[0][1]["final_url"],
                  "retrieved_at": page_captures[0][1]["observed_at"],
                  "http_status": 200,
                  "bytes": sum(int(r["bytes"]) for _, r in page_captures),
                  "sha256": page_roster_sha,
                  "page_query_count": len(page_captures),
                  "page_capture_window": [min(r["observed_at"] for _, r in page_captures),
                                          max(r["observed_at"] for _, r in page_captures)],
                  "page_receipts_sha256_basis": "SHA-256 over the canonical JSON array of the "
                                                "per-page receipt objects in ascending page name "
                                                "order, staged verbatim as raw/<name>.receipt.json "
                                                "next to each page's raw body and wire transcript",
                  "release_date": None,
                  "release_date_unknown_reason": _unknown_release_reason,
                  "license": _input_license, "attribution": _input_attribution}
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Area - Large Scale (USGS 3DHP Waterbody, non-Lake areal hydrography)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 60 "
                       "'Waterbody', rows with featuretype<>3 (River, Canal, Ocean or Great "
                       "Lake), captured completely from the producer's own REST publication as "
                       "the maintained successor of the retired NHD high-resolution NHDArea "
                       "layer (HIFLD inventory nhd/area-large-scale-2). Complete source "
                       "attributes and Z-aware EPSG:4326 polygon geometry are represented in "
                       "the candidate, except the producer-refused records "
                       "(source_refused_ids in the audit): their attributes are captured "
                       "verbatim through the producer's working attributes-only channel and "
                       "their candidate geometry is explicitly gated to null because the "
                       "producer itself refuses to serve it. The 3DHP schema replaces the "
                       "legacy NHDArea schema; succession, freshness, rights, public "
                       "schema/ID contract, coverage and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no access or "
                   "use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [_input_entry(name, receipt) for name, receipt in doc_captures] + [page_entry],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": POPULATION_WHERE + " over the captured "
                                                          "complete OBJECTID roster (legacy "
                                                          "NHDArea water-extent successor scope; "
                                                          "Lake rows belong to the waterbody "
                                                          "datasets)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types, including the service-generated "
                                                             "Shape__Length and Shape__Area doubles",
                                            "source_ids": "Preserve producer OBJECTID and id3dhp "
                                                          "values unchanged (id3dhp is the producer's "
                                                          "base-36 identifier, documented as not "
                                                          "persistent); no substitute IDs, casts or "
                                                          "joins",
                                            "geometry": "Source EPSG:4326 polygon rings with Z "
                                                        "retained as WKB without projection under "
                                                        "the producer's verified Esri ring semantics "
                                                        "(closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes); no vertex "
                                                        "dropped, added or reordered; null source "
                                                        "geometry retained as null; "
                                                        "producer-refused records keep their "
                                                        "captured attributes with explicitly "
                                                        "gated null candidate geometry and the "
                                                        "producer's actual refusal responses "
                                                        "are staged verbatim as raw attempt "
                                                        "evidence; producer-served features whose rings "
                                                        "cannot form a valid OGC polygon are "
                                                        "retained verbatim in the digests and "
                                                        "candidate geometry with counted "
                                                        "diagnostics",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/area-large-scale-2 ('Area - Large Scale', "
                   "agency US Geological Survey, last HIFLD refresh January 2024) carried the "
                   "legacy NHD high-resolution NHDArea layer and its description already said "
                   "'Please use 3DHP for updated data'. USGS retired the NHD as a maintained "
                   "product on 2023-10-01 (usgs.gov National Hydrography Dataset page) and the "
                   "DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, consolidated row '3D "
                   "Hydrography Program (3DHP) Layers', Open ID 650c07e542fa43b6a327c7f683681712) "
                   "directs all NHD-family layers to the 3dhp.nationalmap.gov usgs_3dhp_all "
                   "FeatureServer captured here; the same-org hydro.nationalmap.gov 3DHP_all "
                   "service is captured as the producer's alias publication. Per USGS's own "
                   "NHD->3DHP crosswalk, legacy NHDArea water-extent content (StreamRiver 460, "
                   "CanalDitch 336, SeaOcean 445) maps to 3DHP Waterbody featuretypes River, "
                   "Canal and Ocean or Great Lake, while legacy Lake/Pond/Reservoir content "
                   "(NHDWaterbody) maps to featuretype Lake and is excluded here as belonging "
                   "to the waterbody datasets. Legacy NHDArea landmark subtypes (bridge, "
                   "dam/weir, lock chamber, rapids, wash, levee and others) have no polygon "
                   "counterpart in the 3DHP crosswalk and are not reconstructible from this "
                   "maintained successor; legacy attributes (Permanent_Identifier, ReachCode, "
                   "FType/FCode, Resolution) have no 3DHP equivalents (3DHP id3dhp is "
                   "documented as not persistent). That succession coverage and contract change "
                   "is recorded here and remains an unaccepted maintainer decision. The "
                   "DataLumos project 239953 archive copy (archive modification 2025-07-03) is "
                   "identification and comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and Z-aware "
                      "geometry retained in the producer page CRS; all succession, freshness, "
                      "contract, rights and release approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile "
                                                        "field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized "
                                                  "geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    full_audit = {**audit, "source_fields": fields, "official_layer_name": LAYER_NAME,
                  "layer_id": LAYER_ID, "service_url": SERVICE_URL,
                  "alias_service_url": ALIAS_SERVICE_URL, "service_doc_url": SERVICE_DOC_URL,
                  "crosswalk_doi": "10.3886/E241367V1",
                  "crosswalk_layer_row": "3D Hydrography Program (3DHP) Layers",
                  "crosswalk_open_id": "650c07e542fa43b6a327c7f683681712",
                  "release_date_unknown": True, "freshness_accepted": False,
                  "succession_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "coverage_accepted": False,
                  "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": audit["source_reported_count"],
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "failed_attempt_count": len(attempt_receipts),
                    "failed_attempt_receipts": attempt_receipts,
                    "receipts": {name: receipt for name, (_, receipt) in ((name, captures[name]) for name in sorted(captures))}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(full_audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = capture_root / f"{name}.body"
        outputs[f"raw/{name}.wire"] = capture_root / f"{name}.wire"
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs.update(attempt_evidence)
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "succession_accepted": False,
         "freshness_accepted": False, "schema_contract_accepted": False,
         "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
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
                "feature_count": audit["source_feature_count"],
                "source_reported_count": audit["source_reported_count"],
                "source_population_complete": True,
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_id_sha256": audit["source_id_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                "release_accepted": False}
    descriptors = write_candidate_outputs(
        storage, prefix, outputs, readback_order="after-all",
        error_prefix="candidate_output_physical_readback_failed:")
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_candidate_objects(
        storage, state_key, version,
        payload['processing_identity'], payload['capture_receipts'], payload['objects'],
        source_record_sha256=payload['source_record_sha256'],
        source_schema_sha256=payload['source_schema_sha256'],
        fingerprint=fingerprint)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_reported_count": audit["source_reported_count"],
            "source_population_complete": True,
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}
