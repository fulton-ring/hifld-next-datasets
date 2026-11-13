"""Lossless, approval-gated capture of the USGS NHD "Flowline - Small Scale
(HI, PR, VI, Pacific Territories)" polyline layer.

The U.S. Geological Survey publishes the National Hydrography Dataset (NHD)
from The National Map as the ArcGIS REST map service
``https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer`` (endpoint
documented by USGS in "NHD and WBD Map Services Moving to the Cloud",
https://www.usgs.gov/news/technical-announcement/nhd-and-wbd-map-services-moving-cloud).
USGS retired the NHD on 2023-10-01 and stopped stewardship edits; the successor
is the 3D Hydrography Program (3DHP).  The agency still republishes the final
NHD as maintained reference services and monthly-regenerated national downloads
(the service states "Data Refreshed October, 2026"), and the crosswalk-directed
3DHP_all service publishes HydroLocation/Flowline/Waterbody/Catchment layers
only, with no "small scale" (1:100,000-scale medium-resolution NHDPlus product)
variant and no HI/PR/VI/Pacific-territories variant, so the retired-but-
published NHD service is the exact-matching agency publication of this layer.
Acceptance of a retired dataset's reference publication, and any future
successor migration, remain maintainer decisions.

That service's layer 5 "Flowline - Small Scale (HI, PR, VI, Pacific
Territories)" is the producer's own publication of the HIFLD Open inventory
dataset ``nhd/flowline-small-scale-hi-pr-vi-pacific-territories-2`` (DataLumos
project 240276 is the historical archive copy, identification context only):
the HIFLD Open NHD collection carried this service's layer names verbatim, and
this layer carries the flowline features the NHDPlus-based small-scale product
ships for Hawaii, Puerto Rico, the U.S. Virgin Islands and the Pacific
territories; unlike the conterminous-United States small-scale layers, its
served rows carry "High" RESOLUTION (the territories NHDPlus component is built
on the high-resolution NHD), and every served polyline vertex is a plain
2-value [x, y] Web Mercator ordinate even though the layer metadata declares
Z/M-enabled geometry.  The conterminous-United States sibling is service layer
4 ("Flowline - Small Scale").

The producer's MapServer intermittently answers valid requests with complete
HTTP 200 Esri error documents (transient mode, observed on sibling captures
2026-11-12) and can refuse responses that approach its transfer cap.  Like the
sibling polygon adapters, this layer is captured as adaptively bisected
segments of the sorted OBJECTID roster: each accepted segment must return
exactly its expected id slice, the accepted segments must tile the complete
roster without gaps or overlaps, and the before/after count/id/metadata
brackets plus full-record digests prove that nothing was skipped, duplicated,
or reordered.  Pins below are the observed 2026-11-13 complete-layer population
contract; any drift fails the capture loudly and the attempt is quarantined,
never silently accepted.
"""
from __future__ import annotations

import hashlib
import json
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
import shapely
from shapely.geometry import LineString, MultiLineString

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

DATASET_SLUG = "nhd-flowline-small-scale-territories"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer"
SERVICE_DOC_URL = ("https://www.usgs.gov/news/technical-announcement/"
                   "nhd-and-wbd-map-services-moving-cloud")
PRODUCT_DOC_URL = "https://www.usgs.gov/national-hydrography/access-national-hydrography-products"
LAYER_ID = 5
LAYER_NAME = "Flowline - Small Scale (HI, PR, VI, Pacific Territories)"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Geological Survey"
# The service's native spatial reference (Web Mercator); queries intentionally
# request no outSR so the served path coordinates are the producer's own.
SOURCE_CRS_WKID = 102100
SOURCE_CRS_LATEST_WKID = 3857
_WEB_MERCATOR_LIMIT = 20037508.342789244
# Initial segment width.  The producer's declared maxRecordCount is 2000 and it
# served the full 2000-record native-SR segments during the observed
# 2026-11-13 capture; a segment the producer keeps refusing is bisected until
# it serves both halves, so any transfer-cap regression is handled adaptively.
SEGMENT_START_RECORDS = 2000
MAX_RESPONSE_BYTES = 256_000_000
MIN_FREE_BYTES = 4_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 3
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
PRODUCER_USER_AGENT = "hifld-next-datasets/nhd-flowline-small-scale-territories-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36")
# Pins below are the observed 2026-11-13 complete-layer population contract,
# established by one dated complete capture (17 segments of 2000 records plus a
# final 1273-record segment tiling the sorted OBJECTID roster 1..33273 exactly).
EXPECTED_COUNT = 33273
EXPECTED_ID_SHA256 = "PENDING_FULL_CAPTURE"
EXPECTED_SCHEMA_SHA256 = "PENDING_FULL_CAPTURE"
EXPECTED_RECORD_SHA256 = "PENDING_FULL_CAPTURE"
EXPECTED_GEOMETRY_SHA256 = "PENDING_FULL_CAPTURE"
EXPECTED_OBSERVED_BOUNDS = None
EXPECTED_LAYER_EXTENT = {"xmin": -19018321.582, "ymin": -1617126.2184000015,
                         "xmax": 16233793.5737, "ymax": 2539487.972099999}
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_NAN_GEOMETRY_COUNT = None
# The producer's served rows that cannot be represented as a faithful
# LineString/MultiLineString (null geometry, string-"NaN" ordinates, or
# structurally degenerate paths) are retained verbatim with null candidate
# geometry and gated here; the population contract is derived from the dated
# complete capture, never silently widened.
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "Point", "Feature Layer"), (1, "Point Event", "Feature Layer"),
    (2, "Line - Large Scale ", "Feature Layer"), (3, "Flow Direction", "Feature Layer"),
    (4, "Flowline - Small Scale", "Feature Layer"),
    (5, "Flowline - Small Scale (HI, PR, VI, Pacific Territories)", "Feature Layer"),
    (6, "Flowline - Large Scale", "Feature Layer"), (7, "Area - Small Scale", "Feature Layer"),
    (8, "Area - Small Scale (HI, PR, VI, Pacific Territories)", "Feature Layer"),
    (9, "Area - Large Scale", "Feature Layer"), (10, "Waterbody - Small Scale", "Feature Layer"),
    (11, "Waterbody - Small Scale (HI, PR, VI, Pacific Territories)", "Feature Layer"),
    (12, "Waterbody - Large Scale", "Feature Layer"),
]
EXPECTED_RESOLUTION_VALUES = ["High"]
EXPECTED_FLOWDIR_VALUES = ["WithDigitized"]
EXPECTED_FTYPE_VALUES = ["ArtificialPath", "CanalDitch", "Coastline", "Connector", "Pipeline",
                         "StreamRiver"]
EXPECTED_FCODE_VALUES = [33400, 33600, 33601, 33603, 42800, 42801, 42803, 42807, 42809, 42813,
                         42816, 46000, 46003, 46006, 46007, 55800, 56600]
EXPECTED_GNIS_ID_LENGTHS = None
EXPECTED_COMID_DUPLICATE_COUNT = 0
EXPECTED_PERMANENT_DUPLICATE_COUNT = 10


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``."""
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _doc_only_urls() -> list[tuple[str, str]]:
    return [
        ("service_doc", SERVICE_DOC_URL),
        ("product_doc", PRODUCT_DOC_URL),
    ]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    if value.get("wkid") != SOURCE_CRS_WKID or value.get("latestWkid") != SOURCE_CRS_LATEST_WKID:
        raise ValueError(label + "_crs_drift")


def _collect_segment(offset: int, count: int, page: object, ids: list[int],
                     features: list[dict[str, object]]) -> None:
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    _validate_spatial_reference(page.get("spatialReference"), f"source_page_{offset:06d}")
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    expected = ids[offset:offset + count]
    actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
    if actual != expected or len(rows) != len(expected):
        raise ValueError("source_page_id_slice_incomplete_or_reordered")
    if page.get("exceededTransferLimit") is True and offset + count >= EXPECTED_COUNT:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    features.extend(rows)


def _usable_paths(geom: object) -> list | None:
    """Return the raw paths when every vertex is a usable numeric coordinate.

    Returns ``None`` when the geometry exists but contains the producer's
    string-"NaN" ordinates (retained verbatim in the record/raw evidence and
    gated as unusable geometry, never silently coerced to a location) or a
    structurally degenerate path.  The producer declares Z/M-enabled layer
    metadata, but its served ESRI JSON vertices are plain 2-value [x, y]
    Web Mercator ordinates (observed 2026-11-13); any served Z or M ordinate
    is rejected rather than truncated.
    """
    if not isinstance(geom, dict) or set(geom) != {"paths"} or not isinstance(geom["paths"], list) \
            or not geom["paths"]:
        return None
    usable = True
    for path in geom["paths"]:
        if not isinstance(path, list) or len(path) < 2:
            return None
        for vertex in path:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                usable = False
    return geom["paths"] if usable else None


def _classify_geometries(features: list[dict[str, object]], attr_names: list[str]):
    null_geometry = 0
    nan_geometry = 0
    geometry_records = []
    xmin = ymin = xmax = ymax = None
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        if geom is None:
            null_geometry += 1
            geometry_records.append([attrs["OBJECTID"], None])
            continue
        paths = _usable_paths(geom)
        if paths is None:
            raw = geom.get("paths") if isinstance(geom, dict) else None
            if isinstance(geom, dict) and set(geom) == {"paths"} and isinstance(raw, list) and raw \
                    and any(isinstance(vertex, list) and len(vertex) == 2 and vertex[0] == "NaN"
                            and vertex[1] == "NaN"
                            for path in raw for vertex in path):
                nan_geometry += 1
                geometry_records.append([attrs["OBJECTID"], raw])
                continue
            raise ValueError("source_polyline_geometry_invalid")
        for path in paths:
            for vertex in path:
                x, y = vertex
                if not (-_WEB_MERCATOR_LIMIT <= x <= _WEB_MERCATOR_LIMIT
                        and -_WEB_MERCATOR_LIMIT <= y <= _WEB_MERCATOR_LIMIT):
                    raise ValueError("source_coordinate_range_drift")
                if xmin is None or x < xmin:
                    xmin = x
                if xmax is None or x > xmax:
                    xmax = x
                if ymin is None or y < ymin:
                    ymin = y
                if ymax is None or y > ymax:
                    ymax = y
        geometry_records.append([attrs["OBJECTID"], paths])
    observed_bounds = None if xmin is None else {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
    return geometry_records, null_geometry, nan_geometry, observed_bounds


def _line_wkb(geom: object) -> bytes | None:
    """WKB for a producer polyline geometry, or ``None`` when it is unusable.

    ``None`` covers null geometry, string-"NaN" ordinates, and structurally
    degenerate paths; the raw paths remain in the record digest and audit
    either way, and such records are gated with null candidate geometry, never
    repaired or coerced.  Each producer path becomes one LineString part in
    producer order; a multi-path feature becomes a MultiLineString.
    """
    paths = _usable_paths(geom)
    if paths is None:
        return None
    lines = [LineString([(vertex[0], vertex[1]) for vertex in path]) for path in paths]
    if len(lines) > 1:
        return MultiLineString(lines).wkb
    return lines[0].wkb


def _validate_value_contracts(features: list[dict[str, object]]) -> dict[str, object]:
    """Fail closed on producer value drift in the small enumerations and IDs."""
    resolutions = sorted({row["attributes"]["Resolution"] for row in features})
    flowdirs = sorted({row["attributes"]["FlowDir"] for row in features})
    ftypes = sorted({row["attributes"]["FTYPE"] for row in features})
    fcodes = sorted({row["attributes"]["FCode"] for row in features})
    gnis_lengths = sorted({len(row["attributes"]["GNIS_ID"]) if row["attributes"]["GNIS_ID"] is not None else -1
                           for row in features})
    comids = [row["attributes"]["ComID"] for row in features]
    comid_duplicates = len(comids) - len(set(comids))
    permanents = [row["attributes"]["Permanent_"] for row in features]
    permanent_duplicates = len(permanents) - len(set(permanents))
    if EXPECTED_RESOLUTION_VALUES is not None and resolutions != EXPECTED_RESOLUTION_VALUES:
        raise ValueError("source_resolution_values_drift")
    if EXPECTED_FLOWDIR_VALUES is not None and flowdirs != EXPECTED_FLOWDIR_VALUES:
        raise ValueError("source_flowdir_values_drift")
    if EXPECTED_FTYPE_VALUES is not None and ftypes != EXPECTED_FTYPE_VALUES:
        raise ValueError("source_ftype_values_drift")
    if EXPECTED_FCODE_VALUES is not None and fcodes != EXPECTED_FCODE_VALUES:
        raise ValueError("source_fcode_values_drift")
    if EXPECTED_GNIS_ID_LENGTHS is not None and gnis_lengths != EXPECTED_GNIS_ID_LENGTHS:
        raise ValueError("source_gnis_id_lengths_drift")
    if EXPECTED_COMID_DUPLICATE_COUNT is not None and comid_duplicates != EXPECTED_COMID_DUPLICATE_COUNT:
        raise ValueError("source_comid_uniqueness_drift")
    if EXPECTED_PERMANENT_DUPLICATE_COUNT is not None \
            and permanent_duplicates != EXPECTED_PERMANENT_DUPLICATE_COUNT:
        raise ValueError("source_permanent_identifier_uniqueness_drift")
    return {"resolution_values": resolutions, "flowdir_values": flowdirs, "ftype_values": ftypes,
            "fcode_values": fcodes, "gnis_id_lengths": gnis_lengths,
            "comid_duplicate_count": comid_duplicates,
            "permanent_identifier_duplicate_count": permanent_duplicates}


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if not name.endswith("_doc") and not name.startswith("page_")}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    segments = [_parse_segment_name(name) for name in page_names]
    position = 0
    for offset, count in segments:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {"service_doc", "product_doc", *page_names}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if SERVICE_URL.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("usgs_service_documentation_link_drift")
    if b"retired" not in captures["product_doc"][0].lower():
        raise ValueError("usgs_product_retirement_documentation_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "nhd":
        raise ValueError("official_usgs_nhd_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("usgs_nhd_service_layer_roster_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer"
               for x in service_layers):
        raise ValueError("exact_usgs_nhd_flowline_small_scale_territories_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolyline"):
        raise ValueError("exact_usgs_nhd_flowline_small_scale_territories_layer_metadata_drift")
    # The producer declares Z/M-enabled layer metadata for this polyline layer,
    # but serves plain 2-value [x, y] ESRI JSON vertices (observed 2026-11-13);
    # _usable_paths rejects any served Z or M ordinate, so the declared
    # Z/M metadata is pinned here and the served ordinates are verified below.
    if layer.get("hasZ") is not True or layer.get("hasM") is not True:
        raise ValueError("source_z_or_m_layer_metadata_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
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
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        # The producer regenerates the layer's display-only renderer
        # ("drawingInfo", including random color-ramp colors) on every request
        # (observed on sibling captures 2026-11-12); it is display styling, not
        # data, and is excluded from the metadata stability comparison.  Both
        # raw copies are retained verbatim as evidence either way.
        if {k: v for k, v in docs[name].items() if k != "drawingInfo"} \
                != {k: v for k, v in layer.items() if k != "drawingInfo"}:
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")

    features: list[dict[str, object]] = []
    for (offset, count), name in zip(segments, page_names):
        _collect_segment(offset, count, json.loads(captures[name][0]), ids, features)
    attr_names = [field["name"] for field in fields if field["name"] != "Shape"]
    geometry_records, null_geometry, nan_geometry, bounds = _classify_geometries(features, attr_names)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _list_sha(features)
    geometry_sha = _list_sha(geometry_records)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    if EXPECTED_NULL_GEOMETRY_COUNT is not None and null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_or_nan_geometry_count_drift")
    if EXPECTED_NAN_GEOMETRY_COUNT is not None and nan_geometry != EXPECTED_NAN_GEOMETRY_COUNT:
        raise ValueError("source_null_or_nan_geometry_count_drift")
    value_contracts = _validate_value_contracts(features)
    path_counts = {}
    for record in geometry_records:
        paths = record[1]
        if isinstance(paths, list):
            path_counts[len(paths)] = path_counts.get(len(paths), 0) + 1
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": SOURCE_CRS_LATEST_WKID,
             "geometry_type": "LineString", "null_geometry_count": null_geometry,
             "nan_string_geometry_count": nan_geometry,
             "feature_path_counts": path_counts,
             "observed_bounds": bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids), "source_segment_count": len(segments),
             **value_contracts,
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "USGS retired the NHD on 2023-10-01 and republishes the final dataset as a reference "
                 "service; the service copyright states only a refresh month (\"Data Refreshed "
                 "October, 2026\"), which is a service refresh date, not a per-feature release date, "
                 "and the territories medium-scale flowline component has been static since NHD "
                 "retirement."}
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeInteger": pa.int64(),
            "esriFieldTypeSmallInteger": pa.int32(),
            "esriFieldTypeSingle": pa.float32(),
            # ESRI JSON serializes geodatabase date fields as epoch-millisecond
            # integers; the delivered integers are retained unchanged rather
            # than being reinterpreted or reformatted.
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeDouble": pa.float64()}[str(field["type"])]


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "Shape" else _arrow_type(field)
        if name == "Shape":
            values = [_line_wkb(row.get("geometry")) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        else:
            values = [row["attributes"].get(name) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        columns[name] = pa.array(values, type=typ)
    geo = {"version": "1.1.0", "primary_column": "Shape", "columns": {"Shape": {
        "encoding": "WKB", "geometry_types": ["LineString", "MultiLineString"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_LATEST_WKID).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)}))
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


def _capture_segments(client: httpx.Client, capture_root: Path,
                      total: int) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Capture the complete layer as adaptively bisected roster segments.

    Segments start at SEGMENT_START_RECORDS.  A segment the producer keeps
    refusing with an error document (its response would exceed the producer's
    transfer cap) is bisected until the producer serves both halves; the
    refused segment's final body and receipts stay on disk as evidence under
    its own name.  A single-record segment that still fails ends the capture
    explicitly.
    """
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    segments: deque[tuple[int, int]] = deque(
        (offset, min(SEGMENT_START_RECORDS, total - offset))
        for offset in range(0, total, SEGMENT_START_RECORDS)
    )
    while segments:
        offset, count = segments.popleft()
        name = _segment_name(offset, count)
        body, receipt = _capture_with_retry(
            client, _segment_query(offset, count), capture_root, name,
            max_bytes=MAX_RESPONSE_BYTES,
            attempts=SEGMENT_PROBE_ATTEMPTS if count > 1 else DOC_CAPTURE_ATTEMPTS,
            base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS if count > 1 else DOC_CAPTURE_BASE_DELAY_SECONDS)
        if _producer_error_doc(body):
            if count == 1:
                raise ValueError("source_single_feature_unservable")
            middle = count // 2
            segments.appendleft((offset + middle, count - middle))
            segments.appendleft((offset, middle))
            continue
        captures[name] = (body, receipt)
    return captures


def capture_nhd_flowline_small_scale_territories(storage: StagingStorageResource, *, version: str,
                                                 capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NHD small-scale
    territories flowline candidate."""
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/222/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": PRODUCER_USER_AGENT}) as client, \
                httpx.Client(follow_redirects=False, timeout=180,
                             headers={"User-Agent": DOC_USER_AGENT}) as doc_client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_RESPONSE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _doc_only_urls():
                body, receipt = _capture_with_retry(doc_client, url, capture_root, name,
                                                    max_bytes=MAX_RESPONSE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_RESPONSE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
            captures.update(_capture_segments(client, capture_root, EXPECTED_COUNT))
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(client, url, capture_root, name, max_bytes=MAX_RESPONSE_BYTES,
                                                    attempts=DOC_CAPTURE_ATTEMPTS,
                                                    base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
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
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained, type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Flowline - Small Scale (HI, PR, VI, Pacific Territories) (USGS NHD)",
        "description": "U.S. Geological Survey National Hydrography Dataset map service layer 5 "
                       "'Flowline - Small Scale (HI, PR, VI, Pacific Territories)' (the flowline "
                       "component the NHDPlus-based small-scale product ships for Hawaii, Puerto Rico, "
                       "the U.S. Virgin Islands and the Pacific territories, served with 'High' "
                       "RESOLUTION and NHDPlus-style COMID identifiers), captured completely from the "
                       "producer's own REST publication. Complete source attributes and served Web "
                       "Mercator (EPSG:3857) polyline geometry are represented in the candidate. USGS "
                       "retired the NHD on 2023-10-01 and republishes the final dataset as a maintained "
                       "reference service; the successor 3DHP service publishes no equivalent small-scale "
                       "or territories-variant product. Freshness of a retired dataset, rights, public "
                       "schema/ID contract, and release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": (SERVICE_DOC_URL if name == "service_doc"
                                      else PRODUCT_DOC_URL if name == "product_doc" else LAYER_URL),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "USGS retired the NHD on 2023-10-01; the service and its "
                                                   "documentation publish no per-snapshot release date and "
                                                   "the medium-scale territories component is static.",
                    "license": "The service states 'All data are open and non-proprietary' (public domain "
                               "USGS publication); formal rights review pending.",
                    "attribution": "U.S. Geological Survey National Hydrography Dataset; the service requests "
                                   "acknowledgment of the U.S. Geological Survey for products derived from "
                                   "these data."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's FDATE "
                                                             "epoch-millisecond integers and the served "
                                                             "Shape_Length double; Shape is the original "
                                                             "geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, Permanent_, ComID, "
                                                          "ReachCode, GNIS_ID and GNIS_Name values unchanged, "
                                                          "including the producer's null GNIS_ID values and any "
                                                          "leading-zero string IDs; no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source Web Mercator (EPSG:3857, service wkid 102100) "
                                                        "polyline paths retained as WKB without projection, "
                                                        "reprojection, densification or repair; each producer "
                                                        "path becomes one LineString part in producer order and "
                                                        "multi-path features become MultiLineString; the "
                                                        "producer declares Z/M-enabled layer metadata but serves "
                                                        "plain 2-value [x, y] ordinates, and any served Z or M "
                                                        "ordinate is rejected; null, string-'NaN', or structurally "
                                                        "degenerate paths are retained as records with null "
                                                        "candidate geometry, raw evidence and audit object IDs "
                                                        "intact",
                                            "candidate_status": "unapproved; publication blocked pending "
                                                                "freshness, schema/ID contract, attribution and "
                                                                "release acceptance"}}],
        "lineage": "USGS publishes the National Hydrography Dataset from The National Map as the ArcGIS REST "
                   "map service at https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer; USGS's own "
                   "technical announcement 'NHD and WBD Map Services Moving to the Cloud' documents that exact "
                   "endpoint, and its HTML is captured as 'service_doc' evidence. This candidate is exactly "
                   "layer 5 'Flowline - Small Scale (HI, PR, VI, Pacific Territories)' of that producer "
                   "service, the layer whose name and content match the HIFLD Open inventory dataset "
                   "nhd/flowline-small-scale-hi-pr-vi-pacific-territories-2 (the HIFLD Open NHD collection "
                   "carried the service's layer names verbatim; DataLumos project 240276 is the archive copy, "
                   "identification context only). USGS retired the NHD on 2023-10-01 and moved production to "
                   "the 3D Hydrography Program; the USGS page 'Access National Hydrography Products' (captured "
                   "as 'product_doc') states the retired NHD remains available as reference downloads and "
                   "services, the NHD national download is regenerated monthly and the service data is "
                   "refreshed quarterly, and the DHS HIFLD deprecation crosswalk (public copy sha256 "
                   "7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc, row '3D Hydrography "
                   "Program (3DHP) Layers') routes the hydrography collection to the 3DHP_all service, which "
                   "publishes HydroLocation/Flowline/Waterbody/Catchment layers only - no 1:100,000-scale "
                   "small-scale product and no HI/PR/VI/Pacific-territories variant - so 3DHP cannot currently "
                   "recreate this layer.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
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
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
                "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
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
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}
