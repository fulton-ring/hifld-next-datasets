"""Streaming, approval-gated capture of the USGS 3DHP_all Catchment layer.

The HIFLD Open inventory entry ``nhd/catchment`` ("Catchment", agency US
Geological Survey) carries the description of the USGS 3D Hydrography Program
(3DHP) ``3DHP_all`` ArcGIS REST service verbatim; the DHS HIFLD Open crosswalk
(DOI 10.3886/E241367V1) consolidates the NHD-family rows into one "3D
Hydrography Program (3DHP) Layers" row whose Open REST Service is the USGS
``3dhp.nationalmap.gov`` ``usgs_3dhp_all`` FeatureServer. Its layer 80
"Catchment" is the maintained producer publication captured here (USGS retired
the NHD as a maintained product on 2023-10-01; 3DHP is the successor). The
layer held 300,070 features on 2026-11-13, so this module streams the source
the way the FRS INTERESTS extractor does: every roster segment is retained as
raw evidence in a resumable capture directory before any candidate is sealed.
The DataLumos project 240107 archive copy is identification context only and is
never fetched.

The producer serves the complete OBJECTID roster (``returnIdsOnly`` returned
exactly the contiguous span 1..300070 on 2026-11-13) but refuses some
``maxRecordCount``-sized pages with a complete HTTP 200 Esri error document
while serving smaller slices of the same span, so the layer is captured as
adaptively bisected segments of the sorted roster: each accepted segment must
return exactly its expected id slice, the accepted segments must tile the
complete roster without gaps or overlaps, and before/after count/roster/
service/layer brackets prove that nothing was skipped, duplicated, or
reordered. A single-record segment the producer still refuses ends the capture
explicitly rather than guessing. Every captured query page must carry a
top-level ``hasZ: true`` and must not carry ``hasM: true``; the producer's
responses for this Z-only layer omit the top-level ``hasM`` key entirely
(observed on every captured page) while the layer document declares
``hasM: false``, and M ordinates stay structurally refused by the
three-ordinate vertex contract.

The per-value and per-geometry baseline digests, geometry gate counts and
observed bounds are computed from the retained segments of every capture and
reported in the candidate audit rather than pinned; an upstream edit between
captures changes the reported population and is witnessed by the
approved-baseline drift check, not by a code pin. Publication stays blocked
pending the succession, freshness, schema/ID contract, rights and release
decisions; all runtime approval flags are false and the draft refresh
schedule is stopped.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from array import array
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import pyogrio
from shapely import from_wkb, is_valid_reason

from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources import arcgis_layer, nhd_capture
from dagster_hifld.sources.capture_common import (
    load_checkpoint_receipt_map,
    candidate_prefix,
    capture_get,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    verify_cached_candidate,
    write_immutable,
    reuse_sealed_candidate,
    load_candidate_checkpoint,
    stage_candidate_objects,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import (
    RetainedCaptureViews as _RetainedCaptureViews,
    capture_with_retry as _capture_with_retry,
    preserve_failed_attempt as _preserve_failed_attempt,
    producer_error_doc as _producer_error_doc,
    save_state as _save_state,
    witness_aside as _witness_aside,
)

DATASET_SLUG = "nhd-catchment"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 80
LAYER_NAME = "Catchment"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolygon"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-catchment-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
# The producer's maxRecordCount is 2500, but 2500-record pages were refused
# with complete HTTP 200 Esri error documents at several roster offsets while
# 2000-record slices of the same span were served (observed 2026-11-13), so
# segments start below the cap and refused segments are bisected.
MAX_RECORD_COUNT = 2500
SEGMENT_START_RECORDS = 2000
MAX_BODY_BYTES = 96 * 1024 * 1024
MAX_CAPTURE_BYTES = 40 * 1024 * 1024 * 1024
MIN_FREE_BYTES = 40_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 3
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Producer-stated service refresh ("Data refreshed September 4, 2026" in the
# captured service copyrightText) is freshness evidence, not a release date.
EXPECTED_COPYRIGHT_TEXT = ("Credits: USGS TNM / NGTOC \u2013 3D National Hydrographic Program "
                           "(3DHP.) Data refreshed September 4, 2026.")
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
# The producer's two same-org endpoints (3dhp.nationalmap.gov and its
# hydro.nationalmap.gov alias) were observed on 2026-11-13 carrying the three
# "Spatial Metadata" layer names on permuted ids (90/92/94) while the core
# hydrography roster matched exactly; the pin therefore fixes the core rows and
# only requires the Spatial Metadata names to stay a permutation of these ids.
SPATIAL_METADATA_IDS = frozenset({90, 92, 94})
SPATIAL_METADATA_NAMES = frozenset({"Spatial Metadata - DEM LimitationArea",
                                    "Spatial Metadata - EDH Workunit Area",
                                    "Spatial Metadata - Elevation Workunit Area"})
# Schema contract pin: the 3DHP_all Catchment field roster is the producer's
# current service contract, not a snapshot of the population (the population
# itself is witnessed at capture time, see the module docstring).
EXPECTED_SCHEMA_SHA256 = "1b32b55650559d19c1d0fe2e3c876940d179925bf2d0f3bbe2d2ac262c7a57a5"
EXPECTED_LAYER_EXTENT = {"xmin": -17704943.152400002, "ymin": 4248263.417000003,
                         "xmax": -8080166.934599999, "ymax": 10571108.752799997}
# The producer serves volatile display-only metadata (drawingInfo) in the
# layer document; the before/after witness comparison covers only the
# data-bearing contract fields, each pinned individually above.
LAYER_CONTRACT_KEYS = ("id", "name", "type", "geometryType", "hasZ", "hasM",
                       "capabilities", "advancedQueryCapabilities",
                       "sourceSpatialReference", "extent", "fields", "maxRecordCount")


def _layer_contract_view(document: dict) -> dict:
    return {key: document.get(key) for key in LAYER_CONTRACT_KEYS}

NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("id3dhp", "esriFieldTypeString", True, 7),
    ("featuredate", "esriFieldTypeDate", True, 8),
    ("mainstemid", "esriFieldTypeString", True, 200),
    ("areasqkm", "esriFieldTypeDouble", True, None),
    ("totestdrainareasqkm", "esriFieldTypeDouble", True, None),
    ("workunitid", "esriFieldTypeString", True, 100),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
    ("Shape__Area", "esriFieldTypeDouble", True, None),
)
METADATA_NAMES = ("service_doc", "alias_service", "service_before", "layer_before",
                  "count_before", "ids_seed", "ids_before")
AFTER_NAMES = ("layer_after", "service_after", "count_after", "ids_after")
GEOMETRY_COLUMN = "geometry"

_ARROW_TYPES: dict[str, pa.DataType] = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int32(),
    "esriFieldTypeSmallInteger": pa.int16(),
    "esriFieldTypeSingle": pa.float32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    # Esri JSON delivers date fields as raw epoch milliseconds; the producer's
    # exact numeric wire values are retained verbatim as int64 with no
    # timezone or calendar interpretation applied.
    "esriFieldTypeDate": pa.int64(),
}


class NhdCatchmentError(ValueError):
    """The source cannot be represented as the approved-contract Catchment candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise NhdCatchmentError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _segment_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "returnZ": "true", "returnM": "false", "outSR": str(SOURCE_CRS_WKID),
                   "orderByFields": "OBJECTID ASC", "resultOffset": str(offset),
                   "resultRecordCount": str(count), "f": "json"})


_SEGMENT_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _segment_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_segment_name(name: str) -> tuple[int, int]:
    match = _SEGMENT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise NhdCatchmentError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _validate_roster(roster: object, label: str) -> list[int]:
    """Structural roster contract: unique positive OBJECTIDs (population ties below).

    The population itself is not pinned: the roster's length must equal the
    producer's own count answers, enforced by the capture-time witness.
    """
    if not isinstance(roster, list) or not roster \
            or any(type(value) is not int or value <= 0 for value in roster) \
            or len(set(roster)) != len(roster):
        raise NhdCatchmentError("source_id_population_drift:" + label)
    return sorted(roster)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise NhdCatchmentError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise NhdCatchmentError("usgs_3dhp_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list):
        raise NhdCatchmentError("usgs_3dhp_service_layer_roster_drift:" + label)
    core = len(EXPECTED_SERVICE_LAYERS) - len(SPATIAL_METADATA_IDS)
    if roster[:core] != EXPECTED_SERVICE_LAYERS[:core]:
        raise NhdCatchmentError("usgs_3dhp_service_layer_roster_drift:" + label)
    metadata_rows = roster[core:]
    if (len(metadata_rows) != len(SPATIAL_METADATA_IDS)
            or {entry_id for entry_id, _name in metadata_rows} != set(SPATIAL_METADATA_IDS)
            or {name for _entry_id, name in metadata_rows} != set(SPATIAL_METADATA_NAMES)):
        raise NhdCatchmentError("usgs_3dhp_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "3DHP_all" not in description or "3D Hydrography Program" not in description:
        raise NhdCatchmentError("usgs_3dhp_service_description_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdCatchmentError("source_service_spatial_reference_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise NhdCatchmentError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise NhdCatchmentError("exact_3dhp_catchment_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise NhdCatchmentError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise NhdCatchmentError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise NhdCatchmentError("source_record_limit_drift")
    source_reference = layer.get("sourceSpatialReference")
    if (not isinstance(source_reference, dict)
            or source_reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise NhdCatchmentError("source_layer_spatial_reference_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdCatchmentError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise NhdCatchmentError("source_layer_extent_drift")
    return _profile_schema(layer)


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    try:
        return _ARROW_TYPES[str(field["type"])]
    except KeyError:
        raise NhdCatchmentError("source_field_type_unsupported:" + str(field["type"])) from None


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise NhdCatchmentError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise NhdCatchmentError("source_native_type_changed:" + name)
        if name == "id3dhp" and value and any(
                character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in value):
            raise NhdCatchmentError("source_id3dhp_value_drift")
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise NhdCatchmentError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise NhdCatchmentError("source_native_type_changed:" + name)
        return
    raise NhdCatchmentError("unsupported_source_physical_type:" + esri_type)


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      expected_objectid: int) -> tuple[int, list]:
    """Validate one feature's attributes and geometry structure.

    Returns the OBJECTID and the verbatim rings (or None when the feature has
    no geometry). Ring ordinate values are validated here so that every
    retained record digest is computed from a structurally sound feature; the
    shared GDAL conversion runs in the caller.
    """
    if not isinstance(feature, dict):
        raise NhdCatchmentError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(expected_names):
        raise NhdCatchmentError("source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise NhdCatchmentError("source_object_id_type_changed")
    if objectid != expected_objectid:
        raise NhdCatchmentError("source_page_id_slice_incomplete_or_reordered")
    rings: list | None = None
    if geometry is not None:
        if not isinstance(geometry, dict) or set(geometry) != {"hasZ", "rings"} \
                or geometry.get("hasZ") is not True:
            raise NhdCatchmentError("unexpected_source_geometry_members")
        rings = geometry["rings"]
        if not isinstance(rings, list):
            raise NhdCatchmentError("source_polygon_geometry_invalid")
        for ring in rings:
            if not isinstance(ring, list) or len(ring) < 4:
                raise NhdCatchmentError("source_polygon_ring_too_short")
            for vertex in ring:
                if not isinstance(vertex, list) or len(vertex) != 3:
                    raise NhdCatchmentError("source_polygon_ordinate_contract_drift")
                x, y, z = vertex
                if x == "NaN" and y == "NaN":
                    continue
                if not (type(x) in (int, float) and type(y) in (int, float)
                        and type(z) in (int, float)):
                    raise NhdCatchmentError("source_polygon_ordinate_not_numeric")
                if not all(value == value and -1e308 < value < 1e308 for value in (x, y, z)):
                    raise NhdCatchmentError("source_polygon_ordinate_not_finite")
                if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                    raise NhdCatchmentError("source_coordinate_range_drift")
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    return objectid, rings


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    return arcgis_layer.load_receipt(capture_root, name, NhdCatchmentError)


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    return arcgis_layer.read_capture(capture_root, name, NhdCatchmentError)


def _load_state(capture_root: Path) -> dict[str, object]:
    return arcgis_layer.load_state(capture_root, NhdCatchmentError)


def _retained_or_fetch(client: httpx.Client, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    return nhd_capture.retained_capture(
        client, capture_root, name, url, read_capture=_read_capture,
        fetch=lambda client, url, root, name: capture_get(client, url, root, name, max_bytes=MAX_BODY_BYTES),
        discard=_witness_aside, error_type=NhdCatchmentError)


def _plan_segment_gaps(capture_root: Path, roster: list[int]) -> list[tuple[int, int]]:
    return nhd_capture.plan_segment_gaps(
        capture_root, roster, load_state=_load_state, read_capture=_read_capture,
        witness=_witness_aside, name_pattern=_SEGMENT_NAME_PATTERN,
        parse_name=_parse_segment_name, segment_name=_segment_name,
        producer_error=_producer_error_doc, error_type=NhdCatchmentError,
        chunk_size=SEGMENT_START_RECORDS, strict_resume=False)


def _fetch_segments(client: httpx.Client, capture_root: Path, roster: list[int],
                    state: dict[str, object]) -> bool:
    return nhd_capture.fetch_segments(
        client, capture_root, roster, state, plan_gaps=_plan_segment_gaps,
        segment_name=_segment_name, segment_query=_segment_query,
        capture_retry=_capture_with_retry, witness=_witness_aside,
        producer_error=_producer_error_doc, save_state=_save_state,
        error_type=NhdCatchmentError, probe_attempts=SEGMENT_PROBE_ATTEMPTS,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, probe_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS,
        doc_delay=DOC_CAPTURE_BASE_DELAY_SECONDS, max_body_bytes=MAX_BODY_BYTES,
        strict_resume=False)


def fetch_nhd_catchment_pages(capture_root: Path, *, client: httpx.Client | None = None,
                              doc_client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the approved-contract layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required segment
    and document.
    """
    capture_root = Path(capture_root).absolute()
    segments, count_before, first_captured_at, last_captured_at = nhd_capture.capture_roster_layer(
        capture_root, client=client, doc_client=doc_client,
        default_client=_default_client, document_client=_doc_client,
        load_state=_load_state, save_state=_save_state,
        retained_or_fetch=_retained_or_fetch, read_capture=_read_capture,
        capture_get=capture_get, witness=_witness_aside,
        producer_error=_producer_error_doc, validate_roster=_validate_roster,
        fetch_segments=_fetch_segments, metadata_names=METADATA_NAMES,
        after_names=AFTER_NAMES, urls={"service_doc": SERVICE_DOC_URL, "alias_service": ALIAS_SERVICE_URL + "?f=pjson",
        "service_before": SERVICE_URL + "?f=pjson", "layer_before": LAYER_URL + "?f=pjson",
        "count_before": _count_url(), "ids_seed": _ids_url(), "ids_before": _ids_url(),
        "layer_after": LAYER_URL + "?f=pjson", "service_after": SERVICE_URL + "?f=pjson",
        "count_after": _count_url(), "ids_after": _ids_url()},
        error_type=NhdCatchmentError, max_body_bytes=MAX_BODY_BYTES,
        refresh_witnesses=False)
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": extracted["feature_count"],
        "source_reported_count": count_before,
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_nan_geometry_count": extracted["nan_geometry_count"],
        "source_empty_geometry_count": extracted["empty_geometry_count"],
        "source_unusable_geometry_counts": extracted["unusable_geometry_counts"],
        "source_ogc_invalid_geometry_counts": extracted["ogc_invalid_geometry_counts"],
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "observed_bounds": extracted["observed_bounds"],
        "resumed_segments": len(segments),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    return {"capture_directory": str(capture_root), "segments": len(segments), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


class _SegmentStream:
    """Streaming per-segment validator, digester and candidate parquet writer."""

    def __init__(self, capture_root: Path, fields: list[dict[str, object]],
                 parquet_sink: Path | None):
        self._capture_root = Path(capture_root).absolute()
        self.expected_names = tuple(entry["name"] for entry in fields)
        declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
        if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
            raise NhdCatchmentError("source_schema_drift")
        arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True)
                        for field in fields]
        arrow_fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True))
        crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
        geo = {"version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
               "columns": {GEOMETRY_COLUMN: {"encoding": "WKB",
                                             "geometry_types": ["Polygon Z", "MultiPolygon Z"],
                                             "crs": crs, "edges": "planar"}}}
        self.schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
        self.parquet_sink = parquet_sink
        self.tmp_path = (parquet_sink.with_name(f"{parquet_sink.name}.construction-tmp")
                         if parquet_sink is not None else None)
        if self.tmp_path is not None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        # Never trust an existing output file: reconstruction always streams
        # from the validated segments into a temporary file that atomically
        # replaces the sink only after every check passes, so an interrupted
        # construction can never be sealed.
        self._writer = (pq.ParquetWriter(self.tmp_path, self.schema, compression="zstd",
                                         use_dictionary=False)
                        if self.tmp_path is not None else None)
        self.record_hash = hashlib.sha256()
        self.geometry_hash = hashlib.sha256()
        self.ids: array = array("q")
        self.last_objectid = 0
        self.null_geometry = 0
        self.nan_geometry = 0
        self.empty_geometry = 0
        self.unusable_geometry: dict[str, int] = {}
        self.ogc_invalid_geometry: dict[str, int] = {}
        self.ogc_invalid_geometry_sample_objectids: list[int] = []
        self.ring_count = 0
        self.vertex_count = 0
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page_name: str, offset: int, count: int, roster: list[int]) -> None:
        """Digest one retained roster segment into the digests and optional parquet sink."""
        if self._writer_closed:
            raise NhdCatchmentError("stream_writer_already_closed")
        page = json.loads(_read_capture(self._capture_root, page_name)[0])
        if not isinstance(page, dict) or "error" in page:
            raise NhdCatchmentError("source_page_error:" + page_name)
        reference = page.get("spatialReference")
        if (not isinstance(reference, dict)
                or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                or reference.get("latestWkid") != SOURCE_CRS_WKID):
            raise NhdCatchmentError("source_page_spatial_reference_drift:" + page_name)
        if page.get("objectIdFieldName") != "OBJECTID":
            raise NhdCatchmentError("source_id_field_changed:" + page_name)
        if page.get("hasZ") is not True or page.get("hasM") is True:
            raise NhdCatchmentError("source_page_z_or_m_flag_missing:" + page_name)
        features = page.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise NhdCatchmentError("source_page_row_count_incomplete:" + page_name)
        columns: dict[str, list[object]] = {name: [] for name in self.expected_names}
        geometries: list[bytes | None] = []
        for index, feature in enumerate(features):
            expected_objectid = roster[offset + index]
            objectid, rings = _validate_feature(feature, self.expected_names, expected_objectid)
            attrs = feature["attributes"]
            self.record_hash.update(_canonical(feature) + b"\x00")
            self.ids.append(objectid)
            self.last_objectid = objectid
            if rings is None:
                self.null_geometry += 1
                geometries.append(None)
            elif any(vertex[0] == "NaN" and vertex[1] == "NaN"
                     for ring in rings for vertex in ring):
                self.nan_geometry += 1
                geometries.append(None)
            elif not rings:
                self.empty_geometry += 1
                geometries.append(None)
            else:
                for ring in rings:
                    for vertex in ring:
                        x, y = vertex[0], vertex[1]
                        if self._xmin is None or x < self._xmin:
                            self._xmin = x
                        if self._ymin is None or y < self._ymin:
                            self._ymin = y
                        if self._xmax is None or x > self._xmax:
                            self._xmax = x
                        if self._ymax is None or y > self._ymax:
                            self._ymax = y
                self.ring_count += len(rings)
                self.vertex_count += sum(len(ring) for ring in rings)
                # Conversion failures refuse the capture; OGC invalidity is a
                # compatibility diagnostic and never substitutes null geometry.
                wkb = to_wkb(feature["geometry"], GEOMETRY_TYPE, has_z=True, has_m=False)
                reason = is_valid_reason(from_wkb(wkb))
                if reason != "Valid Geometry":
                    self.ogc_invalid_geometry[reason] = self.ogc_invalid_geometry.get(reason, 0) + 1
                    if len(self.ogc_invalid_geometry_sample_objectids) < 20:
                        self.ogc_invalid_geometry_sample_objectids.append(objectid)
                self.geometry_hash.update(_canonical([objectid, rings]) + b"\x00")
                geometries.append(wkb)
            for name in self.expected_names:
                columns[name].append(attrs[name])
        if self._writer is not None:
            batch = pa.Table.from_pydict({**columns, GEOMETRY_COLUMN: geometries},
                                         schema=self.schema)
            self._writer.write_table(batch)

    def close_writer(self) -> None:
        if not self._writer_closed:
            self._writer_closed = True
            if self._writer is not None:
                self._writer.close()
                self._writer = None

    def finish(self, parquet_ok: bool) -> dict[str, object]:
        self.close_writer()
        if self.tmp_path is not None:
            if parquet_ok:
                self.tmp_path.replace(self.parquet_sink)
            else:
                self.tmp_path.unlink(missing_ok=True)
        return {"record_sha": self.record_hash.hexdigest(),
                "geometry_sha": self.geometry_hash.hexdigest(),
                "ids_sha": _sha(_canonical(list(self.ids))),
                "null_geometry_count": self.null_geometry,
                "nan_geometry_count": self.nan_geometry,
                "empty_geometry_count": self.empty_geometry,
                "unusable_geometry_counts": dict(sorted(self.unusable_geometry.items())),
                "ogc_invalid_geometry_counts": dict(sorted(self.ogc_invalid_geometry.items())),
                "ogc_invalid_geometry_sample_objectids": self.ogc_invalid_geometry_sample_objectids,
                "ring_count": self.ring_count, "vertex_count": self.vertex_count,
                "max_objectid": self.last_objectid,
                "observed_bounds": None if self._xmin is None else
                {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax, "ymax": self._ymax}}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained capture and optionally reconstruct the native geoparquet."""
    capture_root = Path(capture_root).absolute()
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    if b"3dhp.nationalmap.gov" not in _read_capture(capture_root, "service_doc")[0]:
        raise NhdCatchmentError("usgs_service_documentation_link_drift")
    _validate_service_document(docs["service_before"], "service_before")
    _validate_service_document(docs["alias_service"], "alias_service")
    fields, schema_sha = _validate_layer_document(docs["layer_before"])
    declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise NhdCatchmentError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise NhdCatchmentError("source_schema_baseline_drift")
    count_before = docs["count_before"].get("count")
    if type(count_before) is not int or count_before <= 0:
        raise NhdCatchmentError("source_count_drift")
    seed = _validate_roster(docs["ids_seed"].get("objectIds"), "ids_seed")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise NhdCatchmentError("source_id_field_changed")
    roster = _validate_roster(docs["ids_before"].get("objectIds"), "ids_before")
    if roster != seed:
        raise NhdCatchmentError("source_id_population_drift:ids_seed_vs_before")
    after_roster = _validate_roster(json.loads(
        _read_capture(capture_root, "ids_after")[0]).get("objectIds"), "ids_after")
    if after_roster != roster:
        raise NhdCatchmentError("source_id_population_drift:ids_after")
    count_after = json.loads(_read_capture(capture_root, "count_after")[0]).get("count")
    if type(count_after) is not int or count_after != count_before:
        raise NhdCatchmentError("source_metadata_or_count_changed_during_capture")
    for name in ("layer_after", "service_after"):
        body, _receipt = _read_capture(capture_root, name)
        document = json.loads(body)
        if name == "layer_after":
            if _layer_contract_view(document) != _layer_contract_view(docs["layer_before"]):
                raise NhdCatchmentError("source_layer_metadata_changed_during_capture")
        elif document != docs["service_before"]:
            raise NhdCatchmentError("source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise NhdCatchmentError("source_field_schema_changed_during_capture")
    state = _load_state(capture_root)
    segments = state.get("segments")
    if not isinstance(segments, list) or not segments \
            or any(not isinstance(item, dict) for item in segments):
        raise NhdCatchmentError("source_capture_roster_incomplete_or_unexpected")
    cursor = 0
    ordered: list[tuple[str, int, int]] = []
    for entry in sorted(segments, key=lambda item: (item.get("offset", -1), item.get("count", -1))):
        offset, count, name = entry.get("offset"), entry.get("count"), entry.get("name")
        if (type(offset) is not int or type(count) is not int or offset < 0 or count < 1
                or name != _segment_name(offset, count) or offset != cursor
                or offset + count > count_before or entry.get("features") != count):
            raise NhdCatchmentError("source_capture_roster_incomplete_or_unexpected")
        ordered.append((name, offset, count))
        cursor = offset + count
    if cursor != count_before:
        raise NhdCatchmentError("source_capture_roster_incomplete_or_unexpected")
    stream = _SegmentStream(capture_root, fields, parquet_sink)
    try:
        for name, offset, count in ordered:
            stream.process(name, offset, count, roster)
        summary = stream.finish(parquet_ok=True)
    except BaseException:
        stream.finish(parquet_ok=False)
        raise
    if len(stream.ids) != count_before or summary["max_objectid"] <= 0:
        raise NhdCatchmentError("source_page_population_incomplete")
    # The retained segment walk is the ID roster witness: its length and
    # contents must equal the producer's before/after count answers and the
    # enumerated OBJECTID roster brackets (capture_completeness).
    try:
        validate_capture_witness(count_before, roster, count_after, after_roster,
                                 list(stream.ids))
    except ValueError as exc:
        raise NhdCatchmentError("source_count_or_id_roster_drift") from exc
    ids_sha = summary["ids_sha"]
    return {"fields": fields, "schema_sha": schema_sha, "segments": len(ordered),
            "feature_count": count_before,
            "record_sha": summary["record_sha"], "geometry_sha": summary["geometry_sha"],
            "ids_sha": ids_sha, "parquet_path": parquet_sink,
            "null_geometry_count": summary["null_geometry_count"],
            "nan_geometry_count": summary["nan_geometry_count"],
            "empty_geometry_count": summary["empty_geometry_count"],
            "unusable_geometry_counts": summary["unusable_geometry_counts"],
            "ogc_invalid_geometry_counts": summary["ogc_invalid_geometry_counts"],
            "ogc_invalid_geometry_sample_objectids": summary["ogc_invalid_geometry_sample_objectids"],
            "ring_count": summary["ring_count"], "vertex_count": summary["vertex_count"],
            "max_objectid": summary["max_objectid"],
            "observed_bounds": summary["observed_bounds"]}


def _code_identity() -> dict[str, object]:
    return {**processing_identity(Path(__file__).parents[3]),
            "gdal": pyogrio.__gdal_version_string__, "pyogrio": pyogrio.__version__}


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": USER_AGENT},
    )


def _doc_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=True, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": DOC_USER_AGENT},
    )


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_segments: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"

    def verify_existing():
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"],
             "source_ids_sha256": extracted["ids_sha"],
             "feature_count": extracted["feature_count"],
             "source_null_geometry_count": extracted["null_geometry_count"],
             "ogc_invalid_geometry_counts": extracted["ogc_invalid_geometry_counts"]},
            parquet_body)

    reused = reuse_sealed_candidate(storage, state_key, verify_existing)
    if reused is not None:
        return reused
    audit = {
        "source_feature_count": extracted["feature_count"],
        "source_reported_count": extracted["feature_count"],
        "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Polygon Z / MultiPolygon Z",
        "null_geometry_count": extracted["null_geometry_count"],
        "nan_geometry_count": extracted["nan_geometry_count"],
        "empty_geometry_count": extracted["empty_geometry_count"],
        "unusable_geometry_counts": extracted["unusable_geometry_counts"],
        "ogc_invalid_geometry_counts": extracted["ogc_invalid_geometry_counts"],
        "ogc_invalid_geometry_sample_objectids": extracted["ogc_invalid_geometry_sample_objectids"],
        "unique_source_id_count": extracted["feature_count"],
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "alias_service_url": ALIAS_SERVICE_URL,
        "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": "3D Hydrography Program (3DHP) Layers",
        "crosswalk_open_id": "650c07e542fa43b6a327c7f683681712",
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The 3DHP_all service publishes no per-snapshot release date; the service "
            "copyrightText states a service refresh (September 4, 2026) and per-feature "
            "featuredate values are core-database load dates; neither is a dataset release "
            "date. Quarterly service updates and annual staged products are documented by USGS.",
        "pagination": {"mode": "enumerated_roster_bisected_segments",
                       "segment_start_records": SEGMENT_START_RECORDS,
                       "producer_max_record_count": MAX_RECORD_COUNT,
                       "segments": extracted["segments"], "resumable": True,
                       "resumed_segments": resumed_segments,
                       "id_roster_enumeration_note":
                           "returnIdsOnly enumerated the complete contiguous OBJECTID roster "
                           "(1..300070 observed 2026-11-13). 2500-record pages were refused "
                           "with complete HTTP 200 Esri error documents while 2000-record "
                           "slices served, so segments start at 2000 records and refused "
                           "segments are bisected until served; accepted segments must tile "
                           "the roster exactly and before/after count/roster brackets prove "
                           "nothing was skipped, duplicated or reordered."},
        "capture_window_utc": {"first_segment": first_captured_at,
                               "last_segment": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    receipts = load_checkpoint_receipt_map(capture_root, (*METADATA_NAMES, *AFTER_NAMES),
                                           _load_state(capture_root), _load_receipt)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Catchment (USGS 3DHP_all)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 80 "
                       "'Catchment', captured completely from the producer's own REST "
                       "publication as the maintained successor named by the HIFLD Open "
                       "inventory entry nhd/catchment. Complete source attributes and "
                       "EPSG:4326 polygon geometry are represented in the candidate. "
                       "Succession, freshness, schema/ID contract, coverage, rights and "
                       "release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no access or "
                   "use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication", "canonical_url": SERVICE_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason":
                        "The 3DHP_all service publishes no per-snapshot release date; its "
                        "copyrightText records a service refresh (September 4, 2026) and USGS "
                        "documents quarterly service updates and annual staged products.",
                    "license": "Public domain (U.S. Government work) per the producer service; "
                               "rights acceptance pending.",
                    "attribution": "USGS 3D Hydrography Program (3DHP_all), The National Map; "
                                   "formal attribution terms require maintainer review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": "the complete enumerated OBJECTID "
                                                          "roster captured as adaptively "
                                                          "bisected roster segments (full 3DHP "
                                                          "Catchment population)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types, including the service-generated "
                                                             "Shape__Length and Shape__Area doubles",
                                            "source_ids": "Preserve producer OBJECTID and id3dhp "
                                                          "values unchanged (id3dhp is the producer's "
                                                          "base-36 identifier, documented as not "
                                                          "persistent); no substitute IDs, casts or "
                                                          "joins",
                                            "geometry": "Served EPSG:4326 polygon coordinates with Z "
                                                        "retained through GDAL ESRIJSON as WKB. "
                                                        "No projection, repair or coordinate loss. "
                                                        "Ring start/winding can differ. OGC-invalid "
                                                        "geometry is retained and counted by reason; "
                                                        "conversion fidelity failures refuse capture.",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/catchment ('Catchment', agency US Geological "
                   "Survey) carries the description of the USGS 3D Hydrography Program (3DHP) "
                   "3DHP_all service verbatim. USGS retired the NHD as a maintained product on "
                   "2023-10-01 and the DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, "
                   "consolidated row '3D Hydrography Program (3DHP) Layers', Open ID "
                   "650c07e542fa43b6a327c7f683681712) directs the NHD-family layers to the "
                   "3dhp.nationalmap.gov usgs_3dhp_all FeatureServer captured here (layer 80 "
                   "'Catchment'); the same-org hydro.nationalmap.gov 3DHP_all service is captured "
                   "as the producer's alias publication. The 3DHP Catchment schema (9 fields, "
                   "non-persistent base-36 id3dhp) replaces the legacy NHD catchment schema "
                   "wholesale, so legacy attributes beyond these fields have no successor "
                   "equivalents, and EDH-sourced catchment boundaries replace NHD-derived content "
                   "over time (per-feature workunitid), so coverage changes between refreshes are "
                   "expected and unaccepted. The DataLumos project 240107 archive copy (archive "
                   "modification 2025-04-08) is identification and comparison context only and is "
                   "never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "Z-aware geometry retained in the producer page CRS; all succession, "
                      "freshness, contract, rights and release approvals remain pending.",
            "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile "
                                                        "field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized "
                                                  "geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"],
             "succession_accepted": False, "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    metadata_outputs: dict[str, bytes] = {
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the seal never
    # holds more than one segment body/wire pair plus the small receipt maps.
    stage_candidate_objects(
        storage, prefix, state_key, version, identity, metadata_outputs,
        capture_root, receipts, _read_capture,
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'])
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": extracted["feature_count"],
            "source_reported_count": extracted["feature_count"],
            "source_population_complete": True,
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_nan_geometry_count": extracted["nan_geometry_count"],
            "source_empty_geometry_count": extracted["empty_geometry_count"],
            "source_unusable_geometry_counts": extracted["unusable_geometry_counts"],
            "source_ogc_invalid_geometry_counts": extracted["ogc_invalid_geometry_counts"],
            "source_id_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                             version: str, identity: Mapping[str, object],
                             audit: Mapping[str, object], current_parquet: bytes) -> dict[str, object]:
    fingerprint, payload = load_candidate_checkpoint(
        storage, state_key, version, identity,
        audit['source_record_sha256'], audit['source_schema_sha256'], NhdCatchmentError)
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise NhdCatchmentError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"],
            "source_reported_count": audit["feature_count"],
            "source_population_complete": True,
            "source_null_geometry_count": audit["source_null_geometry_count"],
            "source_id_sha256": audit["source_ids_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_ogc_invalid_geometry_counts": audit["ogc_invalid_geometry_counts"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def capture_nhd_catchment(storage: StagingStorageResource, *, version: str,
                          capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NhdCatchmentError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise NhdCatchmentError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise NhdCatchmentError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise NhdCatchmentError("disk_floor_before_acquisition")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (default_capture_dir(DATASET_SLUG, "captures") / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise NhdCatchmentError("unsafe_capture_directory")
    resumed_segments = len(_load_state(capture_root).get("segments", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_nhd_catchment_pages(capture_root)
        state = _load_state(capture_root)
        segments = [entry for entry in state["segments"] if isinstance(entry, dict)]
        captured = [str(entry.get("captured_at", "")) for entry in segments]
        extracted = _stream_validate(capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted, resumed_segments=resumed_segments,
                               first_captured_at=captured[0] if captured else "",
                               last_captured_at=captured[-1] if captured else "")
    except Exception as exc:
        valid_names = []
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) \
                if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                _read_capture(capture_root, name)
            except (OSError, NhdCatchmentError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names,
                                                                NhdCatchmentError),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise

