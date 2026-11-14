"""Streaming, approval-gated capture of the USGS 3DHP_all Flowline layer.

The HIFLD Open inventory entry ``nhd/flowline`` ("Flowline", agency US
Geological Survey) carries the description of the USGS 3D Hydrography Program
(3DHP) ``3DHP_all`` ArcGIS REST service verbatim; the DHS HIFLD Open crosswalk
(DOI 10.3886/E241367V1) consolidates the NHD-family rows into one "3D
Hydrography Program (3DHP) Layers" row whose Open REST Service is the USGS
``3dhp.nationalmap.gov`` ``usgs_3dhp_all`` FeatureServer. Its layer 50
"Flowline" is the maintained producer publication captured here. The layer held
30,251,533 features on 2026-11-13, so this module streams the source the way
the FRS INTERESTS extractor does: each keyset page is retained as raw evidence
in a resumable capture directory before any candidate is sealed. The
DataLumos project 240108 archive copy is identification context only and is
never fetched.

Completeness at this scale is established without the producer's
``returnIdsOnly`` roster: that request fails server-side with an error document
for this population (observed 2026-11-13), so the pinned feature count, strict
ascending OBJECTID keyset walk, page-count ceiling, per-page
``exceededTransferLimit`` consistency and the empty tail-page witness together
prove the captured population. The per-value, per-geometry and ID-roster
baseline digests stay unpinned until the first complete capture; they are
computed and reported by ``build_nhd_flowline_baseline`` and then pinned as
constants for later drift refusal.

Scale note (observed 2026-11-13): one 2,500-feature keyset page carries about
6.4-8.5 MB of JSON, so a complete raw-evidence capture is on the order of 100
GB per retained representation, a multi-hour sequential fetch, and roughly
double that again when the sealed candidate copies the raw evidence into
staging. The capture state file makes every session resumable; the disk floor
below refuses to start where the complete capture cannot fit. Retaining the
raw pages inside the sealed candidate follows the repository's established
fidelity contract; changing that for this dataset is a maintainer decision.
Publication stays blocked pending the succession, freshness, schema/ID
contract, rights and release decisions; all runtime approval flags are false
and the draft refresh schedule is stopped.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import struct
from array import array
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

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
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "nhd-flowline"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 50
LAYER_NAME = "Flowline"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolyline"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-flowline-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
PAGE_SIZE = 2500
MAX_BODY_BYTES = 96 * 1024 * 1024
MIN_FREE_BYTES = 350_000_000_000
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
EXPECTED_COUNT = 30_251_533
EXPECTED_SCHEMA_SHA256 = "d0d84dca81d1be2999febebda4c6527ac6fe67cfb4470b48f6f926090a6f3c6f"
# The per-value, geometry and ID-roster baselines cannot be computed without a
# complete capture of the 30.25M-feature population; they are pinned here as
# empty (checked only when non-empty) and recorded by the first complete
# capture for the maintainer to pin.
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_IDS_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_LAYER_EXTENT = {"xmin": -19824887.0683, "ymin": -1615773.7974999994,
                         "xmax": 20007564.228499997, "ymax": 11518862.833800001}
BASELINE_PENDING_NOTE = (
    "The record, geometry and ID-roster baseline digests are not pinned yet: the "
    "30,251,533-feature population cannot be captured inside one session, so no "
    "complete capture existed when this draft was implemented (observed "
    "2026-11-13). build_nhd_flowline_baseline computes them from a completed "
    "resumable capture directory for the maintainer to pin.")

NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("id3dhp", "esriFieldTypeString", True, 7),
    ("featuredate", "esriFieldTypeDate", True, 8),
    ("mainstemid", "esriFieldTypeString", True, 200),
    ("gnisid", "esriFieldTypeInteger", True, None),
    ("gnisidlabel", "esriFieldTypeString", True, 120),
    ("featuretype", "esriFieldTypeSmallInteger", True, None),
    ("featuretypelabel", "esriFieldTypeString", True, 200),
    ("lengthkm", "esriFieldTypeDouble", True, None),
    ("waterbodyid3dhp", "esriFieldTypeString", True, 7),
    ("flowdirection", "esriFieldTypeSmallInteger", True, None),
    ("flowdirectionlabel", "esriFieldTypeString", True, 200),
    ("catchmentid3dhp", "esriFieldTypeString", True, 7),
    ("flowpathid3dhp", "esriFieldTypeString", True, 7),
    ("streamlevel", "esriFieldTypeInteger", True, None),
    ("startflag", "esriFieldTypeSmallInteger", True, None),
    ("terminalflag", "esriFieldTypeSmallInteger", True, None),
    ("streamorder", "esriFieldTypeInteger", True, None),
    ("streamcalculator", "esriFieldTypeInteger", True, None),
    ("hydrosequence", "esriFieldTypeDouble", True, None),
    ("dnhydrosequence", "esriFieldTypeDouble", True, None),
    ("uphydrosequence", "esriFieldTypeDouble", True, None),
    ("dnlevelpath", "esriFieldTypeDouble", True, None),
    ("uplevelpath", "esriFieldTypeDouble", True, None),
    ("pathlength", "esriFieldTypeDouble", True, None),
    ("arbolatesum", "esriFieldTypeDouble", True, None),
    ("divergence", "esriFieldTypeSmallInteger", True, None),
    ("divergencelabel", "esriFieldTypeString", True, 200),
    ("rtrndivergence", "esriFieldTypeSmallInteger", True, None),
    ("levelpath", "esriFieldTypeInteger", True, None),
    ("terminalpath", "esriFieldTypeDouble", True, None),
    ("workunitid", "esriFieldTypeString", True, 100),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
)
METADATA_NAMES = ("service_doc", "alias_service", "service_before", "layer_before",
                  "count_before")


class NhdFlowlineError(ValueError):
    """The source snapshot cannot be represented as the pinned Flowline candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise NhdFlowlineError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _page_url(last_objectid: int) -> str:
    return _query({"where": f"OBJECTID>{last_objectid}", "outFields": "*",
                   "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                   "outSR": str(SOURCE_CRS_WKID), "orderByFields": "OBJECTID ASC",
                   "resultRecordCount": str(PAGE_SIZE), "f": "json"})


def _expected_capture_names(count: int) -> list[str]:
    data_pages = (count + PAGE_SIZE - 1) // PAGE_SIZE
    return [*METADATA_NAMES, *(f"page_{index:06d}" for index in range(data_pages)),
            "page_tail", "layer_after", "service_after", "count_after"]


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGlobalID": pa.string()}.get(str(field["type"]), pa.string())


_WKB_LINESTRING_ZM = 0xC0000002
_WKB_MULTILINESTRING_ZM = 0xC0000005
_GEOMETRY_COLUMN = "geometry"


def _polyline_wkb(geometry: object) -> bytes | None:
    """WKB for one feature's Esri polyline paths, Z and M retained verbatim.

    Esri JSON (verified on the pinned captures): every vertex carries exactly
    four ordinates (x, y, z, m) because the layer declares both hasZ and hasM;
    a feature with a single path is a LineString ZM, several paths are one
    MultiLineString ZM whose parts keep the producer's path order and vertex
    order. Nothing is projected, quantized, dropped or repaired.
    """
    if geometry is None:
        return None
    if not isinstance(geometry, dict) or set(geometry) - {"hasZ", "hasM", "paths"}:
        raise NhdFlowlineError("unexpected_source_geometry_members")
    if geometry.get("hasZ") is not True or geometry.get("hasM") is not True:
        raise NhdFlowlineError("source_geometry_z_or_m_flag_missing")
    paths = geometry.get("paths")
    if not isinstance(paths, list) or not paths:
        raise NhdFlowlineError("source_polyline_paths_missing")
    parts: list[bytes] = []
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            raise NhdFlowlineError("source_polyline_path_too_short")
        vertices = []
        for vertex in path:
            if not isinstance(vertex, list) or len(vertex) != 4:
                raise NhdFlowlineError("source_polyline_ordinate_contract_drift")
            x, y, z, m = vertex
            if not (type(x) in (int, float) and type(y) in (int, float)
                    and type(z) in (int, float) and type(m) in (int, float)):
                raise NhdFlowlineError("source_polyline_ordinate_not_numeric")
            if not all(value == value and -1e308 < value < 1e308 for value in (x, y, z, m)):
                raise NhdFlowlineError("source_polyline_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise NhdFlowlineError("source_coordinate_range_drift")
            vertices.append((x, y, z, m))
        parts.append(struct.pack("<BII", 1, _WKB_LINESTRING_ZM, len(vertices))
                     + b"".join(struct.pack("<4d", *vertex) for vertex in vertices))
    if len(parts) == 1:
        return parts[0]
    return struct.pack("<BII", 1, _WKB_MULTILINESTRING_ZM, len(parts)) + b"".join(parts)


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise NhdFlowlineError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise NhdFlowlineError("source_native_type_changed:" + name)
        if name == "id3dhp" and value and any(
                character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in value):
            raise NhdFlowlineError("source_id3dhp_value_drift")
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise NhdFlowlineError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise NhdFlowlineError("source_native_type_changed:" + name)
        return
    raise NhdFlowlineError("unsupported_source_physical_type:" + esri_type)


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      last_objectid: int) -> tuple[int, int]:
    if not isinstance(feature, dict):
        raise NhdFlowlineError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(expected_names):
        raise NhdFlowlineError("source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise NhdFlowlineError("source_object_id_type_changed")
    if objectid <= last_objectid:
        raise NhdFlowlineError("source_object_ids_not_strictly_increasing")
    vertex_count = 0
    if geometry is not None:
        paths = geometry.get("paths")
        if not isinstance(paths, list):
            raise NhdFlowlineError("unexpected_source_geometry_members")
        vertex_count = sum(len(path) for path in paths)
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    return objectid, vertex_count


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NhdFlowlineError("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise NhdFlowlineError("retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = _load_receipt(capture_root, name)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise NhdFlowlineError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"data_pages": [], "tail": None, "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NhdFlowlineError("capture_state_invalid") from exc
    if (not isinstance(state, dict) or not isinstance(state.get("data_pages"), list)
            or not (state.get("tail") is None or isinstance(state.get("tail"), dict))):
        raise NhdFlowlineError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def _retained_or_fetch(client: httpx.Client | None, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except NhdFlowlineError:
            if client is None:
                raise
            for suffix in ("body", "wire", "receipt.json"):
                (capture_root / f"{name}.{suffix}").unlink(missing_ok=True)
    if client is None:
        raise NhdFlowlineError("retained_capture_file_missing:" + name)
    return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)


def _witness_error_response(capture_root: Path, name: str) -> None:
    """Move a retained producer error answer aside so the slot can be refetched.

    A complete HTTP 200 error body carries a self-consistent receipt, so without
    this witness step a resumed capture would replay the same error forever.
    """
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def _reject_error_response(capture_root: Path, name: str, body: bytes) -> None:
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return
    if isinstance(document, dict) and "error" in document:
        _witness_error_response(capture_root, name)
        raise NhdFlowlineError("source_page_error")


def _load_receipt_map(capture_root: Path) -> dict[str, dict[str, object]]:
    """Load every expected receipt without holding any page body in memory."""
    return {name: _load_receipt(capture_root, name)
            for name in _expected_capture_names(EXPECTED_COUNT)}


class _RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return _read_capture(self._capture_root, name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise NhdFlowlineError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise NhdFlowlineError("usgs_3dhp_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list):
        raise NhdFlowlineError("usgs_3dhp_service_layer_roster_drift:" + label)
    if roster[:len(EXPECTED_SERVICE_LAYERS) - len(SPATIAL_METADATA_IDS)] != \
            EXPECTED_SERVICE_LAYERS[:len(EXPECTED_SERVICE_LAYERS) - len(SPATIAL_METADATA_IDS)]:
        raise NhdFlowlineError("usgs_3dhp_service_layer_roster_drift:" + label)
    metadata_rows = roster[len(EXPECTED_SERVICE_LAYERS) - len(SPATIAL_METADATA_IDS):]
    if (len(metadata_rows) != len(SPATIAL_METADATA_IDS)
            or {entry_id for entry_id, _name in metadata_rows} != set(SPATIAL_METADATA_IDS)
            or {name for _entry_id, name in metadata_rows} != set(SPATIAL_METADATA_NAMES)):
        raise NhdFlowlineError("usgs_3dhp_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "3DHP_all" not in description or "3D Hydrography Program" not in description:
        raise NhdFlowlineError("usgs_3dhp_service_description_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdFlowlineError("source_service_spatial_reference_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise NhdFlowlineError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise NhdFlowlineError("exact_3dhp_flowline_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not True:
        raise NhdFlowlineError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise NhdFlowlineError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != PAGE_SIZE:
        raise NhdFlowlineError("source_record_limit_drift")
    source_reference = layer.get("sourceSpatialReference")
    if (not isinstance(source_reference, dict)
            or source_reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise NhdFlowlineError("source_layer_spatial_reference_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdFlowlineError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise NhdFlowlineError("source_layer_extent_drift")
    return _profile_schema(layer)


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained page byte and optionally reconstruct the native geoparquet."""
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    if b"3dhp.nationalmap.gov" not in _read_capture(capture_root, "service_doc")[0]:
        raise NhdFlowlineError("usgs_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    _validate_service_document(service, "service_before")
    alias = docs["alias_service"]
    _validate_service_document(alias, "alias_service")
    fields, schema_sha = _validate_layer_document(layer)
    declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise NhdFlowlineError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise NhdFlowlineError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise NhdFlowlineError("source_count_drift")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise NhdFlowlineError("source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise NhdFlowlineError("source_field_schema_changed_during_capture")
    tail = json.loads(_read_capture(capture_root, "page_tail")[0])
    if tail.get("features") != []:
        raise NhdFlowlineError("source_tail_page_not_empty")

    expected_names = tuple(entry["name"] for entry in fields)
    arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True) for field in fields]
    arrow_fields.append(pa.field(_GEOMETRY_COLUMN, pa.binary(), nullable=True))
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": _GEOMETRY_COLUMN,
           "columns": {_GEOMETRY_COLUMN: {"encoding": "WKB",
                                          "geometry_types": ["LineString ZM", "MultiLineString ZM"],
                                          "crs": crs, "edges": "planar"}}}
    schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
    sink_path = parquet_sink
    tmp_path = (sink_path.with_name(f"{sink_path.name}.construction-tmp")
                if sink_path is not None else None)
    if sink_path is not None:
        sink_path.parent.mkdir(parents=True, exist_ok=True)
    # Never trust an existing output file: reconstruction always streams from
    # the validated pages into a temporary file that atomically replaces the
    # sink only after every check passes, so an interrupted construction can
    # never be sealed.
    writer = pq.ParquetWriter(tmp_path, schema, compression="zstd", use_dictionary=False) \
        if tmp_path is not None else None

    ids = array("q")
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    last_objectid = 0
    null_geometry_count = 0
    path_count = 0
    vertex_count = 0
    featuretype_distribution: dict[object, int] = {}
    bounds_extrema: list[float] | None = None
    page_bodies = [name for name in _expected_capture_names(EXPECTED_COUNT)
                   if name.startswith("page_") and name != "page_tail"]
    try:
        for page_name in page_bodies:
            page = json.loads(_read_capture(capture_root, page_name)[0])
            if not isinstance(page, dict) or "error" in page:
                raise NhdFlowlineError("source_page_error:" + page_name)
            reference = page.get("spatialReference")
            if (not isinstance(reference, dict)
                    or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                    or reference.get("latestWkid") != SOURCE_CRS_WKID):
                raise NhdFlowlineError("source_page_spatial_reference_drift:" + page_name)
            if page.get("objectIdFieldName") != "OBJECTID":
                raise NhdFlowlineError("source_id_field_changed:" + page_name)
            if page.get("hasZ") is not True or page.get("hasM") is not True:
                raise NhdFlowlineError("source_page_z_or_m_flag_missing:" + page_name)
            features = page.get("features")
            if not isinstance(features, list) or not features \
                    or len(features) > PAGE_SIZE:
                raise NhdFlowlineError("source_page_row_count_incomplete:" + page_name)
            if len(features) < PAGE_SIZE and page.get("exceededTransferLimit") is True:
                raise NhdFlowlineError("source_page_transfer_truncated:" + page_name)
            columns: dict[str, list[object]] = {name: [] for name in expected_names}
            geometries: list[bytes | None] = []
            for feature in features:
                objectid, feature_vertices = _validate_feature(feature, expected_names, last_objectid)
                last_objectid = objectid
                attrs, geometry = feature["attributes"], feature.get("geometry")
                record_hash.update(_canonical(feature) + b"\x00")
                ids.append(objectid)
                if geometry is None:
                    null_geometry_count += 1
                    geometries.append(None)
                else:
                    paths = geometry["paths"]
                    path_count += len(paths)
                    vertex_count += feature_vertices
                    for path in paths:
                        for vertex in path:
                            x, y = vertex[0], vertex[1]
                            if bounds_extrema is None:
                                bounds_extrema = [x, y, x, y]
                                continue
                            if x < bounds_extrema[0]:
                                bounds_extrema[0] = x
                            if y < bounds_extrema[1]:
                                bounds_extrema[1] = y
                            if x > bounds_extrema[2]:
                                bounds_extrema[2] = x
                            if y > bounds_extrema[3]:
                                bounds_extrema[3] = y
                    geometries.append(_polyline_wkb(geometry))
                    geometry_hash.update(_canonical([objectid, paths]) + b"\x00")
                featuretype = attrs["featuretype"]
                featuretype_distribution[featuretype] = \
                    featuretype_distribution.get(featuretype, 0) + 1
                for name in expected_names:
                    columns[name].append(attrs[name])
            if writer is not None:
                batch = pa.Table.from_pydict({**columns, _GEOMETRY_COLUMN: geometries},
                                             schema=schema)
                writer.write_table(batch)
        if writer is not None:
            writer.close()
            writer = None
        if len(ids) != EXPECTED_COUNT or last_objectid == 0:
            raise NhdFlowlineError("source_page_population_incomplete")
        ids_sha = _sha(_canonical(list(ids)))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
        if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
            raise NhdFlowlineError("source_value_or_geometry_baseline_drift")
        if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
            raise NhdFlowlineError("source_geometry_or_coverage_drift")
        if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
            raise NhdFlowlineError("source_id_roster_drift")
        if null_geometry_count != EXPECTED_NULL_GEOMETRY_COUNT:
            raise NhdFlowlineError("source_null_geometry_count_drift")
        if tmp_path is not None:
            tmp_path.replace(sink_path)
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
    bounds = None
    if bounds_extrema is not None:
        bounds = {"xmin": bounds_extrema[0], "ymin": bounds_extrema[1],
                  "xmax": bounds_extrema[2], "ymax": bounds_extrema[3]}
    return {"fields": fields, "schema_sha": schema_sha, "record_sha": record_sha,
            "geometry_sha": geometry_sha, "ids_sha": ids_sha,
            "null_geometry_count": null_geometry_count, "parquet_path": sink_path,
            "path_count": path_count, "vertex_count": vertex_count,
            "featuretype_distribution": featuretype_distribution, "observed_bounds": bounds,
            "max_objectid": last_objectid}


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def _parquet_descriptor(path: Path) -> dict[str, object]:
    """Size and digest of a candidate parquet without holding it in memory."""
    return {"bytes": path.stat().st_size, "sha256": sha256_file(path)}


def _write_immutable_file(storage: StagingStorageResource, key: str, source: Path,
                          descriptor: Mapping[str, object]) -> None:
    """write_immutable for a local file too large to buffer as bytes."""
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.upload_local_file(source, key)
        return
    if snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
        raise ValueError("immutable_staged_object_changed:" + key)


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


def fetch_nhd_flowline_pages(capture_root: Path, *, client: httpx.Client | None = None,
                             doc_client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    pages: list[dict[str, object]] = list(state.get("data_pages", []))
    tail: dict[str, object] | None = state.get("tail")
    owner_client = client is None
    owner_doc = doc_client is None
    active = client or _default_client()
    docs_active = doc_client or _doc_client()
    first_captured_at = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
    try:
        retained: dict[str, tuple[bytes, dict[str, object]]] = {}
        for name in METADATA_NAMES:
            url = {"service_doc": SERVICE_DOC_URL,
                   "alias_service": ALIAS_SERVICE_URL + "?f=pjson",
                   "service_before": SERVICE_URL + "?f=pjson",
                   "layer_before": LAYER_URL + "?f=pjson",
                   "count_before": _count_url()}[name]
            active_client = docs_active if name == "service_doc" else active
            retained[name] = _retained_or_fetch(active_client, capture_root, name, url)
            if name != "service_doc":
                _reject_error_response(capture_root, name, retained[name][0])
        count_before = json.loads(retained["count_before"][0]).get("count")
        if count_before != EXPECTED_COUNT:
            raise NhdFlowlineError("source_count_drift")
        last_objectid = pages[-1]["last_objectid"] if pages else 0
        while not (capture_root / "page_tail.body").exists():
            name = f"page_{len(pages):06d}"
            body, receipt = _retained_or_fetch(active, capture_root, name, _page_url(last_objectid))
            page = json.loads(body)
            if not isinstance(page, dict) or "error" in page:
                _witness_error_response(capture_root, name)
                raise NhdFlowlineError("source_page_error")
            features = page.get("features")
            if not isinstance(features, list):
                raise NhdFlowlineError("source_page_missing_features")
            if not features:
                if last_objectid == 0:
                    raise NhdFlowlineError("source_page_population_incomplete")
                # An empty page in a data slot is the completeness witness: the same
                # query URL defines page_tail, so retain its exact bytes under that name.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").write_bytes(
                        (capture_root / f"{name}.{suffix}").read_bytes())
                break
            for feature in features:
                last_objectid, _vertices = _validate_feature(
                    feature, tuple(entry[0] for entry in NATIVE_FIELDS), last_objectid)
            more = page.get("exceededTransferLimit")
            if len(features) < PAGE_SIZE and more is True:
                raise NhdFlowlineError("source_page_transfer_truncated")
            pages.append({"name": name, "last_objectid": last_objectid, "features": len(features),
                          "exceeded_transfer_limit": more, "captured_at": receipt["observed_at"],
                          "body_sha256": _sha(body)})
            _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
            if len(pages) > (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
                raise NhdFlowlineError("source_page_count_exceeds_pinned_population")
            if len(features) < PAGE_SIZE or more is False:
                tail_body, tail_receipt = _retained_or_fetch(active, capture_root, "page_tail",
                                                             _page_url(last_objectid))
                _reject_error_response(capture_root, "page_tail", tail_body)
                if json.loads(tail_body).get("features") != []:
                    raise NhdFlowlineError("source_tail_page_not_empty")
                tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                        "captured_at": tail_receipt["observed_at"], "body_sha256": _sha(tail_body)}
                _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
                break
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise NhdFlowlineError("source_tail_page_not_empty")
        for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                          ("service_after", SERVICE_URL + "?f=pjson"),
                          ("count_after", _count_url())):
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            _reject_error_response(capture_root, name, body)
            retained[name] = (body, _read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise NhdFlowlineError("source_metadata_or_count_changed_during_capture")
    except NhdFlowlineError:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise
    except Exception as exc:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise NhdFlowlineError("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()

    last_captured_at = str(tail["captured_at"])
    # Construction happens once at seal time (capture_nhd_flowline);
    # retention and validation stay the only fetch-time side effects.
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": EXPECTED_COUNT,
        "source_max_objectid": extracted["max_objectid"],
        "resumed_pages": len(pages),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": True})
    return {"capture_directory": str(capture_root), "pages": len(pages), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_pages: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    parquet_path = Path(extracted["parquet_path"])
    parquet_descriptor = _parquet_descriptor(parquet_path)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    if storage.object_snapshot(state_key) is not None:
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"]},
            parquet_descriptor)
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "LineString ZM / MultiLineString ZM",
        "null_geometry_count": extracted["null_geometry_count"],
        "unique_source_id_count": EXPECTED_COUNT,
        "source_path_count": extracted["path_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_featuretype_distribution": extracted["featuretype_distribution"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "alias_service_url": ALIAS_SERVICE_URL,
        "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_layer_row": "3D Hydrography Program (3DHP) Layers",
        "crosswalk_open_id": "650c07e542fa43b6a327c7f683681712",
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": bool(
            not EXPECTED_RECORD_SHA256 or not EXPECTED_GEOMETRY_SHA256
            or not EXPECTED_IDS_SHA256),
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The 3DHP_all service publishes no per-snapshot release date; the service "
            "copyrightText states a service refresh (September 4, 2026) and per-feature "
            "featuredate values are core-database load dates; neither is a dataset release "
            "date. Quarterly service updates and annual staged products are documented by USGS.",
        "pagination": {"mode": "objectid_keyset_asc", "page_size": PAGE_SIZE,
                       "pages": (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE,
                       "tail_witness_page": True, "resumable": True,
                       "resumed_pages": resumed_pages,
                       "id_roster_enumeration_note":
                           "returnIdsOnly over the full population fails server-side "
                           "(error 500 observed 2026-11-13), so completeness uses the pinned "
                           "count, strict ascending OBJECTID walk, page-count ceiling and the "
                           "empty tail-page witness instead of an enumerated roster."},
        "capture_window_utc": {"first_page": first_captured_at, "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    source_objects = {parquet_name: parquet_descriptor}
    receipts = _load_receipt_map(capture_root)
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Flowline (USGS 3DHP_all)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 50 "
                       "'Flowline', captured completely from the producer's own REST "
                       "publication as the maintained successor named by the HIFLD Open "
                       "inventory entry nhd/flowline. Complete source attributes and Z/M "
                       "EPSG:4326 polyline geometry are represented in the candidate. "
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
                                            "population": "1=1 over the complete keyset-ordered "
                                                          "OBJECTID walk (full 3DHP Flowline "
                                                          "population)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types, including the service-generated "
                                                             "Shape__Length double",
                                            "source_ids": "Preserve producer OBJECTID and id3dhp "
                                                          "values unchanged (id3dhp is the producer's "
                                                          "base-36 identifier, documented as not "
                                                          "persistent); no substitute IDs, casts or "
                                                          "joins",
                                            "geometry": "Source EPSG:4326 polyline paths with Z "
                                                        "and M retained as WKB without projection "
                                                        "or repair; single-path features are "
                                                        "LineString ZM, multi-path features are "
                                                        "MultiLineString ZM; null source geometry "
                                                        "retained as null",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/flowline ('Flowline', agency US Geological "
                   "Survey, issued 2025-02-27) carries the description of the USGS 3D "
                   "Hydrography Program (3DHP) 3DHP_all service verbatim. USGS retired the NHD "
                   "as a maintained product on 2023-10-01 and the DHS HIFLD Open crosswalk (DOI "
                   "10.3886/E241367V1, consolidated row '3D Hydrography Program (3DHP) Layers', "
                   "Open ID 650c07e542fa43b6a327c7f683681712) directs the NHD-family layers to "
                   "the 3dhp.nationalmap.gov usgs_3dhp_all FeatureServer captured here (layer "
                   "50 'Flowline'); the same-org hydro.nationalmap.gov 3DHP_all service is "
                   "captured as the producer's alias publication. The 3DHP schema (33 fields, "
                   "non-persistent base-36 id3dhp) replaces the legacy NHDFlowline schema "
                   "wholesale, so legacy attributes (Permanent_Identifier, ReachCode, "
                   "FType/FCode, Resolution) have no successor equivalents; EDH-sourced areas "
                   "replace NHD-derived content over time, so coverage changes are expected and "
                   "unaccepted. That succession coverage and contract change is recorded here "
                   "and remains an unaccepted maintainer decision. The DataLumos project 240108 "
                   "archive copy (archive modification 2025-04-08) is identification and "
                   "comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and Z/M-aware "
                      "geometry retained in the producer page CRS; all succession, freshness, "
                      "contract, rights and release approvals remain pending.",
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
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the full
    # population holds tens of millions of features, so the seal never holds
    # more than one page body/wire pair plus the small receipt and descriptor
    # maps, and the parquet moves from disk to storage as a streamed file.
    descriptors: dict[str, dict[str, object]] = {}
    _write_immutable_file(storage, f"{prefix}/{parquet_name}", parquet_path, parquet_descriptor)
    descriptors[parquet_name] = dict(parquet_descriptor)
    for relative, body in metadata_outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
        descriptors[relative] = {"bytes": len(body), "sha256": _sha(body)}
    for name in sorted(receipts):
        body, receipt = _read_capture(capture_root, name)
        wire = (capture_root / f"{name}.wire").read_bytes()
        for suffix, blob in (("body", body), ("wire", wire),
                             ("receipt.json", _canonical(receipt) + b"\n")):
            relative = f"raw/{name}.{suffix}"
            write_immutable(storage, f"{prefix}/{relative}", blob)
            descriptors[relative] = {"bytes": len(blob), "sha256": _sha(blob)}
    fingerprint = _sha(_canonical(identity))
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": receipts, "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_id_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                             version: str, identity: Mapping[str, object],
                             audit: Mapping[str, object],
                             current_parquet: Mapping[str, object]) -> dict[str, object]:
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity",
            "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise NhdFlowlineError("cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != dict(current_parquet):
        raise NhdFlowlineError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": EXPECTED_NULL_GEOMETRY_COUNT,
            "source_id_sha256": audit.get("source_ids_sha256", ""),
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def capture_nhd_flowline(storage: StagingStorageResource, *, version: str,
                         capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NhdFlowlineError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise NhdFlowlineError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise NhdFlowlineError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise NhdFlowlineError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory \
        else Path("/data/workspace/scratch/nhd-flowline/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise NhdFlowlineError("unsafe_capture_directory")
    resumed_pages = len(_load_state(capture_root).get("data_pages", [])) \
        if capture_root.exists() else 0
    try:
        fetch_nhd_flowline_pages(capture_root)
        state = _load_state(capture_root)
        captured = [page["captured_at"] for page in state["data_pages"]]
        extracted = _stream_validate(capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted, resumed_pages=resumed_pages,
                               first_captured_at=captured[0],
                               last_captured_at=str(state["tail"]["captured_at"]))
    except Exception as exc:
        valid_names = []
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) \
                if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                _read_capture(capture_root, name)
            except (OSError, NhdFlowlineError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_nhd_flowline_baseline(capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": EXPECTED_COUNT,
            "source_max_objectid": extracted["max_objectid"]}
