"""Capture the maintained USGS PAD-US Protection Mechanism Category view.

Resumable gzip keyset pages retain native values, EPSG:4326 page geometry,
and source EPSG:3857 layer metadata. Stable source count/ID witnesses and
exact page coverage establish completeness; source-null and OGC-invalid
geometry remain observed diagnostics, never repaired. Construction and
retained replay preserve immutable originals. All approvals remain false.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from array import array
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping
from urllib.parse import urlencode, urlsplit

from shapely import from_wkb as _shapely_from_wkb, is_valid_reason as _shapely_is_valid_reason
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.http_capture import capture_retention_budget
from dagster_hifld.sources.nhd_capture import capture_fixed_keyset_layer
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_layer import load_replay, save_state as _save_state
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    capture_get,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    verify_cached_candidate,
    validate_capture_request_identity,
    write_immutable,
    reuse_sealed_candidate,
    load_candidate_checkpoint,
    stage_candidate_objects,
    write_immutable_file as _write_immutable_file,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "pad-us-protection-mechanism-category"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = ("https://services.arcgis.com/v01gqwM5QqNysAAi"
               "/ArcGIS/rest/services/Protection_Mechanism_Category_PADUS/FeatureServer")
# USGS-owned ArcGIS Online evidence: the view item linked by USGS's PAD-US Web
# Services page (owner mcroft@usgs.gov_USGS, org v01gqwM5QqNysAAi), and the
# anonymously readable view-layer sources document that names the underlying
# source layer. The source layer's own service item (126eef94...) answers
# anonymous item requests with HTTP 403 and its service requires a token, so
# neither is fetched; the sources document is the pinned view-to-input
# relationship evidence.
VIEW_ITEM_ID = "80c7a138989c4cf8937c4502371d6e99"
SOURCE_SERVICE_ITEM_ID = "126eef9409ce48b0a9e55c2fe8fd77b8"
SOURCE_LAYER_URL = ("https://services.arcgis.com/v01gqwM5QqNysAAi"
                    "/ArcGIS/rest/services/ProtMechCat_A/FeatureServer/0")
SHARING_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/{item}?f=json"
SERVICE_DOC_URL = ("https://www.usgs.gov/programs/gap-analysis-project/"
                   "science/pad-us-web-services")
LAYER_ID = 0
LAYER_NAME = "PADUS4_1ProtMechCat"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolygon"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/pad-us-protection-mechanism-category-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
PAGE_SIZE = 1000
MAX_BODY_BYTES = 1024 * 1024 * 1024
MIN_FREE_BYTES = 25_000_000_000
# The producer answers gzip when it is requested (observed 2026-11-13); the
# receipt keeps the gzip wire and the decoded identity body side by side.
ACCEPT_ENCODING = "gzip"
# The combined view service document publishes an empty copyrightText and
# serviceDescription (observed 2026-11-14); the producer identity statement is
# carried by the layer document below.
EXPECTED_COPYRIGHT_TEXT = ("U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2025, "
                           "Protected Areas Database of the United States (PAD-US) 4.1: "
                           "U.S. Geological Survey data release, "
                           "https://doi.org/10.5066/P96WBCHS")
# The view service document's own description field is empty as served; it is
# pinned as the observed identity statement, not as a content description.
EXPECTED_SERVICE_DESCRIPTION = ""
EXPECTED_SERVICE_LAYERS = [(0, LAYER_NAME)]
EXPECTED_MAX_RECORD_COUNT = 2000
EXPECTED_SCHEMA_SHA256 = "020f3a55a746d9280b8750d6401a8fdd6c1579ffb6a0c8d325dd854ff4762f4a"
EXPECTED_LAYER_EXTENT = {"xmin": -20037507.0672, "ymin": -1733741.441300001,
                         "xmax": 20037507.067199998, "ymax": 12930675.6069}
BASELINE_PENDING_NOTE = (
    "Completeness uses capture-time source count/ID witnesses and exact page coverage. "
    "No independently approved release baseline is wired; drift comparison is skipped, "
    "not approved.")

NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("FeatClass", "esriFieldTypeString", True, 12),
    ("Category", "esriFieldTypeString", True, 50),
    ("Own_Type", "esriFieldTypeString", True, 50),
    ("Own_Name", "esriFieldTypeString", True, 70),
    ("Loc_Own", "esriFieldTypeString", True, 250),
    ("Mang_Type", "esriFieldTypeString", True, 50),
    ("Mang_Name", "esriFieldTypeString", True, 70),
    ("Loc_Mang", "esriFieldTypeString", True, 250),
    ("Des_Tp", "esriFieldTypeString", True, 75),
    ("Loc_Ds", "esriFieldTypeString", True, 250),
    ("Unit_Nm", "esriFieldTypeString", True, 250),
    ("Loc_Nm", "esriFieldTypeString", True, 250),
    ("State_Nm", "esriFieldTypeString", True, 50),
    ("Agg_Src", "esriFieldTypeString", True, 150),
    ("GIS_Src", "esriFieldTypeString", True, 200),
    ("Src_Date", "esriFieldTypeString", True, 15),
    ("GIS_Acres", "esriFieldTypeInteger", True, None),
    ("Source_PAID", "esriFieldTypeString", True, 100),
    ("WDPA_Cd", "esriFieldTypeInteger", True, None),
    ("Pub_Access", "esriFieldTypeString", True, 20),
    ("Access_Src", "esriFieldTypeString", True, 150),
    ("Access_Dt", "esriFieldTypeString", True, 4),
    ("GAP_Sts", "esriFieldTypeString", True, 95),
    ("GAPCdSrc", "esriFieldTypeString", True, 150),
    ("GAPCdDt", "esriFieldTypeString", True, 4),
    ("IUCN_Cat", "esriFieldTypeString", True, 70),
    ("IUCNCtSrc", "esriFieldTypeString", True, 150),
    ("IUCNCtDt", "esriFieldTypeString", True, 4),
    ("Date_Est", "esriFieldTypeString", True, 4),
    ("Comments", "esriFieldTypeString", True, 255),
    ("EsmtHldr", "esriFieldTypeString", True, 100),
    ("EHoldTyp", "esriFieldTypeString", True, 50),
    ("WebServiceMangName", "esriFieldTypeString", True, 150),
    ("TranspPct", "esriFieldTypeSmallInteger", True, None),
    ("Shape__Area", "esriFieldTypeDouble", True, None),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
)
# The view layer declares no GlobalID field (objectIdField OBJECTID only,
# globalIdField empty as served 2026-11-14).
LAYER_GLOBAL_ID_FIELD = ""
METADATA_NAMES = ("service_doc", "view_item", "layer_sources", "service_before",
                  "layer_before", "count_before", "ids_before")
# Data-page receipts are staged verbatim per page; the bounded source-manifest
# layer represents them with one aggregate input whose digest covers the full
# receipt roster (the catalog reads manifest layers under a 2 MiB bound).
PAGED_INPUT_ID = "pad_us_paged_keyset_queries"


class PadUsProtectionError(ValueError):
    """The source snapshot cannot be represented as the pinned PAD-US candidate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise PadUsProtectionError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _page_url(last_objectid: int) -> str:
    return _query({"where": f"OBJECTID>{last_objectid}", "outFields": "*",
                   "returnGeometry": "true",
                   "outSR": str(SOURCE_CRS_WKID), "orderByFields": "OBJECTID ASC",
                   "resultRecordCount": str(PAGE_SIZE), "f": "json"})


def _ids_url() -> str:
    return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _metadata_urls() -> dict[str, str]:
    return {"service_doc": SERVICE_DOC_URL,
            "view_item": SHARING_ITEM_URL.format(item=VIEW_ITEM_ID),
            "layer_sources": LAYER_URL + "/sources?f=pjson",
            "service_before": SERVICE_URL + "?f=pjson",
            "layer_before": LAYER_URL + "?f=pjson",
            "count_before": _count_url(), "ids_before": _ids_url(),
            "service_after": SERVICE_URL + "?f=pjson",
            "layer_after": LAYER_URL + "?f=pjson",
            "count_after": _count_url(), "ids_after": _ids_url()}


def _expected_capture_names(count: int) -> list[str]:
    data_pages = (count + PAGE_SIZE - 1) // PAGE_SIZE
    return [*METADATA_NAMES, *(f"page_{index:06d}" for index in range(data_pages)),
            "page_tail", "layer_after", "service_after", "count_after", "ids_after"]


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGlobalID": pa.string()}.get(str(field["type"]), pa.string())


_WKB_POLYGON = 3
_WKB_MULTIPOLYGON = 6
_GEOMETRY_COLUMN = "geometry"


def _validate_polygon_geometry(geometry: object, rings: list) -> None:
    """Structural contract for one served polygon geometry (no assembly).

    Esri JSON (verified on the 2026-11-13 probes of the superseded service and
    on the 2026-11-14 probes of this view): the layer has no Z or M, every
    vertex carries exactly two finite ordinates in the source lon/lat domain
    and every ring is explicitly closed. Structurally invalid geometry refuses
    the capture with the pinned codes. Ring orientation and hole assignment
    are GDAL's (shared native Esri polygon conversion); OGC-invalid-but-
    representable producer polygons are retained with a counted diagnostic.
    """
    if not isinstance(geometry, dict) or set(geometry) - {"rings"}:
        raise PadUsProtectionError("unexpected_source_geometry_members")
    if not isinstance(rings, list) or not rings:
        raise PadUsProtectionError("source_polygon_rings_missing")
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise PadUsProtectionError("source_polygon_ring_invalid")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise PadUsProtectionError("source_polygon_ordinate_contract_drift")
            x, y = vertex
            if not (type(x) in (int, float) and type(y) in (int, float)):
                raise PadUsProtectionError("source_polygon_ordinate_not_numeric")
            if not (x == x and y == y and -1e308 < x < 1e308 and -1e308 < y < 1e308):
                raise PadUsProtectionError("source_polygon_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise PadUsProtectionError("source_coordinate_range_drift")
        if ring[0] != ring[-1]:
            raise PadUsProtectionError("source_polygon_ring_not_closed")


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise PadUsProtectionError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise PadUsProtectionError("source_native_type_changed:" + name)
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise PadUsProtectionError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise PadUsProtectionError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeGlobalID":
        if not isinstance(value, str) or not value:
            raise PadUsProtectionError("source_native_type_changed:" + name)
        return
    raise PadUsProtectionError("unsupported_source_physical_type:" + esri_type)


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      last_objectid: int) -> tuple[int, int]:
    if not isinstance(feature, dict):
        raise PadUsProtectionError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or list(attrs) != list(expected_names):
        raise PadUsProtectionError("source_record_schema_or_geometry_drift")
    objectid = attrs["OBJECTID"]
    if type(objectid) is not int or objectid <= 0:
        raise PadUsProtectionError("source_object_id_type_changed")
    if objectid <= last_objectid:
        raise PadUsProtectionError("source_object_ids_not_strictly_increasing")
    vertex_count = 0
    if geometry is not None:
        rings = geometry.get("rings") if isinstance(geometry, dict) else None
        if not isinstance(rings, list):
            raise PadUsProtectionError("unexpected_source_geometry_members")
        vertex_count = sum(len(ring) for ring in rings)
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    return objectid, vertex_count


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PadUsProtectionError("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise PadUsProtectionError("retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    _load_receipt(capture_root, name)
    try:
        return load_replay(capture_root, name)
    except ValueError as exc:
        raise PadUsProtectionError(str(exc)) from exc


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"data_pages": [], "tail": None, "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PadUsProtectionError("capture_state_invalid") from exc
    if (not isinstance(state, dict) or not isinstance(state.get("data_pages"), list)
            or not (state.get("tail") is None or isinstance(state.get("tail"), dict))):
        raise PadUsProtectionError("capture_state_invalid")
    return state


def _retained_or_fetch(client: httpx.Client | None, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except PadUsProtectionError:
            if client is None:
                raise
            for suffix in ("body", "wire", "receipt.json"):
                (capture_root / f"{name}.{suffix}").unlink(missing_ok=True)
    if client is None:
        raise PadUsProtectionError("retained_capture_file_missing:" + name)
    return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES,
                       accept_encoding=ACCEPT_ENCODING)


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
        raise PadUsProtectionError("source_page_error")


def _load_receipt_map(capture_root: Path, count: int) -> dict[str, dict[str, object]]:
    """Load every expected receipt without holding any page body in memory."""
    return {name: _load_receipt(capture_root, name)
            for name in _expected_capture_names(count)}


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


def _validate_item_document(item: object, item_id: str, label: str) -> None:
    """Validate one USGS-owned ArcGIS Online item identity document."""
    if not isinstance(item, dict):
        raise PadUsProtectionError("usgs_arcgis_item_document_invalid:" + label)
    if item.get("id") != item_id or item.get("orgId") != "v01gqwM5QqNysAAi":
        raise PadUsProtectionError("usgs_arcgis_item_identity_drift:" + label)
    owner = str(item.get("owner", ""))
    if not owner.endswith("@usgs.gov_USGS"):
        raise PadUsProtectionError("usgs_arcgis_item_owner_drift:" + label)


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise PadUsProtectionError("source_service_or_layer_document_invalid")
    # The combined view service document carries an empty copyrightText and
    # serviceDescription as served (observed 2026-11-14); the producer
    # identity statement is pinned on the layer document instead.
    # The combined view service document carries an empty copyrightText and
    # serviceDescription as served (observed 2026-11-14); the producer
    # identity statement is pinned on the layer document instead.
    if service.get("copyrightText") != "":
        raise PadUsProtectionError("usgs_padus_service_identity_drift:" + label)
    if str(service.get("serviceDescription", "")) != EXPECTED_SERVICE_DESCRIPTION:
        raise PadUsProtectionError("usgs_padus_service_description_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if roster != EXPECTED_SERVICE_LAYERS:
        raise PadUsProtectionError("usgs_padus_service_layer_roster_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise PadUsProtectionError("source_service_spatial_reference_drift:" + label)


def _validate_sources_document(document: object) -> None:
    """Pin the actual view-to-input relationship of the USGS-linked view.

    The combined view's ``/0/sources`` document names the source layer that
    backs it; it is the anonymously readable evidence that this pipeline
    captures the producer's maintained view rather than an unrelated service.
    The source service itself answers anonymous requests with permission
    errors (item HTTP 403, service token-required, observed 2026-11-14), so
    only this document is retained.
    """
    if not isinstance(document, dict):
        raise PadUsProtectionError("usgs_padus_sources_document_invalid")
    layers = document.get("layers")
    if not isinstance(layers, list) or len(layers) != 1 or not isinstance(layers[0], dict):
        raise PadUsProtectionError("usgs_padus_sources_document_invalid")
    entry = layers[0]
    if (entry.get("name") != LAYER_NAME
            or entry.get("serviceItemId") != SOURCE_SERVICE_ITEM_ID
            or entry.get("url") != SOURCE_LAYER_URL):
        raise PadUsProtectionError("usgs_padus_view_source_relationship_drift")


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise PadUsProtectionError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise PadUsProtectionError("exact_padus_protection_mechanism_layer_metadata_drift")
    if layer.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise PadUsProtectionError("usgs_padus_layer_identity_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise PadUsProtectionError("source_layer_z_or_m_contract_drift")
    if (layer.get("objectIdField") != "OBJECTID"
            or layer.get("globalIdField") != LAYER_GLOBAL_ID_FIELD):
        raise PadUsProtectionError("source_id_field_changed")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise PadUsProtectionError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT \
            or PAGE_SIZE > EXPECTED_MAX_RECORD_COUNT:
        raise PadUsProtectionError("source_record_limit_drift")
    source_reference = layer.get("sourceSpatialReference")
    if (source_reference is not None
            and (not isinstance(source_reference, dict)
                 or source_reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])
                 or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"])):
        raise PadUsProtectionError("source_layer_spatial_reference_drift")
    reference = layer.get("spatialReference")
    if not isinstance(reference, dict) \
            or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise PadUsProtectionError("source_layer_extent_spatial_reference_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise PadUsProtectionError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise PadUsProtectionError("source_layer_extent_drift")
    return _profile_schema(layer)


def _validate_ids_roster(roster: object, label: str) -> list[int]:
    if (not isinstance(roster, list) or not roster
            or any(type(value) is not int or value <= 0 for value in roster)
            or len(set(roster)) != len(roster)):
        raise PadUsProtectionError("source_id_population_drift:" + label)
    return sorted(roster)


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained page byte and optionally reconstruct the native geoparquet."""
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    service_doc_body = _read_capture(capture_root, "service_doc")[0]
    # The USGS GAP program page (the retained service documentation) links the
    # PAD-US ArcGIS Online item by id (usgs.maps.arcgis.com/home/item.html?id=
    # <VIEW_ITEM_ID>, observed 2026-11-14 in the first complete capture); it
    # never embeds the REST endpoint string, whose identity stays pinned by the
    # retained service/layer documents and the endpoint receipts.
    if VIEW_ITEM_ID.encode("utf-8") not in service_doc_body:
        raise PadUsProtectionError("usgs_service_documentation_link_drift")
    _validate_item_document(docs["view_item"], VIEW_ITEM_ID, "view_item")
    _validate_sources_document(docs["layer_sources"])
    service, layer = docs["service_before"], docs["layer_before"]
    _validate_service_document(service, "service_before")
    fields, schema_sha = _validate_layer_document(layer)
    declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise PadUsProtectionError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise PadUsProtectionError("source_schema_baseline_drift")
    count = docs["count_before"].get("count")
    roster_before = _validate_ids_roster(docs["ids_before"].get("objectIds"), "ids_before")
    for name in ("layer_after", "service_after", "count_after", "ids_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before", "ids_after": "ids_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise PadUsProtectionError("source_metadata_or_count_changed_during_capture")
    roster_after = _validate_ids_roster(
        json.loads(_read_capture(capture_root, "ids_after")[0]).get("objectIds"), "ids_after")
    validate_capture_witness(count, roster_before,
                             json.loads(_read_capture(capture_root, "count_after")[0]).get("count"),
                             roster_after)
    for name, url in _metadata_urls().items():
        validate_capture_request_identity(name, _load_receipt(capture_root, name), url)
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise PadUsProtectionError("source_field_schema_changed_during_capture")
    tail = json.loads(_read_capture(capture_root, "page_tail")[0])
    if tail.get("features") != []:
        raise PadUsProtectionError("source_tail_page_not_empty")

    expected_names = tuple(entry["name"] for entry in fields)
    arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True) for field in fields]
    arrow_fields.append(pa.field(_GEOMETRY_COLUMN, pa.binary(), nullable=True))
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": _GEOMETRY_COLUMN,
           "columns": {_GEOMETRY_COLUMN: {"encoding": "WKB",
                                          "geometry_types": ["Polygon", "MultiPolygon"],
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
    invalid_geometry_count = 0
    ring_count = 0
    vertex_count = 0
    ogc_invalid_geometry: dict[str, int] = {}
    ogc_invalid_sample_objectids: list[int] = []
    featclass_distribution: dict[object, int] = {}
    bounds_extrema: list[float] | None = None
    page_bodies = [name for name in _expected_capture_names(count)
                   if name.startswith("page_") and name != "page_tail"]
    try:
        if any(not (capture_root / f"{name}.{suffix}").is_file()
               for name in page_bodies for suffix in ("body", "wire", "receipt.json")):
            raise PadUsProtectionError("source_page_population_incomplete")
        for index, page_name in enumerate(page_bodies):
            body, receipt = _read_capture(capture_root, page_name)
            validate_capture_request_identity(page_name, receipt, _page_url(last_objectid))
            page = json.loads(body)
            if not isinstance(page, dict) or "error" in page:
                raise PadUsProtectionError("source_page_error:" + page_name)
            reference = page.get("spatialReference")
            if (not isinstance(reference, dict)
                    or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                    or reference.get("latestWkid") != SOURCE_CRS_WKID):
                raise PadUsProtectionError("source_page_spatial_reference_drift:" + page_name)
            if page.get("objectIdFieldName") != "OBJECTID":
                raise PadUsProtectionError("source_id_field_changed:" + page_name)
            # The producer's query responses omit the optional hasZ/hasM flags
            # (observed 2026-11-14 across repeated page probes); the non-Z/M
            # shape stays pinned by the layer document, and an explicit true
            # remains drift.
            if page.get("hasZ", False) is not False or page.get("hasM", False) is not False:
                raise PadUsProtectionError("source_page_z_or_m_flag_drift:" + page_name)
            features = page.get("features")
            if not isinstance(features, list) or not features \
                    or len(features) > PAGE_SIZE:
                raise PadUsProtectionError("source_page_row_count_incomplete:" + page_name)
            if len(features) < PAGE_SIZE and page.get("exceededTransferLimit") is True:
                raise PadUsProtectionError("source_page_transfer_truncated:" + page_name)
            expected_ids = roster_before[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
            if len(features) != len(expected_ids):
                raise PadUsProtectionError("source_page_row_count_incomplete:" + page_name)
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
                    rings = geometry["rings"]
                    ring_count += len(rings)
                    vertex_count += feature_vertices
                    for ring in rings:
                        for vertex in ring:
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
                    _validate_polygon_geometry(geometry, rings)
                    # Ring orientation and hole assignment are GDAL's through
                    # the shared native Esri polygon conversion; a conversion
                    # failure refuses the capture. OGC self-intersection and
                    # similar defects are producer facts of the faithful
                    # capture: retained verbatim in digests and geometry,
                    # counted by reason, never repaired or coerced.
                    wkb = to_wkb(geometry, GEOMETRY_TYPE, has_z=False, has_m=False)
                    geometries.append(wkb)
                    reason = _shapely_is_valid_reason(_shapely_from_wkb(wkb))
                    if reason != "Valid Geometry":
                        ogc_invalid_geometry[reason] = \
                            ogc_invalid_geometry.get(reason, 0) + 1
                        if len(ogc_invalid_sample_objectids) < 20:
                            ogc_invalid_sample_objectids.append(objectid)
                        invalid_geometry_count += 1
                    geometry_hash.update(_canonical([objectid, rings]) + b"\x00")
                featclass = attrs["FeatClass"]
                featclass_distribution[featclass] = \
                    featclass_distribution.get(featclass, 0) + 1
                for name in expected_names:
                    columns[name].append(attrs[name])
            if columns["OBJECTID"] != expected_ids:
                raise PadUsProtectionError("source_id_roster_drift")
            if writer is not None:
                batch = pa.Table.from_pydict({**columns, _GEOMETRY_COLUMN: geometries},
                                             schema=schema)
                writer.write_table(batch)
        if writer is not None:
            writer.close()
            writer = None
        if len(ids) != count or last_objectid == 0:
            raise PadUsProtectionError("source_page_population_incomplete")
        validate_capture_witness(count, roster_before, count, roster_after, list(ids))
        validate_capture_request_identity("page_tail", _load_receipt(capture_root, "page_tail"),
                                         _page_url(last_objectid))
        ids_sha = _sha(_canonical(list(ids)))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
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
    return {"feature_count": count, "source_reported_count": count,
            "source_population_complete": True,
            "fields": fields, "schema_sha": schema_sha, "record_sha": record_sha,
            "geometry_sha": geometry_sha, "ids_sha": ids_sha,
            "null_geometry_count": null_geometry_count, "parquet_path": sink_path,
            "invalid_geometry_count": invalid_geometry_count,
            "ring_count": ring_count, "vertex_count": vertex_count,
            "unusable_geometry": {},
            "ogc_invalid_geometry": ogc_invalid_geometry,
            "ogc_invalid_sample_objectids": ogc_invalid_sample_objectids,
            "featclass_distribution": featclass_distribution, "observed_bounds": bounds,
            "max_objectid": last_objectid}


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def _parquet_descriptor(path: Path) -> dict[str, object]:
    """Size and digest of a candidate parquet without holding it in memory."""
    return {"bytes": path.stat().st_size, "sha256": sha256_file(path)}




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


def _keyset_before(retained: dict) -> int:
    count = json.loads(retained["count_before"][0]).get("count")
    roster = _validate_ids_roster(
        json.loads(retained["ids_before"][0]).get("objectIds"), "ids_before")
    try:
        validate_capture_witness(count, roster, count, roster)
    except ValueError as exc:
        raise PadUsProtectionError(str(exc)) from exc
    return count


def _keyset_cursor(features: list[dict], last_objectid: int) -> int:
    for feature in features:
        last_objectid, _vertices = _validate_feature(
            feature, tuple(entry[0] for entry in NATIVE_FIELDS), last_objectid)
    return last_objectid


def _capture_capacity_budget(root: Path) -> int:
    free = shutil.disk_usage(root).free
    if free < MIN_FREE_BYTES:
        raise PadUsProtectionError("disk_floor_before_acquisition")
    retained = sum(path.stat().st_size for path in root.iterdir()
                   if path.is_file() and path.name.endswith((".body", ".wire")))
    return retained + max(0, free - MIN_FREE_BYTES)


def fetch_pad_us_protection_mechanism_category_pages(capture_root: Path, *, client: httpx.Client | None = None,
                             doc_client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    with capture_retention_budget(capture_root, _capture_capacity_budget(capture_root)):
        pages, tail, first_captured_at = capture_fixed_keyset_layer(
            capture_root, client=client, doc_client=doc_client,
            load_state=_load_state, save_state=_save_state,
            default_client=_default_client, document_client=_doc_client,
            retained_or_fetch=_retained_or_fetch, reject_error_response=_reject_error_response,
            witness=_witness_error_response, validate_cursor=_keyset_cursor,
            page_url=_page_url, read_capture=_read_capture,
            metadata_urls={name: _metadata_urls()[name] for name in METADATA_NAMES},
            closing_urls={"layer_after": LAYER_URL + "?f=pjson",
                          "service_after": SERVICE_URL + "?f=pjson",
                          "count_after": _count_url(), "ids_after": _ids_url()},
            error_type=PadUsProtectionError, page_size=PAGE_SIZE,
            validate_before=_keyset_before,
            page_count_error="source_page_count_exceeds_reported_population")

    last_captured_at = str(tail["captured_at"])
    # Construction happens once at seal time (capture_pad_us_protection_mechanism_category);
    # retention and validation stay the only fetch-time side effects.
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": extracted["feature_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_unusable_geometry_counts": {},
        "source_ogc_invalid_geometry_counts": dict(sorted(extracted["ogc_invalid_geometry"].items())),
        "source_ogc_invalid_geometry_sample_objectids": extracted["ogc_invalid_sample_objectids"],
        "source_invalid_geometry_count": extracted["invalid_geometry_count"],
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
    def verify_existing():
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_ids_sha256": extracted["ids_sha"],
             "source_schema_sha256": extracted["schema_sha"],
             "feature_count": extracted["feature_count"],
             "source_reported_count": extracted["source_reported_count"],
             "null_geometry_count": extracted["null_geometry_count"]},
            parquet_descriptor)

    reused = reuse_sealed_candidate(storage, state_key, verify_existing)
    if reused is not None:
        return reused
    audit = {
        "source_feature_count": extracted["feature_count"], "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Polygon / MultiPolygon",
        "null_geometry_count": extracted["null_geometry_count"],
        "unique_source_id_count": extracted["feature_count"],
        "producer_invalid_geometry_count": extracted["invalid_geometry_count"],
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_featclass_distribution": extracted["featclass_distribution"],
        "unusable_geometry_feature_counts": {},
        "ogc_invalid_geometry_counts": dict(sorted(extracted["ogc_invalid_geometry"].items())),
        "ogc_invalid_geometry_sample_objectids": extracted["ogc_invalid_sample_objectids"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "view_item_id": VIEW_ITEM_ID,
        "source_service_item_id": SOURCE_SERVICE_ITEM_ID,
        "source_layer_url": SOURCE_LAYER_URL,
        "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_row_note": "The DHS HIFLD Open crosswalk (created by DHS in August 2025) "
                              "links HIFLD Open layers to their producing agencies; the "
                              "2026-10-20 research snapshot matched this inventory entry to "
                              "USGS GAP's PAD-US publications and the exact crosswalk row "
                              "text was not re-verified in this session.",
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": True,
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The service publishes no per-snapshot release date: the layer copyrightText "
            "cites the PAD-US 4.1 data release (2025, DOI 10.5066/P96WBCHS), the layer is "
            "named PADUS4_1ProtMechCat, and the service data was last edited 2025-06-04 "
            "(editingInfo.dataLastEditDate; schema last edit 2026-04-22). The exact "
            "source release date is an unaccepted maintainer freshness decision.",
        "pagination": {"mode": "objectid_keyset_asc", "page_size": PAGE_SIZE,
                       "pages": (extracted["feature_count"] + PAGE_SIZE - 1) // PAGE_SIZE,
                       "tail_witness_page": True, "resumable": True,
                       "resumed_pages": resumed_pages,
                       "id_roster_enumeration_note":
                           "Completeness uses stable before/after source count and unique ID "
                           "witnesses, exact sorted-roster page coverage, the strict "
                           "ascending OBJECTID walk, a source-derived page ceiling and "
                           "an empty tail-page witness.",
                       "content_coding": ACCEPT_ENCODING,
                       "gzip_note": "The producer answers gzip when requested (observed "
                                    "2026-11-13); each receipt retains the gzip wire and the "
                                    "decoded identity body."},
        "capture_window_utc": {"first_page": first_captured_at, "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    source_objects = {parquet_name: parquet_descriptor}
    receipts = _load_receipt_map(capture_root, extracted["feature_count"])
    _unknown_release_reason = (
        "The service publishes no per-snapshot release date; its layer copyrightText "
        "records the PAD-US 4.1 data release (2025, DOI 10.5066/P96WBCHS) and its data "
        "was last edited 2025-06-04 (editingInfo.dataLastEditDate). The exact source "
        "release date is an unaccepted maintainer freshness decision.")
    _input_license = ("Public domain (U.S. Government work) per the producer metadata; "
                      "rights acceptance pending.")
    _input_attribution = ("USGS Gap Analysis Project (GAP), Protected Areas Database of the "
                          "United States (PAD-US); formal attribution terms require "
                          "maintainer review.")

    def _input_entry(name, receipt):
        return {"input_id": name, "producer": PUBLISHER,
                "host": urlsplit(receipt["url"]).hostname,
                "role": "producer_publication", "canonical_url": SERVICE_URL,
                "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                "release_date": None,
                "release_date_unknown_reason": _unknown_release_reason,
                "license": _input_license, "attribution": _input_attribution}

    page_names = sorted(name for name in receipts if name.startswith("page_"))
    if not page_names:
        raise PadUsProtectionError("paged_capture_inputs_missing")
    if {receipts[name]["status"] for name in page_names} != {200}:
        raise PadUsProtectionError("paged_capture_http_status_drift")
    page_roster_sha = _sha(_canonical([receipts[name] for name in page_names]))
    page_entry = {"input_id": PAGED_INPUT_ID, "producer": PUBLISHER,
                  "host": urlsplit(receipts[page_names[0]]["url"]).hostname,
                  "role": "producer_publication",
                  "canonical_url": SERVICE_URL,
                  "retrieval_url": receipts[page_names[0]]["url"],
                  "observed_final_url": receipts[page_names[0]]["final_url"],
                  "retrieved_at": receipts[page_names[0]]["observed_at"],
                  "http_status": 200,
                  "bytes": sum(int(receipts[name]["bytes"]) for name in page_names),
                  "sha256": page_roster_sha,
                  "page_query_count": len(page_names),
                  "page_capture_window": [min(receipts[name]["observed_at"] for name in page_names),
                                          max(receipts[name]["observed_at"] for name in page_names)],
                  "page_receipts_sha256_basis": "SHA-256 over the canonical JSON array of the "
                                                "per-page receipt objects in ascending page name "
                                                "order, staged verbatim as raw/<name>.receipt.json "
                                                "next to each page's raw gzip wire and decoded body",
                  "release_date": None,
                  "release_date_unknown_reason": _unknown_release_reason,
                  "license": _input_license, "attribution": _input_attribution}
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "PAD-US Protection Mechanism Category (USGS GAP)",
        "description": "USGS Gap Analysis Project (GAP) PAD-US Protection Mechanism Category "
                       "feature-layer view (services.arcgis.com "
                       "Protection_Mechanism_Category_PADUS/FeatureServer/0, layer "
                       "PADUS4_1ProtMechCat; USGS-owned ArcGIS view item " + VIEW_ITEM_ID +
                       ", whose anonymously readable /0/sources document names the "
                       "underlying source layer " + SOURCE_LAYER_URL + ", hosted service "
                       "item " + SOURCE_SERVICE_ITEM_ID + "), captured completely from the "
                       "producer's own REST publication as the maintained USGS-linked "
                       "publication named by the HIFLD Open inventory entry "
                       "pad-us-protection-mechanism-category. Complete source attributes and "
                       "EPSG:4326 polygon geometry are represented in the candidate. The "
                       "layer copyright cites PAD-US 4.1 (2025, DOI 10.5066/P96WBCHS) and "
                       "its data was last edited 2025-06-04; schema/ID contract, coverage, "
                       "rights and release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer metadata states no access "
                   "or use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [_input_entry(name, receipt) for name, receipt in sorted(receipts.items())
                   if not name.startswith("page_")] + [page_entry],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": "1=1 over the complete keyset-ordered "
                                                          "OBJECTID walk (full PAD-US Protection "
                                                          "Mechanism Category population)",
                                            "source_fields": "all producer fields retained in "
                                                             "producer order and declared physical "
                                                             "types, including the service-generated "
                                                             "Shape__Area and Shape__Length doubles",
                                             "source_ids": "Preserve producer OBJECTID values "
                                                           "unchanged (the view layer declares "
                                                           "no GlobalID field); no substitute "
                                                           "IDs, casts or joins",
                                            "geometry": "Source EPSG:4326 polygon rings retained "
                                                        "as WKB without projection or repair under "
                                                        "the producer's verified ring semantics "
                                                        "(closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes, 2-ordinate "
                                                        "vertices); null source geometry retained "
                                                        "as null; GDAL converts served rings "
                                                        "without repair, and OGC-invalid geometry "
                                                        "remains in the candidate with observed "
                                                        "diagnostics rather than null coercion",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry pad-us-protection-mechanism-category "
                   "(agency and publisher US Geological Survey, USGS Gap Analysis Project) is "
                   "the producer's combined protection-mechanism view (fee simple, internal "
                   "designations, easements, leases and agreements, marine areas, proclamation "
                   "outlines). USGS's own PAD-US Web Services page links the feature-layer view "
                   "(ArcGIS item 80c7a138989c4cf8937c4502371d6e99, owner "
                   "mcroft@usgs.gov_USGS, org v01gqwM5QqNysAAi); that item's service URL is the "
                   "combined view Protection_Mechanism_Category_PADUS/FeatureServer (layer "
                   "PADUS4_1ProtMechCat), whose anonymously readable /0/sources document names "
                   "the underlying source layer ProtMechCat_A/FeatureServer/0 (hosted service "
                   "item 126eef9409ce48b0a9e55c2fe8fd77b8; both that item document and the "
                   "source service answer anonymous requests with permission errors, so only "
                   "the sources document is retained as the view-to-input relationship "
                   "evidence). The layer copyright cites PAD-US 4.1 (2025, DOI 10.5066/P96WBCHS "
                   "- the release DOI of the PAD-US 4.1 Full Inventory Database, ScienceBase "
                   "item 652d4fc5d34e44db0e2ee45e, observed HTTP 200 anonymously on "
                   "2026-11-13) and the service data was last edited 2025-06-04 "
                   "(editingInfo.dataLastEditDate). The previously captured combined service "
                   "Protection_Mechanism_Category/FeatureServer (hosted item "
                   "22670023fd124c799d5ddd08297dde85; 441,609 features; layer self-labeled "
                   "PAD-US v3.0 with copyright citing the 2022 PAD-US 3.0 release, DOI "
                   "10.5066/P9Q9LQ4B; data last edited 2024-03-20; USGS FGDC metadata "
                   "describing it as PAD-US 4.0, publication date 2024-04-25) is superseded by "
                   "this USGS-linked view and stays research context only. The DataLumos "
                   "project 240685 archive copy (archive modification 2025-06-04) is "
                   "identification and comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and "
                      "polygon geometry retained in the producer page CRS; all succession, "
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
    stage_candidate_objects(
        storage, prefix, state_key, version, identity, metadata_outputs,
        capture_root, receipts, _read_capture,
        descriptors=descriptors,
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'])
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": extracted["feature_count"],
            "source_reported_count": extracted["source_reported_count"],
            "source_population_complete": True,
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
    fingerprint, payload = load_candidate_checkpoint(
        storage, state_key, version, identity,
        audit['source_record_sha256'], audit['source_schema_sha256'], PadUsProtectionError)
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != dict(current_parquet):
        raise PadUsProtectionError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"],
            "source_reported_count": audit["source_reported_count"],
            "source_population_complete": True,
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_id_sha256": audit["source_ids_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}


def capture_pad_us_protection_mechanism_category(storage: StagingStorageResource, *, version: str,
                         capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise PadUsProtectionError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise PadUsProtectionError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise PadUsProtectionError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise PadUsProtectionError("disk_floor_before_acquisition")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (default_capture_dir(DATASET_SLUG, "captures") / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise PadUsProtectionError("unsafe_capture_directory")
    resumed_pages = len(_load_state(capture_root).get("data_pages", [])) \
        if capture_root.exists() else 0
    try:
        fetch_pad_us_protection_mechanism_category_pages(capture_root)
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
            except (OSError, PadUsProtectionError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_pad_us_protection_mechanism_category_baseline(capture_root: str) -> dict[str, object]:
    """Recompute observed content diagnostics from a completed capture."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": extracted["feature_count"],
            "source_max_objectid": extracted["max_objectid"]}
