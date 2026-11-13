"""Streaming, approval-gated capture of the PAD-US Protection Mechanism Category layer.

The HIFLD Open inventory entry ``pad-us-protection-mechanism-category``
("PAD-US Protection Mechanism Category", agency and publisher US Geological
Survey, USGS Gap Analysis Project) is the producer's combined
fee/designation/easement/marine/proclamation protection-mechanism view. USGS's
own PAD-US Web Services page links the feature-layer view (ArcGIS item
``80c7a138989c4cf8937c4502371d6e99``, owned by a usgs.gov account in USGS's
ArcGIS Online organization ``v01gqwM5QqNysAAi``), backed by the USGS-owned
hosted service item ``22670023fd124c799d5ddd08297dde85``; its public layer is
``services.arcgis.com`` ``Protection_Mechanism_Category/FeatureServer/0``.
That explicitly USGS-linked publication is captured here; the DataLumos project
240685 archive copy is identification context only and is never fetched.

Version relationship (observed 2026-11-13, documented, unaccepted): the
service's own labels are stale producer labels — the layer is named
"...(PAD-US) v3.0" and the copyright cites the 2022 PAD-US 3.0 release (DOI
10.5066/P9Q9LQ4B) — while the service's data was last edited 2024-03-20
(``editingInfo.lastEditDate``) and USGS's published FGDC metadata for this
service describes it as PAD-US 4.0 (publication date 2024-04-25, processing
environment ``PADUS4_0Geodatabase.gdb``). The PAD-US 4.1 Full Inventory
Database (2024, DOI 10.5066/P96WBCHS, ScienceBase item
``652d4fc5d34e44db0e2ee45e``, observed HTTP 200 anonymously on 2026-11-13) is
the current national database release, but it publishes the separate PAD-US
feature classes, not this combined producer view, so the draft pins the
maintained USGS-linked view service as the source and records the version gap
as an unaccepted maintainer freshness/succession decision.

The layer held 441,609 features on 2026-11-13 with a contiguous OBJECTID
roster 1..441,609 (``returnIdsOnly`` works for this producer), so completeness
is established by the pinned count, the pinned ID-roster digest (captured
before and after the walk), the strict ascending OBJECTID keyset walk, the
page-count ceiling, per-page ``exceededTransferLimit`` consistency and the
empty tail-page witness. The per-value and per-geometry baseline digests stay
unpinned until the first complete capture; they are computed and reported by
``build_pad_us_protection_mechanism_category_baseline`` and then pinned as
constants for later drift refusal.

Scale note (observed 2026-11-13): the population's geometry weight is heavily
skewed — the first ~1,000 features alone transfer ~82 MB of gzip wire (~251 s)
while most later pages run ~0.3-0.8 MB per 1,000 features — so the module
requests gzip content coding and retains both the gzip wire and the decoded
identity body per page. The capture state file makes every session resumable;
the disk floor below refuses to start where the complete capture cannot fit.
Retaining the raw pages inside the sealed candidate follows the repository's
established fidelity contract. Publication stays blocked pending the
succession, freshness, schema/ID contract, rights and release decisions; all
runtime approval flags are false and the draft refresh schedule is stopped.
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
import shapely
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
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "pad-us-protection-mechanism-category"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = ("https://services.arcgis.com/v01gqwM5QqNysAAi"
               "/ArcGIS/rest/services/Protection_Mechanism_Category/FeatureServer")
# USGS-owned ArcGIS Online items captured as identity evidence (the view item
# linked by USGS's PAD-US Web Services page and the hosted service item that
# backs it; both owners are usgs.gov accounts in the USGS organization).
VIEW_ITEM_ID = "80c7a138989c4cf8937c4502371d6e99"
SERVICE_ITEM_ID = "22670023fd124c799d5ddd08297dde85"
SHARING_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/{item}?f=json"
SERVICE_DOC_URL = ("https://www.usgs.gov/programs/gap-analysis-project/"
                   "science/pad-us-web-services")
LAYER_ID = 0
LAYER_NAME = "Protected Areas Database of the United States (PAD-US) v3.0"
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
# The service's own copyright label is a stale producer label (observed
# 2026-11-13): it cites the 2022 PAD-US 3.0 release while the service data was
# last edited 2024-03-20 and USGS's published metadata describes the service as
# PAD-US 4.0. It is pinned as the observed identity statement, not as a
# freshness or release claim.
EXPECTED_COPYRIGHT_TEXT = ("U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, "
                           "Protected Areas Database of the United States (PAD-US) 3.0: "
                           "U.S. Geological Survey (USGS), https://doi.org/10.5066/P9Q9LQ4B .")
EXPECTED_SERVICE_DESCRIPTION = ("Protection mechanism category-fee simple, internal designations, "
                                "easements, leases and agreements, and Marine Areas. "
                                "Proclamation outlines.")
EXPECTED_SERVICE_LAYERS = [(0, LAYER_NAME)]
EXPECTED_COUNT = 441_609
EXPECTED_SCHEMA_SHA256 = "2afc737fbcf4bca1d622a0b73582d1fc2a643ae280cfbadcc10bb3a04125310e"
EXPECTED_IDS_SHA256 = "6971d2092b3ae91a8f3ec092687fd184a1057ce622d832e9686e9b6fa677d765"
# The per-value and per-geometry baselines cannot be computed without a
# complete capture of the 441,609-feature population; they are pinned here as
# empty (checked only when non-empty) and recorded by the first complete
# capture for the maintainer to pin.
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = 0
# Producer-served features whose rings cannot form a valid OGC polygon are
# gated, never silently coerced. No such feature was observed in the bounded
# 2026-11-13 probes (4,098 rings over 3,200 features), so the pinned counts
# start empty; any occurrence in the first complete capture fails closed and
# is then recorded for the maintainer decision.
UNUSABLE_GEOMETRY_MODES = frozenset({
    "source_polygon_ring_degenerate", "source_polygon_exterior_missing",
    "source_polygon_hole_unassigned",
})
EXPECTED_UNUSABLE_GEOMETRY_COUNTS: dict[str, int] = {}
EXPECTED_LAYER_EXTENT = {"xmin": -20037507.0642, "ymin": -1733741.441300001,
                         "xmax": 20037507.140600003, "ymax": 12930677.553099995}
BASELINE_PENDING_NOTE = (
    "The record and geometry baseline digests are not pinned yet: the "
    "441,609-feature population cannot be captured inside one session, so no "
    "complete capture existed when this draft was implemented (observed "
    "2026-11-13). build_pad_us_protection_mechanism_category_baseline computes them from a completed "
    "resumable capture directory for the maintainer to pin.")

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
    ("WebMercAc", "esriFieldTypeInteger", True, None),
    ("Shape__Area", "esriFieldTypeDouble", True, None),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
    ("GlobalID", "esriFieldTypeGlobalID", False, 38),
)
METADATA_NAMES = ("service_doc", "view_item", "service_item", "service_before",
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


def _polygon_wkb(geometry: object) -> tuple[bytes | None, str | None]:
    """WKB for one feature's Esri rings under the producer's verified ring semantics.

    Esri JSON (verified on the 2026-11-13 probes: 4,098 rings over 3,200
    features): the layer has no Z or M, every vertex carries exactly two
    ordinates, every ring is explicitly closed, clockwise rings are exteriors
    and counter-clockwise rings are holes. Vertices are passed to shapely
    verbatim; exteriors with no claimable hole keep their ring order and holes
    join their smallest containing exterior. Nothing is projected, quantized,
    dropped or repaired. Returns the WKB plus the unusable-geometry failure
    mode when the producer served rings that cannot form a valid OGC polygon
    (gated to null candidate geometry, never coerced).
    """
    if geometry is None:
        return None, None
    if not isinstance(geometry, dict) or set(geometry) - {"rings"}:
        raise PadUsProtectionError("unexpected_source_geometry_members")
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise PadUsProtectionError("source_polygon_rings_missing")
    exteriors: list[list] = []
    holes: list[list] = []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise PadUsProtectionError("source_polygon_ring_invalid")
        area = 0.0
        for index, vertex in enumerate(ring):
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise PadUsProtectionError("source_polygon_ordinate_contract_drift")
            x, y = vertex
            if not (type(x) in (int, float) and type(y) in (int, float)):
                raise PadUsProtectionError("source_polygon_ordinate_not_numeric")
            if not (x == x and y == y and -1e308 < x < 1e308 and -1e308 < y < 1e308):
                raise PadUsProtectionError("source_polygon_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise PadUsProtectionError("source_coordinate_range_drift")
            nxt = ring[(index + 1) % len(ring)]
            area += x * nxt[1] - nxt[0] * y
        area /= 2
        if ring[0] != ring[-1]:
            raise PadUsProtectionError("source_polygon_ring_not_closed")
        if area == 0:
            return None, "source_polygon_ring_degenerate"
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        return None, "source_polygon_exterior_missing"
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    # Prepared shell geometries: hole containment is queried against every
    # exterior, and large combined features can carry many holes; preparation
    # builds an index only and the containment results are identical.
    for shell, _area, _index in shells:
        shapely.prepare(shell)
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        probe = Polygon(hole).representative_point()
        candidates = [(abs(area), index) for shell, area, index in shells if shell.contains(probe)]
        if not candidates:
            return None, "source_polygon_hole_unassigned"
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for _, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    wkb = (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb
    return wkb, None


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        nxt = ring[(index + 1) % len(ring)]
        area += vertex[0] * nxt[1] - nxt[0] * vertex[1]
    return area / 2


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
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = _load_receipt(capture_root, name)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise PadUsProtectionError("retained_capture_receipt_invalid:" + name)
    return body, receipt


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


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


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
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise PadUsProtectionError("usgs_padus_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if roster != EXPECTED_SERVICE_LAYERS:
        raise PadUsProtectionError("usgs_padus_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if description != EXPECTED_SERVICE_DESCRIPTION:
        raise PadUsProtectionError("usgs_padus_service_description_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise PadUsProtectionError("source_service_spatial_reference_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise PadUsProtectionError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise PadUsProtectionError("exact_padus_protection_mechanism_layer_metadata_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise PadUsProtectionError("source_layer_z_or_m_contract_drift")
    if layer.get("objectIdField") != "OBJECTID" or layer.get("globalIdField") != "GlobalID":
        raise PadUsProtectionError("source_id_field_changed")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise PadUsProtectionError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000 or PAGE_SIZE > 2000:
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
    if (not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
            or any(type(value) is not int for value in roster)
            or len(set(roster)) != EXPECTED_COUNT):
        raise PadUsProtectionError("source_id_population_drift:" + label)
    if _sha(_canonical(sorted(roster))) != EXPECTED_IDS_SHA256:
        raise PadUsProtectionError("source_id_roster_drift:" + label)
    return sorted(roster)


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained page byte and optionally reconstruct the native geoparquet."""
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    service_doc_body = _read_capture(capture_root, "service_doc")[0]
    if (SERVICE_URL.encode("utf-8") not in service_doc_body
            or VIEW_ITEM_ID.encode("utf-8") not in service_doc_body):
        raise PadUsProtectionError("usgs_service_documentation_link_drift")
    _validate_item_document(docs["view_item"], VIEW_ITEM_ID, "view_item")
    _validate_item_document(docs["service_item"], SERVICE_ITEM_ID, "service_item")
    service, layer = docs["service_before"], docs["layer_before"]
    _validate_service_document(service, "service_before")
    fields, schema_sha = _validate_layer_document(layer)
    declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise PadUsProtectionError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise PadUsProtectionError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise PadUsProtectionError("source_count_drift")
    roster_before = _validate_ids_roster(docs["ids_before"].get("objectIds"), "ids_before")
    for name in ("layer_after", "service_after", "count_after", "ids_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before", "ids_after": "ids_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise PadUsProtectionError("source_metadata_or_count_changed_during_capture")
    _validate_ids_roster(json.loads(_read_capture(capture_root, "ids_after")[0]).get("objectIds"),
                         "ids_after")
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
    ring_count = 0
    vertex_count = 0
    unusable_geometry: dict[str, int] = {}
    featclass_distribution: dict[object, int] = {}
    bounds_extrema: list[float] | None = None
    page_bodies = [name for name in _expected_capture_names(EXPECTED_COUNT)
                   if name.startswith("page_") and name != "page_tail"]
    try:
        for page_name in page_bodies:
            page = json.loads(_read_capture(capture_root, page_name)[0])
            if not isinstance(page, dict) or "error" in page:
                raise PadUsProtectionError("source_page_error:" + page_name)
            reference = page.get("spatialReference")
            if (not isinstance(reference, dict)
                    or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
                    or reference.get("latestWkid") != SOURCE_CRS_WKID):
                raise PadUsProtectionError("source_page_spatial_reference_drift:" + page_name)
            if page.get("objectIdFieldName") != "OBJECTID":
                raise PadUsProtectionError("source_id_field_changed:" + page_name)
            if page.get("hasZ") is not False or page.get("hasM") is not False:
                raise PadUsProtectionError("source_page_z_or_m_flag_drift:" + page_name)
            features = page.get("features")
            if not isinstance(features, list) or not features \
                    or len(features) > PAGE_SIZE:
                raise PadUsProtectionError("source_page_row_count_incomplete:" + page_name)
            if len(features) < PAGE_SIZE and page.get("exceededTransferLimit") is True:
                raise PadUsProtectionError("source_page_transfer_truncated:" + page_name)
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
                    wkb, unusable_mode = _polygon_wkb(geometry)
                    if unusable_mode is not None:
                        # The producer served rings that cannot form a valid OGC
                        # polygon: the row and its raw rings stay in the
                        # record/geometry digests and bounds verbatim, the
                        # candidate geometry is explicitly gated to null, and
                        # the failure mode is counted against the pinned
                        # family (never silently coerced).
                        null_geometry_count += 1
                        geometries.append(None)
                        unusable_geometry[unusable_mode] = \
                            unusable_geometry.get(unusable_mode, 0) + 1
                    else:
                        geometries.append(wkb)
                    geometry_hash.update(_canonical([objectid, rings]) + b"\x00")
                featclass = attrs["FeatClass"]
                featclass_distribution[featclass] = \
                    featclass_distribution.get(featclass, 0) + 1
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
            raise PadUsProtectionError("source_page_population_incomplete")
        ids_sha = _sha(_canonical(list(ids)))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
        if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
            raise PadUsProtectionError("source_value_or_geometry_baseline_drift")
        if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
            raise PadUsProtectionError("source_geometry_or_coverage_drift")
        if ids_sha != EXPECTED_IDS_SHA256 or list(ids) != roster_before:
            raise PadUsProtectionError("source_id_roster_drift")
        if null_geometry_count != EXPECTED_NULL_GEOMETRY_COUNT + sum(unusable_geometry.values()):
            raise PadUsProtectionError("source_null_geometry_count_drift")
        if unusable_geometry != EXPECTED_UNUSABLE_GEOMETRY_COUNTS:
            raise PadUsProtectionError("source_unusable_geometry_count_drift")
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
            "ring_count": ring_count, "vertex_count": vertex_count,
            "unusable_geometry": unusable_geometry,
            "featclass_distribution": featclass_distribution, "observed_bounds": bounds,
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


def fetch_pad_us_protection_mechanism_category_pages(capture_root: Path, *, client: httpx.Client | None = None,
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
                   "view_item": SHARING_ITEM_URL.format(item=VIEW_ITEM_ID),
                   "service_item": SHARING_ITEM_URL.format(item=SERVICE_ITEM_ID),
                   "service_before": SERVICE_URL + "?f=pjson",
                   "layer_before": LAYER_URL + "?f=pjson",
                   "count_before": _count_url(),
                   "ids_before": _ids_url()}[name]
            active_client = docs_active if name == "service_doc" else active
            retained[name] = _retained_or_fetch(active_client, capture_root, name, url)
            if name != "service_doc":
                _reject_error_response(capture_root, name, retained[name][0])
        count_before = json.loads(retained["count_before"][0]).get("count")
        if count_before != EXPECTED_COUNT:
            raise PadUsProtectionError("source_count_drift")
        _validate_ids_roster(json.loads(retained["ids_before"][0]).get("objectIds"), "ids_before")
        last_objectid = pages[-1]["last_objectid"] if pages else 0
        while not (capture_root / "page_tail.body").exists():
            name = f"page_{len(pages):06d}"
            body, receipt = _retained_or_fetch(active, capture_root, name, _page_url(last_objectid))
            page = json.loads(body)
            if not isinstance(page, dict) or "error" in page:
                _witness_error_response(capture_root, name)
                raise PadUsProtectionError("source_page_error")
            features = page.get("features")
            if not isinstance(features, list):
                raise PadUsProtectionError("source_page_missing_features")
            if not features:
                if last_objectid == 0:
                    raise PadUsProtectionError("source_page_population_incomplete")
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
                raise PadUsProtectionError("source_page_transfer_truncated")
            pages.append({"name": name, "last_objectid": last_objectid, "features": len(features),
                          "exceeded_transfer_limit": more, "captured_at": receipt["observed_at"],
                          "body_sha256": _sha(body)})
            _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
            if len(pages) > (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
                raise PadUsProtectionError("source_page_count_exceeds_pinned_population")
            if len(features) < PAGE_SIZE or more is False:
                tail_body, tail_receipt = _retained_or_fetch(active, capture_root, "page_tail",
                                                             _page_url(last_objectid))
                _reject_error_response(capture_root, "page_tail", tail_body)
                if json.loads(tail_body).get("features") != []:
                    raise PadUsProtectionError("source_tail_page_not_empty")
                tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                        "captured_at": tail_receipt["observed_at"], "body_sha256": _sha(tail_body)}
                _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
                break
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise PadUsProtectionError("source_tail_page_not_empty")
        for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                          ("service_after", SERVICE_URL + "?f=pjson"),
                          ("count_after", _count_url()),
                          ("ids_after", _ids_url())):
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            _reject_error_response(capture_root, name, body)
            retained[name] = (body, _read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise PadUsProtectionError("source_metadata_or_count_changed_during_capture")
    except PadUsProtectionError:
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
        raise PadUsProtectionError("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()

    last_captured_at = str(tail["captured_at"])
    # Construction happens once at seal time (capture_pad_us_protection_mechanism_category);
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
        "geometry_type": "Polygon / MultiPolygon",
        "null_geometry_count": extracted["null_geometry_count"],
        "unique_source_id_count": EXPECTED_COUNT,
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_featclass_distribution": extracted["featclass_distribution"],
        "unusable_geometry_feature_counts": dict(sorted(extracted["unusable_geometry"].items())),
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID,
        "service_url": SERVICE_URL, "view_item_id": VIEW_ITEM_ID,
        "service_item_id": SERVICE_ITEM_ID,
        "service_doc_url": SERVICE_DOC_URL,
        "crosswalk_doi": "10.3886/E241367V1",
        "crosswalk_row_note": "The DHS HIFLD Open crosswalk (created by DHS in August 2025) "
                              "links HIFLD Open layers to their producing agencies; the "
                              "2026-10-20 research snapshot matched this inventory entry to "
                              "USGS GAP's PAD-US publications and the exact crosswalk row "
                              "text was not re-verified in this session.",
        "baseline_pending_note": BASELINE_PENDING_NOTE,
        "baseline_pending_complete_capture": bool(
            not EXPECTED_RECORD_SHA256 or not EXPECTED_GEOMETRY_SHA256),
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The service's own labels are stale producer labels: the layer name and "
            "copyrightText cite the 2022 PAD-US 3.0 release (DOI 10.5066/P9Q9LQ4B) while the "
            "service data was last edited 2024-03-20 (editingInfo.lastEditDate) and USGS's "
            "published FGDC metadata for this service describes it as PAD-US 4.0 "
            "(publication date 2024-04-25). No per-snapshot release date is published by the "
            "service; the version relationship is an unaccepted maintainer decision.",
        "pagination": {"mode": "objectid_keyset_asc", "page_size": PAGE_SIZE,
                       "pages": (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE,
                       "tail_witness_page": True, "resumable": True,
                       "resumed_pages": resumed_pages,
                       "id_roster_enumeration_note":
                           "returnIdsOnly works for this producer (441,609 contiguous ids "
                           "1..441,609 observed 2026-11-13); completeness uses the pinned "
                           "count, the pinned sorted-roster digest captured before and after "
                           "the walk, the strict ascending OBJECTID walk, the page-count "
                           "ceiling and the empty tail-page witness.",
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
    receipts = _load_receipt_map(capture_root)
    _unknown_release_reason = (
        "The service publishes no per-snapshot release date; its copyrightText records the "
        "2022 PAD-US 3.0 release while its data was last edited 2024-03-20 and USGS's "
        "published metadata describes the service as PAD-US 4.0 (2024-04-25). The version "
        "relationship is an unaccepted maintainer decision.")
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
                       "Protection_Mechanism_Category/FeatureServer/0, USGS-owned ArcGIS "
                       "items " + VIEW_ITEM_ID + " and " + SERVICE_ITEM_ID + "), captured "
                       "completely from the producer's own REST publication as the maintained "
                       "USGS-linked publication named by the HIFLD Open inventory entry "
                       "pad-us-protection-mechanism-category. Complete source attributes and "
                       "EPSG:4326 polygon geometry are represented in the candidate. The "
                       "service's own labels cite PAD-US 3.0 (2022) while its data was last "
                       "edited 2024-03-20 and USGS metadata describes the service as PAD-US "
                       "4.0; succession, freshness, schema/ID contract, coverage, rights and "
                       "release acceptance remain unresolved and unapproved.",
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
                                            "source_ids": "Preserve producer OBJECTID and GlobalID "
                                                          "values unchanged; no substitute IDs, "
                                                          "casts or joins",
                                            "geometry": "Source EPSG:4326 polygon rings retained "
                                                        "as WKB without projection or repair under "
                                                        "the producer's verified ring semantics "
                                                        "(closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes, 2-ordinate "
                                                        "vertices); null source geometry retained "
                                                        "as null; producer-served features whose "
                                                        "rings cannot form a valid OGC polygon are "
                                                        "retained verbatim in the digests with "
                                                        "explicitly gated null candidate geometry "
                                                        "and pinned failure modes",
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
                   "mcroft@usgs.gov_USGS, org v01gqwM5QqNysAAi) backed by the USGS-owned hosted "
                   "service item 22670023fd124c799d5ddd08297dde85; both item documents are "
                   "captured as identity evidence. The service's own layer name and "
                   "copyrightText cite PAD-US 3.0 (2022, DOI 10.5066/P9Q9LQ4B) while the "
                   "service data was last edited 2024-03-20 (editingInfo.lastEditDate) and "
                   "USGS's published FGDC metadata for this service describes it as PAD-US 4.0 "
                   "(publication date 2024-04-25, processing environment PADUS4_0Geodatabase.gdb). "
                   "The PAD-US 4.1 Full Inventory Database (2024, DOI 10.5066/P96WBCHS, "
                   "ScienceBase item 652d4fc5d34e44db0e2ee45e, observed HTTP 200 anonymously on "
                   "2026-11-13) is the current national database release but publishes the "
                   "separate PAD-US feature classes, not this combined producer view, so the "
                   "draft pins the maintained USGS-linked view service and the version "
                   "relationship remains an unaccepted maintainer freshness/succession "
                   "decision. The DataLumos project 240685 archive copy (archive modification "
                   "2025-06-04) is identification and comparison context only and is never "
                   "fetched.",
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
        raise PadUsProtectionError("cached_first_candidate_identity_or_source_changed")
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
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": EXPECTED_NULL_GEOMETRY_COUNT,
            "source_id_sha256": audit.get("source_ids_sha256", ""),
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
    attempt_root = Path(capture_directory) if capture_directory \
        else Path("/data/workspace/scratch/pad-us-protection-mechanism-category/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
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
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": EXPECTED_COUNT,
            "source_max_objectid": extracted["max_objectid"]}
