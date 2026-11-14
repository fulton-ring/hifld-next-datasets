"""Lossless, approval-gated capture of the FEMA NFHL "PLSS" polygon layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 5 "PLSS" as the
producer's own publication of this HIFLD inventory dataset (``nfhl/plss``,
DataLumos 239982 historical archive copy used for identification only). The
layer is continuously updated and publishes no per-snapshot release date;
source freshness, rights, and public-schema compatibility are not accepted by
this draft.

This is the largest NFHL layer served by the producer (1,643,556 features, 822
fixed pages at the producer's maxRecordCount of 2000), so the capture streams:
every page is validated against its exact sorted-OBJECTID roster slice the
moment it is fetched, and the record/geometry digests, coverage bounds and
candidate GeoParquet row groups are built incrementally so no complete-feature
materialization is ever held in memory. Because pages are fetched and verified
in ascending roster-slice order, the streamed feature order is provably the
sorted record order the pins are computed over. Producer polygon semantics
(closed rings; clockwise exteriors, counter-clockwise holes) follow the Esri
JSON convention verified on the merged sibling NFHL polygon adapters and
re-verified here by the pinned record/geometry digests of the dated complete
capture; null or string-"NaN" geometry is retained as evidence and gated, never
silently coerced. Memory stays bounded through late failures too: coverage
extrema are running values, the abandoned parquet writer is closed, and
quarantine preserves each capture's evidence from its capture path one body at
a time instead of materializing the gigabyte-scale corpus.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
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

DATASET_SLUG = "nfhl-plss"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 5
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "PLSS"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
PAGE_SIZE = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 10_000_000_000
# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or transport failures (observed 2026-11-12
# while paging this layer; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact page id slices, and streamed
# full-record digests, so retries can never mask drift.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed population contract of the dated complete 822-page
# producer capture finished 2026-11-12T18:32Z: every page was validated against
# its exact sorted OBJECTID roster slice on arrival, the before/after metadata
# brackets were stable, and the record/geometry digests were streamed in
# roster-slice order over all 1,643,556 features. A live capture failing any pin
# quarantines and reports the exact drift instead of accepting stale values.
EXPECTED_COUNT = 1643556
EXPECTED_ID_SHA256 = "9db0d786ba0e857a81bc02e3dac04767eb674d840ec96b10f44b9ddd0d532449"
EXPECTED_SCHEMA_SHA256 = "85e698d7079536f49599b9d7ac1b4b34ac969d679ea45cd8c9dacf8a03da1470"
EXPECTED_RECORD_SHA256 = "14ee62eabce0e8bdb65a153f5bfae5b35c769ff6c55dfc36fb28f3ef05476936"
EXPECTED_GEOMETRY_SHA256 = "d6242c48e2200a69bc8da3c681cdbde68ac07e9c237721fc47589bf53eedf0ce"
EXPECTED_NULL_GEOMETRY_COUNT = 14
EXPECTED_NAN_GEOMETRY_COUNT = 0
EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS = {
    "source_polygon_hole_unassigned": 4,
    "source_polygon_ring_degenerate": 30,
    "source_polygon_ring_self_intersecting": 73,
}
EXPECTED_OBSERVED_BOUNDS = {"xmin": -165.51601141942217, "ymin": 25.23236613772616,
                            "xmax": -80.03142507888776, "ymax": 66.61058213599716}
EXPECTED_LAYER_EXTENT = {"xmin": -166.16706527995404, "ymin": 25.23236613772616,
                         "xmax": -80.03142500030742, "ymax": 66.61058213599716}
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "NFHL Availability", "Feature Layer"), (3, "FIRM Panels", "Feature Layer"),
    (1, "LOMRs", "Feature Layer"), (34, "LOMAs", "Feature Layer"),
    (22, "Political Jurisdictions", "Feature Layer"), (17, "Profile Baselines", "Feature Layer"),
    (20, "Water Lines", "Feature Layer"), (14, "Cross-Sections", "Feature Layer"),
    (16, "Base Flood Elevations", "Feature Layer"), (23, "Levees", "Feature Layer"),
    (29, "Seclusion Boundaries", "Feature Layer"), (15, "Coastal Transects", "Feature Layer"),
    (18, "Transect Baselines", "Feature Layer"), (24, "General Structures", "Feature Layer"),
    (7, "River Mile Markers", "Feature Layer"), (32, "Water Areas", "Feature Layer"),
    (5, "PLSS", "Feature Layer"), (19, "Limit of Moderate Wave Action", "Feature Layer"),
    (27, "Flood Hazard Boundaries", "Feature Layer"), (28, "Flood Hazard Zones", "Feature Layer"),
    (25, "Primary Frontal Dunes", "Feature Layer"), (4, "Base Index", "Feature Layer"),
    (6, "Topographic Low Confidence Areas", "Feature Layer"), (8, "Datum Conversion Points", "Feature Layer"),
    (9, "Coastal Gages", "Feature Layer"), (10, "Gages", "Feature Layer"),
    (11, "Nodes", "Feature Layer"), (12, "High Water Marks", "Feature Layer"),
    (13, "Station Start Points", "Feature Layer"), (26, "Hydrologic Reaches", "Feature Layer"),
    (30, "Alluvial Fans", "Feature Layer"), (31, "Subbasins", "Feature Layer"),
]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _StreamDigest:
    """Incremental equivalent of hashing a canonical JSON array of items.

    ``update(items)`` feeds each list exactly as ``_canonical`` would frame the
    concatenated array, so ``hexdigest`` equals
    ``_sha(_canonical(concatenated_items))`` without holding the array.
    """

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._count = 0
        self._closed = False

    def update(self, items: list) -> None:
        if self._closed:
            raise AssertionError("stream_digest_already_finalized")
        for item in items:
            if self._count:
                self._digest.update(b",")
            self._digest.update(_canonical(item))
            self._count += 1

    @property
    def count(self) -> int:
        return self._count

    def hexdigest(self) -> str:
        if not self._closed:
            self._digest.update(b"]")
            self._closed = True
        return self._digest.hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_count() -> int:
    return (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE


def _page_name(index: int) -> str:
    return f"page_{index:03d}"


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("service_doc", SERVICE_DOC_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _page_urls() -> list[tuple[str, str]]:
    urls = []
    for index in range(_page_count()):
        offset = index * PAGE_SIZE
        params = {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                  "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                  "resultRecordCount": PAGE_SIZE, "f": "json"}
        urls.append((_page_name(index), _query(params)))
    return urls


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
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
            raise ValueError(label + "_crs_drift")


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


def _validate_docs(docs: Mapping[str, object], service_doc_body: bytes) -> tuple[dict, list, list[int]]:
    """Validate the opening metadata documents and return (layer, fields, sorted ids)."""
    if SERVICE_URL.encode("utf-8") not in service_doc_body:
        raise ValueError("fema_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("fema_nfhl_service_layer_roster_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer"
               for x in service_layers):
        raise ValueError("exact_nfhl_plss_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("exact_nfhl_plss_layer_metadata_drift")
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
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
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if sorted(rosters[0]) != sorted(rosters[1]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_seed")):
        raise ValueError("source_id_field_changed")
    return layer, fields_value, ids


def _validate_after_docs(docs: Mapping[str, object], layer: dict, service: dict, count: dict,
                         fields_value: list, ids: list[int]) -> None:
    """Validate the closing metadata documents captured after the page stream."""
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] \
            != [_plain_field(x) for x in fields_value]:
        raise ValueError("source_field_schema_changed_during_capture")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_after"] != count:
        raise ValueError("source_count_changed_during_capture")
    roster = docs["ids_after"].get("objectIds")
    if (not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
            or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
            or sorted(roster) != ids):
        raise ValueError("source_id_population_drift")
    if docs["ids_after"].get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_id_field_changed")


def _validate_page(page: object, ids: list[int], index: int, offset: int) -> list[dict[str, object]]:
    """Validate one fetched page document against its exact roster slice."""
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    _validate_spatial_reference(page.get("spatialReference"), f"source_{_page_name(index)}")
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    expected = ids[offset:offset + PAGE_SIZE]
    actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
    full_nonfinal = len(expected) == PAGE_SIZE and offset + PAGE_SIZE < EXPECTED_COUNT
    exceeded = page.get("exceededTransferLimit") is True
    if actual != expected or len(rows) != len(expected):
        raise ValueError("source_page_id_slice_incomplete_or_reordered")
    if exceeded and not full_nonfinal:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    return rows


def _validated_rings(geometry: object) -> list | None:
    """Validate one feature's ring geometry, returning the source rings verbatim."""
    if geometry is None:
        return None
    if not isinstance(geometry, dict) or set(geometry) != {"rings"} or not isinstance(geometry["rings"], list) \
            or not geometry["rings"]:
        raise ValueError("source_polygon_geometry_invalid")
    for ring in geometry["rings"]:
        if not isinstance(ring, list) or not ring:
            raise ValueError("source_polygon_geometry_invalid")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise ValueError("source_polygon_geometry_invalid")
            x, y = vertex
            if type(x) in (int, float) and type(y) in (int, float):
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError("source_coordinate_range_drift")
            elif not (x == "NaN" and y == "NaN"):
                # String-"NaN" ordinates (seen on FEMA's sibling NFHL layers)
                # are retained verbatim in the record/raw evidence and gated
                # as unusable geometry, never silently coerced to a location.
                raise ValueError("source_polygon_geometry_invalid")
    return geometry["rings"]


def _has_nan_vertex(geometry: object) -> bool:
    return any(vertex[0] == "NaN" and vertex[1] == "NaN"
               for ring in geometry["rings"] for vertex in ring)


def _geo_metadata() -> dict:
    return {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
        "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64(),
            # Esri JSON delivers date fields as raw epoch milliseconds; the
            # producer's exact numeric wire values are retained verbatim as
            # int64 with no timezone or calendar interpretation applied.
            "esriFieldTypeDate": pa.int64()}[str(field["type"])]


UNUSABLE_GEOMETRY_MODES = frozenset({
    "source_polygon_ring_degenerate", "source_polygon_exterior_missing",
    "source_polygon_ring_self_intersecting", "source_polygon_hole_unassigned",
})


class _PageStream:
    """Streaming per-page validator: digests, coverage and candidate parquet."""

    def __init__(self, fields: list, parquet_path: Path) -> None:
        self.attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
        self.schema = pa.schema([pa.field(field["name"],
                                          pa.binary() if field["name"] == "SHAPE"
                                          else _arrow_type(field),
                                          nullable=True) for field in fields],
                                metadata={b"geo": _canonical(_geo_metadata())})
        self.parquet_path = parquet_path
        self._writer = pq.ParquetWriter(str(parquet_path), self.schema, compression="zstd")
        self.record_digest = _StreamDigest()
        self.geometry_digest = _StreamDigest()
        self.null_geometry = 0
        self.nan_geometry = 0
        self.unusable_geometry: dict[str, int] = {}
        self.feature_count = 0
        # Running extrema only: retaining every ordinate of the complete layer
        # (~28 million vertices) would hold ~1.8 GB through any late failure.
        self._xmin = self._ymin = self._xmax = self._ymax = None
        self._writer_closed = False

    def process(self, page: object, ids: list[int], index: int, offset: int) -> None:
        rows = _validate_page(page, ids, index, offset)
        geometry_records = []
        columns = {name: [] for name in self.attr_names}
        shapes = []
        for feature in rows:
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != self.attr_names:
                raise ValueError("source_record_attribute_schema_drift")
            geometry_records.append([attrs["OBJECTID"], _validated_rings(feature.get("geometry"))])
            for name in self.attr_names:
                columns[name].append(attrs.get(name))
            try:
                shapes.append(_polygon_wkb(feature.get("geometry")))
            except ValueError as exc:
                if str(exc) not in UNUSABLE_GEOMETRY_MODES:
                    raise
                # The producer publishes a small number of features whose rings
                # cannot form a valid OGC polygon (zero/signed-zero area slivers,
                # unassignable holes). The row and its raw rings are retained
                # verbatim; the candidate geometry is explicitly gated to null
                # and the failure mode is counted and pinned, never silently
                # coerced into an invented polygon.
                shapes.append(None)
                self.unusable_geometry[str(exc)] = self.unusable_geometry.get(str(exc), 0) + 1
            geometry = feature.get("geometry")
            if geometry is None:
                self.null_geometry += 1
            elif _has_nan_vertex(geometry):
                self.nan_geometry += 1
            else:
                for ring in geometry["rings"]:
                    for vertex in ring:
                        if type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float):
                            x, y = vertex
                            if self._xmin is None or x < self._xmin:
                                self._xmin = x
                            if self._ymin is None or y < self._ymin:
                                self._ymin = y
                            if self._xmax is None or x > self._xmax:
                                self._xmax = x
                            if self._ymax is None or y > self._ymax:
                                self._ymax = y
        self.feature_count += len(rows)
        self.record_digest.update(rows)
        self.geometry_digest.update(geometry_records)
        self._writer.write_table(pa.Table.from_pydict(
            {**columns, "SHAPE": pa.array(shapes, type=pa.binary())}, schema=self.schema))

    def observed_bounds(self) -> dict[str, float] | None:
        if self._xmin is None:
            return None
        return {"xmin": self._xmin, "ymin": self._ymin, "xmax": self._xmax, "ymax": self._ymax}

    def close_writer(self) -> None:
        """Finalize the candidate parquet once, whether or not the capture succeeds."""
        if not self._writer_closed:
            self._writer_closed = True
            self._writer.close()

    def finish(self) -> dict[str, object]:
        self.close_writer()
        return {"source_feature_count": self.feature_count,
                "source_record_sha256": self.record_digest.hexdigest(),
                "source_geometry_sha256": self.geometry_digest.hexdigest(),
                "geometry_type": "Polygon",
                "null_geometry_count": self.null_geometry,
                "nan_string_geometry_count": self.nan_geometry,
                "unusable_geometry_feature_counts": dict(sorted(self.unusable_geometry.items())),
                "observed_bounds": self.observed_bounds()}


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        x1, y1 = vertex
        x2, y2 = ring[(index + 1) % len(ring)]
        area += x1 * y2 - x2 * y1
    return area / 2


def _usable_rings(geom: object) -> list | None:
    if not isinstance(geom, dict) or set(geom) != {"rings"} or not isinstance(geom["rings"], list) or not geom["rings"]:
        return None
    for ring in geom["rings"]:
        if not isinstance(ring, list) or not ring:
            return None
        for vertex in ring:
            if not (isinstance(vertex, list) and len(vertex) == 2
                    and type(vertex[0]) in (int, float) and type(vertex[1]) in (int, float)):
                return None
    return geom["rings"]


def _polygon_wkb(geom: object) -> bytes | None:
    """WKB for one feature's rings under the producer's verified ring semantics.

    Esri JSON (verified on the pinned capture): every ring is explicitly closed,
    clockwise rings are exteriors, counter-clockwise rings are holes. Vertices
    are passed to shapely verbatim; exteriors with no claimable hole keep their
    ring order and holes join their smallest containing exterior.
    """
    rings = _usable_rings(geom)
    if rings is None:
        return None
    exteriors, holes = [], []
    for ring in rings:
        area = _signed_area(ring)
        if area == 0:
            raise ValueError("source_polygon_ring_degenerate")
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        raise ValueError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    for shell, _, _ in shells:
        if not shell.is_valid:
            raise ValueError("source_polygon_ring_self_intersecting")
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        polygon = Polygon(hole)
        candidates = [(abs(area), index) for shell, area, index in shells
                      if shell.contains(polygon.representative_point())]
        if not candidates:
            raise ValueError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    polygons = []
    for shell, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


class _QuarantineCaptureBodies(Mapping[str, tuple[bytes, dict[str, object]]]):
    """Path-backed quarantine evidence view that materializes one body at a time.

    The complete PLSS corpus is gigabytes, so quarantine must never build an
    in-memory ``{name: body}`` dict over every capture: the shared quarantine
    helper reads each item exactly once while writing it, and this mapping
    reads the body from its capture path on that single access. Names without
    a retained body (failed retry attempts) are excluded, matching the
    evidence-preservation contract of the shared helper.
    """

    def __init__(self, capture_root: Path, receipts: Mapping[str, dict[str, object]]) -> None:
        self._capture_root = capture_root
        self._receipts = receipts
        self._names = sorted(name for name in receipts if (capture_root / f"{name}.body").exists())

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return (self._capture_root / f"{name}.body").read_bytes(), self._receipts[name]

    def __iter__(self):
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def capture_nfhl_plss(storage: StagingStorageResource, *, version: str,
                      capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL PLSS candidate."""
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/205/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, dict[str, object]] = {}
    parquet_path = capture_root / "candidate.geoparquet"
    stream: _PageStream | None = None
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-plss-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed_doc = json.loads(seed_body)
            seed = seed_doc.get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            receipts["ids_seed"] = seed_receipt
            docs = {"ids_seed": seed_doc}
            service_doc_body = None
            for name, url in _doc_urls():
                body, receipt = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                receipts[name] = receipt
                if name == "service_doc":
                    service_doc_body = body
                else:
                    docs[name] = json.loads(body)
            layer, source_fields, ids = _validate_docs(docs, service_doc_body)
            del docs["ids_before"], docs["ids_seed"], seed
            stream = _PageStream(source_fields, parquet_path)
            for index, (name, url) in enumerate(_page_urls()):
                body, receipt = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=PAGE_CAPTURE_ATTEMPTS, base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS)
                receipts[name] = receipt
                stream.process(json.loads(body), ids, index, index * PAGE_SIZE)
                del body
            if stream.feature_count != EXPECTED_COUNT:
                raise ValueError("source_page_tiling_incomplete_or_overlapped")
            after = {}
            for name, url in _after_doc_urls():
                body, receipt = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                receipts[name] = receipt
                after[name] = json.loads(body)
            _validate_after_docs(after, layer, docs["service_before"], docs["count_before"],
                                 source_fields, ids)
            del after, ids, docs
            audit_stream = stream.finish()
            if audit_stream["source_record_sha256"] != EXPECTED_RECORD_SHA256:
                raise ValueError("source_record_value_drift")
            if audit_stream["source_geometry_sha256"] != EXPECTED_GEOMETRY_SHA256:
                raise ValueError("source_geometry_or_coverage_drift")
            if EXPECTED_NULL_GEOMETRY_COUNT is not None \
                    and audit_stream["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT:
                raise ValueError("source_null_geometry_count_drift")
            if EXPECTED_NAN_GEOMETRY_COUNT is not None \
                    and audit_stream["nan_string_geometry_count"] != EXPECTED_NAN_GEOMETRY_COUNT:
                raise ValueError("source_nan_geometry_count_drift")
            if EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS is not None \
                    and audit_stream["unusable_geometry_feature_counts"] != EXPECTED_UNUSABLE_GEOMETRY_FEATURE_COUNTS:
                raise ValueError("source_unusable_geometry_count_drift")
            if EXPECTED_OBSERVED_BOUNDS is not None \
                    and audit_stream["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS:
                raise ValueError("source_observed_coverage_drift")
    except Exception as exc:
        quarantine_receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                quarantine_receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                quarantine_receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": quarantine_receipts}) + b"\n")
        if stream is not None:
            try:
                stream.close_writer()
            except Exception:
                # Best-effort finalization of the abandoned parquet; quarantine
                # and the original drift error must survive even if it cannot close.
                pass
        # Evidence is preserved one capture at a time from the capture paths; the
        # gigabyte-scale complete corpus is never materialized in memory here.
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                      _QuarantineCaptureBodies(capture_root, quarantine_receipts),
                                      type(exc).__name__)
        raise
    if set(receipts) != {"ids_seed", "service_doc", "service_before", "layer_before", "count_before", "ids_before",
                         *(_page_name(i) for i in range(_page_count())),
                         "layer_after", "service_after", "count_after", "ids_after"}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    identity = _code_identity()
    parquet_body = parquet_path.read_bytes()
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "PLSS (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 5 'PLSS', captured "
                       "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                       "polygon geometry are represented in the candidate. The NFHL is continuously updated and "
                       "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                       "release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": SERVICE_DOC_URL if name == "service_doc" else LAYER_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The NFHL is continuously updated; the service and its "
                                                   "documentation page publish no per-snapshot release date.",
                    "license": "Not stated by the captured endpoints; rights review pending.",
                    "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                                   "review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's derived "
                                                             "SHAPE.STArea() and SHAPE.STLength() doubles; SHAPE is "
                                                             "the original geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, DFIRM_ID, VERSION_ID, "
                                                          "PLSS_AR_ID, GFID and GlobalID values unchanged; no "
                                                          "substitute IDs, casts, trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) polygon rings retained as WKB "
                                                        "without projection under the producer's verified Esri "
                                                        "ring semantics (closed rings; clockwise exteriors, "
                                                        "counter-clockwise holes); no vertex dropped, added, or "
                                                        "reordered; null source geometry retained as null",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 5 'PLSS' of that producer service (the "
                   "Public Land Survey System base map). The HIFLD Open inventory lists this dataset as nfhl/plss "
                   "with DataLumos project 239982; that ICPSR archive copy and the 2024-10-31 archive modification "
                   "date are identification and comparison context only, never pipeline inputs.",
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
    audit = {"source_id_sha256": EXPECTED_ID_SHA256,
             "source_schema_sha256": EXPECTED_SCHEMA_SHA256,
             "source_crs_epsg": SOURCE_CRS_WKID,
             "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": EXPECTED_COUNT,
             "source_page_count": _page_count(),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates.",
              **audit_stream,
             "source_fields": [_plain_field(f) for f in source_fields],
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
                    "receipts": {name: receipt for name, receipt in sorted(receipts.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
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
    descriptors = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    write_immutable(storage, f"{prefix}/{rel}", parquet_body)
    del parquet_body
    for relative, body in (("metadata/source_manifest.json", _canonical(manifest) + b"\n"),
                           ("metadata/source_audit.json", _canonical(audit) + b"\n"),
                           ("metadata/processing_identity.json", _canonical(identity) + b"\n"),
                           ("metadata/candidate_ready.json", _canonical(
                               {"approved": False, "version": version, "objects": source_objects,
                                "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
                                "schema_contract_accepted": False, "id_contract_accepted": False,
                                "rights_accepted": False, "mapping_approved": False, "release_accepted": False})
                            + b"\n")):
        write_immutable(storage, f"{prefix}/{relative}", body)
        descriptors[relative] = {"bytes": len(body), "sha256": _sha(body)}
    for name, receipt in sorted(receipts.items()):
        raw = (capture_root / f"{name}.body").read_bytes()
        if len(raw) != receipt["bytes"] or _sha(raw) != receipt["sha256"]:
            raise ValueError("candidate_raw_body_readback_failed:" + name)
        write_immutable(storage, f"{prefix}/raw/{name}.body", raw)
        descriptors[f"raw/{name}.body"] = {"bytes": len(raw), "sha256": _sha(raw)}
        del raw
        wire = (capture_root / f"{name}.wire").read_bytes()
        if len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]:
            raise ValueError("candidate_raw_wire_readback_failed:" + name)
        write_immutable(storage, f"{prefix}/raw/{name}.wire", wire)
        descriptors[f"raw/{name}.wire"] = {"bytes": len(wire), "sha256": _sha(wire)}
        del wire
        receipt_body = _canonical(receipt) + b"\n"
        write_immutable(storage, f"{prefix}/raw/{name}.receipt.json", receipt_body)
        descriptors[f"raw/{name}.receipt.json"] = {"bytes": len(receipt_body), "sha256": _sha(receipt_body)}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, receipt in receipts.items()},
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
