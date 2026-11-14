"""Streaming, approval-gated capture of the USGS 3DHP_all Waterbody layer.

The HIFLD Open inventory entry ``nhd/waterbody`` ("Waterbody", agency US
Geological Survey) carries the description of the USGS 3D Hydrography Program
(3DHP) ``3DHP_all`` ArcGIS REST service verbatim; the DHS HIFLD Open crosswalk
(DOI 10.3886/E241367V1) consolidates the NHD-family rows into one "3D
Hydrography Program (3DHP) Layers" row whose Open REST Service is the USGS
``3dhp.nationalmap.gov`` ``usgs_3dhp_all`` FeatureServer. Its layer 60
"Waterbody" — the complete water-body population (River, Canal, Lake, Ocean or
Great Lake) — is the maintained producer publication captured here (USGS
retired the NHD as a maintained product on 2023-10-01; 3DHP is the successor;
the legacy Lake content belongs to this waterbody inventory, unlike the
non-Lake areal successor capture of ``nhd/area-large-scale-2``). The DataLumos
project 240398 archive copy named by the issue is identification context only
and is never fetched.

The layer held 7,158,943 features on 2026-11-13, so this module streams the
source the way the 3DHP Flowline capture does: each keyset page is retained as
raw evidence in a resumable capture directory before any candidate is sealed.
Completeness is established without the producer's ``returnIdsOnly`` roster:
that request fails server-side with an error document for this population
(observed 2026-11-13), so the pinned feature count, the pinned minimum and
maximum OBJECTIDs, the strict ascending OBJECTID keyset walk, the per-page
``exceededTransferLimit`` consistency, the page-count ceiling and the empty
tail-page witness together prove the captured population (a strict ascending
walk of ``EXPECTED_COUNT`` unique positive OBJECTIDs that starts at 1 and ends
at ``EXPECTED_MAX_OBJECTID`` is exactly the contiguous roster).

Scale and density notes (observed 2026-11-13): the roster head carries
exceptionally dense main-stem polygons (single features with tens of thousands
of Z vertices; one 10-record page over the head holds ~8.7 MB while a 50-record
page over the same span is refused with the producer's error document), while a
2,500-record page in the lake regions carries ~3.5-6.2 MB. The producer
therefore cannot be asked for one uniform page size: each walk position starts
at the producer's ``maxRecordCount`` and, when it answers with its complete
HTTP 200 error document (deterministic for the requested window), the
requested record count is bisected down to single records. A single record
that still refuses after bounded retries on both producer hosts is captured
through the producer's working attributes-only channel and staged verbatim
with explicitly gated null candidate geometry; its id joins the capture's
refusal set, which is re-verified against the pin by every later capture. The
per-value, per-geometry and ID-roster baseline digests, the null-geometry
count, the refusal set and the degenerate-ring contract were computed by the
first complete capture (2026-11-13, reported by
``build_nhd_waterbody_baseline``) and are pinned as module constants; every
later capture that disagrees fails closed. Publication stays blocked pending
the succession, freshness, schema/ID contract, rights and release decisions;
all runtime approval flags are false and the draft refresh schedule is
stopped.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import struct
import time
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
from shapely.geometry import MultiPolygon, Point, Polygon

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

DATASET_SLUG = "nhd-waterbody"
FILE_SLUG = DATASET_SLUG
SERVICE_HOST = "https://3dhp.nationalmap.gov"
SERVICE_URL = SERVICE_HOST + "/arcgis/rest/services/usgs_3dhp_all/FeatureServer"
ALIAS_SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer"
SERVICE_DOC_URL = "https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products"
LAYER_ID = 60
LAYER_NAME = "Waterbody"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
GEOMETRY_TYPE = "esriGeometryPolygon"
PUBLISHER = "U.S. Geological Survey"
USER_AGENT = "hifld-next-datasets/nhd-waterbody-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SOURCE_CRS_WKID = 4326
NATIVE_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
PAGE_SIZE = 2500
MAX_BODY_BYTES = 96 * 1024 * 1024
# The complete population is unusually heavy per feature (the roster head
# carries main-stem polygons with tens of thousands of Z vertices each), and
# the acquisition budget counts both wire and decoded bytes under the
# producer's identity encoding, so it is sized for the observed
# full-population envelope with headroom rather than a generic page-domain
# budget.
MAX_CAPTURE_BYTES = 90 * 1024 * 1024 * 1024
# The entry floor is sized to this dataset's measured envelope on the current
# factory task-lease cap of 100 GiB (observed 2026-11-13; the 150 GiB value
# was calibrated for the earlier 200 GiB leases and can never pass on the
# capped leases): the restored capture (~33 GB) plus the candidate GeoParquet
# and its staged raw copy peak near 85 GiB, so 55 GiB of free space at entry
# still refuses to start without room for the full candidate while leaving
# slack above that envelope. The budget above still bounds every fetch.
MIN_FREE_BYTES = 55_000_000_000
PAGE_ATTEMPTS = 3
PAGE_RETRY_BASE_DELAY_SECONDS = 5.0
DOC_ATTEMPTS = 4
DOC_RETRY_BASE_DELAY_SECONDS = 10.0
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
CORE_SERVICE_LAYERS = EXPECTED_SERVICE_LAYERS[:-len(SPATIAL_METADATA_IDS)]
EXPECTED_COUNT = 7_158_943
EXPECTED_MAX_OBJECTID = 7_158_943
EXPECTED_SCHEMA_SHA256 = "51f11bd107670196d396c0908b3c68a4dce0f37466d3dd179c3df149fd4a04d3"
# The per-value, per-geometry and ID-roster baselines cannot be computed
# without a complete capture of the 7,158,943-feature population; they are
# pinned here as empty (checked only when non-empty) and recorded by the first
# complete capture for the maintainer to pin.
# Per-value, per-geometry and ID-roster baseline digests computed by the first
# complete capture (3,037 retained pages over the full 7,158,943-feature
# population, captured 2026-11-13 07:24-11:23Z; computed_baseline.json
# 2026-11-13T19:51Z). Any later capture that disagrees fails closed.
EXPECTED_RECORD_SHA256 = "5a484e15ecf90b9fbe749e167220a0699acef61d02b9aeddaf59e9859bf8dd25"
EXPECTED_GEOMETRY_SHA256 = "8ffcff7a516ec6925e4d6b2960c9ca37b8484e64ff7304aff15342133c729baa"
EXPECTED_IDS_SHA256 = "1e32ac2f6f5f5142de8648257d716009208d40f72cb457e295acc94f82dc2d66"
# Null geometry: the nine producer-refused records (below) are the only null
# candidate geometries; every served record carried geometry.
EXPECTED_NULL_GEOMETRY_COUNT: int | None = 9
EXPECTED_REFUSED_IDS: tuple[int, ...] | None = (
    1293173, 2520496, 4245000, 4694427, 7059540, 7059541, 7098625, 7135496, 7157221)
# Producer-served zero-area rings are retained verbatim (see
# ``_polygon_wkb``): 13 rings over 13 features in the complete 2026-11-13
# capture. The count and the feature-id set are pinned from
# ``build_nhd_waterbody_baseline``; any change fails closed.
EXPECTED_DEGENERATE_RING_COUNT: int | None = 13
EXPECTED_DEGENERATE_RING_IDS: tuple[int, ...] | None = (
    279258, 2727549, 2954106, 3590176, 3601265, 4575241, 5019081, 5144328,
    5802262, 7072361, 7074673, 7147977, 7151432)
# Feature-type population observed 2026-11-13 through the producer's own
# statistics channel; the total agrees with the pinned count and both producer
# hosts answer it identically.
EXPECTED_FEATURETYPE_DISTRIBUTION = {1: 85220, 2: 9235, 3: 7063520, 4: 968}
EXPECTED_FEATURETYPE_LABELS = {1: "River", 2: "Canal", 3: "Lake", 4: "Ocean or Great Lake"}
EXPECTED_LAYER_EXTENT = {"xmin": -19951755.6817, "ymin": -1622985.3665999994,
                         "xmax": 20021551.747199997, "ymax": 11575301.958199998}
BASELINE_PENDING_NOTE = (
    "The record, geometry and ID-roster baseline digests, the null-geometry "
    "count, the producer-refusal id set and the producer-served degenerate "
    "(zero-area) ring count/id set were unpinned when this draft was "
    "implemented (the 7,158,943-feature population cannot be captured inside "
    "one session, so no complete capture existed on 2026-11-13 when the draft "
    "was written). They were computed by the first complete capture "
    "(3,037 retained pages, fetch window 2026-11-13 07:24-11:23Z, "
    "computed_baseline 19:51Z) and are now pinned as module constants; any "
    "later capture that disagrees fails closed.")

NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("id3dhp", "esriFieldTypeString", True, 7),
    ("featuredate", "esriFieldTypeDate", True, 8),
    ("mainstemid", "esriFieldTypeString", True, 200),
    ("gnisid", "esriFieldTypeInteger", True, None),
    ("gnisidlabel", "esriFieldTypeString", True, 120),
    ("featuretype", "esriFieldTypeSmallInteger", True, None),
    ("featuretypelabel", "esriFieldTypeString", True, 200),
    ("areasqkm", "esriFieldTypeDouble", True, None),
    ("workunitid", "esriFieldTypeString", True, 100),
    ("Shape__Length", "esriFieldTypeDouble", True, None),
    ("Shape__Area", "esriFieldTypeDouble", True, None),
)
METADATA_NAMES = ("service_doc", "alias_service", "service_before", "layer_before",
                  "count_before")


class NhdWaterbodyError(ValueError):
    """The source snapshot cannot be represented as the pinned Waterbody candidate."""

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
        raise NhdWaterbodyError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _count_url() -> str:
    return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _page_url(last_objectid: int, size: int) -> str:
    return _query({"where": f"OBJECTID>{last_objectid}", "outFields": "*",
                   "returnGeometry": "true", "returnZ": "true",
                   "outSR": str(SOURCE_CRS_WKID), "orderByFields": "OBJECTID ASC",
                   "resultRecordCount": str(size), "f": "json"})


def _record_url(oid: int, *, geometry: bool) -> str:
    return _query({"objectIds": str(oid), "outFields": "*",
                   "returnGeometry": "true" if geometry else "false",
                   **({"returnZ": "true"} if geometry else {}),
                   **({"outSR": str(SOURCE_CRS_WKID)} if geometry else {}),
                   "f": "json"})


def _attrs_keyset_url(last_objectid: int) -> str:
    return _query({"where": f"OBJECTID>{last_objectid}", "outFields": "*",
                   "returnGeometry": "false", "orderByFields": "OBJECTID ASC",
                   "resultRecordCount": "1", "f": "json"})


def _page_name(last_objectid: int, size: int) -> str:
    return f"page_{last_objectid:09d}_rec{size:04d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    body = name.removeprefix("page_")
    left, separator, right = body.partition("_")
    if (not separator or not left.isdigit() or len(left) != 9
            or not right.startswith("rec") or len(right) != 7
            or not right[3:].isdigit()):
        raise NhdWaterbodyError("source_page_name_invalid:" + name)
    return int(left), int(right[3:])


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGlobalID": pa.string()}.get(str(field["type"]), pa.string())


_GEOMETRY_COLUMN = "geometry"


def _polygon_wkb(geometry: object, *, degenerate_rings: list | None = None) -> bytes | None:
    """WKB for one feature's Esri rings under the producer's verified ring semantics.

    Esri JSON (verified on the pinned captures of this producer's Waterbody
    layer): every ring is explicitly closed, clockwise rings are exteriors,
    counter-clockwise rings are holes, and every ordinate triple carries the
    layer's Z value (``returnZ=true`` response). Vertices are passed to
    shapely verbatim; exteriors with no claimable hole keep their ring order
    and holes join their smallest containing exterior.

    The real population also serves closed zero-area rings (13 rings over 13
    features observed in the complete 2026-11-13 capture: four-vertex spikes
    whose shoelace area collapses to exactly ``0.0`` in float64 under the
    per-term computation below). They are the producer's own coordinates, so
    they are retained verbatim rather than refused: a zero-area ring joins its
    smallest covering exterior as an additional (coverage-neutral) hole ring,
    and a zero-area ring covered by no exterior of its feature — or a feature
    whose only rings are degenerate — is retained as its own zero-area shell
    polygon. ``degenerate_rings`` optionally collects the retained zero-area
    rings so callers can pin the degenerate-ring contract.
    """
    if geometry is None:
        return None
    if not isinstance(geometry, dict) or "rings" not in geometry \
            or not isinstance(geometry["rings"], list) or not geometry["rings"]:
        raise NhdWaterbodyError("source_polygon_geometry_invalid")
    if set(geometry) - {"hasZ", "rings"}:
        raise NhdWaterbodyError("unexpected_source_geometry_members")
    if geometry.get("hasZ") is not True:
        raise NhdWaterbodyError("source_geometry_z_flag_missing")
    exteriors: list[list] = []
    holes: list[list] = []
    degenerate: list[list] = []
    standalone: list[list] = []
    for ring in geometry["rings"]:
        if not isinstance(ring, list) or len(ring) < 4:
            raise NhdWaterbodyError("source_polygon_ring_invalid")
        area = 0.0
        for index, vertex in enumerate(ring):
            if not isinstance(vertex, list) or len(vertex) != 3:
                raise NhdWaterbodyError("source_polygon_ordinate_contract_drift")
            x, y, z = vertex
            if not (type(x) in (int, float) and type(y) in (int, float) and type(z) in (int, float)):
                raise NhdWaterbodyError("source_polygon_ordinate_not_numeric")
            if not (all(value == value and -1e308 < value < 1e308 for value in (x, y, z))):
                raise NhdWaterbodyError("source_polygon_ordinate_not_finite")
            if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                raise NhdWaterbodyError("source_coordinate_range_drift")
            nxt = ring[(index + 1) % len(ring)]
            area += x * nxt[1] - nxt[0] * y
        area /= 2
        if ring[0] != ring[-1]:
            raise NhdWaterbodyError("source_polygon_ring_not_closed")
        if area == 0:
            degenerate.append(ring)
            if degenerate_rings is not None:
                degenerate_rings.append(ring)
        else:
            (exteriors if area < 0 else holes).append(ring)
    if not exteriors:
        if degenerate and not holes:
            # The producer served no enclosing exterior, so each retained
            # zero-area ring stands as its own shell (coordinates verbatim,
            # no coverage claimed).
            polygons = [Polygon(ring) for ring in degenerate]
            return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb
        raise NhdWaterbodyError("source_polygon_exterior_missing")
    shells = [(Polygon(ring), _signed_area(ring), index) for index, ring in enumerate(exteriors)]
    # Prepared shell geometries: hole containment is queried against every
    # exterior, and the producer's main-stem features carry thousands of holes
    # over six-figure-vertex exteriors where unprepared contains() per hole is
    # pathologically slow. Preparation builds an index only and the containment
    # results are identical.
    for shell, _area, _index in shells:
        shapely.prepare(shell)
    assigned: dict[int, list] = {index: [] for index in range(len(exteriors))}
    for hole in holes:
        probe = Polygon(hole).representative_point()
        candidates = [(abs(area), index) for shell, area, index in shells if shell.contains(probe)]
        if not candidates:
            raise NhdWaterbodyError("source_polygon_hole_unassigned")
        assigned[min(candidates)[1]].append(hole)
    for ring in degenerate:
        probe = Point(ring[0])
        candidates = [(abs(area), index) for shell, area, index in shells if shell.covers(probe)]
        if candidates:
            assigned[min(candidates)[1]].append(ring)
        else:
            standalone.append(ring)
    polygons = []
    for _, _, index in sorted(shells, key=lambda item: item[2]):
        polygons.append(Polygon(exteriors[index], assigned[index]))
    polygons.extend(Polygon(ring) for ring in standalone)
    return (MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]).wkb


def _signed_area(ring: list) -> float:
    area = 0.0
    for index, vertex in enumerate(ring):
        nxt = ring[(index + 1) % len(ring)]
        area += vertex[0] * nxt[1] - nxt[0] * vertex[1]
    return area / 2


def _validate_value(name: str, esri_type: str, nullable: bool, value: object) -> None:
    if value is None:
        if not nullable:
            raise NhdWaterbodyError("source_nonnullable_value_is_null:" + name)
        return
    if esri_type == "esriFieldTypeString":
        if not isinstance(value, str):
            raise NhdWaterbodyError("source_native_type_changed:" + name)
        if name == "id3dhp" and value and any(
                character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in value):
            raise NhdWaterbodyError("source_id3dhp_value_drift")
        return
    if esri_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger",
                     "esriFieldTypeDate"):
        if type(value) is not int:
            raise NhdWaterbodyError("source_native_type_changed:" + name)
        return
    if esri_type == "esriFieldTypeDouble":
        if type(value) not in (int, float) or not (value == value and -1e308 < value < 1e308):
            raise NhdWaterbodyError("source_native_type_changed:" + name)
        return
    raise NhdWaterbodyError("unsupported_source_physical_type:" + esri_type)


def _validate_attrs(attrs: Mapping[str, object], expected_names: tuple[str, ...]) -> None:
    if list(attrs) != list(expected_names):
        raise NhdWaterbodyError("source_record_schema_or_geometry_drift")
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        _validate_value(name, esri_type, nullable, attrs[name])
    featuretype = attrs["featuretype"]
    if featuretype not in EXPECTED_FEATURETYPE_LABELS:
        raise NhdWaterbodyError("source_featuretype_outside_population:" + str(attrs["OBJECTID"]))
    if attrs["featuretypelabel"] != EXPECTED_FEATURETYPE_LABELS[featuretype]:
        raise NhdWaterbodyError("source_featuretype_label_drift:" + str(attrs["OBJECTID"]))


def _validate_feature(feature: object, expected_names: tuple[str, ...],
                      last_objectid: int) -> tuple[int, int, int]:
    if not isinstance(feature, dict):
        raise NhdWaterbodyError("source_feature_shape_changed")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict):
        raise NhdWaterbodyError("source_record_schema_or_geometry_drift")
    objectid = attrs.get("OBJECTID")
    if type(objectid) is not int or objectid <= 0:
        raise NhdWaterbodyError("source_object_id_type_changed")
    if objectid <= last_objectid:
        raise NhdWaterbodyError("source_object_ids_not_strictly_increasing")
    ring_count = 0
    vertex_count = 0
    if geometry is not None:
        if not isinstance(geometry, dict):
            raise NhdWaterbodyError("unexpected_source_geometry_members")
        rings = geometry.get("rings")
        if not isinstance(rings, list):
            raise NhdWaterbodyError("unexpected_source_geometry_members")
        ring_count = len(rings)
        vertex_count = sum(len(ring) for ring in rings)
    _validate_attrs(attrs, expected_names)
    return objectid, ring_count, vertex_count


def _load_receipt(capture_root: Path, name: str) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NhdWaterbodyError("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise NhdWaterbodyError("retained_capture_receipt_invalid:" + name)
    return receipt


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = _load_receipt(capture_root, name)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise NhdWaterbodyError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"pages": [], "tail": None, "refused": {}, "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NhdWaterbodyError("capture_state_invalid") from exc
    if (not isinstance(state, dict) or not isinstance(state.get("pages"), list)
            or not isinstance(state.get("refused"), dict)
            or not (state.get("tail") is None or isinstance(state.get("tail"), dict))):
        raise NhdWaterbodyError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def _budget_guard(budget: list[int]) -> Callable[[str, bytes], None]:
    def before_retain(kind: str, data: bytes) -> None:
        budget[0] += len(data)
        if budget[0] > MAX_CAPTURE_BYTES:
            raise NhdWaterbodyError("source_capture_budget_exceeded")
    return before_retain


def _budget_check(budget: list[int]) -> None:
    if budget[0] > MAX_CAPTURE_BYTES:
        raise NhdWaterbodyError("source_capture_budget_exceeded")


def _retained_or_fetch(client: httpx.Client | None, capture_root: Path, name: str,
                       url: str, *, budget: list[int] | None = None
                       ) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except NhdWaterbodyError:
            if client is None:
                raise
            for suffix in ("body", "wire", "receipt.json"):
                (capture_root / f"{name}.{suffix}").unlink(missing_ok=True)
    if client is None:
        raise NhdWaterbodyError("retained_capture_file_missing:" + name)
    return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES,
                       before_retain=_budget_guard(budget) if budget is not None else None)


def _witness_attempt(capture_root: Path, name: str) -> None:
    """Move one complete-but-unusable retained answer aside as dated evidence.

    This covers both producer error documents (which carry a self-consistent
    receipt, so without the witness step a resumed capture would replay the
    same error forever) and abandoned failed attempts. Witnessed evidence never
    enters the capture roster.
    """
    stamp = time.time_ns()
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
        _witness_attempt(capture_root, name)
        raise NhdWaterbodyError("source_page_error:" + name)


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def _failed_receipt_error_code(capture_root: Path, name: str) -> str | None:
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, ValueError):
        return None
    code = receipt.get("error_code")
    return code if isinstance(code, str) else None


def _capture_doc(client: httpx.Client, url: str, capture_root: Path, name: str,
                 budget: list[int]) -> tuple[bytes, dict[str, object]]:
    """Capture one producer document, retrying bounded times on transient failures."""
    for attempt in range(1, DOC_ATTEMPTS + 1):
        _budget_check(budget)
        try:
            return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES,
                               before_retain=_budget_guard(budget))
        except (ValueError, httpx.HTTPError) as exc:
            if "source_capture_budget_exceeded" in str(exc):
                raise
            retryable = (isinstance(exc, httpx.HTTPError)
                         or str(exc).startswith("incomplete_source_capture:"))
            if attempt >= DOC_ATTEMPTS or not retryable:
                raise
            time.sleep(DOC_RETRY_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
    raise AssertionError("unreachable_retry_state")


def _fetch_page(active: httpx.Client, capture_root: Path, budget: list[int],
                last_objectid: int, size: int) -> tuple[bytes, dict[str, object]]:
    """Fetch one keyset page at ``size`` with bounded transport retries.

    A producer error document or an over-cap answer aborts the attempt
    immediately (deterministic for the requested window, so the caller bisects
    the requested record count instead of retrying it); transport faults are
    retried with backoff and every failed attempt is retained as witnessed
    evidence that never enters the capture roster.
    """
    name = _page_name(last_objectid, size)
    url = _page_url(last_objectid, size)
    last_error: Exception | None = None
    for attempt in range(1, PAGE_ATTEMPTS + 1):
        _budget_check(budget)
        try:
            body, receipt = capture_get(active, url, capture_root, name,
                                        max_bytes=MAX_BODY_BYTES,
                                        before_retain=_budget_guard(budget))
        except (ValueError, httpx.HTTPError) as exc:
            if "source_capture_budget_exceeded" in str(exc):
                raise
            last_error = exc
            error_code = _failed_receipt_error_code(capture_root, name)
            if error_code in ("wire_limit_exceeded", "decoded_limit_exceeded"):
                _witness_attempt(capture_root, name)
                raise NhdWaterbodyError("source_page_body_limit:" + name) from exc
            if attempt < PAGE_ATTEMPTS:
                _witness_attempt(capture_root, name)
                time.sleep(PAGE_RETRY_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
                continue
            _witness_attempt(capture_root, name)
            raise NhdWaterbodyError("source_page_fetch_failed:" + name) from exc
        if _producer_error_doc(body):
            _witness_attempt(capture_root, name)
            raise NhdWaterbodyError("source_page_error:" + name)
        return body, receipt
    raise NhdWaterbodyError("source_page_fetch_failed:" + name) from last_error


def _bisect(size: int) -> int:
    if size <= 1:
        raise AssertionError("bisect_below_single_record")
    return max(1, size // 2)


def _capture_attrs_row(active: httpx.Client, capture_root: Path, budget: list[int],
                       oid: int) -> tuple[dict[str, object], dict[str, object]]:
    """Capture one record's attributes through the working attributes-only channel.

    The retained attributes page is reused when a resumed capture already holds
    it (the request URL is deterministic).
    """
    name = f"refused_attrs_{oid:09d}"
    if (capture_root / f"{name}.body").exists():
        body, receipt = _read_capture(capture_root, name)
    else:
        body, receipt = _capture_doc(active, _record_url(oid, geometry=False),
                                     capture_root, name, budget)
    doc = json.loads(body)
    rows = doc.get("features") if isinstance(doc, dict) else None
    if (not isinstance(rows, list) or len(rows) != 1
            or not isinstance(rows[0], dict) or not isinstance(rows[0].get("attributes"), dict)
            or rows[0].get("geometry") is not None
            or rows[0]["attributes"].get("OBJECTID") != oid):
        raise NhdWaterbodyError("source_refused_attrs_record_drift:" + name)
    attrs = rows[0]["attributes"]
    _validate_attrs(attrs, tuple(entry[0] for entry in NATIVE_FIELDS))
    return attrs, receipt


def _refuse_single_record(active: httpx.Client, alias_client: httpx.Client,
                          capture_root: Path, budget: list[int], last_objectid: int,
                          pages: list[dict[str, object]], persist: Callable[[], None]
                          ) -> tuple[int, bool]:
    """Stage the refusal evidence for the record following ``last_objectid``.

    The record's identity comes from the producer's working attributes-only
    keyset channel (geometry is never requested there); the geometry request is
    then retried bounded times on both producer hosts. When both hosts persist
    with their error document, the record joins the capture's refusal set: its
    attributes stay staged verbatim through the attributes-only channel and its
    candidate geometry is explicitly gated to null. Returns the record's
    OBJECTID and whether a producer host ended up serving the geometry (the
    answer is then retained as the single-record keyset page for this position
    and the walk continues with a served record).
    """
    position = last_objectid + 1
    probe_name = f"refused_probe_{position:09d}_attrs"
    probe_body, _probe_receipt = _retained_or_fetch(
        active, capture_root, probe_name, _attrs_keyset_url(last_objectid), budget=budget)
    probe = json.loads(probe_body)
    probe_rows = probe.get("features") if isinstance(probe, dict) else None
    if (not isinstance(probe_rows, list) or len(probe_rows) != 1
            or not isinstance(probe_rows[0], dict)
            or not isinstance(probe_rows[0].get("attributes"), dict)):
        raise NhdWaterbodyError("source_refused_record_identity_unavailable")
    oid = probe_rows[0]["attributes"].get("OBJECTID")
    if type(oid) is not int or oid <= last_objectid:
        raise NhdWaterbodyError("source_refused_record_identity_unavailable")
    attrs, attrs_receipt = _capture_attrs_row(active, capture_root, budget, oid)
    del attrs
    for attempt in range(1, PAGE_ATTEMPTS + 1):
        for label, probe_client in (("primary", active), ("alias", alias_client)):
            probe_geo_name = f"refused_probe_{position:09d}_geo_{label}"
            _budget_check(budget)
            try:
                body, geo_receipt = capture_get(
                    probe_client, _record_url(oid, geometry=True), capture_root,
                    probe_geo_name, max_bytes=MAX_BODY_BYTES,
                    before_retain=_budget_guard(budget))
            except (ValueError, httpx.HTTPError):
                _witness_attempt(capture_root, probe_geo_name)
                continue
            if _producer_error_doc(body):
                _witness_attempt(capture_root, probe_geo_name)
                continue
            page_name = _page_name(last_objectid, 1)
            for suffix in ("body", "wire", "receipt.json"):
                (capture_root / f"{page_name}.{suffix}").write_bytes(
                    (capture_root / f"{probe_geo_name}.{suffix}").read_bytes())
            pages.append({"name": page_name, "start_after": last_objectid, "size": 1,
                          "last_objectid": oid, "features": 1,
                          "exceeded_transfer_limit": None,
                          "captured_at": geo_receipt["observed_at"],
                          "body_sha256": _sha(body), "rescued_host": label})
            persist()
            return oid, True
        if attempt < PAGE_ATTEMPTS:
            time.sleep(PAGE_RETRY_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
    return oid, False


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(60.0, read=300.0),
        headers={"Connection": "close", "User-Agent": USER_AGENT},
    )


def _doc_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=True, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": DOC_USER_AGENT},
    )


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def _close_owned(owner_client: bool, owner_doc: bool, owner_alias: bool,
                 active: httpx.Client, docs_active: httpx.Client, alias: httpx.Client) -> None:
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()
    if owner_alias:
        alias.close()


def fetch_nhd_waterbody_pages(capture_root: Path, *, client: httpx.Client | None = None,
                              doc_client: httpx.Client | None = None,
                              alias_client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page.
    The walk keeps one accepted page per walk position: each position starts at
    ``PAGE_SIZE`` records and, when the producer answers with its error
    document (or with an over-cap response), bisects the requested record count
    down to a single record before refusing it. An empty page or a short page
    without ``exceededTransferLimit`` ends the population; the canonical empty
    tail page (same walk position, producer page size) is then retained as the
    completeness witness. Bisection is completeness-neutral: the pinned
    count/roster contract and the strict ascending walk prove the population,
    and page boundaries are never part of any pin.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = dict(_load_state(capture_root))
    pages: list[dict[str, object]] = list(state.get("pages", []))
    tail: dict[str, object] | None = state.get("tail")
    refused: dict[str, object] = dict(state.get("refused", {}))

    def persist() -> None:
        _save_state(capture_root, {"pages": pages, "tail": tail, "refused": refused,
                                   "complete": False})

    owner_client = client is None
    owner_doc = doc_client is None
    owner_alias = alias_client is None
    active = client or _default_client()
    docs_active = doc_client or _doc_client()
    alias = alias_client or _default_client()
    budget = [0]
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
            retained[name] = _retained_or_fetch(active_client, capture_root, name, url,
                                                budget=budget)
            if name != "service_doc":
                _reject_error_response(capture_root, name, retained[name][0])
        count_before = json.loads(retained["count_before"][0]).get("count")
        if count_before != EXPECTED_COUNT:
            raise NhdWaterbodyError("source_count_drift")
        last_objectid = pages[-1]["last_objectid"] if pages else 0
        while not (capture_root / "page_tail.body").exists():
            size = PAGE_SIZE
            while True:
                try:
                    body, receipt = _fetch_page(active, capture_root, budget,
                                                last_objectid, size)
                except NhdWaterbodyError as exc:
                    bisectable = (exc.code == "source_page_error"
                                  or exc.code.startswith("source_page_error:")
                                  or exc.code == "source_page_body_limit"
                                  or exc.code.startswith("source_page_body_limit:"))
                    if not bisectable:
                        raise
                    if size > 1:
                        size = _bisect(size)
                        continue
                    oid, rescued = _refuse_single_record(
                        active, alias, capture_root, budget, last_objectid, pages, persist)
                    if not rescued:
                        refused[str(oid)] = {"attrs_name": f"refused_attrs_{oid:09d}",
                                             "captured_at": datetime.now(timezone.utc).isoformat()}
                        persist()
                    last_objectid = oid
                    break
                page = json.loads(body)
                if not isinstance(page, dict) or "error" in page:
                    raise NhdWaterbodyError("source_page_error")
                features = page.get("features")
                if not isinstance(features, list):
                    raise NhdWaterbodyError("source_page_missing_features")
                if not features:
                    if last_objectid == 0:
                        raise NhdWaterbodyError("source_page_population_incomplete")
                    # The population ends here: retain the canonical empty tail
                    # page (same walk position, producer page size) as the
                    # completeness witness.
                    tail_body, tail_receipt = _retained_or_fetch(
                        active, capture_root, "page_tail",
                        _page_url(last_objectid, PAGE_SIZE), budget=budget)
                    _reject_error_response(capture_root, "page_tail", tail_body)
                    if json.loads(tail_body).get("features") != []:
                        raise NhdWaterbodyError("source_tail_page_not_empty")
                    tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                            "captured_at": tail_receipt["observed_at"],
                            "body_sha256": _sha(tail_body)}
                    persist()
                    break
                cursor = last_objectid
                for feature in features:
                    cursor, _rings, _verts = _validate_feature(
                        feature, tuple(entry[0] for entry in NATIVE_FIELDS), cursor)
                more = page.get("exceededTransferLimit")
                if len(features) < size and more is True:
                    raise NhdWaterbodyError("source_page_transfer_truncated")
                pages.append({"name": _page_name(last_objectid, size),
                              "start_after": last_objectid, "size": size,
                              "last_objectid": cursor, "features": len(features),
                              "exceeded_transfer_limit": more,
                              "captured_at": receipt["observed_at"],
                              "body_sha256": _sha(body)})
                last_objectid = cursor
                persist()
                if len(pages) > EXPECTED_COUNT:
                    raise NhdWaterbodyError("source_page_count_exceeds_pinned_population")
                if len(features) < size and more is not True:
                    # The accepted short page is the final data page: retain the
                    # canonical empty tail witness at the producer page size.
                    tail_body, tail_receipt = _retained_or_fetch(
                        active, capture_root, "page_tail",
                        _page_url(last_objectid, PAGE_SIZE), budget=budget)
                    _reject_error_response(capture_root, "page_tail", tail_body)
                    if json.loads(tail_body).get("features") != []:
                        raise NhdWaterbodyError("source_tail_page_not_empty")
                    tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                            "captured_at": tail_receipt["observed_at"],
                            "body_sha256": _sha(tail_body)}
                    persist()
                    break
                break
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise NhdWaterbodyError("source_tail_page_not_empty")
        for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                          ("service_after", SERVICE_URL + "?f=pjson"),
                          ("count_after", _count_url())):
            body, _receipt = _retained_or_fetch(active, capture_root, name, url, budget=budget)
            _reject_error_response(capture_root, name, body)
            retained[name] = (body, _read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise NhdWaterbodyError("source_metadata_or_count_changed_during_capture")
    except NhdWaterbodyError:
        _close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)
        raise
    except Exception as exc:
        _close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)
        raise NhdWaterbodyError("source_capture_failed:" + type(exc).__name__) from exc
    _close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)

    last_captured_at = str(tail["captured_at"])
    # Construction happens once at seal time (capture_nhd_waterbody); retention
    # and validation stay the only fetch-time side effects.
    extracted = _stream_validate(capture_root, parquet_sink=None)
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": EXPECTED_COUNT,
        "source_max_objectid": extracted["max_objectid"],
        "source_null_geometry_count": extracted["null_geometry_count"],
        "source_refused_null_geometry_count": extracted["refused_null_count"],
        "source_refused_ids": sorted(int(key) for key in refused),
        "source_degenerate_ring_count": extracted["degenerate_ring_count"],
        "source_degenerate_ring_ids": extracted["degenerate_ring_ids"],
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_featuretype_distribution": extracted["featuretype_distribution"],
        "observed_bounds": extracted["observed_bounds"],
        "id3dhp_distinct_count": extracted["id3dhp_distinct_count"],
        "resumed_pages": len(pages),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    _save_state(capture_root, {"pages": pages, "tail": tail, "refused": refused,
                               "complete": True})
    return {"capture_directory": str(capture_root), "pages": len(pages), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_refused_ids": sorted(int(key) for key in refused)}


def _validate_service_document(service: object, label: str) -> None:
    if not isinstance(service, dict):
        raise NhdWaterbodyError("source_service_or_layer_document_invalid")
    if service.get("copyrightText") != EXPECTED_COPYRIGHT_TEXT:
        raise NhdWaterbodyError("usgs_3dhp_service_identity_drift:" + label)
    layers = service.get("layers")
    roster = [(entry.get("id"), entry.get("name")) for entry in layers
              if isinstance(entry, dict)] if isinstance(layers, list) else None
    if not isinstance(roster, list):
        raise NhdWaterbodyError("usgs_3dhp_service_layer_roster_drift:" + label)
    if roster[:len(CORE_SERVICE_LAYERS)] != CORE_SERVICE_LAYERS:
        raise NhdWaterbodyError("usgs_3dhp_service_layer_roster_drift:" + label)
    metadata_rows = roster[len(CORE_SERVICE_LAYERS):]
    if (len(metadata_rows) != len(SPATIAL_METADATA_IDS)
            or {entry_id for entry_id, _name in metadata_rows} != set(SPATIAL_METADATA_IDS)
            or {name for _entry_id, name in metadata_rows} != set(SPATIAL_METADATA_NAMES)):
        raise NhdWaterbodyError("usgs_3dhp_service_layer_roster_drift:" + label)
    description = str(service.get("serviceDescription", ""))
    if "3DHP_all" not in description or "3D Hydrography Program" not in description:
        raise NhdWaterbodyError("usgs_3dhp_service_description_drift:" + label)
    reference = service.get("spatialReference")
    if not isinstance(reference, dict) or reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdWaterbodyError("source_service_spatial_reference_drift:" + label)


def _validate_layer_document(layer: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(layer, dict):
        raise NhdWaterbodyError("source_service_or_layer_document_invalid")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != GEOMETRY_TYPE):
        raise NhdWaterbodyError("exact_3dhp_waterbody_layer_metadata_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise NhdWaterbodyError("source_layer_z_or_m_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise NhdWaterbodyError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != PAGE_SIZE:
        raise NhdWaterbodyError("source_record_limit_drift")
    source_reference = layer.get("sourceSpatialReference")
    if (not isinstance(source_reference, dict)
            or source_reference.get("wkid") not in (None, NATIVE_LAYER_SR["wkid"])
            or source_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]):
        raise NhdWaterbodyError("source_layer_spatial_reference_drift")
    extent = layer.get("extent", {})
    extent_reference = extent.get("spatialReference")
    if not isinstance(extent_reference, dict) \
            or extent_reference.get("latestWkid") != NATIVE_LAYER_SR["latestWkid"]:
        raise NhdWaterbodyError("source_layer_extent_spatial_reference_drift")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise NhdWaterbodyError("source_layer_extent_drift")
    return _profile_schema(layer)


def _read_page_document(capture_root: Path, name: str, size: int) -> dict:
    page = json.loads(_read_capture(capture_root, name)[0])
    if not isinstance(page, dict) or "error" in page:
        raise NhdWaterbodyError("source_page_error:" + name)
    reference = page.get("spatialReference")
    if (not isinstance(reference, dict)
            or reference.get("wkid") not in (None, SOURCE_CRS_WKID)
            or reference.get("latestWkid") != SOURCE_CRS_WKID):
        raise NhdWaterbodyError("source_page_spatial_reference_drift:" + name)
    if page.get("objectIdFieldName") != "OBJECTID":
        raise NhdWaterbodyError("source_id_field_changed:" + name)
    if page.get("hasZ") is not True:
        raise NhdWaterbodyError("source_page_z_flag_missing:" + name)
    features = page.get("features")
    if not isinstance(features, list) or not features or len(features) > size:
        raise NhdWaterbodyError("source_page_row_count_incomplete:" + name)
    if page.get("exceededTransferLimit") is True and len(features) < size:
        raise NhdWaterbodyError("source_page_transfer_truncated:" + name)
    return page


def _ordered_page_entries(state_pages: list[dict[str, object]]
                          ) -> list[tuple[int, int, dict[str, object]]]:
    entries: list[tuple[int, int, dict[str, object]]] = []
    for entry in state_pages:
        if not isinstance(entry, dict):
            raise NhdWaterbodyError("capture_state_page_entries_invalid")
        start, size = _parse_page_name(str(entry.get("name")))
        if start != entry.get("start_after") or size != entry.get("size"):
            raise NhdWaterbodyError("capture_state_page_entries_invalid")
        entries.append((start, size, entry))
    entries.sort(key=lambda item: (item[0], item[1]))
    starts = [start for start, _size, _entry in entries]
    if len(set(starts)) != len(starts) or starts != sorted(starts):
        raise NhdWaterbodyError("capture_state_page_entries_invalid")
    return entries


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained page byte and optionally reconstruct the native geoparquet.

    Rows are streamed in ascending OBJECTID order: served pages in walk order
    with producer-refused records (captured through the working attributes-only
    channel) merged at their roster position with explicitly gated null
    candidate geometry. Bounds are tracked incrementally because the full
    population carries tens of millions of vertices.
    """
    docs = {name: json.loads(_read_capture(capture_root, name)[0])
            for name in METADATA_NAMES if name != "service_doc"}
    if b"3dhp.nationalmap.gov" not in _read_capture(capture_root, "service_doc")[0]:
        raise NhdWaterbodyError("usgs_service_documentation_link_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    _validate_service_document(service, "service_before")
    alias = docs["alias_service"]
    _validate_service_document(alias, "alias_service")
    fields, schema_sha = _validate_layer_document(layer)
    declared = {name: (esri_type, nullable) for name, esri_type, nullable, _l in NATIVE_FIELDS}
    if declared != {field["name"]: (field["type"], field["nullable"]) for field in fields}:
        raise NhdWaterbodyError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise NhdWaterbodyError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise NhdWaterbodyError("source_count_drift")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise NhdWaterbodyError("source_metadata_or_count_changed_during_capture")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise NhdWaterbodyError("source_field_schema_changed_during_capture")
    tail = json.loads(_read_capture(capture_root, "page_tail")[0])
    if tail.get("features") != []:
        raise NhdWaterbodyError("source_tail_page_not_empty")

    expected_names = tuple(entry["name"] for entry in fields)
    arrow_fields = [pa.field(field["name"], _arrow_type(field), nullable=True) for field in fields]
    arrow_fields.append(pa.field(_GEOMETRY_COLUMN, pa.binary(), nullable=True))
    crs = pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()
    geo = {"version": "1.1.0", "primary_column": _GEOMETRY_COLUMN,
           "columns": {_GEOMETRY_COLUMN: {"encoding": "WKB",
                                          "geometry_types": ["Polygon Z", "MultiPolygon Z"],
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

    state = _load_state(capture_root)
    state_pages: list[dict[str, object]] = list(state.get("pages", []))
    state_refused: dict[str, object] = dict(state.get("refused", {}))
    if not state_pages:
        raise NhdWaterbodyError("capture_state_page_entries_invalid")
    refused_oids = sorted(int(key) for key in state_refused)
    if EXPECTED_REFUSED_IDS is not None and tuple(refused_oids) != tuple(
            sorted(EXPECTED_REFUSED_IDS)):
        raise NhdWaterbodyError("source_refused_ids_drift")

    ids = array("q")
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    emitted_last = 0
    null_geometry_count = 0
    refused_null_count = 0
    ring_count = 0
    vertex_count = 0
    degenerate_ring_count = 0
    degenerate_feature_ids: set[int] = set()
    featuretype_distribution: dict[object, int] = {}
    bounds: list[float] | None = None
    id3dhp_seen: set[str] = set()

    def track_bounds(vertex: list) -> None:
        nonlocal bounds
        x, y = vertex[0], vertex[1]
        if bounds is None:
            bounds = [x, y, x, y]
        else:
            if x < bounds[0]:
                bounds[0] = x
            elif x > bounds[2]:
                bounds[2] = x
            if y < bounds[1]:
                bounds[1] = y
            elif y > bounds[3]:
                bounds[3] = y

    def emit_refused(oid: int) -> None:
        nonlocal emitted_last, null_geometry_count, refused_null_count
        if oid <= emitted_last:
            raise NhdWaterbodyError("source_refused_records_outside_walk_order")
        attrs_name = f"refused_attrs_{oid:09d}"
        attrs_doc = json.loads(_read_capture(capture_root, attrs_name)[0])
        attrs_rows = attrs_doc.get("features")
        if (not isinstance(attrs_rows, list) or len(attrs_rows) != 1
                or not isinstance(attrs_rows[0], dict)
                or not isinstance(attrs_rows[0].get("attributes"), dict)
                or attrs_rows[0].get("geometry") is not None
                or attrs_rows[0]["attributes"].get("OBJECTID") != oid):
            raise NhdWaterbodyError("source_refused_attrs_record_drift:" + attrs_name)
        attrs = attrs_rows[0]["attributes"]
        _validate_attrs(attrs, expected_names)
        emitted_last = oid
        record_hash.update(_canonical(attrs) + b"\x00")
        ids.append(oid)
        null_geometry_count += 1
        refused_null_count += 1
        geometry_hash.update(_canonical([oid, None]) + b"\x00")
        featuretype = attrs["featuretype"]
        featuretype_distribution[featuretype] = \
            featuretype_distribution.get(featuretype, 0) + 1
        id3dhp_value = attrs["id3dhp"]
        if isinstance(id3dhp_value, str):
            id3dhp_seen.add(id3dhp_value)
        if writer is not None:
            batch = pa.Table.from_pydict({**{name: [attrs[name]] for name in expected_names},
                                          _GEOMETRY_COLUMN: [None]}, schema=schema)
            writer.write_table(batch)

    def emit_page(entry: tuple[int, int, dict[str, object]]) -> None:
        nonlocal emitted_last, null_geometry_count, ring_count, vertex_count
        nonlocal degenerate_ring_count
        start, size, state_entry = entry
        page = _read_page_document(capture_root, str(state_entry["name"]), size)
        features = page.get("features")
        columns: dict[str, list[object]] = {name: [] for name in expected_names}
        geometries: list[bytes | None] = []
        for feature in features:
            objectid, feature_rings, feature_vertices = _validate_feature(
                feature, expected_names, emitted_last)
            attrs, geometry = feature["attributes"], feature.get("geometry")
            emitted_last = objectid
            record_hash.update(_canonical(attrs) + b"\x00")
            ids.append(objectid)
            if geometry is None:
                null_geometry_count += 1
                geometries.append(None)
                geometry_hash.update(_canonical([objectid, None]) + b"\x00")
            else:
                rings = geometry["rings"]
                ring_count += feature_rings
                vertex_count += feature_vertices
                for ring in rings:
                    for vertex in ring:
                        track_bounds(vertex)
                feature_degenerate: list = []
                geometries.append(_polygon_wkb(geometry, degenerate_rings=feature_degenerate))
                if feature_degenerate:
                    degenerate_ring_count += len(feature_degenerate)
                    degenerate_feature_ids.add(objectid)
                geometry_hash.update(_canonical([objectid, rings]) + b"\x00")
            featuretype = attrs["featuretype"]
            featuretype_distribution[featuretype] = \
                featuretype_distribution.get(featuretype, 0) + 1
            id3dhp_value = attrs["id3dhp"]
            if isinstance(id3dhp_value, str):
                id3dhp_seen.add(id3dhp_value)
            for name in expected_names:
                columns[name].append(attrs[name])
        del start
        if writer is not None:
            batch = pa.Table.from_pydict({**columns, _GEOMETRY_COLUMN: geometries},
                                         schema=schema)
            writer.write_table(batch)

    try:
        refused_index = 0
        for entry in _ordered_page_entries(state_pages):
            while refused_index < len(refused_oids) and refused_oids[refused_index] <= entry[0]:
                emit_refused(refused_oids[refused_index])
                refused_index += 1
            emit_page(entry)
        while refused_index < len(refused_oids):
            emit_refused(refused_oids[refused_index])
            refused_index += 1
        if writer is not None:
            writer.close()
            writer = None
        if (len(ids) != EXPECTED_COUNT or not len(ids) or ids[0] != 1
                or emitted_last != EXPECTED_MAX_OBJECTID):
            raise NhdWaterbodyError("source_page_population_incomplete")
        ids_sha = _sha(_canonical(list(ids)))
        record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
        if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
            raise NhdWaterbodyError("source_value_or_geometry_baseline_drift")
        if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
            raise NhdWaterbodyError("source_geometry_or_coverage_drift")
        if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
            raise NhdWaterbodyError("source_id_roster_drift")
        if EXPECTED_NULL_GEOMETRY_COUNT is not None \
                and null_geometry_count != EXPECTED_NULL_GEOMETRY_COUNT:
            raise NhdWaterbodyError("source_null_geometry_count_drift")
        if EXPECTED_DEGENERATE_RING_COUNT is not None \
                and degenerate_ring_count != EXPECTED_DEGENERATE_RING_COUNT:
            raise NhdWaterbodyError("source_degenerate_ring_count_drift")
        if EXPECTED_DEGENERATE_RING_IDS is not None and tuple(
                sorted(degenerate_feature_ids)) != tuple(sorted(EXPECTED_DEGENERATE_RING_IDS)):
            raise NhdWaterbodyError("source_degenerate_ring_ids_drift")
        if featuretype_distribution != EXPECTED_FEATURETYPE_DISTRIBUTION:
            raise NhdWaterbodyError("source_featuretype_distribution_drift")
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
    return {"fields": fields, "schema_sha": schema_sha, "record_sha": record_sha,
            "geometry_sha": geometry_sha, "ids_sha": ids_sha,
            "null_geometry_count": null_geometry_count,
            "refused_null_count": refused_null_count,
            "parquet_path": sink_path,
            "ring_count": ring_count, "vertex_count": vertex_count,
            "degenerate_ring_count": degenerate_ring_count,
            "degenerate_ring_ids": sorted(degenerate_feature_ids),
            "featuretype_distribution": featuretype_distribution,
            "observed_bounds": (None if bounds is None else
                                {"xmin": bounds[0], "ymin": bounds[1],
                                 "xmax": bounds[2], "ymax": bounds[3]}),
            "max_objectid": emitted_last,
            "id3dhp_distinct_count": len(id3dhp_seen)}


def _retained_input_names(capture_root: Path) -> list[str]:
    """Every retained complete response that feeds the sealed candidate.

    Witnessed failed attempts (``.error-<stamp>`` evidence) stay on disk in the
    capture directory but are not candidate inputs.
    """
    names = sorted(path.name.removesuffix(".receipt.json")
                   for path in capture_root.glob("*.receipt.json")
                   if ".error-" not in path.name)
    return names


def _load_receipt_map(capture_root: Path) -> dict[str, dict[str, object]]:
    """Load every retained input receipt without holding page bodies in memory."""
    return {name: _load_receipt(capture_root, name)
            for name in _retained_input_names(capture_root)}


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


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *, resumed_pages: int,
                    first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"]},
            parquet_body)
    refused_oids = sorted(int(key) for key in
                          dict(_load_state(capture_root).get("refused", {})))
    null_geometry_count = int(extracted["null_geometry_count"])
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": SOURCE_CRS_WKID,
        "source_layer_native_sr": NATIVE_LAYER_SR,
        "geometry_type": "Polygon Z / MultiPolygon Z",
        "null_geometry_count": null_geometry_count,
        "served_null_geometry_count": null_geometry_count - len(refused_oids),
        "source_refused_ids": refused_oids,
        "source_refused_count": len(refused_oids),
        "producer_refusal_note":
            ("Records listed in source_refused_ids are part of the pinned population "
             "(they appear inside the pinned count and contiguous OBJECTID roster) but the "
             "producer's own REST publication persistently refuses to serve their geometry "
             "(HTTP 200 Esri error document code 500 'Error performing query operation' on "
             "both 3dhp.nationalmap.gov and the same-org alias hydro.nationalmap.gov/3DHP_all, "
             "single-record and windowed, retried with backoff, attempt evidence retained in "
             "the capture). Their attributes are served normally through the attributes-only "
             "channel and are staged verbatim with explicitly gated null candidate geometry; "
             "publication stays blocked pending the maintainer completeness/coverage decision "
             "for these records." if refused_oids else
             "Every record in the pinned population was served with its geometry by the "
             "producer's own REST publication."),
        "source_degenerate_ring_count": int(extracted["degenerate_ring_count"]),
        "source_degenerate_ring_ids": list(extracted["degenerate_ring_ids"]),
        "producer_degenerate_ring_note":
            ("Rings counted in source_degenerate_ring_count belong to the features listed in "
             "source_degenerate_ring_ids: closed producer-served rings whose shoelace area "
             "collapses to exactly 0.0 in float64 (four-vertex collinear spikes observed in "
             "the complete 2026-11-13 capture). They are the producer's own coordinates and "
             "are retained verbatim in the candidate WKB: a zero-area ring joins its smallest "
             "covering exterior as an additional coverage-neutral hole ring, and a zero-area "
             "ring covered by no exterior of its feature - or a feature whose only rings are "
             "degenerate - is retained as its own zero-area shell polygon. No coordinate was "
             "dropped, added or reordered; the pinned baseline digests already include these "
             "rings."),
        "unique_source_id_count": EXPECTED_COUNT,
        "source_ring_count": extracted["ring_count"],
        "source_vertex_count": extracted["vertex_count"],
        "source_max_objectid": extracted["max_objectid"],
        "source_featuretype_distribution": extracted["featuretype_distribution"],
        "observed_bounds": extracted["observed_bounds"],
        "declared_layer_extent_3857": EXPECTED_LAYER_EXTENT,
        "id3dhp_distinct_count": extracted["id3dhp_distinct_count"],
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
            or not EXPECTED_IDS_SHA256 or EXPECTED_NULL_GEOMETRY_COUNT is None
            or EXPECTED_REFUSED_IDS is None or EXPECTED_DEGENERATE_RING_COUNT is None
            or EXPECTED_DEGENERATE_RING_IDS is None),
        "producer_service_refresh_statement": EXPECTED_COPYRIGHT_TEXT,
        "source_release_date": None,
        "source_release_date_unknown_reason":
            "The 3DHP_all service publishes no per-snapshot release date; the service "
            "copyrightText states a service refresh (September 4, 2026) and per-feature "
            "featuredate values are core-database load dates; neither is a dataset release "
            "date. Quarterly service updates and annual staged products are documented by "
            "USGS.",
        "pagination": {"mode": "objectid_keyset_asc_with_bisection",
                       "page_size": PAGE_SIZE,
                       "pages": (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE,
                       "tail_witness_page": True, "resumable": True,
                       "resumed_pages": resumed_pages,
                       "bisection_note":
                           "The producer refuses uniformly sized pages over the dense roster "
                           "head with its complete HTTP 200 error document (observed "
                           "2026-11-13), so each walk position starts at maxRecordCount and "
                           "bisects the requested record count on that deterministic answer; "
                           "page boundaries are completeness-neutral and are pinned nowhere.",
                       "id_roster_enumeration_note":
                           "returnIdsOnly over the full population fails server-side "
                           "(error 500 observed 2026-11-13), so completeness uses the pinned "
                           "count, the pinned minimum/maximum OBJECTIDs, the strict ascending "
                           "OBJECTID walk, the page-count ceiling and the empty tail-page "
                           "witness instead of an enumerated roster."},
        "capture_window_utc": {"first_page": first_captured_at, "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    receipts = _load_receipt_map(capture_root)
    # The catalog reads source_manifest.json under a bounded 2 MiB layer
    # limit, so the ~3,000 per-page feature-query requests of the complete
    # population are represented by one aggregate input whose digest covers
    # their complete receipt roster; every per-page receipt is still staged
    # verbatim under raw/<name>.receipt.json next to its body and wire
    # transcript, keeping the full request-level provenance machine-readable
    # without bloating the manifest layer.
    _unknown_release_reason = ("The 3DHP_all service publishes no per-snapshot "
                               "release date; its copyrightText records a "
                               "service refresh (September 4, 2026) and USGS "
                               "documents quarterly service updates and "
                               "annual staged products.")
    _input_license = ("Public domain (U.S. Government work) per the producer service; "
                      "rights acceptance pending.")
    _input_attribution = ("USGS 3D Hydrography Program (3DHP_all), The National Map; "
                          "formal attribution terms require maintainer review.")

    def _input_entry(name: str, receipt: Mapping[str, object]) -> dict[str, object]:
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

    page_captures = [(name, receipts[name]) for name in sorted(receipts)
                     if name.startswith("page_")]
    if not page_captures:
        raise NhdWaterbodyError("paged_capture_inputs_missing")
    if {receipt["status"] for _, receipt in page_captures} != {200}:
        raise NhdWaterbodyError("paged_capture_http_status_drift")
    page_roster_sha = _sha(_canonical([receipt for _, receipt in page_captures]))
    page_entry = {"input_id": "3dhp_paged_feature_queries", "producer": PUBLISHER,
                  "host": urlsplit(page_captures[0][1]["url"]).hostname,
                  "role": "producer_publication",
                  "canonical_url": SERVICE_URL,
                  "retrieval_url": page_captures[0][1]["url"],
                  "observed_final_url": page_captures[0][1]["final_url"],
                  "retrieved_at": page_captures[0][1]["observed_at"],
                  "http_status": 200,
                  "bytes": sum(int(receipt["bytes"]) for _, receipt in page_captures),
                  "sha256": page_roster_sha,
                  "page_query_count": len(page_captures),
                  "page_capture_window": [min(receipt["observed_at"] for _, receipt in page_captures),
                                          max(receipt["observed_at"] for _, receipt in page_captures)],
                  "page_receipts_sha256_basis": "SHA-256 over the canonical JSON array of the "
                                                "per-page receipt objects in ascending page name "
                                                "order, staged verbatim as raw/<name>.receipt.json "
                                                "next to each page's raw body and wire transcript",
                  "release_date": None,
                  "release_date_unknown_reason": _unknown_release_reason,
                  "license": _input_license, "attribution": _input_attribution}
    doc_entries = [_input_entry(name, receipts[name]) for name in sorted(receipts)
                   if not name.startswith("page_")]
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Waterbody (USGS 3DHP_all)",
        "description": "USGS 3D Hydrography Program (3DHP) 3DHP_all FeatureServer layer 60 "
                       "'Waterbody', captured completely from the producer's own REST "
                       "publication as the maintained successor named by the HIFLD Open "
                       "inventory entry nhd/waterbody (River, Canal, Lake, Ocean or Great "
                       "Lake). Complete source attributes and Z-aware EPSG:4326 polygon "
                       "geometry are represented in the candidate, except any pinned "
                       "producer-refused records (source_refused_ids in the audit): their "
                       "attributes are captured verbatim through the producer's working "
                       "attributes-only channel and their candidate geometry is explicitly "
                       "gated to null because the producer itself refuses to serve it. "
                       "Producer-served zero-area (degenerate) rings are retained verbatim "
                       "(source_degenerate_ring_ids in the audit). "
                       "Succession, freshness, schema/ID contract, coverage, rights and "
                       "release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": LAYER_URL,
        "license": "Public domain (U.S. Government work); producer service states no access or "
                   "use constraints. Rights acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": doc_entries + [page_entry],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "population": "1=1 over the complete keyset-ordered "
                                                          "OBJECTID walk (full 3DHP Waterbody "
                                                          "population: River, Canal, Lake, "
                                                          "Ocean or Great Lake)",
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
                                                         "geometry retained as null; pinned "
                                                         "producer-refused records keep their "
                                                         "captured attributes with explicitly "
                                                         "gated null candidate geometry; "
                                                         "producer-served zero-area (degenerate) "
                                                         "rings are retained verbatim, joined to "
                                                         "their smallest covering exterior as "
                                                         "coverage-neutral hole rings or, when no "
                                                         "exterior of the feature covers them, "
                                                         "kept as their own zero-area shell "
                                                         "polygon (source_degenerate_ring_ids in "
                                                         "the audit)",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending succession, freshness, "
                                                                "schema/ID contract, coverage, "
                                                                "attribution and release "
                                                                "acceptance"}}],
        "lineage": "The HIFLD Open inventory entry nhd/waterbody ('Waterbody', agency US "
                   "Geological Survey, issued 2025-02-27) carries the description of the USGS "
                   "3D Hydrography Program (3DHP) 3DHP_all service verbatim. USGS retired the "
                   "NHD as a maintained product on 2023-10-01 and the DHS HIFLD Open crosswalk "
                   "(DOI 10.3886/E241367V1, consolidated row '3D Hydrography Program (3DHP) "
                   "Layers', Open ID 650c07e542fa43b6a327c7f683681712) directs the NHD-family "
                   "layers to the 3dhp.nationalmap.gov usgs_3dhp_all FeatureServer captured "
                   "here (layer 60 'Waterbody', the complete water-body population including "
                   "the legacy Lake content that the non-Lake area-succession capture of "
                   "nhd/area-large-scale-2 excludes); the same-org hydro.nationalmap.gov "
                   "3DHP_all service is captured as the producer's alias publication. The 3DHP "
                   "schema (12 fields, non-persistent base-36 id3dhp) replaces the legacy "
                   "NHDWaterbody schema wholesale, so legacy attributes (Permanent_Identifier, "
                   "ReachCode, FType/FCode, Resolution) have no successor equivalents; "
                   "EDH-sourced areas replace NHD-derived content over time, so coverage "
                   "changes are expected and unaccepted. That succession coverage and contract "
                   "change is recorded here and remains an unaccepted maintainer decision. The "
                   "DataLumos project 240398 archive copy (archive modification 2025-04-08) is "
                   "identification and comparison context only and is never fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and Z-aware "
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
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    # Stream every retained capture to storage one page at a time; the full
    # population holds millions of features, so the seal never holds more than
    # one page body/wire pair plus the small receipt and descriptor maps.
    descriptors: dict[str, dict[str, object]] = {}
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
                                        identity["files"])
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
                             audit: Mapping[str, object], current_parquet: bytes) -> dict[str, object]:
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity",
            "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise NhdWaterbodyError("cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != \
            {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise NhdWaterbodyError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
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


def capture_nhd_waterbody(storage: StagingStorageResource, *, version: str,
                          capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NhdWaterbodyError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise NhdWaterbodyError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise NhdWaterbodyError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    attempt_root = Path(capture_directory) if capture_directory \
        else Path("/data/workspace/scratch/nhd-waterbody/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory \
        else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in
                                         (capture_root, *capture_root.parents)):
        raise NhdWaterbodyError("unsafe_capture_directory")
    # The floor gates acquisition: a capture directory that already holds the
    # complete pure-local state (the retained empty tail page) performs no
    # fetches on resume, and its only write is the in-place candidate parquet
    # rewrite, so refusing it for entry-level free space would block sealed
    # reuse on a lease that already holds the staged candidate.
    retained_tail = (capture_root / "page_tail.body").exists()
    if not retained_tail and shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise NhdWaterbodyError("disk_floor_before_acquisition")
    resumed_pages = len(_load_state(capture_root).get("pages", [])) \
        if (capture_root / "capture_state.json").exists() else 0
    try:
        fetch_nhd_waterbody_pages(capture_root)
        state = _load_state(capture_root)
        captured = [page["captured_at"] for page in state["pages"]]
        extracted = _stream_validate(capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted, resumed_pages=resumed_pages,
                               first_captured_at=captured[0],
                               last_captured_at=str(state["tail"]["captured_at"]))
    except Exception as exc:
        valid_names = []
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) \
                if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            if ".error-" in name:
                continue
            try:
                _read_capture(capture_root, name)
            except (OSError, NhdWaterbodyError, ValueError):
                continue
            valid_names.append(name)
        if valid_names:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureViews(capture_root, valid_names),
                                          getattr(exc, "code", type(exc).__name__)
                                          or type(exc).__name__)
        raise


def build_nhd_waterbody_baseline(capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"],
            "source_feature_count": EXPECTED_COUNT,
            "source_max_objectid": extracted["max_objectid"],
            "source_null_geometry_count": extracted["null_geometry_count"],
            "source_refused_null_geometry_count": extracted["refused_null_count"],
            "source_refused_ids": sorted(int(key) for key in
                                         dict(_load_state(Path(capture_root).absolute())
                                              .get("refused", {}))),
            "source_degenerate_ring_count": extracted["degenerate_ring_count"],
            "source_degenerate_ring_ids": extracted["degenerate_ring_ids"],
            "source_ring_count": extracted["ring_count"],
            "source_vertex_count": extracted["vertex_count"],
            "source_featuretype_distribution": extracted["featuretype_distribution"],
            "observed_bounds": extracted["observed_bounds"],
            "id3dhp_distinct_count": extracted["id3dhp_distinct_count"]}
