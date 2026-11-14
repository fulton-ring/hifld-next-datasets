"""USACE Civil Works Divisions draft candidate.

The HIFLD inventory entry ``us-army-corps-of-engineers-usace-civil-works-divisions``
("US Army Corps of Engineers (USACE) Civil Works Divisions", agency U.S. Army Corps
of Engineers) carries HIFLD's own description: "Polygons showing USACE Civil Works
Division boundaries. This dataset was digitized from the NRCS Watershed Boundary
Dataset (WBD). Where districts follow administrative boundaries, such as County
and State lines, National Atlas and Census datasets were used. USACE District GIS
POCs also submitted data to incorporate into this dataset. This dataset has been
simplified +/- 30 feet to reduce file size and speed up drawing time. 04/16/20 -
Update to show new LRC boundary."

Producer-side publication chain (checked 2026-11-14): the DHS HIFLD Open crosswalk
(DOI 10.3886/E241367V1) answers anonymous HTTP 403, and the DataLumos project
240527 listed in the inventory is a third-party archive (research context only,
never fetched). The HIFLD Open Data portal item
``c9055810a0ed453eb8f1b3f87122d2d3`` (owner ``amber.dashnaw.ctr``, the HIFLD
librarian account) is a portal registration that points at
``https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/usace_cw_divisions/FeatureServer/0``,
i.e. it is not a separate source. That service is Esri-hosted content owned by the
producing agency: AGOL item ``4cdab8820d7a4f58aaa003be63f059ac`` ("USACE Civil
Works Divisions", owner ``usace_crrel_als``, organization
``n1YM8pTrFmm7L4hs``), the same USACE-hosted ArcGIS Online organization and
account that was verified hosting the DISDI MIRTA display channel on 2026-11-12
(issue 157). The item description matches the HIFLD inventory description
verbatim and the layer's ``editingInfo`` records a last data edit of
2026-01-28T14:38:22Z, so this is treated as the current maintained producer
publication of the divisions product (the sibling districts product is a
different layer, not captured here).

This module queries that service in bounded, OBJECTID-pinned pages against the
complete ID roster with before/after service/layer/item/count/ids brackets, keeps
every capture byte-for-byte, and builds an unapproved GeoParquet candidate that
retains all thirteen native attributes (including the producer-declared Blob
field ``SE_ANNO_CAD_DATA``, which answers the JSON empty string for every
observed row) and the producer's geographic polygon coordinates without
reprojection, datum change, repair or simplification. The producer publishes the
layer under the untagged geographic wkid 4140 (not a NAD83 4269 declaration);
coordinates are retained verbatim and the EPSG:4140 PROJJSON expansion recorded
in the GeoParquet metadata is a mapping note, not an approved datum contract.
Esri rings are interpreted natively (clockwise shells, counter-clockwise holes
assigned to their smallest containing shell) with no orientation repair.

An ArcGIS Online-hosted service can be republished at any time and can
resequence OBJECTIDs between requests; the capture therefore brackets every
document and fails closed on a mid-capture republish. Baseline digests are
pinned from a complete dated capture; a later republish surfaces as an explicit
roster, record or geometry drift refusal, and the baselines are re-pinned from a
fresh complete capture only with the republish documented. This is an unapproved
draft candidate: source release date, freshness, schema/ID contracts, coverage,
rights and release acceptance remain pending; all runtime approval flags are
false and the draft refresh schedule is stopped.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import Polygon
from time import sleep, time_ns

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "usace-civil-works-divisions"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = ("https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/"
               "services/usace_cw_divisions/FeatureServer")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "USACE Divisions"
AGENCY = "U.S. Army Corps of Engineers"
PUBLISHER = ("U.S. Army Corps of Engineers (USACE-hosted ArcGIS Online organization "
             "n1YM8pTrFmm7L4hs, publisher account usace_crrel_als)")
HOST = "services7.arcgis.com"
ITEM_ID = "4cdab8820d7a4f58aaa003be63f059ac"
ITEM_OWNER = "usace_crrel_als"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
HIFLD_PORTAL_ITEM_ID = "c9055810a0ed453eb8f1b3f87122d2d3"
CROSSWALK_DOI = "10.3886/E241367V1"
CROSSWALK_ANONYMOUS_403_NOTE = (
    "The DHS HIFLD Open crosswalk (DOI " + CROSSWALK_DOI + ") answers anonymous HTTP "
    "403 (checked 2026-11-14); the DataLumos project 240527 named in the inventory "
    "is a third-party archive kept as research context only and never fetched.")
SERVICE_DESCRIPTION_PHRASE = "Polygons showing USACE Civil Works Division boundaries."
PAGE_SIZE = 200
MAX_BODY_BYTES = 64_000_000
SOURCE_SPATIAL_REFERENCE = {"wkid": 4140, "latestWkid": 4140}
MAX_RECORD_COUNT = 1_000
OBSERVED_LAST_DATA_EDIT = "2026-01-28T14:38:22.679000+00:00"
# The per-value and per-geometry baselines, gate counts and observed bounds are
# pinned from a complete dated capture (2026-11-14). Unpinned values (None or
# "") are checked only once pinned; a later producer republish surfaces as an
# explicit roster, record or geometry drift refusal.
EXPECTED_COUNT = 8
EXPECTED_IDS_SHA256 = ("b2ddd6543011e658aaebc223b42021e89fa9a18bd1f2207f6cb68e73ca"
                       "5688b3")
EXPECTED_SCHEMA_SHA256 = ("d068c175b5fe1d48ed3d05549c313655c38a68a04f7d587fa2ce651d2"
                          "1d7a4f8")
EXPECTED_RECORD_SHA256 = ("76fb71879f960856fe609c9f206746215ef0cbe30cc020250e30e615b"
                          "5c6808f")
EXPECTED_GEOMETRY_SHA256 = ("4e5400aaffd04f623be76d54ade5eb7e9c47b7be290f2386d7bfdc4c"
                            "3b8b993b")
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_SHELL_COUNT_DISTRIBUTION = {1: 4, 2: 1, 7: 2, 94: 1}
EXPECTED_HOLE_COUNT = 119
EXPECTED_VERTEX_COUNT = 404_216
EXPECTED_OBSERVED_BOUNDS = [-179.231070639888, -14.6018199641378,
                            179.775108112044, 71.439786190856]
BASELINE_PENDING_NOTE = (
    "The record and geometry baseline digests, gate counts and observed bounds "
    "are pinned from a completed capture; unpinned values are checked only once "
    "pinned. The producer republishes this hosted service and can resequence all "
    "OBJECTIDs between requests; a later republish surfaces as an explicit drift "
    "refusal.")
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", False),
    ("DIVISION", "esriFieldTypeString", True),
    ("DIV_SYM", "esriFieldTypeString", True),
    ("EROC", "esriFieldTypeString", True),
    ("HQ_EROC", "esriFieldTypeString", True),
    ("REPORTS_TO", "esriFieldTypeString", True),
    ("USACE_DIVISION_CODE", "esriFieldTypeString", True),
    ("INPOLY_FID", "esriFieldTypeInteger", True),
    ("MAXSIMPTOL", "esriFieldTypeDouble", True),
    ("MINSIMPTOL", "esriFieldTypeDouble", True),
    ("SE_ANNO_CAD_DATA", "esriFieldTypeBlob", True),
    ("Shape__Area", "esriFieldTypeDouble", True),
    ("Shape__Length", "esriFieldTypeDouble", True),
]
STRING_FIELDS = ["DIVISION", "DIV_SYM", "EROC", "HQ_EROC", "REPORTS_TO",
                 "USACE_DIVISION_CODE"]
INTEGER_FIELDS = ["INPOLY_FID"]
DOUBLE_FIELDS = ["MAXSIMPTOL", "MINSIMPTOL", "Shape__Area", "Shape__Length"]
BLOB_FIELDS = ["SE_ANNO_CAD_DATA"]
# The producer-declared Blob field answered the JSON empty string for every
# observed row; the pinned JSON value is retained verbatim as a string column
# and no binary or base64 interpretation is applied.
EXPECTED_BLOB_JSON_VALUE = ""
FIELD_NAMES = [field[0] for field in EXPECTED_FIELDS]
ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeSmallInteger": pa.int32(), "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(), "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"),
    "esriFieldTypeDouble": pa.float64(), "esriFieldTypeSingle": pa.float32(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(**params: object) -> str:
    return LAYER_URL + "/query?" + urlencode({"f": "json", **params})


def _input_fingerprint(captures: Mapping[str, tuple[bytes, Mapping[str, object]]],
                       identity: Mapping[str, object]) -> str:
    """Hash exact source body/wire evidence and code identity; timestamps are excluded."""
    return _sha(_canonical({"bodies": {name: {"bytes": len(body), "sha256": _sha(body),
                                              "wire_bytes": receipt.get("wire_bytes"),
                                              "wire_sha256": receipt.get("wire_sha256")}
                            for name, (body, receipt) in sorted(captures.items())},
                            "processing_identity": identity}))


def _validate_source_spatial_reference(value: object, label: str) -> None:
    """Accept only the producer's untagged geographic wkid 4140 declaration."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_spatial_reference_missing")
    identifiers = [value.get(name) for name in ("wkid", "latestWkid")
                   if value.get(name) is not None]
    if not identifiers or any(type(code) is not int or code != 4140 for code in identifiers):
        raise ValueError(f"{label}_spatial_reference_drift")
    for name in ("wkt", "wkt2"):
        wkt = value.get(name)
        if wkt:
            try:
                if not pyproj.CRS.from_user_input(wkt).equals(pyproj.CRS.from_epsg(4140)):
                    raise ValueError(f"{label}_spatial_reference_drift")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"{label}_spatial_reference_invalid") from exc


def _capture_names(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("item_before", ITEM_URL),
        ("count_before", _query(where="1=1", returnCountOnly="true")),
        ("ids_before", _query(where="1=1", returnIdsOnly="true")),
    ]
    # Pages walk the sorted roster in OBJECTID order with
    # resultOffset/resultRecordCount pagination below the producer's
    # maxRecordCount cap so every page is bounded and independently verifiable.
    for page in range(0, (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE):
        urls.append((f"page_{page:03d}", _query(
            where="1=1", outFields="*", returnGeometry="true",
            orderByFields="OBJECTID ASC",
            resultOffset=page * PAGE_SIZE, resultRecordCount=PAGE_SIZE)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
        ("item_after", ITEM_URL),
        ("count_after", _query(where="1=1", returnCountOnly="true")),
        ("ids_after", _query(where="1=1", returnIdsOnly="true")),
    ])
    return urls


_BRACKET_RETRY_ATTEMPTS = 4
_BRACKET_RETRY_BASE_DELAY_SECONDS = 5.0


def _witness_refused(root: Path, name: str) -> None:
    """Move a retained refused answer aside so the slot can be refetched."""
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = root / f"{name}.{suffix}"
        if source.exists():
            source.replace(root / f"{name}.error-{stamp}.{suffix}")


def _document_complete(name: str, body: bytes) -> bool:
    """Distinguish a well-formed capture document from a transient answer."""
    try:
        document = json.loads(body)
    except ValueError:
        return False
    if not isinstance(document, dict) or "error" in document:
        return False
    if name.startswith("ids_"):
        return isinstance(document.get("objectIds"), list) and bool(document.get("objectIds"))
    if name.startswith("count_"):
        return isinstance(document.get("count"), int)
    return bool(document)


def _acquire_document(client: httpx.Client, root: Path, name: str, url: str) -> \
        tuple[bytes, dict[str, object]]:
    """Fetch one bracket or page document, refetching transient null/error answers."""
    body: bytes = b""
    receipt: dict[str, object] = {}
    for attempt in range(_BRACKET_RETRY_ATTEMPTS):
        body, receipt = capture_get(client, url, root, name, max_bytes=MAX_BODY_BYTES)
        if _document_complete(name, body):
            return body, receipt
        _witness_refused(root, name)
        if attempt + 1 < _BRACKET_RETRY_ATTEMPTS:
            sleep(_BRACKET_RETRY_BASE_DELAY_SECONDS * attempt)
    return body, receipt


def _load_offline(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    summary = json.loads((root / "capture-summary.json").read_bytes())
    if summary.get("complete") is not True:
        raise ValueError("offline_native_capture_incomplete")
    captures = {}
    for receipt_path in sorted(root.glob("*.receipt.json")):
        receipt = json.loads(receipt_path.read_bytes())
        name = receipt_path.name.removesuffix(".receipt.json")
        body = (root / f"{name}.body").read_bytes()
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
            raise ValueError("offline_capture_body_receipt_mismatch:" + name)
        receipt.update({"complete_body": True, "wire_bytes": receipt.get("wire_bytes", len(body)),
                        "wire_sha256": receipt.get("wire_sha256", _sha(body)),
                        "content_encoding": receipt.get("content_encoding", "identity"),
                        "error_code": receipt.get("error_code")})
        captures[name] = (body, receipt)
    return captures


def _acquire(root: Path, storage: StagingStorageResource) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=120,
                          headers={"User-Agent":
                                   "hifld-next-datasets/usace-civil-works-divisions-draft"}) as client:
            seed, receipt = _acquire_document(client, root, "ids_before",
                                              _query(where="1=1", returnIdsOnly="true"))
            captures["ids_before"] = (seed, receipt)
            ids = json.loads(seed).get("objectIds")
            if not isinstance(ids, list) or not ids or any(type(i) is not int for i in ids) \
                    or len(ids) != len(set(ids)):
                raise ValueError("source_seed_id_roster_invalid")
            if EXPECTED_COUNT is not None and len(ids) != EXPECTED_COUNT:
                raise ValueError("source_count_drift")
            urls = [(n, u) for n, u in _capture_names(sorted(ids)) if n != "ids_before"]
            for name, url in urls:
                body, receipt = _acquire_document(client, root, name, url)
                captures[name] = (body, receipt)
        return captures
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, "incomplete",
                                          captures, type(exc).__name__)
        raise


def _profile_fields(layer: Mapping[str, object]) -> list[dict[str, object]]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise ValueError("source_field_roster_missing")
    simple = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain")
               if k in f} for f in fields]
    if [(f.get("name"), f.get("type"), f.get("nullable")) for f in simple] != EXPECTED_FIELDS:
        raise ValueError("source_field_schema_drift")
    for field in simple:
        if field["type"] not in ARROW_TYPES and field["type"] != "esriFieldTypeBlob":
            raise ValueError("unsupported_source_physical_type:" + str(field["type"]))
    return simple


def _ring_signed_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0]
    return math.fsum((ring[i][0] - x0) * (ring[i + 1][1] - y0)
                     - (ring[i + 1][0] - x0) * (ring[i][1] - y0)
                     for i in range(len(ring) - 1)) / 2


def _validate_ring_points(ring: object) -> list[list[float]]:
    if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
        raise ValueError("source_polygon_ring_invalid")
    for point in ring:
        if not isinstance(point, list) or len(point) != 2:
            raise ValueError("source_polygon_ring_invalid")
        if any(type(v) not in (int, float) or v != v
               or v in (float("inf"), float("-inf")) for v in point):
            raise ValueError("source_polygon_vertex_invalid")
        if not (-180 <= point[0] <= 180 and -90 <= point[1] <= 90):
            raise ValueError("source_polygon_coordinates_invalid")
    return ring


def _ring_geometry(rings: object) -> tuple[object, int, int, int] | None:
    """Build one feature geometry from native Esri rings, or None when empty.

    Rings are interpreted natively (Esri convention: clockwise outer shells,
    counter-clockwise holes) with no orientation repair; every hole must lie
    inside exactly one shell and is assigned to the smallest containing shell.
    Returns (shapely geometry, shell count, hole count, vertex count) or None
    for the producer's empty/absent rings member.
    """
    if rings is None:
        return None
    if not isinstance(rings, list):
        raise ValueError("source_polygon_geometry_invalid")
    if not rings:
        return None
    shells: list[tuple[list[list[float]], Polygon]] = []
    holes: list[tuple[list[list[float]], Polygon]] = []
    for ring in rings:
        points = _validate_ring_points(ring)
        polygon = Polygon(points)
        if not polygon.is_valid or polygon.area == 0:
            raise ValueError("source_polygon_ring_invalid_geometry")
        area = _ring_signed_area(points)
        if area < 0:
            shells.append((points, polygon))
        elif area > 0:
            holes.append((points, polygon))
        else:
            raise ValueError("source_polygon_ring_invalid_geometry")
    assigned: dict[int, list[list[list[float]]]] = {index: [] for index in range(len(shells))}
    for ring, polygon in holes:
        parents = [(shell.area, index) for index, (_points, shell) in enumerate(shells)
                   if shell.contains(polygon)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        assigned[min(parents)[1]].append(ring)
    parts = [Polygon(shell, assigned[index]) for index, (shell, _points) in enumerate(shells)]
    if any(not part.is_valid for part in parts):
        raise ValueError("source_polygon_invalid_no_repair_permitted")
    if len(parts) == 1:
        geometry = parts[0]
    else:
        geometry = shapely.geometry.MultiPolygon(parts)
    vertices = sum(len(ring) for ring in rings)
    return geometry, len(shells), len(holes), vertices


def _geometry_rings(geom: object) -> list[list[list[float]]] | None:
    """Project one feature geometry to validated rings, or None for null geometry.

    ArcGIS publishes a null polygon geometry either as ``{"rings": []}`` or as a
    geometry without a ``rings`` member; both are gated to null and counted. No
    repair, reorientation or coordinate change is applied to rings that are
    present.
    """
    if geom is None:
        return None
    if not isinstance(geom, dict) or set(geom) - {"rings", "spatialReference"}:
        raise ValueError("source_polygon_geometry_invalid")
    rings = geom.get("rings")
    if rings is None or rings == []:
        return None
    _validate_ring_points_all(rings)
    return rings


def _validate_ring_points_all(rings: list) -> None:
    for ring in rings:
        _validate_ring_points(ring)


def _validate(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) \
        -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or not ids_before or sorted(ids_before) != sorted(ids_after or [])
            or len(ids_before) != len(set(ids_before))):
        raise ValueError("source_id_population_changed_during_capture")
    count = len(ids_before)
    if EXPECTED_COUNT is not None and count != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if EXPECTED_IDS_SHA256 and _sha(_canonical(sorted(ids_before))) != EXPECTED_IDS_SHA256:
        raise ValueError("source_id_roster_drift")
    if docs["count_before"].get("count") != count or docs["count_after"].get("count") != count:
        raise ValueError("source_count_bracket_mismatch")
    service, layer, item = docs["service_before"], docs["layer_before"], docs["item_before"]
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("source_layer_identity_or_geometry_drift")
    if not any(item_entry.get("id") == LAYER_ID and item_entry.get("name") == LAYER_NAME
               for item_entry in service.get("layers", []) if isinstance(item_entry, dict)):
        raise ValueError("source_layer_not_in_service_roster")
    if SERVICE_DESCRIPTION_PHRASE not in (service.get("serviceDescription") or ""):
        raise ValueError("producer_service_description_drift")
    if (layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False
            or layer.get("hasM") is not False):
        raise ValueError("source_spatial_contract_drift")
    if layer.get("maxRecordCount") != MAX_RECORD_COUNT or service.get("maxRecordCount") != MAX_RECORD_COUNT:
        raise ValueError("source_transfer_limit_drift")
    if not isinstance(layer.get("advancedQueryCapabilities"), dict) \
            or layer["advancedQueryCapabilities"].get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if (item.get("id") != ITEM_ID or item.get("owner") != ITEM_OWNER
            or item.get("type") != "Feature Service" or item.get("url") != SERVICE_URL
            or item.get("access") != "public"):
        raise ValueError("source_publisher_item_identity_drift")
    _validate_source_spatial_reference(layer.get("sourceSpatialReference")
                                       if layer.get("sourceSpatialReference") is not None
                                       else service.get("spatialReference"), "source_service")
    _validate_source_spatial_reference(layer.get("spatialReference"), "source_layer")
    _validate_source_spatial_reference(layer.get("extent", {}).get("spatialReference"),
                                       "source_layer_extent")
    fields = _profile_fields(layer)
    if fields != _profile_fields(docs["layer_after"]):
        raise ValueError("source_schema_changed_during_capture")
    if EXPECTED_SCHEMA_SHA256 and _sha(_canonical(fields)) != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_baseline_drift")
    if (docs["service_before"] != docs["service_after"] or layer != docs["layer_after"]
            or item != docs["item_after"]):
        raise ValueError("source_metadata_changed_during_capture")
    pages = sorted(name for name in docs if name.startswith("page_"))
    if len(pages) != (count + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for index, name in enumerate(pages):
        if int(name.split("_")[1]) != index:
            raise ValueError("source_page_roster_incomplete")
        page = docs[name]
        rows = page.get("features")
        expected_ids = sorted(ids_before)[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE \
                or (page.get("exceededTransferLimit") is True and len(rows) != PAGE_SIZE):
            raise ValueError("source_page_error_or_truncation")
        _validate_source_spatial_reference(page.get("spatialReference"), "source_page")
        page_ids = [f.get("attributes", {}).get("OBJECTID") for f in rows
                    if isinstance(f, dict) and isinstance(f.get("attributes"), dict)]
        if page_ids != expected_ids:
            raise ValueError("source_page_id_slice_drift:" + name)
        features.extend(rows)
    feature_ids = [f.get("attributes", {}).get("OBJECTID") for f in features
                   if isinstance(f, dict) and isinstance(f.get("attributes"), dict)]
    if (len(features) != count or len(feature_ids) != count
            or any(type(value) is not int for value in feature_ids)
            or sorted(feature_ids) != sorted(ids_before) or len(set(feature_ids)) != count):
        raise ValueError("source_feature_population_incomplete")
    if feature_ids != sorted(feature_ids):
        raise ValueError("source_page_order_drift")
    null_geometry = 0
    shell_counts: dict[int, int] = {}
    hole_count = 0
    vertex_count = 0
    geometry_parts: list[list[list[list[float]]] | None] = []
    built: list[object] = []
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(FIELD_NAMES):
            raise ValueError("source_feature_field_roster_drift")
        if geom is not None and not isinstance(geom, dict):
            raise ValueError("source_polygon_geometry_invalid")
        if feature.get("spatialReference") is not None:
            _validate_source_spatial_reference(feature["spatialReference"], "source_feature")
        if isinstance(geom, dict) and geom.get("spatialReference") is not None:
            _validate_source_spatial_reference(geom["spatialReference"], "source_geometry")
        rings = _geometry_rings(geom)
        if rings is None:
            null_geometry += 1
            geometry_parts.append(None)
            built.append(None)
        else:
            geometry, shells, holes, vertices = _ring_geometry(rings)
            shell_counts[shells] = shell_counts.get(shells, 0) + 1
            hole_count += holes
            vertex_count += vertices
            geometry_parts.append(rings)
            built.append(geometry)
        if type(attrs["OBJECTID"]) is not int:
            raise ValueError("source_native_identifier_invalid")
        for name in STRING_FIELDS:
            value = attrs[name]
            if value is not None and (not isinstance(value, str)
                                      or len(value.encode("utf-16-le")) // 2 > _field_width(name)):
                raise ValueError("source_string_field_type_or_width_drift:" + name)
        for name in INTEGER_FIELDS:
            value = attrs[name]
            if value is not None and type(value) is not int:
                raise ValueError("source_integer_field_type_drift:" + name)
        for name in DOUBLE_FIELDS:
            value = attrs[name]
            if value is not None and (type(value) not in (int, float) or value != value
                                      or value in (float("inf"), float("-inf"))):
                raise ValueError("source_double_field_type_drift:" + name)
        for name in BLOB_FIELDS:
            if attrs[name] != EXPECTED_BLOB_JSON_VALUE:
                raise ValueError("source_blob_field_json_value_drift:" + name)
    if EXPECTED_NULL_GEOMETRY_COUNT is not None and null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if (EXPECTED_SHELL_COUNT_DISTRIBUTION is not None
            and dict(sorted(shell_counts.items())) != EXPECTED_SHELL_COUNT_DISTRIBUTION):
        raise ValueError("source_shell_count_distribution_drift")
    if EXPECTED_HOLE_COUNT is not None and hole_count != EXPECTED_HOLE_COUNT:
        raise ValueError("source_hole_count_drift")
    if EXPECTED_VERTEX_COUNT is not None and vertex_count != EXPECTED_VERTEX_COUNT:
        raise ValueError("source_vertex_count_drift")
    flat = [point for rings in geometry_parts if rings is not None
            for ring in rings for point in ring]
    bounds = None if not flat else [min(p[0] for p in flat), min(p[1] for p in flat),
                                    max(p[0] for p in flat), max(p[1] for p in flat)]
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_bounds_drift")
    record_sha = _sha(_canonical(features))
    if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_value_or_geometry_baseline_drift")
    geometry_sha = _sha(_canonical([[f["attributes"]["OBJECTID"], rings]
                                    for f, rings in zip(features, geometry_parts)]))
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    nulls = {name: sum(1 for f in features if f["attributes"][name] is None)
             for name in FIELD_NAMES}
    invalid_geometry_ids = [f["attributes"]["OBJECTID"] for f, geometry in zip(features, built)
                            if geometry is not None and not geometry.is_valid]
    audit = {"feature_count": count, "unique_objectid_count": len(set(feature_ids)),
             "null_counts_by_field": nulls,
             "null_geometry_count": null_geometry,
             "shell_count_distribution": dict(sorted(shell_counts.items())),
             "hole_count": hole_count,
             "vertex_count": vertex_count,
             "geometry_bounds": bounds,
             "invalid_geometry_ids": invalid_geometry_ids,
             "source_id_sha256": _sha(_canonical(sorted(ids_before))),
             "source_record_sha256": record_sha,
             "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": _sha(_canonical(fields)),
             "source_crs_wkid": 4140,
             "native_geometry_type": "Polygon",
             "native_dimension": 2,
             "source_release_date": None,
             "observed_service_last_data_edit": OBSERVED_LAST_DATA_EDIT,
             "service_current_version": service.get("currentVersion"),
             "source_fields": fields,
             "baseline_pending_note": BASELINE_PENDING_NOTE,
             "crosswalk_doi": CROSSWALK_DOI,
             "crosswalk_anonymous_403_note": CROSSWALK_ANONYMOUS_403_NOTE,
             "hifld_portal_item_id": HIFLD_PORTAL_ITEM_ID,
             "release_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "freshness_accepted": False,
             "rights_accepted": False, "source_authority_accepted": False,
             "mapping_approved": False}
    return layer, fields, features, audit


# Producer-declared maximum character widths for the string fields (UTF-16 code units).
_STRING_WIDTHS = [("DIVISION", 40), ("DIV_SYM", 3), ("EROC", 255), ("HQ_EROC", 255),
                  ("REPORTS_TO", 255), ("USACE_DIVISION_CODE", 255)]


def _field_width(name: str) -> int:
    return dict(_STRING_WIDTHS)[name]


def _rings_wkb(rings: list[list[list[float]]]) -> bytes:
    """Store validated Esri rings verbatim as Polygon/MultiPolygon WKB."""
    built = _ring_geometry(rings)
    if built is None:
        raise ValueError("source_polygon_geometry_invalid")
    geometry = built[0]
    return geometry.wkb


def _geoparquet(features: list[dict[str, object]], fields: list[dict[str, object]],
                root: Path) -> bytes:
    if [field["name"] for field in fields] != FIELD_NAMES:
        raise ValueError("source_field_order_drift")
    kinds: dict[str, str] = {}
    for name in STRING_FIELDS:
        kinds[name] = "string"
    for name in INTEGER_FIELDS:
        kinds[name] = "integer"
    for name in DOUBLE_FIELDS:
        kinds[name] = "double"
    for name in BLOB_FIELDS:
        kinds[name] = "blob"
    arrow_field = {"string": pa.string(), "integer": pa.int64(), "double": pa.float64(),
                   "blob": pa.string()}
    schema = pa.schema([
        pa.field("OBJECTID", pa.int64(), nullable=False),
        *[pa.field(name, arrow_field[kinds[name]], nullable=True)
          for name in FIELD_NAMES if name != "OBJECTID"],
        pa.field("geometry", pa.binary(), nullable=True),
    ])
    rings_parts = [_geometry_rings(f.get("geometry")) for f in features]
    columns = {
        "OBJECTID": pa.array([f["attributes"]["OBJECTID"] for f in features], type=pa.int64()),
    }
    for name in FIELD_NAMES:
        if name == "OBJECTID":
            continue
        kind = kinds[name]
        if kind in ("string", "blob"):
            columns[name] = pa.array([f["attributes"][name] for f in features],
                                     type=pa.string())
        elif kind == "integer":
            columns[name] = pa.array([None if f["attributes"][name] is None
                                      else int(f["attributes"][name]) for f in features],
                                     type=pa.int64())
        else:
            columns[name] = pa.array([None if f["attributes"][name] is None
                                      else float(f["attributes"][name]) for f in features],
                                     type=pa.float64())
    columns["geometry"] = pa.array([None if rings is None else _rings_wkb(rings)
                                    for rings in rings_parts], type=pa.binary())
    built = [_ring_geometry(rings)[0] if rings is not None else None for rings in rings_parts]
    present = [geometry for geometry in built if geometry is not None]
    bounds = [min(geometry.bounds[i] for geometry in present) for i in (0, 1)] \
        + [max(geometry.bounds[i] for geometry in present) for i in (2, 3)]
    column_metadata = {"encoding": "WKB",
                       "geometry_types": sorted(set(g.geom_type for g in present)),
                       "crs": pyproj.CRS.from_epsg(4140).to_json_dict(),
                       "edges": "planar"}
    if present:
        column_metadata["bbox"] = bounds
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": column_metadata}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(
        columns, schema=schema.with_metadata({b"geo": _canonical(geo)})), sink,
        compression="zstd")
    body = sink.getvalue().to_pybytes()
    (root / "candidate-verification.json").write_bytes(_canonical({
        "rows": len(features),
        "objectid_first": features[0]["attributes"]["OBJECTID"],
        "objectid_last": features[-1]["attributes"]["OBJECTID"],
        "readback_columns": schema.names}) + b"\n")
    return body


def capture_usace_civil_works_divisions(storage: StagingStorageResource, *,
                                        version: str = "draft",
                                        capture_directory: str | None = None,
                                        offline_capture_directory: str | None = None) -> dict[str, object]:
    """Capture an unapproved USACE Civil Works Divisions candidate; offline replay is supported."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 1_000_000_000:
        raise ValueError("unsafe_or_low_space_staging_root")
    capture_parent = Path(capture_directory).absolute() if capture_directory \
        else Path(tempfile.gettempdir()) / "usace-civil-works-divisions-captures"
    capture_root = capture_parent / f"{version}-{uuid4().hex}"
    if ".." in capture_root.parts or any(p.is_symlink() for p in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    if offline_capture_directory:
        captures = _load_offline(Path(offline_capture_directory))
        for name, (body, _receipt) in captures.items():
            (capture_root / f"{name}.wire").write_bytes(body)
    else:
        captures = _acquire(capture_root, storage)
        (capture_root / "capture-summary.json").write_bytes(_canonical(
            {"complete": True}) + b"\n")
    try:
        layer, fields, features, audit = _validate(captures)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ValueError) and str(exc) else type(exc).__name__
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, "invalid",
                                      captures, reason)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    fingerprint = _input_fingerprint(captures, identity)
    version = "v" + fingerprint[:32]
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version,
                                                    _sha(_canonical(identity))))
        payload = bundle.get("payload", {})
        receipts = payload.get("capture_receipts", {})
        input_key = f"{prefix}/metadata/source_inputs.json"
        if (payload.get("processing_identity") != identity or "objects" not in payload
                or storage.read_key(input_key) != _canonical({"sha256": fingerprint}) + b"\n"):
            raise ValueError("cached_first_candidate_identity_changed")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                _sha(_canonical(identity)), identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["feature_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "null_geometry_count": audit["null_geometry_count"],
                "source_id_sha256": audit["source_id_sha256"],
                "reused": True, "source_release_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "freshness_accepted": False, "rights_accepted": False,
                "source_authority_accepted": False, "mapping_approved": False,
                "release_accepted": False}

    parquet_body = _geoparquet(features, fields, capture_root)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    item_metadata = json.loads(captures["item_before"][0])
    inputs = [{"input_id": n, "producer": PUBLISHER, "canonical_url": SERVICE_URL,
               "host": urlsplit(receipt["url"]).hostname,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt.get("wire_bytes"), "wire_sha256": receipt.get("wire_sha256"),
               "license": item_metadata.get("licenseInfo")
                          or "Unknown; independent rights decision required",
               "attribution": "U.S. Army Corps of Engineers (publisher attribution claim "
                              "unaccepted)",
               "role": "producer_publication",
               "release_date": None,
               "release_date_unknown_reason": "The service publishes no dated source release "
                                              "for the divisions layer; the AGOL item reports "
                                              "a last data edit of 2026-01-28T14:38:22Z "
                                              "(an editing timestamp, not a release date) and "
                                              "the producer description records a 04/16/20 "
                                              "boundary update."}
              for n, (_, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False,
                "title": "US Army Corps of Engineers (USACE) Civil Works Divisions",
                "description": "Polygons showing USACE Civil Works Division boundaries, as "
                               "digitized by USACE from the NRCS Watershed Boundary Dataset "
                               "(WBD), National Atlas and Census boundaries and USACE "
                               "District GIS POC submissions, simplified +/- 30 feet, with "
                               "the 04/16/20 update for the new LRC boundary. Unapproved "
                               "draft candidate; not for official use.",
                "agency": AGENCY, "publisher": PUBLISHER,
                "source_host_first_party": False,
                "source_authority_note": (
                    "services7.arcgis.com is Esri's ArcGIS Online hosting, not USACE-owned "
                    "infrastructure; the producing-agency chain is the AGOL item "
                    "4cdab8820d7a4f58aaa003be63f059ac owned by USACE account usace_crrel_als "
                    "in organization n1YM8pTrFmm7L4hs - the same USACE-hosted organization "
                    "and account verified hosting the DISDI MIRTA display channel on "
                    "2026-11-12 (issue 157) - whose item description matches the HIFLD "
                    "inventory description verbatim, and which the HIFLD Open Data portal "
                    "item " + HIFLD_PORTAL_ITEM_ID + " (HIFLD librarian account "
                    "amber.dashnaw.ctr) registers as its source service. Source selection, "
                    "authority, freshness and rights acceptance remain false pending "
                    "maintainer approval."),
                "source_authority_accepted": False,
                "source_url": LAYER_URL, "source_release_date": None,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"],
                "inputs": inputs,
                "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                                     "parameters": {"layer_id": LAYER_ID,
                                                    "source_fields": "all thirteen native "
                                                                     "fields in producer order; "
                                                                     "the producer-declared Blob "
                                                                     "field SE_ANNO_CAD_DATA "
                                                                     "answers the JSON empty "
                                                                     "string for every observed "
                                                                     "row and is preserved "
                                                                     "verbatim; no binary "
                                                                     "interpretation",
                                                    "geometry": "Esri JSON rings read natively "
                                                                "(clockwise shells, "
                                                                "counter-clockwise holes assigned "
                                                                "to the smallest containing "
                                                                "shell) into Polygon/"
                                                                "MultiPolygon WKB; the producer's "
                                                                "untagged geographic wkid 4140 "
                                                                "coordinates retained verbatim "
                                                                "without reprojection, datum "
                                                                "change, densification or "
                                                                "simplification; no orientation "
                                                                "repair; empty rings gated to "
                                                                "null geometry and counted",
                                                    "release_status": "unapproved draft"}}],
                "lineage": ("USACE Civil Works Divisions served by the USACE-hosted ArcGIS "
                            "Online organization n1YM8pTrFmm7L4hs (account usace_crrel_als), "
                            "AGOL item " + ITEM_ID + ". The HIFLD Open Data portal item "
                            + HIFLD_PORTAL_ITEM_ID + " registers the same service URL, so "
                            "the HIFLD portal entry is a portal registration of this "
                            "producer service, not a separate source. " +
                            CROSSWALK_ANONYMOUS_403_NOTE + " The producer description "
                            "records digitization from the NRCS Watershed Boundary Dataset, "
                            "National Atlas/Census administrative boundaries and USACE "
                            "District GIS POC submissions, with a 04/16/20 update for the "
                            "new LRC boundary. The layer's editingInfo reports a last data "
                            "edit of 2026-01-28T14:38:22Z (observed 2026-11-14). An "
                            "ArcGIS Online-hosted service can be republished at any time "
                            "and can resequence OBJECTIDs between requests; the capture "
                            "brackets every document and fails closed on a mid-capture "
                            "republish, and baseline digests are re-pinned only from a "
                            "fresh complete capture with the republish documented."),
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "Source attributes and the producer's untagged geographic "
                              "polygon coordinates retained for an unapproved candidate.",
                    "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
                "unsupported_output_formats": {
                    "shapefile_zip": "No approved published schema mapping; release remains blocked.",
                    "geopackage": "No approved published schema mapping; release remains blocked.",
                    "pmtiles": "No approved display transformation or generalized geometry."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_service_url": SERVICE_URL, "source_layer_id": LAYER_ID,
                  "source_layer_name": LAYER_NAME, "source_inputs_sha256": fingerprint,
                  "source_item_id": ITEM_ID, "source_item_owner": ITEM_OWNER,
                  "source_crs_note": ("The producer publishes the untagged geographic wkid "
                                      "4140 (not a NAD83 EPSG:4269 declaration); coordinates "
                                      "are retained verbatim and the EPSG:4140 PROJJSON "
                                      "expansion in the GeoParquet metadata is a mapping "
                                      "note, not an approved datum contract."),
                  "source_release_accepted": False, "id_contract_accepted": False,
                  "freshness_accepted": False, "rights_accepted": False,
                  "source_authority_accepted": False, "schema_contract_accepted": False,
                  "release_accepted": False})
    outputs = {parquet_name: parquet_body,
               "metadata/source_inputs.json": _canonical({"sha256": fingerprint}) + b"\n",
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes() \
            if (capture_root / f"{name}.wire").exists() else body
        outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt)) + b"\n"
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False,
             "source_authority_accepted": False, "mapping_approved": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    objects = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or descriptor != {"bytes": snap.size, "sha256": storage.sha256_key(key)} \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_physical_readback_failed:" + name)
    payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {n: r for n, (_, r) in sorted(captures.items())},
               "objects": objects}
    seal_ready_candidate(storage, state_key, version, _sha(_canonical(identity)), payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "null_geometry_count": audit["null_geometry_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "reused": False,
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False,
            "rights_accepted": False, "source_authority_accepted": False,
            "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}


def build_usace_civil_works_divisions_baseline(capture_directory: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    capture_root = Path(capture_directory)
    summary = json.loads((capture_root / "capture-summary.json").read_bytes())
    if summary.get("complete") is not True:
        raise ValueError("offline_native_capture_incomplete")
    captures = _load_offline(capture_root)
    _layer, _fields, _features, audit = _validate(captures)
    return {"source_feature_count": audit["feature_count"],
            "source_ids_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_shell_count_distribution": audit["shell_count_distribution"],
            "source_hole_count": audit["hole_count"],
            "source_vertex_count": audit["vertex_count"],
            "observed_bounds": audit["geometry_bounds"]}
