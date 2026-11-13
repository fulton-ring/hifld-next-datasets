"""BOEM/BSEE national MMC cadastre OCS lease-block candidate from gis.boem.gov.

The HIFLD inventory entry ``outer-continental-shelf-ocs-lease-blocks-1``
("Outer Continental Shelf (OCS) Lease Blocks", agency Bureau of Ocean Energy
Management) describes the BOEM cadastre lease-block grid. The DHS HIFLD Open
crosswalk (DOI 10.3886/E241367V1) row for this layer (Status Deprecate, DUA/GII
not required, Agency Bureau of Ocean Energy Management, Open ID
``b7ca0052df4147f3b60bfd1894354b34``) directs the layer to the GeoPlatform
GeoSeas hosted ``BOEM_Offshore_Marine_Cadastre_Data_Collection`` FeatureServer
(layer 3), whose host no longer resolves; the same cadastre product is served
first-party on BOEM's own ArcGIS Server (``gis.boem.gov``), whose "BOEM OCS
Lease Blocks" layer (id 11) carries exactly this product in the producer's
EPSG:4269 publication. That layer is queried here in bounded, OBJECTID-pinned
``resultOffset`` pages (the server drops very long ``objectIds`` lists) against
the complete ID roster with before/after count/ids/layer/service brackets.
Source JSON is retained byte-for-byte; a GeoParquet candidate keeps all six
native attributes (including space-padded BLOCK_NUMBER values) and the
producer's EPSG:4269 polygon coordinates without reprojection, orientation
repair or simplification. Features the producer publishes with an empty
``rings`` array are retained as null-geometry rows and counted instead of
being dropped or repaired. This is an unapproved draft candidate: source
release date, schema/ID contracts, rights and release acceptance remain
pending.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
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

DATASET_SLUG = "outer-continental-shelf-ocs-lease-blocks-1"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://gis.boem.gov/server/rest/services/BOEM_BSEE/MMC_Layers/FeatureServer"
LAYER_ID = 11
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "BOEM OCS Lease Blocks"
AGENCY = "Bureau of Ocean Energy Management"
PUBLISHER = ("Bureau of Ocean Energy Management, Office of Strategic Resources, "
             "Geospatial Services Division")
HOST = "gis.boem.gov"
PAGE_SIZE = 10_000
MAX_BODY_BYTES = 64_000_000
CROSSWALK_OPEN_ID = "b7ca0052df4147f3b60bfd1894354b34"
CROSSWALK_COPY_SHA256 = ("7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38"
                         "772bc")
CROSSWALK_DEAD_REDIRECT_NOTE = (
    "The crosswalk row's Open REST Service (geoseas.geoplatform.gov/hosting/rest/"
    "services/Hosted/BOEM_Offshore_Marine_Cadastre_Data_Collection/FeatureServer/3) "
    "no longer resolves; the same cadastre product is served first-party on BOEM's "
    "own gis.boem.gov server (layer 11).")
# The per-value and per-geometry baselines, gate counts and observed bounds
# are pinned from the dated complete capture (2026-11-13, 28 accepted
# resultOffset pages, /workspace/captures258 in the task sandbox); unpinned
# values (None or "") are checked only once pinned.
EXPECTED_COUNT = 278_438
EXPECTED_IDS_SHA256 = "dc891e1182e69533063605d67764153a2dcb410d4fe0fcb9166e31d830e3cab5"
EXPECTED_SCHEMA_SHA256 = "9294be8bdc15558774532cf2c997830b3ef153aefd8cd4c3efa88c4de92ebe05"
EXPECTED_RECORD_SHA256 = "adafad8e40022e9067950767ed6c9a89bfd33ae4c7f804890af92a036546ef35"
EXPECTED_GEOMETRY_SHA256 = "221967344317097c4cf5f53610e2ce88e1095e72f68c1fd6a25830819a950eca"
EXPECTED_NULL_GEOMETRY_COUNT = 45
EXPECTED_RING_COUNT_DISTRIBUTION = {1: 278_373, 2: 20}
EXPECTED_UNIQUE_PROT_NUMBER_COUNT = 476
EXPECTED_UNIQUE_BLOCK_KEY_COUNT = 278_347
# The producer publishes 91 blocks (all in protraction NG16-02) as two
# same-key features each; rows are kept verbatim and the count is pinned as a
# drift gate.
EXPECTED_DUPLICATE_BLOCK_KEY_COUNT = 91
EXPECTED_OBSERVED_BOUNDS = [-180.0, 23.8179931640625, -64.1082763671875, 74.99639892578125]
BASELINE_PENDING_NOTE = (
    "The record and geometry baseline digests, gate counts and observed bounds "
    "are pinned from a completed capture; unpinned values are checked only once "
    "pinned.")
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", False),
    ("PROT_NUMBER", "esriFieldTypeString", True),
    ("BLOCK_NUMBER", "esriFieldTypeString", True),
    ("BLK_FED_APRV_DT", "esriFieldTypeString", True),
    ("Shape__Area", "esriFieldTypeDouble", True),
    ("Shape__Length", "esriFieldTypeDouble", True),
]
STRING_FIELDS = ["PROT_NUMBER", "BLOCK_NUMBER", "BLK_FED_APRV_DT"]
DOUBLE_FIELDS = ["Shape__Area", "Shape__Length"]
FIELD_NAMES = [field[0] for field in EXPECTED_FIELDS]
STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}
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
    """Accept only EPSG:4269 and its ArcGIS 4269 alias, including WKT evidence."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_spatial_reference_missing")
    identifiers = [value.get(name) for name in ("wkid", "latestWkid")
                   if value.get(name) is not None]
    if identifiers and any(type(code) is not int or code != 4269 for code in identifiers):
        raise ValueError(f"{label}_spatial_reference_drift")
    if not identifiers and not any(value.get(name) for name in ("wkt", "wkt2")):
        raise ValueError(f"{label}_spatial_reference_missing")
    for name in ("wkt", "wkt2"):
        wkt = value.get(name)
        if wkt:
            try:
                if not pyproj.CRS.from_user_input(wkt).equals(pyproj.CRS.from_epsg(4269)):
                    raise ValueError(f"{label}_spatial_reference_drift")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"{label}_spatial_reference_invalid") from exc


def _capture_names(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query(where="1=1", returnCountOnly="true")),
        ("ids_before", _query(where="1=1", returnIdsOnly="true")),
    ]
    # The server drops very long objectIds lists, so pages walk the sorted
    # roster in OBJECTID order with resultOffset/resultRecordCount pagination.
    for page in range(0, (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE):
        urls.append((f"page_{page:03d}", _query(
            where="1=1", outFields="*", returnGeometry="true", outSR="4269",
            orderByFields="OBJECTID ASC", resultOffset=page * PAGE_SIZE,
            resultRecordCount=PAGE_SIZE)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
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
    """Distinguish a well-formed capture document from a transient answer.

    ArcGIS Server intermittently answers cheap bracket queries with HTTP 200
    bodies like ``{"objectIdFieldName": "OBJECTID", "objectIds": null}`` or an
    error document. Such an answer is a service failure, not a capture result:
    real drift publishes a valid roster with different identifiers, which the
    bracket comparison in _validate refuses.
    """
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
        # The preserved one-shot capture was recorded with identity encoding;
        # reconstruct the shared receipt shape without changing observation time.
        receipt.update({"complete_body": True, "wire_bytes": len(body), "wire_sha256": _sha(body),
                        "content_encoding": "identity", "error_code": None})
        captures[name] = (body, receipt)
    return captures


def _acquire(root: Path, storage: StagingStorageResource) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=120,
                          headers={"User-Agent": "hifld-next-datasets/boem-ocs-lease-blocks-draft"}) as client:
            seed, receipt = _acquire_document(client, root, "ids_before",
                                              _query(where="1=1", returnIdsOnly="true"))
            captures["ids_before"] = (seed, receipt)
            ids = json.loads(seed).get("objectIds")
            if not isinstance(ids, list) or not ids or any(type(i) is not int for i in ids) \
                    or len(ids) != len(set(ids)):
                raise ValueError("source_seed_id_roster_invalid")
            if EXPECTED_COUNT is not None and len(ids) != EXPECTED_COUNT:
                raise ValueError("source_count_drift")
            urls = [(n, u) for n, u in _capture_names(ids) if n != "ids_before"]
            for name, url in urls:
                body, receipt = _acquire_document(client, root, name, url)
                captures[name] = (body, receipt)
        return captures
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, "incomplete", captures, type(exc).__name__)
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
        if field["type"] not in ARROW_TYPES:
            raise ValueError("unsupported_source_physical_type:" + str(field["type"]))
    return simple


def _validate_rings(rings: object) -> list[list[list[float]]] | None:
    """Validate one Esri JSON polygon geometry; empty rings means null geometry.

    Rings are checked structurally (closed, exactly two finite geographic
    ordinates per vertex) and passed through verbatim. Every ring is treated as
    an outer part: the complete capture (2026-11-13) found only disjoint
    clockwise outer rings (one- and two-ring features; no ring contained in
    another), so nested/hole rings fail closed rather than being reassembled.
    """
    if not isinstance(rings, list):
        raise ValueError("source_polygon_geometry_invalid")
    if not rings:
        return None
    if any(not isinstance(ring, list) or not ring for ring in rings):
        raise ValueError("source_polygon_ring_invalid")
    for ring in rings:
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise ValueError("source_polygon_vertex_invalid")
            if any(type(v) not in (int, float) or v != v
                   or v in (float("inf"), float("-inf")) for v in vertex):
                raise ValueError("source_polygon_vertex_invalid")
            if not (-180 <= vertex[0] <= 180 and -90 <= vertex[1] <= 90):
                raise ValueError("source_polygon_coordinates_invalid")
        if ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_unclosed")
    parts = [shapely.geometry.Polygon(ring) for ring in rings]
    if len(parts) > 1:
        for index, part in enumerate(parts):
            point = part.representative_point()
            for other_index, other in enumerate(parts):
                if other_index == index:
                    continue
                if other.contains(point) and other.area > part.area:
                    raise ValueError("source_polygon_nested_rings_unexpected")
    return rings


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
    service, layer = docs["service_before"], docs["layer_before"]
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("source_layer_identity_or_geometry_drift")
    if not any(item.get("id") == LAYER_ID and item.get("name") == LAYER_NAME
               for item in service.get("layers", []) if isinstance(item, dict)):
        raise ValueError("source_layer_not_in_service_roster")
    description = service.get("serviceDescription", "")
    if ("Bureau of Ocean Energy Management" not in description
            or "Bureau of Safety and Environmental Enforcement" not in description
            or "mappingboundarybranch@boem.gov" not in description):
        raise ValueError("producer_service_description_drift")
    if layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False \
            or layer.get("hasM") is not False:
        raise ValueError("source_spatial_contract_drift")
    if layer.get("maxRecordCount") != PAGE_SIZE:
        raise ValueError("source_transfer_limit_drift")
    extent = layer.get("extent", {})
    _validate_source_spatial_reference(extent.get("spatialReference"), "source_layer")
    _validate_source_spatial_reference(
        layer.get("sourceSpatialReference")
        if layer.get("sourceSpatialReference") is not None
        else service.get("spatialReference"), "source_service")
    fields = _profile_fields(layer)
    if fields != _profile_fields(docs["layer_after"]):
        raise ValueError("source_schema_changed_during_capture")
    if EXPECTED_SCHEMA_SHA256 and _sha(_canonical(fields)) != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_baseline_drift")
    if docs["service_before"] != docs["service_after"] or layer != docs["layer_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    pages = sorted(name for name in docs if name.startswith("page_"))
    if len(pages) != (count + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for index, name in enumerate(pages):
        page = docs[name]
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
            raise ValueError("source_page_error_or_truncation")
        # A page below the record cap must not claim more rows exist; only a
        # completely full page may report exceededTransferLimit.
        if page.get("exceededTransferLimit") is True and len(rows) != PAGE_SIZE:
            raise ValueError("source_page_error_or_truncation")
        _validate_source_spatial_reference(page.get("spatialReference"), "source_page")
        features.extend(rows)
    feature_ids = [f.get("attributes", {}).get("OBJECTID") for f in features
                   if isinstance(f, dict) and isinstance(f.get("attributes"), dict)]
    if (len(features) != count or len(feature_ids) != count
            or any(type(value) is not int for value in feature_ids)
            or sorted(feature_ids) != sorted(ids_before)):
        raise ValueError("source_feature_population_incomplete")
    if feature_ids != sorted(feature_ids) or len(set(feature_ids)) != count:
        raise ValueError("source_page_order_drift")
    null_geometry = 0
    ring_counts: dict[int, int] = {}
    geometry_parts = []
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(FIELD_NAMES):
            raise ValueError("source_feature_field_roster_drift")
        if not isinstance(geom, dict) or set(geom) - {"rings", "spatialReference"}:
            raise ValueError("source_polygon_geometry_invalid")
        if feature.get("spatialReference") is not None:
            _validate_source_spatial_reference(feature["spatialReference"], "source_feature")
        if geom.get("spatialReference") is not None:
            _validate_source_spatial_reference(geom["spatialReference"], "source_geometry")
        rings = _validate_rings(geom.get("rings"))
        if type(attrs["OBJECTID"]) is not int:
            raise ValueError("source_native_identifier_invalid")
        for name in STRING_FIELDS:
            value = attrs[name]
            if value is not None and not isinstance(value, str):
                raise ValueError("source_string_field_type_drift:" + name)
        for name in DOUBLE_FIELDS:
            value = attrs[name]
            if value is not None and (type(value) not in (int, float) or value != value
                                      or value in (float("inf"), float("-inf"))):
                raise ValueError("source_double_field_type_drift:" + name)
        if rings is None:
            null_geometry += 1
            geometry_parts.append(None)
        else:
            ring_counts[len(rings)] = ring_counts.get(len(rings), 0) + 1
            geometry_parts.append(rings)
    if EXPECTED_NULL_GEOMETRY_COUNT is not None and null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if (EXPECTED_RING_COUNT_DISTRIBUTION is not None
            and dict(sorted(ring_counts.items())) != EXPECTED_RING_COUNT_DISTRIBUTION):
        raise ValueError("source_ring_count_distribution_drift")
    # The producer publication itself is not unique by (PROT_NUMBER,
    # BLOCK_NUMBER): the complete capture found 91 blocks (all in protraction
    # NG16-02) published as two same-key features each. Rows are never merged
    # or dropped; the duplicate count is audited and pinned as a drift gate
    # instead.
    featureclass = [f["attributes"] for f in features]
    prot_numbers = [attrs["PROT_NUMBER"] for attrs in featureclass]
    block_keys = [(attrs["PROT_NUMBER"], attrs["BLOCK_NUMBER"]) for attrs in featureclass]
    duplicate_block_keys = len(block_keys) - len(set(block_keys))
    if EXPECTED_DUPLICATE_BLOCK_KEY_COUNT is not None \
            and duplicate_block_keys != EXPECTED_DUPLICATE_BLOCK_KEY_COUNT:
        raise ValueError("source_block_key_uniqueness_drift")
    spatial = [rings for rings in geometry_parts if rings is not None]
    flat = [vertex for rings in spatial for ring in rings for vertex in ring]
    bounds = None if not flat else [min(v[0] for v in flat), min(v[1] for v in flat),
                                    max(v[0] for v in flat), max(v[1] for v in flat)]
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_bounds_drift")
    record_sha = _sha(_canonical(features))
    if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_value_or_geometry_baseline_drift")
    geometry_sha = _sha(_canonical([[f["attributes"]["OBJECTID"], rings]
                                    for f, rings in zip(features, geometry_parts)]))
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    nulls = {name: sum(1 for attrs in featureclass if attrs[name] is None)
             for name in FIELD_NAMES}
    audit = {"feature_count": count, "unique_objectid_count": len(set(feature_ids)),
             "unique_prot_number_count": len(set(prot_numbers)),
             "unique_block_key_count": len(set(block_keys)),
             "duplicate_block_key_count": duplicate_block_keys,
             "null_counts_by_field": nulls,
             "null_geometry_count": null_geometry,
             "ring_count_distribution": dict(sorted(ring_counts.items())),
             "geometry_bounds": bounds,
             "source_id_sha256": _sha(_canonical(sorted(ids_before))),
             "source_record_sha256": record_sha,
             "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": _sha(_canonical(fields)),
             "source_crs_epsg": 4269, "native_geometry_type": "Polygon",
             "native_dimension": 2,
             "source_release_date": None,
             "service_current_version": service.get("currentVersion"),
             "source_fields": fields,
             "baseline_pending_note": BASELINE_PENDING_NOTE,
             "release_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "freshness_accepted": False,
             "rights_accepted": False, "source_authority_accepted": False,
             "mapping_approved": False}
    return layer, fields, features, audit


def _rings_wkb(rings: list[list[list[float]]]) -> bytes:
    """Store validated disjoint outer rings verbatim as Polygon/MultiPolygon WKB."""
    parts = [shapely.geometry.Polygon(ring) for ring in rings]
    if len(parts) == 1:
        return parts[0].wkb
    return shapely.geometry.MultiPolygon([(part.exterior.coords, None) for part in parts]).wkb


def _geoparquet(features: list[dict[str, object]], fields: list[dict[str, object]],
                root: Path) -> bytes:
    if [field["name"] for field in fields] != FIELD_NAMES:
        raise ValueError("source_field_order_drift")
    schema = pa.schema([
        pa.field("OBJECTID", pa.int64(), nullable=False),
        *[pa.field(name, pa.string(), nullable=True) for name in STRING_FIELDS],
        *[pa.field(name, pa.float64(), nullable=True) for name in DOUBLE_FIELDS],
        pa.field("geometry", pa.binary(), nullable=True),
    ])
    geometry_parts = [_validate_rings(f["geometry"]["rings"]) for f in features]
    columns = {
        "OBJECTID": pa.array([f["attributes"]["OBJECTID"] for f in features], type=pa.int64()),
        **{name: pa.array([f["attributes"][name] for f in features], type=pa.string())
           for name in STRING_FIELDS},
        **{name: pa.array([None if f["attributes"][name] is None
                           else float(f["attributes"][name]) for f in features],
                          type=pa.float64()) for name in DOUBLE_FIELDS},
        "geometry": pa.array([None if rings is None else _rings_wkb(rings)
                              for rings in geometry_parts], type=pa.binary()),
    }
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB",
                                    "geometry_types": ["Polygon", "MultiPolygon"],
                                    "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
                                    "edges": "planar"}}}
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


def capture_boem_ocs_lease_blocks(storage: StagingStorageResource, *, version: str = "draft",
                                  capture_directory: str | None = None,
                                  offline_capture_directory: str | None = None) -> dict[str, object]:
    """Capture an unapproved lease-block polygon candidate; offline replay is supported."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    if shutil.disk_usage(base).free < 1_000_000_000:
        raise ValueError("unsafe_or_low_space_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    capture_parent = Path(capture_directory).absolute() if capture_directory \
        else Path(tempfile.gettempdir()) / "boem-ocs-lease-block-captures"
    capture_root = capture_parent / f"{version}-{uuid4().hex}"
    if ".." in capture_root.parts or any(p.is_symlink() for p in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    if offline_capture_directory:
        captures = _load_offline(Path(offline_capture_directory))
        # The offline loader records identity-encoding receipts whose wire
        # equals the body; materialize the wire evidence so acquisition
        # failures replayed offline quarantine complete evidence.
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
        # Same content version can be observed on a later refresh. Keep the first
        # sealed receipts and manifest verbatim instead of rewriting timestamps.
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                _sha(_canonical(identity)), identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["feature_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "unique_block_key_count": audit["unique_block_key_count"],
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
    inputs = [{"input_id": n, "producer": PUBLISHER, "canonical_url": SERVICE_URL,
               "host": urlsplit(receipt["url"]).hostname,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt.get("wire_bytes"), "wire_sha256": receipt.get("wire_sha256"),
               "license": "https://www.usa.gov/publicdomain/label/1.0/",
               "attribution": "U.S. Government work (BOEM/BSEE cadastre, 30 CFR 256.8); "
                              "rights acceptance remains false.",
               "role": "producer_publication",
               "release_date": None,
               "release_date_unknown_reason": "The cadastre service publishes no dated source "
                                              "release for the lease-block grid; per-block "
                                              "BLK_FED_APRV_DT values are per-block effective "
                                              "dates, not a dataset release date."}
              for n, (_, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False,
                "title": "Outer Continental Shelf (OCS) Lease Blocks",
                "description": "Federal OCS lease-block grid polygons (the leasing cadastre "
                               "blocks identified by BOEM protraction diagram and OCS block "
                               "number), published by BOEM's Geospatial Services Division "
                               "Marine Mineral Cadastre service. Not for official or legal "
                               "use per the service description.",
                "agency": AGENCY, "publisher": PUBLISHER,
                "source_host_first_party": True,
                "source_authority_note": ("gis.boem.gov is BOEM's own ArcGIS Server host (no "
                                          "ArcGIS Online hosting account involved); the service "
                                          "description and BOEM Geospatial Services Division "
                                          "contact match the HIFLD inventory entry. Source "
                                          "authority, freshness and rights acceptance remain "
                                          "false pending maintainer approval."),
                "source_authority_accepted": False,
                "source_url": LAYER_URL, "source_release_date": None,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"],
                "inputs": inputs,
                "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                                     "parameters": {"layer_id": LAYER_ID,
                                                    "source_fields": "all six native fields in "
                                                                     "producer order; space-padded "
                                                                     "BLOCK_NUMBER values, string "
                                                                     "BLK_FED_APRV_DT dates and "
                                                                     "Shape__Area/Shape__Length "
                                                                     "retain source meaning",
                                                    "geometry": "Esri JSON rings read directly "
                                                                "into Polygon/MultiPolygon "
                                                                "WKB; EPSG:4269 retained "
                                                                "without reprojection, "
                                                                "orientation repair or "
                                                                "simplification; empty rings "
                                                                "gated to null geometry and "
                                                                "counted; disjoint outer-ring "
                                                                "multiparts kept as "
                                                                "MultiPolygon parts",
                                                    "release_status": "unapproved draft"}}],
                "lineage": "National Scale Marine Mineral Cadastre Map Server served from BOEM's "
                           "own gis.boem.gov ArcGIS Server by BOEM's Office of Strategic "
                           "Resources, Geospatial Services Division (service contact matches "
                           "the HIFLD inventory entry). The DHS HIFLD Open crosswalk row for "
                           "this layer (Open ID " + CROSSWALK_OPEN_ID + ", byte-identical public "
                           "copy sha256 " + CROSSWALK_COPY_SHA256 + ") directed the layer to the "
                           "GeoPlatform GeoSeas hosted geoseas.geoplatform.gov "
                           "BOEM_Offshore_Marine_Cadastre_Data_Collection "
                           "FeatureServer layer 3, whose host no longer resolves (checked "
                           "2026-11-13); this capture is the same producer's own first-party "
                           "publication of that product, layer 11, not a mirror or archive copy. "
                           "Crosswalk-classified as a maintained producer REST publication.",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "Source attributes and EPSG:4269 polygon geometry retained for an "
                              "unapproved candidate.",
                    "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
                "unsupported_output_formats": {
                    "shapefile_zip": "No approved published schema mapping; release remains blocked.",
                    "geopackage": "No approved published schema mapping; release remains blocked.",
                    "pmtiles": "No approved display transformation or generalized geometry."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_service_url": SERVICE_URL, "source_layer_id": LAYER_ID,
                  "source_layer_name": LAYER_NAME, "source_inputs_sha256": fingerprint,
                  "crosswalk_open_id": CROSSWALK_OPEN_ID,
                  "crosswalk_public_copy_sha256": CROSSWALK_COPY_SHA256,
                  "crosswalk_dead_redirect_note": CROSSWALK_DEAD_REDIRECT_NOTE,
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
            "unique_block_key_count": audit["unique_block_key_count"],
            "duplicate_block_key_count": audit["duplicate_block_key_count"],
            "null_geometry_count": audit["null_geometry_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "reused": False,
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False,
            "rights_accepted": False, "source_authority_accepted": False,
            "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}


def build_boem_ocs_lease_blocks_baseline(capture_directory: str) -> dict[str, object]:
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
            "source_ring_count_distribution": audit["ring_count_distribution"],
            "source_unique_prot_number_count": audit["unique_prot_number_count"],
            "source_unique_block_key_count": audit["unique_block_key_count"],
            "source_duplicate_block_key_count": audit["duplicate_block_key_count"],
            "observed_bounds": audit["geometry_bounds"]}
