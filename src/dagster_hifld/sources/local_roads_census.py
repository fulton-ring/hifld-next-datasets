"""Complete native capture of the official Census TIGERweb local roads layer.

The DHS HIFLD Open crosswalk (row 304, Agency Census, DOI 10.3886/E241367V1)
points the retired HIFLD Open item 003cb563461a4129a5042f340dabea58 at the
producer's maintained TIGERweb Transportation (Roads and Railroads) MapServer
layer 8, whose name is exactly "Local Roads". Its observed population is the
full local-road class (16098190 segments across MTFCC S1400/S1500/S1630/S1640/
S1710/S1720/S1730/S1740/S1750/S1780/S1810/S1820/S1830). The layer serves
20_000 features per response (a 100_000-feature request fails with the
service's Esri 500 "Error performing query operation"), so pages are ordered
OBJECTID range windows; the OBJECTID roster is dense 1..16098190, which the
roster digest pin enforces. The capture is resumable: per-page receipts are
checkpointed atomically in the capture manifest, so a restarted attempt
continues instead of repeating completed pages. The inventory row's Download
pin (the Census TIGER/Line All Roads national geodatabase) holds the general
roads product and is recorded as lineage context, never fetched.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, seal_ready_candidate,
    verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.secondary_roads_census import (
    EXPECTED_SERVICE_LAYERS, EXPECTED_SERVICE_MAP_NAME, EXPECTED_SERVICE_MAX_RECORD_COUNT,
    SERVICE_FOLDER_URL, SERVICE_URL, WEB_MERCATOR_LIMIT, WEB_MERCATOR_PROJJSON,
    EXTENT_TOLERANCE_METERS, _arrow_type, _publisher_stated_vintage,
    _validate_web_mercator_reference,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _esri_polyline_wkb, _load_capture, _sha, _validate_capture_request_identity,
)

DATASET_SLUG = "local-roads-2"
FILE_SLUG = DATASET_SLUG
LAYER_ID = 8
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Census Bureau, TIGERweb Transportation (Roads and Railroads) service"
EXPECTED_COUNT = 16_098_190
EXPECTED_ID_MIN = 1
EXPECTED_ID_MAX = 16_098_190
# The live layer roster (probed 2026-11-15) is dense 1..16098190; the digest
# pins the exact population and rejects sparsity drift.
EXPECTED_ID_SHA256 = "24aa30d5f04afb174716e3d06cb7f4f4c1b5dd5b8d6ff0f29f90926fa7aaedba"
PAGE_SIZE = 20_000
MAX_PAGE_BYTES = 64_000_000
MAX_ROSTER_BYTES = 256_000_000
MAX_CAPTURE_BYTES = 45_000_000_000
EXPECTED_FIELD_DECLARATIONS = [
    {"alias": "BASENAME", "domain": None, "length": 100, "name": "BASENAME", "type": "esriFieldTypeString"},
    {"alias": "MTFCC", "domain": None, "length": 5, "name": "MTFCC", "type": "esriFieldTypeString"},
    {"alias": "NAME", "domain": None, "length": 100, "name": "NAME", "type": "esriFieldTypeString"},
    {"alias": "OBJECTID", "domain": None, "name": "OBJECTID", "type": "esriFieldTypeOID"},
    {"alias": "OID", "domain": None, "length": 22, "name": "OID", "type": "esriFieldTypeString"},
    {"alias": "PREDIR", "domain": None, "name": "PREDIR", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PREDIRABRV", "domain": None, "length": 15, "name": "PREDIRABRV", "type": "esriFieldTypeString"},
    {"alias": "PREQUAL", "domain": None, "name": "PREQUAL", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PREQUALABRV", "domain": None, "length": 15, "name": "PREQUALABRV", "type": "esriFieldTypeString"},
    {"alias": "PRETYP", "domain": None, "name": "PRETYP", "type": "esriFieldTypeSmallInteger"},
    {"alias": "PRETYPEABRV", "domain": None, "length": 14, "name": "PRETYPEABRV", "type": "esriFieldTypeString"},
    {"alias": "RTTYP", "domain": None, "length": 1, "name": "RTTYP", "type": "esriFieldTypeString"},
    {"alias": "STGEOMETRY", "domain": None, "name": "STGEOMETRY", "type": "esriFieldTypeGeometry"},
    {"alias": "SUFDIR", "domain": None, "name": "SUFDIR", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFDIRABRV", "domain": None, "length": 15, "name": "SUFDIRABRV", "type": "esriFieldTypeString"},
    {"alias": "SUFQUAL", "domain": None, "name": "SUFQUAL", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFQUALABRV", "domain": None, "length": 15, "name": "SUFQUALABRV", "type": "esriFieldTypeString"},
    {"alias": "SUFTYP", "domain": None, "name": "SUFTYP", "type": "esriFieldTypeSmallInteger"},
    {"alias": "SUFTYPEABRV", "domain": None, "length": 14, "name": "SUFTYPEABRV", "type": "esriFieldTypeString"},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["type"] != "esriFieldTypeGeometry"]
EXPECTED_EXTENT = {"xmin": -19698975.6412, "ymin": -1615788.0086999983,
                   "xmax": 19976614.354900002, "ymax": 11535623.297200002}
EXPECTED_LAYER_NAME = "Local Roads"
# The publisher stamps the layer description with the vintage of the observed
# layer population (verified live 2026-11-15); the trailing date is recorded as
# the publisher-stated source vintage, not a per-feature last-edit timestamp
# and not the HTTP retrieval time. Freshness/release acceptance stays pending.
EXPECTED_LAYER_DESCRIPTION = "Local roads; January 1, 2026"
SOURCE_VINTAGE_NOTE = ("Publisher-stated vintage of the observed TIGERweb layer population, parsed from the "
    "captured layer description 'Local roads; January 1, 2026'; it dates the published layer population and is "
    "not a per-feature last-edit timestamp or the HTTP retrieval time. Freshness acceptance remains pending.")
EXPECTED_SCALE_RANGE = {"minScale": 42000, "maxScale": 1001}
# Population contract observed on the live layer (2026-11-15 group-by probe):
# 16098190 features across 13 road MTFCC classes and 59 exact (MTFCC, RTTYP)
# pairs; RTTYP is nullable for the local class (the "null" key).
EXPECTED_MTFCC_VALUES = {"S1400", "S1500", "S1630", "S1640", "S1710", "S1720", "S1730",
                         "S1740", "S1750", "S1780", "S1810", "S1820", "S1830"}
EXPECTED_RTTYPE_VALUES = {"M", "C", "O", "S", "U", "I"}
EXPECTED_MTFCC_RTTYPE_COUNTS = {
    "S1400|C": 347353, "S1400|I": 62, "S1400|M": 8589911, "S1400|O": 93657,
    "S1400|S": 103771, "S1400|U": 1803, "S1400|null": 4766510,
    "S1500|C": 3488, "S1500|M": 37613, "S1500|O": 3046, "S1500|S": 104,
    "S1500|U": 15, "S1500|null": 258592,
    "S1630|M": 3, "S1630|null": 199521,
    "S1640|C": 66, "S1640|I": 155, "S1640|M": 51282, "S1640|O": 1803,
    "S1640|S": 253, "S1640|U": 182, "S1640|null": 81034,
    "S1710|C": 9, "S1710|M": 5650, "S1710|O": 64, "S1710|S": 3, "S1710|null": 10281,
    "S1720|M": 177, "S1720|null": 76,
    "S1730|C": 5, "S1730|M": 15238, "S1730|O": 17, "S1730|S": 20,
    "S1730|U": 6, "S1730|null": 57077,
    "S1740|C": 2597, "S1740|M": 66878, "S1740|O": 1039, "S1740|S": 127,
    "S1740|U": 15, "S1740|null": 1150794,
    "S1750|C": 10, "S1750|M": 464, "S1750|O": 1, "S1750|null": 198513,
    "S1780|C": 135, "S1780|M": 7836, "S1780|O": 71, "S1780|S": 8,
    "S1780|U": 2, "S1780|null": 36081,
    "S1810|M": 143, "S1810|null": 275,
    "S1820|C": 1, "S1820|M": 1223, "S1820|O": 3, "S1820|null": 3104,
    "S1830|M": 4, "S1830|null": 19,
}
# Baseline digests of the complete decoded record and geometry streams are
# pinned here only after one complete capture is reviewed; the empty
# placeholders keep the record/geometry gates inactive (the capture computes
# and reports the digests instead) until that baseline exists.
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
BASELINE_PENDING_NOTE = (
    "Record and geometry stream digests are computed by the complete capture "
    "of the 16098190-feature layer and pinned into this module only after "
    "that baseline is reviewed; until then the record/geometry gates stay "
    "inactive and the capture reports the computed digests.")
_BEFORE_NAMES = ("folder", "service_before", "layer_before", "count_before", "ids_before")
_AFTER_NAMES = ("layer_after", "count_after", "ids_after", "service_after")
_TRANSIENT_RETRY_DELAYS = (5.0, 15.0, 45.0, 90.0)


def page_count() -> int:
    return (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE


def page_window(index: int) -> tuple[int, int]:
    """Inclusive OBJECTID window for page index over the dense pinned roster."""
    if not 0 <= index < page_count():
        raise ValueError("source_page_index_out_of_range")
    start = EXPECTED_ID_MIN + index * PAGE_SIZE
    return start, min(start + PAGE_SIZE - 1, EXPECTED_ID_MAX)


def _capture_names() -> set[str]:
    return set(_BEFORE_NAMES) | set(_AFTER_NAMES) | {f"page_{index:04d}" for index in range(page_count())}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_urls() -> dict[str, str]:
    urls = {
        "folder": SERVICE_FOLDER_URL,
        "service_before": SERVICE_URL + "?f=pjson",
        "layer_before": LAYER_URL + "?f=pjson",
        "count_before": _query({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
        "ids_before": _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
    }
    for index in range(page_count()):
        lo, hi = page_window(index)
        urls[f"page_{index:04d}"] = _query({"where": f"(objectid>={lo}) AND (objectid<={hi})",
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "orderByFields": "OBJECTID ASC", "f": "json"})
    urls.update({
        "layer_after": LAYER_URL + "?f=pjson",
        "count_after": _query({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
        "ids_after": _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
        "service_after": SERVICE_URL + "?f=pjson",
    })
    return urls


def _validate_roster_body(body: bytes, label: str) -> list[int]:
    ids = json.loads(body).get("objectIds")
    if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
            or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
            or min(ids) != EXPECTED_ID_MIN or max(ids) != EXPECTED_ID_MAX
            or _sha(_canonical(ids)) != EXPECTED_ID_SHA256):
        raise ValueError(f"source_count_or_id_population_drift:{label}")
    return ids


def _capture(client: httpx.Client, url: str, root: Path, name: str, *, max_bytes: int) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire") for path in root.glob(suffix)
                   if path.is_file())
    if retained + 2 * max_bytes > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    for attempt, extra_delay in enumerate((0.0, *_TRANSIENT_RETRY_DELAYS)):
        if attempt:
            for suffix in (".body", ".wire", ".receipt.json"):
                path = root / f"{name}{suffix}"
                if path.exists():
                    path.unlink()
            if extra_delay:
                time.sleep(extra_delay)
        try:
            return capture_get(client, url, root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if (attempt == len(_TRANSIENT_RETRY_DELAYS)
                    or not str(exc).startswith(f"incomplete_source_capture:{name}")):
                raise
    raise ValueError(f"incomplete_source_capture:{name}")


def _manifest_path(root: Path) -> Path:
    return root / "capture_manifest.json"


def _write_progress(root: Path, receipts: dict[str, dict], *, error_code: str | None = None) -> None:
    """Atomically checkpoint per-page receipts so a restarted attempt resumes."""
    payload = {"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
               "receipts": dict(receipts)}
    if error_code is not None:
        payload["error_code"] = error_code
    temporary = _manifest_path(root).with_name("capture_manifest.json.tmp")
    temporary.write_bytes(_canonical(payload) + b"\n")
    temporary.replace(_manifest_path(root))


def _read_progress(root: Path) -> dict[str, dict]:
    path = _manifest_path(root)
    if path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(path.read_bytes())
    receipts = manifest.get("receipts")
    if manifest.get("complete") is not False or not isinstance(receipts, dict) \
            or any(not isinstance(receipt, dict) for receipt in receipts.values()):
        raise ValueError("retained_capture_progress_invalid")
    return receipts


def _write_complete_manifest(root: Path, audit: dict[str, object]) -> None:
    receipts = {}
    for name in sorted(_capture_names()):
        _body, _wire, receipt = _load_capture(root, name)
        receipts[name] = receipt
    manifest = {"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
        "source_feature_count": audit["source_feature_count"],
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "receipts": receipts,
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
    temporary = _manifest_path(root).with_name("capture_manifest.json.tmp")
    temporary.write_bytes(_canonical(manifest) + b"\n")
    temporary.replace(_manifest_path(root))


def _read_replay(root: Path) -> dict[str, dict]:
    manifest_path = _manifest_path(root)
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != _capture_names():
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    urls = _capture_urls()
    for name in sorted(manifest["receipts"]):
        _body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"][name]:
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, urls[name])
    return manifest["receipts"]


def _validate_ids_page(page_ids: list[int], index: int) -> None:
    lo, hi = page_window(index)
    if page_ids != list(range(lo, hi + 1)):
        raise ValueError(f"source_page_id_population_drift:page_{index:04d}")


def _streaming_candidate_writer(fields: list[dict], path: Path):
    """Stream typed candidate parquet page by page, preserving native order."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    properties = [field for field in fields if field["type"] != "esriFieldTypeGeometry"]
    schema = pa.schema([pa.field(field["name"], _arrow_type(field)) for field in properties]
                       + [pa.field("geometry", pa.binary())])
    geo = _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiLineString"],
                                 "crs": WEB_MERCATOR_PROJJSON}}})
    schema = schema.with_metadata({b"geo": geo})
    return pq.ParquetWriter(path, schema, compression="zstd", use_dictionary=False), properties


def _validate_stream(root: Path, receipts: dict[str, dict]) -> tuple[list[dict], Path, dict]:
    """One streaming pass over the retained pages: validate, digest, parquet."""
    if set(receipts) != _capture_names():
        raise ValueError("source_capture_name_roster_drift")
    urls = _capture_urls()
    for name in sorted(receipts):
        _validate_capture_request_identity(name, receipts[name], urls[name])
    docs: dict[str, object] = {}
    for name in ("folder", "service_before", "layer_before", "count_before",
                 "layer_after", "count_after", "service_after"):
        body, _wire, _receipt = _load_capture(root, name)
        docs[name] = json.loads(body)
    folder = docs["folder"]
    if not any(service.get("name") == "TIGERweb/Transportation" and service.get("type") == "MapServer"
               for service in folder.get("services", [])):
        raise ValueError("official_tigerweb_service_directory_drift")
    service = docs["service_before"]
    advertised = [(layer.get("id"), layer.get("name")) for layer in service.get("layers", [])]
    if (service.get("mapName") != EXPECTED_SERVICE_MAP_NAME
            or service.get("maxRecordCount") != EXPECTED_SERVICE_MAX_RECORD_COUNT
            or advertised != EXPECTED_SERVICE_LAYERS):
        raise ValueError("official_tigerweb_transportation_service_drift")
    _validate_web_mercator_reference(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != EXPECTED_LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("description") != EXPECTED_LAYER_DESCRIPTION
            or {key: layer.get(key) for key in EXPECTED_SCALE_RANGE} != EXPECTED_SCALE_RANGE
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_scale_drift")
    vintage = _publisher_stated_vintage(layer.get("description"))
    _validate_web_mercator_reference(layer.get("spatialReference"), "source_layer")
    _validate_web_mercator_reference(layer.get("extent", {}).get("spatialReference"), "source_layer_extent")
    _validate_roster_body((root / "ids_before.body").read_bytes(), "ids_before")
    _validate_roster_body((root / "ids_after.body").read_bytes(), "ids_after")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_or_id_population_drift")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")

    # The derived candidate parquet lives outside the capture root so the
    # retained-evidence roster stays exactly the capture artifacts.
    parquet_path = root.parent / f"candidate-{root.name}.parquet"
    if parquet_path.exists():
        parquet_path.unlink()
    writer, properties = _streaming_candidate_writer(layer["fields"], parquet_path)
    record_digest, geometry_digest = hashlib.sha256(), hashlib.sha256()
    vertices = 0
    nulls = {field["name"]: 0 for field in ATTRIBUTE_FIELDS}
    geometry_valid = geometry_empty = 0
    pair_counts: dict[str, int] = {}
    attribute_names = {field["name"] for field in ATTRIBUTE_FIELDS}
    feature_count = 0
    try:
        import pyarrow as pa
        record_digest.update(b"[")
        geometry_digest.update(b"[")
        first = True
        for index in range(page_count()):
            name = f"page_{index:04d}"
            body, _wire, _receipt = _load_capture(root, name)
            page = json.loads(body)
            if "error" in page or page.get("exceededTransferLimit") is True:
                raise ValueError("source_page_error_or_transfer_limit:" + name)
            _validate_web_mercator_reference(page.get("spatialReference"), "source_" + name)
            rows = page.get("features")
            if not isinstance(rows, list):
                raise ValueError(f"source_page_population_incomplete:{name}")
            page_ids = []
            page_records: list[bytes] = []
            page_geometries: list[bytes] = []
            page_wkbs: list[bytes] = []
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("source_attribute_roster_or_order_drift:" + name)
                attrs, geometry = row.get("attributes"), row.get("geometry")
                if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                        or set(attrs) != attribute_names):
                    raise ValueError("source_attribute_roster_or_order_drift:" + name)
                for field in ATTRIBUTE_FIELDS:
                    value = attrs[field["name"]]
                    if value is None:
                        nulls[field["name"]] += 1
                        continue
                    kind = field["type"]
                    if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                        raise ValueError("source_oid_value_type_drift")
                    if kind == "esriFieldTypeSmallInteger" and type(value) is not int:
                        raise ValueError("source_integer_value_type_drift:" + field["name"])
                    if kind == "esriFieldTypeString":
                        if not isinstance(value, str):
                            raise ValueError("source_string_value_type_drift:" + field["name"])
                        if len(value) > int(field["length"]):
                            raise ValueError("source_string_length_drift:" + field["name"])
                if attrs["MTFCC"] is None or attrs["MTFCC"] not in EXPECTED_MTFCC_VALUES:
                    raise ValueError("source_mtfcc_population_contract_drift:" + name)
                if attrs["RTTYP"] is not None and attrs["RTTYP"] not in EXPECTED_RTTYPE_VALUES:
                    raise ValueError("source_rttype_population_contract_drift:" + name)
                pair = attrs["MTFCC"] + "|" + (attrs["RTTYP"] if attrs["RTTYP"] is not None else "null")
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
                if not isinstance(geometry, dict) or set(geometry) - {"paths", "hasZ", "hasM", "spatialReference"}:
                    raise ValueError("source_polyline_geometry_structure_drift:" + name)
                if geometry.get("spatialReference") is not None:
                    _validate_web_mercator_reference(geometry.get("spatialReference"),
                                                     "source_" + name + "_feature")
                if geometry.get("hasZ", False) is not False or geometry.get("hasM", False) is not False:
                    raise ValueError("source_polyline_zm_not_supported:" + name)
                paths = geometry.get("paths")
                if not isinstance(paths, list) or not paths:
                    raise ValueError("source_polyline_paths_missing:" + name)
                for path in paths:
                    if not isinstance(path, list) or len(path) < 2:
                        raise ValueError("source_polyline_path_invalid:" + name)
                    for point in path:
                        if (not isinstance(point, list) or len(point) != 2
                                or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                            raise ValueError("source_xy_ordinate_contract_drift:" + name)
                        if not (-WEB_MERCATOR_LIMIT <= point[0] <= WEB_MERCATOR_LIMIT
                                and -WEB_MERCATOR_LIMIT <= point[1] <= WEB_MERCATOR_LIMIT):
                            raise ValueError("source_xy_web_mercator_domain_violation:" + name)
                        if not (EXPECTED_EXTENT["xmin"] - EXTENT_TOLERANCE_METERS <= point[0]
                                <= EXPECTED_EXTENT["xmax"] + EXTENT_TOLERANCE_METERS
                                and EXPECTED_EXTENT["ymin"] - EXTENT_TOLERANCE_METERS <= point[1]
                                <= EXPECTED_EXTENT["ymax"] + EXTENT_TOLERANCE_METERS):
                            raise ValueError("source_vertex_outside_declared_extent:" + name)
                        vertices += 1
                wkb = _esri_polyline_wkb(geometry)
                geom = from_wkb(wkb)
                geometry_empty += int(geom.is_empty)
                geometry_valid += int(geom.is_valid and not geom.is_empty)
                page_records.append(_canonical(attrs))
                page_geometries.append(_canonical([attrs["OBJECTID"], geometry]))
                page_wkbs.append(wkb)
                page_ids.append(attrs["OBJECTID"])
            _validate_ids_page(page_ids, index)
            separator = b"" if first else b","
            record_digest.update(separator + b",".join(page_records))
            geometry_digest.update(separator + b",".join(page_geometries))
            first = False
            feature_count += len(page_ids)
            arrays = [pa.array([row["attributes"][field["name"]] for row in rows], type=_arrow_type(field))
                      for field in properties]
            arrays.append(pa.array(page_wkbs, type=pa.binary()))
            writer.write_table(pa.Table.from_arrays(arrays, names=[field["name"] for field in properties]
                                                    + ["geometry"]))
            del page, rows, page_records, page_geometries, page_wkbs
    finally:
        writer.close()
    record_digest.update(b"]")
    geometry_digest.update(b"]")
    record_sha, geometry_sha = record_digest.hexdigest(), geometry_digest.hexdigest()
    if feature_count != EXPECTED_COUNT or pair_counts != EXPECTED_MTFCC_RTTYPE_COUNTS:
        raise ValueError("source_feature_id_or_population_contract_drift")
    if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_sha256_drift")
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_sha256_drift")
    audit = {
        "source_feature_count": feature_count,
        "source_id_sha256": EXPECTED_ID_SHA256, "source_id_min": EXPECTED_ID_MIN,
        "source_id_max": EXPECTED_ID_MAX,
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_geometry_type": "esriGeometryPolyline", "source_wkid": 102100, "source_latest_wkid": 3857,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": feature_count - geometry_valid - geometry_empty,
        "source_null_counts": nulls,
        "source_mtfcc_rttype_counts": pair_counts,
        "source_vintage": vintage, "source_vintage_evidence": layer.get("description"),
        "source_vintage_note": SOURCE_VINTAGE_NOTE,
        "source_coverage_scope": "Complete observed TIGERweb Transportation MapServer layer 8 "
            "Local Roads (16098190 segments across the local-road MTFCC classes S1400/S1500/S1630/"
            "S1640/S1710/S1720/S1730/S1740/S1750/S1780/S1810/S1820/S1830) from the directly queried "
            "U.S. Census Bureau service; the full-detail continuously maintained national layer at its "
            "1:1001-1:42000 display scale, not the TIGER/Line All Roads geodatabase product.",
        "drift_guard": "Exact TIGERweb folder/service/layer roster, full field declarations/types, "
            "Web Mercator layer and response-page SRs, scale range, count and before/after dense-roster "
            "digest, per-page OBJECTID windows against the pinned dense roster, per-feature attributes/"
            "types/transfer limits, XY path dimensions, Web Mercator domain and pinned-extent containment "
            "of every vertex, and the observed MTFCC/RTTYP pair population contract.",
    }
    return layer["fields"], parquet_path, audit


def _streaming_capture_identity(receipts: dict[str, dict]) -> dict[str, dict]:
    """Per-artifact decoded/wire digests, bound by capture_get's fail-closed readback."""
    return {name: {"decoded_sha256": receipt["sha256"], "wire_sha256": receipt["wire_sha256"]}
            for name, receipt in sorted(receipts.items())}


def _streaming_quarantine(storage: StagingStorageResource, root: Path, version: str,
                          receipts: dict[str, dict], reason_code: str) -> str:
    """Retain all completed acquisition evidence under a unique quarantine key."""
    token = f"{version}-{uuid4().hex}"
    base = f"_quarantine/{DATASET_SLUG}/{token}"
    for name in sorted(receipts):
        try:
            body, wire, _receipt = _load_capture(root, name)
        except (OSError, ValueError):
            continue
        write_immutable(storage, f"{base}/raw/{name}.body", body)
        write_immutable(storage, f"{base}/raw/{name}.wire", wire)
        write_immutable(storage, f"{base}/raw/{name}.receipt.json", _canonical(receipts[name]) + b"\n")
    write_immutable(storage, f"{base}/metadata/failure.json",
                    _canonical({"reason_code": reason_code, "captured_names": sorted(receipts)}) + b"\n")
    return base


def _disk_receipts(root: Path) -> dict[str, dict]:
    """Receipts of every completed capture artifact currently on disk."""
    receipts = {}
    for path in sorted(root.glob("*.receipt.json")):
        name = path.name.removesuffix(".receipt.json")
        try:
            receipt = json.loads(path.read_bytes())
        except (OSError, ValueError):
            continue
        if not isinstance(receipt, dict) or receipt.get("complete_body") is not True or receipt.get("status") != 200:
            continue
        try:
            _load_capture(root, name)
        except ValueError:
            continue
        receipts[name] = receipt
    return receipts


def _file_sha256_stream(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def capture_local_roads_census(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict[str, object]:
    """Capture, resume, or replay the complete TIGERweb Local Roads candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 4_000_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    if requested is not None:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        replay = False
        if _manifest_path(root).exists():
            replay = json.loads(_manifest_path(root).read_bytes()).get("complete") is True
        else:
            if any(root.glob("*.body")) or any(root.glob("*.wire")):
                raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        if not replay:
            root.mkdir(parents=True, exist_ok=True)
    else:
        replay = False
        root = Path("/tmp/hifld-native-reconstruction/local-roads/captures") / (version + "-" + uuid4().hex)
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=True)

    identity = processing_identity(Path(__file__).parents[3])
    parquet_path: Path | None = None
    # Transport failures during a capture attempt keep their per-page progress
    # checkpoint on disk for the next resuming attempt; only validation drift
    # of a completed capture quarantines evidence.
    receipts = _read_replay(root) if replay else _capture_attempt(root)
    try:
        fields, parquet_path, audit = _validate_stream(root, receipts)
        _write_complete_manifest(root, audit)
    except Exception as exc:
        if parquet_path is not None and parquet_path.exists():
            parquet_path.unlink()
        disk_receipts = _disk_receipts(root)
        if disk_receipts:
            quarantine = _streaming_quarantine(storage, root, version, disk_receipts,
                                               type(exc).__name__)
            raise ValueError(f"local_roads_capture_failed; quarantined_at:{quarantine}") from exc
        raise
    capture_hash = _sha(_canonical(_streaming_capture_identity(receipts)))
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _streaming_capture_identity(receipts),
        "source_receipts": dict(sorted(receipts.items())),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    parquet_bytes = parquet_path.stat().st_size
    parquet_sha = _file_sha256_stream(parquet_path)
    inputs = []
    vintage = audit["source_vintage"]
    for name, receipt in sorted(receipts.items()):
        canonical = (SERVICE_FOLDER_URL if name == "folder" else
            SERVICE_URL if name.startswith("service_") else
            LAYER_URL if name.startswith("layer_") else
            LAYER_URL + "/query")
        inputs.append({"input_id": name, "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": canonical, "retrieval_url": receipt["url"],
            **({} if receipt["final_url"] == receipt["url"] else {"observed_final_url": receipt["final_url"]}),
            "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": vintage, "release_date_meaning": SOURCE_VINTAGE_NOTE,
            "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
            "attribution": "U.S. Census Bureau, TIGERweb Transportation (Roads and Railroads), layer 8 Local Roads."})
    manifest = {"schema_version": 1, "approved": False, "title": "Local Roads",
        "description": "Complete observed Census TIGERweb Transportation MapServer layer 8 Local Roads: "
            "16098190 local-class road segments (MTFCC S1400/S1500/S1630/S1640/S1710/S1720/S1730/S1740/"
            "S1750/S1780/S1810/S1820/S1830) at the layer's 1:1001-1:42000 display scale. Historical "
            "HIFLD Open equivalence, contract, baseline, rights and release remain unaccepted.",
        "publisher": PUBLISHER, "agency": "U.S. Census Bureau", "source_url": LAYER_URL,
        "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
        "source_modified": None, "date_issued": vintage, "date_modified": None,
        "source_vintage_note": SOURCE_VINTAGE_NOTE,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polyline (Web Mercator) to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID and OID retained unchanged", "source_crs": "EPSG:3857 (service wkid 102100)",
            "geometry": "2D path order and coordinates preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": "The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, row 'Local Roads', Status Deprecate, "
            "Agency Census) points the retired HIFLD Open geoplatform item "
            "003cb563461a4129a5042f340dabea58 at this producer service layer. The HIFLD Open inventory row's "
            "Download pin (www2.census.gov TIGER/Line All Roads national geodatabase) holds the general roads "
            "product and is recorded as lineage context and never fetched. The DataLumos record 240800 is a "
            "historical archive copy consulted as research context only.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D paths are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": parquet_bytes, "sha256": parquet_sha}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": EXPECTED_LAYER_NAME, "official_layer_id": LAYER_ID,
                  "source_population_complete": True})
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": parquet_bytes, "sha256": parquet_sha}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(receipts):
            raise ValueError("cached_candidate_capture_roster_changed")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        parquet_path.unlink()
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": EXPECTED_ID_MIN,
            "source_id_max": EXPECTED_ID_MAX,
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, receipt in sorted(receipts.items()):
        body, wire, _receipt = _load_capture(root, name)
        for key, payload in ((f"raw/{name}.body", body), (f"raw/{name}.wire", wire),
                             (f"raw/{name}.receipt.json", _canonical(receipt) + b"\n")):
            write_immutable(storage, f"{prefix}/{key}", payload)
            snapshot = storage.object_snapshot(f"{prefix}/{key}")
            descriptors[key] = {"bytes": len(payload), "sha256": _sha(payload)}
            if snapshot is None or snapshot.size != len(payload) or storage.sha256_key(f"{prefix}/{key}") != descriptors[key]["sha256"]:
                raise ValueError("candidate_object_readback_failed:" + key)
    parquet_key = f"{prefix}/{output_path}"
    if storage.object_exists(parquet_key):
        if storage.sha256_key(parquet_key) != parquet_sha:
            raise ValueError("immutable_staged_object_changed:" + parquet_key)
    else:
        storage.upload_local_file(parquet_path, parquet_key)
    if storage.sha256_key(parquet_key) != parquet_sha:
        raise ValueError("candidate_object_readback_failed:" + output_path)
    parquet_path.unlink()
    descriptors[output_path] = {"bytes": parquet_bytes, "sha256": parquet_sha}
    for key, payload in (("metadata/source_manifest.json", _canonical(manifest) + b"\n"),
                         ("metadata/source_audit.json", _canonical(audit) + b"\n"),
                         ("metadata/processing_identity.json", _canonical(identity) + b"\n"),
                         ("metadata/candidate_ready.json", _canonical(ready) + b"\n")):
        write_immutable(storage, f"{prefix}/{key}", payload)
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": {name: receipt for name, receipt in sorted(receipts.items())},
        "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": EXPECTED_ID_MIN,
        "source_id_max": EXPECTED_ID_MAX,
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}


def _capture_attempt(root: Path) -> dict[str, dict]:
    """Run one capture attempt over the pinned dense roster, resuming progress."""
    urls = _capture_urls()
    receipts: dict[str, dict] = {}
    if _manifest_path(root).exists():
        retained = _read_progress(root)
        for name in sorted(_BEFORE_NAMES):
            if name in retained:
                _load_capture(root, name)
                receipts[name] = retained[name]
        for index in range(page_count()):
            name = f"page_{index:04d}"
            if name in retained:
                _load_capture(root, name)
                receipts[name] = retained[name]
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(240, connect=15),
                      headers={"User-Agent": "hifld-next-datasets/census-tigerweb-draft"}) as client:
        for name in _BEFORE_NAMES:
            if name in receipts:
                continue
            _body, receipts[name] = _capture(client, urls[name], root, name,
                                             max_bytes=MAX_ROSTER_BYTES if name.startswith("ids_") else MAX_PAGE_BYTES)
            _write_progress(root, receipts)
        for index in range(page_count()):
            name = f"page_{index:04d}"
            if name in receipts:
                continue
            try:
                _body, receipts[name] = _capture(client, urls[name], root, name, max_bytes=MAX_PAGE_BYTES)
            except Exception as exc:
                _write_progress(root, receipts, error_code=type(exc).__name__)
                raise
            _write_progress(root, receipts)
        for name in _AFTER_NAMES:
            _body, receipts[name] = _capture(client, urls[name], root, name,
                                             max_bytes=MAX_ROSTER_BYTES if name.startswith("ids_") else MAX_PAGE_BYTES)
    return receipts
