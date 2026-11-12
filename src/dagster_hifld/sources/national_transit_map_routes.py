"""Bounded native capture for the current BTS NTAD National Transit Map Routes layer.

The hosted layer declares hasZ=true, so page queries send returnZ=true and the
native Z coordinate is validated, digested and published (WKB 'MultiLineString
Z'); it is never silently dropped. The source is a current BTS publication.
DataLumos V1 is a distinct 2024 vintage; archive equivalence, baseline, rights,
field mapping, and release acceptance are intentionally unresolved. No source
data are captured by importing this module.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
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
from shapely.geometry import MultiLineString

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    CaptureProgress, candidate_prefix, capture_get, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

ITEM_ID = "985bf490e9fa462f80b1cb8ad5f6e139"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Transit_Map_Routes/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "National Transit Map Routes"
DATASET_SLUG = "national-transit-map-routes"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)"
LICENSE = "https://creativecommons.org/licenses/by/3.0/us/"
PAGE_SIZE = 1000
# 2026-11-09 live measurement: a full 1000-row returnZ=true page body is
# 21,510,735 B; the previous 12,000,000 cap rejected every real page. 64 MiB is
# ~3x the largest measured page, so a modest producer-side growth still fails
# closed instead of silently truncating the population.
MAX_BODY_BYTES = 64_000_000
# 2026-11-09 forecast from measured pages: 77 pages x ~21.5 MiB body + ~21.5 MiB
# wire at Accept-Encoding identity ~= 3.32 GiB, plus small metadata documents.
# 6 GiB gives bounded headroom over that forecast while still refusing an
# unbounded or runaway feed; the previous 1 GiB cap could never hold one capture.
MAX_CAPTURE_TOTAL_BYTES = 6_000_000_000
MAX_FAILURE_RECEIPT_BYTES = 64_000
MAX_FAILURE_RECEIPTS_TOTAL_BYTES = 1_000_000
MAX_CAPTURE_PAGES = 1_024
MAX_CAPTURE_RESPONSES = MAX_CAPTURE_PAGES + 11
# 2026-11-09 live evidence: the ArcGIS Online gateway intermittently returned
# HTTP 504 (twice, at page 12 and page 19 of full captures) and reset one
# stream mid-body, clearing within a minute each time. Retry incomplete
# transport responses a bounded number of times with growing backoff; any
# persistent failure still fails closed, and each accepted response remains a
# complete hash-pinned producer body.
PAGE_ATTEMPTS = 4
PAGE_RETRY_BACKOFF_SECONDS = (15, 30, 60)
# The complete candidate keeps a second raw copy in staging (bodies + wire,
# ~3.4 GiB forecast) plus the parquet candidate; keep 2 GiB free after writing.
DISK_FLOOR_BYTES = 2_000_000_000
MAX_OBJECT_ID_URL_LENGTH = 1800
# Dated 2026-11-09 anonymous probes of the live producer (see PR #694):
# count 76,687; sorted-ID-roster and canonical-schema digests recomputed from
# FeatureServer/0 metadata; the item was republished 2026-11-02T15:21:08Z.
EXPECTED_SOURCE_SCHEMA_SHA256 = "86e2861b4da8fce1f1b837afe74382b27992ff7956ec06eec641b5ffe81a689b"
EXPECTED_SOURCE_ID_SHA256 = "b22ed62787e1a89324226b0957339ed34bcaba3ff4413b22e9fbcd7e88d2a250"
EXPECTED_SOURCE_COUNT = 76_687
EXPECTED_FIELD_NAMES = ["OBJECTID", "ntd_id", "feed_id", "shape_id", "route_id", "agency_id",
    "route_short_name", "route_long_name", "route_desc", "route_type", "route_type_text",
    "route_url", "route_color", "route_text_color", "download_date", "Shape__Length"]
EXPECTED_FIELD_TYPES = ["esriFieldTypeOID", *(["esriFieldTypeString"] * 14), "esriFieldTypeDouble"]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    # This is a proposed native-preservation encoding. Esri dates remain their
    # source epoch-millisecond integers; review can approve a richer mapping.
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.int64(), "esriFieldTypeGUID": pa.string(),
        "esriFieldTypeGlobalID": pa.string(),
    }[str(field["type"])]


def _query_url(**params: object) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _source_metadata_urls() -> list[tuple[str, str]]:
    return [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query_url(where="1=1", returnCountOnly="true", f="json")),
        ("ids_before", _query_url(where="1=1", returnIdsOnly="true", f="json")),
    ]


def _source_after_urls() -> list[tuple[str, str]]:
    return [
        ("portal_item_after", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query_url(where="1=1", returnCountOnly="true", f="json")),
        ("ids_after", _query_url(where="1=1", returnIdsOnly="true", f="json")),
    ]


def _supports_ordered_pagination(layer: Mapping[str, object]) -> bool:
    capabilities = layer.get("advancedQueryCapabilities")
    return (isinstance(capabilities, dict)
            and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True)


def _object_id_chunks(ids: list[int]) -> list[list[int]]:
    """Split fallback ObjectID GET requests below the server URL limit."""
    chunks: list[list[int]] = []
    current: list[int] = []
    for object_id in ids:
        candidate = [*current, object_id]
        params = {"objectIds": ",".join(map(str, candidate)), "outFields": "*",
            "returnGeometry": "true", "returnZ": "true",
            "orderByFields": "OBJECTID ASC", "f": "json"}
        if len(_query_url(**params)) >= MAX_OBJECT_ID_URL_LENGTH:
            if not current:
                raise ValueError("single_object_id_exceeds_get_url_limit")
            chunks.append(current)
            current = [object_id]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _page_plan(ids: list[int], layer: Mapping[str, object]) -> tuple[str, list[tuple[str, str, list[int]]]]:
    """Plan deterministic source pages with expected ordered IDs."""
    ordered_ids = sorted(ids)
    pages: list[tuple[str, str, list[int]]] = []
    if _supports_ordered_pagination(layer):
        mode = "ordered_offset"
        for offset in range(0, len(ordered_ids), PAGE_SIZE):
            page_ids = ordered_ids[offset:offset + PAGE_SIZE]
            url = _query_url(where="1=1", outFields="*", returnGeometry="true",
                returnZ="true", orderByFields="OBJECTID ASC",
                resultOffset=offset, resultRecordCount=PAGE_SIZE, f="json")
            if len(url) >= MAX_OBJECT_ID_URL_LENGTH:
                raise ValueError("pagination_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    else:
        mode = "bounded_object_ids"
        for page_ids in _object_id_chunks(ordered_ids):
            params = {"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                      "returnGeometry": "true", "returnZ": "true",
                      "orderByFields": "OBJECTID ASC", "f": "json"}
            url = _query_url(**params)
            if len(url) >= MAX_OBJECT_ID_URL_LENGTH:
                raise ValueError("object_id_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    if not pages:
        raise ValueError("source_page_plan_empty")
    if len(pages) > MAX_CAPTURE_PAGES:
        raise ValueError("source_page_count_exceeds_capture_limit")
    return mode, pages


def _read_capture_body(capture_root: Path, name: str) -> bytes:
    """Read one retained capture body below the per-response bound."""
    body = (capture_root / f"{name}.body").read_bytes()
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("capture_artifact_type_or_size_limit")
    return body


def _validate_before_page_requests(capture_root: Path,
                                   receipts: dict[str, dict[str, object]]) -> tuple[list[int], dict]:
    """Validate all source pins available before requesting any feature page."""
    docs = {name: json.loads(_read_capture_body(capture_root, name)) for name in receipts}
    seed_doc = docs["ids_seed"]
    seed = seed_doc.get("objectIds")
    if (seed_doc.get("objectIdFieldName") != "OBJECTID" or not isinstance(seed, list)
            or len(seed) != EXPECTED_SOURCE_COUNT or any(type(value) is not int for value in seed)):
        raise ValueError("source_seed_id_roster_missing_or_invalid")
    seed_ids = sorted(seed)
    if (len(set(seed_ids)) != EXPECTED_SOURCE_COUNT
            or _sha(_canonical(seed_ids)) != EXPECTED_SOURCE_ID_SHA256):
        raise ValueError("source_seed_id_roster_drift")

    count_doc = docs["count_before"]
    ids_doc = docs["ids_before"]
    ids = ids_doc.get("objectIds")
    if (type(count_doc.get("count")) is not int or count_doc["count"] != EXPECTED_SOURCE_COUNT
            or ids_doc.get("objectIdFieldName") != "OBJECTID" or not isinstance(ids, list)
            or len(ids) != EXPECTED_SOURCE_COUNT or any(type(value) is not int for value in ids)):
        raise ValueError("source_count_or_id_roster_drift")
    if (len(set(ids)) != EXPECTED_SOURCE_COUNT or sorted(ids) != seed_ids
            or _sha(_canonical(sorted(ids))) != EXPECTED_SOURCE_ID_SHA256):
        raise ValueError("source_id_roster_drift")

    layer, service, portal = docs["layer_before"], docs["service_before"], docs["portal_item"]
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_field_roster_missing")
    plain_fields = [_plain_field(field) for field in fields]
    if (_sha(_canonical(plain_fields)) != EXPECTED_SOURCE_SCHEMA_SHA256
            or [field.get("name") for field in fields] != EXPECTED_FIELD_NAMES
            or [field.get("type") for field in fields] != EXPECTED_FIELD_TYPES):
        raise ValueError("source_schema_drift")
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "USDOT_BTS"
            or portal.get("title") != LAYER_NAME or portal.get("url") != SERVICE_URL
            or portal.get("access") != "public"
            or portal.get("licenseInfo") != "Creative Commons Attribution 3.0 United States (CC-BY-3.0)"):
        raise ValueError("source_item_identity_or_access_drift")
    desc = str(portal.get("description", ""))
    if ("Bureau of Transportation Statistics (BTS)" not in desc
            or "National Transportation Atlas Database (NTAD)" not in desc
            or "National Transit Map" not in desc or "September 23, 2026" not in desc):
        raise ValueError("official_BTS_NTAD_relationship_or_vintage_drift")
    if (service.get("serviceItemId") != ITEM_ID
            or not any(isinstance(x, dict) and x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME
                       for x in service.get("layers", []))
            or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not True
            or layer.get("hasM") is not False
            or layer.get("extent", {}).get("spatialReference", {}).get("latestWkid") != 4326):
        raise ValueError("source_layer_identity_or_crs_drift")
    return seed_ids, layer


def _validate_page_ids(body: bytes, expected_ids: list[int]) -> None:
    page = json.loads(body)
    if "error" in page:
        raise ValueError("source_page_error_or_truncated")
    rows = page.get("features")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("source_page_missing_features")
    page_ids = []
    for row in rows:
        attrs = row.get("attributes")
        if not isinstance(attrs, dict) or type(attrs.get("OBJECTID")) is not int:
            raise ValueError("source_page_object_id_type_drift")
        page_ids.append(attrs["OBJECTID"])
    if page_ids != expected_ids:
        raise ValueError("source_page_ids_do_not_match_requested_slice")


class _StreamingCanonicalListDigest:
    """Incrementally digest json.dumps(list, ...) without materializing the list.

    Produces exactly sha256(_canonical(elements)) for any element sequence: the
    list serialization is '[' + ','.join(element serializations) + ']', and
    json.dumps applies the same options recursively when dumping the whole list.
    """

    def __init__(self) -> None:
        self._hash = hashlib.sha256()
        self._hash.update(b"[")
        self._count = 0

    def add(self, element: object) -> None:
        if self._count:
            self._hash.update(b",")
        self._hash.update(_canonical(element))
        self._count += 1

    def hexdigest(self) -> str:
        closing = self._hash.copy()
        closing.update(b"]")
        return closing.hexdigest()


def _bounded_regular_file(path: Path, limit: int, *, dir_fd: int | None = None) -> bytes:
    """Read at most limit bytes from a no-follow regular file."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path.name if dir_fd is not None else path, flags, dir_fd=dir_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError("capture_artifact_type_or_size_limit")
        chunks, remaining = [], limit
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        value = b"".join(chunks)
        if len(value) > limit or os.fstat(fd).st_size > limit:
            raise ValueError("capture_artifact_type_or_size_limit")
        return value
    finally:
        os.close(fd)


def _bounded_failure_evidence(capture_root: Path, captured_names: set[str],
                              error_code: str) -> tuple[dict[str, tuple[bytes, bytes, dict]], list[str]]:
    """Collect bounded partial artifacts; malformed/oversized files are named, not read."""
    by_name: dict[str, set[str]] = {}
    allowed = re.compile(r"(?:ids_seed|portal_item|service_before|layer_before|count_before|ids_before|"
                         r"portal_item_after|layer_after|service_after|count_after|ids_after|page_\d{3,4})")
    root_fd = None
    try:
        root_fd = os.open(capture_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                          | getattr(os, "O_NOFOLLOW", 0))
        with os.scandir(root_fd) as entries:
            for index, entry in enumerate(entries):
                if index >= MAX_CAPTURE_RESPONSES * 3:
                    raise ValueError("capture_artifact_file_count_limit")
                match = re.fullmatch(r"(.+)\.(body|wire|receipt\.json)", entry.name)
                if match and allowed.fullmatch(match.group(1)):
                    by_name.setdefault(match.group(1), set()).add(match.group(2))
    except (FileNotFoundError, NotADirectoryError):
        if root_fd is not None:
            os.close(root_fd)
        return {}, ["capture_root_missing_or_unsafe"]
    except ValueError:
        if root_fd is not None:
            os.close(root_fd)
        return {}, ["capture_artifact_file_count_limit"]
    except OSError:
        if root_fd is not None:
            os.close(root_fd)
        return {}, ["capture_root_missing_or_unsafe"]

    result: dict[str, tuple[bytes, bytes, dict]] = {}
    omitted: list[str] = []
    total_body_wire = 0
    total_receipts = 0
    for name in sorted(set(by_name) | set(captured_names)):
        if len(result) >= MAX_CAPTURE_RESPONSES:
            omitted.append("response_count_limit")
            break
        paths = {suffix: capture_root / f"{name}.{suffix}" for suffix in ("body", "wire", "receipt.json")}
        if by_name.get(name, set()) != {"body", "wire", "receipt.json"}:
            omitted.append(name)
            continue
        body: bytes = b""
        wire: bytes = b""
        receipt: dict = {}
        try:
            receipt_remaining = MAX_FAILURE_RECEIPTS_TOTAL_BYTES - total_receipts
            if receipt_remaining <= 0:
                raise ValueError("capture_receipt_total_size_limit")
            raw = _bounded_regular_file(paths["receipt.json"],
                                        min(MAX_FAILURE_RECEIPT_BYTES, receipt_remaining), dir_fd=root_fd)
            total_receipts += len(raw)
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                receipt = parsed
            remaining_total = MAX_CAPTURE_TOTAL_BYTES - total_body_wire
            body = _bounded_regular_file(paths["body"], min(MAX_BODY_BYTES, remaining_total), dir_fd=root_fd)
            remaining_total -= len(body)
            if remaining_total < 0:
                raise ValueError("capture_artifact_total_size_limit")
            wire = _bounded_regular_file(paths["wire"], min(MAX_BODY_BYTES, remaining_total), dir_fd=root_fd)
            next_total = total_body_wire + len(body) + len(wire)
            if len(body) > MAX_BODY_BYTES or len(wire) > MAX_BODY_BYTES or next_total > MAX_CAPTURE_TOTAL_BYTES:
                raise ValueError("capture_artifact_total_size_limit")
            total_body_wire = next_total
        except (OSError, ValueError, json.JSONDecodeError):
            omitted.append(name)
            continue
        if not receipt:
            receipt = {"method":"GET","url":None,"final_url":None,
                "observed_at":datetime.now(timezone.utc).isoformat(),"status":None,
                "complete_body":False,"wire_bytes":len(wire),"wire_sha256":_sha(wire),
                "bytes":len(body),"sha256":_sha(body),"content_encoding":None,
                "content_length":None,"error_code":error_code}
        result[name] = (body, wire, receipt)
    if root_fd is not None:
        os.close(root_fd)
    return result, omitted


def _quarantine_failure_evidence(storage: StagingStorageResource, dataset_slug: str,
                                 captures: dict[str, tuple[bytes, bytes, dict]],
                                 error_code: str, omitted: list[str]) -> str:
    token = uuid4().hex
    base = f"_quarantine/{dataset_slug}/{token}"
    for name, (body, wire, receipt) in sorted(captures.items()):
        write_immutable(storage, f"{base}/raw/{name}.body", body)
        write_immutable(storage, f"{base}/raw/{name}.wire", wire)
        write_immutable(storage, f"{base}/raw/{name}.receipt.json", _canonical(receipt) + b"\n")
    write_immutable(storage, f"{base}/metadata/failure.json", _canonical({
        "reason_code":error_code,"captured_names":sorted(captures),"omitted_artifacts":omitted,
        "evidence_complete":not omitted})+b"\n")
    return base


def _candidate_schema(fields: list[dict]) -> pa.Schema:
    """Native Z-preserving GeoParquet schema for the sixteen source fields."""
    schema_fields = [pa.field(field["name"], _arrow_type(field),
                              nullable=bool(field.get("nullable", True))) for field in fields]
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["MultiLineString Z"],
        "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar",
    }}}
    return pa.schema(schema_fields, metadata={b"geo":_canonical(geo)})


def _page_rows_table(schema: pa.Schema, fields: list[dict], rows: list[dict],
                     geometries: list[bytes | None]) -> pa.Table:
    arrays = {}
    for field in fields:
        name = field["name"]
        arrays[name] = pa.array([row["attributes"][name] for row in rows], type=_arrow_type(field))
    arrays["geometry"] = pa.array(geometries, type=pa.binary())
    return pa.Table.from_pydict(arrays, schema=schema)


def _build_candidate(capture_root: Path, receipts: dict[str, dict[str, object]]
                     ) -> tuple[list[dict], dict, bytes]:
    """Validate every retained response and stream the native Z-preserving parquet.

    Page bodies are re-read from the capture directory one at a time so a full
    capture (~1.7 GiB of decoded pages) never has to fit in memory. The record
    and geometry digests are computed incrementally and equal the sha256 of the
    fully materialized canonical lists.
    """
    def doc(name: str) -> dict:
        return json.loads(_read_capture_body(capture_root, name))

    layer_before = doc("layer_before")
    fields = layer_before.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    plain_fields = [_plain_field(f) for f in fields]
    schema_sha = _sha(_canonical(plain_fields))
    if schema_sha != EXPECTED_SOURCE_SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    portal = doc("portal_item")
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "USDOT_BTS"
            or portal.get("title") != LAYER_NAME
            or portal.get("url") != SERVICE_URL or portal.get("access") != "public"
            or portal.get("licenseInfo") != "Creative Commons Attribution 3.0 United States (CC-BY-3.0)"):
        raise ValueError("source_item_identity_or_access_drift")
    desc = str(portal.get("description", ""))
    if ("Bureau of Transportation Statistics (BTS)" not in desc
            or "National Transportation Atlas Database (NTAD)" not in desc
            or "National Transit Map" not in desc or "September 23, 2026" not in desc):
        raise ValueError("official_BTS_NTAD_relationship_or_vintage_drift")
    portal_after = doc("portal_item_after")
    if any(portal.get(key) != portal_after.get(key) for key in (
            "id", "owner", "title", "url", "access", "licenseInfo", "modified", "description")):
        raise ValueError("source_item_changed_during_capture")
    service_before = doc("service_before")
    if (service_before.get("serviceItemId") != ITEM_ID
            or not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME for x in service_before.get("layers", []) if isinstance(x, dict))
            or layer_before.get("name") != LAYER_NAME or layer_before.get("geometryType") != "esriGeometryPolyline"
            or layer_before.get("objectIdField") != "OBJECTID" or layer_before.get("hasZ") is not True
            or layer_before.get("hasM") is not False):
        raise ValueError("exact_BTS_air_to_truck_layer_identity_drift")
    if layer_before.get("extent", {}).get("spatialReference", {}).get("latestWkid") != 4326:
        raise ValueError("source_native_crs_drift")
    if (len(fields) != len(EXPECTED_FIELD_NAMES)
            or [f.get("name") for f in fields] != EXPECTED_FIELD_NAMES
            or [f.get("type") for f in fields] != EXPECTED_FIELD_TYPES):
        raise ValueError("source_schema_roster_drift")
    service_after = doc("service_after")
    layer_after = doc("layer_after")
    if (service_before.get("serviceItemId") != service_after.get("serviceItemId")
            or service_before.get("layers") != service_after.get("layers")
            or service_before.get("serviceDescription") != service_after.get("serviceDescription")
            or layer_before.get("name") != layer_after.get("name")
            or layer_before.get("geometryType") != layer_after.get("geometryType")
            or layer_before.get("objectIdField") != layer_after.get("objectIdField")
            or layer_before.get("spatialReference") != layer_after.get("spatialReference")
            or layer_before.get("extent") != layer_after.get("extent")
            or layer_before.get("editingInfo") != layer_after.get("editingInfo")):
        raise ValueError("source_metadata_changed_during_capture")
    if [_plain_field(f) for f in layer_after.get("fields", [])] != plain_fields:
        raise ValueError("source_field_schema_changed_during_capture")
    count_before, count_after = doc("count_before"), doc("count_after")
    ids_before, ids_after = doc("ids_before"), doc("ids_after")
    counts = (count_before.get("count"), count_after.get("count"))
    ids = (ids_before.get("objectIds"), ids_after.get("objectIds"))
    if (type(counts[0]) is not int or counts[0] != EXPECTED_SOURCE_COUNT
            or type(counts[1]) is not int or counts[1] != counts[0]
            or not isinstance(ids[0], list) or not isinstance(ids[1], list)
            or len(ids[0]) != counts[0] or len(ids[1]) != counts[0]
            or any(type(value) is not int for value in ids[0] + ids[1])):
        raise ValueError("source_count_or_id_roster_drift")
    got_ids = sorted(ids[0])
    if (len(set(got_ids)) != counts[0] or sorted(ids[1]) != got_ids
            or ids_before.get("objectIdFieldName") != "OBJECTID"
            or ids_after.get("objectIdFieldName") != "OBJECTID"):
        raise ValueError("source_id_roster_duplicate")
    if _sha(_canonical(got_ids)) != EXPECTED_SOURCE_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    seed_doc = doc("ids_seed")
    seed = seed_doc.get("objectIds")
    if (not isinstance(seed, list) or any(type(value) is not int for value in seed)
            or seed_doc.get("objectIdFieldName") != "OBJECTID"
            or len(seed) != counts[0] or len(set(seed)) != counts[0] or sorted(seed) != got_ids):
        raise ValueError("source_id_roster_changed_before_capture")
    page_names = sorted(name for name in receipts if name.startswith("page_"))
    pagination_mode, page_plan = _page_plan(got_ids, layer_before)
    expected_pages = [name for name, _url, _ids in page_plan]
    if page_names != expected_pages:
        raise ValueError("source_page_roster_incomplete")
    record_digest = _StreamingCanonicalListDigest()
    geometry_digest = _StreamingCanonicalListDigest()
    schema = _candidate_schema(fields)
    field_names = [f["name"] for f in fields]
    parquet_sink = pa.BufferOutputStream()
    writer = pq.ParquetWriter(parquet_sink, schema, compression="zstd")
    feature_count = 0
    null_geometry_count = 0
    invalid_geometry_count = 0
    z_point_count = 0
    nonzero_z_point_count = 0
    try:
        for page_index, (name, expected_url, expected_page_ids) in enumerate(page_plan):
            receipt = receipts[name]
            if receipt.get("url") != expected_url:
                raise ValueError("source_page_query_plan_drift")
            if pagination_mode == "bounded_object_ids" and len(expected_url) >= MAX_OBJECT_ID_URL_LENGTH:
                raise ValueError("source_page_get_url_exceeds_safe_limit")
            page = doc(name)
            if "error" in page:
                raise ValueError("source_page_error_or_truncated")
            # The BTS layer sets exceededTransferLimit on a full offset page when
            # additional ordered rows remain. The independently pinned ID roster
            # and exact requested slice prove page completeness; a partial or
            # unexpectedly flagged final page still fails closed.
            if (page.get("exceededTransferLimit") is True
                    and (pagination_mode != "ordered_offset" or len(expected_page_ids) != PAGE_SIZE
                         or page_index == len(page_plan) - 1)):
                raise ValueError("source_page_error_or_truncated")
            if page.get("spatialReference", {}).get("latestWkid") != 4326:
                raise ValueError("source_page_crs_drift")
            if "hasZ" in page and page.get("hasZ") is not True:
                raise ValueError("source_page_dimension_drift")
            rows = page.get("features")
            if not isinstance(rows, list):
                raise ValueError("source_page_missing_features")
            page_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows
                        if isinstance(row, dict) and isinstance(row.get("attributes"), dict)]
            if page_ids != expected_page_ids:
                raise ValueError("source_page_ids_do_not_match_requested_slice")
            wkb_geometries: list[bytes | None] = []
            for row in rows:
                attrs, geom = row.get("attributes"), row.get("geometry")
                if not isinstance(attrs, dict) or set(attrs) != set(field_names):
                    raise ValueError("source_attribute_roster_drift")
                for field in fields:
                    value = attrs[field["name"]]
                    if value is None:
                        if field.get("nullable") is False:
                            raise ValueError("source_nonnullable_attribute_is_null")
                        continue
                    kind = field["type"]
                    if kind in ("esriFieldTypeOID", "esriFieldTypeInteger"):
                        valid = type(value) is int
                    elif kind == "esriFieldTypeDouble":
                        valid = type(value) in (int, float) and math.isfinite(value)
                    elif kind == "esriFieldTypeString":
                        valid = isinstance(value, str)
                    else:
                        raise ValueError("unsupported_source_field_type")
                    if not valid:
                        raise ValueError("source_attribute_value_type_drift")
                record_digest.add(row)
                feature_count += 1
                if geom is None:
                    null_geometry_count += 1
                    geometry_digest.add([attrs["OBJECTID"], None])
                    wkb_geometries.append(None)
                    continue
                if not isinstance(geom, dict) or set(geom) != {"paths", "hasZ"}:
                    raise ValueError("source_polyline_geometry_roster_drift")
                if geom.get("hasZ") is not True:
                    raise ValueError("source_polyline_geometry_dimension_drift")
                paths = geom.get("paths")
                if not isinstance(paths, list) or not paths:
                    raise ValueError("source_polyline_paths_missing")
                normalized_paths = []
                for path in paths:
                    if not isinstance(path, list) or len(path) < 2:
                        raise ValueError("source_polyline_path_invalid")
                    points = []
                    for point in path:
                        if (not isinstance(point, list) or len(point) != 3
                                or any(type(coord) not in (int, float) for coord in point)):
                            raise ValueError("source_polyline_coordinate_invalid")
                        x, y, z = point
                        if (not math.isfinite(x) or not math.isfinite(y) or not math.isfinite(z)
                                or not (-180 <= x <= 180 and -90 <= y <= 90)):
                            invalid_geometry_count += 1
                        z_point_count += 1
                        if z != 0:
                            nonzero_z_point_count += 1
                        points.append([x, y, z])
                    normalized_paths.append(points)
                geometry_digest.add([attrs["OBJECTID"], normalized_paths])
                wkb_geometries.append(MultiLineString(normalized_paths).wkb)
            writer.write_table(_page_rows_table(schema, fields, rows, wkb_geometries))
            del page, rows, wkb_geometries
        writer.close()
    except BaseException:
        writer.close()
        raise
    if feature_count != counts[0]:
        raise ValueError("source_feature_population_incomplete")
    audit = {
        "source_feature_count": feature_count, "source_id_sha256": _sha(_canonical(got_ids)),
        "source_population_complete": True,
        "source_record_sha256": record_digest.hexdigest(),
        "source_geometry_sha256": geometry_digest.hexdigest(), "source_schema_sha256": schema_sha,
        "source_fields": plain_fields, "source_crs_wkid": 4326,
        "source_geometry_type": "esriGeometryPolyline", "source_geometry_has_z": True,
        "source_z_point_count": z_point_count,
        "source_nonzero_z_point_count": nonzero_z_point_count,
        "source_null_geometry_count": null_geometry_count,
        "source_invalid_geometry_count": invalid_geometry_count,
        "pagination_mode":pagination_mode,"page_count":len(page_plan),"page_size":PAGE_SIZE,
        "proposed_physical_field_mappings":[{"field":field["name"],"source_type":field["type"],
            "proposed_arrow_type":str(_arrow_type(field)),"approved":False} for field in plain_fields],
    }
    return plain_fields, audit, parquet_sink.getvalue().to_pybytes()


def _code_identity() -> dict:
    return processing_identity(Path(__file__).parents[3])


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                            version: str, fingerprint: str, identity: Mapping[str, object],
                            audit: Mapping[str, object], expected_names: set[str]) -> dict:
    import json as _json
    bundle = _json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
        identity, audit["source_record_sha256"], audit["source_schema_sha256"], payload["objects"], payload["capture_receipts"])
    if set(payload["objects"]) != expected_names:
        raise ValueError("cached_candidate_roster_changed")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
        "feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,
        "baseline_accepted":False,"archive_equivalence_accepted":False,
        "mapping_approved":False,"release_accepted":False}


def capture_national_transit_map_routes(storage: StagingStorageResource, *,
                                        capture_directory: str | None = None) -> dict:
    """Acquire, validate and stage a complete unapproved BTS candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < DISK_FLOOR_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    capture_base_value = capture_directory or os.environ.get("HIFLD_NTM_ROUTES_CAPTURE_DIR")
    if not capture_base_value:
        raise ValueError("explicit_capture_directory_required")
    capture_base = Path(capture_base_value)
    capture_token = uuid4().hex
    capture_root = (capture_base / f"capture-{capture_token}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, dict] = {}
    total_captured_bytes = 0

    def capture_one(client, name: str, url: str) -> bytes:
        nonlocal total_captured_bytes

        def reserve(progress: CaptureProgress) -> None:
            reserved_total = (total_captured_bytes + progress.wire_bytes_retained
                              + progress.decoded_bytes_retained + progress.reserve_bytes)
            if reserved_total > MAX_CAPTURE_TOTAL_BYTES:
                raise ValueError("source_capture_exceeds_scratch_budget")
            if shutil.disk_usage(capture_root).free - progress.reserve_bytes < DISK_FLOOR_BYTES:
                raise ValueError("capture_disk_floor_during_acquisition")
            # Source raw evidence is also copied into immutable staging. Reserve
            # that second copy before accepting another response block.
            if (shutil.disk_usage(base).free - reserved_total < DISK_FLOOR_BYTES):
                raise ValueError("staging_disk_floor_during_acquisition")

        body, receipt = capture_get(client, url, capture_root, name,
                                    max_bytes=MAX_BODY_BYTES, reserve=reserve)
        total_captured_bytes += receipt["bytes"] + receipt["wire_bytes"]
        if total_captured_bytes > MAX_CAPTURE_TOTAL_BYTES:
            raise ValueError("source_capture_exceeds_scratch_budget")
        receipts[name] = receipt
        return body

    def capture_with_retry(client, name: str, url: str) -> bytes:
        for attempt in range(PAGE_ATTEMPTS):
            if attempt:
                time.sleep(PAGE_RETRY_BACKOFF_SECONDS[min(attempt - 1, len(PAGE_RETRY_BACKOFF_SECONDS) - 1)])
            try:
                return capture_one(client, name, url)
            except ValueError as exc:
                if attempt == PAGE_ATTEMPTS - 1 or not str(exc).startswith("incomplete_source_capture:"):
                    raise
        raise AssertionError("unreachable_retry_state")

    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/national-transit-map-routes-draft"}) as client:
            # Response bodies are validated as they arrive and then dropped: the
            # durable copy on disk is the only retained form, so a full capture
            # never accumulates in memory.
            seed_body = capture_with_retry(client, "ids_seed",
                                           _query_url(where="1=1", returnIdsOnly="true", f="json"))
            seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or not seed or any(type(value) is not int for value in seed):
                raise ValueError("source_seed_id_roster_missing_or_invalid")
            del seed_body
            for name, url in _source_metadata_urls():
                capture_with_retry(client, name, url)
            pinned_ids, layer_before = _validate_before_page_requests(capture_root, receipts)
            _pagination_mode, page_plan = _page_plan(pinned_ids, layer_before)
            for name, url, expected_page_ids in page_plan:
                page_body = capture_with_retry(client, name, url)
                _validate_page_ids(page_body, expected_page_ids)
                del page_body
            for name, url in _source_after_urls():
                capture_with_retry(client, name, url)
        _plain_fields, audit, parquet = _build_candidate(capture_root, receipts)
    except Exception as exc:
        error_code = str(exc) if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum() else type(exc).__name__
        bounded, omitted = _bounded_failure_evidence(capture_root, set(receipts), error_code)
        try:
            (capture_root/"capture_manifest.json").write_bytes(_canonical({
                "captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,
                "error_code":error_code,"receipts":{name:receipt for name,(_,_,receipt) in sorted(bounded.items())},
                "omitted_artifacts":omitted,"evidence_complete":not omitted})+b"\n")
        except OSError:
            omitted.append("capture_manifest_write_failed")
        if bounded:
            _quarantine_failure_evidence(storage, DATASET_SLUG, bounded, error_code, omitted)
        raise
    identity = _code_identity()
    native_path = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {native_path:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    manifest = {
        "schema_version":1,"approved":False,"title":LAYER_NAME,
        "description":"Current BTS NTAD National Transit Map Routes line layer, described as compiled 2026-09-23. The hosted layer declares hasZ=true; page queries send returnZ=true and the native Z coordinate is preserved. DataLumos V1 is an older 2024 vintage and archive equivalence is unverified; schema, ID, freshness, rights and release approvals remain pending.",
        "publisher":PUBLISHER,"agency":"Department of Transportation","source_url":LAYER_URL,
        "license":LICENSE,"source_modified":None,"date_issued":None,
        "date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,
            "role":"producer_publication","canonical_url":f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json" if name.startswith("portal") else LAYER_URL,
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],
            "http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "license":LICENSE,"attribution":"BTS NTAD; item metadata states CC-BY-3.0; rights review remains pending.",
            "release_date":None,"release_date_unknown_reason":"ArcGIS modified timestamp is not a source release date."}
            for name,receipt in sorted(receipts.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
            "feature_layer_id":LAYER_ID,"source_fields":"all sixteen declared source fields retained with declared source types, including producer-owned Shape__Length",
            "source_ids":"OBJECTID preserved unchanged; before, seed and after source ID rosters are reconciled","geometry":"native polyline paths requested with returnZ=true and encoded as WKB MultiLineString Z at EPSG:4326, preserving the producer's native Z coordinate without reprojection, simplification or repair",
            "physical_mapping":"Per-field source-to-Arrow proposals are recorded in source_audit.json; OID widening to int64 preserves values, and all mapping approvals remain false.",
            "transport":"Bounded transient retry of incomplete transport responses (at most 4 attempts per request with growing backoff); every accepted response is a complete hash-pinned producer body.",
            "candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, archive equivalence, baseline, rights and mapping acceptance"}}],
        "lineage":f"USDOT_BTS official NTAD account item {ITEM_ID} exposes FeatureServer layer 0. The current item describes a 2026-09-23 compilation. HIFLD inventory references DataLumos project 239727 V1 (2024 vintage); archive equivalence is not assumed.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri fields (including OBJECTID and Shape__Length), source IDs, polyline paths with native Z coordinates and coordinates retained; approvals pending.","objects":[{"path":native_path,**source_objects[native_path]}]}}},
        "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"official_layer_name":LAYER_NAME,"arcgis_item_id":ITEM_ID,"source_relationship":"BTS-published NTAD National Transit Map Routes layer",
        "physical_mapping_approved":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "baseline_accepted":False,"archive_equivalence_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    version = content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix = candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,
        "source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],
        "receipts":dict(sorted(receipts.items()))})+b"\n")
    metadata_outputs = {"metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "baseline_accepted":False,"archive_equivalence_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    candidate_ready = _canonical(ready)+b"\n"
    output_names = ({native_path, "metadata/candidate_ready.json", *metadata_outputs}
                    | {f"raw/{name}.{suffix}" for name in receipts for suffix in ("body","wire","receipt.json")})
    planned_bytes = (len(parquet) + len(candidate_ready) + sum(map(len, metadata_outputs.values()))
                     + sum(receipt["bytes"] + receipt["wire_bytes"] for receipt in receipts.values()))
    if shutil.disk_usage(base).free - planned_bytes < DISK_FLOOR_BYTES:
        raise ValueError("staging_disk_floor_before_candidate_write")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage,prefix,state_key,version,fingerprint,identity,audit,output_names)
    write_immutable(storage,f"{prefix}/{native_path}",parquet)
    del parquet
    for name,body in metadata_outputs.items():
        write_immutable(storage,f"{prefix}/{name}",body)
    for name,receipt in sorted(receipts.items()):
        write_immutable(storage,f"{prefix}/raw/{name}.body",_read_capture_body(capture_root,name))
        write_immutable(storage,f"{prefix}/raw/{name}.wire",(capture_root/f"{name}.wire").read_bytes())
        write_immutable(storage,f"{prefix}/raw/{name}.receipt.json",_canonical(receipt)+b"\n")
    write_immutable(storage,f"{prefix}/metadata/candidate_ready.json",candidate_ready)
    descriptors = {native_path:dict(source_objects[native_path])}
    for name,body in metadata_outputs.items():
        descriptors[name] = {"bytes":len(body),"sha256":_sha(body)}
    descriptors["metadata/candidate_ready.json"] = {"bytes":len(candidate_ready),"sha256":_sha(candidate_ready)}
    for name,receipt in receipts.items():
        receipt_body = _canonical(receipt)+b"\n"
        descriptors[f"raw/{name}.body"] = {"bytes":receipt["bytes"],"sha256":receipt["sha256"]}
        descriptors[f"raw/{name}.wire"] = {"bytes":receipt["wire_bytes"],"sha256":receipt["wire_sha256"]}
        descriptors[f"raw/{name}.receipt.json"] = {"bytes":len(receipt_body),"sha256":_sha(receipt_body)}
    for name,d in descriptors.items():
        key=f"{prefix}/{name}"; snap=storage.object_snapshot(key)
        if snap is None or snap.size != d["bytes"] or storage.sha256_key(key) != d["sha256"] or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":dict(receipts),"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],
        "source_z_point_count":audit["source_z_point_count"],
        "source_nonzero_z_point_count":audit["source_nonzero_z_point_count"],
        "candidate_ready":prefix+"/metadata/candidate_ready.json",
        "capture_directory":str(capture_root),"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"baseline_accepted":False,
        "archive_equivalence_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
