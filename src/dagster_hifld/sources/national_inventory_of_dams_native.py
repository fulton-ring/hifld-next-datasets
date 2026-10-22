"""Bounded, source-faithful USACE NID FileGDB capture and GeoParquet draft.

The earlier Esri JSON adapter remains available in
``national_inventory_of_dams`` for audit/history. This adapter uses the
producer's advertised Extract/fileGDB operation and preserves the layer's
native Point Z WKB, source OID, and all declared attributes. It does not claim
historical DataLumos equivalence or accept rights, schema, ID, or release.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import stat
import struct
import tempfile
import time
import zipfile
import zlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.http_capture import CaptureProgress, DECODE_BLOCK_BYTES
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import national_inventory_of_dams as legacy
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction

DATASET_SLUG = FILE_SLUG = legacy.DATASET_SLUG
SERVICE_URL = legacy.SERVICE_URL
LAYER_URL = legacy.LAYER_URL
LAYER_NAME = legacy.LAYER_NAME
PUBLISHER = legacy.PUBLISHER
EXPORT_URL = SERVICE_URL + "/createReplica"
MAX_NATIVE_RESPONSE_BYTES = 12_000_000
MAX_NATIVE_BLOCK_BYTES = DECODE_BLOCK_BYTES
MAX_NATIVE_CAPTURE_BYTES = 32_000_000
MAX_NATIVE_MANIFEST_BYTES = 2_000_000
MAX_NATIVE_RECEIPT_BYTES = 64_000
MAX_NATIVE_ZIP_MEMBERS = 256
MAX_NATIVE_ZIP_CRC_CHUNK_BYTES = 1_000_000
MAX_NATIVE_EXPANDED_BYTES = 80_000_000
MAX_NATIVE_MEMBER_BYTES = 70_000_000
MAX_NATIVE_BATCH_ROWS = 256
MAX_NATIVE_BATCH_BYTES = 8_000_000
MAX_NATIVE_CANDIDATE_BYTES = 80_000_000
MAX_NATIVE_TOTAL_STAGE_BYTES = 48_000_000
MIN_DISK_FREE_BYTES = 400_000_000
MAX_REPLICA_POLLS = 12
REPLICA_POLL_INTERVAL_SECONDS = 5
NATIVE_LAYER_NAME = "V_DAMS"
NATIVE_FID_NAME = "OBJECTID"
NATIVE_GEOMETRY_NAME = "SHAPE"

_OGR_TYPES = {
    "esriFieldTypeString": "OFTString",
    "esriFieldTypeInteger": "OFTInteger",
    "esriFieldTypeDouble": "OFTReal",
    "esriFieldTypeDate": "OFTDateTime",
    "esriFieldTypeBlob": "OFTBinary",
}
_ARROW_TYPES = {
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeInteger": pa.int32(),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeDate": pa.timestamp("ms"),
    "esriFieldTypeBlob": pa.binary(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _safe_official_url(url: str) -> str:
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "geospatial.sec.usace.army.mil"
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError("usace_nid_untrusted_export_url")
    return url


def _url_without_query(url: str) -> str:
    parsed = urlsplit(url)
    return parsed._replace(query="", fragment="").geturl()


def _ids_url() -> str:
    return legacy._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})


def _count_url() -> str:
    return legacy._query({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _expected_field_names() -> list[str]:
    return legacy.EXPECTED_FIELD_NAMES


def _expected_attribute_names() -> list[str]:
    return [name for name in _expected_field_names() if name != NATIVE_FID_NAME]


def _safe_capture_entries(root: Path, *, allow_manifest: bool = False) -> list[Path]:
    if not root.is_dir() or root.is_symlink():
        raise ValueError("usace_nid_native_capture_root_invalid")
    entries = list(root.iterdir())
    for entry in entries:
        info = entry.lstat()
        if entry.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("usace_nid_native_capture_path_unsafe")
        if entry.name == "native_capture_manifest.json" and not allow_manifest:
            continue
    return entries


def _capture_size(root: Path, current_name: str | None = None) -> int:
    total = 0
    for path in _safe_capture_entries(root):
        if current_name and path.name in {current_name + ".body", current_name + ".wire"}:
            continue
        total += path.stat().st_size
    return total


def _reserve_capture_block(root: Path, name: str, latest: list[CaptureProgress],
                           pending_bytes: int = 0) -> None:
    progress = latest[0] if latest else CaptureProgress("capture", 0, 0, 0, 0, 0)
    required = (_capture_size(root, name) + progress.wire_bytes_retained
                + progress.decoded_bytes_retained + progress.reserve_bytes
                + pending_bytes + MAX_NATIVE_RECEIPT_BYTES)
    if required > MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_capture_budget_exceeded")
    if shutil.disk_usage(root).free < MIN_DISK_FREE_BYTES + pending_bytes:
        raise ValueError("usace_nid_native_disk_floor_before_capture_write")


def _get(client: httpx.Client, root: Path, name: str, url: str) -> tuple[bytes, dict[str, object]]:
    _safe_official_url(url)
    client.cookies.clear()
    latest: list[CaptureProgress] = []

    def reserve(progress: CaptureProgress) -> None:
        latest[:] = [progress]
        _reserve_capture_block(root, name, latest)

    def before_retain(_kind: str, block: bytes) -> None:
        if len(block) > MAX_NATIVE_BLOCK_BYTES:
            raise ValueError("usace_nid_native_capture_block_exceeded")
        _reserve_capture_block(root, name, latest, len(block))

    try:
        body, receipt = capture_get(
            client, url, root, name,
            max_bytes=MAX_NATIVE_RESPONSE_BYTES,
            max_wire_block_bytes=MAX_NATIVE_BLOCK_BYTES,
            include_observed_wire=True,
            reserve=reserve,
            before_retain=before_retain,
        )
    finally:
        # httpx stores Set-Cookie responses in its client jar. Never send them
        # on a subsequent producer request.
        client.cookies.clear()
    if receipt.get("status") != 200 or receipt.get("complete_body") is not True:
        raise ValueError("usace_nid_native_incomplete_http_response")
    if _capture_size(root) > MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_capture_budget_exceeded")
    return body, receipt


def _capture_post(client: httpx.Client, root: Path, params: Mapping[str, object]) -> tuple[bytes, dict[str, object]]:
    name = "replica_create"
    payload = urlencode({k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
                        for k, v in params.items()}).encode("utf-8")
    _reserve_capture_block(root, name, [], len(payload))
    request_record = {
        "method": "POST", "url": EXPORT_URL, "parameters": dict(params),
        "body_sha256": _sha(payload), "body_bytes": len(payload),
        "authentication_sent": False, "cookies_sent": False,
    }
    request_path = root / "replica_create.request.json"
    request_path.write_bytes(_canonical(request_record) + b"\n")
    (root / "replica_create.request.body").write_bytes(payload)
    started = datetime.now(timezone.utc).isoformat()
    client.cookies.clear()
    try:
        with client.stream("POST", EXPORT_URL, data=params,
                           headers={"Content-Type": "application/x-www-form-urlencoded"}) as response:
            body = bytearray()
            for block in response.iter_raw():
                if len(block) > 64_000 or len(body) + len(block) > MAX_NATIVE_RECEIPT_BYTES:
                    raise ValueError("usace_nid_native_replica_response_oversized")
                _reserve_capture_block(root, name, [], len(block))
                body.extend(block)
            body_bytes = bytes(body)
            (root / "replica_create.body").write_bytes(body_bytes)
            headers = {k.lower(): v for k, v in response.headers.items()
                       if k.lower() not in {"set-cookie", "location"}}
            receipt = {
                "method": "POST", "url": EXPORT_URL, "observed_at": started,
                "status": response.status_code, "complete_body": True,
                "bytes": len(body_bytes), "sha256": _sha(body_bytes),
                "content_length": response.headers.get("content-length"),
                "headers": headers, "set_cookie_received": bool(response.headers.get("set-cookie")),
                "redirect_location_present": bool(response.headers.get("location")),
                "authentication_sent": False, "cookies_sent": False,
            }
    except Exception:
        client.cookies.clear()
        raise
    finally:
        client.cookies.clear()
    (root / "replica_create.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if receipt["status"] != 200:
        raise ValueError("usace_nid_native_replica_create_http_error")
    return body_bytes, receipt


def _source_docs(root: Path, names: tuple[str, ...]) -> dict[str, dict[str, object]]:
    docs: dict[str, dict[str, object]] = {}
    for name in names:
        body_path = root / f"{name}.body"
        body = legacy._bounded_capture_file(body_path, MAX_NATIVE_RESPONSE_BYTES)
        try:
            doc = legacy._strict_json(body)
        except Exception as exc:
            raise ValueError("usace_nid_native_source_json_invalid:" + name) from exc
        if not isinstance(doc, dict):
            raise ValueError("usace_nid_native_source_json_invalid:" + name)
        docs[name] = doc
    return docs


def _validate_metadata_and_rosters(docs: Mapping[str, Mapping[str, object]]) -> tuple[dict[str, object], list[int]]:
    service = docs["service_before"]
    layer = docs["layer_before"]
    layers = service.get("layers")
    capabilities_raw = service.get("capabilities", "")
    formats_raw = service.get("supportedExportFormats", "")
    capabilities = ({str(part).strip().lower() for part in capabilities_raw}
                    if isinstance(capabilities_raw, list)
                    else {part.strip().lower() for part in str(capabilities_raw).split(",")})
    formats = ({str(part).strip().lower().replace(" ", "") for part in formats_raw}
               if isinstance(formats_raw, list)
               else {part.strip().lower().replace(" ", "") for part in str(formats_raw).split(",")})
    if (not isinstance(layers, list) or not any(
            isinstance(item, dict) and item.get("id") == legacy.LAYER_ID
            and item.get("name") == legacy.LAYER_NAME and item.get("type") == "Feature Layer"
            for item in layers)
            or "extract" not in capabilities or "filegdb" not in formats):
        raise ValueError("usace_nid_native_extract_capability_drift")
    # syncEnabled=false is not a failure: this producer explicitly advertises
    # Extract/filegdb, which is the operation used here.
    if (layer.get("id") != legacy.LAYER_ID or layer.get("name") != legacy.LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != NATIVE_FID_NAME or layer.get("hasZ") is not True
            or layer.get("hasM") is not False or not legacy._is_native_4269(
                (layer.get("extent") or {}).get("spatialReference"))):
        raise ValueError("usace_nid_native_layer_identity_crs_or_dimensions_drift")
    fields = layer.get("fields")
    if (not isinstance(fields, list)
            or tuple(legacy._field_signature(field) for field in fields) != legacy.EXPECTED_FIELD_SIGNATURES):
        raise ValueError("usace_nid_native_layer_schema_drift")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("usace_nid_native_metadata_changed_during_capture")
    count = docs["count_before"].get("count")
    if type(count) is not int or count <= 0 or docs["count_after"].get("count") != count:
        raise ValueError("usace_nid_native_count_changed_or_invalid")
    before = docs["ids_before"].get("objectIds")
    after = docs["ids_after"].get("objectIds")
    if (not isinstance(before, list) or not isinstance(after, list)
            or len(before) != count or len(after) != count
            or any(type(v) is not int for v in before + after)
            or len(set(before)) != count or len(set(after)) != count
            or set(before) != set(after)):
        raise ValueError("usace_nid_native_oid_roster_changed_or_invalid")
    return dict(layer), sorted(before)


def _validate_native_reader(info: Mapping[str, object], schema: pa.Schema,
                            layer: Mapping[str, object], expected_count: int) -> None:
    expected_fields = [field for field in legacy.EXPECTED_FIELD_SIGNATURES if field[0] != NATIVE_FID_NAME]
    if (info.get("layer_name") != NATIVE_LAYER_NAME or info.get("features") != expected_count
            or info.get("crs") != "EPSG:4269" or info.get("geometry_type") != "Point Z"
            or info.get("fid_column") != NATIVE_FID_NAME
            or info.get("geometry_name") != NATIVE_GEOMETRY_NAME
            or not info.get("capabilities", {}).get("fast_feature_count", False)):
        raise ValueError("usace_nid_native_filegdb_metadata_drift")
    if list(info.get("fields", [])) != [item[0] for item in expected_fields]:
        raise ValueError("usace_nid_native_filegdb_attribute_roster_drift")
    bounds = info.get("total_bounds")
    extent = layer.get("extent", {})
    if (not isinstance(bounds, (tuple, list)) or len(bounds) != 4
            or any(type(value) not in (int, float) or not math.isfinite(value) for value in bounds)
            or any(abs(value - extent[key]) > 1e-4 for value, key in zip(
                bounds, ("xmin", "ymin", "xmax", "ymax")))):
        raise ValueError("usace_nid_native_filegdb_bounds_drift")
    expected_columns = [NATIVE_FID_NAME] + [item[0] for item in expected_fields] + [NATIVE_GEOMETRY_NAME]
    if schema.names != expected_columns:
        raise ValueError("usace_nid_native_filegdb_arrow_roster_drift")
    if schema.field(NATIVE_FID_NAME).type != pa.int64() or schema.field(NATIVE_FID_NAME).nullable:
        raise ValueError("usace_nid_native_filegdb_oid_type_drift")
    type_by_name = dict(zip(info.get("fields", []), info.get("ogr_types", [])))
    for name, esri_type, length in expected_fields:
        field = schema.field(name)
        if type_by_name.get(name) != _OGR_TYPES.get(esri_type) or field.type != _ARROW_TYPES.get(esri_type):
            raise ValueError("usace_nid_native_filegdb_field_type_drift:" + name)
        width = (field.metadata or {}).get(b"GDAL:OGR:width")
        if esri_type == "esriFieldTypeString" and width not in (None, str(length).encode()):
            raise ValueError("usace_nid_native_filegdb_field_width_drift:" + name)
    shape = schema.field(NATIVE_GEOMETRY_NAME)
    if not pa.types.is_binary(shape.type):
        raise ValueError("usace_nid_native_filegdb_wkb_type_drift")
    sr = layer.get("extent", {}).get("spatialReference", {})
    if not legacy._is_native_4269(sr):
        raise ValueError("usace_nid_native_crs_drift")


def _candidate_schema(source_schema: pa.Schema, extent: Mapping[str, object]) -> pa.Schema:
    output_fields = []
    for field in source_schema:
        if field.name == NATIVE_GEOMETRY_NAME:
            output_fields.append(pa.field("geometry", pa.binary(), nullable=field.nullable))
        else:
            output_fields.append(field)
    geo = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["Point Z"],
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
            "bbox": [extent[k] for k in ("xmin", "ymin", "xmax", "ymax")],
            "edges": "planar",
        }},
    }
    return pa.schema(output_fields, metadata={b"geo": _canonical(geo)})


def _parquet_from_native_batches(reader, expected_ids: list[int], extent: Mapping[str, object]) -> tuple[bytes, dict[str, object]]:
    schema = reader.schema
    output_schema = _candidate_schema(schema, extent)
    expected = set(expected_ids)
    seen: set[int] = set()
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    null_count = invalid_count = count = 0
    z_min: float | None = None
    z_max: float | None = None
    z_zero_count = 0
    sink = pa.BufferOutputStream()
    writer = pq.ParquetWriter(sink, output_schema, compression="zstd", version="2.6")
    try:
        for batch in reader:
            if batch.num_rows > MAX_NATIVE_BATCH_ROWS or batch.nbytes > MAX_NATIVE_BATCH_BYTES:
                raise ValueError("usace_nid_native_arrow_batch_exceeded")
            if batch.schema != schema:
                raise ValueError("usace_nid_native_arrow_schema_changed")
            ids = batch.column(NATIVE_FID_NAME).to_pylist()
            geometries = batch.column(NATIVE_GEOMETRY_NAME).to_pylist()
            if len(ids) != batch.num_rows or len(geometries) != batch.num_rows:
                raise ValueError("usace_nid_native_arrow_batch_shape_invalid")
            for oid, raw_wkb in zip(ids, geometries):
                if type(oid) is not int or oid not in expected or oid in seen:
                    raise ValueError("usace_nid_native_filegdb_oid_population_drift")
                seen.add(oid)
                if raw_wkb is None:
                    null_count += 1
                    continue
                try:
                    geom = shapely.from_wkb(raw_wkb)
                    coords = list(geom.coords)
                except Exception as exc:
                    raise ValueError("usace_nid_native_wkb_invalid") from exc
                if (geom.geom_type != "Point" or not geom.has_z or len(coords) != 1
                        or len(coords[0]) != 3):
                    raise ValueError("usace_nid_native_point_z_required")
                x, y, z = coords[0]
                if math.isfinite(z):
                    z_min = z if z_min is None else min(z_min, z)
                    z_max = z if z_max is None else max(z_max, z)
                    z_zero_count += z == 0.0
                if not all(math.isfinite(v) for v in (x, y, z)):
                    invalid_count += 1
                elif not (-180 <= x <= 180 and -90 <= y <= 90):
                    invalid_count += 1
                geometry_hash.update(struct.pack(">qI", oid, len(raw_wkb)))
                geometry_hash.update(raw_wkb)
            # Arrow IPC is only a deterministic per-capture digest input; it is
            # never written as an expanded intermediate.
            record_hash.update(batch.serialize().to_pybytes())
            arrays = [batch.column(i) for i in range(batch.num_columns)]
            table = pa.Table.from_arrays(arrays, schema=output_schema)
            writer.write_table(table, row_group_size=MAX_NATIVE_BATCH_ROWS)
            count += batch.num_rows
            if sink.tell() > MAX_NATIVE_CANDIDATE_BYTES:
                raise ValueError("usace_nid_native_candidate_size_exceeded")
    finally:
        writer.close()
    candidate = sink.getvalue().to_pybytes()
    if count != len(expected_ids) or seen != expected:
        raise ValueError("usace_nid_native_filegdb_oid_roster_mismatch")
    if len(candidate) > MAX_NATIVE_CANDIDATE_BYTES:
        raise ValueError("usace_nid_native_candidate_size_exceeded")
    return candidate, {
        "source_feature_count": count,
        "source_objectid_sha256": _sha(_canonical(sorted(seen))),
        "source_record_sha256": record_hash.hexdigest(),
        "source_geometry_sha256": geometry_hash.hexdigest(),
        "source_schema_sha256": _sha(schema.serialize().to_pybytes()),
        "source_fields": schema.names,
        "source_crs": "EPSG:4269",
        "source_geometry_type": "Point Z",
        "null_geometry_count": null_count,
        "invalid_geometry_count": invalid_count,
        "geometry_z_preserved": True,
        "geometry_z_min": z_min,
        "geometry_z_max": z_max,
        "geometry_z_zero_count": z_zero_count,
        "native_fid_name": NATIVE_FID_NAME,
    }


@contextmanager
def _gdal_zip_path(archive_path: Path, gdb_name: str):
    """Give GDAL's /vsizip handler a .zip alias without copying the archive."""
    with tempfile.TemporaryDirectory(prefix="nid-vsizip-") as temporary:
        alias = Path(temporary) / "native-export.zip"
        alias.symlink_to(archive_path.resolve())
        yield f"/vsizip/{alias}/{gdb_name}"


def _inspect_archive(archive_path: Path, expected_count: int, expected_ids: list[int],
                     layer: Mapping[str, object]) -> tuple[bytes, dict[str, object]]:
    if archive_path.stat().st_size > MAX_NATIVE_RESPONSE_BYTES:
        raise ValueError("usace_nid_native_archive_response_too_large")
    try:
        with zipfile.ZipFile(archive_path) as zf:
            entries = zf.infolist()
            names = [entry.filename for entry in entries]
            if (not entries or len(entries) > MAX_NATIVE_ZIP_MEMBERS
                    or len(names) != len(set(names))
                    or any(name.startswith("/") or "\\" in name
                           or ".." in PurePosixPath(name).parts for name in names)
                    or any(entry.flag_bits & 0x1 for entry in entries)):
                raise ValueError("usace_nid_native_zip_roster_or_path_invalid")
            files = [entry for entry in entries if not entry.is_dir()]
            expanded = sum(entry.file_size for entry in files)
            if (not files or expanded > MAX_NATIVE_EXPANDED_BYTES
                    or max(entry.file_size for entry in files) > MAX_NATIVE_MEMBER_BYTES):
                raise ValueError("usace_nid_native_zip_expanded_size_exceeded")
            actual_expanded = 0
            for entry in entries:
                if entry.is_dir() and entry.file_size != 0:
                    raise ValueError("usace_nid_native_zip_directory_payload_invalid")
                actual_member = 0
                try:
                    with zf.open(entry, "r") as member:
                        while True:
                            block = member.read(MAX_NATIVE_ZIP_CRC_CHUNK_BYTES)
                            if not block:
                                break
                            actual_member += len(block)
                            if (actual_member > entry.file_size
                                    or actual_member > MAX_NATIVE_MEMBER_BYTES):
                                raise ValueError("usace_nid_native_zip_member_actual_size_exceeded")
                            if not entry.is_dir():
                                actual_expanded += len(block)
                                if actual_expanded > MAX_NATIVE_EXPANDED_BYTES:
                                    raise ValueError("usace_nid_native_zip_actual_expansion_exceeded")
                except (zipfile.BadZipFile, EOFError, RuntimeError, OSError, zlib.error) as exc:
                    raise ValueError("usace_nid_native_zip_member_crc_or_stream_invalid") from exc
                if actual_member != entry.file_size:
                    raise ValueError("usace_nid_native_zip_member_size_mismatch")
            if actual_expanded != expanded:
                raise ValueError("usace_nid_native_zip_expansion_mismatch")
            roots = {PurePosixPath(entry.filename).parts[0] for entry in files}
            if len(roots) != 1 or not next(iter(roots)).endswith(".gdb"):
                raise ValueError("usace_nid_native_zip_filegdb_missing")
            gdb_name = next(iter(roots))
    except zipfile.BadZipFile as exc:
        raise ValueError("usace_nid_native_archive_invalid_zip") from exc
    with _gdal_zip_path(archive_path, gdb_name) as vsi_path:
        info = pyogrio.read_info(vsi_path, layer=NATIVE_LAYER_NAME,
                                 force_feature_count=False, force_total_bounds=False)
        with pyogrio.open_arrow(vsi_path, layer=NATIVE_LAYER_NAME, return_fids=True,
                                batch_size=MAX_NATIVE_BATCH_ROWS, use_pyarrow=True,
                                force_2d=False, datetime_as_string=False) as (meta, reader):
            _validate_native_reader(info, reader.schema, layer, expected_count)
            candidate, audit = _parquet_from_native_batches(reader, expected_ids, layer["extent"])
    audit.update({
        "source_extent": layer["extent"],
        "archive_bytes": archive_path.stat().st_size,
        "archive_sha256": _sha(archive_path.read_bytes()),
        "archive_member_count": len(entries),
        "archive_declared_expanded_bytes": expanded,
        "archive_actual_expanded_bytes_crc_checked": actual_expanded,
        "archive_members_crc_checked": len(entries),
        "archive_member_names": names,
        "source_release_date": None,
        "release_date_unknown_reason": "USACE does not expose a verified release date; retrieval and service metadata times are not release dates.",
        "nid_scope_note": "Current USACE NID selection as published; USACE says qualifying dams are not yet all represented. This capture does not claim a census of all dams.",
        "historical_crosswalk": "DataLumos project 239785 V1 has not been compared to this current producer layer.",
        "rights_and_release_status": "pending; candidate remains unapproved",
    })
    return candidate, audit


def _read_bounded(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_size > limit):
            raise ValueError("usace_nid_native_capture_file_unsafe_or_oversized")
        body = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if (len(body) != before.st_size or len(body) > limit
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
        raise ValueError("usace_nid_native_capture_file_changed")
    return body


def _read_native_capture(root: Path) -> tuple[dict[str, bytes], dict[str, object]]:
    manifest_path = root / "native_capture_manifest.json"
    manifest_body = _read_bounded(manifest_path, MAX_NATIVE_MANIFEST_BYTES)
    manifest = legacy._strict_json(manifest_body)
    if not isinstance(manifest, dict) or manifest.get("complete") is not True:
        raise ValueError("usace_nid_native_capture_manifest_invalid")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict):
        raise ValueError("usace_nid_native_capture_file_list_missing")
    expected_names = set(descriptors) | {manifest_path.name}
    entries = _safe_capture_entries(root, allow_manifest=True)
    if {p.name for p in entries} != expected_names:
        raise ValueError("usace_nid_native_capture_roster_mismatch")
    sizes = 0
    limits = {}
    for name, descriptor in descriptors.items():
        if not isinstance(name, str) or Path(name).name != name or name in {"", ".", ".."}:
            raise ValueError("usace_nid_native_capture_name_invalid")
        limit = (MAX_NATIVE_RECEIPT_BYTES if ".receipt." in name or name.endswith(".request.json")
                 else MAX_NATIVE_MANIFEST_BYTES if name.endswith(".json")
                 else MAX_NATIVE_RESPONSE_BYTES)
        info = (root / name).lstat()
        if (not isinstance(descriptor, dict) or type(descriptor.get("bytes")) is not int
                or not isinstance(descriptor.get("sha256"), str)
                or info.st_size > limit):
            raise ValueError("usace_nid_native_capture_file_unsafe_or_oversized:" + name)
        sizes += info.st_size
        limits[name] = limit
    if sizes + len(manifest_body) > MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_replay_aggregate_size_exceeded")
    bodies = {}
    for name, descriptor in descriptors.items():
        body = _read_bounded(root / name, limits[name])
        if descriptor != {"bytes": len(body), "sha256": _sha(body)}:
            raise ValueError("usace_nid_native_capture_file_digest_mismatch:" + name)
        bodies[name] = body
    return bodies, manifest


def _capture_get_names() -> tuple[str, ...]:
    return ("service_before", "layer_before", "count_before", "ids_before",
            "service_after", "layer_after", "count_after", "ids_after", "native_archive")


def _replica_params() -> dict[str, object]:
    return {
        "f": "json",
        "replicaName": "hifld-nid-native-" + uuid4().hex,
        "layers": str(legacy.LAYER_ID),
        "layerQueries": json.dumps({str(legacy.LAYER_ID): {
            "where": "1=1", "outFields": "*",
        }}, separators=(",", ":")),
        "returnAttachments": "false",
        "syncModel": "none",
        "dataFormat": "filegdb",
        "async": "true",
        "transportType": "esriTransportTypeUrl",
        "syncDirection": "download",
    }


def _validate_replica_request_body(body: bytes, params: Mapping[str, object]) -> None:
    expected = {
        str(key): json.dumps(value, separators=(",", ":"))
        if isinstance(value, (dict, list)) else str(value)
        for key, value in params.items()
    }
    try:
        pairs = parse_qsl(body.decode("ascii"), keep_blank_values=True, strict_parsing=True)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("usace_nid_native_replica_request_body_malformed") from exc
    if len(pairs) != len(expected) or len({key for key, _ in pairs}) != len(pairs) or dict(pairs) != expected:
        raise ValueError("usace_nid_native_replica_request_body_mismatch")


def _replica_status_action(status: object) -> str:
    if not isinstance(status, str):
        raise ValueError("usace_nid_native_replica_status_unrecognized")
    if status == "Completed":
        return "completed"
    if status in {"Pending", "InProgress"}:
        return "continue"
    if status in {"Failed", "Cancelled", "Error", "CompletedWithErrors"}:
        raise ValueError("usace_nid_native_replica_job_failed")
    raise ValueError("usace_nid_native_replica_status_unrecognized")


def _capture_native_export(root: Path) -> None:
    if root.exists():
        if any(root.iterdir()):
            raise ValueError("usace_nid_native_incomplete_capture_directory_refused")
    else:
        root.mkdir(parents=True, exist_ok=False)
    _safe_capture_entries(root)
    urls = {
        "service_before": SERVICE_URL + "?f=json",
        "layer_before": LAYER_URL + "?f=json",
        "count_before": _count_url(),
        "ids_before": _ids_url(),
        "service_after": SERVICE_URL + "?f=json",
        "layer_after": LAYER_URL + "?f=json",
        "count_after": _count_url(),
        "ids_after": _ids_url(),
    }
    responses: dict[str, dict[str, object]] = {}
    client = httpx.Client(follow_redirects=False, timeout=60,
                          headers={"User-Agent": "hifld-next-datasets/nid-native-draft",
                                   "Accept-Encoding": "identity"})
    try:
        for name in ("service_before", "layer_before", "count_before", "ids_before"):
            _body, responses[name] = _get(client, root, name, urls[name])
        initial_docs = _source_docs(root, ("service_before", "layer_before", "count_before", "ids_before"))
        _validate_initial_population(initial_docs)

        params = _replica_params()
        _request_body, responses["replica_create"] = _capture_post(client, root, params)
        try:
            create_result = legacy._strict_json(_read_bounded(root / "replica_create.body", MAX_NATIVE_RECEIPT_BYTES))
        except Exception as exc:
            raise ValueError("usace_nid_native_replica_create_invalid_json") from exc
        if not isinstance(create_result, dict) or "error" in create_result:
            raise ValueError("usace_nid_native_replica_create_rejected")
        status_url = create_result.get("statusUrl")
        if not isinstance(status_url, str):
            raise ValueError("usace_nid_native_replica_status_url_missing")
        status_url = _safe_official_url(urljoin(EXPORT_URL, status_url))
        status_parts = urlsplit(status_url)
        status_query = parse_qsl(status_parts.query, keep_blank_values=True)
        if not any(key.lower() == "f" for key, _value in status_query):
            status_query.append(("f", "json"))
        status_url = status_parts._replace(query=urlencode(status_query)).geturl()
        result_url = None
        last_status = None
        for attempt in range(MAX_REPLICA_POLLS):
            if attempt:
                time.sleep(REPLICA_POLL_INTERVAL_SECONDS)
            name = f"replica_poll_{attempt:02d}"
            _body, receipt = _get(client, root, name, status_url)
            responses[name] = receipt
            status_doc = legacy._strict_json(_read_bounded(root / f"{name}.body", MAX_NATIVE_RESPONSE_BYTES))
            if not isinstance(status_doc, dict):
                raise ValueError("usace_nid_native_replica_status_invalid")
            last_status = status_doc.get("status")
            action = _replica_status_action(last_status)
            if action == "completed":
                result_url = status_doc.get("resultUrl")
                break
        if not isinstance(result_url, str):
            raise ValueError("usace_nid_native_replica_poll_limit_exceeded")
        result_url = _safe_official_url(urljoin(status_url, result_url))
        _body, responses["native_archive"] = _get(client, root, "native_archive", result_url)
        for name in ("service_after", "layer_after", "count_after", "ids_after"):
            _body, responses[name] = _get(client, root, name, urls[name])
        # The source transfer used identity encoding; retain the wire copy and
        # decoded archive body separately and require both to remain complete.
        receipt = responses["native_archive"]
        if receipt.get("content_encoding", "identity").lower() not in ("", "identity"):
            raise ValueError("usace_nid_native_archive_transport_encoding_unexpected")
        if receipt.get("wire_bytes") != receipt.get("bytes") or receipt.get("wire_sha256") != receipt.get("sha256"):
            raise ValueError("usace_nid_native_archive_wire_body_mismatch")
        _write_native_manifest(root, responses, params)
    except Exception as exc:
        failure = {
            "complete": False, "error_code": type(exc).__name__,
            "captured_response_names": sorted(responses),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        failure_path = root / "capture_failure.json"
        if not failure_path.exists():
            failure_path.write_bytes(_canonical(failure) + b"\n")
        raise
    finally:
        client.cookies.clear()
        client.close()


def _validate_initial_population(docs: Mapping[str, Mapping[str, object]]) -> None:
    service = docs["service_before"]
    layer = docs["layer_before"]
    capabilities_raw = service.get("capabilities", "")
    formats_raw = service.get("supportedExportFormats", "")
    capabilities = ({str(part).strip().lower() for part in capabilities_raw}
                    if isinstance(capabilities_raw, list)
                    else {part.strip().lower() for part in str(capabilities_raw).split(",")})
    formats = ({str(part).strip().lower().replace(" ", "") for part in formats_raw}
               if isinstance(formats_raw, list)
               else {part.strip().lower().replace(" ", "") for part in str(formats_raw).split(",")})
    if "extract" not in capabilities or "filegdb" not in formats:
        raise ValueError("usace_nid_native_extract_capability_missing")
    count = docs["count_before"].get("count")
    ids = docs["ids_before"].get("objectIds")
    if (type(count) is not int or count <= 0 or not isinstance(ids, list)
            or len(ids) != count or any(type(value) is not int for value in ids)
            or len(set(ids)) != count):
        raise ValueError("usace_nid_native_pre_export_population_invalid")
    if (layer.get("id") != legacy.LAYER_ID or layer.get("name") != legacy.LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != NATIVE_FID_NAME or layer.get("hasZ") is not True
            or layer.get("hasM") is not False or not legacy._is_native_4269(
                (layer.get("extent") or {}).get("spatialReference"))
            or tuple(legacy._field_signature(field) for field in layer.get("fields", []))
            != legacy.EXPECTED_FIELD_SIGNATURES):
        raise ValueError("usace_nid_native_pre_export_metadata_drift")


def _write_native_manifest(root: Path, responses: Mapping[str, Mapping[str, object]],
                           params: Mapping[str, object]) -> None:
    files = {}
    for path in sorted(root.iterdir(), key=lambda p: p.name):
        if path.name in {"native_capture_manifest.json", "capture_failure.json"}:
            continue
        body = _read_bounded(path, MAX_NATIVE_RECEIPT_BYTES if path.name.endswith(".json")
                             and ("receipt" in path.name or "request" in path.name)
                             else MAX_NATIVE_RESPONSE_BYTES)
        files[path.name] = {"bytes": len(body), "sha256": _sha(body)}
    total = sum(item["bytes"] for item in files.values())
    if total > MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_capture_budget_exceeded")
    manifest = {
        "schema_version": 1, "complete": True,
        "producer_service": SERVICE_URL, "layer_id": legacy.LAYER_ID,
        "export_format": "filegdb", "sync_model": "none",
        "create_parameters": dict(params),
        "response_names": sorted(responses),
        "files": files,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    body = _canonical(manifest) + b"\n"
    if len(body) > MAX_NATIVE_MANIFEST_BYTES or total + len(body) > MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_capture_budget_exceeded")
    (root / "native_capture_manifest.json").write_bytes(body)


def _validate_native_capture(root: Path, bodies: Mapping[str, bytes],
                             manifest: Mapping[str, object]) -> tuple[dict[str, object], list[int], Path]:
    response_names = manifest.get("response_names")
    if (not isinstance(response_names, list) or len(response_names) != len(set(response_names))
            or any(not isinstance(n, str) for n in response_names)):
        raise ValueError("usace_nid_native_capture_response_manifest_invalid")
    poll_names = sorted(n for n in response_names if n.startswith("replica_poll_"))
    if poll_names != [f"replica_poll_{i:02d}" for i in range(len(poll_names))]:
        raise ValueError("usace_nid_native_capture_poll_roster_invalid")
    expected_names = set(_capture_get_names()) | {"replica_create"} | set(poll_names)
    if set(response_names) != expected_names:
        raise ValueError("usace_nid_native_capture_response_roster_invalid")
    request_doc = legacy._strict_json(_read_bounded(root / "replica_create.request.json", MAX_NATIVE_RECEIPT_BYTES))
    create_params = manifest.get("create_parameters")
    if (not isinstance(request_doc, dict) or request_doc.get("method") != "POST"
            or request_doc.get("url") != EXPORT_URL or request_doc.get("parameters") != create_params
            or not isinstance(create_params, dict)
            or not str(create_params.get("replicaName", "")).startswith("hifld-nid-native-")
            or request_doc.get("body_sha256") != _sha(_read_bounded(
                root / "replica_create.request.body", MAX_NATIVE_RECEIPT_BYTES))):
        raise ValueError("usace_nid_native_replica_request_identity_invalid")
    create_request_body = _read_bounded(root / "replica_create.request.body", MAX_NATIVE_RECEIPT_BYTES)
    if request_doc.get("body_bytes") != len(create_request_body):
        raise ValueError("usace_nid_native_replica_request_body_size_mismatch")
    _validate_replica_request_body(create_request_body, create_params)
    expected_urls = {
        "service_before": SERVICE_URL + "?f=json", "layer_before": LAYER_URL + "?f=json",
        "count_before": _count_url(), "ids_before": _ids_url(),
        "service_after": SERVICE_URL + "?f=json", "layer_after": LAYER_URL + "?f=json",
        "count_after": _count_url(), "ids_after": _ids_url(),
    }
    create_receipt = legacy._strict_json(_read_bounded(
        root / "replica_create.receipt.json", MAX_NATIVE_RECEIPT_BYTES))
    if (not isinstance(create_receipt, dict) or create_receipt.get("url") != EXPORT_URL
            or create_receipt.get("method") != "POST" or create_receipt.get("status") != 200
            or create_receipt.get("cookies_sent") is not False
            or create_receipt.get("authentication_sent") is not False):
        raise ValueError("usace_nid_native_replica_request_receipt_invalid")
    create_doc = legacy._strict_json(bodies["replica_create.body"])
    status_url = create_doc.get("statusUrl") if isinstance(create_doc, dict) else None
    if not isinstance(status_url, str):
        raise ValueError("usace_nid_native_replica_status_url_missing")
    status_url = _safe_official_url(urljoin(EXPORT_URL, status_url))
    status_parts = urlsplit(status_url)
    status_query = parse_qsl(status_parts.query, keep_blank_values=True)
    if not any(key.lower() == "f" for key, _value in status_query):
        status_query.append(("f", "json"))
    status_url = status_parts._replace(query=urlencode(status_query)).geturl()
    for name in response_names:
        receipt_path = root / f"{name}.receipt.json"
        receipt = legacy._strict_json(_read_bounded(receipt_path, MAX_NATIVE_RECEIPT_BYTES))
        expected_method = "POST" if name == "replica_create" else "GET"
        if (not isinstance(receipt, dict) or receipt.get("complete_body") is not True
                or receipt.get("status") != 200 or receipt.get("method") != expected_method
                or receipt.get("bytes") != len(bodies[f"{name}.body"])
                or receipt.get("sha256") != _sha(bodies[f"{name}.body"])):
            raise ValueError("usace_nid_native_capture_receipt_invalid:" + name)
        if name in _capture_get_names() or name.startswith("replica_poll_"):
            _safe_official_url(str(receipt.get("url", "")))
            if receipt.get("final_url") != receipt.get("url"):
                raise ValueError("usace_nid_native_capture_redirect_refused:" + name)
            if name in expected_urls and receipt.get("url") != expected_urls[name]:
                raise ValueError("usace_nid_native_metadata_request_identity_invalid:" + name)
            if name.startswith("replica_poll_") and receipt.get("url") != status_url:
                raise ValueError("usace_nid_native_poll_request_identity_invalid")
            if name != "replica_create":
                wire = bodies.get(f"{name}.wire")
                if (wire is None or receipt.get("wire_bytes") != len(wire)
                        or receipt.get("wire_sha256") != _sha(wire)):
                    raise ValueError("usace_nid_native_capture_wire_receipt_invalid:" + name)
    if not poll_names:
        raise ValueError("usace_nid_native_capture_poll_missing")
    poll_doc = legacy._strict_json(bodies[f"{poll_names[-1]}.body"])
    if not isinstance(poll_doc, dict) or poll_doc.get("status") != "Completed":
        raise ValueError("usace_nid_native_capture_job_not_completed")
    result_url = poll_doc.get("resultUrl")
    archive_receipt = legacy._strict_json(_read_bounded(root / "native_archive.receipt.json", MAX_NATIVE_RECEIPT_BYTES))
    expected_result_url = urljoin(status_url, result_url) if isinstance(result_url, str) else ""
    if (not isinstance(result_url, str) or archive_receipt.get("url") != expected_result_url
            or archive_receipt.get("content_encoding", "identity").lower() not in ("", "identity")
            or archive_receipt.get("wire_bytes") != archive_receipt.get("bytes")
            or archive_receipt.get("wire_sha256") != archive_receipt.get("sha256")):
        raise ValueError("usace_nid_native_archive_job_url_mismatch")
    docs = _source_docs(root, ("service_before", "layer_before", "count_before", "ids_before",
                               "service_after", "layer_after", "count_after", "ids_after"))
    layer, ids = _validate_metadata_and_rosters(docs)
    count = docs["count_before"]["count"]
    archive_path = root / "native_archive.body"
    candidate, audit = _inspect_archive(archive_path, count, ids, layer)
    audit.update({
        "pre_export_objectid_sha256": _sha(_canonical(sorted(docs["ids_before"]["objectIds"]))),
        "post_export_objectid_sha256": _sha(_canonical(sorted(docs["ids_after"]["objectIds"]))),
        "source_metadata_before_sha256": _sha(bodies["layer_before.body"]),
        "source_metadata_after_sha256": _sha(bodies["layer_after.body"]),
    })
    return {"candidate": candidate, "audit": audit, "docs": docs}, ids, archive_path


def _safe_receipt(receipt: Mapping[str, object]) -> dict[str, object]:
    """Project a private receipt without a signed query or session material."""
    raw_url = str(receipt.get("url", ""))
    parsed = urlsplit(raw_url)
    safe = {key: receipt.get(key) for key in (
        "method", "status", "complete_body", "bytes", "sha256", "wire_bytes",
        "wire_sha256", "content_encoding", "content_length", "error_code",
    ) if key in receipt}
    safe.update({
        "url_host": parsed.hostname,
        "url_path": parsed.path,
        "url_query_redacted": bool(parsed.query),
        "url_sha256": _sha(raw_url.encode()),
    })
    return safe


def _input_provenance(name: str, receipt: Mapping[str, object]) -> dict[str, object]:
    """Adapt a captured response receipt to the shared provenance contract safely."""
    raw_url = str(receipt.get("url", ""))
    parsed = urlsplit(raw_url)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.path:
        raise ValueError("usace_nid_native_receipt_url_invalid:" + name)
    retrieval_url = parsed._replace(query="", fragment="").geturl()
    canonical_url = SERVICE_URL if name.startswith(("service_", "replica_", "native_archive")) else LAYER_URL
    return {
        "input_id": name,
        "producer": PUBLISHER,
        "host": parsed.hostname,
        "role": "producer_publication",
        "canonical_url": canonical_url,
        "retrieval_url": retrieval_url,
        "retrieval_query_redacted": bool(parsed.query),
        "retrieved_at": receipt.get("observed_at"),
        "http_status": receipt.get("status"),
        "bytes": receipt.get("bytes"),
        "sha256": receipt.get("sha256"),
        "license": "pending_rights_review",
        "attribution": "USACE attribution and terms review pending.",
        "release_date": None,
        "release_date_unknown_reason": (
            "The producer response does not establish a dataset release date; retrieval time is not a release date."
        ),
    }


def _native_stage_outputs(root: Path, bodies: Mapping[str, bytes], manifest: Mapping[str, object],
                          candidate: bytes, audit: Mapping[str, object], identity: Mapping[str, object],
                          version_key: str) -> dict[str, bytes]:
    source_files = manifest.get("files")
    if not isinstance(source_files, dict):
        raise ValueError("usace_nid_native_capture_file_manifest_missing")
    output_path = f"geoparquet/{DATASET_SLUG}.parquet"
    candidate_record = {"bytes": len(candidate), "sha256": _sha(candidate)}
    receipts = {}
    for name in manifest["response_names"]:
        receipt_path = root / f"{name}.receipt.json"
        receipt = legacy._strict_json(_read_bounded(receipt_path, MAX_NATIVE_RECEIPT_BYTES))
        receipts[name] = _safe_receipt(receipt)
    safe_capture_provenance = {
        "schema_version": 1,
        "complete": True,
        "producer_service": SERVICE_URL,
        "layer_id": legacy.LAYER_ID,
        "export_format": "filegdb",
        "sync_model": "none",
        "capture_manifest_sha256": _sha(_read_bounded(
            root / "native_capture_manifest.json", MAX_NATIVE_MANIFEST_BYTES)),
        "receipts": receipts,
        "source_file_digests": source_files,
        "signed_urls_included": False,
        "authentication_sent": False,
        "cookies_sent": False,
        "native_parity": {
            "count": audit["source_feature_count"],
            "objectid_roster_sha256": audit["source_objectid_sha256"],
            "schema_sha256": audit["source_schema_sha256"],
            "geometry_sha256": audit["source_geometry_sha256"],
            "crs": "EPSG:4269",
            "geometry_type": "Point Z",
            "fid_column": NATIVE_FID_NAME,
        },
    }
    source_manifest = {
        "schema_version": 1,
        "approved": False,
        "title": LAYER_NAME,
        "description": (
            "Current USACE NID producer FeatureServer layer 0 exported through the "
            "advertised Extract/fileGDB operation. The candidate preserves all source "
            "attributes, OBJECTID, native EPSG:4269 Point Z WKB, and the exact producer "
            "OID roster. USACE says qualifying dams are not yet all represented. "
            "Historical DataLumos V1 equivalence, source freshness, schema and ID "
            "contracts, rights, and release approval remain pending."
        ),
        "publisher": PUBLISHER,
        "agency": "U.S. Army Corps of Engineers",
        "source_url": LAYER_URL,
        "license": None,
        "source_modified": None,
        "date_issued": None,
        "date_modified": None,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": [_input_provenance(name, legacy._strict_json(
            _read_bounded(root / f"{name}.receipt.json", MAX_NATIVE_RECEIPT_BYTES)))
                   for name in sorted(receipts)],
        "transformations": [{
            "operation": "Native USACE FileGDB to GeoParquet WKB",
            "parameters": {
                "source_fields": "80 FileGDB data attributes preserved in native Arrow types plus source OBJECTID FID",
                "source_identity": "complete pre/post OBJECTID rosters matched; NIDID uniqueness/stability is not asserted",
                "geometry": "native EPSG:4269 Point Z WKB retained byte-for-byte; no XY flattening, reprojection, or repair",
                "candidate_status": "unapproved; historical, schema, ID, freshness, rights and release acceptance pending",
            },
        }],
        "lineage": "Direct USACE producer FeatureServer service; no delegated cache is used.",
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Faithful native source draft; acceptance remains unresolved.",
                "objects": [{"path": output_path, **candidate_record}],
            }},
        },
        "unsupported_output_formats": {
            "geopackage": "No accepted historical field and geometry contract.",
            "pmtiles": "No approved mapping or display contract.",
        },
    }
    validate_release_provenance(source_manifest)
    validate_output_policy(source_manifest)
    docs = _source_docs(root, ("service_before", "layer_before", "count_before", "ids_before",
                               "service_after", "layer_after", "count_after", "ids_after"))
    safe_rosters = {
        "before": sorted(docs["ids_before"]["objectIds"]),
        "after": sorted(docs["ids_after"]["objectIds"]),
        "before_sha256": audit["pre_export_objectid_sha256"],
        "after_sha256": audit["post_export_objectid_sha256"],
    }
    output = {
        output_path: candidate,
        "metadata/source_manifest.json": _canonical(source_manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({
            "approved": False, "version": version_key,
            "objects": {output_path: candidate_record},
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        }) + b"\n",
        "metadata/native_capture_provenance.json": _canonical(safe_capture_provenance) + b"\n",
        "metadata/native_source_rosters.json": _canonical(safe_rosters) + b"\n",
        "raw/captures/native_export.filegdb.zip": bodies["native_archive.body"],
        "raw/captures/source_metadata_before.json": _canonical({
            "service": docs["service_before"], "layer": docs["layer_before"],
            "count": docs["count_before"],
        }) + b"\n",
        "raw/captures/source_metadata_after.json": _canonical({
            "service": docs["service_after"], "layer": docs["layer_after"],
            "count": docs["count_after"],
        }) + b"\n",
    }
    if sum(map(len, output.values())) > MAX_NATIVE_TOTAL_STAGE_BYTES:
        raise ValueError("usace_nid_native_stage_size_exceeded")
    return output


def capture_national_inventory_of_dams_native(
    storage: StagingStorageResource, *, version: str,
    capture_directory: str | None = None,
) -> dict[str, object]:
    """Capture/replay one native FileGDB extract and stage an unapproved draft."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    staging_root = Path(storage.local_dir).absolute()
    legacy._validate_local_directory_path(staging_root, storage.local_dir)
    staging_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging_root).free < MIN_DISK_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    capture_root = (Path(capture_directory).absolute() if capture_directory else
                    Path("/tmp/hifld-native-reconstruction/165/native-captures") / f"{version}-{uuid4().hex}")
    legacy._validate_local_directory_path(capture_root, capture_directory)
    if capture_root.exists():
        os.chmod(capture_root, 0o700)
    if capture_root.exists() and (capture_root / "native_capture_manifest.json").is_file():
        bodies, native_manifest = _read_native_capture(capture_root)
        reused_capture = True
    else:
        if capture_root.exists() and any(capture_root.iterdir()):
            raise ValueError("usace_nid_native_incomplete_capture_directory_refused")
        capture_root.parent.mkdir(parents=True, exist_ok=True)
        if not capture_root.exists():
            capture_root.mkdir(mode=0o700)
        os.chmod(capture_root, 0o700)
        _capture_native_export(capture_root)
        bodies, native_manifest = _read_native_capture(capture_root)
        reused_capture = False
    validated, expected_ids, _archive_path = _validate_native_capture(capture_root, bodies, native_manifest)
    candidate, audit, docs = validated["candidate"], validated["audit"], validated["docs"]
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    version_key = content_derived_version(audit["source_record_sha256"],
                                          audit["source_geometry_sha256"],
                                          audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version_key)
    outputs = _native_stage_outputs(capture_root, bodies, native_manifest, candidate, audit, identity, version_key)
    additional_bytes = sum(len(body) for body in outputs.values())
    capture_bytes = sum(path.stat().st_size for path in _safe_capture_entries(capture_root, allow_manifest=True))
    if capture_bytes + additional_bytes > MAX_NATIVE_TOTAL_STAGE_BYTES + MAX_NATIVE_CAPTURE_BYTES:
        raise ValueError("usace_nid_native_total_evidence_exceeded")
    if shutil.disk_usage(staging_root).free < MIN_DISK_FREE_BYTES + additional_bytes:
        raise ValueError("disk_floor_before_candidate_staging")
    state_key = f"_construction-state/{DATASET_SLUG}/{version_key}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = legacy._strict_json(load_first_construction(storage, state_key, version_key, fingerprint))
        payload = bundle.get("payload") if isinstance(bundle, dict) else None
        if not isinstance(payload, dict):
            raise ValueError("usace_nid_native_cached_candidate_invalid")
        verify_cached_candidate(storage, repo_root, state_key, prefix, version_key, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                payload.get("objects", {}), payload.get("capture_receipts", {}))
        result = legacy._candidate_result(version_key, audit, prefix, True)
    else:
        for name, body in outputs.items():
            write_immutable(storage, f"{prefix}/{name}", body)
        descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
        for name, descriptor in descriptors.items():
            key = f"{prefix}/{name}"
            snap = storage.object_snapshot(key)
            if (snap is None or snap.size != descriptor["bytes"]
                    or storage.sha256_key(key) != descriptor["sha256"]
                    or storage.object_snapshot(key) != snap):
                raise ValueError("usace_nid_native_candidate_readback_failed:" + name)
        payload = {
            "processing_identity": identity,
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "capture_receipts": {name: _safe_receipt(legacy._strict_json(
                _read_bounded(capture_root / f"{name}.receipt.json", MAX_NATIVE_RECEIPT_BYTES)))
                for name in native_manifest["response_names"]},
            "objects": descriptors,
        }
        seal_ready_candidate(storage, state_key, version_key, fingerprint, payload)
        validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"])
        result = legacy._candidate_result(version_key, audit, prefix, False)
    result.update({
        "native_filegdb": True,
        "native_geometry_type": "Point Z",
        "source_fid_name": NATIVE_FID_NAME,
        "source_data_field_count": len(_expected_attribute_names()),
        "capture_reused": reused_capture,
        "source_null_geometry_count": audit["null_geometry_count"],
        "source_invalid_geometry_count": audit["invalid_geometry_count"],
    })
    return result
