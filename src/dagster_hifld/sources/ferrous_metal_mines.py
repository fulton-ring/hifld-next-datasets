"""Draft-only reconstruction of the retired USGS National Atlas Ferrous Metal Mines layer.

The historical producer release (``minop3x020_nt00020.tar.gz`` on
``dds.cr.usgs.gov``) has been removed from the live server and the inventoried
DataLumos archive (project 239022, version V1) rejects anonymous access, so
this module stages only an explicitly provided preserved capture of the
producer archive.  It independently captures the surviving producer FGDC
metadata document from the USGS National Geospatial Technical Operations
Center legacy WAF, decodes the original shapefile members byte-strictly, and
writes an unapproved typed GeoParquet candidate.  No default data fetch, no
archive polling, no substitute feature IDs, no datum correction, no schema
mapping, and no release acceptance is performed.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import os
import shutil
import stat
import struct
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import (
    canonical_construction_bundle,
    seal_first_construction,
)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    STAGED_RELEASE_DERIVATIVE_NAMES,
    capture_get,
    candidate_prefix,
    content_derived_version,
    processing_identity as shared_processing_identity,
    validate_staged_release_derivatives,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "ferrous-metal-mines"
STEM = "minop3x020"
PUBLISHER = "U.S. Geological Survey, National Atlas of the United States (retired distribution)"
AGENCY = "U.S. Geological Survey"
METADATA_URL = "https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop3x020.xml"
METADATA_BYTES = 18_347
METADATA_SHA256 = "8302b0f0028247f00d0a96a3b9d537c2e8db712f2425e09a2f1fc1317f94a232"
ARCHIVE_CANONICAL_URL = "http://dds.cr.usgs.gov/pub/data/nationalatlas/minop3x020_nt00020.tar.gz"
MAX_ARCHIVE_BYTES = 64_000_000
MAX_METADATA_BYTES = 128_000
REQUIRED_MEMBERS = frozenset(STEM + suffix for suffix in (".shp", ".shx", ".dbf"))
OPTIONAL_MEMBERS = frozenset(STEM + suffix for suffix in (".prj", ".txt", ".sbn", ".sbx", ".shp.xml"))
DBF_FIELD_NAMES = (
    "MINOP3X020", "COMMODITY", "SITE_NAME", "COMPANY_NA", "STATE_LOCA",
    "COUNTY", "LATITUDE", "LONGITUDE", "PLANT_MIN",
)
DBF_FIELD_KINDS = ("N", "C", "C", "C", "C", "C", "N", "N", "C")
ID_FIELD = "MINOP3X020"
GEODETIC_FIELDS = ("LATITUDE", "LONGITUDE")
# Proposed diagnostic sanity bound only: one physical DBF coordinate unit.
# FGDC does not define an equivalence/rounding rule between these DBF values
# and SHP XY; both are retained literally and no coordinate is rewritten.
DBF_GEOMETRY_SANITY_BOUND = Decimal("0.001")
FGDC_HORIZONTAL_DATUM = "North American Datum of 1983"
PRODUCER_PUBDATE = "200512"
PRODUCER_RELEASE_DATE = "2005-12-01"
GEOPARQUET_PATH = f"geoparquet/{FILE_SLUG}.parquet"
LICENSE = ("USGS National Atlas FGDC metadata records access constraints 'None' and use constraints "
           "'None; acknowledgment of the National Atlas of the United States of America and/or the "
           "U.S. Geological Survey appreciated'. Final rights review pending.")
READY_MARKER = "metadata/ferrous_metal_mines_candidate_ready.json"
AUDIT_MARKER = "metadata/ferrous_metal_mines_source_audit.json"


class FerrousMinesQuarantineError(ValueError):
    """Unrecognized, incomplete, or drifted source retained under quarantine."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise FerrousMinesQuarantineError("duplicate_json_key")
            result[key] = value
        return result

    value = json.loads(body, object_pairs_hook=pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(
                           FerrousMinesQuarantineError("nonfinite_json")))
    if not isinstance(value, dict):
        raise FerrousMinesQuarantineError("json_object_required")
    return value


def local_only(storage: StagingStorageResource) -> None:
    if (isinstance(storage, PublishedStorageResource) or storage.bucket
            or not storage.use_local or not storage.local_dir):
        raise FerrousMinesQuarantineError("draft_requires_explicit_isolated_local_staging")
    root = Path(storage.local_dir)
    if ".." in root.parts or storage.prefix or any(p.is_symlink() for p in (root, *root.parents)):
        raise FerrousMinesQuarantineError("aliased_staging_refused")
    root.mkdir(parents=True, exist_ok=True)


def bounded_input(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            raise FerrousMinesQuarantineError("unsafe_or_oversize_input")
        body = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    fields = lambda value: (value.st_dev, value.st_ino, value.st_size,
                            value.st_mtime_ns, value.st_ctime_ns, value.st_nlink)
    if len(body) > limit or len(body) != before.st_size or fields(before) != fields(after):
        raise FerrousMinesQuarantineError("incomplete_or_changed_bounded_input")
    return body


def validate_capture_receipt(receipt: object, label: str) -> None:
    if not isinstance(receipt, dict):
        raise FerrousMinesQuarantineError("receipt_object_required:" + label)
    if (type(receipt.get("status")) is not int or receipt["status"] != 200
            or receipt.get("complete_body") is not True
            or type(receipt.get("bytes")) is not int or receipt["bytes"] < 0
            or not isinstance(receipt.get("sha256"), str) or len(receipt["sha256"]) != 64
            or not isinstance(receipt.get("url"), str)
            or not isinstance(receipt.get("final_url"), str)
            or not isinstance(receipt.get("observed_at"), str)):
        raise FerrousMinesQuarantineError("capture_receipt_fields_invalid:" + label)
    parsed = urlsplit(receipt["url"])
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise FerrousMinesQuarantineError("capture_receipt_url_unsafe:" + label)
    try:
        parsed_at = datetime.fromisoformat(receipt["observed_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise FerrousMinesQuarantineError("capture_receipt_timestamp_invalid:" + label) from exc
    if parsed_at.tzinfo is None:
        raise FerrousMinesQuarantineError("capture_receipt_requires_timezone:" + label)


def validate_capture(body: bytes, receipt: dict, label: str) -> None:
    validate_capture_receipt(receipt, label)
    if receipt["bytes"] != len(body) or receipt["sha256"] != digest(body):
        raise FerrousMinesQuarantineError("capture_receipt_length_or_hash_mismatch:" + label)


def quarantine(storage: StagingStorageResource, payload: bytes, receipt: object, reason: str,
               kind: str) -> str:
    location = f"_quarantine/{DATASET_SLUG}/{kind}/{uuid4().hex}"
    if payload:
        write_immutable(storage, f"{location}/body", payload)
    if isinstance(receipt, dict):
        write_immutable(storage, f"{location}/receipt.json", encode(receipt))
    write_immutable(storage, f"{location}/failure.json",
                    encode({"approved": False, "reason": reason}))
    return location


def acquire_preserved_archive(storage: StagingStorageResource, archive_path: Path | None,
                              receipt_path: Path | None) -> tuple[bytes, dict]:
    """Require an explicit preserved producer archive; never fetch a default URL."""
    if archive_path is None or receipt_path is None:
        raise FerrousMinesQuarantineError(
            "explicit_preserved_producer_archive_required_no_default_fetch")
    body = b""
    receipt: dict = {}
    try:
        body = bounded_input(Path(archive_path), MAX_ARCHIVE_BYTES)
        receipt = strict_json(bounded_input(Path(receipt_path), 65_536))
        validate_capture(body, receipt, "producer_archive")
        capture_key = f"_raw-captures/{DATASET_SLUG}/{digest(body)}/archive.body"
        write_immutable(storage, capture_key, body)
        write_immutable(storage, capture_key + ".receipt.json", encode(receipt))
        return body, receipt
    except (OSError, ValueError, TypeError, KeyError) as exc:
        location = quarantine(storage, body, receipt, str(exc), "acquired-failures")
        raise FerrousMinesQuarantineError(f"{exc}; retained:{location}") from exc


def acquire_producer_metadata(storage: StagingStorageResource, metadata_path: Path | None,
                              metadata_receipt_path: Path | None,
                              capture_root: Path) -> tuple[bytes, dict, bytes | None]:
    """Capture the surviving producer FGDC document, or replay an explicit preserved copy.

    Live captures retain both wire and decoded bytes; a preserved replay has no
    wire bytes and its receipt must already pin the same document digest.
    """
    if metadata_path is not None:
        if metadata_receipt_path is None:
            raise FerrousMinesQuarantineError("preserved_metadata_requires_original_receipt")
        body = bounded_input(Path(metadata_path), MAX_METADATA_BYTES)
        receipt = strict_json(bounded_input(Path(metadata_receipt_path), 65_536))
        validate_capture(body, receipt, "producer_metadata")
        return body, receipt, None
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                      headers={"User-Agent": "hifld-next-datasets/usgs-national-atlas-draft"}) as client:
        body, receipt = capture_get(client, METADATA_URL, capture_root, "producer_metadata",
                                    max_bytes=MAX_METADATA_BYTES)
    if len(body) != METADATA_BYTES or digest(body) != METADATA_SHA256:
        raise FerrousMinesQuarantineError("producer_metadata_changed_truncated_or_unknown_bytes")
    return body, receipt, (capture_root / "producer_metadata.wire").read_bytes()


def decode_dbf(body: bytes) -> tuple[list[dict], list[tuple[str, str, int, int]]]:
    if len(body) < 33:
        raise FerrousMinesQuarantineError("truncated_dbf_header")
    count, header, width = struct.unpack_from("<IHH", body, 4)
    if count < 1 or header < 33 or (header - 1) % 32 != 0 or header > len(body) or width < 1:
        raise FerrousMinesQuarantineError("incomplete_or_unsafe_dbf_header")
    fields = []
    offset = 32
    while offset + 32 <= header and body[offset] != 13:
        descriptor = body[offset:offset + 32]
        fields.append((descriptor[:11].split(b"\0")[0].decode("ascii"), chr(descriptor[11]),
                       descriptor[16], descriptor[17]))
        offset += 32
    if tuple(name for name, _kind, _width, _scale in fields) != DBF_FIELD_NAMES:
        raise FerrousMinesQuarantineError("missing_renamed_or_extra_source_columns")
    if tuple(kind for _name, kind, _width, _scale in fields) != DBF_FIELD_KINDS:
        raise FerrousMinesQuarantineError("source_field_type_drift")
    if body[offset] != 13:
        raise FerrousMinesQuarantineError("dbf_header_terminator_missing")
    if len(body) not in (header + count * width, header + count * width + 1):
        raise FerrousMinesQuarantineError("incomplete_dbf_record_population")
    if len(body) == header + count * width + 1 and body[-1] != 26:
        raise FerrousMinesQuarantineError("unexpected_dbf_tail")
    rows = []
    for fid in range(count):
        record = body[header + fid * width:header + (fid + 1) * width]
        if record[:1] != b" ":
            raise FerrousMinesQuarantineError("deleted_or_unknown_dbf_record")
        offset = 1
        row: dict = {"source_fid": fid, "source_shp_record_number": fid + 1}
        for name, kind, size, scale in fields:
            raw = record[offset:offset + size]
            offset += size
            if kind == "C":
                if any(char > 127 or char < 32 for char in raw):
                    raise FerrousMinesQuarantineError("undeclared_nonASCII_source_encoding")
                row[name] = raw.rstrip(b" ").decode("ascii")
            else:
                try:
                    text = raw.decode("ascii", errors="strict").strip()
                except UnicodeDecodeError as exc:
                    raise FerrousMinesQuarantineError(
                        f"undeclared_nonASCII_source_encoding:{name}") from exc
                if not text:
                    raise FerrousMinesQuarantineError(f"null_source_value_not_representable:{name}")
                if not text.replace("-", "", 1).replace(".", "", 1).isdigit():
                    raise FerrousMinesQuarantineError(f"nonfinite_or_unparseable_DBF_number:{name}")
                value = Decimal(text)
                if value.as_tuple().exponent != -scale:
                    raise FerrousMinesQuarantineError(f"declared_DBF_scale_drift:{name}")
                row[name] = value
        rows.append(row)
    return rows, fields


def decode_points(body: bytes, index: bytes, count: int) -> list[bytes]:
    if len(body) < 100 or len(index) != 100 + 8 * count:
        raise FerrousMinesQuarantineError("incomplete_SHP_SHX")
    for data in (body, index):
        if (struct.unpack_from(">i", data)[0] != 9994
                or struct.unpack_from(">i", data, 24)[0] * 2 != len(data)
                or struct.unpack_from("<ii", data, 28) != (1000, 1)):
            raise FerrousMinesQuarantineError("native_geometry_header_type_or_length_drift")
    out = []
    offset = 100
    for fid in range(count):
        if offset + 28 > len(body):
            raise FerrousMinesQuarantineError("incomplete_point_records")
        record, words = struct.unpack_from(">ii", body, offset)
        shape_type, x, y = struct.unpack_from("<idd", body, offset + 8)
        if (record != fid + 1 or words != 10 or shape_type != 1
                or struct.unpack_from(">ii", index, 100 + 8 * fid) != (offset // 2, 10)
                or not all(math.isfinite(value) for value in (x, y))):
            raise FerrousMinesQuarantineError("SHP_SHX_record_ID_offset_type_or_coordinate_drift")
        out.append(shapely.Point(x, y).wkb)
        offset += 28
    if offset != len(body):
        raise FerrousMinesQuarantineError("extra_point_records")
    geometry = shapely.from_wkb(out)
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):
        raise FerrousMinesQuarantineError("invalid_empty_or_nonpoint_native_geometry")
    return out


def _native_crs(members: dict[str, bytes], metadata_body: bytes | None) -> pyproj.CRS:
    prj = members.get(f"{STEM}.prj")
    if prj is not None:
        try:
            crs = pyproj.CRS(prj.decode("ascii", errors="strict"))
        except (UnicodeDecodeError, pyproj.exceptions.CRSError) as exc:
            raise FerrousMinesQuarantineError("unparseable_native_prj") from exc
    else:
        # The original producer tarball contains no .prj file. Its independently
        # hash-pinned FGDC XML explicitly declares NAD83 and decimal degrees.
        if metadata_body is None or digest(metadata_body) != METADATA_SHA256:
            raise FerrousMinesQuarantineError("missing_prj_requires_exact_producer_fgdc_metadata")
        try:
            root = ET.fromstring(metadata_body)
            datum = root.findtext(".//horizdn", "").strip()
            units = root.findtext(".//geogunit", "").strip()
        except ET.ParseError as exc:
            raise FerrousMinesQuarantineError("invalid_pinned_producer_fgdc_xml") from exc
        if datum != FGDC_HORIZONTAL_DATUM or units != "Decimal degrees":
            raise FerrousMinesQuarantineError("fgdc_native_crs_declaration_missing_or_drifted")
        crs = pyproj.CRS.from_epsg(4269)
    if not crs.is_geographic:
        raise FerrousMinesQuarantineError("native_prj_not_geographic")
    return crs


def inspect_archive(body: bytes, metadata_body: bytes | None = None
                    ) -> tuple[list[dict], dict, list[tuple[str, str, int, int]]]:
    if not body.startswith(b"\x1f\x8b"):
        raise FerrousMinesQuarantineError("changed_truncated_or_unknown_producer_archive")
    members: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
        for member in archive:
            if not member.isfile() or PurePosixPath(member.name).name != member.name:
                raise FerrousMinesQuarantineError("unsafe_or_nonregular_archive_member")
            name = PurePosixPath(member.name).name
            if name in members:
                raise FerrousMinesQuarantineError("duplicate_archive_member")
            stream = archive.extractfile(member)
            members[name] = stream.read() if stream is not None else b""
    roster = set(members)
    if not REQUIRED_MEMBERS <= roster or roster - REQUIRED_MEMBERS - OPTIONAL_MEMBERS:
        raise FerrousMinesQuarantineError("unexpected_missing_or_duplicate_archive_member")
    rows, fields = decode_dbf(members[f"{STEM}.dbf"])
    count = len(rows)
    geometry = decode_points(members[f"{STEM}.shp"], members[f"{STEM}.shx"], count)
    for row, wkb in zip(rows, geometry):
        row["geometry"] = wkb
    id_field = next((field for field in fields if field[0] == ID_FIELD), None)
    if id_field is None or id_field[1] != "N" or id_field[3] != 0:
        raise FerrousMinesQuarantineError("source_identifier_not_integer")
    for row in rows:
        row[ID_FIELD] = int(row[ID_FIELD])
    ids = [row[ID_FIELD] for row in rows]
    if any(value <= 0 for value in ids) or len(set(ids)) != count:
        raise FerrousMinesQuarantineError("missing_duplicate_or_nonpositive_source_identifier")
    crs = _native_crs(members, metadata_body)
    deviations = []
    coordinate_diagnostics = []
    for row in rows:
        point = shapely.from_wkb(row["geometry"])
        longitude_deviation = abs(row["LONGITUDE"] - Decimal(repr(point.x)))
        latitude_deviation = abs(row["LATITUDE"] - Decimal(repr(point.y)))
        deviation = max(longitude_deviation, latitude_deviation)
        if deviation >= DBF_GEOMETRY_SANITY_BOUND:
            raise FerrousMinesQuarantineError("dbf_coordinate_geometry_drift")
        deviations.append(float(deviation))
        coordinate_diagnostics.append({
            "source_fid": row["source_fid"],
            "source_shp_record_number": row["source_shp_record_number"],
            "source_id": row[ID_FIELD],
            "dbf_latitude_literal": str(row["LATITUDE"]),
            "dbf_longitude_literal": str(row["LONGITUDE"]),
            "shp_x_longitude_literal": repr(point.x),
            "shp_y_latitude_literal": repr(point.y),
            "longitude_absolute_difference_degrees": str(longitude_deviation),
            "latitude_absolute_difference_degrees": str(latitude_deviation),
            "max_absolute_difference_degrees": str(deviation),
        })
    bounds = shapely.total_bounds(shapely.from_wkb([row["geometry"] for row in rows])).tolist()
    schema_sha = digest(encode([list(item) for item in zip(DBF_FIELD_NAMES, DBF_FIELD_KINDS)]))
    record_sha = digest(encode([
        {name: (str(row[name]) if isinstance(row[name], Decimal) else row[name])
         for name in DBF_FIELD_NAMES} for row in rows]))
    geometry_sha = digest(encode([[row[ID_FIELD], row["geometry"].hex()] for row in rows]))
    audit = {
        "approved": False,
        "status": "producer_source_fidelity_pass",
        "producer": PUBLISHER,
        "producer_pubdate": PRODUCER_PUBDATE,
        "fgdc_metadata_url": METADATA_URL,
        "fgdc_metadata_sha256": METADATA_SHA256,
        "feature_count": count,
        "source_ids": {ID_FIELD: {"unique": len(set(ids)), "nulls": 0, "min": min(ids),
                                  "max": max(ids),
                                  "contiguous_from_one": ids == list(range(1, count + 1))}},
        "dbf_physical_fields": [{"name": name, "kind": kind, "width": width, "scale": scale}
                                for name, kind, width, scale in fields],
        "native_crs_wkt": crs.to_wkt(),
        "native_crs_is_geographic": crs.is_geographic,
        "fgdc_declared_horizontal_datum": FGDC_HORIZONTAL_DATUM,
        "datum_contradiction": "83" not in (crs.datum.name or "").replace(" ", ""),
        "native_bounds": bounds,
        "proposed_dbf_geometry_sanity_bound_degrees": str(DBF_GEOMETRY_SANITY_BOUND),
        "dbf_geometry_max_coordinate_deviation_degrees": max(deviations),
        "dbf_geometry_coordinate_diagnostics": coordinate_diagnostics,
        "coordinate_relationship_status": (
            "diagnostic_only_unapproved; FGDC defines no DBF-to-SHP equivalence or rounding rule; "
            "literal DBF coordinate attributes and native SHP XY are both retained unchanged"
        ),
        "archive_members": {name: {"bytes": len(data), "sha256": digest(data)}
                            for name, data in sorted(members.items())},
        "source_schema_sha256": schema_sha,
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "candidate_scope": "unapproved preserved producer archive; no release/mapping acceptance",
    }
    return rows, audit, fields


def arrow_schema(fields: list[tuple[str, str, int, int]], crs: pyproj.CRS,
                 bounds: list[float]) -> pa.Schema:
    schema_fields = [
        pa.field("source_fid", pa.int64(), nullable=False),
        pa.field("source_shp_record_number", pa.int32(), nullable=False),
    ]
    for name, kind, width, scale in fields:
        if kind == "C":
            schema_fields.append(pa.field(name, pa.string(), nullable=False))
        elif scale == 0 and width <= 18:
            schema_fields.append(pa.field(name, pa.int64(), nullable=False))
        else:
            if width > 38:
                raise FerrousMinesQuarantineError(f"source_numeric_width_not_representable:{name}")
            schema_fields.append(pa.field(name, pa.decimal128(width, scale), nullable=False))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": crs.to_json_dict(),
        "bbox": bounds}}}
    return pa.schema(schema_fields).with_metadata({b"geo": encode(geo)})


def parquet_bytes(rows: list[dict], fields: list[tuple[str, str, int, int]],
                  crs: pyproj.CRS, bounds: list[float]) -> bytes:
    schema = arrow_schema(fields, crs, bounds)
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def processing_identity() -> dict:
    return shared_processing_identity(Path(__file__).parents[3])


def metadata_input(receipt: dict) -> dict:
    return {
        "input_id": f"usgs-national-atlas-fgdc-metadata:{METADATA_SHA256}",
        "producer": AGENCY,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": METADATA_URL,
        "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": METADATA_BYTES,
        "sha256": METADATA_SHA256,
        "release_date": PRODUCER_RELEASE_DATE,
        "license": LICENSE,
        "attribution": "U.S. Geological Survey, National Atlas of the United States; FGDC metadata retained unchanged.",
        "lineage": ("Surviving producer FGDC metadata for the retired National Atlas 'Ferrous Metal "
                    f"Mines' layer ({STEM}); documents the December 2005 Minerals Information Team "
                    "publication of operations active in 2003 and the removed dds.cr.usgs.gov direct "
                    "download."),
    }


def archive_input(receipt: dict) -> dict:
    return {
        "input_id": "usgs-national-atlas-producer-archive:" + receipt["sha256"],
        "producer": AGENCY,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "archive",
        "canonical_url": ARCHIVE_CANONICAL_URL,
        "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "release_date_unknown_reason": (
            "The preserved capture is a historical producer archive; the FGDC metadata pubdate "
            "(200512) documents the December 2005 National Atlas publication, but the exact edition "
            "transfer date of this preserved archive copy is not asserted."),
        "license": LICENSE,
        "attribution": "U.S. Geological Survey National Atlas producer archive preserved as provided.",
        "lineage": ("Original producer release location removed from dds.cr.usgs.gov (404 observed "
                    "2026-10-21); the inventoried DataLumos project 239022 V1 rejects anonymous "
                    "access; capture provenance rests entirely on the supplied preserved receipt."),
    }


def manifest_for(identity: dict, metadata_receipt: dict, archive_receipt: dict,
                 parquet_pin: dict) -> dict:
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "approved": False,
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "processing_fingerprint": digest(encode(identity)),
        "title": "Ferrous Metal Mines — unapproved preserved producer archive draft",
        "publisher": PUBLISHER,
        "agency": AGENCY,
        "source_url": METADATA_URL,
        "license": LICENSE,
        "date_issued": PRODUCER_RELEASE_DATE,
        "inputs": [archive_input(archive_receipt), metadata_input(metadata_receipt)],
        "transformations": [{
            "operation": "preserved_producer_shapefile_lossless_decode",
            "parameters": {
                "archive_members": ("Original tar.gz member bytes retained under raw/; only the "
                                    f"declared {STEM} shapefile roster is accepted."),
                "attributes": ("All DBF columns and values preserved exactly; fixed-point decimals "
                               "keep declared width/scale; strings decode ASCII with padding trimmed."),
                "IDs": ("MINOP3X020 internal feature number plus implicit source_fid/SHP record "
                        "number retained; no substitute row identifiers."),
                "geometry": ("Native SHP point coordinates preserved without reprojection or repair; "
                             "SHX index cross-checked."),
                "coordinate_cross_check": ("DBF LATITUDE/LONGITUDE decimals compared against SHP "
                                           "point coordinates within documented rounding tolerance."),
                "candidate_status": "unapproved; schema/ID/coverage/rights/release decisions pending",
            },
        }],
        "unsupported_output_formats": {
            "shapefile_zip": ("Rebuilt archives are not byte-equivalent producer artifacts; no "
                              "shapefile output mapping is approved."),
            "geopackage": ("DBF decimal width/scale cannot be silently coerced to GPKG REAL/INTEGER "
                           "declarations; no migration approved."),
            "pmtiles": ("Visualization requires reprojection and quantization the source does not "
                        "declare; not approved."),
        },
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": ("Preserve original IDs, fixed-point attribute values, native point WKB, "
                           "and native CRS metadata as typed GeoParquet."),
                "objects": [{"path": GEOPARQUET_PATH, "sha256": parquet_pin["sha256"],
                             "bytes": parquet_pin["bytes"]}],
            }},
        },
        "lineage": ("USGS National Atlas of the United States, Minerals Information Team, December "
                    "2005 publication (2003 active operations); retired and explicitly not updated. "
                    "HIFLD Open distribution ended; DataLumos project 239022 V1 is the inventoried "
                    "archive and rejects anonymous access. No maintained successor feed is asserted."),
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    return manifest


def candidate_object_names(metadata_wire: bool, members: dict[str, bytes]) -> set[str]:
    names = {
        "raw/archive.body", "raw/archive.receipt.json",
        "raw/producer_metadata.body", "raw/producer_metadata.receipt.json",
        GEOPARQUET_PATH, "candidate/typed-source.parquet",
        "metadata/source_manifest.json", "metadata/processing_identity.json",
        AUDIT_MARKER, "metadata/proposed_source_contract.json",
    }
    if metadata_wire:
        names.add("raw/producer_metadata.wire")
    names |= {f"raw/archive_members/{name}" for name in members}
    return names


def construction_record_key(version: str) -> str:
    return f"_construction-state/{DATASET_SLUG}/{version}.json"


_METADATA_RECEIPT_STABLE_FIELDS = ("complete_body", "status", "bytes", "sha256",
                                   "url", "final_url")


def reconcile_cache(storage: StagingStorageResource, prefix: str, version: str,
                    payloads: dict[str, bytes], identity: dict, fingerprint: str) -> None:
    root = Path(storage.local_dir) / prefix
    if not (root / READY_MARKER).is_file():
        raise FerrousMinesQuarantineError("incomplete_existing_candidate_missing_ready")
    # Cache reuse routes the original stored receipt and wire bytes through
    # stage_ferrous_metal_mines before this check, so the payload map here is
    # already the immutable candidate evidence.
    stored = strict_json((root / READY_MARKER).read_bytes())
    if (stored.get("version") != version or stored.get("approved") is not False
            or stored.get("processing_fingerprint") != fingerprint
            or set(stored.get("objects", {})) != set(payloads)):
        raise FerrousMinesQuarantineError("ready_identity_or_inventory_drift")
    for name, expected in payloads.items():
        if storage.read_key(f"{prefix}/{name}") != expected:
            raise FerrousMinesQuarantineError(f"source_owned_object_changed:{name}")
    for name, expected_sha in stored["objects"].items():
        if digest(storage.read_key(f"{prefix}/{name}")) != expected_sha:
            raise FerrousMinesQuarantineError(f"ready_object_digest_changed:{name}")
    actual = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink != 1) for p in root.rglob("*")):
        raise FerrousMinesQuarantineError("linked_staging_object")
    derived = {
        "metadata/quality_manifest.json", "metadata/data_dictionary.json",
        "metadata/resolved_provenance.json",
        "metadata/source/source_manifest.json", "metadata/source/quality_manifest.json",
        "metadata/source/data_dictionary.json",
    } | STAGED_RELEASE_DERIVATIVE_NAMES
    if actual - set(payloads) - {READY_MARKER} - derived:
        raise FerrousMinesQuarantineError("unknown_existing_candidate_object")
    try:
        validate_staged_release_derivatives(storage, prefix, {f"{prefix}/{name}" for name in actual})
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        raise FerrousMinesQuarantineError("invalid_staged_release_derivatives") from exc
    manifest = strict_json(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
    if manifest.get("code_commit") != identity["code_commit"]:
        raise FerrousMinesQuarantineError("cached_candidate_code_identity_drift")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    if storage.object_exists(f"{prefix}/metadata/resolved_provenance.json"):
        resolved = provenance_projection(manifest)
        if strict_json(storage.read_key(f"{prefix}/metadata/resolved_provenance.json")) != resolved:
            raise FerrousMinesQuarantineError("cached_resolved_provenance_drift")


def expose_candidate(storage: StagingStorageResource, prefix: str, version: str,
                     payloads: dict[str, bytes], fingerprint: str, identity: dict) -> None:
    """Write the candidate only after every object is complete, then CAS-seal it."""
    target = Path(storage.local_dir) / storage._ensure_prefixed(prefix)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ferrous-build-", dir=target.parent) as temporary:
        build = Path(temporary)
        for name, contents in payloads.items():
            destination = build / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(contents)
        ready = {"version": version, "approved": False, "processing_fingerprint": fingerprint,
                 "objects": {name: digest(contents) for name, contents in sorted(payloads.items())}}
        (build / READY_MARKER).write_bytes(encode(ready))
        if target.exists() or target.is_symlink():
            raise FerrousMinesQuarantineError("candidate_appeared_before_atomic_exposure")
        os.rename(build, target)
    for name, contents in sorted(payloads.items()):
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(contents) or storage.sha256_key(key) != digest(contents):
            raise FerrousMinesQuarantineError("candidate_readback_failed:" + name)
    seal_first_construction(storage, construction_record_key(version), version, fingerprint,
                            canonical_construction_bundle(version, fingerprint, {
                                "processing_identity": identity,
                                "objects": {name: {"bytes": len(body), "sha256": digest(body)}
                                            for name, body in payloads.items()}}))


def stage_ferrous_metal_mines(storage: StagingStorageResource, *, archive_path: Path | None = None,
                              archive_receipt_path: Path | None = None,
                              metadata_path: Path | None = None,
                              metadata_receipt_path: Path | None = None) -> dict:
    local_only(storage)
    archive_body, archive_receipt = acquire_preserved_archive(
        storage, archive_path, archive_receipt_path)
    capture_root = Path(tempfile.mkdtemp(prefix="ferrous-metal-mines-capture-"))
    try:
        try:
            metadata_body, metadata_receipt, metadata_wire = acquire_producer_metadata(
                storage, metadata_path, metadata_receipt_path, capture_root)
        except FerrousMinesQuarantineError as exc:
            location = quarantine(storage, b"", None, str(exc), "metadata-failures")
            raise FerrousMinesQuarantineError(f"{exc}; retained:{location}") from exc
        rows, audit, fields = inspect_archive(archive_body, metadata_body)
        members_bytes = _archive_members(archive_body)
        identity = processing_identity()
        fingerprint = digest(encode(identity))
        version = content_derived_version(audit["source_record_sha256"],
                                          audit["source_geometry_sha256"],
                                          audit["source_schema_sha256"], identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        parquet = parquet_bytes(rows, fields,
                                pyproj.CRS.from_wkt(audit["native_crs_wkt"]),
                                audit["native_bounds"])
        parquet_pin = {"sha256": digest(parquet), "bytes": len(parquet)}
        # A candidate's provenance is immutable. On cache reuse the fresh
        # capture only proves content identity via stable receipt fields; the
        # original stored receipt and wire bytes remain the candidate evidence.
        prefix_exists = storage.object_exists(f"{prefix}/{READY_MARKER}")
        if prefix_exists:
            stored_metadata_receipt = strict_json(
                storage.read_key(f"{prefix}/raw/producer_metadata.receipt.json"))
            if any(stored_metadata_receipt.get(field) != metadata_receipt.get(field)
                   for field in _METADATA_RECEIPT_STABLE_FIELDS):
                raise FerrousMinesQuarantineError("cached_metadata_content_changed")
            metadata_receipt = stored_metadata_receipt
            if storage.object_exists(f"{prefix}/raw/producer_metadata.wire"):
                metadata_wire = storage.read_key(f"{prefix}/raw/producer_metadata.wire")
        manifest = manifest_for(identity, metadata_receipt, archive_receipt, parquet_pin)
        payloads = {
            "raw/archive.body": archive_body,
            "raw/archive.receipt.json": encode(archive_receipt),
            "raw/producer_metadata.body": metadata_body,
            "raw/producer_metadata.receipt.json": encode(metadata_receipt),
            GEOPARQUET_PATH: parquet,
            "candidate/typed-source.parquet": parquet,
            "metadata/source_manifest.json": encode(manifest),
            "metadata/processing_identity.json": encode(identity),
            AUDIT_MARKER: encode(audit),
            "metadata/proposed_source_contract.json": encode({
                "approved": False,
                "id_fields": [ID_FIELD, "source_fid", "source_shp_record_number"],
                "geometry": True,
                "crs": audit["native_crs_wkt"],
                "columns": {name: {
                    "type": "decimal" if kind == "N" else "string",
                    "width": width, "scale": scale, "nullable": False}
                    for name, kind, width, scale in fields},
                "acceptance": "not_established",
            }),
        }
        payloads.update({f"raw/archive_members/{name}": data
                         for name, data in members_bytes.items()})
        if metadata_wire is not None:
            payloads["raw/producer_metadata.wire"] = metadata_wire
        if set(payloads) != candidate_object_names(metadata_wire is not None, members_bytes):
            raise FerrousMinesQuarantineError("incomplete_new_candidate_inventory")
        if prefix_exists:
            reconcile_cache(storage, prefix, version, payloads, identity, fingerprint)
            return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                    "feature_count": audit["feature_count"], "reused": True,
                    "source_population_complete": True,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "source_schema_sha256": audit["source_schema_sha256"],
                    "accepted": False}
        try:
            expose_candidate(storage, prefix, version, payloads, fingerprint, identity)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            location = quarantine(storage, b"", None, str(exc), "candidate-failures")
            raise FerrousMinesQuarantineError(f"{exc}; retained:{location}") from exc
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["feature_count"], "reused": False,
                "source_population_complete": True,
                "source_record_sha256": audit["source_record_sha256"],
                "source_geometry_sha256": audit["source_geometry_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "accepted": False}
    except FerrousMinesQuarantineError:
        raise
    except (OSError, ValueError, TypeError, KeyError, tarfile.TarError,
            pyproj.exceptions.ProjError, shapely.errors.ShapelyError) as exc:
        location = quarantine(storage, b"", None, str(exc), "pipeline-failures")
        raise FerrousMinesQuarantineError(f"{exc}; retained:{location}") from exc
    finally:
        shutil.rmtree(capture_root, ignore_errors=True)


def _archive_members(body: bytes) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
        for member in archive:
            if member.isfile() and PurePosixPath(member.name).name == member.name:
                stream = archive.extractfile(member)
                members[PurePosixPath(member.name).name] = stream.read() if stream else b""
    return members
