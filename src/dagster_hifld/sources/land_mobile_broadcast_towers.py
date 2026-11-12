"""FCC ULS Land Mobile Broadcast Auxiliary tower locations, draft source adapter.

Issue #132 (land-mobile-broadcast-towers-1). The producer input is the FCC's own
maintained weekly "Land Mobile - Broadcast Auxiliary" complete license-records
file (l_LMbcast.zip) from the Universal Licensing System public access files.
Tower features are the fixed (Location Type Code F) location records of the
official LO table, joined to the official HD license header and EN licensee
records. The FCC states the licensing data contain known latitude, longitude and
ground elevation errors, so invalid coordinates keep their raw DMS values with
null geometry instead of being dropped or repaired.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import struct
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "land-mobile-broadcast-towers-1"
SOURCE_URL = "https://data.fcc.gov/download/pub/uls/complete/l_LMbcast.zip"
LANDING = "https://www.fcc.gov/wireless/data/public-access-files-database-downloads"
DAILY_WEEKLY = "https://www.fcc.gov/uls/transactions/daily-weekly"
PUBLISHER = "Federal Communications Commission, Wireless Telecommunications Bureau"
MAX_SOURCE_BYTES = 64_000_000
MAX_STAGE_BYTES = 256_000_000

# Official member roster of the current weekly l_LMbcast.zip license-records
# archive (verified 2026-11-12). The `counts` manifest lists every table.
EXPECTED_MEMBERS = frozenset(
    ["AN.dat", "AS.dat", "BC.dat", "BF.dat", "CO.dat", "CP.dat", "EM.dat", "EN.dat",
     "FF.dat", "FR.dat", "HD.dat", "HS.dat", "LA.dat", "LF.dat", "LM.dat", "LO.dat",
     "LS.dat", "OP.dat", "SC.dat", "SF.dat", "counts", "ir.dat"])

# Radio service codes the FCC scopes into the Land Mobile - Broadcast Auxiliary
# file (FCC Broadcast Auxiliary page): LP Broadcast Auxiliary Low Power,
# RP Broadcast Auxiliary Remote Pickup, LV Low Power Wireless Assist Video
# Devices. Any other code in this file is an FCC scope change and quarantines.
EXPECTED_SERVICE_CODES = frozenset({"LP", "RP", "LV"})

# Official Public Access LO (Location) record layout, positions 1..51 as named in
# the FCC Public Access Database Definitions; verified against the live file
# (every LO record carries exactly 51 pipe-delimited fields, 2026-11-12).
LOCATION_FIELDS: tuple[tuple[str, str], ...] = (
    ("record_type", "string"),
    ("unique_system_identifier", "int64"),
    ("uls_file_number", "string"),
    ("ebf_number", "string"),
    ("call_sign", "string"),
    ("location_action_performed", "string"),
    ("location_type_code", "string"),
    ("location_class_code", "string"),
    ("location_number", "int64"),
    ("site_status", "string"),
    ("corresponding_fixed_location", "int64"),
    ("location_address", "string"),
    ("location_city", "string"),
    ("location_county", "string"),
    ("location_state", "string"),
    ("radius_of_operation", "float64"),
    ("area_of_operation_code", "string"),
    ("clearance_indicator", "string"),
    ("ground_elevation", "float64"),
    ("latitude_degrees", "int64"),
    ("latitude_minutes", "int64"),
    ("latitude_seconds", "float64"),
    ("latitude_direction", "string"),
    ("longitude_degrees", "int64"),
    ("longitude_minutes", "int64"),
    ("longitude_seconds", "float64"),
    ("longitude_direction", "string"),
    ("max_latitude_degrees", "int64"),
    ("max_latitude_minutes", "int64"),
    ("max_latitude_seconds", "float64"),
    ("max_latitude_direction", "string"),
    ("max_longitude_degrees", "int64"),
    ("max_longitude_minutes", "int64"),
    ("max_longitude_seconds", "float64"),
    ("max_longitude_direction", "string"),
    ("nepa", "string"),
    ("quiet_zone_notification_date", "string"),
    ("tower_registration_number", "string"),
    ("height_of_support_structure", "float64"),
    ("overall_height_of_structure", "float64"),
    ("structure_type", "string"),
    ("airport_id", "string"),
    ("location_name", "string"),
    ("units_hand_held", "int64"),
    ("units_mobile", "int64"),
    ("units_temp_fixed", "int64"),
    ("units_aircraft", "int64"),
    ("units_itinerant", "int64"),
    ("location_status_code", "string"),
    ("location_status_date", "string"),
    ("earth_station_agreement", "string"),
)
LO_EXPECTED_FIELD_COUNT = len(LOCATION_FIELDS)
LOCATION_TYPE_INDEX = [name for name, _ in LOCATION_FIELDS].index("location_type_code")

# Fixed locations are the tower-bearing licensed sites. Per the ULS code
# definitions, LO Location Type Code F = Fixed (fixed stations); M, I, L, T and
# 6 are mobile, itinerant, temporary or control-station location types.
FIXED_LOCATION_TYPE = "F"

# Official axis directions of the LO DMS columns per the Public Access Database
# Definitions: latitude_direction carries only N/S and longitude_direction only
# E/W; a direction from the wrong axis is invalid, never a mapped coordinate.
LATITUDE_DIRECTIONS = ("N", "S")
LONGITUDE_DIRECTIONS = ("E", "W")

# HD license-header layout carries 59 positions (51-55 added 2019/2020 and 56-59
# for 900 MHz certifications; verified 59 fields on every HD record 2026-11-12).
HD_EXPECTED_FIELD_COUNT = 59
# 0-based field indices of the HD join columns (official positions 2, 5, 6, 7, 8, 9).
HD_JOIN_INDEXES = {
    "license_status": 5, "radio_service_code": 6, "grant_date": 7, "expired_date": 8,
}
HD_CALL_SIGN_INDEX = 4
# EN entity records: one licensee ('L' Entity Type) per license was observed.
EN_EXPECTED_FIELD_COUNT = 30
# Official positions 5-8 are Call Sign, Entity Type, Licensee ID and Entity Name
# (0-based indices 4-7); the licensee row's call sign must agree with the license.
EN_CALL_SIGN_INDEX = 4
EN_ENTITY_TYPE_INDEX = 5
EN_LICENSEE_INDEXES = {"licensee_id": 6, "licensee_name": 7}
LICENSEE_ENTITY_TYPE = "L"

JOINED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("license_status", "string"), ("radio_service_code", "string"),
    ("grant_date", "string"), ("expired_date", "string"),
    ("licensee_id", "string"), ("licensee_name", "string"),
)
GEOMETRY_COLUMN = "geometry"
OUTPUT_FIELD_TYPES: tuple[tuple[str, str], ...] = tuple(LOCATION_FIELDS) + JOINED_COLUMNS + ((GEOMETRY_COLUMN, "binary"),)
NAD83 = CRS.from_epsg(4269)
CRS_DECLARATION_BASIS = (
    "FCC ULS license-search help states ULS coordinates are referenced in NAD83; "
    "the GeoParquet geometry column is declared EPSG:4269 on that basis. Final "
    "datum declaration remains a source acceptance decision.")
KNOWN_ERRORS_NOTE = ("The FCC states the licensing data contain known latitude, "
                     "longitude and ground elevation errors; such records keep "
                     "their raw DMS columns with null geometry.")


class ULSDraftError(ValueError):
    """Incomplete, malformed, or drifted official FCC ULS response."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _records(table: bytes, name: str, required_field_count: int) -> list[list[str]]:
    """Split one official ULS table on \\n boundaries, keeping raw CR characters.

    Embedded CR characters occur inside field values, so records are delimited
    on b"\\n" only and a trailing CR is stripped. Every record must start with
    its record type and carry exactly the official field count.
    """
    if not table.endswith(b"\n"):
        raise ULSDraftError(f"uls_table_missing_final_newline:{name}")
    records = []
    for raw in table.split(b"\n")[:-1]:
        text = raw.rstrip(b"\r").decode("latin-1")
        if not text.startswith(name + "|"):
            raise ULSDraftError(f"uls_record_type_drift:{name}")
        fields = text.split("|")
        if len(fields) != required_field_count:
            raise ULSDraftError(f"uls_field_count_drift:{name}:{len(fields)}")
        records.append(fields)
    return records


_MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
           "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def _creation_date_iso(creation: str) -> str:
    """ISO date of the producer-declared local file-creation timestamp.

    The counts manifest stamps the weekly file in FCC local time, e.g.
    "Sun Nov  8 09:29:39 EST 2026"; the declared local calendar date is kept as
    the release date without timezone shifting.
    """
    match = re.match(r"\w{3} (\w{3}) +(\d+) \d{2}:\d{2}:\d{2} (?:EST|EDT) (\d{4})$", creation)
    if not match or match.group(1) not in _MONTHS:
        raise ULSDraftError(f"uls_counts_creation_date_unparseable:{creation}")
    month, day, year = _MONTHS[match.group(1)], int(match.group(2)), int(match.group(3))
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        raise ULSDraftError(f"uls_counts_creation_date_invalid:{creation}") from None


def _counts_manifest(counts: bytes) -> dict[str, object]:
    """Read the producer's own per-table record counts and creation timestamp."""
    text = counts.decode("latin-1")
    match = re.search(r"File Creation Date:\s*(\S.*)", text)
    if not match or not match.group(1).strip():
        raise ULSDraftError("uls_counts_creation_date_missing")
    table_names = {member[:-4] for member in EXPECTED_MEMBERS if member.endswith(".dat")}
    table_counts: dict[str, int] = {}
    for line in text.splitlines():
        entry = re.match(r"\s*(\d+) \S+/(\w+)\.dat$", line)
        if not entry:
            continue
        if entry.group(2) not in table_names:
            raise ULSDraftError(f"uls_counts_unexpected_table:{entry.group(2)}")
        table_counts[entry.group(2)] = int(entry.group(1))
    missing = sorted(table_names - set(table_counts))
    if missing:
        raise ULSDraftError(f"uls_counts_missing_table:{missing}")
    return {"file_creation_date": match.group(1).strip(),
            "file_creation_date_iso": _creation_date_iso(match.group(1).strip()),
            "table_counts": dict(sorted(table_counts.items()))}


def _cast(field: str, kind: str) -> object:
    if field == "":
        return None
    if kind == "int64":
        try:
            return int(field)
        except ValueError:
            raise ULSDraftError(f"uls_integer_field_unparsable:{field}") from None
    if kind == "float64":
        try:
            value = float(field)
        except ValueError:
            raise ULSDraftError(f"uls_numeric_field_unparsable:{field}") from None
        if value != value or value in (float("inf"), float("-inf")):
            raise ULSDraftError(f"uls_numeric_field_nonfinite:{field}")
        return value
    return field


def _dms_to_decimal(degrees: object, minutes: object, seconds: object,
                    direction: object, *, max_degrees: int,
                    directions: tuple[str, str]) -> float | None:
    """Native DMS to decimal degrees; invalid or incomplete values stay None.

    ``directions`` pins the axis (latitude N/S, longitude E/W), so a direction
    borrowed from the wrong axis is invalid. The composed decimal value is bound
    too: a degrees-only check would accept 90°30' N as a valid latitude.
    """
    if None in (degrees, minutes, seconds, direction):
        return None
    if not 0 <= degrees <= max_degrees or not 0 <= minutes < 60 or not 0 <= seconds < 60:
        return None
    if direction not in directions:
        return None
    decimal = degrees + minutes / 60 + seconds / 3600
    if decimal > max_degrees:
        return None
    if direction in ("S", "W"):
        decimal = -decimal
    return decimal


def _point_wkb(longitude: float, latitude: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(longitude), float(latitude))


def _join_features(tables: Mapping[str, bytes], counts: Mapping[str, object]) -> tuple[list[dict], dict]:
    """Validate the official tables and build fixed-location tower features.

    Completeness is verified against the producer's own `counts` manifest: every
    listed table must contain exactly the declared number of records. Every fixed
    LO population record joins to its HD license header and its single licensee
    EN record on Unique System Identifier, with license and licensee call-sign
    agreement enforced; join-integrity violations reject the whole capture.
    """
    for member in sorted(EXPECTED_MEMBERS):
        if member not in tables:
            raise ULSDraftError(f"uls_member_missing:{member}")
    table_names = sorted({member[:-4] for member in EXPECTED_MEMBERS if member.endswith(".dat")} | {"ir"})
    actual_counts = {name: tables[f"{name}.dat" if name != "ir" else "ir.dat"].count(b"\n")
                     for name in table_names}
    declared = counts["table_counts"]
    if declared != actual_counts:
        drifted = {name: {"declared": declared.get(name), "observed": actual_counts.get(name)}
                   for name in table_names if declared.get(name) != actual_counts.get(name)}
        raise ULSDraftError(f"uls_counts_manifest_mismatch:{drifted}")

    headers: dict[int, list[str]] = {}
    service_codes: dict[str, int] = {}
    for fields in _records(tables["HD.dat"], "HD", HD_EXPECTED_FIELD_COUNT):
        uid = _cast(fields[1], "int64")
        if uid is None:
            raise ULSDraftError("uls_hd_uid_missing")
        if uid in headers:
            raise ULSDraftError(f"uls_hd_duplicate_license:{uid}")
        service = fields[HD_JOIN_INDEXES["radio_service_code"]]
        service_codes[service] = service_codes.get(service, 0) + 1
        unexpected = set(service_codes) - EXPECTED_SERVICE_CODES
        if unexpected:
            raise ULSDraftError(f"uls_unexpected_radio_service_code:{sorted(unexpected)}")
        headers[uid] = fields
    licensees: dict[int, list[str]] = {}
    entity_counts: dict[str, int] = {}
    for fields in _records(tables["EN.dat"], "EN", EN_EXPECTED_FIELD_COUNT):
        uid = _cast(fields[1], "int64")
        entity_type = fields[EN_ENTITY_TYPE_INDEX]
        entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1
        if entity_type == LICENSEE_ENTITY_TYPE:
            if uid is None:
                raise ULSDraftError("uls_en_licensee_uid_missing")
            if uid in licensees:
                raise ULSDraftError(f"uls_en_duplicate_licensee:{uid}")
            licensees[uid] = fields

    locations = _records(tables["LO.dat"], "LO", LO_EXPECTED_FIELD_COUNT)
    fixed_records = [fields for fields in locations
                     if fields[LOCATION_TYPE_INDEX] == FIXED_LOCATION_TYPE]
    features = []
    invalid_coordinates: list[int] = []
    missing_coordinates: list[int] = []
    missing_header: list[int] = []
    callsign_disagreements: list[int] = []
    missing_licensee: list[int] = []
    licensee_callsign_disagreements: list[int] = []
    for fields in fixed_records:
        record = {name: _cast(fields[index], kind)
                  for index, (name, kind) in enumerate(LOCATION_FIELDS)}
        uid = record["unique_system_identifier"]
        header = headers.get(uid)
        if header is None:
            missing_header.append(uid)
            continue
        for name, index in HD_JOIN_INDEXES.items():
            record[name] = header[index]
        licensee = licensees.get(uid)
        if licensee is None:
            # The documented invariant is exactly one matching licensee EN record
            # per license; a missing join is drift, never a silently null field.
            missing_licensee.append(uid)
            continue
        if licensee[EN_CALL_SIGN_INDEX] != record["call_sign"]:
            licensee_callsign_disagreements.append(uid)
        for name, index in EN_LICENSEE_INDEXES.items():
            record[name] = licensee[index]
        if record["call_sign"] != header[HD_CALL_SIGN_INDEX]:
            callsign_disagreements.append(uid)
        latitude = _dms_to_decimal(record["latitude_degrees"], record["latitude_minutes"],
                                   record["latitude_seconds"], record["latitude_direction"],
                                   max_degrees=90, directions=LATITUDE_DIRECTIONS)
        longitude = _dms_to_decimal(record["longitude_degrees"], record["longitude_minutes"],
                                    record["longitude_seconds"], record["longitude_direction"],
                                    max_degrees=180, directions=LONGITUDE_DIRECTIONS)
        if latitude is None or longitude is None:
            # The FCC states the licensing data contain known coordinate errors:
            # unmappable fixed locations are excluded from candidate features but
            # fully accounted here, with raw DMS values still staged under raw/.
            if record["latitude_degrees"] is None and record["longitude_degrees"] is None:
                missing_coordinates.append(uid)
            else:
                invalid_coordinates.append(uid)
            continue
        record["_latitude"], record["_longitude"] = latitude, longitude
        features.append(record)
    if missing_header:
        raise ULSDraftError(f"uls_fixed_location_without_license_header:{len(missing_header)}")
    if missing_licensee:
        raise ULSDraftError(f"uls_fixed_location_without_licensee:{len(missing_licensee)}")
    if callsign_disagreements:
        raise ULSDraftError(f"uls_fixed_location_callsign_disagreement:{len(callsign_disagreements)}")
    if licensee_callsign_disagreements:
        raise ULSDraftError(
            f"uls_en_licensee_callsign_disagreement:{len(licensee_callsign_disagreements)}")
    audit = {
        "uls_file_creation_date": counts["file_creation_date"],
        "uls_file_creation_date_iso": counts["file_creation_date_iso"],
        "counts_table_counts": declared,
        "service_code_counts": dict(sorted(service_codes.items())),
        "entity_type_counts": dict(sorted(entity_counts.items())),
        "lo_record_count": len(locations),
        "fixed_location_count": len(fixed_records),
        "mobile_location_count": len(locations) - len(fixed_records),
        "invalid_coordinate_record_count": len(invalid_coordinates),
        "invalid_coordinate_unique_system_identifiers": sorted(invalid_coordinates)[:50],
        "missing_coordinate_record_count": len(missing_coordinates),
        "missing_coordinate_unique_system_identifiers": sorted(missing_coordinates)[:50],
        "license_header_count": len(headers),
        "licensee_entity_count": len(licensees),
        "licensee_join_complete": True,
    }
    return features, audit


def _parquet(features: list[dict]) -> bytes:
    """GeoParquet candidate preserving every official LO field and raw DMS."""
    columns: list[tuple[str, str, list[object]]] = [
        (name, kind, [feature[name] for feature in features]) for name, kind in LOCATION_FIELDS]
    columns.extend((name, "string", [feature.get(name) for feature in features])
                   for name, _ in JOINED_COLUMNS)
    columns.append((GEOMETRY_COLUMN, "binary", [
        _point_wkb(feature["_longitude"], feature["_latitude"]) for feature in features]))
    arrays, fields = [], []
    for name, kind, values in columns:
        arrow_type = (pa.string() if kind == "string" else pa.int64() if kind == "int64"
                      else pa.float64() if kind == "float64" else pa.binary())
        arrays.append(pa.array(values, type=arrow_type, safe=True))
        fields.append(pa.field(name, arrow_type, nullable=kind != "binary"))
    geo_metadata = {
        "version": "1.1.0", "primary_column": GEOMETRY_COLUMN,
        "columns": {GEOMETRY_COLUMN: {"encoding": "WKB", "geometry_types": ["Point"],
                                      "crs": NAD83.to_json_dict()}},
    }
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo_metadata)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    readback = pq.read_table(pa.BufferReader(sink.getvalue()))
    if readback.num_rows != len(features):
        raise ULSDraftError("candidate_row_count_readback_drift")
    for name, _kind, values in columns:
        if readback[name].to_pylist() != values:
            raise ULSDraftError(f"candidate_attribute_readback_drift:{name}")
    geo = json.loads(readback.schema.metadata[b"geo"])
    if geo["columns"][GEOMETRY_COLUMN]["crs"] != NAD83.to_json_dict():
        raise ULSDraftError("candidate_crs_metadata_drift")
    return sink.getvalue()


def _decode(body: bytes) -> tuple[list[dict], dict, bytes]:
    """Decode the weekly archive into validated tower features plus the audit."""
    if len(body) > MAX_SOURCE_BYTES:
        raise ULSDraftError("uls_archive_size_limit")
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile:
        raise ULSDraftError("uls_archive_unreadable") from None
    names = []
    for info in archive.infolist():
        parts = Path(info.filename).parts
        mode = info.external_attr >> 16
        if (not info.filename or info.filename.startswith(("/", "\\")) or "\\" in info.filename
                or ".." in parts or len(parts) != 1 or info.is_dir() or (mode & 0o170000) == 0o120000):
            raise ULSDraftError(f"uls_archive_member_path_rejected:{info.filename}")
        names.append(info.filename)
    if len(set(names)) != len(names) or set(names) != EXPECTED_MEMBERS:
        raise ULSDraftError("uls_archive_member_roster_drift")
    if sum(info.compress_size for info in archive.infolist()) > MAX_STAGE_BYTES:
        raise ULSDraftError("uls_archive_stage_size_limit")
    tables = {name: archive.read(name) for name in EXPECTED_MEMBERS}
    counts = _counts_manifest(tables["counts"])
    features, audit = _join_features(tables, counts)
    audit.update({
        "feature_count": len(features),
        "null_geometry_count": 0,
        "counts_completeness_verified": True,
        "lo_field_count": LO_EXPECTED_FIELD_COUNT,
        "hd_field_count": HD_EXPECTED_FIELD_COUNT,
        "en_field_count": EN_EXPECTED_FIELD_COUNT,
        "fixed_location_scope": "Location Type Code F (Fixed) per FCC ULS code definitions",
        "feature_scope_note": ("Candidate features are fixed locations with parseable "
                               "coordinates; fixed locations without coordinates or with "
                               "invalid DMS values are excluded and counted, never silently "
                               "dropped, and remain staged in the raw tables."),
        "crs_declaration_basis": CRS_DECLARATION_BASIS,
        "known_errors_note": KNOWN_ERRORS_NOTE,
    })
    excluded = audit["missing_coordinate_record_count"] + audit["invalid_coordinate_record_count"]
    if excluded > 0.01 * max(audit["fixed_location_count"], 1):
        raise ULSDraftError(
            f"uls_excessive_unmappable_fixed_locations:{excluded}")
    parquet = _parquet(features)
    return features, audit, parquet


def _load_capture(root: Path) -> tuple[bytes, bytes, dict]:
    paths = [root / "lm_bcast.body", root / "lm_bcast.wire", root / "lm_bcast.receipt.json"]
    if any(p.is_symlink() for p in paths):
        raise ULSDraftError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if (receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
            or receipt.get("final_url") != SOURCE_URL or receipt.get("complete_body") is not True
            or receipt.get("status") != 200 or receipt.get("bytes") != len(body)
            or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire)
            or receipt.get("wire_sha256") != _sha(wire)):
        raise ULSDraftError("capture_receipt_invalid")
    return body, wire, receipt


def _capture(root: Path, storage: StagingStorageResource, version: str) -> tuple[bytes, bytes, dict]:
    manifest_path = root / "capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents):
        raise ULSDraftError("unsafe_capture_directory")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_bytes())
        if manifest.get("complete") is not True or set(manifest.get("receipts", {})) != {"lm_bcast"}:
            raise ULSDraftError("capture_manifest_invalid")
        roster = manifest.get("artifact_roster")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise ULSDraftError("capture_roster_digest_drift")
        body, wire, receipt = _load_capture(root)
        if receipt != manifest["receipts"]["lm_bcast"]:
            raise ULSDraftError("capture_manifest_receipt_drift")
        return body, wire, receipt
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ULSDraftError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=30),
                          headers={"User-Agent": "hifld-next-datasets/land-mobile-broadcast-towers-draft"}) as client:
            body, receipt = capture_get(client, SOURCE_URL, root, "lm_bcast", max_bytes=MAX_SOURCE_BYTES)
        if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL or receipt.get("final_url") != SOURCE_URL:
            raise ULSDraftError("source_request_identity_mismatch")
        captures["lm_bcast"] = (body, receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp:
            _decode(body)
        manifest_path.write_bytes(_canonical(
            {"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
             "receipts": {"lm_bcast": receipt},
             "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        return body, (root / "lm_bcast.wire").read_bytes(), receipt
    except Exception as exc:
        receipts: dict[str, dict] = {}
        for path in root.glob("*.receipt.json"):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipts[name] = json.loads(path.read_bytes())
                captures.setdefault(name, ((root / f"{name}.body").read_bytes(), receipts[name]))
            except Exception:
                pass
        manifest_path.write_bytes(_canonical(
            {"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
             "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures,
                                          type(exc).__name__)
        raise


def capture_land_mobile_broadcast_towers(storage: StagingStorageResource, *, version: str,
                                         capture_directory: str | None = None) -> dict:
    """Capture, validate and seal one weekly FCC LMbcast candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ULSDraftError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version:
        raise ULSDraftError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)):
        raise ULSDraftError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 400_000_000:
        raise ULSDraftError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/132/captures") / (version + "-" + uuid4().hex)
    body, wire, receipt = _capture(root, storage, version)
    features, audit, parquet = _decode(body)
    identity = processing_identity(Path(__file__).parents[3])
    record_sha = _sha(_canonical([dict(sorted(feature.items())) for feature in features]))
    geometry_sha = _sha(_canonical([[feature["unique_system_identifier"], feature["_longitude"],
                                     feature["_latitude"]] for feature in features]))
    schema_sha = _sha(_canonical(list(OUTPUT_FIELD_TYPES)))
    evidence_sha = _sha(_canonical({"body": receipt["sha256"], "wire": receipt["wire_sha256"],
                                    "receipt": receipt}))
    # The candidate version and the construction fingerprint both include the
    # captured archive identity: a weekly regeneration is a new release even when
    # decoded features are unchanged, and a replay of the same archive bytes
    # reuses the same immutable candidate. A version that omitted the archive
    # sha256 would collide with its own refresh and then fail the fingerprint
    # check as a construction_bundle_identity_mismatch.
    candidate_version = "v" + _sha(_canonical({
        "archive_sha256": receipt["sha256"], "record_sha256": record_sha,
        "geometry_sha256": geometry_sha, "schema_sha256": schema_sha,
        "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    op = f"geoparquet/{FILE_SLUG}.parquet"
    creation = audit["uls_file_creation_date"]
    inputs = [{
        "input_id": "fcc_uls_lmbcast_weekly_license_records",
        "producer": PUBLISHER,
        "host": urlsplit(SOURCE_URL).hostname,
        "role": "producer_publication",
        "canonical_url": LANDING,
        "retrieval_url": SOURCE_URL,
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"],
        "wire_sha256": receipt["wire_sha256"],
        "release_date": audit["uls_file_creation_date_iso"],
        "file_creation_timestamp_local": creation,
        "release_date_basis": ("Producer-declared weekly file creation timestamp from the "
                               "archive's own counts manifest (kept in FCC local time); the "
                               "complete weekly license file is regenerated by the FCC, "
                               "typically Sunday morning."),
        "cadence": "Weekly complete refresh, plus daily transaction files",
        "license": "Rights and reuse review pending; consult FCC terms and disclaimers.",
        "attribution": "Federal Communications Commission; review pending",
    }]
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "Land Mobile Broadcast Towers (FCC ULS Broadcast Auxiliary, weekly license extract)",
        "description": ("Draft candidate of fixed-location tower features for the FCC Land "
                        "Mobile Broadcast Auxiliary services (LP, RP, LV) built from the "
                        "producer's own weekly Universal Licensing System license-records "
                        "file. Not represented as an accepted HIFLD equivalent; the "
                        "historical HIFLD tower filter and published schema remain "
                        "unresolved and unapproved."),
        "publisher": PUBLISHER,
        "agency": "Federal Communications Commission",
        "source_url": SOURCE_URL,
        "license": None,
        "date_issued": audit["uls_file_creation_date_iso"],
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": inputs,
        "transformations": [{
            "operation": "Official ULS pipe-delimited tables to GeoParquet fixed-location points",
            "parameters": {
                "source_tables": ["LO", "HD", "EN"],
                "feature_scope": ("Location Type Code F (Fixed) records of the official LO "
                                  "table, one candidate feature per fixed licensed location; "
                                  "mobile, itinerant, temporary and control-station location "
                                  "types are not tower features."),
                "field_preservation": ("Every official LO field is carried with its declared "
                                       "name, type and native value, including raw DMS "
                                       "components; no columns are dropped, renamed into new "
                                       "meanings, or substituted."),
                "joins": ("License status, radio service code and grant/expire dates from the "
                          "official HD table plus licensee id/name from the single licensee EN "
                          "record, joined on Unique System Identifier; exactly one matching "
                          "licensee per fixed location and license/licensee call-sign "
                          "agreement are enforced and violations reject the capture; every "
                          "raw table is staged losslessly under raw/."),
                "geometry": ("DMS converted to decimal degrees only for the geometry column; "
                             "invalid or incomplete DMS keeps null geometry with raw values "
                             "retained. No reprojection or coordinate repair."),
                "candidate_status": "draft, unapproved; historical equivalence unresolved",
            },
        }],
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": ("Native ULS attribute values, identifiers and DMS coordinate "
                           "components are retained in a draft candidate; the source contract "
                           "and published mapping remain unapproved."),
                "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet",
                             "sha256": _sha(parquet), "bytes": len(parquet)}],
            }},
        },
        "lineage": ("FCC Universal Licensing System public access files: the maintained weekly "
                    "complete license-records file for Land Mobile - Broadcast Auxiliary "
                    "(l_LMbcast.zip), regenerated by the FCC (counts manifest creation timestamp "
                    f"{creation}). Contains radio service codes LP (Broadcast Auxiliary Low "
                    "Power), RP (Broadcast Auxiliary Remote Pickup) and LV (Low Power Wireless "
                    "Assist Video Devices) per the FCC radio service code list. The historical "
                    "HIFLD Land Mobile Broadcast Towers layer was generated from the FCC "
                    "licensing database; the historical shapefile itself is available only "
                    "through prohibited archive mirrors, so the historical schema and feature "
                    "filter remain unverified."),
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {
        op: parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({
            "approved": False, "version": candidate_version,
            "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}},
            "source_population_complete": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
        }) + b"\n",
        "raw/lm_bcast.body": body,
        "raw/lm_bcast.wire": wire,
        "raw/lm_bcast.receipt.json": _canonical(receipt) + b"\n",
    }
    state = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    fingerprint = _sha(_canonical({
        "identity": identity, "archive_sha256": receipt["sha256"],
        "record_sha256": record_sha, "schema_sha256": schema_sha}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, candidate_version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ULSDraftError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix,
                                candidate_version, fingerprint, identity, record_sha,
                                schema_sha, original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
                "feature_count": len(features), "source_population_complete": True,
                "record_sha256": record_sha, "geometry_sha256": geometry_sha,
                "schema_sha256": schema_sha,
                "fixed_location_count": audit["fixed_location_count"],
                "null_geometry_count": audit["null_geometry_count"],
                "invalid_coordinate_record_count": audit["invalid_coordinate_record_count"],
                "missing_coordinate_record_count": audit["missing_coordinate_record_count"],
                "service_code_counts": audit["service_code_counts"],
                "licensee_join_complete": audit["licensee_join_complete"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root),
                "release_accepted": False}
    descriptors = {}
    for name, payload in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, payload)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(payload) or storage.sha256_key(key) != _sha(payload):
            raise ULSDraftError("candidate_object_readback_failed:" + name)
        descriptors[name] = {"bytes": len(payload), "sha256": _sha(payload)}
    seal_ready_candidate(storage, state, candidate_version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": record_sha,
        "source_schema_sha256": schema_sha, "capture_receipts": {"lm_bcast": receipt},
        "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": len(features), "source_population_complete": True,
            "record_sha256": record_sha, "geometry_sha256": geometry_sha,
            "schema_sha256": schema_sha,
            "fixed_location_count": audit["fixed_location_count"],
            "null_geometry_count": audit["null_geometry_count"],
            "invalid_coordinate_record_count": audit["invalid_coordinate_record_count"],
            "missing_coordinate_record_count": audit["missing_coordinate_record_count"],
            "service_code_counts": audit["service_code_counts"],
            "licensee_join_complete": audit["licensee_join_complete"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
