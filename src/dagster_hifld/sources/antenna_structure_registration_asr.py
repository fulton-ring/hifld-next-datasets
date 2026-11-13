"""Complete native capture of the FCC ULS weekly antenna structure registration tower file.

The Antenna Structure Registration (ASR) tower tables are the registration side
of the FCC ULS public access tower data family: the weekly ``r_tower.zip`` file
carries the current registrations (RA), their coordinates (CO, one tower row and
optionally one array row per registration) and their entities (EN), while the
sibling ``a_tower.zip`` carries the weekly application tables and is not the
published registry. Field layouts follow the FCC ASR tower table definitions
(https://www.fcc.gov/sites/default/files/patower-4.pdf and the ASR
``asr_sql_table_definitions_3.txt``: RA 49 fields, CO 18 fields, EN 25 fields).
"""
from __future__ import annotations

import io
import json
import re
import shutil
import struct
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable, capture_get,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _load_capture, _sha, _validate_capture_request_identity,
)

DATASET_SLUG = "antenna-structure-registration-asr"
FILE_SLUG = DATASET_SLUG
# FCC moved the ULS daily and weekly public access files from wireless2.fcc.gov
# to data.fcc.gov in July 2021 (FCC Public Access Updates notice). The weekly
# tower registration file r_tower.zip is the current official bulk distribution
# of the ASR registration tables (RA registrations, CO coordinates, EN entities,
# HS history, RE remarks, SC special conditions).
R_TOWER_URL = "https://data.fcc.gov/download/pub/uls/complete/r_tower.zip"
PUBLISHER = ("Federal Communications Commission Wireless Telecommunications Bureau; "
             "official ULS weekly public access complete database files (ASR tower tables)")
CAPTURE_NAMES = {"r_tower"}
MAX_CAPTURE_BYTES = 192_000_000
CAPTURE_BODY_LIMIT = 64 * 1024 * 1024
DISK_FLOOR = 400_000_000
ZIP_MEMBER_ROSTER = ["CO.dat", "EN.dat", "HS.dat", "RA.dat", "RE.dat", "SC.dat", "counts"]
RECORD_FIELD_COUNTS = {"RA": 49, "CO": 18, "EN": 25}
EXPECTED_RA_RECORDS = 198010
EXPECTED_CO_RECORDS = 203245
EXPECTED_EN_RECORDS = 374223
EXPECTED_CREATION_DATE = "Sun Nov  8 06:21:02 EST 2026"
EXPECTED_STATUS_COUNTS = {"C": 141986, "G": 18447, "A": 15913, "I": 12740, "T": 8911, "": 13}
EXPECTED_ARCHIVE_COUNTS = {"C": 160682, "A": 37327, "": 1}
EXPECTED_COORDINATE_TYPE_COUNTS = {"T": 198009, "A": 5236}
EXPECTED_LATITUDE_DIRECTIONS = {"N": 203102, "S": 23, "": 120}
EXPECTED_LONGITUDE_DIRECTIONS = {"W": 203023, "E": 102, "": 120}
EXPECTED_OWNER_ENTITY_ROWS = 197994
EXPECTED_REGISTRANT_ENTITY_ROWS = 176229
EXPECTED_REGISTRATIONS_WITHOUT_ENTITY = 16
EXPECTED_ROWS_WITHOUT_OWNER_ENTITY = 15
EXPECTED_ARRAY_ROWS_WITH_POSITION = 354
EXPECTED_ARRAY_ROWS_WITHOUT_POSITION = 4882
EXPECTED_PUBLISHED_ROWS = 203245
EXPECTED_NULL_GEOMETRY_COUNT = 124
EXPECTED_ID_SHA256 = "437a2afcb638554c7693c673d93204e6a8f0815629c58eebc61bf1b2bc63ae0c"
EXPECTED_GEOMETRY_SHA256 = "a86d768bb3705b0a08bd372a915add439a94d25e07e2daa89bd6beb186136712"
EXPECTED_STATE_ROSTER_SHA256 = "5fdba7b3f3aa1b6bdb4bc8bd862b5a1bf87142658ea1bcb279e8c6bb062411cf"
EXPECTED_ROWS_SHA256 = "31b7d83dc0f0b287b6802f819e6251ec464473bac52cee36e120c72afd3dc1c4"
EXPECTED_BOUNDS = [-176.705472222, -15.15, 174.138861111, 71.3103611111]
RA_FIELDS = [
    "record_type", "content_indicator", "file_number", "registration_number",
    "unique_system_identifier", "application_purpose", "previous_purpose",
    "input_source_code", "status_code", "date_entered", "date_received",
    "date_issued", "date_constructed", "date_dismantled", "date_action",
    "archive_flag_code", "version", "signature_first_name", "signature_middle_initial",
    "signature_last_name", "signature_suffix", "signature_title", "invalid_signature",
    "structure_street_address", "structure_city", "structure_state_code",
    "county_code", "zip_code", "height_of_structure", "ground_elevation",
    "overall_height_above_ground", "overall_height_amsl", "structure_type",
    "date_faa_determination_issued", "faa_study_number", "faa_circular_number",
    "specification_option", "painting_and_lighting", "mark_light_code",
    "mark_light_other", "faa_emi_flag", "nepa_flag", "date_signed",
    "signature_last_or", "signature_first_or", "signature_mi_or",
    "signature_suffix_or", "title_signed_or", "date_signed_or",
]
CO_FIELDS = [
    "record_type", "content_indicator", "file_number", "registration_number",
    "unique_system_identifier", "coordinate_type", "latitude_degrees",
    "latitude_minutes", "latitude_seconds", "latitude_direction",
    "latitude_total_seconds", "longitude_degrees", "longitude_minutes",
    "longitude_seconds", "longitude_direction", "longitude_total_seconds",
    "array_tower_position", "array_total_tower",
]
EN_FIELDS = [
    "record_type", "content_indicator", "file_number", "registration_number",
    "unique_system_identifier", "entity_type", "entity_type_code",
    "entity_type_other", "licensee_id", "entity_name", "first_name", "mi",
    "last_name", "suffix", "phone", "fax", "internet_address", "street_address",
    "street_address2", "po_box", "city", "state", "zip_code", "attention", "frn",
]
RA_REGISTRATION_NUMBER, RA_UNIQUE_SYSTEM_IDENTIFIER = 3, 4
RA_STATUS_CODE, RA_ARCHIVE_FLAG_CODE, RA_STRUCTURE_STATE_CODE = 8, 15, 25
EN_ENTITY_TYPE = 5
CO_COORDINATE_TYPE = 5
CO_LAT_DEG, CO_LAT_MIN, CO_LAT_SEC, CO_LAT_DIR = 6, 7, 8, 9
CO_LON_DEG, CO_LON_MIN, CO_LON_SEC, CO_LON_DIR = 11, 12, 13, 14
CO_ARRAY_TOWER_POSITION, CO_ARRAY_TOTAL_TOWER = 16, 17
CREATION_DATE_RE = re.compile(rb"File Creation Date: (.+?)\r?$", re.MULTILINE)
PUBLISHED_SCHEMA = (
    [("geometry", pa.binary())]
    + [("ra_" + name, pa.string()) for name in RA_FIELDS]
    + [("co_" + name, pa.string()) for name in CO_FIELDS]
    + [("en_" + name, pa.string()) for name in EN_FIELDS]
    + [("latdec", pa.float64()), ("londec", pa.float64())]
)
PUBLISHED_COLUMN_NAMES = [name for name, _ in PUBLISHED_SCHEMA]
RECORD_VALUE_NAMES = [name for name in PUBLISHED_COLUMN_NAMES if name != "geometry"]
ROW_KEY_COLUMNS = ("ra_registration_number", "ra_unique_system_identifier", "co_coordinate_type")
COVERAGE_SCOPE = (
    "Every coordinate record of every antenna structure registration in the weekly FCC ULS "
    "antenna structure registration tower public-access file (r_tower.zip), across the United "
    "States and its territories. Registered antenna structures are point locations (one tower "
    "coordinate row per registration, plus one array row for the 5236 array registrations); the "
    "single registration without a coordinate row is retained in raw staging but has no published "
    "row. This is the FCC's registered-structure population, not a census of physical towers."
)
# ASR coordinates are referenced to the North American Datum of 1983: the FCC's
# ASR registration search states "Based on NAD83 (convert from NAD27)"
# (https://wireless2.fcc.gov/UlsApp/AsrSearch/asrRegistrationSearch.jsp) and FCC
# Form 854 ASR filings must use NAD83 coordinates. The published WKB points carry
# the DMS-derived decimal values unchanged, so the GeoParquet geometry is
# declared in the native EPSG:4269 datum; no datum transformation, reprojection,
# repair or geocoding is applied.
GEOMETRY_CRS = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "NAD83",
    "datum": {"type": "GeodeticReferenceFrame", "name": "North American Datum 1983",
              "ellipsoid": {"name": "GRS 1980", "semi_major_axis": 6378137, "inverse_flattening": 298.257222101},
              "id": {"authority": "EPSG", "code": 6269}},
    "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                             {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
    "id": {"authority": "EPSG", "code": 4269},
}
GEOMETRY_PROVENANCE = ("WKB Point (londec, latdec) in EPSG:4269 (NAD83), the datum the FCC's antenna "
                       "structure registration search states ASR coordinates are based on; the DMS-derived "
                       "decimal values are written unchanged, with no datum transformation, reprojection, "
                       "repair or geocoding")


def _capture_budget(root: Path) -> None:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * CAPTURE_BODY_LIMIT > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")


def _capture(client: httpx.Client, root: Path) -> tuple[bytes, dict]:
    _capture_budget(root)
    return capture_get(client, R_TOWER_URL, root, "r_tower", max_bytes=CAPTURE_BODY_LIMIT)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    captures = {}
    for name in sorted(CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, R_TOWER_URL)
        captures[name] = (body, receipt)
    return captures


def _zip_tables(body: bytes) -> tuple[dict[str, list[list[str]]], bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        raise ValueError("source_zip_unreadable") from exc
    roster = sorted(info.filename for info in archive.infolist())
    if roster != sorted(ZIP_MEMBER_ROSTER):
        raise ValueError("source_zip_member_roster_drift")
    tables: dict[str, list[list[str]]] = {}
    try:
        for member in ("RA.dat", "CO.dat", "EN.dat"):
            raw = archive.read(member)
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("source_record_encoding_drift:" + member) from exc
            lines = text.split("\n")
            if lines and lines[-1] == "":
                lines = lines[:-1]
            if any(not line for line in lines):
                raise ValueError("source_record_line_invalid:" + member)
            lines = [line[:-1] if line.endswith("\r") else line for line in lines]
            if any("\r" in line for line in lines):
                raise ValueError("source_record_line_invalid:" + member)
            rows = [line.split("|") for line in lines]
            tables[member[:-4]] = rows
        counts = archive.read("counts")
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as exc:
        raise ValueError("source_zip_member_unreadable") from exc
    return tables, counts


def _counts_tables(counts: bytes) -> dict[str, int]:
    table_counts = {}
    for line in counts.decode("utf-8", "strict").splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].rsplit("/", 1)[-1].endswith(".dat"):
            table_counts[parts[1].rsplit("/", 1)[-1][:-4]] = int(parts[0])
    for name, expected in (("RA", EXPECTED_RA_RECORDS), ("CO", EXPECTED_CO_RECORDS),
                           ("EN", EXPECTED_EN_RECORDS)):
        if table_counts.get(name) != expected:
            raise ValueError("source_counts_file_population_drift:" + name)


def _decimal_degrees(degrees: str, minutes: str, seconds: str,
                     direction: str, negative_direction: str) -> float:
    value = (int(degrees) + (int(minutes) if minutes else 0) / 60
             + (float(seconds) if seconds else 0.0) / 3600)
    if direction == negative_direction:
        value = -value
    # Derived decimal coordinates follow the established ULS-family convention
    # of formatting the DMS conversion to 12 significant digits.
    return float(f"{value:.12g}")


def _point_wkb(longitude: float, latitude: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, longitude, latitude)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[list], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    body, receipt = captures["r_tower"]
    _validate_capture_request_identity("r_tower", receipt, R_TOWER_URL)
    if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
        raise ValueError("source_capture_status_length_hash_or_completeness_drift")
    tables, counts = _zip_tables(body)
    for name, rows in tables.items():
        expected = RECORD_FIELD_COUNTS[name]
        if any(len(row) != expected for row in rows):
            raise ValueError("source_record_field_count_drift:" + name)
        if any(row[0] != name for row in rows):
            raise ValueError("source_record_type_prefix_drift:" + name)
    if (len(tables["RA"]) != EXPECTED_RA_RECORDS or len(tables["CO"]) != EXPECTED_CO_RECORDS
            or len(tables["EN"]) != EXPECTED_EN_RECORDS):
        raise ValueError("source_record_population_drift")
    _counts_tables(counts)
    match = CREATION_DATE_RE.search(counts)
    if match is None:
        raise ValueError("source_creation_date_missing")
    creation_date = match.group(1).decode("utf-8", "replace").strip()
    if creation_date != EXPECTED_CREATION_DATE:
        raise ValueError("source_creation_date_drift")

    ra_by_key: dict[tuple[str, str], list[str]] = {}
    registration_numbers: set[str] = set()
    unique_system_identifiers: set[str] = set()
    status_counts: dict[str, int] = {}
    archive_counts: dict[str, int] = {}
    for row in tables["RA"]:
        regnum, usi = row[RA_REGISTRATION_NUMBER], row[RA_UNIQUE_SYSTEM_IDENTIFIER]
        if not regnum.strip() or not usi.strip():
            raise ValueError("source_registration_identity_missing")
        if regnum in registration_numbers:
            raise ValueError("source_duplicate_registration_number")
        if usi in unique_system_identifiers:
            raise ValueError("source_duplicate_unique_system_identifier")
        registration_numbers.add(regnum)
        unique_system_identifiers.add(usi)
        key = (regnum, usi)
        status_counts[row[RA_STATUS_CODE]] = status_counts.get(row[RA_STATUS_CODE], 0) + 1
        archive_counts[row[RA_ARCHIVE_FLAG_CODE]] = archive_counts.get(row[RA_ARCHIVE_FLAG_CODE], 0) + 1
        ra_by_key[key] = row
    if status_counts != EXPECTED_STATUS_COUNTS:
        raise ValueError("source_registration_status_distribution_drift")
    if archive_counts != EXPECTED_ARCHIVE_COUNTS:
        raise ValueError("source_archive_flag_distribution_drift")
    states = sorted({row[RA_STRUCTURE_STATE_CODE].strip() for row in tables["RA"]
                     if row[RA_STRUCTURE_STATE_CODE].strip()})
    if _sha(_canonical(states)) != EXPECTED_STATE_ROSTER_SHA256:
        raise ValueError("source_state_roster_drift")

    owner_by_key: dict[tuple[str, str], list[str]] = {}
    registrant_rows = 0
    entity_types: dict[str, int] = {}
    for row in tables["EN"]:
        entity_types[row[EN_ENTITY_TYPE]] = entity_types.get(row[EN_ENTITY_TYPE], 0) + 1
        if row[EN_ENTITY_TYPE] == "O":
            key = (row[RA_REGISTRATION_NUMBER], row[RA_UNIQUE_SYSTEM_IDENTIFIER])
            if key in owner_by_key:
                raise ValueError("source_duplicate_owner_entity_row")
            owner_by_key[key] = row
        elif row[EN_ENTITY_TYPE] == "R":
            registrant_rows += 1
    if len(owner_by_key) != EXPECTED_OWNER_ENTITY_ROWS:
        raise ValueError("source_owner_entity_population_drift")
    if registrant_rows != EXPECTED_REGISTRANT_ENTITY_ROWS:
        raise ValueError("source_registrant_entity_population_drift")
    if entity_types.get("O", 0) != EXPECTED_OWNER_ENTITY_ROWS:
        raise ValueError("source_entity_type_distribution_drift")
    without_entity = sum(1 for key in ra_by_key if key not in owner_by_key)
    if without_entity != EXPECTED_REGISTRATIONS_WITHOUT_ENTITY:
        raise ValueError("source_registration_without_entity_drift")

    coordinate_types: dict[str, int] = {}
    latitude_directions: dict[str, int] = {}
    longitude_directions: dict[str, int] = {}
    keys = set()
    array_position_rows = 0
    null_geometry = 0
    records: list[list] = []
    for row in tables["CO"]:
        ra = ra_by_key.get((row[RA_REGISTRATION_NUMBER], row[RA_UNIQUE_SYSTEM_IDENTIFIER]))
        if ra is None:
            raise ValueError("source_coordinate_join_drift")
        coordinate_type = row[CO_COORDINATE_TYPE]
        coordinate_types[coordinate_type] = coordinate_types.get(coordinate_type, 0) + 1
        latitude_directions[row[CO_LAT_DIR]] = latitude_directions.get(row[CO_LAT_DIR], 0) + 1
        longitude_directions[row[CO_LON_DIR]] = longitude_directions.get(row[CO_LON_DIR], 0) + 1
        key = (row[RA_REGISTRATION_NUMBER], row[RA_UNIQUE_SYSTEM_IDENTIFIER], coordinate_type)
        if key in keys:
            raise ValueError("source_coordinate_key_duplication_drift")
        keys.add(key)
        if coordinate_type == "A" and row[CO_ARRAY_TOWER_POSITION].strip():
            array_position_rows += 1
        owner = owner_by_key.get((row[RA_REGISTRATION_NUMBER], row[RA_UNIQUE_SYSTEM_IDENTIFIER]))
        lat_deg, lat_min, lat_sec = row[CO_LAT_DEG].strip(), row[CO_LAT_MIN].strip(), row[CO_LAT_SEC].strip()
        lon_deg, lon_min, lon_sec = row[CO_LON_DEG].strip(), row[CO_LON_MIN].strip(), row[CO_LON_SEC].strip()
        lat_dir, lon_dir = row[CO_LAT_DIR].strip(), row[CO_LON_DIR].strip()
        latdec = londec = None
        if lat_deg and lon_deg:
            if lat_dir not in {"N", "S"} or lon_dir not in {"W", "E"}:
                raise ValueError("source_coordinate_direction_drift")
            for text in (lat_deg, lat_min, lat_sec, lon_deg, lon_min, lon_sec):
                if text:
                    try:
                        value = float(text)
                    except ValueError as exc:
                        raise ValueError("source_coordinate_component_invalid") from exc
                    if value != value or value in (float("inf"), float("-inf")):
                        raise ValueError("source_coordinate_component_invalid")
            try:
                latdec = _decimal_degrees(lat_deg, lat_min, lat_sec, lat_dir, "S")
                londec = _decimal_degrees(lon_deg, lon_min, lon_sec, lon_dir, "W")
            except ValueError as exc:
                raise ValueError("source_coordinate_component_invalid") from exc
            if not -90 <= latdec <= 90 or not -180 <= londec <= 180:
                raise ValueError("source_coordinate_out_of_range")
        else:
            null_geometry += 1
        record = [*ra, *row, *(owner if owner is not None else [None] * 25), latdec, londec]
        records.append(record)
    if coordinate_types != EXPECTED_COORDINATE_TYPE_COUNTS:
        raise ValueError("source_coordinate_type_distribution_drift")
    if latitude_directions != EXPECTED_LATITUDE_DIRECTIONS:
        raise ValueError("source_latitude_direction_distribution_drift")
    if longitude_directions != EXPECTED_LONGITUDE_DIRECTIONS:
        raise ValueError("source_longitude_direction_distribution_drift")
    rows_without_owner = sum(1 for record in records if record[49 + 18] is None)
    if rows_without_owner != EXPECTED_ROWS_WITHOUT_OWNER_ENTITY:
        raise ValueError("source_rows_without_owner_entity_drift")
    if array_position_rows != EXPECTED_ARRAY_ROWS_WITH_POSITION:
        raise ValueError("source_array_position_population_drift")
    if null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if len(records) != EXPECTED_PUBLISHED_ROWS:
        raise ValueError("source_published_population_drift")

    order = {name: index for index, name in enumerate(RECORD_VALUE_NAMES)}
    records.sort(key=lambda record: tuple(record[order[name]] for name in ROW_KEY_COLUMNS))
    roster = [[record[order[name]] for name in ROW_KEY_COLUMNS] for record in records]
    if _sha(_canonical(roster)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    geometry_roster = [[record[order["ra_registration_number"]],
                        record[order["ra_unique_system_identifier"]],
                        record[order["co_coordinate_type"]],
                        record[order["londec"]], record[order["latdec"]]]
                       for record in records if record[order["londec"]] is not None]
    if _sha(_canonical(geometry_roster)) != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_roster_drift")
    if _sha(_canonical(records)) != EXPECTED_ROWS_SHA256:
        raise ValueError("source_record_values_drift")
    coordinates = [(record[order["londec"]], record[order["latdec"]])
                   for record in records if record[order["londec"]] is not None]
    bounds = [min(x for x, _y in coordinates), min(y for _x, y in coordinates),
              max(x for x, _y in coordinates), max(y for _x, y in coordinates)]
    if bounds != EXPECTED_BOUNDS:
        raise ValueError("source_bounds_drift")
    schema_contract = {"zip_members": sorted(ZIP_MEMBER_ROSTER),
                       "record_field_counts": RECORD_FIELD_COUNTS,
                       "published_columns": PUBLISHED_COLUMN_NAMES}
    audit = {
        "source_feature_count": len(records), "source_record_sha256": _sha(_canonical(records)),
        "source_geometry_sha256": _sha(_canonical(geometry_roster)),
        "source_schema_sha256": _sha(_canonical(schema_contract)),
        "source_id_sha256": _sha(_canonical(roster)),
        "source_state_roster_sha256": _sha(_canonical(states)),
        "source_state_count": len(states),
        "source_ra_record_count": len(tables["RA"]), "source_co_record_count": len(tables["CO"]),
        "source_en_record_count": len(tables["EN"]),
        "source_registration_status_counts": status_counts,
        "source_archive_flag_counts": archive_counts,
        "source_coordinate_type_counts": coordinate_types,
        "source_latitude_directions": latitude_directions,
        "source_longitude_directions": longitude_directions,
        "source_owner_entity_rows": len(owner_by_key),
        "source_registrant_entity_rows": registrant_rows,
        "source_registrations_without_entity": without_entity,
        "source_rows_without_owner_entity": rows_without_owner,
        "source_array_rows_with_position": array_position_rows,
        "source_array_rows_without_position": EXPECTED_COORDINATE_TYPE_COUNTS["A"] - array_position_rows,
        "source_file_creation_date": creation_date,
        "source_null_geometry_count": null_geometry,
        "source_geometry_datum": ("NAD83 (EPSG:4269); the FCC ASR registration search states ASR "
                                  "coordinates are 'Based on NAD83 (convert from NAD27)' and FCC Form 854 "
                                  "ASR filings must use NAD83 coordinates"),
        "source_bounds": bounds, "source_coverage_scope": COVERAGE_SCOPE,
        "drift_guard": ("Exact capture roster, HTTP status/completeness/length/hash receipt, zip member roster, "
                        "per-record-type field counts and record-type prefixes, RA/CO/EN record totals, counts-file "
                        "record totals and creation date, distinct registration_number and unique_system_identifier "
                        "identities, status/archive flag distributions, structure state roster digest, one unique "
                        "EN owner (entity_type O) row per registered registration, registrant row population, "
                        "registrations without entity rows, CO/RA join, unique (registration_number, "
                        "unique_system_identifier, coordinate_type) keys, coordinate type and DMS direction "
                        "distributions, array position population, derived-decimal coordinate range, "
                        "full published ID/geometry/row-value digests, bounds and null-geometry count are checked; "
                        "duplicate identities or coordinate keys fail closed. All declared RA/CO/EN field values "
                        "are retained verbatim, including empty values, leading zeros and fixed-width char "
                        "padding; joined EN fields stay null for the 15 published rows whose registration carries "
                        "no EN owner row."),
    }
    return records, audit


def _parquet_bytes(records: list[list]) -> bytes:
    import io
    index = {name: position for position, name in enumerate(RECORD_VALUE_NAMES)}
    arrays = [pa.array([record[index[name]] for record in records], type=kind)
              for name, kind in PUBLISHED_SCHEMA[1:]]
    geometry = pa.array([_point_wkb(record[index["londec"]], record[index["latdec"]])
                         if record[index["londec"]] is not None else None
                         for record in records], type=pa.binary())
    table = pa.Table.from_arrays([geometry, *arrays], names=PUBLISHED_COLUMN_NAMES)
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                                             "crs": GEOMETRY_CRS}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    digests = {}
    for name in sorted(captures):
        _body, wire, _receipt = _load_capture(root, name)
        digests[name] = {"decoded_sha256": _sha(captures[name][0]), "wire_sha256": _sha(wire)}
    return _sha(_canonical(digests))


def capture_antenna_structure_registration_asr(storage: StagingStorageResource, *, version: str,
                                               capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved FCC ASR tower source candidate."""
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
    if shutil.disk_usage(staging).free < DISK_FLOOR:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/18/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=20),
                              headers={"User-Agent": "hifld-next-datasets/fcc-asr-draft"}) as client:
                captures["r_tower"] = _capture(client, root)
            records, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__,
                "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    records, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(records)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": "r_tower:" + receipts["r_tower"]["sha256"], "producer": PUBLISHER,
        "host": urlsplit(receipts["r_tower"]["url"]).hostname, "role": "producer_publication",
        "canonical_url": R_TOWER_URL, "retrieval_url": receipts["r_tower"]["url"],
        "observed_final_url": receipts["r_tower"]["final_url"], "retrieved_at": receipts["r_tower"]["observed_at"],
        "http_status": receipts["r_tower"]["status"], "bytes": receipts["r_tower"]["bytes"],
        "sha256": receipts["r_tower"]["sha256"], "wire_bytes": receipts["r_tower"]["wire_bytes"],
        "wire_sha256": receipts["r_tower"]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The weekly tower registration file is regenerated every Sunday; the "
                                        "payload's own counts file creation date is retained in the audit while "
                                        "no per-release edition date is declared by the FCC for this distribution."),
        "license": "Unresolved; rights review pending.", "attribution": "FCC Wireless Telecommunications Bureau ULS public access ASR tower tables; attribution and rights review pending."}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Antenna Structure Registration (FCC ASR)",
        "description": ("Registered antenna structures from the FCC ULS weekly public access ASR tower "
                        "registration file r_tower.zip: one published row per CO coordinate row joined to its "
                        "RA registration row and the registration's unique EN owner entity row, with every "
                        "declared RA/CO/EN field retained verbatim plus DMS-derived latdec/londec and WKB Point "
                        "geometry in the native NAD83 datum (EPSG:4269). Unapproved candidate; source edition, "
                        "baseline, schema/ID contract, rights and release decisions remain pending."),
        "publisher": PUBLISHER, "agency": "Federal Communications Commission", "source_url": R_TOWER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "ULS pipe-delimited ASR tower registration file to GeoParquet candidate", "parameters": {
            "source_tables": "RA registration, CO coordinates and EN entity tables of r_tower.zip decoded as CRLF pipe-delimited UTF-8 records (trailing CR removed per line)",
            "population": ("one published row per CO coordinate row (198009 tower rows and 5236 array rows) "
                           "joined to its RA registration row and to the registration's unique EN owner entity "
                           "row (entity_type O); the single registration without a coordinate row has no "
                           "published row and the 15 published rows whose registration carries no EN owner row "
                           "keep null joined EN fields"),
            "source_ids": ("ra_registration_number, ra_unique_system_identifier and co_coordinate_type retained "
                           "verbatim and jointly unique; no substitute identifiers are generated"),
            "source_values": ("all 92 declared RA/CO/EN fields retained verbatim as strings, including empty "
                              "values, leading zeros and fixed-width char padding; the historical HIFLD layer's "
                              "31-column schema/ID contract (its declared columns are positionally misaligned "
                              "relative to these tables) remains a pending maintainer mapping decision"),
            "derived_columns": ("latdec/londec are DMS-to-decimal conversions of the raw components formatted "
                                "to 12 significant digits, the established ULS-family derived-coordinate "
                                "convention (missing minutes or seconds contribute zero); the 124 coordinate "
                                "rows without parsable degrees and directions keep null latdec/londec and null "
                                "geometry and are counted"),
            "geometry": GEOMETRY_PROVENANCE + " (FCC ASR registration search: "
                        "https://wireless2.fcc.gov/UlsApp/AsrSearch/asrRegistrationSearch.jsp)",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("The FCC moved ULS daily and weekly public access downloads from wireless2.fcc.gov to "
                    "data.fcc.gov/download/pub/uls/ in July 2021; the weekly tower registration file r_tower.zip "
                    "is the current official bulk distribution of the ASR registration tables from which the "
                    "historical HIFLD Antenna Structure Registration layer was received from the FCC (a_tower.zip "
                    "carries the weekly application tables and is not the published registry). The archived HIFLD "
                    "OPEN ASR layer (DataLumos project 238655 per the inventory row) and the DHS crosswalk DOI "
                    "10.3886/E241367V1 both return HTTP 403 anonymously, so archive identity is cited from the "
                    "issue record rather than fetched payloads. The existing published HIFLD Next v1.0.0 frozen "
                    "copy (17666 point features, data SHA-256 fc43f476d4181538ad139d0e6504122cf4f258e0dd1d4817981377c62b73258d) "
                    "is retained as comparison-only context: its 11-distinct-value id column, its 17656 rows with "
                    "non-direction values in LatDir/LonDir/url and its declared latdec/londec columns disagreeing "
                    "with its own geometry are recorded schema/ID diagnostics, and its declared-column layout does "
                    "not match the FCC tower table definitions, so no positional mapping to it is applied. The "
                    "maintained feed's population (198010 registrations, 203245 coordinate rows) is materially "
                    "larger than the frozen 17666-row candidate; publishing the current registry instead of the "
                    "frozen snapshot is a compatibility decision for maintainers, not a draft implementation "
                    "blocker. No publication or acceptance is implied; runtime approval flags remain false."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native declared RA/CO/EN fields, identifiers, verbatim strings and WKB points are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved.",
            "shapefile_zip": "No accepted published schema mapping; the raw weekly zip remains lossless evidence."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            body, wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_PUBLISHED_ROWS, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash,
            "source_null_geometry_count": audit["source_null_geometry_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_PUBLISHED_ROWS, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash,
        "source_null_geometry_count": audit["source_null_geometry_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
