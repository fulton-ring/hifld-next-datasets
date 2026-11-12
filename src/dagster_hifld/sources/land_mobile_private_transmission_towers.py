"""Complete native capture of the FCC ULS weekly Land Mobile (Private) licenses file.

The FCC Universal Licensing System weekly complete Land Mobile {Private}
public-access file (l_LMpriv.zip) is the official bulk distribution for the
private land mobile radio services (47 CFR Part 90 pools and related radio
service codes); it is the same ULS data family from which the historical HIFLD
Land Mobile Private Transmission Towers layer was received from the FCC.
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

DATASET_SLUG = "land-mobile-private-transmission-towers"
FILE_SLUG = DATASET_SLUG
# FCC moved the ULS daily and weekly public access files from wireless2.fcc.gov
# to data.fcc.gov in July 2021 (FCC Public Access Updates notice). The weekly
# complete Land Mobile {Private} licenses file l_LMpriv.zip (l_ files are the
# weekly license tables generated under licweekzipdata; a_ files carry the
# weekly application tables) is the current official bulk distribution for the
# private land mobile radio license data.
L_LMPRIV_URL = "https://data.fcc.gov/download/pub/uls/complete/l_LMpriv.zip"
PUBLISHER = ("Federal Communications Commission Wireless Telecommunications Bureau; "
             "official ULS weekly public access complete database files")
CAPTURE_NAMES = {"l_LMpriv"}
MAX_CAPTURE_BYTES = 1_600_000_000
CAPTURE_BODY_LIMIT = 512 * 1024 * 1024
DISK_FLOOR = 4_000_000_000
ZIP_MEMBER_ROSTER = [
    "AN.dat", "AS.dat", "BF.dat", "CO.dat", "CP.dat", "EM.dat", "EN.dat", "F2.dat",
    "FF.dat", "FR.dat", "HD.dat", "HS.dat", "L2.dat", "LA.dat", "LF.dat", "LM.dat",
    "LO.dat", "LS.dat", "MF.dat", "MK.dat", "O2.dat", "OP.dat", "SC.dat", "SF.dat",
    "a3.dat", "counts", "f3.dat", "f5.dat", "f6.dat", "ir.dat", "l3.dat", "l4.dat",
    "l5.dat", "l6.dat", "lc.dat", "ll.dat",
]
RECORD_FIELD_COUNTS = {"HD": 59, "EN": 30, "LO": 51}
EXPECTED_COUNTS_FILE = {
    "AN.dat": 2055559, "AS.dat": 127120, "BF.dat": 3244400, "CO.dat": 544368,
    "CP.dat": 937766, "EM.dat": 9384618, "EN.dat": 1528207, "F2.dat": 38669,
    "FF.dat": 16616, "FR.dat": 5722625, "HD.dat": 861663, "HS.dat": 7644006,
    "L2.dat": 54493, "LA.dat": 52072, "LF.dat": 16283, "LM.dat": 860785,
    "LO.dat": 2008597, "LS.dat": 47921, "MF.dat": 106, "MK.dat": 56,
    "O2.dat": 19, "OP.dat": 92480, "SC.dat": 209176, "SF.dat": 169918,
    "a3.dat": 3941, "f3.dat": 3941, "f5.dat": 8, "f6.dat": 2, "ir.dat": 42137,
    "l3.dat": 3941, "l4.dat": 51, "l5.dat": 86, "l6.dat": 18, "lc.dat": 585,
    "ll.dat": 585,
}
EXPECTED_HD_RECORDS = 861663
EXPECTED_EN_RECORDS = 1528207
EXPECTED_LO_RECORDS = 2008597
EXPECTED_RADIO_SERVICE_COUNTS = {
    "GB": 5258, "GE": 6329, "GF": 540, "GI": 1312, "GJ": 865, "GO": 2030,
    "GP": 997, "GU": 1122, "IG": 609503, "IQ": 120, "PA": 4667, "PB": 229,
    "PW": 178580, "QM": 74, "QQ": 542, "RS": 4654, "SG": 899, "SL": 59,
    "SP": 2, "SY": 2459, "YB": 1672, "YE": 9240, "YF": 1328, "YG": 17574,
    "YI": 1484, "YJ": 917, "YO": 1664, "YP": 1055, "YU": 3558, "YW": 2930,
}
EXPECTED_LICENSE_STATUS_COUNTS = {"A": 332080, "C": 66936, "E": 420397, "T": 42250}
EXPECTED_LICENSEE_ROWS = 861663
# The published population is one row per LO location record whose HD license
# status is A (active) and whose LO record carries both latitude and longitude
# degree components; the historical layer likewise contains only coordinate-
# bearing filed sites (its 605297 rows all carry geometry). Location records
# without coordinates are excluded and pinned by count below. Location records
# in the FCC do-not-convert territories (DO_NOT_CONVERT_STATE_CODES) carry
# local-datum coordinates with no FCC-published conversion to NAD83, so they
# are also excluded from the published coordinate-bearing population and
# preserved in the audit with their filed attributes, pinned by count and
# digest.
EXPECTED_ACTIVE_LOCATION_ROWS = 925853
EXPECTED_MISSING_COORDINATE_ROWS = 322063
EXPECTED_ACTIVE_LOCATIONS = 603775
EXPECTED_LOCAL_DATUM_QUARANTINED_ROWS = 15
EXPECTED_LOCAL_DATUM_QUARANTINED_SHA256 = "2cc2aeaf177775b5fdc485a8ab3fd2a244b054b2c3770a37025801f556472b86"
EXPECTED_DUPLICATE_LOCATION_KEY_COUNT = 42
EXPECTED_PUBLISHED_DUPLICATE_LOCATION_KEY_COUNT = 27
EXPECTED_ACTIVE_ID_SHA256 = "79d0162517b0ccefa27bf0f3151fdc9ee3ad760ebc495e974e2c7de2b7cb57b7"
EXPECTED_STATE_ROSTER_SHA256 = "a0614b1f389cb264f8313538e1422b4adf4618806081a4d3ea1cf9c49f1fa813"
EXPECTED_EMPTY_CALLSIGN_ROWS = 2
EXPECTED_OUT_OF_RANGE_COORDINATE_ROWS = 1
EXPECTED_LAT_DIRECTION_COUNTS = {"": 5, "0": 2, "N": 603759, "S": 9}
EXPECTED_LON_DIRECTION_COUNTS = {"": 6, "3": 2, "E": 82, "N": 1, "W": 603682, "w": 2}
EXPECTED_EXTRA_FIELD_ROWS = {"HD": 1, "EN": 2, "LO": 0}
EXPECTED_BOUNDS = [-179.999972222, -14.3295, 174.099444444, 93.0]
HD_CALLSIGN, HD_STATUS, HD_SERVICE = 4, 5, 6
EN_CALLSIGN, EN_ENTITY_TYPE, EN_NAME = 4, 5, 7
LO_UNIQUE_SYSTEM_IDENTIFIER, LO_CALLSIGN, LO_LOCATION_NUMBER = 1, 4, 8
LO_ADDRESS, LO_CITY, LO_COUNTY, LO_STATE = 11, 12, 13, 14
LO_LAT_DEG, LO_LAT_MIN, LO_LAT_SEC, LO_LAT_DIR = 19, 20, 21, 22
LO_LON_DEG, LO_LON_MIN, LO_LON_SEC, LO_LON_DIR = 23, 24, 25, 26
LO_NEPA, LO_QUIET_ZONE, LO_TOWER_REGISTRATION = 35, 36, 37
LO_SUPPORT_HEIGHT, LO_OVERALL_HEIGHT, LO_STRUCTURE_TYPE = 38, 39, 40
LICENSE_URL_TEMPLATE = "http://wireless2.fcc.gov/UlsApp/UlsSearch/licenseLocSum.jsp?licKey={}"
CREATION_DATE_RE = re.compile(rb"File Creation Date: (.+?)\r?$", re.MULTILINE)
COUNTS_ROW_RE = re.compile(rb"[ \t]+(\d+)[ \t]+\S+/([A-Za-z0-9_.]+)\r?$", re.MULTILINE)
PUBLISHED_SCHEMA = [
    ("geometry", pa.binary()), ("UniqSysID", pa.int64()), ("Licensee", pa.string()),
    ("Callsign", pa.string()), ("LocNum", pa.int64()), ("LatDeg", pa.float64()),
    ("LatMin", pa.float64()), ("LatSec", pa.float64()), ("LatDir", pa.string()),
    ("LonDeg", pa.float64()), ("LonMin", pa.float64()), ("LonSec", pa.float64()),
    ("LonDir", pa.string()), ("LocAdd", pa.string()), ("LocCity", pa.string()),
    ("LocCounty", pa.string()), ("LocState", pa.string()), ("Nepa", pa.string()),
    ("QZone", pa.string()), ("TowReg", pa.string()), ("SupStruc", pa.float64()),
    ("AllStruc", pa.float64()), ("StrucType", pa.string()), ("LicStatus", pa.string()),
    ("latdec", pa.float64()), ("londec", pa.float64()), ("url", pa.string()),
]
COVERAGE_SCOPE = (
    "Every location record with filed coordinates of every active private land "
    "mobile radio license in the weekly ULS public access complete Land Mobile "
    "{Private} licenses file, including mainland, Alaska, Hawaii, offshore and "
    "US territories. Location records whose latitude or longitude degree "
    "components are absent are excluded (the historical layer likewise contains "
    "only coordinate-bearing filed sites), as are location records in the FCC "
    "do-not-convert territories whose ULS coordinates remain in an applicable "
    "local datum with no FCC-published conversion to NAD83 (excluded from the "
    "published coordinate-bearing population and preserved in the audit with "
    "their filed attributes). Private land mobile licensing is "
    "site-specific; ULS carries only the site locations licensees filed, so "
    "this is the filed-site population the FCC publishes, not a census of "
    "physical towers."
)
# FCC ULS policy (https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates):
# "Geographic coordinates provided to the Commission via the Universal Licensing
# System must be referenced to the North American Datum of 1983 (NAD83)", with
# the FCC's territorial conversion table marking American Samoa and Guam as
# converted to NAD83 by specified shifts and certain other Pacific island areas
# as do-not-convert (coordinates remain in the applicable local datum). The
# filed DMS values are therefore published unchanged under an NAD83 (EPSG:4269)
# declaration instead of being relabeled WGS 84; no datum transformation is
# applied and none is available for the do-not-convert areas.
SOURCE_COORDINATE_DATUM = {
    "crs": "EPSG:4269",
    "datum": "NAD83 (North American Datum of 1983)",
    "fcc_policy_url": "https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates",
    "statement": (
        "FCC ULS geographic coordinates must be referenced to the North American Datum of 1983 (NAD83), so "
        "the published geometry declares NAD83 (EPSG:4269) and the filed DMS components and derived "
        "latdec/londec are carried unchanged with no datum transformation or reprojection. Per FCC policy, "
        "American Samoa and Guam filings were converted to NAD83 with the FCC's specified shifts, while "
        "coordinates for the Pacific island areas the FCC marks do-not-convert (Baker, Howland, Jarvis, "
        "Johnston, Kingman Reef, Midway, the Northern Mariana Islands, Palmyra and Wake) remain referenced "
        "to their applicable local datum. The FCC publishes neither the local datum nor a conversion for "
        "those areas, so their filed coordinates cannot be represented accurately under the geometry "
        "column's single declared CRS; such rows are excluded from the published coordinate-bearing "
        "population and preserved in the audit (source_local_datum_quarantined_records) with all filed "
        "attributes, never silently removed, filtered or shifted."
    ),
}
# ULS state codes of the FCC do-not-convert territories. MP is the ANSI/USPS
# code for the Commonwealth of the Northern Mariana Islands (FIPS 69). UM is
# the code for the aggregated U.S. Minor Outlying Islands (FIPS 74), which
# contains the do-not-convert islands Baker, Howland, Jarvis, Johnston,
# Kingman Reef, Midway, Palmyra and Wake (plus Navassa, which is not on the
# FCC's list but cannot be distinguished from a do-not-convert island by the
# ULS state field alone, so the aggregate code is quarantined conservatively).
DO_NOT_CONVERT_STATE_CODES = ("MP", "UM")


def _capture_budget(root: Path) -> None:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * CAPTURE_BODY_LIMIT > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")


def _capture(client: httpx.Client, root: Path) -> tuple[bytes, dict]:
    _capture_budget(root)
    return capture_get(client, L_LMPRIV_URL, root, "l_LMpriv", max_bytes=CAPTURE_BODY_LIMIT)


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
        _validate_capture_request_identity(name, receipt, L_LMPRIV_URL)
        captures[name] = (body, receipt)
    return captures


def _records(raw: bytes, name: str) -> list[list[str]]:
    if raw.endswith(b"\r\n"):
        raw = raw[:-2]
    elif raw.endswith(b"\n") or raw.endswith(b"\r"):
        raw = raw.rstrip(b"\r\n")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("source_record_encoding_drift:" + name) from exc
    return [line.rstrip("\r").split("|") for line in text.split("\n")]


def _zip_tables(body: bytes) -> tuple[dict[str, list[list[str]]], bytes, zipfile.ZipFile]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        raise ValueError("source_zip_unreadable") from exc
    roster = sorted(info.filename for info in archive.infolist())
    if roster != sorted(ZIP_MEMBER_ROSTER):
        raise ValueError("source_zip_member_roster_drift")
    try:
        counts = archive.read("counts")
        raw_hd = archive.read("HD.dat")
        raw_en = archive.read("EN.dat")
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as exc:
        raise ValueError("source_zip_member_unreadable") from exc
    tables = {"HD": _records(raw_hd, "HD"), "EN": _records(raw_en, "EN")}
    return tables, counts, archive


def _parse_counts(counts: bytes) -> tuple[dict[str, int], str]:
    match = CREATION_DATE_RE.search(counts)
    if match is None:
        raise ValueError("source_creation_date_missing")
    creation_date = match.group(1).decode("utf-8", "replace").strip()
    counts_map = {name.decode("utf-8"): int(total) for total, name in COUNTS_ROW_RE.findall(counts)}
    if counts_map != EXPECTED_COUNTS_FILE:
        raise ValueError("source_counts_file_drift")
    return counts_map, creation_date


def _integer(value: str, label: str) -> int:
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ValueError("source_" + label + "_invalid") from exc


def _decimal(value: str, label: str) -> float | None:
    text = value.strip()
    if not text:
        return None
    try:
        result = float(text)
    except ValueError as exc:
        raise ValueError("source_" + label + "_invalid") from exc
    if result != result or result in (float("inf"), float("-inf")):
        raise ValueError("source_" + label + "_invalid")
    return result


def _decimal_degrees(degrees: float, minutes: float | None, seconds: float | None,
                     sign: int) -> float:
    # The historical published layer derived latdec/londec from its own DMS
    # components with this formula at 12 significant digits (605289 of its
    # 605297 rows agree with their own DMS columns). Only "S" marks southern
    # latitude and only "E" marks eastern longitude: missing or non-canonical
    # direction components default to north/west, matching the historical
    # extract's convention (frozen rows whose source direction is empty carry
    # positive latdec and negative londec with identical DMS).
    value = degrees + (minutes or 0.0) / 60 + (seconds or 0.0) / 3600
    return float(f"{sign * value:.12g}")


def _point_wkb(longitude: float, latitude: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, longitude, latitude)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    body, receipt = captures["l_LMpriv"]
    _validate_capture_request_identity("l_LMpriv", receipt, L_LMPRIV_URL)
    if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
        raise ValueError("source_capture_status_length_hash_or_completeness_drift")
    tables, counts, archive = _zip_tables(body)
    for name in ("HD", "EN"):
        rows = tables[name]
        expected = RECORD_FIELD_COUNTS[name]
        for row in rows:
            if len(row) == expected:
                continue
            # The pinned edition contains a small number of ULS records with
            # one extra empty trailing field; tolerate exactly that artifact
            # (the trailing empty is dropped, published fields are unaffected)
            # and fail closed on anything else.
            if len(row) == expected + 1 and row[-1] == "":
                continue
            raise ValueError("source_record_field_count_drift:" + name)
        if any(row[0] != name for row in rows):
            raise ValueError("source_record_type_prefix_drift:" + name)
    if len(tables["HD"]) != EXPECTED_HD_RECORDS or len(tables["EN"]) != EXPECTED_EN_RECORDS:
        raise ValueError("source_record_population_drift")
    counts_map, creation_date = _parse_counts(counts)
    for name, expected_total in (("HD", EXPECTED_HD_RECORDS), ("EN", EXPECTED_EN_RECORDS),
                                 ("LO", EXPECTED_LO_RECORDS)):
        if counts_map.get(name + ".dat") != expected_total:
            raise ValueError("source_counts_file_record_total_drift:" + name)

    header = {}
    status_counts = {}
    service_counts = {}
    for row in tables["HD"]:
        identifier = _integer(row[1], "unique_system_identifier")
        if identifier in header:
            raise ValueError("source_duplicate_license_identity")
        if not row[HD_STATUS]:
            raise ValueError("source_license_status_missing")
        status_counts[row[HD_STATUS]] = status_counts.get(row[HD_STATUS], 0) + 1
        service_counts[row[HD_SERVICE]] = service_counts.get(row[HD_SERVICE], 0) + 1
        header[identifier] = (row[HD_CALLSIGN], row[HD_STATUS])
    if service_counts != EXPECTED_RADIO_SERVICE_COUNTS:
        raise ValueError("source_radio_service_roster_drift")
    if status_counts != EXPECTED_LICENSE_STATUS_COUNTS:
        raise ValueError("source_license_status_distribution_drift")
    active_licenses = {identifier for identifier, (_call, status) in header.items() if status == "A"}

    licensees: dict[int, str] = {}
    licensee_rows = 0
    for row in tables["EN"]:
        if row[EN_ENTITY_TYPE] != "L":
            continue
        licensee_rows += 1
        identifier = _integer(row[1], "unique_system_identifier")
        if identifier in licensees:
            raise ValueError("source_duplicate_licensee_row")
        licensees[identifier] = row[EN_NAME]
    if licensee_rows != EXPECTED_LICENSEE_ROWS or set(licensees) != set(header):
        raise ValueError("source_licensee_join_drift")

    keys = set()
    duplicate_keys = 0
    published = []
    quarantined_records = []
    missing_coordinates = 0
    empty_callsign_rows = 0
    out_of_range_rows = 0
    local_datum_quarantined_rows = 0
    lo_extra_field_rows = 0
    lo_rows = 0
    active_location_rows = 0
    lat_direction_counts = {}
    lon_direction_counts = {}
    try:
        raw_lo = archive.read("LO.dat")
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as exc:
        raise ValueError("source_zip_member_unreadable") from exc
    if raw_lo.endswith(b"\r\n"):
        raw_lo = raw_lo[:-2]
    elif raw_lo.endswith(b"\n") or raw_lo.endswith(b"\r"):
        raw_lo = raw_lo.rstrip(b"\r\n")
    try:
        lo_text = raw_lo.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("source_record_encoding_drift:LO") from exc
    del raw_lo
    expected_lo_width = RECORD_FIELD_COUNTS["LO"]
    for line in lo_text.split("\n"):
        row = line.rstrip("\r").split("|")
        lo_rows += 1
        if len(row) == expected_lo_width:
            pass
        elif len(row) == expected_lo_width + 1 and row[-1] == "":
            lo_extra_field_rows += 1
        else:
            raise ValueError("source_record_field_count_drift:LO")
        if row[0] != "LO":
            raise ValueError("source_record_type_prefix_drift:LO")
        identifier = _integer(row[LO_UNIQUE_SYSTEM_IDENTIFIER], "unique_system_identifier")
        if not row[LO_CALLSIGN].strip():
            empty_callsign_rows += 1
        if identifier not in header or row[LO_CALLSIGN] != header[identifier][0]:
            raise ValueError("source_callsign_join_drift")
        location_number = _integer(row[LO_LOCATION_NUMBER], "location_number")
        key = (identifier, location_number)
        if key in keys:
            duplicate_keys += 1
        keys.add(key)
        if header[identifier][1] != "A":
            continue
        active_location_rows += 1
        lat_deg = _decimal(row[LO_LAT_DEG], "latitude_degrees")
        lon_deg = _decimal(row[LO_LON_DEG], "longitude_degrees")
        if lat_deg is None or lon_deg is None:
            # Filed locations without a complete coordinate pair are excluded
            # from the published population, matching the historical layer.
            missing_coordinates += 1
            continue
        lat_min = _decimal(row[LO_LAT_MIN], "latitude_minutes")
        lon_min = _decimal(row[LO_LON_MIN], "longitude_minutes")
        lat_sec = _decimal(row[LO_LAT_SEC], "latitude_seconds")
        lon_sec = _decimal(row[LO_LON_SEC], "longitude_seconds")
        lat_dir, lon_dir = row[LO_LAT_DIR], row[LO_LON_DIR]
        # Raw DMS components and direction strings are preserved verbatim; the
        # FCC's own layer description warns of coordinate errors in the
        # licensing data. The pinned edition contains one filed latitude
        # (93 degrees) whose derived decimal leaves the nominal range and a
        # handful of rows whose direction components are empty or non-canonical
        # ("0", "3", lowercase "w"); all are published as filed — with the
        # derived columns following the historical extract's hemisphere
        # convention — and pinned by count instead of being clamped, dropped or
        # hard-failed.
        lat_sign = -1 if lat_dir == "S" else 1
        lon_sign = 1 if lon_dir == "E" else -1
        lat_dec = _decimal_degrees(lat_deg, lat_min, lat_sec, lat_sign)
        lon_dec = _decimal_degrees(lon_deg, lon_min, lon_sec, lon_sign)
        if identifier not in licensees:
            raise ValueError("source_licensee_join_drift")
        record = {
            "UniqSysID": identifier, "Licensee": licensees[identifier],
            "Callsign": row[LO_CALLSIGN], "LocNum": location_number,
            "LatDeg": lat_deg, "LatMin": lat_min, "LatSec": lat_sec, "LatDir": row[LO_LAT_DIR],
            "LonDeg": lon_deg, "LonMin": lon_min, "LonSec": lon_sec, "LonDir": row[LO_LON_DIR],
            "LocAdd": row[LO_ADDRESS], "LocCity": row[LO_CITY], "LocCounty": row[LO_COUNTY],
            "LocState": row[LO_STATE], "Nepa": row[LO_NEPA], "QZone": row[LO_QUIET_ZONE],
            "TowReg": row[LO_TOWER_REGISTRATION], "SupStruc": _decimal(row[LO_SUPPORT_HEIGHT], "support_height"),
            "AllStruc": _decimal(row[LO_OVERALL_HEIGHT], "overall_height"),
            "StrucType": row[LO_STRUCTURE_TYPE], "LicStatus": "A",
            "latdec": lat_dec, "londec": lon_dec,
            "url": LICENSE_URL_TEMPLATE.format(identifier),
        }
        if row[LO_STATE] in DO_NOT_CONVERT_STATE_CODES:
            # FCC do-not-convert territory: the filed coordinates remain in the
            # applicable local datum, the FCC publishes neither the local datum
            # nor a conversion to NAD83, and a GeoParquet column declares one
            # CRS for every row, so these filings cannot be represented
            # accurately under the published geometry contract. They are
            # excluded from the published coordinate-bearing population and
            # preserved in full in the audit, pinned by count and digest —
            # never silently removed, shifted or mislabeled NAD83.
            local_datum_quarantined_rows += 1
            quarantined_records.append(record)
            continue
        if not -90 <= lat_dec <= 90 or not -180 <= lon_dec <= 180:
            out_of_range_rows += 1
        lat_direction_counts[lat_dir] = lat_direction_counts.get(lat_dir, 0) + 1
        lon_direction_counts[lon_dir] = lon_direction_counts.get(lon_dir, 0) + 1
        record["geometry"] = _point_wkb(lon_dec, lat_dec)
        published.append(record)
    if lo_rows != EXPECTED_LO_RECORDS:
        raise ValueError("source_record_population_drift")
    if lo_extra_field_rows != EXPECTED_EXTRA_FIELD_ROWS["LO"]:
        raise ValueError("source_extra_field_row_count_drift:LO")
    hd_extra_field_rows = sum(1 for row in tables["HD"] if len(row) != RECORD_FIELD_COUNTS["HD"])
    en_extra_field_rows = sum(1 for row in tables["EN"] if len(row) != RECORD_FIELD_COUNTS["EN"])
    if (hd_extra_field_rows != EXPECTED_EXTRA_FIELD_ROWS["HD"]
            or en_extra_field_rows != EXPECTED_EXTRA_FIELD_ROWS["EN"]):
        raise ValueError("source_extra_field_row_count_drift")
    if empty_callsign_rows != EXPECTED_EMPTY_CALLSIGN_ROWS:
        raise ValueError("source_empty_callsign_row_count_drift")
    if active_location_rows != EXPECTED_ACTIVE_LOCATION_ROWS:
        raise ValueError("source_active_location_row_count_drift")
    if missing_coordinates != EXPECTED_MISSING_COORDINATE_ROWS:
        raise ValueError("source_missing_coordinate_row_count_drift")
    if local_datum_quarantined_rows != EXPECTED_LOCAL_DATUM_QUARANTINED_ROWS:
        raise ValueError("source_local_datum_quarantined_row_count_drift")
    quarantined_sha = _sha(_canonical(quarantined_records))
    if quarantined_sha != EXPECTED_LOCAL_DATUM_QUARANTINED_SHA256:
        raise ValueError("source_local_datum_quarantined_records_drift")
    if out_of_range_rows != EXPECTED_OUT_OF_RANGE_COORDINATE_ROWS:
        raise ValueError("source_out_of_range_coordinate_row_count_drift")
    if lat_direction_counts != EXPECTED_LAT_DIRECTION_COUNTS:
        raise ValueError("source_latitude_direction_distribution_drift")
    if lon_direction_counts != EXPECTED_LON_DIRECTION_COUNTS:
        raise ValueError("source_longitude_direction_distribution_drift")
    if len(published) != EXPECTED_ACTIVE_LOCATIONS:
        raise ValueError("source_active_population_drift")
    if duplicate_keys != EXPECTED_DUPLICATE_LOCATION_KEY_COUNT:
        raise ValueError("source_duplicate_location_key_count_drift")
    published_duplicate_keys = len(published) - len({(item["UniqSysID"], item["LocNum"]) for item in published})
    if published_duplicate_keys != EXPECTED_PUBLISHED_DUPLICATE_LOCATION_KEY_COUNT:
        raise ValueError("source_published_duplicate_location_key_count_drift")
    published.sort(key=lambda item: (item["UniqSysID"], item["LocNum"]))
    roster = [[item["UniqSysID"], item["LocNum"]] for item in published]
    if _sha(_canonical(roster)) != EXPECTED_ACTIVE_ID_SHA256:
        raise ValueError("source_active_id_roster_drift")
    states = sorted({item["LocState"].strip() for item in published if item["LocState"].strip()})
    if _sha(_canonical(states)) != EXPECTED_STATE_ROSTER_SHA256:
        raise ValueError("source_state_roster_drift")
    record_sha = _sha(_canonical([{key: item[key] for key, _ in PUBLISHED_SCHEMA if key != "geometry"}
                                  for item in published]))
    geometry_sha = _sha(_canonical([[item["UniqSysID"], item["LocNum"], item["londec"], item["latdec"]]
                                     for item in published]))
    coordinates = [(item["londec"], item["latdec"]) for item in published]
    bounds = [min(x for x, _y in coordinates), min(y for _x, y in coordinates),
              max(x for x, _y in coordinates), max(y for _x, y in coordinates)]
    if bounds != EXPECTED_BOUNDS:
        raise ValueError("source_bounds_drift")
    schema_contract = {"zip_members": sorted(ZIP_MEMBER_ROSTER), "record_field_counts": RECORD_FIELD_COUNTS,
                       "published_columns": [name for name, _ in PUBLISHED_SCHEMA]}
    audit = {
        "source_feature_count": len(published), "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(schema_contract)),
        "source_active_id_sha256": _sha(_canonical(roster)),
        "source_state_roster_sha256": _sha(_canonical(states)),
        "source_hd_record_count": len(tables["HD"]), "source_en_record_count": len(tables["EN"]),
        "source_lo_record_count": lo_rows,
        "source_license_status_counts": status_counts,
        "source_radio_service_codes": sorted(EXPECTED_RADIO_SERVICE_COUNTS),
        "source_counts_file": counts_map, "source_file_creation_date": creation_date,
        "source_empty_callsign_rows": empty_callsign_rows,
        "source_missing_coordinate_rows": missing_coordinates,
        "source_out_of_range_coordinate_rows": out_of_range_rows,
        "source_local_datum_quarantined_rows": local_datum_quarantined_rows,
        "source_local_datum_quarantined_sha256": quarantined_sha,
        "source_local_datum_quarantined_state_codes": sorted({record["LocState"] for record in quarantined_records}),
        "source_local_datum_quarantined_records": quarantined_records,
        "source_latitude_direction_counts": lat_direction_counts,
        "source_longitude_direction_counts": lon_direction_counts,
        "source_extra_field_rows": {"HD": hd_extra_field_rows, "EN": en_extra_field_rows,
                                    "LO": lo_extra_field_rows},
        "source_duplicate_location_key_count": duplicate_keys,
        "source_published_duplicate_location_key_count": published_duplicate_keys,
        "source_bounds": bounds, "source_state_count": len(states),
        "source_coverage_scope": COVERAGE_SCOPE,
        "source_coordinate_datum": SOURCE_COORDINATE_DATUM,
        "drift_guard": ("Exact capture roster, HTTP status/completeness/length/hash receipt, zip member roster, "
                        "counts-file totals cross-checked against parsed HD/EN/LO records, per-record-type field "
                        "counts and record-type prefixes with only the pinned empty-trailing-field artifact rows "
                        "tolerated, radio service and license status distributions, unique license and licensee "
                        "identities, LO/HD callsign join, unique USID join, active coordinate-bearing population "
                        "count, full active ID roster digest (duplicate location keys preserved as distinct filed "
                        "records: 42 duplicate occurrences across all filed rows, 27 within the published "
                        "coordinate-bearing population), state roster digest, latitude/longitude direction distributions with the "
                        "historical hemisphere default (only S marks southern latitude and only E marks eastern "
                        "longitude; missing or non-canonical direction components default to north/west), bounds, "
                        "empty-callsign, missing-coordinate, out-of-range-coordinate and extra-field row counts are "
                        "checked; counts drift fails closed. Raw DMS components and direction strings are preserved "
                        "verbatim, including the pinned out-of-range filed latitude (93 degrees) that the FCC's "
                        "layer description anticipates. Location records in the FCC do-not-convert territories "
                        "(state codes MP, UM; coordinates remain in their applicable local datum with no "
                        "FCC-published conversion to NAD83) are excluded from the published coordinate-bearing "
                        "population and preserved in full in the audit, pinned by count and record digest; "
                        "counts drift fails closed."),
    }
    return published, audit


def _parquet_bytes(published: list[dict]) -> bytes:
    arrays = [pa.array([item[name] for item in published], type=kind) for name, kind in PUBLISHED_SCHEMA]
    table = pa.Table.from_arrays(arrays, names=[name for name, _ in PUBLISHED_SCHEMA])
    # FCC ULS coordinates are NAD83 by FCC policy (with the documented local-datum
    # exceptions in SOURCE_COORDINATE_DATUM), so the geometry column declares
    # EPSG:4269; the WKB coordinates themselves are the unchanged DMS conversions.
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "NAD83",
           "datum": {"type": "GeodeticReferenceFrame", "name": "North American Datum 1983",
                     "ellipsoid": {"name": "GRS 1980", "semi_major_axis": 6378137, "inverse_flattening": 298.257222101},
                     "id": {"authority": "EPSG", "code": 6269}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4269}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                   "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    digests = {}
    for name in sorted(captures):
        _body, wire, _receipt = _load_capture(root, name)
        digests[name] = {"decoded_sha256": _sha(captures[name][0]), "wire_sha256": _sha(wire)}
    return _sha(_canonical(digests))


def capture_land_mobile_private_transmission_towers(storage: StagingStorageResource, *, version: str,
                                                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved FCC ULS Land Mobile (Private) source candidate."""
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
                Path("/tmp/hifld-native-reconstruction/134/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(600, connect=20),
                              headers={"User-Agent": "hifld-next-datasets/fcc-uls-draft"}) as client:
                captures["l_LMpriv"] = _capture(client, root)
            published, audit = _validate(captures)
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

    published, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(published)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": "l_LMpriv:" + receipts["l_LMpriv"]["sha256"], "producer": PUBLISHER,
        "host": urlsplit(receipts["l_LMpriv"]["url"]).hostname, "role": "producer_publication",
        "canonical_url": L_LMPRIV_URL, "retrieval_url": receipts["l_LMpriv"]["url"],
        "observed_final_url": receipts["l_LMpriv"]["final_url"], "retrieved_at": receipts["l_LMpriv"]["observed_at"],
        "http_status": receipts["l_LMpriv"]["status"], "bytes": receipts["l_LMpriv"]["bytes"],
        "sha256": receipts["l_LMpriv"]["sha256"], "wire_bytes": receipts["l_LMpriv"]["wire_bytes"],
        "wire_sha256": receipts["l_LMpriv"]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The weekly complete file is regenerated every Sunday; the payload's own "
                                        "counts file creation date is retained in the audit while no per-release "
                                        "edition date is declared by the FCC for this distribution."),
        "license": "Unresolved; rights review pending.", "attribution": "FCC Wireless Telecommunications Bureau ULS public access; attribution and rights review pending."}]
    manifest = {"schema_version": 1, "approved": False, "title": "Land Mobile Private Transmission Towers (FCC ULS)",
        "description": ("Location records with filed coordinates of active private land mobile radio licenses from "
                        "the FCC ULS weekly public access complete Land Mobile {Private} licenses file, republished "
                        "under the historical HIFLD Land Mobile Private Transmission Towers column contract. "
                        "Location records without a complete coordinate pair and location records in the FCC "
                        "do-not-convert local-datum territories (coordinates with no FCC-published conversion to "
                        "NAD83, preserved in the audit) are excluded. Unapproved candidate; source edition, "
                        "baseline, schema/ID contract, rights and release decisions remain pending."),
        "publisher": PUBLISHER, "agency": "Federal Communications Commission", "source_url": L_LMPRIV_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "ULS pipe-delimited licenses file to GeoParquet candidate", "parameters": {
            "source_tables": "HD license header, EN licensee entity and LO location tables of l_LMpriv.zip decoded as pipe-delimited UTF-8 records",
            "population": ("one published row per LO location record whose HD license status is A (active) and "
                           "whose LO record carries both latitude and longitude degree components; location "
                           "records without a complete coordinate pair are excluded (322063 rows in the pinned "
                           "edition) to match the historical layer's coordinate-bearing population, and location "
                           "records in the FCC do-not-convert territories (state codes MP, UM; coordinates remain "
                           "in their applicable local datum with no FCC-published conversion to NAD83) are also "
                           "excluded and preserved in the audit with all filed attributes (15 rows in the pinned "
                           "edition: 12 MP, 3 UM), never silently removed, shifted or mislabeled NAD83"),
            "source_ids": "UniqSysID (HD/EN/LO unique system identifier) and LocNum (LO location number) retained unchanged; duplicate (UniqSysID, LocNum) filed records are preserved as distinct rows as in the historical layer",
            "source_values": "all published attribute strings retained verbatim including empty fields; TowReg kept as the source string (the frozen copy typed it int64 with 0 for unregistered records and the source also carries N/A values); SupStruc/AllStruc typed float with null for empty source fields (the frozen copy stored 0.0 there); DMS degree/minute components typed float to match the frozen contract",
            "derived_columns": ("latdec/londec are DMS-to-decimal conversions of the raw components formatted "
                                "to 12 significant digits, matching the historical published layer's derived-"
                                "column convention (missing minutes or seconds contribute zero; only S marks "
                                "southern latitude and only E marks eastern longitude, so missing or non-canonical "
                                "direction components default to north/west as in the historical extract); url "
                                "embeds UniqSysID as the ULS license-location key"),
            "geometry": ("WKB Point (londec, latdec) declared in the FCC ULS source datum NAD83 (EPSG:4269); "
                        "the filed DMS components and derived decimals are published unchanged with no datum "
                        "transformation, reprojection, repair or geocoding. FCC policy requires ULS "
                        "coordinates in NAD83 (https://www.fcc.gov/wireless/systems-utilities/"
                        "north-american-datum-1983-coordinates): American Samoa and Guam filings were "
                        "converted to NAD83 with the FCC's specified shifts, while the do-not-convert "
                        "Pacific island areas (Baker, Howland, Jarvis, Johnston, Kingman Reef, Midway, the "
                        "Northern Mariana Islands, Palmyra and Wake) remain in their applicable local datum "
                        "with no FCC-published conversion; those filings are excluded from the published "
                        "coordinate-bearing population and preserved in the audit with all filed attributes, "
                        "because a GeoParquet column declares one CRS for every row and their datum cannot be "
                        "represented accurately."),
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("The FCC moved ULS daily and weekly public access downloads from wireless2.fcc.gov to "
                    "data.fcc.gov/download/pub/uls/ in July 2021; the weekly complete Land Mobile {Private} "
                    "licenses file l_LMpriv.zip is the current official bulk distribution of the same ULS data "
                    "family from which the historical HIFLD Land Mobile Private Transmission Towers layer was "
                    "received from the FCC. The archived HIFLD OPEN layer (DataLumos project 239397) and the DHS "
                    "crosswalk DOI 10.3886/E241367V1 both return HTTP 403 anonymously, so archive identity is "
                    "cited from the DataLumos landing page rather than fetched payloads. The existing published "
                    "HIFLD Next v1.0.0 frozen copy (605297 point features, issued 2024-01-24) is retained as "
                    "comparison-only context: its (UniqSysID, LocNum) roster overlaps this candidate's coordinate-"
                    "bearing keys on 545011 of 605297; 60284 frozen-only keys no longer carry coordinates in this "
                    "edition (84 of them remain filed without coordinates, the rest are absent from the current "
                    "file), 58752 current-only keys were filed or re-coordinated since, and 1976 overlapping rows "
                    "have source-edited coordinates while stable rows agree field-for-field. Frozen-copy "
                    "artifacts (TowReg typed int64 with 0 for unregistered records, single-space placeholders for "
                    "empty strings, one bbox column, and 8 rows whose stored latdec/londec carry more precision "
                    "than their own DMS columns derive) are documented as historical extraction artifacts, not "
                    "source values. Private land mobile licensing is site-specific; ULS location records are the "
                    "sites licensees filed, so this candidate publishes the FCC's filed-site population, not a "
                    "census of physical towers. No publication or acceptance is implied; runtime approval flags "
                    "remain false."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native published-contract columns, identifiers, verbatim strings and WKB points are retained in a typed candidate.",
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
            "feature_count": EXPECTED_ACTIVE_LOCATIONS, "source_population_complete": True,
            "source_id_sha256": audit["source_active_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash,
            "source_missing_coordinate_rows": audit["source_missing_coordinate_rows"],
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
        "feature_count": EXPECTED_ACTIVE_LOCATIONS, "source_population_complete": True,
        "source_id_sha256": audit["source_active_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash,
        "source_missing_coordinate_rows": audit["source_missing_coordinate_rows"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
