"""Complete native capture of the FCC ULS weekly microwave public-access licenses file."""
from __future__ import annotations

import io
import json
import re
import shutil
import struct
import zipfile
from collections import Counter
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

DATASET_SLUG = "microwave-service-towers"
FILE_SLUG = DATASET_SLUG
# FCC moved the ULS daily and weekly public access files from wireless2.fcc.gov
# to data.fcc.gov in July 2021 (FCC Public Access Updates notice). The weekly
# complete microwave licenses file l_micro.zip is the current official bulk
# distribution for the ULS Microwave service group covering 47 CFR Part 101
# fixed microwave, the rule part the historical HIFLD Microwave Service Towers
# layer description cites.
L_MICRO_URL = "https://data.fcc.gov/download/pub/uls/complete/l_micro.zip"
PUBLISHER = ("Federal Communications Commission Wireless Telecommunications Bureau; "
             "official ULS weekly public access complete database files")
CAPTURE_NAMES = {"l_micro"}
MAX_CAPTURE_BYTES = 960_000_000
CAPTURE_BODY_LIMIT = 240_000_000
DISK_FLOOR = 1_500_000_000
ZIP_MEMBER_ROSTER = [
    "AN.dat", "AS.dat", "BC.dat", "BF.dat", "CO.dat", "CP.dat", "CS.dat", "EC.dat",
    "EM.dat", "EN.dat", "F2.dat", "FF.dat", "FR.dat", "HD.dat", "HS.dat", "L2.dat",
    "LA.dat", "LF.dat", "LM.dat", "LO.dat", "LS.dat", "MF.dat", "MK.dat", "MW.dat",
    "OP.dat", "PA.dat", "RC.dat", "SC.dat", "SF.dat", "SG.dat", "a3.dat", "counts",
    "f3.dat", "f5.dat", "f6.dat", "ir.dat", "lc.dat", "ll.dat", "p2.dat", "tp.dat",
]
RECORD_FIELD_COUNTS = {"HD": 59, "EN": 30, "LO": 51}
EXPECTED_HD_RECORDS = 367714
EXPECTED_EN_RECORDS = 708208
EXPECTED_LO_RECORDS = 926693
EXPECTED_LICENSEE_ROWS = 367714
# The pinned 2026-11-08 weekly edition carries the complete ULS Microwave
# service-group radio service roster: Part 101 fixed microwave (CF common
# carrier, MG operational-fixed, MM/NN multiple address, PF private fixed,
# CT local television transmission) plus the co-located ULS broadcast
# auxiliary codes (AB, AI, AS, MW, TB, TI, TP, TS, TT) and WA/PE.
EXPECTED_HD_RADIO_SERVICES = [
    "AB", "AI", "AS", "CE", "CF", "CT", "MG", "MM", "MW", "NN", "PE", "PF", "TB",
    "TI", "TP", "TS", "TT", "WA",
]
EXPECTED_LICENSE_STATUS_COUNTS = {"A": 165339, "C": 133569, "P": 516, "E": 48979, "T": 19311}
EXPECTED_ACTIVE_LOCATIONS = 422808
EXPECTED_ACTIVE_ID_SHA256 = "0cd9dcf48276480d7050d20d3525ccbeb92ed7b27e50384f8772dca004d6275b"
EXPECTED_STATE_ROSTER_SHA256 = "87db4904970d966a96dcda07f1fe4c46e3b838c10a25a13b1a9be985f6657a8f"
EXPECTED_NULL_GEOMETRY_COUNT = 449
# The source itself carries duplicate (UniqSysID, LocNum) location rows
# (re-filed coordinates for one location number); they are published verbatim
# rather than dropped or de-duplicated. The 16-key roster is pinned so any
# change fails closed for review.
EXPECTED_DUPLICATE_LOCATION_KEYS = 16
EXPECTED_DUPLICATE_KEY_SHA256 = "3446a607b62c6fbcd3a30c2bfc68ab83b6d77f86c30e5fa59e3d5beabe99d929"
EXPECTED_ACTIVE_SERVICE_ROW_COUNTS = {
    "AB": 10, "AI": 1394, "AS": 18744, "CF": 139508, "CT": 175, "MG": 143695,
    "MM": 20, "MW": 90425, "PF": 13365, "TB": 2, "TI": 7810, "TP": 2830,
    "TS": 4220, "TT": 600, "WA": 10,
}
EXPECTED_INVALID_DIRECTION_COUNT = 3
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
PUBLISHED_SCHEMA = [
    ("geometry", pa.binary()), ("UniqSysID", pa.int64()), ("Licensee", pa.string()),
    ("Callsign", pa.string()), ("LocNum", pa.int64()), ("LatDeg", pa.int64()),
    ("LatMin", pa.int64()), ("LatSec", pa.float64()), ("LatDir", pa.string()),
    ("LonDeg", pa.int64()), ("LonMin", pa.int64()), ("LonSec", pa.float64()),
    ("LonDir", pa.string()), ("LocAdd", pa.string()), ("LocCity", pa.string()),
    ("LocCounty", pa.string()), ("LocState", pa.string()), ("Nepa", pa.string()),
    ("QZone", pa.string()), ("TowReg", pa.string()), ("SupStruc", pa.float64()),
    ("AllStruc", pa.float64()), ("StrucType", pa.string()), ("LicStatus", pa.string()),
    ("latdec", pa.float64()), ("londec", pa.float64()), ("url", pa.string()),
]
COVERAGE_SCOPE = (
    "Every location record of every active license in the weekly ULS public access "
    "complete Microwave service-group licenses file, covering 47 CFR Part 101 fixed "
    "microwave (common carrier, operational fixed, multiple address, private fixed and "
    "local television transmission codes) plus the broadcast auxiliary microwave codes "
    "co-located in that ULS file group, across the mainland, Alaska, Hawaii, Gulf of "
    "Mexico offshore sites and US territories. Microwave licenses authorize point links "
    "and the ULS carries the locations licensees filed for them, so this is the "
    "filed-site population the FCC publishes, not a census of physical towers."
)


def _capture_budget(root: Path) -> None:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * CAPTURE_BODY_LIMIT > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")


def _capture(client: httpx.Client, root: Path) -> tuple[bytes, dict]:
    _capture_budget(root)
    return capture_get(client, L_MICRO_URL, root, "l_micro", max_bytes=CAPTURE_BODY_LIMIT)


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
        _validate_capture_request_identity(name, receipt, L_MICRO_URL)
        captures[name] = (body, receipt)
    return captures


def _iter_rows(text: str, label: str):
    """Stream one pipe-delimited ULS table, validating field counts per row."""
    expected = RECORD_FIELD_COUNTS[label]
    for line in text.split("\n"):
        row = line.split("|")
        if len(row) != expected:
            raise ValueError("source_record_field_count_drift:" + label)
        if row[0] != label:
            raise ValueError("source_record_type_prefix_drift:" + label)
        yield row


def _member_texts(body: bytes) -> tuple[dict[str, str], bytes]:
    """Decode the HD/EN/LO tables and counts file, retaining the raw text."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        raise ValueError("source_zip_unreadable") from exc
    roster = sorted(info.filename for info in archive.infolist())
    if roster != sorted(ZIP_MEMBER_ROSTER):
        raise ValueError("source_zip_member_roster_drift")
    texts: dict[str, str] = {}
    try:
        for member, label in (("HD.dat", "HD"), ("EN.dat", "EN"), ("LO.dat", "LO")):
            raw = archive.read(member)
            if raw.endswith(b"\r\n"):
                raw = raw[:-2]
            elif raw.endswith(b"\n") or raw.endswith(b"\r"):
                raw = raw.rstrip(b"\r\n")
            try:
                texts[label] = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("source_record_encoding_drift:" + label) from exc
        counts = archive.read("counts")
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as exc:
        raise ValueError("source_zip_member_unreadable") from exc
    return texts, counts


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


def _decimal_degrees(degrees: int, minutes: int, seconds: float | None,
                     direction: str, negative_direction: str) -> float:
    value = degrees + minutes / 60 + (seconds or 0.0) / 3600
    if direction == negative_direction:
        value = -value
    # The historical published layer derived latdec/londec with this plain
    # formula formatted to 12 significant digits; direction letters outside the
    # nominal set do not flip the sign and are preserved verbatim.
    return float(f"{value:.12g}")


def _point_wkb(longitude: float, latitude: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, longitude, latitude)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    body, receipt = captures["l_micro"]
    _validate_capture_request_identity("l_micro", receipt, L_MICRO_URL)
    if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
        raise ValueError("source_capture_status_length_hash_or_completeness_drift")
    texts, counts = _member_texts(body)
    match = CREATION_DATE_RE.search(counts)
    if match is None:
        raise ValueError("source_creation_date_missing")
    creation_date = match.group(1).decode("utf-8", "replace").strip()

    header = {}
    status_counts = {}
    hd_records = 0
    for row in _iter_rows(texts["HD"], "HD"):
        hd_records += 1
        identifier = _integer(row[1], "unique_system_identifier")
        if identifier in header:
            raise ValueError("source_duplicate_license_identity")
        if row[HD_SERVICE] not in EXPECTED_HD_RADIO_SERVICES:
            raise ValueError("source_radio_service_roster_drift")
        status = row[HD_STATUS]
        if not status:
            raise ValueError("source_license_status_missing")
        status_counts[status] = status_counts.get(status, 0) + 1
        header[identifier] = (row[HD_CALLSIGN], status, row[HD_SERVICE])
    if hd_records != EXPECTED_HD_RECORDS:
        raise ValueError("source_record_population_drift")
    if status_counts != EXPECTED_LICENSE_STATUS_COUNTS:
        raise ValueError("source_license_status_distribution_drift")

    licensees: dict[int, str] = {}
    licensee_rows = 0
    en_records = 0
    for row in _iter_rows(texts["EN"], "EN"):
        en_records += 1
        if row[EN_ENTITY_TYPE] != "L":
            continue
        licensee_rows += 1
        identifier = _integer(row[1], "unique_system_identifier")
        if identifier in licensees:
            raise ValueError("source_duplicate_licensee_row")
        licensees[identifier] = row[EN_NAME]
    if en_records != EXPECTED_EN_RECORDS:
        raise ValueError("source_record_population_drift")
    if licensee_rows != EXPECTED_LICENSEE_ROWS or set(licensees) != set(header):
        raise ValueError("source_licensee_join_drift")

    keys = Counter()
    published = []
    null_geometry = 0
    invalid_directions = 0
    service_row_counts = Counter()
    lo_records = 0
    for row in _iter_rows(texts["LO"], "LO"):
        lo_records += 1
        identifier = _integer(row[LO_UNIQUE_SYSTEM_IDENTIFIER], "unique_system_identifier")
        if identifier not in header or row[LO_CALLSIGN] != header[identifier][0]:
            raise ValueError("source_callsign_join_drift")
        location_number = _integer(row[LO_LOCATION_NUMBER], "location_number")
        _callsign, status, service = header[identifier]
        if status != "A":
            continue
        keys[(identifier, location_number)] += 1
        service_row_counts[service] += 1
        lat_deg_text, lon_deg_text = row[LO_LAT_DEG].strip(), row[LO_LON_DEG].strip()
        if bool(lat_deg_text) != bool(lon_deg_text):
            raise ValueError("source_partial_dms_drift")
        if not lat_deg_text:
            null_geometry += 1
            lat_deg = lon_deg = lat_dec = lon_dec = None
            lat_min = lon_min = lat_sec = lon_sec = None
        else:
            lat_dir, lon_dir = row[LO_LAT_DIR].strip(), row[LO_LON_DIR].strip()
            if lat_dir not in {"N", "S"} or lon_dir not in {"E", "W"}:
                invalid_directions += 1
            lat_deg = _integer(lat_deg_text, "latitude_degrees")
            lon_deg = _integer(lon_deg_text, "longitude_degrees")
            lat_sec = _decimal(row[LO_LAT_SEC], "latitude_seconds")
            lon_sec = _decimal(row[LO_LON_SEC], "longitude_seconds")
            # Raw DMS components are preserved verbatim, including direction
            # letters outside the nominal set and degenerate rows the FCC's
            # own layer description warns about ("It is known that there are
            # some errors in the licensing information - Latitude, Longitude
            # and Ground Elevation data"); the derived decimal uses the same
            # plain formula as the historical published layer.
            lat_dec = _decimal_degrees(lat_deg, _integer(row[LO_LAT_MIN], "latitude_minutes") if row[LO_LAT_MIN].strip() else 0,
                                       lat_sec, lat_dir, "S")
            lon_dec = _decimal_degrees(lon_deg, _integer(row[LO_LON_MIN], "longitude_minutes") if row[LO_LON_MIN].strip() else 0,
                                       lon_sec, lon_dir, "W")
            if not -90 <= lat_dec <= 90 or not -180 <= lon_dec <= 180:
                raise ValueError("source_coordinate_out_of_range")
            lat_min = _integer(row[LO_LAT_MIN], "latitude_minutes") if row[LO_LAT_MIN].strip() else None
            lon_min = _integer(row[LO_LON_MIN], "longitude_minutes") if row[LO_LON_MIN].strip() else None
        if identifier not in licensees:
            raise ValueError("source_licensee_join_drift")
        published.append({
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
            "geometry": None if not lat_deg_text else _point_wkb(lon_dec, lat_dec),
        })
    duplicates = sorted([list(key) for key, count in keys.items() if count > 1])
    if lo_records != EXPECTED_LO_RECORDS:
        raise ValueError("source_record_population_drift")
    if len(duplicates) != EXPECTED_DUPLICATE_LOCATION_KEYS:
        raise ValueError("source_duplicate_location_key_count_drift")
    if _sha(_canonical(duplicates)) != EXPECTED_DUPLICATE_KEY_SHA256:
        raise ValueError("source_duplicate_key_roster_drift")
    if null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
    if invalid_directions != EXPECTED_INVALID_DIRECTION_COUNT:
        raise ValueError("source_invalid_direction_count_drift")
    if service_row_counts != Counter(EXPECTED_ACTIVE_SERVICE_ROW_COUNTS):
        raise ValueError("source_service_row_distribution_drift")
    if len(published) != EXPECTED_ACTIVE_LOCATIONS:
        raise ValueError("source_active_population_drift")
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
    bounds = None
    coordinates = [(item["londec"], item["latdec"]) for item in published if item["londec"] is not None]
    if coordinates:
        bounds = [min(x for x, _y in coordinates), min(y for _x, y in coordinates),
                  max(x for x, _y in coordinates), max(y for _x, y in coordinates)]
    schema_contract = {"zip_members": sorted(ZIP_MEMBER_ROSTER), "record_field_counts": RECORD_FIELD_COUNTS,
                       "published_columns": [name for name, _ in PUBLISHED_SCHEMA]}
    audit = {
        "source_feature_count": len(published), "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(schema_contract)),
        "source_active_id_sha256": _sha(_canonical(roster)),
        "source_state_roster_sha256": _sha(_canonical(states)),
        "source_hd_record_count": hd_records, "source_en_record_count": en_records,
        "source_lo_record_count": lo_records,
        "source_license_status_counts": status_counts,
        "source_radio_service_codes": sorted(EXPECTED_HD_RADIO_SERVICES),
        "source_service_row_counts": dict(sorted(service_row_counts.items())),
        "source_duplicate_location_key_count": len(duplicates),
        "source_duplicate_key_roster_sha256": _sha(_canonical(duplicates)),
        "source_invalid_direction_count": invalid_directions,
        "source_file_creation_date": creation_date,
        "source_null_geometry_count": null_geometry,
        "source_bounds": bounds, "source_state_count": len(states),
        "source_coverage_scope": COVERAGE_SCOPE,
        "drift_guard": ("Exact capture roster, HTTP status/completeness/length/hash receipt, zip member roster, "
                        "per-record-type field counts and record-type prefixes, HD/EN/LO record totals, radio "
                        "service roster, license status distribution, unique license and licensee identities, "
                        "LO/HD callsign join, active-location count, full active ID roster digest, service-row "
                        "distribution, duplicate (UniqSysID, LocNum) key count and roster digest, state roster "
                        "digest, DMS direction preservation count, derived-decimal coordinate range and "
                        "null-geometry count are checked. Duplicate source location rows are published verbatim "
                        "rather than dropped; direction letters outside the nominal set are preserved and "
                        "converted by the plain formula without sign flip; fully empty DMS rows stay null "
                        "geometry, and one-sided empty DMS rows fail closed."),
    }
    return published, audit


def _parquet_bytes(published: list[dict]) -> bytes:
    import io
    arrays = [pa.array([item[name] for item in published], type=kind) for name, kind in PUBLISHED_SCHEMA]
    table = pa.Table.from_arrays(arrays, names=[name for name, _ in PUBLISHED_SCHEMA])
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
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


def capture_microwave_service_towers(storage: StagingStorageResource, *, version: str,
                                     capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved FCC ULS microwave source candidate."""
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
                Path("/tmp/hifld-native-reconstruction/154/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(600, connect=30),
                              headers={"User-Agent": "hifld-next-datasets/fcc-uls-draft"}) as client:
                captures["l_micro"] = _capture(client, root)
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
    inputs = [{"input_id": "l_micro:" + receipts["l_micro"]["sha256"], "producer": PUBLISHER,
        "host": urlsplit(receipts["l_micro"]["url"]).hostname, "role": "producer_publication",
        "canonical_url": L_MICRO_URL, "retrieval_url": receipts["l_micro"]["url"],
        "observed_final_url": receipts["l_micro"]["final_url"], "retrieved_at": receipts["l_micro"]["observed_at"],
        "http_status": receipts["l_micro"]["status"], "bytes": receipts["l_micro"]["bytes"],
        "sha256": receipts["l_micro"]["sha256"], "wire_bytes": receipts["l_micro"]["wire_bytes"],
        "wire_sha256": receipts["l_micro"]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The weekly complete file is regenerated every Sunday; the payload's own "
                                        "counts file creation date is retained in the audit while no per-release "
                                        "edition date is declared by the FCC for this distribution."),
        "license": "Unresolved; rights review pending.", "attribution": "FCC Wireless Telecommunications Bureau ULS public access; attribution and rights review pending."}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Microwave Service Towers (FCC ULS)",
        "description": ("Location records of active licenses from the FCC ULS weekly public access complete "
                        "Microwave service-group licenses file, republished under the historical HIFLD Microwave "
                        "Service Towers column contract. Unapproved candidate; source edition, baseline, "
                        "schema/ID contract, rights and release decisions remain pending."),
        "publisher": PUBLISHER, "agency": "Federal Communications Commission", "source_url": L_MICRO_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "ULS pipe-delimited licenses file to GeoParquet candidate", "parameters": {
            "source_tables": "HD license header, EN licensee entity and LO location tables of l_micro.zip decoded as pipe-delimited UTF-8 records",
            "population": "one published row per LO location record whose HD license status is A (active); the weekly file's Microwave service-group radio service roster is retained",
            "source_ids": "UniqSysID (HD/EN/LO unique system identifier) and LocNum (LO location number) retained unchanged; duplicate (UniqSysID, LocNum) location rows present in the source are preserved verbatim, not dropped or re-keyed",
            "source_values": "all published attribute strings retained verbatim including empty fields; numeric columns typed from the FCC public access database definitions",
            "derived_columns": ("latdec/londec are DMS-to-decimal conversions of the raw components formatted "
                                "to 12 significant digits, matching the historical published layer's derived-"
                                "column convention (missing minutes or seconds contribute zero; direction "
                                "letters outside the nominal set are preserved verbatim and do not flip the "
                                "sign); url embeds UniqSysID as the ULS license-location key"),
            "geometry": "WKB Point (londec, latdec) in EPSG:4326, no reprojection, repair or geocoding; fully empty DMS rows keep null geometry",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("The historical HIFLD OPEN Microwave Service Towers layer is described by its publisher as an "
                    "extract of the Universal Licensing System (ULS) licensed by the Wireless Telecommunications "
                    "Bureau, consisting of Microwave Transmitters under the Fixed Microwave Service rule part "
                    "47 CFR Part 101, received by HIFLD from the FCC as data provider. The FCC moved ULS daily and "
                    "weekly public access downloads from wireless2.fcc.gov to data.fcc.gov/download/pub/uls/ in "
                    "July 2021; the weekly complete Microwave service-group licenses file l_micro.zip is the "
                    "current official bulk distribution of that same ULS data family from the producing agency. "
                    "The archived HIFLD OPEN Microwave Service Towers layer (DataLumos project 239596, DOI "
                    "10.3886/E239596V1) and the DHS crosswalk DOI 10.3886/E241367V1 both return HTTP 403 "
                    "anonymously and are banned as pipeline inputs by the source policy, so archive identity is "
                    "cited from the DataLumos landing metadata rather than fetched payloads. The weekly l_micro "
                    "file groups the full ULS Microwave radio-service roster (Part 101 fixed microwave codes plus "
                    "co-located broadcast auxiliary codes), and the published candidate retains every active "
                    "license's filed location records including the source's own duplicate location rows and "
                    "invalid direction letters; whether the historical layer used the same complete group or a "
                    "narrower subset remains an unaccepted coverage decision. No publication or acceptance is "
                    "implied; runtime approval flags remain false."),
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
        "feature_count": EXPECTED_ACTIVE_LOCATIONS, "source_population_complete": True,
        "source_id_sha256": audit["source_active_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash,
        "source_null_geometry_count": audit["source_null_geometry_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
