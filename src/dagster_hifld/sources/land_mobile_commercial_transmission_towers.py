"""Complete native capture of the FCC ULS weekly Land Mobile - Commercial licenses file.

The ULS weekly "LMcomm" licenses file is the official bulk distribution for the
FCC's Land Mobile - Commercial service group: Specialized Mobile Radio (SMR),
commercial Industrial/Business pool, 220 MHz commercial land mobile, 900 MHz
location services and private carrier paging (radio service codes GL, GM, GR,
GS, GX, IK, LN, LW, NC, QD, QO, QT, YK, YL, YM, YS, YX). The historical HIFLD
OPEN Land Mobile Commercial Transmission Towers layer was "extracted from the
FCC Licensing Database" per its own layer description, so this weekly licenses
file is the original agency distribution of the same data family.
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

DATASET_SLUG = "land-mobile-commercial-transmission-towers"
FILE_SLUG = DATASET_SLUG
# FCC moved the ULS daily and weekly public access files from wireless2.fcc.gov
# to data.fcc.gov in July 2021 (FCC Public Access Updates notice). The weekly
# complete Land Mobile - Commercial licenses file l_LMcomm.zip (l_ files are the
# weekly license tables generated under licweekzipdata; a_ files carry the
# weekly application tables) is the current official bulk distribution for the
# Land Mobile - Commercial service group listed on the FCC Daily & Weekly
# Transaction Files page.
L_LMCOMM_URL = "https://data.fcc.gov/download/pub/uls/complete/l_LMcomm.zip"
PUBLISHER = ("Federal Communications Commission Wireless Telecommunications Bureau; "
             "official ULS weekly public access complete database files")
CAPTURE_NAMES = {"l_LMcomm"}
MAX_CAPTURE_BYTES = 320_000_000
CAPTURE_BODY_LIMIT = 128 * 1024 * 1024
DISK_FLOOR = 800_000_000
ZIP_MEMBER_ROSTER = [
    "AN.dat", "AS.dat", "BF.dat", "CO.dat", "CP.dat", "EC.dat", "EM.dat", "EN.dat",
    "FF.dat", "FR.dat", "HD.dat", "HS.dat", "LA.dat", "LF.dat", "LM.dat", "LO.dat",
    "LS.dat", "O2.dat", "OP.dat", "SC.dat", "SF.dat", "a3.dat", "counts", "f3.dat",
    "f5.dat", "f6.dat", "ir.dat", "l3.dat", "l4.dat", "l5.dat", "l6.dat", "lc.dat",
    "ll.dat",
]
RECORD_FIELD_COUNTS = {"HD": 59, "EN": 30, "LO": 51}
EXPECTED_HD_RECORDS = 81081
EXPECTED_EN_RECORDS = 153247
EXPECTED_LO_RECORDS = 185230
# FCC ULS Code Definitions / public access files documentation list the
# Land Mobile - Commercial radio service roster as GL, GM, GR, GS, GX, IK, LN,
# LW, NC, QD, QO, QT, YK, YL, YM, YS, YX (SMR, commercial Industrial/Business,
# 220 MHz commercial, 902-928 MHz location and private carrier paging). The
# pinned file carries 15 of the 17 roster codes (GL and YL have no records).
EXPECTED_HD_RADIO_SERVICES = ["GM", "GR", "GS", "GX", "IK", "LN", "LW", "NC", "QD", "QO", "QT", "YK", "YM", "YS", "YX"]
EXPECTED_LICENSE_STATUS_COUNTS = {"A": 8096, "C": 59916, "E": 11450, "T": 1619}
EXPECTED_LICENSEE_ROWS = 81081
EXPECTED_ACTIVE_LOCATIONS = 25501
EXPECTED_ACTIVE_ID_SHA256 = "566a66503eee03c00f8cc31b3e4a8bbf5a2fd9f37cc73337e1011ab3c1d1073c"
EXPECTED_STATE_ROSTER_SHA256 = "db7a9f190bfc75658d2f989b38a5a5e95b3e90992234d039262d439f6802056a"
EXPECTED_NULL_GEOMETRY_COUNT = 7530
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
    "Every location record of every active license in the weekly ULS public access complete "
    "Land Mobile - Commercial licenses file (radio service codes GM, GR, GS, GX, IK, LN, LW, "
    "NC, QD, QO, QT, YK, YM, YS, YX in the pinned file; the FCC roster also lists GL and YL), "
    "covering SMR, commercial Industrial/Business, 220 MHz commercial, 902-928 MHz location "
    "and private carrier paging sites across the mainland United States, Alaska, Hawaii, Gulf "
    "of Mexico offshore areas and US territories. Land Mobile - Commercial is a site-based ULS "
    "service group (FCC public access files documentation): ULS carries the site locations "
    "licensees filed, so this is the filed-site population the FCC publishes, not a census of "
    "physical towers. The pinned population includes 7530 active location records filed without "
    "latitude/longitude components (published with null geometry, coordinates retained as null)."
)
# FCC policy requires ULS geographic coordinates to be referenced to the North
# American Datum of 1983 (https://www.fcc.gov/wireless/systems-utilities/
# north-american-datum-1983-coordinates). The published WKB points carry the
# DMS-derived decimal values unchanged, so the GeoParquet geometry is declared
# in the native EPSG:4269 datum; no datum transformation, reprojection, repair
# or geocoding is applied.
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
GEOMETRY_PROVENANCE = ("WKB Point (londec, latdec) in EPSG:4269 (NAD83), the datum the FCC requires for "
                       "ULS location coordinates; the DMS-derived decimal values are written unchanged, "
                       "with no datum transformation, reprojection, repair or geocoding")


def _capture_budget(root: Path) -> None:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * CAPTURE_BODY_LIMIT > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")


def _capture(client: httpx.Client, root: Path) -> tuple[bytes, dict]:
    _capture_budget(root)
    return capture_get(client, L_LMCOMM_URL, root, "l_LMcomm", max_bytes=CAPTURE_BODY_LIMIT)


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
        _validate_capture_request_identity(name, receipt, L_LMCOMM_URL)
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
        for member in ("HD.dat", "EN.dat", "LO.dat"):
            raw = archive.read(member)
            if raw.endswith(b"\r\n"):
                raw = raw[:-2]
            elif raw.endswith(b"\n") or raw.endswith(b"\r"):
                raw = raw.rstrip(b"\r\n")
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("source_record_encoding_drift:" + member) from exc
            rows = [line.split("|") for line in text.split("\n")]
            if any(not row for row in rows):
                raise ValueError("source_record_line_invalid:" + member)
            tables[member[:-4]] = rows
        counts = archive.read("counts")
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as exc:
        raise ValueError("source_zip_member_unreadable") from exc
    return tables, counts


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
    # The derived-column convention follows the ULS-derived FCC/HIFLD tower
    # family: DMS-to-decimal formatted to 12 significant digits. Verified on
    # the frozen HIFLD Next v1.0.0 Land Mobile Commercial Transmission Towers
    # copy: all 19148 of its published rows match this formula computed from
    # the frozen copy's own DMS components.
    return float(f"{value:.12g}")


def _point_wkb(longitude: float, latitude: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, longitude, latitude)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    body, receipt = captures["l_LMcomm"]
    _validate_capture_request_identity("l_LMcomm", receipt, L_LMCOMM_URL)
    if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
        raise ValueError("source_capture_status_length_hash_or_completeness_drift")
    tables, counts = _zip_tables(body)
    for name, rows in tables.items():
        expected = RECORD_FIELD_COUNTS[name]
        if any(len(row) != expected for row in rows):
            raise ValueError("source_record_field_count_drift:" + name)
        prefix = name
        if any(row[0] != prefix for row in rows):
            raise ValueError("source_record_type_prefix_drift:" + name)
    if (len(tables["HD"]) != EXPECTED_HD_RECORDS or len(tables["EN"]) != EXPECTED_EN_RECORDS
            or len(tables["LO"]) != EXPECTED_LO_RECORDS):
        raise ValueError("source_record_population_drift")
    match = CREATION_DATE_RE.search(counts)
    if match is None:
        raise ValueError("source_creation_date_missing")
    creation_date = match.group(1).decode("utf-8", "replace").strip()

    header = {}
    status_counts = {}
    for row in tables["HD"]:
        identifier = _integer(row[1], "unique_system_identifier")
        if identifier in header:
            raise ValueError("source_duplicate_license_identity")
        if row[HD_SERVICE] not in EXPECTED_HD_RADIO_SERVICES:
            raise ValueError("source_radio_service_roster_drift")
        status = row[HD_STATUS]
        if not status:
            raise ValueError("source_license_status_missing")
        status_counts[status] = status_counts.get(status, 0) + 1
        header[identifier] = (row[HD_CALLSIGN], status)
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
    published = []
    null_geometry = 0
    for row in tables["LO"]:
        identifier = _integer(row[LO_UNIQUE_SYSTEM_IDENTIFIER], "unique_system_identifier")
        if identifier not in header or row[LO_CALLSIGN] != header[identifier][0]:
            raise ValueError("source_callsign_join_drift")
        location_number = _integer(row[LO_LOCATION_NUMBER], "location_number")
        key = (identifier, location_number)
        if key in keys:
            raise ValueError("source_location_key_duplication_drift")
        keys.add(key)
        if header[identifier][1] != "A":
            continue
        # Every published (active) row must carry a callsign; the pinned file
        # has no empty callsigns anywhere, so an empty one fails closed.
        if not row[LO_CALLSIGN].strip():
            raise ValueError("source_active_callsign_missing")
        lat_deg_text, lat_min_text = row[LO_LAT_DEG].strip(), row[LO_LAT_MIN].strip()
        lon_deg_text, lon_min_text = row[LO_LON_DEG].strip(), row[LO_LON_MIN].strip()
        lat_sec = _decimal(row[LO_LAT_SEC], "latitude_seconds")
        lon_sec = _decimal(row[LO_LON_SEC], "longitude_seconds")
        lat_dir, lon_dir = row[LO_LAT_DIR].strip(), row[LO_LON_DIR].strip()
        lat_deg = _integer(row[LO_LAT_DEG], "latitude_degrees") if lat_deg_text else None
        lat_min = _integer(row[LO_LAT_MIN], "latitude_minutes") if lat_min_text else None
        lon_deg = _integer(row[LO_LON_DEG], "longitude_degrees") if lon_deg_text else None
        lon_min = _integer(row[LO_LON_MIN], "longitude_minutes") if lon_min_text else None
        if not lat_deg_text or not lon_deg_text:
            null_geometry += 1
            lat_dec = lon_dec = None
            geometry = None
        else:
            if lat_dir not in {"N", "S"}:
                raise ValueError("source_latitude_direction_drift")
            if lon_dir not in {"E", "W"}:
                raise ValueError("source_longitude_direction_drift")
            # Raw DMS components are preserved verbatim even when out of nominal
            # range; the FCC's own layer description warns of coordinate errors
            # in licensing information.
            lat_dec = _decimal_degrees(lat_deg, lat_min or 0, lat_sec, lat_dir, "S")
            lon_dec = _decimal_degrees(lon_deg, lon_min or 0, lon_sec, lon_dir, "W")
            if not -90 <= lat_dec <= 90 or not -180 <= lon_dec <= 180:
                raise ValueError("source_coordinate_out_of_range")
            geometry = _point_wkb(lon_dec, lat_dec)
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
            "geometry": geometry,
        })
    if null_geometry != EXPECTED_NULL_GEOMETRY_COUNT:
        raise ValueError("source_null_geometry_count_drift")
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
        "source_hd_record_count": len(tables["HD"]), "source_en_record_count": len(tables["EN"]),
        "source_lo_record_count": len(tables["LO"]),
        "source_license_status_counts": status_counts,
        "source_radio_service_codes": sorted(EXPECTED_HD_RADIO_SERVICES),
        "source_file_creation_date": creation_date,
        "source_null_geometry_count": null_geometry,
        "source_geometry_datum": "NAD83 (EPSG:4269), the FCC-required ULS location-coordinate datum; "
                                 "coordinates are written unchanged with no datum transformation",
        "source_incomplete_seconds_count": sum(1 for item in published if item["LatSec"] is None or item["LonSec"] is None),
        "source_bounds": bounds, "source_state_count": len(states),
        "source_coverage_scope": COVERAGE_SCOPE,
        "drift_guard": ("Exact capture roster, HTTP status/completeness/length/hash receipt, zip member roster, "
                        "per-record-type field counts and record-type prefixes, HD/EN/LO record totals, radio "
                        "service roster (the 15 pinned Land Mobile - Commercial codes), license status "
                        "distribution, unique license and licensee identities, verbatim LO/HD callsign join, "
                        "non-empty callsign on every published row, unique (UniqSysID, LocNum) keys, "
                        "active-location count, full active ID roster digest, state roster digest, DMS direction "
                        "codes, derived-decimal coordinate range and null-geometry count are checked; duplicate "
                        "location keys fail closed. Raw DMS components are preserved verbatim even when out of "
                        "nominal range, and the 7530 pinned coordinate-less active location records are published "
                        "with null geometry. Published geometry is declared in the FCC-required native NAD83 "
                        "datum (EPSG:4269) with no datum transformation."),
    }
    return published, audit


def _parquet_bytes(published: list[dict]) -> bytes:
    import io
    arrays = [pa.array([item[name] for item in published], type=kind) for name, kind in PUBLISHED_SCHEMA]
    table = pa.Table.from_arrays(arrays, names=[name for name, _ in PUBLISHED_SCHEMA])
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": GEOMETRY_CRS}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    digests = {}
    for name in sorted(captures):
        _body, wire, _receipt = _load_capture(root, name)
        digests[name] = {"decoded_sha256": _sha(captures[name][0]), "wire_sha256": _sha(wire)}
    return _sha(_canonical(digests))


def capture_land_mobile_commercial_transmission_towers(storage: StagingStorageResource, *, version: str,
                                                       capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved FCC ULS Land Mobile - Commercial source candidate."""
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
                Path("/tmp/hifld-native-reconstruction/133/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=20),
                              headers={"User-Agent": "hifld-next-datasets/fcc-uls-draft"}) as client:
                captures["l_LMcomm"] = _capture(client, root)
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
    inputs = [{"input_id": "l_LMcomm:" + receipts["l_LMcomm"]["sha256"], "producer": PUBLISHER,
        "host": urlsplit(receipts["l_LMcomm"]["url"]).hostname, "role": "producer_publication",
        "canonical_url": L_LMCOMM_URL, "retrieval_url": receipts["l_LMcomm"]["url"],
        "observed_final_url": receipts["l_LMcomm"]["final_url"], "retrieved_at": receipts["l_LMcomm"]["observed_at"],
        "http_status": receipts["l_LMcomm"]["status"], "bytes": receipts["l_LMcomm"]["bytes"],
        "sha256": receipts["l_LMcomm"]["sha256"], "wire_bytes": receipts["l_LMcomm"]["wire_bytes"],
        "wire_sha256": receipts["l_LMcomm"]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The weekly complete licenses file is regenerated every Sunday; the "
                                        "payload's own counts file creation date is retained in the audit while "
                                        "no per-release edition date is declared by the FCC for this distribution."),
        "license": "Unresolved; rights review pending.", "attribution": "FCC Wireless Telecommunications Bureau ULS public access; attribution and rights review pending."}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False,
        "title": "Land Mobile Commercial Transmission Towers (FCC ULS)",
        "description": ("Location records of active licenses in the FCC ULS weekly public access complete "
                        "Land Mobile - Commercial licenses file (SMR, commercial Industrial/Business, 220 MHz "
                        "commercial, 902-928 MHz location and private carrier paging services), republished "
                        "under the historical HIFLD Land Mobile Commercial Transmission Towers column "
                        "contract. Unapproved candidate; source edition, baseline, schema/ID contract, "
                        "coverage, rights and release decisions remain pending."),
        "publisher": PUBLISHER, "agency": "Federal Communications Commission", "source_url": L_LMCOMM_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "ULS pipe-delimited licenses file to GeoParquet candidate", "parameters": {
            "source_tables": "HD license header, EN licensee entity and LO location tables of l_LMcomm.zip decoded as pipe-delimited UTF-8 records",
            "population": ("one published row per LO location record whose HD license status is A (active); "
                           "radio service codes are the Land Mobile - Commercial roster (GM, GR, GS, GX, IK, "
                           "LN, LW, NC, QD, QO, QT, YK, YM, YS, YX in the pinned file; the FCC roster also "
                           "lists GL and YL, which carry no records)"),
            "source_ids": "UniqSysID (HD/EN/LO unique system identifier) and LocNum (LO location number) retained unchanged and jointly unique",
            "source_values": ("all published attribute strings retained verbatim including empty fields; "
                              "numeric columns typed from the FCC public access database definitions"),
            "derived_columns": ("latdec/londec are DMS-to-decimal conversions of the raw components formatted "
                                "to 12 significant digits, matching the ULS-derived FCC/HIFLD tower column "
                                "family convention (missing minutes or seconds contribute zero; out-of-nominal "
                                "raw components are preserved verbatim and converted by the same formula; all "
                                "19148 published rows of the frozen HIFLD Next v1.0.0 copy match this formula "
                                "computed from that copy's own DMS components); url embeds UniqSysID as the "
                                "ULS license-location key"),
            "null_geometry": ("active location records filed without latitude/longitude components (7530 in "
                              "the pinned file) are published with null geometry and null latdec/londec; the "
                              "frozen historical copy holds no null-geometry rows, so the coordinate-less "
                              "population difference is flagged for the maintainer compatibility decision"),
            "geometry": GEOMETRY_PROVENANCE + " (FCC ULS NAD83 datum policy: "
                        "https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates)",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("The FCC moved ULS daily and weekly public access downloads from wireless2.fcc.gov to "
                    "data.fcc.gov/download/pub/uls/ in July 2021; the weekly complete Land Mobile - Commercial "
                    "licenses file l_LMcomm.zip (Licenses, listed on the FCC Daily & Weekly Transaction Files "
                    "page as 'Land Mobile - Commercial') is the current official bulk distribution of the same "
                    "ULS licensing data family from which the historical HIFLD OPEN Land Mobile Commercial "
                    "Transmission Towers layer says it was 'extracted from the FCC Licensing Database'. The "
                    "archived HIFLD OPEN layer (DataLumos project 239395, the inventory's download location) "
                    "and the DHS crosswalk DOI 10.3886/E241367V1 return HTTP 403 "
                    "anonymously, so archive identity is cited from the inventory and DataLumos landing page "
                    "rather than fetched payloads. The existing published HIFLD Next v1.0.0 frozen copy "
                    "(19148 point features, issued 2024-01-24, the same column contract as the Cellular "
                    "Towers layer) is retained as comparison-only context: 17566 of its 19148 keys are still "
                    "active in the pinned file (1582 dropped: 954 licenses now expired, 549 cancelled, 7 "
                    "removed from ULS, 13 in other current statuses, 59 active licenses whose location row "
                    "changed), and 7935 current active "
                    "keys are new (405 with coordinates, 7530 coordinate-less location records on 4406 "
                    "licenses with DMS components entirely or partly empty - the frozen copy has no "
                    "null-geometry rows, a coverage difference flagged for the maintainer compatibility "
                    "decision rather than silently dropped). Land Mobile - Commercial is a site-based ULS "
                    "service group: ULS location records are the sites licensees filed, so this candidate "
                    "publishes the FCC's filed-site population, not a census of physical towers. No "
                    "publication or acceptance is implied; runtime approval flags remain false."),
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
