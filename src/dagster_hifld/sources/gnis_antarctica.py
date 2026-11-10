"""Lossless, approval-gated capture of the USGS GNIS Antarctica named-features layer.

The HIFLD Open inventory row ``nmg/antarctica-1`` ("Antarctica", agency US Geological
Survey, generic National Map Gazetteer description, historical archive DataLumos
240193) is the GNIS "Antarctica Features" point layer. The original producer publishes
it directly: the official USGS "Download Names Data" page
(https://www.usgs.gov/us-board-on-geographic-names/download-gnis-data) lists The
National Map staged-products directory
``https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Antarctica/`` as
"Names data for the continent of Antarctica approved by the Advisory Committee on
Antarctic Names of the U.S. Board on Geographic Names", and the USGS "Antarctic
Names" page links the same directory. Dated 2026-11-09 probes captured the producer's
own ``Gazetteer_Antarctica_GPKG.zip`` (primary input: native vector payload) and
``Gazetteer_Antarctica_Text.zip`` (the documented pipe-delimited product family
member, captured for independent cross-format verification), both published
2026-11-02 (FGDC ``pubdate 20261102``, S3 LastModified 2026-11-02T20:37:44Z; the
inventory's 2025-03-20 archive date is not a producer release date). The LIMA
``USGS_EROS_Antarctica_Reference`` coastline service is a different product and is
not this dataset. The DataLumos archive copy and every HIFLD rescue mirror are
identification/comparison context only, never pipeline inputs.

The producer's GeoPackage stores the complete Full Model: ``AntarcticaNames`` (the
named-features point layer this candidate publishes) and ``AntarcticaAllNames``
(variant names, retained as raw evidence with audit digests). The declared geometry
column is MULTIPOINT in EPSG:4326, while every stored geometry is a single ISO WKB
Point Z (type 1001) with Z == 0.0; the producer's 7-decimal ``prim_*_dec``
attributes agree with the native geometry within 5.000005387501005e-08 degrees and
both representations are preserved without coercion. A dangling ``CLIPPOLY``
``gpkg_contents`` row (table absent) is a producer artifact covered by the schema
pin. The pipe-delimited product carries the same population (including one row
whose history field contains an embedded quoted newline), and this adapter
cross-verifies both captured formats before sealing anything.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import shutil
import sqlite3
import struct
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "antarctica"
FILE_SLUG = DATASET_SLUG
DOWNLOAD_PAGE_URL = "https://www.usgs.gov/us-board-on-geographic-names/download-gnis-data"
ANTARCTIC_NAMES_PAGE_URL = "https://www.usgs.gov/us-board-on-geographic-names/antarctic-names"
S3_LISTING_URL = ("https://prd-tnm.s3.amazonaws.com/?list-type=2"
                  "&prefix=StagedProducts/GeographicNames/Antarctica/&max-keys=100")
GPKG_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GPKG.zip"
TEXT_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_Text.zip"
FILE_FORMAT_DOC_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/GNIS_file_format.pdf"
PUBLISHER = "U.S. Geological Survey"
SOURCE_CRS_WKID = 4326
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MAX_ZIP_MEMBER_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 12_000_000_000
CAPTURE_NAMES = ("s3_before", "download_page", "antarctic_names_page", "file_format_pdf",
                 "gpkg_zip", "text_zip", "s3_after")
CAPTURE_URLS = {
    "s3_before": S3_LISTING_URL,
    "download_page": DOWNLOAD_PAGE_URL,
    "antarctic_names_page": ANTARCTIC_NAMES_PAGE_URL,
    "file_format_pdf": FILE_FORMAT_DOC_URL,
    "gpkg_zip": GPKG_URL,
    "text_zip": TEXT_URL,
    "s3_after": S3_LISTING_URL,
}
NAMES_TABLE = "AntarcticaNames"
ALLNAMES_TABLE = "AntarcticaAllNames"
DATA_COLUMNS = ("feature_id", "antarctica_id", "scar_placeid", "feature_name", "feature_class",
                "date_created", "date_edited", "bgn_type", "bgn_authority", "bgn_date",
                "prim_lat_dms", "prim_long_dms", "prim_lat_dec", "prim_long_dec",
                "description", "history", "bgn_type_desc", "bgn_authority_desc")
ALLNAMES_COLUMNS = ("feature_id", "feature_name", "feature_name_official",
                    "sourcecitationabbreviation", "sourceoriginator", "sourcereftype", "title",
                    "edition", "sourceurl", "publicationdate", "seriesname", "seriesissue",
                    "ending_date", "date_created", "citation", "feature_name_official_desc",
                    "sourcereftype_desc")
# Cross-format domain maps: text desc -> GPKG coded value (gpkg_data_column_constraints).
TEXT_TO_BGN_TYPE = {"Official": 2}
TEXT_TO_BGN_AUTHORITY = {"Board Decision": 1}
TEXT_TO_NAME_STATUS = {"Official": 1, "Not Official": 2}
ISO_DATETIME_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\Z")
TEXT_DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}\Z")
GPKG_ZIP_MEMBERS = ("Gazetteer_Antarctica_GPKG.gpkg", "Gazetteer_Antarctica_GPKG.jpg",
                    "Gazetteer_Antarctica_GPKG.xml")
TEXT_ZIP_MEMBERS = ("Gazetteer_Antarctica_Text.jpg", "Gazetteer_Antarctica_Text.xml",
                    "Text/AntarcticaAllNames_Antarctica.txt", "Text/AntarcticaNames_Antarctica.txt")
# Pins below are the observed 2026-11-09 complete-population contract of the producer's
# 2026-11-02 edition; any producer change fails closed for explicit re-review.
# EXPECTED_SCHEMA_SHA256 is the module's own canonical schema representation (see
# _extract_gpkg_population); EXPECTED_ALLNAMES_ID_SHA256 is the sorted feature_id
# multiset of the variant-name table (one entry per variant row), pinning the exact
# feature->variant multiplicity alongside the distinct-roster identity check.
EXPECTED_PUBDATE = "20261102"
EXPECTED_S3_KEYS = (
    "StagedProducts/GeographicNames/Antarctica/",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GDB.jpg",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GDB.xml",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GDB.zip",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GPKG.jpg",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GPKG.xml",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GPKG.zip",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_Text.jpg",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_Text.xml",
    "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_Text.zip",
)
EXPECTED_COUNT = 14380
EXPECTED_ALLNAMES_COUNT = 18250
EXPECTED_ID_SHA256 = "999bde273e09be9b400279fd2bfeadf1cb5158c1b7457f87cdd91c128824a389"
EXPECTED_ALLNAMES_ID_SHA256 = "1b645d4033297d6530f36fb7f7950e04e5af65aabe5ab864fd49726b1d4a2803"
EXPECTED_RECORD_SHA256 = "42f4468e64f361eee8fb0a0e03f90ee86daf70de706c460b9c9592b539a92945"
EXPECTED_ALLNAMES_RECORD_SHA256 = "21caf453ff22ef96d885d386518ccda5d4c60ee6e2c21022903b2ec7a0945a42"
EXPECTED_GEOMETRY_SHA256 = "b5eff3013161eb562a2609cabea85a24f507de4e71cdf66f7279d2ec8398aa8f"
EXPECTED_SCHEMA_SHA256 = "1db6e1b56156ece02f139fad08762d9eae8a4efa1dbe1dc672f9321abac646b7"
EXPECTED_OBSERVED_BOUNDS = [-179.99999999999997, -89.99999999999994, 179.98333330000003,
                            -52.89999999999998]
EXPECTED_GEOMETRY_ATTRIBUTE_MAX_DELTA = 5.000005387501005e-08
EXPECTED_NULL_COUNTS = {
    "feature_id": 0, "antarctica_id": 150, "scar_placeid": 1354, "feature_name": 0,
    "feature_class": 0, "date_created": 12636, "date_edited": 2620, "bgn_type": 1323,
    "bgn_authority": 1323, "bgn_date": 1323, "prim_lat_dms": 0, "prim_long_dms": 0,
    "prim_lat_dec": 0, "prim_long_dec": 0, "description": 6, "history": 14185,
    "bgn_type_desc": 0, "bgn_authority_desc": 0,
}
EXPECTED_ALLNAMES_NULL_COUNTS = {
    "feature_id": 0, "feature_name": 0, "feature_name_official": 0,
    "sourcecitationabbreviation": 18250, "sourceoriginator": 0, "sourcereftype": 0, "title": 0,
    "edition": 18250, "sourceurl": 18250, "publicationdate": 18226, "seriesname": 18250,
    "seriesissue": 18250, "ending_date": 18250, "date_created": 18249, "citation": 0,
    "feature_name_official_desc": 0, "sourcereftype_desc": 0,
}
GPKG_WKB_POINT_Z_TYPE = 1001  # ISO SQL/MM WKB Point Z
GPKG_HEADER_FLAGS_EXPECTED = 1  # little-endian WKB, no envelope, non-empty, standard binary


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``."""
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _zip_member_names(zip_bytes: bytes) -> tuple[str, ...]:
    """Return the sorted member roster of a zip, refusing directories and oversized members."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except (zipfile.BadZipFile, OSError) as exc:
        raise ValueError("source_zip_invalid") from exc
    with archive:
        names = []
        for info in archive.infolist():
            if info.is_dir():
                raise ValueError("source_zip_member_roster_drift")
            if info.file_size > MAX_ZIP_MEMBER_BYTES:
                raise ValueError("source_zip_member_oversized")
            names.append(info.filename)
        return tuple(sorted(names))


def _zip_member(zip_bytes: bytes, member: str) -> bytes:
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except (zipfile.BadZipFile, OSError) as exc:
        raise ValueError("source_zip_invalid") from exc
    with archive:
        try:
            info = archive.getinfo(member)
        except KeyError as exc:
            raise ValueError("source_zip_member_roster_drift") from exc
        if info.file_size > MAX_ZIP_MEMBER_BYTES:
            raise ValueError("source_zip_member_oversized")
        return archive.read(member)


def _parse_s3_listing(body: bytes) -> dict[str, dict[str, object]]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise ValueError("source_listing_invalid") from exc
    if root.tag.endswith("ListBucketResult"):
        entries = [child for child in root if child.tag.endswith("Contents")]
    else:
        raise ValueError("source_listing_invalid")
    listing: dict[str, dict[str, object]] = {}
    for entry in entries:
        key = size = last_modified = etag = None
        for child in entry:
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "Key":
                key = child.text
            elif tag == "Size":
                size = int(child.text)
            elif tag == "LastModified":
                last_modified = child.text
            elif tag == "ETag":
                etag = child.text
        if key is None or key in listing:
            raise ValueError("source_listing_invalid")
        listing[key] = {"size": size, "last_modified": last_modified, "etag": etag}
    return listing


def _fgdc_field(xml_body: bytes, tag: str) -> str | None:
    match = re.search(rf"<{tag}>(.*?)</{tag}>", xml_body.decode("utf-8", "replace"), re.S)
    if match is None:
        return None
    return " ".join(match.group(1).split())


def _parse_pipe_table(body: bytes) -> tuple[list[str], list[list[str]]]:
    text = body.decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text, newline=""), delimiter="|", quotechar='"'))
    if not rows:
        raise ValueError("source_text_table_empty")
    return rows[0], [row for row in rows[1:] if any(cell.strip() for cell in row)]


def _parse_iso_datetime(value: str) -> datetime:
    if not ISO_DATETIME_PATTERN.fullmatch(value):
        raise ValueError("source_datetime_format_drift")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def _text_date_matches_iso(text_date: str, iso_value: str | None) -> bool:
    """True when the pipe-format MM/DD/YYYY value is the date of the GPKG ISO value."""
    if text_date == "":
        return iso_value is None
    if iso_value is None or not TEXT_DATE_PATTERN.fullmatch(text_date):
        return False
    month, day, year = text_date.split("/")
    return iso_value[:10] == f"{year}-{month}-{day}"


def _validate_names_row(values: list[object]) -> None:
    if type(values[0]) is not int:
        raise ValueError("source_feature_id_type_drift")
    if not isinstance(values[3], str) or not values[3]:
        raise ValueError("source_feature_name_drift")
    if not isinstance(values[4], str) or not values[4]:
        raise ValueError("source_feature_class_drift")
    for index in (10, 11):
        if not isinstance(values[index], str) or not values[index]:
            raise ValueError("source_dms_coordinate_drift")
    for index in (5, 6, 9):
        if values[index] is not None and (not isinstance(values[index], str)):
            raise ValueError("source_datetime_type_drift")
    for index in (12, 13):
        value = values[index]
        if type(value) is not float and type(value) is not int:
            raise ValueError("source_decimal_type_drift")
        lower, upper = (-90.0, 90.0) if index == 12 else (-180.0, 180.0)
        if not lower <= float(value) <= upper:
            raise ValueError("source_coordinate_range_drift")
    for index in (16, 17):
        if not isinstance(values[index], str) or not values[index]:
            raise ValueError("source_domain_descriptor_drift")


def _validate_geometry(blob: object, feature_id: int, lat: float, lon: float,
                       z_values: set[float], bounds: list[float] | None) -> list[float] | None:
    if not isinstance(blob, (bytes, bytearray)) or len(blob) <= 8:
        raise ValueError("source_geometry_invalid")
    if blob[0:2] != b"GP":
        raise ValueError("source_geometry_invalid")
    flags = blob[3]
    if flags != GPKG_HEADER_FLAGS_EXPECTED:
        raise ValueError("source_geometry_header_drift")
    body = bytes(blob[8:])
    if len(body) != 29 or body[0] != 1:
        raise ValueError("source_geometry_invalid")
    if struct.unpack("<I", body[1:5])[0] != GPKG_WKB_POINT_Z_TYPE:
        raise ValueError("source_geometry_type_drift")
    x, y, z = struct.unpack("<ddd", body[5:29])
    if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
        raise ValueError("source_coordinate_range_drift")
    if z != 0.0:
        raise ValueError("source_geometry_z_drift")
    z_values.add(z)
    if abs(x - lon) > EXPECTED_GEOMETRY_ATTRIBUTE_MAX_DELTA or abs(y - lat) > EXPECTED_GEOMETRY_ATTRIBUTE_MAX_DELTA:
        raise ValueError("source_geometry_attribute_delta_drift")
    if bounds is None:
        return [x, y, x, y]
    bounds[0] = min(bounds[0], x)
    bounds[1] = min(bounds[1], y)
    bounds[2] = max(bounds[2], x)
    bounds[3] = max(bounds[3], y)
    return None


def _gpkg_schema(cursor: sqlite3.Cursor) -> dict[str, object]:
    """Canonical schema contract of the producer's Full Model GeoPackage."""
    return {
        "columns": sorted([row[1], row[2], row[3], row[5]] for row in
                          cursor.execute(f"PRAGMA table_info({NAMES_TABLE})")),
        "geometry_column": sorted(list(row) for row in cursor.execute(
            "SELECT table_name, column_name, geometry_type_name, srs_id FROM gpkg_geometry_columns")),
        "contents": sorted(list(row) for row in cursor.execute(
            "SELECT table_name, data_type, srs_id FROM gpkg_contents")),
        "data_columns": sorted(list(row) for row in cursor.execute(
            "SELECT table_name, column_name, constraint_name FROM gpkg_data_columns")),
        "constraints": sorted(list(row) for row in cursor.execute(
            "SELECT constraint_name, constraint_type, value, description "
            "FROM gpkg_data_column_constraints ORDER BY constraint_name, value")),
        "allnames_columns": sorted([row[1], row[2], row[3], row[5]] for row in
                                   cursor.execute(f"PRAGMA table_info({ALLNAMES_TABLE})")),
    }


def _extract_gpkg_population(gpkg_body: bytes) -> dict[str, object]:
    """Load the producer GeoPackage in memory and validate its complete population."""
    try:
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
    except sqlite3.Error as exc:
        raise ValueError("source_geopackage_invalid") from exc
    try:
        cursor = connection.cursor()
        schema = _gpkg_schema(cursor)
        if _sha(_canonical(schema)) != EXPECTED_SCHEMA_SHA256:
            raise ValueError("source_schema_contract_drift")
        names_geometry = cursor.execute(
            f"SELECT {','.join(DATA_COLUMNS)}, Shape FROM {NAMES_TABLE} ORDER BY feature_id"
        ).fetchall()
        if len(names_geometry) != EXPECTED_COUNT:
            raise ValueError("source_count_drift")
        records: list[list] = []
        geometry_records: list[list] = []
        null_counts = dict.fromkeys(DATA_COLUMNS, 0)
        bounds: list[float] | None = None
        z_values: set[float] = set()
        seen: set[int] = set()
        for row in names_geometry:
            values = list(row[:-1])
            blob = row[-1]
            feature_id = values[0]
            if feature_id in seen:
                raise ValueError("source_duplicate_feature_id_drift")
            seen.add(feature_id)
            _validate_names_row(values)
            for index, column in enumerate(DATA_COLUMNS):
                if values[index] is None:
                    null_counts[column] += 1
            for index in (5, 6, 9):
                if values[index] is not None:
                    _parse_iso_datetime(values[index])
            bounds = _validate_geometry(blob, feature_id, values[12], values[13],
                                        z_values, bounds) or bounds
            wkb = bytes(blob[8:]).hex()
            records.append([values, wkb])
            geometry_records.append([feature_id, wkb])
        if bounds is None or list(bounds) != EXPECTED_OBSERVED_BOUNDS:
            raise ValueError("source_observed_coverage_drift")
        if null_counts != EXPECTED_NULL_COUNTS:
            raise ValueError("source_null_contract_drift")
        if len(seen) != EXPECTED_COUNT:
            raise ValueError("source_id_roster_drift")
        if _sha(_canonical(sorted(seen))) != EXPECTED_ID_SHA256:
            raise ValueError("source_id_roster_drift")
        if _list_sha(records) != EXPECTED_RECORD_SHA256:
            raise ValueError("source_record_value_drift")
        if _list_sha(geometry_records) != EXPECTED_GEOMETRY_SHA256:
            raise ValueError("source_geometry_or_coverage_drift")

        allnames_rows = cursor.execute(
            f"SELECT {','.join(ALLNAMES_COLUMNS)} FROM {ALLNAMES_TABLE} ORDER BY feature_id"
        ).fetchall()
        if len(allnames_rows) != EXPECTED_ALLNAMES_COUNT:
            raise ValueError("source_allnames_count_drift")
        allnames_records = sorted([list(row) for row in allnames_rows],
                                  key=lambda row: (row[0], _canonical(row[1:]).decode()))
        allnames_nulls = dict.fromkeys(ALLNAMES_COLUMNS, 0)
        # The variant-name table repeats feature_id by design (one row per variant
        # name); identity is the distinct full row and the distinct-roster equality
        # with the point layer, while the pinned multiset sha fixes the exact
        # feature -> variant multiplicity of the captured edition.
        allnames_ids: list[int] = []
        distinct_rows: set[tuple] = set()
        for row in allnames_rows:
            if not isinstance(row[0], int):
                raise ValueError("source_allnames_id_type_drift")
            if tuple(row) in distinct_rows:
                raise ValueError("source_allnames_duplicate_row_drift")
            distinct_rows.add(tuple(row))
            allnames_ids.append(row[0])
            for index, column in enumerate(ALLNAMES_COLUMNS):
                if row[index] is None:
                    allnames_nulls[column] += 1
        if allnames_nulls != EXPECTED_ALLNAMES_NULL_COUNTS:
            raise ValueError("source_allnames_null_contract_drift")
        if set(allnames_ids) != seen:
            raise ValueError("source_allnames_roster_drift")
        if _sha(_canonical(sorted(allnames_ids))) != EXPECTED_ALLNAMES_ID_SHA256:
            raise ValueError("source_allnames_id_roster_drift")
        if _list_sha(allnames_records) != EXPECTED_ALLNAMES_RECORD_SHA256:
            raise ValueError("source_allnames_record_value_drift")
    finally:
        connection.close()
    return {"records": records, "allnames_records": allnames_records, "bounds": list(bounds),
            "z_values": sorted(z_values), "null_counts": null_counts,
            "allnames_nulls": allnames_nulls, "schema_sha256": _sha(_canonical(schema))}


def _cross_verify_text(text_zip_body: bytes, gpkg_population: dict[str, object]) -> None:
    """Verify the pipe-delimited product carries the same population and values."""
    names_header, names_rows = _parse_pipe_table(_zip_member(text_zip_body,
                                                            "Text/AntarcticaNames_Antarctica.txt"))
    if names_header != list(DATA_COLUMNS[:16]):
        raise ValueError("text_schema_contract_drift")
    records: list[list[object]] = gpkg_population["records"]
    if len(names_rows) != EXPECTED_COUNT:
        raise ValueError("text_count_drift")
    text_by_id: dict[int, list[str]] = {}
    for row in names_rows:
        try:
            feature_id = int(row[0])
        except ValueError as exc:
            raise ValueError("text_feature_id_drift") from exc
        if feature_id in text_by_id:
            raise ValueError("text_duplicate_feature_id_drift")
        text_by_id[feature_id] = row
    if sorted(text_by_id) != [record[0][0] for record in records]:
        raise ValueError("text_id_roster_drift")
    for values, _wkb in records:
        row = text_by_id[values[0]]
        if row[3] != values[3] or row[4] != values[4]:
            raise ValueError("text_field_drift")
        if row[10] != values[10] or row[11] != values[11]:
            raise ValueError("text_field_drift")
        if float(row[12]) != values[12] or float(row[13]) != values[13]:
            raise ValueError("text_coordinate_drift")
        if row[14] != (values[14] or "") or row[15] != (values[15] or ""):
            raise ValueError("text_field_drift")
        if (row[1] == "") != (values[1] is None) or (row[1] != "" and float(row[1]) != values[1]):
            raise ValueError("text_identifier_drift")
        if (row[2] == "") != (values[2] is None) or (row[2] != "" and float(row[2]) != values[2]):
            raise ValueError("text_identifier_drift")
        for iso_index in (5, 6, 9):
            if not _text_date_matches_iso(row[iso_index], values[iso_index]):
                raise ValueError("text_datetime_drift")
        for text_index, iso_index, domain in ((7, 7, TEXT_TO_BGN_TYPE),
                                              (8, 8, TEXT_TO_BGN_AUTHORITY)):
            text_value = row[text_index]
            if (text_value == "") != (values[iso_index] is None):
                raise ValueError("text_domain_drift")
            if text_value != "" and values[iso_index] != domain.get(text_value):
                raise ValueError("text_domain_drift")

    allnames_header, allnames_rows = _parse_pipe_table(
        _zip_member(text_zip_body, "Text/AntarcticaAllNames_Antarctica.txt"))
    if allnames_header != list(ALLNAMES_COLUMNS[:15]):
        raise ValueError("text_schema_contract_drift")
    if len(allnames_rows) != EXPECTED_ALLNAMES_COUNT:
        raise ValueError("text_allnames_count_drift")
    gpkg_allnames: dict[int, list] = {}
    for row in gpkg_population["allnames_records"]:
        gpkg_allnames.setdefault(row[0], []).append(row)
    text_ids: set[int] = set()
    for row in allnames_rows:
        try:
            feature_id = int(row[0])
        except ValueError as exc:
            raise ValueError("text_allnames_id_drift") from exc
        text_ids.add(feature_id)
        if not any(candidate[1] == row[1]
                   and candidate[2] == TEXT_TO_NAME_STATUS.get(row[2])
                   and candidate[14] == (row[14] or "")
                   for candidate in gpkg_allnames.get(feature_id, [])):
            raise ValueError("text_allnames_field_drift")
    if sorted(text_ids) != sorted(gpkg_allnames):
        raise ValueError("text_allnames_id_roster_drift")


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]) -> dict[str, object]:
    if set(captures) != set(CAPTURE_NAMES):
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    for name, (body, receipt) in captures.items():
        if receipt.get("status") != 200 or not receipt.get("complete_body"):
            raise ValueError("incomplete_source_capture:" + name)
    if captures["s3_before"][0] != captures["s3_after"][0]:
        raise ValueError("source_publication_changed_during_capture")
    listing = _parse_s3_listing(captures["s3_before"][0])
    if tuple(sorted(listing)) != EXPECTED_S3_KEYS:
        raise ValueError("source_listing_roster_drift")
    for name, url_key in (("gpkg_zip", GPKG_URL.rsplit("/", 1)[-1]),
                          ("text_zip", TEXT_URL.rsplit("/", 1)[-1])):
        receipt = captures[name][1]
        listed = listing["StagedProducts/GeographicNames/Antarctica/" + url_key]
        if receipt.get("bytes") != receipt.get("content_length") or receipt.get("bytes") != listed["size"]:
            raise ValueError("source_capture_listing_size_drift:" + name)

    page = captures["download_page"][0]
    if (b"StagedProducts/GeographicNames/Antarctica/" not in page
            or b"GNIS_file_format.pdf" not in page):
        raise ValueError("official_download_page_link_drift")
    acan_page = captures["antarctic_names_page"][0]
    if (b"StagedProducts/GeographicNames/Antarctica/" not in acan_page
            or b"Advisory Committee on Antarctic Names" not in acan_page):
        raise ValueError("antarctic_names_page_link_drift")
    if not captures["file_format_pdf"][0].startswith(b"%PDF"):
        raise ValueError("file_format_document_invalid")

    if _zip_member_names(captures["gpkg_zip"][0]) != GPKG_ZIP_MEMBERS:
        raise ValueError("gpkg_member_roster_drift")
    if _zip_member_names(captures["text_zip"][0]) != TEXT_ZIP_MEMBERS:
        raise ValueError("text_member_roster_drift")
    gpkg_xml = _zip_member(captures["gpkg_zip"][0], "Gazetteer_Antarctica_GPKG.xml")
    text_xml = _zip_member(captures["text_zip"][0], "Gazetteer_Antarctica_Text.xml")
    for xml_body, suffix in ((gpkg_xml, "GeoPackage"), (text_xml, "pipes")):
        if _fgdc_field(xml_body, "pubdate") != EXPECTED_PUBDATE:
            raise ValueError("gpkg_publication_date_drift")
        if (_fgdc_field(xml_body, "origin") != "U.S. Geological Survey, National Geospatial Program"
                or _fgdc_field(xml_body, "title") !=
                f"Geographic Names Information System (GNIS) Full Model Antarctica "
                f"(published {EXPECTED_PUBDATE}) {suffix}"):
            raise ValueError("gpkg_metadata_drift")

    population = _extract_gpkg_population(_zip_member(captures["gpkg_zip"][0],
                                                      "Gazetteer_Antarctica_GPKG.gpkg"))
    _cross_verify_text(captures["text_zip"][0], population)
    publication_stamps = {key: listing[key]["last_modified"] for key in
                          ("StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GPKG.zip",
                           "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_Text.zip",
                           "StagedProducts/GeographicNames/Antarctica/Gazetteer_Antarctica_GDB.zip")}
    audit = {
        "source_feature_count": EXPECTED_COUNT,
        "source_allnames_count": EXPECTED_ALLNAMES_COUNT,
        "source_id_sha256": EXPECTED_ID_SHA256,
        "source_allnames_id_sha256": EXPECTED_ALLNAMES_ID_SHA256,
        "source_record_sha256": EXPECTED_RECORD_SHA256,
        "source_allnames_record_sha256": EXPECTED_ALLNAMES_RECORD_SHA256,
        "source_geometry_sha256": EXPECTED_GEOMETRY_SHA256,
        "source_schema_sha256": population["schema_sha256"],
        "source_crs_epsg": SOURCE_CRS_WKID,
        "geometry_type": "Point Z",
        "declared_geometry_column_type": "MULTIPOINT",
        "geometry_z_values": population["z_values"],
        "geometry_attribute_max_delta_degrees": EXPECTED_GEOMETRY_ATTRIBUTE_MAX_DELTA,
        "observed_bounds": population["bounds"],
        "null_counts": population["null_counts"],
        "allnames_null_counts": population["allnames_nulls"],
        "cross_format_verified": True,
        "source_release_date": "2026-11-02",
        "source_release_date_basis":
            f"FGDC pubdate {EXPECTED_PUBDATE} in the producer's in-zip metadata for both captured "
            "staged products; S3 LastModified agrees (2026-11-02T20:37:44Z).",
        "source_publication_timestamps": publication_stamps,
        "source_freshness": "The producer republishes this staged product irregularly "
                            "(FGDC update: 'Irregular'); the inventory archive modification date "
                            "(2025-03-20) is not a producer release date.",
        "release_date_unknown": False, "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False,
    }
    return population | {"audit": audit}


_ARROW_FIELDS = {
    "feature_id": pa.int32(), "antarctica_id": pa.int32(), "scar_placeid": pa.float64(),
    "feature_name": pa.string(), "feature_class": pa.string(),
    "date_created": pa.timestamp("ms"), "date_edited": pa.timestamp("ms"),
    "bgn_type": pa.int32(), "bgn_authority": pa.int32(), "bgn_date": pa.timestamp("ms"),
    "prim_lat_dms": pa.string(), "prim_long_dms": pa.string(),
    "prim_lat_dec": pa.float64(), "prim_long_dec": pa.float64(),
    "description": pa.string(), "history": pa.string(),
    "bgn_type_desc": pa.string(), "bgn_authority_desc": pa.string(),
}
_DATETIME_COLUMNS = frozenset({"date_created", "date_edited", "bgn_date"})


def _parquet_bytes(population: dict[str, object]) -> bytes:
    """Serialize the complete native point layer without coercion; bytes are pinned."""
    records: list[list] = population["records"]
    if len(records) != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    columns: dict[str, list[object]] = {name: [] for name in DATA_COLUMNS}
    columns["Shape"] = []
    for values, wkb in records:
        if len(values) != len(DATA_COLUMNS):
            raise ValueError("source_record_width_drift")
        for index, name in enumerate(DATA_COLUMNS):
            value = values[index]
            if name in _DATETIME_COLUMNS:
                columns[name].append(None if value is None else _parse_iso_datetime(value))
            else:
                columns[name].append(value)
        columns["Shape"].append(bytes.fromhex(wkb))
    schema_fields = [pa.field(name, _ARROW_FIELDS[name]) for name in DATA_COLUMNS]
    schema_fields.append(pa.field("Shape", pa.binary(), nullable=True))
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields))
    geo = {"version": "1.1.0", "primary_column": "Shape", "columns": {"Shape": {
        "encoding": "WKB", "geometry_types": ["Point Z"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict()}}}
    table = table.replace_schema_metadata({b"geo": _canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_gnis_antarctica(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved GNIS Antarctica candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/233/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/gnis-antarctica-draft"}) as client:
            for name in CAPTURE_NAMES:
                body, receipt = capture_get(client, CAPTURE_URLS[name], capture_root, name,
                                            max_bytes=MAX_CAPTURE_BYTES)
                captures[name] = (body, receipt)
        extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        retained = dict(captures)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(),
                                  json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          type(exc).__name__)
        raise
    identity = _code_identity()
    audit = extracted["audit"]
    parquet_body = _parquet_bytes(extracted)
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    release_date = {"gpkg_zip": audit["source_release_date"], "text_zip": audit["source_release_date"]}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Antarctica (GNIS Antarctic Names)",
        "description": "GNIS Full Model Antarctica named-features point layer from the producer's "
                       "own The National Map staged product (Gazetteer_Antarctica_GPKG.zip, published "
                       f"{audit['source_release_date']}), cross-verified against the producer's "
                       "pipe-delimited product. Complete source attributes and native Point Z geometry "
                       "in EPSG:4326 are represented in the candidate. Freshness, rights, public "
                       "schema/ID contract, and release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "U.S. Geological Survey", "source_url": DOWNLOAD_PAGE_URL,
        "license": "Public domain (U.S. Geological Survey); FGDC useconst: 'None. Acknowledgment of "
                   "the U.S. Geological Survey would be appreciated for products derived from these "
                   "data.'",
        "source_modified": audit["source_release_date"], "date_issued": None,
        "date_modified": audit["source_release_date"],
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{
            "input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication", "canonical_url": CAPTURE_URLS[name],
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "release_date": release_date.get(name),
            **({} if name in release_date else {
                "release_date_unknown_reason":
                    "Live index/documentation page without its own per-snapshot release date; the "
                    f"captured staged products it lists are published {audit['source_release_date']} "
                    "(FGDC pubdate 20261102)."}),
            "license": "Public domain (U.S. Geological Survey).",
            "attribution": "U.S. Geological Survey, National Geospatial Program; U.S. Board on "
                           "Geographic Names (Advisory Committee on Antarctic Names).",
        } for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{
            "operation": "native_gpkg_to_geoparquet_candidate",
            "parameters": {
                "source_fields": "All 18 producer AntarcticaNames columns retained in producer order "
                                 "and physical types (MEDIUMINT int32, REAL float64, DATETIME "
                                 "timestamp[ms], TEXT string); the producer's GeoPackage rowid (fid) "
                                 "is GPKG-internal storage, absent from the documented product format, "
                                 "and is retained only in the raw capture evidence",
                "source_ids": "Preserve producer feature_id, antarctica_id and scar_placeid values "
                              "unchanged; feature_id is unique and non-null; no substitute IDs, casts, "
                              "trimming or joins",
                "geometry": "Native ISO WKB Point Z (type 1001, z == 0.0) bytes preserved verbatim in "
                            "EPSG:4326 without reprojection, deduplication or repair; the producer's "
                            "7-decimal prim_*_dec attributes are retained unchanged alongside the "
                            "higher-precision geometry (max delta "
                            f"{EXPECTED_GEOMETRY_ATTRIBUTE_MAX_DELTA} degrees)",
                "variant_names": "The producer's AntarcticaAllNames variant-name table ("
                                 f"{EXPECTED_ALLNAMES_COUNT} rows) is part of the captured Full Model; "
                                 "it is retained as raw evidence with audit digests and is not part of "
                                 "this point-layer candidate pending a scope decision",
                "candidate_status": "unapproved; publication blocked pending freshness, schema/ID "
                                   "contract, attribution and release acceptance"}}],
        "lineage": "USGS publishes the GNIS Full Model Antarctica staged product on its own The "
                   "National Map staged-products distribution "
                   "(https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Antarctica/). "
                   "The official USGS 'Download Names Data' page "
                   "(https://www.usgs.gov/us-board-on-geographic-names/download-gnis-data) lists that "
                   "directory for 'Names data for the continent of Antarctica approved by the Advisory "
                   "Committee on Antarctic Names of the U.S. Board of Geographic Names', and the USGS "
                   "Antarctic Names page links the same directory; both pages are captured as "
                   "evidence. This candidate is the AntarcticaNames layer of the producer's "
                   f"Gazetteer_Antarctica_GPKG.zip published {audit['source_release_date']}. The HIFLD "
                   "Open inventory lists this dataset as nmg/antarctica-1 with DataLumos project "
                   "240193; that ICPSR archive copy, the 2025-03-20 archive modification date, and "
                   "every HIFLD rescue mirror are identification and comparison context only, never "
                   "pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls and geometry "
                      "retained in the producer CRS; all release and compatibility approvals remain "
                      "pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    combined_record_sha256 = _list_sha([extracted["records"], extracted["allnames_records"]])
    version = content_derived_version(combined_record_sha256, audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}
                   ) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, combined_record_sha256,
                                audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "variant_name_count": EXPECTED_ALLNAMES_COUNT,
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
         "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n"
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": combined_record_sha256,
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "variant_name_count": EXPECTED_ALLNAMES_COUNT,
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}
