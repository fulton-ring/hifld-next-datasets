"""Bounded, source-faithful capture of the USGS OFR 2012-1205 coal fields shapefile.

The assigned dataset is the "US Coal Fields" polygon layer of the retired HIFLD
Open catalog (inventory ID ``us-coal-fields-1``). The DHS crosswalk row for
"US Coal Fields" records Status "Deprecate" (New Hosting N/A, justification "No
longer supported as part of the HIFLD mission") and names the producer
publication landing page https://pubs.usgs.gov/of/2012/1205/Downloads/. The
producer is the U.S. Geological Survey: the layer is the ``Coal_Fields`` ESRI
shapefile inside the official ``GIS.zip`` package of Open-File Report 2012-1205
("Coal Fields of the Conterminous United States: National Coal Resource
Assessment Updated Version", Joseph A. East; first posted April 2, 2013;
publication year 2013, dataset caldate 2013, update frequency not planned).
The deprecated DHS ArcGIS FeatureServer export and the DataLumos archive copy
are retired copies and are never fetched.

This module performs one complete, bounded HTTP capture of the producer's
publication package plus its publication evidence, validates pinned digests and
the package structure, decodes the source shapefile natively (DBF values
preserved as exact decimals, SHP polygons rebuilt without repair), and stages
the exact source member bytes for the shared catalog flow. No source
authority, schema/ID contract, baseline, rights, coverage or release
acceptance is performed; every acceptance flag stays false.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import struct
import time
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import Polygon

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "us-coal-fields-1"
FILE_SLUG = DATASET_SLUG
PUBS_LANDING_URL = "https://pubs.usgs.gov/of/2012/1205/"
DOWNLOADS_DIR_URL = "https://pubs.usgs.gov/of/2012/1205/Downloads/"
README_URL = "https://pubs.usgs.gov/of/2012/1205/Downloads/Readme.txt"
METADATA_XML_URL = "https://pubs.usgs.gov/of/2012/1205/Downloads/Metadata/Coal_Fields.xml"
GIS_ZIP_URL = "https://pubs.usgs.gov/of/2012/1205/Downloads/GIS.zip"
DOCUMENT_CAPTURE_LIMIT = 4_000_000
ZIP_CAPTURE_LIMIT = 240_000_000
TOTAL_CAPTURE_BUDGET = 520_000_000
REPORT_FIRST_POSTED_DATE = "2013-04-02"

# Byte pins observed on 2026-11-14 against https://pubs.usgs.gov/of/2012/1205/Downloads/.
GIS_ZIP_PIN = {
    "bytes": 230_322_811,
    "sha256": "e491215d0a937cae48cc5e9a2763ea43c85a4d11c183ec7c5b1ac559d2a585a5",
}
README_PIN = {
    "bytes": 4_771,
    "sha256": "66b45d9f1c38a3ddc97a05d3933b85ef6d0f28b189a89eb4e66d0baab93813f9",
}
METADATA_XML_PIN = {
    "bytes": 43_681,
    "sha256": "17299068e32ab1bffce6391fd803ce70a6dd08fea91de49772e432fbb2833ba5",
}
ZIP_MEMBER_COUNT = 914
UPDATED_COAL_FIELDS_PREFIX = "GIS/Updated Coal Fields/"
UPDATED_COAL_FIELDS_ROSTER = {
    "GIS/Updated Coal Fields/": (0, 0x0),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.dbf": (7563, 0x9A259CF0),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.prj": (467, 0x193DA029),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.sbn": (1620, 0x74D4D3C0),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.sbx": (212, 0x8D0FE817),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.shp": (4216, 0xCD61EDD8),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.shp.xml": (11397, 0x4A84E66E),
    "GIS/Updated Coal Fields/Coal_Field_Point_locations.shx": (1276, 0x5D477C07),
    "GIS/Updated Coal Fields/Coal_Fields.dbf": (103770, 0xAE47CC5D),
    "GIS/Updated Coal Fields/Coal_Fields.prj": (167, 0xBA106304),
    "GIS/Updated Coal Fields/Coal_Fields.sbn": (6268, 0x1961FD0D),
    "GIS/Updated Coal Fields/Coal_Fields.sbx": (532, 0x384978B9),
    "GIS/Updated Coal Fields/Coal_Fields.shp": (4811824, 0xE18D5C37),
    "GIS/Updated Coal Fields/Coal_Fields.shp.xml": (15868, 0x848A8F52),
    "GIS/Updated Coal Fields/Coal_Fields.shx": (4916, 0x930FD131),
    "GIS/Updated Coal Fields/Coal_Provinces.dbf": (476, 0xCECE006D),
    "GIS/Updated Coal Fields/Coal_Provinces.prj": (167, 0xBA106304),
    "GIS/Updated Coal Fields/Coal_Provinces.sbn": (204, 0x8BDF97FC),
    "GIS/Updated Coal Fields/Coal_Provinces.sbx": (132, 0x79DB4767),
    "GIS/Updated Coal Fields/Coal_Provinces.shp": (12832896, 0xF455D414),
    "GIS/Updated Coal Fields/Coal_Provinces.shp.xml": (9047, 0x536F1648),
    "GIS/Updated Coal Fields/Coal_Provinces.shx": (148, 0xA4ED013B),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.dbf": (23114, 0x3CEF539E),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.prj": (167, 0xBA106304),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.sbn": (612, 0xF9322049),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.sbx": (164, 0x9FEDD67D),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.shp": (17202152, 0xBD29B848),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.shp.xml": (14222, 0x4B4A8759),
    "GIS/Updated Coal Fields/EIA_Coal_Production_dd.shx": (492, 0x647067F2),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.dbf": (32298, 0x27D479F8),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.prj": (167, 0xBA106304),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.sbn": (6380, 0x396AD9A5),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.sbx": (348, 0xB73EB1AD),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.shp": (3379380, 0xDA1D7199),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.shp.xml": (9822, 0x438DC603),
    "GIS/Updated Coal Fields/NCRA_Assessed Basins.shx": (5156, 0xC6537534),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.dbf": (2118, 0xC5739D1D),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.prj": (167, 0xBA106304),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.sbn": (316, 0xDA84A841),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.sbx": (132, 0xA5FD7648),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.shp": (75456, 0x07742A46),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.shp.xml": (11667, 0x0B964E6A),
    "GIS/Updated Coal Fields/Unassessed Mesozoic Basins.shx": (260, 0x3C61E715),
}
COAL_FIELDS_MEMBER_PINS = {
    "Coal_Fields.shp": {"bytes": 4_811_824, "sha256": "37ee53bb51f826734ae2b658658d80b8141b17d49b9dd8025e252513dfbc2ce4"},
    "Coal_Fields.shx": {"bytes": 4_916, "sha256": "9ad488303922cdc699e81a2a9944c585ce42e005bcfc72e1ba5f3b9641faecf7"},
    "Coal_Fields.dbf": {"bytes": 103_770, "sha256": "d09a99fa2eea1656b5ee6fe50f0564b2875911978ff6b35be58b38c0852494c2"},
    "Coal_Fields.prj": {"bytes": 167, "sha256": "84cf5b9a3c1a444f83ac30ff76f8c5035990223027ba69b2f4d628388adcbd65"},
    "Coal_Fields.sbn": {"bytes": 6_268, "sha256": "0cd785b50def7e6bc647951d1d31e0ef3bd043a2c4cea92bbb36a53766c522ae"},
    "Coal_Fields.sbx": {"bytes": 532, "sha256": "1d3249649259c91e13c877af6bf58fd3d44071a42109c40a7fc80ccb01d36e14"},
    "Coal_Fields.shp.xml": {"bytes": 15_868, "sha256": "6475c5044a24632e89a88681c97ed8f68835f731e5987a53bfada5130fc113ad"},
}
SHAPEFILE_STAGE_STEM = "Coal_Fields"
# The in-package ESRI-profile metadata member (distinct from the standalone
# Downloads/Metadata/Coal_Fields.xml FGDC record).
IN_PACKAGE_SHAPEFILE_XML_MARKERS = (
    "<CreaDate>20111130</CreaDate>",
    "<SyncDate>20111122</SyncDate>",
    "<abstract>This data set is a polygon shapefile representing the coal field of the United States.",
    "OFR 96-92",
    "1:5000000",
)

# Pinned structural markers of the producer publication (fail-closed identity
# anchors; the landing page and downloads index are dynamic HTML and are pinned
# by content markers, not bytes).
LANDING_MARKERS = (
    "Coal Fields of the Conterminous United States",
    "National Coal Resource Assessment Updated Version",
    "Joseph A. East",
    "First posted April 2, 2013",
    "Downloads Directory",
)
DOWNLOADS_INDEX_MARKERS = (
    "Index of /pubs/of/2012/1205/Downloads",
    "GIS.zip",
    "Readme.txt",
    "Metadata/",
)
README_MARKERS = (
    "Coal Fields of the Conterminous United States: National Coal Resource Assessment Updated Version",
    "U.S. Geological Survey Open-File Report 2012-1205",
    "Joseph A. East",
    "Coal_Fields_GIS.mxd",
)
METADATA_XML_MARKERS = (
    "<title>Coal Fields of the Conterminous United States: National Coal Resource Assessment Updated Version</title>",
    "<originator>East, J.A.</originator>",
    "<pubdate>2012</pubdate>",
    "<publish>U.S. Geological Survey</publish>",
    "<pubplace>Reston, VA</pubplace>",
    "http://pubs.usgs.gov/of/2012/1205/",
    "<abstract>This dataset is a polygon shapefile representing the most recent update of the coal fields of the conterminous United States.",
)

# Pinned native declarations of the staged source shapefile.
NATIVE_PRJ = (
    'GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",'
    'SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]]'
)
NATIVE_CRS_EPSG = 4269
DBF_FIELDS = [
    ("AREA", "N", 12, 3),
    ("PERIMETER", "N", 12, 3),
    ("PROVINCE", "C", 31, 0),
    ("NAME", "C", 36, 0),
    ("AGE", "C", 30, 0),
    ("RANK", "C", 50, 0),
]
DBF_RECORDS = 602
DBF_HEADER_BYTES = 225
DBF_RECORD_WIDTH = 172
SHP_SHAPE_TYPE = 5
EXPECTED_BOUNDS = [-124.36347198486328, 26.034013748169002, -71.1495132446289, 49.00178527832048]
# One source-published ring self-intersection ("Ring Self-intersection" at
# [-110.25617980957, 47.0629196166993], record 95 / NAME "Great Falls Field").
# The geometry is retained exactly as published; no repair is applied.
EXPECTED_INVALID_RECORD_NUMBERS = (95,)
PROVINCE_COUNTS = {
    "Eastern": 247, "Gulf": 4, "Interior": 56, "Northern Great Plains": 53,
    "Pacific Coast": 70, "Rocky Mountain": 172,
}
AGE_COUNTS = {
    "Cretaceous": 187, "Cretaceous-Jurassic": 2, "Mississippian": 9,
    "Pennsylvanian-Permian": 266, "Tertiary": 123, "Tertiary-Cretaceous": 8,
    "Triassic": 7,
}
RANK_COUNTS = {
    "Anthracite": 29, "Lignite": 57, "Low Volatile Bituminous": 29,
    "Medium and High Volatile Bituminous": 381, "Subbituminous": 106,
}
DISTINCT_NAME_COUNT = 110
LICENSE = (
    "US government publication distributed by the U.S. Geological Survey without fees "
    "(OFR 2012-1205 distribution liability statement). No source, contract, baseline, "
    "rights or release acceptance by this pipeline."
)
_COAL_FIELDS_ZIP_MEMBERS = tuple(f"{UPDATED_COAL_FIELDS_PREFIX}{name}" for name in COAL_FIELDS_MEMBER_PINS)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _document_pins() -> dict[str, dict]:
    return {"Readme.txt": README_PIN, "Coal_Fields.xml": METADATA_XML_PIN, "GIS.zip": GIS_ZIP_PIN}


def _request_url(name: str) -> str:
    return {
        "landing.html": PUBS_LANDING_URL,
        "downloads.html": DOWNLOADS_DIR_URL,
        "Readme.txt": README_URL,
        "Coal_Fields.xml": METADATA_XML_URL,
        "GIS.zip": GIS_ZIP_URL,
    }[name]


def _document_capture_limit(name: str) -> int:
    return ZIP_CAPTURE_LIMIT if name == "GIS.zip" else DOCUMENT_CAPTURE_LIMIT


def _validate_document(name: str, body: bytes) -> None:
    """Fail-closed identity anchors for one captured producer document."""
    pin = _document_pins().get(name)
    if pin is not None:
        _validate_pin(name, body, pin)
    if name == "GIS.zip":
        return
    markers = {
        "landing.html": LANDING_MARKERS,
        "downloads.html": DOWNLOADS_INDEX_MARKERS,
        "Readme.txt": README_MARKERS,
        "Coal_Fields.xml": METADATA_XML_MARKERS,
    }[name]
    text = body.decode("cp1252" if name == "Readme.txt" else "utf-8", errors="strict")
    for marker in markers:
        if marker not in text:
            raise ValueError(f"source_publication_marker_drift:{name}")


def _validate_pin(name: str, body: bytes, pin: dict) -> None:
    if len(body) != pin["bytes"] or _digest(body) != pin["sha256"]:
        raise ValueError(f"pinned_source_bytes_drift:{name}")


def _request(client: httpx.Client, staging, version_key: str, name: str, budget: list[int]) -> bytes:
    """Stream one bounded GET, retain wire/body/receipt evidence, validate identity."""
    max_bytes = _document_capture_limit(name)
    retained = getattr(client, "retained_capture", None)
    if retained is not None:
        body, wire, receipt = retained.read_response(name)
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.receipt.json", _json_bytes(receipt))
        _validate_document(name, body)
        return body
    url = _request_url(name)
    for attempt in range(3):
        raw_parts: list[bytes] = []
        wire_parts: list[bytes] = []
        try:
            with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
                capture = capture_http_response(
                    response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                    reserve=lambda progress: _reserve(budget, progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes),
                    retain_wire=wire_parts.append, retain_decoded=raw_parts.append,
                )
        except Exception as exc:
            raise ValueError("source_transport_error:" + type(exc).__name__) from exc
        body = b"".join(raw_parts)
        wire = b"".join(wire_parts)
        status_ok = capture.complete and 200 <= capture.status_code < 300
        # Preserve unsuccessful responses under attempt-specific names before retrying.
        artifact_name = name if status_ok else f"{name}.attempt_{attempt + 1}"
        receipt = {
            "method": "GET", "url": url, "final_url": str(response.request.url), "status": capture.status_code,
            "attempt": attempt + 1, "observed_at": datetime.now(timezone.utc).isoformat(), "bytes": len(body),
            "sha256": _digest(body), "wire_bytes": len(wire), "wire_sha256": _digest(wire),
            "content_type": response.headers.get("content-type"), "content_encoding": capture.content_encoding,
            "complete": capture.complete, "error_code": capture.error_code,
        }
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.receipt.json", _json_bytes(receipt))
        if status_ok:
            _validate_document(name, body)
            return body
        if capture.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            time.sleep(0.25 * (attempt + 1))
            continue
        raise ValueError("incomplete_source_response:" + name)
    raise ValueError("source_retry_limit_exceeded:" + name)


def _reserve(budget: list[int], upcoming: int) -> None:
    if budget[0] + upcoming > TOTAL_CAPTURE_BUDGET:
        raise ValueError("source_total_capture_budget_exceeded")


class _RetainedCaptureClient:
    """Read immutable successful responses from an earlier retained capture."""

    def __init__(self, staging, source_version: str):
        self.staging = staging
        self.source_version = source_version
        self.retained_capture = self

    def read_response(self, name: str):
        root = self.staging.build_target_location(DATASET_SLUG, FILE_SLUG, self.source_version, "")
        try:
            body = self.staging.read_key(root + f"raw/{name}")
            wire = self.staging.read_key(root + f"raw/{name}.wire")
            receipt = json.loads(self.staging.read_key(root + f"raw/{name}.receipt.json"))
        except (OSError, ValueError) as exc:
            raise ValueError("retained_source_response_missing:" + name) from exc
        if (receipt.get("method") != "GET" or receipt.get("complete") is not True
                or type(receipt.get("status")) is not int or not 200 <= receipt["status"] < 300
                or receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("sha256") != _digest(body) or receipt.get("wire_sha256") != _digest(wire)):
            raise ValueError("retained_source_receipt_mismatch:" + name)
        if receipt.get("url") != _request_url(name):
            raise ValueError("retained_source_request_identity_mismatch:" + name)
        return body, wire, receipt


def decode_dbf(body: bytes) -> list[dict]:
    """Decode the producer DBF exactly: text verbatim, numbers as base-10 Decimals."""
    if len(body) < 33 or len(body) not in (DBF_HEADER_BYTES + DBF_RECORDS * DBF_RECORD_WIDTH,
                                           DBF_HEADER_BYTES + DBF_RECORDS * DBF_RECORD_WIDTH + 1):
        raise ValueError("incomplete_or_oversized_source_dbf")
    count, header, width = struct.unpack_from("<IHH", body, 4)
    fields = []
    offset = 32
    while offset + 32 <= header and body[offset] != 13:
        desc = body[offset:offset + 32]
        fields.append((desc[:11].split(b"\0")[0].decode("ascii"), chr(desc[11]), desc[16], desc[17]))
        offset += 32
    if (fields != DBF_FIELDS or count != DBF_RECORDS or header != DBF_HEADER_BYTES
            or width != DBF_RECORD_WIDTH or body[offset] != 13):
        raise ValueError("missing_renamed_extra_or_changed_source_dbf_schema")
    if len(body) == DBF_HEADER_BYTES + DBF_RECORDS * DBF_RECORD_WIDTH + 1 and body[-1] != 26:
        raise ValueError("unexpected_source_dbf_tail")
    rows = []
    for fid in range(count):
        record = body[header + fid * width:header + (fid + 1) * width]
        if record[:1] != b" ":
            raise ValueError("deleted_or_unknown_source_dbf_record")
        offset = 1
        row = {"source_fid": fid, "source_shp_record_number": fid + 1}
        for name, kind, size, scale in fields:
            raw = record[offset:offset + size]
            offset += size
            if any(value > 127 or value < 32 for value in raw):
                raise ValueError("undeclared_nonASCII_source_dbf_encoding")
            if kind == "C":
                row[name] = raw.rstrip(b" ").decode("ascii")
            else:
                text = raw.decode("ascii").strip()
                if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
                    raise ValueError("null_nonfinite_or_unparseable_source_dbf_number")
                value = Decimal(text)
                if value.as_tuple().exponent < -scale:
                    raise ValueError("declared_source_dbf_scale_drift")
                row[name] = value
        rows.append(row)
    return rows


def decode_shx(body: bytes) -> list[tuple[int, int]]:
    if len(body) < 108 or struct.unpack_from(">i", body)[0] != 9994:
        raise ValueError("incomplete_source_shx")
    count = (len(body) - 100) // 8
    if count != DBF_RECORDS or len(body) != 100 + 8 * count:
        raise ValueError("source_shx_record_count_drift")
    if struct.unpack_from(">i", body, 24)[0] * 2 != len(body) or struct.unpack_from("<ii", body, 28) != (1000, SHP_SHAPE_TYPE):
        raise ValueError("source_shx_header_drift")
    return [struct.unpack_from(">ii", body, 100 + 8 * fid) for fid in range(count)]


def _ring_area(ring: list[tuple[float, float]]) -> float:
    # Translate to the first vertex to avoid catastrophic cancellation.
    x0, y0 = ring[0]
    return sum((ring[i][0] - x0) * (ring[i + 1][1] - y0) - (ring[i + 1][0] - x0) * (ring[i][1] - y0)
               for i in range(len(ring) - 1)) / 2


def _polygon_record(body: bytes, offset: int, content_length: int, record_number: int) -> tuple[object, list[list[list[float]]]]:
    """Decode one native ESRI polygon record without repair.

    Returns the shapely geometry and the exact source ring lists grouped as
    [shell, holes...] per polygon member.
    """
    record_number_read, words = struct.unpack_from(">ii", body, offset)
    # SHX entry words and the SHP record-header words both count the content
    # only; the 8-byte record header is excluded from both.
    if record_number_read != record_number or words * 2 != content_length:
        raise ValueError("source_shp_record_header_drift")
    shape_type = struct.unpack_from("<i", body, offset + 8)[0]
    if shape_type != SHP_SHAPE_TYPE:
        raise ValueError("source_shp_shape_type_drift")
    num_parts, num_points = struct.unpack_from("<ii", body, offset + 44)
    content_size = 44 + 4 * num_parts + 16 * num_points
    if num_parts < 1 or num_points < 1 or content_size != content_length:
        raise ValueError("source_shp_record_layout_drift")
    parts = struct.unpack_from(f"<{num_parts}i", body, offset + 52)
    if any(part < 0 or part >= num_points for part in parts) or list(parts) != sorted(parts):
        raise ValueError("source_shp_part_index_drift")
    values = struct.unpack_from(f"<{2 * num_points}d", body, offset + 52 + 4 * num_parts)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("nonfinite_source_shp_coordinate")
    points = [(values[2 * index], values[2 * index + 1]) for index in range(num_points)]
    rings = []
    for part in range(num_parts):
        start = parts[part]
        end = parts[part + 1] if part + 1 < num_parts else num_points
        ring = points[start:end]
        if len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid")
        rings.append(ring)
    shells = [(ring, Polygon(ring)) for ring in rings if _ring_area(ring) < 0]
    holes = [ring for ring in rings if _ring_area(ring) >= 0]
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list[list[tuple[float, float]]]] = {index: [] for index in range(len(shells))}
    for hole in holes:
        hole_polygon = Polygon(hole)
        try:
            containers = [(shells[index][1].area, index) for index in range(len(shells))
                          if shells[index][1].contains(hole_polygon)]
        except Exception as exc:
            raise ValueError("source_polygon_ring_nesting_ambiguous") from exc
        if not containers:
            raise ValueError("source_polygon_ring_nesting_ambiguous")
        _area, owner = min(containers)
        assigned[owner].append(hole)
    members = [Polygon(shells[index][0], assigned[index]) for index in range(len(shells))]
    geometry = members[0] if len(members) == 1 else shapely.MultiPolygon(members)
    if shapely.is_empty(geometry):
        raise ValueError("empty_source_polygon")
    grouped = [[shells[index][0], *assigned[index]] for index in range(len(shells))]
    return geometry, grouped


def decode_shp(body: bytes, index: bytes) -> tuple[list[object], list[list[list[list[float]]]]]:
    """Decode every native polygon record in source order; no repair, no reprojection."""
    if len(body) < 108 or struct.unpack_from(">i", body)[0] != 9994:
        raise ValueError("incomplete_source_shp")
    if struct.unpack_from(">i", body, 24)[0] * 2 != len(body) or struct.unpack_from("<ii", body, 28) != (1000, SHP_SHAPE_TYPE):
        raise ValueError("source_shp_header_drift")
    shape_type = struct.unpack_from("<i", body, 32)[0]
    if shape_type != SHP_SHAPE_TYPE:
        raise ValueError("source_shp_shape_type_drift")
    entries = decode_shx(index)
    geometries: list[object] = []
    grouped_all: list[list[list[list[float]]]] = []
    walked = 100
    for record_number, (entry_offset, entry_words) in enumerate(entries, start=1):
        offset = entry_offset * 2
        record_bytes = 8 + entry_words * 2
        if offset != walked or offset + record_bytes > len(body):
            raise ValueError("source_shp_shx_alignment_drift")
        geometry, grouped = _polygon_record(body, offset, entry_words * 2, record_number)
        geometries.append(geometry)
        grouped_all.append(grouped)
        walked += record_bytes
    if walked != len(body):
        raise ValueError("extra_source_shp_records")
    header_bounds = list(struct.unpack_from("<4d", body, 36))
    bounds = shapely.total_bounds(geometries).tolist()
    if header_bounds != EXPECTED_BOUNDS or bounds != EXPECTED_BOUNDS:
        raise ValueError("source_geometry_bounds_drift")
    invalid = [record_number for record_number, geometry in enumerate(geometries, start=1)
               if not shapely.is_valid(geometry)]
    if tuple(invalid) != EXPECTED_INVALID_RECORD_NUMBERS:
        raise ValueError("source_geometry_validity_drift")
    return geometries, grouped_all


def verify_attribute_counts(rows: list[dict]) -> None:
    provinces: dict[str, int] = {}
    ages: dict[str, int] = {}
    ranks: dict[str, int] = {}
    names: set[str] = set()
    for row in rows:
        provinces[row["PROVINCE"]] = provinces.get(row["PROVINCE"], 0) + 1
        ages[row["AGE"]] = ages.get(row["AGE"], 0) + 1
        ranks[row["RANK"]] = ranks.get(row["RANK"], 0) + 1
        names.add(row["NAME"])
    if provinces != PROVINCE_COUNTS or ages != AGE_COUNTS or ranks != RANK_COUNTS:
        raise ValueError("source_attribute_value_population_drift")
    if len(names) != DISTINCT_NAME_COUNT:
        raise ValueError("source_name_population_drift")


def _extract_coal_fields_members(zip_body: bytes) -> dict[str, bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_body))
    except (OSError, ValueError, zipfile.BadZipFile, EOFError) as exc:
        raise ValueError("invalid_source_package_container") from exc
    infos = archive.infolist()
    if len(infos) != ZIP_MEMBER_COUNT:
        raise ValueError("source_package_member_roster_drift")
    names = {info.filename for info in infos}
    for name in names:
        if name.startswith(UPDATED_COAL_FIELDS_PREFIX) and name not in UPDATED_COAL_FIELDS_ROSTER:
            raise ValueError(f"source_package_member_roster_drift:{name}")
    for name, (size, crc) in UPDATED_COAL_FIELDS_ROSTER.items():
        if name not in names:
            raise ValueError(f"source_package_member_missing:{name}")
        info = archive.getinfo(name)
        if info.file_size != size or info.CRC != crc:
            raise ValueError(f"source_package_member_size_or_crc_drift:{name}")
    members = {}
    for member_name, pin in COAL_FIELDS_MEMBER_PINS.items():
        name = f"{UPDATED_COAL_FIELDS_PREFIX}{member_name}"
        if name not in names:
            raise ValueError(f"source_package_member_missing:{member_name}")
        body = archive.read(name)  # zipfile verifies the member CRC on read.
        if len(body) != pin["bytes"] or _digest(body) != pin["sha256"]:
            raise ValueError(f"pinned_source_member_bytes_drift:{member_name}")
        members[member_name] = body
    if members["Coal_Fields.prj"].decode("ascii") != NATIVE_PRJ:
        raise ValueError("source_prj_drift")
    text = members["Coal_Fields.shp.xml"].decode("utf-8", errors="strict")
    for marker in IN_PACKAGE_SHAPEFILE_XML_MARKERS:
        if marker not in text:
            raise ValueError("source_package_member_metadata_marker_drift:Coal_Fields.shp.xml")
    return members


def staged_shapefile_name(member_name: str) -> str:
    """The staged shapefile keeps the exact source member name."""
    return member_name


def _make_parquet(rows: list[dict], geometries: list[object]) -> bytes:
    fields = [
        pa.field("source_fid", pa.int64(), nullable=False, metadata={
            b"identity": b"Original zero-based USGS DBF/SHP physical record index of the source row, not a published or replacement feature ID"}),
        pa.field("source_shp_record_number", pa.int64(), nullable=False, metadata={
            b"identity": b"Original USGS SHP/DBF record number (1-based) in source order"}),
    ]
    for name, kind, width, scale in DBF_FIELDS:
        fields.append(pa.field(name, pa.string() if kind == "C" else pa.decimal128(width, scale),
                               nullable=False,
                               metadata={b"original_DBF_physical_declaration": _json_bytes(
                                   {"type": kind, "width": width, "decimals": scale}).strip()}))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    table = pa.Table.from_pylist([
        {**{name: row[name] for name, *_ in DBF_FIELDS},
         "source_fid": row["source_fid"], "source_shp_record_number": row["source_shp_record_number"],
         "geometry": shapely.to_wkb(geometries[row["source_fid"]], byte_order=1, output_dimension=2)}
        for row in rows
    ], schema=pa.schema(fields))
    metadata_bytes = dict(table.schema.metadata or {})
    metadata_bytes[b"geo"] = _json_bytes({
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "edges": "planar",
                                 "crs": pyproj.CRS.from_epsg(NATIVE_CRS_EPSG).to_json_dict(),
                                 "bbox": EXPECTED_BOUNDS}},
    }).strip()
    table = table.replace_schema_metadata(metadata_bytes)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _validate_capture_receipt(name: str, body: bytes, wire: bytes, receipt: dict) -> None:
    if (receipt.get("url") != _request_url(name) or type(receipt.get("status")) is not int
            or receipt["status"] != 200 or receipt.get("complete") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _digest(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _digest(wire)):
        raise ValueError(f"capture_receipt_drift:{name}")


def _write_source_manifest(staging, version_key: str, capture_receipts: dict[str, dict],
                           identity: dict, row_digest: str, raw_source_version: str | None) -> dict:
    release_note = (
        "First posted April 2, 2013 per the captured USGS Pubs Warehouse landing page; the publication is the "
        "current maintained USGS release of this dataset (Pubs Warehouse 'Year Published' 2013; FGDC pubdate 2012, "
        "dataset caldate 2013; update frequency not planned). The 2021 'Simplified Presentation Format' explicitly "
        "revised no data."
    )
    input_notes = {
        "landing.html": ("U.S. Geological Survey (Pubs Warehouse publication landing page)", PUBS_LANDING_URL, None),
        "downloads.html": ("U.S. Geological Survey (publication Downloads directory index)", DOWNLOADS_DIR_URL,
                           "The Apache directory listing timestamps are server file times, not release dates."),
        "Readme.txt": ("U.S. Geological Survey (OFR 2012-1205 package readme)", README_URL, None),
        "Coal_Fields.xml": ("U.S. Geological Survey (OFR 2012-1205 FGDC metadata for the Coal_Fields shapefile)",
                            METADATA_XML_URL,
                            "FGDC publication date 2012 (report) with dataset caldate 2013; the byte-pinned metadata "
                            "file is part of the producer publication package."),
        "GIS.zip": ("U.S. Geological Survey (OFR 2012-1205 GIS package)", GIS_ZIP_URL,
                    "The package member 'GIS/Updated Coal Fields/Coal_Fields.*' is the assigned coal-fields polygon "
                    "dataset. The deprecated DHS HIFLD FeatureServer export of it was vertex-faithful (602/602 "
                    "features, identical attributes, NAD83-to-CRS84 datum offset only)."),
    }
    inputs = []
    for name in ("landing.html", "downloads.html", "Readme.txt", "Coal_Fields.xml", "GIS.zip"):
        receipt = capture_receipts[name]
        producer, canonical_url, extra_basis = input_notes[name]
        inputs.append({
            "input_id": f"{name}:{receipt['sha256']}", "producer": producer,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": canonical_url, "retrieval_url": receipt["url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "raw_path": f"raw/{name}",
            "license": LICENSE,
            "attribution": "U.S. Geological Survey Open-File Report 2012-1205 (East, J.A., 2013).",
            "release_date": REPORT_FIRST_POSTED_DATE,
            "release_date_basis": release_note + (f" {extra_basis}" if extra_basis else ""),
        })
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "title": "US Coal Fields — USGS OFR 2012-1205 producer publication capture (unapproved draft)",
        "description": "Complete pinned capture of the USGS Open-File Report 2012-1205 Downloads package with the "
                       "Coal_Fields ESRI shapefile (602 coal-field polygons, EPSG:4269) staged losslessly. The "
                       "deprecated DHS/HIFLD ArcGIS export and the DataLumos archive copy are never fetched. "
                       "Edition, HIFLD continuity, baseline, schema/ID contract, coverage, rights and release "
                       "decisions remain pending maintainer acceptance.",
        "agency": "U.S. Geological Survey (producer; authority acceptance pending)",
        "publisher": "U.S. Geological Survey (Pubs Warehouse; OFR 2012-1205)",
        "source_url": PUBS_LANDING_URL,
        "documentation_url": PUBS_LANDING_URL,
        "license": LICENSE,
        "date_issued": REPORT_FIRST_POSTED_DATE, "date_modified": None,
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "processing_code": identity, "inputs": inputs,
        "transformations": [{"operation": "Complete pinned USGS publication-package capture to staged native shapefile candidate",
                             "parameters": {
                                 "package": "The full GIS.zip package is pinned by bytes and SHA-256 and retained raw; "
                                            "only its 'GIS/Updated Coal Fields/Coal_Fields.*' members are staged as the "
                                            "source dataset.",
                                 "attributes": "AREA and PERIMETER preserved as exact base-10 Decimal values of the "
                                               "source DBF N(12,3) fields; PROVINCE/NAME/AGE/RANK preserved verbatim as "
                                               "ASCII text.",
                                 "identifiers": "The shapefile declares no feature ID column; source_fid and "
                                                "source_shp_record_number retain the original DBF/SHP physical record "
                                                "positions. No synthetic identifier is generated.",
                                 "geometry": "Native ESRI Polygon records decoded from SHP/SHX with exact source ring "
                                             "lists; clockwise shells and counterclockwise holes grouped without "
                                             "dissolve or repair; the one source-published ring self-intersection "
                                             "(record 95, Great Falls Field) is retained unchanged. Native CRS "
                                             "EPSG:4269 (NAD83) as declared by the source PRJ; no reprojection.",
                                 "candidate_status": "Unapproved draft; no output or release format approval implied."}}],
        "lineage": {
            "scope": "The 'Coal_Fields' polygon shapefile of USGS OFR 2012-1205 (GIS.zip member "
                     "'GIS/Updated Coal Fields/Coal_Fields.*'); 602 coal-field polygons with rank, province, name and "
                     "age attributes at 1:5,000,000 scale for the conterminous United States.",
            "producer_publication": "U.S. Geological Survey Open-File Report 2012-1205, first posted April 2, 2013 "
                                    "(https://doi.org/10.3133/ofr20121205); update of Tully (1996) OFR 96-92 using "
                                    "NCRA assessments. USGS maintains the publication permanently; update frequency "
                                    "not planned.",
            "hifld_continuity": {
                "basis": "The DHS crosswalk row for 'US Coal Fields' records Status Deprecate with New Hosting N/A and "
                         "justification 'No longer supported as part of the HIFLD mission', and names "
                         "https://pubs.usgs.gov/of/2012/1205/Downloads/ as the external landing page. The deprecated "
                         "HIFLD FeatureServer export (602 features) was verified to match this shapefile row-for-row "
                         "with identical attributes and a vertex-faithful NAD83-to-CRS84 datum offset (298,530/298,530 "
                         "vertices, max deviation 1.7e-05 degrees); that export is retired and never fetched.",
                "hifld_continuity_accepted": False,
            },
            "banned_copies_never_fetched": (
                "DataLumos archive project 240310 (inventory Download Location), the deprecated "
                "services1.arcgis.com/Hp6G80Pky0om7QvQ FeatureServer, and GeoPlatform item "
                "13ad01f317f74e5989c3ee199496fa32 are retired/rescued copies; none is fetched or pinned as an input."
            ),
            "source_release_date_known": True,
            "source_release_date": REPORT_FIRST_POSTED_DATE,
            "source_release_date_basis": release_note,
            "source_record_sha256": row_digest,
            "raw_capture_mode": "retained_response_replay" if raw_source_version else "live_http_capture",
            "retained_source_capture_version": raw_source_version,
            "failed_retry_responses": [],
            "source_contract_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False,
            "rights_accepted": False, "baseline_accepted": False, "release_accepted": False,
        },
        "unsupported_output_formats": {
            "shapefile_zip": "The source package member set is staged exactly; no approved packaging transformation exists.",
            "geopackage": "No approved container-schema or release mapping exists.",
            "pmtiles": "No approved reprojection, quantization, or visualization mapping exists.",
        },
    }
    validate_provenance(manifest)
    staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/source_manifest.json", _json_bytes(manifest))
    return manifest


def _capture(staging, version_key: str, client: httpx.Client | None = None,
             retained_source_version: str | None = None) -> dict:
    """Fetch, validate, and stage one complete pinned producer-package capture."""
    owned = client is None
    client = client or httpx.Client(timeout=300, follow_redirects=False)
    try:
        budget = [0]
        bodies = {}
        for name in ("landing.html", "downloads.html", "Readme.txt", "Coal_Fields.xml", "GIS.zip"):
            bodies[name] = _request(client, staging, version_key, name, budget)
        members = _extract_coal_fields_members(bodies["GIS.zip"])
        rows = decode_dbf(members["Coal_Fields.dbf"])
        geometries, _grouped = decode_shp(members["Coal_Fields.shp"], members["Coal_Fields.shx"])
        if len(rows) != len(geometries):
            raise ValueError("source_record_geometry_alignment_drift")
        verify_attribute_counts(rows)
        capture_receipts = {}
        for name in bodies:
            root = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
            body = staging.read_key(root + f"raw/{name}")
            wire = staging.read_key(root + f"raw/{name}.wire")
            receipt = json.loads(staging.read_key(root + f"raw/{name}.receipt.json"))
            _validate_capture_receipt(name, body, wire, receipt)
            capture_receipts[name] = receipt
        row_digest = _digest(_json_bytes([
            {"source_fid": row["source_fid"], "source_shp_record_number": row["source_shp_record_number"],
             **{name: str(row[name]) for name, *_ in DBF_FIELDS}}
            for row in rows
        ]))
        for member_name, body in members.items():
            staging.write(DATASET_SLUG, FILE_SLUG, version_key,
                          f"shapefile/{staged_shapefile_name(member_name)}", body)
        parquet = _make_parquet(rows, geometries)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key,
                      f"metadata/native_candidate/{FILE_SLUG}.parquet", parquet)
        processor_identity = processing_identity(Path(__file__).parents[3])
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/native_capture.json", _json_bytes({
            "source_landing_page": PUBS_LANDING_URL,
            "source_downloads_directory": DOWNLOADS_DIR_URL,
            "source_package_url": GIS_ZIP_URL,
            "producer": "U.S. Geological Survey",
            "publication": "USGS Open-File Report 2012-1205 (East, J.A., 2013), first posted April 2, 2013",
            "source_release_date": REPORT_FIRST_POSTED_DATE,
            "source_release_date_known": True,
            "dataset_shapefile": "GIS/Updated Coal Fields/Coal_Fields.*",
            "package_member_count": ZIP_MEMBER_COUNT,
            "pinned_package": GIS_ZIP_PIN,
            "pinned_document_pins": {"Readme.txt": README_PIN, "Coal_Fields.xml": METADATA_XML_PIN},
            "pinned_member_pins": COAL_FIELDS_MEMBER_PINS,
            "native_crs": f"EPSG:{NATIVE_CRS_EPSG}",
            "native_prj": NATIVE_PRJ,
            "dbf_fields": DBF_FIELDS,
            "record_count": DBF_RECORDS,
            "geometry_bounds": EXPECTED_BOUNDS,
            "source_published_invalid_records": list(EXPECTED_INVALID_RECORD_NUMBERS),
            "attribute_value_counts": {"PROVINCE": PROVINCE_COUNTS, "AGE": AGE_COUNTS, "RANK": RANK_COUNTS,
                                       "distinct_NAME": DISTINCT_NAME_COUNT},
            "record_digest": row_digest, "parquet_sha256": _digest(parquet),
            "capture_mode": "retained_response_replay" if retained_source_version else "live_http_capture",
            "retained_source_capture_version": retained_source_version,
            "coverage_statement": "Complete captured USGS OFR 2012-1205 'Coal_Fields' polygon shapefile (602 "
                                  "coal-field polygons, conterminous United States); no claim is made beyond this "
                                  "published layer.",
            "code_identity": processor_identity,
            "refresh_semantics": "Default runs capture a fresh source snapshot. An explicit retained_response_replay "
                                 "is an evidence-only reprocessing operation, not a cache or currentness claim.",
            "historical_hifld_continuity_accepted": False,
            "source_contract_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "release_accepted": False,
        }))
        _write_source_manifest(staging, version_key, capture_receipts, processor_identity, row_digest,
                               retained_source_version)
        return {
            "feature_count": len(rows), "records_sha256": row_digest,
            "parquet_sha256": _digest(parquet), "release_accepted": False,
            "source_population_complete": True,
            "source_coverage_scope": "Complete captured USGS OFR 2012-1205 'Coal_Fields' polygon shapefile "
                                     "(602 coal-field polygons, conterminous United States).",
        }
    finally:
        if owned:
            client.close()


def _quarantined_capture(staging, version_key: str, client: httpx.Client | None,
                         retained_source_version: str | None) -> dict:
    if isinstance(staging, PublishedStorageResource) or not staging.use_local or staging.bucket:
        raise ValueError("us_coal_fields_draft_requires_isolated_local_staging")
    try:
        if retained_source_version is not None:
            if client is not None:
                raise ValueError("retained_replay_does_not_accept_http_client")
            client = _RetainedCaptureClient(staging, retained_source_version)
        return _capture(staging, version_key, client, retained_source_version)
    except Exception as exc:
        error = str(exc).split(":", 1)[0]
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_package_url": GIS_ZIP_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(), "failure_code": error,
            "capture_module_sha256": _digest(Path(__file__).read_bytes()),
            "release_accepted": False,
        }))
        raise


def capture(staging, version_key: str, client: httpx.Client | None = None,
            retained_source_version: str | None = None) -> dict:
    """Baseline capture primitive: one complete pinned producer-package capture
    with fail-closed byte, package-structure, schema, geometry and value
    validation, retaining failed attempts as quarantine evidence."""
    return _quarantined_capture(staging, version_key, client, retained_source_version)


def capture_validated(staging, version_key: str, client: httpx.Client | None = None,
                      retained_source_version: str | None = None) -> dict:
    """Production capture entry: capture() plus a staged-bytes readback proving
    the pinned package and document digests of the candidate that was actually
    persisted. Retained replay replays the earlier capture's exact bodies."""
    result = _quarantined_capture(staging, version_key, client, retained_source_version)
    root = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
    for name, pin in _document_pins().items():
        _validate_pin(name, staging.read_key(root + f"raw/{name}"), pin)
    _extract_coal_fields_members(staging.read_key(root + "raw/GIS.zip"))
    return result
