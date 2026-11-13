"""Lossless, approval-gated capture of the USGS 2023 NSHM uniform-hazard ground
motion polygons for 5.0-second spectral acceleration, 5% in 50 years, NEHRP
site class B/C (VS30 = 760 m/s).

The U.S. Geological Survey Geologic Hazards Science Center publishes the 2023
U.S. 50-State National Seismic Hazard Model data release (DOI 10.5066/P9GNPCOD)
on ScienceBase. The catalog item
``04. Uniform-hazard ground motion maps for the conterminous U.S., Alaska, and
Hawaii`` (item ``64ff886dd34ed30c2057b4d9``) carries the hazard maps and
shapefiles computed on a 0.2 degree grid for the conterminous U.S. and Alaska
(0.02 degrees over Hawaii) for PGA and 0.2/1.0/5.0 second spectral
accelerations at 2%, 5% and 10% probability of exceedance in 50 years, for
NEHRP site classes B/C (VS30 = 760 m/s) and D (VS30 = 260 m/s). The FGDC
metadata in the item (``HazardMapsAndGISdata_2023.xml``) records publication
date 2023-12-28 (metadata date 2023-12-21) and states the data may not be used
for areas outside the United States.

This dataset is the single producer shapefile ``US_SA5P0_5Pct50Yrs_BC_poly``
inside ``US_SA5P0_2Pct_5Pct_10Pct_50Yrs_BC.zip`` ("5.0-second maps and
shapefiles for Vs30=760"): 5.0-second spectral acceleration, 5% in 50 years,
site class B/C, contour-band polygons. The sibling layers in the same archive
(2% and 10% polygons, all contour arcs, the .lyrx styles and map PDFs) and the
sibling archives for other periods/site classes are separate HIFLD inventory
files, not this dataset. The archive is fetched through the ScienceBase item
JSON API, which resolves the exact download URL by file name: the ``__disk__``
path in the URL is an opaque storage key that changes when the producer
re-uploads a file, so it is never hard-coded. The producer item reports an MD5
checksum and byte size for the archive; both are pinned here in addition to the
full SHA-256.

The shapefile has no native attribute feature identifier: each record is a
contour band between two contour values, with ``low_cont``, ``high_cont`` and
``range_cont`` describing the band, ``valley`` marking interior (``Y``) versus
edge (``N``) bands, and producer ``Shape_Leng``/``Shape_Area`` values. The
shapefile record number is storage order only and is preserved as row order;
no substitute published identifier is generated. The producer export contains
two no-data rows (``low_cont``/``high_cont``/``range_cont`` = ``-1000000``, one
with ``range_cont`` ``<NoData>``) and seven natively self-intersecting
polygons; both populations are pinned and preserved unchanged, never repaired,
and the release remains blocked until a maintainer accepts the contract. The
producer grid quantizes coordinates to 1/2048 degree, which leaves 99 vertices
at the antimeridian up to 1.14e-13 degrees beyond the nominal plus/minus 180
bound; they are preserved exactly as published.

The HIFLD Open inventory lists this dataset as
``uniform-hazard-ground-motion/us-sa5p0-5pct50yrs-bc-poly`` (agency: US
Geological Survey; publisher: U.S. Geological Survey - Geologic Hazards Science
Center). Its DataLumos project 240445 archive copy is HIFLD-rescue
identification context only and is never a pipeline input.
"""
from __future__ import annotations

import hashlib
import io
import json
import shutil
import stat
import struct
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely import is_valid_reason
from shapely.geometry import MultiPolygon, shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version,
    processing_identity, quarantine_incomplete_capture, seal_ready_candidate,
    verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "uniform-hazard-ground-motion"
FILE_SLUG = "us-sa5p0-5pct50yrs-bc-poly"
PUBLISHER = ("U.S. Geological Survey, Geologic Hazards Science Center "
             "(National Seismic Hazard Mapping Project)")
AGENCY = "U.S. Geological Survey"
ITEM_ID = "64ff886dd34ed30c2057b4d9"
SCIENCEBASE_HOST = "www.sciencebase.gov"
ITEM_URL = f"https://{SCIENCEBASE_HOST}/catalog/item/{ITEM_ID}?format=json"
LANDING_URL = f"https://{SCIENCEBASE_HOST}/catalog/item/{ITEM_ID}"
DOI_URL = "https://doi.org/10.5066/P9GNPCOD"
EXPECTED_ITEM_TITLE = ("04. Uniform-hazard ground motion maps for the conterminous U.S., "
                       "Alaska, and Hawaii")
RELEASE_DATE = "2023-12-28"
FGDC_METADATA_NAME = "HazardMapsAndGISdata_2023.xml"
FGDC_METADATA_DATE = "2023-12-21"
LICENSE = "https://www.usa.gov/publicdomain/label/1.0/"
ARCHIVE_NAME = "US_SA5P0_2Pct_5Pct_10Pct_50Yrs_BC.zip"
EXPECTED_ARCHIVE_FILE_TITLE = "5.0-second maps and shapefiles for Vs30=760"
ARCHIVE_BYTES = 30_410_865
ARCHIVE_SHA256 = "a62ff4bdb57526b363dd3c7c1d4e201761c68727ce51cc75ebf3d2be5d0e2f91"
ARCHIVE_MD5 = "ffa88187c2cec4f95be52e663da97a06"
ARCHIVE_UPLOADED_AT = "2023-12-14T04:27:50Z"
TARGET_STEM = "US_SA5P0_5Pct50Yrs_BC_poly"
TARGET_SUFFIXES = (".shp", ".shx", ".dbf", ".prj")
# Producer archive roster observed 2026-11-13 (member name, uncompressed bytes, sha256).
EXPECTED_ARCHIVE_MEMBERS = (
    ("SA5P0_2Pct.lyrx", 53762, "289a1224ede1c6985cba41c30ebe5db5c7e31aa49227a051cd3eb1a8a3c7cc17"),
    ("SA5P0_5Pct_10Pct.lyrx", 53822, "dc9d9dc2a505b7f3ef7eab4b01a94f8904c116340e346145b647e6a64dd65ef9"),
    ("US_2023_5sec_10PctIn50Yrs_Vs30_760.pdf", 2855297, "58a802375d6b3d1bf588a774dfe4c9e7bbf8c073d0918427f9e7b6e5e14a7a82"),
    ("US_2023_5sec_2PctIn50Yrs_Vs30_760.pdf", 3625155, "eceae487d684dfbf12173f79644e6d164335c354f86f4b52a5c0b54524fe804a"),
    ("US_2023_5sec_5PctIn50Yrs_Vs30_760.pdf", 3183709, "eab0db041111936725a89ace938635735f8fce36295a3747b5e7ca2ff9662e58"),
    ("US_SA5P0_10Pct50Yrs_BC_arc.dbf", 635354, "6805fb86e80780b491fd53d6b102e18b9c2da811fdacb932393a75ff2daecb9d"),
    ("US_SA5P0_10Pct50Yrs_BC_arc.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_10Pct50Yrs_BC_arc.shp", 3557984, "092b39a863e0cb5ac7da0818b5c3175c1c32d2bca58c510576c2ad016ed94d63"),
    ("US_SA5P0_10Pct50Yrs_BC_arc.shx", 17444, "5e1ee6d9d267afeb16256396ba2ab523d99403fabee7c8e8cdf56b9eb69c7bb9"),
    ("US_SA5P0_10Pct50Yrs_BC_poly.dbf", 1383331, "e7c549d6a917dbcfe167d2ec54fc11b6a95c8ef8bf63e39e0626ad6a13d09e4c"),
    ("US_SA5P0_10Pct50Yrs_BC_poly.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_10Pct50Yrs_BC_poly.shp", 5200656, "525b94ba478f7ae2d495b8f61599e11d570c30b25b8d4d417d329bde26c666c5"),
    ("US_SA5P0_10Pct50Yrs_BC_poly.shx", 10588, "cdd6c1d82cb4bdb7593f5327ef5e3d896ab6ab87d0de6795dc0498e9c3724f79"),
    ("US_SA5P0_2Pct50Yrs_BC_arc.dbf", 1523144, "2ec1246ce3802be9fae94a0047bd6142ee74b819271fca44482d233a8f8e33b3"),
    ("US_SA5P0_2Pct50Yrs_BC_arc.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_2Pct50Yrs_BC_arc.shp", 6321292, "b5a01ea4b5e055d306514f6bb30577d786bc5da58249f7afd30495949124f665"),
    ("US_SA5P0_2Pct50Yrs_BC_arc.shx", 41684, "02037c6566a622e1c12878884db4719b27e752dad846837406f86e4e75bcef30"),
    ("US_SA5P0_2Pct50Yrs_BC_poly.dbf", 3643141, "5fe46a3fd0ad6966000e362559aa84b9c44dcf845f2a19e1d9fe10bb882ab7f4"),
    ("US_SA5P0_2Pct50Yrs_BC_poly.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_2Pct50Yrs_BC_poly.shp", 10472912, "32a06c163790c9a6ee00abc5611c0f9a872b7c076d12822c0dc1f14740200168"),
    ("US_SA5P0_2Pct50Yrs_BC_poly.shx", 27724, "085984bc838780d87e0144d0de06a04cad1be105cb5ed097fe27cd96658988c5"),
    ("US_SA5P0_5Pct50Yrs_BC_arc.dbf", 1474213, "a9e996ef4c3e4c3c7ec49108f5ffa0c9c9ba125df030834fcc554df26019ab46"),
    ("US_SA5P0_5Pct50Yrs_BC_arc.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_5Pct50Yrs_BC_arc.shp", 4698588, "0209075b054df4e6d2a94dd787a801b7d5dfda3fbc47ff58251ea40f960ec44b"),
    ("US_SA5P0_5Pct50Yrs_BC_arc.shx", 40348, "2a6c5fec41914b8e3382e06dbf441dd5ba158976cdb756d7c9cef836d9e61d38"),
    ("US_SA5P0_5Pct50Yrs_BC_poly.dbf", 2908861, "8628d96177eaec0c16b9f81e0367fcff58a42232435680bd8f4aa8a2ebb026e4"),
    ("US_SA5P0_5Pct50Yrs_BC_poly.prj", 145, "a02a27b1d1982c8516d83398e85a3c8b1aef1713c13ef4d84d7bde17430c07c4"),
    ("US_SA5P0_5Pct50Yrs_BC_poly.shp", 7173076, "b91fe0453cc5ea2b3303a536eb7e7015fabc5b19d228203efe6d0a98127d1117"),
    ("US_SA5P0_5Pct50Yrs_BC_poly.shx", 22156, "e2d13ad8789eea70d1f0129b05af2f55e07800835a85812e7ae9c7d7ddf869aa"),
)
MAX_ITEM_JSON_BYTES = 4_000_000
MAX_ARCHIVE_BYTES = 48_000_000
MAX_EXPANDED_BYTES = 96_000_000
MIN_FREE_BYTES = 2_000_000_000
EXPECTED_COUNT = 2757
EXPECTED_FIELD_NAMES = ["low_cont", "high_cont", "range_cont", "valley",
                        "Shape_Leng", "Shape_Area"]
EXPECTED_FIELD_KINDS = {"low_cont": "str:254", "high_cont": "str:254",
                        "range_cont": "str:254", "valley": "str:254",
                        "Shape_Leng": "float:19.11", "Shape_Area": "float:19.11"}
EXPECTED_SCHEMA_GEOMETRY = "Polygon"
EXPECTED_CRS = "EPSG:4326"
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_MULTIPOLYGON_COUNT = 32
EXPECTED_VERTEX_TOTAL = 438_293
# Producer-published self-intersecting contour polygons (row = shapefile record
# number); preserved unchanged, never repaired.
EXPECTED_INVALID_GEOMETRY_ROWS = frozenset({100, 228, 484, 708, 1595, 2503, 2660})
EXPECTED_NODATA_ROW_COUNT = 2
EXPECTED_VALLEY_VALUES = frozenset({"Y", "N"})
EXPECTED_DISTINCT_CONTOUR_COMBOS = 274
EXPECTED_OBSERVED_BOUNDS = {"xmin": -179.99999999999997, "ymin": 18.700000000000045,
                            "xmax": 180.0000000000001, "ymax": 71.60012207000005}
# The producer grid rounds coordinates to 1/2048 degree; at the antimeridian
# that leaves a few vertices a rounding epsilon beyond the nominal bound.
NOMINAL_RANGE_TOLERANCE = 1e-9
EXPECTED_NOMINAL_RANGE_EXCEEDED_COUNT = 99
EXPECTED_SCHEMA_SHA256 = "2ce2a761aa3eda57a8248437eec95e58ec3254f97d7b43d90fca7154d6444706"
EXPECTED_RECORD_SHA256 = "210f9903f8d04be416e8a45b5258c2b1733bb5a3318b7f3ada30de974127700a"
EXPECTED_GEOMETRY_SHA256 = "8b19f331143dce990d566a51a1028d13b5575e70a60678c52c982a73226e2ed2"


class UniformHazardGroundMotionError(ValueError):
    """Unrecognized or drifted producer payload retained under quarantine."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


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


def _normalize(value: object) -> object:
    """Normalize fiona geometry nesting (tuples) into JSON-list structure."""
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    return value


def _require_local(storage: StagingStorageResource) -> None:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise UniformHazardGroundMotionError("explicit_local_staging_resource_required")


def _validate_item_document(document: object) -> str:
    """Validate the ScienceBase item document and resolve the archive URL."""
    if not isinstance(document, dict) or "error" in document:
        raise UniformHazardGroundMotionError("source_item_document_invalid")
    if document.get("id") != ITEM_ID or document.get("title") != EXPECTED_ITEM_TITLE:
        raise UniformHazardGroundMotionError("source_item_identity_drift")
    files = document.get("files")
    if not isinstance(files, list):
        raise UniformHazardGroundMotionError("source_item_file_roster_drift")
    matches = [entry for entry in files
               if isinstance(entry, dict) and entry.get("name") == ARCHIVE_NAME]
    if len(matches) != 1:
        raise UniformHazardGroundMotionError("source_item_file_roster_drift")
    entry = matches[0]
    if entry.get("title") != EXPECTED_ARCHIVE_FILE_TITLE or entry.get("contentType") != "application/zip":
        raise UniformHazardGroundMotionError("source_item_file_identity_drift")
    if entry.get("size") != ARCHIVE_BYTES:
        raise UniformHazardGroundMotionError("source_item_file_size_drift")
    checksum = entry.get("checksum")
    if not isinstance(checksum, dict) or checksum.get("type") != "MD5" \
            or checksum.get("value") != ARCHIVE_MD5:
        raise UniformHazardGroundMotionError("source_item_file_checksum_drift")
    url = entry.get("url")
    if not isinstance(url, str):
        raise UniformHazardGroundMotionError("source_item_file_url_missing")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or (parsed.hostname or "").casefold() != SCIENCEBASE_HOST \
            or not parsed.path.startswith(f"/catalog/file/get/{ITEM_ID}"):
        raise UniformHazardGroundMotionError("source_item_file_host_drift")
    return url


def _validate_archive(body: bytes) -> dict[str, bytes]:
    """Validate the pinned producer archive and return its decoded members."""
    if len(body) != ARCHIVE_BYTES or _sha(body) != ARCHIVE_SHA256 or not body.startswith(b"PK\x03\x04"):
        raise UniformHazardGroundMotionError("changed_truncated_or_unknown_producer_archive")
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise UniformHazardGroundMotionError("duplicate_archive_member")
            expected = {name: (size, sha) for name, size, sha in EXPECTED_ARCHIVE_MEMBERS}
            if set(names) != set(expected):
                raise UniformHazardGroundMotionError("unexpected_or_missing_archive_member")
            for info in infos:
                if PurePosixPath(info.filename).name != info.filename or info.flag_bits & 1 \
                        or stat.S_ISLNK(info.external_attr >> 16):
                    raise UniformHazardGroundMotionError("unsafe_encrypted_or_symlink_archive_member")
            if sum(info.file_size for info in infos) > MAX_EXPANDED_BYTES:
                raise UniformHazardGroundMotionError("expanded_archive_size_limit")
            members = {}
            for name, (size, sha) in sorted(expected.items()):
                data = archive.read(name)
                if len(data) != size or _sha(data) != sha:
                    raise UniformHazardGroundMotionError("archive_member_digest_drift:" + name)
                members[name] = data
            if archive.testzip() is not None:
                raise UniformHazardGroundMotionError("archive_member_crc_invalid")
    except zipfile.BadZipFile as exc:
        raise UniformHazardGroundMotionError("archive_container_invalid") from exc
    return members


def _decode_shapefile(members: dict[str, bytes], directory: Path) -> tuple[list[dict], list[list], dict]:
    """Decode and validate the pinned target shapefile; return canonical records.

    The shapefile is written out only inside the caller's scratch directory; the
    staged candidate retains the original archive bytes, never extracted copies.
    """
    missing = [TARGET_STEM + suffix for suffix in TARGET_SUFFIXES
               if TARGET_STEM + suffix not in members]
    if missing:
        raise UniformHazardGroundMotionError("target_shapefile_members_missing:" + ",".join(missing))
    dbf = members[TARGET_STEM + ".dbf"]
    if len(dbf) < 32 or dbf[:1] not in (b"\x03", b"\x04"):
        raise UniformHazardGroundMotionError("target_dbf_header_invalid")
    dbf_record_count = struct.unpack_from("<I", dbf, 4)[0]
    if dbf_record_count != EXPECTED_COUNT:
        raise UniformHazardGroundMotionError("target_dbf_population_drift")
    for suffix in TARGET_SUFFIXES:
        (directory / (TARGET_STEM + suffix)).write_bytes(members[TARGET_STEM + suffix])
    with fiona.open(directory / f"{TARGET_STEM}.shp") as source:
        schema = dict(source.schema)
        crs = source.crs
        if (list(schema.get("properties", {})) != EXPECTED_FIELD_NAMES
                or schema.get("properties") != EXPECTED_FIELD_KINDS
                or schema.get("geometry") != EXPECTED_SCHEMA_GEOMETRY):
            raise UniformHazardGroundMotionError("source_schema_contract_drift")
        if not pyproj.CRS(crs).equals(pyproj.CRS.from_epsg(4326)):
            raise UniformHazardGroundMotionError("source_crs_drift")
        rows = list(source)
    if len(rows) != EXPECTED_COUNT:
        raise UniformHazardGroundMotionError("source_feature_count_drift")
    records, geometry_records, stats = _classify_rows(rows)
    observed_bounds = stats["observed_bounds"]
    if observed_bounds != EXPECTED_OBSERVED_BOUNDS:
        raise UniformHazardGroundMotionError("source_observed_coverage_drift")
    if stats["null_geometry"] != EXPECTED_NULL_GEOMETRY_COUNT:
        raise UniformHazardGroundMotionError("source_null_geometry_population_drift")
    if stats["multipolygon_count"] != EXPECTED_MULTIPOLYGON_COUNT:
        raise UniformHazardGroundMotionError("source_geometry_population_drift")
    if stats["vertex_total"] != EXPECTED_VERTEX_TOTAL:
        raise UniformHazardGroundMotionError("source_vertex_total_drift")
    if stats["nominal_range_exceeded"] != EXPECTED_NOMINAL_RANGE_EXCEEDED_COUNT:
        raise UniformHazardGroundMotionError("source_nominal_range_excess_population_drift")
    if stats["invalid_rows"] != set(EXPECTED_INVALID_GEOMETRY_ROWS):
        raise UniformHazardGroundMotionError("source_invalid_geometry_population_drift")
    if stats["nodata_rows"] != EXPECTED_NODATA_ROW_COUNT:
        raise UniformHazardGroundMotionError("source_nodata_population_drift")
    if stats["valley_values"] != set(EXPECTED_VALLEY_VALUES):
        raise UniformHazardGroundMotionError("source_valley_value_drift")
    if len(stats["contour_combos"]) != EXPECTED_DISTINCT_CONTOUR_COMBOS:
        raise UniformHazardGroundMotionError("source_contour_combo_population_drift")
    record_sha = _list_sha(records)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise UniformHazardGroundMotionError("source_record_value_drift")
    geometry_sha = _list_sha(geometry_records)
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise UniformHazardGroundMotionError("source_geometry_or_coverage_drift")
    schema_sha = _sha(_canonical(schema))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise UniformHazardGroundMotionError("source_schema_contract_drift")
    invalid_reasons = {}
    for index, row in enumerate(rows):
        if index in stats["invalid_rows"]:
            invalid_reasons[index] = is_valid_reason(shape(row["geometry"]))
    audit = {
        "source_feature_count": len(records),
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha,
        "source_crs": EXPECTED_CRS,
        "source_geometry_declared_type": EXPECTED_SCHEMA_GEOMETRY,
        "source_polygon_count": len(records) - EXPECTED_MULTIPOLYGON_COUNT,
        "source_multipolygon_count": EXPECTED_MULTIPOLYGON_COUNT,
        "source_vertex_total": EXPECTED_VERTEX_TOTAL,
        "source_observed_bounds": observed_bounds,
        "source_null_geometry_count": stats["null_geometry"],
        "source_invalid_geometry_rows": sorted(stats["invalid_rows"]),
        "source_invalid_geometry_reasons": invalid_reasons,
        "source_nodata_row_count": stats["nodata_rows"],
        "source_valley_values": sorted(stats["valley_values"]),
        "source_distinct_contour_combos": len(stats["contour_combos"]),
        "source_nominal_range_exceeded_vertex_count": stats["nominal_range_exceeded"],
        "source_release_date": RELEASE_DATE,
        "source_release_date_evidence": (
            f"FGDC metadata {FGDC_METADATA_NAME} in ScienceBase item {ITEM_ID} records "
            f"pubdate {RELEASE_DATE} (metadata date {FGDC_METADATA_DATE}); the ScienceBase "
            f"item reports the archive upload timestamp {ARCHIVE_UPLOADED_AT}."),
    }
    return records, geometry_records, audit


def _validate_rings(rings: list) -> None:
    if not isinstance(rings, list) or not rings:
        raise UniformHazardGroundMotionError("source_geometry_structure_drift")
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise UniformHazardGroundMotionError("source_ring_structure_drift")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2 \
                    or type(vertex[0]) not in (int, float) or type(vertex[1]) not in (int, float):
                raise UniformHazardGroundMotionError("source_vertex_structure_drift")
            if vertex[0] != vertex[0] or vertex[1] != vertex[1]:
                raise UniformHazardGroundMotionError("source_nonfinite_vertex_drift")


def _polygon_wkb(geometry_value: dict) -> bytes:
    if geometry_value["type"] == "Polygon":
        return shape(geometry_value).wkb
    parts = [shape({"type": "Polygon", "coordinates": part})
             for part in geometry_value["coordinates"]]
    return MultiPolygon(parts).wkb


def _classify_rows(rows: list) -> tuple[list[dict], list[list], dict]:
    """Decode fiona rows into canonical records plus population statistics.

    Pure with respect to the pin checks: every structural error still fails
    closed here, while population/digest comparisons stay in the caller.
    """
    records: list[dict] = []
    geometry_records: list[list] = []
    null_geometry = 0
    multipolygon_count = 0
    vertex_total = 0
    nominal_range_exceeded = 0
    invalid_rows: set[int] = set()
    nodata_rows = 0
    contour_combos: set[tuple] = set()
    valley_values: set[str] = set()
    xmin = ymin = xmax = ymax = None
    for index, row in enumerate(rows):
        properties = dict(row["properties"])
        if list(properties) != EXPECTED_FIELD_NAMES:
            raise UniformHazardGroundMotionError("source_field_roster_drift")
        for name in EXPECTED_FIELD_NAMES:
            value = properties[name]
            if value is None:
                raise UniformHazardGroundMotionError(f"source_attribute_null_drift:{name}")
            expected_type = str if name in ("low_cont", "high_cont", "range_cont", "valley") else float
            if type(value) is not expected_type:
                raise UniformHazardGroundMotionError(f"source_attribute_type_drift:{name}")
            if name == "valley":
                valley_values.add(value)
        combo = tuple(properties[name] for name in EXPECTED_FIELD_NAMES[:4])
        if properties["low_cont"] == "-1000000":
            nodata_rows += 1
        contour_combos.add(combo)
        geometry = row["geometry"]
        if geometry is None:
            null_geometry += 1
            records.append({**properties, "geometry": None})
            geometry_records.append([index, None])
            continue
        geometry = dict(geometry)
        if geometry.get("type") not in ("Polygon", "MultiPolygon"):
            raise UniformHazardGroundMotionError("source_geometry_type_drift")
        rings = _normalize(geometry["coordinates"])
        parts = [rings] if geometry["type"] == "Polygon" else rings
        if geometry["type"] == "MultiPolygon":
            multipolygon_count += 1
            for part in parts:
                if not isinstance(part, list) or not part:
                    raise UniformHazardGroundMotionError("source_geometry_structure_drift")
                _validate_rings(part)
        else:
            _validate_rings(rings)
        for part in parts:
            for ring in part:
                for vertex in ring:
                    x, y = vertex
                    vertex_total += 1
                    if not (-180.0 - NOMINAL_RANGE_TOLERANCE <= x <= 180.0 + NOMINAL_RANGE_TOLERANCE
                            and -90.0 - NOMINAL_RANGE_TOLERANCE <= y <= 90.0 + NOMINAL_RANGE_TOLERANCE):
                        raise UniformHazardGroundMotionError("source_coordinate_range_drift")
                    if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
                        nominal_range_exceeded += 1
                    if xmin is None or x < xmin:
                        xmin = x
                    if xmax is None or x > xmax:
                        xmax = x
                    if ymin is None or y < ymin:
                        ymin = y
                    if ymax is None or y > ymax:
                        ymax = y
        geometry_value = {"type": geometry["type"], "coordinates": rings}
        polygon = shape(geometry_value)
        if polygon.is_empty:
            raise UniformHazardGroundMotionError("source_empty_geometry_drift")
        if not polygon.is_valid:
            invalid_rows.add(index)
        records.append({**properties, "geometry": geometry_value})
        geometry_records.append([index, geometry_value])
    stats = {"null_geometry": null_geometry, "multipolygon_count": multipolygon_count,
             "vertex_total": vertex_total, "nominal_range_exceeded": nominal_range_exceeded,
             "invalid_rows": invalid_rows, "nodata_rows": nodata_rows,
             "contour_combos": contour_combos, "valley_values": valley_values,
             "observed_bounds": None if xmin is None else
             {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}}
    return records, geometry_records, stats


def _parquet_bytes(records: list[dict]) -> bytes:
    columns = {}
    schema_fields = []
    for name in EXPECTED_FIELD_NAMES:
        values = [row[name] for row in records]
        typ = pa.string() if name in ("low_cont", "high_cont", "range_cont", "valley") else pa.float64()
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(name, typ, nullable=True))
    columns["geometry"] = pa.array([
        None if row["geometry"] is None else _polygon_wkb(row["geometry"]) for row in records
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"],
        "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar",
        "bbox": [EXPECTED_OBSERVED_BOUNDS["xmin"], EXPECTED_OBSERVED_BOUNDS["ymin"],
                 EXPECTED_OBSERVED_BOUNDS["xmax"], EXPECTED_OBSERVED_BOUNDS["ymax"]]}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata(
        {b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def capture_uniform_hazard_ground_motion(storage: StagingStorageResource, *,
                                         capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved uniform-hazard candidate."""
    _require_local(storage)
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise UniformHazardGroundMotionError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise UniformHazardGroundMotionError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path(
        "/tmp/hifld-native-reconstruction/350/captures")
    capture_root = (attempt_root / f"{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise UniformHazardGroundMotionError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/usgs-uniform-hazard-ground-motion-draft"}) as client:
            item_body, item_receipt = capture_get(client, ITEM_URL, capture_root, "item_document",
                                                  max_bytes=MAX_ITEM_JSON_BYTES)
            captures["item_document"] = (item_body, item_receipt)
            try:
                item_document = json.loads(item_body)
            except ValueError as exc:
                raise UniformHazardGroundMotionError("source_item_document_invalid") from exc
            archive_url = _validate_item_document(item_document)
            archive_body, archive_receipt = capture_get(client, archive_url, capture_root, "archive",
                                                        max_bytes=MAX_ARCHIVE_BYTES)
            captures["archive"] = (archive_body, archive_receipt)
    except UniformHazardGroundMotionError:
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, "draft", captures,
                                      "uniform_hazard_ground_motion_capture_rejected")
        raise
    except ValueError as exc:
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, "draft", captures,
                                      type(exc).__name__)
        raise UniformHazardGroundMotionError(f"incomplete_source_capture:{exc}") from exc
    try:
        members = _validate_archive(archive_body)
        with tempfile.TemporaryDirectory(prefix="uniform-hazard-ground-motion-") as tmp:
            records, _, audit = _decode_shapefile(members, Path(tmp))
    except (UniformHazardGroundMotionError, zipfile.BadZipFile, OSError, ValueError) as exc:
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, "draft", captures,
                                      type(exc).__name__)
        raise UniformHazardGroundMotionError(str(exc)) from exc
    identity = _code_identity()
    parquet_body = _parquet_bytes(records)
    rel = f"geoparquet/{FILE_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    item_document = json.loads(item_body)
    archive_entry = [entry for entry in item_document["files"] if entry.get("name") == ARCHIVE_NAME][0]
    vintage_evidence = {
        "sciencebase_item_title": EXPECTED_ITEM_TITLE,
        "sciencebase_item_id": ITEM_ID,
        "data_release_doi": DOI_URL,
        "fgdc_metadata_name": FGDC_METADATA_NAME,
        "fgdc_metadata_date": FGDC_METADATA_DATE,
        "fgdc_publication_date": RELEASE_DATE,
        "archive_uploaded_at": ARCHIVE_UPLOADED_AT,
        "archive_file_title": EXPECTED_ARCHIVE_FILE_TITLE,
        "archive_checksum_md5": ARCHIVE_MD5,
        "item_provenance_last_updated": (item_document.get("provenance") or {}).get("lastUpdated"),
        "assessment": (
            "USGS publishes the 2023 50-State NSHM data release on ScienceBase; the FGDC "
            "metadata publication date is the source release date (2023-12-28). The ScienceBase "
            "item upload timestamp and item-edit provenance are separate packaging vintage "
            "evidence."),
    }
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "US SA5P0 5Pct50Yrs BC poly (USGS 2023 NSHM uniform-hazard ground motion)",
        "description": "USGS 2023 U.S. 50-State National Seismic Hazard Model uniform-hazard "
                       "ground motion polygons: 5.0-second spectral acceleration, 5% probability "
                       "of exceedance in 50 years, NEHRP site class B/C (VS30 = 760 m/s), "
                       "captured losslessly from the producer shapefile "
                       "US_SA5P0_5Pct50Yrs_BC_poly inside the ScienceBase archive "
                       f"{ARCHIVE_NAME} (item {ITEM_ID}, DOI 10.5066/P9GNPCOD). All source "
                       "columns, values, band polygons, the two no-data rows and the seven "
                       "natively invalid polygons are preserved unchanged. Source release "
                       "freshness acceptance, schema/ID contract, rights and release acceptance "
                       "remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LANDING_URL,
        "license": LICENSE,
        "source_modified": ARCHIVE_UPLOADED_AT, "date_issued": RELEASE_DATE,
        "date_modified": ARCHIVE_UPLOADED_AT,
        "source_vintage_evidence": vintage_evidence,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": "sciencebase-archive:" + ARCHIVE_SHA256, "producer": PUBLISHER,
                    "host": SCIENCEBASE_HOST, "role": "producer_publication",
                    "canonical_url": LANDING_URL, "retrieval_url": archive_url,
                    "observed_final_url": archive_receipt["final_url"],
                    "retrieved_at": archive_receipt["observed_at"],
                    "http_status": archive_receipt["status"],
                    "bytes": archive_receipt["bytes"], "sha256": archive_receipt["sha256"],
                    "release_date": RELEASE_DATE,
                    "release_date_evidence": (
                        f"FGDC metadata {FGDC_METADATA_NAME} pubdate {RELEASE_DATE}; the archive "
                        f"itself was uploaded {ARCHIVE_UPLOADED_AT}"),
                    "license": LICENSE,
                    "attribution": ("Petersen, M.D., et al., 2023, Data Release for the 2023 U.S. "
                                    "50-State National Seismic Hazard Model - Overview: U.S. "
                                    "Geological Survey data release, "
                                    "https://doi.org/10.5066/P9GNPCOD")},
                   {"input_id": "sciencebase-item-document:" + _sha(item_body), "producer": PUBLISHER,
                    "host": SCIENCEBASE_HOST, "role": "producer_publication",
                    "canonical_url": LANDING_URL, "retrieval_url": ITEM_URL,
                    "observed_final_url": item_receipt["final_url"],
                    "retrieved_at": item_receipt["observed_at"],
                    "http_status": item_receipt["status"],
                    "bytes": item_receipt["bytes"], "sha256": item_receipt["sha256"],
                    "release_date": RELEASE_DATE,
                    "release_date_evidence": "Same FGDC-pinned source release as the archive",
                    "license": LICENSE,
                    "attribution": "ScienceBase catalog item document used only to resolve the "
                                   "producer archive URL by exact file name"}],
        "transformations": [{"operation": "producer_shapefile_to_geoparquet_candidate",
                             "parameters": {"archive_member": TARGET_STEM,
                                            "source_fields": "all six source columns retained in "
                                                             "producer order and physical types "
                                                             "(low_cont, high_cont, range_cont, "
                                                             "valley, Shape_Leng, Shape_Area)",
                                            "source_ids": "No native attribute identifier exists in "
                                                          "the producer shapefile; record order is "
                                                          "preserved; no substitute published ID is "
                                                          "generated",
                                            "geometry": "Source EPSG:4326 polygon rings retained "
                                                        "as WKB without reprojection, vertex "
                                                        "removal, or repair; the two no-data rows "
                                                        "and the seven natively invalid polygons "
                                                        "are preserved unchanged",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending release acceptance, "
                                                                "schema/ID contract, and rights "
                                                                "review"}}],
        "lineage": "The U.S. Geological Survey Geologic Hazards Science Center computes and "
                   "publishes the National Seismic Hazard Model; the 2023 U.S. 50-State NSHM data "
                   "release (DOI 10.5066/P9GNPCOD) distributes uniform-hazard ground motion maps "
                   "and shapefiles through its ScienceBase item "
                   f"{LANDING_URL}. The HIFLD Open inventory lists this dataset as "
                   "uniform-hazard-ground-motion/us-sa5p0-5pct50yrs-bc-poly (agency US Geological "
                   "Survey, publisher U.S. Geological Survey - Geologic Hazards Science Center); "
                   "its DataLumos project 240445 archive copy is HIFLD-rescue identification "
                   "context only and is never a pipeline input.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source columns, contour-band values, physical types, and "
                      "polygon geometry retained in the producer CRS; all release and "
                      "compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    full_audit = {**audit, "source_archive": {"name": ARCHIVE_NAME, "bytes": ARCHIVE_BYTES,
                                              "sha256": ARCHIVE_SHA256, "md5": ARCHIVE_MD5,
                                              "uploaded_at": ARCHIVE_UPLOADED_AT},
                  "source_item_id": ITEM_ID, "source_item_url": LANDING_URL,
                  "source_relationship": "USGS GHSC producer publication (2023 50-State NSHM data release)",
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(full_audit) + b"\n",
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
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise UniformHazardGroundMotionError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT,
                "source_null_geometry_count": audit["source_null_geometry_count"],
                "source_invalid_geometry_count": len(audit["source_invalid_geometry_rows"]),
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise UniformHazardGroundMotionError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": audit["source_null_geometry_count"],
            "source_invalid_geometry_count": len(audit["source_invalid_geometry_rows"]),
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}
