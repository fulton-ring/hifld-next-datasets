"""USGS National Atlas Miscellaneous Industrial Minerals Operations source record and refusal.

The assigned producer publication ``minop5x020_nt00022.tar.gz`` (December 2005 USGS
National Atlas release built from the Minerals Information Team's 2003 Automated
Minerals Information System survey) is retired from its only official distribution
host: ``dds.cr.usgs.gov`` returns 404, and the live USGS NGTOC legacy FGDC XML for
the layer links only to that retired URL (dated probes 2026-11-10). The producer
currently serves the same 2003 AMIS survey only through the different combined
"Active Mines and Mineral Processing Plants" product on mrdata.usgs.gov, which
changes the record identity (rec_no versus the original MINOP5X020 roster), splits
multi-commodity sites, and exports three records with truncated site names, rounded
coordinates, or abbreviated state text. Third-party snapshots of the retired
publication (including the Internet Archive copy previously captured here) are
research/comparison context only and are never pipeline inputs.

Until a maintainer accepts an official distribution of the retired publication, or
approves a documented successor scope for the live mrdata product, this module
refuses candidate construction: the executable ingest fails closed with
``national_atlas_publication_retired_no_official_distribution``. The byte pins,
native DBF/SHP/SHX decoders and metadata validators below record the producer
contract that any accepted distribution must reproduce; focused tests exercise
them with synthetic fixtures and no network or archive access is performed.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import struct
import tarfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb, is_empty, is_valid, total_bounds

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

DATASET_SLUG = FILE_SLUG = "miscellaneous-industrial-mineral-operations"
PUBLISHER = "U.S. Geological Survey National Atlas of the United States"
# The producer's own declared publication URL. Retired: the host returns 404 and
# the live USGS NGTOC FGDC metadata for this layer links only to this URL.
# It is recorded as the canonical producer citation, never fetched.
ORIGINAL_PACKAGE_URL = "https://dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz"
# Live USGS-hosted FGDC metadata document for the retired layer (producer
# reference evidence; not a data distribution).
LEGACY_METADATA_URL = "https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop5x020.xml"
COUNT = 304
EXPECTED_PACKAGE_BYTES = 23_380
EXPECTED_PACKAGE_SHA256 = "8b7458200c3e8115a99f6edca4438e91844456a8ee0467d34c1a7749f2ea082c"
EXPECTED_METADATA_BYTES = 20_018
EXPECTED_METADATA_SHA256 = "1ff2b2c8910aca0fe86e3241c0c3c3e1c45a24c396c4401c074c110a98b7c9ee"
MEMBER_NAMES = ("minop5x020.dbf", "minop5x020.shp", "minop5x020.shx", "minop5x020.txt")
MEMBER_PINS = {
    "minop5x020.dbf": {"bytes": 59_602, "sha256": "59903827e8df547f5e0e174b0d1412c48a3c1895894dc2b93c3fc11b272690b0"},
    "minop5x020.shp": {"bytes": 8_612, "sha256": "1b1099c461a7471c901cae043900f2cb84b72e666bace6adb06de2dd03e36871"},
    "minop5x020.shx": {"bytes": 2_532, "sha256": "40f271bbab085700c77cd842142167cd47e394bdd4be5e9491d3caf06e80012d"},
    "minop5x020.txt": {"bytes": 19_691, "sha256": "d9357340cc6de4b4a36c971e3ddcc495589552df8548812298c5cb8b0c7ebe21"},
}
SHAPEFILE_MEMBERS = ("minop5x020.dbf", "minop5x020.shp", "minop5x020.shx")
DBF_FIELDS = [
    ("MINOP5X020", "N", 3, 0),
    ("COMMODITY", "C", 24, 0),
    ("SITE_NAME", "C", 57, 0),
    ("COMPANY_NA", "C", 39, 0),
    ("STATE_LOCA", "C", 14, 0),
    ("COUNTY", "C", 16, 0),
    ("LATITUDE", "N", 15, 6),
    ("LONGITUDE", "N", 16, 6),
    ("PLANT_MIN", "C", 10, 0),
]
DECLARED_NAMES = [field[0] for field in DBF_FIELDS]
ARROW_TYPES = {
    "MINOP5X020": pa.decimal128(3, 0),
    "COMMODITY": pa.string(),
    "SITE_NAME": pa.string(),
    "COMPANY_NA": pa.string(),
    "STATE_LOCA": pa.string(),
    "COUNTY": pa.string(),
    "LATITUDE": pa.decimal128(15, 6),
    "LONGITUDE": pa.decimal128(16, 6),
    "PLANT_MIN": pa.string(),
}
IDENTITY_COLUMN_METADATA = {
    "source_fid": b"Original zero-based DBF physical record index, not a published or replacement feature ID",
    "source_shp_record_number": b"Original SHP record-header number, not a substitute source identifier",
}
# The producer's own identifier roster, decoded from the pinned package bytes:
# 304 unique MINOP5X020 internal feature numbers spanning 1..305 with exactly
# one producer gap at 187 (no duplicates). The physical DBF order is strictly
# increasing across this roster. This is the observed producer fact, not a
# synthetic renumbering, and no substitute row ID is ever generated.
EXPECTED_IDS = tuple(value for value in range(1, 306) if value != 187)
# The bundled FGDC text metadata carries one Windows-1252 right single quote
# byte (0x92 in "fuller\x92s earth"). Marker validation decodes the pinned
# bytes as cp1252; producer bytes are never rewritten.
METADATA_TEXT_ENCODING = "cp1252"
EXPECTED_TXT_MARKERS = (
    "Title: Miscellaneous Industrial Minerals Operations",
    "Originator: U.S. Geological Survey",
    "Publication_Date: 200512",
    "Source_Citation_Abbreviation: AMIS",
    "Horizontal_Datum_Name: North American Datum of 1983",
    "Latitude_Resolution: 0.000278",
    "Longitude_Resolution: 0.000278",
)
EXPECTED_XML_MARKERS = (
    "<title>Miscellaneous Industrial Minerals Operations - Direct Download</title>",
    "<pubdate>200512</pubdate>",
    "<onlink>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz</onlink>",
    "<geogunit>Decimal degrees</geogunit>",
    "<horizdn>North American Datum of 1983</horizdn>",
    "<ellips>GRS1980</ellips>",
    "<semiaxis>6378137</semiaxis>",
    "<denflat>298.257222</denflat>",
)
NATIVE_CRS_DECLARATION = (
    "The package ships no .prj sidecar; the pinned USGS FGDC XML for the "
    "Miscellaneous Industrial Minerals Operations layer declares Decimal degrees, "
    "North American Datum of 1983, GRS1980, semiaxis 6378137 m, and inverse flattening "
    "298.257222. These producer parameters identify native EPSG:4269. Coordinates "
    "are retained unchanged with no reprojection or synthesized PRJ."
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _json_safe_row(row: dict[str, object]) -> dict[str, object]:
    return {key: (str(value) if isinstance(value, Decimal) else value)
            for key, value in row.items()}


def _extract_package(body: bytes) -> dict[str, bytes]:
    """Return the exact producer master members of the National Atlas tar.gz."""
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
            members = archive.getmembers()
            names = tuple(member.name for member in members if member.isfile())
            if sorted(names) != sorted(MEMBER_NAMES) or any(member.size <= 0 for member in members):
                raise ValueError("package_member_roster_drift")
            extracted = {}
            for name in MEMBER_NAMES:
                member = archive.getmember(name)
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError("package_member_unreadable:" + name)
                extracted[name] = stream.read()
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise ValueError("package_tar_gz_unreadable") from exc
    for name, pin in MEMBER_PINS.items():
        if len(extracted[name]) != pin["bytes"] or _sha(extracted[name]) != pin["sha256"]:
            raise ValueError("pinned_package_member_drift:" + name)
    return extracted


def _decode_dbf(body: bytes) -> list[dict[str, object]]:
    if len(body) < 321:
        raise ValueError("incomplete_dbf")
    count, header, width = struct.unpack_from("<IHH", body, 4)
    fields = []
    offset = 32
    while offset + 32 <= header and body[offset] != 13:
        desc = body[offset:offset + 32]
        fields.append((desc[:11].split(b"\0")[0].decode("ascii"), chr(desc[11]),
                       desc[16], desc[17]))
        offset += 32
    if fields != DBF_FIELDS or count != COUNT or width != 195 or header != 321:
        raise ValueError("missing_renamed_extra_ID_type_width_count_or_DBF_field_drift")
    if body[offset] != 13 or len(body) not in (header + count * width, header + count * width + 1):
        raise ValueError("incomplete_or_extra_DBF_bytes")
    if len(body) == header + count * width + 1 and body[-1] != 26:
        raise ValueError("unexpected_dbf_tail")
    rows = []
    for fid in range(count):
        record = body[header + fid * width:header + (fid + 1) * width]
        if record[:1] != b" ":
            raise ValueError("deleted_or_unknown_dbf_record")
        offset = 1
        row: dict[str, object] = {"source_fid": fid, "source_shp_record_number": fid + 1}
        for name, kind, size, scale in fields:
            raw = record[offset:offset + size]
            offset += size
            if kind == "C":
                if any(char > 127 or char < 32 for char in raw):
                    raise ValueError("undeclared_nonASCII_source_encoding:" + name)
                row[name] = raw.rstrip(b" ").decode("ascii")
            else:
                text = raw.decode("ascii").strip()
                if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
                    raise ValueError("null_nonfinite_or_unparseable_DBF_number:" + name)
                value = Decimal(text)
                if value.as_tuple().exponent != -scale:
                    raise ValueError("declared_DBF_scale_drift:" + name)
                row[name] = value
        rows.append(row)
    if [int(row["MINOP5X020"]) for row in rows] != [int(value) for value in EXPECTED_IDS]:
        raise ValueError("missing_duplicate_reordered_or_gapped_original_MINOP5X020")
    return rows


def _decode_points(shp: bytes, shx: bytes) -> list[bytes]:
    if len(shp) != 100 + 28 * COUNT or len(shx) != 100 + 8 * COUNT:
        raise ValueError("incomplete_SHP_SHX")
    for data in (shp, shx):
        if (struct.unpack_from(">i", data)[0] != 9994
                or struct.unpack_from(">i", data, 24)[0] * 2 != len(data)
                or struct.unpack_from("<ii", data, 28) != (1000, 1)):
            raise ValueError("native_geometry_header_type_or_length_drift")
    if struct.unpack_from("<i", shp, 32)[0] != 1:
        raise ValueError("native_geometry_type_drift")
    out = []
    offset = 100
    for fid in range(COUNT):
        if offset + 28 > len(shp):
            raise ValueError("incomplete_point_records")
        record, words = struct.unpack_from(">ii", shp, offset)
        shape_type, x, y = struct.unpack_from("<idd", shp, offset + 8)
        if (record != fid + 1 or words != 10 or shape_type != 1
                or struct.unpack_from(">ii", shx, 100 + 8 * fid) != (offset // 2, 10)
                or not all(math.isfinite(value) for value in (x, y))):
            raise ValueError("SHP_SHX_record_ID_offset_type_or_coordinate_drift")
        out.append(struct.pack("<BIdd", 1, 1, x, y))
        offset += 28
    if offset != len(shp):
        raise ValueError("extra_point_records")
    geometry = from_wkb(out)
    if not all(is_valid(geometry)) or any(is_empty(geometry)):
        raise ValueError("invalid_empty_native_point")
    if list(struct.unpack_from("<4d", shp, 36)) != total_bounds(geometry).tolist():
        raise ValueError("native_bounds_drift")
    if shx[36:68] != shp[36:68]:
        raise ValueError("native_bounds_index_drift")
    return out


def _validate_metadata_text(body: bytes) -> dict[str, object]:
    non_ascii = sorted({byte for byte in body if byte >= 128})
    if non_ascii != [0x92]:
        raise ValueError("producer_metadata_text_encoding_drift:" + repr(non_ascii))
    text = body.decode(METADATA_TEXT_ENCODING)
    for marker in EXPECTED_TXT_MARKERS:
        if marker not in text:
            raise ValueError("producer_metadata_marker_drift:" + marker.split(":")[0])
    return {"document_format": "FGDC text metadata bundled in the producer package",
            "declared_title": "Miscellaneous Industrial Minerals Operations",
            "declared_originator": "U.S. Geological Survey",
            "declared_publication_date": "200512",
            "declared_source": "Automated Minerals Information System (AMIS) 2003 operations, "
                               "USGS Minerals Yearbook Volume 1 (2004)",
            "declared_horizontal_datum": "North American Datum of 1983",
            "declared_coordinate_resolution": "0.000278 degree (one arc-second)",
            "declared_online_linkage": ORIGINAL_PACKAGE_URL,
            "text_encoding_note": ("one Windows-1252 right single quote byte (0x92) is present in "
                                   "the producer text; bytes are retained unchanged")}


def _validate_metadata_xml(body: bytes) -> dict[str, object]:
    text = body.decode("ascii")
    for marker in EXPECTED_XML_MARKERS:
        if marker not in text:
            raise ValueError("producer_metadata_marker_drift:" + marker.split('"')[0][:60])
    if not all(marker in text for marker in EXPECTED_XML_MARKERS[3:]):
        raise ValueError("producer_metadata_crs_declaration_drift")
    return {"document_format": "FGDC XML metadata served live by USGS",
            "declared_title": "Miscellaneous Industrial Minerals Operations - Direct Download",
            "declared_publication_date": "200512",
            "declared_online_linkage": ORIGINAL_PACKAGE_URL,
            "declared_coordinate_system": "geographic",
            "declared_coordinate_units": "Decimal degrees",
            "declared_horizontal_datum": "North American Datum of 1983",
            "declared_ellipsoid": "GRS1980",
            "declared_semimajor_axis_m": 6378137,
            "declared_inverse_flattening": 298.257222,
            "native_epsg": 4269}


def _validate_population(rows: list[dict[str, object]], wkbs: list[bytes]) -> tuple[list[dict[str, object]], dict]:
    if len(rows) != COUNT or len(wkbs) != COUNT:
        raise ValueError("source_population_incomplete")
    if [int(row["MINOP5X020"]) for row in rows] != [int(value) for value in EXPECTED_IDS]:
        raise ValueError("missing_duplicate_reordered_or_gapped_original_MINOP5X020")
    if [int(row["MINOP5X020"]) for row in rows] != sorted(int(row["MINOP5X020"]) for row in rows):
        raise ValueError("source_cross_file_record_order_drift")
    geometries = from_wkb(wkbs)
    max_delta = 0.0
    for row, geometry in zip(rows, geometries):
        latitude, longitude = Decimal(str(row["LATITUDE"])), Decimal(str(row["LONGITUDE"]))
        max_delta = max(max_delta, abs(float(latitude) - geometry.y), abs(float(longitude) - geometry.x))
    plant_min_counts: dict[str, int] = {}
    for row in rows:
        value = str(row["PLANT_MIN"])
        plant_min_counts[value] = plant_min_counts.get(value, 0) + 1
    features = []
    for row, wkb in zip(rows, wkbs):
        feature = dict(row)
        feature["geometry"] = wkb
        features.append(feature)
    audit = {
        "source_feature_count": COUNT,
        "source_id_sha256": _sha(_canonical([int(row["MINOP5X020"]) for row in rows])),
        "source_record_sha256": _sha(_canonical([_json_safe_row(row) for row in rows])),
        "source_geometry_sha256": _sha(_canonical([[int(row["MINOP5X020"]), list(geometry.coords)[0]]
                                                   for row, geometry in zip(rows, geometries)])),
        "source_schema_sha256": _sha(_canonical(DBF_FIELDS)),
        "source_geometry_type": "esriGeometryPoint (shapefile shape type 1)",
        "states": len({row["STATE_LOCA"] for row in rows}),
        "commodities": len({row["COMMODITY"] for row in rows}),
        "plant_min_counts": plant_min_counts,
        "max_coordinate_vs_declared_attribute_delta_degrees": max_delta,
        "native_crs_declaration": NATIVE_CRS_DECLARATION,
        "source_coverage_scope": ("Complete archived USGS National Atlas Miscellaneous Industrial "
                                  "Minerals Operations package as published (304 features); the "
                                  "legacy dataset is explicitly not expected to be updated and no "
                                  "maintained agency feed exists."),
        "classification": ("retired USGS National Atlas producer publication; third-party "
                           "snapshots (Internet Archive, DataLumos HIFLD copy) are research "
                           "context only and are not used as pipeline inputs"),
        "geometry_attribute_relation": ("Native SHP coordinates carry more precision than the rounded "
                                        "DBF LATITUDE/LONGITUDE attribute values; the FGDC metadata "
                                        "defines no equivalence rule between them, so the difference "
                                        "is recorded, not corrected or reconciled."),
        "id_roster_note": ("Producer MINOP5X020 identifiers are unique and strictly increasing but "
                           "not contiguous: 304 values span 1..305 with one producer gap at 187; the "
                           "gap is preserved and no substitute row ID is generated."),
    }
    return features, audit


def _parquet_bytes(rows: list[dict[str, object]], wkbs: list[bytes]) -> bytes:
    columns = []
    for name in ("source_fid", "source_shp_record_number"):
        columns.append(pa.array([row[name] for row in rows], type=pa.int64()))
    for name in DECLARED_NAMES:
        columns.append(pa.array([row[name] for row in rows], type=ARROW_TYPES[name]))
    columns.append(pa.array(wkbs, type=pa.binary()))
    table = pa.Table.from_arrays(
        columns,
        names=["source_fid", "source_shp_record_number"] + DECLARED_NAMES + ["geometry"])
    fields = []
    for index, name in enumerate(["source_fid", "source_shp_record_number"]):
        fields.append(pa.field(name, table.schema.field(index).type, nullable=False,
                               metadata={b"identity": IDENTITY_COLUMN_METADATA[name]}))
    for name, field in zip(DECLARED_NAMES, DBF_FIELDS):
        kind, width, scale = field[1], field[2], field[3]
        fields.append(pa.field(name, ARROW_TYPES[name], nullable=False,
                               metadata={b"original_DBF_physical_declaration":
                                         _canonical({"type": kind, "width": width, "decimals": scale})}))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    table = table.cast(pa.schema(fields))
    metadata = {
        b"geo": _canonical({
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                # The pinned USGS FGDC XML identifies NAD83; source coordinates
                # are retained as-is, without reprojection.
                "crs": CRS.from_epsg(4269).to_json_dict(),
            }},
        }),
        b"native_crs_declaration": NATIVE_CRS_DECLARATION.encode("utf-8"),
    }
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd",
                   use_dictionary=False)
    return sink.getvalue()



def capture_miscellaneous_industrial_mineral_operations(storage: StagingStorageResource,
                                                        *, version: str) -> None:
    """Refuse candidate construction until an official distribution is established.

    Dated 2026-11-10 probes: the December 2005 National Atlas publication
    ``minop5x020_nt00022.tar.gz`` is retired from its only official host
    (``dds.cr.usgs.gov`` returns 404) and the live USGS NGTOC FGDC metadata for
    the layer links only to that retired URL. The producer's live mrdata.usgs.gov
    "Active Mines and Mineral Processing Plants" product carries the same 2003
    AMIS survey records under the layer's 15-commodity scope, but with a
    different record identity (rec_no roster instead of the original MINOP5X020
    1..305 roster with the producer gap at 187), differently split
    multi-commodity sites, three records exported with truncated site names,
    rounded coordinates, or abbreviated state text, extra fields (fips, huc),
    and a different CRS declaration. Accepting it, or any third-party snapshot
    of the retired publication, is a source-identity decision that belongs to
    maintainers, not to this pipeline. No archive or mirror bytes are fetched,
    replayed, staged, or quarantined here.
    """
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_isolated_local_staging_required")
    if (not isinstance(version, str) or not version or Path(version).name != version
            or version in {".", ".."} or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    raise ValueError("national_atlas_publication_retired_no_official_distribution")
