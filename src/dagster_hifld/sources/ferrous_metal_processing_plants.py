"""Draft-only lossless reconstruction of the USGS National Atlas Ferrous Metal Processing Plants package.

The original producer publication ``minop4x020_nt00021.tar.gz`` (December 2005
USGS National Atlas release built from the Minerals Information Team's 2003
Automated Minerals Information System survey) is retired from its original
``dds.cr.usgs.gov`` host. The Internet Archive snapshot of those producer bytes
is a historical archive, not a maintained agency feed. The live USGS legacy
metadata XML document (``minop4x020.xml``) is retained as producer-published
historical metadata. This module validates both inputs against exact byte
pins, decodes the native DBF/SHP/SHX masters losslessly, and stages an
unapproved typed GeoParquet candidate. It does not approve the source,
archive authority, datum, schema/ID contract, rights, or release, and it
never substitutes the DataLumos HIFLD mirror.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import shutil
import struct
import tarfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb, is_empty, is_valid, total_bounds

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "ferrous-metal-processing-plants"
PUBLISHER = "U.S. Geological Survey National Atlas of the United States"
ORIGINAL_PACKAGE_URL = "https://dds.cr.usgs.gov/pub/data/nationalatlas/minop4x020_nt00021.tar.gz"
# Internet Archive identity snapshot of the retired producer file. The archived
# bytes are frozen; this is a historical archive, not a maintained agency feed.
PACKAGE_CAPTURE_URL = (
    "https://web.archive.org/web/20161212153047id_/"
    "https://dds.cr.usgs.gov/pub/data/nationalatlas/minop4x020_nt00021.tar.gz"
)
LEGACY_METADATA_URL = "https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop4x020.xml"
COUNT = 39
EXPECTED_PACKAGE_BYTES = 7420
EXPECTED_PACKAGE_SHA256 = "6137b0a5c6f3f5dc3a92262abc4e879001ff4f82f3d660e6214a0b01c3b3d168"
EXPECTED_METADATA_BYTES = 18646
EXPECTED_METADATA_SHA256 = "5580370deb4f4589c9e37a05c58bb223f847cbd1b090d6ab8d24088e0aa72eaa"
PACKAGE_MEMBER = "national_atlas_package"
LEGACY_METADATA_MEMBER = "legacy_metadata_xml"
EXPECTED_CAPTURE_NAMES = {PACKAGE_MEMBER, LEGACY_METADATA_MEMBER}
MEMBER_NAMES = ("minop4x020.dbf", "minop4x020.shp", "minop4x020.shx", "minop4x020.txt")
MEMBER_PINS = {
    "minop4x020.dbf": {"bytes": 6991, "sha256": "3b1dc492160a8bd78256b6deb9444b8f2945db35b4817fb31287f572e8c1f83f"},
    "minop4x020.shp": {"bytes": 1192, "sha256": "e8fde124eea62ed9ca53a36b439e64fc0da918cca453bc71e40b51393ef5c3ec"},
    "minop4x020.shx": {"bytes": 412, "sha256": "62880e16eded581d4ba6182163b27705768338dbd43f3276eea6041fb1979579"},
    "minop4x020.txt": {"bytes": 18119, "sha256": "136f209d0865eccc4513577f167bc6ca2e577fd852a9d2f8949f69d3b95f89a7"},
}
SHAPEFILE_MEMBERS = ("minop4x020.dbf", "minop4x020.shp", "minop4x020.shx")
DBF_FIELDS = [
    ("MINOP4X020", "N", 2, 0),
    ("COMMODITY", "C", 26, 0),
    ("SITE_NAME", "C", 29, 0),
    ("COMPANY_NA", "C", 57, 0),
    ("STATE_LOCA", "C", 14, 0),
    ("COUNTY", "C", 12, 0),
    ("LATITUDE", "N", 9, 3),
    ("LONGITUDE", "N", 11, 3),
    ("PLANT_MIN", "C", 10, 0),
]
DECLARED_NAMES = [field[0] for field in DBF_FIELDS]
ARROW_TYPES = {
    "MINOP4X020": pa.decimal128(2, 0),
    "COMMODITY": pa.string(),
    "SITE_NAME": pa.string(),
    "COMPANY_NA": pa.string(),
    "STATE_LOCA": pa.string(),
    "COUNTY": pa.string(),
    "LATITUDE": pa.decimal128(9, 3),
    "LONGITUDE": pa.decimal128(11, 3),
    "PLANT_MIN": pa.string(),
}
IDENTITY_COLUMN_METADATA = {
    "source_fid": b"Original zero-based DBF physical record index, not a published or replacement feature ID",
    "source_shp_record_number": b"Original SHP record-header number, not a substitute source identifier",
}
EXPECTED_TXT_MARKERS = (
    "Title: Ferrous Metal Processing Plants",
    "Originator: U.S. Geological Survey",
    "Publication_Date: 200512",
    "Source_Citation_Abbreviation: AMIS",
    "Horizontal_Datum_Name: North American Datum of 1983",
    "Latitude_Resolution: 0.000278",
    "Longitude_Resolution: 0.000278",
)
EXPECTED_XML_MARKERS = (
    "<title>Ferrous Metal Processing Plants - Direct Download</title>",
    "<pubdate>200512</pubdate>",
    "<onlink>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop4x020_nt00021.tar.gz</onlink>",
    "<geogunit>Decimal degrees</geogunit>",
    "<horizdn>North American Datum of 1983</horizdn>",
    "<ellips>GRS1980</ellips>",
    "<semiaxis>6378137</semiaxis>",
    "<denflat>298.257222</denflat>",
)
NATIVE_CRS_DECLARATION = (
    "The package ships no .prj sidecar; the pinned USGS FGDC XML for the "
    "Ferrous Metal Processing Plants layer declares Decimal degrees, North "
    "American Datum of 1983, GRS1980, semiaxis 6378137 m, and inverse flattening "
    "298.257222. These producer parameters identify native EPSG:4269. Coordinates "
    "are retained unchanged with no reprojection or synthesized PRJ."
)
MAX_PACKAGE_BYTES = 4_000_000
MAX_METADATA_BYTES = 1_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _json_safe_row(row: dict[str, object]) -> dict[str, object]:
    return {key: (str(value) if isinstance(value, Decimal) else value)
            for key, value in row.items()}


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                          root / f"{name}.receipt.json")
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = body_path.read_bytes(), wire_path.read_bytes(), json.loads(receipt_path.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("capture_receipt_invalid:" + name)
    return body, wire, receipt


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    receipts = manifest.get("receipts", {})
    if manifest.get("complete") is not True or not isinstance(receipts, dict) or not receipts:
        raise ValueError("capture_manifest_invalid")
    captures = {}
    for name in sorted(receipts):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != receipts.get(name):
            raise ValueError("capture_receipt_manifest_mismatch:" + name)
        captures[name] = (body, receipt)
    return captures


def _validate_input_pins(captures: dict[str, tuple[bytes, dict]]) -> None:
    pins = {PACKAGE_MEMBER: {"bytes": EXPECTED_PACKAGE_BYTES, "sha256": EXPECTED_PACKAGE_SHA256},
            LEGACY_METADATA_MEMBER: {"bytes": EXPECTED_METADATA_BYTES, "sha256": EXPECTED_METADATA_SHA256}}
    if set(captures) != EXPECTED_CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    for name, pin in pins.items():
        body, receipt = captures[name]
        if len(body) != pin["bytes"] or _sha(body) != pin["sha256"]:
            raise ValueError("pinned_archive_bytes_drift:" + name)
        if receipt.get("url") != receipt.get("final_url"):
            raise ValueError("source_capture_redirect_drift:" + name)


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
    if fields != DBF_FIELDS or count != COUNT or width != 171 or header != 321:
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
    if [int(row["MINOP4X020"]) for row in rows] != list(range(1, COUNT + 1)):
        raise ValueError("missing_duplicate_reordered_original_MINOP4X020")
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
    text = body.decode("ascii")
    for marker in EXPECTED_TXT_MARKERS:
        if marker not in text:
            raise ValueError("producer_metadata_marker_drift:" + marker.split(":")[0])
    return {"document_format": "FGDC text metadata bundled in the producer package",
            "declared_title": "Ferrous Metal Processing Plants",
            "declared_originator": "U.S. Geological Survey",
            "declared_publication_date": "200512",
            "declared_source": "Automated Minerals Information System (AMIS) 2003 operations, "
                               "USGS Minerals Yearbook Volume 1 (2004)",
            "declared_horizontal_datum": "North American Datum of 1983",
            "declared_coordinate_resolution": "0.000278 degree (one arc-second)",
            "declared_online_linkage": ORIGINAL_PACKAGE_URL}


def _validate_metadata_xml(body: bytes) -> dict[str, object]:
    text = body.decode("ascii")
    for marker in EXPECTED_XML_MARKERS:
        if marker not in text:
            raise ValueError("producer_metadata_marker_drift:" + marker.split('"')[0][:60])
    if not all(marker in text for marker in EXPECTED_XML_MARKERS[3:]):
        raise ValueError("producer_metadata_crs_declaration_drift")
    return {"document_format": "FGDC XML metadata served live by USGS",
            "declared_title": "Ferrous Metal Processing Plants - Direct Download",
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
    if [int(row["MINOP4X020"]) for row in rows] != [row["source_shp_record_number"] for row in rows]:
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
        "source_id_sha256": _sha(_canonical([int(row["MINOP4X020"]) for row in rows])),
        "source_record_sha256": _sha(_canonical([_json_safe_row(row) for row in rows])),
        "source_geometry_sha256": _sha(_canonical([[int(row["MINOP4X020"]), list(geometry.coords)[0]]
                                                   for row, geometry in zip(rows, geometries)])),
        "source_schema_sha256": _sha(_canonical(DBF_FIELDS)),
        "source_geometry_type": "esriGeometryPoint (shapefile shape type 1)",
        "states": len({row["STATE_LOCA"] for row in rows}),
        "commodities": len({row["COMMODITY"] for row in rows}),
        "plant_min_counts": plant_min_counts,
        "max_coordinate_vs_declared_attribute_delta_degrees": max_delta,
        "native_crs_declaration": NATIVE_CRS_DECLARATION,
        "source_coverage_scope": ("Complete archived USGS National Atlas Ferrous Metal Processing Plants "
                                  "package as published (39 features); the legacy dataset is explicitly "
                                  "not expected to be updated and no maintained agency feed exists."),
        "classification": ("historical archive of the producer publication via the Internet Archive; "
                           "DataLumos HIFLD copy is a separately labeled fallback candidate"),
        "geometry_attribute_relation": ("Native SHP coordinates carry more precision than the declared "
                                        "0.000278-degree DBF attribute resolution; the difference is "
                                        "recorded, not corrected or reconciled."),
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


def _capture_evidence_hash(captures: dict[str, tuple[bytes, dict]]) -> str:
    roster = {name: {"body_sha256": receipt["sha256"], "wire_sha256": receipt["wire_sha256"]}
              for name, (_body, receipt) in sorted(captures.items())}
    return _sha(_canonical(roster))


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(root, identity["code_commit"], identity["files"],
                                        identity=identity)
    return identity


def _preserve_original_receipts(captures: dict[str, tuple[bytes, dict]],
                                original_receipts: object) -> dict[str, dict]:
    if not isinstance(original_receipts, dict) or set(original_receipts) != set(captures):
        raise ValueError("cached_original_receipt_roster_changed")
    for name, (_body, receipt) in captures.items():
        sealed = original_receipts[name]
        fields = ("complete_body", "status", "bytes", "sha256", "wire_bytes", "wire_sha256")
        if any(receipt.get(field) != sealed.get(field) for field in fields):
            raise ValueError("cached_source_capture_content_changed:" + name)
    return original_receipts


def capture_ferrous_metal_processing_plants(storage: StagingStorageResource, *, version: str,
                                            capture_directory: str | None = None,
                                            repo_root: Path | None = None) -> dict[str, object]:
    """Capture, validate, and stage the unapproved Ferrous Metal Processing Plants candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_isolated_local_staging_required")
    if (not isinstance(version, str) or not version or Path(version).name != version
            or version in {".", ".."} or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 100_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("unsealed_capture_attempt_exists")
        root = ((requested / version) if requested
                else Path("/tmp/hifld-ferrous-metal-processing-plants-capture")) / uuid4().hex
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures: dict[str, tuple[bytes, dict]] = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usgs-national-atlas-draft"}) as client:
                captures[PACKAGE_MEMBER] = capture_get(client, PACKAGE_CAPTURE_URL, root,
                                                       PACKAGE_MEMBER, max_bytes=MAX_PACKAGE_BYTES)
                captures[LEGACY_METADATA_MEMBER] = capture_get(client, LEGACY_METADATA_URL, root,
                                                               LEGACY_METADATA_MEMBER,
                                                               max_bytes=MAX_METADATA_BYTES)
            _validate_input_pins(captures)
            _decode_dbf(_extract_package(captures[PACKAGE_MEMBER][0])["minop4x020.dbf"])
            manifest = {
                "complete": True,
                "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            }
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            captures = _read_replay(root)
        except Exception as exc:
            names = sorted(path.name.removesuffix(".receipt.json") for path in root.glob("*.receipt.json"))
            receipts = {}
            for name in names:
                try:
                    receipts[name] = json.loads((root / f"{name}.receipt.json").read_bytes())
                except (OSError, ValueError):
                    receipts[name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({
                "complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "error_code": type(exc).__name__,
                "receipts": receipts,
            }) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version,
                                              captures, type(exc).__name__)
            raise
    _validate_input_pins(captures)
    extracted = _extract_package(captures[PACKAGE_MEMBER][0])
    rows = _decode_dbf(extracted["minop4x020.dbf"])
    wkbs = _decode_points(extracted["minop4x020.shp"], extracted["minop4x020.shx"])
    _validate_metadata_text(extracted["minop4x020.txt"])
    _validate_metadata_xml(captures[LEGACY_METADATA_MEMBER][0])
    features, audit = _validate_population(rows, wkbs)
    parquet = _parquet_bytes(rows, wkbs)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    repo = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = _processing_identity(repo)
    capture_hash = _capture_evidence_hash(captures)
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    fingerprint = _sha(_canonical({"identity": identity,
                                   "source_record_sha256": audit["source_record_sha256"],
                                   "source_schema_sha256": audit["source_schema_sha256"],
                                   "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    original_payload = None
    if storage.object_snapshot(state_key) is not None:
        original_payload = json.loads(load_first_construction(storage, state_key, version,
                                                              fingerprint)).get("payload")
        receipts = _preserve_original_receipts(
            captures, original_payload.get("capture_receipts") if isinstance(original_payload, dict) else None)
    audit.update({"source_capture_evidence_sha256": capture_hash, "source_population_complete": True,
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    if original_payload is None:
        receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    release_reason = ("The producer FGDC metadata declares publication month 200512 with no exact "
                      "day; the Internet Archive capture (2016-12-12) and every retrieval time are "
                      "not source release dates.")
    inputs = [{
        "input_id": PACKAGE_MEMBER,
        "producer": PUBLISHER,
        "host": urlsplit(receipts[PACKAGE_MEMBER]["url"]).hostname,
        "role": "archive",
        "canonical_url": ORIGINAL_PACKAGE_URL,
        "retrieval_url": receipts[PACKAGE_MEMBER]["url"],
        "observed_final_url": receipts[PACKAGE_MEMBER]["final_url"],
        "retrieved_at": receipts[PACKAGE_MEMBER]["observed_at"],
        "http_status": receipts[PACKAGE_MEMBER]["status"],
        "bytes": receipts[PACKAGE_MEMBER]["bytes"],
        "sha256": receipts[PACKAGE_MEMBER]["sha256"],
        "wire_bytes": receipts[PACKAGE_MEMBER]["wire_bytes"],
        "wire_sha256": receipts[PACKAGE_MEMBER]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": release_reason,
        "license": "USGS National Atlas metadata states no use restrictions; rights review pending.",
        "attribution": ("U.S. Geological Survey National Atlas; Minerals Information Team AMIS 2003 "
                        "operations; final attribution review pending."),
    }, {
        "input_id": LEGACY_METADATA_MEMBER,
        "producer": "U.S. Geological Survey",
        "host": urlsplit(receipts[LEGACY_METADATA_MEMBER]["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": LEGACY_METADATA_URL,
        "retrieval_url": receipts[LEGACY_METADATA_MEMBER]["url"],
        "observed_final_url": receipts[LEGACY_METADATA_MEMBER]["final_url"],
        "retrieved_at": receipts[LEGACY_METADATA_MEMBER]["observed_at"],
        "http_status": receipts[LEGACY_METADATA_MEMBER]["status"],
        "bytes": receipts[LEGACY_METADATA_MEMBER]["bytes"],
        "sha256": receipts[LEGACY_METADATA_MEMBER]["sha256"],
        "wire_bytes": receipts[LEGACY_METADATA_MEMBER]["wire_bytes"],
        "wire_sha256": receipts[LEGACY_METADATA_MEMBER]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": release_reason,
        "license": "USGS-published historical FGDC metadata document; rights review pending.",
        "attribution": ("U.S. Geological Survey historical metadata document retained as evidence; "
                        "it is not proof of a maintained current distribution."),
    }]
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "Ferrous Metal Processing Plants",
        "description": ("Lossless reconstruction of the December 2005 USGS National Atlas Ferrous "
                        "Metal Processing Plants package from an Internet Archive snapshot of the "
                        "retired producer publication; unapproved candidate only."),
        "publisher": PUBLISHER,
        "agency": "U.S. Geological Survey",
        "source_url": ORIGINAL_PACKAGE_URL,
        "license": None,
        "source_modified": None,
        "date_issued": None,
        "date_modified": None,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "processing_identity": identity,
        "inputs": inputs,
        "transformations": [{
            "operation": "Lossless native DBF/SHP/SHX decode to typed GeoParquet WKB",
            "parameters": {
                "package_inputs": ("Exact pinned Internet Archive snapshot bytes of the retired "
                                   "producer publication; archive classification is retained, and "
                                   "no mirror or guessed successor is substituted."),
                "source_fields": ("All nine declared DBF fields retained with exact ASCII text and "
                                  "base-10 decimal values at their declared DBF widths and scales; "
                                  "no binary floating substitution."),
                "source_field_type_mapping": [
                    {"source_name": name, "source_dbf_type": f"{kind}({width},{scale})",
                     "output_arrow_type": str(ARROW_TYPES[name])}
                    for name, (kind, width, scale) in
                    ((field[0], field[1:]) for field in DBF_FIELDS)
                ],
                "source_ids": ("Original MINOP4X020 decimal(2,0) identifiers 1..39 retained unchanged "
                               "in DBF order; DBF record indices and SHP record numbers are separate "
                               "physical identity columns; no substitute row identifiers."),
                "source_crs": NATIVE_CRS_DECLARATION,
                "geometry": "Native SHP point coordinates preserved exactly without reprojection, repair, or simplification.",
                "raw_evidence": ("The complete tar.gz package, the USGS metadata XML, every wire "
                                 "body, and hash-pinned receipts are retained under raw/."),
                "candidate_status": ("unapproved; archive authority, source edition equivalence, "
                                     "schema/ID contract, rights, coverage, and release decisions "
                                     "pending"),
            },
        }],
        "lineage": ("USGS National Atlas mineral operations series (minop4x020), built from the "
                    "Minerals Information Team's 2003 Automated Minerals Information System survey "
                    "and published December 2005. The original dds.cr.usgs.gov host is retired; the "
                    "HIFLD 2017-12-18 DataLumos item is a separately labeled fallback mirror whose "
                    "equivalence is unresolved. No maintained agency feed is known."),
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": ("Preserve all declared DBF widths/scales, original IDs, native points, "
                           "and the unknown-CRS declaration as typed GeoParquet."),
                "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}],
            }},
        },
        "unsupported_output_formats": {
            "shapefile_zip": "No accepted schema/ID mapping; preserved original masters remain raw evidence.",
            "geopackage": "DBF decimal128 precision/scale cannot be silently coerced to GPKG REAL; no migration approved.",
            "pmtiles": "No display generalization, datum choice, or release policy approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    outputs = {
        output_path: parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
    }
    for name, (body, _receipt) in captures.items():
        _b, wire, _r = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipts[name]) + b"\n"
    for name in MEMBER_NAMES:
        outputs[f"raw/{name}"] = extracted[name]
    ready = {
        "approved": False,
        "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_release_accepted": False,
        "freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    if original_payload is not None:
        verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original_payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"],
                "source_record_sha256": audit["source_record_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "source_capture_evidence_sha256": capture_hash, "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(repo, identity["code_commit"], identity["files"],
                                        identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_capture_evidence_sha256": capture_hash,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(root), "reused": replay and original_payload is not None,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
