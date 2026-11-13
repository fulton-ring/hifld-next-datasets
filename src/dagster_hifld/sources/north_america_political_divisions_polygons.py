"""Candidate-only acquisition of the maintained CEC North American Atlas Political Boundaries."""
from __future__ import annotations

import hashlib
import json
import math
import platform
import shutil
import struct
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

import geopandas as gpd
import httpx
import pyogrio
import pyproj
import shapely

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.epa_frs_common import (
    capture_get,
    quarantine_incomplete_capture,
    write_immutable,
)

DATASET_SLUG = "north-america-political-divisions-polygons"
FILE_SLUG = DATASET_SLUG
SOURCE_PAGE = "https://www.cec.org/north-american-environmental-atlas/political-boundaries-2021/"
SOURCE_URL = "https://www.cec.org/files/atlas_layers/0_reference/0_01_political_boundaries/politicalboundaries_shapefile.zip"
SOURCE_FILE = "politicalboundaries_shapefile.zip"
PUBLISHER = "Commission for Environmental Cooperation (CEC)"
INVENTORY_AGENCY = "U.S. Geological Survey"
SOURCE_PRODUCT = "North American Environmental Atlas - Political Boundaries"
SOURCE_RELEASE = "Political Boundaries, 2021 (edition 3, boundaries_p_2021_v3)"
SOURCE_RELEASE_DATE = "2022-10-26"
SOURCE_PRODUCT_CREATION = "2022-09-15"
IDENTIFIER_COLUMN = "source_record_number"
EXPECTED_SHA256 = "29f3aec70604ed5be9e18c22e2d504da883fedd6d81aff27156456b4899a0c28"
EXPECTED_BYTES = 184_774_593
EXPECTED_UNCOMPRESSED_BYTES = 252_775_497
EXPECTED_FEATURE_COUNT = 98
EXPECTED_LAYER = "boundaries_p_2021_v3"
EXPECTED_CPG = b"UTF-8"
EXPECTED_FIELDS = [
    ("NAME_En", "C", 254, 0), ("NAME_Es", "C", 254, 0), ("NAME_Fr", "C", 254, 0),
    ("COUNTRY", "C", 20, 0), ("STATEABB", "C", 20, 0), ("CAPITAL", "C", 254, 0),
]
EXPECTED_PRJ = ('PROJCS["Sphere_ARC_INFO_Lambert_Azimuthal_Equal_Area",GEOGCS["GCS_Sphere_ARC_INFO",'
                'DATUM["D_Sphere_ARC_INFO",SPHEROID["Sphere_ARC_INFO",6370997.0,0.0]],PRIMEM["Greenwich",0.0],'
                'UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Azimuthal_Equal_Area"],'
                'PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],'
                'PARAMETER["Central_Meridian",-100.0],PARAMETER["Latitude_Of_Origin",45.0],UNIT["Meter",1.0]]')
EXPECTED_POLYGON_COUNT = 40
EXPECTED_MULTIPOLYGON_COUNT = 58
EXPECTED_RING_COUNT = 164_361
EXPECTED_VERTEX_COUNT = 15_722_772
EXPECTED_INVALID_GEOMETRY_COUNT = 1
EXPECTED_INVALID_REASON = "Ring Self-intersection[448287.487127878 3092671.08586955]"
EXPECTED_COUNTRY_COUNTS = {"CAN": 13, "MEX": 32, "USA": 53}
ID_SHA = "f2da88c58b8f5a2ab267dd4c25915ae5e74260d3c8aa944a745a81763120bd52"
RECORD_SHA = "f5563f8b171b5daaf6b9b310fe68359428059c2ca00e9cc03670fc9aeaf85e5a"
GEOMETRY_SHA = "6a58fdf66ec25b9ab1e4e93ad985b68238d3e0a7a4c8e5558c02b1456e3de012"
BOUNDS = [-6420071.874616642, -3311873.4810999967, 3745362.7551991306, 4267586.044877373]
PREFIX = "politicalboundaries_shapefile/NA_PoliticalDivisions/data/boundaries_p_2021_v3."
SHP_MEMBERS = {PREFIX + s for s in ("CPG", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")}
MEMBERS = SHP_MEMBERS | {
    "politicalboundaries_shapefile/",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/data/",
    "politicalboundaries_shapefile/HowToCite_ComoCitar_CommentCiter.txt",
    "politicalboundaries_shapefile/NA_boundaries_2021_metadata.html",
    "politicalboundaries_shapefile/Terms of use.doc",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_en.lyrx",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_en.qlr",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_es.lyrx",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_es.qlr",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_fr.lyrx",
    "politicalboundaries_shapefile/NA_PoliticalDivisions/NA_PoliticalDivisions_fr.qlr",
}
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 320_000_000
MAX_DECOMPRESSED_BYTES = 400_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _native(value):
    if hasattr(value, "item"):
        value = value.item()
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("source_nonfinite_attribute")
    return value


def _processing_runtime() -> dict[str, str]:
    """Report runtime versions observed during extraction without asserting Git identity."""
    return {
        "python": platform.python_version(),
        "geopandas": gpd.__version__,
        "httpx": httpx.__version__,
        "pyogrio": pyogrio.__version__,
        "gdal": ".".join(str(part) for part in pyogrio.__gdal_version__),
        "pyproj": pyproj.__version__,
        "proj": pyproj.proj_version_str,
        "shapely": shapely.__version__,
        "geos": shapely.geos_version_string,
    }


CODE_IDENTITY_SCHEME = "sha256-packaged-dagster_hifld-py-v1"
PROCESSING_SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROCESSING_SOURCE_PACKAGE = PROCESSING_SOURCE_ROOT / "dagster_hifld"


def processing_code_identity() -> str:
    """Hash packaged Dagster source files without requiring repository metadata."""
    files = sorted(PROCESSING_SOURCE_PACKAGE.rglob("*.py"))
    if not files or not PROCESSING_SOURCE_PACKAGE.is_dir():
        raise ValueError("processing_source_package_missing")
    digest = hashlib.sha256()
    for path in files:
        name = path.relative_to(PROCESSING_SOURCE_ROOT).as_posix().encode("utf-8")
        body = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def candidate_version() -> str:
    identity = _canonical({"source_sha256": EXPECTED_SHA256,
                           "code_identity_scheme": CODE_IDENTITY_SCHEME,
                           "code_sha256": processing_code_identity()})
    return "v" + _sha(identity)[:32]


def _schema_dbf(path: Path):
    b = path.read_bytes()
    header = int.from_bytes(b[8:10], "little")
    if header < 33 or header > len(b):
        raise ValueError("source_dbf_header_invalid")
    out = []
    offset = 32
    while offset < header and b[offset] != 0x0D:
        descriptor = b[offset:offset + 32]
        if len(descriptor) != 32:
            raise ValueError("source_dbf_descriptor_incomplete")
        out.append((descriptor[:11].split(b"\0", 1)[0].decode("ascii"), chr(descriptor[11]), descriptor[16], descriptor[17]))
        offset += 32
    if offset >= header or b[offset] != 0x0D:
        raise ValueError("source_dbf_terminator_missing")
    return out


def _fid_roster(shp: Path, shx: Path, count: int):
    """Read the native record numbers straight from the shapefile index.

    The ESRI Shapefile driver exposes no FID column, so the source row identity
    is validated from the .shx offsets and the .shp record headers instead of
    any reader-derived index.
    """
    index = shx.read_bytes()
    body = shp.read_bytes()
    if len(index) != 100 + 8 * count:
        raise ValueError("source_shx_length_drift")
    fids = []
    for i in range(count):
        offset, _words = struct.unpack(">2I", index[100 + 8 * i:100 + 8 * i + 8])
        head = body[offset * 2:offset * 2 + 4]
        if len(head) != 4:
            raise ValueError("source_shp_record_missing")
        fids.append(int(struct.unpack(">I", head)[0]))
    if fids != list(range(1, count + 1)):
        raise ValueError("source_fid_roster_drift")
    return fids


def _content_digests(g: gpd.GeoDataFrame, names: list[str], fids: list[int]):
    attrs = [[_native(v) for v in row] for row in g[names].itertuples(index=False, name=None)]
    ids = sorted(int(f) for f in fids)
    wkbs = shapely.to_wkb(g.geometry.array, byte_order=1, output_dimension=2)
    geometry = [[int(f), _sha(w)] for f, w in zip(fids, wkbs)]
    id_sha, record_sha, geometry_sha = (_sha(_canonical(x)) for x in (ids, attrs, geometry))
    bounds = [float(v) for v in g.total_bounds]
    return id_sha, record_sha, geometry_sha, bounds


def _quarantine_attempt(
    storage: StagingStorageResource,
    capture_root: Path,
    version: str,
    reason_code: str,
) -> str:
    """Retain all response bytes and a receipt before rejecting a bad capture."""
    body_path = capture_root / "source.body"
    wire_path = capture_root / "source.wire"
    receipt_path = capture_root / "source.receipt.json"
    body = body_path.read_bytes() if body_path.is_file() else b""
    wire = wire_path.read_bytes() if wire_path.is_file() else b""
    body_path.write_bytes(body)
    wire_path.write_bytes(wire)
    try:
        receipt = json.loads(receipt_path.read_text("utf-8"))
        if not isinstance(receipt, dict):
            raise ValueError("receipt_not_object")
    except (OSError, ValueError):
        receipt = {
            "method": "GET", "url": SOURCE_URL, "final_url": None,
            "status": None, "observed_at": None, "complete_body": False,
            "bytes": len(body), "sha256": _sha(body),
            "wire_bytes": len(wire), "wire_sha256": _sha(wire),
            "error_code": reason_code,
        }
        receipt_path.write_bytes(_canonical(receipt) + b"\n")
    return quarantine_incomplete_capture(
        storage, capture_root, DATASET_SLUG, version,
        {"source": (body, receipt)}, reason_code,
    )


def _capture(
    capture_directory: str | Path | None,
    staging_storage: StagingStorageResource,
    version: str,
) -> tuple[bytes, dict[str, object]]:
    temporary = tempfile.TemporaryDirectory(prefix="cec-political-boundaries-capture-") if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir():
                raise ValueError("source_capture_directory_invalid")
            if {p.name for p in root.iterdir()} != CAPTURE_FILES:
                raise ValueError("source_capture_roster_mismatch")
            body_path, wire_path, receipt_path = (root / "source.body", root / "source.wire", root / "source.receipt.json")
            if any(p.is_symlink() or not p.is_file() for p in (body_path, wire_path, receipt_path)):
                raise ValueError("source_capture_nonregular_file")
            body = body_path.read_bytes()
            wire = wire_path.read_bytes()
            receipt = json.loads(receipt_path.read_text("utf-8"))
            if (
                receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
                or receipt.get("final_url") != SOURCE_URL or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("content_encoding") != "identity"
            ):
                raise ValueError("source_capture_receipt_identity_mismatch")
        else:
            try:
                with httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0), follow_redirects=False) as client:
                    body, receipt = capture_get(client, SOURCE_URL, root, "source", max_bytes=MAX_SOURCE_BYTES)
            except Exception as exc:
                quarantine = _quarantine_attempt(staging_storage, root, version, type(exc).__name__)
                raise ValueError(f"official_cec_source_capture_failed; quarantined_at:{quarantine}") from exc

        if (
            receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
            or receipt.get("final_url") != SOURCE_URL or receipt.get("status") != 200
            or receipt.get("complete_body") is not True or receipt.get("bytes") != EXPECTED_BYTES
            or receipt.get("sha256") != EXPECTED_SHA256 or len(body) != EXPECTED_BYTES
            or receipt.get("content_encoding") != "identity"
            or receipt.get("wire_bytes") != EXPECTED_BYTES or receipt.get("wire_sha256") != EXPECTED_SHA256
            or receipt.get("content_length") not in (None, EXPECTED_BYTES)
            or _sha(body) != EXPECTED_SHA256
        ):
            quarantine = _quarantine_attempt(
                staging_storage, root, version, "official_cec_archive_identity_or_release_drift",
            )
            raise ValueError(f"official_cec_archive_identity_or_release_drift; quarantined_at:{quarantine}")
        return body, receipt
    finally:
        if temporary is not None:
            temporary.cleanup()


def _ring_vertex_stats(geoms) -> tuple[int, int]:
    rings = vertices = 0
    for g in geoms:
        parts = [g] if g.geom_type == "Polygon" else list(g.geoms)
        rings += sum(1 + len(p.interiors) for p in parts)
        vertices += sum(len(p.exterior.coords) + sum(len(r.coords) for r in p.interiors) for p in parts)
    return rings, vertices


def _inspect_archive(body: bytes) -> dict[str, object]:
    if _sha(body) != EXPECTED_SHA256 or len(body) != EXPECTED_BYTES:
        raise ValueError("pinned_cec_snapshot_drift")
    names = []
    with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
        for info in archive.infolist():
            parts = Path(info.filename).parts
            if (
                not info.filename or info.filename.startswith(("/", "\\"))
                or "\\" in info.filename or ".." in parts
                or info.filename.encode("utf-8", "surrogateescape") != info.filename.encode("utf-8", "surrogateescape")
            ):
                raise ValueError("cec_archive_unsafe_member")
            names.append(info.filename)
        if len(set(names)) != len(names) or set(names) != MEMBERS or len(archive.infolist()) != len(MEMBERS):
            raise ValueError("cec_archive_member_roster_drift")
        if sum(info.file_size for info in archive.infolist()) != EXPECTED_UNCOMPRESSED_BYTES:
            raise ValueError("cec_archive_member_size_drift")
        if archive.testzip() is not None:
            raise ValueError("cec_archive_crc_failure")
        with tempfile.TemporaryDirectory(prefix="cec-political-boundaries-") as tmp:
            root = Path(tmp)
            for name in sorted(SHP_MEMBERS):
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(name) as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
            shp = root / (PREFIX + "shp")
            if (root / (PREFIX + "CPG")).read_bytes() != EXPECTED_CPG:
                raise ValueError("cec_source_cpg_drift")
            if (root / (PREFIX + "prj")).read_text().strip() != EXPECTED_PRJ:
                raise ValueError("cec_source_prj_drift")
            if _schema_dbf(root / (PREFIX + "dbf")) != EXPECTED_FIELDS:
                raise ValueError("cec_source_dbf_schema_drift")
            info = pyogrio.read_info(shp)
            if (
                info["layer_name"] != EXPECTED_LAYER or info["geometry_type"] not in ("Polygon", "MultiPolygon")
                or int(info["features"]) != EXPECTED_FEATURE_COUNT
            ):
                raise ValueError("cec_layer_metadata_drift")
            fids = _fid_roster(shp, root / (PREFIX + "shx"), EXPECTED_FEATURE_COUNT)
            g = gpd.read_file(shp, engine="pyogrio")
    field_names = [name for name, *_ in EXPECTED_FIELDS]
    if list(g.columns) != field_names + ["geometry"] or len(g) != EXPECTED_FEATURE_COUNT:
        raise ValueError("cec_source_population_or_schema_drift")
    if any(str(g[column].dtype) != "object" for column in field_names):
        raise ValueError("cec_source_attribute_physical_type_drift")
    if g[field_names].isna().any().any() or (g[field_names] == "").any().any():
        raise ValueError("cec_source_attribute_null_or_empty_drift")
    if g["STATEABB"].duplicated().any():
        raise ValueError("cec_source_identifier_uniqueness_drift")
    if g["COUNTRY"].value_counts().to_dict() != EXPECTED_COUNTRY_COUNTS:
        raise ValueError("cec_source_country_coverage_drift")
    if g.geometry.isna().any() or g.geometry.is_empty.any():
        raise ValueError("cec_source_geometry_null_or_empty_drift")
    if any(shapely.get_coordinate_dimension(x) != 2 for x in g.geometry.array):
        raise ValueError("cec_source_non_2d_geometry")
    counts = g.geometry.geom_type.value_counts().to_dict()
    if counts.get("Polygon", 0) != EXPECTED_POLYGON_COUNT or counts.get("MultiPolygon", 0) != EXPECTED_MULTIPOLYGON_COUNT:
        raise ValueError("cec_source_geometry_type_drift")
    invalid = int((~g.geometry.is_valid).sum())
    if invalid != EXPECTED_INVALID_GEOMETRY_COUNT:
        raise ValueError("cec_source_geometry_validity_drift")
    reasons = sorted({str(r) for r in shapely.is_valid_reason(g.geometry.array) if r != "Valid Geometry"})
    if reasons != [EXPECTED_INVALID_REASON]:
        raise ValueError("cec_source_geometry_validity_reason_drift")
    rings, vertices = _ring_vertex_stats(g.geometry.array)
    if rings != EXPECTED_RING_COUNT or vertices != EXPECTED_VERTEX_COUNT:
        raise ValueError("cec_source_geometry_vertex_drift")
    id_sha, record_sha, geometry_sha, bounds = _content_digests(g, field_names, fids)
    if id_sha != ID_SHA or record_sha != RECORD_SHA or geometry_sha != GEOMETRY_SHA:
        raise ValueError("cec_source_content_snapshot_drift")
    if bounds != BOUNDS:
        raise ValueError("cec_source_bounds_drift")
    schema = [{"name": name, "type": "string", "nullable": False} for name, *_ in EXPECTED_FIELDS]
    return {
        "feature_count": len(g),
        "attribute_fields": field_names,
        "attribute_types": {name: "string" for name, *_ in EXPECTED_FIELDS},
        "source_id_field": "FID (shapefile record number); unique native STATEABB preserved",
        "source_id_sha256": id_sha,
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(schema)),
        "source_crs": "ESRI PRJ Sphere_ARC_INFO_Lambert_Azimuthal_Equal_Area (lat_0 45, lon_0 -100, metres)",
        "geometry_type": "Polygon/MultiPolygon",
        "polygon_count": EXPECTED_POLYGON_COUNT,
        "multipolygon_count": EXPECTED_MULTIPOLYGON_COUNT,
        "ring_count": EXPECTED_RING_COUNT,
        "vertex_count": EXPECTED_VERTEX_COUNT,
        "bounds": bounds,
        "null_geometry_count": int(g.geometry.isna().sum()),
        "empty_geometry_count": int(g.geometry.is_empty.sum()),
        "invalid_geometry_count": invalid,
        "invalid_geometry_reasons": reasons,
        "country_counts": EXPECTED_COUNTRY_COUNTS,
        "stateabb_unique": bool(g["STATEABB"].is_unique),
        "null_counts": {name: int(g[name].isna().sum()) for name in field_names},
    }


def capture_north_america_political_divisions_polygons(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture one exact pinned CEC archive and stage it immutably for catalog."""
    stage = Path(staging_storage.local_dir).absolute() if staging_storage.use_local and staging_storage.local_dir else None
    if stage is not None and shutil.disk_usage(stage).free < 900_000_000:
        raise ValueError("disk_floor_before_acquisition")
    body, receipt = _capture(capture_directory, staging_storage, version)
    audit = _inspect_archive(body)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{SOURCE_FILE}"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != EXPECTED_BYTES or staging_storage.sha256_key(archive_key) != EXPECTED_SHA256:
        raise ValueError("staged_cec_archive_integrity_failure")
    reused = before is not None
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": _processing_runtime(),
    }
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "title": "North America Political Divisions Polygons",
        "description": (
            "Maintained CEC North American Environmental Atlas 'Political Boundaries, 2021' (edition 3, "
            "boundaries_p_2021_v3): 98 first-level jurisdiction polygons for Canada (10 provinces, 3 territories), "
            "Mexico (32 states) and the United States (48 conterminous states, Alaska, Hawaii, District of Columbia, "
            "Puerto Rico, U.S. Virgin Islands), integrating the 2021 national boundary publications of each country. "
            "Candidate only; source relationship, schema/ID contract and baseline compatibility remain unapproved."
        ),
        "publisher": PUBLISHER,
        "agency": "Commission for Environmental Cooperation (CEC); HIFLD inventory agency of record U.S. Geological Survey",
        "source_url": SOURCE_URL,
        "license": "CC BY 4.0 (CEC Terms of use.doc); rights acceptance pending maintainer review.",
        "source_modified": None,
        "date_issued": SOURCE_RELEASE_DATE,
        "inputs": [{
            "input_id": SOURCE_URL,
            "producer": PUBLISHER,
            "host": urlsplit(SOURCE_URL).hostname,
            "canonical_url": SOURCE_PAGE,
            "retrieval_url": SOURCE_URL,
            "retrieved_at": observed_at,
            "sha256": EXPECTED_SHA256,
            "bytes": EXPECTED_BYTES,
            "license": "https://creativecommons.org/licenses/by/4.0/",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": "Producer-declared publication date 2022-10-26 recorded in the package metadata (NA_boundaries_2021_metadata.html and boundaries_p_2021_v3.shp.xml, publication date 2022-10-26T00:00:00, edition 3, 20221026); product creation 2022-09-15; content integrates the countries' 2021 boundary publications. The observed HTTP Last-Modified of the zip (2026-09-15) is a server file date, not a data release date.",
        }],
        "transformations": [
            "Preserve the complete official CEC zip and every member; catalog the boundaries_p_2021_v3 Shapefile without ID synthesis, reprojection or attribute mapping.",
            "Native Lambert Azimuthal Equal Area (sphere) CRS and native geometry retained verbatim, including the one natively self-intersecting Nunavut ring left unrepaired.",
        ],
        "lineage": (
            "Commission for Environmental Cooperation (CEC) North American Environmental Atlas 'Political Boundaries, 2021' "
            "(cec.org landing page; product creation 2022-09-15, Atlas publication 2022-10-26, edition 3 of the 2010-vintage "
            "2010 CEC release, integrating political boundaries published by each country in 2021: Statistics Canada "
            "'Province and Territory 2021, Cartographic Boundary Files (CBF)' 2021-01-01 and Mexico INEGI Marco Geoestadistico). "
            "The HIFLD Open row north-america-political-divisions-polygons lists US Geological Survey as agency of record "
            "(USGS historically co-produced and hosted the North American Atlas); the DHS HIFLD Open crosswalk row names CEC "
            "as agency, points to this maintained CEC landing page, and its Open REST Service "
            "(services7.arcgis.com/oF9CDB4lUYF7Um9q ... NA_Political_Divisions_Polygons/FeatureServer) is dead (verified "
            "2026-11-13: HTTP 200 Esri error {code:400, 'Invalid URL'}). CEC geoprocessing history shows maintenance through "
            "2026-08-17, so this is the maintained publication rather than a frozen release. DataLumos project 240869 V2 and "
            "Data Rescue Project entries are final-snapshot archive context only and are never pipeline inputs."
        ),
    }
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if staging_storage.object_exists(manifest_key):
        encoded_manifest = staging_storage.read_key(manifest_key)
        prior = json.loads(encoded_manifest)
        validate_provenance(prior)
        expected_prior = json.loads(_canonical(manifest))
        if isinstance(prior.get("inputs"), list) and len(prior["inputs"]) == 1:
            expected_prior["inputs"][0]["retrieved_at"] = prior["inputs"][0].get("retrieved_at")
        if prior != expected_prior:
            raise ValueError("cached_source_manifest_identity_mismatch")
    else:
        encoded_manifest = _canonical(manifest) + b"\n"
        write_immutable(staging_storage, manifest_key, encoded_manifest)
    stable_receipt = {key: receipt.get(key) for key in (
        "method", "url", "final_url", "status", "complete_body", "wire_bytes",
        "wire_sha256", "bytes", "sha256", "content_encoding", "content_length",
    )}
    audit_doc = {
        "source": {
            "url": SOURCE_URL, "landing_page": SOURCE_PAGE, "product": SOURCE_PRODUCT,
            "release": SOURCE_RELEASE, "release_date": SOURCE_RELEASE_DATE,
            "product_creation_date": SOURCE_PRODUCT_CREATION,
            "archive_sha256": EXPECTED_SHA256, "archive_bytes": EXPECTED_BYTES,
        },
        "processing_code": processing_code,
        "acquisition_receipt": stable_receipt,
        "inventory_agency_of_record": INVENTORY_AGENCY,
        **audit,
    }
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_record_sha256": audit["source_record_sha256"], "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": EXPECTED_BYTES,
        "source_archive_sha256": EXPECTED_SHA256, "source_cache_reused": reused,
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": audit["source_id_field"], "source_crs": audit["source_crs"],
        "source_release_date": SOURCE_RELEASE_DATE, "source_release": SOURCE_RELEASE,
        "source_coverage_scope": "Complete 98-feature first-level jurisdiction population (CAN 13, MEX 32, USA 53) of the CEC North American Atlas Political Boundaries 2021 edition 3 shapefile boundaries_p_2021_v3",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key],
    }
