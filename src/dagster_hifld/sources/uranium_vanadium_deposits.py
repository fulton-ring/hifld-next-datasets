"""Candidate-only acquisition of the official USGS MRDS uranium/vanadium digest.

The DHS HIFLD Open crosswalk (row "Uranium and Vanadium Deposits", Agency
U.S. Geological Survey, DOI 10.3886/E241367V1) records that the retired HIFLD
Open item 2c9b52ce91be4384931b64dd551cb6ea (whose services1.arcgis.com
FeatureServer now answers only an ArcGIS "Invalid URL" error document) should
be replaced from the producer's Mineral Resources Data System (MRDS)
publication. USGS currently publishes MRDS — the maintained database that
"subsumes the original MRDS and MAS/MILS" named in the inventory row — at
https://mrdata.usgs.gov/mrds/ with shapefile, CSV and relational download
options plus WMS/WFS services. The producer states that systematic MRDS
updates ceased as of 2011 and that MRDS remains its best available collection
for this domain, so the database is frozen but the publication is the
producer's current one; the package Last-Modified date and the packaged FGDC
metadata are retained as packaging evidence, never an accepted release date.

The pipeline pins the producer's prebuilt point digest (mrds-trim.zip, the
"Compact" shapefile of the fields deemed most useful — the same digest family
the HIFLD Open copy described) and stages the token-exact Uranium/Vanadium
subset as the unapproved candidate: every record whose commodity code list
contains the exact tokens U (uranium) and/or V (vanadium). The proposed
subset predicate is a draft contract requiring maintainer acceptance; the
complete digest is preserved losslessly beside it, and no mirror, archive or
third-party copy is fetched (the DataLumos record 240898 is identification
context only).
"""
from __future__ import annotations

import hashlib
import platform
import json
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
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

DATASET_SLUG = "uranium-and-vanadium-deposits"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://mrdata.usgs.gov/mrds/mrds-trim.zip"
SOURCE_FILE = "mrds-trim.zip"
PUBLISHER = "U.S. Geological Survey, Mineral Resources Data System (MRDS)"
MRDS_LANDING_URL = "https://mrdata.usgs.gov/mrds/"
EXPECTED_SHA256 = "f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84"
EXPECTED_BYTES = 11_236_725
EXPECTED_ARCHIVE_MEMBERS = {"mrds-trim.prj", "mrds-trim.shp", "mrds-trim.shx",
                            "mrds-trim.dbf", "mrds-trim.lyr", "mrds.met"}
EXPECTED_FIELDS = ["DEP_ID", "SITE_NAME", "DEV_STAT", "URL", "CODE_LIST"]
EXPECTED_CRS = "EPSG:4326"
# Complete digest population contract (dated complete inspection 2026-11-14).
EXPECTED_FEATURE_COUNT = 304_632
EXPECTED_ID_MIN = "10000001"
EXPECTED_ID_MAX = "60001858"
EXPECTED_FULL_ROSTER_SHA256 = "63c612bb6041f5253e995661c0c7105dfc364111323bbaf6c403b9bb860b7ca7"
EXPECTED_FULL_BOUNDS = [-178.8205, -72.99718, 179.1318, 80.00096]
EXPECTED_CODE_LIST_NULL_COUNT = 88
EXPECTED_DEV_STAT_COUNTS = {"Past Producer": 128823, "Occurrence": 67526, "Producer": 42879,
                            "Prospect": 35587, "Unknown": 26809, "Plant": 3008}
# Unapproved candidate contract: the token-exact Uranium (U) / Vanadium (V)
# subset of the complete digest.
SUBSET_TOKENS = ("U", "V")
EXPECTED_SUBSET_COUNT = 16_340
EXPECTED_SUBSET_ROSTER_SHA256 = "d5382281f4c66dd2a7eeba99db975b5638e9e8340245e46f954b5955969b5f0e"
EXPECTED_SUBSET_RECORD_SHA256 = "f4640250e1e5d10350b51918161bdb8b25ba1d3d6814aff6b6b1e2a961de0f8e"
EXPECTED_SUBSET_GEOMETRY_SHA256 = "d2a92590a13cd697431e4cd7ad24a5fc41d4bf96c88d69f6749b7a07ea10bd61"
EXPECTED_SUBSET_BOUNDS = [-167.98676, -46.08444, 174.9982, 70.49872]
EXPECTED_SUBSET_TOKEN_COUNTS = {"U": 15348, "V": 4826, "both": 3834}
EXPECTED_SUBSET_DEV_STAT_COUNTS = {"Past Producer": 5588, "Occurrence": 5447, "Prospect": 1961,
                                   "Producer": 1796, "Unknown": 1509, "Plant": 39}
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
MAX_SOURCE_BYTES = 16_000_000
BOUNDED_EXTRACTION_BYTES = 600_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _list_sha(items: list) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


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
    try:
        receipt = json.loads(receipt_path.read_text("utf-8"))
        if not isinstance(receipt, dict):
            raise ValueError("receipt_not_object")
    except (OSError, ValueError):
        receipt = {
            "method": "GET", "url": SOURCE_URL, "final_url": None, "status": None,
            "observed_at": None, "complete_body": False, "bytes": len(body),
            "sha256": _sha(body), "wire_bytes": len(wire), "wire_sha256": _sha(wire),
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
    temporary = tempfile.TemporaryDirectory(prefix="usgs-mrds-uv-capture-") if capture_directory is None else None
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
                with httpx.Client(timeout=httpx.Timeout(180.0, connect=20.0), follow_redirects=False) as client:
                    body, receipt = capture_get(client, SOURCE_URL, root, "source", max_bytes=MAX_SOURCE_BYTES)
            except Exception as exc:
                quarantine = _quarantine_attempt(staging_storage, root, version, type(exc).__name__)
                raise ValueError(f"official_mrds_source_capture_failed; quarantined_at:{quarantine}") from exc

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
                staging_storage, root, version, "official_mrds_archive_identity_or_release_drift",
            )
            raise ValueError(f"official_mrds_archive_identity_or_release_drift; quarantined_at:{quarantine}")
        return body, receipt
    finally:
        if temporary is not None:
            temporary.cleanup()


def _parquet_bytes(frame: gpd.GeoDataFrame) -> bytes:
    """Typed candidate parquet: native attributes plus Point WKB, EPSG:4326."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    crs = pyproj.CRS.from_epsg(4326).to_json_dict()
    columns = []
    schema_fields = []
    for name in EXPECTED_FIELDS:
        typ = pa.string()
        columns.append(pa.array([None if value is None else str(value) for value in frame[name]], type=typ))
        schema_fields.append(pa.field(name, typ, nullable=True))
    columns.append(pa.array([
        None if geom is None else geom.wkb
        for geom in frame.geometry
    ], type=pa.binary()))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": crs, "edges": "planar"}}}
    table = pa.Table.from_arrays(columns, schema=pa.schema(schema_fields).with_metadata(
        {b"geo": _canonical(geo)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _inspect_archive(body: bytes) -> dict[str, object]:
    expected_members = EXPECTED_ARCHIVE_MEMBERS
    with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
        names = {Path(info.filename).name for info in archive.infolist() if not info.is_dir()}
        if names != expected_members or len(archive.infolist()) != len(expected_members):
            raise ValueError("mrds_archive_member_roster_drift")
        if sum(info.file_size for info in archive.infolist()) > BOUNDED_EXTRACTION_BYTES:
            raise ValueError("mrds_archive_expansion_budget_drift")
        for info in archive.infolist():
            path = PurePosixPath(info.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("mrds_archive_unsafe_member")
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError("mrds_archive_crc_failure:" + bad_member)
        with tempfile.TemporaryDirectory(prefix="usgs-mrds-uv-") as tmp:
            archive.extractall(tmp)
            path = Path(tmp) / "mrds-trim.shp"
            frame = gpd.read_file(path, engine="pyogrio")
    fields = [name for name in frame.columns if name != frame.geometry.name]
    if fields != EXPECTED_FIELDS or str(frame.crs) != EXPECTED_CRS:
        raise ValueError("mrds_source_schema_or_crs_drift")
    if len(frame) != EXPECTED_FEATURE_COUNT:
        raise ValueError("mrds_source_population_drift")
    dep_ids = frame["DEP_ID"]
    if dep_ids.isna().any() or dep_ids.duplicated().any():
        raise ValueError("mrds_source_id_drift")
    if str(dep_ids.min()) != EXPECTED_ID_MIN or str(dep_ids.max()) != EXPECTED_ID_MAX:
        raise ValueError("mrds_source_id_range_drift")
    if any(str(frame[column].dtype) != "object" for column in EXPECTED_FIELDS):
        raise ValueError("mrds_source_attribute_physical_type_drift")
    for column in ("SITE_NAME", "DEV_STAT", "URL"):
        if frame[column].isna().any():
            raise ValueError("mrds_source_null_attribute_drift:" + column)
    if int(frame["CODE_LIST"].isna().sum()) != EXPECTED_CODE_LIST_NULL_COUNT:
        raise ValueError("mrds_source_null_population_drift:CODE_LIST")
    dev_stat_counts = {key: int(value) for key, value in frame["DEV_STAT"].value_counts().items()}
    if dev_stat_counts != EXPECTED_DEV_STAT_COUNTS:
        raise ValueError("mrds_source_dev_stat_population_drift")
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("mrds_source_geometry_quality_failure")
    if set(geometry.geom_type) != {"Point"}:
        raise ValueError("mrds_source_geometry_type_drift")
    full_bounds = [float(value) for value in frame.total_bounds]
    if full_bounds != EXPECTED_FULL_BOUNDS:
        raise ValueError("mrds_source_coverage_extent_drift")
    roster_sha = _list_sha(sorted(dep_ids.tolist()))
    if roster_sha != EXPECTED_FULL_ROSTER_SHA256:
        raise ValueError("mrds_source_id_roster_drift")
    code_lists = frame["CODE_LIST"].fillna("")
    tokens = code_lists.str.split()
    subset_mask = tokens.apply(lambda parts: any(token in SUBSET_TOKENS for token in parts))
    subset = frame[subset_mask]
    if len(subset) != EXPECTED_SUBSET_COUNT:
        raise ValueError("mrds_uv_subset_population_drift")
    subset_ids = subset["DEP_ID"]
    if subset_ids.isna().any() or subset_ids.duplicated().any():
        raise ValueError("mrds_uv_subset_id_drift")
    if _list_sha(sorted(subset_ids.tolist())) != EXPECTED_SUBSET_ROSTER_SHA256:
        raise ValueError("mrds_uv_subset_roster_drift")
    token_counts = {
        "U": int(tokens.apply(lambda parts: "U" in parts).sum()),
        "V": int(tokens.apply(lambda parts: "V" in parts).sum()),
        "both": int((tokens.apply(lambda parts: "U" in parts) & tokens.apply(lambda parts: "V" in parts)).sum()),
    }
    if token_counts != EXPECTED_SUBSET_TOKEN_COUNTS:
        raise ValueError("mrds_uv_token_count_drift")
    subset_dev_stat = {key: int(value) for key, value in subset["DEV_STAT"].value_counts().items()}
    if subset_dev_stat != EXPECTED_SUBSET_DEV_STAT_COUNTS:
        raise ValueError("mrds_uv_subset_dev_stat_drift")
    if subset[["SITE_NAME", "DEV_STAT", "URL"]].isna().any().any() or subset["CODE_LIST"].isna().any():
        raise ValueError("mrds_uv_subset_null_drift")
    records = [
        {"DEP_ID": row["DEP_ID"], "SITE_NAME": row["SITE_NAME"], "DEV_STAT": row["DEV_STAT"],
         "URL": row["URL"], "CODE_LIST": row["CODE_LIST"]}
        for _, row in subset.iterrows()
    ]
    record_sha = _list_sha(records)
    if record_sha != EXPECTED_SUBSET_RECORD_SHA256:
        raise ValueError("mrds_uv_subset_record_drift")
    geometry_records = [
        [row["DEP_ID"], float(row.geometry.x), float(row.geometry.y)]
        for _, row in subset.iterrows()
    ]
    geometry_sha = _list_sha(geometry_records)
    if geometry_sha != EXPECTED_SUBSET_GEOMETRY_SHA256:
        raise ValueError("mrds_uv_subset_geometry_drift")
    subset_bounds = [float(value) for value in subset.total_bounds]
    if subset_bounds != EXPECTED_SUBSET_BOUNDS:
        raise ValueError("mrds_uv_subset_coverage_drift")
    schema = [{"name": name, "type": "string", "nullable": bool(frame[name].isna().any())}
              for name in EXPECTED_FIELDS]
    return {
        "feature_count": len(frame), "attribute_fields": EXPECTED_FIELDS,
        "source_id_field": "DEP_ID", "source_id_sha256": roster_sha,
        "source_schema_sha256": _sha(_canonical(schema)), "source_crs": EXPECTED_CRS,
        "geometry_type": "Point", "bounds": full_bounds,
        "null_geometry_count": int(geometry.isna().sum()), "empty_geometry_count": int(geometry.is_empty.sum()),
        "invalid_geometry_count": int((~geometry.is_valid).sum()),
        "code_list_null_count": EXPECTED_CODE_LIST_NULL_COUNT,
        "dev_stat_counts": dev_stat_counts,
        "subset_count": len(subset), "subset_roster_sha256": EXPECTED_SUBSET_ROSTER_SHA256,
        "subset_record_sha256": record_sha, "subset_geometry_sha256": geometry_sha,
        "subset_bounds": subset_bounds, "subset_token_counts": token_counts,
        "subset_dev_stat_counts": subset_dev_stat,
        "subset_predicate": "exact commodity-code tokens U (uranium) and/or V (vanadium) in CODE_LIST",
        "archive_members": sorted(names),
    }


def capture_uranium_vanadium_deposits(
    staging_storage: StagingStorageResource, *, version: str, capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture one exact official MRDS digest zip and stage the U/V candidate."""
    body, receipt = _capture(capture_directory, staging_storage, version)
    audit = _inspect_archive(body)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    archive_key = f"{prefix}/shapefile/{SOURCE_FILE}"
    before = staging_storage.object_snapshot(archive_key)
    write_immutable(staging_storage, archive_key, body)
    after = staging_storage.object_snapshot(archive_key)
    if after is None or after.size != EXPECTED_BYTES or staging_storage.sha256_key(archive_key) != EXPECTED_SHA256:
        raise ValueError("staged_mrds_archive_integrity_failure")
    reused = before is not None
    code_sha256 = processing_code_identity()
    code_commit = f"{CODE_IDENTITY_SCHEME}:{code_sha256}"
    processing_code = {
        "scheme": CODE_IDENTITY_SCHEME,
        "sha256": code_sha256,
        "runtime_versions": _processing_runtime(),
    }
    with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
        with tempfile.TemporaryDirectory(prefix="usgs-mrds-uv-stage-") as tmp:
            archive.extract("mrds-trim.shp", tmp)
            archive.extract("mrds-trim.shx", tmp)
            archive.extract("mrds-trim.dbf", tmp)
            archive.extract("mrds-trim.prj", tmp)
            frame = gpd.read_file(Path(tmp) / "mrds-trim.shp", engine="pyogrio")
    frame = frame.sort_values("DEP_ID", key=lambda series: series.astype(int))
    tokens = frame["CODE_LIST"].fillna("").str.split()
    subset = frame[tokens.apply(lambda parts: any(token in SUBSET_TOKENS for token in parts))]
    parquet = _parquet_bytes(subset)
    observed_at = receipt.get("observed_at")
    manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": code_commit,
        "processing_code": processing_code,
        "title": "Uranium and Vanadium Deposits",
        "description": "Token-exact Uranium (U) / Vanadium (V) subset of the complete USGS MRDS "
                       "point digest (mrds-trim), staged as an unapproved candidate. The subset "
                       "predicate, the digest-to-HIFLD equivalence, and all release and rights "
                       "decisions remain pending.",
        "publisher": PUBLISHER,
        "agency": "U.S. Geological Survey",
        "source_url": SOURCE_URL,
        "license": "U.S. government work; source license and downstream redistribution terms require maintainer review.",
        "source_modified": "2018-04-06",
        "date_issued": None,
        "inputs": [{
            "input_id": SOURCE_URL,
            "producer": PUBLISHER,
            "host": urlsplit(SOURCE_URL).hostname,
            "canonical_url": SOURCE_URL,
            "retrieval_url": receipt["final_url"],
            "retrieved_at": observed_at,
            "sha256": EXPECTED_SHA256,
            "bytes": EXPECTED_BYTES,
            "license": "https://www.usa.gov/government-works",
            "attribution": PUBLISHER,
            "role": "producer_publication",
            "release_date_unknown_reason": "The producer states systematic MRDS updates ceased as of 2011 and "
                "publishes this frozen database without a per-package release date; the HTTP Last-Modified "
                "(2018-04-06) and the packaged FGDC metadata are retained as packaging evidence, not an "
                "accepted release date.",
        }],
        "transformations": ["Preserve the complete source ZIP (all six members, including the packaged FGDC "
                            "metadata) losslessly; stage the token-exact Uranium/Vanadium subset of the digest "
                            "as the unapproved candidate without ID synthesis, attribute mapping or geometry repair."],
        "lineage": "Official U.S. Geological Survey MRDS publication (https://mrdata.usgs.gov/mrds/), the "
                   "maintained database that subsumes the original MRDS and MAS/MILS datasets named in the "
                   "HIFLD Open inventory row for uranium-and-vanadium-deposits. The DHS HIFLD Open crosswalk "
                   "(DOI 10.3886/E241367V1) records this producer relationship; the retired HIFLD Open item "
                   "2c9b52ce91be4384931b64dd551cb6ea and its services1.arcgis.com FeatureServer (now answering "
                   "only an ArcGIS 'Invalid URL' error document) are historical context. The DataLumos record "
                   "240898 is identification context only, never a pipeline input.",
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
    audit_doc = {"source": {"url": SOURCE_URL, "release_modified": "2018-04-06",
                            "archive_sha256": EXPECTED_SHA256, "archive_bytes": EXPECTED_BYTES},
                 "processing_code": processing_code, "acquisition_receipt": stable_receipt, **audit}
    audit_key = f"{prefix}/metadata/source_audit.json"
    write_immutable(staging_storage, audit_key, _canonical(audit_doc) + b"\n")
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    parquet_key = f"{prefix}/{parquet_path}"
    write_immutable(staging_storage, parquet_key, parquet)
    snapshot = staging_storage.object_snapshot(parquet_key)
    if snapshot is None or snapshot.size != len(parquet) or staging_storage.sha256_key(parquet_key) != _sha(parquet):
        raise ValueError("staged_uv_candidate_readback_failure")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["subset_count"], "source_population_complete": True,
        "complete_digest_feature_count": audit["feature_count"],
        "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "subset_roster_sha256": audit["subset_roster_sha256"],
        "subset_record_sha256": audit["subset_record_sha256"],
        "subset_geometry_sha256": audit["subset_geometry_sha256"],
        "source_capture_evidence_sha256": receipt["sha256"], "source_archive_bytes": EXPECTED_BYTES,
        "source_archive_sha256": EXPECTED_SHA256, "source_cache_reused": reused,
        "candidate_path": parquet_key,
        "code_commit": code_commit,
        "code_identity_scheme": CODE_IDENTITY_SCHEME, "code_sha256": code_sha256,
        "processing_runtime_versions": processing_code["runtime_versions"],
        "source_id_field": "DEP_ID",
        "source_coverage_scope": "Token-exact Uranium/Vanadium subset of the complete USGS MRDS worldwide "
                                 "point digest; the proposed subset predicate remains an unapproved draft contract.",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "written_keys": [archive_key, manifest_key, audit_key, parquet_key],
    }
