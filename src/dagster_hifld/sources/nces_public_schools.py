"""Candidate-only acquisition of official NCES EDGE public-school locations."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import geopandas as gpd
import httpx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "public-schools"
FILE_SLUG = "public-schools"
PUBLISHER = "National Center for Education Statistics (NCES), U.S. Department of Education"
SOURCE_URL = "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip"
SOURCE_FILE = "EDGE_GEOCODE_PUBLICSCH_2425.zip"
SOURCE_VINTAGE = "2024-2025"
EXPECTED_BYTES = 30_448_565
EXPECTED_SHA256 = "9dc51ecd5cdf6e38bdb679f7ac95e2bf73ee3fb48718b8612cf830800cffb288"
EXPECTED_FEATURE_COUNT = 102_178
EXPECTED_ID_SHA256 = "2fa37754051a457c280f089eba68d6b42b54dd680780e00f6d56bd103e6c02a3"
EXPECTED_FIELDS = [
    "NCESSCH", "LEAID", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP",
    "STFIP", "CNTY", "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA",
    "CBSATYPE", "CSA", "NMCSA", "CD", "SLDL", "SLDU", "SCHOOLYEAR",
]
EXPECTED_STRING_FIELDS = [name for name in EXPECTED_FIELDS if name not in {"LAT", "LON"}]
EXPECTED_BOUNDS = (-174.198594, -14.348924, 145.784430, 71.300337)
MAX_SOURCE_BYTES = 40_000_000
MAX_OUTER_UNCOMPRESSED_BYTES = 120_000_000
MAX_INNER_UNCOMPRESSED_BYTES = 70_000_000
CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}
OUTER_MEMBERS = {
    "Shapefile_SCH.zip", "EDGE_GEOCODE_PUBLICSCH_2425.xlsx",
    "EDGE_GEOCODE_PUBLICSCH_2425.TXT", "EDGE_GEOCODE_PUBLICSCH_2425.sas7bdat",
}
INNER_MEMBERS = {
    "EDGE_GEOCODE_PUBLICSCH_2425.shx", "EDGE_GEOCODE_PUBLICSCH_2425.cpg",
    "EDGE_GEOCODE_PUBLICSCH_2425.dbf", "EDGE_GEOCODE_PUBLICSCH_2425.NCESSCH.atx",
    "EDGE_GEOCODE_PUBLICSCH_2425.prj", "EDGE_GEOCODE_PUBLICSCH_2425.sbn",
    "EDGE_GEOCODE_PUBLICSCH_2425.sbx", "EDGE_GEOCODE_PUBLICSCH_2425.shp",
    "EDGE_GEOCODE_PUBLICSCH_2425.shp.xml",
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _file_descriptor(body: bytes) -> dict[str, object]:
    return {"bytes": len(body), "sha256": _sha(body)}


def _safe_zip_roster(archive: zipfile.ZipFile, expected: set[str], limit: int,
                     reason: str) -> list[zipfile.ZipInfo]:
    infos = archive.infolist()
    names = [info.filename for info in infos if not info.is_dir()]
    if (len(names) != len(expected) or set(names) != expected
            or sum(info.file_size for info in infos) > limit):
        raise ValueError(reason)
    for info in infos:
        path = PurePosixPath(info.filename)
        if (info.is_dir() or path.is_absolute() or ".." in path.parts
                or path.as_posix() != info.filename or len(path.parts) != 1
                or info.file_size < 0 or info.file_size > limit):
            raise ValueError("nces_archive_unsafe_member")
    if archive.testzip() is not None:
        raise ValueError("nces_archive_crc_failure")
    return infos


def _validate_frame(frame: gpd.GeoDataFrame, *, expected_count: int,
                    expected_id_sha256: str,
                    expected_bounds: tuple[float, float, float, float] | None = None) -> dict[str, object]:
    """Fail closed on source population, native types, CRS, IDs or point drift."""
    if not isinstance(frame, gpd.GeoDataFrame) or list(frame.columns) != [*EXPECTED_FIELDS, "geometry"]:
        raise ValueError("nces_public_schools_field_roster_drift")
    if any(str(frame[name].dtype) != "object" for name in EXPECTED_STRING_FIELDS):
        raise ValueError("nces_public_schools_string_field_type_drift")
    if any(str(frame[name].dtype) != "float64" for name in ("LAT", "LON")):
        raise ValueError("nces_public_schools_coordinate_field_type_drift")
    if len(frame) != expected_count or frame[EXPECTED_FIELDS].isna().any().any():
        raise ValueError("nces_public_schools_population_or_attribute_null_drift")
    try:
        crs = CRS.from_user_input(frame.crs)
    except Exception as exc:
        raise ValueError("nces_public_schools_crs_drift") from exc
    if not crs.equals(CRS.from_epsg(4326), ignore_axis_order=True):
        raise ValueError("nces_public_schools_crs_drift")
    ids = frame["NCESSCH"].tolist()
    if (any(not isinstance(value, str) or len(value) != 12 for value in ids)
            or len(set(ids)) != expected_count or ids != sorted(ids)
            or _sha(_canonical(ids)) != expected_id_sha256):
        raise ValueError("nces_public_schools_id_roster_drift")
    if set(frame["SCHOOLYEAR"].unique()) != {SOURCE_VINTAGE}:
        raise ValueError("nces_public_schools_vintage_drift")
    geometry = frame.geometry
    if (geometry.isna().any() or geometry.is_empty.any()
            or (~geometry.is_valid).any()
            or geometry.has_z.any()
            or set(geometry.geom_type.unique()) != {"Point"}):
        raise ValueError("nces_public_schools_geometry_drift")
    x = geometry.x.to_numpy(dtype=np.float64)
    y = geometry.y.to_numpy(dtype=np.float64)
    lon = frame["LON"].to_numpy(dtype=np.float64)
    lat = frame["LAT"].to_numpy(dtype=np.float64)
    if (not np.isfinite(x).all() or not np.isfinite(y).all()
            or np.max(np.abs(x - lon)) > 1e-12
            or np.max(np.abs(y - lat)) > 1e-12
            or np.any(np.abs(x) > 180) or np.any(np.abs(y) > 90)):
        raise ValueError("nces_public_schools_point_coordinate_drift")
    bounds = tuple(float(value) for value in geometry.total_bounds)
    if expected_bounds is not None and any(abs(actual - expected) > 1e-6
                                            for actual, expected in zip(bounds, expected_bounds)):
        raise ValueError("nces_public_schools_coverage_bounds_drift")

    record_hash = hashlib.sha256()
    geometry_hash = hashlib.sha256()
    values = {name: frame[name].tolist() for name in EXPECTED_FIELDS}
    for index, school_id in enumerate(ids):
        row = {name: (float(values[name][index]) if name in {"LAT", "LON"}
                      else values[name][index]) for name in EXPECTED_FIELDS}
        record_hash.update(_canonical(row) + b"\n")
        geometry_hash.update(_canonical([school_id, float(x[index]), float(y[index])]) + b"\n")
    schema = {
        "attributes": [{"name": name, "physical_type": "float64" if name in {"LAT", "LON"} else "string"}
                       for name in EXPECTED_FIELDS],
        "geometry_column": "geometry", "geometry_type": "Point", "geometry_encoding": "WKB",
        "crs": "EPSG:4326", "dimensions": 2,
    }
    return {
        "source_feature_count": expected_count,
        "source_id_sha256": _sha(_canonical(ids)),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_record_sha256": record_hash.hexdigest(),
        "source_geometry_sha256": geometry_hash.hexdigest(),
        "source_schema": schema,
        "source_schema_sha256": _sha(_canonical(schema)),
        "source_bounds": list(bounds),
        "source_field_null_counts": {name: 0 for name in EXPECTED_FIELDS},
        "point_coordinate_max_abs_difference": float(max(np.max(np.abs(x - lon)), np.max(np.abs(y - lat)))),
    }


def _validate_text_table(body: bytes, frame: gpd.GeoDataFrame) -> None:
    """Cross-check all released text rows and values against the shapefile table."""
    try:
        text = body.decode("utf-8")
        rows = csv.reader(io.StringIO(text, newline=""), delimiter="|")
        values = {name: frame[name].tolist() for name in EXPECTED_FIELDS}
        for index, row in enumerate(rows):
            if index >= len(frame) or len(row) != len(EXPECTED_FIELDS):
                raise ValueError("nces_public_schools_text_population_drift")
            for column, name in enumerate(EXPECTED_FIELDS):
                expected = values[name][index]
                if name in {"LAT", "LON"}:
                    try:
                        equal = math.isclose(float(row[column]), float(expected), rel_tol=0, abs_tol=1e-12)
                    except ValueError:
                        equal = False
                else:
                    equal = row[column] == expected
                if not equal:
                    raise ValueError("nces_public_schools_text_attribute_drift:" + name)
        if index + 1 != len(frame):
            raise ValueError("nces_public_schools_text_population_drift")
    except UnicodeDecodeError as exc:
        raise ValueError("nces_public_schools_text_encoding_drift") from exc


def _parquet_bytes(frame: gpd.GeoDataFrame) -> bytes:
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False, compression="zstd", schema_version="1.1.0",
                     geometry_encoding="WKB")
    body = stream.getvalue()
    table = pq.read_table(io.BytesIO(body))
    metadata = table.schema.metadata or {}
    try:
        geo = json.loads(metadata[b"geo"])
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("nces_public_schools_geoparquet_metadata_missing") from exc
    if (geo.get("primary_column") != "geometry"
            or geo.get("columns", {}).get("geometry", {}).get("encoding") != "WKB"
            or geo.get("columns", {}).get("geometry", {}).get("geometry_types") != ["Point"]
            or not CRS.from_json_dict(geo["columns"]["geometry"]["crs"]).equals(CRS.from_epsg(4326), ignore_axis_order=True)):
        raise ValueError("nces_public_schools_geoparquet_metadata_drift")
    roundtrip = gpd.read_parquet(io.BytesIO(body))
    if list(roundtrip.columns) != [*EXPECTED_FIELDS, "geometry"] or len(roundtrip) != len(frame):
        raise ValueError("nces_public_schools_geoparquet_row_or_field_drift")
    for name in EXPECTED_FIELDS:
        if str(roundtrip[name].dtype) != str(frame[name].dtype) or not roundtrip[name].equals(frame[name]):
            raise ValueError("nces_public_schools_geoparquet_attribute_drift:" + name)
    if (not CRS.from_user_input(roundtrip.crs).equals(CRS.from_epsg(4326), ignore_axis_order=True)
            or not np.array_equal(roundtrip.geometry.x.to_numpy(), frame.geometry.x.to_numpy())
            or not np.array_equal(roundtrip.geometry.y.to_numpy(), frame.geometry.y.to_numpy())):
        raise ValueError("nces_public_schools_geoparquet_geometry_drift")
    return body


def _inspect_archive(body: bytes, *, expected_count: int = EXPECTED_FEATURE_COUNT,
                     expected_id_sha256: str = EXPECTED_ID_SHA256,
                     expected_bounds: tuple[float, float, float, float] | None = EXPECTED_BOUNDS) -> tuple[gpd.GeoDataFrame, dict[str, object], dict[str, object]]:
    if len(body) != EXPECTED_BYTES or _sha(body) != EXPECTED_SHA256:
        raise ValueError("nces_public_schools_archive_identity_or_release_drift")
    try:
        outer = zipfile.ZipFile(io.BytesIO(body))
        outer_infos = _safe_zip_roster(outer, OUTER_MEMBERS, MAX_OUTER_UNCOMPRESSED_BYTES,
                                       "nces_public_schools_outer_archive_roster_drift")
        inner_body = outer.read("Shapefile_SCH.zip")
        text_body = outer.read("EDGE_GEOCODE_PUBLICSCH_2425.TXT")
        inner = zipfile.ZipFile(io.BytesIO(inner_body))
        inner_infos = _safe_zip_roster(inner, INNER_MEMBERS, MAX_INNER_UNCOMPRESSED_BYTES,
                                       "nces_public_schools_shapefile_archive_roster_drift")
    except zipfile.BadZipFile as exc:
        raise ValueError("nces_public_schools_archive_invalid") from exc
    with tempfile.TemporaryDirectory(prefix="nces-public-schools-") as temporary:
        root = Path(temporary)
        for info in inner_infos:
            (root / info.filename).write_bytes(inner.read(info.filename))
        path = root / "EDGE_GEOCODE_PUBLICSCH_2425.shp"
        frame = gpd.read_file(path, engine="pyogrio")
    audit = _validate_frame(frame, expected_count=expected_count,
                            expected_id_sha256=expected_id_sha256,
                            expected_bounds=expected_bounds)
    _validate_text_table(text_body, frame)
    roster = {
        "outer": {info.filename: {"bytes": info.file_size, "sha256": _sha(outer.read(info.filename))}
                  for info in outer_infos},
        "shapefile": {info.filename: {"bytes": info.file_size, "sha256": _sha(inner.read(info.filename))}
                      for info in inner_infos},
    }
    audit.update({
        "official_filename": SOURCE_FILE,
        "outer_archive_members": sorted(roster["outer"]),
        "shapefile_archive_members": sorted(roster["shapefile"]),
        "text_table_row_count": expected_count,
        "text_table_all_attributes_match_shapefile": True,
        "source_population_complete": True,
    })
    return frame, audit, roster


def _normalize_retained_receipt(receipt: dict[str, object], body: bytes, wire: bytes) -> dict[str, object]:
    """Normalize the prior capture receipt while preserving its original bytes separately."""
    if "body_bytes" in receipt:
        headers = receipt.get("headers")
        if not isinstance(headers, dict):
            raise ValueError("nces_public_schools_retained_receipt_invalid")
        observed_at = receipt.get("captured_at_utc")
        normalized = {
            "method": receipt.get("method"), "url": receipt.get("requested_url"),
            "final_url": receipt.get("final_url"), "observed_at": observed_at,
            "status": receipt.get("status"), "complete_body": receipt.get("status") == 200,
            "bytes": receipt.get("body_bytes"), "sha256": receipt.get("body_sha256"),
            "wire_bytes": receipt.get("wire_bytes"), "wire_sha256": receipt.get("wire_sha256"),
            "content_encoding": receipt.get("content_encoding"),
            "content_length": int(headers["content-length"]) if headers.get("content-length") else None,
            "last_modified": headers.get("last-modified"),
        }
    else:
        normalized = receipt
    if (normalized.get("method") != "GET" or normalized.get("url") != SOURCE_URL
            or normalized.get("final_url") != SOURCE_URL or normalized.get("status") != 200
            or normalized.get("complete_body") is not True
            or normalized.get("bytes") != len(body) or normalized.get("sha256") != _sha(body)
            or normalized.get("wire_bytes") != len(wire) or normalized.get("wire_sha256") != _sha(wire)
            or normalized.get("content_encoding") != "identity"):
        raise ValueError("nces_public_schools_capture_receipt_mismatch")
    return normalized


def _capture(capture_directory: str | Path | None,
             storage: StagingStorageResource) -> tuple[Path, dict[str, bytes], dict[str, object]]:
    temporary = tempfile.TemporaryDirectory(prefix="nces-public-schools-capture-") if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir() or {path.name for path in root.iterdir()} != CAPTURE_FILES:
                raise ValueError("nces_public_schools_capture_roster_mismatch")
            paths = {name: root / name for name in CAPTURE_FILES}
            if any(path.is_symlink() or not path.is_file() for path in paths.values()):
                raise ValueError("nces_public_schools_capture_nonregular_file")
            artifacts = {name: path.read_bytes() for name, path in paths.items()}
            raw_receipt = json.loads(artifacts["source.receipt.json"])
            receipt = _normalize_retained_receipt(raw_receipt, artifacts["source.body"], artifacts["source.wire"])
        else:
            try:
                with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=15)) as client:
                    _body, receipt = capture_get(client, SOURCE_URL, root, "source", max_bytes=MAX_SOURCE_BYTES)
            except Exception as exc:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG,
                    "v" + EXPECTED_SHA256[:32], {}, type(exc).__name__)
                raise ValueError("nces_public_schools_source_capture_failed") from exc
            artifacts = {name: (root / name).read_bytes() for name in CAPTURE_FILES}
            raw_receipt = json.loads(artifacts["source.receipt.json"])
        body, wire = artifacts["source.body"], artifacts["source.wire"]
        if (len(body) != EXPECTED_BYTES or _sha(body) != EXPECTED_SHA256
                or len(wire) != EXPECTED_BYTES or _sha(wire) != EXPECTED_SHA256
                or receipt.get("content_length") not in (None, EXPECTED_BYTES)):
            raise ValueError("nces_public_schools_archive_identity_or_release_drift")
        return root, artifacts, {**receipt, "raw_receipt_sha256": _sha(artifacts["source.receipt.json"])}
    except Exception as exc:
        if root.is_dir() and (root / "source.wire").is_file() and (root / "source.receipt.json").is_file():
            try:
                body = (root / "source.body").read_bytes()
                receipt_raw = json.loads((root / "source.receipt.json").read_bytes())
                wire = (root / "source.wire").read_bytes()
                normalized = _normalize_retained_receipt(receipt_raw, body, wire)
                quarantine_incomplete_capture(storage, root, DATASET_SLUG,
                    "v" + EXPECTED_SHA256[:32], {"source": (body, normalized)}, type(exc).__name__)
            except Exception:
                pass
        if isinstance(exc, ValueError):
            raise
        raise ValueError("nces_public_schools_capture_invalid") from exc
    finally:
        if temporary is not None:
            temporary.cleanup()


def _write_outputs(storage: StagingStorageResource, prefix: str,
                   outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        before = storage.object_snapshot(key)
        if (before is None or before.size != len(body)
                or storage.sha256_key(key) != _sha(body)
                or storage.object_snapshot(key) != before):
            raise ValueError("nces_public_schools_candidate_object_readback_failed:" + name)
        descriptors[name] = _file_descriptor(body)
    return descriptors


def capture_nces_public_schools(
    staging_storage: StagingStorageResource,
    *,
    capture_directory: str | Path | None = None,
) -> dict[str, object]:
    """Capture or replay one complete official NCES source release into staging."""
    root, artifacts, receipt = _capture(capture_directory, staging_storage)
    body = artifacts["source.body"]
    try:
        frame, audit, archive_roster = _inspect_archive(body)
        parquet = _parquet_bytes(frame)
    except Exception as exc:
        quarantine_incomplete_capture(staging_storage, root, DATASET_SLUG,
            "v" + EXPECTED_SHA256[:32], {"source": (body, receipt)}, type(exc).__name__)
        raise

    identity = processing_identity(Path(__file__).parents[3])
    capture_evidence = {
        "schema_version": 1,
        "complete": True,
        "source_url": SOURCE_URL,
        "source_vintage": SOURCE_VINTAGE,
        "observed_at": receipt["observed_at"],
        "receipt": receipt,
        "archive_roster": archive_roster,
        "raw_artifacts": {
            name: {"bytes": len(artifacts[name]), "sha256": _sha(artifacts[name])}
            for name in sorted(CAPTURE_FILES)
        },
    }
    capture_sha = _sha(_canonical(capture_evidence))
    audit.update({
        "source_capture_sha256": capture_sha,
        "source_archive_sha256": _sha(body),
        "source_archive_bytes": len(body),
        "source_vintage": SOURCE_VINTAGE,
        "distribution_last_modified": receipt.get("last_modified"),
        "coverage_scope": "The complete NCES EDGE public-school 2024-25 shapefile within the official distribution; exact equivalence to the archived HIFLD 2022-23 layer is unverified.",
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "baseline_comparison_accepted": False,
        "release_accepted": False,
    })
    schema_sha = audit["source_schema_sha256"]
    fingerprint = _sha(_canonical({
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": schema_sha,
        "source_capture_sha256": capture_sha,
        "processing_identity": identity,
    }))
    version = "v" + fingerprint[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    inputs = [{
        "input_id": "nces_edge_public_schools_2024_25",
        "producer": PUBLISHER,
        "host": urlsplit(SOURCE_URL).hostname,
        "role": "producer_publication",
        "canonical_url": SOURCE_URL,
        "retrieval_url": SOURCE_URL,
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "license": "NCES states the information in its 2024-25 public-school locations layer is in the public domain.",
        "attribution": "National Center for Education Statistics, U.S. Department of Education.",
        "release_date": None,
        "release_date_unknown_reason": "The 2024-25 label identifies the CCD school-year vintage; the archive Last-Modified header is server file metadata, not a producer release date.",
    }]
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Public School Locations (NCES 2024-25)",
        "description": "Complete 2024-25 NCES EDGE public-school location archive extraction, proposed as a current producer-source candidate for the HIFLD Public Schools inventory. Historical HIFLD equivalence and the published contract remain unapproved.",
        "publisher": PUBLISHER,
        "agency": "U.S. Department of Education, Institute of Education Sciences, National Center for Education Statistics",
        "source_url": SOURCE_URL,
        "license": "Public domain claim stated in the NCES 2024-25 layer description; HIFLD reuse and compatibility decisions remain pending.",
        "source_modified": receipt.get("last_modified"),
        "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "inputs": inputs,
        "transformations": [{
            "operation": "NCES shapefile point layer to GeoParquet candidate",
            "parameters": {
                "source_vintage": SOURCE_VINTAGE,
                "source_fields": "All 23 shapefile attributes retained in source order and physical types.",
                "source_id": "NCESSCH retained as 12-character string; no substitute identifiers generated.",
                "geometry": "All source Point XY coordinates preserved as EPSG:4326 WKB; no reprojection, snapping or repair.",
                "completeness": "All 102,178 shapefile rows and IDs validated; complete tabular TXT member cross-checked against all 23 shapefile attributes.",
                "candidate_status": "Unapproved source proposal; no HIFLD historical equivalence is asserted.",
            },
        }],
        "lineage": "NCES EDGE publishes annual public-school geocodes based on the Common Core of Data. DataLumos metadata for the archived HIFLD Open Public Schools release describes 2022-23 CCD data. This proposed NCES 2024-25 source is newer and shares the NCES producer, but no feature-by-feature historical crosswalk or accepted replacement decision is established.",
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "All source attributes, NCESSCH identifiers and native point geometries are retained in an unapproved candidate.",
                "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet",
                             "bytes": len(parquet), "sha256": _sha(parquet)}],
            }},
        },
        "unsupported_output_formats": {
            "geopackage": "No accepted HIFLD-compatible schema and delivery contract.",
            "pmtiles": "No approved display or generalization policy.",
            "shapefile_zip": "No approved published field and identifier contract.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {
        f"geoparquet/{FILE_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({
            "approved": False, "version": version,
            "objects": {f"geoparquet/{FILE_SLUG}.parquet": _file_descriptor(parquet)},
            "feature_count": audit["source_feature_count"],
            "source_population_complete": True,
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "baseline_comparison_accepted": False,
            "release_accepted": False,
        }) + b"\n",
        "raw/capture_manifest.json": _canonical(capture_evidence) + b"\n",
        "raw/source.body": artifacts["source.body"],
        "raw/source.wire": artifacts["source.wire"],
        # Keep the complete legacy receipt byte-for-byte and separately write
        # the normalized receipt expected by the shared cache verifier.
        "raw/source.receipt.json": _canonical(receipt) + b"\n",
        "raw/source.original_receipt.json": artifacts["source.receipt.json"],
    }
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts = {"source": receipt}
    if staging_storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(staging_storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or original.get("capture_receipts") != receipts:
            raise ValueError("nces_public_schools_cached_receipt_identity_mismatch")
        if staging_storage.read_key(f"{prefix}/raw/source.body") != body:
            raise ValueError("nces_public_schools_cached_source_bytes_changed")
        verify_cached_candidate(staging_storage, Path(__file__).parents[3], state_key,
            prefix, version, fingerprint, identity, audit["source_record_sha256"],
            audit["source_schema_sha256"], original.get("objects", {}), receipts)
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"], "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_capture_sha256": capture_sha,
            "source_coverage_scope": audit["coverage_scope"],
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "baseline_comparison_accepted": False,
            "release_accepted": False,
        }
    descriptors = _write_outputs(staging_storage, prefix, outputs)
    seal_ready_candidate(staging_storage, state_key, version, fingerprint, {
        "processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts,
        "objects": descriptors,
    })
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_sha,
        "source_coverage_scope": audit["coverage_scope"],
        "candidate_ready": f"{prefix}/metadata/candidate_ready.json", "reused": False,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "baseline_comparison_accepted": False,
        "release_accepted": False,
    }
