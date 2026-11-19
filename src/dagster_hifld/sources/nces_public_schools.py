"""Candidate-only acquisition of the newest published NCES EDGE public schools."""
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
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.nces_edge_common import (
    LANDING,
    NcesEdgeReleaseError,
    dbf_record_count,
    discover_release,
    release_from_url,
    shapefile_index_count,
    shapefile_record_count,
)

DATASET_SLUG = "public-schools"
FILE_SLUG = "public-schools"
PRODUCT = "PUBLICSCH"
PUBLISHER = "National Center for Education Statistics (NCES), U.S. Department of Education"
EXPECTED_FIELDS = [
    "NCESSCH", "LEAID", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP",
    "STFIP", "CNTY", "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA",
    "CBSATYPE", "CSA", "NMCSA", "CD", "SLDL", "SLDU", "SCHOOLYEAR",
]
EXPECTED_STRING_FIELDS = [name for name in EXPECTED_FIELDS if name not in {"LAT", "LON"}]
MAX_SOURCE_BYTES = 40_000_000
MAX_LANDING_BYTES = 2_000_000
MAX_OUTER_UNCOMPRESSED_BYTES = 120_000_000
MAX_INNER_UNCOMPRESSED_BYTES = 70_000_000
REPLAY_FILES = {"source.body", "source.wire", "source.receipt.json"}


def _members(stem: str) -> tuple[set[str], set[str]]:
    outer = {"Shapefile_SCH.zip", f"{stem}.xlsx", f"{stem}.TXT", f"{stem}.sas7bdat"}
    inner = {
        f"{stem}.shx", f"{stem}.cpg", f"{stem}.dbf", f"{stem}.NCESSCH.atx",
        f"{stem}.prj", f"{stem}.sbn", f"{stem}.sbx", f"{stem}.shp", f"{stem}.shp.xml",
    }
    return outer, inner


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
            raise ValueError("nces_public_schools_archive_unsafe_member")
    if archive.testzip() is not None:
        raise ValueError("nces_public_schools_archive_crc_failure")
    return infos


def _validate_frame(frame: gpd.GeoDataFrame, *, schoolyear: str,
                    expected_count: int) -> dict[str, object]:
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
            or len(set(ids)) != expected_count):
        raise ValueError("nces_public_schools_id_roster_drift")
    if set(frame["SCHOOLYEAR"].unique()) != {schoolyear}:
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


def _validate_text_table(body: bytes, frame: gpd.GeoDataFrame) -> int:
    """Cross-check every released text row against the shapefile table."""
    try:
        text = body.decode("utf-8")
        rows = csv.reader(io.StringIO(text, newline=""), delimiter="|")
        values = {name: frame[name].tolist() for name in EXPECTED_FIELDS}
        index = -1
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
        return index + 1
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


def _inspect_archive(body: bytes, release: dict[str, object]) -> tuple[gpd.GeoDataFrame, dict[str, object], dict[str, object]]:
    stem = str(release["stem"])
    outer_members, inner_members = _members(stem)
    try:
        outer = zipfile.ZipFile(io.BytesIO(body))
        outer_infos = _safe_zip_roster(outer, outer_members, MAX_OUTER_UNCOMPRESSED_BYTES,
                                       "nces_public_schools_outer_archive_roster_drift")
        inner_body = outer.read("Shapefile_SCH.zip")
        text_body = outer.read(f"{stem}.TXT")
        inner = zipfile.ZipFile(io.BytesIO(inner_body))
        inner_infos = _safe_zip_roster(inner, inner_members, MAX_INNER_UNCOMPRESSED_BYTES,
                                       "nces_public_schools_shapefile_archive_roster_drift")
    except zipfile.BadZipFile as exc:
        raise ValueError("nces_public_schools_archive_invalid") from exc
    dbf_count = dbf_record_count(inner.read(f"{stem}.dbf"), "nces_public_schools")
    shx_count = shapefile_index_count(inner.read(f"{stem}.shx"), "nces_public_schools")
    shp_count = shapefile_record_count(inner.read(f"{stem}.shp"), "nces_public_schools")
    if not dbf_count == shx_count == shp_count:
        raise ValueError("nces_public_schools_source_record_count_disagreement")
    with tempfile.TemporaryDirectory(prefix="nces-public-schools-") as temporary:
        root = Path(temporary)
        for info in inner_infos:
            (root / info.filename).write_bytes(inner.read(info.filename))
        frame = gpd.read_file(root / f"{stem}.shp", engine="pyogrio")
    audit = _validate_frame(frame, schoolyear=str(release["schoolyear"]),
                            expected_count=dbf_count)
    text_count = _validate_text_table(text_body, frame)
    if text_count != dbf_count:
        raise ValueError("nces_public_schools_source_record_count_disagreement")
    roster = {
        "outer": {info.filename: {"bytes": info.file_size, "sha256": _sha(outer.read(info.filename))}
                  for info in outer_infos},
        "shapefile": {info.filename: {"bytes": info.file_size, "sha256": _sha(inner.read(info.filename))}
                      for info in inner_infos},
    }
    audit.update({
        "official_filename": f"{stem}.zip",
        "outer_archive_members": sorted(roster["outer"]),
        "shapefile_archive_members": sorted(roster["shapefile"]),
        "source_counts": {
            "dbf": dbf_count, "shx": shx_count, "shp": shp_count,
            "decoded": len(frame), "text": text_count,
        },
        "text_table_row_count": text_count,
        "text_table_all_attributes_match_shapefile": True,
        "schoolyear": release["schoolyear"],
        "publication_selection": release,
        "source_population_complete": True,
    })
    return frame, audit, roster


def _normalize_retained_receipt(receipt: dict[str, object], body: bytes, wire: bytes,
                                release: dict[str, object]) -> dict[str, object]:
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
    url = release["source_url"]
    if (normalized.get("method") != "GET" or normalized.get("url") != url
            or normalized.get("final_url") != url or normalized.get("status") != 200
            or normalized.get("complete_body") is not True
            or normalized.get("bytes") != len(body) or normalized.get("sha256") != _sha(body)
            or normalized.get("wire_bytes") != len(wire) or normalized.get("wire_sha256") != _sha(wire)
            or normalized.get("content_encoding") != "identity"):
        raise ValueError("nces_public_schools_capture_receipt_mismatch")
    return normalized


def _quarantine_capture(storage: StagingStorageResource, artifacts: dict[str, bytes],
                        receipt: dict[str, object], reason: str) -> None:
    """Retain refusal evidence even after the acquisition workspace is cleaned up."""
    with tempfile.TemporaryDirectory(prefix="nces-public-schools-quarantine-") as temporary:
        root = Path(temporary)
        (root / "source.wire").write_bytes(artifacts["source.wire"])
        prefix = quarantine_incomplete_capture(storage, root, DATASET_SLUG,
            "v" + _sha(artifacts["source.body"])[:32],
            {"source": (artifacts["source.body"], receipt)}, reason)
        write_immutable(storage, f"{prefix}/raw/source.original_receipt.json",
                        artifacts["source.receipt.json"])


def _capture(capture_directory: str | Path | None,
             storage: StagingStorageResource) -> tuple[dict[str, bytes], dict[str, object], dict[str, object]]:
    temporary = tempfile.TemporaryDirectory(prefix="nces-public-schools-capture-") if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir() or {path.name for path in root.iterdir()} != REPLAY_FILES:
                raise ValueError("nces_public_schools_capture_roster_mismatch")
            paths = {name: root / name for name in REPLAY_FILES}
            if any(path.is_symlink() or not path.is_file() for path in paths.values()):
                raise ValueError("nces_public_schools_capture_nonregular_file")
            artifacts = {name: path.read_bytes() for name, path in paths.items()}
            raw_receipt = json.loads(artifacts["source.receipt.json"])
            url = raw_receipt.get("url") or raw_receipt.get("requested_url")
            try:
                release = release_from_url(str(url), PRODUCT)
            except NcesEdgeReleaseError as exc:
                raise ValueError("nces_public_schools_" + str(exc)) from exc
            receipt = _normalize_retained_receipt(raw_receipt, artifacts["source.body"], artifacts["source.wire"], release)
        else:
            try:
                with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=15)) as client:
                    landing, _landing_receipt = capture_get(client, LANDING, root, "landing", max_bytes=MAX_LANDING_BYTES)
                    try:
                        release = discover_release(landing, PRODUCT)
                    except NcesEdgeReleaseError as exc:
                        raise ValueError("nces_public_schools_" + str(exc)) from exc
                    _body, receipt = capture_get(client, str(release["source_url"]), root, "source", max_bytes=MAX_SOURCE_BYTES)
                    landing_after, _after_receipt = capture_get(client, LANDING, root, "landing_after", max_bytes=MAX_LANDING_BYTES)
                    try:
                        if discover_release(landing_after, PRODUCT) != release:
                            raise ValueError("nces_public_schools_published_release_changed_during_capture")
                    except NcesEdgeReleaseError as exc:
                        raise ValueError("nces_public_schools_" + str(exc)) from exc
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError("nces_public_schools_source_capture_failed") from exc
            artifacts = {name: (root / name).read_bytes() for name in REPLAY_FILES}
            raw_receipt = json.loads(artifacts["source.receipt.json"])
        body, wire = artifacts["source.body"], artifacts["source.wire"]
        if receipt.get("content_encoding") != "identity":
            raise ValueError("nces_public_schools_source_capture_not_identity")
        if receipt.get("content_length") not in (None, len(body)):
            raise ValueError("nces_public_schools_archive_length_drift")
        for name in ("landing", "landing_after"):
            if (root / f"{name}.body").is_file():
                artifacts[f"{name}.body"] = (root / f"{name}.body").read_bytes()
                artifacts[f"{name}.wire"] = (root / f"{name}.wire").read_bytes()
                artifacts[f"{name}.receipt.json"] = (root / f"{name}.receipt.json").read_bytes()
        return artifacts, receipt, release
    except Exception as exc:
        if (root.is_dir() and not root.is_symlink()
                and all((root / name).is_file() and not (root / name).is_symlink()
                        for name in REPLAY_FILES)):
            try:
                artifacts = {name: (root / name).read_bytes() for name in REPLAY_FILES}
                receipt_raw = json.loads(artifacts["source.receipt.json"])
                _quarantine_capture(storage, artifacts, receipt_raw, type(exc).__name__)
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
    """Discover, capture or replay the newest published NCES public-school release."""
    artifacts, receipt, release = _capture(capture_directory, staging_storage)
    body = artifacts["source.body"]
    try:
        frame, audit, archive_roster = _inspect_archive(body, release)
        parquet = _parquet_bytes(frame)
    except Exception as exc:
        _quarantine_capture(staging_storage, artifacts, receipt, type(exc).__name__)
        raise

    schoolyear = str(release["schoolyear"])
    source_url = str(release["source_url"])
    identity = processing_identity(Path(__file__).parents[3])
    capture_evidence = {
        "schema_version": 1,
        "complete": True,
        "source_url": source_url,
        "source_vintage": schoolyear,
        "schoolyear": schoolyear,
        "publication_selection": {
            "schoolyear": schoolyear, "source_url": source_url, "stem": release["stem"],
        },
        "observed_at": receipt["observed_at"],
        "receipt": receipt,
        "archive_roster": archive_roster,
        "raw_artifacts": {
            name: {"bytes": len(artifacts[name]), "sha256": _sha(artifacts[name])}
            for name in sorted(REPLAY_FILES)
        },
    }
    capture_sha = _sha(_canonical(capture_evidence))
    audit.update({
        "source_capture_sha256": capture_sha,
        "source_archive_sha256": _sha(body),
        "source_archive_bytes": len(body),
        "source_vintage": schoolyear,
        "schoolyear": schoolyear,
        "publication_selection": release,
        "distribution_last_modified": receipt.get("last_modified"),
        "coverage_scope": (
            f"The complete NCES EDGE public-school {schoolyear} shapefile within the official "
            "distribution; exact equivalence to the archived HIFLD layer is unverified."
        ),
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
    feature_count = audit["source_feature_count"]
    year_slug = schoolyear.replace("-", "_")
    inputs = [{
        "input_id": f"nces_edge_public_schools_{year_slug}",
        "producer": PUBLISHER,
        "host": urlsplit(source_url).hostname,
        "role": "producer_publication",
        "canonical_url": source_url,
        "retrieval_url": source_url,
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "license": f"NCES states the information in its {schoolyear} public-school locations layer is in the public domain.",
        "attribution": "National Center for Education Statistics, U.S. Department of Education.",
        "release_date": None,
        "release_date_unknown_reason": (
            f"The {schoolyear} label identifies the CCD school-year vintage; the archive "
            "Last-Modified header is server file metadata, not a producer release date."
        ),
    }]
    manifest = {
        "schema_version": 1, "approved": False,
        "title": f"Public School Locations (NCES {schoolyear})",
        "description": (
            f"Complete {schoolyear} NCES EDGE public-school location archive extraction, proposed "
            "as a current producer-source candidate for the HIFLD Public Schools inventory. "
            "Historical HIFLD equivalence and the published contract remain unapproved."
        ),
        "publisher": PUBLISHER,
        "agency": "U.S. Department of Education, Institute of Education Sciences, National Center for Education Statistics",
        "source_url": source_url,
        "license": (
            f"Public domain claim stated in the NCES {schoolyear} layer description; HIFLD reuse "
            "and compatibility decisions remain pending."
        ),
        "source_modified": receipt.get("last_modified"),
        "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "inputs": inputs,
        "transformations": [{
            "operation": "NCES shapefile point layer to GeoParquet candidate",
            "parameters": {
                "source_vintage": schoolyear,
                "source_fields": f"All {len(EXPECTED_FIELDS)} shapefile attributes retained in source order and physical types.",
                "source_id": "NCESSCH retained as 12-character string; no substitute identifiers generated.",
                "geometry": "All source Point XY coordinates preserved as EPSG:4326 WKB; no reprojection, snapping or repair.",
                "completeness": (
                    f"All {feature_count:,} shapefile rows and IDs validated; complete tabular TXT "
                    f"member cross-checked against all {len(EXPECTED_FIELDS)} shapefile attributes."
                ),
                "candidate_status": "Unapproved source proposal; no HIFLD historical equivalence is asserted.",
            },
        }],
        "lineage": (
            "NCES EDGE publishes annual public-school geocodes based on the Common Core of Data. "
            f"This candidate is the newest public-school release named by the official NCES "
            f"SchoolLocations page at capture time ({schoolyear}). No feature-by-feature historical "
            "crosswalk or accepted replacement decision is established."
        ),
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
            "feature_count": feature_count,
            "schoolyear": schoolyear,
            "source_url": source_url,
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
    for name in ("landing", "landing_after"):
        if f"{name}.body" in artifacts:
            outputs[f"raw/{name}.body"] = artifacts[f"{name}.body"]
            outputs[f"raw/{name}.wire"] = artifacts[f"{name}.wire"]
            outputs[f"raw/{name}.receipt.json"] = _canonical(json.loads(artifacts[f"{name}.receipt.json"])) + b"\n"
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
            "feature_count": feature_count, "source_reported_count": audit["source_counts"]["dbf"],
            "schoolyear": schoolyear,
            "source_url": source_url, "source_population_complete": True,
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
        "feature_count": feature_count, "source_reported_count": audit["source_counts"]["dbf"],
        "schoolyear": schoolyear,
        "source_url": source_url, "source_population_complete": True,
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
