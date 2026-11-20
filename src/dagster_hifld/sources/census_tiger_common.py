"""Shared Census TIGER/Line capture, quarantine, archive inspection and staging.

Behavior-preserving consolidation of the per-dataset copies in the Census TIGER
acquisition family. Every dataset identity stays in its source adapter: the URL,
archive member roster, pins, byte budgets and record validation are passed
explicitly by each caller, so no caller can inherit another dataset's URL,
filename or pin. The config-driven school-district geodatabase extractor also
lives here; each school-district adapter passes its own
``SchoolDistrictLayerConfig``.
"""
from __future__ import annotations

import hashlib
import fcntl
import io
import json
import os
import platform
import re
import stat
import tempfile
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import geopandas as gpd
import httpx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, capture_root
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import (
    content_derived_version, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

CODE_IDENTITY_SCHEME = "sha256-packaged-dagster_hifld-py-v1"
TIGER_CAPTURE_FILES = {"source.body", "source.wire", "source.receipt.json"}


def processing_runtime() -> dict[str, str]:
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


def packaged_code_identity(source_root: Path, source_package: Path) -> str:
    """Hash packaged Dagster source files without requiring repository metadata."""
    files = sorted(source_package.rglob("*.py"))
    if not files or not source_package.is_dir():
        raise ValueError("processing_source_package_missing")
    digest = hashlib.sha256()
    for path in files:
        name = path.relative_to(source_root).as_posix().encode("utf-8")
        body = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def candidate_version(expected_sha256: str, code_sha256: str,
                      code_identity_scheme: str = CODE_IDENTITY_SCHEME) -> str:
    """Bind the candidate version key to the pinned source and packaged code identity."""
    identity = _canonical({"source_sha256": expected_sha256,
                           "code_identity_scheme": code_identity_scheme,
                           "code_sha256": code_sha256})
    return "v" + _sha(identity)[:32]


def quarantine_tiger_capture_attempt(
    storage: StagingStorageResource,
    capture_root: Path,
    dataset_slug: str,
    source_url: str,
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
            "method": "GET",
            "url": source_url,
            "final_url": None,
            "status": None,
            "observed_at": None,
            "complete_body": False,
            "bytes": len(body),
            "sha256": _sha(body),
            "wire_bytes": len(wire),
            "wire_sha256": _sha(wire),
            "error_code": reason_code,
        }
        receipt_path.write_bytes(_canonical(receipt) + b"\n")
    # The shared helper also writes a failure marker and refuses non-staging
    # destinations, keeping unrecognized source bytes out of catalog paths.
    return quarantine_incomplete_capture(
        storage, capture_root, dataset_slug, version,
        {"source": (body, receipt)}, reason_code,
    )


def capture_tiger_zip(
    capture_directory: str | Path | None,
    staging_storage: StagingStorageResource,
    version: str,
    *,
    dataset_slug: str,
    source_url: str,
    max_source_bytes: int,
    expected_bytes: int,
    expected_sha256: str,
    client_timeout: httpx.Timeout,
    capture_tmp_prefix: str,
    capture_get: Callable[..., tuple[bytes, dict[str, object]]],
) -> tuple[bytes, dict[str, object], bytes, bytes]:
    """Capture one pinned official TIGER/Line ZIP, or replay it from a cache directory.

    Returns ``(body, receipt, wire, raw_receipt_bytes)``. A fresh capture persists
    bounded body/wire evidence and a hash-pinned receipt via ``capture_get``;
    a replayed capture must present the exact three-file roster with a receipt
    whose identity matches the cached bytes and ``source_url``. Any mismatch with
    the pinned ``expected_bytes``/``expected_sha256`` quarantines the evidence
    under ``_quarantine/<dataset_slug>/`` before the caller sees the failure.
    """
    temporary = tempfile.TemporaryDirectory(prefix=capture_tmp_prefix) if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir():
                raise ValueError("source_capture_directory_invalid")
            if {p.name for p in root.iterdir()} != TIGER_CAPTURE_FILES:
                raise ValueError("source_capture_roster_mismatch")
            body_path, wire_path, receipt_path = (root / "source.body", root / "source.wire", root / "source.receipt.json")
            if any(p.is_symlink() or not p.is_file() for p in (body_path, wire_path, receipt_path)):
                raise ValueError("source_capture_nonregular_file")
            body = body_path.read_bytes()
            wire = wire_path.read_bytes()
            receipt = json.loads(receipt_path.read_text("utf-8"))
            if (
                receipt.get("method") != "GET" or receipt.get("url") != source_url
                or receipt.get("final_url") != source_url or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("content_encoding") != "identity"
            ):
                raise ValueError("source_capture_receipt_identity_mismatch")
        else:
            try:
                with httpx.Client(timeout=client_timeout, follow_redirects=False) as client:
                    body, receipt = capture_get(client, source_url, root, "source", max_bytes=max_source_bytes)
                wire = (root / "source.wire").read_bytes()
            except Exception as exc:
                quarantine = quarantine_tiger_capture_attempt(
                    staging_storage, root, dataset_slug, source_url, version, type(exc).__name__,
                )
                raise ValueError(f"official_tiger_source_capture_failed; quarantined_at:{quarantine}") from exc

        if (
            receipt.get("method") != "GET" or receipt.get("url") != source_url
            or receipt.get("final_url") != source_url or receipt.get("status") != 200
            or receipt.get("complete_body") is not True or receipt.get("bytes") != expected_bytes
            or receipt.get("sha256") != expected_sha256 or len(body) != expected_bytes
            or receipt.get("content_encoding") != "identity"
            or receipt.get("wire_bytes") != expected_bytes or receipt.get("wire_sha256") != expected_sha256
            or receipt.get("content_length") not in (None, expected_bytes)
            or _sha(body) != expected_sha256
        ):
            quarantine = quarantine_tiger_capture_attempt(
                staging_storage, root, dataset_slug, source_url, version,
                "official_tiger_archive_identity_or_release_drift",
            )
            raise ValueError(f"official_tiger_archive_identity_or_release_drift; quarantined_at:{quarantine}")
        return body, receipt, wire, (root / "source.receipt.json").read_bytes()
    finally:
        if temporary is not None:
            temporary.cleanup()


def read_tiger_shapefile(
    body: bytes,
    *,
    expected_members: set[str],
    shapefile_name: str,
    extract_tmp_prefix: str,
    read_file: Callable[[Path], gpd.GeoDataFrame],
) -> gpd.GeoDataFrame:
    """Open a pinned TIGER/Line ZIP safely and read its shapefile member.

    Validates the member roster before extraction, refuses absolute paths,
    parent traversal and subdirectory members, verifies CRC integrity, then
    extracts into a private temporary directory and returns the loaded frame.
    Dataset schema, CRS, population and geometry validation stay with the
    caller's source adapter.
    """
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = {Path(info.filename).name for info in archive.infolist() if not info.is_dir()}
        if names != expected_members or len(archive.infolist()) != len(expected_members):
            raise ValueError("tiger_archive_member_roster_drift")
        for info in archive.infolist():
            path = PurePosixPath(info.filename)
            if path.is_absolute() or ".." in path.parts or path.name != info.filename:
                raise ValueError("tiger_archive_unsafe_member")
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError("tiger_archive_crc_failure:" + bad_member)
        with tempfile.TemporaryDirectory(prefix=extract_tmp_prefix) as tmp:
            archive.extractall(tmp)
            return read_file(Path(tmp) / shapefile_name)


def shapefile_release_from_receipt(
    receipt: dict[str, object], *, layer: str, product: str, geoid: str | None = None,
) -> census_latest.ShapefileRelease:
    """Reconstruct the discovered TIGER release from a retained receipt URL."""
    url = receipt.get("url")
    area = geoid or "us"
    pattern = (rf"https://www2\.census\.gov/geo/tiger/TIGER(20\d{{2}})/{re.escape(layer)}/"
               rf"tl_\1_{re.escape(area)}_{re.escape(product)}\.zip")
    match = re.fullmatch(pattern, url if isinstance(url, str) else "")
    if match is None:
        raise ValueError("source_url_not_current_tiger_shapefile")
    vintage = int(match.group(1))
    source_file = url.rsplit("/", 1)[-1]
    return census_latest.ShapefileRelease(
        vintage=vintage, layer=layer, source_file=source_file, source_url=url,
        directory_url=url.rsplit("/", 1)[0] + "/", listing_sha256="", listing_bytes=0,
        latest_checked_at=str(receipt.get("observed_at") or ""), annual_listing_sha256="",
    )


def capture_tiger_shapefile(
    capture_directory: str | Path | None,
    staging_storage: StagingStorageResource,
    version: str,
    *,
    dataset_slug: str,
    layer: str,
    product: str,
    geoid: str | None = None,
    max_source_bytes: int,
    client_timeout: httpx.Timeout,
    capture_tmp_prefix: str,
    capture_get: Callable[..., tuple[bytes, dict[str, object]]],
) -> tuple[bytes, dict[str, object], bytes, bytes, census_latest.ShapefileRelease]:
    """Capture the newest published TIGER/Line shapefile or replay a retained one.

    Live capture resolves ``TIGER<year>/<layer>/tl_<year>_<area>_<product>.zip``
    through the producer index at capture time. A retained capture derives the
    same release from its receipt URL and stays labeled as replay. No edition,
    byte size or archive digest is pinned in the adapter.
    """
    temporary = tempfile.TemporaryDirectory(prefix=capture_tmp_prefix) if capture_directory is None else None
    root = Path(temporary.name) if temporary is not None else Path(capture_directory)
    client = None
    release = None
    try:
        if temporary is None:
            if root.is_symlink() or not root.is_dir():
                raise ValueError("source_capture_directory_invalid")
            if {p.name for p in root.iterdir()} != TIGER_CAPTURE_FILES:
                raise ValueError("source_capture_roster_mismatch")
            body_path, wire_path, receipt_path = (root / "source.body", root / "source.wire", root / "source.receipt.json")
            if any(p.is_symlink() or not p.is_file() for p in (body_path, wire_path, receipt_path)):
                raise ValueError("source_capture_nonregular_file")
            body = body_path.read_bytes()
            wire = wire_path.read_bytes()
            receipt = json.loads(receipt_path.read_text("utf-8"))
            release = shapefile_release_from_receipt(receipt, layer=layer, product=product, geoid=geoid)
            if (
                receipt.get("method") != "GET" or receipt.get("url") != release.source_url
                or receipt.get("final_url") != release.source_url or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("content_encoding") != "identity"
            ):
                raise ValueError("source_capture_receipt_identity_mismatch")
        else:
            try:
                client = httpx.Client(timeout=client_timeout, follow_redirects=False)
                release = census_latest.discover_shapefile(client, root, layer, product, geoid=geoid)
                body, receipt = capture_get(client, release.source_url, root, "source", max_bytes=max_source_bytes)
                wire = (root / "source.wire").read_bytes()
            except Exception as exc:
                quarantine = quarantine_tiger_capture_attempt(
                    staging_storage, root, dataset_slug, release.source_url if release else "",
                    version, type(exc).__name__,
                )
                raise ValueError(f"official_tiger_source_capture_failed; quarantined_at:{quarantine}") from exc

        if (
            receipt.get("method") != "GET" or receipt.get("url") != release.source_url
            or receipt.get("final_url") != release.source_url or receipt.get("status") != 200
            or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("content_encoding") != "identity"
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
        ):
            quarantine = quarantine_tiger_capture_attempt(
                staging_storage, root, dataset_slug, release.source_url, version,
                "official_tiger_archive_identity_or_release_drift",
            )
            raise ValueError(f"official_tiger_archive_identity_or_release_drift; quarantined_at:{quarantine}")
        return body, receipt, wire, (root / "source.receipt.json").read_bytes(), release
    finally:
        if client is not None:
            client.close()
        if temporary is not None:
            temporary.cleanup()


def tiger_shapefile_members(source_file: str) -> set[str]:
    """Return the producer's seven sidecar names for one TIGER shapefile ZIP."""
    stem = Path(source_file).stem
    return {
        f"{stem}.cpg", f"{stem}.dbf", f"{stem}.prj", f"{stem}.shp",
        f"{stem}.shp.ea.iso.xml", f"{stem}.shp.iso.xml", f"{stem}.shx",
    }


def tiger_shapefile_name(source_file: str) -> str:
    return Path(source_file).stem + ".shp"


MAX_ARCHIVE_BYTES = 128_000_000
MAX_UNCOMPRESSED_BYTES = 1_500_000_000
EXPECTED_SOURCE_FIELDS = [
    ("GEOID", pa.string()),
    ("GEOIDFQ", pa.string()),
    ("NAME", pa.string()),
    ("LOGRADE", pa.string()),
    ("HIGRADE", pa.string()),
    ("SDTYP", pa.string()),
    ("ALAND", pa.float64()),
    ("AWATER", pa.float64()),
    ("INTPTLAT", pa.string()),
    ("INTPTLON", pa.string()),
    ("SHAPE", pa.binary()),
]
EXPECTED_OUTPUT_FIELDS = ["OBJECTID", "GEOID", "GEOIDFQ", "NAME", "LOGRADE", "HIGRADE", "SDTYP", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "geometry"]


@dataclass(frozen=True)
class SchoolDistrictLayerConfig:
    """Immutable issue-specific contract for one Census school district layer."""

    dataset_slug: str
    layer_name: str
    expected_count: int | None
    issue_number: int
    inventory_id: str
    title: str
    coverage_statement: str
    source_url: str = "https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_school.gdb.zip"
    source_host: str = "www2.census.gov"
    source_vintage: str = "2024"
    expected_source_fields: list[tuple[str, pa.DataType]] = field(
        default_factory=lambda: list(EXPECTED_SOURCE_FIELDS))
    discover_current: bool = False
    feature_count_range: tuple[int, int] | None = None

    @property
    def file_slug(self) -> str:
        return self.dataset_slug

    @property
    def output_fields(self) -> list[str]:
        attribute_names = [name for name, _ in self.expected_source_fields if name != "SHAPE"]
        return ["OBJECTID", *attribute_names, "geometry"]


SOURCE_URL = "https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_school.gdb.zip"
SOURCE_HOST = "www2.census.gov"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _capture_archive(staging, version: str, client: httpx.Client, config: SchoolDistrictLayerConfig) -> tuple[bytes, dict]:
    wire = bytearray()
    attempt_observed_at = datetime.now(timezone.utc).isoformat()
    transfer_receipt_written = False
    try:
        with client.stream("GET", config.source_url, headers={"Accept-Encoding": "identity"}) as response:
            final_host = response.request.url.host
            result = capture_http_response(
                response,
                max_wire_bytes=MAX_ARCHIVE_BYTES,
                max_decoded_bytes=MAX_ARCHIVE_BYTES,
                reserve=lambda progress: _reserve(progress.wire_bytes_received, progress.reserve_bytes),
                retain_wire=wire.extend,
                # Census serves this file with identity encoding. Retaining the
                # wire bytes once avoids storing a second identical 80 MB blob.
                retain_decoded=lambda _block: None,
            )
            receipt = {
                "method": "GET", "requested_url": config.source_url,
                "final_url": str(response.request.url), "final_host": final_host,
                "status": result.status_code,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "content_type": response.headers.get("content-type"),
                "last_modified": response.headers.get("last-modified"),
                "content_encoding": result.content_encoding,
                "content_length_header": result.content_length_header,
                "wire_bytes_received": result.wire_bytes_received,
                "wire_bytes_retained": result.wire_bytes_retained,
                "wire_sha256": result.wire_sha256,
                "decoded_bytes_produced": result.decoded_bytes_produced,
                "transfer_complete": result.complete,
                "transfer_error_code": result.error_code,
                "raw_archive_key": f"raw/{Path(config.source_url).name}",
                "raw_sha256": _sha256(bytes(wire)),
                "release_accepted": False,
            }
            staging.write(config.dataset_slug, config.file_slug, version, receipt["raw_archive_key"], bytes(wire))
            staging.write(config.dataset_slug, config.file_slug, version, "raw/source_archive.receipt.json", _json_bytes(receipt))
            transfer_receipt_written = True
            if (
                not result.complete
                or not 200 <= result.status_code < 300
                or result.content_encoding != "identity"
                or result.decoded_bytes_produced != len(wire)
                or final_host.lower() != config.source_host
                or not str(response.headers.get("content-type", "")).lower().startswith("application/zip")
            ):
                raise ValueError("source_archive_transfer_drift")
            if len(wire) == 0 or len(wire) > MAX_ARCHIVE_BYTES:
                raise ValueError("source_archive_size_out_of_bounds")
            return bytes(wire), receipt
    except ValueError as exc:
        if not transfer_receipt_written:
            _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__, config)
        raise
    except Exception as exc:
        _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__, config)
        raise ValueError("source_archive_transport_error:" + type(exc).__name__) from exc


def _retain_partial_transfer(staging, version: str, wire: bytearray, observed_at: str, error_type: str, config: SchoolDistrictLayerConfig) -> None:
    partial = bytes(wire)
    partial_key = "raw/source_archive.partial.zip" if partial else None
    if partial_key:
        staging.write(config.dataset_slug, config.file_slug, version, partial_key, partial)
    staging.write(config.dataset_slug, config.file_slug, version, "raw/source_archive.failure_receipt.json", _json_bytes({
        "method": "GET", "requested_url": config.source_url,
        "observed_at": observed_at,
        "wire_bytes_retained": len(partial),
        "wire_sha256": _sha256(partial),
        "partial_wire_key": partial_key,
        "transfer_error_type": error_type, "complete": False,
    }))


def _reserve(current: int, upcoming: int) -> None:
    if current + upcoming > MAX_ARCHIVE_BYTES:
        raise ValueError("source_archive_capture_budget_exceeded")


def _validate_archive(archive: bytes) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as source:
            entries = source.infolist()
            if not entries or sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("source_archive_expansion_limit_exceeded")
            if any(item.file_size > MAX_UNCOMPRESSED_BYTES for item in entries):
                raise ValueError("source_archive_member_limit_exceeded")
            for item in entries:
                parts = Path(item.filename).parts
                if Path(item.filename).is_absolute() or ".." in parts or item.filename.startswith(("/", "\\")):
                    raise ValueError("source_archive_unsafe_member_path")
            gdb_roots = {"/".join(item.filename.split("/")[:1]) for item in entries if ".gdb/" in item.filename}
            if len(gdb_roots) != 1 or not next(iter(gdb_roots)).endswith(".gdb"):
                raise ValueError("source_archive_geodatabase_roster_drift")
            corrupt = source.testzip()
            if corrupt is not None:
                raise ValueError("source_archive_crc_failure")
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc


def _school_count_matches(count: int, config: SchoolDistrictLayerConfig) -> bool:
    if type(count) is not int:
        return False
    if config.expected_count is not None:
        return count == config.expected_count
    return config.feature_count_range is not None and config.feature_count_range[0] <= count <= config.feature_count_range[1]


def _native_table(archive_path: Path, config: SchoolDistrictLayerConfig) -> tuple[pa.Table, list[int], dict]:
    try:
        layers = pyogrio.list_layers(archive_path)
        layer_names = [str(row[0]) for row in layers]
        if config.layer_name not in layer_names:
            raise ValueError("source_target_layer_missing")
        info = pyogrio.read_info(archive_path, layer=config.layer_name, force_feature_count=True)
        if not _school_count_matches(info.get("features"), config) or info.get("geometry_type") != "MultiPolygon":
            raise ValueError("source_layer_population_or_geometry_drift")
        try:
            crs = pyproj.CRS.from_user_input(info.get("crs"))
        except Exception as exc:
            raise ValueError("source_crs_drift") from exc
        if not crs.equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_crs_drift")
        metadata, table = pyogrio.raw.read_arrow(archive_path, layer=config.layer_name)
        if metadata.get("geometry_type") != "MultiPolygon" or not pyproj.CRS.from_user_input(metadata.get("crs")).equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_arrow_layer_metadata_drift")
        geometry_name = metadata.get("geometry_name")
        if geometry_name != "SHAPE":
            raise ValueError("source_geometry_column_drift")
        actual = [(field.name, field.type) for field in table.schema]
        if actual != config.expected_source_fields or table.num_rows != info["features"]:
            raise ValueError("source_schema_or_feature_count_drift")
        fid_frame = pyogrio.read_dataframe(archive_path, layer=config.layer_name, fid_as_index=True, read_geometry=False, use_arrow=True)
        ids = [int(value) for value in fid_frame.index.tolist()]
        if len(ids) != table.num_rows or len(set(ids)) != table.num_rows or sorted(ids) != list(range(1, table.num_rows + 1)):
            raise ValueError("source_native_id_population_drift")
        table_geoids = table.column("GEOID").to_pylist()
        if fid_frame["GEOID"].tolist() != table_geoids or len(set(table_geoids)) != table.num_rows:
            raise ValueError("source_geoid_population_drift")
        return table, ids, {"info": info, "table_metadata": metadata, "layers": layer_names}
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc


def _build_geoparquet(table: pa.Table, ids: list[int], config: SchoolDistrictLayerConfig) -> tuple[bytes, dict]:
    geometry_values = table.column("SHAPE").to_pylist()
    if not _school_count_matches(len(geometry_values), config) or any(not isinstance(value, bytes) for value in geometry_values):
        raise ValueError("source_geometry_population_drift")
    geometry_types: set[str] = set()
    xmin = ymin = float("inf")
    xmax = ymax = float("-inf")
    for raw in geometry_values:
        try:
            geometry = shapely.from_wkb(raw)
        except Exception as exc:
            raise ValueError("source_geometry_wkb_invalid") from exc
        if geometry.is_empty or geometry.geom_type != "MultiPolygon" or not geometry.is_valid:
            raise ValueError("source_geometry_type_or_validity_drift")
        bounds = geometry.bounds
        if len(bounds) != 4 or not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
            raise ValueError("source_geometry_coordinate_bounds_drift")
        xmin, ymin = min(xmin, bounds[0]), min(ymin, bounds[1])
        xmax, ymax = max(xmax, bounds[2]), max(ymax, bounds[3])
        geometry_types.add(geometry.geom_type)
    if geometry_types != {"MultiPolygon"}:
        raise ValueError("source_geometry_type_drift")

    names = [name for name, _ in config.expected_source_fields if name != "SHAPE"]
    columns = [pa.array(ids, type=pa.int64())]
    columns.extend(table.column(name) for name in names)
    columns.append(table.column("SHAPE"))
    output = pa.Table.from_arrays(columns, names=["OBJECTID", *names, "geometry"])
    geo = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["MultiPolygon"],
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
            "bbox": [xmin, ymin, xmax, ymax],
        }},
    }
    schema_metadata = dict(output.schema.metadata or {})
    schema_metadata[b"geo"] = _json_bytes(geo).strip()
    output = output.replace_schema_metadata(schema_metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(output, sink, compression="zstd", version="2.6")
    parquet = sink.getvalue().to_pybytes()
    _assert_full_parity(table, ids, parquet, config)
    return parquet, {"field_names": output.column_names, "arrow_schema": str(output.schema), "geometry_bbox": [xmin, ymin, xmax, ymax]}


def _assert_full_parity(source: pa.Table, ids: list[int], parquet: bytes, config: SchoolDistrictLayerConfig) -> None:
    output = pq.read_table(pa.BufferReader(parquet))
    if not _school_count_matches(output.num_rows, config) or output.num_rows != source.num_rows or output.column_names != config.output_fields:
        raise ValueError("native_output_population_or_field_roster_drift")
    if output.column("OBJECTID").to_pylist() != ids:
        raise ValueError("native_output_id_parity_drift")
    for source_name in [name for name, _ in config.expected_source_fields if name != "SHAPE"]:
        if output.column(source_name).to_pylist() != source.column(source_name).to_pylist():
            raise ValueError("native_output_attribute_parity_drift:" + source_name)
    if output.column("geometry").to_pylist() != source.column("SHAPE").to_pylist():
        raise ValueError("native_output_geometry_parity_drift")
    if len(set(output.column("OBJECTID").to_pylist())) != source.num_rows:
        raise ValueError("native_output_duplicate_ids")


def _source_records_sha256(table: pa.Table, ids: list[int],
                           config: SchoolDistrictLayerConfig | None = None) -> str:
    fields = config.expected_source_fields if config is not None else EXPECTED_SOURCE_FIELDS
    records = []
    columns = {name: table.column(name).to_pylist() for name, _ in fields}
    for index, identity in enumerate(ids):
        records.append({"OBJECTID": identity, **{name: (columns[name][index].hex() if name == "SHAPE" else columns[name][index]) for name, _ in fields}})
    return _sha256(_json_bytes(records))


def _packaged_code_sha256() -> str:
    root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        relative, body = path.relative_to(root).as_posix().encode(), path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big") + relative + len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def _construct_school_capture(
    staging,
    version: str,
    config: SchoolDistrictLayerConfig,
    client: httpx.Client | None = None,
    *,
    retained_archive: bytes | None = None,
    retained_receipt: dict | None = None,
    retained_receipt_bytes: bytes | None = None,
) -> dict:
    owned = client is None and retained_archive is None
    client = client or (httpx.Client(timeout=120, follow_redirects=True) if owned else None)
    try:
        if retained_archive is None:
            archive, receipt = _capture_archive(staging, version, client, config)
            replayed = False
        else:
            if retained_receipt is None or retained_receipt_bytes is None:
                raise ValueError("retained_capture_receipt_missing")
            archive, receipt = retained_archive, retained_receipt
            if _sha256(archive) != receipt.get("raw_sha256") or len(archive) != receipt.get("wire_bytes_retained"):
                raise ValueError("retained_capture_archive_receipt_mismatch")
            # Copy the original evidence bytes verbatim. This path is for a
            # local replay of an already observed transfer; it makes no new
            # network request and does not invent a retrieval timestamp.
            staging.write(config.dataset_slug, config.file_slug, version, receipt["raw_archive_key"], archive)
            staging.write(config.dataset_slug, config.file_slug, version, "raw/source_archive.receipt.json", retained_receipt_bytes)
            replayed = True
        _validate_archive(archive)
        with tempfile.TemporaryDirectory(prefix="hifld-school-districts-") as temporary:
            path = Path(temporary) / Path(config.source_url).name
            path.write_bytes(archive)
            table, ids, source_meta = _native_table(path, config)
        parquet, output_schema = _build_geoparquet(table, ids, config)
        parquet_sha = _sha256(parquet)
        output_key = f"geoparquet/{config.file_slug}.parquet"
        staging.write(config.dataset_slug, config.file_slug, version, output_key, parquet)
        ids_sha = _sha256(_json_bytes(ids))
        records_sha = _source_records_sha256(table, ids, config)
        source_schema = [{"name": name, "type": str(kind)} for name, kind in config.expected_source_fields]
        archive_sha = receipt["raw_sha256"]
        retrieved_at = receipt["observed_at"]
        identity = processing_identity(Path(__file__).parents[3])
        if config.discover_current:
            identity = {**identity, "native_runtime": {
                "pyogrio": pyogrio.__version__, "gdal": pyogrio.__gdal_version_string__,
                "pyproj": pyproj.__version__, "httpx": httpx.__version__,
                "numpy": np.__version__, "shapely": shapely.__version__,
            }}
        try:
            validate_construction_code_identity(
                Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
            )
        except Exception as exc:
            raise ValueError("source_code_identity_invalid:" + type(exc).__name__) from exc
        supplied_claim = os.environ.get("HIFLD_CODE_COMMIT") or os.environ.get("GITHUB_SHA")
        manifest = {
            "schema_version": 1,
            "title": config.title,
            "description": f"Draft extraction of the complete {config.source_vintage} Census TIGER/Line {config.layer_name} layer. It is a proposed source route for HIFLD inventory issue #{config.issue_number}; historical HIFLD archive equivalence and release acceptance remain unresolved.",
            "publisher": "U.S. Census Bureau",
            "agency": "National Center for Education Statistics (HIFLD inventory attribution; source producer is U.S. Census Bureau)",
            "source_url": config.source_url,
            "processor": "HIFLD Next",
            "code_commit": identity["code_commit"],
            "processing_identity": identity,
            "supplied_code_commit_claim": supplied_claim,
            "inputs": [{
                "input_id": f"census_tigerline_{config.source_vintage}_school_geodatabase",
                "producer": "U.S. Census Bureau",
                "host": config.source_host,
                "role": "producer_publication",
                "canonical_url": config.source_url,
                "retrieval_url": receipt["final_url"],
                "retrieved_at": retrieved_at,
                "sha256": archive_sha,
                "bytes": receipt["wire_bytes_received"],
                "http_status": receipt["status"],
                "release_date_unknown_reason": "No explicit release date was established from the archive; the HTTP Last-Modified value is retained in the receipt and is not treated as a producer release date.",
                "license": "Not adjudicated; source reuse terms and rights require maintainer review before release.",
                "attribution": f"U.S. Census Bureau, TIGER/Line {config.source_vintage}; historical HIFLD attribution and reuse terms remain under review.",
            }],
            "transformations": [
                {"name": "extract_geodatabase_layer", "parameters": {"layer": config.layer_name, "field_subset": "none", "geometry_simplification": "none", "reprojection": "none"}},
                {"name": "write_geoparquet", "parameters": {"compression": "zstd", "geometry_encoding": "WKB", "native_crs": "EPSG:4269"}},
            ],
            "lineage": f"Direct extraction from the official U.S. Census Bureau TIGER/Line {config.source_vintage} nationwide school geodatabase. This is a proposed source route for HIFLD inventory {config.inventory_id} / issue #{config.issue_number}, not proof of byte/row equivalence with its DataLumos archive or DHS crosswalk entry.",
            "prebuilt_output_policy": {
                "schema_version": 1,
                "formats": {"geoparquet": {
                    "reason": f"Source-faithful preservation of all {config.source_vintage} native layer attributes, FIDs, WKB geometries, and EPSG:4269 coordinates in one pinned object; this is a draft conversion only.",
                    "objects": [{"path": output_key, "sha256": parquet_sha, "bytes": len(parquet)}],
                }},
            },
        }
        staging.write(config.dataset_slug, config.file_slug, version, "metadata/source_manifest.json", _json_bytes(manifest))
        ids_rows = [{"OBJECTID": identity, "GEOID": table.column("GEOID")[index].as_py()} for index, identity in enumerate(ids)]
        capture_manifest = {
            "source_url": config.source_url, "source_host": config.source_host,
            "archive_sha256": archive_sha, "archive_bytes": len(archive),
            "archive_receipt": "raw/source_archive.receipt.json",
            "archive_last_modified": receipt.get("last_modified"),
            "archive_members_crc_checked": True,
            "source_layer": config.layer_name, "source_layer_roster": source_meta["layers"],
            "feature_count": len(ids), "expected_feature_count": config.expected_count,
            "page_count": 1, "page_mode": "single_complete_file_geodatabase_layer",
            "page": {"index": 1, "source_ids_sha256": ids_sha, "feature_count": len(ids)},
            "id_field": "OBJECTID (native GDB FID)", "ids_sha256": ids_sha,
            "record_sha256": records_sha, "ids_and_geoids": ids_rows,
            "source_schema": source_schema, "output_schema": output_schema,
            "native_geometry_type": source_meta["info"]["geometry_type"],
            "native_crs": "EPSG:4269", "native_crs_wkt": pyproj.CRS.from_epsg(4269).to_wkt(),
            "source_row_output_full_parity": True, "output_sha256": parquet_sha,
            "output_bytes": len(parquet), "output_key": output_key,
            "code_identity": {
                "processing_identity": identity,
                "capture_module_sha256": _sha256(Path(__file__).read_bytes()),
                "runtime_package_source_sha256": _packaged_code_sha256(),
                "python": identity["python"],
            },
            "coverage_statement": f"Full {len(ids)}-feature population of the named {config.source_vintage} Census source layer. {config.coverage_statement} Equivalence to HIFLD archive coverage remains unverified.",
            "capture_replay": {
                "mode": "retained_transfer_replay" if replayed else "live_http_capture",
                "original_retrieved_at": retrieved_at,
                "new_network_retrieval_performed": not replayed,
            },
            "refresh_semantics": "Each live run fetches a fresh official archive; prior captured source bytes and receipts remain at the versioned raw path.",
            "release_accepted": False,
            "rights_contract_approved": False,
        }
        staging.write(config.dataset_slug, config.file_slug, version, "metadata/native_capture.json", _json_bytes(capture_manifest))
        return {
            "feature_count": len(ids), "ids_sha256": ids_sha,
            "records_sha256": records_sha, "source_archive_sha256": archive_sha,
            "parquet_sha256": parquet_sha, "release_accepted": False,
        }
    except Exception as exc:
        staging.write(config.dataset_slug, config.file_slug, version, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_url": config.source_url,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "failure_code": str(exc).split(":", 1)[0],
            "release_accepted": False,
        }))
        raise
    finally:
        if owned:
            client.close()


def _school_release_from_receipt(receipt: dict, archive: bytes) -> census_latest.GeodatabaseRelease:
    if not isinstance(receipt, dict):
        raise ValueError("retained_capture_receipt_invalid")
    size = len(archive)
    if not 0 < size <= MAX_ARCHIVE_BYTES:
        raise ValueError("retained_capture_archive_receipt_mismatch")
    digest = _sha256(archive)
    if receipt.get("raw_sha256") != digest or receipt.get("wire_bytes_retained") != size:
        raise ValueError("retained_capture_archive_receipt_mismatch")
    url = receipt.get("requested_url")
    if "url" in receipt and receipt["url"] != url:
        raise ValueError("retained_capture_release_mismatch")
    release = census_latest.geodatabase_release_from_receipt({**receipt, "url": url}, "school")
    if (receipt.get("method") != "GET" or receipt.get("status") != 200
            or receipt.get("final_url") != release.source_url
            or receipt.get("final_host") != "www2.census.gov"
            or receipt.get("content_encoding") != "identity"
            or not str(receipt.get("content_type", "")).lower().startswith("application/zip")
            or receipt.get("transfer_complete") is not True
            or receipt.get("transfer_error_code") is not None
            or receipt.get("raw_archive_key") != "raw/" + release.source_file
            or receipt.get("wire_sha256") != digest
            or any(receipt.get(name) != size for name in (
                "wire_bytes_received", "decoded_bytes_produced"))):
        raise ValueError("retained_capture_archive_receipt_mismatch")
    if receipt.get("content_length_header") is not None and receipt["content_length_header"] != size:
        raise ValueError("retained_capture_archive_receipt_mismatch")
    try:
        if datetime.fromisoformat(receipt["observed_at"]).utcoffset() is None:
            raise ValueError("missing_timezone")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("retained_capture_receipt_invalid") from exc
    return release


def _construct_current_school(private, prefix, config, archive, receipt, receipt_bytes, replayed):
    release = _school_release_from_receipt(receipt, archive)
    config = replace(config, source_url=release.source_url, source_vintage=str(release.vintage))
    result = _construct_school_capture(
        private, "private", config, retained_archive=archive,
        retained_receipt=receipt, retained_receipt_bytes=receipt_bytes)
    manifest_key = prefix + "/metadata/source_manifest.json"
    manifest = json.loads(private.read_key(manifest_key))
    identity = manifest["processing_identity"]
    manifest["source_release"] = release.as_receipt_evidence()
    private.write_key(manifest_key, _json_bytes(manifest))
    report_key = prefix + "/metadata/native_capture.json"
    report = json.loads(private.read_key(report_key))
    report["source_release"] = release.as_receipt_evidence()
    report["plausible_feature_count_range"] = list(config.feature_count_range)
    report["capture_replay"]["mode"] = "retained_transfer_replay" if replayed else "live_http_capture"
    report["capture_replay"]["new_network_retrieval_performed"] = not replayed
    private.write_key(report_key, _json_bytes(report))
    schema_sha = _sha256(_json_bytes({
        "source_url": release.source_url, "layer": config.layer_name,
        "fields": report["source_schema"]}))
    version = content_derived_version(result["records_sha256"], result["source_archive_sha256"], schema_sha, identity)
    return {**result, "version": version}, identity, schema_sha


def _capture(staging, version, config: SchoolDistrictLayerConfig, client=None, *,
             retained_archive=None, retained_receipt=None, retained_receipt_bytes=None) -> dict:
    if not config.discover_current:
        return _construct_school_capture(
            staging, version, config, client, retained_archive=retained_archive,
            retained_receipt=retained_receipt, retained_receipt_bytes=retained_receipt_bytes)
    if (not isinstance(staging, StagingStorageResource) or isinstance(staging, PublishedStorageResource)
            or not staging.use_local or staging.bucket or not staging.local_dir
            or Path(staging.prefix).is_absolute()):
        raise ValueError("school_current_capture_requires_isolated_local_staging")
    replayed = retained_archive is not None
    owned = client is None and not replayed
    client = client or (httpx.Client(timeout=120, follow_redirects=True) if owned else None)
    with tempfile.TemporaryDirectory(prefix="hifld-school-current-", dir=capture_root()) as temporary:
        root = Path(temporary)
        private = StagingStorageResource(local_dir=str(root / "candidate"))
        prefix = f"{config.dataset_slug}/{config.file_slug}/private"
        try:
            if replayed:
                archive, receipt = retained_archive, retained_receipt
                receipt_bytes = retained_receipt_bytes
                (root / "retained.archive").write_bytes(archive)
                (root / "retained.receipt.json").write_bytes(receipt_bytes or b"")
            else:
                release = census_latest.discover_geodatabase(client, root / "discovery", "school")
                config = replace(config, source_url=release.source_url, source_vintage=str(release.vintage))
                archive, receipt = _capture_archive(private, "private", client, config)
                receipt["source_release"] = release.as_receipt_evidence()
                receipt_bytes = _json_bytes(receipt)
            release = _school_release_from_receipt(receipt, archive)
            if receipt_bytes is None or json.loads(receipt_bytes) != receipt:
                raise ValueError("retained_capture_receipt_invalid")
            result, identity, schema_sha = _construct_current_school(
                private, prefix, config, archive, receipt, receipt_bytes, replayed)
            version = result["version"]
            target = f"{config.dataset_slug}/{config.file_slug}/{version}"
            state_root = f"_construction-state/{config.dataset_slug}/{version}"
            state_key = state_root + "/bundle.json"
            fingerprint = _sha256(_json_bytes(identity))
            candidate_storage = staging.model_copy(update={
                "local_dir": str(Path(staging.local_dir) / staging.prefix), "prefix": ""})
            observation = f"_capture-observations/{config.dataset_slug}/{_sha256(receipt_bytes)}"
            write_immutable(staging, observation + "/source_archive.receipt.json", receipt_bytes)
            for path in sorted((root / "discovery").rglob("*")):
                if path.is_file():
                    write_immutable(staging, observation + "/discovery/" + path.relative_to(root / "discovery").as_posix(), path.read_bytes())
            lock_key = f"_locks/{config.dataset_slug}.lock"
            write_immutable(staging, lock_key, b"")
            lock_path = Path(staging.local_dir) / staging.prefix / lock_key
            with os.fdopen(os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as lock:
                if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode) or os.fstat(lock.fileno()).st_nlink != 1:
                    raise ValueError("school_candidate_lock_aliased")
                fcntl.flock(lock, fcntl.LOCK_EX)
                reused = bool(candidate_storage.list_prefix(state_root))
                if reused:
                    original = json.loads(load_first_construction(candidate_storage, state_key, version, fingerprint))
                    objects = original["payload"]["objects"]
                else:
                    attempt_key = f"_capture-observations/{config.dataset_slug}/construction/{version}.json"
                    if candidate_storage.object_exists(attempt_key):
                        attempt_body = candidate_storage.read_key(attempt_key)
                        attempt = json.loads(attempt_body)
                        if (not isinstance(attempt, dict) or set(attempt) != {"receipt_sha256", "replayed"}
                                or type(attempt["replayed"]) is not bool
                                or not isinstance(attempt["receipt_sha256"], str)
                                or re.fullmatch(r"[0-9a-f]{64}", attempt["receipt_sha256"]) is None):
                            raise ValueError("school_original_observation_invalid")
                        write_immutable(candidate_storage, attempt_key, attempt_body)
                        original_key = f'_capture-observations/{config.dataset_slug}/{attempt["receipt_sha256"]}/source_archive.receipt.json'
                        if not candidate_storage.object_exists(original_key):
                            raise ValueError("school_original_observation_invalid")
                        original_bytes = candidate_storage.read_key(original_key)
                        if _sha256(original_bytes) != attempt["receipt_sha256"]:
                            raise ValueError("school_original_observation_invalid")
                        if original_bytes != receipt_bytes or attempt["replayed"] != replayed:
                            original_result, original_identity, original_schema = _construct_current_school(
                                private, prefix, config, archive, json.loads(original_bytes),
                                original_bytes, attempt["replayed"])
                            if original_result != result or original_identity != identity or original_schema != schema_sha:
                                raise ValueError("school_original_observation_identity_changed")
                    else:
                        write_immutable(candidate_storage, attempt_key, _json_bytes({
                            "receipt_sha256": _sha256(receipt_bytes), "replayed": replayed}))
                    objects = {name[len(prefix) + 1:]: {
                        "bytes": private.object_snapshot(name).size, "sha256": private.sha256_key(name)}
                        for name in private.list_prefix(prefix)}
                    existing = set(candidate_storage.list_prefix(target))
                    expected = {target + "/" + name for name in objects}
                    if existing - expected:
                        raise ValueError("school_unsealed_candidate_roster_drift")
                    for key in existing:
                        write_immutable(candidate_storage, key, private.read_key(prefix + key[len(target):]))
                    for name in objects:
                        write_immutable(candidate_storage, target + "/" + name, private.read_key(prefix + "/" + name))
                    staged_objects = {key[len(target) + 1:]: {
                        "bytes": candidate_storage.object_snapshot(key).size,
                        "sha256": candidate_storage.sha256_key(key)}
                        for key in candidate_storage.list_prefix(target)}
                    if staged_objects != objects:
                        raise ValueError("school_unsealed_candidate_integrity_failed")
                    seal_ready_candidate(candidate_storage, state_key, version, fingerprint, {
                        "processing_identity": identity, "source_record_sha256": result["records_sha256"],
                        "source_schema_sha256": schema_sha, "capture_receipts": {}, "objects": objects})
                verify_cached_candidate(
                    candidate_storage, Path(__file__).parents[3], state_key, target, version, fingerprint,
                    identity, result["records_sha256"], schema_sha, objects, {})
                return {**result, "version": version, "reused": reused}
        except Exception as exc:
            quarantine = f"_quarantine/{config.dataset_slug}/{uuid4().hex}"
            for path in sorted(root.rglob("*")):
                if path.is_file():
                    write_immutable(staging, quarantine + "/" + path.relative_to(root).as_posix(), path.read_bytes())
            write_immutable(staging, quarantine + "/metadata/failure.json", _json_bytes({
                "failure_code": str(exc).split(":", 1)[0], "release_accepted": False}))
            raise
        finally:
            if owned:
                client.close()


def capture(
    staging,
    version: str,
    config: SchoolDistrictLayerConfig,
    client: httpx.Client | None = None,
) -> dict:
    """Capture the configured layer's bounded source archive and evidence."""
    return _capture(staging, version, config, client)


def replay_retained_capture(
    staging,
    version: str,
    archive: bytes,
    receipt_bytes: bytes,
    *,
    config: SchoolDistrictLayerConfig,
) -> dict:
    """Reprocess one retained transfer while preserving its original receipt."""
    try:
        receipt = json.loads(receipt_bytes)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("retained_capture_receipt_invalid") from exc
    return _capture(
        staging, version, config, retained_archive=archive, retained_receipt=receipt,
        retained_receipt_bytes=receipt_bytes,
    )


# Shared TIGERweb Transportation service identity, Web Mercator CRS pins
# and publisher-vintage parsing used by the Census TIGERweb road adapters.
SERVICE_FOLDER_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb?f=pjson"
SERVICE_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Transportation/MapServer"
WEB_MERCATOR_WKIDS = {102100, 3857}
WEB_MERCATOR_LIMIT = 20037508.342789244
# The pinned layer extent is the observed data bbox; every XY vertex must stay
# inside the Web Mercator domain and the pinned extent within float noise.
EXTENT_TOLERANCE_METERS = 1e-6
EXPECTED_SERVICE_MAP_NAME = "Transportation (Roads and Railroads)"
EXPECTED_SERVICE_MAX_RECORD_COUNT = 100000
EXPECTED_SERVICE_LAYERS = [
    (0, "Primary Roads Interstates 5M scale"), (1, "Primary Roads 2_1M scale"),
    (2, "Primary Roads"), (3, "Secondary Roads Interstates and US Highways"),
    (4, "Secondary Roads 578k scale"), (5, "Secondary Roads 289_144k scale"),
    (6, "Secondary Roads 72_1k scale"), (7, "Local Roads 72k scale"),
    (8, "Local Roads"), (9, "Railroads"),
]


def _validate_web_mercator_reference(value: object, label: str) -> None:
    """Validate every advertised ArcGIS Web Mercator representation."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid not in WEB_MERCATOR_WKIDS:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = pyproj.CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(pyproj.CRS.from_epsg(3857), ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
    if not found:
        raise ValueError(f"{label}_crs_drift")


_MONTH_NAMES = {name: number for number, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"), start=1)}


def _publisher_stated_vintage(description: object) -> str:
    """Parse the publisher-stated vintage from the captured layer description.

    The TIGERweb Transportation layer description ends with a 'Month D, YYYY'
    stamp for the vintage of the published layer population. The stamp is
    preserved as the publisher-stated source vintage; no last-edit timestamp
    is invented for it.
    """
    match = (re.fullmatch(r"(?s)(.*);\s+([A-Z][a-z]+) (\d{1,2}), (\d{4})", description)
             if isinstance(description, str) else None)
    if not match or match.group(2) not in _MONTH_NAMES:
        raise ValueError("source_layer_description_or_vintage_drift")
    return f"{int(match.group(4)):04d}-{_MONTH_NAMES[match.group(2)]:02d}-{int(match.group(3)):02d}"


WEB_MERCATOR_PROJJSON = {
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "area": "World between 85.06°S and 85.06°N.",
        "base_crs": {
            "coordinate_system": {
                "axis": [
                    {
                        "abbreviation": "Lat",
                        "direction": "north",
                        "name": "Geodetic latitude",
                        "unit": "degree"
                    },
                    {
                        "abbreviation": "Lon",
                        "direction": "east",
                        "name": "Geodetic longitude",
                        "unit": "degree"
                    }
                ],
                "subtype": "ellipsoidal"
            },
            "datum_ensemble": {
                "accuracy": "2.0",
                "ellipsoid": {
                    "inverse_flattening": 298.257223563,
                    "name": "WGS 84",
                    "semi_major_axis": 6378137
                },
                "id": {
                    "authority": "EPSG",
                    "code": 6326
                },
                "members": [
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1166
                        },
                        "name": "World Geodetic System 1984 (Transit)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1152
                        },
                        "name": "World Geodetic System 1984 (G730)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1153
                        },
                        "name": "World Geodetic System 1984 (G873)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1154
                        },
                        "name": "World Geodetic System 1984 (G1150)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1155
                        },
                        "name": "World Geodetic System 1984 (G1674)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1156
                        },
                        "name": "World Geodetic System 1984 (G1762)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1309
                        },
                        "name": "World Geodetic System 1984 (G2139)"
                    },
                    {
                        "id": {
                            "authority": "EPSG",
                            "code": 1383
                        },
                        "name": "World Geodetic System 1984 (G2296)"
                    }
                ],
                "name": "World Geodetic System 1984 ensemble"
            },
            "id": {
                "authority": "EPSG",
                "code": 4326
            },
            "name": "WGS 84"
        },
        "bbox": {
            "east_longitude": 180,
            "north_latitude": 85.06,
            "south_latitude": -85.06,
            "west_longitude": -180
        },
        "conversion": {
            "method": {
                "id": {
                    "authority": "EPSG",
                    "code": 1024
                },
                "name": "Popular Visualisation Pseudo Mercator"
            },
            "name": "Popular Visualisation Pseudo-Mercator",
            "parameters": [
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8801
                    },
                    "name": "Latitude of natural origin",
                    "unit": "degree",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8802
                    },
                    "name": "Longitude of natural origin",
                    "unit": "degree",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8806
                    },
                    "name": "False easting",
                    "unit": "metre",
                    "value": 0
                },
                {
                    "id": {
                        "authority": "EPSG",
                        "code": 8807
                    },
                    "name": "False northing",
                    "unit": "metre",
                    "value": 0
                }
            ]
        },
        "coordinate_system": {
            "axis": [
                {
                    "abbreviation": "X",
                    "direction": "east",
                    "name": "Easting",
                    "unit": "metre"
                },
                {
                    "abbreviation": "Y",
                    "direction": "north",
                    "name": "Northing",
                    "unit": "metre"
                }
            ],
            "subtype": "Cartesian"
        },
        "id": {
            "authority": "EPSG",
            "code": 3857
        },
        "name": "WGS 84 / Pseudo-Mercator",
        "scope": "Web mapping and visualisation.",
        "type": "ProjectedCRS"
    }
