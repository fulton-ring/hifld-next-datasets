"""Shared Census TIGER/Line ZIP capture, quarantine and archive inspection.

Behavior-preserving consolidation of the per-dataset copies in the Census TIGER
acquisition family. Every dataset identity stays in its source adapter: the URL,
archive member roster, pins, byte budgets and record validation are passed
explicitly by each caller, so no caller can inherit another dataset's URL,
filename or pin.
"""
from __future__ import annotations

import hashlib
import io
import json
import platform
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath

import geopandas as gpd
import httpx
import pyogrio
import pyproj
import shapely

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.capture_common import quarantine_incomplete_capture
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
