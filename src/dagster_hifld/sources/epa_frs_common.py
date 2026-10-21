"""Shared FRS capture and candidate-construction primitives.

Endpoint/schema interpretation stays in each issue adapter. These functions
share only byte custody, immutable writes, incomplete-capture retention, and
the first-construction CAS contract.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from uuid import uuid4

import httpx
import pyarrow as pa
import pyproj
import shapely
from dagster_hifld.construction_state import (
    canonical_construction_bundle,
    load_first_construction,
    seal_first_construction,
)
from dagster_hifld.construction_provenance import (
    _MAX_BLOB_BYTES,
    _MAX_FILES,
    _MAX_TOTAL_BLOB_BYTES,
    _MAX_TOTAL_PATH_BYTES,
    _MAX_PATH_BYTES,
    validate_construction_code_identity,
)
from dagster_hifld.http_capture import CaptureProgress, capture_http_response
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def content_derived_version(source_record_sha256: str, source_geometry_sha256: str,
                            source_schema_sha256: str,
                            identity: Mapping[str, object]) -> str:
    """Return the immutable candidate version for one source/code identity."""
    fingerprint = _sha(_canonical({
        "source_record_sha256": source_record_sha256,
        "source_geometry_sha256": source_geometry_sha256,
        "source_schema_sha256": source_schema_sha256,
        "processing_identity": identity,
    }))
    return "v" + fingerprint[:32]


def candidate_prefix(dataset_slug: str, file_slug: str, version: str) -> str:
    """Build the storage prefix from the candidate version, never the run ID."""
    if not dataset_slug or not file_slug or not version:
        raise ValueError("invalid_candidate_prefix")
    return f"{dataset_slug}/{file_slug}/{version}"


def capture_get(client: httpx.Client, url: str, capture_root: Path, name: str,
                *, max_bytes: int = 128 * 1024 * 1024,
                reserve: Callable[[CaptureProgress], None] | None = None) -> tuple[bytes, dict[str, object]]:
    """Persist complete GET decoded and wire bodies plus a hash-pinned receipt.

    Incomplete/status-failed responses and mid-stream HTTP errors still leave
    bounded body/wire evidence and a receipt before this helper raises.
    """
    capture_root.mkdir(parents=True, exist_ok=True)
    wire_path, body_path = capture_root / f"{name}.wire", capture_root / f"{name}.body"
    for path in (wire_path, body_path):
        path.unlink(missing_ok=True)
    wire_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    wire_size = body_size = 0
    observed = datetime.now(timezone.utc).isoformat()

    def retain(path: Path, digest, data: bytes, kind: str) -> None:
        nonlocal wire_size, body_size
        with path.open("ab") as stream:
            stream.write(data)
        if kind == "wire":
            wire_size += len(data); wire_hash.update(data)
        else:
            body_size += len(data); body_hash.update(data)

    try:
        with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
            result = capture_http_response(
                response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                reserve=reserve or (lambda _progress: None),
                retain_wire=lambda data: retain(wire_path, wire_hash, data, "wire"),
                retain_decoded=lambda data: retain(body_path, body_hash, data, "body"),
            )
            final_url = str(response.url)
            status = result.status_code
            complete = result.complete
            content_encoding = result.content_encoding
            content_length = result.content_length
            error_code = result.error_code
    except httpx.HTTPError as exc:
        # Connection/timeout failures before a response still get a durable receipt.
        # Keep the exception class, never its text (which can contain request details).
        wire_path.touch(exist_ok=True)
        body_path.touch(exist_ok=True)
        receipt = {
            "method": "GET", "url": url, "final_url": None, "observed_at": observed,
            "status": None, "complete_body": False,
            "wire_bytes": 0, "wire_sha256": hashlib.sha256().hexdigest(),
            "bytes": 0, "sha256": hashlib.sha256().hexdigest(),
            "content_encoding": None, "content_length": None,
            "error_code": "request_error", "error_type": type(exc).__name__,
        }
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        raise ValueError(f"incomplete_source_capture:{name}") from None
    wire_path.touch(exist_ok=True)
    body_path.touch(exist_ok=True)
    receipt = {
        "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": status, "complete_body": complete,
        "wire_bytes": result.wire_bytes_retained, "wire_sha256": result.wire_sha256,
        "bytes": result.decoded_bytes_retained, "sha256": result.decoded_sha256,
        "content_encoding": content_encoding, "content_length": content_length,
        "error_code": error_code,
        "error_type": result.error_type,
    }
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    wire_body, body = wire_path.read_bytes(), body_path.read_bytes()
    if (not complete or status != 200 or final_url != url
            or len(wire_body) != receipt["wire_bytes"] or _sha(wire_body) != receipt["wire_sha256"]
            or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]):
        raise ValueError(f"incomplete_source_capture:{name}")
    return body, receipt


def write_immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise ValueError("immutable_staged_object_changed:" + key)


CODE_IDENTITY_GIT = "git-commit-blobs-v1"
CODE_IDENTITY_PACKAGED = "sha256-packaged-dagster-hifld-v1"


def _packaged_files(root: Path) -> dict[str, str]:
    package = root / "src/dagster_hifld"
    if not package.is_dir() or package.is_symlink():
        package = root / "dagster_hifld"
        package_prefix = "dagster_hifld"
    else:
        package_prefix = "src/dagster_hifld"
    if not package.is_dir() or package.is_symlink():
        raise ValueError("processing_source_package_missing")
    files = {}
    total_size = 0

    def add_file(path: Path, name: str) -> None:
        nonlocal total_size
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("processing_source_nonregular_file")
        if before.st_size > _MAX_BLOB_BYTES or total_size + before.st_size > _MAX_TOTAL_BLOB_BYTES:
            raise ValueError("processing_source_size_limit")
        digest = sha256_file(path)
        after = path.lstat()
        if (after.st_dev != before.st_dev or after.st_ino != before.st_ino
                or after.st_size != before.st_size or after.st_mtime_ns != before.st_mtime_ns):
            raise ValueError("processing_source_changed_during_hash")
        total_size += before.st_size
        files[name] = digest

    for path in sorted(package.rglob("*")):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink():
            raise ValueError("processing_source_symlink_unsupported")
        if not path.is_file():
            continue
        relative = path.relative_to(package).as_posix()
        add_file(path, f"{package_prefix}/{relative}")
    if not files:
        raise ValueError("processing_source_package_missing")
    for name in ("pyproject.toml", "uv.lock"):
        if not (root / name).is_file():
            raise ValueError("processing_dependency_input_missing:" + name)
        add_file(root / name, name)
    files = dict(sorted(files.items()))
    if len(files) > _MAX_FILES:
        raise ValueError("processing_source_file_limit")
    total_path_bytes = sum(len(name.encode("utf-8")) + 1 for name in files)
    if (any(len(name.encode("utf-8")) > _MAX_PATH_BYTES for name in files)
            or total_path_bytes > _MAX_TOTAL_PATH_BYTES):
        raise ValueError("processing_source_path_limit")
    return files


def _installed_distributions() -> list[dict[str, str]]:
    result = []
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        version = distribution.version
        if name and version:
            result.append({"name": name, "version": version})
    return sorted(result, key=lambda item: (item["name"].casefold(), item["name"], item["version"]))


def _identity_payload(identity: Mapping[str, object]) -> dict[str, object]:
    return {key: identity[key] for key in (
        "code_identity_scheme", "working_tree_status", "files", "installed_distributions",
        "python", "pyarrow", "proj", "geos",
    )}


def _packaged_digest(identity: Mapping[str, object]) -> str:
    return _sha(_canonical(_identity_payload(identity)))


def processing_identity(repo_root: Path) -> dict[str, object]:
    """Bind candidates to exact Git blobs or a versioned installed package identity.

    A packaged runtime without Git records an explicitly non-Git digest over
    every installed Dagster source file, the project and lock files, and the
    complete installed Python distribution/version inventory.
    """
    root = Path(repo_root).resolve()
    versions = {
        "python": platform.python_version(), "pyarrow": pa.__version__,
        "proj": pyproj.proj_version_str, "geos": shapely.geos_version_string,
    }
    git_metadata = root / ".git"
    if git_metadata.exists() or git_metadata.is_symlink():
        files = {str(path.relative_to(root)): sha256_file(path)
                 for path in sorted(root.joinpath("src/dagster_hifld").rglob("*.py"))}
        files.update({name: sha256_file(root / name) for name in ("pyproject.toml", "uv.lock")})
        status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=root,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False)
        working_tree_status = ("clean" if status.returncode == 0 and not status.stdout else
                               "dirty" if status.returncode == 0 else "unknown")
        return {"code_identity_scheme": CODE_IDENTITY_GIT, "working_tree_status": working_tree_status,
                "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "files": files, **versions}

    identity: dict[str, object] = {
        "code_identity_scheme": CODE_IDENTITY_PACKAGED,
        "working_tree_status": "unknown",
        "files": _packaged_files(root),
        "installed_distributions": _installed_distributions(),
        **versions,
    }
    identity["code_commit"] = f"packaged-sha256:{_packaged_digest(identity)}"
    return identity


def quarantine_incomplete_capture(storage: StagingStorageResource, capture_root: Path,
                                  dataset_slug: str, version: str,
                                  captures: Mapping[str, tuple[bytes, Mapping[str, object]]],
                                  reason_code: str) -> str:
    """Retain all completed acquisition evidence under a unique quarantine key."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_staging_resource_required")
    token = f"{version}-{uuid4().hex}"
    base = f"_quarantine/{dataset_slug}/{token}"
    for name, (body, receipt) in captures.items():
        wire = (Path(capture_root) / f"{name}.wire").read_bytes()
        write_immutable(storage, f"{base}/raw/{name}.body", body)
        write_immutable(storage, f"{base}/raw/{name}.wire", wire)
        write_immutable(storage, f"{base}/raw/{name}.receipt.json", _canonical(dict(receipt)) + b"\n")
    write_immutable(storage, f"{base}/metadata/failure.json",
                    _canonical({"reason_code": reason_code, "captured_names": sorted(captures)}) + b"\n")
    return base


def seal_ready_candidate(storage: StagingStorageResource, state_key: str, version: str,
                         fingerprint: str, payload: Mapping[str, object]) -> None:
    """CAS-seal the READY candidate using the shared construction registry."""
    seal_first_construction(storage, state_key, version, fingerprint,
                            canonical_construction_bundle(version, fingerprint, dict(payload)))


def verify_cached_candidate(storage: StagingStorageResource, repo_root: Path, state_key: str,
                            prefix: str, version: str, fingerprint: str, identity: Mapping[str, object],
                            source_record_sha256: str, source_schema_sha256: str,
                            expected_objects: Mapping[str, Mapping[str, object]],
                            capture_receipts: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    """Verify a same-version candidate and original raw receipts before reuse."""
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != source_record_sha256
            or payload["source_schema_sha256"] != source_schema_sha256
            or payload["capture_receipts"] != dict(capture_receipts)
            or payload["objects"] != dict(expected_objects)):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    expected_keys = {f"{prefix}/{name}" for name in expected_objects}
    actual_keys = set(storage.list_prefix(prefix))
    # Catalog owns these derived documents; they are not acquisition objects
    # and do not grant source, quality or release approval on cache reuse.
    derived_names = {
        "metadata/quality_manifest.json", "metadata/data_dictionary.json",
        "metadata/resolved_provenance.json", "metadata/source/quality_manifest.json",
        "metadata/source/data_dictionary.json",
    }
    snapshot_names = set()
    manifest_descriptor = expected_objects.get("metadata/source_manifest.json")
    if manifest_descriptor is not None:
        snapshot_names = {
            "metadata/source/source_manifest.json",
            f"metadata/source/version_{manifest_descriptor['sha256']}_source_manifest.json",
        }
    allowed_keys = expected_keys | {f"{prefix}/{name}" for name in derived_names | snapshot_names}
    if not expected_keys <= actual_keys or actual_keys - allowed_keys:
        raise ValueError("cached_candidate_storage_roster_changed")
    for name in snapshot_names:
        key = f"{prefix}/{name}"
        if key not in actual_keys:
            continue
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or manifest_descriptor != {
                "bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_catalog_source_snapshot_changed:" + name)
    for name, descriptor in expected_objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    for name, receipt in capture_receipts.items():
        body = storage.read_key(f"{prefix}/raw/{name}.body")
        wire = storage.read_key(f"{prefix}/raw/{name}.wire")
        receipt_body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt_body != _canonical(dict(receipt)) + b"\n"):
            raise ValueError("cached_original_capture_changed:" + name)
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"], identity=identity)
    return {"version": version, "reused": True}
