"""Shared source capture, candidate-construction, seal and quarantine primitives.

Endpoint/schema interpretation stays in each dataset adapter. These functions
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
from typing import Callable, Iterable, Iterator, Mapping
from uuid import uuid4

import httpx
import pyarrow as pa
import pyproj
import shapely
from dagster_hifld.catalog import summarize_staged_catalog
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
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.validation import sha256_file


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


def safe_candidate_version(value: str) -> bool:
    """Whether a version token is safe to use as a candidate storage path segment."""
    return (isinstance(value, str) and 1 <= len(value) <= 128
            and all(char.isascii() and (char.isalnum() or char in "._-") for char in value)
            and value not in {".", ".."})


def capture_get(client: httpx.Client, url: str, capture_root: Path, name: str,
                *, max_bytes: int = 128 * 1024 * 1024,
                max_wire_block_bytes: int | None = None,
                include_observed_wire: bool = False,
                accept_encoding: str = "identity",
                reserve: Callable[[CaptureProgress], None] | None = None,
                before_retain: Callable[[str, bytes], None] | None = None) -> tuple[bytes, dict[str, object]]:
    """Persist complete GET decoded and wire bodies plus a hash-pinned receipt.

    Incomplete/status-failed responses and mid-stream HTTP errors still leave
    bounded body/wire evidence and a receipt before this helper raises. When
    supplied, ``before_retain`` runs before each block is written, so an adapter
    can refuse an oversized delivered block without exceeding its disk budget.
    ``accept_encoding`` selects the requested content coding ("identity" by
    default); producers that answer gzip keep the receipt's wire side
    compressed and the body side decoded, and the response is rejected when the
    coding is neither identity, gzip nor deflate.
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
        if before_retain is not None:
            before_retain(kind, data)
        with path.open("ab") as stream:
            stream.write(data)
        if kind == "wire":
            wire_size += len(data); wire_hash.update(data)
        else:
            body_size += len(data); body_hash.update(data)

    try:
        with client.stream("GET", url, headers={"Accept-Encoding": accept_encoding}) as response:
            result = capture_http_response(
                response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                max_wire_block_bytes=max_wire_block_bytes,
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
    if include_observed_wire:
        receipt["observed_wire_bytes"] = result.wire_bytes_received
        receipt["observed_wire_sha256"] = result.received_wire_sha256
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    wire_body, body = wire_path.read_bytes(), body_path.read_bytes()
    if (not complete or status != 200 or final_url != url
            or len(wire_body) != receipt["wire_bytes"] or _sha(wire_body) != receipt["wire_sha256"]
            or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]):
        raise ValueError(f"incomplete_source_capture:{name}")
    return body, receipt


def validate_capture_request_identity(name: str, receipt: Mapping, expected_url: str) -> None:
    """Bind retained response bytes to the exact GET endpoint that names them."""
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def load_checkpoint_receipt_map(
    capture_root: Path,
    document_names: tuple[str, ...],
    state: Mapping,
    load_receipt: Callable[[Path, str], dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Read the expected document and checkpoint-segment receipts without loading bodies."""
    names = [*document_names, *(str(entry["name"]) for entry in state.get("segments", [])
                               if isinstance(entry, dict) and entry.get("name"))]
    return {name: load_receipt(capture_root, name) for name in names}


def write_immutable(storage: StagingStorageResource, key: str, body: bytes, *,
                    error_type: type[Exception] = ValueError,
                    error_name: str | None = None,
                    error_prefix: str = "immutable_staged_object_changed:",
                    before_create: Callable[[], None] | None = None) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        if before_create is not None:
            before_create()
        storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise error_type(error_prefix + (key if error_name is None else error_name))


def write_immutable_key(storage, key, body, *, error_type=ValueError,
                        error_prefix="immutable_staged_object_changed:") -> str:
    """Write immutable bytes and return the storage-prefixed key for provenance."""
    write_immutable(storage, key, body, error_type=error_type, error_prefix=error_prefix)
    return storage._ensure_prefixed(key)


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


def load_receipt(capture_root: Path, name: str, error: type[ValueError]) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise error("retained_capture_receipt_invalid:" + name)
    return receipt


def read_capture(capture_root: Path, name: str,
                 error: type[ValueError]) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = load_receipt(capture_root, name, error)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise error("retained_capture_receipt_invalid:" + name)
    return body, receipt


def read_retained_capture(capture_root: Path, name: str,
                          error: type[ValueError]) -> tuple[bytes, dict[str, object]]:
    """Read retained failure evidence without requiring a complete GET.

    Failure retention keeps partial attempts: the three files must be present,
    the receipt must parse, and body and wire must match the receipt's sizes
    and digests. The complete-GET contract belongs to replay, not to evidence.
    """
    root = Path(capture_root)
    receipt_path, body_path, wire_path = (root / f"{name}.receipt.json",
                                          root / f"{name}.body", root / f"{name}.wire")
    if not receipt_path.is_file():
        raise error("retained_capture_receipt_missing:" + name)
    try:
        receipt = json.loads(receipt_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("retained_capture_receipt_unreadable:" + name) from exc
    if not isinstance(receipt, dict):
        raise error("retained_capture_receipt_unreadable:" + name)
    missing = [suffix for suffix, path in (("body", body_path), ("wire", wire_path))
               if not path.is_file()]
    if missing:
        raise error("retained_capture_file_missing:" + name + ":" + ",".join(missing))
    try:
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
    except OSError as exc:
        raise error("retained_capture_file_unreadable:" + name) from exc
    if (receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
            or receipt.get("sha256") != _sha(body) or receipt.get("wire_sha256") != _sha(wire)):
        raise error("retained_capture_evidence_mismatch:" + name)
    return body, receipt


class RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str], error: type[ValueError],
                 reader: Callable[[Path, str, type[ValueError]],
                                  tuple[bytes, dict[str, object]]] | None = None):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)
        self._error = error
        self._reader = reader or read_capture

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return self._reader(self._capture_root, name, self._error)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def quarantine_incomplete_capture(storage: StagingStorageResource, capture_root: Path,
                                  dataset_slug: str, version: str,
                                  captures: Mapping[str, tuple[bytes, Mapping[str, object]]],
                                  reason_code: str, *,
                                  not_retained: Iterable[Mapping[str, str]] = ()) -> str:
    """Retain all completed acquisition evidence under a unique quarantine key.

    A capture whose bytes cannot be read is recorded under ``not_retained``
    (name and reason) in ``metadata/failure.json`` instead of raising from
    the failure path.
    """
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_staging_resource_required")
    token = f"{version}-{uuid4().hex}"
    base = f"_quarantine/{dataset_slug}/{token}"
    captured_names: list[str] = []
    # not_retained is omitted while empty so adapters not yet migrated on
    # issue #1225 keep their existing failure.json contract.
    skipped: list[dict[str, str]] = [dict(entry) for entry in not_retained]
    for name in captures:
        try:
            body, receipt = captures[name]
        except Exception as failure:
            skipped.append({"name": str(name), "reason": str(failure)})
            continue
        try:
            wire = (Path(capture_root) / f"{name}.wire").read_bytes()
        except FileNotFoundError:
            skipped.append({"name": str(name),
                            "reason": "retained_capture_wire_missing:" + str(name)})
            continue
        except OSError as failure:
            skipped.append({"name": str(name), "reason": str(failure)})
            continue
        write_immutable(storage, f"{base}/raw/{name}.body", body)
        write_immutable(storage, f"{base}/raw/{name}.wire", wire)
        write_immutable(storage, f"{base}/raw/{name}.receipt.json", _canonical(dict(receipt)) + b"\n")
        captured_names.append(str(name))
    failure = {"reason_code": reason_code, "captured_names": sorted(captured_names)}
    if skipped:
        failure["not_retained"] = sorted(skipped, key=lambda entry: str(entry.get("name")))
    write_immutable(storage, f"{base}/metadata/failure.json", _canonical(failure) + b"\n")
    return base


def retain_failed_capture(storage: StagingStorageResource, capture_root: Path,
                          dataset_slug: str, version: str, exc: BaseException, *,
                          error: type[ValueError]) -> str | None:
    """Retain every readable capture after a failure, recording what cannot be.

    Scans the capture root once; each capture's bytes are read lazily by the
    quarantine, one at a time. A capture whose receipt, body or wire is
    missing, unreadable or inconsistent is recorded under ``not_retained``
    (name and reason) in ``metadata/failure.json`` instead of being dropped
    or replacing the failure. This helper catches its own failures and
    records them where it can; it never raises in place of ``exc`` — the
    caller re-raises the original error.
    """
    root = Path(capture_root)
    not_retained: list[dict[str, str]] = []
    try:
        names = sorted({path.name.removesuffix(suffix)
                        for path in root.iterdir()
                        for suffix in (".body", ".wire", ".receipt.json")
                        if path.name.endswith(suffix)})
    except OSError as failure:
        names = []
        not_retained.append({"name": "*",
                             "reason": "capture_root_unreadable:" + type(failure).__name__})
    views = RetainedCaptureViews(root, names, error, reader=read_retained_capture)
    try:
        return quarantine_incomplete_capture(
            storage, root, dataset_slug, version, views, type(exc).__name__,
            not_retained=not_retained)
    except Exception as failure:
        try:
            (root / "capture_retention_failed.json").write_bytes(_canonical({
                "error_type": type(failure).__name__,
                "not_retained": not_retained}) + b"\n")
        except OSError:
            pass
        return None


def seal_ready_candidate(storage: StagingStorageResource, state_key: str, version: str,
                         fingerprint: str, payload: Mapping[str, object]) -> None:
    """CAS-seal the READY candidate using the shared construction registry."""
    seal_first_construction(storage, state_key, version, fingerprint,
                            canonical_construction_bundle(version, fingerprint, dict(payload)))


def write_candidate_outputs(
    storage: StagingStorageResource, prefix: str, outputs: Mapping[str, bytes | Path], *,
    snapshot_check: str = "stable", readback_order: str = "per-object",
    error_prefix: str = "candidate_object_readback_failed:",
    error_type: type[Exception] = ValueError,
    writer: Callable[[StagingStorageResource, str, bytes], None] | None = None,
    reserve: Callable[[int], None] | None = None,
    file_snapshot_check: str = "content",
) -> dict[str, dict[str, object]]:
    """Write an ordered candidate inventory and prove each stored object's bytes.

    Preserve the adapter's admission policy: some producers verify content
    only, others also require a stable snapshot; two-pass candidates finish
    every immutable write before admitting any readback. Mapping order is the
    upload order, so callers can supply sorted outputs when required. Native
    file paths use the streamed immutable-file writer and are never buffered.
    """
    if snapshot_check not in {"content", "stable", "observed", "none"} or readback_order not in {"per-object", "after-all"}:
        raise ValueError("invalid_candidate_readback_policy")
    write = writer or write_immutable
    descriptors = {}
    files = {name: {"bytes": body.stat().st_size, "sha256": sha256_file(body)}
             for name, body in outputs.items() if isinstance(body, Path)}

    def retain(name, body):
        if reserve is not None:
            reserve(files[name]["bytes"] if isinstance(body, Path) else len(body))
        if isinstance(body, Path):
            write_immutable_file(storage, (f"{prefix}/{name}" if prefix else name), body, files[name],
                                 recheck_snapshot=file_snapshot_check == "stable")
        else:
            write(storage, (f"{prefix}/{name}" if prefix else name), body)

    def admit(name: str, body: bytes | Path) -> None:
        expected = files[name] if isinstance(body, Path) else {"bytes": len(body), "sha256": _sha(body)}
        if snapshot_check == "none":
            descriptors[name] = expected
        else:
            descriptors[name] = _readback_candidate_output(
                storage, prefix, name, None if snapshot_check == "observed" else expected,
                snapshot_check == "stable", error_prefix, error_type)

    if readback_order == "after-all":
        for name, body in outputs.items():
            retain(name, body)
        for name, body in outputs.items():
            admit(name, body)
    else:
        for name, body in outputs.items():
            retain(name, body)
            admit(name, body)
    return descriptors


def _readback_candidate_output(storage, prefix, name, expected, stable, error_prefix, error_type, error_name=None):
    key = (f"{prefix}/{name}" if prefix else name)
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        raise error_type(error_prefix + (name if error_name is None else error_name))
    if expected is None:
        return {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
    if (snapshot.size != expected["bytes"] or storage.sha256_key(key) != expected["sha256"]
            or (stable and storage.object_snapshot(key) != snapshot)):
        raise error_type(error_prefix + (name if error_name is None else error_name))
    return dict(expected)


def verify_candidate_outputs(
    storage: StagingStorageResource, prefix: str,
    descriptors: Mapping[str, Mapping[str, object]], *,
    error_prefix: str = "candidate_output_physical_readback_failed:", error_suffix: str = "relative",
) -> None:
    """Admit a prepared inventory after every streamed output has been written."""
    for name, expected in descriptors.items():
        _readback_candidate_output(storage, prefix, name, expected, True, error_prefix, ValueError,
                                   None if error_suffix == "relative" else "")


def write_candidate_capture_files(
    storage: StagingStorageResource, prefix: str, root: Path,
    receipts: Mapping[str, Mapping[str, object]], read_body: Callable, *,
    order: str = "sorted", receipt_encoding: str = "canonical",
) -> None:
    """Write raw evidence one blob at a time before a separate inventory readback."""
    items = sorted(receipts.items()) if order == "sorted" else receipts.items()
    for name, receipt in items:
        write_immutable(storage, f"{prefix}/raw/{name}.body", read_body(root, name))
        write_immutable(storage, f"{prefix}/raw/{name}.wire", (root / f"{name}.wire").read_bytes())
        if receipt_encoding == "canonical":
            write_immutable(storage, f"{prefix}/raw/{name}.receipt.json", _canonical(receipt) + b"\n")


def write_candidate_captures(
    storage: StagingStorageResource, prefix: str, root: Path, names: Iterable[str],
    read_capture: Callable, *, snapshot_check: str = "content",
) -> dict[str, dict[str, object]]:
    """Admit retained body/wire/receipt triples without buffering the population."""
    inventory = {}
    for name in names:
        body, wire, receipt = read_capture(root, name)
        outputs = {f"raw/{name}.body": body, f"raw/{name}.wire": wire,
                   f"raw/{name}.receipt.json": _canonical(receipt) + b"\n"}
        inventory.update(write_candidate_outputs(storage, prefix, outputs,
                                                 snapshot_check=snapshot_check))
    return inventory


def upload_candidate_local_file(
    storage: StagingStorageResource, prefix: str, relative: str, path: Path,
    expected: Mapping[str, object],
) -> dict[str, object]:
    """Upload a streamed native file and admit its observed size and digest."""
    key = f"{prefix}/{relative}"
    storage.upload_local_file(path, key)
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        raise ValueError("candidate_object_readback_failed:" + relative)
    actual = {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
    if actual != expected:
        raise ValueError("candidate_object_readback_failed:" + relative)
    return actual


def write_candidate_file_by_sha(
    storage: StagingStorageResource, prefix: str, relative: str, path: Path,
    expected_sha256: str,
) -> None:
    """Preserve an existing file's bytes or upload a new file, then prove its SHA."""
    key = f"{prefix}/{relative}"
    if storage.object_exists(key):
        if storage.sha256_key(key) != expected_sha256:
            raise ValueError("immutable_staged_object_changed:" + key)
    else:
        storage.upload_local_file(path, key)
    if storage.sha256_key(key) != expected_sha256:
        raise ValueError("candidate_object_readback_failed:" + relative)


def write_candidate_blobs(storage, prefix, blobs, *, writer=None, reserve=None):
    """Admit a lazy, ordered native artifact stream through ordinary custody."""
    for name, body in blobs:
        write_candidate_outputs(storage, prefix, {name: body}, snapshot_check="none",
                                writer=writer, reserve=reserve)


def write_candidate_transport_evidence(
    storage: StagingStorageResource, prefix: str, root: Path,
    descriptors: Mapping[str, Mapping[str, object]], captures: Mapping,
    receipt_body: Callable[[str], bytes], reserve: Callable[[int], None],
) -> None:
    """Retain exact wire/receipt evidence, then release verified decoded copies."""
    for name in descriptors:
        raw_name = name.removeprefix("raw/").removesuffix(".receipt.json")
        body = ((root / name.removeprefix("raw/")).read_bytes()
                if name.endswith(".wire") else receipt_body(raw_name))
        reserve(len(body))
        write_immutable(storage, f"{prefix}/{name}", body)
        if name.endswith(".receipt.json"):
            body_path = root / f"{raw_name}.body"
            if body_path.exists():
                decoded = body_path.read_bytes()
                receipt = captures[raw_name][1]
                if len(decoded) != receipt["bytes"] or _sha(decoded) != receipt["sha256"]:
                    raise ValueError("capture_decoded_body_changed:" + raw_name)
                body_path.unlink()


def write_candidate_ready_state(storage: StagingStorageResource, key: str, body: bytes,
                                error_type: type[Exception]) -> None:
    """Preserve ready-state admission for private transactional source candidates."""
    if storage.object_snapshot(key) is None:
        storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body:
        raise error_type("candidate_ready_state_changed")


def stage_candidate_object(storage: StagingStorageResource, key: str, *,
                           body: bytes | None = None, path: Path | None = None) -> dict[str, object]:
    """Admit one bytes-or-file NFHL object with its full-key error contract."""
    return write_candidate_outputs(
        storage, "", {key: path if path is not None else body},
        error_prefix="candidate_output_physical_readback_failed:")[key]


def reuse_sealed_candidate(storage: StagingStorageResource, state_key: str,
                           verify: Callable[[], dict[str, object]]) -> dict[str, object] | None:
    """Dispatch reuse only after a seal exists; the adapter verifies its source contract.

    A failed verification must propagate. Never fall through to reconstruction
    of a candidate whose immutable construction state already exists.
    """
    if storage.object_snapshot(state_key) is not None:
        return verify()
    return None


def load_candidate_checkpoint(
    storage: StagingStorageResource, state_key: str, version: str,
    identity: Mapping[str, object], source_record_sha256: str,
    source_schema_sha256: str, error_type: type[ValueError],
) -> tuple[str, dict[str, object]]:
    """Load a sealed construction checkpoint bound to the current source and code.

    Keep the caller's error type while sharing the envelope contract. The
    returned original receipts and inventory feed verify_cached_candidate;
    source-native parquet and dataset result checks remain in the adapter.
    """
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity",
            "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != source_record_sha256
            or payload["source_schema_sha256"] != source_schema_sha256):
        raise error_type("cached_first_candidate_identity_or_source_changed")
    return fingerprint, payload


def upload_candidate_objects(
    storage: StagingStorageResource, prefix: str, outputs: Mapping[str, bytes],
    capture_root: Path, capture_names: Iterable[str],
    read_capture: Callable[[Path, str], tuple[bytes, Mapping[str, object]]],
    *, descriptors: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    """Upload prepared outputs and validated raw evidence one capture at a time.

    The reader preserves each family's receipt/checkpoint validation. Existing
    descriptors represent large files already streamed to staging.
    """
    result = {name: dict(value) for name, value in (descriptors or {}).items()}

    def retain(relative: str, blob: bytes) -> None:
        write_immutable(storage, f"{prefix}/{relative}", blob)
        result[relative] = {"bytes": len(blob), "sha256": _sha(blob)}

    for relative, body in outputs.items():
        retain(relative, body)
    for name in sorted(capture_names):
        body, receipt = read_capture(capture_root, name)
        wire = (capture_root / f"{name}.wire").read_bytes()
        for suffix, blob in (("body", body), ("wire", wire),
                             ("receipt.json", _canonical(dict(receipt)) + b"\n")):
            retain(f"raw/{name}.{suffix}", blob)
    return result


def write_immutable_file(storage: StagingStorageResource, key: str, source: Path,
                          descriptor: Mapping[str, object], *, recheck_snapshot: bool = False) -> None:
    """write_immutable for a local file too large to buffer as bytes."""
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        if recheck_snapshot:
            local = StagingStorageResource(use_local=True, local_dir=str(source.parent))
            source_snapshot = local.object_snapshot(source.name)
            if (source_snapshot is None or source_snapshot.size != descriptor["bytes"]
                    or source_snapshot.sha256 != descriptor["sha256"]):
                raise ValueError("immutable_staged_object_changed:" + key)
            local.copy_key_to_if_unchanged(
                storage, source.name, key, source_snapshot=source_snapshot, destination_snapshot=None)
        else:
            storage.upload_local_file(source, key)
        return
    if (snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]
            or (recheck_snapshot and storage.object_snapshot(key) != snapshot)):
        raise ValueError("immutable_staged_object_changed:" + key)


def upload_candidate_file(
    storage: StagingStorageResource, key: str, path: Path,
    readback_descriptor: Callable[[StagingStorageResource, str], Mapping[str, object]],
    error_type: type[ValueError],
) -> dict[str, object]:
    """Stream a new candidate file and verify its stored bytes before sealing."""
    storage.upload_local_file(path, key)
    expected = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    actual = dict(readback_descriptor(storage, key))
    if actual != expected:
        raise error_type("candidate_parquet_readback_mismatch")
    return actual


def upload_refused_candidate_evidence(
    storage: StagingStorageResource, prefix: str, capture_root: Path,
    receipts: Mapping[str, object], descriptors: dict[str, dict[str, object]],
    validate_transport: Callable, error_type: type[ValueError],
) -> None:
    """Retain refused attempts with transport validation and mutation detection."""
    evidence = {path.name.removesuffix(suffix) for path in capture_root.iterdir()
                for suffix in (".body", ".wire", ".receipt.json") if path.name.endswith(suffix)} - set(receipts)
    for name in sorted(evidence):
        paths = [capture_root / f"{name}.{suffix}" for suffix in ("body", "wire", "receipt.json")]
        before = [path.stat() for path in paths]
        validate_transport(capture_root, names=(name,))
        blobs = [path.read_bytes() for path in paths]
        after = [path.stat() for path in paths]
        if any((a.st_dev, a.st_ino, a.st_size, a.st_mtime_ns, a.st_ctime_ns)
               != (b.st_dev, b.st_ino, b.st_size, b.st_mtime_ns, b.st_ctime_ns)
               for a, b in zip(before, after)):
            raise error_type("retained_capture_changed_during_admission:" + name)
        for suffix, blob in zip(("body", "wire", "receipt.json"), blobs):
            relative = f"evidence/refused/{name}.{suffix}"
            write_immutable(storage, f"{prefix}/{relative}", blob)
            descriptors[relative] = {"bytes": len(blob), "sha256": _sha(blob)}


def seal_candidate_objects(
    storage: StagingStorageResource, state_key: str, version: str,
    identity: Mapping[str, object], receipts: Mapping[str, object],
    descriptors: Mapping[str, Mapping[str, object]], *,
    source_record_sha256: str, source_schema_sha256: str,
    fingerprint: str | None = None,
) -> None:
    """CAS-seal the exact uploaded inventory and checkpoint receipt map."""
    payload = {"processing_identity": identity,
               "source_record_sha256": source_record_sha256,
               "source_schema_sha256": source_schema_sha256,
               "capture_receipts": receipts, "objects": descriptors}
    seal_ready_candidate(storage, state_key, version,
                         _sha(_canonical(identity)) if fingerprint is None else fingerprint, payload)


def stage_candidate_objects(
    storage: StagingStorageResource, prefix: str, state_key: str, version: str,
    identity: Mapping[str, object], outputs: Mapping[str, bytes],
    capture_root: Path, receipts: Mapping[str, object], read_capture: Callable,
    *, source_record_sha256: str, source_schema_sha256: str,
    capture_names: Iterable[str] | None = None,
    descriptors: Mapping[str, Mapping[str, object]] | None = None,
    retain_extra: Callable[[dict[str, dict[str, object]]], None] | None = None,
) -> None:
    """Own the upload-to-seal transition for a prepared native candidate.

    Upload failures leave the candidate unsealed. Source-specific preparation
    and reuse verification happen before entry; this boundary records exactly
    the accepted outputs, retained responses and optional failed evidence.
    """
    inventory = upload_candidate_objects(
        storage, prefix, outputs, capture_root,
        receipts if capture_names is None else capture_names, read_capture,
        descriptors=descriptors)
    if retain_extra is not None:
        retain_extra(inventory)
    seal_candidate_objects(
        storage, state_key, version, identity, receipts, inventory,
        source_record_sha256=source_record_sha256,
        source_schema_sha256=source_schema_sha256)


STAGED_RELEASE_DERIVATIVE_NAMES = frozenset({"collection.json", "README.md", "AGENTS.md"})
_MAX_STAGED_RELEASE_DOCUMENT_BYTES = 2 * 1024 * 1024


def validate_staged_release_derivatives(storage: StagingStorageResource, prefix: str,
                                        actual_keys: set[str]) -> set[str]:
    """Validate only the exact optional trio generated by release-metadata preparation.

    These are derived STAC/document files, not source-owned candidate objects.
    If any one is present, require all three; validate the Collection against
    this candidate's dataset/file/semantic version and bound the two UTF-8 docs.
    """
    expected = {f"{prefix}/{name}" for name in STAGED_RELEASE_DERIVATIVE_NAMES}
    found = actual_keys & expected
    if not found:
        return set()
    if found != expected:
        raise ValueError("incomplete_staged_release_derivatives")
    from dagster_hifld.portolan.release import read_staged_release

    read_staged_release(storage, prefix)
    for name in ("README.md", "AGENTS.md"):
        body = storage.read_key(f"{prefix}/{name}")
        if not isinstance(body, bytes) or not 0 < len(body) <= _MAX_STAGED_RELEASE_DOCUMENT_BYTES:
            raise ValueError("invalid_staged_release_document_size:" + name)
        try:
            body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid_staged_release_document_encoding:" + name) from exc
    return found


def _validate_cached_catalog_sidecars(storage: StagingStorageResource, prefix: str,
                                      actual_keys: set[str]) -> None:
    """Recompute every catalog derivative from the sealed native candidate."""
    flat_names = ("metadata/quality_manifest.json", "metadata/data_dictionary.json")
    flat_keys = {f"{prefix}/{name}" for name in flat_names}
    found_flat = actual_keys & flat_keys
    catalog_derivative_keys = flat_keys | {
        f"{prefix}/metadata/resolved_provenance.json",
        f"{prefix}/metadata/source/quality_manifest.json",
        f"{prefix}/metadata/source/data_dictionary.json",
    }
    found_catalog_derivatives = actual_keys & catalog_derivative_keys
    if not found_catalog_derivatives:
        return
    if found_flat != flat_keys:
        raise ValueError("cached_catalog_metadata_incomplete")

    source_catalog_names = (
        "metadata/source/quality_manifest.json",
        "metadata/source/data_dictionary.json",
    )
    source_catalog_keys = {f"{prefix}/{name}" for name in source_catalog_names}
    found_source_catalog = actual_keys & source_catalog_keys
    if found_source_catalog not in (set(), source_catalog_keys):
        raise ValueError("cached_catalog_source_snapshot_incomplete")

    parts = prefix.split("/")
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError("cached_catalog_prefix_invalid")
    dataset_slug, file_slug, version = parts
    resolved_manifest = load_resolved_source_manifest(storage, dataset_slug, file_slug, version)
    summary = summarize_staged_catalog(
        storage,
        dataset_slug,
        file_slug,
        version,
        dataset_slug,
        source_metadata=resolved_manifest.metadata,
    )

    def read_stable_json(key: str) -> object:
        before = storage.object_snapshot(key)
        if before is None:
            raise ValueError("cached_catalog_metadata_missing:" + key.rsplit("/", 1)[-1])
        body = storage.read_key(key)
        if storage.object_snapshot(key) != before:
            raise ValueError("cached_catalog_metadata_changed_during_validation")
        try:
            return json.loads(body)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("cached_catalog_metadata_invalid") from exc

    quality_key = f"{prefix}/metadata/quality_manifest.json"
    dictionary_key = f"{prefix}/metadata/data_dictionary.json"
    quality = read_stable_json(quality_key)
    dictionary = read_stable_json(dictionary_key)
    if quality != summary.quality_manifest or dictionary != summary.data_dictionary:
        raise ValueError("cached_catalog_metadata_mismatch")

    resolved_key = f"{prefix}/metadata/resolved_provenance.json"
    expected_resolved = provenance_projection(resolved_manifest.metadata)
    if (resolved_key in actual_keys) != (expected_resolved is not None):
        raise ValueError("cached_catalog_resolved_provenance_mismatch")
    if expected_resolved is not None and read_stable_json(resolved_key) != expected_resolved:
        raise ValueError("cached_catalog_resolved_provenance_mismatch")

    if found_source_catalog:
        source_quality_key = f"{prefix}/metadata/source/quality_manifest.json"
        source_dictionary_key = f"{prefix}/metadata/source/data_dictionary.json"
        if (read_stable_json(source_quality_key) != quality
                or read_stable_json(source_dictionary_key) != dictionary):
            raise ValueError("cached_catalog_source_snapshot_changed")


def verify_cached_objects(storage, prefix, objects, *,
                          error_prefix="cached_candidate_object_integrity_failed:",
                          descriptor_check="equality", error_type=ValueError):
    """Check exact stored descriptors and stable object identities for reuse."""
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if ((descriptor_check in {"dict", "fields"} and not isinstance(descriptor, dict))
                or snapshot is None):
            raise error_type(error_prefix + str(name))
        if descriptor_check == "fields":
            changed = (snapshot.size != descriptor.get("bytes")
                       or storage.sha256_key(key) != descriptor.get("sha256")
                       or storage.object_snapshot(key) != snapshot)
        else:
            changed = (descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                       or storage.object_snapshot(key) != snapshot)
        if changed:
            raise error_type(error_prefix + str(name))


def verify_cached_capture_receipts(storage, prefix, capture_receipts):
    """Bind cached raw body/wire objects to their original complete receipts."""
    for name, receipt in capture_receipts.items():
        body = storage.read_key(f"{prefix}/raw/{name}.body")
        wire = storage.read_key(f"{prefix}/raw/{name}.wire")
        receipt_body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt_body != _canonical(dict(receipt)) + b"\n"):
            raise ValueError("cached_original_capture_changed:" + name)


def verify_cached_transport_receipts(storage, prefix, receipts, verify_wire, *, receipt_encoding="canonical"):
    """Validate wire-only retained habitat evidence under its native receipt policy."""
    for name, receipt in receipts.items():
        if not isinstance(receipt, dict):
            raise ValueError("cached_receipt_invalid:" + name)
        wire = storage.read_key(f"{prefix}/raw/{name}.wire")
        verify_wire(wire, receipt, name)
        body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        valid = (_sha(body) == receipt.get("raw_receipt_sha256") if receipt_encoding == "sha256"
                 else body == _canonical(receipt) + b"\n")
        if not valid:
            raise ValueError("cached_original_capture_changed:" + name)


def verify_cached_source_snapshots(storage, prefix, names, source_manifest, *, error_prefix):
    """Validate catalog source snapshots against native primary metadata, in order."""
    for name in names:
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if name.endswith("source_manifest.json"):
            expected = source_manifest
        else:
            primary_key = f"{prefix}/metadata/{name.removeprefix('metadata/source/')}"
            primary = storage.object_snapshot(primary_key)
            expected = ({"bytes": primary.size, "sha256": storage.sha256_key(primary_key)}
                        if primary is not None else None)
        if (snapshot is None or expected != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError(error_prefix + name)


def verify_cached_raw_bodies(storage, prefix, receipts, *, skip_names=(), error_prefix):
    """Bind body-only native FileGDB metadata evidence to original receipt hashes."""
    for name, receipt in receipts.items():
        if name in skip_names:
            continue
        body = storage.read_key(f"{prefix}/raw/{name}.body")
        if receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body):
            raise ValueError(error_prefix + name)


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
        found_snapshots = {f"{prefix}/{name}" for name in snapshot_names} & actual_keys
        if found_snapshots and len(found_snapshots) != len(snapshot_names):
            raise ValueError("cached_catalog_source_snapshot_incomplete")
    staged_release_keys = {f"{prefix}/{name}" for name in STAGED_RELEASE_DERIVATIVE_NAMES}
    allowed_keys = expected_keys | {f"{prefix}/{name}" for name in derived_names | snapshot_names} | staged_release_keys
    if not expected_keys <= actual_keys or actual_keys - allowed_keys:
        raise ValueError("cached_candidate_storage_roster_changed")
    validate_staged_release_derivatives(storage, prefix, actual_keys)
    verify_cached_objects(storage, prefix, {name: manifest_descriptor for name in snapshot_names
        if f"{prefix}/{name}" in actual_keys}, error_prefix="cached_catalog_source_snapshot_changed:")
    verify_cached_objects(storage, prefix, expected_objects)
    verify_cached_capture_receipts(storage, prefix, capture_receipts)
    # Page evidence whose receipts are not duplicated into the construction
    # state is still bound to the first construction through the state's own
    # object descriptors above; here the staged receipt must self-canonicalize
    # and bind exactly the staged body and wire bytes of its page.
    doc_receipt_names = set(capture_receipts)
    for name in sorted(expected_objects):
        if not name.startswith("raw/") or not name.endswith(".body"):
            continue
        page = name[len("raw/"):-len(".body")]
        if page in doc_receipt_names:
            continue
        wire_descriptor = expected_objects.get(f"raw/{page}.wire")
        receipt_descriptor = expected_objects.get(f"raw/{page}.receipt.json")
        if not isinstance(wire_descriptor, dict) or not isinstance(receipt_descriptor, dict):
            raise ValueError("cached_page_evidence_descriptor_missing:" + page)
        body = storage.read_key(f"{prefix}/{name}")
        wire = storage.read_key(f"{prefix}/raw/{page}.wire")
        receipt_body = storage.read_key(f"{prefix}/raw/{page}.receipt.json")
        try:
            receipt = json.loads(receipt_body)
        except ValueError as exc:
            raise ValueError("cached_original_capture_changed:" + page) from exc
        if (not isinstance(receipt, dict)
                or _canonical(receipt) + b"\n" != receipt_body
                or receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("cached_original_capture_changed:" + page)
    _validate_cached_catalog_sidecars(storage, prefix, actual_keys)
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"], identity=identity)
    return {"version": version, "reused": True}


def existing_filesystem_path(path: str | Path) -> Path:
    """Find the configured path's filesystem without creating directories."""
    root = Path(path).absolute()
    return next(parent for parent in (root, *root.parents) if parent.exists())
