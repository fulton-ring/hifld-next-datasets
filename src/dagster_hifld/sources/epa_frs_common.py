"""Shared FRS capture and candidate-construction primitives.

Endpoint/schema interpretation stays in each issue adapter. These functions
share only byte custody, immutable writes, incomplete-capture retention, and
the first-construction CAS contract.
"""
from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
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
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import capture_http_response
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
                *, max_bytes: int = 128 * 1024 * 1024, reserve=None) -> tuple[bytes, dict[str, object]]:
    """Persist complete GET decoded and wire bodies plus a hash-pinned receipt."""
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

    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        result = capture_http_response(
            response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
            reserve=reserve or (lambda _progress: None),
            retain_wire=lambda data: retain(wire_path, wire_hash, data, "wire"),
            retain_decoded=lambda data: retain(body_path, body_hash, data, "body"),
        )
        final_url = str(response.url)
    receipt = {
        "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": result.status_code, "complete_body": result.complete,
        "wire_bytes": result.wire_bytes_retained, "wire_sha256": result.wire_sha256,
        "bytes": result.decoded_bytes_retained, "sha256": result.decoded_sha256,
        "content_encoding": result.content_encoding, "content_length": result.content_length,
        "error_code": result.error_code,
    }
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    wire_body, body = wire_path.read_bytes(), body_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
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


def processing_identity(repo_root: Path) -> dict[str, object]:
    """Bind the candidate to the committed code and exact runtime versions."""
    root = Path(repo_root).resolve()
    files = {str(path.relative_to(root)): sha256_file(path)
             for path in sorted(root.joinpath("src/dagster_hifld").rglob("*.py"))}
    files.update({name: sha256_file(root / name) for name in ("pyproject.toml", "uv.lock")})
    return {
        "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "files": files, "python": platform.python_version(), "pyarrow": pa.__version__,
        "proj": pyproj.proj_version_str, "geos": shapely.geos_version_string,
    }


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
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"])
    return {"version": version, "reused": True}
