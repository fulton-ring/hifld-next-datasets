"""Derive a dataset alias candidate from another dataset's staged candidate.

A derived alias reuses a complete staged source candidate instead of fetching
the same producer release again. Only the dataset identity (title, native file
names and the derived-output path) changes; source inputs, native rows, geometry
and provenance stay the source's. The alias is staged under its own
``{dataset}/{file}/{version}`` prefix so the shared publish graph can catalog and
release it independently.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path, PurePosixPath
from uuid import uuid4

from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

READY_MARKER = "metadata/candidate_ready.json"
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024


class CandidateAliasError(ValueError):
    """The source candidate is incomplete, changed or unsafe."""


def _encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _local_root(storage: StagingStorageResource) -> Path:
    if (isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local
            or not storage.local_dir or storage.prefix):
        raise CandidateAliasError("explicit_isolated_local_staging_required")
    root = Path(storage.local_dir).absolute()
    if ".." in root.parts or any(path.is_symlink() for path in [root, *root.parents]):
        raise CandidateAliasError("staging_path_symlink_refused")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _safe_relative(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise CandidateAliasError("invalid_candidate_path")
    path = PurePosixPath(name)
    if (path.is_absolute() or ".." in path.parts or "." in path.parts
            or path.as_posix() != name or "\\" in name):
        raise CandidateAliasError("invalid_candidate_path")
    return name


def _bounded_read(path: Path, limit: int = MAX_FILE_BYTES) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            raise CandidateAliasError("unsafe_or_oversized_candidate_input")
        chunks = []
        total = 0
        while True:
            block = os.read(descriptor, min(65536, limit + 1 - total))
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > limit:
                raise CandidateAliasError("oversized_candidate_input")
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (len(body) != before.st_size or before.st_size != after.st_size
            or (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns)):
        raise CandidateAliasError("changed_or_incomplete_candidate_input")
    return body


def _read_json(path: Path, limit: int = 2_000_000) -> dict:
    try:
        value = json.loads(_bounded_read(path, limit))
    except (UnicodeError, ValueError) as exc:
        raise CandidateAliasError("invalid_alias_metadata") from exc
    if not isinstance(value, dict):
        raise CandidateAliasError("invalid_alias_metadata")
    return value


def _write_private(root: Path, name: str, body: bytes) -> None:
    _safe_relative(name)
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise CandidateAliasError("private_write_alias_or_special_file_refused")
        os.ftruncate(descriptor, 0)
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if _bounded_read(path, len(body) + 1) != body:
        raise CandidateAliasError("incomplete_private_write:" + name)


def _regular_files(root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for path in root.rglob("*"):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise CandidateAliasError("candidate_alias_or_special_file_refused")
        files[_safe_relative(str(path.relative_to(root)))] = path
    return files


def _is_ready_marker(name: str) -> bool:
    path = PurePosixPath(name)
    return path.parent == PurePosixPath("metadata") and path.name.endswith("candidate_ready.json")


def _manifest_with_alias(manifest: dict, *, source_file_slug: str, target_file_slug: str,
                         title: str, transformation: dict) -> dict:
    value = json.loads(json.dumps(manifest, ensure_ascii=False, allow_nan=False))
    value["title"] = title
    source_parquet = f"geoparquet/{source_file_slug}.parquet"
    target_parquet = f"geoparquet/{target_file_slug}.parquet"
    rewritten = 0
    for spec in value.get("prebuilt_output_policy", {}).get("formats", {}).values():
        for obj in spec.get("objects", []):
            if obj.get("path") == source_parquet:
                obj["path"] = target_parquet
                rewritten += 1
    if rewritten != 1:
        raise CandidateAliasError("source_prebuilt_output_path_unexpected")
    value.setdefault("transformations", []).append(transformation)
    validate_release_provenance(value)
    validate_output_policy(value)
    return value


def _prebuilt_parquet_sha(manifest: dict, target_file_slug: str) -> str:
    target = f"geoparquet/{target_file_slug}.parquet"
    for spec in manifest.get("prebuilt_output_policy", {}).get("formats", {}).values():
        for obj in spec.get("objects", []):
            if obj.get("path") == target:
                return obj["sha256"]
    raise CandidateAliasError("source_prebuilt_output_path_unexpected")


def _source_roster(source_root: Path, source_ready_marker: str, source_file_slug: str) -> dict[str, bytes]:
    files = _regular_files(source_root)
    if source_ready_marker not in files:
        raise CandidateAliasError("source_ready_marker_missing")
    ready = _read_json(files[source_ready_marker])
    if ready.get("approved") is not False:
        raise CandidateAliasError("source_ready_marker_not_unapproved")
    roster = ready.get("objects")
    if not isinstance(roster, dict) or not roster:
        raise CandidateAliasError("source_ready_roster_missing")
    actual = set(files) - {source_ready_marker}
    if set(roster) != actual:
        raise CandidateAliasError("source_candidate_roster_drift")
    expected: dict[str, bytes] = {}
    total = 0
    for name, digest in roster.items():
        _safe_relative(name)
        body = _bounded_read(files[name])
        total += len(body)
        if total > MAX_TOTAL_BYTES:
            raise CandidateAliasError("oversized_candidate_input")
        if not isinstance(digest, str) or hashlib.sha256(body).hexdigest() != digest:
            raise CandidateAliasError("source_object_changed:" + name)
        expected[name] = body
    if f"geopackage/{source_file_slug}.gpkg" not in expected:
        raise CandidateAliasError("source_geopackage_name_unexpected")
    if f"geoparquet/{source_file_slug}.parquet" not in expected:
        raise CandidateAliasError("source_geoparquet_name_unexpected")
    return expected


def _expected_alias(source_root: Path, *, source_ready_marker: str, source_file_slug: str,
                    target_file_slug: str, title: str, transformation: dict) -> tuple[dict[str, bytes], dict, dict]:
    manifest = _read_json(source_root / "metadata/source_manifest.json")
    rewritten = _manifest_with_alias(manifest, source_file_slug=source_file_slug,
                                     target_file_slug=target_file_slug, title=title,
                                     transformation=transformation)
    source_files = _source_roster(source_root, source_ready_marker, source_file_slug)
    renames = {
        f"geopackage/{source_file_slug}.gpkg": f"geopackage/{target_file_slug}.gpkg",
        f"geoparquet/{source_file_slug}.parquet": f"geoparquet/{target_file_slug}.parquet",
    }
    expected: dict[str, bytes] = {}
    for name, body in source_files.items():
        if _is_ready_marker(name):
            continue
        target = renames.get(name, name)
        if target in expected:
            raise CandidateAliasError("alias_rename_collision")
        expected[target] = body
    expected["metadata/source_manifest.json"] = _encode(rewritten)
    return expected, rewritten, manifest


def _ready_body(expected: dict[str, bytes], *, target_version: str, source_dataset_slug: str,
                source_file_slug: str, source_version: str) -> bytes:
    return _encode({
        "approved": False,
        "version": target_version,
        "source_dataset_slug": source_dataset_slug,
        "source_file_slug": source_file_slug,
        "source_version": source_version,
        "objects": {name: hashlib.sha256(body).hexdigest() for name, body in sorted(expected.items())},
    })


def _matches_target(target_root: Path, expected: dict[str, bytes], ready_body: bytes) -> bool:
    if not target_root.is_dir() or target_root.is_symlink():
        return False
    try:
        files = _regular_files(target_root)
    except (CandidateAliasError, OSError):
        return False
    if set(files) != set(expected) | {READY_MARKER}:
        return False
    try:
        if _bounded_read(files[READY_MARKER]) != ready_body:
            return False
        return all(_bounded_read(files[name]) == body for name, body in expected.items())
    except (CandidateAliasError, OSError):
        return False


def _quarantine(root: Path, target_dataset_slug: str, target_root: Path) -> None:
    destination = root / "_quarantine" / target_dataset_slug / ("invalid-alias-" + uuid4().hex)
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.rename(target_root, destination)


def stage_candidate_alias(storage: StagingStorageResource, *, source_dataset_slug: str,
                          source_file_slug: str, source_version: str, source_ready_marker: str,
                          target_dataset_slug: str, target_file_slug: str, target_version: str,
                          title: str, transformation: dict,
                          source_audit_marker: str | None = None) -> dict:
    """Stage a byte-faithful alias of a complete source candidate.

    The source must be an unapproved, complete local candidate with a ready
    roster. The alias renames the native GeoPackage and GeoParquet to the target
    file slug, rewrites the manifest title and prebuilt-output path, and records
    the derivation as an extra transformation. No source is fetched.
    """
    root = _local_root(storage)
    source_root = root / source_dataset_slug / source_file_slug / source_version
    if not source_root.is_dir() or source_root.is_symlink():
        raise CandidateAliasError("source_candidate_missing")
    expected, rewritten, manifest = _expected_alias(
        source_root, source_ready_marker=source_ready_marker, source_file_slug=source_file_slug,
        target_file_slug=target_file_slug, title=title, transformation=transformation)
    ready_body = _ready_body(expected, target_version=target_version,
                             source_dataset_slug=source_dataset_slug,
                             source_file_slug=source_file_slug, source_version=source_version)
    target_root = root / target_dataset_slug / target_file_slug / target_version
    reused = _matches_target(target_root, expected, ready_body)
    if not reused and target_root.exists():
        _quarantine(root, target_dataset_slug, target_root)
    if not reused:
        build = root / "_building" / target_dataset_slug
        build.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="alias-", dir=build) as tmp:
            temp = Path(tmp)
            for name, body in expected.items():
                _write_private(temp, name, body)
            _write_private(temp, READY_MARKER, ready_body)
            if not _matches_target(temp, expected, ready_body):
                raise CandidateAliasError("alias_candidate_readback_mismatch")
            target_root.parent.mkdir(parents=True, exist_ok=True)
            os.rename(temp, target_root)
    if not _matches_target(target_root, expected, ready_body):
        raise CandidateAliasError("alias_candidate_readback_mismatch")
    parquet = f"geoparquet/{target_file_slug}.parquet"
    source_parquet = source_root / "geoparquet" / f"{source_file_slug}.parquet"
    return {
        "dataset_slug": target_dataset_slug,
        "file_slug": target_file_slug,
        "version": target_version,
        "reused": reused,
        "source_dataset_slug": source_dataset_slug,
        "source_file_slug": source_file_slug,
        "source_version": source_version,
        "prebuilt_parquet_sha256": hashlib.sha256(expected[parquet]).hexdigest(),
        "source_prebuilt_parquet_sha256": hashlib.sha256(_bounded_read(source_parquet)).hexdigest(),
        "manifest_prebuilt_parquet_sha256": _prebuilt_parquet_sha(rewritten, target_file_slug),
        "source_manifest_title": manifest.get("title"),
        "alias_manifest_title": rewritten.get("title"),
        "source_audit": _read_json(source_root / _safe_relative(source_audit_marker))
        if source_audit_marker and (source_root / _safe_relative(source_audit_marker)).is_file() else None,
    }
