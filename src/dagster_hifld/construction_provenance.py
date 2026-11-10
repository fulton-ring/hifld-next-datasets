"""Read-only Git content proof for an original processing-code claim.

Callers must separately bind the original manifest/identity to preserved capture
evidence. A reachable Git commit is not proof of execution time, source authority,
signing identity, dataset acceptance, or permission to release any artifact.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

_MAX_FILES = 4096
_MAX_PATH_BYTES = 1024
_MAX_TOTAL_PATH_BYTES = 128 * 1024
_MAX_BLOB_BYTES = 16 * 1024 * 1024
_MAX_TOTAL_BLOB_BYTES = 256 * 1024 * 1024
_TIMEOUT = 30
_PACKAGED_SCHEME = "sha256-packaged-dagster-hifld-v1"


class ConstructionCodeIdentityError(ValueError):
    """Stable public reason; Git stderr and environment values are not exposed."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _git(root: Path, args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["git", "--no-replace-objects", "--literal-pathspecs", "-C", str(root), *args],
                              env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              timeout=_TIMEOUT, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ConstructionCodeIdentityError("construction_git_read_failed") from exc


def validate_construction_code_identity(repo_root: Path, code_commit: str,
                                        processing_files: Mapping[str, str], *,
                                        identity: Mapping[str, object] | None = None) -> None:
    """Require canonical reachable commit + SHA256 of EVERY regular Git blob.

    Reachable means ancestor of HEAD or a local branch/remote-tracking/tag ref;
    reflog-only/dangling objects do not qualify. Historical commits remain valid
    after docs-only HEAD advances. Current working files are never opened or
    compared, filters/replacements are not applied, and no fetch is requested.
    Linked worktrees are supported through their actual shared Git object store.
    """
    if not isinstance(repo_root, (str, Path)):
        raise ConstructionCodeIdentityError("construction_invalid_repository")
    try:
        root = Path(repo_root).resolve(strict=True)
    except (OSError, ValueError, RuntimeError) as exc:
        raise ConstructionCodeIdentityError("construction_invalid_repository") from exc
    try:
        if not root.is_dir():
            raise ConstructionCodeIdentityError("construction_invalid_repository")
    except OSError as exc:
        raise ConstructionCodeIdentityError("construction_invalid_repository") from exc
    if identity is not None and identity.get("code_identity_scheme") == _PACKAGED_SCHEME:
        _validate_packaged_identity(root, code_commit, processing_files, identity)
        return
    if (identity is not None and identity.get("code_identity_scheme") != "git-commit-blobs-v1"):
        raise ConstructionCodeIdentityError("construction_unknown_identity_scheme")
    if (not isinstance(code_commit, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", code_commit)
            or set(code_commit) == {"0"}):
        raise ConstructionCodeIdentityError("construction_invalid_commit")
    if not isinstance(processing_files, Mapping) or not 0 < len(processing_files) <= _MAX_FILES:
        raise ConstructionCodeIdentityError("construction_invalid_processing_files")
    paths = []
    for path, digest in processing_files.items():
        if (not isinstance(path, str) or not path or any(ord(char) < 32 or char == "\\" for char in path)
                or PurePosixPath(path).is_absolute() or re.match(r"^[A-Za-z]:", path) or PurePosixPath(path).as_posix() != path
                or any(part in {".", "..", ".git"} for part in PurePosixPath(path).parts)):
            raise ConstructionCodeIdentityError("construction_unsafe_processing_path")
        try:
            size = len(path.encode("utf-8"))
        except UnicodeError as exc:
            raise ConstructionCodeIdentityError("construction_unsafe_processing_path") from exc
        if size > _MAX_PATH_BYTES:
            raise ConstructionCodeIdentityError("construction_processing_path_limit")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ConstructionCodeIdentityError("construction_invalid_processing_digest")
        paths.append(path)
    if sum(len(path.encode("utf-8")) + 1 for path in paths) > _MAX_TOTAL_PATH_BYTES:
        raise ConstructionCodeIdentityError("construction_processing_path_limit")
    # Ignore only inherited repository-selection controls for this read-only
    # subprocess. Runtime proxies/authentication remain untouched; nothing is
    # printed or persisted. Do not use global/system user configuration.
    env = os.environ.copy()
    for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_NAMESPACE",
                "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_SHALLOW_FILE"):
        env.pop(key, None)
    env.update(GIT_NO_REPLACE_OBJECTS="1", GIT_NO_LAZY_FETCH="1", GIT_OPTIONAL_LOCKS="0",
               GIT_TERMINAL_PROMPT="0", GIT_PAGER="cat", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_CONFIG_SYSTEM=os.devnull, GIT_GRAFT_FILE=os.devnull)
    top = _git(root, ["rev-parse", "--show-toplevel"], env)
    if top.returncode or Path(os.fsdecode(top.stdout).strip()).resolve() != root:
        raise ConstructionCodeIdentityError("construction_invalid_repository")
    object_format = _git(root, ["rev-parse", "--show-object-format"], env)
    expected_length = {b"sha1\n": 40, b"sha256\n": 64}.get(object_format.stdout)
    if object_format.returncode or expected_length is None:
        raise ConstructionCodeIdentityError("construction_git_read_failed")
    if len(code_commit) != expected_length:
        raise ConstructionCodeIdentityError("construction_invalid_commit")
    kind = _git(root, ["cat-file", "-t", code_commit], env)
    if kind.returncode or kind.stdout != b"commit\n":
        raise ConstructionCodeIdentityError("construction_commit_not_found")
    ancestor = _git(root, ["merge-base", "--is-ancestor", code_commit, "HEAD"], env)
    if ancestor.returncode not in (0, 1):
        raise ConstructionCodeIdentityError("construction_git_read_failed")
    if ancestor.returncode:
        refs = _git(root, ["for-each-ref", "--count=1", "--format=%(refname)", f"--contains={code_commit}",
                           "refs/heads", "refs/remotes", "refs/tags"], env)
        if refs.returncode:
            raise ConstructionCodeIdentityError("construction_git_read_failed")
        if not refs.stdout.strip():
            raise ConstructionCodeIdentityError("construction_commit_not_reachable")
    tree = _git(root, ["ls-tree", "-z", code_commit, "--", *sorted(paths)], env)
    if tree.returncode:
        raise ConstructionCodeIdentityError("construction_git_read_failed")
    objects = {}
    try:
        for entry in tree.stdout.split(b"\0"):
            if not entry:
                continue
            header, path = entry.split(b"\t", 1)
            mode, object_type, oid = header.split(b" ")
            path = path.decode("utf-8")
            if path not in processing_files or path in objects:
                raise ValueError("unexpected tree entry")
            if mode not in (b"100644", b"100755") or object_type != b"blob":
                raise ConstructionCodeIdentityError("construction_nonregular_processing_blob")
            objects[path] = oid.decode("ascii")
    except (UnicodeError, ValueError) as exc:
        if isinstance(exc, ConstructionCodeIdentityError):
            raise
        raise ConstructionCodeIdentityError("construction_invalid_git_tree") from exc
    if set(objects) != set(processing_files):
        raise ConstructionCodeIdentityError("construction_processing_blob_missing")
    total = 0
    for path, oid in sorted(objects.items()):
        size_result = _git(root, ["cat-file", "-s", oid], env)
        try:
            size = int(size_result.stdout) if size_result.returncode == 0 else -1
        except ValueError:
            size = -1
        total += size
        if size < 0:
            raise ConstructionCodeIdentityError("construction_git_read_failed")
        if size > _MAX_BLOB_BYTES or total > _MAX_TOTAL_BLOB_BYTES:
            raise ConstructionCodeIdentityError("construction_processing_blob_size_limit")
        blob = _git(root, ["cat-file", "blob", oid], env)
        if blob.returncode or len(blob.stdout) != size:
            raise ConstructionCodeIdentityError("construction_git_read_failed")
        if hashlib.sha256(blob.stdout).hexdigest() != processing_files[path]:
            raise ConstructionCodeIdentityError("construction_processing_blob_hash_mismatch")


def _validate_packaged_identity(root: Path, code_commit: str,
                                processing_files: Mapping[str, str],
                                identity: Mapping[str, object]) -> None:
    """Recompute an explicit installed-package identity when no Git repo ships."""
    git_metadata = root / ".git"
    if git_metadata.exists() or git_metadata.is_symlink():
        raise ConstructionCodeIdentityError("construction_packaged_identity_has_git_metadata")
    if (not isinstance(code_commit, str)
            or not re.fullmatch(r"packaged-sha256:[0-9a-f]{64}", code_commit)
            or identity.get("working_tree_status") != "unknown"):
        raise ConstructionCodeIdentityError("construction_invalid_packaged_identity")
    if not isinstance(processing_files, Mapping) or not 0 < len(processing_files) <= _MAX_FILES:
        raise ConstructionCodeIdentityError("construction_invalid_processing_files")
    if identity.get("files") != processing_files:
        raise ConstructionCodeIdentityError("construction_packaged_file_inventory_mismatch")
    package = root / "src/dagster_hifld"
    if package.is_dir():
        package_prefix = "src/dagster_hifld"
    else:
        package = root / "dagster_hifld"
        package_prefix = "dagster_hifld"
    if not package.is_dir() or package.is_symlink():
        raise ConstructionCodeIdentityError("construction_packaged_source_missing")
    expected_paths = {"pyproject.toml", "uv.lock"}
    for path in package.rglob("*"):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink():
            raise ConstructionCodeIdentityError("construction_packaged_source_symlink")
        if not path.is_file():
            continue
        expected_paths.add(package_prefix + "/" + path.relative_to(package).as_posix())
    if len(expected_paths) <= 2:
        raise ConstructionCodeIdentityError("construction_packaged_source_missing")
    if set(processing_files) != expected_paths:
        raise ConstructionCodeIdentityError("construction_packaged_file_inventory_mismatch")
    allowed_roots = ("src/dagster_hifld/", "dagster_hifld/")
    if not {"pyproject.toml", "uv.lock"} <= set(processing_files):
        raise ConstructionCodeIdentityError("construction_packaged_dependency_inputs_missing")
    total_path_bytes = 0
    total_blob_bytes = 0
    for path, digest in processing_files.items():
        if (not isinstance(path, str) or not path or any(ord(char) < 32 or char == "\\" for char in path)
                or PurePosixPath(path).is_absolute() or PurePosixPath(path).as_posix() != path
                or any(part in {".", "..", ".git", "__pycache__"} for part in PurePosixPath(path).parts)
                or (path not in {"pyproject.toml", "uv.lock"} and not path.startswith(allowed_roots))):
            raise ConstructionCodeIdentityError("construction_unsafe_processing_path")
        try:
            total_path_bytes += len(path.encode("utf-8")) + 1
        except UnicodeError as exc:
            raise ConstructionCodeIdentityError("construction_unsafe_processing_path") from exc
        if (len(path.encode("utf-8")) > _MAX_PATH_BYTES
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise ConstructionCodeIdentityError("construction_invalid_processing_digest")
        if total_path_bytes > _MAX_TOTAL_PATH_BYTES:
            raise ConstructionCodeIdentityError("construction_processing_path_limit")
        file_path = root / path
        try:
            before = file_path.lstat()
        except OSError as exc:
            raise ConstructionCodeIdentityError("construction_packaged_file_missing") from exc
        if not stat.S_ISREG(before.st_mode):
            raise ConstructionCodeIdentityError("construction_nonregular_processing_blob")
        if before.st_size > _MAX_BLOB_BYTES or total_blob_bytes + before.st_size > _MAX_TOTAL_BLOB_BYTES:
            raise ConstructionCodeIdentityError("construction_processing_blob_size_limit")
        total_blob_bytes += before.st_size
        try:
            with file_path.open("rb") as stream:
                opened = os.fstat(stream.fileno())
                if (not stat.S_ISREG(opened.st_mode) or opened.st_dev != before.st_dev
                        or opened.st_ino != before.st_ino or opened.st_size != before.st_size):
                    raise ConstructionCodeIdentityError("construction_packaged_file_changed")
                content_hash = hashlib.sha256()
                size = 0
                while chunk := stream.read(1024 * 1024):
                    size += len(chunk)
                    if size > _MAX_BLOB_BYTES or total_blob_bytes - before.st_size + size > _MAX_TOTAL_BLOB_BYTES:
                        raise ConstructionCodeIdentityError("construction_processing_blob_size_limit")
                    content_hash.update(chunk)
                after = os.fstat(stream.fileno())
        except OSError as exc:
            raise ConstructionCodeIdentityError("construction_packaged_file_missing") from exc
        if (size != before.st_size or after.st_dev != before.st_dev or after.st_ino != before.st_ino
                or after.st_size != before.st_size or after.st_mtime_ns != before.st_mtime_ns):
            raise ConstructionCodeIdentityError("construction_packaged_file_changed")
        if content_hash.hexdigest() != digest:
            raise ConstructionCodeIdentityError("construction_packaged_file_hash_mismatch")
    distributions = identity.get("installed_distributions")
    if not isinstance(distributions, list):
        raise ConstructionCodeIdentityError("construction_packaged_distributions_missing")
    actual_distributions = []
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        version = distribution.version
        if name and version:
            actual_distributions.append({"name": name, "version": version})
    actual_distributions.sort(key=lambda item: (item["name"].casefold(), item["name"], item["version"]))
    if distributions != actual_distributions:
        raise ConstructionCodeIdentityError("construction_packaged_distributions_changed")
    runtime_versions = _packaged_runtime_versions()
    if any(identity.get(key) != value for key, value in runtime_versions.items()):
        raise ConstructionCodeIdentityError("construction_packaged_runtime_versions_changed")
    required = ("code_identity_scheme", "working_tree_status", "files", "installed_distributions",
                "python", "pyarrow", "proj", "geos")
    if any(key not in identity for key in required):
        raise ConstructionCodeIdentityError("construction_packaged_identity_incomplete")
    try:
        body = json.dumps({key: identity[key] for key in required}, sort_keys=True,
                          separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise ConstructionCodeIdentityError("construction_packaged_identity_invalid") from exc
    digest = hashlib.sha256(body).hexdigest()
    if code_commit != f"packaged-sha256:{digest}":
        raise ConstructionCodeIdentityError("construction_packaged_identity_hash_mismatch")


def _packaged_runtime_versions() -> dict[str, str]:
    """Read runtime versions lazily so Git-only checks need no geo imports."""
    try:
        pyarrow = importlib.import_module("pyarrow")
        pyproj = importlib.import_module("pyproj")
        shapely = importlib.import_module("shapely")
        import platform
        return {
            "python": platform.python_version(), "pyarrow": str(pyarrow.__version__),
            "proj": str(pyproj.proj_version_str), "geos": str(shapely.geos_version_string),
        }
    except (ImportError, AttributeError) as exc:
        raise ConstructionCodeIdentityError("construction_packaged_runtime_versions_unavailable") from exc
