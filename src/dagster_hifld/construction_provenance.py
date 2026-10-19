"""Read-only Git content proof for an original processing-code claim.

Callers must separately bind the original manifest/identity to preserved capture
evidence. A reachable Git commit is not proof of execution time, source authority,
signing identity, dataset acceptance, or permission to release any artifact.
"""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

_MAX_FILES = 4096
_MAX_PATH_BYTES = 1024
_MAX_TOTAL_PATH_BYTES = 128 * 1024
_MAX_BLOB_BYTES = 16 * 1024 * 1024
_MAX_TOTAL_BLOB_BYTES = 256 * 1024 * 1024
_TIMEOUT = 30


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
                                        processing_files: Mapping[str, str]) -> None:
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
