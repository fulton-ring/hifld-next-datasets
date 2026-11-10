"""Local evidence writes that never truncate a linked original capture."""
from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, IO


def validate_evidence_directory(directory: Path, *, protected_files: tuple[Path, ...] = ()) -> None:
    """Refuse linked paths before any evidence mutation, including stale temps.

    Existing plain evidence may be replaced on retry. Symlinked directories,
    symlinked files and multiply linked files are refused instead of followed.
    Concurrent hostile directory replacement is outside this local workspace
    trust model; individual file opens still use exclusive/no-follow creation.
    """
    absolute = directory.absolute()
    for path in [*reversed(absolute.parents), absolute]:
        if path.is_symlink():
            raise ValueError("Symlink in evidence directory path")
    if directory.exists():
        if not directory.is_dir():
            raise ValueError("Evidence output is not a directory")
        for path in directory.rglob("*"):
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1):
                raise ValueError("Symlink/hardlink in evidence output tree")
    reserved = {directory / name for name in ("baseline_comparison.json", "baseline_comparison.md", "baseline_differences.jsonl", "baseline_field_changes.jsonl")}
    if any(source.resolve() == target.resolve() for source in protected_files for target in reserved):
        raise ValueError("Evidence target would replace a protected comparison input")


def write_text_atomic(path: Path, text: str) -> None:
    """Exclusive random temporary inode followed by replace, never target open."""
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".pending", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json_atomic(path: Path, value: Any) -> None:
    write_text_atomic(path, json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def new_evidence_stream(path: Path, *, binary: bool = False) -> IO:
    """Create a new stream inode; unlinking a stale name cannot edit its target."""
    path.unlink(missing_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    return os.fdopen(descriptor, "wb" if binary else "w", **({} if binary else {"encoding": "utf-8"}))
