"""Private candidate installation under the adapter's native lock and trust protocol.

The caller owns temporary-directory lifetime, schemas and custody metadata. The
shared transaction writes and validates private artifacts, anchors outside trust,
then revalidates before the single directory rename that exposes a candidate.
"""
from __future__ import annotations

import os
import stat
import fcntl
from contextlib import contextmanager
from collections.abc import Callable, Iterable
from pathlib import Path

from dagster_hifld.construction_state import load_first_construction, seal_first_construction


def _acquire_lock(handle, *, error_type: type[ValueError], unsafe_code: str,
                  require_regular: bool) -> None:
    descriptor = handle if isinstance(handle, int) else handle.fileno()
    info = os.fstat(descriptor)
    if info.st_nlink != 1 or (require_regular and not stat.S_ISREG(info.st_mode)):
        raise error_type(unsafe_code)
    fcntl.flock(handle, fcntl.LOCK_EX)


@contextmanager
def lock_path(path: Path, *, error_type: type[ValueError], unsafe_code: str,
              require_regular: bool = True, buffered: bool = False):
    """Hold the native lock handle and inode until custody completes; never unlink it."""
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    if buffered:
        # These native transactions require fdopen's seekability check before
        # inode validation or flock; descriptor-only locks have a different contract.
        with os.fdopen(descriptor, 'r+b') as held:
            _acquire_lock(held, error_type=error_type, unsafe_code=unsafe_code,
                          require_regular=require_regular)
            yield
    else:
        try:
            _acquire_lock(descriptor, error_type=error_type, unsafe_code=unsafe_code,
                          require_regular=require_regular)
            yield
        finally:
            os.close(descriptor)


def write_private_bytes(root: Path, name: str, body: bytes) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def write_exclusive_private_file(root: Path, name: str, body: bytes, *,
                                 error_type: type[ValueError], short_write_code: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        written = stream.write(body)
        if written != len(body):
            raise error_type(short_write_code)
        stream.flush()
        os.fsync(stream.fileno())


def write_checked_private_file(root: Path, name: str, body: bytes, *,
                               readback: Callable[[Path, str], bytes],
                               error_type: type[ValueError],
                               unsafe_code: str = 'private_write_alias_or_special_file_refused',
                               short_write_prefix: str = 'incomplete_private_write:') -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise error_type(unsafe_code)
        os.ftruncate(descriptor, 0)
        if os.write(descriptor, body) != len(body):
            raise error_type(short_write_prefix + name)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if readback(root, name) != body:
        raise error_type(short_write_prefix + name)


def anchor_first_construction(storage, key: str, version: str, fingerprint: str,
                              envelope: bytes, *, original: bool,
                              error_type: type[ValueError], changed_code: str,
                              require_same_result: bool = False) -> None:
    if original:
        anchored = load_first_construction(storage, key, version, fingerprint)
        if anchored != envelope:
            raise error_type(changed_code)
    else:
        sealed = seal_first_construction(storage, key, version, fingerprint, envelope)
        if require_same_result and sealed != envelope:
            raise error_type(changed_code)


def install_private_candidate(
    private: Path, target: Path, objects: Iterable[tuple[str, bytes]], *,
    write_one: Callable[[Path, str, bytes], None],
    ready: Callable[[Path], tuple[str, bytes]] | None = None,
    validate_before: Callable[[Path], None] | None = None,
    anchor: Callable[[], None] | None = None,
    validate_after: Callable[[Path], None] | None = None,
) -> None:
    """Install completed private custody; never expose a failed write/validation/seal."""
    for name, body in objects:
        write_one(private, name, body)
    if ready is not None:
        name, body = ready(private)
        write_one(private, name, body)
    if validate_before is not None:
        validate_before(private)
    if anchor is not None:
        anchor()
    if validate_after is not None:
        validate_after(private)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.rename(private, target)
