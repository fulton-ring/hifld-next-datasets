"""Write-once local first-construction witness, outside candidate discovery.

Caller owns the common version lock, completed private-candidate validation,
original-capture semantics and Git identity checks. This is neither approval nor
a general transaction/lock implementation. Loss of *all* outside witnesses cannot
be distinguished from first use without a separately protected inventory.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from dagster_hifld.resources import StagingStorageResource

_MAX_BODY = 2 * 1024 * 1024


class ConstructionStateError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def canonical_construction_bundle(version: str, processing_fingerprint: str, payload: dict) -> bytes:
    """Encode a bounded generic envelope; caller validates the payload semantics."""
    _identity(version, processing_fingerprint)
    if type(payload) is not dict:
        raise ConstructionStateError('construction_invalid_bundle')
    try:
        body = _encode({'schema_version': 1, 'version': version,
                        'processing_fingerprint': processing_fingerprint, 'payload': payload})
    except (TypeError, ValueError, RecursionError, UnicodeError) as exc:
        raise ConstructionStateError('construction_invalid_bundle') from exc
    _bundle(body, version, processing_fingerprint)
    return body


def _json_types(value, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError('construction JSON nesting limit')
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError('construction JSON keys must be strings')
        for item in value.values(): _json_types(item, depth + 1)
    elif type(value) is list:
        for item in value: _json_types(item, depth + 1)
    elif type(value) is float:
        if not math.isfinite(value): raise ValueError('construction JSON numbers must be finite')
    elif value is not None and type(value) not in (str, int, bool):
        raise ValueError('construction JSON type unsupported')


def _encode(value: dict) -> bytes:
    _json_types(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _identity(version: str, fingerprint: str) -> None:
    if (not isinstance(version, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,159}', version)
            or version in ('.', '..')):
        raise ConstructionStateError('construction_invalid_version')
    if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint):
        raise ConstructionStateError('construction_invalid_fingerprint')


def _key(value: str) -> str:
    if (not isinstance(value, str) or len(value) > 512 or
            not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*(?:/[A-Za-z0-9_][A-Za-z0-9_.-]*){0,7}', value)
            or any(part in ('.', '..') for part in value.split('/'))):
        raise ConstructionStateError('construction_unsafe_record_key')
    return value


def _context(storage: StagingStorageResource, key: str, version: str, fingerprint: str) -> tuple[Path, str]:
    _identity(version, fingerprint)
    _key(key)
    if not key.startswith('_construction-state/') or not key.endswith('.json'):
        raise ConstructionStateError('construction_unsafe_record_key')
    if not (storage.use_local or not storage.bucket):
        raise ConstructionStateError('construction_remote_protocol_unsupported')
    prefix = _key(storage.prefix) + '/' if storage.prefix else ''
    try:
        root = Path(storage.local_dir).resolve(strict=True)
        if not root.is_dir():
            raise OSError()
    except (OSError, ValueError, RuntimeError) as exc:
        raise ConstructionStateError('construction_invalid_storage_root') from exc
    return root, prefix + key


def _json(body: bytes) -> dict:
    if type(body) is not bytes or not 0 < len(body) <= _MAX_BODY:
        raise ConstructionStateError('construction_invalid_record_body')
    try:
        value = json.loads(body)
        if type(value) is not dict or _encode(value) != body:
            raise ValueError()
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ConstructionStateError('construction_noncanonical_record_body') from exc


def _bundle(body: bytes, version: str, fingerprint: str) -> None:
    value = _json(body)
    if (set(value) != {'schema_version', 'version', 'processing_fingerprint', 'payload'} or
            type(value['schema_version']) is not int or value['schema_version'] != 1 or
            value['version'] != version or value['processing_fingerprint'] != fingerprint or
            type(value['payload']) is not dict):
        raise ConstructionStateError('construction_bundle_identity_mismatch')


@contextmanager
def _directory(root: Path, parts: list[str], create: bool = False):
    descriptors = []
    try:
        descriptors.append(os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        for part in parts:
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptors[-1])
                    os.fsync(descriptors[-1])
                except FileExistsError:
                    pass
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        yield descriptors[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _read(root: Path, key: str) -> bytes | None:
    try:
        with _directory(root, key.split('/')[:-1]) as directory:
            descriptor = os.open(key.split('/')[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                 dir_fd=directory)
            try:
                before = os.fstat(descriptor)
                if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > _MAX_BODY:
                    raise ConstructionStateError('construction_unsafe_record_object')
                pieces = []; size = 0
                while block := os.read(descriptor, min(65536, _MAX_BODY + 1 - size)):
                    pieces.append(block); size += len(block)
                    if size > _MAX_BODY:
                        raise ConstructionStateError('construction_invalid_record_body')
                after = os.fstat(descriptor)
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_nlink) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink):
                    raise ConstructionStateError('construction_record_changed_during_read')
                return b''.join(pieces)
            finally:
                os.close(descriptor)
    except FileNotFoundError:
        return None


def _evidence_exists(root: Path, key: str) -> bool:
    try:
        with _directory(root, (key + '.evidence').split('/')) as directory:
            return bool(os.listdir(directory))
    except FileNotFoundError:
        return False


def _install_once(root: Path, key: str, body: bytes) -> None:
    """Atomic no-replace hard-link commit; preserve failed temporary evidence."""
    with _directory(root, key.split('/')[:-1], create=True) as directory:
        temporary = '.pending-' + uuid4().hex
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(body); stream.flush(); os.fsync(stream.fileno())
            try:
                os.link(temporary, key.split('/')[-1], src_dir_fd=directory,
                        dst_dir_fd=directory, follow_symlinks=False)
                os.unlink(temporary, dir_fd=directory)
                os.fsync(directory)
            except FileExistsError:
                os.unlink(temporary, dir_fd=directory)
                if _read(root, key) != body:
                    raise ConstructionStateError('construction_concurrent_winner_differs')
        except BaseException:
            # Includes interruptions: no rollback of complete/partial evidence.
            raise


def _load(root: Path, key: str, version: str, fingerprint: str) -> bytes:
    alias = _read(root, key); marker_body = _read(root, key + '.first-construction.json')
    if alias is None or marker_body is None:
        raise ConstructionStateError('construction_missing_original_state')
    _bundle(alias, version, fingerprint)
    digest = hashlib.sha256(alias).hexdigest()
    evidence_key = key + '.evidence/' + digest + '.json'
    expected = {'schema_version': 1, 'version': version, 'processing_fingerprint': fingerprint,
                'original_bundle_sha256': digest, 'original_bundle_evidence_key': evidence_key}
    _json(marker_body)
    if marker_body != _encode(expected):
        raise ConstructionStateError('construction_marker_mismatch')
    if _read(root, evidence_key) != alias:
        raise ConstructionStateError('construction_original_evidence_mismatch')
    return alias


def load_first_construction(storage: StagingStorageResource, record_key: str,
                            version: str, processing_fingerprint: str) -> bytes:
    """Load original bytes; never bootstrap or repair incomplete outside state."""
    root, key = _context(storage, record_key, version, processing_fingerprint)
    try:
        return _load(root, key, version, processing_fingerprint)
    except OSError as exc:
        raise ConstructionStateError('construction_state_io_failed') from exc


def seal_first_construction(storage: StagingStorageResource, record_key: str,
                            version: str, processing_fingerprint: str, bundle_body: bytes) -> bytes:
    """Seal only a genuinely empty state after private validation, under caller lock.

    Existing valid state returns its original bytes, NEVER newer observations.
    Any incomplete state, including CAS-only or pending evidence, blocks retry.
    Explicit operator restoration of exact trusted bytes is outside this API.
    """
    root, key = _context(storage, record_key, version, processing_fingerprint)
    _bundle(bundle_body, version, processing_fingerprint)
    try:
        if (_read(root, key) is not None or _read(root, key + '.first-construction.json') is not None
                or _evidence_exists(root, key)):
            return _load(root, key, version, processing_fingerprint)
        digest = hashlib.sha256(bundle_body).hexdigest()
        evidence_key = key + '.evidence/' + digest + '.json'
        _install_once(root, evidence_key, bundle_body)
        marker = _encode({'schema_version': 1, 'version': version,
                          'processing_fingerprint': processing_fingerprint,
                          'original_bundle_sha256': digest, 'original_bundle_evidence_key': evidence_key})
        _install_once(root, key + '.first-construction.json', marker)
        _install_once(root, key, bundle_body)
        return _load(root, key, version, processing_fingerprint)
    except OSError as exc:
        raise ConstructionStateError('construction_state_io_failed') from exc
