"""Capture an HTTPX *wire* stream before decoding or rejecting its contents.

The caller owns safe sinks, disk reservation, response acceptance, retries and
receipts. Sinks must retain the whole supplied block or raise; their persisted
bytes still need caller readback checks. No URL, source or release is approved
here. Callers can reject an oversized delivered wire block before persistence
with ``max_wire_block_bytes``; uncapped callers retain the prior one-block
overshoot behavior so evidence is never silently clipped.
"""
from __future__ import annotations

import hashlib
import copy
import json
import stat
import zlib
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

import httpx


DECODE_BLOCK_BYTES = 1024 * 1024


@dataclass
class _RetentionBudget:
    root: Path
    limit: int
    exceeded: bool = False
    parent: _RetentionBudget | None = None

    def response_charge(self):
        # Capture helpers unlink the current response slots before calling the
        # HTTP owner. Rescan here so resumed attempts, renames and overwritten
        # slots are charged by their physical retained size exactly once.
        retained = sum(path.stat().st_size for path in self.root.iterdir()
                       if path.is_file() and path.name.endswith((".body", ".wire")))
        applicable = []
        ancestor = self
        while ancestor is not None:
            if ancestor.root == self.root:
                applicable.append(ancestor)
            ancestor = ancestor.parent
        limit = min(budget.limit for budget in applicable)

        def refuse() -> None:
            for budget in applicable:
                budget.exceeded = True
            raise ValueError("source_capture_budget_exceeded")

        if any(budget.exceeded for budget in applicable) or retained > limit:
            refuse()

        def charge(block: bytes) -> None:
            nonlocal retained
            if any(budget.exceeded for budget in applicable) or retained + len(block) > limit:
                refuse()
            retained += len(block)
        return charge


_retention_budget: ContextVar[_RetentionBudget | None] = ContextVar("capture_retention_budget", default=None)


@contextmanager
def capture_retention_budget(root: Path, limit: int):
    """Bound one synchronous acquisition, including opaque family fetch callbacks.

    Actual wire and decoded sink blocks are charged before persistence. Native
    reservation callbacks retain their allocation/disk checks independently;
    speculative decoder allocation is not charged as retained disk evidence.
    A refused block stays fatal even if a callback retries or swallows its
    incomplete result. Nested scopes for the same physical directory also honor
    every enclosing limit and latch refusals in each applicable scope. Native
    capture helpers get to write truthful receipts
    before the acquisition boundary raises the budget refusal.
    """
    budget = _RetentionBudget(Path(root).resolve(), limit, parent=_retention_budget.get())
    token = _retention_budget.set(budget)
    try:
        yield
    except Exception as exc:
        if budget.exceeded:
            raise ValueError("source_capture_budget_exceeded") from exc
        raise
    else:
        if budget.exceeded:
            raise ValueError("source_capture_budget_exceeded")
    finally:
        _retention_budget.reset(token)


def validate_capture_roster(root: Path, expected: set[str] | dict[str, dict[str, object]]) -> None:
    """Require an exact flat roster of regular, non-symlink capture files.

    ``expected`` is the complete set of direct-child file names derived from a
    sealed capture manifest. This rejects unbound additions as well as missing
    files and unsafe file types before any replay bytes are consumed.
    """
    if not isinstance(root, Path) or not root.is_dir() or root.is_symlink():
        raise ValueError("capture_roster_root_invalid")
    expected_names = set(expected) if isinstance(expected, dict) else expected
    if not isinstance(expected_names, set) or any(
        not isinstance(name, str) or not name or Path(name).name != name or name in {".", ".."}
        for name in expected_names
    ):
        raise ValueError("capture_roster_manifest_invalid")
    actual = set()
    for entry in root.iterdir():
        actual.add(entry.name)
        if entry.is_symlink() or not stat.S_ISREG(entry.lstat().st_mode):
            raise ValueError("capture_roster_nonregular_path:" + entry.name)
    if actual != expected_names:
        raise ValueError("capture_roster_mismatch")
    if isinstance(expected, dict):
        for name, descriptor in expected.items():
            if not isinstance(descriptor, dict) or not isinstance(descriptor.get("bytes"), int) or not isinstance(descriptor.get("sha256"), str):
                raise ValueError("capture_roster_manifest_invalid")
            body = (root / name).read_bytes()
            if len(body) != descriptor["bytes"] or hashlib.sha256(body).hexdigest() != descriptor["sha256"]:
                raise ValueError("capture_roster_artifact_mismatch:" + name)


def capture_roster_digests(root: Path, *, exclude: set[str] = frozenset()) -> dict[str, dict[str, object]]:
    """Return byte count and SHA-256 for every regular direct-child file."""
    if not isinstance(root, Path) or not root.is_dir() or root.is_symlink():
        raise ValueError("capture_roster_root_invalid")
    result = {}
    for entry in sorted(root.iterdir(), key=lambda path: path.name):
        if entry.name in exclude:
            continue
        if entry.is_symlink() or not stat.S_ISREG(entry.lstat().st_mode):
            raise ValueError("capture_roster_nonregular_path:" + entry.name)
        body = entry.read_bytes()
        result[entry.name] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    return result


class CaptureDocuments(Mapping):
    """Read capture documents one at a time with eager decoder validity checks.

    Tuple captures default to JSON decoding. Explicit projection and decoder
    callbacks preserve byte-only or stricter source document contracts.
    """
    def __init__(self, captures: Mapping, names=None, *, decode=json.loads,
                 body_from_capture=lambda value: value[0], load_document=None):
        self._read = load_document or (lambda name: decode(body_from_capture(captures[name])))
        self._names = dict.fromkeys(captures if names is None else names)
        for name in self:
            self[name]

    def __getitem__(self, name):
        if name not in self._names:
            raise KeyError(name)
        return self._read(name)

    def __iter__(self):
        return iter(self._names)

    def __len__(self):
        return len(self._names)


class RetainedReplayView(Mapping):
    """Validated capture identities with bodies loaded and rechecked on demand.

    The loader owns the source's body/wire digest and safe-path checks. Receipts
    are pinned independently so changing both artifacts and their receipt after
    validation cannot introduce new bytes into an existing replay.
    """
    def __init__(self, root, *, receipts, expected_urls, load_capture,
                 validate_request_identity,
                 receipt_error="retained_capture_manifest_receipt_mismatch"):
        self._root = root
        self._receipts = copy.deepcopy(receipts)
        self._urls = dict(expected_urls)
        self._load = load_capture
        self._validate_identity = validate_request_identity
        self._receipt_error = receipt_error

    def __getitem__(self, name):
        expected = self._receipts[name]
        body, _wire, receipt = self._load(self._root, name)
        if receipt != expected:
            raise ValueError(self._receipt_error + ":" + name)
        self._validate_identity(name, receipt, self._urls[name])
        return body, receipt

    def __iter__(self):
        return iter(self._receipts)

    def __len__(self):
        return len(self._receipts)


def validated_capture_view(root: Path, *, receipts: Mapping[str, dict],
                           expected_urls: Mapping[str, str],
                           load_capture: Callable[[Path, str], tuple[bytes, bytes, dict]],
                           validate_request_identity: Callable[[str, Mapping[str, object], str], None],
                           receipt_error: str = "retained_capture_manifest_receipt_mismatch",
                           ) -> Mapping[str, tuple[bytes, dict]]:
    """Check every sealed receipt before exposing a disk-backed replay view.

    Callers must first validate their manifest, exact file roster and artifact
    digests. Bodies are discarded after each check and never cached by the view.
    """
    view = RetainedReplayView(root, receipts=receipts, expected_urls=expected_urls,
        load_capture=load_capture, validate_request_identity=validate_request_identity,
        receipt_error=receipt_error)
    for name in sorted(view):
        view[name]
    return view


def validate_retained_capture_replay(
    root: Path,
    *,
    capture_names: set[str],
    expected_urls: Mapping[str, str],
    load_capture: Callable[[Path, str], tuple[bytes, bytes, dict]],
    validate_request_identity: Callable[[str, Mapping[str, object], str], None],
) -> Mapping[str, tuple[bytes, dict]]:
    """Replay a retained capture directory against its sealed manifest.

    Reads ``capture_manifest.json`` (refusing a symlinked manifest), requires a
    complete manifest whose receipt names match ``capture_names`` exactly,
    validates the artifact roster and per-artifact digests with
    ``validate_capture_roster`` and ``capture_roster_digests``, then loads every
    capture with the caller's established loader and binds each stored receipt
    to the manifest receipt plus the caller's expected request URL. Artifact
    names and URLs are passed explicitly, so the validator never inherits one
    dataset's pins from another. Returns a disk-backed ``Mapping`` of
    ``name: (body, receipt)``; each lookup revalidates the retained capture.
    """
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != capture_names:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    return validated_capture_view(root, receipts=manifest["receipts"],
        expected_urls=expected_urls, load_capture=load_capture,
        validate_request_identity=validate_request_identity)


@dataclass(frozen=True)
class CaptureProgress:
    """Reservation request made BEFORE the next read or decoder allocation."""

    phase: str
    wire_bytes_received: int
    wire_bytes_retained: int
    decoded_bytes_produced: int
    decoded_bytes_retained: int
    reserve_bytes: int


@dataclass(frozen=True)
class HTTPCaptureResult:
    """Truthful transfer evidence, including failures; not source acceptance.

    Retained counts/digests acknowledge successful sink calls, not independent
    persistence proof. A failing sink's current block is returned as pending;
    its physical partial write, if any, remains caller-owned evidence. A result
    with ``complete=False`` must never be used as a successful source receipt.
    HTTP error statuses can have complete bodies: status acceptance is caller
    policy. Byte hashes are SHA-256, including an actually empty complete body.
    """

    complete: bool
    status_code: int
    content_encoding: str
    content_length_header: str | None
    content_length: int | None
    wire_bytes_received: int
    wire_bytes_retained: int
    wire_sha256: str
    received_wire_sha256: str
    decoded_bytes_produced: int
    decoded_bytes_retained: int
    decoded_sha256: str
    produced_decoded_sha256: str
    httpx_bytes_downloaded: int
    stream_exhausted: bool
    decoder_eof: bool | None
    error_code: str | None
    error_type: str | None
    pending_wire: bytes = field(default=b"", repr=False)
    pending_decoded: bytes = field(default=b"", repr=False)


def capture_http_response(
    response: httpx.Response,
    *,
    max_wire_bytes: int,
    max_decoded_bytes: int,
    max_wire_block_bytes: int | None = None,
    reserve: Callable[[CaptureProgress], None],
    retain_wire: Callable[[bytes], None],
    retain_decoded: Callable[[bytes], None],
    apply_retention_budget: bool = True,
) -> HTTPCaptureResult:
    """Consume a fresh synchronous streamed response using unbuffered iter_raw.

    Supports identity, gzip and RFC zlib-wrapped deflate. Raw deflate, stacked
    encodings, concatenated gzip members and trailing bytes are rejected. Gzip
    CRC/trailer and decoder EOF are checked even when HTTPX considers transport
    complete. Content-Length is checked against wire bytes, never decoded size.
    ``reserve`` must raise on insufficient capacity; it runs before EVERY next
    (including the possible EOF read), again with actual wire-block size before
    persistence, and before each bounded decompression.
    Ordinary transport, reserve, decoder and sink exceptions become incomplete
    results; process interruption exceptions propagate with prior sink evidence
    intact. No exception messages/URLs/credentials are copied into results.
    """
    for name, value in (("max_wire_bytes", max_wire_bytes), ("max_decoded_bytes", max_decoded_bytes)):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name}_must_be_nonnegative_integer")
    if max_wire_block_bytes is not None and (type(max_wire_block_bytes) is not int or max_wire_block_bytes < 0):
        raise ValueError("max_wire_block_bytes_must_be_nonnegative_integer")
    if not isinstance(response, httpx.Response):
        raise TypeError("response_must_be_httpx_response")
    if not all(callable(callback) for callback in (reserve, retain_wire, retain_decoded)):
        raise TypeError("capture_callbacks_must_be_callable")

    encoding = response.headers.get("content-encoding", "identity").strip().lower() or "identity"
    length_header = response.headers.get("content-length")
    length = None
    received = retained = produced = decoded_retained = 0
    received_hash = hashlib.sha256()
    retained_hash = hashlib.sha256()
    produced_hash = hashlib.sha256()
    decoded_hash = hashlib.sha256()
    exhausted = False
    decoder = None
    pending_wire = pending_decoded = b""
    failure = error_type = None
    phase = "response_validation"
    budget = _retention_budget.get() if apply_retention_budget else None
    charge = None

    def reservation(kind: str, allocation: int) -> None:
        nonlocal phase
        phase = "reserve"
        reserve(CaptureProgress(kind, received, retained, produced, decoded_retained, allocation))

    def decoded(block: bytes) -> None:
        nonlocal produced, decoded_retained, pending_decoded, phase
        if not block:
            return
        produced += len(block)
        produced_hash.update(block)
        pending_decoded = block
        phase = "decoded_sink"
        if charge is not None:
            charge(block)
        retain_decoded(block)
        decoded_retained += len(block)
        decoded_hash.update(block)
        pending_decoded = b""

    try:
        charge = budget.response_charge() if budget is not None else None
        if response.is_stream_consumed:
            failure = "wire_stream_unavailable"
        elif encoding not in {"identity", "gzip", "deflate"}:
            failure = "unsupported_content_encoding"
        elif length_header is not None and (not length_header or not length_header.isascii() or not length_header.isdecimal()):
            failure = "invalid_content_length"
        else:
            length = int(length_header) if length_header is not None else None
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS) if encoding != "identity" else None
            iterator = iter(response.iter_raw())  # chunk_size would hide consumed partial wire.
            while failure is None:
                reservation("wire", DECODE_BLOCK_BYTES)
                phase = "transport"
                try:
                    block = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                if not isinstance(block, bytes):
                    raise TypeError("wire_block_must_be_bytes")
                received += len(block)
                received_hash.update(block)
                pending_wire = block
                if (max_wire_block_bytes is not None
                        and (len(block) > max_wire_block_bytes or received > max_wire_bytes)):
                    # The transport has already yielded this block, so account
                    # for what was observed, but refuse it before any sink can
                    # persist bytes beyond the caller's declared bounds.
                    failure = ("wire_block_limit_exceeded" if len(block) > max_wire_block_bytes
                               else "wire_limit_exceeded")
                    break
                if max_wire_block_bytes is not None:
                    reservation("wire_block", len(block))
                phase = "wire_sink"
                if charge is not None:
                    charge(block)
                retain_wire(block)
                retained += len(block)
                retained_hash.update(block)
                pending_wire = b""
                if max_wire_block_bytes is None and received > max_wire_bytes:
                    failure = "wire_limit_exceeded"
                    break
                if decoder is None:
                    reservation("decode", len(block))
                    decoded(block)
                    if produced > max_decoded_bytes:
                        failure = "decoded_limit_exceeded"
                else:
                    pending = block
                    while True:
                        reservation("decode", DECODE_BLOCK_BYTES)
                        phase = "decode"
                        output = decoder.decompress(pending, DECODE_BLOCK_BYTES)
                        decoded(output)
                        if produced > max_decoded_bytes:
                            failure = "decoded_limit_exceeded"
                            break
                        if decoder.unused_data:
                            failure = "compressed_trailing_data"
                            break
                        pending = decoder.unconsumed_tail
                        if not pending and len(output) < DECODE_BLOCK_BYTES:
                            break
            if failure is None:
                if decoder is not None and not decoder.eof:
                    failure = "compressed_stream_incomplete"
                elif length is not None and length != received:
                    failure = "content_length_mismatch"
                elif response.num_bytes_downloaded != received:
                    failure = "httpx_wire_count_mismatch"
    except Exception as exc:
        # Incomplete evidence must survive callback/transport/decoder errors;
        # KeyboardInterrupt/SystemExit are deliberately not converted to data.
        failure = f"{phase}_error"
        error_type = type(exc).__name__

    return HTTPCaptureResult(
        complete=failure is None, status_code=response.status_code,
        content_encoding=encoding, content_length_header=length_header,
        content_length=length, wire_bytes_received=received,
        wire_bytes_retained=retained, wire_sha256=retained_hash.hexdigest(),
        received_wire_sha256=received_hash.hexdigest(), decoded_bytes_produced=produced,
        decoded_bytes_retained=decoded_retained, decoded_sha256=decoded_hash.hexdigest(),
        produced_decoded_sha256=produced_hash.hexdigest(),
        httpx_bytes_downloaded=response.num_bytes_downloaded, stream_exhausted=exhausted,
        decoder_eof=decoder.eof if decoder is not None else None,
        error_code=failure, error_type=error_type, pending_wire=pending_wire,
        pending_decoded=pending_decoded,
    )
