"""Capture an HTTPX *wire* stream before decoding or rejecting its contents.

The caller owns safe sinks, disk reservation, response acceptance, retries and
receipts. Sinks must retain the whole supplied block or raise; their persisted
bytes still need caller readback checks. No URL, source or release is approved
here. Limits can overshoot by one delivered wire block (HTTPX controls its size)
or one bounded decoded block so evidence is never silently clipped.
"""
from __future__ import annotations

import hashlib
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx


DECODE_BLOCK_BYTES = 1024 * 1024


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
    reserve: Callable[[CaptureProgress], None],
    retain_wire: Callable[[bytes], None],
    retain_decoded: Callable[[bytes], None],
) -> HTTPCaptureResult:
    """Consume a fresh synchronous streamed response using unbuffered iter_raw.

    Supports identity, gzip and RFC zlib-wrapped deflate. Raw deflate, stacked
    encodings, concatenated gzip members and trailing bytes are rejected. Gzip
    CRC/trailer and decoder EOF are checked even when HTTPX considers transport
    complete. Content-Length is checked against wire bytes, never decoded size.
    ``reserve`` must raise on insufficient capacity; it runs before EVERY next
    (including the possible EOF read) and before each bounded decompression.
    Ordinary transport, reserve, decoder and sink exceptions become incomplete
    results; process interruption exceptions propagate with prior sink evidence
    intact. No exception messages/URLs/credentials are copied into results.
    """
    for name, value in (("max_wire_bytes", max_wire_bytes), ("max_decoded_bytes", max_decoded_bytes)):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name}_must_be_nonnegative_integer")
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
        retain_decoded(block)
        decoded_retained += len(block)
        decoded_hash.update(block)
        pending_decoded = b""

    try:
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
                phase = "wire_sink"
                retain_wire(block)
                retained += len(block)
                retained_hash.update(block)
                pending_wire = b""
                if received > max_wire_bytes:
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
