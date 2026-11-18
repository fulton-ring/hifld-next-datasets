"""Shared bounded HTTP range reader for EPA's FRS national combined CSV archive.

EPA's combined national CSV download page links one national ZIP. Dataset
adapters that read it must not download the whole archive or copy the ZIP
reader. This module verifies the authority page, reads the archive EOCD and
central directory with bounded HTTP range requests, and can stream one deflated
CSV member in bounded compressed chunks with producer CRC-32 and size
verification.

Streaming members are never retained whole. Callers keep only bounded capture
evidence and per-chunk receipts. Source, schema, baseline, rights and release
acceptance remain with each dataset adapter and are never granted here.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import stat
import struct
import time
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response, validate_capture_roster
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.capture_common import _validate_cached_catalog_sidecars
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha


AUTHORITY_PAGE_URL = "https://www.epa.gov/frs/epa-state-combined-csv-download-files"


NATIONAL_COMBINED_ZIP_URL = "https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip"


AUTHORITY_LINK_MARKER = f'href="{NATIONAL_COMBINED_ZIP_URL}"'.encode()


AUTHORITY_TEXT_MARKER = b"A combined National Zip file of 732 MB is available"


PUBLISHER = "U.S. Environmental Protection Agency, Office of Environmental Information"


class EPAFRSNationalArchiveError(ValueError):
    """The EPA FRS national combined archive is incomplete or has drifted."""


EXPECTED_ZIP_TOTAL_BYTES = 1_374_821_998


EXPECTED_ARCHIVE_MEMBER_NAMES = frozenset({
    "NATIONAL_ALTERNATIVE_NAME_FILE.CSV",
    "NATIONAL_CONTACT_FILE.CSV",
    "NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV",
    "NATIONAL_FACILITY_FILE.CSV",
    "NATIONAL_MAILING_ADDRESS_FILE.CSV",
    "NATIONAL_NAICS_FILE.CSV",
    "NATIONAL_ORGANIZATION_FILE.CSV",
    "NATIONAL_PROGRAM_FILE.CSV",
    "NATIONAL_SIC_FILE.CSV",
    "NATIONAL_SUPP_INTEREST_FILE.CSV",
    "Facility State File Documentation 11132012_new.pdf",
})


EXPECTED_MEMBER_METHOD = 8


EOCD_TAIL_BYTES = 65_536


LOCAL_HEADER_PROBE_BYTES = 1_024


MEMBER_CHUNK_BYTES = 8 * 1024 * 1024


STREAM_WINDOW = 8


STREAM_ATTEMPTS = 3


DECOMPRESS_BLOCK_BYTES = 4 * 1024 * 1024


MAX_CAPTURE_BYTES = 96_000_000


MAX_RESPONSE_BYTES = 17_000_000


MIN_CAPTURE_BYTES = 1


MAX_CAPTURE_MANIFEST_BYTES = 1_000_000


MAX_CAPTURE_REQUESTS = 64


MAX_CAPTURE_RECEIPT_BYTES = 64_000


MAX_CAPTURE_RECEIPT_TOTAL_BYTES = 1_000_000


MAX_MEMBER_BYTES_IN_MEMORY = 128 * 1024 * 1024


DISK_FLOOR_BYTES = 2_000_000_000


STAGING_OVERHEAD_BYTES = 8_000_000


def _require_disk_space(path: Path, reserve_bytes: int = 0) -> None:
    if type(reserve_bytes) is not int or reserve_bytes < 0:
        raise ValueError("disk_reserve_invalid")
    if shutil.disk_usage(path).free < DISK_FLOOR_BYTES + reserve_bytes:
        raise ValueError("local_storage_floor")


def _read_bounded_capture_file(path: Path, max_bytes: int, *,
                               expected_bytes: int | None = None) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise ValueError("retained_capture_artifact_unavailable") from exc
    if not stat.S_ISREG(before.st_mode) or path.is_symlink():
        raise ValueError("retained_capture_artifact_type_invalid")
    if before.st_size > max_bytes:
        raise ValueError("retained_capture_artifact_oversized")
    if expected_bytes is not None and before.st_size != expected_bytes:
        raise ValueError("retained_capture_artifact_size_mismatch")
    read_limit = before.st_size if expected_bytes is None else expected_bytes
    with path.open("rb") as stream:
        data = stream.read(read_limit + 1)
    after = path.lstat()
    if (not stat.S_ISREG(after.st_mode) or path.is_symlink()
            or after.st_dev != before.st_dev or after.st_ino != before.st_ino
            or after.st_size != before.st_size or len(data) != before.st_size):
        raise ValueError("retained_capture_artifact_changed_during_read")
    return data


def _capture_range_get(client: httpx.Client, url: str, capture_root: Path, name: str,
                       *, start: int | None = None, end: int | None = None,
                       max_bytes: int = MAX_RESPONSE_BYTES,
                       retained_bytes: list[int] | None = None) -> tuple[bytes, dict[str, object]]:
    """Retain one complete GET, optionally ranged, with wire/body/receipt evidence."""
    if start is None and end is None:
        expected_status = 200
    elif (type(start) is not int or type(end) is not int or start < 0
            or end < start or end >= EXPECTED_ZIP_TOTAL_BYTES):
        raise ValueError("invalid_capture_range:" + name)
    else:
        expected_status = 206
    request_headers = {"Accept-Encoding": "identity"}
    if start is not None:
        request_headers["Range"] = f"bytes={start}-{end}"
    capture_root.mkdir(parents=True, exist_ok=True)
    wire_path, body_path = capture_root / f"{name}.wire", capture_root / f"{name}.body"
    for path in (wire_path, body_path):
        path.unlink(missing_ok=True)
    budget = retained_bytes if retained_bytes is not None else []
    observed = datetime.now(timezone.utc).isoformat()

    def reserve(progress) -> None:
        if sum(budget) + progress.reserve_bytes > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
        _require_disk_space(capture_root, progress.reserve_bytes + STAGING_OVERHEAD_BYTES)

    def retain(path: Path, data: bytes, kind: str) -> None:
        if sum(budget) + len(data) > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
        with path.open("ab") as stream:
            stream.write(data)
        budget.append(len(data))

    result = None
    try:
        with client.stream("GET", url, headers=request_headers) as response:
            result = capture_http_response(
                response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                reserve=reserve,
                retain_wire=lambda data: retain(wire_path, data, "wire"),
                retain_decoded=lambda data: retain(body_path, data, "body"),
            )
            status = response.status_code
            content_range = response.headers.get("Content-Range")
            accept_ranges = response.headers.get("Accept-Ranges")
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            final_url = str(response.url)
    except httpx.HTTPError:
        for path in (wire_path, body_path):
            path.touch(exist_ok=True)
        receipt = {
            "method": "GET", "url": url, "final_url": None, "observed_at": observed,
            "status": None, "content_range": None, "zip_total_bytes": None,
            "accept_ranges": None, "etag": None, "last_modified": None,
            "complete_body": False, "wire_bytes": 0, "wire_sha256": hashlib.sha256().hexdigest(),
            "bytes": 0, "sha256": hashlib.sha256().hexdigest(),
            "content_encoding": None, "content_length": None,
            "error_code": "request_error", "error_type": None,
        }
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        raise ValueError(f"incomplete_source_capture:{name}") from None
    for path in (wire_path, body_path):
        path.touch(exist_ok=True)
    zip_total = None
    if start is not None:
        match = re.fullmatch(rf"bytes {start}-{end}/(\d+)", content_range or "")
        if match:
            zip_total = int(match.group(1))
    receipt = {
        "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": status, "content_range": content_range, "accept_ranges": accept_ranges,
        "etag": etag, "last_modified": last_modified, "zip_total_bytes": zip_total,
        "complete_body": result.complete, "wire_bytes": result.wire_bytes_retained,
        "wire_sha256": result.wire_sha256, "bytes": result.decoded_bytes_retained,
        "sha256": result.decoded_sha256, "content_encoding": result.content_encoding,
        "content_length": result.content_length,
        "error_code": result.error_code, "error_type": result.error_type,
    }
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    body = body_path.read_bytes()
    wire = wire_path.read_bytes()
    if (not result.complete or status != expected_status or final_url != url
            or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
            or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError(f"incomplete_source_capture:{name}")
    if start is not None:
        if (content_range != f"bytes {start}-{end}/{EXPECTED_ZIP_TOTAL_BYTES}"
                or zip_total != EXPECTED_ZIP_TOTAL_BYTES or len(body) != end - start + 1):
            raise ValueError(f"range_response_contract_failed:{name}")
    return body, receipt


def _parse_eocd(tail: bytes, total: int) -> tuple[int, int, int]:
    index = tail.rfind(b"PK\x05\x06")
    if index < 0 or tail.rfind(b"PK\x06\x06") >= 0:
        raise EPAFRSNationalArchiveError("zip_eocd_missing_or_zip64_unsupported")
    (_, _, _, _, entries, cd_size, cd_offset, comment_length) = struct.unpack(
        "<IHHHHIIH", tail[index:index + 22])
    if (entries != len(EXPECTED_ARCHIVE_MEMBER_NAMES) or comment_length != 0
            or cd_offset + cd_size + 22 != total):
        raise EPAFRSNationalArchiveError("zip_eocd_roster_or_layout_drift")
    return entries, cd_size, cd_offset


def _parse_central_directory_members(body: bytes) -> dict[str, dict[str, object]]:
    if len(body) < 46 or not body.startswith(b"PK\x01\x02"):
        raise EPAFRSNationalArchiveError("zip_central_directory_invalid")
    members: dict[str, dict[str, object]] = {}
    offset = 0
    while offset < len(body):
        if body[offset:offset + 4] != b"PK\x01\x02":
            raise EPAFRSNationalArchiveError("zip_central_directory_entry_invalid")
        (_, _, _, flags, method, _, _, crc, compressed, uncompressed,
         name_length, extra_length, comment_length, _, _, _, local_header_offset) = struct.unpack(
            "<IHHHHHHIIIHHHHHII", body[offset:offset + 46])
        name = body[offset + 46:offset + 46 + name_length].decode("utf-8")
        members[name] = {
            "flags": flags, "method": method, "crc32": f"{crc:08x}",
            "compressed": compressed, "uncompressed": uncompressed,
            "local_header_offset": local_header_offset,
        }
        offset += 46 + name_length + extra_length + comment_length
    return members


def _parse_member_local_header(body: bytes, members: dict[str, dict[str, object]],
                               member_name: str) -> int:
    if len(body) < 30 or not body.startswith(b"PK\x03\x04"):
        raise EPAFRSNationalArchiveError("zip_member_local_header_invalid")
    (_, _, flags, method, _, _, crc, compressed, uncompressed,
     name_length, extra_length) = struct.unpack("<IHHHHHIIIHH", body[:30])
    name = body[30:30 + name_length].decode("utf-8")
    member = members[member_name]
    if (name != member_name or method != member["method"] or flags != member["flags"]
            or f"{crc:08x}" != member["crc32"] or compressed != member["compressed"]
            or uncompressed != member["uncompressed"]):
        raise EPAFRSNationalArchiveError("zip_member_local_header_drift")
    if 30 + name_length + extra_length > len(body):
        raise EPAFRSNationalArchiveError("zip_member_local_header_truncated")
    return member["local_header_offset"] + 30 + name_length + extra_length


def _stream_range_get(client: httpx.Client, url: str, start: int, end: int, *,
                      max_bytes: int = MAX_RESPONSE_BYTES) -> tuple[bytes, dict[str, object]]:
    """Fetch one ranged response for streaming; retain only its receipt.

    The compressed block is returned to the caller and never written as capture
    evidence, so a multi-hundred-megabyte member stays within a bounded window.
    Transport failures and retryable server statuses are retried a fixed number
    of times because the producer's archive endpoint closes long transfers.
    """
    if (type(start) is not int or type(end) is not int or start < 0
            or end < start or end >= EXPECTED_ZIP_TOTAL_BYTES):
        raise ValueError("invalid_stream_range")
    expected = end - start + 1
    if expected > max_bytes:
        raise ValueError("stream_range_exceeds_response_budget")
    for attempt in range(STREAM_ATTEMPTS):
        body_buffer = bytearray()
        observed = datetime.now(timezone.utc).isoformat()

        def reserve(progress) -> None:
            try:
                if progress.wire_bytes_received + progress.reserve_bytes > max_bytes + DECOMPRESS_BLOCK_BYTES:
                    raise ValueError("stream_response_budget_exceeded")
            except Exception as exc:
                print("DEBUG_RESERVE_EXC", repr(exc), progress, max_bytes, DECODE_BLOCK_BYTES, flush=True)
                raise

        try:
            with client.stream("GET", url, headers={
                    "Accept-Encoding": "identity", "Range": f"bytes={start}-{end}"}) as response:
                result = capture_http_response(
                    response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                    reserve=reserve,
                    retain_wire=lambda data: None,
                    retain_decoded=body_buffer.extend,
                )
                status = response.status_code
                content_range = response.headers.get("Content-Range")
                accept_ranges = response.headers.get("Accept-Ranges")
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")
                final_url = str(response.url)
        except httpx.TransportError:
            if attempt + 1 < STREAM_ATTEMPTS:
                time.sleep(1.0 * (attempt + 1))
                continue
            raise ValueError(f"incomplete_stream_source_capture:{start}-{end}") from None
        body = bytes(body_buffer)
        retryable_status = status in {429, 500, 502, 503, 504}
        retryable_incomplete = (not result.complete and result.error_code not in {
            "reserve_error", "wire_sink_error", "decoded_sink_error",
            "wire_limit_exceeded", "decoded_limit_exceeded",
            "wire_block_limit_exceeded",
        })
        if (retryable_status or retryable_incomplete) and attempt + 1 < STREAM_ATTEMPTS:
            time.sleep(1.0 * (attempt + 1))
            continue
        receipt = {
            "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
            "status": status, "content_range": content_range, "accept_ranges": accept_ranges,
            "etag": etag, "last_modified": last_modified,
            "zip_total_bytes": EXPECTED_ZIP_TOTAL_BYTES,
            "complete_body": result.complete,
            "wire_bytes": result.wire_bytes_received,
            "wire_sha256": result.received_wire_sha256,
            "bytes": len(body), "sha256": result.decoded_sha256,
            "content_encoding": result.content_encoding,
            "content_length": result.content_length,
            "error_code": result.error_code, "error_type": result.error_type,
        }
        if (not result.complete or status != 206 or final_url != url
                or result.content_encoding != "identity"
                or content_range != f"bytes {start}-{end}/{EXPECTED_ZIP_TOTAL_BYTES}"
                or len(body) != expected or result.decoded_bytes_retained != expected
                or _sha(body) != result.decoded_sha256):
            raise ValueError(f"stream_range_response_contract_failed:{start}-{end}")
        return body, receipt
    raise ValueError(f"incomplete_stream_source_capture:{start}-{end}")


def _stream_member_csv(client: httpx.Client, member_name: str, *, data_offset: int,
                       compressed_bytes: int, uncompressed_bytes: int, crc32: str,
                       summary: dict[str, object], chunk_bytes: int = MEMBER_CHUNK_BYTES,
                       max_workers: int = STREAM_WINDOW) -> object:
    """Yield one deflated CSV member as lines without retaining the member.

    Compressed ranges are fetched through a bounded worker window and inflated
    in source order. The producer's declared compressed size, uncompressed size
    and CRC-32 are verified before the final line is released, and every chunk
    receipt is recorded in ``summary``.
    """
    end = data_offset + compressed_bytes
    ranges: list[tuple[int, int]] = []
    cursor = data_offset
    while cursor < end:
        chunk_end = min(cursor + chunk_bytes - 1, end - 1)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1
    if not ranges:
        raise EPAFRSNationalArchiveError("zip_member_empty")
    decoder = zlib.decompressobj(-15)
    buffer = b""
    total_out = 0
    observed_crc = 0
    compressed_sha = hashlib.sha256()
    compressed_total = 0
    receipts: list[dict[str, object]] = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    pending: deque[tuple[int, int, object]] = deque()
    next_index = 0

    def submit_next() -> None:
        nonlocal next_index
        if next_index < len(ranges):
            start, stop = ranges[next_index]
            next_index += 1
            pending.append((start, stop, executor.submit(
                _stream_range_get, client, NATIONAL_COMBINED_ZIP_URL, start, stop)))

    try:
        for _ in range(min(max_workers, len(ranges))):
            submit_next()
        while pending:
            start, stop, future = pending.popleft()
            body, receipt = future.result()
            submit_next()
            compressed_total += len(body)
            if compressed_total > compressed_bytes:
                raise EPAFRSNationalArchiveError("zip_member_compressed_size_drift")
            compressed_sha.update(body)
            receipts.append({"start": start, "end": stop, **receipt})
            pending_bytes = body
            while pending_bytes:
                output = decoder.decompress(pending_bytes, DECOMPRESS_BLOCK_BYTES)
                pending_bytes = decoder.unconsumed_tail
                if not output:
                    continue
                observed_crc = zlib.crc32(output, observed_crc)
                total_out += len(output)
                if total_out > uncompressed_bytes:
                    raise EPAFRSNationalArchiveError(
                        "zip_member_uncompressed_size_exceeds_declaration")
                buffer += output
                parts = buffer.split(b"\n")
                buffer = parts.pop()
                for line in parts:
                    yield line.decode("utf-8")
        tail_data = decoder.flush()
        observed_crc = zlib.crc32(tail_data, observed_crc)
        total_out += len(tail_data)
        if total_out > uncompressed_bytes:
            raise EPAFRSNationalArchiveError("zip_member_uncompressed_size_exceeds_declaration")
        buffer += tail_data
        if buffer:
            yield buffer.decode("utf-8")
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise EPAFRSNationalArchiveError("zip_member_deflate_stream_incomplete")
    finally:
        for _start, _stop, future in pending:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
    if (compressed_total != compressed_bytes or total_out != uncompressed_bytes
            or f"{observed_crc & 0xffffffff:08x}" != crc32):
        raise EPAFRSNationalArchiveError("zip_member_content_crc_or_size_drift")
    summary.update({
        "member_name": member_name,
        "compressed_bytes": compressed_total,
        "uncompressed_bytes": total_out,
        "crc32": f"{observed_crc & 0xffffffff:08x}",
        "compressed_sha256": compressed_sha.hexdigest(),
        "receipts": receipts,
    })


def _write_capture_manifest(root: Path, captures: dict[str, tuple[bytes, dict[str, object]]],
                            retained_bytes: int) -> tuple[dict[str, object], str]:
    if not 1 <= len(captures) <= MAX_CAPTURE_REQUESTS:
        raise ValueError("capture_request_count_invalid")
    _require_disk_space(root, STAGING_OVERHEAD_BYTES)
    descriptors: dict[str, dict[str, object]] = {}
    total_payload = total_receipts = 0
    expected = {"capture_manifest.json"}
    for name, (body, receipt) in sorted(captures.items()):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("capture_name_invalid:" + name)
        if (root / f"{name}.body").read_bytes() != body:
            raise ValueError("capture_body_file_mismatch:" + name)
        wire = _read_bounded_capture_file(root / f"{name}.wire", MAX_RESPONSE_BYTES)
        receipt_bytes = _read_bounded_capture_file(root / f"{name}.receipt.json",
                                                   MAX_CAPTURE_RECEIPT_BYTES)
        if json.loads(receipt_bytes) != receipt:
            raise ValueError("capture_receipt_file_mismatch:" + name)
        if (receipt.get("method") != "GET" or receipt.get("complete_body") is not True
                or receipt.get("final_url") != receipt.get("url")
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("error_code") is not None or receipt.get("error_type") is not None):
            raise ValueError("capture_receipt_invalid:" + name)
        descriptors[name] = {
            "request_url": receipt.get("url"),
            "body": {"file": f"{name}.body", "bytes": len(body), "sha256": _sha(body)},
            "wire": {"file": f"{name}.wire", "bytes": len(wire), "sha256": _sha(wire)},
            "receipt": {"file": f"{name}.receipt.json", "bytes": len(receipt_bytes),
                        "sha256": _sha(receipt_bytes)},
        }
        if len(body) + len(wire) > MAX_RESPONSE_BYTES:
            raise ValueError("capture_response_budget_exceeded:" + name)
        total_payload += len(body) + len(wire)
        total_receipts += len(receipt_bytes)
        expected.update({f"{name}.body", f"{name}.wire", f"{name}.receipt.json"})
    if (total_receipts > MAX_CAPTURE_RECEIPT_TOTAL_BYTES
            or not MIN_CAPTURE_BYTES <= total_payload <= MAX_CAPTURE_BYTES
            or total_payload != retained_bytes):
        raise ValueError("capture_artifact_totals_invalid")
    manifest = {"schema_version": 1, "capture_complete": True,
                "capture_budget_bytes": MAX_CAPTURE_BYTES,
                "response_budget_bytes": MAX_RESPONSE_BYTES,
                "capture_retained_wire_plus_decoded_bytes": total_payload,
                "captures": descriptors}
    encoded = _canonical(manifest) + b"\n"
    (root / "capture_manifest.json").write_bytes(encoded)
    validate_capture_roster(root, expected)
    return manifest, _sha(encoded)


def _load_retained_capture(root: Path) -> tuple[dict[str, object],
                                                dict[str, tuple[bytes, dict[str, object]]], str]:
    root = Path(root).absolute()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("retained_capture_directory_invalid")
    encoded = _read_bounded_capture_file(root / "capture_manifest.json", MAX_CAPTURE_MANIFEST_BYTES)
    manifest = json.loads(encoded)
    if (not isinstance(manifest, dict) or set(manifest) != {
                "schema_version", "capture_complete", "capture_budget_bytes",
                "response_budget_bytes", "capture_retained_wire_plus_decoded_bytes", "captures"}
            or manifest.get("schema_version") != 1 or manifest.get("capture_complete") is not True
            or not isinstance(manifest.get("captures"), dict)
            or not 1 <= len(manifest["captures"]) <= MAX_CAPTURE_REQUESTS
            or type(manifest.get("capture_retained_wire_plus_decoded_bytes")) is not int
            or not MIN_CAPTURE_BYTES <= manifest["capture_retained_wire_plus_decoded_bytes"] <= MAX_CAPTURE_BYTES
            or manifest.get("capture_budget_bytes") != MAX_CAPTURE_BYTES
            or manifest.get("response_budget_bytes") != MAX_RESPONSE_BYTES):
        raise ValueError("retained_capture_manifest_invalid")
    descriptors = manifest["captures"]
    expected = {"capture_manifest.json"}
    total = 0
    for name, descriptor in descriptors.items():
        if (not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
                or not isinstance(descriptor, dict)
                or set(descriptor) != {"request_url", "body", "wire", "receipt"}):
            raise ValueError("retained_capture_manifest_invalid")
        for kind, suffix in (("body", ".body"), ("wire", ".wire"), ("receipt", ".receipt.json")):
            artifact = descriptor.get(kind)
            if (not isinstance(artifact, dict) or set(artifact) != {"file", "bytes", "sha256"}
                    or artifact.get("file") != name + suffix
                    or type(artifact.get("bytes")) is not int or artifact["bytes"] < 0
                    or not isinstance(artifact.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"])):
                raise ValueError("retained_capture_manifest_invalid")
            expected.add(artifact["file"])
        body = descriptor["body"]
        wire = descriptor["wire"]
        if body["bytes"] + wire["bytes"] > MAX_RESPONSE_BYTES:
            raise ValueError("retained_capture_response_budget_exceeded:" + name)
        total += body["bytes"] + wire["bytes"]
    if total != manifest["capture_retained_wire_plus_decoded_bytes"]:
        raise ValueError("retained_capture_descriptor_totals_invalid")
    validate_capture_roster(root, expected)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    for name, descriptor in descriptors.items():
        artifacts = {kind: _read_bounded_capture_file(
                         root / descriptor[kind]["file"],
                         MAX_CAPTURE_RECEIPT_BYTES if kind == "receipt" else MAX_RESPONSE_BYTES,
                         expected_bytes=descriptor[kind]["bytes"])
                     for kind in ("body", "wire", "receipt")}
        for kind, data in artifacts.items():
            if (len(data) != descriptor[kind]["bytes"]
                    or _sha(data) != descriptor[kind]["sha256"]):
                raise ValueError("retained_capture_artifact_mismatch:" + name + ":" + kind)
        body, wire, receipt_bytes = artifacts["body"], artifacts["wire"], artifacts["receipt"]
        receipt = json.loads(receipt_bytes)
        url = descriptor.get("request_url")
        if (not isinstance(url, str) or receipt.get("method") != "GET" or receipt.get("url") != url
                or receipt.get("final_url") != url or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("error_code") is not None or receipt.get("error_type") is not None):
            raise ValueError("retained_capture_receipt_invalid:" + name)
        captures[name] = (body, receipt)
    return manifest, captures, _sha(encoded)


def _input_provenance(name: str, receipt: dict[str, object]) -> dict[str, object]:
    url = receipt["url"]
    return {
        "input_id": name,
        "producer": PUBLISHER,
        "host": urlsplit(url).hostname,
        "role": "producer_publication",
        "canonical_url": AUTHORITY_PAGE_URL if name == "authority_page" else NATIONAL_COMBINED_ZIP_URL,
        "retrieval_url": url,
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"],
        "wire_sha256": receipt["wire_sha256"],
        "license": "Unreviewed; no license claim made.",
        "attribution": "EPA Facility Registry Service combined national CSV delivery; original producer attribution retained.",
        "release_date": None,
        "release_date_unknown_reason": (
            "The archive's HTTP Last-Modified header is a server file timestamp, not an "
            "established EPA release date for the FRS content."
        ),
    }


def _verify_cached_candidate(storage: StagingStorageResource, repo_root: Path, state_key: str,
                             prefix: str, version: str, fingerprint: str,
                             identity: dict[str, object], csv_sha: str, schema_sha: str,
                             expected_objects: dict[str, dict[str, object]],
                             capture_receipts: dict[str, dict[str, object]]) -> None:
    """Verify a same-version candidate and its original range receipts."""
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {
                "processing_identity", "source_record_sha256", "source_schema_sha256",
                "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != csv_sha
            or payload["source_schema_sha256"] != schema_sha
            or payload["capture_receipts"] != dict(capture_receipts)
            or payload["objects"] != dict(expected_objects)):
        raise EPAFRSNationalArchiveError("cached_first_candidate_identity_or_source_changed")
    expected_keys = {f"{prefix}/{name}" for name in expected_objects}
    actual_keys = set(storage.list_prefix(prefix))
    derived_names = {
        "metadata/quality_manifest.json", "metadata/data_dictionary.json",
        "metadata/resolved_provenance.json", "metadata/source/quality_manifest.json",
        "metadata/source/data_dictionary.json",
    }
    snapshot_names: set[str] = set()
    manifest_descriptor = expected_objects.get("metadata/source_manifest.json")
    if isinstance(manifest_descriptor, dict):
        snapshot_names = {
            "metadata/source/source_manifest.json",
            f"metadata/source/version_{manifest_descriptor.get('sha256')}_source_manifest.json",
        }
    found_snapshots = {f"{prefix}/{name}" for name in snapshot_names} & actual_keys
    if found_snapshots and len(found_snapshots) != len(snapshot_names):
        raise EPAFRSNationalArchiveError("cached_catalog_source_snapshot_incomplete")
    allowed_keys = expected_keys | {f"{prefix}/{name}" for name in derived_names | snapshot_names}
    if not expected_keys <= actual_keys or actual_keys - allowed_keys:
        raise EPAFRSNationalArchiveError("cached_candidate_storage_roster_changed")
    for name, descriptor in expected_objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise EPAFRSNationalArchiveError("cached_candidate_object_integrity_failed:" + name)
    for name in snapshot_names:
        key = f"{prefix}/{name}"
        if key not in actual_keys:
            continue
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or manifest_descriptor != {
                "bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise EPAFRSNationalArchiveError("cached_catalog_source_snapshot_changed:" + name)
    for name, receipt in capture_receipts.items():
        body = storage.read_key(f"{prefix}/raw/{name}.body")
        wire = storage.read_key(f"{prefix}/raw/{name}.wire")
        receipt_body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if (receipt.get("complete_body") is not True
                or type(receipt.get("status")) is not int or not 200 <= receipt["status"] < 300
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt_body != _canonical(dict(receipt)) + b"\n"):
            raise EPAFRSNationalArchiveError("cached_original_capture_changed:" + name)
    _validate_cached_catalog_sidecars(storage, prefix, actual_keys)
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"],
                                         identity=identity)
