"""Native ArcGIS request custody and exact-ID acquisition into staged raw objects."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import time
from urllib.parse import urlencode
from uuid import uuid4

import httpx

from dagster_hifld.http_capture import capture_http_response


def _sha(body):
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class RawCapturePolicy:
    response_limit: int
    total_limit: int
    attempts: int = 1
    body_suffix: str = 'json'
    extended_receipt: bool = False
    exact_status: bool = False
    preserve_transport: bool = False


def expected_request(method, url, params):
    if method == 'POST':
        return url, urlencode(params).encode('ascii')
    return str(httpx.URL(url).copy_merge_params(params)), b''


def _parsed(body, name):
    parsed = json.loads(body)
    if not isinstance(parsed, dict) or 'error' in parsed:
        raise ValueError('invalid_arcgis_response:' + name)
    return parsed


def read_staged_response(read, prefix, name, method, url, params):
    """Validate immutable retained POST/GET evidence before copying it forward."""
    expected_url, expected_body = expected_request(method, url, params)
    body = read(f'{prefix}/raw/{name}.body')
    wire = read(f'{prefix}/raw/{name}.wire')
    raw_receipt = read(f'{prefix}/raw/{name}.receipt.json')
    receipt = json.loads(raw_receipt)
    if (receipt.get('complete') is not True or receipt.get('status') != 200
            or receipt.get('sha256') != _sha(body) or receipt.get('bytes') != len(body)
            or receipt.get('wire_sha256') != _sha(wire) or receipt.get('wire_bytes') != len(wire)
            or receipt.get('method') != method or receipt.get('url') != expected_url
            or receipt.get('final_url') != expected_url):
        raise ValueError('retained_capture_receipt_mismatch:' + name)
    if method == 'POST':
        request_body = read(f'{prefix}/raw/{name}.request.body')
        if (request_body != expected_body or receipt.get('request_body_sha256') != _sha(expected_body)
                or receipt.get('request_body_bytes') != len(expected_body)):
            raise ValueError('retained_capture_request_mismatch:' + name)
    return body, wire, receipt, raw_receipt


def request_raw(client, url, params, name, budget, *, write, encode, policy,
                method='GET', retained=None):
    """Stream one bounded request, retain failed responses, and stage native receipts.

    The policy records historical custody contracts: suffixes, POST request bodies,
    receipt fields, status acceptance and bounded status retry. Source identity and
    candidate metadata remain with the caller. ``retained`` supplies validated
    replay bytes; its optional fourth item preserves the original receipt bytes.
    """
    expected_url, request_body = expected_request(method, url, params)

    def reserve(upcoming):
        if budget[0] + upcoming > policy.total_limit:
            raise ValueError('source_total_capture_budget_exceeded')

    def stage(artifact_name, body, wire, receipt, raw_receipt=None, prefix='raw'):
        for suffix, data in ((policy.body_suffix, body), ('wire', wire),
                             ('receipt.json', raw_receipt if raw_receipt is not None else encode(receipt))):
            write(f'{prefix}/{artifact_name}.{suffix}', data)
        if request_body:
            write(f'{prefix}/{artifact_name}.request.body', request_body)

    if retained is not None:
        values = retained(name, params)
        body, wire, receipt = values[:3]
        # Docks historically writes replay artifacts before its aggregate check;
        # the .json family checks first. Keep that custody ordering explicit.
        if policy.extended_receipt:
            stage(name, body, wire, receipt, values[3] if len(values) > 3 else None)
        budget[0] += len(body) + len(wire)
        reserve(0)
        if not policy.extended_receipt:
            stage(name, body, wire, receipt, values[3] if len(values) > 3 else None)
        return _parsed(body, name)

    for attempt in range(policy.attempts):
        body_parts, wire_parts = [], []
        response_obj = None
        retained_size = 0
        sink_sizes = {'wire': 0, 'decoded': 0}

        def retain(parts, block, kind):
            nonlocal retained_size
            if sink_sizes[kind] + len(block) > policy.response_limit:
                raise ValueError('source_response_capture_budget_exceeded')
            reserve(retained_size + len(block))
            parts.append(block)
            retained_size += len(block)
            sink_sizes[kind] += len(block)

        try:
            with client.stream(method, url, params=params if method == 'GET' else None,
                    data=params if method == 'POST' else None,
                    headers={'Accept-Encoding': 'identity'}) as response:
                response_obj = response
                capture = capture_http_response(response,
                    max_wire_bytes=policy.response_limit, max_decoded_bytes=policy.response_limit,
                    max_wire_block_bytes=policy.response_limit,
                    reserve=lambda p: reserve(p.wire_bytes_retained + p.decoded_bytes_retained + p.reserve_bytes),
                    retain_wire=lambda block: retain(wire_parts, block, 'wire'),
                    retain_decoded=lambda block: retain(body_parts, block, 'decoded'))
        except Exception as exc:
            if policy.preserve_transport:
                body, wire = b''.join(body_parts), b''.join(wire_parts)
                artifact = f"{name}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.{uuid4().hex[:8]}"
                receipt = {'method': method, 'url': expected_url,
                    'final_url': str(response_obj.url) if response_obj is not None else expected_url,
                    'request_body_bytes': len(request_body),
                    'request_body_sha256': _sha(request_body) if request_body else None,
                    'status': response_obj.status_code if response_obj is not None else None,
                    'observed_at': datetime.now(timezone.utc).isoformat(), 'bytes': len(body), 'sha256': _sha(body),
                    'wire_bytes': len(wire), 'wire_sha256': _sha(wire), 'complete': False,
                    'error_code': 'transport_error', 'error_type': type(exc).__name__}
                stage(artifact, body, wire, receipt, prefix='quarantine/partial')
            raise ValueError('source_transport_error:' + type(exc).__name__) from exc
        body, wire = b''.join(body_parts), b''.join(wire_parts)
        status_ok = capture.complete and (capture.status_code == 200 if policy.exact_status
                                         else 200 <= capture.status_code < 300)
        receipt = {'method': method, 'url': str(response_obj.request.url), 'status': capture.status_code,
            'observed_at': datetime.now(timezone.utc).isoformat(), 'bytes': len(body), 'sha256': _sha(body),
            'wire_bytes': len(wire), 'wire_sha256': _sha(wire),
            'content_type': response_obj.headers.get('content-type'), 'content_encoding': capture.content_encoding,
            'complete': capture.complete, 'error_code': capture.error_code}
        if not capture.complete:
            receipt.update(wire_bytes_received=capture.wire_bytes_received,
                received_wire_sha256=capture.received_wire_sha256,
                decoded_bytes_produced=capture.decoded_bytes_produced,
                produced_decoded_sha256=capture.produced_decoded_sha256)
        if policy.attempts > 1:
            receipt['attempt'] = attempt + 1
        if policy.extended_receipt:
            receipt.update(final_url=str(response_obj.url), request_body_bytes=len(request_body),
                request_body_sha256=_sha(request_body) if request_body else None)
            status_ok = status_ok and str(response_obj.url) == str(response_obj.request.url)
        budget[0] += len(body) + len(wire)
        reserve(0)
        artifact = name if status_ok or policy.attempts == 1 else f'{name}.attempt_{attempt + 1}'
        stage(artifact, body, wire, receipt)
        if status_ok:
            return _parsed(body, name)
        if capture.status_code in {429, 500, 502, 503, 504} and attempt < policy.attempts - 1:
            time.sleep(0.25 * (attempt + 1))
            continue
        raise ValueError('incomplete_source_response:' + name)
    raise ValueError('source_retry_limit_exceeded:' + name)


def capture_exact_id_population(ids, page_size, *, fetch_page, validate_page, close_witnesses):
    """Acquire exact ID slices in order and only then acquire closing witnesses.

    Validation callbacks contain source schema/geometry rules. Request selection,
    page slicing, population aggregation and closing-witness ordering have one owner.
    """
    features, pages = [], []
    for offset in range(0, len(ids), page_size):
        requested = ids[offset:offset + page_size]
        result = fetch_page(offset, requested)
        page = validate_page(offset, requested, result)
        returned = [feature.get('attributes', {}).get('OBJECTID') for feature in page]
        features.extend(page)
        pages.append({'offset': offset, 'requested_ids': requested, 'returned_ids': returned, 'count': len(page)})
    return features, pages, close_witnesses()


def population_snapshot(count, ids, *, ids_first=False):
    """Read a count/ID pair in the caller's original witness order."""
    if ids_first:
        roster = ids()
        return count(), roster
    value = count()
    return value, ids()


def validate_population_snapshot(count_before, before, count_after, after, features):
    """Require the captured native IDs to equal both population witnesses."""
    fetched_ids = [feature['attributes']['OBJECTID'] for feature in features]
    if count_before != count_after or before != after or fetched_ids != before or len(features) != len(before):
        raise ValueError('source_population_changed_during_capture')


@dataclass(frozen=True)
class FileRequestPolicy:
    response_limit: int
    space_floor: int
    raw_prefix: str
    receipt_first: bool = False


@dataclass(frozen=True)
class MemoryRequestPolicy:
    wire_limit: int
    decoded_limit: int
    space_floor: int
    raw_prefix: str
    error: type[ValueError]


def _retain_file_fragment(path, body, read):
    import os

    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if os.write(descriptor, body) != len(body):
            raise OSError('short_pending_capture_write')
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if read(path, len(body)) != body:
        raise OSError('pending_capture_readback_mismatch')


def _bounded_pending(block, retained, limit):
    # HTTPX has already delivered the entire block. Keep its full observation
    # identity while limiting physical refusal evidence to the remaining budget.
    prefix = block[:max(0, limit - retained)]
    observation = {'bytes': len(block), 'sha256': _sha(block)}
    if len(prefix) != len(block):
        observation.update(retained_bytes=len(prefix), retained_sha256=_sha(prefix), truncated=True)
    return prefix, observation


def _preserve_file_transfer(path, result, read, encode, limit):
    from dagster_hifld.validation import sha256_file

    transfer = {key: value for key, value in vars(result).items()
                if key not in ('pending_wire', 'pending_decoded')}
    for label in ('wire', 'decoded'):
        observed = path / (label + '.body')
        pending, observation = _bounded_pending(
            getattr(result, 'pending_' + label), observed.stat().st_size, limit)
        if pending:
            _retain_file_fragment(path / ('pending_' + label + '.body'), pending, read)
        transfer['pending_' + label + '_artifact'] = {
            **observation, 'path': 'pending_' + label + '.body' if pending else None,
        }
        transfer['physical_' + label] = {'bytes': observed.stat().st_size, 'sha256': sha256_file(observed)}
    readback = (transfer['physical_wire']['bytes'] == result.wire_bytes_retained
                and transfer['physical_wire']['sha256'] == result.wire_sha256
                and transfer['physical_decoded']['bytes'] == result.decoded_bytes_retained
                and transfer['physical_decoded']['sha256'] == result.decoded_sha256)
    transfer['independent_physical_readback_matches_acknowledged_sinks'] = readback
    _retain_file_fragment(path / 'transfer.json', encode(transfer), read)
    return readback


def _retain_verified_block(stream, block, target, limit):
    import os
    import stat

    before = os.fstat(stream.fileno())
    if before.st_size + len(block) > limit:
        raise ValueError('source_response_capture_budget_exceeded')
    written = stream.write(block)
    stream.flush()
    os.fsync(stream.fileno())
    if written != len(block):
        raise OSError('short_capture_write')
    reader = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        actual = os.fstat(reader)
        if (not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1
                or (actual.st_dev, actual.st_ino) != (before.st_dev, before.st_ino)
                or actual.st_size != before.st_size + len(block)
                or os.pread(reader, len(block), before.st_size) != block):
            raise OSError('capture_block_readback_mismatch')
    finally:
        os.close(reader)


def _capture_file_response(url, path, *, policy, read, encode, client=None):
    """Retain a GET in exclusive dual file sinks and verify every acknowledged block.

    Failed sink blocks and the independent transfer/readback record remain beside
    the physical prefixes. ``read`` preserves the source's existing file-admission
    errors; receipt serialization and final source validation stay with callers.
    """
    from contextlib import nullcontext
    from pathlib import Path
    import shutil

    path = Path(path)
    path.mkdir(parents=True)
    with (path / 'wire.body').open('xb') as wire, (path / 'decoded.body').open('xb') as decoded:
        def reserve(progress):
            if shutil.disk_usage(path).free < policy.space_floor + progress.reserve_bytes:
                raise OSError('disk_floor_before_read')

        receipt = {'url': url, 'observed_at': datetime.now(timezone.utc).isoformat()}
        result = None
        try:
            context = (nullcontext(client) if client is not None else
                       httpx.Client(follow_redirects=False, timeout=60, headers={'Accept-Encoding': 'identity'}))
            with context as active, active.stream('GET', url) as response:
                result = capture_http_response(
                    response, max_wire_bytes=policy.response_limit, max_decoded_bytes=policy.response_limit,
                    max_wire_block_bytes=policy.response_limit, reserve=reserve,
                    retain_wire=lambda block: _retain_verified_block(wire, block, path / 'wire.body', policy.response_limit),
                    retain_decoded=lambda block: _retain_verified_block(decoded, block, path / 'decoded.body', policy.response_limit),
                )
                receipt.update(final_url=str(response.url), status=response.status_code, complete_body=result.complete,
                    bytes=result.decoded_bytes_retained, sha256=result.decoded_sha256,
                    wire_bytes=result.wire_bytes_received, error_code=result.error_code)
        finally:
            if result is not None:
                readback = _preserve_file_transfer(path, result, read, encode, policy.response_limit)
            wire.flush()
            decoded.flush()
    receipt['complete_body'] = result.complete and readback
    receipt['independent_sink_readback_matches'] = readback
    return read(path / 'decoded.body', policy.response_limit + 1024 * 1024), receipt


def _capture_memory_response(url, name, *, policy, space_root, write, encode,
                            pieces, on_interrupt, client=None):
    """Own bounded dual memory sinks, native receipts and successful raw custody."""
    from contextlib import nullcontext
    import shutil

    wire, decoded = bytearray(), bytearray()

    def retain(sink, block, limit):
        if len(sink) + len(block) > limit:
            raise policy.error('source_response_capture_budget_exceeded')
        sink.extend(block)

    def reserve(progress):
        if shutil.disk_usage(space_root()).free < (policy.space_floor + len(wire) + len(decoded)
                                                   + progress.reserve_bytes):
            raise policy.error('capture_reserve_before_pull_or_decode')

    context = (nullcontext(client) if client is not None else
               httpx.Client(timeout=60, follow_redirects=False, headers={'Accept-Encoding': 'identity'}))
    with context as active:
        with active.stream('GET', url) as response:
            try:
                result = capture_http_response(
                    response, max_wire_bytes=policy.wire_limit, max_decoded_bytes=policy.decoded_limit,
                    max_wire_block_bytes=policy.wire_limit, reserve=reserve,
                    retain_wire=lambda block: retain(wire, block, policy.wire_limit),
                    retain_decoded=lambda block: retain(decoded, block, policy.decoded_limit),
                )
            except (KeyboardInterrupt, SystemExit):
                on_interrupt({'wire.body': bytes(wire), 'response.body': bytes(decoded)})
                raise
            body = bytes(decoded)
            pieces.update({'wire.body': bytes(wire), 'response.body': body})
            for label, retained, limit in (('wire', len(wire), policy.wire_limit),
                                           ('decoded', len(body), policy.decoded_limit)):
                pending, observation = _bounded_pending(getattr(result, 'pending_' + label), retained, limit)
                pieces['pending-' + label + '.body'] = pending
                if observation.get('truncated'):
                    pieces['pending-' + label + '.observation.json'] = encode(observation)
            if (len(wire) != result.wire_bytes_retained or _sha(bytes(wire)) != result.wire_sha256
                    or len(body) != result.decoded_bytes_retained or _sha(body) != result.decoded_sha256):
                raise policy.error('capture_sink_readback_mismatch')
            receipt = {'requested_url': url, 'final_url': str(response.url), 'status': response.status_code,
                'observed_at': datetime.now(timezone.utc).isoformat(), 'bytes': len(body), 'sha256': _sha(body),
                'complete': result.complete, 'wire_bytes': result.wire_bytes_received,
                'wire_sha256': result.received_wire_sha256, 'content_encoding': result.content_encoding,
                'content_length': result.content_length, 'capture_error': result.error_code}
            receipt_body = encode(receipt)
            pieces['receipt.json'] = receipt_body
            write(f'{policy.raw_prefix}/{_sha(body)}/{name}.body', body)
            write(f'{policy.raw_prefix}/receipts/{_sha(receipt_body)}.body', receipt_body)
            if not result.complete:
                raise policy.error(result.error_code or 'incomplete_transfer')
    return body, receipt, receipt_body


def capture_file_request(url, name, storage_root, request_path, *, policy, read, encode,
                         parse_receipt, validate, write, write_receipt, replay_root=None,
                         write_replay_reference=None, client=None):
    """Own file-backed capture/replay custody before exposing a lazy raw reference."""
    from pathlib import Path

    def body_key(body):
        return f'{policy.raw_prefix}/{_sha(body)}/{name}.body'

    if replay_root is not None:
        body = read(Path(replay_root) / (name + '.body'), policy.response_limit)
        if not policy.receipt_first:
            write(body_key(body), body)
            if write_replay_reference is not None:
                write_replay_reference(encode({'bytes': len(body), 'sha256': _sha(body),
                                               'body_key': body_key(body)}))
        raw = read(Path(replay_root) / (name + '.receipt.json'), 65536)
    else:
        body, receipt = _capture_file_response(
            url, request_path, policy=policy, read=read, encode=encode, client=client)
        if not policy.receipt_first:
            write(body_key(body), body)
        raw = encode(receipt)
    write_receipt(raw)
    if policy.receipt_first:
        write(body_key(body), body)
    receipt = parse_receipt(raw)
    validate(body, receipt)
    return Path(storage_root) / body_key(body), receipt


def capture_memory_request(url, name, *, policy, space_root, write, encode,
                           read, parse_receipt, validate, diagnostic,
                           replay_root=None, client=None):
    """Own memory capture/replay, original receipt storage and failure diagnostics."""
    from pathlib import Path

    body = receipt_body = b''
    pieces = {}
    try:
        if replay_root is not None:
            body, error = read(Path(replay_root) / (name + '.body'), policy.decoded_limit)
            pieces['response.body'] = body
            write(f'{policy.raw_prefix}/{_sha(body)}/{name}.body', body)
            if error:
                raise policy.error(error)
            receipt_body, error = read(Path(replay_root) / (name + '.receipt.json'), 65536)
            pieces['original-receipt.prefix.body' if error else 'original-receipt.body'] = receipt_body
            write(f'{policy.raw_prefix}/receipts/{_sha(receipt_body)}.body', receipt_body)
            if error:
                raise policy.error(error)
            receipt = parse_receipt(receipt_body)
        else:
            body, receipt, receipt_body = _capture_memory_response(
                url, name, policy=policy, space_root=space_root, write=write, encode=encode,
                pieces=pieces, on_interrupt=lambda value: diagnostic(value, 'capture_interrupted_no_retry'),
                client=client,
            )
        validate(body, receipt)
        return body, receipt, receipt_body
    except (OSError, ValueError, TypeError, httpx.HTTPError) as exc:
        where = diagnostic(pieces, str(exc))
        raise policy.error(f'{exc}; preserved:{where}') from exc
