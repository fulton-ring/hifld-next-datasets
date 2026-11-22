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
        try:
            with client.stream(method, url, params=params if method == 'GET' else None,
                    data=params if method == 'POST' else None,
                    headers={'Accept-Encoding': 'identity'}) as response:
                response_obj = response
                capture = capture_http_response(response,
                    max_wire_bytes=policy.response_limit, max_decoded_bytes=policy.response_limit,
                    reserve=lambda p: reserve(p.wire_bytes_retained + p.decoded_bytes_retained + p.reserve_bytes),
                    retain_wire=wire_parts.append, retain_decoded=body_parts.append)
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
