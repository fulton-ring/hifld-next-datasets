"""WBD native keyset windows, refusal evidence and ordered acquisition."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import httpx

from dagster_hifld.http_capture import capture_retention_budget


@dataclass(frozen=True, kw_only=True)
class WindowPolicy:
    page_size: int
    byte_budget: int
    minimum_ids: int = 1
    max_depth: int | None = None
    single_refusal_retries: int = 0
    single_refusal_retry_seconds: float = 0
    refuse_evidence_collision: bool = False


class _WindowCapture:
    def __init__(self, client, root, capture, policy):
        self.client = client
        self.root = root
        self.capture = capture
        self.policy = policy
        self.parts = {}
        self.refusals = []

    def fetch(self, name, url):
        return self.capture(self.client, url, self.root, name)

    def retain_refusal(self, name, receipt):
        retained = retain_refusal_files(self.root, name,
            reject_collision=self.policy.refuse_evidence_collision)
        self.refusals.append(retained)
        self.parts[retained] = receipt


def retain_refusal_files(root, name, *, reject_collision=True):
    """Retain the body, wire and receipt under one refused-window identity."""
    retained = name + '_refusal'
    for suffix in ('.body', '.wire', '.receipt.json'):
        source, target = root / (name + suffix), root / (retained + suffix)
        if reject_collision and target.exists():
            raise ValueError('source_refusal_evidence_name_collision:' + target.name)
        source.replace(target)
    return retained


def fail_refused_window(context, name, path, ids):
    raise ValueError('source_page_window_refused_after_bisection:' + name)


def fail_single_feature(context, name, path, ids):
    raise ValueError('source_page_window_refused_single_feature:' + name + ':objectid=' + str(ids[0]))


def capture_protobuf_fallback(context, name, path, ids, *, pbf_url, is_pbf_refusal):
    """Keep the exact native protobuf window, or both refused serializers."""
    pbf_name = name + '_pbf'
    body, receipt = context.fetch(pbf_name, pbf_url(path))
    if is_pbf_refusal(body):
        context.retain_refusal(pbf_name, receipt)
        fail_refused_window(context, name, path, ids)
    context.parts[pbf_name] = receipt


def capture_attributes_fallback(context, name, path, ids, *, attrs_url):
    """Retain a single refused geometry's independent attributes channel."""
    attrs_name = name + '_attrs'
    _body, receipt = context.fetch(attrs_name, attrs_url(ids[0]))
    context.parts[attrs_name] = receipt


def capture_window_receipts(client, roster, index, root, *, policy: WindowPolicy,
                             window_url: Callable, capture: Callable,
                             is_refusal: Callable, at_floor: Callable):
    """Bisect native windows once, with explicit terminal serializer policies.

    Only receipts stay in memory. Artifact names and URLs come from the source's
    pinned roster; JSON refusal ancestors remain alongside every served leaf.
    """
    root = Path(root)
    context = _WindowCapture(client, root, capture, policy)

    def walk(path, ids, depth):
        name = f'page_{index:04d}' + path
        url = window_url(roster, index, path)
        body, receipt = context.fetch(name, url)
        if is_refusal(body) and len(ids) == 1:
            for _attempt in range(policy.single_refusal_retries):
                time.sleep(policy.single_refusal_retry_seconds)
                body, receipt = context.fetch(name, url)
                if not is_refusal(body):
                    break
        if not is_refusal(body):
            context.parts[name] = receipt
            return
        context.retain_refusal(name, receipt)
        del body
        if len(ids) <= policy.minimum_ids:
            at_floor(context, name, path, ids)
            return
        if policy.max_depth is not None and depth + 1 > policy.max_depth:
            fail_refused_window(context, name, path, ids)
        half = (len(ids) + 1) // 2
        walk(path + 'a', ids[:half], depth + 1)
        walk(path + 'b', ids[half:], depth + 1)

    with capture_retention_budget(root, policy.byte_budget):
        walk('', roster[index * policy.page_size:(index + 1) * policy.page_size], 0)
    return context.parts, context.refusals


@dataclass(frozen=True, kw_only=True)
class OrderedWindowPolicy:
    byte_budget: int
    seed_url: str
    page_max_bytes: int
    doc_max_bytes: int
    doc_user_agent: str


def receipt_only(value):
    return value[1]


def body_and_receipt(value):
    return value


def capture_ordered_windows(policy: OrderedWindowPolicy, client, root, captures, *,
                             capture, validate_seed, roster, window_capture,
                             project=receipt_only):
    """Own seed, metadata, recursive pages and closing requests for WBD layers.

    The caller's mapping receives the successful prefix for quarantine. The
    service-document client retains its distinct redirect and timeout policy.
    """
    page_parts, page_refusals = {}, []
    with capture_retention_budget(root, policy.byte_budget):
        seed = capture(client, policy.seed_url, root, 'ids_before', max_bytes=policy.page_max_bytes)
        ids = validate_seed(json.loads(seed[0]), 'seed')
        captures['ids_before'] = project(seed)
        del seed
        with httpx.Client(follow_redirects=True,
                          timeout=httpx.Timeout(60, connect=20, read=300),
                          headers={'Connection': 'close', 'User-Agent': policy.doc_user_agent}) as docs:
            for name, url in roster(ids):
                if name in captures:
                    continue
                if name.startswith('page_'):
                    parts, refusals = window_capture(client, ids, int(name[5:9]), root)
                    captures.update(parts)
                    page_parts[name] = [part for part in sorted(parts) if not part.endswith('_refusal')]
                    page_refusals.extend(refusals)
                    continue
                value = capture(docs if name == 'service_doc' else client, url, root, name,
                                max_bytes=policy.doc_max_bytes if name == 'service_doc' else policy.page_max_bytes)
                captures[name] = project(value)
                del value
    return page_parts, page_refusals


@dataclass(frozen=True, kw_only=True)
class SegmentDocuments:
    before: list[tuple[str, str]]
    attributes: Callable
    after: list[tuple[str, str]]
    fetch_group: Callable
    read: Callable
    validate_roster: Callable


@dataclass(frozen=True, kw_only=True)
class SegmentCheckpoints:
    page_size: int
    oid_field: str
    covered: Callable
    gaps: Callable
    parse_name: Callable
    save: Callable


def capture_document_group(client, urls, root, *, capture, read, producer_error,
                            error_type, refresh=False):
    """Reuse validated documents or refresh a complete closing bracket together."""
    from dagster_hifld.sources.capture_common import validate_capture_request_identity

    if refresh:
        for name, _url in urls:
            for suffix in ('.body', '.wire', '.receipt.json'):
                (root / (name + suffix)).unlink(missing_ok=True)
    for name, url in urls:
        if (root / (name + '.body')).exists():
            validate_capture_request_identity(name, read(root, name)[1], url)
            continue
        body, _receipt = capture(client, url, root, name)
        if producer_error(body):
            raise error_type('source_document_error_envelope:' + name)


def capture_segmented_layer(slices, client, root, state, *,
                             documents: SegmentDocuments, checkpoints: SegmentCheckpoints):
    """Acquire HU8's resumable native segments, independent attributes and brackets.

    The adaptive owner bisects producer refusals. This owner commits accepted
    tiling checkpoints and refreshes closing witnesses after resumed extraction.
    """
    from datetime import datetime, timezone
    from dagster_hifld.capture_completeness import validate_capture_witness
    from dagster_hifld.sources.arcgis_adaptive import capture_slices

    with capture_retention_budget(root, slices.byte_budget):
        documents.fetch_group(client, documents.before, root)
        seed = documents.validate_roster(json.loads(documents.read(root, 'ids_before')[0]), 'ids_seed')
        count = json.loads(documents.read(root, 'count_before')[0]).get('count')
        validate_capture_witness(count, seed, count, seed)
        segments = list(state.get('segments') or [])
        spans = checkpoints.covered({'segments': segments}, len(seed))
        gaps = checkpoints.gaps(spans, len(seed))
        ranges = [(offset + start, min(checkpoints.page_size, length - start))
                  for offset, length in gaps for start in range(0, length, checkpoints.page_size)]

        def skip(name):
            offset, count = checkpoints.parse_name(name)
            covered = checkpoints.covered({'segments': segments}, len(seed))
            state['complete'] = False
            checkpoints.save(root, state)
            return any(start <= offset and offset + count <= end for start, end in covered)

        def refused(name):
            _offset, count = checkpoints.parse_name(name)
            if count == 1:
                raise slices.error('source_segment_error_envelope:' + name)

        def accept(name, offset, count, body, receipt):
            page = json.loads(body)
            rows = page.get('features') if isinstance(page, dict) else None
            if (not isinstance(rows, list) or len(rows) != count
                    or [row.get('attributes', {}).get(checkpoints.oid_field) for row in rows]
                    != seed[offset:offset + count]):
                raise slices.error('source_segment_roster_incomplete:' + name)
            covered = checkpoints.covered({'segments': segments}, len(seed))
            if any(start <= offset and offset + count <= end for start, end in covered):
                raise slices.error('source_segment_roster_duplicate:' + name)
            segments.append({'offset': offset, 'count': count, 'name': name,
                             'features': count, 'captured_at': datetime.now(timezone.utc).isoformat()})
            state['segments'] = segments
            checkpoints.save(root, state)
            return True

        capture_slices(slices, client, root, ranges, skip=skip, refused=refused, accept=accept)
        state['segments'] = segments
        attrs = documents.attributes(len(seed))
        if any(not (root / (name + '.body')).exists() for name, _url in attrs):
            state['complete'] = False
            checkpoints.save(root, state)
        documents.fetch_group(client, attrs, root)
        documents.fetch_group(client, documents.after, root,
                              refresh=bool(ranges) or state.get('complete') is not True)
        state['complete'] = True
        checkpoints.save(root, state)
