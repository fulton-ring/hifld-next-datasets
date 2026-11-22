"""Ordered acquisition for ArcGIS sources with established receipt protocols.

The request sequence is shared while transport, raw receipt retention and
source document validation remain explicit callbacks. This supports existing
receipt-only and staging-backed families without changing their evidence shape.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable, Sequence

from dagster_hifld.sources import capture_common


def capture_request_sequence(captures, discovery, derive_requests, fetch):
    """Acquire discovery, derive the native page/witness plan, and retain it.

    A failed request leaves the successful prefix available for quarantine.
    Existing entries are reused; callers validate their replay identities.
    """
    def acquire(plan):
        plan = list(plan)
        names = [name for name, _ in plan]
        if len(set(names)) != len(names):
            raise ValueError('capture_roster_name_duplicate')
        for name, url in plan:
            if name in captures:
                continue
            value = fetch(name, url)
            if value is not None:
                captures[name] = value
    acquire(discovery)
    acquire(derive_requests(captures))
    return captures


class LayerCaptureFetch:
    """Bound a GET fetch and preserve the current request for failure custody."""
    def __init__(self, *, max_body_bytes, max_capture_bytes, capture=None):
        self.max_body_bytes = max_body_bytes
        self.max_capture_bytes = max_capture_bytes
        self.capture = capture
        self.current_name = None
        self.current_url = None
        self.retained_bytes = 0

    def fetch(self, client, url, root, name):
        self.current_name, self.current_url = name, url
        transport = self.capture or capture_common.capture_get
        from dagster_hifld.http_capture import capture_retention_budget
        with capture_retention_budget(root, self.max_capture_bytes):
            value = transport(client, url, root, name, max_bytes=self.max_body_bytes)
        receipt = value[1]
        self.retained_bytes += receipt['bytes'] + receipt['wire_bytes']
        if self.retained_bytes > self.max_capture_bytes:
            raise ValueError('source_capture_exceeds_scratch_budget')
        return value


@dataclass(frozen=True)
class ExactIdCapturePlan:
    """Native request and validation policy for the common ID population owner."""
    ids: Sequence[int]
    page_size: int | Callable
    page_request: Callable
    validate_page: Callable
    read_page: Callable
    closing_requests: Sequence[tuple] = ()
    project_rows: Callable | None = None


def capture_request_steps(steps, fetch, *, capture_root=None, byte_budget=None):
    """Drive native request steps, validating each response before advancing.

    Source generators yield declarations and receive their decoded result. The
    shared owner performs every request; metadata-dependent plans can refuse a
    source before any later request, preserving their original failure custody.
    """
    from contextlib import nullcontext
    from dagster_hifld.http_capture import capture_retention_budget

    scope = (capture_retention_budget(capture_root, byte_budget)
             if capture_root is not None else nullcontext())
    with scope:
        result = None
        while True:
            try:
                request = steps.send(result)
            except StopIteration as completed:
                return completed.value
            if isinstance(request, ExactIdCapturePlan):
                from dagster_hifld.sources.arcgis_raw_capture import capture_exact_id_population
                result = capture_exact_id_population(request.ids, request.page_size,
                    fetch_page=lambda offset, ids: fetch(*request.page_request(offset, ids)),
                    validate_page=request.validate_page,
                    read_page=request.read_page, project_rows=request.project_rows,
                    close_witnesses=lambda: capture_request_sequence({},
                        [(item[0], item[1:]) for item in request.closing_requests],
                        lambda captures: (), lambda name, args: fetch(name, *args)))
            else:
                result = fetch(*request)


def retry_capture_request(operation, *, attempts, schedule, retryable, preserve_failed):
    """Retry a native capture policy while retaining each refused response."""
    import time

    for attempt in range(1, attempts + 1):
        try:
            return operation(attempt)
        except Exception as exc:
            if attempt >= attempts or not retryable(exc, attempt):
                raise
            preserve_failed(exc, attempt)
            delay = schedule[min(attempt - 1, len(schedule) - 1)] if schedule else 0
            if delay:
                time.sleep(delay)
    raise AssertionError('unreachable_retry_state')


def quarantine_retry_response(storage, root, dataset_slug, version, name, reason):
    """Retain failed evidence outside a successful capture's replay namespace."""
    import json
    from pathlib import Path

    root = Path(root)
    receipt_path = root / f'{name}.receipt.json'
    body_path = root / f'{name}.body'
    if not receipt_path.is_file():
        return
    receipt = json.loads(receipt_path.read_bytes())
    body = body_path.read_bytes() if body_path.is_file() else b''
    capture_common.quarantine_incomplete_capture(storage, root, dataset_slug, version,
                                               {name: (body, receipt)}, reason)


def capture_validated_document(client, url, root, name, *, max_bytes, attempts,
                               complete, preserve_refusal, schedule,
                               retained_path=False, capture=None):
    """Capture bounded null/error documents under a source's bracket policy.

    Refusal callbacks retain the native error filename and return its body
    path. A disk-backed caller can then read the last refused source response
    without restoring or duplicating an active artifact that policy moved.
    """
    import time
    from pathlib import Path

    value = (b'', {})
    refused_path = None
    for attempt in range(attempts):
        value = (capture or capture_common.capture_get)(client, url, root, name,
                                                        max_bytes=max_bytes)
        if complete(value[0]):
            return (Path(root) / f'{name}.body', value[1]) if retained_path else value
        prior = set(Path(root).glob(f'{name}.error-*.body')) if retained_path else set()
        refused_path = preserve_refusal()
        if retained_path and refused_path is None:
            created = set(Path(root).glob(f'{name}.error-*.body')) - prior
            if len(created) != 1:
                raise ValueError('retained_refused_response_path_missing:' + name)
            refused_path = created.pop()
        if attempt + 1 < attempts:
            time.sleep(schedule[attempt] if attempt < len(schedule) else 0)
    return (refused_path, value[1]) if retained_path else value
