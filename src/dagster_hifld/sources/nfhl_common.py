"""Shared capture-retry primitives for the FEMA NFHL MapServer layer adapters.

Every NFHL layer adapter captures the producer's REST documents through
``capture_with_retry``: a bounded retry against the producer's transient
complete-200 Esri error documents and incomplete transport captures, with each
failed attempt preserved under ``<name>.attempt-NN`` evidence names that never
enter the capture roster. Layer-specific capture rosters, validation, and
candidate construction stay in the adapter modules, which import these helpers
and pass their own retry budgets.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import httpx

from dagster_hifld.sources.capture_common import capture_get

# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or transport failures (observed 2026-11-12
# while probing these layers; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact page id slices, and
# full-record digests, so retries can never mask drift.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0


def producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster. The
    final attempt's body is always returned as captured, so a persistent
    producer failure surfaces to the caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if producer_error_doc(body) and attempt < attempts:
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")
