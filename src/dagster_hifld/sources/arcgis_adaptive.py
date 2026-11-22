"""ArcGIS adaptive paging, retained checkpoints and closing witnesses."""
from __future__ import annotations

import json
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx

from dagster_hifld.sources.esri_common import sha
from dagster_hifld.http_capture import capture_retention_budget


@dataclass(frozen=True, kw_only=True)
class AdaptiveCaptureSpec:
    """Layer policy and retained-evidence operations for the shared acquisition."""

    service_url: str
    layer_url: str
    count_url: str
    ids_url: str
    metadata_names: tuple[str, ...]
    after_names: tuple[str, ...]
    page_size: int
    byte_budget: int
    page_pattern: re.Pattern
    page_name: Callable[[int, int], str]
    parse_page_name: Callable[[str], tuple[int, int]]
    page_url: Callable[[list[int], int, int], str]
    page_matches: Callable[[bytes, int, int, list[int]], bool]
    validate_roster: Callable[[object, str], list[int]]
    load_state: Callable[[Path], dict]
    save_state: Callable[[Path, dict], None]
    read_capture: Callable[[Path, str], tuple[bytes, dict]]
    witness_aside: Callable[[Path, str], None]
    capture: Callable[..., tuple[bytes, dict]]
    producer_error: Callable[[bytes], bool]
    default_client: Callable[[], httpx.Client]
    error: type[ValueError]
    page_attempts: int
    page_delay: float
    doc_attempts: int
    doc_delay: float
    reconcile_invalidated_pages: bool = False
    metadata_urls: dict[str, str] | None = None
    closing_urls: dict[str, str] | None = None
    strict_count_type: bool = False
    refresh_closing_after_pages: bool = False
    capture_closing: Callable[..., tuple[bytes, dict]] | None = None


@dataclass(frozen=True)
class AdaptiveCaptureResult:
    root: Path
    roster: list[int]
    pages: list[dict]
    first_captured_at: str
    last_captured_at: str


def _forget_page(spec: AdaptiveCaptureSpec, root: Path, state: dict,
                 accepted: set, name: str) -> None:
    if not spec.reconcile_invalidated_pages:
        return
    pages = state.get("pages")
    if not isinstance(pages, list):
        return
    pruned = [entry for entry in pages
              if not (isinstance(entry, dict) and entry.get("name") == name)]
    if len(pruned) == len(pages):
        return
    pages[:] = pruned
    accepted.discard(name)
    spec.save_state(root, state)


def plan_page_gaps(spec: AdaptiveCaptureSpec, root: Path, roster: list[int],
                   state: dict) -> list[tuple[int, int]]:
    """Reuse verified accepted slices and plan the uncovered roster positions."""
    pages = state["pages"]
    accepted = {entry.get("name") for entry in pages if isinstance(entry, dict)}
    retained = []
    present = set()
    for path in sorted(root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        if spec.page_pattern.fullmatch(name) is None:
            continue
        present.add(name)
        try:
            body, _receipt = spec.read_capture(root, name)
        except spec.error:
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            continue
        if body.lstrip().startswith(b'{"error"') or name not in accepted:
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            continue
        offset, count = spec.parse_page_name(name)
        if offset + count > len(roster):
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            continue
        retained.append((offset, count))
    if spec.reconcile_invalidated_pages:
        for name in [entry.get("name") for entry in pages
                     if isinstance(entry, dict) and entry.get("name") not in present]:
            _forget_page(spec, root, state, accepted, name)
    covered = bytearray(len(roster))
    for offset, count in sorted(retained):
        if offset < 0 or count < 1 or offset + count > len(roster) \
                or any(covered[offset:offset + count]):
            name = spec.page_name(offset, count)
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            continue
        covered[offset:offset + count] = b"\x01" * count
    return uncovered_ranges(covered, spec.page_size)


@dataclass(frozen=True, kw_only=True)
class SliceCaptureSpec:
    """Request, retry and evidence policy for bisectable position ranges."""

    byte_budget: int
    page_name: Callable[[int, int], str]
    page_url: Callable[[int, int], str]
    capture: Callable[..., tuple[bytes, dict]]
    producer_error: Callable[[bytes], bool]
    page_attempts: int
    page_delay: float
    doc_attempts: int
    doc_delay: float
    error: type[ValueError] = ValueError
    wrap_capture_errors: bool = False
    bisect_incomplete: bool = True


def capture_slices(spec: SliceCaptureSpec, client: httpx.Client, root: Path,
                   ranges, *, skip=None, refused=None, incomplete=None,
                   accept=None) -> tuple[dict, bool]:
    with capture_retention_budget(root, spec.byte_budget):
        return _capture_slices(spec, client, root, ranges, skip=skip,
            refused=refused, incomplete=incomplete, accept=accept)


def _capture_slices(spec: SliceCaptureSpec, client: httpx.Client, root: Path,
                    ranges, *, skip=None, refused=None, incomplete=None,
                    accept=None) -> tuple[dict, bool]:
    """Capture ordered ranges, bisect transient refusals and retain singleton errors.

    Evidence and checkpoint callbacks run before replacement children are queued.
    Source row/geometry contracts remain in the acceptance callback.
    """
    todo = deque(ranges)
    captures = {}
    refreshed = False
    while todo:
        offset, count = todo.popleft()
        name = spec.page_name(offset, count)
        if skip is not None and skip(name):
            continue
        try:
            body, receipt = spec.capture(
                client, spec.page_url(offset, count), root, name,
                attempts=spec.page_attempts if count > 1 else spec.doc_attempts,
                base_delay=spec.page_delay if count > 1 else spec.doc_delay)
        except ValueError as exc:
            if spec.bisect_incomplete and count > 1 and str(exc).startswith("incomplete_source_capture:"):
                if incomplete is not None:
                    incomplete(name)
            else:
                if spec.wrap_capture_errors:
                    raise spec.error("source_capture_failed:" + type(exc).__name__) from exc
                raise
        else:
            if not spec.producer_error(body):
                if accept is None:
                    captures[name] = (body, receipt)
                    refreshed = True
                else:
                    refreshed = accept(name, offset, count, body, receipt) or refreshed
                continue
            if refused is not None:
                refused(name)
            if count == 1:
                raise spec.error("source_single_feature_unservable")
        middle = count // 2
        todo.appendleft((offset + middle, count - middle))
        todo.appendleft((offset, middle))
    return captures, refreshed


def capture_offset_pages(spec: SliceCaptureSpec, client: httpx.Client, root: Path,
                         total: int, *, page_size: int) -> dict:
    """Capture a complete offset universe with adaptive refusal bisection."""
    ranges = ((offset, min(page_size, total - offset))
              for offset in range(0, total, page_size))
    with capture_retention_budget(root, spec.byte_budget):
        captures, _refreshed = capture_slices(spec, client, root, ranges)
    return captures


def capture_pages(spec: AdaptiveCaptureSpec, client: httpx.Client, root: Path,
                  roster: list[int], state: dict) -> bool:
    """Capture resumable roster slices and persist every accepted checkpoint."""
    pages = state.setdefault("pages", [])
    gaps = plan_page_gaps(spec, root, roster, state)
    accepted = {entry.get("name") for entry in pages if isinstance(entry, dict)}

    def skip(name):
        path = root / f"{name}.body"
        return name in accepted and path.exists() and not path.read_bytes().lstrip().startswith(b'{"error"')

    def refused(name):
        spec.witness_aside(root, name)
        _forget_page(spec, root, state, accepted, name)

    def accept(name, offset, count, body, receipt):
        if not spec.page_matches(body, offset, count, roster):
            refused(name)
            raise spec.error("source_page_id_slice_incomplete_or_reordered")
        if name in accepted:
            return False
        pages.append({"name": name, "offset": offset, "count": count,
                      "features": count, "captured_at": receipt["observed_at"],
                      "body_sha256": sha(body)})
        accepted.add(name)
        spec.save_state(root, state)
        return True

    _captures, refreshed = capture_slices(
        SliceCaptureSpec(byte_budget=spec.byte_budget, page_name=spec.page_name,
            page_url=lambda offset, count: spec.page_url(roster, offset, count),
            capture=spec.capture, producer_error=spec.producer_error,
            page_attempts=spec.page_attempts, page_delay=spec.page_delay,
            doc_attempts=spec.doc_attempts, doc_delay=spec.doc_delay,
            error=spec.error, wrap_capture_errors=True),
        client, root, gaps, skip=skip, refused=refused,
        incomplete=refused if spec.reconcile_invalidated_pages else None,
        accept=accept)
    return refreshed


def _retained_or_fetch(spec: AdaptiveCaptureSpec, client: httpx.Client, root: Path,
                       name: str, url: str) -> tuple[bytes, dict]:
    if (root / f"{name}.body").exists():
        try:
            return spec.read_capture(root, name)
        except spec.error:
            spec.witness_aside(root, name)
    return spec.capture(client, url, root, name, attempts=spec.doc_attempts,
                        base_delay=spec.doc_delay)


def capture_adaptive_layer(spec: AdaptiveCaptureSpec, capture_root: Path, *,
                           client: httpx.Client | None = None) -> AdaptiveCaptureResult:
    with capture_retention_budget(capture_root, spec.byte_budget):
        return _capture_adaptive_layer(spec, capture_root, client=client)


def _capture_adaptive_layer(spec: AdaptiveCaptureSpec, capture_root: Path, *,
                            client: httpx.Client | None = None) -> AdaptiveCaptureResult:
    """Acquire discovery, resumable pages and producer closing witnesses."""
    root = Path(capture_root).absolute()
    root.mkdir(parents=True, exist_ok=True)
    state = spec.load_state(root)
    pages = list(state.get("pages", []))
    first = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
    active = client or spec.default_client()
    before_urls = {"service_before": spec.service_url + "?f=pjson",
                   "layer_before": spec.layer_url + "?f=pjson",
                   "count_before": spec.count_url, "ids_seed": spec.ids_url,
                   "ids_before": spec.ids_url}
    after_urls = {"layer_after": spec.layer_url + "?f=pjson",
                  "service_after": spec.service_url + "?f=pjson",
                  "count_after": spec.count_url, "ids_after": spec.ids_url}
    before_urls.update(spec.metadata_urls or {})
    after_urls.update(spec.closing_urls or {})
    try:
        for name in spec.metadata_names:
            body, _receipt = _retained_or_fetch(spec, active, root, name, before_urls[name])
            if spec.producer_error(body):
                raise spec.error("source_page_error:" + name)
        seed = json.loads(spec.read_capture(root, "ids_seed")[0]).get("objectIds")
        ids = spec.validate_roster(seed, "ids_seed")
        before = json.loads(spec.read_capture(root, "ids_before")[0])
        if before.get("objectIdFieldName") != "OBJECTID":
            raise spec.error("source_id_field_changed")
        roster = spec.validate_roster(before.get("objectIds"), "ids_before")
        if roster != ids:
            raise spec.error("source_id_population_drift:ids_seed_vs_before")
        count_before = json.loads(spec.read_capture(root, "count_before")[0]).get("count")
        if count_before != len(roster) or (spec.strict_count_type and type(count_before) is not int):
            raise spec.error("source_count_drift")
        refreshed = capture_pages(spec, active, root, roster, state)
        pages = list(state["pages"])
        refresh_closing = False
        if spec.refresh_closing_after_pages:
            refresh_closing = refreshed or state.get("complete") is not True
            if not refresh_closing:
                for name in spec.after_names:
                    try:
                        body, _receipt = spec.read_capture(root, name)
                        document = json.loads(body)
                        if not isinstance(document, dict) or "error" in document:
                            raise spec.error("source_page_error:" + name)
                    except (OSError, ValueError):
                        refresh_closing = True
                        break
            if refresh_closing:
                state["complete"] = False
                spec.save_state(root, state)
                for name in spec.after_names:
                    spec.witness_aside(root, name)
        for name in spec.after_names:
            if spec.refresh_closing_after_pages and not refresh_closing:
                continue
            if refresh_closing and spec.capture_closing is not None:
                body, _receipt = spec.capture_closing(active, after_urls[name], root, name)
            else:
                body, _receipt = _retained_or_fetch(spec, active, root, name, after_urls[name])
            if spec.producer_error(body):
                raise spec.error("source_page_error:" + name)
        count_after = json.loads(spec.read_capture(root, "count_after")[0]).get("count")
        if count_after != len(roster) or (spec.strict_count_type and type(count_after) is not int):
            raise spec.error("source_metadata_or_count_changed_during_capture")
        after = json.loads(spec.read_capture(root, "ids_after")[0])
        if after.get("objectIdFieldName") != "OBJECTID" \
                or spec.validate_roster(after.get("objectIds"), "ids_after") != roster:
            raise spec.error("source_id_population_drift:ids_after")
    except spec.error:
        raise
    except Exception as exc:
        raise spec.error("source_capture_failed:" + type(exc).__name__) from exc
    finally:
        if client is None:
            active.close()
    spec.save_state(root, {"pages": pages, "complete": True})
    last = max((str(entry.get("captured_at", "")) for entry in pages), default=first)
    return AdaptiveCaptureResult(root, roster, pages, first, last)


def plan_verified_segments(root: Path, roster: list[int], state: dict, *,
                           read_capture, load_receipt, witness, name_pattern,
                           parse_name, segment_name, page_ids, error_type,
                           admission_error, save_state, page_size: int) -> list[tuple[int, int]]:
    """Reconcile segment checkpoints against receipt-bound decoded populations."""
    accepted = {entry.get("name"): entry for entry in state["segments"]
                if isinstance(entry, dict)}
    for path in (*root.glob("page_*.wire"), *root.glob("page_*.receipt.json")):
        name = path.name.removesuffix(".wire").removesuffix(".receipt.json")
        if name_pattern.fullmatch(name) and not (root / f"{name}.body").exists():
            witness(root, name)
    retained = []
    for path in sorted(root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        if name_pattern.fullmatch(name) is None:
            continue
        try:
            body, receipt = read_capture(root, name)
        except admission_error:
            raise
        except error_type:
            witness(root, name)
            continue
        entry = accepted.get(name)
        if (entry is None or entry.get("body_sha256") != receipt["sha256"]
                or entry.get("captured_at") != receipt["observed_at"]):
            witness(root, name)
            continue
        try:
            ids = page_ids(body)
        except ValueError:
            witness(root, name)
            continue
        offset, count = parse_name(name)
        if offset + count > len(roster) or ids != roster[offset:offset + count]:
            witness(root, name)
            continue
        retained.append((offset, count))
    covered = bytearray(len(roster))
    segments = []
    for offset, count in sorted(retained):
        name = segment_name(offset, count)
        if offset < 0 or count < 1 or offset + count > len(roster) \
                or any(covered[offset:offset + count]):
            witness(root, name)
            continue
        covered[offset:offset + count] = b"\x01" * count
        receipt = load_receipt(root, name)
        segments.append({"name": name, "offset": offset, "count": count,
                         "features": count, "captured_at": receipt["observed_at"],
                         "body_sha256": receipt["sha256"]})
    state["segments"] = segments
    save_state(root, state)
    return uncovered_ranges(covered, page_size)


def uncovered_ranges(covered: bytearray, page_size: int) -> list[tuple[int, int]]:
    """Partition uncovered positions into bounded ascending ranges."""
    gaps = []
    cursor = 0
    while cursor < len(covered):
        if covered[cursor]:
            cursor += 1
            continue
        start = cursor
        while cursor < len(covered) and not covered[cursor]:
            cursor += 1
        while start < cursor:
            count = min(page_size, cursor - start)
            gaps.append((start, count))
            start += count
    return gaps


def capture_checkpoint_segments(spec: SliceCaptureSpec, client: httpx.Client,
                                root: Path, roster: list[int], state: dict, *,
                                plan_gaps, validate_page, witness, save_state) -> None:
    """Capture planned segment gaps through the shared bisection/checkpoint engine."""
    gaps = plan_gaps(root, roster, state)
    segments = state["segments"]
    accepted = {entry["name"] for entry in segments}

    def accept(name, offset, count, body, receipt):
        validate_page(body, name, count, roster[offset:offset + count])
        if name in accepted:
            return False
        segments.append({"name": name, "offset": offset, "count": count,
                         "features": count, "captured_at": receipt["observed_at"],
                         "body_sha256": sha(body)})
        accepted.add(name)
        save_state(root, state)
        return True

    capture_slices(spec, client, root, gaps,
        skip=lambda name: name in accepted and (root / f"{name}.body").exists(),
        refused=lambda name: witness(root, name), accept=accept)


def capture_document_group(client: httpx.Client, root: Path, requests, *, fetch,
                           captures: dict | None = None, retained_bytes: int = 0,
                           max_total_bytes: int | None = None,
                           client_for=None, validate=None) -> tuple[dict, int]:
    """Acquire ordered documents and update the native receipt-byte ledger."""
    captures = {} if captures is None else captures
    for name, url in requests:
        active = client if client_for is None else client_for(name)
        body, receipt = fetch(active, url, root, name)
        captures[name] = (body, receipt)
        if max_total_bytes is not None:
            retained_bytes += receipt["bytes"] + receipt["wire_bytes"]
            if retained_bytes > max_total_bytes:
                raise ValueError("source_capture_exceeds_scratch_budget")
        if validate is not None:
            validate(name, body)
    return captures, retained_bytes


def capture_offset_documents(spec: SliceCaptureSpec, client: httpx.Client, root: Path,
                             requests, captures: dict, *, retained_bytes: int = 0,
                             max_total_bytes: int | None = None) -> int:
    """Use the layer's native document retry policy and receipt ledger."""
    def fetch(client, url, root, name):
        return spec.capture(client, url, root, name,
                            attempts=spec.doc_attempts, base_delay=spec.doc_delay)

    _captures, retained_bytes = capture_document_group(client, root, requests,
        fetch=fetch, captures=captures, retained_bytes=retained_bytes,
        max_total_bytes=max_total_bytes)
    return retained_bytes
