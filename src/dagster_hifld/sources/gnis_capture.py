"""Resumable GNIS layer discovery, roster paging and closing witnesses."""
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


@dataclass(frozen=True, kw_only=True)
class GnisCaptureSpec:
    """Layer policy and retained-evidence operations for the shared acquisition."""

    service_url: str
    layer_url: str
    count_url: str
    ids_url: str
    metadata_names: tuple[str, ...]
    after_names: tuple[str, ...]
    page_size: int
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


@dataclass(frozen=True)
class GnisCaptureResult:
    root: Path
    roster: list[int]
    pages: list[dict]
    first_captured_at: str
    last_captured_at: str


def _forget_page(spec: GnisCaptureSpec, root: Path, state: dict,
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


def plan_page_gaps(spec: GnisCaptureSpec, root: Path, roster: list[int],
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
    gaps = []
    cursor = 0
    while cursor < len(roster):
        if covered[cursor]:
            cursor += 1
            continue
        start = cursor
        while cursor < len(roster) and not covered[cursor]:
            cursor += 1
        while start < cursor:
            count = min(spec.page_size, cursor - start)
            gaps.append((start, count))
            start += count
    return gaps


def capture_pages(spec: GnisCaptureSpec, client: httpx.Client, root: Path,
                  roster: list[int], state: dict) -> None:
    """Bisect refused slices, preserving evidence and accepted-page checkpoints."""
    pages = state.setdefault("pages", [])
    todo = deque(plan_page_gaps(spec, root, roster, state))
    accepted = {entry.get("name") for entry in pages if isinstance(entry, dict)}
    while todo:
        offset, count = todo.popleft()
        name = spec.page_name(offset, count)
        path = root / f"{name}.body"
        if name in accepted and path.exists() \
                and not path.read_bytes().lstrip().startswith(b'{"error"'):
            continue
        url = spec.page_url(roster, offset, count)
        try:
            body, receipt = spec.capture(
                client, url, root, name,
                attempts=spec.page_attempts if count > 1 else spec.doc_attempts,
                base_delay=spec.page_delay if count > 1 else spec.doc_delay)
        except ValueError as exc:
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                if spec.reconcile_invalidated_pages:
                    spec.witness_aside(root, name)
                    _forget_page(spec, root, state, accepted, name)
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise spec.error("source_capture_failed:" + type(exc).__name__) from exc
        if spec.producer_error(body):
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            if count == 1:
                raise spec.error("source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        if not spec.page_matches(body, offset, count, roster):
            spec.witness_aside(root, name)
            _forget_page(spec, root, state, accepted, name)
            raise spec.error("source_page_id_slice_incomplete_or_reordered")
        if name not in accepted:
            pages.append({"name": name, "offset": offset, "count": count,
                          "features": count, "captured_at": receipt["observed_at"],
                          "body_sha256": sha(body)})
            accepted.add(name)
            spec.save_state(root, state)


def _retained_or_fetch(spec: GnisCaptureSpec, client: httpx.Client, root: Path,
                       name: str, url: str) -> tuple[bytes, dict]:
    if (root / f"{name}.body").exists():
        try:
            return spec.read_capture(root, name)
        except spec.error:
            spec.witness_aside(root, name)
    return spec.capture(client, url, root, name, attempts=spec.doc_attempts,
                        base_delay=spec.doc_delay)


def capture_gnis_layer(spec: GnisCaptureSpec, capture_root: Path, *,
                       client: httpx.Client | None = None) -> GnisCaptureResult:
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
        if json.loads(spec.read_capture(root, "count_before")[0]).get("count") != len(roster):
            raise spec.error("source_count_drift")
        capture_pages(spec, active, root, roster, state)
        pages = list(state["pages"])
        for name in spec.after_names:
            body, _receipt = _retained_or_fetch(spec, active, root, name, after_urls[name])
            if spec.producer_error(body):
                raise spec.error("source_page_error:" + name)
        if json.loads(spec.read_capture(root, "count_after")[0]).get("count") != len(roster):
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
    return GnisCaptureResult(root, roster, pages, first, last)
