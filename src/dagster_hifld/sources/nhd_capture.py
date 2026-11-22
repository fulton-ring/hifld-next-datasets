"""Disk-backed acquisition for USGS NHD/3DHP roster and keyset protocols.

Adapters supply source URLs, schema validation and transport policies. This owner
plans retained gaps, bisects refused pages and commits acquisition checkpoints.
"""
from __future__ import annotations

import hashlib
import json
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx

from dagster_hifld.http_capture import capture_retention_budget, reserve_capture_retention
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView


@contextmanager
def _bounded_acquisition(capture_root: Path, byte_budget: int, error_type: type[Exception]):
    capture_root.mkdir(parents=True, exist_ok=True)
    try:
        with capture_retention_budget(capture_root, byte_budget):
            reserve_capture_retention(capture_root, 0)
            yield
    except ValueError as exc:
        if str(exc) == "source_capture_budget_exceeded":
            raise error_type("source_capture_budget_exceeded") from exc
        raise


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()

def plan_segment_gaps(capture_root: Path, roster: list[int], *,
                      load_state: Callable, read_capture: Callable, witness: Callable,
                      name_pattern, parse_name: Callable, segment_name: Callable,
                      producer_error: Callable, error_type: type[Exception],
                      chunk_size: int, strict_resume: bool) -> list[tuple[int, int]]:
    """Verify checkpointed coverage and return bounded, nonoverlapping gaps."""
    state = load_state(capture_root)
    accepted = {entry.get("name"): entry for entry in state["segments"] if isinstance(entry, dict)}
    retained: list[tuple[int, int]] = []
    for path in sorted(capture_root.glob("page_*.body")):
        name = path.name.removesuffix(".body")
        match = name_pattern.fullmatch(name)
        if match is None:
            # Witnessed refusal evidence (page_*.error-*.body) and other
            # non-canonical names are not roster segments; skip them.
            continue
        try:
            body, _receipt = read_capture(capture_root, name)
        except error_type:
            witness(capture_root, name)
            continue
        if (producer_error(body) if strict_resume else body.lstrip().startswith(b'{"error"')) or name not in accepted:
            witness(capture_root, name)
            continue
        offset, count = parse_name(name)
        if strict_resume:
            try:
                rows = json.loads(body).get("features")
                actual = [row.get("attributes", {}).get("OBJECTID") for row in rows]
            except (ValueError, TypeError, AttributeError):
                actual = None
            if (count < 1 or offset + count > len(roster)
                    or actual != roster[offset:offset + count]
                    or accepted[name].get("body_sha256") != _sha(body)):
                witness(capture_root, name)
                continue
        else:
            if offset + count > len(roster) or roster[offset:offset + count] != list(range(offset + 1, offset + count + 1)):
                witness(capture_root, name)
                continue
        retained.append((offset, count))
    gaps: list[tuple[int, int]] = []
    covered = bytearray(len(roster))
    for offset, count in sorted(retained):
        if offset < 0 or count < 1 or offset + count > len(roster) \
                or any(covered[offset:offset + count]):
            witness(capture_root, segment_name(offset, count))
            continue
        covered[offset:offset + count] = b"\x01" * count
    cursor = 0
    while cursor < len(roster):
        if covered[cursor]:
            cursor += 1
            continue
        start = cursor
        while cursor < len(roster) and not covered[cursor]:
            cursor += 1
        chunk_start = start
        while chunk_start < cursor:
            count_chunk = min(chunk_size, cursor - chunk_start)
            gaps.append((chunk_start, count_chunk))
            chunk_start += count_chunk
    return gaps

def fetch_segments(client: httpx.Client, capture_root: Path, roster: list[int],
                   state: dict[str, object], *, plan_gaps: Callable,
                   segment_name: Callable, segment_query: Callable,
                   capture_retry: Callable, witness: Callable,
                   producer_error: Callable, save_state: Callable,
                   error_type: type[Exception], probe_attempts: int,
                   doc_attempts: int, probe_delay: float, doc_delay: float,
                   max_body_bytes: int, strict_resume: bool) -> bool:
    """Acquire roster gaps, retaining refusals before adaptive bisection."""
    segments = state.setdefault("segments", [])
    todo: deque[tuple[int, int]] = deque(plan_gaps(capture_root, roster))
    refreshed = bool(todo)
    if refreshed and strict_resume:
        state["complete"] = False
        if segments:
            save_state(capture_root, state)
    accepted = {entry.get("name") for entry in segments if isinstance(entry, dict)}
    while todo:
        offset, count = todo.popleft()
        name = segment_name(offset, count)
        if not strict_resume and name in accepted and (capture_root / f"{name}.body").exists() and not (capture_root / f"{name}.body").read_bytes().lstrip().startswith(b'{"error"'):
            continue
        try:
            body, receipt = capture_retry(
                client, segment_query(offset, count), capture_root, name,
                attempts=probe_attempts if count > 1 else doc_attempts,
                base_delay=probe_delay if count > 1
                else doc_delay,
                max_bytes=max_body_bytes)
        except ValueError as exc:
            # The producer cap can also manifest as truncation/connection reset
            # instead of a clean error document; a multi-record segment whose
            # attempts were still incomplete is bisected like an error
            # document, while a single-record segment keeps failing explicitly.
            if count > 1 and str(exc).startswith("incomplete_source_capture:"):
                middle = count // 2
                todo.appendleft((offset + middle, count - middle))
                todo.appendleft((offset, middle))
                continue
            raise error_type(
                "source_capture_failed:" + type(exc).__name__) from exc
        if producer_error(body):
            witness(capture_root, name)
            if count == 1:
                raise error_type("source_single_feature_unservable")
            middle = count // 2
            todo.appendleft((offset + middle, count - middle))
            todo.appendleft((offset, middle))
            continue
        page = json.loads(body)
        if not isinstance(page, dict) or "error" in page:
            witness(capture_root, name)
            raise error_type("source_page_error:" + name)
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) != count:
            witness(capture_root, name)
            raise error_type("source_page_row_count_incomplete:" + name)
        expected = roster[offset:offset + count]
        actual = [row.get("attributes", {}).get("OBJECTID") for row in rows
                  if isinstance(row, dict)]
        if actual != expected:
            witness(capture_root, name)
            raise error_type("source_page_id_slice_incomplete_or_reordered")
        if strict_resume or name not in accepted:
            if strict_resume:
                segments[:] = [entry for entry in segments if entry.get("name") != name]
            segments.append({"name": name, "offset": offset, "count": count,
                             "features": len(rows), "captured_at": receipt["observed_at"],
                             "body_sha256": _sha(body)})
            accepted.add(name)
            save_state(capture_root, state)
    return refreshed


def capture_roster_layer(capture_root: Path, *, client, doc_client,
                         default_client: Callable, document_client: Callable,
                         load_state: Callable, save_state: Callable,
                         retained_or_fetch: Callable, read_capture: Callable,
                         capture_get: Callable, witness: Callable,
                         producer_error: Callable, validate_roster: Callable,
                         fetch_segments: Callable, metadata_names, after_names,
                         urls: dict[str, str], error_type: type[Exception],
                         max_body_bytes: int, refresh_witnesses: bool, byte_budget: int):
    """Own the before/page/after acquisition cycle and retained resume contract."""
    with _bounded_acquisition(capture_root, byte_budget, error_type):
        capture_root.mkdir(parents=True, exist_ok=True)
        state = load_state(capture_root)
        segments = list(state.get("segments", []))
        first = segments[0]["captured_at"] if segments else datetime.now(timezone.utc).isoformat()
        active = client or default_client()
        docs = doc_client or document_client()
        try:
            for name in metadata_names:
                body, _ = retained_or_fetch(docs if name == "service_doc" else active,
                                            capture_root, name, urls[name])
                if name != "service_doc" and producer_error(body):
                    raise error_type("source_page_error:" + name)
            ids = validate_roster(json.loads(read_capture(capture_root, "ids_seed")[0]).get("objectIds"), "ids_seed")
            before = json.loads(read_capture(capture_root, "ids_before")[0])
            if before.get("objectIdFieldName") != "OBJECTID":
                raise error_type("source_id_field_changed")
            roster = validate_roster(before.get("objectIds"), "ids_before")
            if sorted(roster) != ids:
                raise error_type("source_id_population_drift:ids_seed_vs_before")
            count = json.loads(read_capture(capture_root, "count_before")[0]).get("count")
            if type(count) is not int or (count != len(roster) if refresh_witnesses else count <= 0):
                raise error_type("source_count_drift")
            if not refresh_witnesses and len(roster) != count:
                raise error_type("source_count_or_id_roster_drift")
            refreshed = fetch_segments(active, capture_root, roster, state)
            segments = list(state["segments"])
            if refresh_witnesses:
                refresh = refreshed or state.get("complete") is not True
                if not refresh:
                    for name in after_names:
                        try:
                            document = json.loads(read_capture(capture_root, name)[0])
                            if not isinstance(document, dict) or "error" in document:
                                raise error_type("source_page_error:" + name)
                        except (OSError, ValueError):
                            refresh = True
                            break
                if refresh:
                    state["complete"] = False
                    save_state(capture_root, state)
                    for name in after_names:
                        witness(capture_root, name)
                    for name in after_names:
                        body, _ = capture_get(active, urls[name], capture_root, name, max_bytes=max_body_bytes)
                        if producer_error(body):
                            raise error_type("source_page_error:" + name)
            else:
                for name in after_names:
                    body, _ = retained_or_fetch(active, capture_root, name, urls[name])
                    if producer_error(body):
                        raise error_type("source_page_error:" + name)
            count_after = json.loads(read_capture(capture_root, "count_after")[0]).get("count")
            if type(count_after) is not int or count_after != count:
                raise error_type("source_metadata_or_count_changed_during_capture")
            after = json.loads(read_capture(capture_root, "ids_after")[0])
            if after.get("objectIdFieldName") != "OBJECTID" or sorted(validate_roster(after.get("objectIds"), "ids_after")) != (ids if refresh_witnesses else roster):
                raise error_type("source_id_population_drift:ids_after")
        except error_type:
            raise
        except Exception as exc:
            raise error_type("source_capture_failed:" + type(exc).__name__) from exc
        finally:
            if client is None:
                active.close()
            if doc_client is None:
                docs.close()
        if not refresh_witnesses:
            save_state(capture_root, {"segments": segments, "complete": True})
        last = max((str(entry.get("captured_at", "")) for entry in segments), default=first)
        return segments, count, first, last

def capture_keyset_layer(capture_root: Path, *, client, doc_client, alias_client,
                         load_state: Callable, save_state: Callable,
                         default_client: Callable, document_client: Callable,
                         retained_or_fetch: Callable, reject_error_response: Callable,
                         fetch_page: Callable, refuse_single_record: Callable,
                         validate_cursor: Callable, page_url: Callable,
                         page_name: Callable, read_capture: Callable,
                         metadata_urls: dict[str, str],
                         closing_urls: dict[str, str], error_type: type[Exception],
                         page_size: int, clear_rescued_refusal: bool, byte_budget: int):
    """Capture a resumable adaptive keyset walk, including refusal rescue and tail."""
    with _bounded_acquisition(capture_root, byte_budget, error_type):
        state = dict(load_state(capture_root))
        pages: list[dict[str, object]] = list(state.get("pages", []))
        tail: dict[str, object] | None = state.get("tail")
        refused: dict[str, object] = dict(state.get("refused", {}))

        def persist() -> None:
            save_state(capture_root, {"pages": pages, "tail": tail, "refused": refused,
                                       "complete": False})

        owner_client = client is None
        owner_doc = doc_client is None
        owner_alias = alias_client is None
        active = client or default_client()
        docs_active = doc_client or document_client()
        alias = alias_client or default_client()
        budget = [0]
        first_captured_at = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
        try:
            retained = capture_documents(
                active, docs_active, capture_root, metadata_urls,
                retained_or_fetch=retained_or_fetch, reject_error_response=reject_error_response,
                budget=budget)
            count_before = json.loads(retained["count_before"][0]).get("count")
            if type(count_before) is not int or count_before <= 0:
                raise error_type("source_count_drift")
            last_objectid = pages[-1]["last_objectid"] if pages else 0
            while not (capture_root / "page_tail.body").exists():
                size = page_size
                while True:
                    try:
                        body, receipt = fetch_page(active, capture_root, budget,
                                                    last_objectid, size)
                    except error_type as exc:
                        bisectable = (exc.code == "source_page_error"
                                      or exc.code.startswith("source_page_error:")
                                      or exc.code == "source_page_body_limit"
                                      or exc.code.startswith("source_page_body_limit:"))
                        if not bisectable:
                            raise
                        if size > 1:
                            size = bisect_page_size(size)
                            continue
                        oid, rescued = refuse_single_record(
                            active, alias, capture_root, budget, last_objectid, pages, persist)
                        if not rescued:
                            refused[str(oid)] = {"attrs_name": f"refused_attrs_{oid:09d}",
                                                 "captured_at": datetime.now(timezone.utc).isoformat()}
                            persist()
                        elif clear_rescued_refusal and refused.pop(str(oid), None) is not None:
                            # A record a producer host now serves must not keep a
                            # stale refusal entry from an earlier attempt: the
                            # refusal state has to match the actually retained
                            # walk (served record, no null-geometry gate).
                            persist()
                        last_objectid = oid
                        break
                    page = json.loads(body)
                    if not isinstance(page, dict) or "error" in page:
                        raise error_type("source_page_error")
                    features = page.get("features")
                    if not isinstance(features, list):
                        raise error_type("source_page_missing_features")
                    if not features:
                        if last_objectid == 0:
                            raise error_type("source_page_population_incomplete")
                        # The population ends here: retain the canonical empty tail
                        # page (same walk position, producer page size) as the
                        # completeness witness.
                        tail = capture_tail(
                            active, capture_root, page_url(last_objectid, page_size), last_objectid,
                            retained_or_fetch=retained_or_fetch, reject_error_response=reject_error_response,
                            error_type=error_type, budget=budget)
                        persist()
                        break
                    cursor = last_objectid
                    cursor = validate_cursor(features, cursor)
                    more = page.get("exceededTransferLimit")
                    if len(features) < size and more is True:
                        raise error_type("source_page_transfer_truncated")
                    pages.append({"name": page_name(last_objectid, size),
                                  "start_after": last_objectid, "size": size,
                                  "last_objectid": cursor, "features": len(features),
                                  "exceeded_transfer_limit": more,
                                  "captured_at": receipt["observed_at"],
                                  "body_sha256": _sha(body)})
                    last_objectid = cursor
                    persist()
                    if len(pages) > count_before:
                        raise error_type("source_page_count_exceeds_reported_count")
                    if len(features) < size and more is not True:
                        # The accepted short page is the final data page: retain the
                        # canonical empty tail witness at the producer page size.
                        tail = capture_tail(
                            active, capture_root, page_url(last_objectid, page_size), last_objectid,
                            retained_or_fetch=retained_or_fetch, reject_error_response=reject_error_response,
                            error_type=error_type, budget=budget)
                        persist()
                        break
                    break
            if tail is None and (capture_root / "page_tail.body").exists():
                tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
                tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                        "captured_at": tail_receipt["observed_at"]}
            if tail is None or not (capture_root / "page_tail.body").exists():
                raise error_type("source_tail_page_not_empty")
            for name, url in closing_urls.items():
                body, _receipt = retained_or_fetch(active, capture_root, name, url, budget=budget)
                reject_error_response(capture_root, name, body)
                retained[name] = (body, read_capture(capture_root, name)[1])
            if json.loads(retained["count_after"][0]).get("count") != count_before:
                raise error_type("source_metadata_or_count_changed_during_capture")
        except error_type:
            close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)
            raise
        except Exception as exc:
            close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)
            raise error_type("source_capture_failed:" + type(exc).__name__) from exc
        close_owned(owner_client, owner_doc, owner_alias, active, docs_active, alias)

        return pages, tail, refused, first_captured_at


def capture_fixed_keyset_layer(capture_root: Path, *, client, doc_client,
                               load_state: Callable, save_state: Callable,
                               default_client: Callable, document_client: Callable,
                               retained_or_fetch: Callable, reject_error_response: Callable,
                               witness: Callable, validate_cursor: Callable,
                               page_url: Callable, read_capture: Callable,
                               metadata_urls: dict[str, str], closing_urls: dict[str, str],
                               error_type: type[Exception], page_size: int,
                               validate_before: Callable | None = None,
                               page_count_error: str = "source_page_count_exceeds_reported_count"):
    """Capture the fixed-size keyset protocol with its original data-page identities."""
    state = load_state(capture_root)
    pages: list[dict[str, object]] = list(state.get("data_pages", []))
    tail: dict[str, object] | None = state.get("tail")
    owner_client = client is None
    owner_doc = doc_client is None
    active = client or default_client()
    docs_active = doc_client or document_client()
    first_captured_at = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
    try:
        retained = capture_documents(
            active, docs_active, capture_root, metadata_urls,
            retained_or_fetch=retained_or_fetch, reject_error_response=reject_error_response)
        if validate_before is not None:
            count_before = validate_before(retained)
        else:
            count_before = json.loads(retained["count_before"][0]).get("count")
            if type(count_before) is not int or count_before <= 0:
                raise error_type("source_count_drift")
        last_objectid = pages[-1]["last_objectid"] if pages else 0
        while not (capture_root / "page_tail.body").exists():
            name = f"page_{len(pages):06d}"
            body, receipt = retained_or_fetch(active, capture_root, name, page_url(last_objectid))
            page = json.loads(body)
            if not isinstance(page, dict) or "error" in page:
                witness(capture_root, name)
                raise error_type("source_page_error")
            features = page.get("features")
            if not isinstance(features, list):
                raise error_type("source_page_missing_features")
            if not features:
                if last_objectid == 0:
                    raise error_type("source_page_population_incomplete")
                # An empty page in a data slot is the completeness witness: the same
                # query URL defines page_tail, so retain its exact bytes under that name.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").write_bytes(
                        (capture_root / f"{name}.{suffix}").read_bytes())
                break
            last_objectid = validate_cursor(features, last_objectid)
            more = page.get("exceededTransferLimit")
            if len(features) < page_size and more is True:
                raise error_type("source_page_transfer_truncated")
            pages.append({"name": name, "last_objectid": last_objectid, "features": len(features),
                          "exceeded_transfer_limit": more, "captured_at": receipt["observed_at"],
                          "body_sha256": _sha(body)})
            save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
            if len(pages) > (count_before + page_size - 1) // page_size:
                raise error_type(page_count_error)
            if len(features) < page_size or more is False:
                tail = capture_tail(
                    active, capture_root, page_url(last_objectid), last_objectid,
                    retained_or_fetch=retained_or_fetch, reject_error_response=reject_error_response,
                    error_type=error_type)
                save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
                break
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise error_type("source_tail_page_not_empty")
        for name, url in closing_urls.items():
            body, _receipt = retained_or_fetch(active, capture_root, name, url)
            reject_error_response(capture_root, name, body)
            retained[name] = (body, read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise error_type("source_metadata_or_count_changed_during_capture")
    except error_type:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise
    except Exception as exc:
        if owner_client:
            active.close()
        if owner_doc:
            docs_active.close()
        raise error_type("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()

    return pages, tail, first_captured_at


def retained_capture(client, capture_root: Path, name: str, url: str, *,
                     read_capture: Callable, fetch: Callable, discard: Callable,
                     error_type: type[Exception]):
    """Reuse receipt-verified bytes; discard invalid captures only when acquisition is allowed."""
    if (capture_root / f"{name}.body").exists():
        try:
            return read_capture(capture_root, name)
        except error_type:
            if client is None:
                raise
            discard(capture_root, name)
    if client is None:
        raise error_type("retained_capture_file_missing:" + name)
    return fetch(client, url, capture_root, name)


def discard_capture(capture_root: Path, name: str) -> None:
    """Remove a corrupt, unverifiable capture before replacing it."""
    for suffix in ("body", "wire", "receipt.json"):
        (capture_root / f"{name}.{suffix}").unlink(missing_ok=True)


def capture_documents(active, docs, capture_root: Path, urls: dict[str, str], *,
                      retained_or_fetch: Callable, reject_error_response: Callable,
                      budget: list[int] | None = None):
    """Retain a document roster with the separate producer-document transport."""
    retained = {}
    for name, url in urls.items():
        kwargs = {} if budget is None else {"budget": budget}
        retained[name] = retained_or_fetch(docs if name == "service_doc" else active,
                                           capture_root, name, url, **kwargs)
        if name != "service_doc":
            reject_error_response(capture_root, name, retained[name][0])
    return retained


def capture_tail(active, capture_root: Path, url: str, last_objectid: int, *,
                 retained_or_fetch: Callable, reject_error_response: Callable,
                 error_type: type[Exception], budget: list[int] | None = None):
    """Retain and validate the canonical empty keyset completeness witness."""
    kwargs = {} if budget is None else {"budget": budget}
    body, receipt = retained_or_fetch(active, capture_root, "page_tail", url, **kwargs)
    reject_error_response(capture_root, "page_tail", body)
    if json.loads(body).get("features") != []:
        raise error_type("source_tail_page_not_empty")
    return {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
            "captured_at": receipt["observed_at"], "body_sha256": _sha(body)}

def capture_id_windows(client: httpx.Client, roster: list[int], capture_root: Path,
                       budget: list[int], *, page_size: int, attempts: int,
                       base_delay: float, max_body_bytes: int, window_url: Callable,
                       budget_check: Callable, budget_guard: Callable,
                       failed_receipt_error_code: Callable,
                       preserve_failed_attempt: Callable, producer_error_doc: Callable,
                       fetch: Callable, sleep: Callable):
    """Capture exact-ID windows, bisecting refusal/limits down to retained refused IDs.

    The source's refusal policy retains a single refused record for a separate
    attributes-only capture; it never silently removes it from the population.
    """
    pages = RetainedCaptureView(capture_root)
    refused: dict[str, list[int]] = {}
    plan: deque[tuple[int, int]] = deque((start, page_size) for start in range(0, len(roster), page_size))
    while plan:
        start, size = plan.popleft()
        window_ids = roster[start:start + size]
        size = len(window_ids)
        name = f"page_{window_ids[0]:07d}"
        url = window_url(window_ids)
        accepted = False
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            budget_check(budget)
            try:
                body, receipt = fetch(client, url, capture_root, name,
                                            max_bytes=max_body_bytes,
                                            before_retain=budget_guard(budget))
            except ValueError as exc:
                if "source_capture_budget_exceeded" in str(exc):
                    raise
                last_error = exc
                error_code = failed_receipt_error_code(capture_root, name)
                # Every failed attempt — including the terminal one before a
                # bisection — is preserved under its collision-free evidence
                # name first, so the next child capture cannot overwrite the
                # producer's actual response at the shared base paths.
                preserve_failed_attempt(capture_root, name, attempt)
                if error_code in ("wire_limit_exceeded", "decoded_limit_exceeded"):
                    break
                if attempt < attempts:
                    sleep(base_delay * (3 ** (attempt - 1)))
                    continue
                break
            if producer_error_doc(body):
                last_error = ValueError("source_window_error_document")
                preserve_failed_attempt(capture_root, name, attempt)
                if size > 1:
                    break
                if attempt < attempts:
                    sleep(base_delay * (3 ** (attempt - 1)))
                    continue
                break
            doc = json.loads(body)
            if doc.get("exceededTransferLimit"):
                last_error = ValueError("source_window_transfer_truncated")
                preserve_failed_attempt(capture_root, name, attempt)
                break
            pages[name] = (body, receipt)
            accepted = True
            break
        if accepted:
            continue
        if size > 1:
            mid = size // 2
            plan.appendleft((start + mid, size - mid))
            plan.appendleft((start, mid))
            continue
        refused[name] = window_ids
    return pages, refused

def bisect_page_size(size: int) -> int:
    if size <= 1:
        raise AssertionError("bisect_below_single_record")
    return max(1, size // 2)


def close_owned(owner_client: bool, owner_doc: bool, owner_alias: bool,
                active: httpx.Client, docs_active: httpx.Client, alias: httpx.Client) -> None:
    if owner_client:
        active.close()
    if owner_doc:
        docs_active.close()
    if owner_alias:
        alias.close()


def load_keyset_state(capture_root: Path, *, error_type: type[Exception],
                      pages_key: str, with_refused: bool = False) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        state = {pages_key: [], "tail": None, "complete": False}
        if with_refused:
            state["refused"] = {}
        return state
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error_type("capture_state_invalid") from exc
    if (not isinstance(state, dict) or not isinstance(state.get(pages_key), list)
            or not (state.get("tail") is None or isinstance(state.get("tail"), dict))
            or (with_refused and not isinstance(state.get("refused"), dict))):
        raise error_type("capture_state_invalid")
    return state

def capture_id_window_layer(client, capture_root: Path, budget: list[int], captures, *,
                            document_urls: list[tuple[str, str]], ids_url: str,
                            capture_doc: Callable, capture_windows: Callable,
                            capture_refused_attrs: Callable, error_type: type[ValueError]) -> None:
    """Bracket exact-ID acquisition and refused-record attributes with producer witnesses."""
    for name, url in document_urls:
        captures[name] = capture_doc(client, url, capture_root, name, budget)
    seed_body, seed_receipt = capture_doc(client, ids_url, capture_root, "ids_seed", budget)
    captures["ids_seed"] = (seed_body, seed_receipt)
    count = json.loads(captures["count_before"][0]).get("count")
    if type(count) is not int or count <= 0:
        raise error_type("source_count_drift")
    seed = json.loads(seed_body).get("objectIds")
    if (not isinstance(seed, list) or len(seed) != count
            or any(type(value) is not int or value <= 0 for value in seed)
            or len(set(seed)) != count):
        raise error_type("source_seed_id_population_drift")
    pages, refused = capture_windows(client, sorted(seed), capture_root, budget)
    captures.update(pages)
    capture_refused_attrs(client, capture_root, budget, captures, refused)
    for name, url in document_urls[2:]:
        captures[name.replace("_before", "_after")] = capture_doc(
            client, url, capture_root, name.replace("_before", "_after"), budget)
