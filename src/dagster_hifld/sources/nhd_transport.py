"""Budgeted transport, retained evidence and producer-host rescue for NHD/3DHP.

Source schemas and endpoint selection stay in adapters. These operations own
attempt lifecycles and promote exact producer bytes into accepted page slots.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable

import httpx

from dagster_hifld.sources import arcgis_layer
from dagster_hifld.http_capture import reserve_capture_retention


def budget_guard(budget: list[int], limit: int, error_type: type[ValueError]):
    def before_retain(kind: str, data: bytes) -> None:
        budget[0] += len(data)
        budget_check(budget, limit, error_type)
    return before_retain


def budget_check(budget: list[int], limit: int, error_type: type[ValueError]) -> None:
    if budget[0] > limit:
        raise error_type("source_capture_budget_exceeded")


def failed_receipt_error_code(capture_root: Path, name: str) -> str | None:
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, ValueError):
        return None
    code = receipt.get("error_code")
    return code if isinstance(code, str) else None


def reject_error_response(capture_root: Path, name: str, body: bytes, *,
                          witness: Callable, error_type: type[ValueError]) -> None:
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return
    if isinstance(document, dict) and "error" in document:
        witness(capture_root, name)
        raise error_type("source_page_error:" + name)


def capture_document(client, url: str, capture_root: Path, name: str, budget: list[int], *,
                     attempts: int, base_delay: float, max_body_bytes: int,
                     check_budget: Callable, retain_budget: Callable,
                     fetch: Callable, sleep: Callable, retry_http_errors: bool,
                     preserve_attempt: Callable = arcgis_layer.preserve_failed_attempt):
    """Retry incomplete document captures, preserving bytes before another attempt."""
    for attempt in range(1, attempts + 1):
        check_budget(budget)
        try:
            return fetch(client, url, capture_root, name, max_bytes=max_body_bytes,
                         before_retain=retain_budget(budget))
        except (ValueError, httpx.HTTPError) as exc:
            if "source_capture_budget_exceeded" in str(exc):
                raise
            retryable = (str(exc).startswith("incomplete_source_capture:")
                         or (retry_http_errors and isinstance(exc, httpx.HTTPError)))
            if attempt >= attempts or not retryable:
                raise
            preserve_attempt(capture_root, name, attempt)
            sleep(base_delay * (3 ** (attempt - 1)))
    raise AssertionError("unreachable_retry_state")


def fetch_keyset_page(active, capture_root: Path, budget: list[int], name: str, url: str, *,
                      attempts: int, base_delay: float, max_body_bytes: int,
                      check_budget: Callable, retain_budget: Callable, fetch: Callable,
                      witness: Callable, producer_error: Callable, sleep: Callable,
                      error_type: type[ValueError]):
    """Retain bounded transport attempts and distinguish deterministic window refusal."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        check_budget(budget)
        try:
            body, receipt = fetch(active, url, capture_root, name,
                                        max_bytes=max_body_bytes,
                                        before_retain=retain_budget(budget))
        except (ValueError, httpx.HTTPError) as exc:
            if "source_capture_budget_exceeded" in str(exc):
                raise
            last_error = exc
            error_code = failed_receipt_error_code(capture_root, name)
            if error_code in ("wire_limit_exceeded", "decoded_limit_exceeded"):
                witness(capture_root, name)
                raise error_type("source_page_body_limit:" + name) from exc
            if attempt < attempts:
                witness(capture_root, name)
                sleep(base_delay * (3 ** (attempt - 1)))
                continue
            witness(capture_root, name)
            raise error_type("source_page_fetch_failed:" + name) from exc
        if producer_error(body):
            witness(capture_root, name)
            raise error_type("source_page_error:" + name)
        return body, receipt
    raise error_type("source_page_fetch_failed:" + name) from last_error

def capture_attributes(active, capture_root: Path, budget: list[int], oid: int, *,
                       record_url: str, read_capture: Callable, capture_doc: Callable,
                       validate_attrs: Callable, error_type: type[ValueError]):
    """Reuse or capture one exact attributes-only identity before schema validation."""
    name = f"refused_attrs_{oid:09d}"
    if (capture_root / f"{name}.body").exists():
        body, receipt = read_capture(capture_root, name)
    else:
        body, receipt = capture_doc(active, record_url,
                                     capture_root, name, budget)
    attrs = _attribute_response(body, oid, name, error_type)
    validate_attrs(attrs)
    return attrs, receipt

def rescue_single_record(active, capture_root: Path, budget: list[int], last_objectid: int,
                         pages: list[dict[str, object]], persist: Callable, *,
                         attrs_url: str, geometry_hosts: Callable, capture_attrs: Callable,
                         retained_or_fetch: Callable, page_name_for: Callable,
                         attempts: int, base_delay: float, max_body_bytes: int,
                         check_budget: Callable, retain_budget: Callable, fetch: Callable,
                         witness: Callable, producer_error: Callable, sleep: Callable,
                         error_type: type[ValueError]):
    """Witness both producer hosts and promote a rescued response without re-encoding it."""
    position = last_objectid + 1
    probe_name = f"refused_probe_{position:09d}_attrs"
    probe_body, _probe_receipt = retained_or_fetch(
        active, capture_root, probe_name, attrs_url, budget=budget)
    probe = json.loads(probe_body)
    probe_rows = probe.get("features") if isinstance(probe, dict) else None
    if (not isinstance(probe_rows, list) or len(probe_rows) != 1
            or not isinstance(probe_rows[0], dict)
            or not isinstance(probe_rows[0].get("attributes"), dict)):
        raise error_type("source_refused_record_identity_unavailable")
    oid = probe_rows[0]["attributes"].get("OBJECTID")
    if type(oid) is not int or oid <= last_objectid:
        raise error_type("source_refused_record_identity_unavailable")
    capture_attrs(active, capture_root, budget, oid)
    for attempt in range(1, attempts + 1):
        for label, probe_client, probe_url in geometry_hosts(oid):
            probe_geo_name = f"refused_probe_{position:09d}_geo_{label}"
            check_budget(budget)
            try:
                body, geo_receipt = fetch(
                    probe_client, probe_url, capture_root,
                    probe_geo_name, max_bytes=max_body_bytes,
                    before_retain=retain_budget(budget))
            except (ValueError, httpx.HTTPError):
                witness(capture_root, probe_geo_name)
                continue
            if producer_error(body):
                witness(capture_root, probe_geo_name)
                continue
            page_name = page_name_for(last_objectid, 1)
            copy_capture_slots(capture_root, probe_geo_name, page_name, budget,
                               check_budget=check_budget)
            pages.append({"name": page_name, "start_after": last_objectid, "size": 1,
                          "last_objectid": oid, "features": 1,
                          "exceeded_transfer_limit": None,
                          "captured_at": geo_receipt["observed_at"],
                          "body_sha256": hashlib.sha256(body).hexdigest(), "rescued_host": label})
            persist()
            return oid, True
        if attempt < attempts:
            sleep(base_delay * (3 ** (attempt - 1)))
    return oid, False

def copy_capture_slots(capture_root: Path, source: str, target: str, budget: list[int], *,
                       check_budget: Callable) -> None:
    """Copy accepted bytes only after reserving both body and wire destinations."""
    retained = sum(path.stat().st_size for path in capture_root.iterdir()
                   if path.is_file() and path.name.endswith((".body", ".wire")))
    sizes = {suffix: (capture_root / f"{source}.{suffix}").stat().st_size
             for suffix in ("body", "wire")}
    targets = {suffix: capture_root / f"{target}.{suffix}" for suffix in sizes}
    previous = {suffix: path.stat().st_size if path.exists() else 0
                for suffix, path in targets.items()}
    copied = sum(sizes.values())
    replaced = sum(previous.values())
    budget[0] = retained
    check_budget([retained - replaced + copied])
    reserve_capture_retention(capture_root, copied, replaced_bytes=replaced)
    # Shrink existing slots before growing others so every intermediate
    # retained state fits whenever the initial and final states fit.
    copy_order = sorted(sizes, key=lambda suffix: sizes[suffix] - previous[suffix])
    try:
        for suffix in (*copy_order, "receipt.json"):
            (capture_root / f"{target}.{suffix}").write_bytes(
                (capture_root / f"{source}.{suffix}").read_bytes())
    finally:
        budget[0] = sum(path.stat().st_size for path in capture_root.iterdir()
                        if path.is_file() and path.name.endswith((".body", ".wire")))

def _attribute_response(body: bytes, oid: int, name: str, error_type: type[ValueError]):
    doc = json.loads(body)
    rows = doc.get("features") if isinstance(doc, dict) else None
    if (not isinstance(rows, list) or len(rows) != 1
            or not isinstance(rows[0], dict) or not isinstance(rows[0].get("attributes"), dict)
            or rows[0].get("geometry") is not None
            or rows[0]["attributes"].get("OBJECTID") != oid):
        raise error_type("source_refused_attrs_record_drift:" + name)
    return rows[0]["attributes"]


def capture_refused_attributes(client, capture_root: Path, budget: list[int], captures, refused, *,
                               attributes_url: Callable, capture_doc: Callable,
                               name_for_id: Callable, error_type: type[ValueError]) -> None:
    """Acquire each refused record through the attributes-only channel without buffering pages."""
    for name in sorted(refused):
        for oid in refused[name]:
            attrs_name = name_for_id(oid)
            body, receipt = capture_doc(client, attributes_url(oid), capture_root, attrs_name, budget)
            _attribute_response(body, oid, attrs_name, error_type)
            captures[attrs_name] = (capture_root / f"{attrs_name}.body", receipt)
