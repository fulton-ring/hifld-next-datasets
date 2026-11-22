"""EPA ArcGIS offset, numeric-ID-window and resumable keyset acquisition."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx

from dagster_hifld.sources.esri_common import sha


@dataclass(frozen=True, kw_only=True)
class OffsetCaptureSpec:
    table_url: str
    page_size: int
    query: Mapping[str, Any]
    page_name: Callable[[int], str]
    error: type[ValueError]
    incomplete_error: str = "source_page_row_count_incomplete"
    reject_full_final_limit: bool = False


@dataclass(frozen=True)
class CapturedPage:
    number: int
    offset: int
    expected: int
    name: str
    response: Any
    payload: dict
    features: list
    evidence: Any


def capture_offset_pages(spec: OffsetCaptureSpec, count: int, *, request: Callable,
                         payload: Callable = lambda response: response,
                         retain: Callable | None = None):
    """Acquire and admit one bounded offset page before yielding it to a sink.

    The request callback owns existing transport, replay and byte-budget policy;
    the optional retain callback preserves protocol-specific wire receipts before
    producer payload validation. No page body population is accumulated here.
    """
    if type(spec.page_size) is not int or spec.page_size < 1:
        raise spec.error("source_page_size_invalid")
    for number, offset in enumerate(range(0, count, spec.page_size)):
        name = spec.page_name(number)
        params = {**spec.query, "resultOffset": offset,
                  "resultRecordCount": spec.page_size, "f": "json"}
        response = request(name, spec.table_url + "/query", params)
        evidence = retain(name, params, response) if retain is not None else None
        document = payload(response)
        features = document.get("features")
        expected = min(spec.page_size, count - offset)
        if not isinstance(features, list) or len(features) != expected:
            raise spec.error(spec.incomplete_error)
        final = offset + expected >= count
        if document.get("exceededTransferLimit") is True and final \
                and (expected < spec.page_size or spec.reject_full_final_limit):
            raise spec.error("source_final_page_exceeded_transfer_limit")
        yield CapturedPage(number, offset, expected, name, response, document, features, evidence)


def capture_id_windows(*, maximum_id: int, window_size: int, table_url: str,
                       request: Callable, payload: Callable, error: type[ValueError]):
    """Stream sorted native IDs from bounded numeric windows without filling gaps."""
    if type(window_size) is not int or window_size < 1:
        raise error("source_id_window_size_invalid")
    previous = 0
    for low in range(0, maximum_id, window_size):
        high = min(low + window_size, maximum_id)
        document = payload(request(table_url + "/query", {
            "where": f"OBJECTID > {low} AND OBJECTID <= {high}",
            "returnIdsOnly": "true", "f": "json"}))
        ids = document.get("objectIds")
        if not isinstance(ids, list) or any(type(value) is not int for value in ids):
            raise error("source_id_window_invalid")
        if ids != sorted(ids) or any(not low < value <= high for value in ids):
            raise error("source_id_window_order_or_bounds_changed")
        for value in ids:
            if value <= previous:
                raise error("source_id_roster_duplicate_or_unordered")
            previous = value
            yield value


@dataclass(frozen=True, kw_only=True)
class KeysetCaptureSpec:
    metadata_names: tuple[str, ...]
    document_urls: Callable[[], dict[str, str]]
    page_url: Callable[[int], str]
    page_size: int
    load_state: Callable
    save_state: Callable
    read_capture: Callable
    witness_aside: Callable
    fetch: Callable
    reject_error: Callable
    validate_feature: Callable
    default_client: Callable
    error: type[ValueError]


def capture_keyset_layer(spec: KeysetCaptureSpec, capture_root: Path, *,
                         client: httpx.Client | None = None) -> dict:
    """Own discovery, resume repair, keyset pages, tail and closing witnesses."""
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = spec.load_state(capture_root)
    pages: list[dict[str, object]] = list(state.get("data_pages", []))
    tail: dict[str, object] | None = state.get("tail")
    owner_client = client is None
    active = client or spec.default_client()
    first_captured_at = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
    try:
        retained: dict[str, tuple[bytes, dict[str, object]]] = {}
        urls = spec.document_urls()
        last_objectid = pages[-1]['last_objectid'] if pages else 0
        tail_valid = False
        try:
            tail_body, tail_receipt = spec.read_capture(capture_root, 'page_tail')
            tail_document = json.loads(tail_body)
        except ValueError:
            pass
        else:
            if tail_receipt['url'] != spec.page_url(last_objectid):
                raise spec.error('retained_capture_receipt_invalid:page_tail')
            tail_valid = isinstance(tail_document, dict) and tail_document.get('features') == []
        refresh_closing = state.get('complete') is not True or tail is None or not tail_valid
        closing_names = ('layer_after', 'service_after', 'count_after')
        if not refresh_closing:
            try:
                for name in (*spec.metadata_names, *closing_names):
                    document = json.loads(spec.read_capture(capture_root, name, expected_url=urls[name])[0])
                    if not isinstance(document, dict) or 'error' in document:
                        refresh_closing = True
                        break
            except ValueError:
                refresh_closing = True
        if refresh_closing:
            spec.save_state(capture_root, {'data_pages': pages, 'tail': tail, 'complete': False})
            for name in closing_names:
                spec.witness_aside(capture_root, name)
            if not tail_valid:
                tail = None
                spec.witness_aside(capture_root, 'page_tail')
                spec.witness_aside(capture_root, f'page_{len(pages):06d}')
        for name in spec.metadata_names:
            url = urls[name]
            retained[name] = spec.fetch(active, capture_root, name, url)
            spec.reject_error(capture_root, name, retained[name][0])
        count_before = json.loads(retained["count_before"][0]).get("count")
        if type(count_before) is not int or count_before <= 0:
            raise spec.error("source_count_drift")
        while not (capture_root / "page_tail.body").exists():
            name = f"page_{len(pages):06d}"
            body, receipt = spec.fetch(active, capture_root, name, spec.page_url(last_objectid))
            page = json.loads(body)
            if "error" in page:
                spec.witness_aside(capture_root, name)
                raise spec.error("source_page_error")
            features = page.get("features")
            if not isinstance(features, list):
                raise spec.error("source_page_missing_features")
            if not features:
                if last_objectid == 0:
                    raise spec.error("source_page_population_incomplete")
                # An empty page in a data slot is the completeness witness: the same
                # query URL defines page_tail, so retain its exact bytes under that name.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").write_bytes(
                        (capture_root / f"{name}.{suffix}").read_bytes())
                break
            for feature in features:
                spec.validate_feature(feature)
                objectid = feature["attributes"]["OBJECTID"]
                if objectid <= last_objectid:
                    raise spec.error("source_object_ids_not_strictly_increasing")
                last_objectid = objectid
            pages.append({"name": name, "last_objectid": last_objectid, "features": len(features),
                          "captured_at": receipt["observed_at"], "body_sha256": sha(body)})
            spec.save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
            if len(features) < spec.page_size:
                tail_body, tail_receipt = spec.fetch(active, capture_root, "page_tail",
                                                             spec.page_url(last_objectid))
                spec.reject_error(capture_root, "page_tail", tail_body)
                if json.loads(tail_body).get("features") != []:
                    raise spec.error("source_tail_page_not_empty")
                tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                        "captured_at": tail_receipt["observed_at"], "body_sha256": sha(tail_body)}
                spec.save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
                break
            if len(pages) > (count_before + spec.page_size - 1) // spec.page_size:
                raise spec.error("source_page_population_incomplete")
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise spec.error("source_tail_page_not_empty")
        if sum(page['features'] for page in pages) != count_before:
            raise spec.error('source_page_population_incomplete')
        for name in ("layer_after", "service_after", "count_after"):
            url = urls[name]
            body, _receipt = spec.fetch(active, capture_root, name, url)
            spec.reject_error(capture_root, name, body)
            retained[name] = (body, spec.read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise spec.error("source_metadata_or_count_changed_during_capture")
    except spec.error:
        raise
    except Exception as exc:
        raise spec.error("source_capture_failed:" + type(exc).__name__) from exc
    finally:
        if owner_client:
            active.close()

    return {"pages": pages, "tail": tail, "first_captured_at": first_captured_at}
