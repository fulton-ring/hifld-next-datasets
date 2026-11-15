"""Shared NHD/3DHP retained-capture checkpoint, receipt and refusal primitives.

One canonical copy of the checkpoint load/save, retained-capture read, refusal
witness, failed-attempt evidence and bounded-retry mechanics previously
duplicated across the segmented NHD adapters (``nhd_catchment.py``,
``nhd_hydrolocation_htdcco.py``,
``nhd_hydrolocation_sink_spring_waterbody_outlet.py``). Each body is identical
to the copies it replaces; only each adapter's own error type and fetch budgets
are injected explicitly. Fetch planning, roster/offset algorithms, checkpoint
state layouts, schema, ID and geometry contracts stay local to each adapter.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable, Iterator, Mapping

import httpx

from dagster_hifld.sources.epa_frs_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha


def load_receipt(capture_root: Path, name: str, error: type[ValueError]) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise error("retained_capture_receipt_invalid:" + name)
    return receipt


def read_capture(capture_root: Path, name: str,
                 error: type[ValueError]) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = load_receipt(capture_root, name, error)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise error("retained_capture_receipt_invalid:" + name)
    return body, receipt


def load_state(capture_root: Path, error: type[ValueError]) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"segments": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("segments"), list):
        raise error("capture_state_invalid")
    return state


def save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def witness_aside(capture_root: Path, name: str) -> None:
    """Move a retained refused/torn response aside so the slot can be refetched.

    A complete HTTP 200 error body carries a self-consistent receipt, so
    without this witness step a resumed capture would replay the same refusal
    forever; the witnessed evidence stays on disk next to the capture.
    """
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    return isinstance(document, dict) and "error" in document


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       attempts: int, base_delay: float,
                       max_bytes: int) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents
    and incomplete transport captures (connection resets, truncation); failed
    attempts are preserved on disk under ``<name>.attempt-NN`` evidence names
    and never enter the capture roster. The final attempt's body is always
    returned as captured, so a persistent producer failure surfaces to the
    caller unchanged.
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


class RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str], error: type[ValueError]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)
        self._error = error

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return read_capture(self._capture_root, name, self._error)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)
