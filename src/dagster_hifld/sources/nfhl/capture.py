"""Resumable OBJECTID-window capture of one FEMA NFHL MapServer layer.

Every request goes through ``capture_with_retry`` (bounded retries for the
producer's transient complete-HTTP-200 Esri error documents and incomplete
transports, each failed attempt kept as ``<name>.attempt-NN`` evidence). The
layer is captured between two witnesses read from the producer itself: the
service and layer documents, ``returnCountOnly`` and OBJECTID count/min/max
statistics. Features are paged by aligned OBJECTID windows, so no ID roster is
held in memory. A window the producer refuses (an Esri error document or a
truncated transport after retries) or truncates (``exceededTransferLimit``) is
set aside as evidence and bisected; a single-OBJECTID window that is still
refused fails the capture.

The capture directory is resumable: a rerun reuses every accepted page whose
receipt binds the exact request URL and whose body and wire bytes still match
their digests, provided the producer's before-witness is unchanged. Any other
earlier evidence (stale or mismatched pages, witnesses, failed attempts) is
moved under ``previous/NNN/`` and never reused.
"""
from __future__ import annotations

import bisect
import fcntl
import json
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

import httpx

from dagster_hifld.sources.epa_frs_common import capture_get
from dagster_hifld.sources.esri_common import canonical, sha

SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
PORTAL_ITEM_ID = "ae38b6f94eaf4abf97f986fa01921e13"
PORTAL_ORG_ID = "XG15cJAlne2vxtgt"
PORTAL_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{PORTAL_ITEM_ID}"
PORTAL_ORG_URL = f"https://www.arcgis.com/sharing/rest/portals/{PORTAL_ORG_ID}"
EXPECTED_PORTAL_ITEM = {"id": PORTAL_ITEM_ID, "owner": "FEMA_MAC", "orgId": PORTAL_ORG_ID,
                        "type": "Map Service", "url": SERVICE_URL}
EXPECTED_PORTAL_ORG = {"id": PORTAL_ORG_ID, "name": "FEMA AGOL"}
DOC_MAX_BYTES = 64 * 1024 * 1024
_EVIDENCE_SUFFIXES = (".body", ".wire", ".receipt.json")
_PAGE_NAME = re.compile(r"page_([0-9]{10})_([0-9]{10})")


@dataclass(frozen=True)
class CaptureSettings:
    """Retry budgets and HTTP client options for one capture."""

    doc_attempts: int = 4
    doc_delay: float = 10.0
    page_attempts: int = 3
    page_delay: float = 5.0
    timeout: float = 180.0
    transport: httpx.BaseTransport | None = None

    def client(self) -> httpx.Client:
        return httpx.Client(follow_redirects=False, timeout=self.timeout, transport=self.transport,
                            headers={"User-Agent": "hifld-next-datasets/nfhl"})


def producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in _EVIDENCE_SUFFIXES:
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names. The final attempt's body is always
    returned as captured, so a persistent producer failure surfaces unchanged.
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


def query_url(layer_url: str, params: dict[str, object]) -> str:
    return layer_url + "/query?" + urlencode(params)


def page_name(lo: int, hi: int) -> str:
    return f"page_{lo:010d}_{hi:010d}"


def strip_rotating_palette(layer_doc: object) -> object:
    """Copy a layer document without the renderer's rotating colorRamp palette.

    The producer rotates ``drawingInfo.renderer.authoringInfo.colorRamp`` colors
    on every request; it is presentation metadata only, and the rest of the
    document is compared unchanged.
    """
    copy = json.loads(json.dumps(layer_doc))
    if isinstance(copy, dict):
        renderer = (copy.get("drawingInfo") or {}).get("renderer") or {}
        authoring = renderer.get("authoringInfo") if isinstance(renderer, dict) else None
        if isinstance(authoring, dict):
            authoring.pop("colorRamp", None)
    return copy


def _move_group(source_root: Path, name: str, target_root: Path, target_name: str | None = None) -> None:
    target_root.mkdir(parents=True, exist_ok=True)
    for suffix in _EVIDENCE_SUFFIXES:
        path = source_root / f"{name}{suffix}"
        if path.exists():
            path.rename(target_root / f"{target_name or name}{suffix}")


class EvidenceDirectory:
    """Exclusive, resumable capture directory for one layer."""

    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self._lock = (root / ".lock").open("a+")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock.close()
            raise ValueError("capture_directory_in_use") from None
        previous = root / "previous"
        used = [int(path.name) for path in previous.glob("[0-9][0-9][0-9]") if path.is_dir()]
        self.previous = previous / f"{max(used, default=0) + 1:03d}"
        try:
            self.prior_witness = json.loads((root / "witness.json").read_bytes())
        except (OSError, ValueError):
            self.prior_witness = None
        groups: dict[str, list[Path]] = {}
        for path in sorted(root.iterdir()):
            if path.is_dir() or path.name == ".lock":
                continue
            stem = next((path.name[:-len(s)] for s in _EVIDENCE_SUFFIXES if path.name.endswith(s)), "")
            groups.setdefault(stem if _PAGE_NAME.fullmatch(stem) else "", []).append(path)
        self.prior_pages: list[tuple[int, int]] = []
        for stem, paths in groups.items():
            if stem and (root / f"{stem}.receipt.json").exists():
                match = _PAGE_NAME.fullmatch(stem)
                self.prior_pages.append((int(match.group(1)), int(match.group(2))))
                continue
            self.previous.mkdir(parents=True, exist_ok=True)
            for path in paths:
                path.rename(self.previous / path.name)
        self.prior_pages.sort()
        self.retired: list[str] = []

    def close(self) -> None:
        self._lock.close()

    def accept_witness(self, witness: dict[str, object]) -> None:
        """Keep prior pages only when the producer's before-witness is unchanged."""
        if self.prior_pages and self.prior_witness != witness:
            for lo, hi in self.prior_pages:
                self.retire(page_name(lo, hi))
            self.prior_pages = []
        (self.root / "witness.json").write_bytes(canonical(witness) + b"\n")

    def retire(self, name: str) -> None:
        _move_group(self.root, name, self.previous)
        self.retired.append(name)

    def set_aside(self, name: str, label: str) -> None:
        _move_group(self.root, name, self.root, f"{name}.{label}")

    def reusable(self, name: str, url: str) -> bytes | None:
        """Return an earlier accepted page only if its evidence still verifies."""
        receipt_path = self.root / f"{name}.receipt.json"
        if not receipt_path.exists():
            return None
        try:
            receipt = json.loads(receipt_path.read_bytes())
            body = (self.root / f"{name}.body").read_bytes()
            wire = (self.root / f"{name}.wire").read_bytes()
            doc = json.loads(body)
            valid = (receipt_path.read_bytes() == canonical(receipt) + b"\n"
                     and receipt.get("url") == url and receipt.get("final_url") == url
                     and receipt.get("status") == 200 and receipt.get("complete_body") is True
                     and receipt.get("bytes") == len(body) and receipt.get("sha256") == sha(body)
                     and receipt.get("wire_bytes") == len(wire) and receipt.get("wire_sha256") == sha(wire)
                     and isinstance(doc, dict) and "error" not in doc
                     and doc.get("exceededTransferLimit") is not True)
        except (OSError, ValueError, AttributeError):
            valid = False
        if valid:
            return body
        self.retire(name)
        return None

    def split_hint(self, lo: int, hi: int) -> bool:
        """True when an earlier run accepted a strict sub-window of ``[lo, hi]``."""
        index = bisect.bisect_left(self.prior_pages, (lo, lo))
        while index < len(self.prior_pages) and self.prior_pages[index][0] <= hi:
            a, b = self.prior_pages[index]
            if b <= hi and (a, b) != (lo, hi):
                return True
            index += 1
        return False


@dataclass
class Page:
    name: str
    lo: int
    hi: int
    doc: dict
    body_path: Path
    reused: bool


@dataclass
class CaptureLog:
    """Accepted capture names in request order, plus refusal evidence counts."""

    accepted: list[str] = field(default_factory=list)
    set_aside: list[str] = field(default_factory=list)
    reused_pages: int = 0


class LayerCapture:
    """Capture producer documents and OBJECTID windows for one layer URL."""

    def __init__(self, client: httpx.Client, evidence: EvidenceDirectory, settings: CaptureSettings,
                 layer_url: str, *, max_page_bytes: int):
        self.client, self.evidence, self.settings = client, evidence, settings
        self.layer_url, self.max_page_bytes = layer_url, max_page_bytes
        self.log = CaptureLog()

    def raw(self, name: str, url: str) -> bytes:
        body, _ = capture_with_retry(self.client, url, self.evidence.root, name, max_bytes=DOC_MAX_BYTES,
                                     attempts=self.settings.doc_attempts, base_delay=self.settings.doc_delay)
        self.log.accepted.append(name)
        return body

    def doc(self, name: str, url: str) -> dict:
        body = self.raw(name, url)
        try:
            doc = json.loads(body)
        except ValueError:
            raise ValueError(f"producer_document_not_json:{name}") from None
        if not isinstance(doc, dict) or "error" in doc:
            raise ValueError(f"producer_error_document:{name}")
        return doc

    def witness(self, phase: str, oid_field: str) -> dict[str, object]:
        """Read the producer's own count and OBJECTID statistics."""
        count = self.doc(f"count_{phase}", query_url(self.layer_url, {
            "where": "1=1", "returnCountOnly": "true", "f": "json"})).get("count")
        stats = [{"statisticType": kind, "onStatisticField": oid_field, "outStatisticFieldName": f"{kind}_oid"}
                 for kind in ("count", "min", "max")]
        doc = self.doc(f"stats_{phase}", query_url(self.layer_url, {
            "where": "1=1", "outStatistics": json.dumps(stats, separators=(",", ":")), "f": "json"}))
        rows = doc.get("features")
        values = rows[0].get("attributes") if isinstance(rows, list) and len(rows) == 1 \
            and isinstance(rows[0], dict) else None
        if type(count) is not int or count < 0 or not isinstance(values, dict):
            raise ValueError(f"source_witness_unreadable:{phase}")
        n, lo, hi = (values.get(f"{kind}_oid") for kind in ("count", "min", "max"))
        if count == 0 and n in (0, None) and lo is None and hi is None:
            return {"count": 0, "min": None, "max": None}
        if any(type(v) is not int for v in (n, lo, hi)) or n != count or not 0 <= lo <= hi:
            raise ValueError(f"source_witness_inconsistent:{phase}:count={count},statistics={n},{lo},{hi}")
        return {"count": count, "min": lo, "max": hi}

    def windows(self, oid_field: str, lo: int, hi: int, width: int, *, spatial: bool) -> Iterator[Page]:
        """Yield accepted pages covering ``[lo, hi]`` in ascending OBJECTID order."""
        start = (lo // width) * width
        for base in range(start, hi + 1, width):
            yield from self._window(oid_field, base, base + width - 1, spatial)

    def _window(self, oid_field: str, lo: int, hi: int, spatial: bool) -> Iterator[Page]:
        name = page_name(lo, hi)
        params: dict[str, object] = {"where": f"{oid_field}>={lo} AND {oid_field}<={hi}", "outFields": "*"}
        if spatial:
            params["returnGeometry"] = "true"
        params.update({"orderByFields": f"{oid_field} ASC", "f": "json"})
        url = query_url(self.layer_url, params)
        body = self.evidence.reusable(name, url)
        reused = body is not None
        refusal = None
        if body is None and self.evidence.split_hint(lo, hi):
            refusal = "split-on-resume"
        elif body is None:
            try:
                body, _ = capture_with_retry(self.client, url, self.evidence.root, name,
                                             max_bytes=self.max_page_bytes, attempts=self.settings.page_attempts,
                                             base_delay=self.settings.page_delay)
            except ValueError as exc:
                if not str(exc).startswith("incomplete_source_capture:"):
                    raise
                refusal = "incomplete"
            if body is not None and producer_error_doc(body):
                refusal = "refused"
        doc = None
        if refusal is None:
            try:
                doc = json.loads(body)
            except ValueError:
                raise ValueError(f"source_page_not_json:{name}") from None
            if not isinstance(doc, dict):
                raise ValueError(f"source_page_not_object:{name}")
            if doc.get("exceededTransferLimit") is True:
                refusal = "truncated"
        if refusal is not None:
            if refusal != "split-on-resume":
                self.evidence.set_aside(name, refusal)
                self.log.set_aside.append(f"{name}.{refusal}")
            if lo == hi:
                raise ValueError(f"source_window_unservable:{oid_field}={lo}:{refusal}")
            mid = lo + (hi - lo + 1) // 2 - 1
            yield from self._window(oid_field, lo, mid, spatial)
            yield from self._window(oid_field, mid + 1, hi, spatial)
            return
        self.log.accepted.append(name)
        self.log.reused_pages += int(reused)
        yield Page(name, lo, hi, doc, self.evidence.root / f"{name}.body", reused)


def window_width(count: int, lo: int, hi: int, target: int) -> int:
    """Largest power of two at or below the span holding ``target`` features on average."""
    estimate = max(1, (target * (hi - lo + 1)) // max(count, 1))
    return 1 << (estimate.bit_length() - 1)


def check_spatial_reference(value: object, wkid: int, label: str) -> None:
    if not isinstance(value, dict) or not any(key in value for key in ("wkid", "latestWkid")):
        raise ValueError(f"{label}_crs_missing")
    for key in ("wkid", "latestWkid"):
        if key in value and value[key] != wkid:
            raise ValueError(f"{label}_crs_drift:{value[key]}")

