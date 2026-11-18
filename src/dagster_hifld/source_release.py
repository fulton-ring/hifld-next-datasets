"""Shared latest-release state, unchanged-poll and same-release correction logic.

Discovery stays with each source family: ArcGIS adapters read the layer's
``editingInfo`` plus count and ID roster, while Census adapters read the
producer's annual directory. This module owns the stable release signal, the
comparison against the last accepted capture, and the staging-only boundary.
It never grants source, rights or release acceptance.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

RELEASE_STATE_SCHEMA_VERSION = 1
RELEASE_STATE_PREFIX = "_release-state"
RELEASE_ACTIONS = frozenset({"new_release", "same_release_correction", "unchanged"})
CORRECTION_NOTE = (
    "Publisher content changed under the same release identity; the new content "
    "is retained as a new immutable version instead of replacing the earlier one."
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_VOLATILE_RESULT_KEYS = frozenset({"capture_directory", "written_keys"})


def canonical(value: object) -> bytes:
    """Encode deterministic JSON for release-state hashes and records."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(value: object) -> str:
    import hashlib

    return hashlib.sha256(canonical(value)).hexdigest()


def _text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(code)
    return value


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _text(value, code)


def _fingerprint(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError("invalid_release_fingerprint")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("invalid_release_accepted_at")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid_release_accepted_at") from exc
    return value


@dataclass(frozen=True)
class ReleaseSignal:
    """One observation of a publisher release, independent of capture time.

    ``release_id`` is a publisher edition/version label and ``release_date`` is
    a publisher data period. ``last_edit`` is the publisher's own edit timestamp
    (for example ArcGIS ``editingInfo.lastEditDate``). ``poll_fingerprint`` is
    the cheap state a poll can compare without downloading full content;
    ``content_sha256`` is the full content digest known after capture.
    """

    release_id: str | None
    release_date: str | None
    last_edit: str | None
    poll_fingerprint: str | None
    content_sha256: str | None
    selection_rule: str
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _optional_text(self.release_id, "invalid_release_id")
        _optional_text(self.release_date, "invalid_release_date")
        _optional_text(self.last_edit, "invalid_release_last_edit")
        _fingerprint(self.poll_fingerprint)
        _fingerprint(self.content_sha256)
        _text(self.selection_rule, "invalid_release_selection_rule")
        if not isinstance(self.evidence, Mapping):
            raise ValueError("invalid_release_evidence")
        canonical(dict(self.evidence))

    @property
    def identity_fingerprint(self) -> str:
        return _sha({"release_id": self.release_id, "release_date": self.release_date})

    @property
    def content_fingerprint(self) -> str:
        """Content-only digest safe to embed in an immutable candidate."""
        return _sha({
            "identity_fingerprint": self.identity_fingerprint,
            "content_sha256": self.content_sha256,
        })

    @property
    def state_fingerprint(self) -> str:
        return _sha({
            "identity_fingerprint": self.identity_fingerprint,
            "poll_fingerprint": self.poll_fingerprint,
            "content_sha256": self.content_sha256,
        })

    def to_dict(self) -> dict[str, object]:
        return {
            "release_id": self.release_id,
            "release_date": self.release_date,
            "last_edit": self.last_edit,
            "poll_fingerprint": self.poll_fingerprint,
            "content_sha256": self.content_sha256,
            "identity_fingerprint": self.identity_fingerprint,
            "state_fingerprint": self.state_fingerprint,
            "selection_rule": self.selection_rule,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class ReleaseDecision:
    action: str
    reason: str
    note: str | None = None

    def __post_init__(self) -> None:
        if self.action not in RELEASE_ACTIONS:
            raise ValueError("invalid_release_action")
        _text(self.reason, "invalid_release_decision_reason")
        _optional_text(self.note, "invalid_release_decision_note")


def decide_unchanged_poll(prior: Mapping[str, object] | None, current: ReleaseSignal) -> ReleaseDecision:
    """Return ``unchanged`` only when the cheap poll state proves no change.

    A missing prior state, a changed release identity, or a publisher that
    exposes no reliable cheap state all require a capture instead of a no-op.
    """
    if prior is None:
        return ReleaseDecision("new_release", "no_prior_accepted_capture")
    if prior["identity_fingerprint"] != current.identity_fingerprint:
        return ReleaseDecision("new_release", "publisher_release_identity_changed")
    if current.poll_fingerprint is None:
        return ReleaseDecision("new_release", "no_reliable_poll_state")
    if prior.get("poll_fingerprint") != current.poll_fingerprint:
        return ReleaseDecision("new_release", "source_poll_state_changed")
    return ReleaseDecision("unchanged", "poll_state_matches_last_accepted_capture")


def decide_release(prior: Mapping[str, object] | None, current: ReleaseSignal) -> ReleaseDecision:
    """Compare captured content with the last accepted capture.

    A publisher edition change is a new release. Content that changes while the
    edition/date identity stays the same is a same-release correction and still
    becomes a new immutable version with a note.
    """
    if current.content_sha256 is None:
        raise ValueError("release_content_fingerprint_required")
    if prior is None:
        return ReleaseDecision("new_release", "no_prior_accepted_capture")
    if prior["identity_fingerprint"] != current.identity_fingerprint:
        return ReleaseDecision("new_release", "publisher_release_identity_changed")
    if prior.get("content_sha256") is None:
        return ReleaseDecision("new_release", "prior_content_fingerprint_missing")
    if prior["content_sha256"] == current.content_sha256:
        return ReleaseDecision("unchanged", "captured_content_matches_last_accepted_capture")
    return ReleaseDecision("same_release_correction",
                           "content_changed_under_same_release_identity", note=CORRECTION_NOTE)


@dataclass(frozen=True)
class TransportProbe:
    """Bounded HEAD observation used only as a cheap unchanged-poll hint."""

    url: str
    final_url: str
    etag: str | None
    last_modified: str | None
    content_length: int | None

    def fingerprint(self) -> str | None:
        # A length alone cannot prove unchanged content; require a publisher
        # validator. Absence means the caller must capture and compare bytes.
        if not self.etag and not self.last_modified:
            return None
        return _sha({"url": self.url, "etag": self.etag, "last_modified": self.last_modified})


def probe_transport(client, url: str) -> TransportProbe:
    """Read a publisher's ETag/Last-Modified without downloading the payload."""
    _text(url, "invalid_release_probe_url")
    response = client.head(url, follow_redirects=False)
    if response.status_code != 200 or str(response.url) != url:
        raise ValueError("release_probe_failed")
    length = response.headers.get("content-length")
    return TransportProbe(
        url=url, final_url=str(response.url),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
        content_length=int(length) if length and length.isdigit() else None,
    )


def stable_candidate_summary(result: Mapping[str, object]) -> dict[str, object]:
    """Drop run-local paths so an unchanged poll can repeat the prior summary."""
    return {key: value for key, value in result.items() if key not in _VOLATILE_RESULT_KEYS}


def release_state_key(dataset_slug: str, file_slug: str) -> str:
    for value in (dataset_slug, file_slug):
        _text(value, "invalid_release_state_identity")
        if "/" in value or "\\" in value or value in {".", ".."}:
            raise ValueError("invalid_release_state_identity")
    return f"{RELEASE_STATE_PREFIX}/{dataset_slug}/{file_slug}.json"


def release_state_record(signal: ReleaseSignal, *, dataset_slug: str, file_slug: str,
                         version: str, accepted_at: str, action: str,
                         candidate_summary: Mapping[str, object],
                         note: str | None = None) -> dict[str, object]:
    """Build the bounded pointer record written only after a sealed capture."""
    _text(version, "invalid_release_state_version")
    if action not in RELEASE_ACTIONS:
        raise ValueError("invalid_release_state_action")
    _timestamp(accepted_at)
    summary = stable_candidate_summary(candidate_summary)
    canonical(dict(summary))
    record = {
        "schema_version": RELEASE_STATE_SCHEMA_VERSION,
        "dataset_slug": dataset_slug,
        "file_slug": file_slug,
        "version": version,
        "action": action,
        "note": note,
        "accepted_at": accepted_at,
        "candidate_summary": summary,
        **signal.to_dict(),
    }
    canonical(record)
    return record


def _validate_state(record: object) -> dict[str, object]:
    if not isinstance(record, dict) or record.get("schema_version") != RELEASE_STATE_SCHEMA_VERSION:
        raise ValueError("invalid_release_state_record")
    required = {
        "schema_version", "dataset_slug", "file_slug", "version", "action", "note",
        "accepted_at", "candidate_summary", "release_id", "release_date", "last_edit",
        "poll_fingerprint", "content_sha256", "identity_fingerprint",
        "state_fingerprint", "selection_rule", "evidence",
    }
    if set(record) != required:
        raise ValueError("invalid_release_state_record")
    if record["action"] not in RELEASE_ACTIONS:
        raise ValueError("invalid_release_state_record")
    _text(record["dataset_slug"], "invalid_release_state_record")
    _text(record["file_slug"], "invalid_release_state_record")
    _text(record["version"], "invalid_release_state_record")
    _timestamp(record["accepted_at"])
    if not isinstance(record["candidate_summary"], dict):
        raise ValueError("invalid_release_state_record")
    signal = ReleaseSignal(
        release_id=record["release_id"], release_date=record["release_date"],
        last_edit=record["last_edit"], poll_fingerprint=record["poll_fingerprint"],
        content_sha256=record["content_sha256"], selection_rule=record["selection_rule"],
        evidence=record["evidence"],
    )
    if record["identity_fingerprint"] != signal.identity_fingerprint:
        raise ValueError("release_state_fingerprint_mismatch")
    if record["state_fingerprint"] != signal.state_fingerprint:
        raise ValueError("release_state_fingerprint_mismatch")
    return record


def load_release_state(storage: StagingStorageResource, dataset_slug: str,
                       file_slug: str) -> dict[str, object] | None:
    """Read the last accepted capture pointer, or ``None`` when never recorded."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_staging_resource_required")
    key = release_state_key(dataset_slug, file_slug)
    if not storage.object_exists(key):
        return None
    body = storage.read_key(key)
    if not isinstance(body, bytes):
        raise ValueError("invalid_release_state_record")
    try:
        record = json.loads(body)
    except ValueError as exc:
        raise ValueError("invalid_release_state_record") from exc
    if canonical(record) + b"\n" != body:
        raise ValueError("noncanonical_release_state_record")
    return _validate_state(record)


def record_release_state(storage: StagingStorageResource, record: Mapping[str, object]) -> dict[str, object]:
    """CAS-write the pointer, treating an identical concurrent write as success."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_staging_resource_required")
    validated = _validate_state(dict(record))
    key = release_state_key(str(validated["dataset_slug"]), str(validated["file_slug"]))
    body = canonical(validated) + b"\n"
    for _attempt in range(3):
        snapshot = storage.object_snapshot(key)
        if snapshot is not None:
            current = storage.read_key(key)
            if current == body:
                return validated
        try:
            storage.write_key_if_unchanged(key, body, snapshot)
            return validated
        except (RuntimeError, ValueError):
            if storage.object_exists(key) and storage.read_key(key) == body:
                return validated
    raise ValueError("release_state_concurrent_update")


def unchanged_result(prior: Mapping[str, object], *, dataset_slug: str, file_slug: str,
                     **extra: object) -> dict[str, object]:
    """Repeat the last accepted candidate summary for an unchanged poll."""
    version = str(prior["version"])
    result = dict(prior.get("candidate_summary") or {})
    result.update({
        "dataset_slug": dataset_slug,
        "file_slug": file_slug,
        "version": version,
        "candidate_ready": f"{dataset_slug}/{file_slug}/{version}/metadata/candidate_ready.json",
        "unchanged": True,
        "reused": True,
        "release_action": "unchanged",
        "release_state_fingerprint": prior["state_fingerprint"],
        "source_release_accepted": False,
        "release_accepted": False,
    })
    result.update(extra)
    return result
