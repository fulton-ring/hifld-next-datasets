"""Validated immutable-catalog release commit pointers."""

from __future__ import annotations

import json
import re
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

_PROTOCOL_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SEMVER = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)


def semantic_version_key(value: str) -> tuple:
    """SemVer precedence; a leading v is only a historical directory convention."""
    match = _SEMVER.fullmatch(value.removeprefix("v")) if isinstance(value, str) else None
    if match is None:
        raise ValueError("Release requires a complete semantic version")
    major, minor, patch, prerelease = match.groups()
    identifiers = (prerelease or "").split(".") if prerelease else []
    if any(part.isascii() and part.isdecimal() and len(part) > 1 and part.startswith("0") for part in identifiers):
        raise ValueError("Semantic version prerelease numbers cannot have leading zeros")
    return (int(major), int(minor), int(patch), int(prerelease is None),
            tuple((0, int(part)) if part.isascii() and part.isdecimal() else (1, part) for part in identifiers))


def read_staged_release(storage, prefix: str) -> tuple[dict, str]:
    """Resolve a candidate to the standard STAC version, never its partition label."""
    try:
        document = json.loads(storage.read_key(f"{prefix}/collection.json"))
    except (OSError, ValueError) as exc:
        raise ValueError("Missing or unreadable staged STAC Collection") from exc
    version = document.get("version") if isinstance(document, dict) else None
    semantic_version_key(version)
    if version.startswith("v"):
        raise ValueError("STAC version must omit the directory's v prefix")
    identifier = document.get("id")
    if not isinstance(identifier, str):
        raise ValueError("Staged Collection requires an ID")
    parts = identifier.split("/")
    source_parts = prefix.split("/")
    if (document.get("type") != "Collection" or len(parts) != 4 or len(source_parts) != 3
            or any(not part or part in {".", ".."} or "\\" in part for part in parts)
            or parts[1:3] != source_parts[:2] or parts[3] not in {version, "v" + version}):
        raise ValueError("Staged Collection ID and semantic version disagree with the selected dataset/file")
    return document, "/".join(parts[1:])


def require_current_baseline(destination, document: dict, baseline: dict) -> None:
    """Check the accepted catalog head, not unselected/orphaned data prefixes."""
    try:
        prior = semantic_version_key(baseline["version"])
    except ValueError:
        prior = None  # An accepted legacy archive may precede the first semantic release.
    if prior is not None and semantic_version_key(document["version"]) <= prior:
        raise ValueError("New release version must advance the accepted baseline")
    published = destination.model_copy(update={"prefix": ""})
    pointer_key = "_catalog/current.json"
    before = published.object_snapshot(pointer_key)
    if before is None:
        return  # An initial release may reference an independently accepted external baseline.
    pointer = ReleasePointer.parse(published.read_key(pointer_key))
    contents = published.read_key(pointer.catalog_key)
    if len(contents) != pointer.size_bytes or hashlib.sha256(contents).hexdigest() != pointer.sha256:
        raise ValueError("Accepted baseline catalog checksum changed")
    with sqlite3.connect(":memory:") as connection:
        connection.deserialize(contents)
        row = connection.execute("SELECT latest_version FROM files WHERE file_path = ?",
                                 (document["id"].rsplit("/", 1)[0],)).fetchone()
    if published.object_snapshot(pointer_key) != before:
        raise ValueError("Accepted baseline changed during verification")
    if row is not None:
        label = row[0]
        if label.removeprefix("v") == document["version"]:
            return  # Immutable-copy checks still reject different bytes on retries.
        if label.removeprefix("v") != baseline["version"].removeprefix("v"):
            raise ValueError("Accepted baseline version changed since comparison")
        if semantic_version_key(document["version"]) <= semantic_version_key(label):
            raise ValueError("New release version must advance the accepted baseline")


@dataclass(frozen=True)
class ReleasePointer:
    """A small mutable commit that selects one complete catalog release."""

    generation: str
    catalog_key: str
    root_key: str
    sha256: str
    size_bytes: int
    published_at: str

    def __post_init__(self) -> None:
        _validate_generation(self.generation)
        release_prefix = f"releases/{self.generation}/"
        if not self.catalog_key.startswith(release_prefix) or not self.catalog_key.endswith(
            "/_catalog/catalog.sqlite"
        ):
            raise ValueError("Catalog key must be inside the selected release.")
        if not self.root_key.startswith(release_prefix) or not self.root_key.endswith(
            "/catalog.json"
        ):
            raise ValueError("Root key must be inside the selected release.")
        if not _SHA256.fullmatch(self.sha256):
            raise ValueError("Release pointer requires a lowercase SHA-256 checksum.")
        if self.size_bytes <= 0:
            raise ValueError("Release pointer catalog size must be positive.")
        try:
            datetime.fromisoformat(self.published_at)
        except ValueError as error:
            raise ValueError("Release pointer published_at must be RFC 3339.") from error

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {
                    "protocol_version": _PROTOCOL_VERSION,
                    "generation": self.generation,
                    "catalog_key": self.catalog_key,
                    "root_key": self.root_key,
                    "sha256": self.sha256,
                    "size_bytes": self.size_bytes,
                    "published_at": self.published_at,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def parse(cls, value: bytes) -> ReleasePointer:
        try:
            decoded = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Release pointer must be JSON.") from error
        if not isinstance(decoded, dict):
            raise TypeError("Release pointer must be a JSON object.")
        if decoded.get("protocol_version") != _PROTOCOL_VERSION:
            raise ValueError("Unsupported release pointer protocol version.")
        generation = _required_text(decoded.get("generation"), "generation")
        catalog_key = _required_text(decoded.get("catalog_key"), "catalog_key")
        root_key = _required_text(decoded.get("root_key"), "root_key")
        sha256 = _required_text(decoded.get("sha256"), "sha256")
        size_bytes = decoded.get("size_bytes")
        published_at = _required_text(decoded.get("published_at"), "published_at")
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool):
            raise TypeError("Release pointer size_bytes must be an integer.")
        return cls(generation, catalog_key, root_key, sha256, size_bytes, published_at)


def _validate_generation(value: str) -> None:
    try:
        UUID(value)
    except ValueError as error:
        raise ValueError("Release pointer generation must be a UUID.") from error


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"Release pointer {name} must be a string.")
    return value
