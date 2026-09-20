"""Validated immutable-catalog release commit pointers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

_PROTOCOL_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
