"""Promote canonical objects without invalidating published catalog releases."""

from collections.abc import Sequence

from dagster_hifld.resources import StagingStorageResource, StorageObjectSnapshot
from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS


def promote_immutable_objects(
    source: StagingStorageResource,
    destination: StagingStorageResource,
    pairs: Sequence[tuple[str, str]],
    *,
    version_prefix: str | None = None,
) -> list[str]:
    """Preflight the whole batch, reuse identical bytes, and create missing keys.

    Existing keys are never replaced or deleted. Source identities and absent
    destinations are pinned by the storage backend's conditional-copy primitive.
    A failed partial upload is safe to retry; changed bytes need a new version.
    """
    if version_prefix is not None:
        prefix = destination._ensure_prefixed(version_prefix).rstrip("/") + "/"
        selected = {destination._ensure_prefixed(key) for _, key in pairs}
        formats = CANONICAL_SOURCE_FORMAT_DIRS | {"geoparquet", "pmtiles"}
        for key in destination.list_prefix(version_prefix):
            if (
                key.removeprefix(prefix).split("/", 1)[0] in formats
                and key not in selected
            ):
                raise ValueError(
                    f"Published data layout is immutable: {key}; publish a new version."
                )
    pending: list[tuple[str, str, StorageObjectSnapshot]] = []
    existing: list[tuple[str, str, StorageObjectSnapshot, StorageObjectSnapshot]] = []
    for source_key, destination_key in pairs:
        before = source.object_snapshot(source_key)
        if before is None:
            raise FileNotFoundError(source_key)
        target = destination.object_snapshot(destination_key)
        if target is not None:
            if not source.object_content_matches(
                destination, source_key, destination_key
            ):
                raise ValueError(
                    f"Published object is immutable: {destination_key}; publish a new version."
                )
            existing.append((source_key, destination_key, before, target))
        else:
            pending.append((source_key, destination_key, before))
    for source_key, destination_key, before, target in existing:
        if (
            source.object_snapshot(source_key) != before
            or destination.object_snapshot(destination_key) != target
        ):
            raise RuntimeError(
                f"Object changed during promotion preflight: {destination_key}"
            )
    for source_key, destination_key, before in pending:
        source.copy_key_to_if_unchanged(
            destination,
            source_key,
            destination_key,
            source_snapshot=before,
            destination_snapshot=None,
        )
    return [destination._ensure_prefixed(key) for _, key in pairs]


def write_immutable_object(
    storage: StagingStorageResource, key: str, contents: bytes
) -> str:
    """Create a generated canonical object, or reuse its identical bytes."""
    before = storage.object_snapshot(key)
    if before is None:
        storage.write_key_if_unchanged(key, contents, None)
    else:
        if storage.read_key(key) != contents:
            raise ValueError(
                f"Published object is immutable: {key}; publish a new version."
            )
        if storage.object_snapshot(key) != before:
            raise RuntimeError(f"Object changed during promotion preflight: {key}")
    return storage._ensure_prefixed(key)
