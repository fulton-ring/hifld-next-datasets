"""Shared dynamic partition definitions for versioned publish assets."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dagster import DynamicPartitionsDefinition

from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS

if TYPE_CHECKING:
    from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

SEMVER_VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
SOURCE_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_DIRS
PUBLISH_PARTITION_NAME = "publish_dataset_file_version"
PUBLISH_PARTITIONS = DynamicPartitionsDefinition(name=PUBLISH_PARTITION_NAME)
_SAFE_PARTITION_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")


def _validate_partition_segment(value: str, *, label: str) -> str:
    if not _SAFE_PARTITION_SEGMENT_RE.fullmatch(value) or value in {".", ".."}:
        raise ValueError(f"Publish partition {label} must be a safe non-empty path segment.")
    return value


def default_publish_collection() -> str:
    """Collection used by unqualified, pre-collection partition keys."""
    value = os.environ.get("HIFLD_PORTOLAN_COLLECTION", "hifld")
    return _validate_partition_segment(value, label="collection")


def parse_staging_collections(value: str) -> tuple[str, ...]:
    """Parse explicit collection scan scopes without accepting ambiguous paths."""
    raw_collections = value.split(",")
    if not raw_collections or any(
        not item or item != item.strip() for item in raw_collections
    ):
        raise ValueError(
            "HIFLD_STAGING_COLLECTIONS must be comma-separated collection slugs "
            "without whitespace."
        )
    collections = tuple(
        _validate_partition_segment(item, label="collection") for item in raw_collections
    )
    if len(set(collections)) != len(collections):
        raise ValueError("HIFLD_STAGING_COLLECTIONS cannot contain duplicate collections.")
    return collections


@dataclass(frozen=True)
class PublishPartitionIdentity:
    collection: str
    dataset_slug: str
    file_slug: str
    version: str
    collection_qualified: bool = False


def build_publish_partition_key(
    dataset_slug: str,
    file_slug: str,
    version: str,
    *,
    collection: str | None = None,
) -> str:
    _validate_partition_segment(dataset_slug, label="dataset")
    _validate_partition_segment(file_slug, label="file")
    _validate_partition_segment(version, label="version")
    if collection and collection != default_publish_collection():
        _validate_partition_segment(collection, label="collection")
        return f"{collection}/{dataset_slug}/{file_slug}/{version}"
    return f"{dataset_slug}/{file_slug}/{version}"


def parse_publish_partition_identity(partition_key: str) -> PublishPartitionIdentity:
    parts = partition_key.split("/")
    if any(not part for part in parts):
        raise ValueError("Publish partition keys cannot contain empty path segments.")
    for index, part in enumerate(parts):
        _validate_partition_segment(
            part,
            label="collection" if len(parts) == 4 and index == 0 else "key",
        )
    if len(parts) == 3:
        dataset_slug, file_slug, version = parts
        return PublishPartitionIdentity(
            default_publish_collection(), dataset_slug, file_slug, version
        )
    if len(parts) == 4:
        collection, dataset_slug, file_slug, version = parts
        return PublishPartitionIdentity(collection, dataset_slug, file_slug, version, True)
    raise ValueError(
        "Publish partition keys must be formatted as 'dataset_slug/file_slug/version' "
        "or 'collection/dataset_slug/file_slug/version'."
    )


def parse_publish_partition_key(partition_key: str) -> tuple[str, str, str]:
    identity = parse_publish_partition_identity(partition_key)
    return identity.dataset_slug, identity.file_slug, identity.version


def resolve_publish_partition(
    partition_key: str,
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource | None = None,
) -> tuple[
    PublishPartitionIdentity,
    StagingStorageResource,
    PublishedStorageResource | None,
]:
    """Resolve logical identity and return storage resources scoped to its collection."""
    identity = parse_publish_partition_identity(partition_key)
    collections_configured = bool(os.environ.get("HIFLD_STAGING_COLLECTIONS"))
    if identity.collection_qualified or collections_configured:
        prefix = identity.collection
        staging_storage = staging_storage.model_copy(update={"prefix": prefix})
        if published_storage is not None:
            published_storage = published_storage.model_copy(update={"prefix": prefix})
    return identity, staging_storage, published_storage

ALL_PARTITIONS_DEFS = [PUBLISH_PARTITIONS]
