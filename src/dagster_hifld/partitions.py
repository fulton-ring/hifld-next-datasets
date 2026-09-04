"""Shared dynamic partition definitions for versioned publish assets."""

from __future__ import annotations

import re

from dagster import DynamicPartitionsDefinition

from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS

SEMVER_VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
SOURCE_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_DIRS
PUBLISH_PARTITION_NAME = "publish_dataset_file_version"
PUBLISH_PARTITIONS = DynamicPartitionsDefinition(name=PUBLISH_PARTITION_NAME)


def build_publish_partition_key(dataset_slug: str, file_slug: str, version: str) -> str:
    return f"{dataset_slug}/{file_slug}/{version}"


def parse_publish_partition_key(partition_key: str) -> tuple[str, str, str]:
    parts = [part for part in partition_key.split("/") if part]
    if len(parts) != 3:
        raise ValueError(
            "Publish partition keys must be formatted as "
            "'dataset_slug/file_slug/version'."
        )
    return parts[0], parts[1], parts[2]

ALL_PARTITIONS_DEFS = [PUBLISH_PARTITIONS]
