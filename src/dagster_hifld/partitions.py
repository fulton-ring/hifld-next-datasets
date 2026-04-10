"""Shared dynamic partition definitions for versioned dataset assets."""

from __future__ import annotations

from dagster import DynamicPartitionsDefinition

from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS

DATASET_FILE_PAIRS = SUPPORTED_DATASET_FILE_PAIRS


def get_partition_name(dataset_slug: str, file_slug: str) -> str:
    return f"{dataset_slug}--{file_slug}"


CATALOG_PARTITIONS = {
    get_partition_name(dataset_slug, file_slug): DynamicPartitionsDefinition(
        name=get_partition_name(dataset_slug, file_slug)
    )
    for dataset_slug, file_slug in DATASET_FILE_PAIRS
}

PARTITIONS_BY_PAIR = {
    (dataset_slug, file_slug): CATALOG_PARTITIONS[
        get_partition_name(dataset_slug, file_slug)
    ]
    for dataset_slug, file_slug in DATASET_FILE_PAIRS
}

ALL_PARTITIONS_DEFS = list(CATALOG_PARTITIONS.values())
