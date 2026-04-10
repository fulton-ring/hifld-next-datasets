"""Publish assets for staged dataset versions."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey, DynamicPartitionsDefinition, Output, asset

from dagster_hifld.conversion import process_staged_dataset_version
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILES
from dagster_hifld.partitions import PARTITIONS_BY_PAIR
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

_PROMOTED_SOURCE_FORMAT_DIRS = {
    "shapefile",
    "geojson",
    "file_geodatabase",
    "unknown",
}


def _copy_metadata_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[str]:
    copied: list[str] = []
    for filename in ("quality_manifest.json", "data_dictionary.json"):
        rel_path = f"metadata/{filename}"
        contents = staging_storage.read_bytes(dataset_slug, file_slug, version, rel_path)
        copied.append(
            published_storage.write(dataset_slug, file_slug, version, rel_path, contents)
        )
    return copied


def _copy_source_format_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    keys: list[str],
) -> list[str]:
    copied: list[str] = []
    version_prefix = f"{dataset_slug}/{file_slug}/{version}/"
    for key in keys:
        if "/metadata/" in key:
            continue
        rel_path = key.removeprefix(version_prefix)
        if not rel_path:
            continue
        format_dir = Path(rel_path).parts[0]
        if format_dir not in _PROMOTED_SOURCE_FORMAT_DIRS:
            continue
        contents = staging_storage.read_bytes(dataset_slug, file_slug, version, key)
        copied.append(
            published_storage.write(dataset_slug, file_slug, version, rel_path, contents)
        )
    return copied


def _make_publish_asset(
    dataset_slug: str,
    file_slug: str,
    partitions_def: DynamicPartitionsDefinition,
):
    @asset(
        key=AssetKey(["publish", dataset_slug, file_slug]),
        partitions_def=partitions_def,
        group_name="publish",
        deps=[AssetKey(["catalog", dataset_slug, file_slug])],
        description=f"Publish derived formats for {dataset_slug}.",
    )
    def _publish_asset(
        context,
        staging_storage: StagingStorageResource,
        published_storage: PublishedStorageResource,
    ) -> Output[dict]:
        version = context.partition_key
        keys = staging_storage.list_keys(dataset_slug, file_slug, version)
        existing_published_keys = published_storage.list_keys(dataset_slug, file_slug, version)
        if existing_published_keys:
            raise ValueError(
                f"Refusing to overwrite published files for {dataset_slug}/{file_slug}/{version}."
            )
        result = process_staged_dataset_version(
            staging_storage=staging_storage,
            published_storage=published_storage,
            keys=keys,
        )
        if not result.get("success"):
            raise ValueError(
                f"Failed publishing {dataset_slug}/{file_slug}/{version}: {result.get('error')}"
            )
        _copy_source_format_files(
            staging_storage=staging_storage,
            published_storage=published_storage,
            dataset_slug=dataset_slug,
            file_slug=file_slug,
            version=version,
            keys=keys,
        )
        _copy_metadata_files(
            staging_storage,
            published_storage,
            dataset_slug=dataset_slug,
            file_slug=file_slug,
            version=version,
        )
        return Output(
            {"version": version},
            metadata={
                "dataset_slug": dataset_slug,
                "file_slug": file_slug,
                "version": version,
            },
        )

    return _publish_asset


publish_assets = [
    _make_publish_asset(
        spec.dataset_slug,
        spec.file_slug,
        PARTITIONS_BY_PAIR[(spec.dataset_slug, spec.file_slug)],
    )
    for spec in SUPPORTED_DATASET_FILES
]
