"""Catalog assets for staged dataset versions."""

from __future__ import annotations

from dagster import AssetKey, MetadataValue, Output, asset

from dagster_hifld.catalog import (
    summarize_staged_catalog,
    write_catalog_metadata,
)
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILES
from dagster_hifld.partitions import PUBLISH_PARTITIONS, parse_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import (
    load_resolved_source_manifest,
    snapshot_source_metadata,
)


def _build_catalog_output_metadata(
    dataset_slug: str,
    file_slug: str,
    version: str,
    description: str,
    quality_manifest: dict,
    data_dictionary: dict,
) -> dict:
    columns = data_dictionary.get("columns", [])
    metadata = {
        "dataset_slug": dataset_slug,
        "file_slug": file_slug,
        "version": version,
        "title": data_dictionary.get("title", file_slug),
        "description": data_dictionary.get("description", description),
        "feature_count": quality_manifest.get("feature_count"),
        "bounds": MetadataValue.json(quality_manifest.get("bounds")),
        "geometry_type": quality_manifest.get("geometry_type"),
        "invalid_geometry_count": quality_manifest.get("invalid_geometry_count"),
        "quality_check_passed": quality_manifest.get("quality_check_passed"),
        "columns_hash": quality_manifest.get("columns_hash"),
        "column_count": len(columns),
        "schema_columns": MetadataValue.json(columns),
    }
    if data_dictionary.get("metadata_sources"):
        metadata["metadata_sources"] = MetadataValue.json(data_dictionary["metadata_sources"])
    if data_dictionary.get("metadata_resolved_from"):
        metadata["metadata_resolved_from"] = MetadataValue.json(data_dictionary["metadata_resolved_from"])
    return metadata


_SUPPORTED_BY_PAIR = {
    (spec.dataset_slug, spec.file_slug): spec for spec in SUPPORTED_DATASET_FILES
}


@asset(
    key=AssetKey(["publish", "catalog"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="catalog",
    description="Generate quality and data dictionary metadata for any staged dataset version.",
)
def publish_catalog(context, staging_storage: StagingStorageResource) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    snapshot_source_metadata(staging_storage, dataset_slug, file_slug, version)
    spec = _SUPPORTED_BY_PAIR.get((dataset_slug, file_slug))
    description = (
        spec.description if spec else f"Staged dataset file {dataset_slug}/{file_slug}."
    )
    resolved_manifest = load_resolved_source_manifest(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    summary = summarize_staged_catalog(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
        dataset_slug,
        source_metadata=resolved_manifest.metadata,
    )
    quality_manifest = summary.quality_manifest
    data_dictionary = summary.data_dictionary
    write_catalog_metadata(
        staging_storage,
        dataset_slug=dataset_slug,
        file_slug=file_slug,
        version=version,
        quality_dict=quality_manifest,
        dictionary_dict=data_dictionary,
    )
    return Output(
        {"version": version},
        metadata=_build_catalog_output_metadata(
            dataset_slug=dataset_slug,
            file_slug=file_slug,
            version=version,
            description=description,
            quality_manifest=quality_manifest,
            data_dictionary=data_dictionary,
        ),
    )


catalog_assets = [publish_catalog]
