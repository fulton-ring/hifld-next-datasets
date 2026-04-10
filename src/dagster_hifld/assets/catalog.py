"""Catalog assets for staged dataset versions."""

from __future__ import annotations

from dagster import AssetKey, DynamicPartitionsDefinition, MetadataValue, Output, asset

from dagster_hifld.catalog import (
    generate_data_dictionary,
    generate_quality_manifest,
    load_staged_geodata,
    write_catalog_metadata,
)
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILES
from dagster_hifld.partitions import PARTITIONS_BY_PAIR
from dagster_hifld.resources import StagingStorageResource


def _build_catalog_output_metadata(
    dataset_slug: str,
    file_slug: str,
    version: str,
    description: str,
    quality_manifest: dict,
    data_dictionary: dict,
) -> dict:
    columns = data_dictionary.get("columns", [])
    return {
        "dataset_slug": dataset_slug,
        "file_slug": file_slug,
        "version": version,
        "description": description,
        "feature_count": quality_manifest.get("feature_count"),
        "bounds": MetadataValue.json(quality_manifest.get("bounds")),
        "geometry_type": quality_manifest.get("geometry_type"),
        "invalid_geometry_count": quality_manifest.get("invalid_geometry_count"),
        "quality_check_passed": quality_manifest.get("quality_check_passed"),
        "columns_hash": quality_manifest.get("columns_hash"),
        "column_count": len(columns),
        "schema_columns": MetadataValue.json(columns),
    }


def _make_catalog_asset(
    dataset_slug: str,
    file_slug: str,
    ingest_asset_key: AssetKey | None,
    description: str,
    partitions_def: DynamicPartitionsDefinition,
):
    @asset(
        key=AssetKey(["catalog", dataset_slug, file_slug]),
        partitions_def=partitions_def,
        group_name="catalog",
        deps=[ingest_asset_key] if ingest_asset_key is not None else None,
        description=f"Generate quality and data dictionary metadata for {dataset_slug}.",
    )
    def _catalog_asset(
        context, staging_storage: StagingStorageResource
    ) -> Output[dict]:
        version = context.partition_key
        gdf = load_staged_geodata(staging_storage, dataset_slug, file_slug, version)
        quality_manifest = generate_quality_manifest(gdf)
        data_dictionary = generate_data_dictionary(gdf, dataset_slug)
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

    return _catalog_asset


catalog_assets = [
    _make_catalog_asset(
        spec.dataset_slug,
        spec.file_slug,
        spec.ingest_asset_key,
        spec.description,
        PARTITIONS_BY_PAIR[(spec.dataset_slug, spec.file_slug)],
    )
    for spec in SUPPORTED_DATASET_FILES
]
