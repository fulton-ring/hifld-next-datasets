"""Canonical dataset/file support list for downstream derived assets."""

from __future__ import annotations

from dataclasses import dataclass

from dagster import AssetKey

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets


@dataclass(frozen=True)
class SupportedDatasetFile:
    dataset_slug: str
    file_slug: str
    description: str
    ingest_asset_key: AssetKey | None = None


def _from_ingest_assets() -> list[SupportedDatasetFile]:
    supported: list[SupportedDatasetFile] = []
    for asset_def in hifld_ingest_assets:
        asset_key = next(iter(asset_def.keys))
        dataset_slug, file_slug = asset_key.path[-2:]
        description = None
        descriptions_by_key = getattr(asset_def, "descriptions_by_key", None) or {}
        description = descriptions_by_key.get(asset_key)
        if not description:
            specs_by_key = getattr(asset_def, "specs_by_key", None) or {}
            spec = specs_by_key.get(asset_key)
            description = getattr(spec, "description", None)
        supported.append(
            SupportedDatasetFile(
                dataset_slug=dataset_slug,
                file_slug=file_slug,
                description=description or f"{dataset_slug} dataset.",
                ingest_asset_key=asset_key,
            )
        )
    return supported


# Backfill-only datasets intentionally supported downstream without a live ingest asset.
STAGING_ONLY_DATASET_FILES = [
    SupportedDatasetFile(
        dataset_slug="119th-congressional-districts",
        file_slug="119th-congressional-districts",
        description="119th Congressional Districts legacy/backfilled dataset.",
    ),
    SupportedDatasetFile(
        dataset_slug="address-ranges",
        file_slug="address-ranges",
        description="Address Ranges legacy/backfilled dataset.",
    ),
    SupportedDatasetFile(
        dataset_slug="agricultural-minerals-operations",
        file_slug="agricultural-minerals-operations",
        description="Agricultural Minerals Operations legacy/backfilled dataset.",
    ),
]

SUPPORTED_DATASET_FILES = [
    *_from_ingest_assets(),
    *STAGING_ONLY_DATASET_FILES,
]

SUPPORTED_DATASET_FILE_PAIRS = [
    (spec.dataset_slug, spec.file_slug) for spec in SUPPORTED_DATASET_FILES
]
