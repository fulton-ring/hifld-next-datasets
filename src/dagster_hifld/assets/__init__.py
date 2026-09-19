"""HIFLD dataset assets: one per dataset, grouped by agency."""

from __future__ import annotations


def __getattr__(name: str):
    if name != "hifld_dataset_assets":
        raise AttributeError(name)
    from dagster_hifld.assets.catalog import catalog_assets
    from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
    from dagster_hifld.assets.publish import publish_assets

    return [
        *hifld_ingest_assets,
        *catalog_assets,
        *publish_assets,
    ]


__all__ = ["hifld_dataset_assets"]
