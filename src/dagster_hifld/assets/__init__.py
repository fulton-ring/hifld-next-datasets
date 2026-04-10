"""HIFLD dataset assets: one per dataset, grouped by agency (see agency modules)."""

from dagster_hifld.assets.catalog import catalog_assets
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.publish import publish_assets

hifld_dataset_assets = [
    *hifld_ingest_assets,
    *catalog_assets,
    *publish_assets,
]
