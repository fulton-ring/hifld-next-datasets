"""Explicit staging-only refresh orchestration shared by migrated sources.

A refresh schedule runs the dataset's ingest asset only. Shared catalog, format
and release-metadata assets run per registered candidate partition; verification,
promotion and Portolan catalog application are separate manual steps. Keeping
that boundary in one helper makes it testable instead of repeating a job
selection in every dataset module.
"""
from __future__ import annotations

from dagster import AssetKey, AssetSelection, define_asset_job

STAGING_SHARED_ASSET_KEYS = frozenset({
    AssetKey(["publish", "catalog"]),
    AssetKey(["publish", "formats", "geopackage"]),
    AssetKey(["publish", "formats", "geoparquet"]),
    AssetKey(["publish", "formats", "pmtiles"]),
    AssetKey(["publish", "formats", "shapefile_zip"]),
    AssetKey(["publish", "release_metadata"]),
})
PUBLICATION_ASSET_KEYS = frozenset({
    AssetKey(["publish", "verify"]),
    AssetKey(["publish", "promote"]),
    AssetKey(["publish", "portolan_catalog"]),
})


def staging_capture_selection(ingest_asset) -> AssetSelection:
    """Select only the capture asset; it writes candidates to staging."""
    keys = set(ingest_asset.keys)
    if keys & PUBLICATION_ASSET_KEYS:
        raise ValueError("publication_asset_in_staging_capture_selection")
    if keys & STAGING_SHARED_ASSET_KEYS:
        raise ValueError("shared_staging_asset_in_capture_selection")
    return AssetSelection.assets(ingest_asset)


def define_staging_capture_job(name: str, ingest_asset):
    """Build a refresh job that cannot select verification or publication."""
    return define_asset_job(name, selection=staging_capture_selection(ingest_asset))
