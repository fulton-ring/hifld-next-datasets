"""Stopped draft-refresh asset for the USGS GNIS Physical Points."""

from dagster_hifld.assets.gnis_historical_points_family import (
    build_gnis_historical_points_assets,
)
from dagster_hifld.sources import gnis_historical_physical_points as source

(
    gnis_historical_physical_points,
    gnis_historical_physical_points_complete,
    gnis_historical_physical_points_approved_baseline_drift,
    gnis_historical_physical_points_draft_job,
    gnis_historical_physical_points_draft_schedule,
) = build_gnis_historical_points_assets(
    source,
    description=(
        "USGS The National Map Gazetteer (GNIS) geonames MapServer layer 14 "
        "'Historical Physical Points' (1,250 source features, complete "
        "resumable capture 2026-11-13) as the "
        "maintained producer publication named by the DHS HIFLD Open crosswalk for "
        "the inventory entry nmg/historical-physical-points-1. "
        "Unapproved faithful candidate; baselines pinned from the dated complete "
        "capture. Succession, freshness, schema/ID contract, rights and release "
        "decisions remain pending."
    ),
)

INGEST_ASSETS = [gnis_historical_physical_points]
ASSET_CHECKS = [gnis_historical_physical_points_complete,
              gnis_historical_physical_points_approved_baseline_drift]
JOBS = [gnis_historical_physical_points_draft_job]
SCHEDULES = [gnis_historical_physical_points_draft_schedule]
