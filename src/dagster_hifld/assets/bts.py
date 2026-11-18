"""Explicit source-specific Amtrak NTAD draft pipeline."""
from dagster_hifld.amtrak_extract import (
    bts_amtrak_stations, amtrak_draft_refresh_job, amtrak_draft_refresh_schedule,
)

__all__ = ["bts_amtrak_stations", "amtrak_draft_refresh_job", "amtrak_draft_refresh_schedule"]

ASSET_CHECKS = []
INGEST_ASSETS = [bts_amtrak_stations]
JOBS = [amtrak_draft_refresh_job]
SCHEDULES = [amtrak_draft_refresh_schedule]
