"""Explicit stopped Census consolidated-city draft; no production activation."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.consolidated_cities import SLUG, stage_consolidated_cities


class CensusConsolidatedCitiesConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Complete seven-state Census2026 source; unapproved successor draft, all original IDs/types/native WKB retained.')
def census_consolidated_cities(context,config:CensusConsolidatedCitiesConfig,staging_storage:StagingStorageResource):
    result=stage_consolidated_cities(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

concity_source_integrity_job=define_asset_job('concity_source_integrity',selection=AssetSelection.assets(census_consolidated_cities))
concity_source_integrity_schedule=ScheduleDefinition(name='concity_source_integrity_monthly',job=concity_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS=[census_consolidated_cities]
JOBS=[concity_source_integrity_job]
SCHEDULES=[concity_source_integrity_schedule]
