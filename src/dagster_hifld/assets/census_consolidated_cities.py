"""Explicit stopped Census consolidated-city draft; no production activation."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.consolidated_cities import SLUG, stage_consolidated_cities

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Complete seven-state Census2026 source; unapproved successor draft, all original IDs/types/native WKB retained.')
def census_consolidated_cities(context,staging_storage:StagingStorageResource):
    result=stage_consolidated_cities(staging_storage,capture_dir=os.environ.get('HIFLD_CONCITY_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

concity_source_integrity_job=define_asset_job('concity_source_integrity',selection=AssetSelection.assets(census_consolidated_cities))
concity_source_integrity_schedule=ScheduleDefinition(name='concity_source_integrity_monthly',job=concity_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
