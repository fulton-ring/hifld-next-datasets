"""Parked ingest registration for the retired USGS Nonferrous Metal Processing Plants layer; always fails closed."""
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nonferrous_metal_processing_plants import PARK_REASON,SLUG,stage_nonferrous_metal_processing_plants

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='usgs',compute_kind='local',description='Parked; no maintained USGS producer publication (original National Atlas release retired; producer mineplant survey publication declares a one-time release with no planned refresh; producer 2026 NMIC release has no coordinates). Every materialization fails closed with '+PARK_REASON+'. No fetching, staging or acceptance.')
def nonferrous_metal_processing_plants_candidate(context,staging_storage:StagingStorageResource):
    stage_nonferrous_metal_processing_plants(staging_storage)

nonferrous_metal_processing_plants_parked_job=define_asset_job('nonferrous_metal_processing_plants_parked_refusal',selection=AssetSelection.assets(nonferrous_metal_processing_plants_candidate))
nonferrous_metal_processing_plants_parked_schedule=ScheduleDefinition(name='nonferrous_metal_processing_plants_parked_monthly',job=nonferrous_metal_processing_plants_parked_job,cron_schedule='0 7 1 * *',execution_timezone='UTC',default_status=DefaultScheduleStatus.STOPPED)
INGEST_ASSETS=[nonferrous_metal_processing_plants_candidate]
JOBS=[nonferrous_metal_processing_plants_parked_job]
SCHEDULES=[nonferrous_metal_processing_plants_parked_schedule]
