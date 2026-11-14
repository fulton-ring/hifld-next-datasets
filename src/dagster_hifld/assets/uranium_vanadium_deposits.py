"""Parked ingest registration for the Uranium and Vanadium Deposits layer; always fails closed."""
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.uranium_vanadium_deposits import PARK_REASON,SLUG,stage_uranium_vanadium_deposits

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='usgs',compute_kind='local',description='Parked; no maintained USGS producer publication (MRDS frozen with systematic updates ceased 2011; USMIN developing with no uranium or vanadium commodity packages; ARDF Alaska-only; Earth MRI focus-area and curated 681-deposit releases are materially different products). Every materialization fails closed with '+PARK_REASON+'. No fetching, staging or acceptance.')
def uranium_vanadium_deposits_candidate(context,staging_storage:StagingStorageResource):
    stage_uranium_vanadium_deposits(staging_storage)

uranium_vanadium_deposits_parked_job=define_asset_job('uranium_vanadium_deposits_parked_refusal',selection=AssetSelection.assets(uranium_vanadium_deposits_candidate))
uranium_vanadium_deposits_parked_schedule=ScheduleDefinition(name='uranium_vanadium_deposits_parked_monthly',job=uranium_vanadium_deposits_parked_job,cron_schedule='0 7 1 * *',execution_timezone='UTC',default_status=DefaultScheduleStatus.STOPPED)
INGEST_ASSETS=[uranium_vanadium_deposits_candidate]
JOBS=[uranium_vanadium_deposits_parked_job]
SCHEDULES=[uranium_vanadium_deposits_parked_schedule]
