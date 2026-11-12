"""Explicit official FEMA Region boundary draft, separate from office points."""
from dagster import (AssetCheckResult,AssetKey,AssetSelection,Config,DefaultScheduleStatus,
    ScheduleDefinition,asset,asset_check,define_asset_job)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fema_regions import SLUG,stage_fema_regions

class FemaRegionsConfig(Config):
    capture_directory:str
    refresh_official:bool=False

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='federal_emergency_management_agency',compute_kind='local',
    description='Draft of all10official FEMA region polygons. Native IDs/types/ordered geometry preserved; no release/contract acceptance.')
def fema_regions_official(context,config:FemaRegionsConfig,staging_storage:StagingStorageResource):
    result=stage_fema_regions(staging_storage,config.capture_directory,config.refresh_official)
    context.add_output_metadata(result);return result

@asset_check(asset=fema_regions_official,blocking=True)
def fema_regions_full_quality(fema_regions_official:dict):
    return AssetCheckResult(passed=fema_regions_official['source_invalid_geometry_count']==0,
        metadata={'feature_count':fema_regions_official['feature_count'],'invalid_geometry_count':fema_regions_official['source_invalid_geometry_count'],
            'scope':'Full native population; no geometry repair/exception or contract approval.'})

fema_regions_draft_job=define_asset_job('fema_regions_draft_refresh',selection=AssetSelection.assets(fema_regions_official))
fema_regions_draft_schedule=ScheduleDefinition(name='fema_regions_draft_refresh_schedule',job=fema_regions_draft_job,
    cron_schedule='0 10 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED,
    run_config={'ops':{fema_regions_official.op.name:{'config':{'capture_directory':{'env':'HIFLD_FEMA_REGIONS_CAPTURE_DIR'},'refresh_official':True}}}})
