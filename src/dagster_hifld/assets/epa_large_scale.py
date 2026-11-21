"""EPA Regions Large Scale from EPA_GEO's maintained 2023 regions service; refresh remains stopped."""
from dagster import AssetKey,AssetSelection,Config,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_large_scale import SLUG,stage_epa_large_scale

class EpaLargeScaleRegionsConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='EPA Regions Large Scale now reads the same EPA_GEO maintained 2023 layer13 as EPA Regions; original fields/IDs/native4326 rings preserved; release unapproved.')
def epa_large_scale_regions(context,config:EpaLargeScaleRegionsConfig,staging_storage:StagingStorageResource):
    result=stage_epa_large_scale(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

epa_large_scale_source_integrity_job=define_asset_job('epa_large_scale_source_integrity',selection=AssetSelection.assets(epa_large_scale_regions))
epa_large_scale_source_integrity_schedule=ScheduleDefinition(name='epa_large_scale_source_integrity_monthly',job=epa_large_scale_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
ASSET_CHECKS = []
INGEST_ASSETS = [epa_large_scale_regions]
JOBS = [epa_large_scale_source_integrity_job]
SCHEDULES = [epa_large_scale_source_integrity_schedule]
