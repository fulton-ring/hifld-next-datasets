"""EPA Regions Small Scale from EPA_GEO's maintained 2023 regions service; refresh remains stopped."""
from dagster import AssetKey,AssetSelection,Config,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_small_scale import SLUG,stage_epa_small_scale

class EpaSmallScaleRegionsConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='EPA Regions Small Scale now reads the same EPA_GEO maintained 2023 layer13 as EPA Regions; original fields/IDs/native4326 rings preserved; release unapproved.')
def epa_small_scale_regions(context,config:EpaSmallScaleRegionsConfig,staging_storage:StagingStorageResource):
    result=stage_epa_small_scale(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

epa_small_scale_source_integrity_job=define_asset_job('epa_small_scale_source_integrity',selection=AssetSelection.assets(epa_small_scale_regions))
epa_small_scale_source_integrity_schedule=ScheduleDefinition(name='epa_small_scale_source_integrity_monthly',job=epa_small_scale_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
ASSET_CHECKS = []
INGEST_ASSETS = [epa_small_scale_regions]
JOBS = [epa_small_scale_source_integrity_job]
SCHEDULES = [epa_small_scale_source_integrity_schedule]
