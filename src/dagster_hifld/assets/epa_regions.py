"""EPA Regions from EPA_GEO's maintained 2023 regions service; refresh remains stopped."""
from dagster import AssetKey,AssetSelection,Config,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_regions import SLUG,stage_epa_regions

class EpaRegionsConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='Complete ten EPA regions from EPA_GEO maintained 2023 layer13; original fields/IDs/native4326 rings preserved; release unapproved.')
def epa_regions(context,config:EpaRegionsConfig,staging_storage:StagingStorageResource):
    result=stage_epa_regions(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

epa_regions_draft_job=define_asset_job('epa_regions_source_integrity',selection=AssetSelection.assets(epa_regions))
epa_regions_draft_schedule=ScheduleDefinition(name='epa_regions_source_integrity_monthly',job=epa_regions_draft_job,cron_schedule='0 10 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
ASSET_CHECKS = []
INGEST_ASSETS = [epa_regions]
JOBS = [epa_regions_draft_job]
SCHEDULES = [epa_regions_draft_schedule]
