"""Complete EPA application-distributed generic region boundaries; unapproved draft."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_regions import SLUG, stage_epa_regions


class EpaRegionsConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',
       description='Complete10 generic EPA region boundaries from explicitly delegated official application; native FID/OBJECTID/types/3857 rings preserved; source/SEGS compatibility unapproved.')
def epa_regions(context,config:EpaRegionsConfig,staging_storage:StagingStorageResource):
    result=stage_epa_regions(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

epa_regions_draft_job=define_asset_job('epa_regions_source_integrity',selection=AssetSelection.assets(epa_regions))
epa_regions_draft_schedule=ScheduleDefinition(name='epa_regions_source_integrity_monthly',job=epa_regions_draft_job,
    cron_schedule='0 10 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
