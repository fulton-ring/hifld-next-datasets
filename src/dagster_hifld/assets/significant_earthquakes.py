"""Complete NOAA earthquake draft; release remains gated."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.sources.significant_earthquakes import SLUG, stage_significant_earthquakes

class EarthquakeConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='noaa',compute_kind='http',description='Complete current official NOAA NCEI significant earthquakes draft; source IDs, physical declarations, BCE/date literals and native3857 geometry retained.')
def significant_earthquakes(context,config:EarthquakeConfig,staging_storage:StagingStorageResource):
    result=stage_significant_earthquakes(staging_storage,capture_dir=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[build_publish_partition_key(SLUG,SLUG,result['version'])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=significant_earthquakes,blocking=True)
def significant_earthquakes_full_quality(significant_earthquakes:dict):
    return AssetCheckResult(passed=significant_earthquakes['source_null_geometry_count']==0,metadata={'full_population':significant_earthquakes['feature_count'],'null_geometry_count':significant_earthquakes['source_null_geometry_count'],'approval_scope':'Full geometry quality only; source, baseline, contract and release remain unaccepted.'})

significant_earthquakes_draft_job=define_asset_job('significant_earthquakes_draft_refresh',selection=AssetSelection.assets(significant_earthquakes))
significant_earthquakes_draft_schedule=ScheduleDefinition(name='significant_earthquakes_draft_refresh_schedule',job=significant_earthquakes_draft_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
