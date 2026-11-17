"""Complete official NOAA tide station archival roster draft; refresh stopped."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.tsunami_tide_stations import SLUG, stage_tide_stations


class NoaaTsunamiTideStationsConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='noaa',compute_kind='http',description='Complete official NOAA tsunami-capable tide station roster draft preserving all IDs, fields and native coordinates. Historical/current-operational and release compatibility remain unapproved.')
def noaa_tsunami_tide_stations(context,config:NoaaTsunamiTideStationsConfig,staging_storage:StagingStorageResource):
    result=stage_tide_stations(staging_storage,capture_dir=config.capture_directory or None);context.add_output_metadata(result);return result

tsunami_tide_source_integrity_job=define_asset_job('tsunami_tide_source_integrity',selection=AssetSelection.assets(noaa_tsunami_tide_stations))
tsunami_tide_source_integrity_schedule=ScheduleDefinition(name='tsunami_tide_source_integrity_weekly',job=tsunami_tide_source_integrity_job,cron_schedule='0 11 * * 1',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS=[noaa_tsunami_tide_stations]
JOBS=[tsunami_tide_source_integrity_job]
SCHEDULES=[tsunami_tide_source_integrity_schedule]
