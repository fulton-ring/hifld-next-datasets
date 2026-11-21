"""Complete official NCES successor draft; schedule remains stopped."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.colleges_universities import SLUG, stage_colleges

class CollegesUniversitiesConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='department_of_education',compute_kind='http',description='Discover the latest published NCES EDGE postsecondary school-year file from official SchoolLocations anchors and preserve the complete native reporting-institution population. Unapproved ORNL successor; schedule stopped and release gates closed.')
def nces_colleges_universities(context,config:CollegesUniversitiesConfig,staging_storage:StagingStorageResource):
    result=stage_colleges(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

colleges_source_integrity_job=define_asset_job('colleges_source_integrity',selection=AssetSelection.assets(nces_colleges_universities))
colleges_source_integrity_schedule=ScheduleDefinition(name='colleges_source_integrity_monthly',job=colleges_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

ASSET_CHECKS = []
INGEST_ASSETS = [nces_colleges_universities]
JOBS = [colleges_source_integrity_job]
SCHEDULES = [colleges_source_integrity_schedule]
