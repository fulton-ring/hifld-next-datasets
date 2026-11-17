"""Explicit stopped Census estate draft; no production activation."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.us_virgin_islands_estates import SLUG, stage_us_virgin_islands_estates


class UsVirginIslandsEstatesConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Census TIGER2026 U.S. Virgin Islands estate source; unapproved draft, all original IDs/types/native WKB retained.')
def us_virgin_islands_estates(context,config:UsVirginIslandsEstatesConfig,staging_storage:StagingStorageResource):
    result=stage_us_virgin_islands_estates(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

estate_source_integrity_job=define_asset_job('estate_source_integrity',selection=AssetSelection.assets(us_virgin_islands_estates))
estate_source_integrity_schedule=ScheduleDefinition(name='estate_source_integrity_monthly',job=estate_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [us_virgin_islands_estates]
JOBS = [estate_source_integrity_job]
SCHEDULES = [estate_source_integrity_schedule]
