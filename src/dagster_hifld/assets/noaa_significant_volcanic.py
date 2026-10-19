"""Complete official NOAA significant volcanic events; refresh remains stopped."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.significant_volcanic_events import SLUG, stage_significant_volcanic

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='noaa',compute_kind='http',description='Complete official NOAA NCEI significant eruption/event draft; all original IDs/types/native geometry/BCE and coded date components retained.')
def noaa_significant_volcanic_events(context,staging_storage:StagingStorageResource):
    result=stage_significant_volcanic(staging_storage,capture_dir=os.environ.get('HIFLD_SIGNIFICANT_VOLCANIC_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

significant_volcanic_source_integrity_job=define_asset_job('significant_volcanic_source_integrity',selection=AssetSelection.assets(noaa_significant_volcanic_events))
significant_volcanic_source_integrity_schedule=ScheduleDefinition(name='significant_volcanic_source_integrity_monthly',job=significant_volcanic_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
