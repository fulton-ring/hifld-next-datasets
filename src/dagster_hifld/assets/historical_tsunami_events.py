"""Explicit NOAA NCEI event-layer draft; scheduled integrity refresh stays stopped."""
import os
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.historical_tsunami_events import SLUG,stage_tsunami_events
@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='noaa',compute_kind='http',description='Complete delegated NOAA NCEI event locations; original IDs/types/native3857 geometry retained, no acceptance.')
def noaa_historical_tsunami_events(context,staging_storage:StagingStorageResource):
    result=stage_tsunami_events(staging_storage,capture_dir=os.environ.get('HIFLD_TSUNAMI_EVENTS_CAPTURE_DIR'));context.add_output_metadata(result);return result
tsunami_events_draft_job=define_asset_job('tsunami_events_draft_refresh',selection=AssetSelection.assets(noaa_historical_tsunami_events))
tsunami_events_draft_schedule=ScheduleDefinition(name='tsunami_events_draft_weekly',job=tsunami_events_draft_job,cron_schedule='0 6 * * 1',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
