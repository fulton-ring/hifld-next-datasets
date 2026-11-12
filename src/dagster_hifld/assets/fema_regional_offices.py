"""Complete FEMA office points; unapproved draft refresh schedule stays stopped."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fema_regional_offices import SLUG, stage_fema_regional_offices

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='fema',compute_kind='http',description='Complete official FEMA office-point layer0 with original IDs/types/native4326; no acceptance.')
def fema_regional_offices(context,staging_storage:StagingStorageResource):
    result=stage_fema_regional_offices(staging_storage,capture_dir=os.environ.get('HIFLD_FEMA_OFFICES_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

fema_offices_draft_job=define_asset_job('fema_regional_offices_draft_refresh',selection=AssetSelection.assets(fema_regional_offices))
fema_offices_draft_schedule=ScheduleDefinition(name='fema_regional_offices_draft_weekly',job=fema_offices_draft_job,cron_schedule='0 6 * * 1',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
