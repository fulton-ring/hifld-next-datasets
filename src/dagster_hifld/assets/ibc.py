"""Explicit producer-hosted IBC boundary reconstruction, local drafts only."""
from pathlib import Path

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.canada_us_border import require_local, stage_canada_us_border_candidate


@asset(key=AssetKey(['ingest','canada-and-us-border','canada-and-us-border']), group_name='ibc', compute_kind='http',
       description='IBC1.3 static official publication; all30native NAD83 mapping segments. Draft only, not legal boundary definition.',
       config_schema={'capture_dir':Field(str,default_value='')})
def ibc_canada_us_border(context,staging_storage:StagingStorageResource)->dict:
    require_local(staging_storage)
    capture=context.op_config.get('capture_dir','')
    return stage_canada_us_border_candidate(staging_storage,capture_dir=Path(capture) if capture else None)


ibc_boundary_draft_refresh_job=define_asset_job('ibc_boundary_draft_refresh',selection=AssetSelection.keys(ibc_canada_us_border.key),
    config={'ops':{ibc_canada_us_border.op.name:{'config':{'capture_dir':''}}}})


@schedule(job=ibc_boundary_draft_refresh_job,cron_schedule='0 11 1 * *',execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,required_resource_keys={'staging_storage'})
def ibc_boundary_draft_refresh_schedule(context):
    # Proposed monthly availability/integrity poll, not a promise of monthly producer updates.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops':{ibc_canada_us_border.op.name:{'config':{'capture_dir':''}}}})
