"""Explicit producer-hosted NCEI current DART reconstruction, local drafts only."""
from pathlib import Path

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.current_dart import require_local, stage_current_dart_candidate


@asset(key=AssetKey(['ingest','current-dart-deployments','current-dart-deployments']), group_name='noaa', compute_kind='http',
       description='Complete62-site NCEI delegated current DART layer, native WebMercator, all source IDs. Draft only; release mapping unapproved.',
       config_schema={'capture_dir':Field(str,default_value='')})
def noaa_current_dart(context,staging_storage:StagingStorageResource)->dict:
    require_local(staging_storage)
    capture=context.op_config.get('capture_dir','')
    return stage_current_dart_candidate(staging_storage,capture_dir=Path(capture) if capture else None)


noaa_dart_draft_refresh_job=define_asset_job('noaa_dart_draft_refresh',selection=AssetSelection.keys(noaa_current_dart.key),
    config={'ops':{noaa_current_dart.op.name:{'config':{'capture_dir':''}}}})


@schedule(job=noaa_dart_draft_refresh_job,cron_schedule='0 10 * * 1',execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,required_resource_keys={'staging_storage'})
def noaa_dart_draft_refresh_schedule(context):
    # Proposed weekly availability/integrity poll; not a claim that metadata timestamps are data releases.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops':{noaa_current_dart.op.name:{'config':{'capture_dir':''}}}})
