"""Complete official BTS NCFO Ferry Terminals, gated local drafts only."""
from pathlib import Path

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferry_terminals import require_local, stage_ferry_terminals_candidate


@asset(key=AssetKey(['ingest','ferry-terminals','ferry-terminals']), group_name='bts',compute_kind='http',
       description='All845nativeBTSNCFO2020operator-terminal rows; all22fields, original string IDs, native4326/int32/float64/nulls. Draft; catalog2022claim conflicts with actual source edition.',
       config_schema={'capture_dir':Field(str,default_value='')})
def bts_ferry_terminals(context,staging_storage:StagingStorageResource)->dict:
    require_local(staging_storage)
    capture=context.op_config.get('capture_dir','')
    return stage_ferry_terminals_candidate(staging_storage,capture_dir=Path(capture) if capture else None)


bts_ferry_terminals_draft_refresh_job=define_asset_job('bts_ferry_terminals_draft_refresh',selection=AssetSelection.keys(bts_ferry_terminals.key),
    config={'ops':{bts_ferry_terminals.op.name:{'config':{'capture_dir':''}}}})


@schedule(job=bts_ferry_terminals_draft_refresh_job,cron_schedule='0 13 1 * *',execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,required_resource_keys={'staging_storage'})
def bts_ferry_terminals_draft_refresh_schedule(context):
    # Proposed source-availability/integrity poll, not a claim that BTS releases data monthly.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops':{bts_ferry_terminals.op.name:{'config':{'capture_dir':''}}}})
