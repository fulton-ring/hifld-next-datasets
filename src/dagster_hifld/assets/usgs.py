"""Explicit USGS static producer-release reconstruction; local drafts only."""
from pathlib import Path

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.aquifers import require_local, stage_aquifers_candidate


@asset(key=AssetKey(['ingest', 'aquifers', 'aquifers']), group_name='usgs', compute_kind='http',
       description='Faithful USGS2003 principal aquifers static producer-release draft; all4,637 polygons and source IDs retained. No HIFLD migration or production approval.',
       config_schema={'archive_path': Field(str, default_value=''), 'receipt_path': Field(str, default_value='')})
def usgs_aquifers(context, staging_storage: StagingStorageResource) -> dict:
    require_local(staging_storage)
    config = context.op_config
    return stage_aquifers_candidate(staging_storage,
        archive_path=Path(config['archive_path']) if config.get('archive_path') else None,
        receipt_path=Path(config['receipt_path']) if config.get('receipt_path') else None)


usgs_aquifers_draft_refresh_job = define_asset_job('usgs_aquifers_draft_refresh', selection=AssetSelection.keys(usgs_aquifers.key),
    config={'ops': {usgs_aquifers.op.name: {'config': {'archive_path': '', 'receipt_path': ''}}}})


@schedule(job=usgs_aquifers_draft_refresh_job, cron_schedule='0 7 1 * *', execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED, required_resource_keys={'staging_storage'})
def usgs_aquifers_draft_refresh_schedule(context):
    # Proposed monthly integrity/availability poll, not an assertion of source updates.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops': {usgs_aquifers.op.name: {'config': {'archive_path': '', 'receipt_path': ''}}}})
