"""Explicit faithful FWS CBRS Prohibitions reconstruction; local drafts only."""
from pathlib import Path

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.sources.cbrs_prohibitions import require_local, stage_cbrs_prohibitions_candidate


@asset(key=AssetKey(['ingest','cbrs-prohibitions-1','cbrs-prohibitions-1']),group_name='fws',compute_kind='http',
       description='Official FWS2024-12-19 Prohibitions: 1693 native NAD83 mixed Polygon/MultiPolygon prohibition areas with native date fields. Faithful draft, not a legal determination or accepted HIFLD replacement.',
       config_schema={'capture_dir':Field(str,default_value='')})
def fws_cbrs_prohibitions(context,staging_storage:StagingStorageResource)->dict:
    require_local(staging_storage)
    capture=context.op_config.get('capture_dir','')
    result=stage_cbrs_prohibitions_candidate(staging_storage,capture_dir=Path(capture) if capture else None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[build_publish_partition_key(result['dataset_slug'],result['file_slug'],result['version'])])
    return result


fws_cbrs_prohibitions_draft_refresh_job=define_asset_job('fws_cbrs_prohibitions_draft_refresh',selection=AssetSelection.keys(fws_cbrs_prohibitions.key),
    config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':''}}}})


@schedule(job=fws_cbrs_prohibitions_draft_refresh_job,cron_schedule='0 13 1 * *',execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,required_resource_keys={'staging_storage'})
def fws_cbrs_prohibitions_draft_refresh_schedule(context):
    # Monthly integrity/availability poll; producer updates follow enacted/adopted CBRS maps.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':''}}}})
