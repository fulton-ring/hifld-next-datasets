"""Explicit exact EPA small-scale regions; refresh remains stopped."""
import os
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_small_scale import SLUG,stage_epa_small_scale

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='Exact EPA_Locations layer2 small-scale regions; original fields/IDs/native4269 rings and producer2011TIGER lineage retained; release unapproved.')
def epa_small_scale_regions(context,staging_storage:StagingStorageResource):
    result=stage_epa_small_scale(staging_storage,capture_dir=os.environ.get('HIFLD_EPA_SMALL_SCALE_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

epa_small_scale_source_integrity_job=define_asset_job('epa_small_scale_source_integrity',selection=AssetSelection.assets(epa_small_scale_regions))
epa_small_scale_source_integrity_schedule=ScheduleDefinition(name='epa_small_scale_source_integrity_monthly',job=epa_small_scale_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
