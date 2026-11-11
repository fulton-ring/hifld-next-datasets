"""Explicit stopped exact2024 CDP reconstruction; no production activation."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.census_designated_places import SLUG, stage_cdp_candidate

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Complete original Census2024 FileGDB CDPs; all original IDs/native geometry retained, unapproved draft.')
def census_designated_places(context,staging_storage:StagingStorageResource):
    result=stage_cdp_candidate(staging_storage,archive_path=os.environ.get('HIFLD_CDP_ARCHIVE'),receipt_path=os.environ.get('HIFLD_CDP_RECEIPT'))
    context.add_output_metadata(result)
    return result

cdp_source_integrity_job=define_asset_job('cdp_source_integrity',selection=AssetSelection.assets(census_designated_places))
cdp_source_integrity_schedule=ScheduleDefinition(name='cdp_source_integrity_monthly',job=cdp_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
