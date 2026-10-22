"""Explicit stopped exact2024 Incorporated Places reconstruction; no production activation."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.incorporated_places import SLUG, stage_incorporated_places_candidate

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Complete original Census2024 FileGDB Incorporated_Place layer; all original IDs/native geometry retained, unapproved draft.')
def incorporated_places(context,staging_storage:StagingStorageResource):
    result=stage_incorporated_places_candidate(staging_storage,archive_path=os.environ.get('HIFLD_INCORPORATED_PLACES_ARCHIVE'),receipt_path=os.environ.get('HIFLD_INCORPORATED_PLACES_RECEIPT'))
    context.add_output_metadata(result)
    return result

incorporated_places_source_integrity_job=define_asset_job('incorporated_places_source_integrity',selection=AssetSelection.assets(incorporated_places))
incorporated_places_source_integrity_schedule=ScheduleDefinition(name='incorporated_places_source_integrity_monthly',job=incorporated_places_source_integrity_job,cron_schedule='0 10 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
