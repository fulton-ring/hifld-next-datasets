"""Explicit stopped draft of complete Census elementary-school districts."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.elementary_school_districts import SLUG, stage_elementary_districts

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',
       description='All25official CensusTIGER2026 elementary-district state publications; local unapproved draft, all original IDs/types/native geometries retained.')
def census_elementary_districts(context,staging_storage:StagingStorageResource):
    result=stage_elementary_districts(staging_storage,capture_dir=os.environ.get('HIFLD_ELSD_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

elsd_source_integrity_job=define_asset_job('elsd_source_integrity',selection=AssetSelection.assets(census_elementary_districts))
elsd_source_integrity_schedule=ScheduleDefinition(name='elsd_source_integrity_monthly',job=elsd_source_integrity_job,
    cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
