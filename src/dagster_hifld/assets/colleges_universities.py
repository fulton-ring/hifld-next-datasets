"""Complete official NCES successor draft; schedule remains stopped."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.colleges_universities import SLUG, stage_colleges

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='department_of_education',compute_kind='http',description='Complete NCES EDGE2025–2026 postsecondary source. Unapproved changed successor to ORNL2020–2021; source IDs/types/native WKB retained.')
def nces_colleges_universities(context,staging_storage:StagingStorageResource):
    result=stage_colleges(staging_storage,capture_dir=os.environ.get('HIFLD_COLLEGES_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

colleges_source_integrity_job=define_asset_job('colleges_source_integrity',selection=AssetSelection.assets(nces_colleges_universities))
colleges_source_integrity_schedule=ScheduleDefinition(name='colleges_source_integrity_monthly',job=colleges_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
