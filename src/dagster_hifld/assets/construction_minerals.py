"""Explicit unapproved historical preservation reconstruction; never polls an archive."""
import os
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.construction_minerals import SLUG,stage_construction_minerals

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='usgs',compute_kind='local',description='Explicit Stanford historical preservation package as provided, local-only unapproved candidate. No authority/datum/schema/ID mapping or release acceptance.')
def construction_minerals_preservation_candidate(context,staging_storage:StagingStorageResource):
    result=stage_construction_minerals(staging_storage,capture_dir=os.environ.get('HIFLD_CONSTRUCTION_MINERALS_CAPTURE_DIR'),package_id=os.environ.get('HIFLD_CONSTRUCTION_MINERALS_PACKAGE_ID'));context.add_output_metadata(result);return result

construction_minerals_draft_job=define_asset_job('construction_minerals_preservation_reconstruction',selection=AssetSelection.assets(construction_minerals_preservation_candidate))
construction_minerals_draft_schedule=ScheduleDefinition(name='construction_minerals_preservation_integrity',job=construction_minerals_draft_job,cron_schedule='0 6 1 * *',execution_timezone='UTC',default_status=DefaultScheduleStatus.STOPPED)
