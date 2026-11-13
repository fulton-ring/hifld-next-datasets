"""Explicit unapproved USGS producer-successor reconstruction; never polls an archive."""
import os
from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nonferrous_metal_mines import SLUG,stage_nonferrous_metal_mines

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='usgs',compute_kind='local',description='Explicit USGS mineplant successor package as provided, local-only unapproved candidate. Only USGS-owned objects are inputs; the retained published HIFLD copy of the retired layer is comparison context. No authority/datum/schema/ID mapping or release acceptance.')
def nonferrous_metal_mines_candidate(context,staging_storage:StagingStorageResource):
    result=stage_nonferrous_metal_mines(staging_storage,capture_dir=os.environ.get('HIFLD_NONFERROUS_METAL_MINES_CAPTURE_DIR'),package_id=os.environ.get('HIFLD_NONFERROUS_METAL_MINES_PACKAGE_ID'));context.add_output_metadata(result);return result

nonferrous_metal_mines_draft_job=define_asset_job('nonferrous_metal_mines_successor_reconstruction',selection=AssetSelection.assets(nonferrous_metal_mines_candidate))
nonferrous_metal_mines_draft_schedule=ScheduleDefinition(name='nonferrous_metal_mines_successor_integrity',job=nonferrous_metal_mines_draft_job,cron_schedule='0 7 1 * *',execution_timezone='UTC',default_status=DefaultScheduleStatus.STOPPED)
