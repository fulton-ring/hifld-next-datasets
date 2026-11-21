"""Explicit historical crushed-stone preservation reconstruction; no archive refresh."""
import os
from dagster import AssetKey,AssetSelection,Config,DefaultScheduleStatus,ScheduleDefinition,asset,define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.crushed_stone import SLUG,stage_crushed_stone


class CrushedStoneConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='usgs',compute_kind='local',description='Explicit unapproved2324-record Stanford crushed-stone historical package; published766MINOP identity correction requires a decision.')
def usgs_crushed_stone_preservation(context,config:CrushedStoneConfig,staging_storage:StagingStorageResource):
    result=stage_crushed_stone(staging_storage,capture_dir=config.capture_directory or None,package_id=os.environ.get('HIFLD_CRUSHED_STONE_PACKAGE_ID'))
    context.add_output_metadata(result)
    return result

crushed_stone_preservation_job=define_asset_job('crushed_stone_preservation_reconstruct',selection=AssetSelection.assets(usgs_crushed_stone_preservation))
crushed_stone_preservation_schedule=ScheduleDefinition(name='crushed_stone_preservation_integrity',job=crushed_stone_preservation_job,cron_schedule='0 6 * * 1',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
