"""FHWA National Bridge Inventory 2025 ASCII reconstruction; unapproved local draft."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_bridge_inventory import SLUG, stage_national_bridge_inventory


class NationalBridgeInventoryConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='fhwa',compute_kind='http',description='Complete FHWA 2025 NBI ASCII reconstruction; preserves all 124 fixed-width Coding Guide items as exact strings and derives points only from Items 16/17 by the official SNBI crosswalk.')
def national_bridge_inventory(context,config:NationalBridgeInventoryConfig,staging_storage:StagingStorageResource):
    result=stage_national_bridge_inventory(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

nbi_source_integrity_job=define_asset_job('nbi_source_integrity',selection=AssetSelection.assets(national_bridge_inventory))
nbi_source_integrity_schedule=ScheduleDefinition(name='nbi_source_integrity_quarterly',job=nbi_source_integrity_job,cron_schedule='0 8 15 1,4,7,10 *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
