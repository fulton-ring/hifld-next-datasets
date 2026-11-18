"""FHWA National Bridge Inventory SNBI reconstruction; unapproved local draft."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_bridge_inventory import SLUG, stage_national_bridge_inventory


class NationalBridgeInventoryConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='fhwa',compute_kind='http',description='Complete FHWA NBI SNBI reconstruction; preserves every top-level SNBI field at its native JSON type, keeps nested SNBI arrays as JSON text and derives points from B.L.05/B.L.06.')
def national_bridge_inventory(context,config:NationalBridgeInventoryConfig,staging_storage:StagingStorageResource):
    result=stage_national_bridge_inventory(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

nbi_source_integrity_job=define_asset_job('nbi_source_integrity',selection=AssetSelection.assets(national_bridge_inventory))
nbi_source_integrity_schedule=ScheduleDefinition(name='nbi_source_integrity_quarterly',job=nbi_source_integrity_job,cron_schedule='0 8 15 1,4,7,10 *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
ASSET_CHECKS = []
INGEST_ASSETS = [national_bridge_inventory]
JOBS = [nbi_source_integrity_job]
SCHEDULES = [nbi_source_integrity_schedule]
