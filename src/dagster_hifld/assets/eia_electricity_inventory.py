"""Stopped EIA-860M drafts; construction witnesses currently require local staging.

Remote staging is rejected before HTTP or writes. This does not implement a GCS
construction protocol or grant approval for release or publication.
"""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.eia_electricity_inventory import DATASET_SLUG, GENERATOR_FILE, PLANT_FILE, capture_inventory

class EIAInventoryConfig(Config):
    capture_directory: str = ''
    replay: bool = False


def _capture(context, config, storage, file_slug):
    capture=capture_inventory(storage,capture_directory=config.capture_directory or None,replay=config.replay)
    result=next(item for item in capture['candidates'] if item['file_slug']==file_slug)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[build_publish_partition_key(DATASET_SLUG,file_slug,result['version'])])
    context.add_output_metadata(result)
    return result

@asset(key=AssetKey(['ingest',DATASET_SLUG,GENERATOR_FILE]),group_name='energy',compute_kind='http',description='Maintained EIA-860M Operating and Operating_PR generators. Native nonspatial draft; datum and acceptance remain gated.')
def eia_860m_existing_generators(context,config:EIAInventoryConfig,staging_storage:StagingStorageResource)->dict:
    return _capture(context,config,staging_storage,GENERATOR_FILE)

@asset(key=AssetKey(['ingest',DATASET_SLUG,PLANT_FILE]),group_name='energy',compute_kind='http',description='Plant references grouped by native Plant ID from maintained EIA-860M existing generators. No conflicting-coordinate averaging; nonspatial draft.')
def eia_860m_plant_reference(context,config:EIAInventoryConfig,staging_storage:StagingStorageResource)->dict:
    return _capture(context,config,staging_storage,PLANT_FILE)

@asset_check(asset=eia_860m_existing_generators,blocking=True)
def eia_860m_generators_complete(candidate:dict)->AssetCheckResult:
    return AssetCheckResult(passed=candidate.get('source_population_complete') is True and candidate.get('row_count',0)>0,metadata={'source_period':candidate.get('source_period','unknown'),'release_accepted':False,'geospatial_release_ready':False})

@asset_check(asset=eia_860m_plant_reference,blocking=True)
def eia_860m_plants_complete(candidate:dict)->AssetCheckResult:
    return AssetCheckResult(passed=candidate.get('source_population_complete') is True and candidate.get('row_count',0)>0,metadata={'source_period':candidate.get('source_period','unknown'),'release_accepted':False,'geospatial_release_ready':False})

INGEST_ASSETS=[eia_860m_existing_generators,eia_860m_plant_reference]
ASSET_CHECKS=[eia_860m_generators_complete,eia_860m_plants_complete]
eia_860m_draft_refresh=define_asset_job('eia_860m_draft_refresh',selection=AssetSelection.assets(*INGEST_ASSETS))
JOBS=[eia_860m_draft_refresh]
SCHEDULES=[ScheduleDefinition(name='eia_860m_draft_refresh_schedule',job=eia_860m_draft_refresh,cron_schedule='0 8 1 * *',default_status=DefaultScheduleStatus.STOPPED)]
