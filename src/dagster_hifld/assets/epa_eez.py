"""Exact EPA EEZ layer draft; monthly refresh remains stopped."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.sources.epa_eez import SLUG, stage_epa_eez

class EpaEezConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='Complete exact EPA EEZ-boundary draft from producer MapServer4; source Shape/attributes/IDs and NAD83 native rings retained.')
def epa_eez(context,config:EpaEezConfig,staging_storage:StagingStorageResource):
    result=stage_epa_eez(staging_storage,capture_dir=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[build_publish_partition_key(SLUG,SLUG,result['version'])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=epa_eez,blocking=True)
def epa_eez_full_quality(epa_eez:dict):
    return AssetCheckResult(passed=epa_eez['source_null_geometry_count']==0 and epa_eez['source_invalid_geometry_count']==0,metadata={'full_population':epa_eez['feature_count'],'null_geometry_count':epa_eez['source_null_geometry_count'],'invalid_geometry_count':epa_eez['source_invalid_geometry_count'],'approval_scope':'Full geometry quality only; source/EEZ coverage/edition/schema/baseline/rights/release unaccepted.'})

epa_eez_draft_job=define_asset_job('epa_eez_draft_refresh',selection=AssetSelection.assets(epa_eez))
epa_eez_draft_schedule=ScheduleDefinition(name='epa_eez_draft_refresh_schedule',job=epa_eez_draft_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
