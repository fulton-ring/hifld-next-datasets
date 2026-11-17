"""Separate current-USGS U/V draft; worldwide MRDS remains parked."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usgs_critical_mineral_deposits import (
    DATASET_SLUG, FILE_SLUG, EXPECTED_SOURCE_COUNT, EXPECTED_SELECTED_COUNT,
    EXPECTED_URANIUM_COUNT, EXPECTED_VANADIUM_COUNT, EXPECTED_BOTH_COUNT, capture_deposits)

class CurrentUSGSDepositsConfig(Config):
    capture_directory: str = ''

@asset(key=AssetKey(['ingest',DATASET_SLUG,FILE_SLUG]),group_name='usgs',compute_kind='http',
    description='Distinct current USGS v3.0 United States uranium/vanadium whole-token subset; unapproved candidate, not worldwide MRDS reconstruction.')
def usgs_uranium_vanadium_deposits(context,config:CurrentUSGSDepositsConfig,staging_storage:StagingStorageResource)->dict:
    candidate=capture_deposits(staging_storage,capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[build_publish_partition_key(DATASET_SLUG,FILE_SLUG,candidate['version'])])
    context.add_output_metadata(candidate)
    return candidate

@asset_check(asset=usgs_uranium_vanadium_deposits,blocking=True)
def usgs_uranium_vanadium_population_complete(candidate:dict)->AssetCheckResult:
    passed=(candidate.get('source_population_complete') is True
        and candidate.get('source_feature_count')==EXPECTED_SOURCE_COUNT
        and candidate.get('feature_count')==EXPECTED_SELECTED_COUNT
        and candidate.get('uranium_count')==EXPECTED_URANIUM_COUNT
        and candidate.get('vanadium_count')==EXPECTED_VANADIUM_COUNT
        and candidate.get('both_count')==EXPECTED_BOTH_COUNT
        and candidate.get('null_geometry_count')==0 and candidate.get('invalid_geometry_count')==0)
    return AssetCheckResult(passed=passed,metadata={'source_feature_count':candidate.get('source_feature_count',0),
        'feature_count':candidate.get('feature_count',0),'rights_accepted':False,'schema_contract_accepted':False,
        'id_contract_accepted':False,'release_accepted':False})

usgs_uranium_vanadium_draft_job=define_asset_job('usgs_uranium_vanadium_draft_refresh',selection=AssetSelection.assets(usgs_uranium_vanadium_deposits))
usgs_uranium_vanadium_draft_schedule=ScheduleDefinition(name='usgs_uranium_vanadium_draft_refresh_schedule',job=usgs_uranium_vanadium_draft_job,cron_schedule='0 9 1 * *',default_status=DefaultScheduleStatus.STOPPED)
INGEST_ASSETS=[usgs_uranium_vanadium_deposits]
ASSET_CHECKS=[usgs_uranium_vanadium_population_complete]
JOBS=[usgs_uranium_vanadium_draft_job]
SCHEDULES=[usgs_uranium_vanadium_draft_schedule]
