"""Separate CMS certified-provider draft; source and release gates remain closed."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.cms_certified_nursing_homes import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, GATES, capture_cms_certified_nursing_homes,
)


class CMSDraftConfig(Config):
    capture_directory: str


@asset(key=AssetKey(['ingest', DATASET_SLUG, FILE_SLUG]), group_name='cms', compute_kind='http')
def cms_certified_nursing_homes(context, config: CMSDraftConfig,
                                staging_storage: StagingStorageResource) -> dict:
    result = capture_cms_certified_nursing_homes(staging_storage, version=build_version_id(context),
                                               capture_directory=config.capture_directory)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result['version'])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=cms_certified_nursing_homes, blocking=True)
def cms_certified_nursing_homes_complete(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(passed=candidate.get('feature_count') == EXPECTED_COUNT,
        metadata={**GATES,'source_feature_count':candidate.get('feature_count',0)},
        description='Technical completeness only; all independent source, mapping and release acceptance gates remain closed.')


cms_certified_nursing_homes_draft_job = define_asset_job('cms_certified_nursing_homes_draft_refresh',
    selection=AssetSelection.assets(cms_certified_nursing_homes))
cms_certified_nursing_homes_draft_schedule = ScheduleDefinition(
    job=cms_certified_nursing_homes_draft_job, cron_schedule='0 9 3 * *',
    execution_timezone='America/New_York', default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [cms_certified_nursing_homes]
ASSET_CHECKS = [cms_certified_nursing_homes_complete]
JOBS = [cms_certified_nursing_homes_draft_job]
SCHEDULES = [cms_certified_nursing_homes_draft_schedule]
