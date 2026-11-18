"""Draft nursing-home asset from the current CMS Provider Information roster."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nursing_homes import (
    DATASET_SLUG, FILE_SLUG, GATES, capture_nursing_homes,
)


class NursingHomesConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(['ingest', DATASET_SLUG, FILE_SLUG]), group_name='health', compute_kind='http',
       description='Current CMS Provider Information nursing homes. Coverage is limited to Medicare- and '
                   'Medicaid-certified facilities; historical ORNL/HIFLD equivalence and all release approvals remain pending.')
def nursing_homes(context, config: NursingHomesConfig,
                  staging_storage: StagingStorageResource) -> dict:
    result = capture_nursing_homes(staging_storage, version=build_version_id(context),
                                   capture_directory=config.capture_directory)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result['version'])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=nursing_homes, blocking=True)
def nursing_homes_population_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get('source_count_witness')
    passed = (candidate.get('source_population_complete') is True
              and candidate.get('source_id_roster_complete') is True
              and type(count) is int and count > 0
              and candidate.get('feature_count') == count
              and candidate.get('source_coordinate_count') == count)
    return AssetCheckResult(passed=passed, metadata={
        **GATES,
        'source_feature_count': candidate.get('feature_count', 0),
        'source_count_witness': count if count is not None else 'missing',
        'source_coordinate_count': candidate.get('source_coordinate_count', 0),
        'source_id_sha256': candidate.get('source_id_sha256', 'missing'),
        'coverage_note': 'CMS-certified facilities only; not all licensed nursing homes or assisted care.',
    }, description='Technical capture-time completeness only; source, coverage, mapping and release acceptance remain closed.')


@asset_check(asset=nursing_homes, blocking=False)
def nursing_homes_approved_baseline_drift(candidate: dict) -> AssetCheckResult:
    return approved_baseline_drift(candidate)


nursing_homes_draft_job = define_asset_job('nursing_homes_draft_refresh',
    selection=AssetSelection.assets(nursing_homes))
nursing_homes_draft_schedule = ScheduleDefinition(
    job=nursing_homes_draft_job, cron_schedule='0 9 3 * *',
    execution_timezone='America/New_York', default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [nursing_homes]
ASSET_CHECKS = [nursing_homes_population_complete, nursing_homes_approved_baseline_drift]
JOBS = [nursing_homes_draft_job]
SCHEDULES = [nursing_homes_draft_schedule]
