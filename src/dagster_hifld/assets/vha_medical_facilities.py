"""Explicit stopped VA locator draft asset; no publication acceptance."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
    DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.vha_medical_facilities import (
    DATASET_SLUG, FILE_SLUG, capture_vha_medical_facilities)


class VhaMedicalFacilitiesConfig(Config):
    capture_directory: str = ''
    offline_capture_directory: str = ''


@asset(key=AssetKey(['ingest', DATASET_SLUG, FILE_SLUG]), group_name='va', compute_kind='http',
       description='Complete current VA health/Vet Center nonmobile locator population, native IDs/values/points; '
                   'CLOSED/COMING_SOON exclusions explicit. Historical NGA enrichment not reconstructed; unapproved draft.')
def vha_medical_facilities(context, config: VhaMedicalFacilitiesConfig,
                           staging_storage: StagingStorageResource) -> dict:
    result = capture_vha_medical_facilities(staging_storage,
        capture_directory=config.capture_directory or None,
        offline_capture_directory=config.offline_capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [
        build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result['version'])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=vha_medical_facilities, blocking=True)
def vha_medical_facilities_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get('source_population_complete') is True
        and type(candidate.get('feature_count')) is int and candidate['feature_count'] > 0
        and candidate.get('release_accepted') is False)
    return AssetCheckResult(passed=passed, metadata={
        'feature_count': candidate.get('feature_count', 0),
        'source_count': candidate.get('source_count', 0), 'release_accepted': False})


vha_medical_facilities_draft_job = define_asset_job('vha_medical_facilities_draft_refresh',
    selection=AssetSelection.assets(vha_medical_facilities))
vha_medical_facilities_draft_schedule = ScheduleDefinition(
    name='vha_medical_facilities_draft_refresh_schedule', job=vha_medical_facilities_draft_job,
    cron_schedule='0 8 1,15 * *', default_status=DefaultScheduleStatus.STOPPED,
    description='Stopped twice-monthly polling proposal; upstream record/coordinate cadence unknown.')

INGEST_ASSETS = [vha_medical_facilities]
ASSET_CHECKS = [vha_medical_facilities_complete]
JOBS = [vha_medical_facilities_draft_job]
SCHEDULES = [vha_medical_facilities_draft_schedule]
