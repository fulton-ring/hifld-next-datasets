"""ARC Chapters candidate snapshot; effective edition and release unaccepted."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.sources.red_cross_chapters import SLUG, stage_red_cross_chapters


class RedCrossChaptersConfig(Config):
    capture_directory: str = ''


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='american_red_cross', compute_kind='local',
    description='Unaccepted226-chapter NHQRedCrossGIS candidate directed by the DHS HIFLD crosswalk to layer3; all native source fields/IDs/3857rings retained, FY26/FY27 edition conflict explicit.')
def red_cross_chapters(context, config: RedCrossChaptersConfig, staging_storage: StagingStorageResource):
    result = stage_red_cross_chapters(staging_storage, config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [result['staging_prefix']])
    context.add_output_metadata(result)
    return result


@asset_check(asset=red_cross_chapters, blocking=True)
def red_cross_chapters_full_quality(red_cross_chapters: dict):
    return AssetCheckResult(passed=red_cross_chapters['source_invalid_geometry_count'] == 0,
        metadata={'full_population': red_cross_chapters['feature_count'], 'invalid_geometry_count': red_cross_chapters['source_invalid_geometry_count'],
            'approval_scope': 'Geometry quality only; source delegation, edition, coverage, schema and release remain unaccepted.'})


red_cross_chapters_draft_job = define_asset_job('red_cross_chapters_draft_refresh', selection=AssetSelection.assets(red_cross_chapters))
red_cross_chapters_draft_schedule = ScheduleDefinition(name='red_cross_chapters_draft_refresh_schedule', job=red_cross_chapters_draft_job,
    cron_schedule='0 9 1 * *', execution_timezone='America/New_York', default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [red_cross_chapters]
ASSET_CHECKS = [red_cross_chapters_full_quality]
JOBS = [red_cross_chapters_draft_job]
SCHEDULES = [red_cross_chapters_draft_schedule]
