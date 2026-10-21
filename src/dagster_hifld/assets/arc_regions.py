"""ARC Regions candidate snapshot; effective edition and release unaccepted."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.sources.arc_regions import SLUG, stage_arc_regions


class ArcRegionsConfig(Config):
    capture_directory: str = ''


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='american_red_cross', compute_kind='local',
    description='Unaccepted47-region NHQRedCrossGIS candidate; all native source fields/IDs/3857rings retained, FY26/FY27 edition conflict explicit.')
def arc_regions(context, config: ArcRegionsConfig, staging_storage: StagingStorageResource):
    result = stage_arc_regions(staging_storage, config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [result['staging_prefix']])
    context.add_output_metadata(result)
    return result


@asset_check(asset=arc_regions, blocking=True)
def arc_regions_full_quality(arc_regions: dict):
    return AssetCheckResult(passed=arc_regions['source_invalid_geometry_count'] == 0,
        metadata={'full_population': arc_regions['feature_count'], 'invalid_geometry_count': arc_regions['source_invalid_geometry_count'],
            'approval_scope': 'Geometry quality only; source, edition, coverage, schema and release remain unaccepted.'})


arc_regions_draft_job = define_asset_job('arc_regions_draft_refresh', selection=AssetSelection.assets(arc_regions))
arc_regions_draft_schedule = ScheduleDefinition(name='arc_regions_draft_refresh_schedule', job=arc_regions_draft_job,
    cron_schedule='0 9 1 * *', execution_timezone='America/New_York', default_status=DefaultScheduleStatus.STOPPED)
