"""Complete official NOAA significant volcanic events; refresh remains stopped."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.significant_volcanic_events import SLUG, stage_significant_volcanic


class NoaaSignificantVolcanicConfig(Config):
    replay_capture_directory: str = ""


@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='noaa',compute_kind='http',description='Complete official NOAA NCEI significant eruption/event draft; completeness from capture-time count and OBJECTID witnesses, all original IDs/types/native geometry/BCE and coded date components retained.')
def noaa_significant_volcanic_events(context,config:NoaaSignificantVolcanicConfig,staging_storage:StagingStorageResource):
    result=stage_significant_volcanic(staging_storage,capture_dir=config.replay_capture_directory or None)
    context.add_output_metadata(result)
    return result


@asset_check(asset=noaa_significant_volcanic_events,blocking=True)
def noaa_significant_volcanic_events_complete(candidate:dict)->AssetCheckResult:
    passed=(type(candidate.get('source_reported_count'))is int
        and candidate['source_reported_count']>0
        and candidate.get('feature_count')==candidate['source_reported_count']
        and candidate.get('source_population_complete')is True)
    return AssetCheckResult(passed=passed,metadata={
        'complete_source_population':candidate.get('feature_count',0),
        'source_reported_count':candidate.get('source_reported_count','missing'),
        'source_id_sha256':candidate.get('source_id_sha256','missing'),
        'release_accepted':False})


noaa_significant_volcanic_events_drift=register_approved_baseline_drift(noaa_significant_volcanic_events,'noaa_significant_volcanic_events_drift')

significant_volcanic_source_integrity_job=define_asset_job('significant_volcanic_source_integrity',selection=AssetSelection.assets(noaa_significant_volcanic_events))
significant_volcanic_source_integrity_schedule=ScheduleDefinition(name='significant_volcanic_source_integrity_monthly',job=significant_volcanic_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS=[noaa_significant_volcanic_events]
ASSET_CHECKS=[noaa_significant_volcanic_events_complete,noaa_significant_volcanic_events_drift]
JOBS=[significant_volcanic_source_integrity_job]
SCHEDULES=[significant_volcanic_source_integrity_schedule]
