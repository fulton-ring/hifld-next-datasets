"""Complete FDIC native JSON preservation; zero coordinates remain blocked."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset,
                     asset_check, define_asset_job)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fdic_locations import SLUG, stage_fdic_locations


class FdicLocationsConfig(Config):
    capture_directory: str
    refresh_official: bool = False


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='federal_deposit_insurance_corporation',
       compute_kind='local', description='All78028official FDIC location records, primitives and absent/null distinctions preserved. Point/CRS interpretation unapproved.')
def fdic_locations_official(context, config: FdicLocationsConfig, staging_storage: StagingStorageResource):
    result = stage_fdic_locations(staging_storage, config.capture_directory, config.refresh_official)
    context.add_output_metadata(result)
    return result


@asset_check(asset=fdic_locations_official, blocking=True)
def fdic_locations_full_quality(fdic_locations_official: dict):
    return AssetCheckResult(passed=fdic_locations_official['quality_check_passed'], metadata={
        'complete_population': fdic_locations_official['feature_count'],
        'zero_coordinate_pairs': fdic_locations_official['zero_coordinate_pairs'],
        'uninterpretable_coordinates': fdic_locations_official['uninterpretable_coordinates'],
        'scope': 'Full native population; no dropped rows, geocoding/fill/repair, geometry/CRS approval or quality exception.'})


fdic_locations_draft_job = define_asset_job('fdic_locations_draft_reconstruction',
    selection=AssetSelection.assets(fdic_locations_official).without_checks(),
    description='Source integrity and lossless preservation ONLY; excludes blocking quality for 803 zero points and 2 uninterpretable coordinate pairs, never a full graph/release pass.')
fdic_locations_draft_schedule = ScheduleDefinition(name='fdic_locations_draft_refresh_schedule',
    job=fdic_locations_draft_job, cron_schedule='0 12 * * 1', execution_timezone='America/New_York',
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={'ops': {fdic_locations_official.op.name: {'config': {
        'capture_directory': {'env': 'HIFLD_FDIC_LOCATIONS_CAPTURE_DIR'}, 'refresh_official': True}}}})
