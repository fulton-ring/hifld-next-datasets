"""Explicit stopped-refresh draft asset for the DISDI MIRTA DoD site boundaries."""
from pathlib import Path

from dagster import (AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus,
                     Field, RunRequest, asset, asset_check, define_asset_job, schedule)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.mirta_dod_sites import (
    BOUNDARY_FEATURES,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    SLUG,
    require_local,
    stage_mirta_boundaries,
)


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='department_of_defense', compute_kind='http',
       description=('Official DoD DISDI MIRTA FY24 DoD site boundaries (825 native EPSG:4326 '
                    'MultiPolygon features; 704 linear WKBs kept bit-exact, 121 curved '
                    'MultiSurface features linearized with GDAL at a pinned 0.01-degree arc '
                    'step; 28 invalid producer geometries preserved unrepaired and '
                    'release-blocked). Faithful draft; publication unapproved.'),
       config_schema={'capture_dir': Field(str, default_value='')})
def mirta_dod_sites_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    require_local(staging_storage)
    capture = context.op_config.get('capture_dir', '')
    result = stage_mirta_boundaries(
        staging_storage, capture_dir=Path(capture) if capture else None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result['dataset_slug'], result['file_slug'], result['version'])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=mirta_dod_sites_boundaries, blocking=True)
def mirta_dod_sites_boundaries_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get('feature_count') == BOUNDARY_FEATURES
              and candidate.get('source_unique_id_count') == BOUNDARY_FEATURES
              and candidate.get('source_null_geometry_count') == 0
              and candidate.get('source_invalid_geometry_count') == EXPECTED_INVALID_GEOMETRY_COUNT)
    return AssetCheckResult(passed=passed, metadata={
        'complete_source_population': candidate.get('feature_count', 0),
        'unique_source_ids': candidate.get('source_unique_id_count'),
        'source_null_geometry_count': candidate.get('source_null_geometry_count'),
        'source_invalid_geometry_count': candidate.get('source_invalid_geometry_count'),
        'invalid_geometries_preserved_unrepaired': True,
        'release_blocked': candidate.get('release_blocked', True),
        'source_release_accepted': False, 'source_freshness_accepted': False,
        'schema_contract_accepted': False, 'id_contract_accepted': False,
        'rights_accepted': False, 'mapping_approved': False, 'release_accepted': False,
    })


mirta_dod_sites_boundaries_draft_job = define_asset_job(
    'mirta_dod_sites_boundaries_draft_refresh',
    selection=AssetSelection.keys(mirta_dod_sites_boundaries.key),
    config={'ops': {mirta_dod_sites_boundaries.op.name: {'config': {'capture_dir': ''}}}})


@schedule(job=mirta_dod_sites_boundaries_draft_job, cron_schedule='0 8 1 * *',
          execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,
          required_resource_keys={'staging_storage'})
def mirta_dod_sites_boundaries_draft_refresh_schedule(context):
    # Annual integrity poll of the DISDI MIRTA release; the schedule stays STOPPED.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops': {
        mirta_dod_sites_boundaries.op.name: {'config': {'capture_dir': ''}}}})


INGEST_ASSETS = [mirta_dod_sites_boundaries]
ASSET_CHECKS = [mirta_dod_sites_boundaries_complete]
JOBS = [mirta_dod_sites_boundaries_draft_job]
SCHEDULES = [mirta_dod_sites_boundaries_draft_refresh_schedule]
