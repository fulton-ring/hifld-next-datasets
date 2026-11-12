"""Explicit stopped-refresh draft asset for the NIFC GeoMAC 2000-2018 perimeters."""
from pathlib import Path

from dagster import (AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus,
                     Field, RunRequest, asset, asset_check, define_asset_job, schedule)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.historic_geomac_perimeters import (
    COMBINED_FEATURES,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    SLUG,
    require_local,
    stage_historic_geomac_perimeters_candidate,
)


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='usgs', compute_kind='http',
       description=('Official NIFC-published GeoMAC historic wildfire perimeters 2000-2018 combined layer '
                    '(23,776 native NAD83 MultiPolygon features, 881 invalid producer geometries preserved '
                    'unrepaired and release-blocked). Faithful draft; publication unapproved.'),
       config_schema={'capture_dir': Field(str, default_value='')})
def historic_geomac_perimeters(context, staging_storage: StagingStorageResource) -> dict:
    require_local(staging_storage)
    capture = context.op_config.get('capture_dir', '')
    result = stage_historic_geomac_perimeters_candidate(
        staging_storage, capture_dir=Path(capture) if capture else None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result['dataset_slug'], result['file_slug'], result['version'])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=historic_geomac_perimeters, blocking=True)
def historic_geomac_perimeters_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get('feature_count') == COMBINED_FEATURES
              and candidate.get('source_unique_id_count') == COMBINED_FEATURES
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


historic_geomac_perimeters_draft_job = define_asset_job(
    'historic_geomac_perimeters_draft_refresh',
    selection=AssetSelection.keys(historic_geomac_perimeters.key),
    config={'ops': {historic_geomac_perimeters.op.name: {'config': {'capture_dir': ''}}}})


@schedule(job=historic_geomac_perimeters_draft_job, cron_schedule='0 14 1 * *',
          execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,
          required_resource_keys={'staging_storage'})
def historic_geomac_perimeters_draft_schedule(context):
    # Monthly integrity poll of a frozen historical archive; the schedule stays STOPPED.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops': {
        historic_geomac_perimeters.op.name: {'config': {'capture_dir': ''}}}})
