"""Explicit local historical-preservation proposal; no autonomous archive feed."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
                     ScheduleDefinition, asset, asset_check, define_asset_job)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.campus_preservation import SLUG, stage_campus_preservation


class CampusReconstructionConfig(Config):
    capture_directory: str


@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='department_of_energy',compute_kind='local',
       description='Unapproved2022HIFLD/ORNL historical campus preservation candidate. Explicit complete local inputs only; PASDA remains third-party historical mirror, not current producer feed.')
def ornl_campus_historical_preservation(context,config:CampusReconstructionConfig,staging_storage:StagingStorageResource):
    result = stage_campus_preservation(staging_storage,config.capture_directory)
    context.add_output_metadata(result)
    return result


@asset_check(asset=ornl_campus_historical_preservation,blocking=True)
def campus_historical_full_quality(ornl_campus_historical_preservation:dict):
    """Block the full graph on actual full-population invalid source geometry."""
    result = ornl_campus_historical_preservation
    return AssetCheckResult(passed=result['source_invalid_geometry_count']==0,
        metadata={'feature_count':result['feature_count'],'invalid_geometry_count':result['source_invalid_geometry_count'],
                  'scope':'All original source polygons, including the15 retained invalid ones; no geometry repair or domain exception approved.'})


# Source-stage success means preservation/construction only. Full graph checks
# remain blocking; selecting this job does not claim quality or release success.
ornl_campus_historical_reconstruction_job = define_asset_job('ornl_campus_historical_reconstruction',
    selection=AssetSelection.assets(ornl_campus_historical_preservation).without_checks())
ornl_campus_historical_integrity_schedule = ScheduleDefinition(
    name='ornl_campus_historical_integrity',job=ornl_campus_historical_reconstruction_job,
    cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED,
    run_config={'ops':{ornl_campus_historical_preservation.op.name:{'config':{'capture_directory':{'env':'HIFLD_CAMPUS_CAPTURE_DIR'}}}}})
