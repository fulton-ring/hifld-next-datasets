"""STOPPED explicit-input historical preservation; all release decisions external."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
                     ScheduleDefinition, asset, asset_check, define_asset_job)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferc_regions import SLUG, stage_ferc_preservation


class FercPreservationConfig(Config):
    capture_directory: str


@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='department_of_energy',compute_kind='local',
       description='Unapproved historical PASDA-as-provided FERC preservation. Explicit pinned local inputs, five CONUS regions only; never an automatic mirror refresh/current producer feed.')
def ferc_historical_preservation(context,config:FercPreservationConfig,staging_storage:StagingStorageResource):
    result=stage_ferc_preservation(staging_storage,config.capture_directory)
    context.add_output_metadata(result)
    return result


@asset_check(asset=ferc_historical_preservation,blocking=True)
def ferc_historical_full_quality(ferc_historical_preservation:dict):
    result=ferc_historical_preservation
    return AssetCheckResult(passed=result['source_invalid_geometry_count']==0,
        metadata={'feature_count':result['feature_count'],'invalid_geometry_count':result['source_invalid_geometry_count'],
            'scope':'All five original regions; two invalid polygons retained without repair. No quality exception or release accepted.'})


# This source-only job explicitly skips checks: success proves preservation only.
ferc_historical_reconstruction_job=define_asset_job('ferc_historical_reconstruction',
    selection=AssetSelection.assets(ferc_historical_preservation).without_checks())
ferc_historical_integrity_schedule=ScheduleDefinition(name='ferc_historical_integrity',
    job=ferc_historical_reconstruction_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={'ops':{ferc_historical_preservation.op.name:{'config':{'capture_directory':{'env':'HIFLD_FERC_CAPTURE_DIR'}}}}})
