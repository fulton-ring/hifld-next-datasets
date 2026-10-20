"""Complete official historical observation/runup local drafts, never release acceptance."""
from pathlib import Path

from dagster import AssetKey,AssetSelection,DefaultScheduleStatus,Field,RunRequest,asset,define_asset_job,schedule
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.tsunami_observations import require_local,stage_tsunami_observations_candidate


@asset(key=AssetKey(['ingest','historical-tsunami-observations','historical-tsunami-observations']),group_name='noaa',compute_kind='http',
       description='Complete34495officialNOAAobservations/runups, all50attrs and originalID/ObjectId/TSEVENT_ID, nativeInt16/Float32/3857; originalwireJSONpreserved. Draftonly.',
       config_schema={'capture_dir':Field(str,default_value='')})
def noaa_tsunami_observations(context,staging_storage:StagingStorageResource)->dict:
    require_local(staging_storage);capture=context.op_config.get('capture_dir','')
    return stage_tsunami_observations_candidate(staging_storage,capture_dir=Path(capture) if capture else None)


noaa_tsunami_observations_draft_refresh_job=define_asset_job('noaa_tsunami_observations_draft_refresh',
    selection=AssetSelection.keys(noaa_tsunami_observations.key),
    config={'ops':{noaa_tsunami_observations.op.name:{'config':{'capture_dir':''}}}})


@schedule(job=noaa_tsunami_observations_draft_refresh_job,cron_schedule='0 11 * * 3',execution_timezone='America/New_York',
          default_status=DefaultScheduleStatus.STOPPED,required_resource_keys={'staging_storage'})
def noaa_tsunami_observations_draft_refresh_schedule(context):
    # Proposed availability/integrity poll, not a declaration of data-release cadence.
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={'ops':{noaa_tsunami_observations.op.name:{'config':{'capture_dir':''}}}})
