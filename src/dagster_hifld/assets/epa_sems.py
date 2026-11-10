"""Complete EPA SEMS interests; all output remains an unapproved local draft."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_sems import SLUG, stage_epa_sems

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='Complete EPA FRS SEMS interest draft including non-NPL sites; preserves all original IDs, Date milliseconds, native NAD83 points and field declarations.')
def epa_sems_interests(context,staging_storage:StagingStorageResource):
    result=stage_epa_sems(staging_storage,capture_dir=os.environ.get('HIFLD_EPA_SEMS_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

epa_sems_source_integrity_job=define_asset_job('epa_sems_source_integrity',selection=AssetSelection.assets(epa_sems_interests))
epa_sems_source_integrity_schedule=ScheduleDefinition(name='epa_sems_source_integrity_monthly',job=epa_sems_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
