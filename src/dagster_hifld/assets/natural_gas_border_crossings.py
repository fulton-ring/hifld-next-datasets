"""WHOLE99 NACEI North America draft, explicitly not the blocked exact EIA US51 release."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.natural_gas_border_crossings import SLUG, stage_natural_gas_border_crossings

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='nacei',compute_kind='http',description='Complete official NACEI NorthAmerica99 native source; changed-scope unapproved draft versus EIA US51.')
def natural_gas_border_crossings(context,staging_storage:StagingStorageResource):
    result=stage_natural_gas_border_crossings(staging_storage,capture_dir=os.environ.get('HIFLD_NATURAL_GAS_CROSSINGS_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

natural_gas_crossings_draft_job=define_asset_job('nacei_natural_gas_border_crossings_draft_refresh',selection=AssetSelection.assets(natural_gas_border_crossings))
natural_gas_crossings_draft_schedule=ScheduleDefinition(name='nacei_natural_gas_border_crossings_draft_monthly',job=natural_gas_crossings_draft_job,cron_schedule='0 6 1 * *',execution_timezone='UTC',default_status=DefaultScheduleStatus.STOPPED)
