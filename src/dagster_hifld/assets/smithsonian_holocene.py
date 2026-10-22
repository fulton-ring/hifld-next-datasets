"""Complete Smithsonian Holocene source snapshot; monthly refresh stays stopped."""
import os
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.holocene_volcanoes import SLUG, stage_holocene

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='smithsonian',compute_kind='http',description='Complete official GVP Holocene WFS draft. Unstable original feature IDs and producer Volcano_Number retained; changed successor and exact decimals remain unapproved.')
def smithsonian_holocene_volcanoes(context,staging_storage:StagingStorageResource):
    result=stage_holocene(staging_storage,capture_dir=os.environ.get('HIFLD_HOLOCENE_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

holocene_source_integrity_job=define_asset_job('holocene_source_integrity',selection=AssetSelection.assets(smithsonian_holocene_volcanoes))
holocene_source_integrity_schedule=ScheduleDefinition(name='holocene_source_integrity_monthly',job=holocene_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
