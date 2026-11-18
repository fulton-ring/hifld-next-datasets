"""Complete Smithsonian Holocene source snapshot; monthly refresh stays stopped."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.holocene_volcanoes import SLUG, stage_holocene


class SmithsonianHoloceneConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='smithsonian',compute_kind='http',description='Complete official GVP Holocene WFS draft. Unstable original feature IDs and producer Volcano_Number retained; changed successor and exact decimals remain unapproved.')
def smithsonian_holocene_volcanoes(context,config:SmithsonianHoloceneConfig,staging_storage:StagingStorageResource):
    result=stage_holocene(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result

holocene_source_integrity_job=define_asset_job('holocene_source_integrity',selection=AssetSelection.assets(smithsonian_holocene_volcanoes))
holocene_source_integrity_schedule=ScheduleDefinition(name='holocene_source_integrity_monthly',job=holocene_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)


ASSET_CHECKS = []
INGEST_ASSETS = [smithsonian_holocene_volcanoes]
JOBS = [holocene_source_integrity_job]
SCHEDULES = [holocene_source_integrity_schedule]
