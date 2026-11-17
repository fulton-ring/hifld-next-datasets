"""Exact EPA large-scale regional layer; candidate release stays blocked."""
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_large_scale import SLUG,stage_epa_large_scale


class EpaLargeScaleConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='epa',compute_kind='http',description='Complete exact EPA_Locations large-scale layer3 with original IDs/Double/string declarations and all native NAD83 ordered rings; no release accepted.')
def epa_large_scale_regions(context,config:EpaLargeScaleConfig,staging_storage:StagingStorageResource):
    result=stage_epa_large_scale(staging_storage,capture_dir=config.capture_directory or None);context.add_output_metadata(result);return result

epa_large_scale_source_integrity_job=define_asset_job('epa_large_scale_source_integrity',selection=AssetSelection.assets(epa_large_scale_regions))
epa_large_scale_source_integrity_schedule=ScheduleDefinition(name='epa_large_scale_source_integrity_monthly',job=epa_large_scale_source_integrity_job,cron_schedule='0 8 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
