"""Explicit Census Metropolitan Statistical Areas draft source asset; release approval remains independent."""
from pathlib import Path
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.metropolitan_statistical_areas import SLUG, stage_msa_candidate


class MetropolitanStatisticalAreasConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Current Census TIGER national GDB Core_Based_Statistical_Area MEMI=1 source; unapproved local draft, native IDs/types/geometry preserved.')
def census_metropolitan_statistical_areas(context,config:MetropolitanStatisticalAreasConfig,staging_storage:StagingStorageResource):
    capture=config.capture_directory or None
    result=stage_msa_candidate(staging_storage,archive_path=Path(capture)/'official_nationgeo.zip' if capture else None,
        receipt_path=Path(capture)/'official_nationgeo.receipt.json' if capture else None)
    context.add_output_metadata(result)
    return result

msa_source_integrity_job=define_asset_job('msa_source_integrity',selection=AssetSelection.assets(census_metropolitan_statistical_areas))
msa_source_integrity_schedule=ScheduleDefinition(name='msa_source_integrity_monthly',job=msa_source_integrity_job,cron_schedule='0 7 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS=[census_metropolitan_statistical_areas]
JOBS=[msa_source_integrity_job]
SCHEDULES=[msa_source_integrity_schedule]
