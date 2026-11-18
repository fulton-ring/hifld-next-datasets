"""Explicit Census CSA draft source asset; release approval remains independent."""
import os
from pathlib import Path
from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.combined_statistical_areas import SLUG, stage_csa_candidate


class CombinedStatisticalAreasConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Exact2024 Census GDB CSA source; unapproved local draft, native IDs/types/geometry preserved.')
def census_combined_statistical_areas(context,config:CombinedStatisticalAreasConfig,staging_storage:StagingStorageResource):
    capture=config.capture_directory or None
    result=stage_csa_candidate(staging_storage,archive_path=Path(capture)/'official2024_nationgeo.zip' if capture else None,
        receipt_path=Path(capture)/'official2024_nationgeo.receipt.json' if capture else None)
    context.add_output_metadata(result)
    return result

csa_source_integrity_job=define_asset_job('csa_source_integrity',selection=AssetSelection.assets(census_combined_statistical_areas))
csa_source_integrity_schedule=ScheduleDefinition(name='csa_source_integrity_monthly',job=csa_source_integrity_job,cron_schedule='0 7 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)

ASSET_CHECKS = []
INGEST_ASSETS = [census_combined_statistical_areas]
JOBS = [csa_source_integrity_job]
SCHEDULES = [csa_source_integrity_schedule]
