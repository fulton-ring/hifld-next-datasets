"""Explicit Census Metropolitan Divisions draft source asset; release approval remains independent."""
import os
from pathlib import Path
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.metropolitan_divisions import SLUG, stage_metdiv_candidate

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='census_bureau',compute_kind='http',description='Exact2024 Census GDB Metropolitan Division source; unapproved local draft, native IDs/types/geometry preserved.')
def census_metropolitan_divisions(context,staging_storage:StagingStorageResource):
    capture=os.environ.get('HIFLD_METDIV_CAPTURE_DIR')
    baseline=os.environ.get('HIFLD_METDIV_BASELINE_DIR')
    result=stage_metdiv_candidate(staging_storage,archive_path=Path(capture)/'official2024_nationgeo.zip' if capture else None,
        receipt_path=Path(capture)/'official2024_nationgeo.receipt.json' if capture else None,
        baseline_dir=Path(baseline) if baseline else None)
    context.add_output_metadata(result)
    return result

metdiv_source_integrity_job=define_asset_job('metdiv_source_integrity',selection=AssetSelection.assets(census_metropolitan_divisions))
metdiv_source_integrity_schedule=ScheduleDefinition(name='metdiv_source_integrity_monthly',job=metdiv_source_integrity_job,cron_schedule='0 7 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
