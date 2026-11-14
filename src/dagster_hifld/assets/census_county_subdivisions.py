"""Explicit stopped complete County Subdivision reconstruction draft."""
import os

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.county_subdivisions import SLUG, stage_county_subdivisions_candidate


@asset(key=AssetKey(['ingest', SLUG, SLUG]), group_name='census_bureau', compute_kind='http',
       description='Complete exact Census2024 county subdivisions with preserved native identifiers/types/geometry; unapproved local draft.')
def census_county_subdivisions(context, staging_storage: StagingStorageResource):
    result = stage_county_subdivisions_candidate(staging_storage,
        archive_path=os.environ.get('HIFLD_COUSUB_ARCHIVE'), receipt_path=os.environ.get('HIFLD_COUSUB_RECEIPT'))
    context.add_output_metadata(result)
    return result


county_subdivisions_draft_refresh_job = define_asset_job('county_subdivisions_draft_refresh',
    selection=AssetSelection.assets(census_county_subdivisions))
county_subdivisions_draft_refresh_schedule = ScheduleDefinition(name='county_subdivisions_draft_monthly',
    job=county_subdivisions_draft_refresh_job, cron_schedule='0 10 1 * *',
    execution_timezone='America/New_York', default_status=DefaultScheduleStatus.STOPPED)
