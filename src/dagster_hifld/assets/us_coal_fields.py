"""Parked ingest registration for the US Coal Fields layer; always fails closed."""

from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.us_coal_fields import PARK_REASON, SLUG, stage_us_coal_fields


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]),
    group_name="usgs",
    compute_kind="local",
    description=(
        "Parked; no maintained USGS producer publication of the national coal-fields polygon layer is "
        "established (origin publication OFR 2012-1205 is a completed one-time release with FGDC Progress "
        "Complete / None planned, and the live producer NCRDS CoalByRank service serves that unchanged "
        "release with no maintenance or edit metadata). Every materialization fails closed with "
        + PARK_REASON + ". No fetching, staging or acceptance."
    ),
)
def us_coal_fields(context, staging_storage: StagingStorageResource):
    stage_us_coal_fields(staging_storage)


us_coal_fields_parked_job = define_asset_job(
    "us_coal_fields_parked_refusal",
    selection=AssetSelection.assets(us_coal_fields),
)

us_coal_fields_parked_schedule = ScheduleDefinition(
    name="us_coal_fields_parked_monthly",
    job=us_coal_fields_parked_job,
    cron_schedule="0 7 1 * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [us_coal_fields]
JOBS = [us_coal_fields_parked_job]
SCHEDULES = [us_coal_fields_parked_schedule]
