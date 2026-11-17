"""Parked ingest registration for the retired USGS Miscellaneous Industrial Minerals Operations layer; always fails closed."""
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.miscellaneous_industrial_mineral_operations import (
    PARK_REASON,
    SLUG,
    stage_miscellaneous_industrial_mineral_operations,
)


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]),
    group_name="usgs",
    compute_kind="local",
    description=(
        "Parked; no maintained USGS producer publication (original National Atlas release retired; producer "
        "mineplant survey publication declares a one-time release with no planned refresh; producer 2026 NMIC "
        "release has no coordinates). Every materialization fails closed with " + PARK_REASON + ". "
        "No fetching, staging or acceptance."
    ),
)
def miscellaneous_industrial_mineral_operations_candidate(context, staging_storage: StagingStorageResource):
    stage_miscellaneous_industrial_mineral_operations(staging_storage)


miscellaneous_industrial_mineral_operations_parked_job = define_asset_job(
    "miscellaneous_industrial_mineral_operations_parked_refusal",
    selection=AssetSelection.assets(miscellaneous_industrial_mineral_operations_candidate),
)

miscellaneous_industrial_mineral_operations_parked_schedule = ScheduleDefinition(
    name="miscellaneous_industrial_mineral_operations_parked_monthly",
    job=miscellaneous_industrial_mineral_operations_parked_job,
    cron_schedule="0 7 1 * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [miscellaneous_industrial_mineral_operations_candidate]
JOBS = [miscellaneous_industrial_mineral_operations_parked_job]
SCHEDULES = [miscellaneous_industrial_mineral_operations_parked_schedule]
