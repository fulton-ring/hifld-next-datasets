"""Congressional Districts (Census TIGER/Line CD layer).

The dataset slug is edition-neutral. The TIGER vintage and Congress number are
derived at capture time from the official Census CD directory, so a new
redistricting does not need a code change. The Census Bureau publishes one ZIP
per state or equivalent area; there is no national ZIP in that directory.
"""

from dagster import (
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.congressional_districts import (
    DATASET_SLUG,
    FILE_SLUG,
    stage_congressional_districts_candidate,
)


class CongressionalDistrictsConfig(Config):
    acquisitions_directory: str = ""


_DESCRIPTION = (
    "Congressional Districts. Publisher: U.S. Census Bureau. "
    "Census TIGER/Line CD archives for every state and equivalent area in the "
    "newest published TIGER edition; the Congress number is derived from the "
    "official archive names. Strict source validation and native GeoParquet "
    "staging run before a version is written."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def congressional_districts(
    context, config: CongressionalDistrictsConfig, staging_storage: StagingStorageResource
) -> dict:
    """Acquire the current official edition or replay a preserved capture."""
    return stage_congressional_districts_candidate(
        staging_storage,
        DATASET_SLUG,
        FILE_SLUG,
        acquisitions_dir=config.acquisitions_directory or None,
    )


congressional_districts_source_refresh_job = define_asset_job(
    name="congressional_districts_source_refresh",
    selection=AssetSelection.assets(congressional_districts),
)
congressional_districts_source_refresh_schedule = ScheduleDefinition(
    name="congressional_districts_monthly_source_refresh",
    job=congressional_districts_source_refresh_job,
    cron_schedule="0 10 1 * *",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [congressional_districts]
JOBS = [congressional_districts_source_refresh_job]
SCHEDULES = [congressional_districts_source_refresh_schedule]
