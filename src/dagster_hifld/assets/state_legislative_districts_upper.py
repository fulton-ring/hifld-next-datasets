"""Stopped refresh asset for the current Census TIGER/Line State Legislative Districts - Upper layer."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.state_legislative_districts_upper import (
    DATASET_SLUG, FILE_SLUG, capture_state_legislative_districts_upper,
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Official Census TIGER/Line current-vintage nationwide legislative geodatabase; the "
        "State_Legislative_Districts_Upper layer is extracted to native GeoParquet. "
        "Candidate only: the DHS crosswalk row and baseline compatibility are unresolved, "
        "and source, freshness, schema/ID contract, coverage, rights, and release remain pending."
    ),
)
def state_legislative_districts_upper(context, staging_storage: StagingStorageResource) -> dict:
    if isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local or staging_storage.bucket:
        raise ValueError("state_legislative_districts_upper_draft_requires_isolated_local_staging")
    result = capture_state_legislative_districts_upper(staging_storage)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=state_legislative_districts_upper, blocking=True)
def state_legislative_districts_upper_source_complete(result: dict) -> AssetCheckResult:
    passed = (
        result.get("feature_count", 0) > 0
        and result.get("source_population_complete") is True
        and bool(result.get("source_archive_sha256"))
        and bool(result.get("native_parquet_sha256"))
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": result.get("feature_count", 0),
        "state_count": result.get("state_count", 0),
        "source_id_sha256": result.get("source_id_sha256", "missing"),
        "source_schema_sha256": result.get("source_schema_sha256", "missing"),
        "source_archive_sha256": result.get("source_archive_sha256", "missing"),
        "native_parquet_sha256": result.get("native_parquet_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


state_legislative_districts_upper_draft_job = define_asset_job(
    "state_legislative_districts_upper_draft_refresh",
    selection=AssetSelection.assets(state_legislative_districts_upper),
)
state_legislative_districts_upper_draft_schedule = ScheduleDefinition(
    name="state_legislative_districts_upper_draft_refresh_schedule",
    job=state_legislative_districts_upper_draft_job,
    cron_schedule="0 8 1 10 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [state_legislative_districts_upper]
ASSET_CHECKS = [state_legislative_districts_upper_source_complete]
JOBS = [state_legislative_districts_upper_draft_job]
SCHEDULES = [state_legislative_districts_upper_draft_schedule]
