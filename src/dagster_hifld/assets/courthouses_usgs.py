"""Draft-only registered USGS Courthouses source and population gate."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.courthouses_usgs import EXPECTED_COUNT, capture_courthouses

DATASET_SLUG = FILE_SLUG = "courthouses-3"


class CourthousesUSGSConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "Complete observed current USGS National Structures Courthouses layer, captured as an "
        "unapproved candidate. HIFLD 2024 equivalence, producer release date, rights, schema, "
        "coverage, and migration decisions remain pending."
    ),
)
def courthouses_usgs(context, config: CourthousesUSGSConfig,
                     staging_storage: StagingStorageResource) -> dict:
    result = capture_courthouses(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=courthouses_usgs, blocking=True)
def courthouses_population_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("source_population_complete") is True and candidate.get("feature_count") == EXPECTED_COUNT
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
    })


courthouses_draft_job = define_asset_job(
    "courthouses_draft_refresh", selection=AssetSelection.assets(courthouses_usgs),
)
courthouses_draft_schedule = ScheduleDefinition(
    name="courthouses_draft_refresh_schedule",
    job=courthouses_draft_job,
    cron_schedule="0 9 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
