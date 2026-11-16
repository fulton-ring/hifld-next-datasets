"""Stopped draft-refresh asset for the official IBWC US–Mexico boundary layer."""

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.mexico_us_border import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    FILE_SLUG,
    capture_mexico_us_border,
)


class MexicoUsBorderConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="ibwc",
    compute_kind="http",
    description=(
        "Official IBWC US–Mexico international boundary FeatureServer layer 0 "
        "(US_MexicoIB, 14 source polyline segments observed). Complete observed "
        "1970 Treaty boundary feature class; unapproved faithful candidate. "
        "Crosswalk succession, source vintage, rights and schema contract remain pending."
    ),
)
def mexico_us_border(
    context,
    config: MexicoUsBorderConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_mexico_us_border(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=mexico_us_border, blocking=True)
def mexico_us_border_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_population_complete") is True
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
            "drift_guard": candidate.get("drift_guard", "missing"),
            "baseline_comparison_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


mexico_us_border_draft_job = define_asset_job(
    "mexico_us_border_draft_refresh",
    selection=AssetSelection.assets(mexico_us_border),
)
mexico_us_border_draft_schedule = ScheduleDefinition(
    name="mexico_us_border_draft_refresh_schedule",
    job=mexico_us_border_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [mexico_us_border]
ASSET_CHECKS = [mexico_us_border_complete]
JOBS = [mexico_us_border_draft_job]
SCHEDULES = [mexico_us_border_draft_schedule]
