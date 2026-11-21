"""Stopped draft asset for county-level EIA-861 utility service territories."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.electric_retail_service_territories_common import (
    DATASET_SLUG, FILE_SLUG, capture_electric_retail_service_territories,
)


class ElectricRetailServiceTerritoriesConfig(Config):
    capture_directory: str = ""
    replay_version: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="energy",
    compute_kind="http",
    description=(
        "County-level EIA-861 utility service territories dissolved from current "
        "Census TIGER county equivalents. This is an approximation, not a legal "
        "boundary or a historical HIFLD equivalence; all acceptance and release "
        "gates remain closed. Optional replay_version config replays one retained "
        "candidate from its staged raw evidence with zero network access; omitted "
        "runs fetch a fresh snapshot."
    ),
)
def electric_retail_service_territories_eia861_candidate(
    context, config: ElectricRetailServiceTerritoriesConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_electric_retail_service_territories(
        staging_storage, capture_directory=config.capture_directory or None,
        replay_version=config.replay_version or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=electric_retail_service_territories_eia861_candidate, blocking=True)
def electric_retail_service_territories_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and type(candidate.get("feature_count")) is int
        and candidate.get("feature_count", 0) > 0
        and type(candidate.get("utility_count")) is int
        and candidate.get("utility_count", 0) > 0
        and type(candidate.get("distinct_county_count")) is int
        and candidate.get("distinct_county_count", 0) > 0
        and type(candidate.get("county_row_count")) is int
        and candidate.get("county_row_count", 0) > 0
    )
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "utility_count": candidate.get("utility_count", 0),
        "state_count": candidate.get("state_count", 0),
        "county_row_count": candidate.get("county_row_count", 0),
        "matched_county_row_count": candidate.get("matched_county_row_count", 0),
        "distinct_county_count": candidate.get("distinct_county_count", 0),
        "skipped_not_applicable_count": candidate.get("skipped_not_applicable_count", 0),
        "source_year": candidate.get("source_year", "missing"),
        "tiger_edition": candidate.get("tiger_edition", "missing"),
        "source_acceptance": False, "rights_acceptance": False,
        "coverage_acceptance": False, "baseline_acceptance": False,
        "release_acceptance": False,
    })


@asset_check(asset=electric_retail_service_territories_eia861_candidate, blocking=True)
def electric_retail_service_territories_geometry_valid(candidate: dict) -> AssetCheckResult:
    nulls = candidate.get("null_geometry_count")
    invalid = candidate.get("invalid_geometry_count")
    return AssetCheckResult(
        passed=type(nulls) is int and type(invalid) is int and nulls == 0 and invalid == 0,
        metadata={"null_geometry_count": nulls if type(nulls) is int else -1,
                  "invalid_geometry_count": invalid if type(invalid) is int else -1,
                  "geometry_repair_applied": False},
    )


@asset_check(asset=electric_retail_service_territories_eia861_candidate, blocking=False)
def electric_retail_service_territories_release_pending(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        description="Release remains blocked pending source, rights, coverage, schema/ID, baseline and release acceptance.",
        metadata={"release_accepted": candidate.get("release_accepted", False)},
    )


electric_retail_service_territories_draft_job = define_asset_job(
    "electric_retail_service_territories_eia861_draft",
    selection=AssetSelection.assets(electric_retail_service_territories_eia861_candidate),
)
electric_retail_service_territories_draft_schedule = ScheduleDefinition(
    name="electric_retail_service_territories_eia861_draft_monthly",
    job=electric_retail_service_territories_draft_job,
    cron_schedule="0 10 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [electric_retail_service_territories_eia861_candidate]
electric_retail_service_territories_approved_baseline_drift = register_approved_baseline_drift(
    electric_retail_service_territories_eia861_candidate,
    "electric_retail_service_territories_approved_baseline_drift")
ASSET_CHECKS = [
    electric_retail_service_territories_population_complete,
    electric_retail_service_territories_geometry_valid,
    electric_retail_service_territories_release_pending,
    electric_retail_service_territories_approved_baseline_drift,
]
JOBS = [electric_retail_service_territories_draft_job]
SCHEDULES = [electric_retail_service_territories_draft_schedule]
