"""Draft Oil and Natural Gas Platforms asset from BSEE/BOEM platform tables."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.oil_and_natural_gas_platforms import (
    DATASET_SLUG, FILE_SLUG, GATES, capture_oil_and_natural_gas_platforms,
)


class OilAndNaturalGasPlatformsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="energy",
    compute_kind="http",
    description=(
        "Current BSEE Gulf and BOEM Pacific platform master, structure and location tables. "
        "Gulf of Mexico federal OCS and U.S. Pacific OCS only; historical ORNL/HIFLD equivalence, "
        "coverage, datum and all release approvals remain pending."
    ),
)
def oil_and_natural_gas_platforms(
    context, config: OilAndNaturalGasPlatformsConfig, staging_storage: StagingStorageResource,
) -> dict:
    result = capture_oil_and_natural_gas_platforms(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=oil_and_natural_gas_platforms, blocking=True)
def oil_and_natural_gas_platforms_population_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("source_count_witness")
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("source_id_roster_complete") is True
              and type(count) is int and count > 0
              and candidate.get("feature_count") == count)
    return AssetCheckResult(passed=passed, metadata={
        **GATES,
        "source_feature_count": candidate.get("feature_count", 0),
        "source_count_witness": count if count is not None else "missing",
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "coverage_note": "Gulf of Mexico federal OCS and U.S. Pacific OCS only.",
    }, description="Capture-time completeness only; source, coverage, mapping and release acceptance remain closed.")


@asset_check(asset=oil_and_natural_gas_platforms, blocking=True)
def oil_and_natural_gas_platforms_geometry_quality(candidate: dict) -> AssetCheckResult:
    invalid = candidate.get("invalid_geometry_count")
    nulls = candidate.get("null_geometry_count")
    passed = (candidate.get("source_population_complete") is True
              and invalid == 0 and type(nulls) is int and nulls >= 0)
    return AssetCheckResult(passed=passed, metadata={
        "invalid_source_coordinate_count": invalid if invalid is not None else "missing",
        "null_source_geometry_count": nulls if nulls is not None else "missing",
        "zero_coordinate_count": candidate.get("zero_coordinate_count", "missing"),
        "crs": "proposed EPSG:4267; datum acceptance false",
        "coverage_acceptance": False,
    })


@asset_check(asset=oil_and_natural_gas_platforms, blocking=False)
def oil_and_natural_gas_platforms_approved_baseline_drift(candidate: dict) -> AssetCheckResult:
    return approved_baseline_drift(candidate)


oil_and_natural_gas_platforms_draft_job = define_asset_job(
    "oil_and_natural_gas_platforms_draft_refresh",
    selection=AssetSelection.assets(oil_and_natural_gas_platforms))
oil_and_natural_gas_platforms_draft_schedule = ScheduleDefinition(
    name="oil_and_natural_gas_platforms_draft_refresh_schedule",
    job=oil_and_natural_gas_platforms_draft_job,
    cron_schedule="0 9 2 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [oil_and_natural_gas_platforms]
ASSET_CHECKS = [
    oil_and_natural_gas_platforms_population_complete,
    oil_and_natural_gas_platforms_geometry_quality,
    oil_and_natural_gas_platforms_approved_baseline_drift,
]
JOBS = [oil_and_natural_gas_platforms_draft_job]
SCHEDULES = [oil_and_natural_gas_platforms_draft_schedule]
