"""Stopped integrity workflow for the frozen public 2025 NBAM copy."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.electric_retail_service_territories import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG,
    capture_electric_retail_service_territories,
)


class ElectricRetailServiceTerritoriesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="energy",
    compute_kind="http",
    description=(
        "Preserves the frozen 2025-08-21 public NBAM ArcGIS copy of the HIFLD "
        "Electric Retail Service Territories layer (2,931 observed object IDs). "
        "This is not a current-feed refresh. Upstream acceptance, rights, geographic "
        "coverage, contracts, baseline and release remain pending."
    ),
)
def electric_retail_service_territories_preservation_candidate(
    context, config: ElectricRetailServiceTerritoriesConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_electric_retail_service_territories(
        staging_storage, capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=electric_retail_service_territories_preservation_candidate, blocking=True)
def electric_retail_service_territories_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "frozen_copy_feature_count": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "native_crs": candidate.get("native_crs", "missing"),
        "source_edition": candidate.get("source_edition", "missing"),
        "producer_release_date": "unknown",
        "source_acceptance": False, "rights_acceptance": False,
        "coverage_acceptance": False, "baseline_acceptance": False,
        "release_acceptance": False,
    })


@asset_check(asset=electric_retail_service_territories_preservation_candidate, blocking=True)
def electric_retail_service_territories_geometry_valid(candidate: dict) -> AssetCheckResult:
    nulls = candidate.get("null_geometry_count")
    invalid = candidate.get("invalid_geometry_count")
    return AssetCheckResult(
        passed=type(nulls) is int and type(invalid) is int and nulls == 0 and invalid == 0,
        metadata={"null_geometry_count": nulls if type(nulls) is int else -1,
                  "invalid_geometry_count": invalid if type(invalid) is int else -1,
                  "geometry_repair_applied": False},
    )


@asset_check(asset=electric_retail_service_territories_preservation_candidate, blocking=False)
def electric_retail_service_territories_release_pending(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        description="Release remains blocked pending upstream, rights, coverage, contract and baseline acceptance.",
        metadata={"release_accepted": candidate.get("release_accepted", False)},
    )


electric_retail_service_territories_preservation_job = define_asset_job(
    "electric_retail_service_territories_frozen_snapshot_integrity",
    selection=AssetSelection.assets(electric_retail_service_territories_preservation_candidate),
)
electric_retail_service_territories_preservation_schedule = ScheduleDefinition(
    name="electric_retail_service_territories_frozen_snapshot_integrity_schedule",
    job=electric_retail_service_territories_preservation_job,
    cron_schedule="0 10 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
