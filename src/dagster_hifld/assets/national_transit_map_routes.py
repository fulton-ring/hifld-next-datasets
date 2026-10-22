"""Draft source and blocking completeness gate for current BTS NTM routes."""
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

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_transit_map_routes import (
    DATASET_SLUG,
    EXPECTED_SOURCE_COUNT,
    FILE_SLUG,
    capture_national_transit_map_routes,
)


class NationalTransitMapRoutesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bureau_of_transportation_statistics",
    compute_kind="http",
    description=(
        "Current BTS NTAD National Transit Map Routes line layer, described as "
        "compiled 2026-09-23. Preserves source fields, IDs, and native EPSG:4326 "
        "paths. DataLumos V1 archive equivalence and all source, baseline, rights, "
        "contract, and release approvals remain pending."
    ),
)
def national_transit_map_routes(
    context,
    config: NationalTransitMapRoutesConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_national_transit_map_routes(
        staging_storage, capture_directory=config.capture_directory or None
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=national_transit_map_routes, blocking=True)
def national_transit_map_routes_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and candidate.get("feature_count") == EXPECTED_SOURCE_COUNT
        and candidate.get("source_null_geometry_count") == 0
        and candidate.get("source_invalid_geometry_count") == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "baseline_accepted": False,
            "archive_equivalence_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


national_transit_map_routes_draft_job = define_asset_job(
    "national_transit_map_routes_draft_refresh",
    selection=AssetSelection.assets(national_transit_map_routes),
)
national_transit_map_routes_draft_schedule = ScheduleDefinition(
    name="national_transit_map_routes_draft_refresh_schedule",
    job=national_transit_map_routes_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
