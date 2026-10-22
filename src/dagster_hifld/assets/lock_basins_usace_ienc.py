"""Stopped draft refresh for the official USACE IENC LOCK_BASIN_AREA layer."""

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
from dagster_hifld.sources.lock_basins_usace_ienc import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    FILE_SLUG,
    capture_lock_basins_usace_ienc,
)


class LockBasinsUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE/Army Geospatial Center IENC LOCK_BASIN_AREA MapServer layer 77. "
        "A bounded metadata and IDs-only preflight pinned 174 OBJECTIDs; feature-level "
        "completeness is checked on capture. Any candidate is limited to inland-chart coverage, "
        "inland-chart coverage, not a claim of nationwide lock-basin completeness. Historical HIFLD continuity, vintage, "
        "rights and schema acceptance remain pending."
    ),
)
def lock_basins_usace_ienc_candidate(
    context,
    config: LockBasinsUsaceIencConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_lock_basins_usace_ienc(
        staging_storage,
        version=context.run_id,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=lock_basins_usace_ienc_candidate, blocking=True)
def lock_basins_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
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


lock_basins_usace_ienc_draft_job = define_asset_job(
    "lock_basins_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(lock_basins_usace_ienc_candidate),
)
lock_basins_usace_ienc_draft_schedule = ScheduleDefinition(
    name="lock_basins_usace_ienc_draft_refresh_schedule",
    job=lock_basins_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
