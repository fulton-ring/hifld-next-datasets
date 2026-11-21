"""Stopped draft-refresh asset for the FRA/BTS NTAD Railroad Bridges layer."""

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

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.railroad_bridges import (
    DATASET_SLUG,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_railroad_bridges,
)


class RailroadBridgesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bts",
    compute_kind="http",
    description=(
        "Federal Railroad Administration (FRA) Railroad Bridges from the "
        "FRA-delegated BTS NTAD FeatureServer publication named by FRA's own GIS "
        "page and the DHS HIFLD Open crosswalk for the inventory entry "
        "railroad-bridges. Unapproved faithful candidate; population completeness "
        "is enforced at capture time by the source's own count and ID witnesses. "
        "Freshness, schema/ID contract, rights and release decisions remain "
        "pending."
    ),
)
def railroad_bridges(
    context,
    config: RailroadBridgesConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_railroad_bridges(
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


@asset_check(asset=railroad_bridges, blocking=True)
def railroad_bridges_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("source_reported_count")
    passed = (
        candidate.get("source_population_complete") is True
        and type(count) is int and count > 0
        and candidate.get("feature_count") == count
        and candidate.get("candidate_ready") is not None
        and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
        and candidate.get("source_invalid_geometry_count") == EXPECTED_INVALID_GEOMETRY_COUNT
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": count if count is not None else "missing",
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "succession_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


railroad_bridges_approved_baseline_drift = register_approved_baseline_drift(
    railroad_bridges, "railroad_bridges_approved_baseline_drift")


railroad_bridges_draft_job = define_asset_job(
    "railroad_bridges_draft_refresh",
    selection=AssetSelection.assets(railroad_bridges),
)
railroad_bridges_draft_schedule = ScheduleDefinition(
    name="railroad_bridges_draft_refresh_schedule",
    job=railroad_bridges_draft_job,
    cron_schedule="0 9 4 4 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [railroad_bridges]
ASSET_CHECKS = [railroad_bridges_complete, railroad_bridges_approved_baseline_drift]
JOBS = [railroad_bridges_draft_job]
SCHEDULES = [railroad_bridges_draft_schedule]
