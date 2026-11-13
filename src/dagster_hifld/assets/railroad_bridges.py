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

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.railroad_bridges import (
    DATASET_SLUG,
    EXPECTED_COUNT,
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
        "Federal Railroad Administration (FRA) Railroad Bridges (69,510 source "
        "features observed 2026-11-13) from the FRA-delegated BTS NTAD "
        "FeatureServer publication named by FRA's own GIS page and the DHS HIFLD "
        "Open crosswalk for the inventory entry railroad-bridges. Unapproved "
        "faithful candidate; record/geometry baselines are pinned from the "
        "complete dated 2026-11-13 capture. Freshness, schema/ID contract, "
        "rights and release decisions remain pending."
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
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and (EXPECTED_NULL_GEOMETRY_COUNT is None
             or candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT)
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
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
ASSET_CHECKS = [railroad_bridges_complete]
JOBS = [railroad_bridges_draft_job]
SCHEDULES = [railroad_bridges_draft_schedule]
