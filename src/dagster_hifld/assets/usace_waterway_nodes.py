"""Unapproved BTS NTAD/USACE waterway node source candidate."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_waterway_nodes import (
    DATASET_SLUG,
    FILE_SLUG,
    EXPECTED_COUNT,
    capture_usace_waterway_nodes,
)


class UsaceWaterwayNodesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "BTS NTAD National Waterway Network nodes, sourced from the linked "
        "USACE-hosted native FileGDB layer. Candidate only: schema/ID contract, "
        "source vintage, rights, mapping, and release remain unapproved."
    ),
)
def usace_waterway_network_nodes(
    context, config: UsaceWaterwayNodesConfig, staging_storage: StagingStorageResource,
) -> dict[str, object]:
    candidate = capture_usace_waterway_nodes(
        staging_storage, capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, str(candidate["version"]))],
    )
    context.add_output_metadata(candidate)
    return candidate


@asset_check(asset=usace_waterway_network_nodes, blocking=True)
def usace_waterway_network_nodes_candidate_complete(candidate: dict[str, object]) -> AssetCheckResult:
    """Require a complete, native, internally consistent research candidate."""
    ready = (
        candidate.get("candidate_ready") is True
        and candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("native_filegdb_preserved") is True
    )
    return AssetCheckResult(
        passed=ready,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "native_filegdb_preserved": candidate.get("native_filegdb_preserved", False),
            "candidate_ready": candidate.get("candidate_ready", False),
        },
    )


usace_waterway_nodes_draft_job = define_asset_job(
    "usace_waterway_nodes_draft_refresh",
    selection=AssetSelection.assets(usace_waterway_network_nodes),
)
usace_waterway_nodes_draft_schedule = ScheduleDefinition(
    name="usace_waterway_nodes_draft_refresh_schedule",
    job=usace_waterway_nodes_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
