"""Explicit stopped-refresh Dagster asset for USACE IENC railways lines."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.railways_usace_ienc import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_railways_usace_ienc,
)


class RailwaysUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE IENC RAILROAD_LINE service layer 45 (10572 features observed). The linked BTS catalog item is a proposed successor to an earlier item; catalog continuity and layer edition remain unaccepted. "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def railways_usace_ienc(context, config: RailwaysUsaceIencConfig,
                        staging_storage: StagingStorageResource) -> dict:
    result = capture_railways_usace_ienc(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=railways_usace_ienc, blocking=True)
def railways_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


railways_usace_ienc_draft_job = define_asset_job(
    "railways_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(railways_usace_ienc),
)
railways_usace_ienc_draft_schedule = ScheduleDefinition(
    name="railways_usace_ienc_draft_refresh_schedule",
    job=railways_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [railways_usace_ienc]
ASSET_CHECKS = [railways_usace_ienc_complete]
JOBS = [railways_usace_ienc_draft_job]
SCHEDULES = [railways_usace_ienc_draft_schedule]
