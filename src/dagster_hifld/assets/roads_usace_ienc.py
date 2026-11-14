"""Explicit stopped-refresh Dagster asset for USACE IENC roads lines."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.roads_usace_ienc import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_roads_usace_ienc,
)


class RoadsUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE IENC ROADWAY_LINE service layer 48 (106747 features observed). The linked BTS catalog item is a proposed successor to an earlier item; catalog continuity and layer edition remain unaccepted. "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def roads_usace_ienc(context, config: RoadsUsaceIencConfig,
                     staging_storage: StagingStorageResource) -> dict:
    result = capture_roads_usace_ienc(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=roads_usace_ienc, blocking=True)
def roads_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
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


roads_usace_ienc_draft_job = define_asset_job(
    "roads_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(roads_usace_ienc),
)
roads_usace_ienc_draft_schedule = ScheduleDefinition(
    name="roads_usace_ienc_draft_refresh_schedule",
    job=roads_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [roads_usace_ienc]
ASSET_CHECKS = [roads_usace_ienc_complete]
JOBS = [roads_usace_ienc_draft_job]
SCHEDULES = [roads_usace_ienc_draft_schedule]
