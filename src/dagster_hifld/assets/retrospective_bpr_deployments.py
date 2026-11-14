"""Draft-only Dagster source asset for NOAA NCEI retrospective BPR deployments."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.retrospective_bpr_deployments import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_retrospective_bpr_deployments,
)


class RetrospectiveBprDeploymentsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description=(
        "Complete native capture of NOAA NCEI's delegated Retrospective BPR Deployments layer "
        "(524 features observed; NOAA GeoPlatform service item 764d3a99a4f8440ea05522815b5f5e19, "
        "layer 1). Unapproved candidate; archive equivalence, source edition, schema/ID contract, "
        "coverage, rights, baseline and release decisions remain pending."
    ),
)
def retrospective_bpr_deployments(context, config: RetrospectiveBprDeploymentsConfig,
                                  staging_storage: StagingStorageResource) -> dict:
    result = capture_retrospective_bpr_deployments(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=retrospective_bpr_deployments, blocking=True)
def retrospective_bpr_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True)
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


retrospective_bpr_draft_job = define_asset_job(
    "retrospective_bpr_draft_refresh",
    selection=AssetSelection.assets(retrospective_bpr_deployments),
)
retrospective_bpr_draft_schedule = ScheduleDefinition(
    name="retrospective_bpr_draft_refresh_schedule",
    job=retrospective_bpr_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [retrospective_bpr_deployments]
ASSET_CHECKS = [retrospective_bpr_population_complete]
JOBS = [retrospective_bpr_draft_job]
SCHEDULES = [retrospective_bpr_draft_schedule]
