"""Explicit stopped-refresh Dagster asset for the EPA-distributed ORNL day care centers."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.child_care_centers import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_child_care_centers,
)


class ChildCareCentersConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="doe",
    compute_kind="http",
    description=(
        "Day Care Centers layer 9 of the EPA OEI SEGS OEI/ORNL_Education service: the 2015 ORNL "
        "Freedom edition of the national day care centers spatial database produced by ORNL's GIST "
        "Group (115036 features observed). The retired 2021-2022 HIFLD edition, the retired HIFLD "
        "hub and HIFLD rescue archives are not pipeline inputs; edition succession and source "
        "authority remain unaccepted. Unapproved candidate; baseline, contract, coverage, rights "
        "and release decisions remain pending."
    ),
)
def child_care_centers(context, config: ChildCareCentersConfig,
                       staging_storage: StagingStorageResource) -> dict:
    result = capture_child_care_centers(staging_storage, version=build_version_id(context),
                                        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=child_care_centers, blocking=True)
def child_care_centers_complete(candidate: dict) -> AssetCheckResult:
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


child_care_centers_draft_job = define_asset_job(
    "child_care_centers_draft_refresh",
    selection=AssetSelection.assets(child_care_centers),
)
child_care_centers_draft_schedule = ScheduleDefinition(
    name="child_care_centers_draft_refresh_schedule",
    job=child_care_centers_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
