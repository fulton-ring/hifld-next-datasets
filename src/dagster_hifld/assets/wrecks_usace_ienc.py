"""Explicit stopped-refresh Dagster asset for USACE IENC wrecks points."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.wrecks_usace_ienc import (
    DATASET_SLUG, FILE_SLUG, capture_wrecks_usace_ienc,
)


class WrecksUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE IENC WRECKS_POINT MapServer layer 31 (560 rows observed), exposed through the "
        "USDOT/BTS NTAD IENC catalog record. Unapproved faithful candidate; source release date, "
        "baseline, schema/ID contract, coverage, rights, and release decisions remain pending."
    ),
)
def wrecks_usace_ienc(context, config: WrecksUsaceIencConfig,
                      staging_storage: StagingStorageResource) -> dict:
    result = capture_wrecks_usace_ienc(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=wrecks_usace_ienc, blocking=True)
def wrecks_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    # Fail closed when any completeness metric is missing or nonzero: a missing
    # metric is a refused candidate, never an implied zero.
    passed = (candidate.get("feature_count") == candidate.get("source_reported_count")
              and candidate.get("source_population_complete") is True
              and candidate.get("geometry_valid_count") == candidate.get("feature_count")
              and candidate.get("geometry_invalid_count") == 0
              and candidate.get("geometry_empty_count") == 0
              and bool(candidate.get("source_id_sha256")))
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_vertex_count": candidate.get("source_vertex_count", "missing"),
        "geometry_valid_count": candidate.get("geometry_valid_count", "missing"),
        "geometry_invalid_count": candidate.get("geometry_invalid_count", "missing"),
        "geometry_empty_count": candidate.get("geometry_empty_count", "missing"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "drift_guard": candidate.get("drift_guard", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


wrecks_usace_ienc_draft_job = define_asset_job(
    "wrecks_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(wrecks_usace_ienc),
)
wrecks_usace_ienc_draft_schedule = ScheduleDefinition(
    name="wrecks_usace_ienc_draft_refresh_schedule",
    job=wrecks_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

wrecks_usace_ienc_drift = register_approved_baseline_drift(
    wrecks_usace_ienc, "wrecks_usace_ienc_drift")

INGEST_ASSETS = [wrecks_usace_ienc]
ASSET_CHECKS = [wrecks_usace_ienc_complete, wrecks_usace_ienc_drift]
JOBS = [wrecks_usace_ienc_draft_job]
SCHEDULES = [wrecks_usace_ienc_draft_schedule]
