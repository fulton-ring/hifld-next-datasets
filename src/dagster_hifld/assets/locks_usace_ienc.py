"""Stopped draft refresh for the current USACE NDC/LPMS Locks source."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.locks_usace_ienc import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_locks_usace_ienc,
)


class LocksUsaceIencConfig(Config):
    capture_directory: str = ""
    resume_completed_replica_from: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Current official USACE NDC/LPMS Lock point layer (234 records, native "
        "EPSG:4019). All source fields and the binary annotation field are retained. "
        "Historical HIFLD archive equivalence, release vintage, baseline, schema, "
        "rights, mapping, and release decisions remain pending."
    ),
)
def locks_usace_ienc(context, config: LocksUsaceIencConfig,
                     staging_storage: StagingStorageResource) -> dict:
    result = capture_locks_usace_ienc(
        staging_storage,
        capture_directory=config.capture_directory or None,
        resume_completed_replica_from=config.resume_completed_replica_from or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=locks_usace_ienc, blocking=True)
def locks_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_crs": "EPSG:4019 (unknown datum; no inferred mapping)",
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


locks_usace_ienc_draft_job = define_asset_job(
    "locks_usace_ienc_draft_refresh", selection=AssetSelection.assets(locks_usace_ienc),
)
locks_usace_ienc_draft_schedule = ScheduleDefinition(
    name="locks_usace_ienc_draft_refresh_schedule", job=locks_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
