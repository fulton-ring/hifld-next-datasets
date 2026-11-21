"""Draft Dagster assets for the FEMA NFHL LOMAs capture."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nfhl_lomas import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_nfhl_lomas,
)


class NfhlLomasConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fema",
    compute_kind="http",
    description=(
        "FEMA NFHL MapServer layer 34 'LOMAs' from the producer's own REST publication. "
        "Completeness is witnessed at capture time by the producer's own before/after count "
        "and ID rosters plus the walked population; upstream edits surface as a non-blocking "
        "approved-baseline drift warning. Candidate extraction only — release date, "
        "freshness, rights, and schema/ID acceptance remain pending."
    ),
)
def fema_nfhl_lomas(context, config: NfhlLomasConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_nfhl_lomas(
        staging_storage,
        version="v20261112",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=fema_nfhl_lomas, blocking=True)
def fema_nfhl_lomas_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("source_reported_count")
    passed = (candidate.get("source_population_complete") is True
              and type(count) is int and count > 0
              and candidate.get("feature_count") == count
              and candidate.get("candidate_ready") is not None)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": count if count is not None else "missing",
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_null_geometry_count": candidate.get("source_null_geometry_count", 0),
            "source_nan_string_geometry_count": candidate.get("source_nan_string_geometry_count", 0),
            "technical_geometry_quality_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


fema_nfhl_lomas_approved_baseline_drift = register_approved_baseline_drift(
    fema_nfhl_lomas, "fema_nfhl_lomas_approved_baseline_drift")


nfhl_lomas_draft_job = define_asset_job(
    "nfhl_lomas_draft_refresh",
    selection=AssetSelection.assets(fema_nfhl_lomas),
)
nfhl_lomas_draft_schedule = ScheduleDefinition(
    name="nfhl_lomas_draft_refresh_schedule",
    job=nfhl_lomas_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [fema_nfhl_lomas]
ASSET_CHECKS = [fema_nfhl_lomas_complete, fema_nfhl_lomas_approved_baseline_drift]
JOBS = [nfhl_lomas_draft_job]
SCHEDULES = [nfhl_lomas_draft_schedule]
