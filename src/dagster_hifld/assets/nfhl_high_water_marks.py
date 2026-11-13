"""Draft Dagster assets for the FEMA NFHL High Water Marks capture."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nfhl_high_water_marks import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_NAN_GEOMETRY_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_nfhl_high_water_marks,
)


class NfhlHighWaterMarksConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fema",
    compute_kind="http",
    description=(
        "FEMA NFHL MapServer layer 12 'High Water Marks' from the producer's own REST publication. "
        "Candidate extraction only; release date, freshness, rights, and schema/ID acceptance remain pending."
    ),
)
def fema_nfhl_high_water_marks(context, config: NfhlHighWaterMarksConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_nfhl_high_water_marks(
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


@asset_check(asset=fema_nfhl_high_water_marks, blocking=True)
def fema_nfhl_high_water_marks_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
              and candidate.get("source_nan_string_geometry_count") == EXPECTED_NAN_GEOMETRY_COUNT)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
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


nfhl_high_water_marks_draft_job = define_asset_job(
    "nfhl_high_water_marks_draft_refresh",
    selection=AssetSelection.assets(fema_nfhl_high_water_marks),
)
nfhl_high_water_marks_draft_schedule = ScheduleDefinition(
    name="nfhl_high_water_marks_draft_refresh_schedule",
    job=nfhl_high_water_marks_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [fema_nfhl_high_water_marks]
ASSET_CHECKS = [fema_nfhl_high_water_marks_complete]
JOBS = [nfhl_high_water_marks_draft_job]
SCHEDULES = [nfhl_high_water_marks_draft_schedule]
