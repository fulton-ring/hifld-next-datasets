"""Explicit Dagster ingest/check/job for the EPA RCRA TSD draft candidate."""
from dagster import (
    AssetCheckResult, AssetKey, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check,
)

from dagster_hifld.assets.staging_refresh import define_staging_capture_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_rcra_tsd import DATASET_SLUG, FILE_SLUG, capture_epa_rcra_tsd


class EpaRcraTsdConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "EPA FRS RCRAInfo hazardous-waste treatment, storage, and disposal facilities. "
        "Draft only; source-vintage and publication-contract approval remain pending."
    ),
)
def epa_rcra_treatment_storage_and_disposal_facilities(
    context, config: EpaRcraTsdConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_epa_rcra_tsd(
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


@asset_check(asset=epa_rcra_treatment_storage_and_disposal_facilities, blocking=True)
def epa_rcra_tsd_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count")
    passed = isinstance(count, int) and count > 0 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": count or 0,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "date_conversion_approved": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_rcra_tsd_draft_job = define_staging_capture_job(
    "epa_rcra_tsd_draft_refresh", epa_rcra_treatment_storage_and_disposal_facilities,
)
epa_rcra_tsd_draft_schedule = ScheduleDefinition(
    name="epa_rcra_tsd_draft_refresh_schedule",
    job=epa_rcra_tsd_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [epa_rcra_treatment_storage_and_disposal_facilities]
ASSET_CHECKS = [epa_rcra_tsd_complete]
JOBS = [epa_rcra_tsd_draft_job]
SCHEDULES = [epa_rcra_tsd_draft_schedule]
