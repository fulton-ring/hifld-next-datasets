"""Dagster entrypoint for the EPA FRS national NAICS CSV candidate."""
from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_naics_national import (
    DATASET_SLUG,
    EXPECTED_COLUMNS,
    EXPECTED_CSV_SHA256,
    EXPECTED_SOURCE_ROWS,
    FILE_SLUG,
    PRIOR_CANDIDATE_ROWS,
    capture_epa_frs_national_naics,
)


class EpaFrsNationalNaicsConfig(Config):
    capture_directory: str = ""
    retained_capture_directory: str = ""
    retained_capture_manifest_sha256: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="epa", compute_kind="http",
       description="Bounded range-request capture of EPA's official combined national CSV "
                   "NATIONAL_NAICS_FILE.CSV member; nonspatial draft, release remains gated.")
def epa_frs_national_naics(context, config: EpaFrsNationalNaicsConfig,
                           staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_frs_national_naics(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        retained_capture_directory=config.retained_capture_directory or None,
        retained_capture_manifest_sha256=config.retained_capture_manifest_sha256 or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_national_naics, blocking=True)
def epa_frs_national_naics_capture_complete(candidate: dict) -> AssetCheckResult:
    row_count = candidate.get("row_count", 0)
    csv_sha = candidate.get("source_csv_sha256", "")
    columns = candidate.get("native_columns", [])
    passed = (row_count == EXPECTED_SOURCE_ROWS and csv_sha == EXPECTED_CSV_SHA256
              and columns == list(EXPECTED_COLUMNS))
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_rows": row_count,
        "source_csv_sha256": csv_sha,
        "native_columns": columns,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


@asset_check(asset=epa_frs_national_naics, blocking=False)
def epa_frs_national_naics_membership_compatible(candidate: dict) -> AssetCheckResult:
    compatible = not candidate.get("drift_reasons")
    return AssetCheckResult(passed=compatible, metadata={
        "candidate_rows": candidate.get("row_count", 0),
        "prior_candidate_rows": PRIOR_CANDIDATE_ROWS,
        "prior_candidate_independently_verified": False,
        "drift_reasons": candidate.get("drift_reasons", []),
        "release_accepted": False,
    })


epa_frs_national_naics_draft_job = define_asset_job(
    "epa_frs_national_naics_draft_refresh",
    selection=AssetSelection.assets(epa_frs_national_naics),
)
epa_frs_national_naics_draft_schedule = ScheduleDefinition(
    name="epa_frs_national_naics_draft_refresh_schedule", job=epa_frs_national_naics_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {epa_frs_national_naics.op.name: {
        "config": {"capture_directory": "", "retained_capture_directory": "",
                   "retained_capture_manifest_sha256": ""}
    }}},
)
