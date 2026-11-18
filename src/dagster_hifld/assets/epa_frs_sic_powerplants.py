"""EPA FRS EIA-860 SIC codes from the current national FRS archive."""
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
from dagster_hifld.sources.epa_frs_sic_national import (
    DATASET_SLUG,
    ENVIROFACTS_EIA860_FACILITY_COUNT,
    EXPECTED_EIA860_PROGRAM_ROWS,
    EXPECTED_OUTPUT_ROWS,
    EXPECTED_OUTPUT_SHA256,
    EXPECTED_SCHEMA_SHA256,
    FILE_SLUG,
    OUTPUT_COLUMNS,
    capture_epa_frs_eia860_sic,
)

EXPECTED_ROWS = EXPECTED_OUTPUT_ROWS


class EPAFRSEIA860SICConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "EIA-860 facility SIC codes derived from EPA's current combined national "
        "FRS CSV archive; nonspatial draft with release gated."
    ),
)
def epa_frs_sic_powerplants(context, config: EPAFRSEIA860SICConfig,
                            staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_frs_eia860_sic(
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


@asset_check(asset=epa_frs_sic_powerplants, blocking=True)
def epa_frs_sic_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("row_count") == EXPECTED_OUTPUT_ROWS
        and candidate.get("source_record_sha256") == EXPECTED_OUTPUT_SHA256
        and candidate.get("source_schema_sha256") == EXPECTED_SCHEMA_SHA256
        and candidate.get("eia860_program_rows") == EXPECTED_EIA860_PROGRAM_ROWS
    )
    return AssetCheckResult(passed=passed, metadata={
        "source_complete_rows": candidate.get("row_count", 0),
        "source_record_sha256": candidate.get("source_record_sha256", ""),
        "source_schema_sha256": candidate.get("source_schema_sha256", ""),
        "native_columns": candidate.get("native_columns", list(OUTPUT_COLUMNS)),
        "eia860_program_rows": candidate.get("eia860_program_rows", 0),
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


@asset_check(asset=epa_frs_sic_powerplants, blocking=False)
def epa_frs_sic_published_membership_compatible(candidate: dict) -> AssetCheckResult:
    eia860_rows = candidate.get("eia860_program_rows")
    compatible = eia860_rows == ENVIROFACTS_EIA860_FACILITY_COUNT
    return AssetCheckResult(passed=compatible, metadata={
        "eia860_program_rows": eia860_rows,
        "envirofacts_eia860_facility_count": ENVIROFACTS_EIA860_FACILITY_COUNT,
        "drift_reasons": candidate.get("drift_reasons", []),
        "release_accepted": False,
    })


epa_frs_sic_draft_job = define_asset_job(
    "epa_frs_sic_draft_refresh", selection=AssetSelection.assets(epa_frs_sic_powerplants)
)
epa_frs_sic_draft_schedule = ScheduleDefinition(
    name="epa_frs_sic_draft_refresh_schedule", job=epa_frs_sic_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {epa_frs_sic_powerplants.op.name: {"config": {"capture_directory": ""}}}},
)

INGEST_ASSETS = [epa_frs_sic_powerplants]
ASSET_CHECKS = [epa_frs_sic_complete, epa_frs_sic_published_membership_compatible]
JOBS = [epa_frs_sic_draft_job]
SCHEDULES = [epa_frs_sic_draft_schedule]
