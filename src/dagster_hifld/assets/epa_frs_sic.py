"""EPA FRS SIC code table draft ingest asset."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_sic import DATASET_SLUG, FILE_SLUG, EXPECTED_ROWS, capture_epa_frs_sic


class EpaFrsSicConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Exact EPA FRS_INTERESTS FRS_SIC code table (full FRS facility universe). "
        "The native field/ID contract, source freshness, and publication approvals "
        "remain pending; refresh stays stopped."
    ),
)
def epa_frs_sic_codes(context, config: EpaFrsSicConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_frs_sic(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_sic_codes, blocking=True)
def epa_frs_sic_codes_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and candidate.get("row_count") == EXPECTED_ROWS
        and isinstance(candidate.get("source_record_sha256"), str)
        and isinstance(candidate.get("source_schema_sha256"), str)
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_rows": candidate.get("row_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


@asset_check(asset=epa_frs_sic_codes, blocking=False)
def epa_frs_sic_codes_publication_gates_pending(candidate: dict) -> AssetCheckResult:
    pending = (candidate.get("release_accepted") is False
               and candidate.get("freshness_accepted") is False
               and candidate.get("mapping_approved") is False)
    return AssetCheckResult(
        passed=pending,
        metadata={
            "refresh_status": "STOPPED",
            "release_accepted": candidate.get("release_accepted"),
            "source_release_accepted": candidate.get("source_release_accepted"),
            "candidate_ready": candidate.get("candidate_ready"),
        },
    )


epa_frs_sic_codes_draft_job = define_asset_job(
    "epa_frs_sic_codes_draft_refresh",
    selection=AssetSelection.assets(epa_frs_sic_codes),
)
epa_frs_sic_codes_draft_schedule = ScheduleDefinition(
    name="epa_frs_sic_codes_draft_refresh_schedule",
    job=epa_frs_sic_codes_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
