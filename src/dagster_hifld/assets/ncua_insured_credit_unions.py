"""Stopped, candidate-only NCUA Locator source capture."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ncua_insured_credit_unions import (
    DATASET_SLUG, FILE_SLUG, capture_ncua_candidate,
)


class NcuaInsuredCreditUnionsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="ncua",
    compute_kind="http",
    description=(
        "Unapproved NCUA Locator point candidate queried from the active charter roster. "
        "Quarterly branch alignment and historical HIFLD successor equivalence remain release gates."
    ),
)
def ncua_insured_credit_unions_source(
    context, config: NcuaInsuredCreditUnionsConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_ncua_candidate(
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


@asset_check(asset=ncua_insured_credit_unions_source, blocking=True)
def ncua_insured_credit_unions_capture_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("capture_complete") is True and candidate.get("feature_count", 0) > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "feature_count": candidate.get("feature_count", 0),
            "active_charter_count": candidate.get("active_charter_count", 0),
            "source_release_accepted": False,
            "quarterly_companion_alignment_accepted": False,
            "historical_successor_equivalence_accepted": False,
            "release_accepted": False,
        },
    )


@asset_check(asset=ncua_insured_credit_unions_source, blocking=False)
def ncua_insured_credit_unions_release_gates(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("release_accepted") is True
    return AssetCheckResult(
        passed=passed,
        metadata={
            "source_release_accepted": candidate.get("source_release_accepted", False),
            "schema_contract_accepted": candidate.get("schema_contract_accepted", False),
            "id_contract_accepted": candidate.get("id_contract_accepted", False),
            "freshness_accepted": candidate.get("freshness_accepted", False),
            "rights_accepted": candidate.get("rights_accepted", False),
            "quarterly_companion_alignment_accepted": candidate.get("quarterly_companion_alignment_accepted", False),
            "historical_successor_equivalence_accepted": candidate.get("historical_successor_equivalence_accepted", False),
        },
    )


ncua_insured_credit_unions_draft_job = define_asset_job(
    "ncua_insured_credit_unions_draft_refresh",
    selection=AssetSelection.assets(ncua_insured_credit_unions_source),
)
ncua_insured_credit_unions_draft_schedule = ScheduleDefinition(
    name="ncua_insured_credit_unions_draft_refresh_schedule",
    job=ncua_insured_credit_unions_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
