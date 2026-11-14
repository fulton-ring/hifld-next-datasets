"""Explicit stopped-refresh Dagster asset for US District Court Jurisdictions."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.us_district_court_jurisdictions import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_us_district_court_jurisdictions,
)


class UsDistrictCourtJurisdictionsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="judicial",
    compute_kind="http",
    description=(
        "Administrative Office of the U.S. Courts (Judicial Branch, AOUSC) hosted "
        "'TMB U.S. Courts Districts Final' FeatureServer layer 5 (the 94 federal "
        "judicial districts plus an unassigned null-geometry 'XXX' row, data edited "
        "March 2026) after the DHS HIFLD Open crosswalk retired the layer with no "
        "named successor. Unapproved candidate; source acceptance, baseline, "
        "contract, coverage, rights, and release decisions remain pending."
    ),
)
def us_district_court_jurisdictions(
    context,
    config: UsDistrictCourtJurisdictionsConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_us_district_court_jurisdictions(
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


@asset_check(asset=us_district_court_jurisdictions, blocking=True)
def us_district_court_jurisdictions_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_population_complete") is True
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
            "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
            "baseline_comparison_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


us_district_court_jurisdictions_draft_job = define_asset_job(
    "us_district_court_jurisdictions_draft_refresh",
    selection=AssetSelection.assets(us_district_court_jurisdictions),
)
us_district_court_jurisdictions_draft_schedule = ScheduleDefinition(
    name="us_district_court_jurisdictions_draft_refresh_schedule",
    job=us_district_court_jurisdictions_draft_job,
    cron_schedule="0 8 5,19 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [us_district_court_jurisdictions]
ASSET_CHECKS = [us_district_court_jurisdictions_complete]
JOBS = [us_district_court_jurisdictions_draft_job]
SCHEDULES = [us_district_court_jurisdictions_draft_schedule]
