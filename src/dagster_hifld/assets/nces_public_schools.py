"""Stopped candidate pipeline for NCES public-school locations."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nces_public_schools import (
    DATASET_SLUG, EXPECTED_FEATURE_COUNT, FILE_SLUG, capture_nces_public_schools,
)


class NcesPublicSchoolsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="nces",
    compute_kind="http",
    description=(
        "Official NCES EDGE 2024-25 public-school locations, preserved as a complete "
        "unapproved producer-source candidate. HIFLD historical equivalence and the "
        "published source/schema/coverage/rights decisions remain pending."
    ),
)
def nces_public_schools(context, config: NcesPublicSchoolsConfig,
                        staging_storage: StagingStorageResource) -> dict:
    result = capture_nces_public_schools(
        staging_storage, capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=nces_public_schools, blocking=True)
def nces_public_schools_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_FEATURE_COUNT
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_sha256": candidate.get("source_capture_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "baseline_comparison_accepted": False,
        "release_accepted": False,
    })


nces_public_schools_draft_job = define_asset_job(
    "nces_public_schools_draft_refresh",
    selection=AssetSelection.assets(nces_public_schools),
)
nces_public_schools_draft_schedule = ScheduleDefinition(
    name="nces_public_schools_draft_refresh_schedule",
    job=nces_public_schools_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
