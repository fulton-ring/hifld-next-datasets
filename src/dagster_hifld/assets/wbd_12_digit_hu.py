"""Explicit stopped-refresh Dagster asset for the USGS WBD 12-digit HU layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.wbd_12_digit_hu import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_wbd_12_digit_hu,
)


class Wbd12DigitHuConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS hydro.nationalmap.gov wbd MapServer layer 6 '12-digit HU (Subwatershed)' "
        "(103,068 features observed), named by the DHS HIFLD Open crosswalk as the "
        "agency's current open REST publication. Unapproved candidate; succession, "
        "baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def wbd_12_digit_hu(context, config: Wbd12DigitHuConfig,
                    staging_storage: StagingStorageResource) -> dict:
    result = capture_wbd_12_digit_hu(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=wbd_12_digit_hu, blocking=True)
def wbd_12_digit_hu_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT \
        and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "null_geometry_count": candidate.get("null_geometry_count", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


wbd_12_digit_hu_draft_job = define_asset_job(
    "wbd_12_digit_hu_draft_refresh",
    selection=AssetSelection.assets(wbd_12_digit_hu),
)
wbd_12_digit_hu_draft_schedule = ScheduleDefinition(
    name="wbd_12_digit_hu_draft_schedule",
    job=wbd_12_digit_hu_draft_job,
    cron_schedule="0 8 7,21 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [wbd_12_digit_hu]
ASSET_CHECKS = [wbd_12_digit_hu_complete]
JOBS = [wbd_12_digit_hu_draft_job]
SCHEDULES = [wbd_12_digit_hu_draft_schedule]
