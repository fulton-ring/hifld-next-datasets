"""Explicit stopped-refresh Dagster asset for the USGS WBDLine layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.wbd_wbdline import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_wbd_wbdline,
)


class WbdWbdlineConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS hydro.nationalmap.gov wbd MapServer layer 0 'WBDLine' "
        "(395,291 features observed), named by the DHS HIFLD Open crosswalk as the "
        "agency's current open REST publication. Unapproved candidate; succession, "
        "baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def wbd_wbdline(context, config: WbdWbdlineConfig,
                staging_storage: StagingStorageResource) -> dict:
    result = capture_wbd_wbdline(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=wbd_wbdline, blocking=True)
def wbd_wbdline_complete(candidate: dict) -> AssetCheckResult:
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


wbd_wbdline_draft_job = define_asset_job(
    "wbd_wbdline_draft_refresh",
    selection=AssetSelection.assets(wbd_wbdline),
)
wbd_wbdline_draft_schedule = ScheduleDefinition(
    name="wbd_wbdline_draft_schedule",
    job=wbd_wbdline_draft_job,
    cron_schedule="0 8 7,21 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [wbd_wbdline]
ASSET_CHECKS = [wbd_wbdline_complete]
JOBS = [wbd_wbdline_draft_job]
SCHEDULES = [wbd_wbdline_draft_schedule]
