"""Unapproved USGS mineplant 2003-survey draft asset for Agricultural Minerals Operations."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
                     ScheduleDefinition, asset, asset_check, define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.agricultural_minerals import (
    AGRICULTURAL_ROWS, DATASET_SLUG, FILE_SLUG, capture_agricultural_minerals,
)


class AgriculturalMineralsConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usgs", compute_kind="http",
       description=("The six agricultural commodity classes (magnesium compounds, peat, phosphate, potash, "
                    "sulfur, vermiculite) of the producer's own USGS MRDATA mineplant package for the 2003 "
                    "Minerals Information Team survey, the survey the retired National Atlas agricultural-"
                    "minerals layer documents. Hash-pinned captures; no substitute IDs, datum correction, "
                    "identifier mapping, or release acceptance; all contract and release decisions pending."))
def agricultural_minerals(context, config: AgriculturalMineralsConfig,
                          staging_storage: StagingStorageResource) -> dict:
    result = capture_agricultural_minerals(staging_storage,
                                           capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=agricultural_minerals, blocking=True)
def agricultural_minerals_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == AGRICULTURAL_ROWS
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False,
    })


agricultural_minerals_draft_job = define_asset_job("agricultural_minerals_draft_refresh",
                                                   selection=AssetSelection.assets(agricultural_minerals))
agricultural_minerals_draft_schedule = ScheduleDefinition(
    name="agricultural_minerals_draft_refresh_schedule", job=agricultural_minerals_draft_job,
    cron_schedule="0 9 1 * *", default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [agricultural_minerals]
ASSET_CHECKS = [agricultural_minerals_population_complete]
JOBS = [agricultural_minerals_draft_job]
SCHEDULES = [agricultural_minerals_draft_schedule]
