"""BSEE OCS Platforms candidate assets; refresh remains stopped."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.bsee_ocs_platforms import DATASET_SLUG, FILE_SLUG, capture_bsee_ocs_platforms


class BseeOcsPlatformsConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="bsee", compute_kind="http",
       description="BSEE Platforms point Shapefile for Gulf of Mexico federal OCS waters. Exact source scope and release/currentness gates remain pending.")
def bsee_ocs_platforms(context, config: BseeOcsPlatformsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_bsee_ocs_platforms(staging_storage, version=context.run_id,
                                        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=bsee_ocs_platforms, blocking=True)
def bsee_ocs_platforms_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 7317 and candidate.get("candidate_ready") and not candidate.get("release_accepted", False)
    return AssetCheckResult(passed=bool(passed), metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


bsee_ocs_platforms_draft_job = define_asset_job("bsee_ocs_platforms_draft_refresh", selection=AssetSelection.assets(bsee_ocs_platforms))
bsee_ocs_platforms_draft_schedule = ScheduleDefinition(
    name="bsee_ocs_platforms_draft_refresh_schedule", job=bsee_ocs_platforms_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
