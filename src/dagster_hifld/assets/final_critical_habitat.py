"""Explicit local-only draft assets for final USFWS critical habitat."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.final_critical_habitat import DATASET_SLUG, FILE_SLUG, capture_final_critical_habitat


class FinalCriticalHabitatConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usfws", compute_kind="http",
       description="Complete final critical habitat polygons from the FWS-owned service; unapproved draft candidate.")
def final_critical_habitat_features(context, config: FinalCriticalHabitatConfig,
                                    staging_storage: StagingStorageResource) -> dict:
    result = capture_final_critical_habitat(staging_storage, version=build_version_id(context),
                                            capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=final_critical_habitat_features, blocking=True)
def final_critical_habitat_complete(candidate: dict) -> AssetCheckResult:
    quality = candidate.get("feature_count", 0) > 0 and candidate.get("source_null_geometry_count") == 0 and candidate.get("source_invalid_geometry_count") == 0
    return AssetCheckResult(passed=quality, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


final_critical_habitat_draft_job = define_asset_job("final_critical_habitat_draft_refresh",
    selection=AssetSelection.assets(final_critical_habitat_features))
final_critical_habitat_draft_schedule = ScheduleDefinition(
    name="final_critical_habitat_draft_refresh_schedule", job=final_critical_habitat_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
