"""Explicit stopped-refresh draft asset for the proposed FWS habitat layer."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.proposed_critical_habitat import DATASET_SLUG, FILE_SLUG, capture_proposed_critical_habitat

class ProposedCriticalHabitatConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usfws", compute_kind="http",
       description="Complete current FWS Proposed Critical Habitat polygon layer (layer 2); 70 features observed. Historical HIFLD baseline equivalence, contract, rights and release remain unapproved.")
def proposed_critical_habitat_features(context, config: ProposedCriticalHabitatConfig,
                                      staging_storage: StagingStorageResource) -> dict:
    result = capture_proposed_critical_habitat(staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=proposed_critical_habitat_features, blocking=True)
def proposed_critical_habitat_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count", 0) > 0
              and candidate.get("source_unique_id_count") == candidate.get("feature_count")
              and candidate.get("source_null_geometry_count") == 0
              and candidate.get("source_invalid_geometry_count") == 0)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "unique_source_ids": candidate.get("source_unique_id_count"),
        "source_null_geometry_count": candidate.get("source_null_geometry_count"),
        "source_invalid_geometry_count": candidate.get("source_invalid_geometry_count"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })

proposed_critical_habitat_draft_job = define_asset_job("proposed_critical_habitat_draft_refresh",
    selection=AssetSelection.assets(proposed_critical_habitat_features))
proposed_critical_habitat_draft_schedule = ScheduleDefinition(
    name="proposed_critical_habitat_draft_refresh_schedule", job=proposed_critical_habitat_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
