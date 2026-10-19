"""Stopped/manual draft assets for the frozen official BTS IPCD snapshot."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.bts_ipcd import DATASET_SLUG, FILE_SLUG, capture_bts_ipcd

class BtsIpcdConfig(Config):
    capture_directory: str = ""
    reuse_capture_directory: str = ""

@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="bts", compute_kind="http",
      description="Official USDOT_BTS IPCD final snapshot. BTS states it is no longer updated and latest information is from 2022; adoption and continuity remain release gates.")
def bts_intermodal_passenger_connectivity_database_ipcd(context, config: BtsIpcdConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_bts_ipcd(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.reuse_capture_directory or config.capture_directory or None,
        reuse_retained_capture=bool(config.reuse_capture_directory),
    )
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=bts_intermodal_passenger_connectivity_database_ipcd, blocking=True)
def bts_intermodal_passenger_connectivity_database_ipcd_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 15488 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(passed=passed, metadata={"complete_source_population": candidate.get("feature_count", 0), "source_is_discontinued": True, "source_latest_information_year": 2022, "source_release_accepted": False, "continuity_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False})

bts_ipcd_frozen_snapshot_job = define_asset_job("bts_ipcd_frozen_snapshot_manual_capture", selection=AssetSelection.assets(bts_intermodal_passenger_connectivity_database_ipcd))
bts_ipcd_frozen_snapshot_schedule = ScheduleDefinition(name="bts_ipcd_frozen_snapshot_schedule", job=bts_ipcd_frozen_snapshot_job, cron_schedule="0 8 1 * *", execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
