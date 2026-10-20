"""Draft-only Dagster source asset for USACE IENC building locations."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.building_locations_usace_ienc import capture_building_locations


class BuildingLocationsConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", "building-locations-usace-ienc", "building-locations-usace-ienc"]),
       group_name="usace", compute_kind="http",
       description="Complete native-ID capture from the official USACE IENC master service; unapproved candidate only.")
def building_locations_usace_ienc(context, config: BuildingLocationsConfig,
                                  staging_storage: StagingStorageResource) -> dict:
    result = capture_building_locations(staging_storage, version=build_version_id(context),
                                        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"],result["file_slug"],result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=building_locations_usace_ienc, blocking=True)
def building_locations_population_complete(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(passed=(candidate.get("feature_count") == 892
                                    and candidate.get("source_population_complete") is True), metadata={
        "feature_count": candidate.get("feature_count", 0),
        "native_id_sha256": candidate.get("native_id_sha256", "missing"),
        "capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
    })


building_locations_draft_job = define_asset_job("building_locations_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(building_locations_usace_ienc))
building_locations_draft_schedule = ScheduleDefinition(
    name="building_locations_usace_ienc_draft_refresh_schedule", job=building_locations_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED)
