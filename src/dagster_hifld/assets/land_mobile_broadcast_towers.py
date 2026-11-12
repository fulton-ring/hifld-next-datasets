"""Explicit stopped-refresh ingest asset for FCC ULS Land Mobile Broadcast towers."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.land_mobile_broadcast_towers import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_land_mobile_broadcast_towers,
)


class LandMobileBroadcastTowersConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="communications",
       compute_kind="http",
       description="Draft fixed-location tower features from the FCC's maintained weekly ULS "
                   "Land Mobile - Broadcast Auxiliary license-records file (l_LMbcast.zip). "
                   "Unapproved candidate; historical equivalence, schema/ID contract, rights "
                   "and release acceptance remain unresolved.")
def land_mobile_broadcast_towers_ingest(context, config: LandMobileBroadcastTowersConfig,
                                        staging_storage: StagingStorageResource) -> dict:
    result = capture_land_mobile_broadcast_towers(staging_storage, version=build_version_id(context),
                                                  capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
                                            [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=land_mobile_broadcast_towers_ingest, blocking=True)
def land_mobile_broadcast_towers_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count", 0) > 0
              and candidate.get("source_population_complete") is True
              and candidate.get("null_geometry_count", 0)
              == candidate.get("invalid_coordinate_record_count", 0)
              + candidate.get("missing_coordinate_record_count", 0))
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "null_geometry_count": candidate.get("null_geometry_count", 0),
        "invalid_coordinate_record_count": candidate.get("invalid_coordinate_record_count", 0),
        "missing_coordinate_record_count": candidate.get("missing_coordinate_record_count", 0),
        "service_code_counts": candidate.get("service_code_counts", {}),
        "record_sha256": candidate.get("record_sha256", "missing"),
        "schema_sha256": candidate.get("schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
    })


land_mobile_broadcast_towers_draft_job = define_asset_job(
    "land_mobile_broadcast_towers_draft_refresh",
    selection=AssetSelection.assets(land_mobile_broadcast_towers_ingest))
land_mobile_broadcast_towers_draft_schedule = ScheduleDefinition(
    name="land_mobile_broadcast_towers_draft_refresh_schedule",
    job=land_mobile_broadcast_towers_draft_job,
    cron_schedule="0 12 * * 0", default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [land_mobile_broadcast_towers_ingest]
ASSET_CHECKS = [land_mobile_broadcast_towers_complete]
JOBS = [land_mobile_broadcast_towers_draft_job]
SCHEDULES = [land_mobile_broadcast_towers_draft_schedule]
