"""Explicit stopped-refresh asset for the pinned historical USGS NSHM uniform-hazard layer."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usgs_uniform_hazard_ground_motion import (
    DATASET_SLUG, EXPECTED_COUNT, EXPECTED_INVALID_GEOMETRY_COUNT, FILE_SLUG,
    capture_usgs_uniform_hazard_ground_motion,
)


class UsgsUniformHazardGroundMotionConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usgs", compute_kind="http",
       description="Pinned 2023 USGS 50-State NSHM uniform-hazard contour polygons (SA1P0 5% in 50 years, site class B/C, 4014 records) from the ScienceBase Overview data release. Acquisition stopped: the producer's Revised 2023 NSHM release (CONUS 2023.R2/AK R1/HI R2, 2026-08-10) explicitly supersedes this data release and publishes no successor polygon product, so the maintained-source admission is no-maintained-source (park or retire); materialization refuses with source_admission_unresolved_no_maintained_successor_park_or_retire until a maintainer decision. Release decisions remain unresolved.")
def usgs_uniform_hazard_ground_motion_historical(context, config: UsgsUniformHazardGroundMotionConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_usgs_uniform_hazard_ground_motion(staging_storage, version=build_version_id(context), capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_uniform_hazard_ground_motion_historical, blocking=True)
def usgs_uniform_hazard_ground_motion_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_geometry_sha256": candidate.get("source_geometry_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "native_invalid_geometry_count_preserved": EXPECTED_INVALID_GEOMETRY_COUNT,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
    })


usgs_uniform_hazard_ground_motion_draft_job = define_asset_job("usgs_uniform_hazard_ground_motion_draft_refresh", selection=AssetSelection.assets(usgs_uniform_hazard_ground_motion_historical))
usgs_uniform_hazard_ground_motion_draft_schedule = ScheduleDefinition(name="usgs_uniform_hazard_ground_motion_draft_refresh_schedule", job=usgs_uniform_hazard_ground_motion_draft_job,
    cron_schedule="0 8 1 * *", default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [usgs_uniform_hazard_ground_motion_historical]
ASSET_CHECKS = [usgs_uniform_hazard_ground_motion_complete]
JOBS = [usgs_uniform_hazard_ground_motion_draft_job]
SCHEDULES = [usgs_uniform_hazard_ground_motion_draft_schedule]
