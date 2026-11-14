"""Stopped draft-refresh asset for the USGS 3DHP Waterbody areal hydrography successor."""

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nhd_area_large_scale import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_nhd_area_large_scale,
)


class NhdAreaLargeScaleConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS 3DHP_all FeatureServer layer 60 Waterbody rows with featuretype<>3 "
        "(River, Canal, Ocean or Great Lake; 95,423 roster ids live-verified) as the "
        "maintained successor of the retired NHD high-resolution NHDArea layer "
        "(HIFLD inventory nhd/area-large-scale-2). Complete non-Lake areal "
        "hydrography with producer-refused records' attributes staged and their "
        "geometry explicitly gated to null (pinned refused set); unapproved "
        "faithful candidate. Succession coverage, freshness, "
        "schema/ID contract, rights and release decisions remain pending."
    ),
)
def nhd_area_large_scale(
    context,
    config: NhdAreaLargeScaleConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_nhd_area_large_scale(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=nhd_area_large_scale, blocking=True)
def nhd_area_large_scale_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "succession_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


nhd_area_large_scale_draft_job = define_asset_job(
    "nhd_area_large_scale_draft_refresh",
    selection=AssetSelection.assets(nhd_area_large_scale),
)
nhd_area_large_scale_draft_schedule = ScheduleDefinition(
    name="nhd_area_large_scale_draft_refresh_schedule",
    job=nhd_area_large_scale_draft_job,
    cron_schedule="0 9 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [nhd_area_large_scale]
ASSET_CHECKS = [nhd_area_large_scale_complete]
JOBS = [nhd_area_large_scale_draft_job]
SCHEDULES = [nhd_area_large_scale_draft_schedule]
