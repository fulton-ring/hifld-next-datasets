"""Draft Dagster assets for the USGS NHD "Area - Small Scale" capture."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nhd_area_small_scale import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_NAN_GEOMETRY_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    EXPECTED_UNASSEMBLED_COUNT,
    FILE_SLUG,
    capture_nhd_area_small_scale,
)


class NhdAreaSmallScaleConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS National Hydrography Dataset service layer 7 'Area - Small Scale' (medium-resolution "
        "1:100,000-scale NHD area polygons) from the producer's own REST publication. Candidate "
        "extraction only; the NHD is retired (2023-10-01) and republished as a reference service, so "
        "source acceptance, freshness, rights, and schema/ID acceptance remain pending."
    ),
)
def usgs_nhd_area_small_scale(context, config: NhdAreaSmallScaleConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_nhd_area_small_scale(
        staging_storage,
        version="v20261112",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_nhd_area_small_scale, blocking=True)
def usgs_nhd_area_small_scale_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
              and candidate.get("source_nan_string_geometry_count") == EXPECTED_NAN_GEOMETRY_COUNT
              and candidate.get("source_unassembled_polygon_count") == EXPECTED_UNASSEMBLED_COUNT)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_null_geometry_count": candidate.get("source_null_geometry_count", 0),
            "source_nan_string_geometry_count": candidate.get("source_nan_string_geometry_count", 0),
            "source_unassembled_polygon_count": candidate.get("source_unassembled_polygon_count", 0),
            "technical_geometry_quality_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


nhd_area_small_scale_draft_job = define_asset_job(
    "nhd_area_small_scale_draft_refresh",
    selection=AssetSelection.assets(usgs_nhd_area_small_scale),
)
nhd_area_small_scale_draft_schedule = ScheduleDefinition(
    name="nhd_area_small_scale_draft_refresh_schedule",
    job=nhd_area_small_scale_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [usgs_nhd_area_small_scale]
ASSET_CHECKS = [usgs_nhd_area_small_scale_complete]
JOBS = [nhd_area_small_scale_draft_job]
SCHEDULES = [nhd_area_small_scale_draft_schedule]
