"""Stopped draft-refresh asset for the USGS GNIS Landforms layer."""

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
from dagster_hifld.sources.landforms_gnis import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_landforms_gnis,
)


class LandformsGnisConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS The National Map Gazetteer (GNIS) geonames MapServer layer 5 "
        "'Landforms' (243,708 source features, complete resumable capture "
        "2026-11-13) as the maintained producer publication named by the DHS "
        "HIFLD Open crosswalk for the inventory entry nmg/landforms-1. "
        "Unapproved faithful candidate; baselines pinned from the dated complete "
        "capture. Succession, freshness, schema/ID contract, rights and release "
        "decisions remain pending."
    ),
)
def landforms_gnis(
    context,
    config: LandformsGnisConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_landforms_gnis(
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


@asset_check(asset=landforms_gnis, blocking=True)
def landforms_gnis_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and (EXPECTED_NULL_GEOMETRY_COUNT is None
             or candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT)
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


landforms_gnis_draft_job = define_asset_job(
    "landforms_gnis_draft_refresh",
    selection=AssetSelection.assets(landforms_gnis),
)
landforms_gnis_draft_schedule = ScheduleDefinition(
    name="landforms_gnis_draft_refresh_schedule",
    job=landforms_gnis_draft_job,
    cron_schedule="0 9 15 2,4,6,8,10,12 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [landforms_gnis]
ASSET_CHECKS = [landforms_gnis_complete]
JOBS = [landforms_gnis_draft_job]
SCHEDULES = [landforms_gnis_draft_schedule]
