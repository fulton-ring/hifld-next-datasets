"""Stopped draft-refresh asset for the USGS US Landslide Regions polygons."""

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

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.us_landslide_regions import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_us_landslide_regions,
)


class UsLandslideRegionsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS ArcGIS Online FeatureServer US_Landslide_poly_v2/FeatureServer layer 12 "
        "'US_Landslide_v3poly' (the polygon feature class of USGS data release "
        "'Landslide Inventories across the United States (ver. 3.0, February 2025)', "
        "DOI 10.5066/P14AJF8I) as the maintained producer publication of the HIFLD "
        "Open inventory entry us-landslide-regions. Resumable roster-segmented "
        "complete capture with capture-time count/ID witnesses; unapproved "
        "faithful candidate. Succession, freshness, schema/ID contract, rights "
        "and release decisions remain pending."
    ),
)
def us_landslide_regions(
    context,
    config: UsLandslideRegionsConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_us_landslide_regions(
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


@asset_check(asset=us_landslide_regions, blocking=True)
def us_landslide_regions_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and type(candidate.get("feature_count")) is int
        and type(candidate.get("source_reported_count")) is int
        and candidate["feature_count"] > 0
        and candidate["feature_count"] == candidate.get("source_reported_count")
        and bool(candidate.get("source_id_sha256"))
        and bool(candidate.get("source_schema_sha256"))
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": candidate.get("source_reported_count", 0),
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


us_landslide_regions_approved_baseline_drift = register_approved_baseline_drift(
    us_landslide_regions,
    "us_landslide_regions_approved_baseline_drift",
)


us_landslide_regions_draft_job = define_asset_job(
    "us_landslide_regions_draft_refresh",
    selection=AssetSelection.assets(us_landslide_regions),
)
us_landslide_regions_draft_schedule = ScheduleDefinition(
    name="us_landslide_regions_draft_refresh_schedule",
    job=us_landslide_regions_draft_job,
    cron_schedule="0 9 15 3,6,9,12 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [us_landslide_regions]
ASSET_CHECKS = [us_landslide_regions_complete,
                us_landslide_regions_approved_baseline_drift]
JOBS = [us_landslide_regions_draft_job]
SCHEDULES = [us_landslide_regions_draft_schedule]
