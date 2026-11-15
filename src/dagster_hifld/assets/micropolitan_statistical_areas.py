"""Stopped refresh asset for the official Census TIGER/Line 2026 Micropolitan Statistical Areas."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.micropolitan_statistical_areas import (
    DATASET_SLUG, EXPECTED_BYTES, EXPECTED_FEATURE_COUNT, EXPECTED_SHA256, FILE_SLUG,
    candidate_version, capture_census_micropolitan_statistical_areas,
)


class MicropolitanStatisticalAreasConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Official Census TIGER/Line 2026 national CBSA archive (the producer's "
        "maintained tl_2026_us_cbsa.zip, released 2026-10-14), with the "
        "producer-defined micropolitan subset MEMI=2 selected and the complete "
        "archive retained as provenance. "
        "Candidate only: the DHS crosswalk row and "
        "baseline compatibility are unresolved, and source, freshness, schema/ID contract, "
        "coverage, rights and release remain pending."
    ),
)
def census_micropolitan_statistical_areas(context, config: MicropolitanStatisticalAreasConfig,
                                          staging_storage: StagingStorageResource) -> dict:
    version = candidate_version()
    result = capture_census_micropolitan_statistical_areas(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=census_micropolitan_statistical_areas, blocking=True)
def census_micropolitan_statistical_areas_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_FEATURE_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("source_archive_bytes") == EXPECTED_BYTES
        and candidate.get("source_archive_sha256") == EXPECTED_SHA256
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_archive_sha256": candidate.get("source_archive_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


census_micropolitan_statistical_areas_draft_job = define_asset_job(
    "census_micropolitan_statistical_areas_draft_refresh",
    selection=AssetSelection.assets(census_micropolitan_statistical_areas),
)
census_micropolitan_statistical_areas_draft_schedule = ScheduleDefinition(
    name="census_micropolitan_statistical_areas_draft_refresh_schedule",
    job=census_micropolitan_statistical_areas_draft_job,
    cron_schedule="0 7 2 3 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [census_micropolitan_statistical_areas]
ASSET_CHECKS = [census_micropolitan_statistical_areas_source_complete]
JOBS = [census_micropolitan_statistical_areas_draft_job]
SCHEDULES = [census_micropolitan_statistical_areas_draft_schedule]
