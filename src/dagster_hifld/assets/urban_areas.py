"""Stopped refresh asset for the official Census TIGER/Line 2026 Urban Areas."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.urban_areas import (
    DATASET_SLUG, EXPECTED_BYTES, EXPECTED_FEATURE_COUNT, EXPECTED_SHA256, FILE_SLUG,
    candidate_version, capture_census_urban_areas,
)


class UrbanAreasConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Official Census TIGER/Line 2026 national Urban Areas archive (the producer's "
        "maintained UAC20 national file, released 2026-10-14, incorporating the "
        "producer's corrections to the 2020 base vintage). "
        "Candidate only: the DHS crosswalk row and "
        "baseline compatibility are unresolved, and source, freshness, schema/ID contract, "
        "coverage, rights and release remain pending."
    ),
)
def census_urban_areas(context, config: UrbanAreasConfig,
                       staging_storage: StagingStorageResource) -> dict:
    version = candidate_version()
    result = capture_census_urban_areas(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=census_urban_areas, blocking=True)
def census_urban_areas_source_complete(candidate: dict) -> AssetCheckResult:
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


census_urban_areas_draft_job = define_asset_job(
    "census_urban_areas_draft_refresh",
    selection=AssetSelection.assets(census_urban_areas),
)
census_urban_areas_draft_schedule = ScheduleDefinition(
    name="census_urban_areas_draft_refresh_schedule",
    job=census_urban_areas_draft_job,
    cron_schedule="0 7 2 3 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [census_urban_areas]
ASSET_CHECKS = [census_urban_areas_source_complete]
JOBS = [census_urban_areas_draft_job]
SCHEDULES = [census_urban_areas_draft_schedule]
