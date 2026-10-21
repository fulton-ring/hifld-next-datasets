"""Stopped draft asset for the official 2023 Census TIGER US county boundaries."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.us_county_boundaries import (
    DATASET_SLUG, EXPECTED_BYTES, EXPECTED_FEATURE_COUNT, EXPECTED_GEOID_SHA256,
    EXPECTED_SHA256, FILE_SLUG, candidate_version, capture_us_county_boundaries,
)


class UsCountyBoundariesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Official Census TIGER/Line 2023 US county boundaries archive. "
        "Candidate only: the raw/published schema and ID contract await maintainer approval, "
        "and source release/freshness, coverage, rights, and release remain pending."
    ),
)
def us_county_boundaries_census(context, config: UsCountyBoundariesConfig,
                                staging_storage: StagingStorageResource) -> dict:
    version = candidate_version()
    result = capture_us_county_boundaries(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=us_county_boundaries_census, blocking=True)
def us_county_boundaries_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_FEATURE_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("source_archive_bytes") == EXPECTED_BYTES
        and candidate.get("source_archive_sha256") == EXPECTED_SHA256
        and candidate.get("source_id_sha256") == EXPECTED_GEOID_SHA256
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


us_county_boundaries_draft_job = define_asset_job(
    "us_county_boundaries_draft_refresh",
    selection=AssetSelection.assets(us_county_boundaries_census),
)
us_county_boundaries_draft_schedule = ScheduleDefinition(
    name="us_county_boundaries_draft_refresh_schedule",
    job=us_county_boundaries_draft_job,
    cron_schedule="0 8 1 9 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
