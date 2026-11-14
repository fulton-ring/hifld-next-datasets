"""Stopped refresh asset for the official Census 2020 P.L. 94-171 VTD publications."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.census_voting_districts import (
    COUNT, DATASET_SLUG, FILE_SLUG, GEOIDS_SHA256, PINS, candidate_version,
    stage_voting_districts,
)


class CensusVotingDistrictsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Captures every official Census 2020 Redistricting Data (P.L. 94-171) Voting District "
        "state shapefile from the producer's own TIGER2020PL directory and stages one combined "
        "native GeoParquet candidate. Candidate only: source release, baseline compatibility, "
        "schema/ID contract, coverage, rights and release acceptance remain pending maintainers."
    ),
)
def census_bureau_voting_districts(context, config: CensusVotingDistrictsConfig,
                                   staging_storage: StagingStorageResource) -> dict:
    version = candidate_version()
    result = stage_voting_districts(
        staging_storage, capture_dir=config.capture_directory or None,
    )
    if result["version"] != version:
        raise ValueError("candidate_version_identity_drift")
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])
    if not context.instance.has_dynamic_partition(PUBLISH_PARTITION_NAME, partition_key):
        context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    context.add_output_metadata(result)
    return result


@asset_check(asset=census_bureau_voting_districts, blocking=True)
def census_voting_districts_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("source_archive_count") == len(PINS)
        and candidate.get("source_id_sha256") == GEOIDS_SHA256
        and bool(candidate.get("native_parquet_sha256"))
        and candidate.get("native_value_parity") is True
        and candidate.get("approved") is False
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_archive_count": candidate.get("source_archive_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "native_parquet_sha256": candidate.get("native_parquet_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


census_voting_districts_draft_job = define_asset_job(
    "census_voting_districts_draft_refresh",
    selection=AssetSelection.assets(census_bureau_voting_districts),
)
census_voting_districts_draft_schedule = ScheduleDefinition(
    name="census_voting_districts_draft_refresh_schedule",
    job=census_voting_districts_draft_job,
    cron_schedule="0 8 1 9 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [census_bureau_voting_districts]
ASSET_CHECKS = [census_voting_districts_source_complete]
JOBS = [census_voting_districts_draft_job]
SCHEDULES = [census_voting_districts_draft_schedule]
