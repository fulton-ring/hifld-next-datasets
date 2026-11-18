"""Stopped refresh asset for the latest published Census TIGER primary roads."""
from dagster import (
    AssetCheckResult, AssetKey, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check,
)

from dagster_hifld.assets.staging_refresh import define_staging_capture_job
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.census_primary_roads import (
    DATASET_SLUG, FILE_SLUG, capture_census_primary_roads,
    capture_latest_census_primary_roads,
)


class CensusPrimaryRoadsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Latest published Census TIGER/Line national Primary Roads archive. "
        "Candidate only: the DHS crosswalk row and baseline compatibility are unresolved, "
        "and source, freshness, schema/ID contract, coverage, rights, and release remain pending."
    ),
)
def census_bureau_primary_roads(context, config: CensusPrimaryRoadsConfig,
                                staging_storage: StagingStorageResource) -> dict:
    if config.capture_directory:
        result = capture_census_primary_roads(
            staging_storage, capture_directory=config.capture_directory,
        )
    else:
        result = capture_latest_census_primary_roads(staging_storage)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=census_bureau_primary_roads, blocking=True)
def census_primary_roads_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and type(candidate.get("feature_count")) is int and candidate["feature_count"] > 0
        and candidate.get("source_archive_bytes", 0) > 0
        and isinstance(candidate.get("source_archive_sha256"), str)
        and len(candidate.get("source_archive_sha256", "")) == 64
        and bool(candidate.get("source_edition"))
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_archive_sha256": candidate.get("source_archive_sha256", "missing"),
        "source_edition": candidate.get("source_edition", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


census_primary_roads_draft_job = define_staging_capture_job(
    "census_primary_roads_draft_refresh", census_bureau_primary_roads,
)
census_primary_roads_draft_schedule = ScheduleDefinition(
    name="census_primary_roads_draft_refresh_schedule",
    job=census_primary_roads_draft_job,
    cron_schedule="0 8 1 9 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
