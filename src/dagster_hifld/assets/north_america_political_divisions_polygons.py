"""Draft ingest asset for the maintained CEC North American Atlas Political Boundaries."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.north_america_political_divisions_polygons import (
    DATASET_SLUG, EXPECTED_BYTES, EXPECTED_FEATURE_COUNT, EXPECTED_SHA256, FILE_SLUG,
    candidate_version, capture_north_america_political_divisions_polygons,
)


class NorthAmericaPoliticalDivisionsPolygonsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="cec",
    compute_kind="http",
    description=(
        "Maintained CEC North American Environmental Atlas 'Political Boundaries, 2021' (edition 3, "
        "boundaries_p_2021_v3): 98 first-level jurisdiction polygons integrating the countries' 2021 "
        "national boundary publications. Candidate only: the CEC producer relationship, schema/ID "
        "contract, baseline compatibility, rights and release acceptance remain unresolved, and the "
        "DHS crosswalk REST URL is dead (verified 2026-11-13)."
    ),
)
def cec_north_america_political_divisions_polygons(context, config: NorthAmericaPoliticalDivisionsPolygonsConfig,
                                                  staging_storage: StagingStorageResource) -> dict:
    version = candidate_version()
    result = capture_north_america_political_divisions_polygons(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=cec_north_america_political_divisions_polygons, blocking=True)
def cec_north_america_political_divisions_polygons_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_FEATURE_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("source_archive_bytes") == EXPECTED_BYTES
        and candidate.get("source_archive_sha256") == EXPECTED_SHA256
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_record_sha256": candidate.get("source_record_sha256", "missing"),
        "source_geometry_sha256": candidate.get("source_geometry_sha256", "missing"),
        "source_archive_sha256": candidate.get("source_archive_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


cec_north_america_political_divisions_polygons_draft_job = define_asset_job(
    "cec_north_america_political_divisions_polygons_draft_refresh",
    selection=AssetSelection.assets(cec_north_america_political_divisions_polygons),
)
cec_north_america_political_divisions_polygons_draft_schedule = ScheduleDefinition(
    name="cec_north_america_political_divisions_polygons_draft_refresh_schedule",
    job=cec_north_america_political_divisions_polygons_draft_job,
    cron_schedule="0 8 1 3 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [cec_north_america_political_divisions_polygons]
ASSET_CHECKS = [cec_north_america_political_divisions_polygons_complete]
JOBS = [cec_north_america_political_divisions_polygons_draft_job]
SCHEDULES = [cec_north_america_political_divisions_polygons_draft_schedule]
