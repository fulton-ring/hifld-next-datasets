"""FAA AIS Runways native draft assets."""

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
from dagster_hifld.sources.runways import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_EMPTY_GEOMETRY_COUNT,
    EXPECTED_GLOBAL_ID_DUPLICATE_COUNT,
    EXPECTED_GLOBAL_ID_NULL_COUNT,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_runways,
)


class RunwaysConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=(
        "FAA Aeronautical Information Services (AIS) Runways layer from the producer's own "
        "ArcGIS publication (FAA Open Data Hub faa::runways; item "
        "4d8fa46181aa470d809776c57a8ab1f6, refreshed with each FAA 8-week aeronautical data "
        "cycle). Candidate extraction only; source release date/freshness, schema/ID, rights, "
        "and release approvals remain pending."
    ),
)
def faa_runways(context, config: RunwaysConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_runways(
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


@asset_check(asset=faa_runways, blocking=True)
def faa_runways_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
              and candidate.get("source_empty_geometry_count") == EXPECTED_EMPTY_GEOMETRY_COUNT
              and candidate.get("source_invalid_geometry_count") == EXPECTED_INVALID_GEOMETRY_COUNT)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "global_id_null_count": EXPECTED_GLOBAL_ID_NULL_COUNT,
            "global_id_duplicate_count": EXPECTED_GLOBAL_ID_DUPLICATE_COUNT,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


faa_runways_draft_job = define_asset_job(
    "faa_runways_draft_refresh",
    selection=AssetSelection.assets(faa_runways),
)
faa_runways_draft_schedule = ScheduleDefinition(
    name="faa_runways_draft_refresh_schedule",
    job=faa_runways_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [faa_runways]
ASSET_CHECKS = [faa_runways_complete]
JOBS = [faa_runways_draft_job]
SCHEDULES = [faa_runways_draft_schedule]
