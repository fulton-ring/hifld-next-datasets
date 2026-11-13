"""USGS GNIS Gazetteer "Streams (Mouth)" native draft assets."""
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

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gnis_streams_mouth import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_COUNTY_NAME_NULL_COUNT,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    EXPECTED_OBJECTID_1_NULL_COUNT,
    EXPECTED_STATE_ALPHA_NULL_COUNT,
    FILE_SLUG,
    capture_gnis_streams_mouth,
)


class GnisStreamsMouthConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS GNIS Gazetteer 'Streams (Mouth)' layer (hydrographic Stream/Canal mouth "
        "points) from the producer's own The National Map geonames MapServer named by "
        "the DHS HIFLD Open crosswalk. Candidate extraction only; release "
        "date/freshness, schema/ID, rights, and release approvals remain pending."
    ),
)
def usgs_gnis_streams_mouth(context, config: GnisStreamsMouthConfig,
                            staging_storage: StagingStorageResource) -> dict:
    result = capture_gnis_streams_mouth(
        staging_storage,
        version="v20261113",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_gnis_streams_mouth, blocking=True)
def usgs_gnis_streams_mouth_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
              and candidate.get("source_invalid_geometry_count") == EXPECTED_INVALID_GEOMETRY_COUNT)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "state_alpha_null_count": EXPECTED_STATE_ALPHA_NULL_COUNT,
            "county_name_null_count": EXPECTED_COUNTY_NAME_NULL_COUNT,
            "gaz_id_null_count": EXPECTED_OBJECTID_1_NULL_COUNT,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


gnis_streams_mouth_draft_job = define_asset_job(
    "gnis_streams_mouth_draft_refresh",
    selection=AssetSelection.assets(usgs_gnis_streams_mouth),
)
gnis_streams_mouth_draft_schedule = ScheduleDefinition(
    name="gnis_streams_mouth_draft_refresh_schedule",
    job=gnis_streams_mouth_draft_job,
    cron_schedule="0 8 6 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [usgs_gnis_streams_mouth]
ASSET_CHECKS = [usgs_gnis_streams_mouth_complete]
JOBS = [gnis_streams_mouth_draft_job]
SCHEDULES = [gnis_streams_mouth_draft_schedule]
