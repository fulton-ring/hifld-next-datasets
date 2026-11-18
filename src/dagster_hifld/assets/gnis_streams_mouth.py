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

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gnis_streams_mouth import (
    DATASET_SLUG,
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
    passed = (candidate.get("capture_complete") is True
              and type(candidate.get("feature_count")) is int
              and candidate["feature_count"] > 0
              and candidate.get("candidate_ready") is not None)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


usgs_gnis_streams_mouth_approved_baseline_drift = register_approved_baseline_drift(
    usgs_gnis_streams_mouth, "usgs_gnis_streams_mouth_approved_baseline_drift")


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
ASSET_CHECKS = [usgs_gnis_streams_mouth_complete, usgs_gnis_streams_mouth_approved_baseline_drift]
JOBS = [gnis_streams_mouth_draft_job]
SCHEDULES = [gnis_streams_mouth_draft_schedule]
