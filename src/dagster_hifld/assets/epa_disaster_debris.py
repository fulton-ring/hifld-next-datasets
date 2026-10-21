"""EPA Disaster Debris Recovery Tool draft assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_disaster_debris import (
    DATASET_SLUG, FILE_SLUG, capture_epa_disaster_debris, replay_epa_disaster_debris,
)


class EpaDisasterDebrisConfig(Config):
    capture_directory: str = ""
    replay_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Native EPA Disaster Debris Recovery Tool nationwide point layer. Draft candidate only; "
        "source release is documented as August 2026, while quality, rights, and release acceptance remain pending."
    ),
)
def epa_disaster_debris_recovery_data(context, config: EpaDisasterDebrisConfig, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    if config.replay_capture_directory:
        result = replay_epa_disaster_debris(
            staging_storage, version=version,
            capture_directory=config.replay_capture_directory,
        )
    else:
        result = capture_epa_disaster_debris(
            staging_storage, version=version,
            capture_directory=config.capture_directory or None,
        )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_disaster_debris_recovery_data, blocking=True)
def epa_disaster_debris_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count", 0) > 0
        and candidate.get("source_id_count") == candidate.get("feature_count")
        and candidate.get("source_null_geometry_count") == 0
        and candidate.get("source_invalid_coordinate_count") == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "null_geometry_count": candidate.get("source_null_geometry_count"),
            "invalid_coordinate_count": candidate.get("source_invalid_coordinate_count"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_disaster_debris_draft_job = define_asset_job(
    "epa_disaster_debris_draft_refresh",
    selection=AssetSelection.assets(epa_disaster_debris_recovery_data),
)
epa_disaster_debris_draft_schedule = ScheduleDefinition(
    name="epa_disaster_debris_draft_refresh_schedule",
    job=epa_disaster_debris_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
