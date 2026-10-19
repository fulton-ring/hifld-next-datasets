"""Draft Dagster asset for the linked USACE IENC isolated-danger buoy layer."""
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
from dagster_hifld.sources.usace_ienc_isolated_danger import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_usace_ienc_isolated_danger,
)


class UsaceIencIsolatedDangerConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Exact ISOLATED_DANGER_BUOY_POINT layer linked by the official USDOT/BTS NTAD IENC item. "
        "Candidate capture only; source release date, rights, schema/ID contract and v1.0.0 migration remain unapproved."
    ),
)
def usace_ienc_isolated_danger_buoy_point(
    context, config: UsaceIencIsolatedDangerConfig, staging_storage: StagingStorageResource
) -> dict:
    version = build_version_id(context)
    result = capture_usace_ienc_isolated_danger(
        staging_storage,
        version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_ienc_isolated_danger_buoy_point, blocking=True)
def usace_ienc_isolated_danger_buoy_point_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == 1
        and candidate.get("source_null_geometry_count") == 0
        and bool(candidate.get("source_feature_id_sha256"))
    )
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


usace_ienc_isolated_danger_draft_job = define_asset_job(
    "usace_ienc_isolated_danger_draft_refresh",
    selection=AssetSelection.assets(usace_ienc_isolated_danger_buoy_point),
)
usace_ienc_isolated_danger_draft_schedule = ScheduleDefinition(
    name="usace_ienc_isolated_danger_draft_refresh_schedule",
    job=usace_ienc_isolated_danger_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
