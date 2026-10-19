"""Stopped, approval-gated draft capture for USACE FUDS MRS polygons."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_mrs import DATASET_SLUG, FILE_SLUG, capture_fuds_mrs


class FudsMrsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Complete native capture of USACE Military/FUDS_Data layer 3, FUDS Munitions "
        "Response Site. Source fields, OBJECTIDs, polygon Z/M coordinates, and nulls "
        "are retained in an unapproved candidate."
    ),
)
def usace_fuds_munitions_response_sites(
    context, config: FudsMrsConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_fuds_mrs(
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


@asset_check(asset=usace_fuds_munitions_response_sites, blocking=True)
def usace_fuds_munitions_response_sites_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 1682 and candidate.get("candidate_approved") is False
    return AssetCheckResult(passed=passed, metadata={
        "complete_population_captured": passed,
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


fuds_mrs_draft_job = define_asset_job(
    "fuds_mrs_draft_refresh",
    selection=AssetSelection.assets(usace_fuds_munitions_response_sites),
)
fuds_mrs_draft_schedule = ScheduleDefinition(
    name="fuds_mrs_draft_refresh_schedule",
    job=fuds_mrs_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
