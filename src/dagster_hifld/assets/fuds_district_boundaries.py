"""Explicit, stopped draft refresh for USACE FUDS district boundaries."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_district_boundaries import (
    DATASET_SLUG, FILE_SLUG, capture_fuds_district_boundaries,
)


class FudsDistrictBoundariesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Exact USACE Military/FUDS_Data MapServer layer 9: FUDS Program District "
        "Boundaries. Native source schema and IDs are retained in an unapproved candidate."
    ),
)
def usace_fuds_program_district_boundaries(
    context, config: FudsDistrictBoundariesConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_fuds_district_boundaries(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_fuds_program_district_boundaries, blocking=True)
def usace_fuds_program_district_boundaries_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 13 and candidate.get("candidate_approved") is False
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


fuds_district_boundaries_draft_job = define_asset_job(
    "fuds_district_boundaries_draft_refresh",
    selection=AssetSelection.assets(usace_fuds_program_district_boundaries),
)
fuds_district_boundaries_draft_schedule = ScheduleDefinition(
    name="fuds_district_boundaries_draft_refresh_schedule",
    job=fuds_district_boundaries_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
