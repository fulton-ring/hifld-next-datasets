"""BTS Intermodal Freight Facilities Rail TOFC/COFC native draft assets."""

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
from dagster_hifld.sources.bts_rail_tofc_cofc import (
    DATASET_SLUG, EXPECTED_SOURCE_COUNT, FILE_SLUG, capture_bts_rail_tofc_cofc,
)


class BtsRailTofcCofcConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bts",
    compute_kind="http",
    description=(
        "Exact BTS-maintained NTAD Rail TOFC/COFC point layer. The current schema, "
        "ID, source vintage, and publication approvals remain pending."
    ),
)
def bts_intermodal_freight_rail_tofc_cofc(
    context, config: BtsRailTofcCofcConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_bts_rail_tofc_cofc(
        staging_storage,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=bts_intermodal_freight_rail_tofc_cofc, blocking=True)
def bts_intermodal_freight_rail_tofc_cofc_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("source_population_complete") is True
        and candidate.get("feature_count") == EXPECTED_SOURCE_COUNT
        and candidate.get("source_null_geometry_count") == 0
        and candidate.get("source_invalid_geometry_count") == 0
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


bts_rail_tofc_cofc_draft_job = define_asset_job(
    "bts_rail_tofc_cofc_draft_refresh",
    selection=AssetSelection.assets(bts_intermodal_freight_rail_tofc_cofc),
)
bts_rail_tofc_cofc_draft_schedule = ScheduleDefinition(
    name="bts_rail_tofc_cofc_draft_refresh_schedule",
    job=bts_rail_tofc_cofc_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
