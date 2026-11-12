"""BTS Intermodal Freight Facilities Marine Roll-on/Roll-off native draft assets."""

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
from dagster_hifld.sources.bts_marine_roro import (
    DATASET_SLUG, EXPECTED_SOURCE_COUNT, FILE_SLUG, capture_bts_marine_roro,
)


class BtsAirTruckConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bts",
    compute_kind="http",
    description=(
        "Exact BTS-maintained NTAD Marine Roll-on/Roll-off point layer. The current schema, "
        "ID, source vintage, and publication approvals remain pending."
    ),
)
def bts_intermodal_freight_marine_roll_on_roll_off(
    context, config: BtsAirTruckConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_bts_marine_roro(
        staging_storage,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=bts_intermodal_freight_marine_roll_on_roll_off, blocking=True)
def bts_intermodal_freight_marine_roll_on_roll_off_complete(candidate: dict) -> AssetCheckResult:
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


bts_marine_roro_draft_job = define_asset_job(
    "bts_marine_roro_draft_refresh",
    selection=AssetSelection.assets(bts_intermodal_freight_marine_roll_on_roll_off),
)
bts_marine_roro_draft_schedule = ScheduleDefinition(
    name="bts_marine_roro_draft_refresh_schedule",
    job=bts_marine_roro_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
