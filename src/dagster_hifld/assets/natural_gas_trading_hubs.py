"""Draft Dagster asset for the dated EIA natural gas hubs product."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.natural_gas_trading_hubs import (
    DATASET_SLUG, FILE_SLUG, capture_natural_gas_trading_hubs,
)


class NaturalGasTradingHubsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="eia",
    compute_kind="http",
    description=(
        "Historical EIA February 2020 natural gas trading hubs candidate. The EIA ZIP is dated "
        "2020-07-10; relationship to the DOE/ORNL HIFLD archive and ongoing source maintenance "
        "remain unresolved, so the schedule is stopped and release gates remain closed."
    ),
)
def natural_gas_trading_hubs_ingest(context, config: NaturalGasTradingHubsConfig,
                                    staging_storage: StagingStorageResource) -> dict:
    result = capture_natural_gas_trading_hubs(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=natural_gas_trading_hubs_ingest, blocking=True)
def natural_gas_trading_hubs_historical_candidate_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == 32
              and candidate.get("source_null_geometry_count") == 0
              and candidate.get("release_accepted") is False)
    return AssetCheckResult(passed=passed, metadata={
        "historical_source_features": candidate.get("feature_count", 0),
        "source_release_accepted": False,
        "hifld_product_content_match_confirmed": candidate.get("hifld_product_content_match_confirmed", False),
        "hifld_distribution_authority_confirmed": False,
        "freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


natural_gas_trading_hubs_draft_job = define_asset_job(
    "natural_gas_trading_hubs_draft_refresh",
    selection=AssetSelection.assets(natural_gas_trading_hubs_ingest),
)
natural_gas_trading_hubs_draft_schedule = ScheduleDefinition(
    name="natural_gas_trading_hubs_draft_refresh_schedule",
    job=natural_gas_trading_hubs_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
