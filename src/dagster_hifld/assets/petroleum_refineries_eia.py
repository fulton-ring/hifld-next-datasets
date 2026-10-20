"""Historical-only stopped Dagster asset for EIA's 2021 refinery point layer."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.petroleum_refineries_eia import DATASET_SLUG, FILE_SLUG, capture_petroleum_refineries_eia


class PetroleumRefineriesEiaConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="eia", compute_kind="http",
      description="Pinned historical EIA refinery point layer as of Jan. 1, 2021 (130 points). This archive is not current; changed source bytes stop for release review.")
def petroleum_refineries_eia_ingest(context, config: PetroleumRefineriesEiaConfig,
                                    staging_storage: StagingStorageResource) -> dict:
    result = capture_petroleum_refineries_eia(staging_storage, capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=petroleum_refineries_eia_ingest, blocking=True)
def petroleum_refineries_eia_historical_candidate_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 130 and candidate.get("source_population_complete") is True and candidate.get("historical_only") is True
    return AssetCheckResult(passed=passed, metadata={
        "historical_source_features": candidate.get("feature_count", 0),
        "source_vintage": candidate.get("source_vintage", "unknown"),
        "source_population_complete": candidate.get("source_population_complete", False),
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


petroleum_refineries_eia_draft_job = define_asset_job(
    "petroleum_refineries_eia_historical_draft",
    selection=AssetSelection.assets(petroleum_refineries_eia_ingest),
)
petroleum_refineries_eia_draft_schedule = ScheduleDefinition(
    name="petroleum_refineries_eia_historical_schedule",
    job=petroleum_refineries_eia_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
