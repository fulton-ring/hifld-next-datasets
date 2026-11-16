"""Explicit stopped-refresh Dagster asset for NOAA OCS ENCDirect shipping fairways."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.shipping_fairways import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_shipping_fairways,
)


class ShippingFairwaysConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description=(
        "NOAA OCS ENCDirect MarineTransportation layer 0 'Shipping Lanes and Regulations' (346 features observed), "
        "the original producer publication of the HIFLD 'Shipping Fairways' item. The inventory row's USACE agency "
        "column contradicts the item's NOAA OCS FGDC metadata; no IENC feature class describes these coastal zones. "
        "Unapproved candidate; historical continuity, baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def shipping_fairways(context, config: ShippingFairwaysConfig,
                      staging_storage: StagingStorageResource) -> dict:
    result = capture_shipping_fairways(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=shipping_fairways, blocking=True)
def shipping_fairways_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


shipping_fairways_draft_job = define_asset_job(
    "shipping_fairways_draft_refresh",
    selection=AssetSelection.assets(shipping_fairways),
)
shipping_fairways_draft_schedule = ScheduleDefinition(
    name="shipping_fairways_draft_refresh_schedule",
    job=shipping_fairways_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [shipping_fairways]
ASSET_CHECKS = [shipping_fairways_complete]
JOBS = [shipping_fairways_draft_job]
SCHEDULES = [shipping_fairways_draft_schedule]
