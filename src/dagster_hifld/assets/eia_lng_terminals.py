"""EIA frozen-vintage LNG import/export terminal candidate assets."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.eia_lng_terminals import DATASET_SLUG, FILE_SLUG, capture_eia_lng_terminals

@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="eia", compute_kind="http",
    description="Official EIA January 2020 frozen LNG import/export terminal snapshot. Candidate only; currentness, stable facility identity, mapping, rights compatibility, and adoption remain gated.",
)
def eia_lng_import_export_terminals(context, staging_storage: StagingStorageResource) -> dict:
    result = capture_eia_lng_terminals(staging_storage, version=build_version_id(context))
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result

@asset_check(asset=eia_lng_import_export_terminals, blocking=True)
def eia_lng_import_export_terminals_release_gate(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        metadata={
            "complete_frozen_source_population": candidate.get("feature_count"),
            "source_vintage": candidate.get("source_vintage"),
            "currentness_accepted": False,
            "successor_source_verified": False,
            "source_id_contract_accepted": False,
            "baseline_mapping_approved": False,
            "rights_compatibility_accepted": False,
            "historical_candidate_adopted": False,
            "release_accepted": False,
        },
        description="Release remains blocked: the exact EIA source is a January 2020 snapshot; no stable public facility ID or accepted mapping to the published 9-row baseline is established.",
    )

eia_lng_draft_job = define_asset_job("eia_lng_draft_refresh", selection=AssetSelection.assets(eia_lng_import_export_terminals))
eia_lng_draft_schedule = ScheduleDefinition(name="eia_lng_draft_refresh_schedule", job=eia_lng_draft_job, cron_schedule="0 9 1 * *", execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
