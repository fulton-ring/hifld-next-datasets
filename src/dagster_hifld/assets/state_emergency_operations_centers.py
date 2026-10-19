"""Explicit FEMA State EOC source asset; draft schedule remains stopped."""
import os

from dagster import AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.sources.state_emergency_operations_centers import SLUG, stage_state_eoc


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]),
    group_name="fema",
    compute_kind="http",
    description="Exact FEMA-hosted State Emergency Operations Centers layer 0. Full source fields, IDs, nullability and WGS84 points are retained in an unapproved native draft.",
)
def fema_state_emergency_operations_centers(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    result = stage_state_eoc(staging_storage, version=version, capture_dir=os.environ.get("HIFLD_EOC_CAPTURE_DIR"))
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(SLUG, SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(
    asset=["ingest", SLUG, SLUG],
    name="source_quality",
    description="Blocking check for complete source IDs, declared field values and native point geometry.",
    blocking=True,
)
def source_quality_check_result(source_result: dict) -> AssetCheckResult:
    errors = source_result.get("quality_errors", [])
    passed = source_result.get("quality_passed") is True and not errors
    return AssetCheckResult(passed=passed, metadata={
        "version": source_result.get("version", "unknown"),
        "row_count": source_result.get("rows", 0),
        "quality_errors": errors,
    })


@asset_check(
    asset=fema_state_emergency_operations_centers,
    name="source_quality",
    description="Blocking check for complete source IDs, declared field values and native point geometry.",
    blocking=True,
)
def fema_state_eoc_source_quality(fema_state_emergency_operations_centers: dict) -> AssetCheckResult:
    return source_quality_check_result(fema_state_emergency_operations_centers)


fema_state_eoc_draft_job = define_asset_job(
    "fema_state_eoc_draft_refresh", selection=AssetSelection.assets(fema_state_emergency_operations_centers)
)
fema_state_eoc_draft_schedule = ScheduleDefinition(
    name="fema_state_eoc_draft_monthly",
    job=fema_state_eoc_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
