"""STOPPED, local-only refresh proposal for Unified School Districts."""
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    MetadataValue,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.unified_school_districts import DATASET_SLUG, FILE_SLUG, capture

_DESCRIPTION = (
    "Draft extraction of the full 2024 U.S. Census TIGER/Line "
    "School_District_Unified layer (10,897 features, native EPSG:4269). "
    "Historical HIFLD coverage equivalence, rights, and release approval remain unresolved."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def census_unified_school_districts(context, staging_storage: StagingStorageResource) -> dict:
    if isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local or staging_storage.bucket:
        raise ValueError("unified_school_districts_draft_requires_isolated_local_staging")
    version = build_version_id(context)
    if staging_storage.list_keys(DATASET_SLUG, FILE_SLUG, version):
        raise ValueError("source_capture_version_already_exists")
    result = capture(staging_storage, version)
    context.add_output_metadata({
        "feature_count": MetadataValue.int(result["feature_count"]),
        "source_ids_sha256": MetadataValue.text(result["ids_sha256"]),
        "source_records_sha256": MetadataValue.text(result["records_sha256"]),
        "source_archive_sha256": MetadataValue.text(result["source_archive_sha256"]),
        "native_parquet_sha256": MetadataValue.text(result["parquet_sha256"]),
        "release_accepted": MetadataValue.bool(False),
    })
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    return {**result, "version": version, "partition_key": partition_key}


unified_school_districts_draft_refresh_job = define_asset_job(
    "unified_school_districts_draft_refresh",
    selection=AssetSelection.assets(census_unified_school_districts),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=unified_school_districts_draft_refresh_job,
    cron_schedule="0 7 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def unified_school_districts_draft_refresh_schedule(context):
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("Unified School Districts draft refresh only runs against isolated local staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(
        run_key=f"unified-school-districts-draft-{day}",
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
    )
