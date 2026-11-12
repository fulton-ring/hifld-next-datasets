"""Native IENC distance-mark capture; refresh is proposal-only and stopped by default."""

from dagster import (
    AssetKey, AssetSelection, DefaultScheduleStatus, Field, MetadataValue, RunRequest, SkipReason,
    asset, define_asset_job, schedule,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.usace_distance_marks import (
    DATASET_SLUG, FILE_SLUG, capture,
)

_DESCRIPTION = (
    "Distance Marks (USACE IENC), captured from official IENC DISTANCE_MARK_POINT "
    "MapServer layer 10. Coverage is limited to the IENC chart service; no "
    "nationwide coverage or accepted HIFLD continuity is asserted."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=_DESCRIPTION,
    config_schema={
        "retained_source_version": Field(
            str, is_required=False, default_value="",
            description="Optional retained source version for explicit evidence-only replay; omitted runs fetch a fresh snapshot.",
        ),
    },
)
def usace_distance_marks_usace_ienc(context, staging_storage: StagingStorageResource) -> dict:
    if isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local or staging_storage.bucket:
        raise ValueError("distance_marks_draft_requires_isolated_local_staging")
    version = build_version_id(context)
    if staging_storage.list_keys(DATASET_SLUG, FILE_SLUG, version):
        raise ValueError("source_capture_version_already_exists")
    retained_version = context.op_config.get("retained_source_version") or None
    result = capture(staging_storage, version, retained_source_version=retained_version)
    context.add_output_metadata({
        "feature_count": MetadataValue.int(result["feature_count"]),
        "source_ids_sha256": MetadataValue.text(result["ids_sha256"]),
        "source_records_sha256": MetadataValue.text(result["records_sha256"]),
        "native_parquet_sha256": MetadataValue.text(result["parquet_sha256"]),
        "release_accepted": MetadataValue.bool(False),
    })
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    return {**result, "version": version, "partition_key": partition_key}


distance_marks_draft_refresh_job = define_asset_job(
    "distance_marks_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(usace_distance_marks_usace_ienc),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=distance_marks_draft_refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def distance_marks_draft_refresh_schedule(context):
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("Distance Marks draft refresh only runs against isolated local staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(run_key=f"distance-marks-ienc-draft-{day}", tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})
