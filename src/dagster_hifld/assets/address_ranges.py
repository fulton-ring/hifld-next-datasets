"""STOPPED, local-only refresh proposal for Census TIGER Address Ranges."""
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
from dagster_hifld.sources.address_ranges import DATASET_SLUG, FILE_SLUG, capture

_DESCRIPTION = (
    "Draft lossless capture of the complete current U.S. Census TIGER national "
    "Address Ranges attribute-only FileGDB layer (TGRGDB26 edition, 37,730,671 "
    "native rows). DHS crosswalk row, historical HIFLD equivalence, schema/ID "
    "contract, rights and release acceptance remain unresolved."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def census_tiger_address_ranges(context, staging_storage: StagingStorageResource) -> dict:
    if isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local or staging_storage.bucket:
        raise ValueError("address_ranges_draft_requires_isolated_local_staging")
    version = build_version_id(context)
    if staging_storage.list_keys(DATASET_SLUG, FILE_SLUG, version):
        raise ValueError("source_capture_version_already_exists")
    result = capture(staging_storage, version)
    context.add_output_metadata({
        "feature_count": MetadataValue.int(result["feature_count"]),
        "source_archive_sha256": MetadataValue.text(result["archive_sha256"]),
        "source_archive_bytes": MetadataValue.int(result["archive_bytes"]),
        "native_record_digest": MetadataValue.text(result["record_digest"]),
        "release_accepted": MetadataValue.bool(False),
    })
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    return {**result, "version": version, "partition_key": partition_key}


address_ranges_draft_refresh_job = define_asset_job(
    "address_ranges_draft_refresh",
    selection=AssetSelection.assets(census_tiger_address_ranges),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=address_ranges_draft_refresh_job,
    cron_schedule="0 7 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def address_ranges_draft_refresh_schedule(context):
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("Address Ranges draft refresh only runs against isolated local staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(
        run_key=f"address-ranges-draft-{day}",
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
    )


INGEST_ASSETS = [census_tiger_address_ranges]
JOBS = [address_ranges_draft_refresh_job]
SCHEDULES = [address_ranges_draft_refresh_schedule]
