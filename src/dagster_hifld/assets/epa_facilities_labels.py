"""EPA Facilities (Labels) draft ingest asset; monthly refresh remains stopped."""

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
from dagster_hifld.sources.epa_facilities_labels import SLUG, stage_epa_facilities_labels


class EpaFacilitiesLabelsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Complete EPA Facilities (Labels) layer 0 draft; original attributes, IDs, "
        "representation-rule codes, and native NAD83 point coordinates are retained."
    ),
)
def epa_facilities_labels(
    context,
    config: EpaFacilitiesLabelsConfig,
    staging_storage: StagingStorageResource,
):
    result = stage_epa_facilities_labels(
        staging_storage,
        capture_dir=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(SLUG, SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_facilities_labels, blocking=True)
def epa_facilities_labels_full_quality(epa_facilities_labels: dict):
    return AssetCheckResult(
        passed=(
            epa_facilities_labels["source_null_geometry_count"] == 0
            and epa_facilities_labels["source_invalid_geometry_count"] == 0
        ),
        metadata={
            "full_population": epa_facilities_labels["feature_count"],
            "null_geometry_count": epa_facilities_labels["source_null_geometry_count"],
            "invalid_geometry_count": epa_facilities_labels["source_invalid_geometry_count"],
            "approval_scope": (
                "Full geometry quality only; historical compatibility, source edition, "
                "rights, and release remain unaccepted."
            ),
        },
    )


epa_facilities_labels_draft_job = define_asset_job(
    "epa_facilities_labels_draft_refresh",
    selection=AssetSelection.assets(epa_facilities_labels),
)
epa_facilities_labels_draft_schedule = ScheduleDefinition(
    name="epa_facilities_labels_draft_refresh_schedule",
    job=epa_facilities_labels_draft_job,
    cron_schedule="0 9 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
