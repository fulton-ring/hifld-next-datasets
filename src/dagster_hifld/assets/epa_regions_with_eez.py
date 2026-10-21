"""Explicit EPA Region Boundaries with EEZ draft asset for inventory #76."""

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
from dagster_hifld.sources.epa_eez import WITH_EEZ_IDENTITY, stage_epa_eez


class EpaRegionsWithEezConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", WITH_EEZ_IDENTITY.slug, WITH_EEZ_IDENTITY.slug]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Complete exact EPA Region Boundaries with Exclusive Economic Zone draft "
        "for inventory #76; separately namespaced from the EEZ-only #75 dataset."
    ),
)
def epa_regions_with_eez(
    context, config: EpaRegionsWithEezConfig, staging_storage: StagingStorageResource
):
    result = stage_epa_eez(
        staging_storage,
        capture_dir=config.capture_directory or None,
        dataset=WITH_EEZ_IDENTITY,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [
            build_publish_partition_key(
                WITH_EEZ_IDENTITY.slug, WITH_EEZ_IDENTITY.slug, result["version"]
            )
        ],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_regions_with_eez, blocking=True)
def epa_regions_with_eez_full_quality(epa_regions_with_eez: dict):
    passed = (
        epa_regions_with_eez["source_null_geometry_count"] == 0
        and epa_regions_with_eez["source_invalid_geometry_count"] == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "full_population": epa_regions_with_eez["feature_count"],
            "null_geometry_count": epa_regions_with_eez["source_null_geometry_count"],
            "invalid_geometry_count": epa_regions_with_eez[
                "source_invalid_geometry_count"
            ],
            "approval_scope": (
                "Full geometry quality only; source identity, coverage, edition, schema, "
                "baseline, rights, and release remain unaccepted."
            ),
        },
    )


epa_regions_with_eez_draft_job = define_asset_job(
    "epa_regions_with_eez_draft_refresh",
    selection=AssetSelection.assets(epa_regions_with_eez),
)
epa_regions_with_eez_draft_schedule = ScheduleDefinition(
    name="epa_regions_with_eez_draft_refresh_schedule",
    job=epa_regions_with_eez_draft_job,
    cron_schedule="0 9 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
