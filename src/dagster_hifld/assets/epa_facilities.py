"""Exact EPA Facilities layer 1 native draft; scheduled refresh stays stopped."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_facilities import EPA_FACILITIES_PROFILE, stage_epa_facilities

SLUG = EPA_FACILITIES_PROFILE.ordinary.slug


class EpaFacilitiesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]), group_name="epa", compute_kind="http",
    description="Complete producer EPA Facilities layer 1 draft; original fields, IDs and NAD83 Shape retained.",
)
def epa_facilities(context, config: EpaFacilitiesConfig, staging_storage: StagingStorageResource):
    result = stage_epa_facilities(
        staging_storage, profile=EPA_FACILITIES_PROFILE.ordinary,
        capture_dir=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(SLUG, SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_facilities, blocking=True)
def epa_facilities_full_quality(epa_facilities: dict):
    passed = epa_facilities["source_null_geometry_count"] == 0 and epa_facilities["source_invalid_geometry_count"] == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "full_population": epa_facilities["feature_count"],
            "null_geometry_count": epa_facilities["source_null_geometry_count"],
            "invalid_geometry_count": epa_facilities["source_invalid_geometry_count"],
            "source_release_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
        },
    )


epa_facilities_draft_job = define_asset_job(
    "epa_facilities_draft_refresh", selection=AssetSelection.assets(epa_facilities),
)
epa_facilities_draft_schedule = ScheduleDefinition(
    name="epa_facilities_draft_refresh_schedule", job=epa_facilities_draft_job,
    cron_schedule="0 9 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
