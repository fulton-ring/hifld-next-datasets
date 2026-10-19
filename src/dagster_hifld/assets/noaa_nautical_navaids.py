"""Explicit draft capture asset for NOAA's nautical ATON archive."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.noaa_nautical_navaids import (
    DATASET_SLUG, FILE_SLUG, capture_noaa_nautical_navaids,
)


class NoaaNauticalNavaidsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description=(
        "Captures NOAA MarineCadastre's complete native AtoN GeoPackage as a draft. "
        "Published schema and population differ; every promotion gate remains false."
    ),
)
def noaa_nautical_navaids_draft(
    context, config: NoaaNauticalNavaidsConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_noaa_nautical_navaids(
        staging_storage, capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=noaa_nautical_navaids_draft, blocking=True)
def noaa_nautical_navaids_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 51443 and candidate.get("published_baseline_compatible") is False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_official_source_population": candidate.get("feature_count", 0),
            "published_baseline_compatible": False,
            "source_release_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


noaa_nautical_navaids_draft_job = define_asset_job(
    "noaa_nautical_navaids_draft_refresh",
    selection=AssetSelection.assets(noaa_nautical_navaids_draft),
)
noaa_nautical_navaids_draft_schedule = ScheduleDefinition(
    name="noaa_nautical_navaids_draft_refresh_schedule",
    job=noaa_nautical_navaids_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
