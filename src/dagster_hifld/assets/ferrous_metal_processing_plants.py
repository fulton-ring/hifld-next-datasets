"""Dormant parked USGS National Atlas definition; public capture refuses I/O."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferrous_metal_processing_plants import (
    COUNT,
    capture_ferrous_metal_processing_plants,
)

DATASET_SLUG = FILE_SLUG = "ferrous-metal-processing-plants"


class FerrousMetalProcessingPlantsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "Parked December 2005 USGS National Atlas Ferrous Metal Processing Plants "
        "definition. Internet Archive acquisition and retained replay are excluded; "
        "the public capture refuses before I/O. Private offline fixture validation "
        "does not establish a maintained source or grant any release approval."
    ),
)
def ferrous_metal_processing_plants_usgs(context, config: FerrousMetalProcessingPlantsConfig,
                                         staging_storage: StagingStorageResource) -> dict:
    result = capture_ferrous_metal_processing_plants(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=ferrous_metal_processing_plants_usgs, blocking=True)
def ferrous_metal_processing_plants_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("feature_count") == COUNT)
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "id_sha256": candidate.get("source_id_sha256", "missing"),
        "record_sha256": candidate.get("source_record_sha256", "missing"),
    })


ferrous_metal_processing_plants_draft_job = define_asset_job(
    "ferrous_metal_processing_plants_draft_refresh",
    selection=AssetSelection.assets(ferrous_metal_processing_plants_usgs),
)
ferrous_metal_processing_plants_draft_schedule = ScheduleDefinition(
    name="ferrous_metal_processing_plants_draft_refresh_schedule",
    job=ferrous_metal_processing_plants_draft_job,
    cron_schedule="0 11 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
