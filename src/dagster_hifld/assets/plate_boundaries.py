"""Draft-only USGS Plate Boundaries source asset with explicit quality gates."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.plate_boundaries import capture_plate_boundaries


class PlateBoundariesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", "plate-boundaries", "plate-boundaries"]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "Complete native EPSG:3857 capture from the official USGS Plate Boundaries service, staged in the standard versioned namespace. "
        "HIFLD archive equality, source edition, schema, rights, and release decisions remain pending."
    ),
)
def plate_boundaries_usgs(context, config: PlateBoundariesConfig,
                          staging_storage: StagingStorageResource) -> dict:
    result = capture_plate_boundaries(staging_storage, version=build_version_id(context),
                                      capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=plate_boundaries_usgs, blocking=True)
def plate_boundaries_population_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("source_population_complete") is True and candidate.get("feature_count") == 1175
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
    })


plate_boundaries_draft_job = define_asset_job(
    "plate_boundaries_draft_refresh",
    selection=AssetSelection.assets(plate_boundaries_usgs),
)
plate_boundaries_draft_schedule = ScheduleDefinition(
    name="plate_boundaries_draft_refresh_schedule",
    job=plate_boundaries_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
