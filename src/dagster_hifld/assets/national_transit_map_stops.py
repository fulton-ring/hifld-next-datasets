"""Stopped Dagster draft for the current BTS National Transit Map Stops layer."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset,
                     asset_check, define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_transit_map_stops import (
    DATASET_SLUG, FILE_SLUG, MAX_CAPTURE_BYTES, capture_national_transit_map_stops,
)


class NationalTransitMapStopsConfig(Config):
    capture_directory: str = ""
    retained_capture_directory: str = ""
    retained_capture_manifest_sha256: str = ""
    max_capture_bytes: int = MAX_CAPTURE_BYTES


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bureau_of_transportation_statistics",
    compute_kind="http",
    description=(
        "Draft native capture of current BTS NTAD National Transit Map Stops. "
        "Historical HIFLD equivalence, source freshness and release remain unaccepted."
    ),
)
def national_transit_map_stops(context, config: NationalTransitMapStopsConfig,
                               staging_storage: StagingStorageResource) -> dict:
    result = capture_national_transit_map_stops(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        retained_capture_directory=config.retained_capture_directory or None,
        retained_capture_manifest_sha256=config.retained_capture_manifest_sha256 or None,
        max_capture_bytes=config.max_capture_bytes,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=national_transit_map_stops, blocking=True)
def national_transit_map_stops_source_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count")
    passed = (type(count) is int and count == 673_251
              and candidate.get("source_null_geometry_count") == 0
              and candidate.get("source_invalid_geometry_count") == 0)
    return AssetCheckResult(passed=passed, metadata={
        "complete_observed_source_population": passed,
        "source_release_accepted": False,
        "freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "historical_equivalence_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


national_transit_map_stops_draft_job = define_asset_job(
    "national_transit_map_stops_draft_refresh",
    selection=AssetSelection.assets(national_transit_map_stops),
)
national_transit_map_stops_draft_schedule = ScheduleDefinition(
    name="national_transit_map_stops_draft_refresh_schedule",
    job=national_transit_map_stops_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {national_transit_map_stops.op.name: {
        "config": {"max_capture_bytes": MAX_CAPTURE_BYTES}
    }}},
)
