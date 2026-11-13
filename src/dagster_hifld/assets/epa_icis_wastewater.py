"""Dagster entrypoint for the EPA ICIS wastewater candidate."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset,
                     asset_check, define_asset_job)
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_icis_wastewater import (
    DATASET_SLUG, FILE_SLUG, MAX_CAPTURE_BYTES, MAX_RESPONSE_BYTES, capture_epa_icis_wastewater,
)


class EpaIcisWastewaterConfig(Config):
    capture_directory: str = ""
    retained_capture_directory: str = ""
    retained_capture_manifest_sha256: str = ""
    max_capture_bytes: int = MAX_CAPTURE_BYTES
    max_response_bytes: int = MAX_RESPONSE_BYTES


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="epa", compute_kind="http",
       description="Complete native EPA_GEO ICIS wastewater point layer capture. Candidate only; release remains gated.")
def epa_icis_wastewater_treatment_plants(context, config: EpaIcisWastewaterConfig,
                                         staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_icis_wastewater(staging_storage, version=build_version_id(context),
                                         capture_directory=config.capture_directory or None,
                                         retained_capture_directory=config.retained_capture_directory or None,
                                         retained_capture_manifest_sha256=config.retained_capture_manifest_sha256 or None,
                                         max_capture_bytes=config.max_capture_bytes,
                                         max_response_bytes=config.max_response_bytes)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_icis_wastewater_treatment_plants, blocking=True)
def epa_icis_wastewater_capture_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = isinstance(count, int) and count > 0 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(passed=passed, metadata={"complete_population_captured":passed,
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})


epa_icis_wastewater_draft_job = define_asset_job("epa_icis_wastewater_draft_refresh",
    selection=AssetSelection.assets(epa_icis_wastewater_treatment_plants))
epa_icis_wastewater_draft_schedule = ScheduleDefinition(
    name="epa_icis_wastewater_draft_refresh_schedule", job=epa_icis_wastewater_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {epa_icis_wastewater_treatment_plants.op.name: {
        "config": {"max_capture_bytes": MAX_CAPTURE_BYTES}
    }}})
