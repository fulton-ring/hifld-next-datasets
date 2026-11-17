"""Parked processing-plant asset: maintained EIA location source unavailable."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.eia_natural_gas_processing_plants import DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_natural_gas_processing_plants

class NaturalGasProcessingPlantsConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="energy", compute_kind="http",
       description="Parked: ingest refuses acquisition and cache replay before storage effects. EIA-757 Schedule A is suspended; the public report ends in 2017 and the Energy Atlas service requires a token.")
def eia_natural_gas_processing_plants_historical(context, config: NaturalGasProcessingPlantsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_natural_gas_processing_plants(staging_storage, version=build_version_id(context), capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=eia_natural_gas_processing_plants_historical, blocking=True)
def eia_natural_gas_processing_plants_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
    })

eia_natural_gas_processing_plants_draft_job = define_asset_job("eia_natural_gas_processing_plants_draft_refresh", selection=AssetSelection.assets(eia_natural_gas_processing_plants_historical), description="Parked pending a maintained EIA processing-plant location source; manual execution also refuses acquisition and cache replay.")
eia_natural_gas_processing_plants_draft_schedule = ScheduleDefinition(name="eia_natural_gas_processing_plants_draft_refresh_schedule", job=eia_natural_gas_processing_plants_draft_job,
    cron_schedule="0 8 1 * *", default_status=DefaultScheduleStatus.STOPPED)
