"""Stopped draft asset for the Power Plants aggregate from the current EIA-860M inventory."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ornl_power_plants import DATASET_SLUG, FILE_SLUG, capture_power_plants

class PowerPlantsConfig(Config):
    capture_directory: str = ""
    replay: bool = False

@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="energy", compute_kind="http",
       description="Operating EIA-860M generators aggregated by native Plant ID with total capacity and capacity-dominant technology. Nonspatial draft; datum, equivalence and release acceptance remain unresolved.")
def eia_power_plants(context, config: PowerPlantsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_power_plants(staging_storage, capture_directory=config.capture_directory or None, replay=config.replay)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result

@asset_check(asset=eia_power_plants, blocking=True)
def eia_power_plants_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("source_population_complete") is True and candidate.get("plant_count", 0) > 0 and candidate.get("generator_count", 0) > 0
    return AssetCheckResult(passed=passed, metadata={
        "plant_count": candidate.get("plant_count", 0),
        "generator_count": candidate.get("generator_count", 0),
        "source_period": candidate.get("source_period", "unknown"),
        "source_population_complete": candidate.get("source_population_complete") is True,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
    })

eia_power_plants_draft_job = define_asset_job("eia_power_plants_draft_refresh", selection=AssetSelection.assets(eia_power_plants))
eia_power_plants_draft_schedule = ScheduleDefinition(name="eia_power_plants_draft_refresh_schedule", job=eia_power_plants_draft_job,
    cron_schedule="0 8 1 * *", default_status=DefaultScheduleStatus.STOPPED)
