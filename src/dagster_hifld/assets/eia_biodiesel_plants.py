"""Explicit stopped-refresh asset for the pinned historical EIA biodiesel plant layer."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.eia_biodiesel_plants import DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, SOURCE_RELEASE, capture_eia_biodiesel_plants


class BiodieselPlantsConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="energy", compute_kind="http",
       description="Pinned historical EIA 'Biodiesel Plants in the United States' shapefile (75 plants as of "
                   "January 1, 2021) from the producer's own eia.gov distribution. Historical unapproved candidate; "
                   "inventory equivalence, currentness and rights remain unresolved.")
def eia_biodiesel_plants(context, config: BiodieselPlantsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_eia_biodiesel_plants(staging_storage, version=f"v{SOURCE_RELEASE.replace('-', '')}",
                                          capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=eia_biodiesel_plants, blocking=True)
def eia_biodiesel_plants_complete(candidate: dict) -> AssetCheckResult:
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


eia_biodiesel_plants_draft_job = define_asset_job("eia_biodiesel_plants_draft_refresh",
                                                  selection=AssetSelection.assets(eia_biodiesel_plants))
eia_biodiesel_plants_draft_schedule = ScheduleDefinition(name="eia_biodiesel_plants_draft_refresh_schedule",
                                                         job=eia_biodiesel_plants_draft_job,
                                                         cron_schedule="0 8 1 * *",
                                                         default_status=DefaultScheduleStatus.STOPPED)
