"""EPA Facility Registry Service (FRS) Power Plants inventory entry draft assets.

Reconstructs the ``epa-facility-registry-service-frs-power-plants-2`` HIFLD Open
inventory entry (issue #392), which is a duplicate catalog row of the same
EPA FRS/EIA-860 power generation product published by the EPA_GEO hosted
FRS_PowerPlants FeatureServer. The capture machinery, source pins and approval
gates are shared with the ``epa-frs-eia-860-power-generation-facilities-by-source``
entry (issue #69); only the staged dataset identity differs. Candidate only; no
publication is authorized.
"""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_power_generation import (
    EPA_ITEM_DATASET_SLUG, EXPECTED_COUNT, capture_epa_power_generation,
)

DATASET_SLUG = EPA_ITEM_DATASET_SLUG
FILE_SLUG = DATASET_SLUG


class EpaFacilityRegistryServiceFrsPowerPlantsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Exact EPA_GEO FRS/EIA-860 power generation point layer staged for the "
        "epa-facility-registry-service-frs-power-plants-2 inventory entry (issue #392), "
        "which duplicates the epa-frs-eia-860-power-generation-facilities-by-source entry "
        "for the same source product. Candidate only: the service describes EIA input as "
        "of June 2014, so freshness and release remain blocked."
    ),
)
def epa_facility_registry_service_frs_power_plants(
    context, config: EpaFacilityRegistryServiceFrsPowerPlantsConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    version = build_version_id(context)
    result = capture_epa_power_generation(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
        dataset_slug=DATASET_SLUG,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_facility_registry_service_frs_power_plants, blocking=True)
def epa_facility_registry_service_frs_power_plants_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_facility_registry_service_frs_power_plants_draft_job = define_asset_job(
    "epa_facility_registry_service_frs_power_plants_draft_refresh",
    selection=AssetSelection.assets(epa_facility_registry_service_frs_power_plants),
)
epa_facility_registry_service_frs_power_plants_draft_schedule = ScheduleDefinition(
    name="epa_facility_registry_service_frs_power_plants_draft_refresh_schedule",
    job=epa_facility_registry_service_frs_power_plants_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [epa_facility_registry_service_frs_power_plants]
ASSET_CHECKS = [epa_facility_registry_service_frs_power_plants_complete]
JOBS = [epa_facility_registry_service_frs_power_plants_draft_job]
SCHEDULES = [epa_facility_registry_service_frs_power_plants_draft_schedule]
