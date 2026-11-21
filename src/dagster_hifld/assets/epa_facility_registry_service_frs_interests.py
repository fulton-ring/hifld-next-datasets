"""EPA Facility Registry Service (FRS) Interests inventory entry draft assets.

Reconstructs the ``epa-facility-registry-service-frs-interests`` HIFLD Open
inventory entry (issue #390), which is a duplicate catalog row of the same
EPA Facility Registry Service full interest universe published by the EPA_GEO
hosted FRS_INTERESTS FeatureServer. The capture machinery, native source contract and
approval gates are shared with the ``frs-interests`` entry; only the staged
dataset identity differs. Candidate only; no publication is authorized.
"""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_interests import (
    EPA_ITEM_DATASET_SLUG, capture_epa_frs_interests,
)

DATASET_SLUG = EPA_ITEM_DATASET_SLUG
FILE_SLUG = DATASET_SLUG


class EpaFacilityRegistryServiceFrsInterestsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Exact EPA_GEO FRS INTERESTS full-universe point layer staged for the "
        "epa-facility-registry-service-frs-interests inventory entry (issue #390), "
        "which duplicates the frs-interests entry for the same source product. "
        "Candidate only; source release date and currentness remain unresolved."
    ),
)
def epa_facility_registry_service_frs_interests(
    context, config: EpaFacilityRegistryServiceFrsInterestsConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    version = build_version_id(context)
    result = capture_epa_frs_interests(
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


@asset_check(asset=epa_facility_registry_service_frs_interests, blocking=True)
def epa_facility_registry_service_frs_interests_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get('source_reported_count')
    passed = (type(count) is int and count > 0 and candidate.get('source_population_complete') is True
              and candidate.get('feature_count') == count and candidate.get('source_null_geometry_count') == 0
              and candidate.get('source_invalid_geometry_count') == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": count if count is not None else 'missing',
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_facility_registry_service_frs_interests_approved_baseline_drift = register_approved_baseline_drift(
    epa_facility_registry_service_frs_interests, 'epa_facility_registry_service_frs_interests_approved_baseline_drift')


epa_facility_registry_service_frs_interests_draft_job = define_asset_job(
    "epa_facility_registry_service_frs_interests_draft_refresh",
    selection=AssetSelection.assets(epa_facility_registry_service_frs_interests),
)
epa_facility_registry_service_frs_interests_draft_schedule = ScheduleDefinition(
    name="epa_facility_registry_service_frs_interests_draft_refresh_schedule",
    job=epa_facility_registry_service_frs_interests_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [epa_facility_registry_service_frs_interests]
ASSET_CHECKS = [epa_facility_registry_service_frs_interests_complete,
                epa_facility_registry_service_frs_interests_approved_baseline_drift]
JOBS = [epa_facility_registry_service_frs_interests_draft_job]
SCHEDULES = [epa_facility_registry_service_frs_interests_draft_schedule]
