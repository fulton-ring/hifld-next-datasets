"""USACE IENC administrative-area native-source draft assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_administrative_areas import (
    DATASET_SLUG, FILE_SLUG, capture_usace_administrative_areas,
)
from dagster_hifld.sources.usace_navigation_lights import (
    DATASET_SLUG as LIGHTS_DATASET_SLUG, FILE_SLUG as LIGHTS_FILE_SLUG,
    EXPECTED_COUNT as LIGHTS_EXPECTED_COUNT, capture_usace_navigation_lights,
)


class UsaceAdministrativeAreasConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE IENC ADMINISTRATIVE_AREA MapServer layer (2,966 rows observed). "
        "Faithful candidate only; baseline and source/schema/ID/coverage acceptance remain pending."
    ),
)
def usace_administrative_areas_usace_ienc(context, config: UsaceAdministrativeAreasConfig,
                                           staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_administrative_areas(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_administrative_areas_usace_ienc, blocking=True)
def usace_administrative_areas_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 2966 and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_id_min": candidate.get("source_id_min", "missing"),
        "source_id_max": candidate.get("source_id_max", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "drift_guard": candidate.get("drift_guard", "missing"),
        "baseline_comparison_accepted": False,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


usace_administrative_areas_draft_job = define_asset_job(
    "usace_administrative_areas_draft_refresh",
    selection=AssetSelection.assets(usace_administrative_areas_usace_ienc),
)
usace_administrative_areas_draft_schedule = ScheduleDefinition(
    name="usace_administrative_areas_draft_refresh_schedule",
    job=usace_administrative_areas_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)


class UsaceNavigationLightsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", LIGHTS_DATASET_SLUG, LIGHTS_FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE IENC LIGHTS_POINT MapServer layer (8,892 records observed). "
        "Unapproved faithful candidate; source release and independent acceptance gates remain pending."
    ),
)
def usace_navigation_lights_usace_ienc(context, config: UsaceNavigationLightsConfig,
                                        staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_navigation_lights(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(LIGHTS_DATASET_SLUG, LIGHTS_FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_navigation_lights_usace_ienc, blocking=True)
def usace_navigation_lights_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == LIGHTS_EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_id_min": candidate.get("source_id_min", "missing"),
        "source_id_max": candidate.get("source_id_max", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "drift_guard": candidate.get("drift_guard", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


usace_navigation_lights_draft_job = define_asset_job(
    "usace_navigation_lights_draft_refresh",
    selection=AssetSelection.assets(usace_navigation_lights_usace_ienc),
)
usace_navigation_lights_draft_schedule = ScheduleDefinition(
    name="usace_navigation_lights_draft_refresh_schedule",
    job=usace_navigation_lights_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
