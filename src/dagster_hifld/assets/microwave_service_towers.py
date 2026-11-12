"""Explicit stopped-refresh Dagster asset for FCC ULS microwave service towers."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.microwave_service_towers import (
    DATASET_SLUG, EXPECTED_ACTIVE_LOCATIONS, FILE_SLUG, capture_microwave_service_towers,
)


class MicrowaveServiceTowersConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fcc",
    compute_kind="http",
    description=(
        "FCC ULS weekly public access complete Microwave service-group licenses file l_micro.zip: location "
        "records of active licenses under the historical HIFLD Microwave Service Towers column contract "
        "(422808 active locations observed). ULS filed-site population, not a census of physical towers. "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def microwave_service_towers(context, config: MicrowaveServiceTowersConfig,
                             staging_storage: StagingStorageResource) -> dict:
    result = capture_microwave_service_towers(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=microwave_service_towers, blocking=True)
def microwave_service_towers_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_ACTIVE_LOCATIONS
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


microwave_service_towers_draft_job = define_asset_job(
    "microwave_service_towers_draft_refresh",
    selection=AssetSelection.assets(microwave_service_towers),
)
microwave_service_towers_draft_schedule = ScheduleDefinition(
    name="microwave_service_towers_draft_refresh_schedule",
    job=microwave_service_towers_draft_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [microwave_service_towers]
ASSET_CHECKS = [microwave_service_towers_complete]
JOBS = [microwave_service_towers_draft_job]
SCHEDULES = [microwave_service_towers_draft_schedule]
