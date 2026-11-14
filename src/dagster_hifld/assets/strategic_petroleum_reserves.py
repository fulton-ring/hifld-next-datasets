"""Explicit stopped-refresh asset for the pinned historical EIA SPR layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.strategic_petroleum_reserves import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_strategic_petroleum_reserves,
)


class StrategicPetroleumReservesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="energy",
    compute_kind="http",
    description=(
        "Pinned January 2021 EIA GIS snapshot (4 Gulf Coast Strategic Petroleum Reserve "
        "storage site points from EIA's own map_data host; underlying site data credited "
        "to DOE, Office of Fossil Energy). The crosswalk-directed Energy Atlas "
        "FeatureServer is token-gated anonymously. Historical unapproved candidate; "
        "currentness and HIFLD equivalence remain unresolved."
    ),
)
def strategic_petroleum_reserves(context, config: StrategicPetroleumReservesConfig,
                                 staging_storage: StagingStorageResource) -> dict:
    result = capture_strategic_petroleum_reserves(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=strategic_petroleum_reserves, blocking=True)
def strategic_petroleum_reserves_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256",
                                                        "missing"),
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "release_accepted": False,
    })


strategic_petroleum_reserves_draft_job = define_asset_job(
    "strategic_petroleum_reserves_draft_refresh",
    selection=AssetSelection.assets(strategic_petroleum_reserves),
)
strategic_petroleum_reserves_draft_schedule = ScheduleDefinition(
    name="strategic_petroleum_reserves_draft_refresh_schedule",
    job=strategic_petroleum_reserves_draft_job,
    cron_schedule="0 8 1 * *", default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [strategic_petroleum_reserves]
ASSET_CHECKS = [strategic_petroleum_reserves_complete]
JOBS = [strategic_petroleum_reserves_draft_job]
SCHEDULES = [strategic_petroleum_reserves_draft_schedule]
