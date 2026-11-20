"""Explicit stopped-refresh Dagster asset for the Census TIGERweb secondary roads layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.secondary_roads_census import (
    DATASET_SLUG, FILE_SLUG, capture_secondary_roads_census,
)


class SecondaryRoadsCensusConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=(
        "Census TIGERweb Transportation MapServer layer 3 Secondary Roads Interstates and US Highways "
        "(S1200 secondary-class segments with US Highway U or Interstate I route types observed). The DHS "
        "crosswalk points the retired HIFLD Open item at this maintained producer layer; historical "
        "equivalence, baseline, contract, coverage, rights and release remain unaccepted."
    ),
)
def secondary_roads_census(context, config: SecondaryRoadsCensusConfig,
                           staging_storage: StagingStorageResource) -> dict:
    result = capture_secondary_roads_census(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=secondary_roads_census, blocking=True)
def secondary_roads_census_complete(candidate: dict) -> AssetCheckResult:
    passed = (type(candidate.get("source_reported_count")) is int
              and candidate["source_reported_count"] > 0
              and candidate.get("feature_count") == candidate["source_reported_count"]
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_reported_count": candidate.get("source_reported_count", "missing"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


secondary_roads_census_draft_job = define_asset_job(
    "secondary_roads_census_draft_refresh",
    selection=AssetSelection.assets(secondary_roads_census),
)
secondary_roads_census_draft_schedule = ScheduleDefinition(
    name="secondary_roads_census_draft_refresh_schedule",
    job=secondary_roads_census_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

secondary_roads_census_drift = register_approved_baseline_drift(
    secondary_roads_census, "secondary_roads_census_drift")

INGEST_ASSETS = [secondary_roads_census]
ASSET_CHECKS = [secondary_roads_census_complete, secondary_roads_census_drift]
JOBS = [secondary_roads_census_draft_job]
SCHEDULES = [secondary_roads_census_draft_schedule]
