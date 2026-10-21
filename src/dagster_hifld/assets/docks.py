"""Draft assets for the current USACE/BTS Navigation Facilities (Dock) layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config,
    DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.docks import DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture


class DocksConfig(Config):
    replay_version: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="maritime",
    compute_kind="http",
    description=(
        "Current USACE-copyrighted Navigation Facilities (Dock) layer distributed through "
        "BTS/NTAD. Candidate scope is the exact observed ArcGIS layer population; historical "
        "HIFLD Docks equivalence and source, coverage, rights, and release acceptance remain pending."
    ),
)
def docks_usace_bts(context, config: DocksConfig, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    result = capture(staging_storage, version, replay_version=config.replay_version or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)],
    )
    context.add_output_metadata({
        "feature_count": result["feature_count"],
        "source_id_sha256": result["source_id_sha256"],
        "source_schema_sha256": result["source_schema_sha256"],
        "source_acceptance": False,
        "rights_accepted": False,
        "coverage_accepted": False,
        "release_accepted": False,
    })
    return result


@asset_check(asset=docks_usace_bts, blocking=True)
def docks_source_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "observed_complete_layer_rows": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "scope": candidate.get("source_coverage_scope", "missing"),
        "historical_hifld_equivalence_accepted": False,
    })


@asset_check(asset=docks_usace_bts, blocking=True)
def docks_source_geometry_quality(candidate: dict) -> AssetCheckResult:
    nulls = candidate.get("null_geometry_count")
    invalid_coordinates = candidate.get("invalid_coordinate_count")
    passed = (candidate.get("source_population_complete") is True
              and type(nulls) is int and nulls == 0
              and type(invalid_coordinates) is int and invalid_coordinates == 0)
    return AssetCheckResult(passed=passed, metadata={
        "null_source_geometries": nulls if nulls is not None else "missing",
        "source_coordinates_outside_epsg4269_geographic_domain": invalid_coordinates if invalid_coordinates is not None else "missing",
        "rows_preserved_in_candidate": candidate.get("feature_count", 0),
        "crs": "EPSG:4269",
        "source_schema_acceptance": False,
        "coverage_acceptance": False,
    })


@asset_check(asset=docks_usace_bts, blocking=False)
def docks_release_acceptance_pending(candidate: dict) -> AssetCheckResult:
    accepted = candidate.get("release_accepted") is True
    return AssetCheckResult(passed=accepted, metadata={
        "status": "accepted" if accepted else "pending source/coverage/rights/baseline decisions",
        "source_accepted": False,
        "rights_accepted": False,
        "coverage_accepted": False,
        "release_accepted": False,
    })


docks_draft_job = define_asset_job(
    "docks_usace_bts_draft_refresh",
    selection=(AssetSelection.assets(docks_usace_bts)
               | AssetSelection.checks(docks_usace_bts)),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)
docks_draft_schedule = ScheduleDefinition(
    name="docks_usace_bts_draft_refresh_schedule",
    job=docks_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
