"""Explicit stopped-refresh Dagster asset for the FHWA TMAS stations NTAD layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.tmas_stations import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_tmas_stations,
)


class TmasStationsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bts",
    compute_kind="http",
    description=(
        "FHWA Travel Monitoring Analysis System (TMAS) Stations via the delegated BTS NTAD "
        "FeatureServer (item a016bbaee4794daca873135a5cf4164d, owner USDOT_BTS; 8060 point "
        "stations observed, 2025-compiled/2026-03-23-published NTAD edition). "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def tmas_stations(context, config: TmasStationsConfig,
                  staging_storage: StagingStorageResource) -> dict:
    result = capture_tmas_stations(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=tmas_stations, blocking=True)
def tmas_stations_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_null_geometry_count", 1) == 0 \
        and candidate.get("source_invalid_geometry_count", 1) == 0
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_null_geometry_count": candidate.get("source_null_geometry_count", "missing"),
        "source_invalid_geometry_count": candidate.get("source_invalid_geometry_count", "missing"),
        "candidate_ready": candidate.get("candidate_ready", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


tmas_stations_draft_job = define_asset_job(
    "tmas_stations_draft_refresh",
    selection=AssetSelection.assets(tmas_stations),
)
tmas_stations_draft_schedule = ScheduleDefinition(
    name="tmas_stations_draft_refresh_schedule",
    job=tmas_stations_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [tmas_stations]
ASSET_CHECKS = [tmas_stations_complete]
JOBS = [tmas_stations_draft_job]
SCHEDULES = [tmas_stations_draft_schedule]
