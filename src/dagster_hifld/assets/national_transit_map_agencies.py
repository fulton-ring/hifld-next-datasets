"""Explicit draft source asset for the BTS National Transit Map Agencies."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_transit_map_agencies import (
    DATASET_SLUG, FILE_SLUG, capture_national_transit_map_agencies,
)


class NationalTransitMapAgenciesConfig(Config):
    capture_directory: str = ""
    replay_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="bts",
    compute_kind="http",
    description=(
        "Official USDOT_BTS National Transit Map Agencies layer compiled "
        "2026-09-23. Captures an immutable draft; acceptance and release remain blocked."
    ),
)
def bts_national_transit_map_agencies(context, config: NationalTransitMapAgenciesConfig,
    staging_storage: StagingStorageResource) -> dict:
    result = capture_national_transit_map_agencies(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        replay_capture_directory=config.replay_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=bts_national_transit_map_agencies, blocking=True)
def bts_national_transit_map_agencies_candidate_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 1239
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_release": "2026-09-23",
        "source_release_accepted": False,
        "historical_baseline_acceptance_pending": True,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


@asset_check(asset=bts_national_transit_map_agencies, blocking=True)
def bts_national_transit_map_agencies_release_gate(candidate: dict) -> AssetCheckResult:
    accepted = all(candidate.get(flag) is True for flag in (
        "source_release_accepted", "schema_contract_accepted", "id_contract_accepted",
        "freshness_accepted", "rights_accepted", "mapping_approved", "release_accepted",
    )) and candidate.get("historical_baseline_acceptance_pending") is False
    return AssetCheckResult(passed=accepted, metadata={
        "source_release_accepted": candidate.get("source_release_accepted", False),
        "historical_baseline_acceptance_pending": candidate.get("historical_baseline_acceptance_pending", True),
        "schema_contract_accepted": candidate.get("schema_contract_accepted", False),
        "id_contract_accepted": candidate.get("id_contract_accepted", False),
        "freshness_accepted": candidate.get("freshness_accepted", False),
        "rights_accepted": candidate.get("rights_accepted", False),
        "mapping_approved": candidate.get("mapping_approved", False),
        "release_accepted": candidate.get("release_accepted", False),
    })


bts_national_transit_map_agencies_draft_job = define_asset_job(
    "bts_national_transit_map_agencies_draft_refresh",
    selection=AssetSelection.assets(bts_national_transit_map_agencies),
)
bts_national_transit_map_agencies_draft_schedule = ScheduleDefinition(
    name="bts_national_transit_map_agencies_draft_refresh_schedule",
    job=bts_national_transit_map_agencies_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
