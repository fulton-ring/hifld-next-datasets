"""Explicit stopped-refresh Dagster asset for FCC ULS cellular towers."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.cellular_towers import (
    DATASET_SLUG, EXPECTED_ACTIVE_LOCATIONS, FILE_SLUG, capture_cellular_towers,
)


class CellularTowersConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fcc",
    compute_kind="http",
    description=(
        "FCC ULS weekly public access complete cellular licenses file l_cell.zip: location records of active "
        "Cellular - 47 CFR Part 22 licenses under the historical HIFLD Cellular Towers column contract "
        "(24362 active locations observed). ULS filed-site population, not a census of physical towers. "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def cellular_towers(context, config: CellularTowersConfig,
                    staging_storage: StagingStorageResource) -> dict:
    result = capture_cellular_towers(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=cellular_towers, blocking=True)
def cellular_towers_complete(candidate: dict) -> AssetCheckResult:
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


cellular_towers_draft_job = define_asset_job(
    "cellular_towers_draft_refresh",
    selection=AssetSelection.assets(cellular_towers),
)
cellular_towers_draft_schedule = ScheduleDefinition(
    name="cellular_towers_draft_refresh_schedule",
    job=cellular_towers_draft_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
