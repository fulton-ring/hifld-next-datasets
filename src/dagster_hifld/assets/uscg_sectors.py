"""Explicit stopped-refresh Dagster asset for USCG Sectors."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.uscg_sectors import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_uscg_sectors,
)


class UscgSectorsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="uscg",
    compute_kind="http",
    description=(
        "USCG-owned USCG FSLTP Portal services9 'USCG Sectors (Public View)' FeatureServer "
        "layer 0 (36 sectors observed, data edited January 2026) after the DHS HIFLD Open "
        "crosswalk's GII successor was verified dead. Unapproved candidate; baseline, contract, "
        "coverage, rights, and release decisions remain pending."
    ),
)
def uscg_sectors(context, config: UscgSectorsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_uscg_sectors(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=uscg_sectors, blocking=True)
def uscg_sectors_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "geometry_repair_accepted": False,
    })


uscg_sectors_draft_job = define_asset_job(
    "uscg_sectors_draft_refresh",
    selection=AssetSelection.assets(uscg_sectors),
)
uscg_sectors_draft_schedule = ScheduleDefinition(
    name="uscg_sectors_draft_refresh_schedule",
    job=uscg_sectors_draft_job,
    cron_schedule="0 8 5,19 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [uscg_sectors]
ASSET_CHECKS = [uscg_sectors_complete]
JOBS = [uscg_sectors_draft_job]
SCHEDULES = [uscg_sectors_draft_schedule]
