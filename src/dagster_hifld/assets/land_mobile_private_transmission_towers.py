"""Explicit stopped-refresh Dagster asset for FCC ULS Land Mobile (Private) towers."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.land_mobile_private_transmission_towers import (
    DATASET_SLUG, FILE_SLUG, capture_land_mobile_private_transmission_towers,
)


class LandMobilePrivateTowersConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fcc",
    compute_kind="http",
    description=(
        "FCC ULS weekly public access complete Land Mobile {Private} licenses file l_LMpriv.zip: location records "
        "with filed coordinates of active private land mobile radio licenses under the historical HIFLD Land Mobile "
        "Private Transmission Towers column contract. Completeness is witnessed at capture time against the "
        "payload's own counts member; the counts member file creation date is the observed edition marker. Active "
        "filed locations without a complete coordinate pair, and active coordinate-bearing filings in the FCC "
        "do-not-convert local-datum territories (state codes MP, UM) whose coordinates have no FCC-published NAD83 "
        "conversion, are excluded from the published population and preserved in the audit. ULS filed-site "
        "population, not a census of physical towers. Unapproved candidate; baseline, contract, coverage, rights, "
        "and release decisions remain pending."
    ),
)
def land_mobile_private_transmission_towers(context, config: LandMobilePrivateTowersConfig,
                                            staging_storage: StagingStorageResource) -> dict:
    result = capture_land_mobile_private_transmission_towers(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=land_mobile_private_transmission_towers, blocking=True)
def land_mobile_private_transmission_towers_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("source_reported_count")
    passed = (type(count) is int and count > 0
              and candidate.get("feature_count") == count
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_reported_count": count if count is not None else "missing",
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


land_mobile_private_transmission_towers_draft_job = define_asset_job(
    "land_mobile_private_transmission_towers_draft_refresh",
    selection=AssetSelection.assets(land_mobile_private_transmission_towers),
)
land_mobile_private_transmission_towers_draft_schedule = ScheduleDefinition(
    name="land_mobile_private_transmission_towers_draft_refresh_schedule",
    job=land_mobile_private_transmission_towers_draft_job,
    cron_schedule="0 10 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

land_mobile_private_transmission_towers_drift = register_approved_baseline_drift(
    land_mobile_private_transmission_towers, "land_mobile_private_transmission_towers_drift")

INGEST_ASSETS = [land_mobile_private_transmission_towers]
ASSET_CHECKS = [land_mobile_private_transmission_towers_complete,
                land_mobile_private_transmission_towers_drift]
JOBS = [land_mobile_private_transmission_towers_draft_job]
SCHEDULES = [land_mobile_private_transmission_towers_draft_schedule]
