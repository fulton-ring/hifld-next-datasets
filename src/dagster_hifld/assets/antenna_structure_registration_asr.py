"""Explicit stopped-refresh Dagster asset for FCC ASR antenna structure registrations."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.antenna_structure_registration_asr import (
    DATASET_SLUG, EXPECTED_PUBLISHED_ROWS, FILE_SLUG,
    capture_antenna_structure_registration_asr,
)


class AntennaStructureRegistrationConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fcc",
    compute_kind="http",
    description=(
        "FCC ULS weekly public access ASR tower registration file r_tower.zip: one row per CO coordinate row "
        "joined to its RA registration row and unique EN owner entity row, with every declared RA/CO/EN field "
        "retained verbatim plus DMS-derived latdec/londec and WKB Point geometry in native NAD83 (EPSG:4269) "
        "(203245 coordinate rows observed over 198010 registrations). Registered structures, not a census of "
        "physical towers. Unapproved candidate; baseline, contract, coverage, rights, and release decisions "
        "remain pending."
    ),
)
def antenna_structure_registration_asr(context, config: AntennaStructureRegistrationConfig,
                                       staging_storage: StagingStorageResource) -> dict:
    result = capture_antenna_structure_registration_asr(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=antenna_structure_registration_asr, blocking=True)
def antenna_structure_registration_asr_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_PUBLISHED_ROWS
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


antenna_structure_registration_asr_draft_job = define_asset_job(
    "antenna_structure_registration_asr_draft_refresh",
    selection=AssetSelection.assets(antenna_structure_registration_asr),
)
antenna_structure_registration_asr_draft_schedule = ScheduleDefinition(
    name="antenna_structure_registration_asr_draft_refresh_schedule",
    job=antenna_structure_registration_asr_draft_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [antenna_structure_registration_asr]
ASSET_CHECKS = [antenna_structure_registration_asr_complete]
JOBS = [antenna_structure_registration_asr_draft_job]
SCHEDULES = [antenna_structure_registration_asr_draft_schedule]
