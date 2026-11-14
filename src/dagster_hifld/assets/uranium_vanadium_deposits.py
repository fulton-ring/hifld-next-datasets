"""Stopped refresh asset for the official USGS MRDS uranium/vanadium candidate."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.uranium_vanadium_deposits import (
    DATASET_SLUG, EXPECTED_BYTES, EXPECTED_SHA256, EXPECTED_SUBSET_COUNT, FILE_SLUG,
    candidate_version, capture_uranium_vanadium_deposits,
)


class UraniumVanadiumDepositsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "Token-exact Uranium/Vanadium subset of the complete USGS MRDS point digest (mrds-trim.zip, "
        "16340 of 304632 records observed). The subset predicate and the digest-to-HIFLD equivalence "
        "are unapproved draft contracts; baseline, coverage, rights, and release decisions remain pending."
    ),
)
def uranium_vanadium_deposits(context, config: UraniumVanadiumDepositsConfig,
                              staging_storage: StagingStorageResource) -> dict:
    result = capture_uranium_vanadium_deposits(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=uranium_vanadium_deposits, blocking=True)
def uranium_vanadium_deposits_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_SUBSET_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("source_archive_bytes") == EXPECTED_BYTES
        and candidate.get("source_archive_sha256") == EXPECTED_SHA256
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "complete_digest_feature_count": candidate.get("complete_digest_feature_count", "missing"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "subset_roster_sha256": candidate.get("subset_roster_sha256", "missing"),
        "subset_record_sha256": candidate.get("subset_record_sha256", "missing"),
        "source_archive_sha256": candidate.get("source_archive_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


uranium_vanadium_deposits_draft_job = define_asset_job(
    "uranium_vanadium_deposits_draft_refresh",
    selection=AssetSelection.assets(uranium_vanadium_deposits),
)
uranium_vanadium_deposits_draft_schedule = ScheduleDefinition(
    name="uranium_vanadium_deposits_draft_refresh_schedule",
    job=uranium_vanadium_deposits_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [uranium_vanadium_deposits]
ASSET_CHECKS = [uranium_vanadium_deposits_source_complete]
JOBS = [uranium_vanadium_deposits_draft_job]
SCHEDULES = [uranium_vanadium_deposits_draft_schedule]
