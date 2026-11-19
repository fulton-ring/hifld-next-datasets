"""Registered, stopped local refresh for the current Census Census_Tract source."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.census_tracts import (
    DATASET_SLUG, FILE_SLUG, FEATURE_COUNT_RANGE,
    stage_census_tracts,
)


class CensusTractsConfig(Config):
    archive_path: str = ""
    receipt_path: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="local_file_geodatabase",
    description=(
        "Streams the complete Census_Tract layer from the newest published national Census substate GDB "
        "(edition resolved from the retained capture receipt) to native GeoParquet. "
        "This local reconstruction remains unapproved; its refresh schedule is stopped."
    ),
)
def census_bureau_tracts(context, config: CensusTractsConfig,
                               staging_storage: StagingStorageResource) -> dict:
    if not config.archive_path or not config.receipt_path:
        raise ValueError("explicit_retained_archive_and_receipt_required")
    result = stage_census_tracts(
        staging_storage, archive_path=config.archive_path, receipt_path=config.receipt_path,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=census_bureau_tracts, blocking=True)
def census_tracts_source_complete(candidate: dict) -> AssetCheckResult:
    feature_count = candidate.get("feature_count")
    passed = (
        isinstance(feature_count, int)
        and FEATURE_COUNT_RANGE[0] <= feature_count <= FEATURE_COUNT_RANGE[1]
        and candidate.get("source_population_complete") is True
        and isinstance(candidate.get("source_archive_bytes"), int) and candidate["source_archive_bytes"] > 0
        and bool(candidate.get("source_archive_sha256"))
        and candidate.get("native_value_parity") is True
        and bool(candidate.get("source_id_sha256"))
        and bool(candidate.get("native_parquet_sha256"))
        and bool(candidate.get("code_identity_scheme"))
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "native_id_sha256": candidate.get("source_id_sha256", "missing"),
        "native_parquet_sha256": candidate.get("native_parquet_sha256", "missing"),
        "native_value_parity": candidate.get("native_value_parity", False),
        "code_identity_scheme": candidate.get("code_identity_scheme", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


census_tracts_draft_job = define_asset_job(
    "census_tracts_draft_refresh",
    selection=AssetSelection.assets(census_bureau_tracts),
)
census_tracts_draft_schedule = ScheduleDefinition(
    name="census_tracts_draft_refresh_schedule",
    job=census_tracts_draft_job,
    cron_schedule="0 8 1 9 *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)


ASSET_CHECKS = [census_tracts_source_complete]
INGEST_ASSETS = [census_bureau_tracts]
JOBS = [census_tracts_draft_job]
SCHEDULES = [census_tracts_draft_schedule]
