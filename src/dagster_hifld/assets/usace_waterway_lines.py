"""Explicit approval-gated ingest asset for USACE waterway network lines."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.sources.usace_waterway_lines import DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_usace_waterway_lines
from dagster_hifld.resources import StagingStorageResource

class UsaceWaterwayLinesConfig(Config):
    capture_directory: str = ""

@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usace", compute_kind="http",
       description=("BTS NTAD listed USACE National Waterway Network layer 1. Captures the native Z/M/BLOB FileGDB as an unapproved candidate; all source, contract, rights, mapping, and release decisions remain false."))
def usace_navigable_waterway_network_lines(context, config: UsaceWaterwayLinesConfig,
                                           staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_waterway_lines(staging_storage, capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions("publish_dataset_file_version", [f"{DATASET_SLUG}/{FILE_SLUG}/{result['version']}"])
    context.add_output_metadata(result)
    return result

@asset_check(asset=usace_navigable_waterway_network_lines, blocking=True)
def usace_navigable_waterway_network_lines_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("candidate_ready") is True and candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("native_Z_M_and_BLOB_preserved") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "native_filegdb_Z_M_BLOB_preserved": candidate.get("native_Z_M_and_BLOB_preserved", False),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })

usace_waterway_lines_draft_job = define_asset_job("usace_waterway_lines_draft_refresh", selection=AssetSelection.assets(usace_navigable_waterway_network_lines))
usace_waterway_lines_draft_schedule = ScheduleDefinition(name="usace_waterway_lines_draft_refresh_schedule", job=usace_waterway_lines_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
