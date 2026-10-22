"""Explicit stopped-refresh Dagster asset for USACE IENC lateral beacons."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_lateral_beacons import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_usace_lateral_beacons,
)


class UsaceLateralBeaconsConfig(Config):
    capture_directory: str = ""
    capture_max_bytes: int = 16_000_000
    response_max_bytes: int = 8_000_000
    layer_response_max_bytes: int = 8_000_000


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE IENC LATERAL_BEACON_POINT MapServer layer 14 (3,406 IDs observed in bounded metadata query). "
        "Regional inland-chart draft; historical equivalence, rights, and release remain unaccepted."
    ),
)
def usace_lateral_beacons_usace_ienc(context, config: UsaceLateralBeaconsConfig,
                                      staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_lateral_beacons(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        capture_max_bytes=config.capture_max_bytes,
        response_max_bytes=config.response_max_bytes,
        layer_response_max_bytes=config.layer_response_max_bytes,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_lateral_beacons_usace_ienc, blocking=True)
def usace_lateral_beacons_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "bts_item_succession_accepted": False,
        "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


usace_lateral_beacons_draft_job = define_asset_job(
    "usace_lateral_beacons_draft_refresh",
    selection=AssetSelection.assets(usace_lateral_beacons_usace_ienc),
)
usace_lateral_beacons_draft_schedule = ScheduleDefinition(
    name="usace_lateral_beacons_draft_refresh_schedule",
    job=usace_lateral_beacons_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
