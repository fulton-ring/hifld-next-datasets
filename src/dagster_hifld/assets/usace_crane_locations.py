"""USACE IENC Crane Locations native-source draft assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_crane_locations import (
    DATASET_SLUG, FILE_SLUG, capture_usace_crane_locations,
)


class UsaceCraneLocationsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Complete observed official IENC CRANES_POINT layer 8 (148 locations). "
        "Candidate is unapproved and does not claim a nationwide crane inventory."
    ),
)
def usace_crane_locations_ienc(context, config: UsaceCraneLocationsConfig,
                               staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_crane_locations(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_crane_locations_ienc, blocking=True)
def usace_crane_locations_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 148 and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_layer_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "drift_guard": candidate.get("drift_guard", "missing"),
    })


@asset_check(asset=usace_crane_locations_ienc, blocking=True)
def usace_crane_locations_quality(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("source_null_geometry_count") == 0
              and candidate.get("source_empty_geometry_count") == 0
              and candidate.get("source_invalid_geometry_count") == 0
              and bool(candidate.get("source_schema_sha256")))
    return AssetCheckResult(passed=passed, metadata={
        "null_geometries": candidate.get("source_null_geometry_count", "missing"),
        "empty_geometries": candidate.get("source_empty_geometry_count", "missing"),
        "invalid_geometries": candidate.get("source_invalid_geometry_count", "missing"),
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
    })


@asset_check(asset=usace_crane_locations_ienc, blocking=False)
def usace_crane_locations_release(candidate: dict) -> AssetCheckResult:
    accepted = candidate.get("release_accepted") is True
    return AssetCheckResult(passed=accepted, metadata={
        "approval_status": "accepted" if accepted else "pending",
        "candidate_claimed_release_accepted": accepted,
        "promotion_enforced_by": "publish/verify independent exact-object release approval registry",
        "source_release_accepted": candidate.get("source_release_accepted", False),
        "rights_accepted": candidate.get("rights_accepted", False),
        "mapping_approved": candidate.get("mapping_approved", False),
    })


usace_crane_locations_draft_job = define_asset_job(
    "usace_crane_locations_draft_refresh",
    selection=(AssetSelection.assets(usace_crane_locations_ienc)
               | AssetSelection.checks(usace_crane_locations_ienc)),
)
usace_crane_locations_draft_schedule = ScheduleDefinition(
    name="usace_crane_locations_draft_refresh_schedule",
    job=usace_crane_locations_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
