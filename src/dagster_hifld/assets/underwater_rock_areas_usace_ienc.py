"""Explicit stopped-refresh Dagster asset for USACE IENC underwater rock areas."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.underwater_rock_areas_usace_ienc import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_underwater_rock_areas_usace_ienc,
)


class UnderwaterRockAreasUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE IENC UNDERWATER_ROCK_AREA service layer 53 (300 features observed). The linked BTS catalog item is a proposed successor to an earlier item; catalog continuity and layer edition remain unaccepted. "
        "Unapproved candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def underwater_rock_areas_usace_ienc(context, config: UnderwaterRockAreasUsaceIencConfig,
                                     staging_storage: StagingStorageResource) -> dict:
    result = capture_underwater_rock_areas_usace_ienc(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=underwater_rock_areas_usace_ienc, blocking=True)
def underwater_rock_areas_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True
              and candidate.get("geometry_valid_count") == EXPECTED_COUNT
              and candidate.get("geometry_invalid_count") == 0
              and candidate.get("geometry_empty_count") == 0)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "geometry_valid_count": candidate.get("geometry_valid_count", "missing"),
        "geometry_invalid_count": candidate.get("geometry_invalid_count", "missing"),
        "geometry_empty_count": candidate.get("geometry_empty_count", "missing"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "drift_guard": candidate.get("drift_guard", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


underwater_rock_areas_usace_ienc_draft_job = define_asset_job(
    "underwater_rock_areas_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(underwater_rock_areas_usace_ienc),
)
underwater_rock_areas_usace_ienc_draft_schedule = ScheduleDefinition(
    name="underwater_rock_areas_usace_ienc_draft_refresh_schedule",
    job=underwater_rock_areas_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [underwater_rock_areas_usace_ienc]
ASSET_CHECKS = [underwater_rock_areas_usace_ienc_complete]
JOBS = [underwater_rock_areas_usace_ienc_draft_job]
SCHEDULES = [underwater_rock_areas_usace_ienc_draft_schedule]
