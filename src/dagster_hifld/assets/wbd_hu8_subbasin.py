"""Explicit stopped-refresh Dagster asset for the USGS WBD 8-digit HU (Subbasin)."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.wbd_hu8_subbasin import (
    DATASET_SLUG, EXPECTED_COUNT, EXPECTED_EMPTY_GEOMETRY_COUNT,
    EXPECTED_INVALID_GEOMETRY_COUNT, EXPECTED_INVALID_OBJECTIDS, FILE_SLUG,
    capture_wbd_hu8_subbasin,
)


class WbdHu8SubbasinConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS The National Map 'wbd' MapServer layer 4 '8-digit HU  (Subbasin)' "
        "(2456 national subbasins, service data refreshed October, 2026) — the DHS "
        "HIFLD Open crosswalk's Open REST Service successor for the retired HIFLD WBD "
        "feature service. Unapproved candidate; succession, freshness, schema/ID "
        "contract, coverage, rights, and release decisions remain pending."
    ),
)
def wbd_hu8_subbasin(context, config: WbdHu8SubbasinConfig,
                     staging_storage: StagingStorageResource) -> dict:
    result = capture_wbd_hu8_subbasin(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=wbd_hu8_subbasin, blocking=True)
def wbd_hu8_subbasin_complete(candidate: dict) -> AssetCheckResult:
    # The producer's own baseline carries exactly the pinned OGC-invalid ring
    # self-intersections (source-invalid); the candidate written geometry must
    # itself be valid (candidate-invalid must be zero after the pinned minimal
    # repair). The two counts are distinct properties of distinct artifacts.
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_invalid_geometry_count")
              == EXPECTED_INVALID_GEOMETRY_COUNT
              and candidate.get("source_invalid_geometry_ids") == EXPECTED_INVALID_OBJECTIDS
              and candidate.get("candidate_invalid_geometry_count") == 0
              and candidate.get("source_empty_geometry_count") == EXPECTED_EMPTY_GEOMETRY_COUNT
              and candidate.get("source_distinct_huc8_count") == EXPECTED_COUNT)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_huc8_sha256": candidate.get("source_huc8_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_vertex_count": candidate.get("source_vertex_count", 0),
        "source_invalid_geometry_count": candidate.get("source_invalid_geometry_count"),
        "source_invalid_geometry_ids": candidate.get("source_invalid_geometry_ids"),
        "source_repair_record_count": candidate.get("source_repair_record_count", 0),
        "candidate_invalid_geometry_count": candidate.get("candidate_invalid_geometry_count"),
        "source_empty_geometry_count": candidate.get("source_empty_geometry_count"),
        "source_coverage_scope": (
            "Complete observed USGS The National Map WBD MapServer layer 4 "
            "'8-digit HU  (Subbasin)' (national 8-digit hydrologic unit population; "
            "the crosswalk-named successor of the retired HIFLD WBD feature service)."),
        "geometry_repair_accepted": False,
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


wbd_hu8_subbasin_draft_job = define_asset_job(
    "wbd_hu8_subbasin_draft_refresh",
    selection=AssetSelection.assets(wbd_hu8_subbasin),
)
wbd_hu8_subbasin_draft_schedule = ScheduleDefinition(
    name="wbd_hu8_subbasin_draft_refresh_schedule",
    job=wbd_hu8_subbasin_draft_job,
    cron_schedule="0 9 6,20 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [wbd_hu8_subbasin]
ASSET_CHECKS = [wbd_hu8_subbasin_complete]
JOBS = [wbd_hu8_subbasin_draft_job]
SCHEDULES = [wbd_hu8_subbasin_draft_schedule]
