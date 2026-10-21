"""Draft-only refresh for the current GCCA warehouse directory candidate."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset,
                     asset_check, define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gcca_warehouse_directory import (
    DATASET_SLUG, FILE_SLUG, capture_gcca_warehouses,
)


class GCCAWarehouseConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="gcca",
    compute_kind="http",
    description=(
        "Current GCCA Warehouse Facility directory, projected to U.S. country records as a changed-successor draft. "
        "Source CRS is undeclared; coordinates remain unassigned and native cataloging fails closed. "
        "Historical 2017 HIFLD continuity, coverage, rights, source acceptance, and release remain pending."
    ),
)
def gcca_warehouses_current(context, config: GCCAWarehouseConfig,
                            staging_storage: StagingStorageResource) -> dict:
    result = capture_gcca_warehouses(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=gcca_warehouses_current, blocking=True)
def gcca_warehouses_geometry_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("missing_geometry_count") == 0
              and candidate.get("candidate_crs_known") is True
              and candidate.get("quality_geometry_accepted") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_category_source_rows": candidate.get("source_population_count", 0),
        "projected_us_candidate_rows": candidate.get("feature_count", 0),
        "missing_geometry_rows_retained": candidate.get("missing_geometry_count", 0),
        "candidate_crs_known": candidate.get("candidate_crs_known") is True,
        "candidate_crs_mapping": candidate.get("candidate_crs_mapping", {"crs": None, "status": "unknown"}),
        "source_population_complete": candidate.get("source_population_complete") is True,
        "historical_2017_baseline_accepted": False,
        "source_accepted": False,
        "rights_accepted": False,
        "coverage_accepted": False,
        "release_accepted": False,
    })


gcca_warehouses_draft_job = define_asset_job(
    "gcca_warehouses_current_draft_refresh",
    selection=AssetSelection.assets(gcca_warehouses_current),
)
gcca_warehouses_draft_schedule = ScheduleDefinition(
    name="gcca_warehouses_current_draft_schedule",
    job=gcca_warehouses_draft_job,
    cron_schedule="0 8 1 * *",
    default_status=DefaultScheduleStatus.STOPPED,
)
