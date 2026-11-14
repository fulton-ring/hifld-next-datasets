"""USGS 2023 NSHM uniform-hazard ground motion (SA5P0 5% in 50 years, B/C) draft assets."""

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usgs_uniform_hazard_ground_motion import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_INVALID_GEOMETRY_ROWS,
    EXPECTED_MULTIPOLYGON_COUNT,
    EXPECTED_NODATA_ROW_COUNT,
    EXPECTED_NULL_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_uniform_hazard_ground_motion,
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS 2023 50-State National Seismic Hazard Model uniform-hazard ground motion "
        "polygons: 5.0-second spectral acceleration, 5% in 50 years, NEHRP site class B/C "
        "(VS30 = 760 m/s), captured losslessly from the producer ScienceBase archive "
        "(DOI 10.5066/P9GNPCOD). Candidate extraction only; freshness/schema/ID/rights "
        "and release approvals remain pending."
    ),
)
def us_sa5p0_5pct50yrs_bc_poly(context, staging_storage: StagingStorageResource) -> dict:
    result = capture_uniform_hazard_ground_motion(staging_storage)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=us_sa5p0_5pct50yrs_bc_poly, blocking=True)
def us_sa5p0_5pct50yrs_bc_poly_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_null_geometry_count") == EXPECTED_NULL_GEOMETRY_COUNT
              and candidate.get("source_invalid_geometry_count") == len(EXPECTED_INVALID_GEOMETRY_ROWS))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "multipolygon_count": EXPECTED_MULTIPOLYGON_COUNT,
            "nodata_rows": EXPECTED_NODATA_ROW_COUNT,
            "invalid_geometry_rows": len(EXPECTED_INVALID_GEOMETRY_ROWS),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


us_sa5p0_5pct50yrs_bc_poly_draft_job = define_asset_job(
    "us_sa5p0_5pct50yrs_bc_poly_draft_refresh",
    selection=AssetSelection.assets(us_sa5p0_5pct50yrs_bc_poly),
)
us_sa5p0_5pct50yrs_bc_poly_draft_schedule = ScheduleDefinition(
    name="us_sa5p0_5pct50yrs_bc_poly_draft_refresh_schedule",
    job=us_sa5p0_5pct50yrs_bc_poly_draft_job,
    cron_schedule="0 7 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [us_sa5p0_5pct50yrs_bc_poly]
ASSET_CHECKS = [us_sa5p0_5pct50yrs_bc_poly_complete]
JOBS = [us_sa5p0_5pct50yrs_bc_poly_draft_job]
SCHEDULES = [us_sa5p0_5pct50yrs_bc_poly_draft_schedule]
