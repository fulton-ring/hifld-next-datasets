"""Dagster draft assets for the DISDI public MIRTA DoD Sites points release."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.mirta_dod_sites_points import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    FILE_SLUG,
    capture_mirta_dod_sites_points,
)


class MirtaDodSitesPointsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="department_of_defense",
    compute_kind="http",
    description=(
        "DISDI Program (OASD EI&E) public FY24 MIRTA release, DoD Sites points layer, from the "
        "producer's own acq.osd.mil ZIP. Draft only; the DHS GII continuation of the former HIFLD "
        "Open layer is credential-gated, the published primary key is degenerate, and source, "
        "schema/ID, coverage, rights and release acceptance remain pending."
    ),
)
def mirta_dod_sites_points(context, config: MirtaDodSitesPointsConfig,
                           staging_storage: StagingStorageResource) -> dict:
    result = capture_mirta_dod_sites_points(
        staging_storage,
        version="v20250902",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=mirta_dod_sites_points, blocking=True)
def mirta_dod_sites_points_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("candidate_ready") is not None
              and candidate.get("source_id_degenerate") is True)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_field": "mirtaLocationsIdpk",
            "source_id_degenerate": candidate.get("source_id_degenerate"),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_release": candidate.get("source_release", "missing"),
            "source_release_date": candidate.get("source_release_date", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


@asset_check(asset=mirta_dod_sites_points, blocking=True)
def mirta_dod_sites_points_geometry_quality(candidate: dict) -> AssetCheckResult:
    invalid = candidate.get("source_invalid_geometry_count")
    nulls = candidate.get("source_null_geometry_count")
    return AssetCheckResult(
        passed=invalid == 0 and nulls == 0,
        metadata={"source_invalid_geometry_count": invalid,
                  "source_null_geometry_count": nulls,
                  "geometry_repaired": False,
                  "source_release_accepted": False},
    )


mirta_dod_sites_points_draft_job = define_asset_job(
    "mirta_dod_sites_points_draft_refresh",
    selection=AssetSelection.assets(mirta_dod_sites_points),
)
mirta_dod_sites_points_draft_schedule = ScheduleDefinition(
    name="mirta_dod_sites_points_draft_refresh_schedule",
    job=mirta_dod_sites_points_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [mirta_dod_sites_points]
ASSET_CHECKS = [mirta_dod_sites_points_complete, mirta_dod_sites_points_geometry_quality]
JOBS = [mirta_dod_sites_points_draft_job]
SCHEDULES = [mirta_dod_sites_points_draft_schedule]
