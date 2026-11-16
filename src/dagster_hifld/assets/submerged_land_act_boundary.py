"""Explicit stopped-refresh Dagster asset for the BOEM SLA boundary layer."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.submerged_land_act_boundary import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_submerged_land_act_boundary,
)


class SubmergedLandActBoundaryConfig(Config):
    capture_directory: str = ""
    offline_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="boem",
    compute_kind="http",
    description=(
        "BOEM/BSEE national Marine Mineral Cadastre FeatureServer layer 8 'Submerged "
        "Lands Act Boundary' (5,746 line features observed 2026-11-14, EPSG:4269): the "
        "producer's own first-party publication of the SLA boundary product named by the "
        "DHS HIFLD Open crosswalk row (Open ID 1d939d74b3104da890d7a8517d4b885f), whose "
        "GeoSeas-hosted link no longer resolves. Unapproved candidate; freshness, "
        "baseline, schema/ID contract, coverage, rights and release decisions remain "
        "pending; the producer republishes this service and can resequence OBJECTIDs, "
        "which the bracketed capture refuses."
    ),
)
def submerged_land_act_boundary(context, config: SubmergedLandActBoundaryConfig,
                                staging_storage: StagingStorageResource) -> dict:
    result = capture_submerged_land_act_boundary(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        offline_capture_directory=config.offline_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=submerged_land_act_boundary, blocking=True)
def submerged_land_act_boundary_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = (isinstance(count, int) and count > 0
              and (EXPECTED_COUNT is None or count == EXPECTED_COUNT)
              and bool(candidate.get("candidate_ready"))
              and not candidate.get("release_accepted", False))
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": count,
        "null_geometry_count": candidate.get("null_geometry_count"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "baseline_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "source_authority_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


submerged_land_act_boundary_draft_job = define_asset_job(
    "submerged_land_act_boundary_draft_refresh",
    selection=AssetSelection.assets(submerged_land_act_boundary),
)
submerged_land_act_boundary_draft_schedule = ScheduleDefinition(
    name="submerged_land_act_boundary_draft_refresh_schedule",
    job=submerged_land_act_boundary_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [submerged_land_act_boundary]
ASSET_CHECKS = [submerged_land_act_boundary_complete]
JOBS = [submerged_land_act_boundary_draft_job]
SCHEDULES = [submerged_land_act_boundary_draft_schedule]
