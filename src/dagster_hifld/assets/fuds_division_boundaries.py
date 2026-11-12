"""Dagster entrypoint for the unapproved FUDS division boundary capture."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset,
                     asset_check, define_asset_job)
from dagster_hifld.download import build_version_id
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_division_boundaries import (
    DATASET_SLUG, FILE_SLUG, capture_fuds_division_boundaries,
)


class FudsDivisionBoundariesConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usace", compute_kind="http",
       description="Complete native USACE FUDS program division polygon capture; release remains gated.")
def fuds_program_division_boundaries(context, config: FudsDivisionBoundariesConfig,
                                     staging_storage: StagingStorageResource) -> dict:
    result = capture_fuds_division_boundaries(staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None)
    context.add_output_metadata(result)
    return result


@asset_check(asset=fuds_program_division_boundaries, blocking=True)
def fuds_program_division_boundaries_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = isinstance(count, int) and count > 0
    return AssetCheckResult(passed=passed, metadata={"complete_population_captured":passed,
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})


fuds_division_draft_job = define_asset_job("fuds_division_draft_refresh",
    selection=AssetSelection.assets(fuds_program_division_boundaries))
fuds_division_draft_schedule = ScheduleDefinition(
    name="fuds_division_draft_refresh_schedule", job=fuds_division_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED)
