"""Stopped, source-only draft asset for the GSA IOLP Buildings candidate."""
from datetime import timezone

from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, RunRequest, ScheduleDefinition, SkipReason,
                     asset, asset_check, define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gsa_iolp_buildings import (
    DATASET_SLUG, FILE_SLUG, capture_gsa_iolp_buildings,
)


class GsaIolpBuildingsConfig(Config):
    version: str
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="gsa",
       compute_kind="http", description=(
           "Complete current GSA IOLP Buildings workbook from the GSA data.gov record and "
           "inventory.data.gov file store. Native 18-column schema, Location Code IDs and WGS84 "
           "points retained; every release, baseline and rights acceptance stays unresolved."))
def gsa_iolp_buildings(context, config: GsaIolpBuildingsConfig,
                       staging_storage: StagingStorageResource) -> dict:
    result = capture_gsa_iolp_buildings(staging_storage, version=config.version,
                                        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [
        build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata({key: value for key, value in result.items()
                                 if key not in {"dataset_slug", "file_slug"}})
    return result


@asset_check(asset=gsa_iolp_buildings, blocking=True,
             description="Require the complete native workbook population; no quality or "
                         "release acceptance is implied.")
def gsa_iolp_buildings_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("source_population_complete") is True and \
        isinstance(candidate.get("feature_count"), int) and 0 < candidate["feature_count"] <= 50_000
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_release_date": candidate.get("source_release_date", "unknown"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "baseline_accepted": False, "release_accepted": False,
    })


gsa_iolp_buildings_draft_job = define_asset_job(
    "gsa_iolp_buildings_draft_refresh",
    selection=AssetSelection.assets(gsa_iolp_buildings),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})


def _draft_request(context):
    if context.scheduled_execution_time is None:
        return SkipReason("A dated schedule context is required for an immutable candidate version")
    stamp = context.scheduled_execution_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    version = f"v0.0.0-research-{stamp}"
    return RunRequest(run_key=f"gsa-iolp-buildings-draft-{stamp}",
                      run_config={"ops": {gsa_iolp_buildings.op.name: {"config": {
                          "version": version, "capture_directory": ""}}}},
                      tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})


gsa_iolp_buildings_draft_schedule = ScheduleDefinition(
    name="gsa_iolp_buildings_draft_schedule",
    job=gsa_iolp_buildings_draft_job,
    cron_schedule="0 7 * * 3",
    execution_fn=_draft_request,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Stopped proposal for a weekly integrity capture after GSA's weekly publication "
                "cadence; no official release cadence or acceptance is claimed.",
)

INGEST_ASSETS = [gsa_iolp_buildings]
ASSET_CHECKS = [gsa_iolp_buildings_complete]
JOBS = [gsa_iolp_buildings_draft_job]
SCHEDULES = [gsa_iolp_buildings_draft_schedule]
