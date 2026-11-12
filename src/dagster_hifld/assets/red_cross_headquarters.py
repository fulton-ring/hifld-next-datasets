"""Stopped, source-only draft schedule for the Red Cross HQ candidate."""
from datetime import timezone

from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, RunRequest, ScheduleDefinition, SkipReason, asset,
                     asset_check, define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.red_cross_headquarters import (
    DATASET_SLUG, FILE_SLUG, capture_and_stage,
)


class RedCrossHeadquartersConfig(Config):
    version: str
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="red_cross",
       compute_kind="http", description=(
           "Complete observed 226-row Red Cross headquarters candidate. Exact publisher authority, FY26/FY27 effective edition, historical mapping and release remain unresolved; source-only local staging is allowed."))
def red_cross_headquarters(context, config: RedCrossHeadquartersConfig,
                           staging_storage: StagingStorageResource) -> dict:
    result = capture_and_stage(staging_storage, config.version,
                               config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [
        build_publish_partition_key(DATASET_SLUG, FILE_SLUG, config.version)])
    context.add_output_metadata({key: value for key, value in result.items()
                                 if key not in {"dataset_slug", "file_slug"}})
    return result


@asset_check(asset=red_cross_headquarters, blocking=True,
             description="Require a complete captured native layer; this check does not accept source authority or quality.")
def red_cross_headquarters_source_population(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 226 and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "source_population_complete": candidate.get("source_population_complete", False),
        "feature_count": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "native_crs": candidate.get("native_crs", "unknown"),
        "source_authority_accepted": False, "source_release_accepted": False,
        "baseline_accepted": False, "rights_accepted": False, "release_accepted": False,
    })


red_cross_headquarters_draft_job = define_asset_job(
    "red_cross_headquarters_draft_refresh",
    selection=AssetSelection.assets(red_cross_headquarters),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


def _draft_request(context):
    if context.scheduled_execution_time is None:
        return SkipReason("A dated schedule context is required for an immutable candidate version")
    stamp = context.scheduled_execution_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    version = f"v0.0.0-research-{stamp}"
    return RunRequest(run_key=f"red-cross-hq-draft-{stamp}",
        run_config={"ops": {red_cross_headquarters.op.name: {"config": {
            "version": version, "capture_directory": ""}}}},
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})

red_cross_headquarters_draft_schedule = ScheduleDefinition(
    name="red_cross_headquarters_draft_schedule",
    job=red_cross_headquarters_draft_job,
    cron_schedule="0 8 1 * *",
    execution_fn=_draft_request,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Stopped proposal for a source-only monthly integrity capture; no official release cadence or acceptance is claimed.",
)
