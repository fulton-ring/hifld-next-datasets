"""Draft-only registered NIFC WFIGS Interagency Fire Perimeters source and gate."""
from pathlib import Path

from dagster import (AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, Field,
                     RunRequest, asset, asset_check, define_asset_job, schedule)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.wfigs_interagency_fire_perimeters import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_wfigs_interagency_fire_perimeters,
)


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="nifc", compute_kind="http",
       description=("NIFC WFIGS Interagency Fire Perimeters producer feature service "
                    "(live continuously refreshed data; one coherent snapshot per run, "
                    "completeness proven by before/after count/ids/edit-witness equality). "
                    "Faithful draft; publication unapproved."),
       config_schema={"capture_dir": Field(str, default_value="")})
def wfigs_interagency_fire_perimeters(context, staging_storage: StagingStorageResource) -> dict:
    capture = context.op_config.get("capture_dir", "")
    result = capture_wfigs_interagency_fire_perimeters(
        staging_storage, capture_directory=Path(capture) if capture else None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=wfigs_interagency_fire_perimeters, blocking=True)
def wfigs_interagency_fire_perimeters_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("feature_count", 0) > 0
              and candidate.get("reused") is not None)
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_record_sha256": candidate.get("source_record_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


wfigs_interagency_fire_perimeters_draft_job = define_asset_job(
    "wfigs_interagency_fire_perimeters_draft_refresh",
    selection=AssetSelection.keys(wfigs_interagency_fire_perimeters.key),
    config={"ops": {wfigs_interagency_fire_perimeters.op.name: {"config": {"capture_dir": ""}}}})


@schedule(job=wfigs_interagency_fire_perimeters_draft_job, cron_schedule="0 9 1 * *",
          execution_timezone="America/New_York",
          default_status=DefaultScheduleStatus.STOPPED,
          required_resource_keys={"staging_storage"})
def wfigs_interagency_fire_perimeters_draft_refresh_schedule(context):
    # Monthly integrity poll of live data; the schedule stays STOPPED pending approvals.
    return RunRequest(run_config={"ops": {
        wfigs_interagency_fire_perimeters.op.name: {"config": {"capture_dir": ""}}}})


INGEST_ASSETS = [wfigs_interagency_fire_perimeters]
ASSET_CHECKS = [wfigs_interagency_fire_perimeters_complete]
JOBS = [wfigs_interagency_fire_perimeters_draft_job]
SCHEDULES = [wfigs_interagency_fire_perimeters_draft_refresh_schedule]
