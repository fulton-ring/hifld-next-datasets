"""Official BTS NCFO ferry terminals, newest edition discovered from data.bts.gov."""
from pathlib import Path

from dagster import (
    AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset,
    define_asset_job, schedule,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferry_terminals import (
    DATASET_SLUG, FILE_SLUG, require_local, stage_ferry_terminals_candidate,
)


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="bts", compute_kind="http",
       description=("Newest BTS NCFO ferry terminals file discovered from data.bts.gov. "
                    "All published terminal columns and native point coordinates; draft, unapproved."),
       config_schema={"capture_dir": Field(str, default_value="")})
def bts_ferry_terminals(context, staging_storage: StagingStorageResource) -> dict:
    require_local(staging_storage)
    capture = context.op_config.get("capture_dir", "")
    result = stage_ferry_terminals_candidate(
        staging_storage,
        capture_dir=Path(capture) if capture else None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


bts_ferry_terminals_draft_refresh_job = define_asset_job(
    "bts_ferry_terminals_draft_refresh",
    selection=AssetSelection.assets(bts_ferry_terminals),
)


@schedule(job=bts_ferry_terminals_draft_refresh_job, cron_schedule="0 13 1 * *",
          execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
def bts_ferry_terminals_draft_refresh_schedule(context, staging_storage: StagingStorageResource):
    # Source-availability/integrity poll; BTS does not publish on a fixed monthly cadence.
    require_local(staging_storage)
    return RunRequest()
