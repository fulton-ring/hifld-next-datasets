"""USGS live fire/EMS service draft ingest; no HIFLD release acceptance."""
from pathlib import Path
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, asset, define_asset_job, schedule
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ems_stations import SLUG, require_local, stage_candidate

@asset(key=AssetKey(["ingest", SLUG, SLUG]), group_name="usgs", compute_kind="http",
       description="USGS The National Map Corps live Fire Stations/EMS Stations layer 51 research draft; native EPSG:3857 XY and source IDs retained. Not the historical DataLumos release and not approved for production.",
       config_schema={"capture_dir": Field(str, default_value="")})
def usgs_ems_stations(context, staging_storage: StagingStorageResource) -> dict:
    require_local(staging_storage)
    capture = context.op_config.get("capture_dir", "")
    return stage_candidate(staging_storage, capture_dir=Path(capture) if capture else None,
                           code_commit=context.run_tags.get("hifld/source_code_commit"))

usgs_ems_stations_draft_refresh_job = define_asset_job(
    "usgs_ems_stations_draft_refresh", selection=AssetSelection.keys(usgs_ems_stations.key),
    config={"ops": {usgs_ems_stations.op.name: {"config": {"capture_dir": ""}}}},
    tags={"hifld/draft": "true", "hifld/promotion": "disabled"},
    description="Refresh the unapproved local USGS layer 51 candidate; no publication or acceptance.")

@schedule(job=usgs_ems_stations_draft_refresh_job, cron_schedule="0 7 1 * *", execution_timezone="America/New_York",
          default_status=DefaultScheduleStatus.STOPPED, required_resource_keys={"staging_storage"})
def usgs_ems_stations_draft_refresh_schedule(context):
    require_local(context.resources.staging_storage)
    return RunRequest(run_config={"ops": {usgs_ems_stations.op.name: {"config": {"capture_dir": ""}}}})
