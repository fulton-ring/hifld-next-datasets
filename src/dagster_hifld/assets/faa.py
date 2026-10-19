"""FAA dataset assets."""

import pathlib

from dagster import (
    AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, SkipReason,
    asset, define_asset_job, schedule,
)

from dagster_hifld.faa_airspace import ingest_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

_AIRSPACE_DATASET_SLUG = "airspace-boundaries-1"
_AIRSPACE_FILE_SLUG = "airspace-boundaries-1"
_AIRSPACE_DESCRIPTION = (
    "Airspace Boundaries (HIFLD inventory). Publisher: FAA Aeronautical "
    "Information Services. Vector airspace boundary data published every eight "
    "weeks by USDOT/FAA. Source: FAA ADDS Open Data "
    "(adds-faa.opendata.arcgis.com)."
)
_AIRSPACE_BASE = (
    "https://adds-faa.opendata.arcgis.com/api/v3/datasets/"
    "67885972e4e940b2aa6d74024901c561_0/downloads/data"
)
_AVIATION_DATASET_SLUG = "aviation-facilities"
_AVIATION_FILE_SLUG = "aviation-facilities"
_AVIATION_DESCRIPTION = (
    "Aviation Facilities (HIFLD inventory). Publisher: FAA. Part of USDOT/BTS "
    "NTAD; updated every 28 days from FAA. Geographic point database of "
    "official/operational aerodromes in the U.S. and territories. Source: BTS "
    "geodata.bts.gov (FAA NASR-derived)."
)
_AVIATION_BASE = (
    "https://geodata.bts.gov/api/v3/datasets/"
    "1551114f78e34d8395fd77bf41cd8a80_0/downloads/data"
)


@asset(
    key=AssetKey(["ingest", _AIRSPACE_DATASET_SLUG, _AIRSPACE_FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=_AIRSPACE_DESCRIPTION,
    config_schema={"capture_dir": Field(str, default_value="", is_required=False)},
)
def faa_airspace_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    capture_dir = context.op_config.get("capture_dir", "")
    result = ingest_candidate(
        staging_storage, pathlib.Path(capture_dir) if capture_dir else None
    )
    key = build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [key])
    return {**result, "partition_key": key}



@asset(
    key=AssetKey(["ingest", _AVIATION_DATASET_SLUG, _AVIATION_FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=_AVIATION_DESCRIPTION,
)
def faa_aviation_facilities(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(f"{_AVIATION_BASE}?format=fgdb&spatialRefId=4326", "Aviation_Facilities.gdb.zip")],
        _AVIATION_DATASET_SLUG,
        _AVIATION_FILE_SLUG,
        version,
        staging_storage,
    )

# Proposed local research poll only; coordinator registers these with Definitions.
# The schedule remains stopped and the asset independently refuses remote storage.

faa_airspace_draft_refresh_job = define_asset_job(
    "faa_airspace_draft_refresh",
    selection=AssetSelection.assets(faa_airspace_boundaries),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=faa_airspace_draft_refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def faa_airspace_draft_refresh_schedule(context):
    """Daily 06:00 New York proposal; activation is a separate human decision."""
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("Draft FAA refresh is limited to isolated local quarantine storage")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft FAA refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(
        run_key=f"faa-airspace-draft-{day}",
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
    )
