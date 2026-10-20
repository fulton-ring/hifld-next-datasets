"""NOAA dataset assets."""

import pathlib

from dagster import (
    AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest,
    SkipReason, asset, define_asset_job, schedule,
)

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.noaa_overview import SLUG as _OVERVIEW_SLUG, ingest_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key

_NOAA_MAPSERVER_BASE = (
    "https://maritimeboundaries.noaa.gov/arcgis/rest/services/"
    "MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer"
)
_TERRITORIAL_SEA_URL = (
    f"{_NOAA_MAPSERVER_BASE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
_TERRITORIAL_SEA_NAME = "12nm-territorial-sea.geojson"
_TERRITORIAL_SEA_DATASET_SLUG = "12nm-territorial-sea"
_TERRITORIAL_SEA_FILE_SLUG = "12nm-territorial-sea"
_EEZ_URL = f"{_NOAA_MAPSERVER_BASE}/3/query?where=1%3D1&outFields=*&f=geojson"
_EEZ_NAME = "200nm-eez-and-maritime-boundaries.geojson"
_EEZ_DATASET_SLUG = "200nm-eez-and-maritime-boundaries"
_EEZ_FILE_SLUG = "200nm-eez-and-maritime-boundaries"
_CONTIGUOUS_ZONE_URL = (
    f"{_NOAA_MAPSERVER_BASE}/2/query?where=1%3D1&outFields=*&f=geojson"
)
_CONTIGUOUS_ZONE_NAME = "24nm-contiguous-zone.geojson"
_CONTIGUOUS_ZONE_DATASET_SLUG = "24nm-contiguous-zone"
_CONTIGUOUS_ZONE_FILE_SLUG = "24nm-contiguous-zone"


@asset(
    key=AssetKey(["ingest", _TERRITORIAL_SEA_DATASET_SLUG, _TERRITORIAL_SEA_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="12NM Territorial Sea (HIFLD inventory). Publisher: National Oceanic and Atmospheric Administration. NOAA depicts on nautical charts the limits of the 12 NM Territorial Sea, 24 NM Contiguous Zone, and 200 NM EEZ from the U.S. normal baseline. Source: maritimeboundaries.noaa.gov.",
)
def noaa_12nm_territorial_sea(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_TERRITORIAL_SEA_URL, _TERRITORIAL_SEA_NAME)],
        _TERRITORIAL_SEA_DATASET_SLUG,
        _TERRITORIAL_SEA_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey(["ingest", _EEZ_DATASET_SLUG, _EEZ_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="200NM EEZ and Maritime Boundaries (HIFLD inventory). Publisher: NOAA. Outer limit of the U.S. Exclusive Economic Zone and maritime boundaries with adjacent countries; from the U.S. Baseline Committee. Source: maritimeboundaries.noaa.gov.",
)
def noaa_200nm_eez_and_maritime_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_EEZ_URL, _EEZ_NAME)],
        _EEZ_DATASET_SLUG,
        _EEZ_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey(["ingest", _CONTIGUOUS_ZONE_DATASET_SLUG, _CONTIGUOUS_ZONE_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="24NM Contiguous Zone (HIFLD inventory). Publisher: NOAA. Maritime limits from the U.S. normal baseline; part of NOAA OCS U.S. Maritime Limits & Boundaries. Source: maritimeboundaries.noaa.gov.",
)
def noaa_24nm_contiguous_zone(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_CONTIGUOUS_ZONE_URL, _CONTIGUOUS_ZONE_NAME)],
        _CONTIGUOUS_ZONE_DATASET_SLUG,
        _CONTIGUOUS_ZONE_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey(["ingest", _OVERVIEW_SLUG, _OVERVIEW_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description=(
        "NOAA OCS US Maritime Limits & Boundaries Overview (HIFLD inventory). "
        "The complete 260-feature polyline layer includes territorial/contiguous "
        "zones, EEZ and maritime boundaries, and land boundary features. The "
        "source is proposed and the historical mapping remains unapproved."
    ),
    config_schema={"capture_dir": Field(str, default_value="", is_required=False)},
)
def noaa_maritime_limits_overview(context, staging_storage: StagingStorageResource) -> dict:
    capture_dir = context.op_config.get("capture_dir", "")
    result = ingest_candidate(
        staging_storage, pathlib.Path(capture_dir) if capture_dir else None
    )
    partition_key = build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    return {**result, "partition_key": partition_key}


noaa_overview_draft_refresh_job = define_asset_job(
    "noaa_overview_draft_refresh",
    selection=AssetSelection.assets(noaa_maritime_limits_overview),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=noaa_overview_draft_refresh_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def noaa_overview_draft_refresh_schedule(context):
    """Proposed monthly local check; source update frequency is event driven."""
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("NOAA Overview refresh is limited to isolated local candidate storage")
    if context.scheduled_execution_time is None:
        return SkipReason("NOAA Overview refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(
        run_key=f"noaa-overview-draft-{day}",
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
    )
