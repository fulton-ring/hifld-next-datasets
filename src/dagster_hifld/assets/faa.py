"""FAA dataset assets."""

import pathlib

import datetime
import hashlib
import json
from dagster_hifld import aviation_extract

from dagster import (
    AssetKey, AssetSelection, DefaultScheduleStatus, Field, RunRequest, SkipReason,
    asset, define_asset_job, schedule,
)

from dagster_hifld.faa_airspace import ingest_candidate

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
    return ingest_candidate(
        staging_storage, pathlib.Path(capture_dir) if capture_dir else None
    )



@asset(
    key=AssetKey(["ingest", _AVIATION_DATASET_SLUG, _AVIATION_FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=_AVIATION_DESCRIPTION,
    config_schema={"capture_dir": Field(str, default_value="", is_required=False)},
)
def faa_aviation_facilities(context, staging_storage: StagingStorageResource) -> dict:
    aviation_extract._require_review_staging(staging_storage)
    capture_dir = context.op_config.get("capture_dir", "")
    if capture_dir:
        return _stage_aviation(pathlib.Path(capture_dir), staging_storage)
    # The live capture goes directly into durable local quarantine; failures and
    # partial response attempts survive without a temporary-directory cleanup.
    retained = pathlib.Path(staging_storage.local_dir) / staging_storage.prefix / "quarantine" / aviation_extract.SLUG / ("capture-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    capture = retained / "capture"
    try:
        aviation_extract.capture_live(capture)
        return _stage_aviation(capture, staging_storage)
    except Exception as error:
        retained.mkdir(parents=True, exist_ok=True)
        (retained / "status.json").write_text(json.dumps({"state": "blocked", "reason": str(error), "promotion_allowed": False}))
        raise


def _stage_aviation(capture, storage):
    aviation_extract._require_review_staging(storage)
    # Preserve invalid raw input before inspection; semantic identity binds code,
    # all input/receipt hashes and the draft physical-schema policy.
    try:
        audit = aviation_extract.inspect_capture(capture)
    except Exception:
        aviation_extract._preserve_for_review(capture, storage, "v0.0.0-invalid-capture")
        raise
    identity = {"raw_files": audit["raw_files"], "processing": aviation_extract.processing_identity()["builder_fingerprint"], "schema_policy": aviation_extract.PROPOSED_SCHEMA_SHA256}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return aviation_extract.stage_aviation_capture(capture, storage, "v0.0.0-draft-" + digest[:24])


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


faa_aviation_draft_refresh_job = define_asset_job(
    "faa_aviation_draft_refresh",
    selection=AssetSelection.assets(faa_aviation_facilities),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(job=faa_aviation_draft_refresh_job, cron_schedule="0 10 * * *",
          execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED,
          required_resource_keys={"staging_storage"})
def faa_aviation_draft_refresh_schedule(context):
    """Proposed daily local integrity poll; source release cadence remains28days."""
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("Draft aviation refresh requires isolated local review staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft aviation refresh requires explicit scheduled time")
    return RunRequest(run_key="aviation-draft-" + context.scheduled_execution_time.date().isoformat(),
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})
