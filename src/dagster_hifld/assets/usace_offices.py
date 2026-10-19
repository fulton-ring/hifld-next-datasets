"""Stopped, draft-only source asset for USACE district/division/research-lab offices."""
import os

from dagster import AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_offices import SLUG, stage_usace_offices


@asset(
    key=AssetKey(["ingest", SLUG, SLUG]),
    group_name="usace",
    compute_kind="http",
    description=("USACE District, Division and Research Lab Locations. This data set does not include "
                 "field offices. Candidate official ArcGIS Online layer; release and DHS contract acceptance "
                 "remain pending."),
)
def usace_offices(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    result = stage_usace_offices(staging_storage, version=version, capture_dir=os.environ.get("HIFLD_USACE_OFFICES_CAPTURE_DIR"))
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(SLUG, SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_offices, name="source_quality", description="Blocking complete-row, schema, and native-geometry gate.", blocking=True)
def usace_offices_source_quality(usace_offices: dict) -> AssetCheckResult:
    errors = usace_offices.get("quality_errors", [])
    return AssetCheckResult(passed=usace_offices.get("quality_passed") is True and not errors,
        metadata={"row_count": usace_offices.get("rows", 0), "quality_errors": errors,
                  "source_release_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False})


usace_offices_draft_job = define_asset_job("usace_offices_draft_refresh", selection=AssetSelection.assets(usace_offices))
usace_offices_draft_schedule = ScheduleDefinition(name="usace_offices_draft_monthly", job=usace_offices_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
