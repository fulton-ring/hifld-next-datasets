"""USGS OFR 2012-1205 coal-fields capture; refresh is proposal-only and stopped by default."""

from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, Field, MetadataValue, RunRequest, SkipReason,
    asset, asset_check, define_asset_job, schedule,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.us_coal_fields import (
    DATASET_SLUG, DBF_RECORDS, FILE_SLUG, capture_validated,
)

_DESCRIPTION = (
    "US Coal Fields, captured from the producer publication USGS Open-File Report "
    "2012-1205 Downloads package (GIS.zip member 'GIS/Updated Coal Fields/Coal_Fields.*'; "
    "602 coal-field polygons, EPSG:4269). Unapproved candidate; edition, HIFLD continuity, "
    "baseline, contract, coverage, rights and release decisions remain pending."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=_DESCRIPTION,
    config_schema={
        "retained_source_version": Field(
            str, is_required=False, default_value="",
            description="Optional retained source version for explicit evidence-only replay; omitted runs fetch a fresh snapshot.",
        ),
    },
)
def us_coal_fields(context, staging_storage: StagingStorageResource) -> dict:
    if isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local or staging_storage.bucket:
        raise ValueError("us_coal_fields_draft_requires_isolated_local_staging")
    version = build_version_id(context)
    if staging_storage.list_keys(DATASET_SLUG, FILE_SLUG, version):
        raise ValueError("source_capture_version_already_exists")
    retained_version = context.op_config.get("retained_source_version") or None
    result = capture_validated(staging_storage, version, retained_source_version=retained_version)
    context.add_output_metadata({
        "feature_count": MetadataValue.int(result["feature_count"]),
        "source_records_sha256": MetadataValue.text(result["records_sha256"]),
        "native_parquet_sha256": MetadataValue.text(result["parquet_sha256"]),
        "release_accepted": MetadataValue.bool(False),
    })
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    return {**result, "version": version, "partition_key": partition_key}


INGEST_ASSETS = [us_coal_fields]


@asset_check(asset=us_coal_fields, blocking=True)
def us_coal_fields_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == DBF_RECORDS and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_record_sha256": candidate.get("records_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


ASSET_CHECKS = [us_coal_fields_complete]

us_coal_fields_draft_refresh_job = define_asset_job(
    "us_coal_fields_draft_refresh",
    selection=AssetSelection.assets(us_coal_fields),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=us_coal_fields_draft_refresh_job,
    cron_schedule="0 7 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def us_coal_fields_draft_refresh_schedule(context):
    storage = context.resources.staging_storage
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        return SkipReason("US Coal Fields draft refresh only runs against isolated local staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(run_key=f"us-coal-fields-draft-{day}", tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"})


JOBS = [us_coal_fields_draft_refresh_job]
SCHEDULES = [us_coal_fields_draft_refresh_schedule]
