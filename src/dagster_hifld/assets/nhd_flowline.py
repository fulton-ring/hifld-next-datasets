"""Stopped draft-refresh asset for the USGS 3DHP_all Flowline successor."""

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nhd_flowline import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_nhd_flowline,
)


class NhdFlowlineConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS 3DHP_all FeatureServer layer 50 'Flowline' (the complete layer: the "
        "national 3DHP flowline network) as the maintained successor named by the "
        "HIFLD inventory entry nhd/flowline. Resumable keyset-streamed complete "
        "capture over the producer's own count answers and ascending OBJECTID "
        "walk; unapproved faithful candidate. Succession coverage, freshness, "
        "schema/ID contract, rights and release decisions remain pending."
    ),
)
def nhd_flowline(
    context,
    config: NhdFlowlineConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_nhd_flowline(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=nhd_flowline, blocking=True)
def nhd_flowline_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("source_reported_count")
    passed = (candidate.get("source_population_complete") is True
              and type(count) is int and count > 0
              and candidate.get("feature_count") == count)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": count if count is not None else "missing",
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "succession_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


nhd_flowline_drift = register_approved_baseline_drift(
    nhd_flowline, "nhd_flowline_drift")


nhd_flowline_draft_job = define_asset_job(
    "nhd_flowline_draft_refresh",
    selection=AssetSelection.assets(nhd_flowline),
)
nhd_flowline_draft_schedule = ScheduleDefinition(
    name="nhd_flowline_draft_refresh_schedule",
    job=nhd_flowline_draft_job,
    cron_schedule="0 9 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [nhd_flowline]
ASSET_CHECKS = [nhd_flowline_complete, nhd_flowline_drift]
JOBS = [nhd_flowline_draft_job]
SCHEDULES = [nhd_flowline_draft_schedule]
