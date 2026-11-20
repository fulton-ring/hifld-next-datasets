"""Stopped draft-refresh asset for the USGS 3DHP_all Waterbody successor."""

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
from dagster_hifld.sources.nhd_waterbody import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_nhd_waterbody,
)


class NhdWaterbodyConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS 3DHP_all FeatureServer layer 60 'Waterbody' (the complete layer: "
        "River, Canal, Lake, Ocean or Great Lake) as the maintained successor "
        "named by the HIFLD inventory entry nhd/waterbody. Resumable "
        "keyset-streamed complete capture with adaptive page bisection over the "
        "dense roster head; unapproved faithful candidate. Succession coverage, "
        "freshness, schema/ID contract, rights and release decisions remain "
        "pending."
    ),
)
def nhd_waterbody(
    context,
    config: NhdWaterbodyConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_nhd_waterbody(
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


@asset_check(asset=nhd_waterbody, blocking=True)
def nhd_waterbody_complete(candidate: dict) -> AssetCheckResult:
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


nhd_waterbody_drift = register_approved_baseline_drift(
    nhd_waterbody, "nhd_waterbody_drift")


nhd_waterbody_draft_job = define_asset_job(
    "nhd_waterbody_draft_refresh",
    selection=AssetSelection.assets(nhd_waterbody),
)
nhd_waterbody_draft_schedule = ScheduleDefinition(
    name="nhd_waterbody_draft_refresh_schedule",
    job=nhd_waterbody_draft_job,
    cron_schedule="0 9 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [nhd_waterbody]
ASSET_CHECKS = [nhd_waterbody_complete, nhd_waterbody_drift]
JOBS = [nhd_waterbody_draft_job]
SCHEDULES = [nhd_waterbody_draft_schedule]
