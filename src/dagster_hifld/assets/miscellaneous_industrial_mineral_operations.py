"""USGS National Atlas Miscellaneous Industrial Minerals Operations asset with explicit gates."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.miscellaneous_industrial_mineral_operations import (
    COUNT,
    capture_miscellaneous_industrial_mineral_operations,
)

DATASET_SLUG = FILE_SLUG = "miscellaneous-industrial-mineral-operations"


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS National Atlas Miscellaneous Industrial Minerals Operations (minop5x020, "
        "December 2005, 304 features from the 2003 AMIS survey). The producer publication "
        "is retired from its only official host (dds.cr.usgs.gov returns 404; the live "
        "USGS NGTOC FGDC metadata links only to that retired URL), so this ingest fails "
        "closed and refuses candidate construction until a maintainer accepts an official "
        "distribution of the retired publication or approves a documented successor scope "
        "for the live mrdata.usgs.gov mineplant product. Third-party archive snapshots are "
        "research context only, never pipeline inputs. Source identity, schema/ID contract, "
        "rights, and release decisions remain pending."
    ),
)
def miscellaneous_industrial_mineral_operations_usgs(
        context, staging_storage: StagingStorageResource) -> dict:
    result = capture_miscellaneous_industrial_mineral_operations(
        staging_storage, version=build_version_id(context))
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=miscellaneous_industrial_mineral_operations_usgs, blocking=True)
def miscellaneous_industrial_mineral_operations_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("feature_count") == COUNT)
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "id_sha256": candidate.get("source_id_sha256", "missing"),
        "record_sha256": candidate.get("source_record_sha256", "missing"),
    })


miscellaneous_industrial_mineral_operations_draft_job = define_asset_job(
    "miscellaneous_industrial_mineral_operations_draft_refresh",
    selection=AssetSelection.assets(miscellaneous_industrial_mineral_operations_usgs),
)
miscellaneous_industrial_mineral_operations_draft_schedule = ScheduleDefinition(
    name="miscellaneous_industrial_mineral_operations_draft_refresh_schedule",
    job=miscellaneous_industrial_mineral_operations_draft_job,
    cron_schedule="0 11 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
