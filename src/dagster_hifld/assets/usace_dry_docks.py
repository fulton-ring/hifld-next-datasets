"""USACE IENC Dry Docks native-source draft asset."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_dry_docks import (
    DATASET_SLUG, FILE_SLUG, capture_usace_dry_docks,
)


class UsaceDryDocksConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace", compute_kind="http",
    description=(
        "Official USACE IENC DRY_DOCK_AREA MapServer layer (two records observed in a local Ohio River area). "
        "This draft is not a national inventory; source release, historical baseline, schema/ID, coverage, "
        "and rights decisions remain pending."
    ),
)
def usace_dry_docks_usace_ienc(context, config: UsaceDryDocksConfig,
                                 staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_dry_docks(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_dry_docks_usace_ienc, blocking=True)
def usace_dry_docks_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 2 and candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "complete_observed_layer_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_coverage_extent": str(candidate.get("source_coverage_extent", "missing")),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    })


usace_dry_docks_draft_job = define_asset_job(
    "usace_dry_docks_draft_refresh",
    selection=AssetSelection.assets(usace_dry_docks_usace_ienc),
)
usace_dry_docks_draft_schedule = ScheduleDefinition(
    name="usace_dry_docks_draft_refresh_schedule",
    job=usace_dry_docks_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
