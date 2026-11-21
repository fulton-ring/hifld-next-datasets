"""Stopped draft-refresh asset for the PAD-US Protection Mechanism Category view."""

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
from dagster_hifld.sources.pad_us_protection_mechanism_category import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_pad_us_protection_mechanism_category,
)


class PadUsProtectionMechanismCategoryConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "USGS GAP PAD-US Protection Mechanism Category FeatureServer layer 0 "
        "(656,986 source features observed 2026-11-14) as the maintained "
        "USGS-linked publication named by the HIFLD inventory entry "
        "pad-us-protection-mechanism-category: the combined view "
        "Protection_Mechanism_Category_PADUS/FeatureServer (layer "
        "PADUS4_1ProtMechCat) linked by USGS's PAD-US Web Services page, whose "
        "layer copyright cites PAD-US 4.1 (2025) and whose data was last edited "
        "2025-06-04. Resumable gzip keyset-streamed complete capture; unapproved "
        "faithful candidate. Schema/ID contract, rights and release decisions "
        "remain pending."
    ),
)
def pad_us_protection_mechanism_category(
    context,
    config: PadUsProtectionMechanismCategoryConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_pad_us_protection_mechanism_category(
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


def pad_us_protection_mechanism_category_complete_contract(candidate: dict) -> bool:
    """Require witnessed coverage and an internally consistent observed null count."""
    count = candidate.get("source_reported_count")
    nulls = candidate.get("source_null_geometry_count")
    return (
        type(count) is int and count > 0
        and candidate.get("feature_count") == count
        and candidate.get("source_population_complete") is True
        and type(nulls) is int and 0 <= nulls <= count
    )


@asset_check(asset=pad_us_protection_mechanism_category, blocking=True)
def pad_us_protection_mechanism_category_complete(candidate: dict) -> AssetCheckResult:
    passed = pad_us_protection_mechanism_category_complete_contract(candidate)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_reported_count": candidate.get("source_reported_count", "missing"),
            "source_null_geometry_count": candidate.get("source_null_geometry_count", "missing"),
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


pad_us_protection_mechanism_category_draft_job = define_asset_job(
    "pad_us_protection_mechanism_category_draft_refresh",
    selection=AssetSelection.assets(pad_us_protection_mechanism_category),
)
pad_us_protection_mechanism_category_draft_schedule = ScheduleDefinition(
    name="pad_us_protection_mechanism_category_draft_refresh_schedule",
    job=pad_us_protection_mechanism_category_draft_job,
    cron_schedule="0 9 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

pad_us_protection_mechanism_category_drift = register_approved_baseline_drift(
    pad_us_protection_mechanism_category, "pad_us_protection_mechanism_category_drift")

INGEST_ASSETS = [pad_us_protection_mechanism_category]
ASSET_CHECKS = [pad_us_protection_mechanism_category_complete,
                pad_us_protection_mechanism_category_drift]
JOBS = [pad_us_protection_mechanism_category_draft_job]
SCHEDULES = [pad_us_protection_mechanism_category_draft_schedule]
