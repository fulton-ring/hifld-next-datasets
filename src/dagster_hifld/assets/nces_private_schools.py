"""Draft-only ingestion for the pinned NCES EDGE 2023-24 private-school snapshot."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nces_private_schools import DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_private_schools


class PrivateSchoolsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="education",
    compute_kind="http",
    description="Pinned NCES EDGE 2023-24 private-school locations; changed successor candidate, all acceptance gates pending.",
)
def nces_private_schools_2023_24(context, config: PrivateSchoolsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_private_schools(
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


@asset_check(asset=nces_private_schools_2023_24, blocking=True)
def nces_private_schools_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("source_population_complete") is True
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "release_accepted": False,
        },
    )


nces_private_schools_draft_job = define_asset_job(
    "nces_private_schools_draft_refresh",
    selection=AssetSelection.assets(nces_private_schools_2023_24),
)
nces_private_schools_draft_schedule = ScheduleDefinition(
    name="nces_private_schools_draft_refresh_schedule",
    job=nces_private_schools_draft_job,
    cron_schedule="0 8 1 3,9 *",
    default_status=DefaultScheduleStatus.STOPPED,
)
