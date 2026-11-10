"""EPA FRS EIA-860 generation layer draft assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_power_generation import DATASET_SLUG, FILE_SLUG, capture_epa_power_generation


class EpaPowerGenerationConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Exact EPA_GEO FRS/EIA-860 power generation point layer. Candidate only: "
        "the service describes EIA input as of June 2014, so freshness and release remain blocked."
    ),
)
def epa_frs_eia_860_power_generation_facilities_by_source(
    context, config: EpaPowerGenerationConfig, staging_storage: StagingStorageResource
) -> dict:
    version = build_version_id(context)
    result = capture_epa_power_generation(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_eia_860_power_generation_facilities_by_source, blocking=True)
def epa_frs_eia_860_power_generation_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 2328 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_power_generation_draft_job = define_asset_job(
    "epa_power_generation_draft_refresh",
    selection=AssetSelection.assets(epa_frs_eia_860_power_generation_facilities_by_source),
)
epa_power_generation_draft_schedule = ScheduleDefinition(
    name="epa_power_generation_draft_refresh_schedule",
    job=epa_power_generation_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
