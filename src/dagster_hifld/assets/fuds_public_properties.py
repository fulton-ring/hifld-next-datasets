"""Draft Dagster assets for USACE FUDS Public Properties point capture."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_public_properties import DATASET_SLUG, FILE_SLUG, EXPECTED_COUNT, capture_fuds_public_properties


class FudsPublicPropertiesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE maintained FUDS Data MapServer layer 1, FUDS Property Points. "
        "Candidate extraction only; release date, freshness, rights, and schema/ID acceptance remain pending."
    ),
)
def usace_fuds_public_properties(context, config: FudsPublicPropertiesConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_fuds_public_properties(
        staging_storage,
        version="v20261019",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_fuds_public_properties, blocking=True)
def usace_fuds_public_properties_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT and candidate.get("candidate_ready") is not None and candidate.get("source_null_geometry_count") == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_null_geometry_count": candidate.get("source_null_geometry_count", 0),
            "technical_geometry_quality_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


fuds_public_properties_draft_job = define_asset_job(
    "fuds_public_properties_draft_refresh",
    selection=AssetSelection.assets(usace_fuds_public_properties),
)
fuds_public_properties_draft_schedule = ScheduleDefinition(
    name="fuds_public_properties_draft_refresh_schedule",
    job=fuds_public_properties_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
