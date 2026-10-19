"""Dagster entry points for the EPA FRS/RMP facilities draft."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_rmp_facilities import (
    DATASET_SLUG, FILE_SLUG, SOURCE_COUNT, SOURCE_FIELD_COUNT, capture_epa_rmp_facilities,
)


class EpaRmpFacilitiesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "EPA_GEO hosted EPA FRS view for the Risk Management Plan program. "
        "Stopped source candidate only; release and public schema contracts remain unapproved."
    ),
)
def epa_emergency_response_rmp_facilities(
    context, config: EpaRmpFacilitiesConfig, staging_storage: StagingStorageResource
) -> dict:
    version = build_version_id(context)
    result = capture_epa_rmp_facilities(
        staging_storage, version=version, capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_emergency_response_rmp_facilities, blocking=True)
def epa_emergency_response_rmp_facilities_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == SOURCE_COUNT
        and candidate.get("source_null_geometry_count") == 0
        and candidate.get("source_invalid_geometry_count") == 0
        and candidate.get("unique_object_id_count") == SOURCE_COUNT
        and candidate.get("unique_registry_id_count") == SOURCE_COUNT
        and candidate.get("null_registry_id_count") == 0
        and candidate.get("duplicate_registry_id_count") == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "expected_source_population": SOURCE_COUNT,
            "expected_source_fields": SOURCE_FIELD_COUNT,
            "unique_registry_id_count": candidate.get("unique_registry_id_count", 0),
            "null_registry_id_count": candidate.get("null_registry_id_count", 0),
            "duplicate_registry_id_count": candidate.get("duplicate_registry_id_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_rmp_facilities_draft_job = define_asset_job(
    "epa_emergency_response_rmp_facilities_draft_refresh",
    selection=AssetSelection.assets(epa_emergency_response_rmp_facilities),
)
epa_rmp_facilities_draft_schedule = ScheduleDefinition(
    name="epa_emergency_response_rmp_facilities_draft_schedule",
    job=epa_rmp_facilities_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
